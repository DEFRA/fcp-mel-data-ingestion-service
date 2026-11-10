const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const crypto = require("node:crypto");
const { execFileSync } = require("node:child_process");

const { parseCsv } = require(
  path.join(process.env.MEL_SCRIPT_DIR, "pilot-csv.cjs")
);

const database = db.getSiblingDB("fcp-mel-data-ingestion-service");
database.getMongo().setReadPref("primary");

const projectId = process.env.MEL_PROJECT_ID;
const limit = Number(process.env.MEL_FILE_LIMIT);

if (
  process.env.SERVICE !== "fcp-mel-data-ingestion-service" ||
  process.env.ENVIRONMENT !== "dev" ||
  process.env.APPROVED_FOR_DEV !== "yes" ||
  !/^[a-z0-9][a-z0-9_-]*$/.test(projectId || "") ||
  !Number.isInteger(limit) ||
  limit < 1 ||
  limit > 30
) {
  throw new Error("Invalid or unapproved execution context.");
}

const bucket = "dev-fcp-mel-ingestion-files-c63f2";
const prefix = `projects/${projectId}/`;
const uploader = "https://cdp-uploader.dev.cdp-int.defra.cloud";
const maxBytes = 20 * 1024 * 1024;

const files = database.getCollection("ingestion_files");
const records = database.getCollection("staging_records");
const runs = database.getCollection("ingestion_runs");

const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "mel-ingest-"));
fs.chmodSync(workDir, 0o700);

const runId = crypto.randomUUID();
let runStarted = false;
let runStatus = "complete";
let exitFailure = false;

const totals = {
  objectsExamined: 0,
  attempted: 0,
  completed: 0,
  skipped: 0,
  failed: 0
};

function fail(code) {
  const error = new Error(code);
  error.pilotCode = code;
  throw error;
}

function safeCode(error) {
  return error.pilotCode || "UNEXPECTED_OPERATION_ERROR";
}

function execute(command, args, errorCode) {
  try {
    return execFileSync(command, args, {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      timeout: 120000,
      maxBuffer: 8 * 1024 * 1024
    });
  } catch {
    // Do not expose raw responses, signed URLs or business contents.
    fail(errorCode);
  }
}

function awsJson(args) {
  const output = execute(
    "aws",
    [...args, "--output", "json"],
    "AWS_OPERATION_FAILED"
  );

  try {
    return JSON.parse(output);
  } catch {
    fail("INVALID_AWS_RESPONSE");
  }
}

function getStatus(uploadId) {
  if (!/^[a-f0-9-]{36}$/i.test(uploadId || "")) {
    fail("INVALID_UPLOAD_ID");
  }

  const output = execute(
    "curl",
    [
      "--fail",
      "--silent",
      "--show-error",
      "--connect-timeout", "10",
      "--max-time", "30",
      "--proxy", "http://localhost:3128",
      `${uploader}/status/${uploadId}`
    ],
    "UPLOADER_STATUS_UNAVAILABLE"
  );

  try {
    return JSON.parse(output);
  } catch {
    fail("INVALID_UPLOADER_RESPONSE");
  }
}

function fileResults(value, output = []) {
  if (value && typeof value === "object") {
    if (Object.prototype.hasOwnProperty.call(value, "fileStatus")) {
      output.push(value);
    }
    for (const child of Object.values(value)) {
      fileResults(child, output);
    }
  }
  return output;
}

function sourceFromStatus(status, key, uploadId) {
  const matching = fileResults(status.form).filter(item =>
    item.fileStatus === "complete" &&
    item.hasError !== true &&
    item.s3Bucket === bucket &&
    item.s3Key === key
  );

  if (matching.length !== 1) fail("FILE_NOT_CONFIRMED_COMPLETE");

  const result = matching[0];
  const metadata = status.metadata || {};
  const filename = metadata.sourceName || metadata.originalFilename;

  if (
    metadata.projectId !== projectId ||
    typeof filename !== "string" ||
    !/\.csv$/i.test(filename)
  ) {
    fail("PROJECT_OR_FILENAME_INVALID");
  }

  for (const field of [
    "sourceDriveId",
    "sourceItemId",
    "sourceETag",
    "sourceFolderId",
    "sourceFolderName"
  ]) {
    if (typeof metadata[field] !== "string" || !metadata[field]) {
      fail("SOURCE_METADATA_MISSING");
    }
  }

  if (!/^[A-Za-z0-9+/]{43}=$/.test(result.checksumSha256 || "")) {
    fail("CHECKSUM_MISSING_OR_INVALID");
  }

  return {
    projectId,
    sourceFilename: filename,
    sourceDriveId: metadata.sourceDriveId,
    sourceItemId: metadata.sourceItemId,
    sourceETag: metadata.sourceETag,
    sourceFolderId: metadata.sourceFolderId,
    sourceFolderName: metadata.sourceFolderName,
    sourceBucket: bucket,
    sourceKey: key,
    sourceChecksumSha256: result.checksumSha256,
    uploadId
  };
}

function identityFor(source) {
  // Same source item and identical bytes are one ingestion.
  // eTag is deliberately not part of this identity.
  return crypto.createHash("sha256")
    .update(JSON.stringify([
      source.projectId,
      source.sourceDriveId,
      source.sourceItemId,
      source.sourceChecksumSha256
    ]))
    .digest("hex");
}

function addEvent(key, status, code, fileId = null) {
  runs.updateOne(
    { _id: runId },
    {
      $push: {
        events: {
          sourceKey: key,
          fileId,
          status,
          code,
          at: new Date()
        }
      },
      $set: { totals }
    }
  );
}

function processObject(object) {
  const key = object.Key;
  let fileId = null;
  let claimed = false;
  let countedAttempt = false;
  const localFile = path.join(workDir, "input.csv");

  totals.objectsExamined++;

  try {
    const head = awsJson([
      "s3api", "head-object",
      "--bucket", bucket,
      "--key", key
    ]);

    if (
      !Number.isSafeInteger(head.ContentLength) ||
      head.ContentLength < 1 ||
      head.ContentLength > maxBytes
    ) {
      fail("EMPTY_OR_OVERSIZED_OBJECT");
    }

    const location = {
      key,
      etag: head.ETag,
      size: head.ContentLength
    };

    // Use durable metadata for objects already registered.
    // ETag is an object-change marker here, NOT our content checksum.
    const knownLocation = files.findOne({
      projectId,
      locations: {
        $elemMatch: location
      }
    });

    if (knownLocation) {
      totals.skipped++;
      addEvent(
        key,
        "skipped",
        knownLocation.status === "complete"
          ? "ALREADY_COMPLETE"
          : "EXISTING_BATCH_REQUIRES_REVIEW",
        knownLocation._id
      );
      return;
    }

    const uploadId = head.Metadata?.uploadid;
    const source = sourceFromStatus(getStatus(uploadId), key, uploadId);
    fileId = identityFor(source);

    const existing = files.findOne({ _id: fileId });

    if (existing) {
      files.updateOne(
        { _id: fileId },
        { $addToSet: { locations: location } }
      );

      totals.skipped++;
      addEvent(
        key,
        "skipped",
        existing.status === "complete"
          ? "DUPLICATE_SOURCE_CONTENT"
          : "EXISTING_BATCH_REQUIRES_REVIEW",
        fileId
      );
      return;
    }

    totals.attempted++;
    countedAttempt = true;

    const startedAt = new Date();

    // Persist source metadata before parsing/writing rows.
    files.insertOne({
      _id: fileId,
      projectId,
      source,
      status: "processing",
      runId,
      startedAt,
      locations: [location],
      validationStatus: "not-business-validated"
    });

    claimed = true;

    execute(
      "aws",
      [
        "s3", "cp",
        `s3://${bucket}/${key}`,
        localFile,
        "--only-show-errors"
      ],
      "S3_DOWNLOAD_FAILED"
    );

    const bytes = fs.readFileSync(localFile);

    if (bytes.length > maxBytes) fail("DOWNLOADED_OBJECT_TOO_LARGE");

    const actualHash = crypto.createHash("sha256")
      .update(bytes)
      .digest("base64");

    if (actualHash !== source.sourceChecksumSha256) {
      fail("CHECKSUM_MISMATCH");
    }

    const parsed = parseCsv(bytes);

    // Parsing and structural checks finish before data rows are written.
    for (let start = 0; start < parsed.rows.length; start += 250) {
      const documents = parsed.rows.slice(start, start + 250)
        .map((data, offset) => ({
          _id: `${fileId}:${start + offset + 1}`,
          projectId,
          fileId,
          runId,
          sourceFilename: source.sourceFilename,
          sourceRecordNumber: start + offset + 1,
          ingestedAt: startedAt,
          validationStatus: "not-business-validated",
          source,
          data
        }));

      records.insertMany(documents, { ordered: true });
    }

    const actualRows = records.countDocuments({ fileId });

    if (actualRows !== parsed.rows.length) {
      fail("STORED_RECORD_COUNT_MISMATCH");
    }

    files.updateOne(
      { _id: fileId, status: "processing" },
      {
        $set: {
          status: "complete",
          completedAt: new Date(),
          parsedRows: parsed.rows.length,
          storedRows: actualRows,
          skippedBlankLines: parsed.blankLines,
          headers: parsed.headers
        }
      }
    );

    totals.completed++;
    addEvent(key, "complete", null, fileId);

    print(JSON.stringify({
      fileId,
      status: "complete",
      importedRows: actualRows
    }));
  } catch (error) {
    if (!countedAttempt) totals.attempted++;
    totals.failed++;

    const code = safeCode(error);

    if (claimed) {
      files.updateOne(
        { _id: fileId, status: "processing" },
        {
          $set: {
            status: "failed",
            failedAt: new Date(),
            errorCode: code
          }
        }
      );
    }

    addEvent(key, "failed", code, fileId);
    print(JSON.stringify({ status: "failed", code, fileId }));

    // Stop the run on an unexpected database/programming failure.
    if (!error.pilotCode) throw error;
  } finally {
    fs.rmSync(localFile, { force: true });
  }
}

try {
  // These create the three collections/indexes if needed.
  files.createIndex({ projectId: 1, status: 1 });

  records.createIndex(
    { fileId: 1, sourceRecordNumber: 1 },
    { unique: true, name: "unique_file_record" }
  );

  records.createIndex({ projectId: 1, sourceFilename: 1 });

  // One active run per project, including across Terminal sessions.
  runs.createIndex(
    { projectId: 1 },
    {
      unique: true,
      name: "one_active_run_per_project",
      partialFilterExpression: { active: true }
    }
  );

  runs.insertOne({
    _id: runId,
    projectId,
    limit,
    status: "running",
    active: true,
    startedAt: new Date(),
    totals,
    events: []
  });

  runStarted = true;
  print("Started run: " + runId);

  // Capture a bounded inventory before importing.
  // AWS CLI pagination is handled explicitly.
  const objects = [];
  let token = null;

  do {
    const args = [
      "s3api", "list-objects-v2",
      "--bucket", bucket,
      "--prefix", prefix,
      "--max-keys", "1000",
      "--no-paginate"
    ];

    if (token) args.push("--continuation-token", token);

    const page = awsJson(args);

    for (const object of page.Contents || []) {
      if (object.Size > 0) objects.push(object);
    }

    if (objects.length > 5000) fail("PILOT_INVENTORY_LIMIT_EXCEEDED");

    token = page.IsTruncated ? page.NextContinuationToken : null;
    if (page.IsTruncated && !token) fail("INVALID_PAGINATION_RESPONSE");
  } while (token);

  objects.sort((a, b) => a.Key < b.Key ? -1 : a.Key > b.Key ? 1 : 0);

  runs.updateOne(
    { _id: runId },
    { $set: { objectsDiscovered: objects.length } }
  );

  for (const object of objects) {
    if (totals.attempted >= limit) break;
    processObject(object);
  }

  if (totals.failed > 0) {
    runStatus = "completed_with_errors";
    exitFailure = true;
  }
} catch (error) {
  exitFailure = true;
  runStatus = "failed";

  if (runStarted) {
    runs.updateOne(
      { _id: runId },
      { $set: { errorCode: safeCode(error) } }
    );
  }

  print("Run stopped. Error category: " + safeCode(error));
  print("If another run is active, investigate it before clearing its lock.");
} finally {
  if (runStarted) {
    runs.updateOne(
      { _id: runId },
      {
        $set: {
          active: false,
          status: runStatus,
          endedAt: new Date(),
          totals
        }
      }
    );
  }

  fs.rmSync(workDir, { recursive: true, force: true });
}

print(JSON.stringify({
  runId,
  projectId,
  status: runStatus,
  totals
}));

if (exitFailure) quit(1);