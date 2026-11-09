const fs = require("fs");

const database = db.getSiblingDB("fcp-mel-data-ingestion-service");
database.getMongo().setReadPref("primary");

const transfer = JSON.parse(
  fs.readFileSync(process.env.MEL_TRANSFER_FILE, "utf8")
);

const batchId = process.env.MEL_BATCH_ID;
const collectionName = process.env.MEL_COLLECTION;
const expectedRows = Number(process.env.MEL_EXPECTED_ROWS);
const phase = process.env.MEL_PHASE;

const batches = database.getCollection("ingestion_batches");
const rows = database.getCollection(collectionName);

const source = {
  projectId: transfer.projectId,

  sourceFolderId: transfer.sourceFolderId,
  sourceFolderName: transfer.sourceFolderName,
  sourceFilename: transfer.sourceName,

  sourceDriveId: transfer.sourceDriveId,
  sourceItemId: transfer.sourceItemId,
  sourceETag: transfer.sourceETag,

  sourceBucket: transfer.result.s3Bucket,
  sourceKey: transfer.result.s3Key,
  sourceChecksumSha256: transfer.result.checksumSha256,

  uploadId: transfer.uploadId,
  transferCompletedAt: transfer.completedAt,
  importerVersion: "project-pilot-1",
  validationStatus: "not-business-validated"
};

if (phase === "reserve") {
  const existing = batches.findOne({ _id: batchId });

  if (existing) {
    if (existing.status === "complete") {
      print("Already imported: no duplicate records created.");
      print("Collection: " + existing.collection);
      quit(20);
    }

    throw new Error(
      "Existing batch status: " + existing.status +
      ". Review before retrying."
    );
  }

  if (database.getCollectionInfos({ name: collectionName }).length) {
    throw new Error("Destination collection exists; refusing overwrite.");
  }

  batches.insertOne({
    _id: batchId,
    status: "importing",
    collection: collectionName,
    expectedRows,
    startedAt: new Date(),
    source
  });

  database.createCollection(collectionName);
  print("Reserved batch: " + batchId);
}

if (phase === "fail") {
  batches.updateOne(
    { _id: batchId, status: "importing" },
    {
      $set: {
        status: "failed",
        failedAt: new Date(),
        failure: "Import or verification failed; manual review required."
      }
    }
  );

  print("Failed batch remains isolated in staging.");
}

if (phase === "finish") {
  const batch = batches.findOne({ _id: batchId });

  if (!batch || batch.status !== "importing") {
    throw new Error("Unexpected batch state.");
  }

  const actualRows = rows.countDocuments({});

  if (actualRows !== expectedRows) {
    throw new Error(
      "Count mismatch: expected " + expectedRows +
      ", imported " + actualRows
    );
  }

  const metadata = {
    ...source,
    batchId,
    ingestedAt: batch.startedAt
  };

  const result = rows.updateMany(
    {},
    [
      {
        $replaceWith: {
          _id: "$_id",
          data: {
            $arrayToObject: {
              $filter: {
                input: { $objectToArray: "$$ROOT" },
                as: "field",
                cond: { $ne: ["$$field.k", "_id"] }
              }
            }
          },
          ingestion: { $literal: metadata }
        }
      }
    ]
  );

  if (result.matchedCount !== actualRows) {
    throw new Error("Metadata update count mismatch.");
  }

  const verified = rows.countDocuments({
    "ingestion.batchId": batchId,
    "ingestion.projectId": source.projectId,
    "ingestion.sourceChecksumSha256": source.sourceChecksumSha256
  });

  if (verified !== expectedRows) {
    throw new Error("Final verification failed.");
  }

  batches.updateOne(
    { _id: batchId, status: "importing" },
    {
      $set: {
        status: "complete",
        importedRows: actualRows,
        completedAt: new Date()
      }
    }
  );

  print(JSON.stringify({
    status: "complete",
    projectId: source.projectId,
    batchId,
    collection: collectionName,
    importedRows: actualRows,
    validationStatus: "not-business-validated"
  }));
}