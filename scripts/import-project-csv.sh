#!/usr/bin/env bash

# Import one completed project-transfer record into dev MongoDB staging.

set +x
set -euo pipefail
umask 077

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

[[ $# -eq 2 ]] ||
  die "Usage: bash import-project-csv.sh TRANSFER.json EXPECTED_DATA_ROWS"

[[ "${SERVICE:-}" == "fcp-mel-data-ingestion-service" ]] ||
  die "Run in the ingestion-service CDP Terminal."

[[ "${ENVIRONMENT:-}" == "dev" ]] || die "Dev only."

[[ "${APPROVED_FOR_DEV:-}" == "yes" ]] ||
  die "Set APPROVED_FOR_DEV=yes only for approved data."

[[ "${CSV_HEADER_REVIEWED:-}" == "yes" ]] ||
  die "Confirm a comma-separated CSV with unique, non-empty first-row headings."

: "${MONGO_URI:?CDP must provide MONGO_URI}"

MANIFEST="$1"
EXPECTED_ROWS="$2"

[[ -f "$MANIFEST" ]] || die "Transfer record not found."
[[ "$EXPECTED_ROWS" =~ ^[1-9][0-9]*$ ]] ||
  die "Expected data rows must be a positive integer."

for tool in aws jq openssl mongoimport mongosh mktemp; do
  command -v "$tool" >/dev/null || die "Missing tool: $tool"
done

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
JS="$SCRIPT_DIR/import-project-csv.mongo.js"
[[ -f "$JS" ]] || die "Place the matching .mongo.js file beside this script."

BUCKET="dev-fcp-mel-ingestion-files-c63f2"
MAX_BYTES=20971520

# Validate trusted handoff fields before making requests.
jq -e --arg bucket "$BUCKET" '
  def nonempty: type == "string" and length > 0;

  .status == "complete" and
  .result.fileStatus == "complete" and
  (.result.hasError != true) and

  (.projectId | type == "string") and
  (.projectId | test("^[a-z0-9][a-z0-9_-]*$")) and

  (.sourceFolderId | nonempty) and
  (.sourceFolderName | nonempty) and
  (.sourceDriveId | nonempty) and
  (.sourceItemId | nonempty) and
  (.sourceETag | nonempty) and
  (.sourceName | nonempty) and
  (.sourceName | test("\\.csv$"; "i")) and
  (.uploadId | nonempty) and

  .result.s3Bucket == $bucket and
  (.result.s3Key | nonempty) and
  (.result.checksumSha256 | nonempty) and

  (.projectId as $p |
    .result.s3Key | startswith("projects/" + $p + "/"))
' "$MANIFEST" >/dev/null ||
  die "Invalid, incomplete or unexpected transfer record."

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cp "$MANIFEST" "$TMP/transfer.json"

S3_KEY=$(jq -er '.result.s3Key' "$TMP/transfer.json")
EXPECTED_CHECKSUM=$(jq -er '.result.checksumSha256' "$TMP/transfer.json")

aws s3api head-object \
  --bucket "$BUCKET" \
  --key "$S3_KEY" > "$TMP/head.json"

SIZE=$(jq -er '.ContentLength' "$TMP/head.json")
[[ "$SIZE" -gt 0 && "$SIZE" -le "$MAX_BYTES" ]] ||
  die "Object is empty or exceeds the 20 MiB pilot limit."

aws s3 cp \
  "s3://$BUCKET/$S3_KEY" \
  "$TMP/input.csv" \
  --only-show-errors

CHECKSUM=$(
  openssl dgst -sha256 -binary "$TMP/input.csv" |
    openssl base64 -A
)

[[ "$CHECKSUM" == "$EXPECTED_CHECKSUM" ]] ||
  die "Checksum mismatch. No database write performed."

# Stable source/version identity, independent of a repeat upload's S3 key.
IDENTITY=$(jq -c '
  [
    .projectId,
    .sourceDriveId,
    .sourceItemId,
    .sourceETag,
    .result.checksumSha256
  ]
' "$TMP/transfer.json")

HASH=$(printf '%s' "$IDENTITY" | openssl dgst -sha256)
BATCH_ID="${HASH##* }"
[[ "$BATCH_ID" =~ ^[a-f0-9]{64}$ ]] || die "Invalid batch identifier."

export MEL_BATCH_ID="$BATCH_ID"
export MEL_COLLECTION="staging_csv_$BATCH_ID"
export MEL_EXPECTED_ROWS="$EXPECTED_ROWS"
export MEL_TRANSFER_FILE="$TMP/transfer.json"

run_db() {
  MEL_PHASE="$1" mongosh \
    --quiet --norc \
    "$MONGO_URI" \
    --file "$JS"
}

if run_db reserve; then
  :
else
  CODE=$?
  [[ "$CODE" -eq 20 ]] && exit 0
  die "Reservation failed; no CSV import performed."
fi

if ! mongoimport \
    --uri="$MONGO_URI" \
    --db="fcp-mel-data-ingestion-service" \
    --collection="$MEL_COLLECTION" \
    --type=csv \
    --headerline \
    --file="$TMP/input.csv" \
    --mode=insert \
    --maintainInsertionOrder \
    --stopOnError \
    > "$TMP/import.log" 2>&1; then

  run_db fail || true
  die "CSV import failed; partial staging may exist. Do not blindly rerun."
fi

if ! run_db finish; then
  run_db fail || true
  die "Verification failed. Batch must not be used for reporting."
fi

echo "Import completed. No source files or existing collections were deleted."