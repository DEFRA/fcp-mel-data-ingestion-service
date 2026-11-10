#!/usr/bin/env bash

set +x
set -euo pipefail
umask 077

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

[[ $# -eq 2 ]] ||
  die "Usage: bash ingest-project.sh PROJECT_ID MAX_NEW_FILE_ATTEMPTS"

[[ "${SERVICE:-}" == "fcp-mel-data-ingestion-service" ]] ||
  die "Run in the ingestion-service Terminal."

[[ "${ENVIRONMENT:-}" == "dev" ]] ||
  die "This pilot is restricted to dev."

[[ "${APPROVED_FOR_DEV:-}" == "yes" ]] ||
  die "Set APPROVED_FOR_DEV=yes only for approved data."

[[ "$1" =~ ^[a-z0-9][a-z0-9_-]*$ ]] ||
  die "Invalid project ID."

[[ "$2" =~ ^[1-9][0-9]*$ ]] ||
  die "Supply a positive file limit."

# Pilot guard: expand only after review.
[[ ${#2} -le 2 && "$2" -le 30 ]] ||
  die "Pilot limit is 30 new file attempts per run."

for tool in aws curl mongosh; do
  command -v "$tool" >/dev/null || die "Missing tool: $tool"
done

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

[[ -f "$HOME/.mongoshrc.js" ]] ||
  die "CDP MongoDB startup configuration is missing."

[[ -f "$SCRIPT_DIR/project-import.mongo.js" ]] ||
  die "Missing project-import.mongo.js."

[[ -f "$SCRIPT_DIR/pilot-csv.cjs" ]] ||
  die "Missing pilot-csv.cjs."

export MEL_SCRIPT_DIR="$SCRIPT_DIR"
export MEL_PROJECT_ID="$1"
export MEL_FILE_LIMIT="$2"

mongosh \
  --quiet \
  --norc \
  --file "$HOME/.mongoshrc.js" \
  --file "$SCRIPT_DIR/project-import.mongo.js"