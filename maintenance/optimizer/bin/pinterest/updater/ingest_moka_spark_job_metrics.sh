#!/bin/bash
#
# Script to download Spark job metrics JSON files from S3, transform them
# into a single JSONL file, and invoke gravitino-optimizer once in bulk mode.
#
# Usage: ./ingest_spark_job_metrics.sh <s3_prefix> [--dry-run]
#
# Example:
#   ./ingest_spark_job_metrics.sh s3://pinterest-seattle/iceberg/moka_spark_job_metrics_json/dt=2026-02-20/
#   ./ingest_spark_job_metrics.sh s3://pinterest-seattle/iceberg/moka_spark_job_metrics_json/dt=2026-02-20/ --dry-run

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 <s3_prefix> [--dry-run]"
    echo ""
    echo "Arguments:"
    echo "  s3_prefix  Required. S3 prefix containing Spark job metrics JSON files."
    echo "             Example: s3://pinterest-seattle/iceberg/moka_spark_job_metrics_json/dt=2026-02-20/"
    echo "  --dry-run  Optional. Print commands without executing them."
    echo ""
    echo "Each JSON file should contain lines in the format:"
    echo '  {"workflow_name":"...","data_job_name":"...","runtime_seconds":...}'
    exit 1
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Parse arguments
if [[ $# -lt 1 ]]; then
    log_error "S3 prefix is required."
    usage
fi

S3_PREFIX="$1"
DRY_RUN=false

if [[ "${2:-}" == "--dry-run" ]]; then
    DRY_RUN=true
    log_warn "Dry-run mode enabled. Commands will be printed but not executed."
fi

# Validate S3 prefix format
if [[ ! "$S3_PREFIX" =~ ^s3:// ]]; then
    log_error "Invalid S3 prefix. Must start with 's3://'"
    exit 1
fi

# Ensure S3 prefix ends with /
if [[ ! "$S3_PREFIX" =~ /$ ]]; then
    S3_PREFIX="${S3_PREFIX}/"
fi

log_info "S3 Prefix: ${S3_PREFIX}"

# Create a temporary directory for downloaded files
TEMP_DIR=$(mktemp -d)
# TODO: temp disabling it for debugging
trap "rm -rf ${TEMP_DIR}" EXIT

log_info "Created temporary directory: ${TEMP_DIR}"

# List JSON files from S3
log_info "Listing JSON files from S3..."
JSON_FILES=$(aws s3 ls "${S3_PREFIX}" | grep '\.json$' | awk '{print $4}')

if [[ -z "$JSON_FILES" ]]; then
    log_error "No JSON files found at ${S3_PREFIX}"
    exit 1
fi

FILE_COUNT=$(echo "$JSON_FILES" | wc -l | tr -d ' ')
log_info "Found ${FILE_COUNT} JSON file(s)"

# Download each JSON file
declare -a DOWNLOADED_FILES
FILE_INDEX=0

while IFS= read -r json_file; do
    if [[ -n "$json_file" ]]; then
        FILE_INDEX=$((FILE_INDEX + 1))
        log_info "Downloading [${FILE_INDEX}/${FILE_COUNT}]: ${json_file}"
        LOCAL_PATH="${TEMP_DIR}/${json_file}"

        if ! aws s3 cp "${S3_PREFIX}${json_file}" "${LOCAL_PATH}"; then
            log_error "Failed to download ${json_file}"
            exit 1
        fi

        DOWNLOADED_FILES+=("${LOCAL_PATH}")
    fi
done <<< "$JSON_FILES"

log_info "Downloaded ${#DOWNLOADED_FILES[@]} file(s). Transforming metrics..."

# Phase A: Transform all downloaded JSON files into a single JSONL file
# in the format expected by AbstractStatisticsReader:
#   {"identifier":"moka-spark##workflow##job","stats-type":"job","job_status":123}
COMBINED_FILE="${TEMP_DIR}/combined_metrics.jsonl"
TRANSFORM_ERRORS="${TEMP_DIR}/transform_errors.log"

TRANSFORM_OUTPUT=$(python3 - "${DOWNLOADED_FILES[@]}" "$COMBINED_FILE" "$TRANSFORM_ERRORS" <<'PYEOF'
import json
import sys

input_files = sys.argv[1:-2]
output_path = sys.argv[-2]
error_path = sys.argv[-1]

total = 0
skipped = 0

with open(output_path, "w") as out, open(error_path, "w") as err:
    for fpath in input_files:
        with open(fpath) as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    workflow = rec["workflow_name"]
                    job = rec["data_job_name"]
                    runtime = rec["runtime_seconds"]
                    identifier = f"moka-spark##{workflow}##{job}"
                    out.write(json.dumps({
                        "identifier": identifier,
                        "stats-type": "job",
                        "job_status": runtime,
                    }) + "\n")
                    total += 1
                except (json.JSONDecodeError, KeyError) as e:
                    err.write(f"{fpath}:{line_num}: {e}: {line}\n")
                    skipped += 1

print(f"{total} {skipped}")
PYEOF
)

read TOTAL_RECORDS SKIP_COUNT <<< "$TRANSFORM_OUTPUT"

log_info "Transformed ${TOTAL_RECORDS} record(s) into ${COMBINED_FILE} (${SKIP_COUNT} skipped)"

if [[ -s "$TRANSFORM_ERRORS" ]]; then
    log_warn "Transform errors:"
    cat "$TRANSFORM_ERRORS"
fi

if [[ "$TOTAL_RECORDS" -eq 0 ]]; then
    log_error "No valid records to ingest."
    exit 1
fi

# Phase B: Single bulk optimizer invocation using file-based ingestion.
# Omitting --identifiers triggers allIdentifiers=true, which uses the bulk
# updateAll() path. This avoids parseAndNormalizeIdentifiers (which rejects
# ##-delimited identifiers) and eliminates per-record JVM startup overhead.
CMD="./bin/gravitino-optimizer.sh --type append-metrics --computer-name local-stats-computer --file-path \"${COMBINED_FILE}\""

if [[ "$DRY_RUN" == true ]]; then
    log_info "Dry-run: would ingest ${TOTAL_RECORDS} records with:"
    echo "$CMD"
else
    log_info "Ingesting ${TOTAL_RECORDS} records via bulk optimizer call..."
    if eval "$CMD"; then
        log_info "Optimizer completed successfully."
    else
        log_error "Optimizer command failed."
        exit 1
    fi
fi

echo ""
echo "Summary:"
echo "  - Files downloaded:    ${FILE_COUNT}"
echo "  - Records transformed: ${TOTAL_RECORDS}"
echo "  - Records skipped:     ${SKIP_COUNT}"

if [[ "$SKIP_COUNT" -gt 0 ]]; then
    log_warn "Some records were skipped during transform. Check output above for details."
fi

log_info "Done."
