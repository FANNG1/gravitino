#!/bin/bash
#
# Script to download and combine JSON files from an S3 prefix into a single JSONL file.
# Usage: ./combine_job_mapping.sh <s3_prefix> [output_file]
#
# Example:
#   ./combine_job_mapping.sh s3://pinterest-seattle/iceberg/table_optimization_job_mapping_json/dt=2026-02-03/
#   ./combine_job_mapping.sh s3://pinterest-seattle/iceberg/table_optimization_job_mapping_json/dt=2026-02-03/ output.jsonl

set -euo pipefail

# Default output file
DEFAULT_OUTPUT="job-mappings.jsonl"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 <s3_prefix> [output_file]"
    echo ""
    echo "Arguments:"
    echo "  s3_prefix    Required. S3 prefix to list and download JSON files from."
    echo "               Example: s3://bucket-name/path/to/files/"
    echo "  output_file  Optional. Local output file path. Default: ${DEFAULT_OUTPUT}"
    echo ""
    echo "Example:"
    echo "  $0 s3://pinterest-seattle/iceberg/table_optimization_job_mapping_json/dt=2026-02-03/"
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

# Check if required argument is provided
if [[ $# -lt 1 ]]; then
    log_error "S3 prefix is required."
    usage
fi

S3_PREFIX="$1"
OUTPUT_FILE="${2:-$DEFAULT_OUTPUT}"

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
log_info "Output File: ${OUTPUT_FILE}"

# Create a temporary directory for downloaded files
TEMP_DIR=$(mktemp -d)
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

# Download each JSON file and track line counts
TOTAL_LINES=0
declare -a DOWNLOADED_FILES

FILE_INDEX=0
while IFS= read -r json_file; do
    if [[ -n "$json_file" ]]; then
        FILE_INDEX=$((FILE_INDEX + 1))
        log_info "Downloading [${FILE_INDEX}/${FILE_COUNT}]: ${json_file}"
        LOCAL_PATH="${TEMP_DIR}/${json_file}"

        # Download with progress visible
        if ! aws s3 cp "${S3_PREFIX}${json_file}" "${LOCAL_PATH}"; then
            log_error "Failed to download ${json_file}"
            exit 1
        fi

        log_info "  -> Download complete, counting lines..."

        # Count lines in this file
        FILE_LINES=$(wc -l < "${LOCAL_PATH}" | tr -d ' ')
        log_info "  -> ${FILE_LINES} lines"
        TOTAL_LINES=$((TOTAL_LINES + FILE_LINES))

        DOWNLOADED_FILES+=("${LOCAL_PATH}")
    fi
done <<< "$JSON_FILES"

log_info "Expected total lines: ${TOTAL_LINES}"

# Combine all JSON files into one
log_info "Combining ${#DOWNLOADED_FILES[@]} files into ${OUTPUT_FILE}..."

# Clear output file if it exists
> "${OUTPUT_FILE}"

# Concatenate all files
for file in "${DOWNLOADED_FILES[@]}"; do
    cat "$file" >> "${OUTPUT_FILE}"
done

# Validate line count
OUTPUT_LINES=$(wc -l < "${OUTPUT_FILE}" | tr -d ' ')
log_info "Output file line count: ${OUTPUT_LINES}"

if [[ "${OUTPUT_LINES}" -eq "${TOTAL_LINES}" ]]; then
    log_info "Validation PASSED: Line count matches (${OUTPUT_LINES} lines)"
    log_info "Successfully created: ${OUTPUT_FILE}"
    echo ""
    echo "Summary:"
    echo "  - Files processed: ${FILE_COUNT}"
    echo "  - Total lines: ${OUTPUT_LINES}"
    echo "  - Output file: ${OUTPUT_FILE}"
    exit 0
else
    log_error "Validation FAILED: Expected ${TOTAL_LINES} lines, got ${OUTPUT_LINES} lines"
    exit 1
fi
