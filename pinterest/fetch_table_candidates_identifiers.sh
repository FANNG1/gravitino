#!/usr/bin/env bash
#
# fetch_table_candidates_identifiers.sh
#
# Reads TEXTFILE-format files from the table optimization eval candidates
# S3 directory for a given date partition. Extracts database_name and
# table_name, writes them as a single comma-separated line of
# "catalog.database_name.table_name" to a local output file.
#
# Usage:
#   ./fetch_table_candidates_identifiers.sh <dt> [output_file] [catalog]
#
# Examples:
#   ./fetch_table_candidates_identifiers.sh 2026-02-24
#   ./fetch_table_candidates_identifiers.sh 2026-02-24 /tmp/my_ids.txt
#   ./fetch_table_candidates_identifiers.sh 2026-02-24 /tmp/my_ids.txt hive_catalog

set -euo pipefail

# ──────────────────────────────────────────────
# Constants / Defaults
# ──────────────────────────────────────────────
S3_PREFIX="s3://pinterest-seattle/iceberg/table_optimization_eval_candidates"
FIELD_DELIMITER=$'\001'
DEFAULT_OUTPUT_FILE="/tmp/table_identifiers_$(date +%Y%m%d_%H%M%S).txt"
DEFAULT_CATALOG="generic"
SKIP_FILES=("_SUCCESS")
TEMP_DIR=$(mktemp -d)

trap 'rm -rf "${TEMP_DIR}"' EXIT INT TERM

# ──────────────────────────────────────────────
# Logging helpers
# ──────────────────────────────────────────────
log_info()  { echo "[INFO]  $(date '+%Y-%m-%d %H:%M:%S') — $*"; }
log_warn()  { echo "[WARN]  $(date '+%Y-%m-%d %H:%M:%S') — $*" >&2; }
log_error() { echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') — $*" >&2; }

# ──────────────────────────────────────────────
# Helper: check if a filename should be skipped
# ──────────────────────────────────────────────
should_skip() {
    local filename="$1"
    for pattern in "${SKIP_FILES[@]}"; do
        [[ "${filename}" == "${pattern}" ]] && return 0
    done
    return 1
}

# ──────────────────────────────────────────────
# Validate dependencies
# ──────────────────────────────────────────────
if ! command -v aws &>/dev/null; then
    log_error "'aws' CLI is required but not installed. Aborting."
    exit 1
fi

# ──────────────────────────────────────────────
# Parse arguments
# ──────────────────────────────────────────────
if [[ $# -lt 1 ]]; then
    log_error "Usage: $0 <dt> [output_file] [catalog]"
    log_error "  dt  — date partition in YYYY-MM-DD format (e.g. 2026-02-24)"
    exit 1
fi

DT="$1"

# Validate date format
if ! [[ "${DT}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    log_error "Invalid date format '${DT}'. Expected YYYY-MM-DD (e.g. 2026-02-24)."
    exit 1
fi

S3_DIR="${S3_PREFIX}/dt=${DT}/"
OUTPUT_FILE="${2:-$DEFAULT_OUTPUT_FILE}"
CATALOG="${3:-$DEFAULT_CATALOG}"

log_info "Date partition      : ${DT}"
log_info "S3 source directory : ${S3_DIR}"
log_info "Output file         : ${OUTPUT_FILE}"
log_info "Catalog             : ${CATALOG}"

# ──────────────────────────────────────────────
# 1. List & validate S3 directory is non-empty
# ──────────────────────────────────────────────
log_info "Listing files in S3 directory…"

mapfile -t S3_FILE_KEYS < <(aws s3 ls "${S3_DIR}" | awk 'NF {print $NF}')

if [[ ${#S3_FILE_KEYS[@]} -eq 0 ]]; then
    log_warn "The S3 directory '${S3_DIR}' is EMPTY or does not exist. Nothing to process."
    log_warn "Check that dt=${DT} is a valid partition."
    exit 0
fi

FILE_COUNT=${#S3_FILE_KEYS[@]}
log_info "Found ${FILE_COUNT} file(s) in S3 directory."

# ──────────────────────────────────────────────
# 2. Prepare output file
# ──────────────────────────────────────────────
mkdir -p "$(dirname "${OUTPUT_FILE}")"

# ──────────────────────────────────────────────
# 3. Process files one-by-one, collect into array
# ──────────────────────────────────────────────
IDENTIFIERS=()
TOTAL_LINES=0
SKIPPED_LINES=0
SKIPPED_FILES=0
FILES_PROCESSED=0

for key in "${S3_FILE_KEYS[@]}"; do

    # ── Skip marker files ─────────────────────
    if should_skip "${key}"; then
        log_info "  Skipping marker file: ${key}"
        SKIPPED_FILES=$((SKIPPED_FILES + 1))
        continue
    fi

    s3_path="${S3_DIR}${key}"
    local_path="${TEMP_DIR}/${key}"

    # ── Download ──────────────────────────────
    log_info "[$((FILES_PROCESSED + 1))/$((FILE_COUNT - SKIPPED_FILES))] Downloading: ${key}"
    if ! aws s3 cp "${s3_path}" "${local_path}" --quiet; then
        log_warn "  Failed to download '${s3_path}', skipping."
        SKIPPED_FILES=$((SKIPPED_FILES + 1))
        continue
    fi

    # ── Parse ─────────────────────────────────
    while IFS="${FIELD_DELIMITER}" read -r db_name tbl_name _remainder || [[ -n "${db_name}" ]]; do
        TOTAL_LINES=$((TOTAL_LINES + 1))

        # Skip blank lines
        [[ -z "${db_name}" ]] && continue

        # Validate both fields are present
        if [[ -z "${db_name}" || -z "${tbl_name}" ]]; then
            log_warn "  ${key}:${TOTAL_LINES} — missing database_name or table_name, skipping."
            SKIPPED_LINES=$((SKIPPED_LINES + 1))
            continue
        fi

        IDENTIFIERS+=("${CATALOG}.${db_name}.${tbl_name}")

    done < "${local_path}"

    # ── Clean up immediately ──────────────────
    rm -f "${local_path}"
    FILES_PROCESSED=$((FILES_PROCESSED + 1))
    log_info "  Done with ${key} — deleted local copy."
done

# ──────────────────────────────────────────────
# 4. Write comma-separated identifiers to output
# ──────────────────────────────────────────────
VALID_IDENTIFIERS=${#IDENTIFIERS[@]}

if [[ "${VALID_IDENTIFIERS}" -gt 0 ]]; then
    OUTPUT_LINE=$(IFS=','; echo "${IDENTIFIERS[*]}")
    echo "${OUTPUT_LINE}" > "${OUTPUT_FILE}"
fi

# ──────────────────────────────────────────────
# 5. Summary
# ──────────────────────────────────────────────
echo ""
log_info "════════════════════════════════════════"
log_info "  Processing complete (dt=${DT})"
log_info "════════════════════════════════════════"
log_info "  Files in S3 directory    : ${FILE_COUNT}"
log_info "  Skipped files            : ${SKIPPED_FILES}"
log_info "  Files processed          : ${FILES_PROCESSED}"
log_info "  Total lines read         : ${TOTAL_LINES}"
log_info "  Valid identifiers        : ${VALID_IDENTIFIERS}"
log_info "  Skipped lines            : ${SKIPPED_LINES}"
log_info "  Output written to        : ${OUTPUT_FILE}"
echo ""

if [[ "${VALID_IDENTIFIERS}" -eq 0 ]]; then
    log_warn "No identifiers were extracted. Please verify the S3 files for dt=${DT} contain valid TEXTFILE data."
fi
