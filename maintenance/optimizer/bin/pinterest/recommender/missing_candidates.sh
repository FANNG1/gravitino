#!/usr/bin/env bash
#
# missing_candidates.sh
#
# Computes the delta between table_optimization_eval_candidates and
# table_optimization_metadata_dump_json — i.e. tables that appear in
# candidates but are MISSING from the metadata dump.
#
# Outputs:
#   1. A comma-separated list of missing identifiers (catalog.db.tbl)
#   2. A SQL script to enrich those missing tables with storage cost data
#
# Usage:
#   ./missing_candidates.sh <dt> [output_dir] [catalog]
#
# Examples:
#   ./missing_candidates.sh 2026-02-24
#   ./missing_candidates.sh 2026-02-24 /tmp/missing_candidates_$(date +%Y%m%d_%H%M%S)
#   ./missing_candidates.sh 2026-02-24 /tmp/missing_candidates_$(date +%Y%m%d_%H%M%S) hive_catalog

set -euo pipefail

# ──────────────────────────────────────────────
# Constants / Defaults
# ──────────────────────────────────────────────
S3_CANDIDATES="s3://pinterest-seattle/iceberg/table_optimization_eval_candidates"
S3_METADATA="s3://pinterest-seattle/iceberg/table_optimization_metadata_dump_json"
FIELD_DELIMITER_CANDIDATES=$'\001'
DEFAULT_OUTPUT_DIR="/tmp/missing_candidates_$(date +%Y%m%d_%H%M%S)"
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
for cmd in aws jq; do
    if ! command -v "${cmd}" &>/dev/null; then
        log_error "'${cmd}' is required but not installed. Aborting."
        exit 1
    fi
done

# ──────────────────────────────────────────────
# Parse arguments
# ──────────────────────────────────────────────
if [[ $# -lt 1 ]]; then
    log_error "Usage: $0 <dt> [output_dir] [catalog]"
    log_error "  dt  — date partition in YYYY-MM-DD format (e.g. 2026-02-24)"
    exit 1
fi

DT="$1"

if ! [[ "${DT}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    log_error "Invalid date format '${DT}'. Expected YYYY-MM-DD (e.g. 2026-02-24)."
    exit 1
fi

OUTPUT_DIR="${2:-$DEFAULT_OUTPUT_DIR}"
CATALOG="${3:-$DEFAULT_CATALOG}"

S3_CANDIDATES_DIR="${S3_CANDIDATES}/dt=${DT}/"
S3_METADATA_DIR="${S3_METADATA}/dt=${DT}/"

DELTA_FILE="${OUTPUT_DIR}/missing_identifiers.txt"
SQL_FILE="${OUTPUT_DIR}/cost_enrichment.sql"

log_info "Date partition         : ${DT}"
log_info "Candidates S3 dir      : ${S3_CANDIDATES_DIR}"
log_info "Metadata dump S3 dir   : ${S3_METADATA_DIR}"
log_info "Output directory       : ${OUTPUT_DIR}"
log_info "Catalog                : ${CATALOG}"

mkdir -p "${OUTPUT_DIR}"

# ══════════════════════════════════════════════
#  STEP 1: Collect candidate identifiers (db.tbl)
# ══════════════════════════════════════════════
log_info "────────────────────────────────────────"
log_info "Reading eval candidates…"

mapfile -t CAND_KEYS < <(aws s3 ls "${S3_CANDIDATES_DIR}" | awk 'NF {print $NF}')

if [[ ${#CAND_KEYS[@]} -eq 0 ]]; then
    log_warn "Candidates directory '${S3_CANDIDATES_DIR}' is EMPTY. Nothing to process."
    exit 0
fi

log_info "Found ${#CAND_KEYS[@]} file(s) in candidates directory."

declare -A CANDIDATE_SET
CAND_COUNT=0

for key in "${CAND_KEYS[@]}"; do
    should_skip "${key}" && continue

    local_path="${TEMP_DIR}/cand_${key}"
    if ! aws s3 cp "${S3_CANDIDATES_DIR}${key}" "${local_path}" --quiet; then
        log_warn "  Failed to download candidates file '${key}', skipping."
        continue
    fi

    while IFS="${FIELD_DELIMITER_CANDIDATES}" read -r db_name tbl_name _remainder || [[ -n "${db_name}" ]]; do
        [[ -z "${db_name}" || -z "${tbl_name}" ]] && continue
        CANDIDATE_SET["${db_name}.${tbl_name}"]=1
        CAND_COUNT=$((CAND_COUNT + 1))
    done < "${local_path}"

    rm -f "${local_path}"
done

log_info "Unique candidate tables: ${#CANDIDATE_SET[@]} (total rows: ${CAND_COUNT})"

# ══════════════════════════════════════════════
#  STEP 2: Collect metadata dump identifiers (db.tbl)
# ══════════════════════════════════════════════
log_info "────────────────────────────────────────"
log_info "Reading metadata dump…"

mapfile -t META_KEYS < <(aws s3 ls "${S3_METADATA_DIR}" | awk 'NF {print $NF}')

if [[ ${#META_KEYS[@]} -eq 0 ]]; then
    log_warn "Metadata directory '${S3_METADATA_DIR}' is EMPTY."
    log_warn "All ${#CANDIDATE_SET[@]} candidate tables will be treated as missing."
fi

declare -A METADATA_SET
META_COUNT=0

for key in "${META_KEYS[@]}"; do
    should_skip "${key}" && continue

    local_path="${TEMP_DIR}/meta_${key}"
    if ! aws s3 cp "${S3_METADATA_DIR}${key}" "${local_path}" --quiet; then
        log_warn "  Failed to download metadata file '${key}', skipping."
        continue
    fi

    # Each line is a JSON object with an "identifier" field in "db.tbl" format
    while IFS= read -r line || [[ -n "${line}" ]]; do
        [[ -z "${line}" ]] && continue

        identifier=$(echo "${line}" | jq -r '.identifier // empty' 2>/dev/null)
        if [[ -n "${identifier}" ]]; then
            METADATA_SET["${identifier}"]=1
            META_COUNT=$((META_COUNT + 1))
        fi
    done < "${local_path}"

    rm -f "${local_path}"
done

log_info "Unique metadata tables : ${#METADATA_SET[@]} (total rows: ${META_COUNT})"

# ══════════════════════════════════════════════
#  STEP 3: Compute delta (in candidates, NOT in metadata)
# ══════════════════════════════════════════════
log_info "────────────────────────────────────────"
log_info "Computing delta…"

MISSING_IDENTIFIERS=()

for tbl_id in "${!CANDIDATE_SET[@]}"; do
    if [[ -z "${METADATA_SET[${tbl_id}]+_}" ]]; then
        MISSING_IDENTIFIERS+=("${tbl_id}")
    fi
done

MISSING_COUNT=${#MISSING_IDENTIFIERS[@]}
log_info "Missing from metadata  : ${MISSING_COUNT} table(s)"

if [[ "${MISSING_COUNT}" -eq 0 ]]; then
    log_info "No delta — all candidate tables exist in the metadata dump."
    echo "" > "${DELTA_FILE}"
    echo "-- No missing tables for dt=${DT}" > "${SQL_FILE}"
    exit 0
fi

# ══════════════════════════════════════════════
#  STEP 4: Write outputs
# ══════════════════════════════════════════════

# ── 4a. Comma-separated identifiers file ─────
CATALOG_IDENTIFIERS=()
for tbl_id in "${MISSING_IDENTIFIERS[@]}"; do
    CATALOG_IDENTIFIERS+=("${CATALOG}.${tbl_id}")
done

OUTPUT_LINE=$(IFS=','; echo "${CATALOG_IDENTIFIERS[*]}")
echo "${OUTPUT_LINE}" > "${DELTA_FILE}"

# ── 4b. SQL cost enrichment script ───────────
{
    echo "-- Cost enrichment for ${MISSING_COUNT} table(s) missing from metadata dump"
    echo "-- Generated: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "-- Partition : dt=${DT}"
    echo "--"
    echo "-- 0.0067 dollar per GB per month"
    echo ""
    echo "SELECT"
    echo "  database_name,"
    echo "  table_name,"
    echo "  size / pow(2,30) * 0.0067 * 12 AS annual_cost_dollar"
    echo "FROM"
    echo "  iceberg.iceberg_table_s3_storage_aggregation"
    echo "WHERE"
    echo "  dt = '${DT}'"
    echo "  AND ("

    first=true
    for tbl_id in "${MISSING_IDENTIFIERS[@]}"; do
        db_name="${tbl_id%%.*}"
        tbl_name="${tbl_id#*.}"

        if [[ "${first}" == true ]]; then
            echo "        (database_name = '${db_name}' AND table_name = '${tbl_name}')"
            first=false
        else
            echo "     OR (database_name = '${db_name}' AND table_name = '${tbl_name}')"
        fi
    done

    echo "  )"
    echo "ORDER BY"
    echo "  annual_cost_dollar DESC"
    echo ";"
} > "${SQL_FILE}"

# ══════════════════════════════════════════════
#  STEP 5: Summary
# ══════════════════════════════════════════════
echo ""
log_info "════════════════════════════════════════"
log_info "  Delta complete (dt=${DT})"
log_info "════════════════════════════════════════"
log_info "  Candidate tables         : ${#CANDIDATE_SET[@]}"
log_info "  Metadata dump tables     : ${#METADATA_SET[@]}"
log_info "  Missing (delta)          : ${MISSING_COUNT}"
log_info "  Identifiers file         : ${DELTA_FILE}"
log_info "  SQL cost script          : ${SQL_FILE}"
echo ""
