#!/usr/bin/env bash
#
# generate_candidates.sh
#
# Orchestrates the table recommendation workflow by fetching candidates,
# running the optimizer, and producing top N recommendations.
#
# Usage:
#   ./generate_candidates.sh <project_root> <dt> [strategy_type] [candidates_number]
#
# Examples:
#   ./generate_candidates.sh /path/to/gravitino-pinterest 2026-02-24
#   ./generate_candidates.sh /path/to/gravitino-pinterest 2026-02-24 compaction 5

set -euo pipefail

# ──────────────────────────────────────────────
# Constants / Defaults
# ──────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FETCH_SCRIPT="${SCRIPT_DIR}/fetch_table_candidates_identifiers.sh"
DEFAULT_STRATEGY_TYPE="compaction"
DEFAULT_CANDIDATES_NUMBER="10"
TEMP_DIR=$(mktemp -d)

trap 'rm -rf "${TEMP_DIR}"' EXIT INT TERM

# ──────────────────────────────────────────────
# Logging helpers
# ──────────────────────────────────────────────
log_info()  { echo "[INFO]  $(date '+%Y-%m-%d %H:%M:%S') — $*"; }
log_warn()  { echo "[WARN]  $(date '+%Y-%m-%d %H:%M:%S') — $*" >&2; }
log_error() { echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') — $*" >&2; }

# ──────────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────────
validate_date_format() {
    local date="$1"
    if ! [[ "${date}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        log_error "Invalid date format '${date}'. Expected YYYY-MM-DD (e.g. 2026-02-24)."
        exit 1
    fi
}

backup_and_modify_config() {
    local config_backup="${TEMP_DIR}/gravitino-optimizer.conf.backup"

    if [[ ! -f "${CONFIG_FILE}" ]]; then
        log_error "Configuration file not found: ${CONFIG_FILE}"
        exit 1
    fi

    # Create backup
    cp "${CONFIG_FILE}" "${config_backup}"
    log_info "Created config backup: ${config_backup}"

    # Check current jobSubmitter setting
    local current_job_submitter
    current_job_submitter=$(grep "^gravitino.optimizer.recommender.jobSubmitter" "${CONFIG_FILE}" | cut -d'=' -f2 | tr -d ' ' || echo "")

    if [[ "${current_job_submitter}" != "noop-job-submitter" ]]; then
        log_info "Current jobSubmitter: '${current_job_submitter}', temporarily setting to 'noop-job-submitter'"

        # Update the config file
        if grep -q "^gravitino.optimizer.recommender.jobSubmitter" "${CONFIG_FILE}"; then
            # Update existing line
            sed -i.bak "s/^gravitino.optimizer.recommender.jobSubmitter.*/gravitino.optimizer.recommender.jobSubmitter = noop-job-submitter/" "${CONFIG_FILE}"
        else
            # Add the line if it doesn't exist
            echo "gravitino.optimizer.recommender.jobSubmitter = noop-job-submitter" >> "${CONFIG_FILE}"
        fi
        echo "${current_job_submitter}" > "${TEMP_DIR}/original_job_submitter"
        return 0
    else
        log_info "jobSubmitter already set to 'noop-job-submitter', no change needed"
        echo "noop-job-submitter" > "${TEMP_DIR}/original_job_submitter"
        return 1
    fi
}

restore_config() {
    local config_backup="${TEMP_DIR}/gravitino-optimizer.conf.backup"
    local original_job_submitter_file="${TEMP_DIR}/original_job_submitter"

    if [[ -f "${config_backup}" && -f "${original_job_submitter_file}" ]]; then
        local original_job_submitter
        original_job_submitter=$(cat "${original_job_submitter_file}")

        if [[ "${original_job_submitter}" != "noop-job-submitter" ]]; then
            log_info "Restoring original jobSubmitter: '${original_job_submitter}'"
            cp "${config_backup}" "${CONFIG_FILE}"
        else
            log_info "No config restoration needed"
        fi
    fi
}

parse_recommendations() {
    local output_file="$1"
    local candidates_number="$2"

    if [[ ! -f "${output_file}" ]]; then
        log_error "Optimizer output file not found: ${output_file}"
        return 1
    fi

    # Parse RECOMMEND lines and extract score, identifier, jobTemplate, jobOptions
    local temp_recommendations="${TEMP_DIR}/recommendations.txt"
    grep "^RECOMMEND:" "${output_file}" > "${temp_recommendations}" || true

    if [[ ! -s "${temp_recommendations}" ]]; then
        log_warn "No recommendation lines found in output"
        return 1
    fi

    # Extract and sort by score (descending)
    local sorted_recommendations="${TEMP_DIR}/sorted_recommendations.txt"
    sed -n 's/^RECOMMEND: strategy=\([^ ]*\) identifier=\([^ ]*\) score=\([^ ]*\) jobTemplate=\([^ ]*\) jobOptions=\(.*\)/\3\t\2\t\4\t\5/p' "${temp_recommendations}" | sort -nr > "${sorted_recommendations}"

    # Display top N recommendations
    local total_recommendations
    total_recommendations=$(wc -l < "${sorted_recommendations}")

    log_info "Found ${total_recommendations} total recommendations, showing top ${candidates_number}:"
    echo ""
    printf "%-6s %-50s %-20s %s\n" "SCORE" "IDENTIFIER" "JOB_TEMPLATE" "JOB_OPTIONS"
    printf "%-6s %-50s %-20s %s\n" "------" "--------------------------------------------------" "--------------------" "-----------"

    # Store top N identifiers for summary
    TOP_CANDIDATE_IDENTIFIERS=$(head -n "${candidates_number}" "${sorted_recommendations}" | cut -f2 | paste -sd ',' -)

    head -n "${candidates_number}" "${sorted_recommendations}" | while IFS=$'\t' read -r score identifier jobTemplate jobOptions; do
        printf "%-6d %-50s %-20s %s\n" "${score}" "${identifier}" "${jobTemplate}" "${jobOptions}"
    done

    echo ""
    log_info "Summary: Processed ${total_recommendations} recommendations, displayed top ${candidates_number}"
}

# ──────────────────────────────────────────────
# Parse arguments
# ──────────────────────────────────────────────
if [[ $# -lt 2 ]]; then
    log_error "Usage: $0 <project_root> <dt> [strategy_type] [candidates_number]"
    log_error "  project_root    — path to gravitino-pinterest project root"
    log_error "  dt              — date partition in YYYY-MM-DD format (e.g. 2026-02-24)"
    log_error "  strategy_type   — strategy type (default: ${DEFAULT_STRATEGY_TYPE})"
    log_error "  candidates_number — number of top candidates to show (default: ${DEFAULT_CANDIDATES_NUMBER})"
    exit 1
fi

PROJECT_ROOT="$1"
DT="$2"
STRATEGY_TYPE="${3:-$DEFAULT_STRATEGY_TYPE}"
CANDIDATES_NUMBER="${4:-$DEFAULT_CANDIDATES_NUMBER}"

# Set derived paths based on PROJECT_ROOT
OPTIMIZER_SCRIPT="${PROJECT_ROOT}/bin/gravitino-optimizer.sh"
CONFIG_FILE="${PROJECT_ROOT}/conf/gravitino-optimizer.conf"

# ──────────────────────────────────────────────
# Validate dependencies
# ──────────────────────────────────────────────
if [[ ! -f "${FETCH_SCRIPT}" ]]; then
    log_error "Fetch script not found: ${FETCH_SCRIPT}"
    exit 1
fi

if [[ ! -f "${OPTIMIZER_SCRIPT}" ]]; then
    log_error "Optimizer script not found: ${OPTIMIZER_SCRIPT}"
    exit 1
fi

# Validate inputs
validate_date_format "${DT}"

if ! [[ "${CANDIDATES_NUMBER}" =~ ^[0-9]+$ ]] || [[ "${CANDIDATES_NUMBER}" -lt 1 ]]; then
    log_error "Invalid candidates_number '${CANDIDATES_NUMBER}'. Must be a positive integer."
    exit 1
fi

log_info "Starting candidate generation with parameters:"
log_info "  Project root        : ${PROJECT_ROOT}"
log_info "  Date partition      : ${DT}"
log_info "  Strategy type       : ${STRATEGY_TYPE}"
log_info "  Candidates number   : ${CANDIDATES_NUMBER}"
echo ""

# ──────────────────────────────────────────────
# Step 1: Fetch table candidates identifiers
# ──────────────────────────────────────────────
log_info "Step 1: Fetching table candidates identifiers..."
log_info "Running: ${FETCH_SCRIPT} ${DT}"

local_candidates_file="${TEMP_DIR}/table_identifiers.txt"
if ! "${FETCH_SCRIPT}" "${DT}" "${local_candidates_file}"; then
    log_error "Failed to fetch table candidates"
    exit 1
fi

if [[ ! -f "${local_candidates_file}" || ! -s "${local_candidates_file}" ]]; then
    log_error "No table identifiers found for dt=${DT}"
    exit 1
fi

TABLE_IDS=$(cat "${local_candidates_file}")
IDENTIFIER_COUNT=$(echo "${TABLE_IDS}" | tr ',' '\n' | wc -l | tr -d ' ')
log_info "Found ${IDENTIFIER_COUNT} table identifier(s)"
echo ""

# ──────────────────────────────────────────────
# Step 2: Ensure jobSubmitter is set to noop-job-submitter
# ──────────────────────────────────────────────
log_info "Step 2: Checking configuration..."
config_modified=false
if backup_and_modify_config; then
    config_modified=true
fi
echo ""

# ──────────────────────────────────────────────
# Step 3: Run optimizer
# ──────────────────────────────────────────────
log_info "Step 3: Running optimizer with ${IDENTIFIER_COUNT} table(s) for strategy '${STRATEGY_TYPE}'..."
optimizer_output="${TEMP_DIR}/optimizer_output.txt"

optimizer_cmd="${OPTIMIZER_SCRIPT} --type recommend_strategy_type --identifiers \"${TABLE_IDS}\" --strategy-type ${STRATEGY_TYPE}"

if ! eval "${optimizer_cmd}" > "${optimizer_output}" 2>&1; then
    log_error "Optimizer execution failed"
    log_error "Check output in: ${optimizer_output}"
    restore_config
    exit 1
fi

log_info "Optimizer execution completed"
echo ""

# ──────────────────────────────────────────────
# Step 4: Parse recommendations and show top N
# ──────────────────────────────────────────────
log_info "Step 4: Processing recommendations..."
if ! parse_recommendations "${optimizer_output}" "${CANDIDATES_NUMBER}"; then
    log_warn "Failed to parse recommendations or no recommendations found"
fi

# ──────────────────────────────────────────────
# Step 5: Restore configuration if modified
# ──────────────────────────────────────────────
if [[ "${config_modified}" == true ]]; then
    log_info "Step 5: Restoring original configuration..."
    restore_config
fi

echo ""
log_info "════════════════════════════════════════"
log_info "  Candidate generation complete"
log_info "════════════════════════════════════════"
log_info "  Date partition      : ${DT}"
log_info "  Strategy type       : ${STRATEGY_TYPE}"
log_info "  Candidates requested: ${CANDIDATES_NUMBER}"
log_info "  Table identifiers   : ${IDENTIFIER_COUNT}"
echo ""
if [[ -n "${TOP_CANDIDATE_IDENTIFIERS:-}" ]]; then
    log_info "Top ${CANDIDATES_NUMBER} recommended candidates:"
    echo "${TOP_CANDIDATE_IDENTIFIERS}"
    echo ""
fi
