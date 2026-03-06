#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

OPTIMIZER_BIN="${OPTIMIZER_BIN:-./bin/gravitino-optimizer.sh}"
OPTIMIZER_CONF="${OPTIMIZER_CONF:-./conf/gravitino-optimizer.conf}"
MONITOR_SERVICE_URL="${MONITOR_SERVICE_URL:-http://localhost:8000}"
GRAVITINO_URI="${GRAVITINO_URI:-http://localhost:8090}"
METALAKE="${METALAKE:-test}"
CATALOG="${CATALOG:-generic}"
SCHEMA="${SCHEMA:-db}"
TABLE1="${TABLE1:-table1}"
TABLE2="${TABLE2:-table2}"
ACTION_TIME="${ACTION_TIME:-}"
RANGE_SECONDS="${RANGE_SECONDS:-3600}"
MAX_POLL_TIMES="${MAX_POLL_TIMES:-30}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"

if [[ ! -x "${OPTIMIZER_BIN}" ]]; then
  echo "Optimizer CLI not found or not executable: ${OPTIMIZER_BIN}" >&2
  exit 1
fi

if [[ ! -f "${OPTIMIZER_CONF}" ]]; then
  echo "Optimizer config file not found: ${OPTIMIZER_CONF}" >&2
  exit 1
fi

update_config_property() {
  local key="$1"
  local value="$2"
  local file="$3"
  local tmp_file

  tmp_file="$(mktemp)"
  awk -v key="$key" -v value="$value" '
    BEGIN { updated=0 }
    $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
      print key " = " value
      updated=1
      next
    }
    { print }
    END { if (!updated) print key " = " value }
  ' "$file" > "$tmp_file"
  mv "$tmp_file" "$file"
}

check_monitor_service() {
  local url="$1"
  if ! curl -sS "${url}/v1/health" >/dev/null; then
    echo "Monitor service is not reachable at ${url}" >&2
    echo "Please start monitor service first, then retry." >&2
    exit 1
  fi
}

poll_monitor_state() {
  local monitor_id="$1"
  local label="$2"
  local i

  for ((i=1; i<=MAX_POLL_TIMES; i++)); do
    local output
    output="$(${OPTIMIZER_BIN} \
      --type get-monitor \
      --monitor-id "${monitor_id}" \
      --monitor-service-url "${MONITOR_SERVICE_URL}" \
      --conf-path "${OPTIMIZER_CONF}")"

    echo "[${label}] poll ${i}: ${output}"

    if echo "${output}" | grep -Eq 'state=(SUCCEEDED|FAILED|PARTIAL_FAILED|CANCELED)'; then
      return 0
    fi

    sleep "${POLL_INTERVAL_SECONDS}"
  done

  echo "[${label}] monitor did not reach terminal state in time: ${monitor_id}" >&2
  return 1
}

echo "Update optimizer conf for CLI-side commands: ${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.gravitinoUri" "${GRAVITINO_URI}" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.gravitinoMetalake" "${METALAKE}" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.gravitinoDefaultCatalog" "${CATALOG}" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules" "table:row_count:latest:le,job:job_status:latest:le" "${OPTIMIZER_CONF}"

echo "Check monitor service health: ${MONITOR_SERVICE_URL}"
check_monitor_service "${MONITOR_SERVICE_URL}"

echo "Append metrics before action"
"${OPTIMIZER_BIN}" \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --statistics-payload "{\"identifier\":\"${CATALOG}.${SCHEMA}.${TABLE1}\",\"stats-type\":\"table\",\"row_count\":100}" \
  --conf-path "${OPTIMIZER_CONF}"

"${OPTIMIZER_BIN}" \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --statistics-payload "{\"identifier\":\"${CATALOG}.${SCHEMA}.${TABLE2}\",\"stats-type\":\"partition\",\"partition-path\":{\"bucket_col_bucket_8\":\"3\"},\"row_count\":50}" \
  --conf-path "${OPTIMIZER_CONF}"

"${OPTIMIZER_BIN}" \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --statistics-payload '{"identifier":"job-1","stats-type":"job","job_status":0}' \
  --conf-path "${OPTIMIZER_CONF}"

if [[ -z "${ACTION_TIME}" ]]; then
  sleep 2
  ACTION_TIME="$(date +%s)"
fi

echo "Submit monitors"
MONITOR_ID_TABLE1="$(${OPTIMIZER_BIN} \
  --type submit-monitor \
  --identifier "${CATALOG}.${SCHEMA}.${TABLE1}" \
  --action-time-seconds "${ACTION_TIME}" \
  --range-seconds "${RANGE_SECONDS}" \
  --monitor-service-url "${MONITOR_SERVICE_URL}" \
  --conf-path "${OPTIMIZER_CONF}" | tr -d '\r\n')"

echo "Monitor ID (table1): ${MONITOR_ID_TABLE1}"

MONITOR_ID_TABLE2="$(${OPTIMIZER_BIN} \
  --type submit-monitor \
  --identifier "${CATALOG}.${SCHEMA}.${TABLE2}" \
  --partition-path 'bucket_col_bucket_8=3' \
  --action-time-seconds "${ACTION_TIME}" \
  --range-seconds "${RANGE_SECONDS}" \
  --monitor-service-url "${MONITOR_SERVICE_URL}" \
  --conf-path "${OPTIMIZER_CONF}" | tr -d '\r\n')"

echo "Monitor ID (table2 partition): ${MONITOR_ID_TABLE2}"

echo "Append metrics after action"
sleep 2
"${OPTIMIZER_BIN}" \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --statistics-payload "{\"identifier\":\"${CATALOG}.${SCHEMA}.${TABLE1}\",\"stats-type\":\"table\",\"row_count\":90}" \
  --conf-path "${OPTIMIZER_CONF}"

"${OPTIMIZER_BIN}" \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --statistics-payload "{\"identifier\":\"${CATALOG}.${SCHEMA}.${TABLE2}\",\"stats-type\":\"partition\",\"partition-path\":{\"bucket_col_bucket_8\":\"3\"},\"row_count\":40}" \
  --conf-path "${OPTIMIZER_CONF}"

"${OPTIMIZER_BIN}" \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --statistics-payload '{"identifier":"job-1","stats-type":"job","job_status":-1}' \
  --conf-path "${OPTIMIZER_CONF}"

echo "Poll monitor status"
poll_monitor_state "${MONITOR_ID_TABLE1}" "table1"
poll_monitor_state "${MONITOR_ID_TABLE2}" "table2-partition"

echo "List monitors"
"${OPTIMIZER_BIN}" \
  --type list-monitors \
  --monitor-service-url "${MONITOR_SERVICE_URL}" \
  --conf-path "${OPTIMIZER_CONF}"

echo "Done."
