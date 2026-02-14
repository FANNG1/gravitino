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

GRAVITINO_URI="${GRAVITINO_URI:-http://localhost:8090}"
METALAKE="${METALAKE:-test}"
CATALOG="${CATALOG:-generic}"
OPTIMIZER_BIN="${OPTIMIZER_BIN:-./bin/gravitino-optimizer.sh}"
OPTIMIZER_CONF="${OPTIMIZER_CONF:-./conf/gravitino-optimizer.conf}"
MONITOR_SERVICE_URL="${MONITOR_SERVICE_URL:-http://localhost:8000}"
RANGE_SECONDS="${RANGE_SECONDS:-3600}"
ACTION_TIME="${ACTION_TIME:-}"
MONITOR_SERVICE_HOST_PORT="${MONITOR_SERVICE_URL#*://}"
MONITOR_SERVICE_HOST_PORT="${MONITOR_SERVICE_HOST_PORT%%/*}"
MONITOR_SERVICE_PORT="${MONITOR_SERVICE_HOST_PORT##*:}"

if ! [[ "${MONITOR_SERVICE_PORT}" =~ ^[0-9]+$ ]]; then
  echo "Invalid MONITOR_SERVICE_URL (cannot parse numeric port): ${MONITOR_SERVICE_URL}" >&2
  exit 1
fi

update_config_property() {
  local key="$1"
  local value="$2"
  local file="$3"
  local tmp_file

  if [ ! -f "$file" ]; then
    echo "Optimizer config not found: ${file}" >&2
    return 1
  fi

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

kill_monitor_service() {
  local port="${MONITOR_SERVICE_URL##*:}"
  local matched
  if command -v pgrep >/dev/null 2>&1; then
    matched="$(pgrep -f "run-monitor-service" || true)"
    if [ -n "${matched}" ]; then
      echo "Stopping monitor service process(es) by name: ${matched}"
      kill ${matched} || true
      sleep 1
      return
    fi
  fi
  if command -v lsof >/dev/null 2>&1; then
    local pids
    pids="$(lsof -ti tcp:"${port}" || true)"
    if [ -n "${pids}" ]; then
      echo "Stopping monitor service on port ${port} (pid: ${pids})"
      kill ${pids} || true
      sleep 1
    fi
  fi
}

if [ ! -x "$OPTIMIZER_BIN" ]; then
  echo "Optimizer CLI not found or not executable: ${OPTIMIZER_BIN}" >&2
  exit 1
fi

echo "Step 1: Prepare job mappings (job provider input)"
cat > job-mappings.jsonl <<'EOF'
{"identifier":"generic.db.table1","job-identifiers":["job-1"]}
{"identifier":"generic.db.table2","job-identifiers":["job-2"]}
EOF

JOB_MAPPINGS_PATH="$(pwd)/job-mappings.jsonl"

echo "Step 2: Update optimizer configuration"
update_config_property "gravitino.optimizer.gravitinoUri" "${GRAVITINO_URI}" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.gravitinoMetalake" "${METALAKE}" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.gravitinoDefaultCatalog" "${CATALOG}" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.h2-metrics.h2-metrics-storage-path" "./data/metrics.db" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.monitor.service.port" "${MONITOR_SERVICE_PORT}" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.monitor.service.interval.seconds" "2" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.monitor.callbacks" "console" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.monitor.jobProvider" "file-job-provider" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.monitor.file-job-provider.file-path" "${JOB_MAPPINGS_PATH}" "${OPTIMIZER_CONF}"

echo "Step 3: Start the monitor service"
kill_monitor_service
"${OPTIMIZER_BIN}" --type run-monitor-service > monitor-server.out 2>&1 &
MONITOR_PID=$!
trap 'kill ${MONITOR_PID} 2>/dev/null || true' EXIT
sleep 2

echo "Step 4: Append table and job metrics before compaction"
"${OPTIMIZER_BIN}" --type append-metrics \
  --computer-name local-stats-computer \
  --statistics-payload '{"identifier":"generic.db.table1","stats-type":"table","row_count":100}'

"${OPTIMIZER_BIN}" --type append-metrics \
  --computer-name local-stats-computer \
  --statistics-payload '{"identifier":"generic.db.table2","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"row_count":50}'

"${OPTIMIZER_BIN}" --type append-metrics \
  --computer-name local-stats-computer \
  --statistics-payload '{"identifier":"job-1","stats-type":"job","job_status":0}'

"${OPTIMIZER_BIN}" --type append-metrics \
  --computer-name local-stats-computer \
  --statistics-payload '{"identifier":"job-2","stats-type":"job","job_status":0}'

if [ -z "${ACTION_TIME}" ]; then
  sleep 2
  ACTION_TIME="$(date +%s)"
fi

echo "Step 5: Submit monitors"
MONITOR_ID_TABLE1="$("${OPTIMIZER_BIN}" --type submit-monitor \
  --identifier generic.db.table1 \
  --action-time-seconds "${ACTION_TIME}" \
  --range-seconds "${RANGE_SECONDS}" \
  --monitor-service-url "${MONITOR_SERVICE_URL}" | tr -d '\r\n')"
echo "Submitted monitorId (table1): ${MONITOR_ID_TABLE1}"

MONITOR_ID_TABLE2="$("${OPTIMIZER_BIN}" --type submit-monitor \
  --identifier generic.db.table2 \
  --partition-path bucket_col_bucket_8=3 \
  --action-time-seconds "${ACTION_TIME}" \
  --range-seconds "${RANGE_SECONDS}" \
  --monitor-service-url "${MONITOR_SERVICE_URL}" | tr -d '\r\n')"
echo "Submitted monitorId (table2 partition): ${MONITOR_ID_TABLE2}"

echo "Step 6: Append table and job metrics after compaction"
sleep 2
"${OPTIMIZER_BIN}" --type append-metrics \
  --computer-name local-stats-computer \
  --statistics-payload '{"identifier":"generic.db.table1","stats-type":"table","row_count":120}'

"${OPTIMIZER_BIN}" --type append-metrics \
  --computer-name local-stats-computer \
  --statistics-payload '{"identifier":"generic.db.table2","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"row_count":45}'

"${OPTIMIZER_BIN}" --type append-metrics \
  --computer-name local-stats-computer \
  --statistics-payload '{"identifier":"job-1","stats-type":"job","job_status":-1}'

"${OPTIMIZER_BIN}" --type append-metrics \
  --computer-name local-stats-computer \
  --statistics-payload '{"identifier":"job-2","stats-type":"job","job_status":-1}'

echo "Step 7: Get monitor status"
"${OPTIMIZER_BIN}" --type get-monitor \
  --monitor-id "${MONITOR_ID_TABLE1}" \
  --monitor-service-url "${MONITOR_SERVICE_URL}"
"${OPTIMIZER_BIN}" --type get-monitor \
  --monitor-id "${MONITOR_ID_TABLE2}" \
  --monitor-service-url "${MONITOR_SERVICE_URL}"

echo "Step 8: List monitors"
"${OPTIMIZER_BIN}" --type list-monitors \
  --monitor-service-url "${MONITOR_SERVICE_URL}"
