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
SCHEMA="${SCHEMA:-db}"
TABLE1="${TABLE1:-table1}"
TABLE2="${TABLE2:-table2}"
POLICY_NAME="${POLICY_NAME:-compactionFilterCount}"
OPTIMIZER_BIN="${OPTIMIZER_BIN:-./bin/gravitino-optimizer.sh}"
OPTIMIZER_CONF="${OPTIMIZER_CONF:-./conf/gravitino-optimizer.conf}"

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

curl_json_allow_exists() {
  local method="$1"
  local url="$2"
  local payload="${3:-}"
  local body_file
  local code

  body_file="$(mktemp)"
  if [[ -n "${payload}" ]]; then
    code="$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" \
      -H "Accept: application/vnd.gravitino.v1+json" \
      -H "Content-Type: application/json" \
      -d "$payload" \
      "$url")"
  else
    code="$(curl -sS -o "$body_file" -w "%{http_code}" -X "$method" \
      -H "Accept: application/vnd.gravitino.v1+json" \
      "$url")"
  fi

  if [[ "$code" =~ ^2[0-9][0-9]$ || "$code" == "409" ]]; then
    cat "$body_file"
    rm -f "$body_file"
    return 0
  fi

  echo "Request failed: ${method} ${url} (status=${code})" >&2
  cat "$body_file" >&2
  rm -f "$body_file"
  return 1
}

echo "Configure optimizer conf: ${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.gravitinoUri" "${GRAVITINO_URI}" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.gravitinoMetalake" "${METALAKE}" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.gravitinoDefaultCatalog" "${CATALOG}" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.strategyHandler.compaction.className" "org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.recommender.jobSubmitter" "noop-job-submitter" "${OPTIMIZER_CONF}"
update_config_property "gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules" "table:table_storage_cost:latest:le" "${OPTIMIZER_CONF}"

echo "Create metalake/catalog/schema/tables (idempotent)"
curl_json_allow_exists "POST" "${GRAVITINO_URI}/api/metalakes" \
  "{\"name\":\"${METALAKE}\",\"comment\":\"Optimizer quickstart\",\"properties\":{}}" >/dev/null

curl_json_allow_exists "POST" "${GRAVITINO_URI}/api/metalakes/${METALAKE}/catalogs" \
  "{\"name\":\"${CATALOG}\",\"type\":\"RELATIONAL\",\"comment\":\"Generic lakehouse catalog\",\"provider\":\"lakehouse-generic\",\"properties\":{\"location\":\"mock-location\"}}" >/dev/null

curl_json_allow_exists "POST" "${GRAVITINO_URI}/api/metalakes/${METALAKE}/catalogs/${CATALOG}/schemas" \
  "{\"name\":\"${SCHEMA}\",\"comment\":\"optimizer schema\",\"properties\":{}}" >/dev/null

curl_json_allow_exists "POST" "${GRAVITINO_URI}/api/metalakes/${METALAKE}/catalogs/${CATALOG}/schemas/${SCHEMA}/tables" \
  "{\"name\":\"${TABLE1}\",\"comment\":\"demo table1\",\"columns\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true},{\"name\":\"country\",\"type\":\"string\",\"nullable\":true}],\"properties\":{\"format\":\"iceberg\"}}" >/dev/null

curl_json_allow_exists "POST" "${GRAVITINO_URI}/api/metalakes/${METALAKE}/catalogs/${CATALOG}/schemas/${SCHEMA}/tables" \
  "{\"name\":\"${TABLE2}\",\"comment\":\"demo table2\",\"columns\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true},{\"name\":\"bucket_col\",\"type\":\"string\",\"nullable\":true}],\"properties\":{\"format\":\"iceberg\"}}" >/dev/null

echo "Create policy and attach to schema"
curl_json_allow_exists "POST" "${GRAVITINO_URI}/api/metalakes/${METALAKE}/policies" \
  "{\"name\":\"${POLICY_NAME}\",\"comment\":\"Compaction policy based on custom filter count\",\"policyType\":\"custom\",\"enabled\":true,\"content\":{\"customRules\":{\"trigger-expr\":\"custom-filter_count > 5\",\"score-expr\":\"custom-filter_count\"},\"supportedObjectTypes\":[\"CATALOG\",\"SCHEMA\",\"TABLE\"],\"properties\":{\"strategy.type\":\"compaction\",\"job.template-name\":\"compaction-job\"}}}" >/dev/null

curl_json_allow_exists "POST" "${GRAVITINO_URI}/api/metalakes/${METALAKE}/objects/schema/${CATALOG}.${SCHEMA}/policies" \
  "{\"policiesToAdd\":[\"${POLICY_NAME}\"],\"policiesToRemove\":[]}" >/dev/null

echo "Prepare local stats/metrics payload files"
cat > p-table-stats.jsonl <<JSONL
{"identifier":"${SCHEMA}.${TABLE1}","stats-type":"table","custom-filter_count":60}
{"identifier":"${SCHEMA}.${TABLE2}","stats-type":"table","custom-filter_count":120}
JSONL

cat > p-table-metrics-before.jsonl <<JSONL
{"identifier":"${SCHEMA}.${TABLE1}","stats-type":"table","table_storage_cost":1000}
{"identifier":"${SCHEMA}.${TABLE2}","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"table_storage_cost":2000}
JSONL

echo "Run update-statistics"
"${OPTIMIZER_BIN}" \
  --type update-statistics \
  --calculator-name local-stats-calculator \
  --file-path ./p-table-stats.jsonl \
  --conf-path "${OPTIMIZER_CONF}"

echo "Append metrics before action"
"${OPTIMIZER_BIN}" \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --file-path ./p-table-metrics-before.jsonl \
  --conf-path "${OPTIMIZER_CONF}"

echo "Dry-run strategy submission"
"${OPTIMIZER_BIN}" \
  --type submit-strategy-jobs \
  --identifiers "${CATALOG}.${SCHEMA}.${TABLE1},${CATALOG}.${SCHEMA}.${TABLE2}" \
  --strategy-name "${POLICY_NAME}" \
  --dry-run \
  --conf-path "${OPTIMIZER_CONF}"

echo "Submit strategy jobs"
"${OPTIMIZER_BIN}" \
  --type submit-strategy-jobs \
  --identifiers "${CATALOG}.${SCHEMA}.${TABLE1},${CATALOG}.${SCHEMA}.${TABLE2}" \
  --strategy-name "${POLICY_NAME}" \
  --limit 2 \
  --conf-path "${OPTIMIZER_CONF}"

ACTION_TIME="$(date +%s)"
sleep 2

cat > p-table-metrics-after.jsonl <<JSONL
{"identifier":"${SCHEMA}.${TABLE1}","stats-type":"table","table_storage_cost":300}
{"identifier":"${SCHEMA}.${TABLE2}","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"table_storage_cost":500}
JSONL

echo "Append metrics after action"
"${OPTIMIZER_BIN}" \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --file-path ./p-table-metrics-after.jsonl \
  --conf-path "${OPTIMIZER_CONF}"

echo "Run monitor-metrics"
"${OPTIMIZER_BIN}" \
  --type monitor-metrics \
  --identifiers "${CATALOG}.${SCHEMA}.${TABLE1}" \
  --action-time "${ACTION_TIME}" \
  --range-seconds 3600 \
  --conf-path "${OPTIMIZER_CONF}"

"${OPTIMIZER_BIN}" \
  --type monitor-metrics \
  --identifiers "${CATALOG}.${SCHEMA}.${TABLE2}" \
  --partition-path '[{"bucket_col_bucket_8":"3"}]' \
  --action-time "${ACTION_TIME}" \
  --range-seconds 3600 \
  --conf-path "${OPTIMIZER_CONF}"

echo "List table metrics"
"${OPTIMIZER_BIN}" \
  --type list-table-metrics \
  --identifiers "${CATALOG}.${SCHEMA}.${TABLE1}" \
  --conf-path "${OPTIMIZER_CONF}"

echo "Done."
