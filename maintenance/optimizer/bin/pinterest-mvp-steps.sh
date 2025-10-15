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
POLICY_NAME="${POLICY_NAME:-compactionFilterCount}"
OPTIMIZER_BIN="${OPTIMIZER_BIN:-./bin/gravitino-optimizer.sh}"

curl_json() {
  curl -sS -H "Accept: application/vnd.gravitino.v1+json" \
    -H "Content-Type: application/json" "$@"
}

echo "Step 2: Create metalake"
curl_json -X POST \
  -d "{\"name\":\"${METALAKE}\",\"comment\":\"Optimizer quickstart\",\"properties\":{}}" \
  "${GRAVITINO_URI}/api/metalakes"

echo "Step 3: Create generic catalog"
curl_json -X POST -d "{
  \"name\": \"${CATALOG}\",
  \"type\": \"RELATIONAL\",
  \"comment\": \"Generic lakehouse catalog for Iceberg datasets\",
  \"provider\": \"lakehouse-generic\",
  \"properties\": {
    \"location\": \"mock-location\"
  }
}" "${GRAVITINO_URI}/api/metalakes/${METALAKE}/catalogs"

echo "Step 5: Write table metadata JSONL"
cat > table-metadata.jsonl <<'JSONL'
{"identifier":"generic.db.table1","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"},{"id":2,"name":"ts","required":false,"type":"timestamp","original-type":"timestamp with time zone"},{"id":3,"name":"country","required":false,"type":"string"}]},"partition_specs":[{"spec-id":0,"fields":[{"source-id":3,"field-id":1000,"name":"country","transform":"identity"}]}],"sort_orders":[{"order-id":1,"fields":[{"transform":"identity","source-id":2,"direction":"asc","null-order":"nulls-last"}]}],"default_spec_id":0,"default_sort_order_id":1}
{"identifier":"db.table2","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"},{"id":2,"name":"bucket_col","required":false,"type":"string"}]},"partition_specs":[{"spec-id":0,"fields":[{"source-id":2,"field-id":2000,"name":"bucket_col_bucket_8","transform":"bucket[8]"}]}],"sort_orders":[{"order-id":1,"fields":[{"transform":"identity","source-id":2,"direction":"asc","null-order":"nulls-first"}]}],"default_spec_id":0,"default_sort_order_id":1}
JSONL

echo "Step 5: Register tables via optimizer CLI"
"${OPTIMIZER_BIN}" -type register_tables --file-path ./table-metadata.jsonl

echo "Step 5: Verify tables are registered"
curl_json -X GET \
  "${GRAVITINO_URI}/api/metalakes/${METALAKE}/catalogs/${CATALOG}/schemas/${SCHEMA}/tables/table1"
curl_json -X GET \
  "${GRAVITINO_URI}/api/metalakes/${METALAKE}/catalogs/${CATALOG}/schemas/${SCHEMA}/tables/table2"

echo "Step 6: Create compaction policy"
curl_json -X POST -d "{
  \"name\": \"${POLICY_NAME}\",
  \"comment\": \"Compaction policy that factors in filter count\",
  \"policyType\": \"custom\",
  \"enabled\": true,
  \"content\": {
    \"customRules\": {
      \"trigger-expr\":\"custom-filter_count > 5\",
      \"score-expr\":\"custom-filter_count\"
    },
    \"supportedObjectTypes\": [
      \"CATALOG\",
      \"SCHEMA\",
      \"TABLE\"
    ],
    \"properties\": {
      \"job.target-file-size\":\"1024000000\",
      \"strategy.type\": \"compaction\",
      \"job.template-name\":\"compaction-job\"
    }
  }
}" "${GRAVITINO_URI}/api/metalakes/${METALAKE}/policies"

echo "Step 7: Attach policy to schema"
curl_json -X POST -d "{
  \"policiesToAdd\": [\"${POLICY_NAME}\"],
  \"policiesToRemove\": []
}" "${GRAVITINO_URI}/api/metalakes/${METALAKE}/objects/schema/${CATALOG}.${SCHEMA}/policies"

echo "Step 8: Write local stats JSONL"
cat > p-table-stats.jsonl <<'JSONL'
{"identifier":"db.table1","stats-type":"table","custom-cost":6000}
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"US"},"custom-filter_count":60}
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"CA"},"custom-filter_count":40}
{"identifier":"db.table2","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"custom-filter_count":120}
JSONL

echo "Step 9: Push table and partition stats"
"${OPTIMIZER_BIN}" -type update_statistics -computer-name local-stats-computer --file-path ./p-table-stats.jsonl

echo "Step 9: Verify stats"
curl_json -X GET \
  "${GRAVITINO_URI}/api/metalakes/${METALAKE}/objects/table/${CATALOG}.${SCHEMA}.table1/statistics"
curl_json -X GET \
  "${GRAVITINO_URI}/api/metalakes/${METALAKE}/objects/table/${CATALOG}.${SCHEMA}.table1/statistics/partitions"

echo "Step 10: Write metrics JSONL"
cat > p-table-metrics.jsonl <<'JSONL'
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"US"},"TABLE_STORAGE_COST":60}
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"CA"},"TABLE_STORAGE_COST":40}
{"identifier":"db.table2","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"TABLE_STORAGE_COST":120}
JSONL

echo "Step 10: Append metrics"
"${OPTIMIZER_BIN}" -type append_metrics -computer-name local-stats-computer --file-path ./p-table-metrics.jsonl

echo "Step 11: Write job mapping JSONL for job metrics"
cat > job-mappings.jsonl <<'JSONL'
{"identifier":"generic.db.table1","job-identifiers":["job-1"]}
{"identifier":"generic.db.table2","job-identifiers":["job-2"]}
JSONL
JOB_MAPPINGS_PATH="$(pwd)/job-mappings.jsonl"
OPTIMIZER_CONF="${OPTIMIZER_CONF:-./conf/gravitino-optimizer.conf}"
if [[ -f "${OPTIMIZER_CONF}" ]]; then
  {
    echo ""
    echo "# Added by pinterest-mvp-steps.sh for local job provider"
    echo "gravitino.optimizer.monitor.jobProvider = file-job-provider"
    echo "gravitino.optimizer.monitor.file-job-provider.file-path = ${JOB_MAPPINGS_PATH}"
  } >> "${OPTIMIZER_CONF}"
  echo "Appended file job provider config to ${OPTIMIZER_CONF}"
else
  echo "Warning: ${OPTIMIZER_CONF} not found; set job provider file path manually to ${JOB_MAPPINGS_PATH}"
fi

echo "Step 12: Recommend strategy type"
"${OPTIMIZER_BIN}" -type recommend_strategy_type --identifiers \
  "${CATALOG}.${SCHEMA}.table1,${CATALOG}.${SCHEMA}.table2" \
  --strategy-type compaction

echo "Step 13: Append job metrics before compaction and list job metrics"
"${OPTIMIZER_BIN}" -type append_metrics -computer-name local-stats-computer \
  --statistics-payload '{"identifier":"job-1","stats-type":"job","JOB_RUNTIME_MS":120,"JOB_OUTPUT_FILES":8}'
"${OPTIMIZER_BIN}" -type list_job_metrics --identifiers job-1

echo "Step 14: Record action time and pause before compaction"
ACTION_TIME="$(date +%s)"
sleep 2

echo "Step 14: Perform compaction action on the table(s)"

echo "Step 15: Write post-compaction metrics JSONL"
cat > p-table-metrics.jsonl <<'JSONL'
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"US"},"TABLE_STORAGE_COST":6}
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"CA"},"TABLE_STORAGE_COST":4}
{"identifier":"db.table2","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"TABLE_STORAGE_COST":12}
JSONL

echo "Step 15: Append post-compaction metrics"
"${OPTIMIZER_BIN}" -type append_metrics -computer-name local-stats-computer --file-path ./p-table-metrics.jsonl

echo "Step 16: Append job metrics after compaction and list job metrics"
"${OPTIMIZER_BIN}" -type append_metrics -computer-name local-stats-computer \
  --statistics-payload '{"identifier":"job-1","stats-type":"job","JOB_RUNTIME_MS":60,"JOB_OUTPUT_FILES":4}'
"${OPTIMIZER_BIN}" -type list_job_metrics --identifiers job-1

echo "Step 17: List and monitor metrics"
"${OPTIMIZER_BIN}" -type list_table_metrics --identifiers \
  "${CATALOG}.${SCHEMA}.table1" --partition-path country=CA
"${OPTIMIZER_BIN}" -type monitor_metrics --identifiers \
  "${SCHEMA}.table1" --action-time "${ACTION_TIME}" \
  --partition-path country=CA
"${OPTIMIZER_BIN}" -type monitor_metrics --identifiers \
  "${SCHEMA}.table1" --action-time "${ACTION_TIME}"
