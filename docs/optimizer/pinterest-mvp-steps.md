---
title: "Pinterest MVP with Gravitino Optimizer"
slug: /pinterest-mvp
keywords:
  - optimizer
license: "This software is licensed under the Apache License version 2."
---

## Goal

This walkthrough validates an MVP optimizer flow using current CLI commands:

1. create Gravitino objects (metalake/catalog/schema/tables),
2. create and bind a compaction policy,
3. write statistics and metrics,
4. submit strategy jobs,
5. evaluate metrics around an action timestamp.

You can run it manually or use:

```shell
bash maintenance/optimizer/bin/pinterest-mvp-steps.sh
```

## 1) Build and start Gravitino

```shell
./gradlew clean compileDistribution -x test -x rat -x lintOpenAPI

export GRAVITINO_HOME="$(pwd)/distribution/package"
cd "${GRAVITINO_HOME}"
./bin/gravitino-server.sh start
```

## 2) Configure optimizer

Use `conf/gravitino-optimizer.conf`:

```properties
gravitino.optimizer.gravitinoUri = http://localhost:8090
gravitino.optimizer.gravitinoMetalake = test
gravitino.optimizer.gravitinoDefaultCatalog = generic

gravitino.optimizer.recommender.jobSubmitter = noop-job-submitter
gravitino.optimizer.strategyHandler.compaction.className = \
  org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler

gravitino.optimizer.monitor.metricsProvider = gravitino-metrics-provider
gravitino.optimizer.monitor.metricsEvaluator = gravitino-metrics-evaluator
# optional for deterministic monitor result:
# gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules = table:table_storage_cost:latest:le
```

## 3) Create metalake/catalog/schema/tables

```shell
curl -sS -X POST -H "Content-Type: application/json" \
  -d '{"name":"test","comment":"Optimizer quickstart","properties":{}}' \
  http://localhost:8090/api/metalakes

curl -sS -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
    "name": "generic",
    "type": "RELATIONAL",
    "comment": "Generic lakehouse catalog",
    "provider": "lakehouse-generic",
    "properties": {"location": "mock-location"}
  }' http://localhost:8090/api/metalakes/test/catalogs

curl -sS -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
    "name": "db",
    "comment": "optimizer schema",
    "properties": {}
  }' http://localhost:8090/api/metalakes/test/catalogs/generic/schemas

curl -sS -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
    "name": "table1",
    "comment": "demo table1",
    "columns": [
      {"name":"id","type":"long","nullable":true},
      {"name":"country","type":"string","nullable":true}
    ],
    "properties": {"format":"iceberg"}
  }' http://localhost:8090/api/metalakes/test/catalogs/generic/schemas/db/tables

curl -sS -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
    "name": "table2",
    "comment": "demo table2",
    "columns": [
      {"name":"id","type":"long","nullable":true},
      {"name":"bucket_col","type":"string","nullable":true}
    ],
    "properties": {"format":"iceberg"}
  }' http://localhost:8090/api/metalakes/test/catalogs/generic/schemas/db/tables
```

## 4) Create compaction policy and attach to schema

Current recommender uses policy property `strategy.type`.

```shell
curl -sS -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
    "name": "compactionFilterCount",
    "comment": "Compaction policy based on custom filter count",
    "policyType": "custom",
    "enabled": true,
    "content": {
      "customRules": {
        "trigger-expr":"custom-filter_count > 5",
        "score-expr":"custom-filter_count"
      },
      "supportedObjectTypes": ["CATALOG","SCHEMA","TABLE"],
      "properties": {
        "strategy.type": "compaction",
        "job.template-name": "compaction-job"
      }
    }
  }' http://localhost:8090/api/metalakes/test/policies

curl -sS -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
    "policiesToAdd": ["compactionFilterCount"],
    "policiesToRemove": []
  }' http://localhost:8090/api/metalakes/test/objects/schema/generic.db/policies
```

## 5) Prepare local JSONL for statistics and metrics

```shell
cat > p-table-stats.jsonl <<'JSONL'
{"identifier":"db.table1","stats-type":"table","custom-filter_count":60}
{"identifier":"db.table2","stats-type":"table","custom-filter_count":120}
JSONL

cat > p-table-metrics-before.jsonl <<'JSONL'
{"identifier":"db.table1","stats-type":"table","table_storage_cost":1000}
{"identifier":"db.table2","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"table_storage_cost":2000}
JSONL
```

## 6) Update statistics and append metrics

```shell
./bin/gravitino-optimizer.sh \
  --type update-statistics \
  --calculator-name local-stats-calculator \
  --file-path ./p-table-stats.jsonl

./bin/gravitino-optimizer.sh \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --file-path ./p-table-metrics-before.jsonl
```

## 7) Dry-run and submit strategy jobs

```shell
./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers generic.db.table1,generic.db.table2 \
  --strategy-name compactionFilterCount \
  --dry-run

./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers generic.db.table1,generic.db.table2 \
  --strategy-name compactionFilterCount \
  --limit 2
```

## 8) Append post-action metrics and run monitor-metrics

```shell
ACTION_TIME=$(date +%s)
sleep 2

cat > p-table-metrics-after.jsonl <<'JSONL'
{"identifier":"db.table1","stats-type":"table","table_storage_cost":300}
{"identifier":"db.table2","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"table_storage_cost":500}
JSONL

./bin/gravitino-optimizer.sh \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --file-path ./p-table-metrics-after.jsonl

./bin/gravitino-optimizer.sh \
  --type monitor-metrics \
  --identifiers generic.db.table1 \
  --action-time "${ACTION_TIME}" \
  --range-seconds 3600

./bin/gravitino-optimizer.sh \
  --type monitor-metrics \
  --identifiers generic.db.table2 \
  --partition-path '[{"bucket_col_bucket_8":"3"}]' \
  --action-time "${ACTION_TIME}" \
  --range-seconds 3600
```

## 9) Optional: query stored metrics

```shell
./bin/gravitino-optimizer.sh --type list-table-metrics --identifiers generic.db.table1
./bin/gravitino-optimizer.sh --type list-table-metrics \
  --identifiers generic.db.table2 \
  --partition-path '[{"bucket_col_bucket_8":"3"}]'
```

## Notes

- `submit-strategy-jobs --strategy-name` expects policy name, not strategy type.
- `monitor-metrics --partition-path` expects JSON array format.
- If monitor evaluator rules are not configured, evaluation defaults to pass.
