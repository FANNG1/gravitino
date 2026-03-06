---
title: "How to use optimizer"
slug: /how-to-use-optimizer
keywords:
  - optimizer
license: "This software is licensed under the Apache License version 2."
---

## Overview

Gravitino Optimizer is a CLI toolkit for:

- updating table/partition statistics,
- appending table/partition/job metrics,
- submitting strategy jobs,
- evaluating monitor results directly,
- interacting with an external monitor service.

The CLI entrypoint is:

```shell
./bin/gravitino-optimizer.sh --type <command> [options]
```

Supported `--type` values:

- `submit-strategy-jobs`
- `update-statistics`
- `append-metrics`
- `monitor-metrics`
- `submit-monitor`
- `list-monitors`
- `get-monitor`
- `cancel-monitor`
- `list-table-metrics`
- `list-job-metrics`

Note:
- `--type` must use kebab-case (for example `update-statistics`, not `update_statistics`).
- The old command names like `recommend_strategy_type`, `append_metrics`, `run-monitor-service`, and `register_tables` are not supported.

## Build

Prerequisite: Java 17.

```shell
./gradlew clean compileDistribution -x test
```

After build, use `distribution/package` as `GRAVITINO_HOME`.

## Minimal config

Create/update `conf/gravitino-optimizer.conf`:

```properties
gravitino.optimizer.gravitinoUri = http://localhost:8090
gravitino.optimizer.gravitinoMetalake = test
gravitino.optimizer.gravitinoDefaultCatalog = generic

# Recommender providers
gravitino.optimizer.recommender.statisticsProvider = gravitino-statistics-provider
gravitino.optimizer.recommender.strategyProvider = gravitino-strategy-provider
gravitino.optimizer.recommender.tableMetaProvider = gravitino-table-metadata-provider
gravitino.optimizer.recommender.jobSubmitter = noop-job-submitter

# Required for strategy.type = compaction
gravitino.optimizer.strategyHandler.compaction.className = \
  org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler

# Updater providers
gravitino.optimizer.updater.statisticsUpdater = gravitino-statistics-updater
gravitino.optimizer.updater.metricsUpdater = gravitino-metrics-updater

# Monitor providers
gravitino.optimizer.monitor.metricsProvider = gravitino-metrics-provider
gravitino.optimizer.monitor.tableJobRelationProvider = dummy-table-job-relation-provider
gravitino.optimizer.monitor.metricsEvaluator = gravitino-metrics-evaluator

# Optional evaluator rules
# gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules = table:table_storage_cost:latest:le
```

To load table->job relations from local JSONL:

```properties
gravitino.optimizer.monitor.tableJobRelationProvider = local-table-job-relation-provider
gravitino.optimizer.monitor.localTableJobRelationProvider.filePath = /path/to/job-mappings.jsonl
```

`job-mappings.jsonl` format (one JSON per line):

```json
{"identifier":"catalog.schema.table","job-identifiers":["job-1","job-2"]}
{"identifier":"schema.table","job-identifiers":["job-3"]}
```

## Core command usage

### 1) `update-statistics`

```shell
./bin/gravitino-optimizer.sh \
  --type update-statistics \
  --calculator-name local-stats-calculator \
  --file-path ./table-stats.jsonl
```

Or inline payload:

```shell
./bin/gravitino-optimizer.sh \
  --type update-statistics \
  --calculator-name local-stats-calculator \
  --statistics-payload '{"identifier":"db.table1","stats-type":"table","custom-row_count":1000}'
```

### 2) `append-metrics`

```shell
./bin/gravitino-optimizer.sh \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --file-path ./table-metrics.jsonl
```

### 3) `submit-strategy-jobs`

```shell
./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers generic.db.table1,generic.db.table2 \
  --strategy-name compactionFilterCount \
  --dry-run
```

Submit real jobs:

```shell
./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers generic.db.table1,generic.db.table2 \
  --strategy-name compactionFilterCount \
  --limit 10
```

### 4) `monitor-metrics`

```shell
ACTION_TIME=$(date +%s)
./bin/gravitino-optimizer.sh \
  --type monitor-metrics \
  --identifiers generic.db.table1 \
  --action-time "${ACTION_TIME}" \
  --range-seconds 3600
```

Partition scope (`--partition-path` for this command must be a JSON array):

```shell
./bin/gravitino-optimizer.sh \
  --type monitor-metrics \
  --identifiers generic.db.table2 \
  --partition-path '[{"bucket_col_bucket_8":"3"}]' \
  --action-time "${ACTION_TIME}" \
  --range-seconds 3600
```

### 5) List metrics

```shell
./bin/gravitino-optimizer.sh --type list-table-metrics --identifiers generic.db.table1
./bin/gravitino-optimizer.sh --type list-job-metrics --identifiers job-1
```

## Monitor service commands

These commands call an **external monitor service** URL.

```shell
# submit
./bin/gravitino-optimizer.sh \
  --type submit-monitor \
  --identifier generic.db.table1 \
  --action-time-seconds "$(date +%s)" \
  --range-seconds 3600 \
  --monitor-service-url http://localhost:8000

# list
./bin/gravitino-optimizer.sh \
  --type list-monitors \
  --monitor-service-url http://localhost:8000

# get
./bin/gravitino-optimizer.sh \
  --type get-monitor \
  --monitor-id <monitor-id> \
  --monitor-service-url http://localhost:8000

# cancel
./bin/gravitino-optimizer.sh \
  --type cancel-monitor \
  --monitor-id <monitor-id> \
  --monitor-service-url http://localhost:8000
```

Important partition-path note:
- `submit-monitor --partition-path` expects legacy format: `col1=v1/col2=v2`.
- `monitor-metrics` / `list-table-metrics --partition-path` expect JSON array format: `[{"col1":"v1"},{"col2":"v2"}]`.

## Local stats/metrics JSON format (`local-stats-calculator`)

Each line is one JSON object.

Supported common fields:
- `identifier`: `catalog.schema.table`, `schema.table`, or job identifier.
- `stats-type`: `table` | `partition` | `job`.
- `timestamp`: optional epoch seconds (if omitted, current time is used for metrics).
- `partition-path`: required for `stats-type=partition`, format is JSON object, for example `{"country":"US"}`.

Example:

```json
{"identifier":"db.table1","stats-type":"table","custom-filter_count":12}
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"US"},"table_storage_cost":100}
{"identifier":"job-1","stats-type":"job","job_runtime_ms":3000}
```

## Policy properties for recommender

For compaction strategy handlers, policy properties should include:

```json
{
  "strategy.type": "compaction",
  "job.template-name": "compaction-job"
}
```

`submit-strategy-jobs --strategy-name` uses the **policy name** (not `strategy.type`).
