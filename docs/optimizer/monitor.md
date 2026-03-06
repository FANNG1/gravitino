---
title: "Monitor service step-by-step guide"
slug: /optimizer-monitor
keywords:
  - optimizer
  - monitor
license: "This software is licensed under the Apache License version 2."
---

## Overview

Optimizer monitoring has two modes:

1. **Direct evaluation mode** via `--type monitor-metrics` (no monitor service).
2. **Asynchronous monitor service mode** via `submit-monitor` / `get-monitor` / `list-monitors` / `cancel-monitor`.

This document focuses on mode 2.

## Prerequisites

- Gravitino server is running.
- Metrics are written through `append-metrics`.
- A monitor service is running and reachable at `http://<host>:<port>`.

Health check:

```shell
curl -sS http://localhost:8000/v1/health
```

Expected response: `OK`.

## Required optimizer config

`conf/gravitino-optimizer.conf` (or your custom conf via `--conf-path`):

```properties
gravitino.optimizer.gravitinoUri = http://localhost:8090
gravitino.optimizer.gravitinoMetalake = test
gravitino.optimizer.gravitinoDefaultCatalog = generic

gravitino.optimizer.monitor.metricsProvider = gravitino-metrics-provider
gravitino.optimizer.monitor.metricsEvaluator = gravitino-metrics-evaluator

# optional: custom evaluator rules
# gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules = table:row_count:latest:le,job:job_status:latest:le

# default provider is dummy-table-job-relation-provider
# for local mapping file, switch to:
# gravitino.optimizer.monitor.tableJobRelationProvider = local-table-job-relation-provider
# gravitino.optimizer.monitor.localTableJobRelationProvider.filePath = /path/to/job-mappings.jsonl
```

## Important format differences

Partition path format is different across commands:

- `submit-monitor --partition-path`: legacy format, for example `dt=2026-03-01/country=US`.
- `monitor-metrics --partition-path`: JSON array format, for example `[{"dt":"2026-03-01"},{"country":"US"}]`.
- `append-metrics` payload `partition-path`: JSON object format, for example `{"dt":"2026-03-01","country":"US"}`.

## End-to-end monitor service flow

### 1) Append metrics before action

```shell
./bin/gravitino-optimizer.sh \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --statistics-payload '{"identifier":"generic.db.table1","stats-type":"table","row_count":100,"table_storage_cost":1000}'

./bin/gravitino-optimizer.sh \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --statistics-payload '{"identifier":"job-1","stats-type":"job","job_status":0}'
```

### 2) Submit monitor request

```shell
ACTION_TIME=$(date +%s)

MONITOR_ID=$(./bin/gravitino-optimizer.sh \
  --type submit-monitor \
  --identifier generic.db.table1 \
  --action-time-seconds "${ACTION_TIME}" \
  --range-seconds 3600 \
  --monitor-service-url http://localhost:8000)

echo "monitor id: ${MONITOR_ID}"
```

Submit partition monitor:

```shell
./bin/gravitino-optimizer.sh \
  --type submit-monitor \
  --identifier generic.db.table2 \
  --partition-path 'bucket_col_bucket_8=3' \
  --action-time-seconds "${ACTION_TIME}" \
  --range-seconds 3600 \
  --monitor-service-url http://localhost:8000
```

### 3) Append metrics after action

```shell
./bin/gravitino-optimizer.sh \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --statistics-payload '{"identifier":"generic.db.table1","stats-type":"table","row_count":90,"table_storage_cost":800}'

./bin/gravitino-optimizer.sh \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --statistics-payload '{"identifier":"job-1","stats-type":"job","job_status":-1}'
```

### 4) Query monitor status

```shell
./bin/gravitino-optimizer.sh \
  --type get-monitor \
  --monitor-id "${MONITOR_ID}" \
  --monitor-service-url http://localhost:8000
```

### 5) List and cancel

```shell
./bin/gravitino-optimizer.sh \
  --type list-monitors \
  --monitor-service-url http://localhost:8000

./bin/gravitino-optimizer.sh \
  --type cancel-monitor \
  --monitor-id "${MONITOR_ID}" \
  --monitor-service-url http://localhost:8000
```

## Notes

- `submit-monitor` returns only the monitor id.
- `get-monitor`/`list-monitors` print the decoded response object.
- If no evaluator rules are configured, `gravitino-metrics-evaluator` treats evaluation as pass.
- The optimizer CLI does not provide a `run-monitor-service` command. Start monitor service from your deployment/runtime and then call it with `--monitor-service-url`.
