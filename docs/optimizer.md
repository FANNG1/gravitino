---
title: "Table Maintenance Service (Optimizer)"
slug: /table-maintenance-service
keyword: table maintenance, optimizer, statistics, metrics, monitor
license: This software is licensed under the Apache License version 2.
---

## What is this service

The Table Maintenance Service (Optimizer) is a CLI service that helps you automate routine
maintenance tasks:

- Update table and partition statistics
- Append table and job metrics
- Evaluate maintenance rules from metrics
- Recommend and submit maintenance jobs

The CLI command and configuration keys keep using the `optimizer` name for compatibility.

## Before you start

- Prepare a running Gravitino server
- Configure `conf/gravitino-optimizer.conf`
- Use fully qualified identifiers where possible, for example `catalog.schema.table`

You can start from the template file `conf/gravitino-optimizer.conf.template` in the project root.

## Quick start

### Quick start A: Built-in table maintenance workflow

This workflow is based on:

- Built-in policy type: `system_iceberg_compaction`
- Built-in update stats job template: `builtin-iceberg-update-stats`
- Built-in rewrite data files job template: `builtin-iceberg-rewrite-data-files`

#### 1. Preflight checks

Check built-in job templates:

```bash
curl -sS "http://localhost:8090/api/metalakes/test/jobs/templates" | jq
```

Expected names include both:

- `builtin-iceberg-update-stats`
- `builtin-iceberg-rewrite-data-files`

If a template is missing, verify your deployment packages include the latest `gravitino-jobs` JAR in `auxlib`, then restart the Gravitino server.

For Spark job templates, set one of the following before starting Gravitino server:

- `gravitino.jobExecutor.local.sparkHome`
- `SPARK_HOME`

Without this, Spark jobs may stay in queued state or fail to start.

#### 2. Submit built-in update stats job

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "jobTemplateName": "builtin-iceberg-update-stats",
    "jobConf": {
      "table_identifier": "db.t1",
      "catalog_name": "rest_catalog",
      "gravitino_uri": "http://localhost:8090",
      "metalake": "test",
      "update_mode": "all",
      "statistics_updater": "gravitino-statistics-updater",
      "metrics_updater": "gravitino-metrics-updater",
      "spark_master": "local[2]",
      "spark_executor_instances": "1",
      "spark_executor_cores": "1",
      "spark_executor_memory": "1g",
      "spark_driver_memory": "1g",
      "catalog_type": "rest",
      "catalog_uri": "http://localhost:9001/iceberg",
      "warehouse_location": ""
    }
  }' \
  http://localhost:8090/api/metalakes/test/jobs/runs
```

#### 3. Ensure built-in compaction policy is attached to the target table

```bash
curl -sS "http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/policies?details=true" | jq
```

The returned policy list should include a policy whose `policyType` is `system_iceberg_compaction`.

#### 4. Submit built-in rewrite data files job

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "jobTemplateName": "builtin-iceberg-rewrite-data-files",
    "jobConf": {
      "catalog_name": "rest_catalog",
      "table_identifier": "db.t1",
      "strategy": "binpack",
      "sort_order": "",
      "where_clause": "",
      "options": "",
      "spark_master": "local[2]",
      "spark_executor_instances": "1",
      "spark_executor_cores": "1",
      "spark_executor_memory": "1g",
      "spark_driver_memory": "1g",
      "catalog_type": "rest",
      "catalog_uri": "http://localhost:9001/iceberg",
      "warehouse_location": "",
      "spark_conf": "{\"spark.master\":\"local[2]\"}"
    }
  }' \
  http://localhost:8090/api/metalakes/test/jobs/runs
```

#### 5. Verify job status

```bash
curl -sS "http://localhost:8090/api/metalakes/test/jobs/runs/<job-id>" | jq
```

### Quick start B: Optimizer CLI (local calculator)

#### 1. Minimal configuration

At minimum, set these in `conf/gravitino-optimizer.conf`:

```properties
gravitino.optimizer.gravitinoUri = http://localhost:8090
gravitino.optimizer.gravitinoMetalake = test
gravitino.optimizer.gravitinoDefaultCatalog = generic
```

#### 2. Prepare a local JSONL file

Create `table-stats.jsonl`:

```json
{"stats-type":"table","identifier":"catalog.db.sales","row_count":100000,"data_size":8388608,"timestamp":1735689600}
{"stats-type":"partition","identifier":"catalog.db.sales","partition-path":{"dt":"2026-01-01"},"row_count":12000,"data_size":1048576,"timestamp":1735689600}
```

#### 3. Update statistics

```bash
./bin/gravitino-optimizer.sh \
  --type update-statistics \
  --calculator-name local-stats-calculator \
  --file-path ./table-stats.jsonl \
  --conf-path ./conf/gravitino-optimizer.conf
```

Expected output:

```text
SUMMARY: statistics totalRecords=... tableRecords=... partitionRecords=... jobRecords=...
```

## Command quick reference

Use `--help` to list all commands, or `--help --type <command>` for command-specific help.

| Command (`--type`) | Required options | Optional options | Purpose |
| --- | --- | --- | --- |
| `submit-strategy-jobs` | `--identifiers`, `--strategy-name` | `--dry-run`, `--limit` | Recommend and optionally submit jobs |
| `update-statistics` | `--calculator-name` | `--identifiers`, `--statistics-payload`, `--file-path` | Calculate and persist statistics |
| `append-metrics` | `--calculator-name` | `--identifiers`, `--statistics-payload`, `--file-path` | Calculate and append metrics |
| `monitor-metrics` | `--identifiers`, `--action-time` | `--range-seconds`, `--partition-path` | Evaluate rules with before/after metrics |
| `list-table-metrics` | `--identifiers` | `--partition-path` | Query stored table/partition metrics |
| `list-job-metrics` | `--identifiers` | None | Query stored job metrics |

## Input format for `local-stats-calculator`

`local-stats-calculator` reads JSON Lines (one JSON object per line).

### Reserved fields

- `stats-type`: `table`, `partition`, or `job`
- `identifier`: object identifier
- `partition-path`: only for partition data, for example `{"dt":"2026-01-01"}`
- `timestamp`: epoch seconds

All other fields are treated as metric/statistic values.

### Supported value forms

Both forms are supported:

```json
{"stats-type":"table","identifier":"catalog.db.t1","row_count":100}
{"stats-type":"table","identifier":"catalog.db.t1","row_count":{"value":100,"timestamp":1735689600}}
```

### Identifier rules

- Table and partition records: `catalog.schema.table`
- If `gravitino.optimizer.gravitinoDefaultCatalog` is set, `schema.table` is also accepted
- Job records: parsed as a regular Gravitino `NameIdentifier`

## Typical workflows

### Update statistics in batch

```bash
./bin/gravitino-optimizer.sh \
  --type update-statistics \
  --calculator-name local-stats-calculator \
  --file-path ./table-stats.jsonl \
  --conf-path ./conf/gravitino-optimizer.conf
```

### Append metrics in batch

```bash
./bin/gravitino-optimizer.sh \
  --type append-metrics \
  --calculator-name local-stats-calculator \
  --file-path ./table-stats.jsonl \
  --conf-path ./conf/gravitino-optimizer.conf
```

### Dry-run strategy submission

```bash
./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers catalog.db.sales,catalog.db.orders \
  --strategy-name compaction-high-file-count \
  --dry-run \
  --limit 10 \
  --conf-path ./conf/gravitino-optimizer.conf
```

### Monitor metrics

```bash
./bin/gravitino-optimizer.sh \
  --type monitor-metrics \
  --identifiers catalog.db.sales \
  --action-time 1735689600 \
  --range-seconds 86400 \
  --conf-path ./conf/gravitino-optimizer.conf
```

You can configure evaluator rules in `gravitino-optimizer.conf`:

```properties
gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules = table:row_count:avg:le,job:duration:latest:le
```

Rule format is `scope:metricName:aggregation:comparison`:

- `scope`: `table` or `job` (`table` rules also apply to partition scope)
- `aggregation`: `max|min|avg|latest`
- `comparison`: `lt|le|gt|ge|eq|ne`

## Output guide

- `SUMMARY: ...`: results for `update-statistics` and `append-metrics`
- `DRY-RUN: ...`: recommendation preview without job submission
- `SUBMIT: ...`: job submitted successfully
- `MetricsResult{...}`: returned by list commands
- `EvaluationResult{...}`: returned by monitor command

## Troubleshooting

### `Invalid --type`

Use kebab-case values such as `update-statistics`, not `update_statistics`.

### `--statistics-payload and --file-path cannot be used together`

For `local-stats-calculator`, use exactly one of them.

### `requires one of --statistics-payload or --file-path`

When `--calculator-name local-stats-calculator` is used, one input source is required.

### `--partition-path must be a JSON array`

Use a JSON array format, for example:

```text
[{"dt":"2026-01-01"}]
```

### `Specified optimizer config file does not exist`

Check your `--conf-path` and file permissions.
