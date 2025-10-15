---
title: "How to use optimizer"
slug: /how-to-use-optimizer
keywords:
  - optimizer
license: "This software is licensed under the Apache License version 2."
---

## Background

Gravitino Optimizer is a small toolkit that talks to a Gravitino server to compute and refresh table-level statistics/metrics, recommend optimization strategies (for example, compaction), and monitor whether those strategies improved the tables. Everything is shipped as a CLI script (`bin/gravitino-optimizer.sh`) that reads a config file (`conf/gravitino-optimizer.conf`) and then calls the right "tool" depending on the `--type` you pass in:

- `update_statistics`: push statistics points (for example, custom delete-file counts) to
  Gravitino.
- `append_metrics`: push table metrics (for example, storage cost) to Gravitino.
- `recommend_strategy_type`: recommend a strategy for one or more tables.
- `monitor_metrics`: compare metrics before/after an action time and report improvements.
- `register_tables`: register table metadata into Gravitino from a local or S3 JSONL file or directory that contains JSONL files.
- `list_table_metrics`: list metrics for one or more tables (optionally for a specific partition path).
- `list_job_metrics`: list job metrics associated with one or more tables.

The optimizer is fully pluggable—each provider can be swapped for your own implementation without changing the workflow. Defaults are wired to Gravitino components; you can override them in `conf/gravitino-optimizer.conf`:

| config key                                                            | Purpose                                              | Default implementation              |
|-----------------------------------------------------------------------|------------------------------------------------------|-------------------------------------|
| `gravitino.optimizer.recommender.statisticsProvider` (`SupportTableStatistics`) | Fetch table/partition statistics for recommendations | `gravitino-statistics-provider`     |
| `gravitino.optimizer.recommender.strategyProvider`                    | Fetch strategies                                     | `gravitino-strategy-provider`       |
| `gravitino.optimizer.recommender.tableMetaProvider`                   | Fetch table metadata (schema/catalog info)           | `gravitino-table-metadata-provider` |
| `gravitino.optimizer.recommender.jobSubmitter`                        | Submit chosen strategies/jobs                        | `noop-job-submitter`                |
| `gravitino.optimizer.updater.statisticsUpdater`                       | Persist statistics during `update_statistics`        | `gravitino-statistics-updater`      |
| `gravitino.optimizer.updater.metricsUpdater`                          | Persist metrics during `append_metrics`              | `gravitino-metrics-updater`         |
| `gravitino.optimizer.monitor.metricsProvider`                         | Read metrics for monitoring                          | `gravitino-metrics-provider`        |
| `gravitino.optimizer.monitor.jobProvider`                             | Retrieve upstream and downstream jobs tied to table  | `dummy-job-provider`                |
| `gravitino.optimizer.monitor.metricsEvaluator`                        | Compare table and job metrics before/after actions   | `gravitino-metrics-evaluator`       |

## How to build

Prerequisites: Java 17. 

```shell
./gradlew clean compileDistribution -x test
```

After the build, the Gravitino package will be in `distribution/package`. Treat that path as `GRAVITINO_HOME` in the steps below.

## How to start up Gravitino server and create metalake & catalog & schema & table

1. Set up environment and start the server

   ```shell
   export GRAVITINO_HOME="$(pwd)/distribution/package"
   cd "$GRAVITINO_HOME"
   ./bin/gravitino-server.sh start
   tail -f logs/gravitino-server.log
   curl -s http://localhost:8090/api/version
   ```

2. Create a metalake

A metalake is the top-level namespace in Gravitino that groups catalogs/schemas/tables plus shared policies. Create one for this walkthrough:

   ```shell
   curl -X POST -H "Content-Type: application/json" \
   -d '{"name":"demo","comment":"Optimizer quickstart","properties":{}}' \
   http://localhost:8090/api/metalakes
   ```

3. Create an Iceberg catalog inside the metalake (requires a reachable Hive Metastore and warehouse path—adjust for your environment)

   ```shell
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
   -H "Content-Type: application/json" -d '{
       "name": "iceberg",
       "comment": "demo iceberg catalog",
       "type": "RELATIONAL",
       "provider": "lakehouse-iceberg",
       "properties": {
           "catalog-backend": "hive",
           "uri": "thrift://127.0.0.1:9083",
           "warehouse": "/user/hive/warehouse-hive/"
       }
   }' http://localhost:8090/api/metalakes/demo/catalogs
   ```
Please refer to https://gravitino.apache.org/docs/1.0.0/lakehouse-iceberg-catalog#catalog-properties for property details.

4. Create a schema in that catalog (choose any name; properties are optional for Iceberg)

   ```shell
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
   -H "Content-Type: application/json" -d '{
  "name": "db",
  "comment": "demo schema",
  "properties": {}
}' http://localhost:8090/api/metalakes/demo/catalogs/iceberg/schemas
   ```

5. Create a table in the schema (simple two-column example)

   ```shell
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
   -H "Content-Type: application/json" -d '{
  "name": "table1",
  "comment": "demo table",
  "columns": [
    {"name": "id", "type": "integer", "nullable": false},
    {"name": "payload", "type": "string", "nullable": true}
  ],
  "properties": {}
}' http://localhost:8090/api/metalakes/demo/catalogs/iceberg/schemas/db/tables
```

You now have `demo.iceberg.db.table1` ready for optimizer workflows. Repeat the table creation call with a different `name` (for example, `table2`) if you want multiple tables.

## How to run the optimizer tools

The optimizer uses `conf/gravitino-optimizer.conf` for defaults. A minimal config that talks to the local Gravitino server and demo metalake looks like:

```
gravitino.optimizer.gravitinoMetalake = demo
gravitino.optimizer.gravitinoUri = http://localhost:8090
gravitino.optimizer.gravitinoDefaultCatalog = iceberg
# Optional: switch job submitter/compute provider later
gravitino.optimizer.recommender.jobSubmitter = noop-job-submitter
```

Key configuration knobs (full list and defaults live in `conf/gravitino-optimizer.conf.template`):
- `gravitino.optimizer.gravitinoUri`: Gravitino server URL (defaults to `http://localhost:8090`).
- `gravitino.optimizer.gravitinoMetalake`: required metalake name the optimizer targets.
- `gravitino.optimizer.gravitinoDefaultCatalog`: fallback catalog if identifiers omit a catalog prefix.
- Recommender providers: `gravitino.optimizer.recommender.statisticsProvider`, `gravitino.optimizer.recommender.strategyProvider`, `gravitino.optimizer.recommender.tableMetaProvider`, `gravitino.optimizer.recommender.jobSubmitter` (defaults point to built-in Gravitino implementations; `noop-job-submitter` keeps jobs local).
- Updaters: `gravitino.optimizer.updater.statisticsUpdater`, `gravitino.optimizer.updater.metricsUpdater` (defaults push back into Gravitino).
- Monitor: `gravitino.optimizer.monitor.metricsProvider`, `gravitino.optimizer.monitor.jobProvider`, `gravitino.optimizer.monitor.metricsEvaluator` (defaults to Gravitino providers with a dummy job provider).

To read table-job mappings from a local file, set:

```
gravitino.optimizer.monitor.jobProvider = file-job-provider
gravitino.optimizer.monitor.file-job-provider.file-path = /path/to/job-mappings.jsonl
```

The job mapping file is JSONL, one table per line:

```
{"identifier":"catalog.schema.table","job-identifiers":["job1","job2"]}
{"identifier":"schema.table","job-identifiers":["job3"]}
```

Run the tool from the packaged directory:

```
cd distribution/package
./bin/gravitino-optimizer.sh --type xx
```

Common parameters:
- `--type`: optimizer action (`update_statistics`, `append_metrics`, `recommend_strategy_type`, `monitor_metrics`, `register_tables`, `list_table_metrics`, `list_job_metrics`).
- `--computer-name`: the computer/provider to pull stats/metrics from (use `local-stats-computer` for JSONL payloads).
- `--identifiers`: comma-separated table identifiers; use fully qualified `catalog.schema.table` if you work across catalogs.
  This is required for listing/monitoring actions but optional for `update_statistics` and `append_metrics`.
- `--partition-path`: optional for `monitor_metrics` and `list_table_metrics`; format `{"col1":"val1"}`, requires exactly one identifier, and switches to partition metrics instead of table metrics.
- `--statistics-payload`: inline JSON Lines payload for statistics/metrics computers that consume CLI payloads
  (for example, `{"identifier":"db.table1","stats-type":"table","TABLE_STORAGE_COST":1000}`).
- `--file-path`: path to a file containing the same payload format; useful when the payload is large. Also used for table registration input.
- `--strategy-type`: which strategy to recommend (for example, `compaction`).
- `--action-time`: epoch seconds used by `monitor_metrics` to compare before/after metrics.

Additional actions:
- Register tables from a local/S3 JSONL file or directory that contains JSONL files (one table definition per line): `./bin/gravitino-optimizer.sh -type register_tables --file-path ./table-metadata.jsonl`
- List table metrics: `./bin/gravitino-optimizer.sh -type list_table_metrics --identifiers demo.iceberg.db.table1`
- List job metrics: `./bin/gravitino-optimizer.sh -type list_job_metrics --identifiers demo.iceberg.db.table1`

CLI output goes to stdout and `logs/gravitino-optimizer.log` under `GRAVITINO_HOME`.

## Quick start 

Here is a quick start workflow to use Gravitino optimizer tools to update table statistics, recommend tables for the compaction strategy, and monitor table metrics after the compaction job is finished.

1. Create strategies (stored as Gravitino policies)

   The two example policies below are both of type `compaction` and can be attached at catalog/schema/table scopes. Each uses simple custom rules to drive the compaction strategy:
   - `trigger-expr`: when true, the strategy is applicable (for example, high delete-file count or high mean squared error of file sizes).
   - `score-expr`: how to rank tables once triggered; higher scores bubble up in recommendations.
   - `gravitino.policy.job.template-name`: the job template the recommender will submit if a job submitter is configured.
   - `gravitino.policy.type`: the strategy type. 
   
`compactionDelete` gives higher weight to position delete files, while `compactionSmall` focuses more on small data files.

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "compactionDelete",
  "comment": "This is a compaction strategy that weighs delete files more heavily",
  "policyType": "custom",
  "enabled": true,
  "content": {
    "customRules": {
      "min_datafile_mse": "1000",
      "trigger-expr":"custom-datafile_size_mse > min_datafile_mse || custom-position_delete_file_number > 1",
      "score-expr":"custom-datafile_size_mse/100 + custom-position_delete_file_number * 100"
    },
    "supportedObjectTypes": [
      "CATALOG",
      "SCHEMA",
      "TABLE"
    ],
    "properties": {
      "gravitino.policy.type": "compaction",
      "gravitino.policy.job.template-name":"compaction-job"
    }
  }
}' http://localhost:8090/api/metalakes/demo/policies
```

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "compactionSmall",
  "comment": "This is a compaction strategy that favors small file consolidation",
  "policyType": "custom",
  "enabled": true,
  "content": {
    "customRules": {
      "min_datafile_mse": "1000",
      "trigger-expr":"custom-datafile_size_mse > min_datafile_mse || custom-position_delete_file_number > 0",
      "score-expr":"custom-datafile_size_mse + custom-position_delete_file_number * 10"
    },
    "supportedObjectTypes": [
      "CATALOG",
      "SCHEMA",
      "TABLE"
    ],
    "properties": {
      "gravitino.policy.type": "compaction",
      "gravitino.policy.job.template-name":"compaction-job"
    }
  }
}' http://localhost:8090/api/metalakes/demo/policies
```

2. Attach the created policies to the schema

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "policiesToAdd": ["compactionDelete", "compactionSmall"],
  "policiesToRemove": []
}' http://localhost:8090/api/metalakes/demo/objects/schema/iceberg.db/policies
```

The tables in schema `iceberg.db` will inherit the attached policies.

Please refer to https://gravitino.apache.org/docs/1.0.0/manage-policies-in-gravitino for more details about policy.

3. Use the computer to compute statistics of the table and update statistics to Gravitino

The optimizer provides a built-in `local-stats-computer` to read JSON Lines from the CLI; you can also implement your own computer to compute statistics from external systems.

```shell
./bin/gravitino-optimizer.sh -type update_statistics -computer-name local-stats-computer \
  --statistics-payload '{"identifier":"db.table1","stats-type":"table","custom-position_delete_file_number":0,"custom-datafile_size_mse":10000.2}'
./bin/gravitino-optimizer.sh -type update_statistics -computer-name local-stats-computer \
  --statistics-payload '{"identifier":"db.table2","stats-type":"table","custom-position_delete_file_number":100,"custom-datafile_size_mse":100.2}'

# Alternatively, load the payload from a file:
./bin/gravitino-optimizer.sh -type update_statistics -computer-name local-stats-computer \
  --file-path /path/to/stats.jsonl
```

By default, the statistics are stored in Gravitino's internal statistics system. You can also implement your own statistics updater to push statistics to external systems.
   
4. Use the updater to update the metrics of the table

The difference between stats and metrics are the metrics has a timestamp.

```shell
./bin/gravitino-optimizer.sh -type append_metrics -computer-name local-stats-computer \
  --statistics-payload '{"identifier":"db.table1","stats-type":"table","TABLE_STORAGE_COST":1000,"DATAFILE_AVG_SIZE":100}'
./bin/gravitino-optimizer.sh -type append_metrics -computer-name local-stats-computer \
  --statistics-payload '{"identifier":"db.table2","stats-type":"table","TABLE_STORAGE_COST":2000,"DATAFILE_AVG_SIZE":200}'
```
   
5. Use the recommender to rank tables for the strategy

```shell
./bin/gravitino-optimizer.sh -type recommend_strategy_type --identifiers iceberg.db.table1,iceberg.db.table2 --strategy-type compaction
```

There is a built-in strategy handler for the strategy type `compaction` to recommend tables for compaction. You can provide your own strategy handler implementation for different strategy types. 

Since there are two strategies `compactionDelete` and `compactionSmall` with the same `compaction` type, each strategy will generate a recommend score for the table. The higher the score, the more urgent the table needs to be compacted.
You can see the recommend score for each strategy in `gravitino-optimizer.log`.

6. Perform your compaction action on the table(s)

Please record the compaction time as `action-time` manually, like `1763864715`.

7. Use the updater to update metrics after compaction

```shell
./bin/gravitino-optimizer.sh -type append_metrics -computer-name local-stats-computer \
  --statistics-payload '{"identifier":"db.table1","stats-type":"table","TABLE_STORAGE_COST":12,"DATAFILE_AVG_SIZE":10}'
./bin/gravitino-optimizer.sh -type append_metrics -computer-name local-stats-computer \
  --statistics-payload '{"identifier":"db.table2","stats-type":"table","TABLE_STORAGE_COST":22,"DATAFILE_AVG_SIZE":20}'
```
   
8. Use the monitor to evaluate the metrics

```shell
./bin/gravitino-optimizer.sh -type monitor_metrics --identifiers db.table1,db.table2 --action-time 1763864715
```

By default, Gravitino metrics evaluator will compare the metrics before and after the `action-time`. You could implement your own metrics evaluator to evaluate the metrics in your way.

You can see detailed metrics before and after compaction in `logs/gravitino-optimizer.log`. 

To monitor a specific partition instead of the whole table, pass `--partition-path` (requires exactly one identifier):

```shell
./bin/gravitino-optimizer.sh -type monitor_metrics --identifiers db.table1 --partition-path '{"country":"US","region":"CA"}' --action-time 1763864715
```
This switches monitoring to partition metrics for that path.
