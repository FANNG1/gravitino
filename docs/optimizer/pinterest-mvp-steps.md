---
title: "Pinterest MVP with Gravitino Optimizer"
slug: /pinterest-mvp
keywords:
  - optimizer
license: "This software is licensed under the Apache License version 2."
---

## Background
This walkthrough shows how to (1) bring mock Iceberg table metadata into Gravitino via the optimizer CLI, (2) load table and partition-level stats/metrics from a local JSONL file, and (3) run the recommender against those registered tables using a compaction strategy (stored as a Gravitino policy) that keys off custom stats.

## Steps

1. Prepare the environment and start the Gravitino server 

   ```shell
   # compile Gravitino and optimizer package to `distribution/package`
   ./gradlew clean compileDistribution -x test -x rat -x lintOpenAPI
   
   cd ./distribution/package/
   
   # start Gravitino server
   ./bin/gravitino.sh start
   ```
   
You could run the `sh ./optimizer/bin/pinterest-mvp-steps.sh` script to execute the following command or the steps one by one manually.

2. Create a metalake

A metalake is the top-level namespace in Gravitino that groups catalogs/schemas/tables plus shared policies. Create one for this walkthrough:

   ```shell
   curl -X POST -H "Content-Type: application/json" \
   -d '{"name":"test","comment":"Optimizer quickstart","properties":{}}' \
   http://localhost:8090/api/metalakes
   ```

3. Create a generic catalog inside the metalake 

A generic lakehouse catalog stores Iceberg table metadata directly in Gravitino (no external Hive Metastore or Iceberg REST needed).

   ```shell
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
   -H "Content-Type: application/json" -d '{
     "name": "generic",
     "type": "RELATIONAL",
     "comment": "Generic lakehouse catalog for Iceberg datasets",
     "provider": "lakehouse-generic",
     "properties": {
       "location": "mock-location"
     }
   }' http://localhost:8090/api/metalakes/test/catalogs
   ```

4. Add configuration for the optimizer

Change the following configuration in `conf/gravitino-optimizer.conf` according to your environment:

```
gravitino.optimizer.gravitinoUri = http://localhost:8090
gravitino.optimizer.gravitinoMetalake = test
## default catalog name when an identifier omits the catalog
gravitino.optimizer.gravitinoDefaultCatalog = generic
## enable local job provider for monitor job metrics
gravitino.optimizer.monitor.jobProvider = file-job-provider
gravitino.optimizer.monitor.file-job-provider.file-path = /path/to/job-mappings.jsonl
```

5. Mock table metadata locally and register it into the catalog

Create a JSONL file (one JSON object per line) that carries Iceberg schema/partition/sort fragments. The optimizer will build tables from `schema`, `partition_specs`, `sort_orders`, and the default IDs. The example below defines two tables: `table1` with columns `id`, `ts` (timestamp with tz), and `country`, partitioned by identity on `country`; and `table2` with columns `id` and `bucket_col`, partitioned by a bucket(8) transform on `bucket_col`.

Required fields per line:
- `identifier`: catalog.schema.table or schema.table (falls back to `gravitinoDefaultCatalog`).
- `schema`: Iceberg schema JSON (from `SchemaParser.toJson`).
- `partition_specs`: Iceberg partition specs JSON array (from `PartitionSpecParser.toJson`).
- `sort_orders`: Iceberg sort orders JSON array (from `SortOrderParser.toJson`); can be empty array for unsorted.
- `default_spec_id`: the spec-id to use.
- `default_sort_order_id`: the sort-order-id to use.

Example `table-metadata.jsonl`:

```shell
cat > table-metadata.jsonl <<'EOF'
{"identifier":"generic.db.table1","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"},{"id":2,"name":"ts","required":false,"type":"timestamp","original-type":"timestamp with time zone"},{"id":3,"name":"country","required":false,"type":"string"}]},"partition_specs":[{"spec-id":0,"fields":[{"source-id":3,"field-id":1000,"name":"country","transform":"identity"}]}],"sort_orders":[{"order-id":1,"fields":[{"transform":"identity","source-id":2,"direction":"asc","null-order":"nulls-last"}]}],"default_spec_id":0,"default_sort_order_id":1}
{"identifier":"db.table2","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"},{"id":2,"name":"bucket_col","required":false,"type":"string"}]},"partition_specs":[{"spec-id":0,"fields":[{"source-id":2,"field-id":2000,"name":"bucket_col_bucket_8","transform":"bucket[8]"}]}],"sort_orders":[{"order-id":1,"fields":[{"transform":"identity","source-id":2,"direction":"asc","null-order":"nulls-first"}]}],"default_spec_id":0,"default_sort_order_id":1}
EOF
```

Register the tables with the optimizer CLI 

```shell
./bin/gravitino-optimizer.sh -type register_tables --file-path ./table-metadata.jsonl
```

Verify the tables were registered via the Gravitino load table endpoint:

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/catalogs/generic/schemas/db/tables/table1
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/test/catalogs/generic/schemas/db/tables/table2
```

6. Create a compaction strategy (stored as a Gravitino policy) that uses custom stats `filter_count`

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "compactionFilterCount",
  "comment": "Compaction policy that factors in filter count",
  "policyType": "custom",
  "enabled": true,
  "content": {
    "customRules": {
      "trigger-expr":"custom-filter_count > 5",
      "score-expr":"custom-filter_count"
    },
    "supportedObjectTypes": [
      "CATALOG",
      "SCHEMA",
      "TABLE"
    ],
    "properties": {
      "job.target-file-size":"1024000000",
      "strategy.type": "compaction",
      "job.template-name":"compaction-job"
    }
  }
}' http://localhost:8090/api/metalakes/test/policies
```

7. Attach the created policy (strategy) to the database `generic.db`

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "policiesToAdd": ["compactionFilterCount"],
  "policiesToRemove": []
}' http://localhost:8090/api/metalakes/test/objects/schema/generic.db/policies
```

The tables in schema `generic.db` will inherit the attached policies.

See https://gravitino.apache.org/docs/1.0.0/manage-policies-in-gravitino for more details about policy.

8. Prepare a local stats file with both table stats and partition stats

`local-stats-computer` can read JSONL where each line contains `identifier`, `stats-type`, and custom stats.
- `identifier`, the table identifier in catalog.schema.table or schema.table format.
- `stats-type`, either `table` or `partition`.
- `partition-path`, should use the logical partition field names. Identity: column name (for example, `country`). Bucket: transform name from the Iceberg spec (for example, `bucket_col_bucket_8`). Truncate: use the generated transform name like `col_trunc_16`; Day/Month/Hour transforms follow `col_day`, `col_month`, `col_hour` conventions.
- Custom stats fields, the name must start with `custom-`

Example `p-table-stats.jsonl`:

```shell
cat > p-table-stats.jsonl <<'EOF'
{"identifier":"db.table1","stats-type":"table","custom-cost":10000}
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"US"},"custom-filter_count":60}
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"CA"},"custom-filter_count":40}
{"identifier":"db.table2","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"custom-filter_count":120}
EOF
```

9. Push table and partition stats to Gravitino

```shell
./bin/gravitino-optimizer.sh -type update_statistics -computer-name local-stats-computer --file-path ./p-table-stats.jsonl
```

Verify table stats with:

```shell
## Get table stats
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" http://localhost:8090/api/metalakes/test/objects/table/generic.db.table1/statistics

## Get partition stats
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" http://localhost:8090/api/metalakes/test/objects/table/generic.db.table1/statistics/partitions
```

10. Append metrics (timestamped samples) for the same table

```shell
cat > p-table-metrics.jsonl <<'EOF'
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"US"},"TABLE_STORAGE_COST":60}
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"CA"},"TABLE_STORAGE_COST":40}
{"identifier":"db.table2","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"TABLE_STORAGE_COST":120}
EOF
```

```shell
./bin/gravitino-optimizer.sh -type append_metrics -computer-name local-stats-computer --file-path ./p-table-metrics.jsonl
```
   
11. Prepare a local job mapping file for monitoring job metrics

```shell
cat > job-mappings.jsonl <<'EOF'
{"identifier":"generic.db.table1","job-identifiers":["job-1"]}
{"identifier":"generic.db.table2","job-identifiers":["job-2"]}
EOF
```

Update `gravitino.optimizer.monitor.file-job-provider.file-path` to the absolute path of
`job-mappings.jsonl`.

12. Use the recommender to rank tables for the strategy

```shell
./bin/gravitino-optimizer.sh -type recommend_strategy_type --identifiers generic.db.table1,generic.db.table2 --strategy-type compaction
```

There is a built-in strategy handler for the `compaction` strategy type to recommend tables. You can provide your own strategy handler implementation for other strategy types.

You can see the recommended score and partitions for each table in `gravitino-optimizer.log`.
```commandline
2025-12-08 11:35:34.087 INFO [main] ... Recommend strategy compactionFilterCount for identifier generic.db.table1 score: 100
2025-12-08 11:35:36.504 INFO [main] ... Recommend strategy compactionFilterCount for identifier generic.db.table2 score: 200
2025-12-22 20:52:38.162 INFO [main] ... NoopJobSubmitter submitJob: template=compaction-job, identifier=generic.db.table1, jobExecutionContext=CompactionJobContext(...)
```

13. Append job metrics for the compaction job(s) before compaction

```shell
./bin/gravitino-optimizer.sh -type append_metrics -computer-name local-stats-computer \
  --statistics-payload '{"identifier":"job-1","stats-type":"job","JOB_RUNTIME_MS":120,"JOB_OUTPUT_FILES":8}'
```

```shell
./bin/gravitino-optimizer.sh -type list_job_metrics --identifiers job-1
```

14. Perform your compaction action on the table(s)

Record the compaction time as `action-time` (for example, `1766483422`). Make sure the action time
is after the "before" metrics and before the "after" metrics.

15. Use the updater to append metrics after compaction

```shell
cat > p-table-metrics.jsonl <<'EOF'
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"US"},"TABLE_STORAGE_COST":6}
{"identifier":"db.table1","stats-type":"partition","partition-path":{"country":"CA"},"TABLE_STORAGE_COST":4}
{"identifier":"db.table2","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"TABLE_STORAGE_COST":12}
EOF
```

```shell
./bin/gravitino-optimizer.sh -type append_metrics -computer-name local-stats-computer --file-path ./p-table-metrics.jsonl
```

16. Append job metrics after compaction

```shell
./bin/gravitino-optimizer.sh -type append_metrics -computer-name local-stats-computer \
  --statistics-payload '{"identifier":"job-1","stats-type":"job","JOB_RUNTIME_MS":60,"JOB_OUTPUT_FILES":4}'
```

```shell
./bin/gravitino-optimizer.sh -type list_job_metrics --identifiers job-1
```
   
17. Use the monitor to evaluate the metrics

```shell
./bin/gravitino-optimizer.sh -type list_table_metrics --identifiers generic.db.table1 --partition-path '{"country":"CA"}'
```

```shell
./bin/gravitino-optimizer.sh -type monitor_metrics --identifiers db.table1 --action-time 1766483422 --partition-path '{"country":"CA"}'
```

```shell
./bin/gravitino-optimizer.sh -type monitor_metrics --identifiers db.table1 --action-time 1766483422
```

By default, the Gravitino metrics evaluator compares metrics before and after `action-time`. You can implement your own evaluator if you need a different comparison.

You can see detailed metrics before and after compaction in `logs/gravitino-optimizer.log`. 
