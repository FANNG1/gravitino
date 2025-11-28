---
title: "Pinterest MVP with Gravitino Optimizer"
slug: /pinterest-mvp
keywords:
  - optimizer
license: "This software is licensed under the Apache License version 2."
---

## Background

## Steps

1. Prepare the environment and start the Gravitino server 

   ```shell
   # compile Gravitino and optimizer package to `distribution/package`
   ./gradlew clean compileDistribution -x test -x rat -x lintOpenAPI
   
   # build the Iceberg catalog bundle and place it into the Iceberg catalog lib folder
   ./gradlew :bundles:iceberg-aws:build -x test
   cp bundles/iceberg-aws-bundle/build/libs/gravitino-iceberg-aws-bundle-1.1.0-SNAPSHOT.jar distribution/package/catalogs/lakehouse-iceberg/libs/
   
   # start Gravitino server
   ./distribution/package/bin/gravitino.sh start
   ```

2. Create a metalake

A metalake is the top-level namespace in Gravitino that groups catalogs/schemas/tables plus shared policies. Create one for this walkthrough:

   ```shell
   curl -X POST -H "Content-Type: application/json" \
   -d '{"name":"test","comment":"Optimizer quickstart","properties":{}}' \
   http://localhost:8090/api/metalakes
   ```

3. Create an Iceberg catalog inside the metalake (requires a reachable Hive Metastore and warehouse path—adjust for your environment)

   ```shell
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
   -H "Content-Type: application/json" -d '{
       "name": "iceberg",
       "comment": "test iceberg catalog",
       "type": "RELATIONAL",
       "provider": "lakehouse-iceberg",
       "properties": {
         "catalog-backend": "hive",
         "uri": "thrift://127.0.0.1:9083",
         "io-impl":"org.apache.iceberg.aws.s3.S3FileIO",
         "warehouse":"s3://iceberg-test-strato/iceberg-docker",
         "s3-access-key-id":"xx",
         "s3-secret-access-key":"xx",
         "s3-region":"ap-southeast-2"
       }
   }' http://localhost:8090/api/metalakes/test/catalogs
   ```
See https://gravitino.apache.org/docs/1.0.0/lakehouse-iceberg-catalog#catalog-properties for property details.

Suppose there is one database `db` with two tables `table1` and `table2`. Check the list of tables with:

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" http://localhost:8090/api/metalakes/test/catalogs/iceberg/schemas/db/tables
```

4. Add configuration for the optimizer

Add the following configuration to `conf/gravitino-optimizer.conf`:

```
gravitino.optimizer.gravitino-uri = http://localhost:8090
gravitino.optimizer.gravitino-metalake = test
## default catalog name when an identifier omits the catalog
gravitino.optimizer.gravitino-default-catalog = iceberg
```

5. Create a compaction policy that uses custom stats `filter_count`

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
      "gravitino.policy.type": "compaction",
      "gravitino.policy.job.template-name":"compaction-job"
    }
  }
}' http://localhost:8090/api/metalakes/test/policies
```

6. Attach the created policy to the database `iceberg.db`

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "policiesToAdd": ["compactionFilterCount"],
  "policiesToRemove": []
}' http://localhost:8090/api/metalakes/test/objects/schema/iceberg.db/policies
```

The tables in schema `iceberg.db` will inherit the attached policies.

See https://gravitino.apache.org/docs/1.0.0/manage-policies-in-gravitino for more details about policy.

7. Generate a local stats file by Spark SQL

```python
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    try:
        query = f"""
          SELECT
            concat(database_name, '.', table_name) AS identifier,
            'table' AS `stats-type`,
            SUM(identity_partition_size_gb) AS custom-identity_partition_size_gb,
            SUM(filter_count) AS custom-filter_count
          FROM {args.input_table}
          GROUP BY database_name, table_name
        """
        result = spark.sql(query)
    except Exception as e:
        sys.stderr.write(f"Failed to query {args.input_table}: {e}\n")
        sys.exit(1)

    rows = result.collect()
    try:
        with open(args.output, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row.asDict()))
                f.write("\n")
    except Exception as e:
        sys.stderr.write(f"Failed to write output {args.output}: {e}\n")
        sys.exit(1)
```

The script lives at `tools/optimizer/bin/pinterest-table-stats.py`. Run it to produce a local stats file `p-table-stats.json`:

```shell
./bin/spark-submit  \
---conf spark.jars=/Users/fanng/deploy/demo/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar,/Users/fanng/deploy/demo/jars/iceberg-aws-bundle-1.9.2.jar,/Users/fanng/deploy/demo/jars/iceberg-gcp-bundle-1.9.2.jar \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.rest.type=rest  \
--conf spark.sql.catalog.rest.uri=http://127.0.0.1:9001/iceberg/ \
--conf spark.sql.catalog.rest.header.X-Iceberg-Access-Delegation=vended-credentials \
/Users/fanng/opensource/gravitino-pinterest/tools/optimizer/bin/pinterest-table-stats.py \
--input-table rest.query_analytics.metastore_dump \
--output ./p-table-stats.json
```

The generated `p-table-stats.json` looks like:
```
{"identifier": "db.table1", "stats-type": "table", "custom-identity_partition_size_gb": 0.0643, "custom-filter_count": 100}
{"identifier": "db.table2", "stats-type": "table", "custom-identity_partition_size_gb": 0.0651, "custom-filter_count": 200}
```

8. Use the local stats computer to load the file and push stats to the Gravitino server

```shell
./bin/gravitino-optimizer.sh -type update_stats -computer-name local-stats-computer --all-identifiers --stats-file ./p-table-stats.json
```

Check the stats with:

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" http://localhost:8090/api/metalakes/test/objects/table/iceberg.db.table1/statistics
```

9. Use the updater to refresh the metrics for each table

The difference between stats and metrics is that metrics include a timestamp.

```shell
./bin/gravitino-optimizer.sh -type update_metrics -computer-name gravitino-cli -identifiers db.table1 --stats-payload table:TABLE_STORAGE_COST=1000,DATAFILE_AVG_SIZE=100
./bin/gravitino-optimizer.sh -type update_metrics -computer-name gravitino-cli -identifiers db.table2 --stats-payload table:TABLE_STORAGE_COST=2000,DATAFILE_AVG_SIZE=200
```
   
10. Use the recommender to rank tables for the policy

```shell
./bin/gravitino-optimizer.sh -type recommend_policy_type --identifiers iceberg.db.table1,iceberg.db.table2 --policy-type compaction
```

There is a built-in policy actor for the `compaction` policy type to recommend tables. You can provide your own policy actor implementation for other policy types.

You can see the recommended score for each table in `gravitino-optimizer.log`.
```commandline
2025-12-08 11:35:34.087 INFO [main] [org.apache.gravitino.optimizer.recommender.Recommender.recommendForOnePolicy(Recommender.java:101)] - Recommend policy compactionFilterCount for identifier iceberg.db.table1 score: 100
2025-12-08 11:35:36.504 INFO [main] [org.apache.gravitino.optimizer.recommender.Recommender.recommendForOnePolicy(Recommender.java:101)] - Recommend policy compactionFilterCount for identifier iceberg.db.table2 score: 200
```

11. Perform your compaction action on the table(s)

Record the compaction time as `action-time` (for example, `1765165137`).

12. Use the updater to refresh metrics after compaction

```shell
./bin/gravitino-optimizer.sh -type update_metrics -computer-name gravitino-cli -identifiers db.table1 --stats-payload table:TABLE_STORAGE_COST=12,DATAFILE_AVG_SIZE=10
./bin/gravitino-optimizer.sh -type update_metrics -computer-name gravitino-cli -identifiers db.table2 --stats-payload table:TABLE_STORAGE_COST=22,DATAFILE_AVG_SIZE=20
```
   
13. Use the monitor to evaluate the metrics

```shell
./bin/gravitino-optimizer.sh -type monitor_metrics --identifiers db.table1,db.table2 --action-time 1765165137
```

By default, the Gravitino metrics evaluator compares metrics before and after `action-time`. You can implement your own evaluator if you need a different comparison.

You can see detailed metrics before and after compaction in `logs/gravitino-optimizer.log`. 
