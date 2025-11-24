---
title: "How to use optimizer"
slug: /how-to-use-optimizer
keywords:
  - optimizer
license: "This software is licensed under the Apache License version 2."
---

## Background

## How to build

```shell
./gradlew clean compileDistribution -x test
```

After the build, the Gravitino package will be in the `distribution/package` directory.

## How to start up Gravitino server and create metalake & catalog & schema & table 

(TODO:) provide detailed steps to start up Gravitino server and create metalake, catalog, schema and table.

## How to run the optimizer tools

```
cd distribution/package
./bin/gravitino-optimizer.sh --type xx
```

## How to control the optimizer tools

There are two ways to control the behavior, you could either define the configuration in `conf/gravitino-optimizer.conf` to provide different `provider` to define the tools to fetch policy, stats, metrics from external system (Gravitino by default), or you could pass the cli parameters to the optimizer tools.

You could control the optimizer tools by the following parameters:
- type: the type of the optimizer tool, could be update_stats, update_metrics, monitor_metrics, etc
- computer-name: the name of the computer to provide stats or metrics
- identifiers: the identifiers of the tables, could be multiple, separated by comma
- custom-content: the custom content of the optimizer tool, could be multiple, separated by comma

## Quick start 

Here is a quick start workflow to use Gravitino optimizer tools to update table stats, recommend the table for the compaction policy and monitor the table metrics after the table compaction job is finished.

1. create Iceberg catalog in Gravitino server, like:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
    "name": "iceberg",
    "comment": "my test catalog",
    "type": "RELATIONAL",
    "provider": "lakehouse-iceberg",
    "properties": {
        "catalog-backend": "hive",
        "uri": "thrift://127.0.0.1:9083",
        "warehouse": "/user/hive/warehouse-hive/"
    }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

Please refer to https://gravitino.apache.org/docs/1.0.0/lakehouse-iceberg-catalog#catalog-properties for more property details.

2. create policy and attach policy to the schema

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "compactionDelete",
  "comment": "This is a compaction policy consider more for delete file",
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
}' http://localhost:8090/api/metalakes/test/policies
```

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "compactionSmall",
  "comment": "This is a compaction policy consider more for small file",
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
}' http://localhost:8090/api/metalakes/test/policies
```

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "policiesToAdd": ["compactionDelete", "policy2"],
  "policiesToRemove": []
}' http://localhost:8090/api/metalakes/test/objects/schema/iceberg.ab/policies
```

Please refer to https://gravitino.apache.org/docs/1.0.0/manage-policies-in-gravitino for more details about policy.

3. use computer to compute stats of the table and update stats to Gravitino

```shell
./bin/gravitino-optimizer.sh -type update_stats -computer-name gravitino-cli -identifiers ab.a1 --custom-content table:custom-position_delete_file_number=0,custom-datafile_size_mse=10000.2
./bin/gravitino-optimizer.sh -type update_stats -computer-name gravitino-cli -identifiers ab.a2 --custom-content table:custom-position_delete_file_number=100,custom-datafile_size_mse=100.2
```

Gravitino provides a build-in `gravitino-cli` computer to get stats from the cli, you could also implement your own computer to get stats from other systems. 
   
4. use updater to update the metrics of the table

```shell
./bin/gravitino-optimizer.sh -type update_metrics -computer-name gravitino-cli -identifiers ab.a1 --custom-content table:TABLE_STORAGE_COST=1000,DATAFILE_AVG_SIZE=100
./bin/gravitino-optimizer.sh -type update_metrics -computer-name gravitino-cli -identifiers ab.a2 --custom-content table:TABLE_STORAGE_COST=2000,DATAFILE_AVG_SIZE=200
```
   
6. use recommender to recommend the best table for the policy

```shell
./bin/gravitino-optimizer.sh -type recommend_policy_type --identifiers iceberg.ab.a1,iceberg.ab.a2 --policy-type compaction
```

You could see the recommend score for each policy in `gravitino-optimizer.log`.

7. do some compaction action bout the table

Please record the compaction time as `action-time` like `1763864715`.

8. use updater to update the metrics of the table after compaction

```shell
./bin/gravitino-optimizer.sh -type update_metrics -computer-name gravitino-cli -identifiers ab.a1 --custom-content table:TABLE_STORAGE_COST=12,DATAFILE_AVG_SIZE=10
./bin/gravitino-optimizer.sh -type update_metrics -computer-name gravitino-cli -identifiers ab.a2 --custom-content table:TABLE_STORAGE_COST=22,DATAFILE_AVG_SIZE=20
```
   
9. use monitor to evaluate the metrics

```shell
./bin/gravitino-optimizer.sh -type monitor_metrics --identifiers ab.a1,ab.a2 --action-time 1763864715
```

You could see the details metrics before and after the compaction in `gravitino-optimizer.log`.