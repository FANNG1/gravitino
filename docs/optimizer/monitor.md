---
title: "Monitor service step-by-step guide"
slug: /optimizer-monitor
keywords:
  - optimizer
  - monitor
license: "This software is licensed under the Apache License version 2."
---

## Background

This guide provides an end-to-end, copy-and-run workflow to test the Gravitino Optimizer monitor
service. It walks through configuration, table registration, metrics registration (table + job),
starting the monitor service, and submitting and querying monitors.

## Steps

1. Prepare a job mapping file (job provider input)

   Create `job-mappings.jsonl` with JSON Lines, one mapping per line. Example:

   ```shell
   cat > job-mappings.jsonl <<'EOF'
   {"identifier":"generic.db.table1","job-identifiers":["job-1"]}
   {"identifier":"generic.db.table2","job-identifiers":["job-2"]}
   EOF
   ```

2. Prepare the optimizer config

   Create or edit `gravitino-optimizer.conf` in your current working directory:

   ```properties
   gravitino.optimizer.gravitinoUri = http://localhost:8090
   gravitino.optimizer.gravitinoMetalake = test
   gravitino.optimizer.gravitinoDefaultCatalog = generic

   gravitino.optimizer.h2-metrics.h2-metrics-storage-path = ./data/metrics.db

   gravitino.optimizer.monitor.service.port = 8000
   gravitino.optimizer.monitor.service.interval.seconds = 2

   gravitino.optimizer.monitor.metricsProvider = gravitino-metrics-provider
   gravitino.optimizer.monitor.metricsEvaluator = gravitino-metrics-evaluator

   gravitino.optimizer.monitor.jobProvider = file-job-provider
   gravitino.optimizer.monitor.file-job-provider.file-path = /absolute/path/to/job-mappings.jsonl
   ```

3. Start the monitor service

   ```shell
   ./bin/gravitino-optimizer.sh \
     --type run-monitor-service
   ```

   Keep this process running. The service prints the bound port (default 8000).

4. Append table and job metrics before compaction

   Table metrics using `local-stats-computer`:

   ```shell
   ./bin/gravitino-optimizer.sh \
     --type append-metrics \
     --computer-name local-stats-computer \
     --statistics-payload '{"identifier":"generic.db.table1","stats-type":"table","row_count":100}' \
     --identifiers generic.db.table1
   ```

   Partition metrics example (use table2):

   ```shell
   ./bin/gravitino-optimizer.sh \
     --type append-metrics \
     --computer-name local-stats-computer \
     --statistics-payload '{"identifier":"generic.db.table2","stats-type":"partition","partition-path":{"bucket_col_bucket_8":"3"},"row_count":50}' \
     --identifiers generic.db.table2
   ```

   Job metrics using `local-stats-computer` (before action time):

   ```shell
   ./bin/gravitino-optimizer.sh \
     --type append-metrics \
     --computer-name local-stats-computer \
     --statistics-payload '{"identifier":"job-1","stats-type":"job","job_status":0}' \
     --identifiers job-1

   ./bin/gravitino-optimizer.sh \
     --type append-metrics \
     --computer-name local-stats-computer \
     --statistics-payload '{"identifier":"job-2","stats-type":"job","job_status":0}' \
     --identifiers job-2
   ```

5. Submit a monitor

    Use an action time (epoch seconds) and a range (seconds). The monitor evaluates metrics within
    `[actionTime - rangeSeconds, actionTime + rangeSeconds]` and splits them into before/after
    action time.

    ```shell
    ACTION_TIME=$(date +%s)

    ./bin/gravitino-optimizer.sh \
      --type submit-monitor \
      --identifier generic.db.table1 \
      --action-time-seconds ${ACTION_TIME} \
      --range-seconds 3600 \
      --monitor-service-url http://localhost:8000
    ```

    Submit a partition monitor for table2:

    ```shell
    ./bin/gravitino-optimizer.sh \
      --type submit-monitor \
      --identifier generic.db.table2 \
     --partition-path '{"bucket_col_bucket_8":"3"}' \
      --action-time-seconds ${ACTION_TIME} \
      --range-seconds 3600 \
      --monitor-service-url http://localhost:8000
    ```

6. Append table and job metrics after compaction

   Append table metrics after the compaction completes:

   ```shell
   ./bin/gravitino-optimizer.sh \
     --type append-metrics \
     --computer-name local-stats-computer \
     --statistics-payload '{"identifier":"generic.db.table1","stats-type":"table","row_count":120}' \
     --identifiers generic.db.table1
   ```

   Append job metrics after the compaction completes:

   ```shell
   ./bin/gravitino-optimizer.sh \
     --type append-metrics \
     --computer-name local-stats-computer \
     --statistics-payload '{"identifier":"job-1","stats-type":"job","job_status":-1}' \
     --identifiers job-1

   ./bin/gravitino-optimizer.sh \
     --type append-metrics \
     --computer-name local-stats-computer \
     --statistics-payload '{"identifier":"job-2","stats-type":"job","job_status":-1}' \
     --identifiers job-2
   ```

7. Get monitor status

    ```shell
    ./bin/gravitino-optimizer.sh \
      --type get-monitor \
      --monitor-id <monitorId> \
      --monitor-service-url http://localhost:8000
    ```

    Full detail:

    ```shell
    ./bin/gravitino-optimizer.sh \
      --type get-monitor \
      --monitor-id <monitorId> \
      --verbose \
      --monitor-service-url http://localhost:8000
    ```

8. List and cancel monitors

    ```shell
    ./bin/gravitino-optimizer.sh \
      --type list-monitors \
      --monitor-service-url http://localhost:8000
    ```

    ```shell
    ./bin/gravitino-optimizer.sh \
      --type cancel-monitor \
      --monitor-id <monitorId> \
      --monitor-service-url http://localhost:8000
    ```

9, could you see the output of monitor service:

```
MONITOR: time=2026-02-02T13:35:32.706847Z scope=TABLE identifier=generic.db.table1 evaluation=true evaluator=gravitino-metrics-evaluator
METRICS BEFORE: {row_count=[1770038953:100, 1770038961:120, 1770039322:100]}
METRICS AFTER: {row_count=[1770039331:120]}
MONITOR: time=2026-02-02T13:35:32.773025Z scope=JOB identifier=job-1 evaluation=true evaluator=gravitino-metrics-evaluator
METRICS BEFORE: {job_status=[1770038955:0, 1770038962:-1, 1770039324:0]}
METRICS AFTER: {job_status=[1770039332:-1]}
MONITOR: time=2026-02-02T13:35:34.788038Z scope=PARTITION identifier=generic.db.table2 partition=PartitionPath [PartitionEntryImpl(partitionName=bucket_col_bucket_8, partitionValue=3)] evaluation=true evaluator=gravitino-metrics-evaluator
METRICS BEFORE: {row_count=[1770038954:50, 1770039323:50]}
METRICS AFTER: {row_count=[]}
MONITOR: time=2026-02-02T13:35:34.845285Z scope=JOB identifier=job-2 evaluation=true evaluator=gravitino-metrics-evaluator
METRICS BEFORE: {job_status=[1770038956:0, 1770038963:-1, 1770039325:0]}
METRICS AFTER: {job_status=[1770039333:-1]}
```
