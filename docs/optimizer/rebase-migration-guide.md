---
title: "Optimizer Rebase Migration Guide"
slug: /optimizer-rebase-migration
keywords:
  - optimizer
  - migration
  - plugin
license: "This software is licensed under the Apache License version 2."
---

## Purpose

This guide describes user-facing changes introduced by the recent optimizer rebase, focusing on:

- plugin extension interfaces,
- CLI commands and parameters,
- configuration keys and behavior changes.

Target audience:

- users integrating optimizer commands into automation scripts,
- plugin developers implementing custom providers/calculators/evaluators/handlers.

## 1. High-impact breaking changes

### 1.1 CLI type format is strictly kebab-case

`--type` now requires kebab-case values (for example, `update-statistics`).
Snake case like `update_statistics` is rejected.

### 1.2 Command set changed

Current supported commands:

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

Removed/unsupported legacy commands include (examples):

- `recommend_strategy_type`
- `register_tables`
- `run-monitor-service`

### 1.3 Monitor job relation SPI changed

Legacy `JobProvider` extension path was removed.
Use `TableJobRelationProvider` instead.

### 1.4 Strategy policy key behavior

`GravitinoStrategy` now reads strategy type from policy property:

- `strategy.type`

Legacy compatibility key fallback is not used.

## 2. Plugin extension interface changes

## 2.1 Provider-based SPI (ServiceLoader via `Provider`)

The following extension points are discovered via:

- `META-INF/services/org.apache.gravitino.maintenance.optimizer.api.common.Provider`

Common interface base:

- `org.apache.gravitino.maintenance.optimizer.api.common.Provider`

Typical implementations include:

- Recommender: `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`, `JobSubmitter`
- Updater: `StatisticsUpdater`, `MetricsUpdater`
- Monitor: `MetricsProvider`, `TableJobRelationProvider`, `MonitorCallback`

### Migration notes

- If you previously implemented job mapping via old job-provider classes, migrate to `TableJobRelationProvider`.
- Ensure `name()` is unique and matches config value.
- Keep a public no-arg constructor for ServiceLoader instantiation.

## 2.2 Statistics calculator SPI

Statistics calculators are discovered via:

- `META-INF/services/org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsCalculator`

CLI selection option:

- `--calculator-name`

Built-in local calculator name:

- `local-stats-calculator`

### Migration notes

- Legacy `--computer-name` / `local-stats-computer` usage must be migrated to `--calculator-name` / `local-stats-calculator`.
- For `local-stats-calculator`, one of `--statistics-payload` or `--file-path` is required.

## 2.3 Metrics evaluator SPI

Metrics evaluators are discovered via:

- `META-INF/services/org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsEvaluator`

Config key for evaluator selection:

- `gravitino.optimizer.monitor.metricsEvaluator`

Important API signature:

- `evaluateMetrics(DataScope scope, Map<String, List<MetricSample>> before, Map<String, List<MetricSample>> after)`

### Migration notes

- Scope type is `DataScope` (not legacy metric scope APIs).
- Built-in evaluator rule config:
  - `gravitino.optimizer.monitor.gravitinoMetricsEvaluator.rules`
  - format: `scope:metricName:aggregation:comparison`

## 2.4 Strategy handler extension path

`StrategyHandler` is class-config driven (not ServiceLoader):

- config key pattern: `gravitino.optimizer.strategyHandler.<strategyType>.className`

### Migration notes

- If your policies use `strategy.type=compaction`, ensure compaction handler class mapping is present.
- `submit-strategy-jobs` uses `--strategy-name` (policy name), not `--strategy-type`.

## 2.5 Optional job adapter extension (for `gravitino-job-submitter`)

Custom template adapter key pattern:

- `gravitino.optimizer.jobAdapter.<jobTemplate>.className`

## 3. CLI changes from user perspective

## 3.1 Command and option mapping

### Strategy execution

Old style (legacy):

- `recommend_strategy_type --strategy-type ...`

New style:

- `submit-strategy-jobs --strategy-name ... [--dry-run] [--limit N]`

### Statistics/metrics update

Old style (legacy):

- `--computer-name local-stats-computer`

New style:

- `--calculator-name local-stats-calculator`

### Monitor service interaction

Use external service URL with:

- `submit-monitor`
- `list-monitors`
- `get-monitor`
- `cancel-monitor`

Required option:

- `--monitor-service-url`

No CLI command is provided to boot monitor service directly.

## 3.2 Partition path input format differences

Different commands expect different formats:

- `submit-monitor --partition-path`: legacy path string
  - example: `dt=2026-03-01/country=US`
- `monitor-metrics --partition-path`: JSON array string
  - example: `[{"dt":"2026-03-01"},{"country":"US"}]`
- local stats/metrics payload (`stats-type=partition`): JSON object
  - example: `"partition-path":{"dt":"2026-03-01","country":"US"}`

## 3.3 Validation behavior tightened

- Per-command unsupported options are rejected.
- `--statistics-payload` and `--file-path` are mutually exclusive.
- `monitor-metrics` and `list-table-metrics` require exactly one identifier when `--partition-path` is set.

## 4. Configuration key migration

### 4.1 Monitor job relation provider

Use:

- `gravitino.optimizer.monitor.tableJobRelationProvider`
- `gravitino.optimizer.monitor.localTableJobRelationProvider.filePath`

Built-in provider names:

- `dummy-table-job-relation-provider`
- `local-table-job-relation-provider`

### 4.2 Policy property keys for strategy execution

Use policy `content.properties` keys:

- `strategy.type`
- `job.template-name`

## 5. Migration checklist

1. Update all CLI `--type` values to kebab-case.
2. Replace `--computer-name` with `--calculator-name`.
3. Replace strategy invocation with `submit-strategy-jobs --strategy-name`.
4. Migrate custom monitor job mapping plugins to `TableJobRelationProvider`.
5. Update monitor config keys to `tableJobRelationProvider` and `localTableJobRelationProvider.filePath`.
6. Ensure policy properties use `strategy.type` and `job.template-name`.
7. Fix partition-path format per command.
8. Verify your SPI registration files (`META-INF/services/...`) and provider names.

## 6. Quick verification commands

```shell
# global help
./bin/gravitino-optimizer.sh --help

# command-scoped help example
./bin/gravitino-optimizer.sh --help --type update-statistics

# dry-run strategy submission
./bin/gravitino-optimizer.sh \
  --type submit-strategy-jobs \
  --identifiers generic.db.table1 \
  --strategy-name yourPolicyName \
  --dry-run

# monitor service list (requires external monitor service)
./bin/gravitino-optimizer.sh \
  --type list-monitors \
  --monitor-service-url http://localhost:8000
```
