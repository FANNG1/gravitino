package org.apache.gravitino.monitor.impl;

import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.monitor.api.Metrics;
import org.apache.gravitino.monitor.api.MetricsEvaluator;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.updater.api.BaseStatistic;
import org.apache.gravitino.util.StatisticValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoEvaluator implements MetricsEvaluator {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoEvaluator.class);

  @SuppressWarnings("UnusedVariable")
  private long actionTime;

  @SuppressWarnings("UnusedVariable")
  private long rangeHours;

  private List<BaseStatistic.Name> evaluateMetricsNames =
      List.of(
          BaseStatistic.Name.TABLE_STORAGE_COST,
          BaseStatistic.Name.DATAFILE_AVG_SIZE,
          BaseStatistic.Name.DATAFILE_NUMBER,
          BaseStatistic.Name.DATAFILE_SIZE_MSE,
          BaseStatistic.Name.POSITION_DELETE_FILE_NUMBER,
          BaseStatistic.Name.EQUAL_DELETE_FILE_NUMBER,
          BaseStatistic.Name.JOB_COST,
          BaseStatistic.Name.JOB_DURATION);

  @Override
  public void initialize(long actionTime, long rangeHours) {
    this.actionTime = actionTime;
    this.rangeHours = rangeHours;
  }

  @Override
  public boolean evaluateTableMetrics(List<Metrics> metrics) {
    //  evaluate table storage cost, data file size, data file number, position file number, etc
    Pair<List<Metrics>, List<Metrics>> tableMetrics = splitMetrics(metrics);
    List<Metrics> beforeMetrics = tableMetrics.getLeft();
    List<Metrics> afterMetrics = tableMetrics.getRight();

    evaluateMetricsNames.stream()
        .forEach(
            metricName -> {
              List<SingleMetric> toBeforeMetrics = toSingleMetrics(beforeMetrics, metricName);
              List<SingleMetric> toAfterMetrics = toSingleMetrics(afterMetrics, metricName);
              doEvaluation(toBeforeMetrics, toAfterMetrics, metricName);
            });
    return false;
  }

  @Override
  public boolean evaluateJobMetrics(List<Metrics> metrics) {
    // evaluate job cost, duration, etc
    return false;
  }

  private void doEvaluation(
      List<SingleMetric> beforeMetrics,
      List<SingleMetric> afterMetrics,
      BaseStatistic.Name metricName) {
    // implement evaluation logic here, 1. print metrics before and after action time, 2 compare the
    // avg metrics before and after action time
    // 1. print metrics before and after action time
    LOG.info(
        String.format(
            "Metrics %s before action time %d: %s",
            metricName, actionTime, beforeMetrics.stream().map(SingleMetric::toString).toList()));
    LOG.info(
        String.format(
            "Metrics %s after action time %d: %s",
            metricName, actionTime, afterMetrics.stream().map(SingleMetric::toString).toList()));

    StatisticValue beforeAvg =
        StatisticValueUtils.avg(beforeMetrics.stream().map(SingleMetric::value).toList());

    StatisticValue afterAvg =
        StatisticValueUtils.avg(afterMetrics.stream().map(SingleMetric::value).toList());

    LOG.info(
        String.format(
            "Metrics %s avg before action time %d: %s", metricName, actionTime, beforeAvg));
    LOG.info(
        String.format("Metrics %s avg after action time %d: %s", metricName, actionTime, afterAvg));
  }

  private Pair<List<Metrics>, List<Metrics>> splitMetrics(List<Metrics> metrics) {
    // split metrics into metrics before and after action time
    long actionTimeInSeconds = actionTime;
    List<Metrics> beforeMetrics =
        metrics.stream().filter(m -> m.timestamp() < actionTimeInSeconds).toList();
    List<Metrics> afterMetrics =
        metrics.stream().filter(m -> m.timestamp() >= actionTimeInSeconds).toList();
    return Pair.of(beforeMetrics, afterMetrics);
  }

  private List<SingleMetric> toSingleMetrics(
      List<Metrics> metricsList, BaseStatistic.Name metricName) {
    return metricsList.stream()
        .map(m -> SingleMetric.fromMetrics(m, metricName))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .toList();
  }

  static class SingleMetric {
    private long timestamp;
    private StatisticValue value;

    private SingleMetric(long timestamp, StatisticValue value) {
      this.timestamp = timestamp;
      this.value = value;
    }

    public static Optional<SingleMetric> fromMetrics(
        Metrics metrics, BaseStatistic.Name metricName) {
      StatisticValue value =
          metrics.statistics().stream()
              .filter(s -> s.name().equals(metricName.name()))
              .findFirst()
              .map(BaseStatistic::value)
              .orElse(null);
      if (value == null) {
        return Optional.empty();
      }
      return Optional.of(new SingleMetric(metrics.timestamp(), value));
    }

    public long timestamp() {
      return timestamp;
    }

    public StatisticValue value() {
      return value;
    }

    @Override
    public String toString() {
      return String.format("Timestamp: %d, Value: %s", timestamp, value);
    }
  }
}
