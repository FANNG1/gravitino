package org.apache.gravitino.monitor.impl;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.monitor.api.MetricsEvaluator;
import org.apache.gravitino.monitor.api.SingleMetric;
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
  public boolean evaluateTableMetrics(
      Map<String, List<SingleMetric>> beforeMetrics, Map<String, List<SingleMetric>> afterMetrics) {
    //  evaluate table storage cost, data file size, data file number, position file number, etc
    evaluateMetricsNames.stream()
        .forEach(
            metricName -> {
              String name = metricName.name();
              doEvaluation(beforeMetrics.get(name), afterMetrics.get(name), metricName);
            });
    return false;
  }

  @Override
  public boolean evaluateJobMetrics(List<org.apache.gravitino.monitor.api.SingleMetric> metrics) {
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
        StatisticValueUtils.avg(beforeMetrics.stream().map(this::toStatisticValue).toList());

    StatisticValue afterAvg =
        StatisticValueUtils.avg(afterMetrics.stream().map(this::toStatisticValue).toList());

    LOG.info(
        String.format(
            "Metrics %s avg before action time %d: %s", metricName, actionTime, beforeAvg));
    LOG.info(
        String.format("Metrics %s avg after action time %d: %s", metricName, actionTime, afterAvg));
  }

  private StatisticValue toStatisticValue(SingleMetric metric) {
    BaseStatistic<?> statistic = metric.statistic();
    return statistic.value();
  }
}
