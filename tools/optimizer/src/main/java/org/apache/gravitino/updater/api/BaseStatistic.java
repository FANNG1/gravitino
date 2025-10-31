package org.apache.gravitino.updater.api;

import org.apache.gravitino.stats.StatisticValue;

public interface BaseStatistic<T> {
  enum Name {
    TABLE_STORAGE_COST,
    DATAFILE_AVG_SIZE,
    DATAFILE_NUMBER,
    DATAFILE_SIZE_MSE,
    POSITION_DELETE_FILE_NUMBER,
    EQUAL_DELETE_FILE_NUMBER,
    JOB_COST,
    JOB_DURATION,
  }

  String name();

  StatisticValue<T> value();
}
