package org.apache.gravitino.updater.api;

public interface ColumnStatistic extends BaseStatistic {
  String columnName();
}
