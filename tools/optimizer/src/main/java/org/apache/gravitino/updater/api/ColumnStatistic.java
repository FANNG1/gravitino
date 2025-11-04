package org.apache.gravitino.updater.api;

public interface ColumnStatistic extends SingleStatistic {
  String columnName();
}
