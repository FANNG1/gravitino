package org.apache.gravitino.monitor.api;

import java.util.List;
import org.apache.gravitino.updater.api.BaseStatistic;

public interface Metrics {

  long timestamp();

  List<BaseStatistic<?>> statistics();
}
