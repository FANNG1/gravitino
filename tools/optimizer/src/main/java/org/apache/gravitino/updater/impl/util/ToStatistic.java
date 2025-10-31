package org.apache.gravitino.updater.impl.util;

import java.util.List;
import org.apache.gravitino.updater.api.BaseStatistic;

public interface ToStatistic {
  List<BaseStatistic<?>> toStatistic();
}
