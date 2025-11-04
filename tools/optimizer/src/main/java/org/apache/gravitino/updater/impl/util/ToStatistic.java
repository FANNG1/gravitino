package org.apache.gravitino.updater.impl.util;

import java.util.List;
import org.apache.gravitino.updater.api.SingleStatistic;

public interface ToStatistic {
  List<SingleStatistic<?>> toStatistic();
}
