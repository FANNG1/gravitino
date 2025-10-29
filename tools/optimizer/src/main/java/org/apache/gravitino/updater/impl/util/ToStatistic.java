package org.apache.gravitino.updater.impl.util;

import java.util.List;
import org.apache.gravitino.updater.api.OStatistic;

public interface ToStatistic {
  List<OStatistic> toStatistic();
}
