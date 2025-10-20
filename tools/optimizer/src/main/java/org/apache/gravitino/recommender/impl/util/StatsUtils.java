package org.apache.gravitino.recommender.impl.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.stats.Statistic;

public class StatsUtils {

  public static Map<String, Object> buildTableStatsContext(List<Statistic> tableStats) {
    Map<String, Object> context = new HashMap<>();
    for (Statistic stat : tableStats) {
      context.put(stat.name(), stat.value());
    }
    return context;
  }
}
