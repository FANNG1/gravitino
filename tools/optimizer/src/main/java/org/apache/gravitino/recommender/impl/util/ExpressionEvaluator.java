package org.apache.gravitino.recommender.impl.util;

import java.util.Map;

public interface ExpressionEvaluator {
  boolean evaluateBool(String expression, Map<String, Object> context);

  long evaluateLong(String expression, Map<String, Object> context);
}
