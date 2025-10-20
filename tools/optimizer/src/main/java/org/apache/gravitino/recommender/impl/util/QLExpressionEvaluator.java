package org.apache.gravitino.recommender.impl.util;

import com.alibaba.qlexpress4.Express4Runner;
import com.alibaba.qlexpress4.InitOptions;
import com.alibaba.qlexpress4.QLOptions;
import java.math.BigDecimal;
import java.util.Map;

public class QLExpressionEvaluator implements ExpressionEvaluator {
  @Override
  public long evaluateLong(String expression, Map<String, Object> context) {
    return toLong(evaluate(expression, context));
  }

  @Override
  public boolean evaluateBool(String expression, Map<String, Object> context) {
    return (boolean) evaluate(expression, context);
  }

  private Object evaluate(String expression, Map<String, Object> context) {
    Express4Runner runner = new Express4Runner(InitOptions.DEFAULT_OPTIONS);
    return runner.execute(expression, context, QLOptions.DEFAULT_OPTIONS).getResult();
  }

  private Long toLong(Object obj) {
    if (obj instanceof Long) {
      return (Long) obj;
    }

    if (obj instanceof Integer) {
      return ((Integer) obj).longValue();
    }

    if (obj instanceof BigDecimal) {
      return ((BigDecimal) obj).longValue();
    }

    if (obj instanceof Number) {
      if (obj instanceof Double || obj instanceof Float) {
        return Math.round(((Number) obj).doubleValue());
      }
      return ((Number) obj).longValue();
    }

    throw new IllegalArgumentException("Object cannot be converted to Long");
  }
}
