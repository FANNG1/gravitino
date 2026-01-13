/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.maintenance.optimizer.recommender.util;

import com.alibaba.qlexpress4.Express4Runner;
import com.alibaba.qlexpress4.InitOptions;
import com.alibaba.qlexpress4.QLOptions;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

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
    Preconditions.checkArgument(StringUtils.isNotBlank(expression), "expression is blank");
    Preconditions.checkArgument(context != null, "context is null");
    Express4Runner runner = new Express4Runner(InitOptions.DEFAULT_OPTIONS);
    return runner
        .execute(formatExpression(expression), formatContextKey(context), QLOptions.DEFAULT_OPTIONS)
        .getResult();
  }

  private Map<String, Object> formatContextKey(Map<String, Object> context) {
    return context.entrySet().stream()
        .collect(
            Collectors.toMap(entry -> formatExpression(entry.getKey()), entry -> entry.getValue()));
  }

  private String formatExpression(String expression) {
    return expression.replace("-", "_");
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
