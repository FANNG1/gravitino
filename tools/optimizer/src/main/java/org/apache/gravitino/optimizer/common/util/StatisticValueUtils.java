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

package org.apache.gravitino.optimizer.common.util;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Type.Name;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;

public class StatisticValueUtils {

  public static StatisticValue avg(List<StatisticValue> values) {
    if (values.isEmpty()) {
      return null;
    }
    Preconditions.checkArgument(values.stream().allMatch(StatisticValueUtils::isNumber));

    StatisticValue sum = sum(values);
    if (sum == null) {
      return null;
    }
    return StatisticValueUtils.div(sum, values.size());
  }

  public static StatisticValue sum(List<StatisticValue> values) {
    if (values.isEmpty()) {
      return null;
    }
    Type type = getNumberType(values);
    Name longName = Types.LongType.get().name();
    Name doubleName = Types.DoubleType.get().name();
    if (type.name().equals(longName)) {
      long longSum = 0L;
      for (StatisticValue value : values) {
        longSum += ((Long) value.value()).longValue();
      }
      return StatisticValues.longValue(longSum);
    } else if (type.name().equals(doubleName)) {
      double doubleSum = 0.0;
      for (StatisticValue value : values) {
        doubleSum += ((Number) value.value()).doubleValue();
      }
      return StatisticValues.doubleValue(doubleSum);
    } else {
      throw new IllegalArgumentException("Unsupported number type: " + type.name());
    }
  }

  public static StatisticValue div(StatisticValue value, long divisor) {
    Preconditions.checkArgument(isNumber(value), "Value must be a number");
    Preconditions.checkArgument(divisor != 0, "Divisor cannot be zero");
    Type type = value.dataType();
    Name longName = Types.LongType.get().name();
    Name doubleName = Types.DoubleType.get().name();
    if (type.name().equals(longName)) {
      long longValue = ((Long) value.value()).longValue();
      return StatisticValues.doubleValue(longValue / divisor);
    } else if (type.name().equals(doubleName)) {
      double doubleValue = ((Number) value.value()).doubleValue();
      return StatisticValues.doubleValue(doubleValue / divisor);
    } else {
      throw new IllegalArgumentException("Unsupported number type: " + type.name());
    }
  }

  @SneakyThrows
  public static String toString(StatisticValue value) {
    Preconditions.checkArgument(value != null, "StatisticValue cannot be null");
    return JsonUtils.anyFieldMapper().writeValueAsString(value);
  }

  @SneakyThrows
  public static StatisticValue fromString(String valueStr) {
    Preconditions.checkArgument(valueStr != null, "StatisticValue string cannot be null");
    return JsonUtils.anyFieldMapper().readValue(valueStr, StatisticValue.class);
  }

  private static Type getNumberType(List<StatisticValue> values) {
    Type type = values.get(0).dataType();
    Preconditions.checkArgument(values.stream().allMatch(v -> v.dataType() == type));
    return type;
  }

  private static boolean isNumber(StatisticValue value) {
    Type type = value.dataType();
    return type == Types.LongType.get() || type == Types.DoubleType.get();
  }
}
