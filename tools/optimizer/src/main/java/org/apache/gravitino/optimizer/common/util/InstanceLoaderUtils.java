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

import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.apache.gravitino.optimizer.api.monitor.MetricsEvaluator;
import org.apache.gravitino.optimizer.api.recommender.PolicyActor;
import org.apache.gravitino.optimizer.api.updater.StatsComputer;

public class InstanceLoaderUtils {

  public static <T extends PolicyActor> T createActorInstance(String policyType) {
    ServiceLoader<PolicyActor> loader = ServiceLoader.load(PolicyActor.class);
    List<Class<? extends T>> providers =
        Streams.stream(loader.iterator())
            .filter(p -> p.policyType().equalsIgnoreCase(policyType))
            .map(p -> (Class<? extends T>) p.getClass())
            .collect(Collectors.toList());

    if (providers.isEmpty()) {
      throw new IllegalArgumentException(
          "No " + PolicyActor.class.getSimpleName() + " class found for: " + policyType);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException(
          "Multiple " + PolicyActor.class.getSimpleName() + " found for: " + policyType);
    } else {
      Class<? extends T> providerClz = Iterables.getOnlyElement(providers);
      try {
        return providerClz.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static <T extends StatsComputer> T createStatsComputerInstance(String computerName) {
    ServiceLoader<StatsComputer> loader = ServiceLoader.load(StatsComputer.class);
    List<Class<? extends T>> providers =
        Streams.stream(loader.iterator())
            .filter(p -> p.name().equalsIgnoreCase(computerName))
            .map(p -> (Class<? extends T>) p.getClass())
            .collect(Collectors.toList());

    if (providers.isEmpty()) {
      throw new IllegalArgumentException(
          "No " + StatsComputer.class.getSimpleName() + " class found for: " + computerName);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException(
          "Multiple " + StatsComputer.class.getSimpleName() + " found for: " + computerName);
    } else {
      Class<? extends T> providerClz = Iterables.getOnlyElement(providers);
      try {
        return providerClz.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static <T extends MetricsEvaluator> T createMetricsEvaluatorInstance(
      String evaluatorName) {
    ServiceLoader<MetricsEvaluator> loader = ServiceLoader.load(MetricsEvaluator.class);
    List<Class<? extends T>> providers =
        Streams.stream(loader.iterator())
            .filter(p -> p.name().equalsIgnoreCase(evaluatorName))
            .map(p -> (Class<? extends T>) p.getClass())
            .collect(Collectors.toList());

    if (providers.isEmpty()) {
      throw new IllegalArgumentException(
          "No " + MetricsEvaluator.class.getSimpleName() + " class found for: " + evaluatorName);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException(
          "Multiple " + MetricsEvaluator.class.getSimpleName() + " found for: " + evaluatorName);
    } else {
      Class<? extends T> providerClz = Iterables.getOnlyElement(providers);
      try {
        return providerClz.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
