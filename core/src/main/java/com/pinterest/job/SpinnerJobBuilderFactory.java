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

package com.pinterest.job;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpinnerJobBuilderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SpinnerJobBuilderFactory.class);

  private static final List<Class<? extends SpinnerJobBuilder>> BUILDER_CLASSES =
      ImmutableList.of(CompactionSpinnerJobBuilder.class);

  private static final Map<String, SpinnerJobBuilder> JOB_BUILDER_REGISTRY =
      createJobBuilders().stream()
          .collect(
              ImmutableMap.toImmutableMap(
                  SpinnerJobBuilder::getJobTemplateName, builder -> builder));

  public static SpinnerJobBuilder getJobBuilder(String jobTemplateName) {
    SpinnerJobBuilder jobBuilder = JOB_BUILDER_REGISTRY.get(jobTemplateName);
    if (jobBuilder == null) {
      throw new IllegalArgumentException("Unknown job template name: " + jobTemplateName);
    }
    return jobBuilder;
  }

  private static List<SpinnerJobBuilder> createJobBuilders() {
    ImmutableList.Builder<SpinnerJobBuilder> builders = ImmutableList.builder();
    for (Class<? extends SpinnerJobBuilder> builderClass : BUILDER_CLASSES) {
      try {
        builders.add(builderClass.getDeclaredConstructor().newInstance());
      } catch (Exception e) {
        LOG.error("Failed to instantiate SpinnerJobBuilder: {}", builderClass.getName(), e);
        throw new RuntimeException("Failed to create job builder: " + builderClass.getName(), e);
      }
    }
    return builders.build();
  }
}
