package com.pinterest.job;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpinnerJobBuilderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SpinnerJobBuilderFactory.class);

  private static final List<Class<? extends SpinnerJobBuilder>> BUILDER_CLASSES =
      ImmutableList.of(CompactionSpinnerJobBuilder.class);

  public static List<SpinnerJobBuilder> createJobBuilders() {
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
