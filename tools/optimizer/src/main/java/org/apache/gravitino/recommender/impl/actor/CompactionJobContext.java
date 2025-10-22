package org.apache.gravitino.recommender.impl.actor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.ToString;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.recommender.api.PolicyActor.JobExecuteContext;
import org.apache.gravitino.rel.Table;
import org.apache.iceberg.actions.RewriteDataFiles;

@ToString
public class CompactionJobContext implements JobExecuteContext {
  private NameIdentifier name;
  private Map<String, Object> config;
  private Policy policy;
  private Table tableMetadata;

  public CompactionJobContext(
      NameIdentifier name, Map<String, Object> config, Policy policy, Table tableMetadata) {
    this.policy = policy;
    this.name = name;
    this.config = config;
    this.tableMetadata = tableMetadata;
  }

  @Override
  public NameIdentifier name() {
    return name;
  }

  @Override
  public Map<String, Object> config() {
    return config;
  }

  @Override
  public Policy policy() {
    return policy;
  }

  public Optional<Long> targetFileSize() {
    return Optional.ofNullable((long) config.get(RewriteDataFiles.TARGET_FILE_SIZE_BYTES));
  }

  public Optional<List<String>> partitionNames() {
    return Optional.empty();
  }
}
