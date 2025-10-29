package org.apache.gravitino.recommender.impl.actor;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.recommender.impl.util.PolicyUtils;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class CompactionPolicyForTest implements Policy {

  @Override
  public String name() {
    return "compaction-policy-for-test";
  }

  @Override
  public String policyType() {
    return "compaction";
  }

  @Override
  public String comment() {
    return "compaction-policy-for-test";
  }

  @Override
  public boolean enabled() {
    return true;
  }

  @Override
  public PolicyContent content() {
    return new PolicyContent() {
      @Override
      public Set<MetadataObject.Type> supportedObjectTypes() {
        return Set.of(MetadataObject.Type.TABLE);
      }

      @Override
      public Map<String, String> properties() {
        return ImmutableMap.of(
            "min_datafile_mse",
            "1000",
            PolicyUtils.JOB_ROLE_PREFIX + RewriteDataFiles.TARGET_FILE_SIZE_BYTES,
            "1024",
            "compaction.trigger-expr",
            "datafile_mse > min_datafile_mse",
            "compaction.score-expr",
            "datafile_mse * delete_file_num");
      }
    };
  }

  @Override
  public Optional<Boolean> inherited() {
    return Optional.empty();
  }

  @Override
  public Audit auditInfo() {
    return null;
  }
}
