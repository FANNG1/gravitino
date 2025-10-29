package org.apache.gravitino.recommender.impl.job;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.recommender.impl.actor.CompactionJobContext;
import org.apache.gravitino.rel.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class TestGravitinoCompactionJobAdapter {

  @Test
  public void testJobTemplateName() {
    GravitinoCompactionJobAdapter jobAdapter = new GravitinoCompactionJobAdapter();
    jobAdapter.initialize(jobExecuteContext());
    Assertions.assertEquals("compaction-job-template", jobAdapter.jobTemplateName());
    Assertions.assertEquals(
        Map.of(
            "table", "db.table",
            "where", "",
            "options", "map('target_file_size_bytes', '1073741824')"),
        jobAdapter.jobConfig());
  }

  private CompactionJobContext jobExecuteContext() {
    Policy policy = Mockito.mock(Policy.class);
    PolicyContent policyContent = Mockito.mock(PolicyContent.class);
    Mockito.when(policy.content()).thenReturn(policyContent);
    Mockito.when(policyContent.properties())
        .thenReturn(ImmutableMap.of("job.template-name", "compaction-job-template"));
    Table table = Mockito.mock(Table.class);
    Map<String, Object> jobConfig = Map.of("target_file_size_bytes", "1073741824");
    return new CompactionJobContext(NameIdentifier.of("db", "table"), jobConfig, policy, table);
  }
}
