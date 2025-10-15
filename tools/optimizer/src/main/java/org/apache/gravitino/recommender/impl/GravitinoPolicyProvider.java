package org.apache.gravitino.recommender.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.recommender.api.PolicyProvider;
import org.apache.gravitino.rel.Table;

public class GravitinoPolicyProvider implements PolicyProvider {

  GravitinoClient client;

  @Override
  public List<Policy> getTablePolicy(NameIdentifier tableIdentifier) {
    Table t = client.loadCatalog("").asTableCatalog().loadTable(tableIdentifier);
    String[] policyNames = t.supportsPolicies().listPolicies();
    List<Policy> policies =
        Arrays.stream(policyNames)
            .map(t.supportsPolicies()::getPolicy)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    return policies;
  }

  @Override
  public Policy getPolicy(String policyName) {
    return client.getPolicy(policyName);
  }
}
