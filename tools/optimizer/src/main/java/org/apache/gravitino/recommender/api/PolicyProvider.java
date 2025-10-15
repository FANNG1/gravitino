package org.apache.gravitino.recommender.api;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.policy.Policy;

// The policy provider to get the policies from Gravitino or external systems.
public interface PolicyProvider {
  List<Policy> getTablePolicy(NameIdentifier tableIdentifier);

  Policy getPolicy(String policyName);
}
