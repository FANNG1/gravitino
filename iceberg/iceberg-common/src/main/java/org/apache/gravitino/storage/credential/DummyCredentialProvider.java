package org.apache.gravitino.storage.credential;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;

public class DummyCredentialProvider implements CredentialProvider {
  @Override
  public Map<String, String> getCredential(boolean allowList, List<String> writeLocations,
      List<String> readLocations) {
    return ImmutableMap.of();
  }
}
