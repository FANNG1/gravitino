package org.apache.gravitino.storage.credential;

import java.util.List;
import java.util.Map;

public interface CredentialProvider {
  Map<String, String> getCredential(boolean allowList, List<String> writeLocations, List<String> readLocations);
}
