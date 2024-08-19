package org.apache.gravitino.storage.s3;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.credential.CredentialProvider;

public class S3CredentialProvider implements CredentialProvider {
  @Override
  public Map<String, String> getCredential(boolean allowList, List<String> writeLocations,
      List<String> readLocations) {
    return null;
  }
}
