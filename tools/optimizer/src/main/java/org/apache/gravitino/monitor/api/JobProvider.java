package org.apache.gravitino.monitor.api;

import java.util.List;
import org.apache.gravitino.NameIdentifier;

// Get upstream and downstream jobs for a table
public interface JobProvider {
  List<NameIdentifier> getJobNames(NameIdentifier tableIdentifier);
}
