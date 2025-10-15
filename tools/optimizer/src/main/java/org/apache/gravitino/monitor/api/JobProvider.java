package org.apache.gravitino.monitor.api;

import org.apache.gravitino.NameIdentifier;

// Get upstream and downstream job names for a table
public interface JobProvider {
  String[] getJobNames(NameIdentifier tableIdentifier);
}
