package org.apache.gravitino.monitor;

import java.util.Optional;
import org.apache.gravitino.NameIdentifier;

public class MonitorCli {

  /**
   * Runs the monitor with the given arguments.
   *
   * @param args The command-line arguments. Expected format: [0] - table identifier (e.g.,
   *     "db.table") [1] - Optimize Action time (in epoch seconds) [2] - Range hours (in hours) [3]
   *     - Optional policy type (e.g., "compaction")
   *     <p>For example: ./monitorCli db.table 1760693151 24 compaction will get the table metrics
   *     and job metrics in the range of [action time - range hours*3600, action time + range
   *     hours*3600], and then compare the metrics before and after the action time.
   */
  public void run(String[] args) {
    NameIdentifier tableIdentifier = NameIdentifier.of("db", "table");
    long time = Long.parseLong(args[1]);
    long rangeSeconds = Long.parseLong(args[2]);
    Optional<String> policyType = Optional.ofNullable(args[3]);
    new Monitor().run(tableIdentifier, time, rangeSeconds, policyType);
  }

  public static void main(String[] args) {
    new MonitorCli().run(args);
  }
}
