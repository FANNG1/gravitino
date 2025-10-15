package org.apache.gravitino.updater;

import java.util.LinkedList;
import java.util.List;
import org.apache.gravitino.NameIdentifier;

public class UpdaterCli {
  /**
   * Computes and updates the stats for the given tables
   *
   * @param args The command-line arguments. Expected format: [0] - stats computer name (e.g.,
   *     "gravitino-table-datasize") [1] - table identifiers (e.g., "db.table,db.table2") [2] -
   *     update type (e.g., metrics or stats)
   *     <p>output: the updated stats for the tables.
   */
  public void run(String[] args) {
    List<NameIdentifier> tableIdentifiers = new LinkedList<>();
    String statsComputerName = args[0];
    new Updater().update(statsComputerName, tableIdentifiers, UpdateType.STATS);
  }

  public static void main(String[] args) {
    new UpdaterCli().run(args);
  }
}
