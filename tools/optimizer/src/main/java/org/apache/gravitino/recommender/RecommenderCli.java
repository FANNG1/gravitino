package org.apache.gravitino.recommender;

import java.util.LinkedList;
import java.util.List;
import org.apache.gravitino.NameIdentifier;

public class RecommenderCli {
  Recommender recommender = new Recommender();
  /**
   * Runs the recommender with the given arguments.
   *
   * @param args The command-line arguments. Expected format: [0] - policy type (e.g., "compact")
   *     [1] - table identifiers (e.g., "db.table,db.table2")
   *     <p>For example ./recommender-cli —tables tableA, tableB, tableC –policyType compaction
   *     <p>Output:
   *     <p>PolicyA: Selected tables: tableA,tableB tableA: Score: 90 jobConfig: Partitions:
   *     [day=2024, day=2025] Target-size: xxx tableB:xx PolicyB: Selected tables: tableB,tableC
   *     tableB:xx tableC:xx
   */
  public void run(String[] args) {
    List<NameIdentifier> tables = new LinkedList<>();
    recommender.recommendForPolicyType(tables, "compact");
  }

  public static void main(String[] args) {
    new RecommenderCli().run(args);
  }
}
