package org.apache.gravitino.util;

import org.apache.gravitino.NameIdentifier;
import org.apache.iceberg.catalog.TableIdentifier;

public class IdentifierUtils {

  /**
   * Convert the name identifier to the string representation.
   *
   * @param nameIdentifier The name identifier
   * @return The string representation of the name identifier
   */
  public static TableIdentifier toIcebergTableIdentifier(NameIdentifier nameIdentifier) {
    return TableIdentifier.parse(nameIdentifier.toString());
  }
}
