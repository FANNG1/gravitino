/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.catalog;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.spark.PropertiesConverter;
import com.datastrato.gravitino.spark.SparkTypeConverter;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.NotSupportedException;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * BaseCatalog is the base class for the specific catalog like GravitinoHiveCatalog, it provides a
 * basic table and namespace interfaces. The advanced interfaces like view and function should be
 * provided by the specific catalog if necessary.
 */
public abstract class BaseCatalog implements TableCatalog, SupportsNamespaces {
  protected TableCatalog sparkCatalog;
  protected Catalog gravitinoCatalog;
  protected PropertiesConverter propertiesConverter;

  private String metalakeName;
  private String catalogName;
  private GravitinoCatalogManager gravitinoCatalogManager;

  // Create a catalog specific table with different capabilities. Proxies schema and property
  // to GravitinoTable while IO operations to internal catalog.
  abstract Table createSparkTable(
      Identifier identifier, com.datastrato.gravitino.rel.Table gravitinoTable);

  // Create a internal catalog to do IO operations.
  abstract TableCatalog createAndInitSparkCatalog(String name, CaseInsensitiveStringMap options);

  abstract PropertiesConverter createPropertiesConverter();

  public BaseCatalog() {
    gravitinoCatalogManager = GravitinoCatalogManager.get();
    metalakeName = gravitinoCatalogManager.getMetalakeName();
    propertiesConverter = createPropertiesConverter();
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    throw new NotSupportedException("Doesn't support listing table");
  }

  @Override
  public Table createTable(
      Identifier ident, Column[] columns, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    NameIdentifier nameIdentifier =
        NameIdentifier.of(metalakeName, catalogName, getDatabase(ident), ident.name());
    ColumnDTO[] gravitinoColumns =
        Arrays.stream(columns)
            .map(column -> createGravitinoColumn(column))
            .toArray(ColumnDTO[]::new);

    Map<String, String> gravitinoProperties =
        propertiesConverter.toGravitinoTableProperties(properties);
    String comment = gravitinoProperties.remove("comment");

    try {
      com.datastrato.gravitino.rel.Table table =
          gravitinoCatalog
              .asTableCatalog()
              .createTable(nameIdentifier, gravitinoColumns, comment, gravitinoProperties);
      return createSparkTable(ident, table);
    } catch (NoSuchSchemaException e) {
      throw new NoSuchNamespaceException(ident.namespace());
    } catch (com.datastrato.gravitino.exceptions.TableAlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  // Will create a catalog specific table by invoking createSparkTable()
  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      com.datastrato.gravitino.rel.Table table =
          gravitinoCatalog
              .asTableCatalog()
              .loadTable(
                  NameIdentifier.of(metalakeName, catalogName, getDatabase(ident), ident.name()));
      return createSparkTable(ident, table);
    } catch (com.datastrato.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    throw new NotSupportedException("Deprecated create table method");
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new NotSupportedException("Doesn't support altering table");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return gravitinoCatalog
        .asTableCatalog()
        .dropTable(NameIdentifier.of(metalakeName, catalogName, getDatabase(ident), ident.name()));
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new NotSupportedException("Doesn't support renaming table");
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    gravitinoCatalog = gravitinoCatalogManager.getGravitinoCatalogInfo(name);
    sparkCatalog = createAndInitSparkCatalog(name, options);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    NameIdentifier[] schemas =
        gravitinoCatalog.asSchemas().listSchemas(Namespace.of(metalakeName, catalogName));
    return Arrays.stream(schemas)
        .map(schema -> new String[] {schema.name()})
        .toArray(String[][]::new);
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    if (namespace.length == 0) {
      return listNamespaces();
    }
    throw new NotSupportedException(
        "Doesn't support listing namespaces with " + String.join(".", namespace));
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    valiateNamespace(namespace);
    try {
      Schema schema =
          gravitinoCatalog
              .asSchemas()
              .loadSchema(NameIdentifier.of(metalakeName, catalogName, namespace[0]));
      String comment = schema.comment();
      Map<String, String> properties = schema.properties();
      if (comment != null) {
        properties = new HashMap<>(schema.properties());
        properties.put(SupportsNamespaces.PROP_COMMENT, comment);
      }
      return properties;
    } catch (NoSuchSchemaException e) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    valiateNamespace(namespace);
    Map<String, String> properties = new HashMap<>(metadata);
    String comment = properties.remove(SupportsNamespaces.PROP_COMMENT);
    try {
      gravitinoCatalog
          .asSchemas()
          .createSchema(
              NameIdentifier.of(metalakeName, catalogName, namespace[0]), comment, properties);
    } catch (SchemaAlreadyExistsException e) {
      throw new NamespaceAlreadyExistsException(namespace);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    throw new NotSupportedException("Doesn't support altering namespace");
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    valiateNamespace(namespace);
    try {
      return gravitinoCatalog
          .asSchemas()
          .dropSchema(NameIdentifier.of(metalakeName, catalogName, namespace[0]), cascade);
    } catch (NonEmptySchemaException e) {
      throw new NonEmptyNamespaceException(namespace);
    }
  }

  private String getDatabase(Identifier ident) {
    String database = "default";
    if (ident.namespace().length > 0) {
      database = ident.namespace()[0];
    }
    return database;
  }

  private void valiateNamespace(String[] namespace) {
    Preconditions.checkArgument(
        namespace.length == 1,
        "Doesn't support multi level namespaces: " + String.join(".", namespace));
  }

  private ColumnDTO createGravitinoColumn(Column sparkColumn) {
    return ColumnDTO.builder()
        .withName(sparkColumn.name())
        .withDataType(SparkTypeConverter.convert(sparkColumn.dataType()))
        .withNullable(sparkColumn.nullable())
        .withComment(sparkColumn.comment())
        .build();
  }

  void convertTableProperties(Map<String, String> properties) {}
}
