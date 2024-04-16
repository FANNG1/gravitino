/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.TestSchema;
import com.datastrato.gravitino.catalog.SchemaDispatcher;
import com.datastrato.gravitino.catalog.SchemaEventDispatcher;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.listener.DummyEventListener;
import com.datastrato.gravitino.listener.EventBus;
import com.datastrato.gravitino.listener.api.info.SchemaInfo;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.stubbing.Answer;

@TestInstance(Lifecycle.PER_CLASS)
public class TestSchemaEvent {
  private SchemaEventDispatcher dispatcher;
  private SchemaEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Schema schema =
      TestSchema.builder()
          .withName("schema")
          .withComment("comment")
          .withProperties(ImmutableMap.of("a", "b1"))
          .build();

  @BeforeAll
  void init() {
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    SchemaDispatcher schemaDispatcher = mockSchemaDispatcher();
    this.dispatcher = new SchemaEventDispatcher(eventBus, schemaDispatcher);
    SchemaDispatcher schemaExceptionDispatcher = mockExceptionSchemaDispatcher();
    this.failureDispatcher = new SchemaEventDispatcher(eventBus, schemaExceptionDispatcher);
  }

  @Test
  void testCreateSchema() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema");
    dispatcher.createSchema(identifier, "", ImmutableMap.of());
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateSchemaEvent.class, event.getClass());
    SchemaInfo schemaInfo = ((CreateSchemaEvent) event).createdSchemaInfo();
    checkSchemaInfo(schemaInfo, schema);
  }

  @Test
  void testLoadSchema() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema");
    dispatcher.loadSchema(identifier);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadSchemaEvent.class, event.getClass());
    SchemaInfo schemaInfo = ((LoadSchemaEvent) event).loadedSchemaInfo();
    checkSchemaInfo(schemaInfo, schema);
  }

  @Test
  void testListSchema() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    dispatcher.listSchemas(namespace);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(ListSchemaEvent.class, event.getClass());
    Assertions.assertEquals(namespace, ((ListSchemaEvent) event).namespace());
  }

  @Test
  void testAlterSchema() {
    SchemaChange schemaChange = SchemaChange.setProperty("a", "b");
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema");
    dispatcher.alterSchema(identifier, schemaChange);
    Event event = dummyEventListener.popEvent();

    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterSchemaEvent.class, event.getClass());
    SchemaInfo schemaInfo = ((AlterSchemaEvent) event).updatedSchemaInfo();
    checkSchemaInfo(schemaInfo, schema);

    Assertions.assertEquals(1, ((AlterSchemaEvent) event).schemaChanges().length);
    Assertions.assertEquals(schemaChange, ((AlterSchemaEvent) event).schemaChanges()[0]);
  }

  @Test
  void testDropSchema() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema");
    dispatcher.dropSchema(identifier, true);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropSchemaEvent.class, event.getClass());
    Assertions.assertEquals(true, ((DropSchemaEvent) event).cascade());
    Assertions.assertEquals(false, ((DropSchemaEvent) event).isExists());
  }

  @Test
  void testCreateSchemaFailure() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.createSchema(identifier, schema.comment(), schema.properties()));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateSchemaFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((CreateSchemaFailureEvent) event).exception().getClass());
    checkSchemaInfo(((CreateSchemaFailureEvent) event).createSchemaRequest(), schema);
  }

  @Test
  void testLoadSchemaFailure() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.loadSchema(identifier));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadSchemaFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((LoadSchemaFailureEvent) event).exception().getClass());
  }

  void testAlterSchemaFailure() {
    // alter schema
    SchemaChange schemaChange = SchemaChange.setProperty("a", "b");
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterSchema(identifier, schemaChange));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterSchemaFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterSchemaFailureEvent) event).exception().getClass());
    Assertions.assertEquals(1, ((AlterSchemaFailureEvent) event).schemaChanges().length);
    Assertions.assertEquals(schemaChange, ((AlterSchemaFailureEvent) event).schemaChanges()[0]);
  }

  @Test
  void testDropSchemaFailure() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropSchema(identifier, true));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropSchemaFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DropSchemaFailureEvent) event).exception().getClass());
    Assertions.assertEquals(true, ((DropSchemaFailureEvent) event).cascade());
  }

  @Test
  void testListSchemaFailure() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listSchemas(namespace));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(NameIdentifier.of(namespace.toString()), event.identifier());
    Assertions.assertEquals(ListSchemaFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListSchemaFailureEvent) event).exception().getClass());
    Assertions.assertEquals(namespace, ((ListSchemaFailureEvent) event).namespace());
  }

  private void checkSchemaInfo(SchemaInfo schemaInfo, Schema schema) {
    Assertions.assertEquals(schema.name(), schemaInfo.name());
    Assertions.assertEquals(schema.properties(), schemaInfo.properties());
    Assertions.assertEquals(schema.comment(), schemaInfo.comment());
  }

  private SchemaDispatcher mockSchemaDispatcher() {
    SchemaDispatcher dispatcher = mock(SchemaDispatcher.class);
    when(dispatcher.createSchema(any(NameIdentifier.class), any(String.class), any(Map.class)))
        .thenReturn(schema);
    when(dispatcher.loadSchema(any(NameIdentifier.class))).thenReturn(schema);
    when(dispatcher.dropSchema(any(NameIdentifier.class), eq(true))).thenReturn(false);
    when(dispatcher.listSchemas(any(Namespace.class))).thenReturn(null);
    when(dispatcher.alterSchema(any(NameIdentifier.class), any(SchemaChange.class)))
        .thenReturn(schema);
    return dispatcher;
  }

  private SchemaDispatcher mockExceptionSchemaDispatcher() {
    SchemaDispatcher dispatcher =
        mock(
            SchemaDispatcher.class,
            (Answer)
                invocation -> {
                  throw new GravitinoRuntimeException("Exception for all methods");
                });
    return dispatcher;
  }
}
