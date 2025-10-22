/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.server.web.filter;

import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Through dynamic proxy, obtain the annotations on the method and parameter list to perform
 * metadata authorization.
 */
public class GravitinoMetadataAuthorizationMethodInterceptor
    extends BaseMetadataAuthorizationMethodInterceptor {

  @Override
  Map<EntityType, NameIdentifier> extractNameIdentifierFromParameters(
      Parameter[] parameters, Object[] args) {
    Map<Entity.EntityType, String> entities = new HashMap<>();
    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap = new HashMap<>();
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      AuthorizationMetadata authorizeResource =
          parameter.getAnnotation(AuthorizationMetadata.class);
      if (authorizeResource == null) {
        continue;
      }
      Entity.EntityType type = authorizeResource.type();
      entities.put(type, String.valueOf(args[i]));
    }
    String metalake = entities.get(Entity.EntityType.METALAKE);
    String catalog = entities.get(Entity.EntityType.CATALOG);
    String schema = entities.get(Entity.EntityType.SCHEMA);
    String table = entities.get(Entity.EntityType.TABLE);
    String topic = entities.get(Entity.EntityType.TOPIC);
    String fileset = entities.get(Entity.EntityType.FILESET);
    entities.forEach(
        (type, metadata) -> {
          switch (type) {
            case CATALOG:
              nameIdentifierMap.put(
                  Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalake, catalog));
              break;
            case SCHEMA:
              nameIdentifierMap.put(
                  Entity.EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalake, catalog, schema));
              break;
            case TABLE:
              nameIdentifierMap.put(
                  Entity.EntityType.TABLE,
                  NameIdentifierUtil.ofTable(metalake, catalog, schema, table));
              break;
            case TOPIC:
              nameIdentifierMap.put(
                  Entity.EntityType.TOPIC,
                  NameIdentifierUtil.ofTopic(metalake, catalog, schema, topic));
              break;
            case FILESET:
              nameIdentifierMap.put(
                  Entity.EntityType.FILESET,
                  NameIdentifierUtil.ofFileset(metalake, catalog, schema, fileset));
              break;
            case MODEL:
              String model = entities.get(Entity.EntityType.MODEL);
              nameIdentifierMap.put(
                  Entity.EntityType.MODEL,
                  NameIdentifierUtil.ofModel(metalake, catalog, schema, model));
              break;
            case METALAKE:
              nameIdentifierMap.put(
                  Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalake));
              break;
            case USER:
              nameIdentifierMap.put(
                  Entity.EntityType.USER,
                  NameIdentifierUtil.ofUser(metadata, entities.get(Entity.EntityType.USER)));
              break;
            case GROUP:
              nameIdentifierMap.put(
                  Entity.EntityType.GROUP,
                  NameIdentifierUtil.ofGroup(metalake, entities.get(Entity.EntityType.GROUP)));
              break;
            case ROLE:
              nameIdentifierMap.put(
                  Entity.EntityType.ROLE,
                  NameIdentifierUtil.ofRole(metalake, entities.get(Entity.EntityType.ROLE)));
              break;
            default:
              break;
          }
        });
    return nameIdentifierMap;
  }
}
