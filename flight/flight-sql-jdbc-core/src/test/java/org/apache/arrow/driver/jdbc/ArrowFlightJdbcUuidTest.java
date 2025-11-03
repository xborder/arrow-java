/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.driver.jdbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.UuidType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ArrowFlightJdbcUuidTest {

  private static final MockFlightSqlProducer PRODUCER = CoreMockedSqlProducers.getLegacyProducer();

  @RegisterExtension
  public static final FlightServerTestExtension flightServer =
      FlightServerTestExtension.createStandardTestExtension(PRODUCER);

  @BeforeAll
  public static void setUp() throws Exception {
    // Ensure extension type registry knows about the uuid extension
    if (ExtensionTypeRegistry.lookup("arrow.uuid") == null) {
      ExtensionTypeRegistry.register(new UuidType());
    }

    // Register a SELECT with a UUID extension-typed column on the same producer used by the server
    final Schema schema = new Schema(List.of(Field.nullable("id", new UuidType())));

    PRODUCER.addSelectQuery(
        "select id from uuid_table",
        schema,
        List.of(
            listener -> {
              try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                  VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                // Build a batch with two UUIDs and a null
                UuidVector uuidVec =
                    (UuidVector)
                        new UuidType()
                            .getNewVector("id", FieldType.nullable(new UuidType()), allocator);
                uuidVec.allocateNew();
                UUID u0 = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");
                UUID u1 = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");
                uuidVec.set(0, u0);
                uuidVec.set(1, u1);
                uuidVec.setNull(2);
                uuidVec.setValueCount(3);

                try (VectorSchemaRoot batch =
                    new VectorSchemaRoot(List.of(uuidVec.getField()), List.of(uuidVec), 3)) {
                  listener.start(batch);
                  listener.putNext();
                }
              } finally {
                listener.completed();
              }
            }));
  }

  @Test
  public void testJdbcApiReadsUuid() throws Exception {
    try (Connection conn = flightServer.getConnection(false); // no TLS
        Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("select id from uuid_table")) {
        int idx = 0;
        while (rs.next()) {
          if (idx == 0) {
            Object o = rs.getObject(1, UUID.class);
            assertThat(o, equalTo(UUID.fromString("123e4567-e89b-12d3-a456-426655440000")));
          } else if (idx == 1) {
            Object o = rs.getObject(1, UUID.class);
            assertThat(o, equalTo(UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001")));
          } else if (idx == 2) {
            Object o = rs.getObject(1, UUID.class);
            assertThat(o, equalTo(null));
          }
          idx++;
        }
      }
    }
  }
}
