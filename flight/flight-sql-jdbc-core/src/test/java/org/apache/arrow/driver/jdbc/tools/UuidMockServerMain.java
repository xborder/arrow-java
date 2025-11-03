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
package org.apache.arrow.driver.jdbc.tools;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.UuidType;

/**
 * Starts an in-process Flight SQL mock server exposing a UUID typed column. Default port is
 * ephemeral; the effective port is printed to stdout.
 */
public final class UuidMockServerMain {
  public static void main(String[] args) throws Exception {
    // Ensure extension registry knows about uuid
    if (ExtensionTypeRegistry.lookup("arrow.uuid") == null) {
      ExtensionTypeRegistry.register(new UuidType());
    }

    final MockFlightSqlProducer producer = CoreMockedSqlProducers.getLegacyProducer();
    final Schema schema = new Schema(List.of(Field.nullable("id", new UuidType())));

    producer.addSelectQuery(
        "select id from uuid_table",
        schema,
        List.of(
            listener -> {
              try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                  VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                UuidVector vec =
                    (UuidVector)
                        new UuidType()
                            .getNewVector("id", FieldType.nullable(new UuidType()), allocator);
                vec.allocateNew();
                vec.set(0, UUID.fromString("123e4567-e89b-12d3-a456-426655440000"));
                vec.set(1, UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001"));
                vec.setNull(2);
                vec.setValueCount(3);
                try (VectorSchemaRoot batch =
                    new VectorSchemaRoot(List.of(vec.getField()), List.of(vec), 3)) {
                  listener.start(batch);
                  listener.putNext();
                }
              } finally {
                listener.completed();
              }
            }));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      UserPasswordAuthentication authentication =
          new UserPasswordAuthentication.Builder()
              .user("flight-test-user", "flight-test-password")
              .build();

      FlightServer server =
          FlightServer.builder(allocator, Location.forGrpcInsecure("localhost", 0), producer)
              .headerAuthenticator(authentication.authenticate())
              .build()
              .start();

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      server.close();
                    } catch (Exception ignored) {
                    }
                    try {
                      allocator.close();
                    } catch (Exception ignored) {
                    }
                  }));

      System.out.println("UUID Mock Flight SQL server started.");
      System.out.println("Host: localhost");
      System.out.println("Port: " + server.getPort());
      System.out.flush();

      // Block until killed (Ctrl+C)
      new CountDownLatch(1).await();
    }
  }
}
