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
package org.apache.arrow.flight.sql.test;

import static org.apache.arrow.util.AutoCloseables.close;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlExchangeProducer;
import org.apache.arrow.flight.sql.example.FlightSqlExample;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Runs the classic Flight SQL tests against a server wrapped in {@link FlightSqlExchangeProducer}:
 * adding DoExchange support must not change the other RPCs.
 */
public class TestFlightSqlThroughExchangeProducer extends TestFlightSql {
  private static final String DB_NAME = "derbyThroughExchangeDB";

  @BeforeAll
  public static void setUp() throws Exception {
    allocator = new RootAllocator(Integer.MAX_VALUE);
    final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 0);
    server =
        FlightServer.builder(
                allocator,
                serverLocation,
                new FlightSqlExchangeProducer(
                    new FlightSqlExample(serverLocation, DB_NAME), allocator))
            .build()
            .start();
    final Location clientLocation = Location.forGrpcInsecure(LOCALHOST, server.getPort());
    sqlClient = new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build());
    setUpExpectedResultsMap();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    close(sqlClient, server, allocator);
    FlightSqlExample.removeDerbyDatabaseIfExists(DB_NAME);
  }
}
