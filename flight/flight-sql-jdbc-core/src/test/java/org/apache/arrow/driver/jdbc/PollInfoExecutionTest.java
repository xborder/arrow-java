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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Any;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.driver.jdbc.utils.PollingMockFlightSqlProducer;
import org.apache.arrow.driver.jdbc.utils.PollingMockFlightSqlProducer.Scenario;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Executable POC coverage for synchronous PollInfo through the unchanged JDBC surface. */
public class PollInfoExecutionTest {
  private static final String DIRECT_QUERY = "SELECT 42 AS answer";
  private static final String PREPARED_QUERY = "SELECT ? AS answer";
  private static final Schema RESULT_SCHEMA =
      new Schema(Collections.singletonList(Field.nullable("answer", new ArrowType.Int(32, true))));
  private static final String STATEMENT_FAMILY =
      Any.pack(CommandStatementQuery.getDefaultInstance()).getTypeUrl();
  private static final String PREPARED_FAMILY =
      Any.pack(CommandPreparedStatementQuery.getDefaultInstance()).getTypeUrl();
  private static final String CATALOGS_FAMILY =
      Any.pack(CommandGetCatalogs.getDefaultInstance()).getTypeUrl();

  private static BufferAllocator allocator;
  private static PollingMockFlightSqlProducer producer;
  private static FlightServer server;

  @BeforeAll
  public static void startServer() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
    producer = new PollingMockFlightSqlProducer();
    producer.addSelectQuery(
        DIRECT_QUERY, RESULT_SCHEMA, Collections.singletonList(PollInfoExecutionTest::send42));
    producer.addSelectQuery(
        PREPARED_QUERY, RESULT_SCHEMA, Collections.singletonList(PollInfoExecutionTest::send7));
    producer.addExpectedParameters(
        PREPARED_QUERY,
        new Schema(
            Collections.singletonList(Field.nullable("parameter", new ArrowType.Int(32, true)))),
        Collections.singletonList(Collections.singletonList(7)));
    producer.addCatalogQuery(
        CommandGetCatalogs.getDefaultInstance(), PollInfoExecutionTest::sendCatalog);
    server =
        FlightServer.builder(allocator, Location.forGrpcInsecure("localhost", 0), producer)
            .build()
            .start();
  }

  @AfterAll
  public static void stopServer() throws Exception {
    server.close();
    producer.close();
    allocator.close();
  }

  @Test
  public void immediateCompletionUsesOnePollAndNoGetFlightInfo() throws Exception {
    producer.configure(Scenario.IMMEDIATE, STATEMENT_FAMILY);
    try (Connection connection = connect(true);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(DIRECT_QUERY)) {
      assertTrue(resultSet.next());
      assertEquals(42, resultSet.getInt(1));
      assertFalse(resultSet.next());
    }
    assertEquals(1, producer.pollDescriptors().size());
    assertEquals(STATEMENT_FAMILY, family(producer.pollDescriptors().get(0)));
    assertEquals(0, producer.getFlightInfoCount());
  }

  @Test
  public void multiStepPollsOnDemandAfterPublishedEndpointIsConsumed() throws Exception {
    producer.configure(Scenario.MULTI_STEP, STATEMENT_FAMILY);
    try (Connection connection = connect(true);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(DIRECT_QUERY)) {
      assertTrue(resultSet.next());
      assertEquals(42, resultSet.getInt(1));
      assertFalse(resultSet.next());
    }
    final List<FlightDescriptor> descriptors = producer.pollDescriptors();
    assertEquals(3, descriptors.size());
    assertEquals(STATEMENT_FAMILY, family(descriptors.get(0)));
    assertEquals(
        "jdbc-continuation-1", new String(descriptors.get(1).getCommand(), StandardCharsets.UTF_8));
    assertEquals(
        "jdbc-continuation-2", new String(descriptors.get(2).getCommand(), StandardCharsets.UTF_8));
    assertEquals(3, descriptors.stream().distinct().count());
    assertEquals(0, producer.getFlightInfoCount());
  }

  @Test
  public void preparedParametersAreBoundOnceBeforePolling() throws Exception {
    producer.configure(Scenario.MULTI_STEP, PREPARED_FAMILY);
    try (Connection connection = connect(true);
        PreparedStatement statement = connection.prepareStatement(PREPARED_QUERY)) {
      statement.setInt(1, 7);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        assertEquals(7, resultSet.getInt(1));
        assertFalse(resultSet.next());
      }
    }
    assertEquals(1, producer.parameterBindCount());
    assertEquals(3, producer.pollDescriptors().size());
    assertEquals(PREPARED_FAMILY, family(producer.pollDescriptors().get(0)));
    assertEquals(0, producer.getFlightInfoCount());
  }

  @Test
  public void metadataUsesPollInfoTransparently() throws Exception {
    producer.configure(Scenario.MULTI_STEP, CATALOGS_FAMILY);
    try (Connection connection = connect(true);
        ResultSet resultSet = connection.getMetaData().getCatalogs()) {
      assertTrue(resultSet.next());
      assertEquals("poll_catalog", resultSet.getString("TABLE_CAT"));
    }
    assertEquals(3, producer.pollDescriptors().size());
    assertEquals(CATALOGS_FAMILY, family(producer.pollDescriptors().get(0)));
    assertEquals(0, producer.getFlightInfoCount());
  }

  @Test
  public void unimplementedFallbackIsCachedByFamilyOnly() throws Exception {
    producer.configure(Scenario.UNIMPLEMENTED, STATEMENT_FAMILY);
    try (Connection connection = connect(true)) {
      assertDirectRow(connection);
      assertDirectRow(connection);
      try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
        assertTrue(resultSet.next());
      }
    }
    assertEquals(2, producer.pollDescriptors().size());
    assertEquals(STATEMENT_FAMILY, family(producer.pollDescriptors().get(0)));
    assertEquals(CATALOGS_FAMILY, family(producer.pollDescriptors().get(1)));
    assertEquals(2, producer.getFlightInfoCount());
  }

  @Test
  public void connectionOptOutUsesOnlyGetFlightInfo() throws Exception {
    producer.configure(Scenario.IMMEDIATE, PREPARED_FAMILY);
    try (Connection connection = connect(false)) {
      assertDirectRow(connection);
    }
    assertEquals(0, producer.pollDescriptors().size());
    assertEquals(1, producer.getFlightInfoCount());
    assertEquals(PREPARED_FAMILY, producer.lastGetFlightInfoFamily());
  }

  @Test
  public void unavailableDoesNotFallbackOrReexecute() throws Exception {
    producer.configure(Scenario.UNAVAILABLE, STATEMENT_FAMILY);
    try (Connection connection = connect(true);
        Statement statement = connection.createStatement()) {
      final Throwable failure =
          assertThrows(Throwable.class, () -> statement.executeQuery(DIRECT_QUERY));
      assertTrue(hasFlightStatus(failure, FlightStatusCode.UNAVAILABLE), failure.toString());
    }
    assertEquals(1, producer.pollDescriptors().size());
    assertEquals(0, producer.getFlightInfoCount());
  }

  @Test
  public void continuationUnimplementedDoesNotFallbackOrReexecute() throws Exception {
    producer.configure(Scenario.CONTINUATION_UNIMPLEMENTED, STATEMENT_FAMILY);
    try (Connection connection = connect(true);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(DIRECT_QUERY)) {
      assertTrue(resultSet.next());
      assertEquals(42, resultSet.getInt(1));
      final Throwable failure = assertThrows(Throwable.class, resultSet::next);
      assertTrue(hasFlightStatus(failure, FlightStatusCode.UNIMPLEMENTED), failure.toString());
    }
    assertEquals(2, producer.pollDescriptors().size());
    assertEquals(0, producer.getFlightInfoCount());
  }

  @Test
  public void oneDeadlineSpansAllPollsAndTerminatesActivePoll() throws Exception {
    producer.configure(Scenario.TIMEOUT, STATEMENT_FAMILY);
    final long startNanos = System.nanoTime();
    try (Connection connection = connect(true);
        Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(1);
      try (ResultSet resultSet = statement.executeQuery(DIRECT_QUERY)) {
        assertTrue(resultSet.next());
        assertEquals(42, resultSet.getInt(1));
        final Throwable failure = assertThrows(Throwable.class, resultSet::next);
        assertTrue(hasFlightStatus(failure, FlightStatusCode.TIMED_OUT), failure.toString());
        assertJdbcTimeoutContract(failure, 1);
      }
    }
    final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    System.out.println("LOCAL_T8_ELAPSED_MILLIS " + elapsedMillis);
    assertTrue(elapsedMillis >= 850, "deadline fired too early: " + elapsedMillis);
    assertTrue(elapsedMillis < 1500, "deadline appears to have reset: " + elapsedMillis);
    assertEquals(2, producer.pollDescriptors().size());
    assertEquals(0, producer.getFlightInfoCount());
    assertTrue(producer.awaitPollTerminated(1, TimeUnit.SECONDS));
  }

  @Test
  public void preparedBindSharesTheOperationDeadline() throws Exception {
    producer.configure(Scenario.BIND_TIMEOUT, PREPARED_FAMILY);
    final long startNanos = System.nanoTime();
    try (Connection connection = connect(true);
        PreparedStatement statement = connection.prepareStatement(PREPARED_QUERY)) {
      statement.setInt(1, 7);
      statement.setQueryTimeout(1);
      final Throwable failure = assertThrows(Throwable.class, statement::executeQuery);
      assertTrue(hasFlightStatus(failure, FlightStatusCode.TIMED_OUT), failure.toString());
      assertJdbcTimeoutContract(failure, 1);
    }
    final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    assertTrue(elapsedMillis >= 850, "bind deadline fired too early: " + elapsedMillis);
    assertTrue(elapsedMillis < 1500, "bind deadline was not operation-wide: " + elapsedMillis);
    assertEquals(1, producer.parameterBindCount());
    assertEquals(0, producer.pollDescriptors().size());
    assertTrue(producer.awaitPollTerminated(1, TimeUnit.SECONDS));
  }

  @Test
  public void preparedBindIsCancelledBeforePolling() throws Exception {
    producer.configure(Scenario.BIND_CANCEL, PREPARED_FAMILY);
    try (Connection connection = connect(true);
        PreparedStatement statement = connection.prepareStatement(PREPARED_QUERY)) {
      statement.setInt(1, 7);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread execution =
          new Thread(
              () -> {
                try (ResultSet ignored = statement.executeQuery()) {
                  // A blocked bind must not produce a ResultSet.
                } catch (Throwable e) {
                  failure.set(e);
                }
              },
              "jdbc-prepared-bind-cancellation-test");
      execution.start();
      assertTrue(producer.awaitActivePoll(2, TimeUnit.SECONDS));
      assertNull(statement.getResultSet());
      statement.cancel();
      execution.join(TimeUnit.SECONDS.toMillis(2));
      assertFalse(execution.isAlive());
      assertNotNull(failure.get());
      assertTrue(
          hasFlightStatus(failure.get(), FlightStatusCode.CANCELLED), failure.get().toString());
      assertEquals(1, producer.parameterBindCount());
      assertEquals(0, producer.pollDescriptors().size());
      assertEquals(0, producer.cancelFlightInfoCount());
      assertTrue(producer.awaitPollTerminated(1, TimeUnit.SECONDS));
    }
  }

  @Test
  public void statementCancelInterruptsContinuationAtReadBoundary() throws Exception {
    producer.configure(Scenario.CANCEL_OBSERVABLE, STATEMENT_FAMILY);
    try (Connection connection = connect(true);
        Statement statement = connection.createStatement()) {
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicBoolean resultSetCreated = new AtomicBoolean();
      final Thread execution =
          new Thread(
              () -> {
                try (ResultSet resultSet = statement.executeQuery(DIRECT_QUERY)) {
                  resultSetCreated.set(true);
                  if (!resultSet.next() || resultSet.getInt(1) != 42) {
                    throw new AssertionError("missing published row");
                  }
                  resultSet.next();
                } catch (Throwable e) {
                  failure.set(e);
                }
              },
              "jdbc-pollinfo-cancellation-test");
      execution.start();
      assertTrue(producer.awaitActivePoll(2, TimeUnit.SECONDS));
      assertNotNull(statement.getResultSet());
      final long cancelStart = System.nanoTime();
      statement.cancel();
      execution.join(TimeUnit.SECONDS.toMillis(2));
      final long cancelMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - cancelStart);
      System.out.println("LOCAL_T9_CANCEL_MILLIS " + cancelMillis);
      assertFalse(execution.isAlive());
      assertTrue(resultSetCreated.get(), "ResultSet should expose the first published endpoint");
      assertNotNull(failure.get());
      assertTrue(
          hasFlightStatus(failure.get(), FlightStatusCode.CANCELLED), failure.get().toString());
      assertTrue(cancelMillis < 1000, "cancellation was not prompt: " + cancelMillis);
      assertTrue(producer.awaitPollTerminated(1, TimeUnit.SECONDS));
      assertEquals(1, producer.cancelFlightInfoCount());
      assertEquals(0, producer.getFlightInfoCount());
    }
  }

  private static Connection connect(final boolean usePollInfo) throws Exception {
    return DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%d?useEncryption=false&usePollInfo=%s",
            server.getPort(), usePollInfo));
  }

  private static void assertDirectRow(final Connection connection) throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(DIRECT_QUERY)) {
      assertTrue(resultSet.next());
      assertEquals(42, resultSet.getInt(1));
    }
  }

  private static String family(final FlightDescriptor descriptor) {
    return PollingMockFlightSqlProducer.commandFamily(descriptor);
  }

  private static boolean hasFlightStatus(
      final Throwable throwable, final FlightStatusCode expected) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof FlightRuntimeException
          && ((FlightRuntimeException) current).status().code() == expected) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static void assertJdbcTimeoutContract(
      final Throwable throwable, final int timeoutSeconds) {
    Throwable current = throwable;
    while (current != null && !(current instanceof SQLTimeoutException)) {
      current = current.getCause();
    }
    assertNotNull(current, "Expected SQLTimeoutException in cause chain: " + throwable);
    assertEquals(
        String.format("Query timed out after %d %s", timeoutSeconds, TimeUnit.SECONDS),
        current.getMessage());
  }

  private static void send42(final FlightSqlProducer.ServerStreamListener listener) {
    sendInt(listener, 42);
  }

  private static void send7(final FlightSqlProducer.ServerStreamListener listener) {
    sendInt(listener, 7);
  }

  private static void sendInt(
      final FlightSqlProducer.ServerStreamListener listener, final int value) {
    try (BufferAllocator streamAllocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(RESULT_SCHEMA, streamAllocator)) {
      root.allocateNew();
      ((IntVector) root.getVector("answer")).setSafe(0, value);
      root.setRowCount(1);
      listener.start(root);
      listener.putNext();
    } finally {
      listener.completed();
    }
  }

  private static void sendCatalog(final FlightSqlProducer.ServerStreamListener listener) {
    try (BufferAllocator streamAllocator = new RootAllocator();
        VectorSchemaRoot root =
            VectorSchemaRoot.create(
                FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA, streamAllocator)) {
      root.allocateNew();
      ((VarCharVector) root.getVector("catalog_name")).setSafe(0, new Text("poll_catalog"));
      root.setRowCount(1);
      listener.start(root);
      listener.putNext();
    } finally {
      listener.completed();
    }
  }
}
