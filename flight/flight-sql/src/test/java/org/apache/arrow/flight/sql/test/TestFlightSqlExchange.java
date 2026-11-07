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

import static org.apache.arrow.flight.FlightMethod.DO_ACTION;
import static org.apache.arrow.flight.FlightMethod.DO_EXCHANGE;
import static org.apache.arrow.flight.FlightMethod.DO_GET;
import static org.apache.arrow.flight.FlightMethod.DO_PUT;
import static org.apache.arrow.flight.FlightMethod.GET_FLIGHT_INFO;
import static org.apache.arrow.util.AutoCloseables.close;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightMethod;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlClient.ExecuteIngestOptions;
import org.apache.arrow.flight.sql.FlightSqlExchangeClient;
import org.apache.arrow.flight.sql.FlightSqlExchangeProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;
import org.apache.arrow.flight.sql.example.FlightSqlExample;
import org.apache.arrow.flight.sql.example.FlightSqlStatelessExample;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions.TableExistsOption;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions.TableNotExistOption;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runs Flight SQL statements with a single DoExchange call each, and compares them with the classic
 * Flight SQL RPC sequences: same results, fewer calls.
 */
public class TestFlightSqlExchange {
  private static final String DB_NAME = "derbyExchangeDB";
  private static final String STATELESS_DB_NAME = "derbyExchangeStatelessDB";
  private static final List<List<String>> ALL_ROWS =
      List.of(
          List.of("1", "one", "1", "1"),
          List.of("2", "zero", "0", "1"),
          List.of("3", "negative one", "-1", "1"));

  private static BufferAllocator allocator;
  private static RpcCounter rpcs;
  private static CountingExample producer;
  private static FlightServer server;
  private static FlightSqlClient classic;
  private static FlightSqlExchangeClient exchange;

  @BeforeAll
  public static void setUp() throws Exception {
    allocator = new RootAllocator(Integer.MAX_VALUE);
    final Location location = Location.forGrpcInsecure("localhost", 0);
    producer = new CountingExample(location, DB_NAME);
    rpcs = new RpcCounter();
    server =
        FlightServer.builder(
                allocator, location, new FlightSqlExchangeProducer(producer, allocator))
            .middleware(FlightServerMiddleware.Key.of("rpc-counter"), rpcs)
            .build()
            .start();
    final Location clientLocation = Location.forGrpcInsecure("localhost", server.getPort());
    classic = new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build());
    exchange = new FlightSqlExchangeClient(FlightClient.builder(allocator, clientLocation).build());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    // Like the other Flight SQL tests, leave the example producer open: its DoGet handlers leak
    // memory from the producer's own allocator (independently of DoExchange).
    close(classic, exchange, server, allocator);
    FlightSqlExample.removeDerbyDatabaseIfExists(DB_NAME);
  }

  @BeforeEach
  public void resetCounters() {
    rpcs.reset();
  }

  @Test
  public void testQuery() throws Exception {
    final String query = "SELECT * FROM intTable WHERE id <= 3";

    final FlightInfo info = classic.execute(query);
    try (FlightStream stream = classic.getStream(info.getEndpoints().get(0).getTicket())) {
      assertThat(rows(stream)).isEqualTo(ALL_ROWS);
    }
    assertThat(rpcs.reset()).isEqualTo(calls(GET_FLIGHT_INFO, 1, DO_GET, 1));

    try (FlightStream stream = exchange.execute(query)) {
      assertThat(stream.getSchema()).isEqualTo(info.getSchemaOptional().orElseThrow());
      assertThat(rows(stream)).isEqualTo(ALL_ROWS);
    }
    assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 1));
  }

  @Test
  public void testQueryWithParameters() throws Exception {
    final String query = "SELECT * FROM intTable WHERE id = ?";

    // Classic Flight SQL needs a prepared statement to bind parameters.
    try (FlightSqlClient.PreparedStatement statement = classic.prepare(query);
        VectorSchemaRoot parameters = ints(1)) {
      statement.setParameters(parameters);
      final FlightInfo info = statement.execute();
      try (FlightStream stream = classic.getStream(info.getEndpoints().get(0).getTicket())) {
        assertThat(rows(stream)).isEqualTo(List.of(ALL_ROWS.get(0)));
      }
    }
    assertThat(rpcs.reset())
        .isEqualTo(calls(DO_ACTION, 2, DO_PUT, 1, GET_FLIGHT_INFO, 1, DO_GET, 1));

    // Over DoExchange the server prepares, binds, executes and closes the statement in one call.
    final int created = producer.created.get();
    try (VectorSchemaRoot parameters = ints(1);
        FlightStream stream = exchange.execute(query, parameters)) {
      assertThat(rows(stream)).isEqualTo(List.of(ALL_ROWS.get(0)));
    }
    assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 1));
    assertThat(producer.created.get()).isEqualTo(created + 1);
    assertThat(producer.openPreparedStatements()).isZero();
  }

  @Test
  public void testInsertUpdateDelete() throws Exception {
    final String key = "exchange-adhoc";
    assertThat(classic.executeUpdate("INSERT INTO intTable (keyName, value) VALUES ('c', 1)"))
        .isEqualTo(1);
    assertThat(classic.executeUpdate("DELETE FROM intTable WHERE keyName = 'c'")).isEqualTo(1);
    assertThat(rpcs.reset()).isEqualTo(calls(DO_PUT, 2));

    assertThat(
            exchange.executeUpdate(
                "INSERT INTO intTable (keyName, value) VALUES ('" + key + "', 10)"))
        .isEqualTo(1);
    assertThat(
            exchange.executeUpdate("UPDATE intTable SET value = 20 WHERE keyName = '" + key + "'"))
        .isEqualTo(1);
    try (FlightStream stream =
        exchange.execute("SELECT keyName, value FROM intTable WHERE keyName = '" + key + "'")) {
      assertThat(rows(stream)).isEqualTo(List.of(List.of(key, "20")));
    }
    assertThat(exchange.executeUpdate("DELETE FROM intTable WHERE keyName = '" + key + "'"))
        .isEqualTo(1);
    assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 4));
  }

  @Test
  public void testInsertUpdateDeleteWithParameters() throws Exception {
    final String insert = "INSERT INTO intTable (keyName, value) VALUES (?, ?)";

    try (FlightSqlClient.PreparedStatement statement = classic.prepare(insert);
        VectorSchemaRoot rows = keysAndValues("classic-", 3, 1)) {
      statement.setParameters(rows);
      assertThat(statement.executeUpdate()).isEqualTo(3);
    }
    assertThat(rpcs.reset()).isEqualTo(calls(DO_ACTION, 2, DO_PUT, 1));
    assertThat(classic.executeUpdate("DELETE FROM intTable WHERE keyName LIKE 'classic-%'"))
        .isEqualTo(3);
    rpcs.reset();

    // Each statement below is a single call, whatever the number of parameter sets.
    try (VectorSchemaRoot rows = keysAndValues("batch-", 10, 1)) {
      assertThat(exchange.executeUpdate(insert, rows)).isEqualTo(10);
    }
    try (VectorSchemaRoot rows = valuesAndKeys("batch-", 10, 100)) {
      assertThat(exchange.executeUpdate("UPDATE intTable SET value = ? WHERE keyName = ?", rows))
          .isEqualTo(10);
    }
    try (VectorSchemaRoot keys = keys("batch-", 0, 1, 2)) {
      assertThat(exchange.executeUpdate("DELETE FROM intTable WHERE keyName = ?", keys))
          .isEqualTo(3);
    }
    try (FlightStream stream =
        exchange.execute(
            "SELECT keyName, value FROM intTable WHERE keyName LIKE 'batch-%' ORDER BY value")) {
      final List<List<String>> expected = new ArrayList<>();
      for (int i = 3; i < 10; i++) {
        expected.add(List.of("batch-" + i, Integer.toString(i * 100)));
      }
      assertThat(rows(stream)).isEqualTo(expected);
    }
    assertThat(exchange.executeUpdate("DELETE FROM intTable WHERE keyName LIKE 'batch-%'"))
        .isEqualTo(7);
    assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 5));
    assertThat(producer.openPreparedStatements()).isZero();
  }

  @Test
  public void testReusablePreparedStatements() throws Exception {
    try (FlightSqlExchangeClient.PreparedStatement select =
        exchange.prepare("SELECT * FROM intTable WHERE id = ?")) {
      assertThat(select.getParameterSchema().getFields()).hasSize(1);
      assertThat(select.getResultSetSchema().getFields()).hasSize(4);
      assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 1));

      for (int id = 1; id <= 3; id++) {
        try (VectorSchemaRoot parameters = ints(id);
            FlightStream stream = select.execute(parameters)) {
          assertThat(rows(stream)).isEqualTo(List.of(ALL_ROWS.get(id - 1)));
        }
        // Classic Flight SQL: DoPut (bind) + GetFlightInfo + DoGet for every execution.
        assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 1));
      }
    }
    assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 1));

    try (FlightSqlExchangeClient.PreparedStatement insert =
        exchange.prepare("INSERT INTO intTable (keyName, value) VALUES (?, ?)")) {
      for (int batch = 0; batch < 3; batch++) {
        try (VectorSchemaRoot rows = keysAndValues("reuse-" + batch + "-", 5, 1)) {
          assertThat(insert.executeUpdate(rows)).isEqualTo(5);
        }
      }
    }
    assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 5));
    assertThat(exchange.executeUpdate("DELETE FROM intTable WHERE keyName LIKE 'reuse-%'"))
        .isEqualTo(15);
    assertThat(producer.openPreparedStatements()).isZero();
  }

  @Test
  public void testPreparedStatementWithoutParameters() throws Exception {
    try (FlightSqlExchangeClient.PreparedStatement select =
            exchange.prepare("SELECT * FROM intTable WHERE id <= 3");
        FlightStream stream = select.execute(null)) {
      assertThat(rows(stream)).isEqualTo(ALL_ROWS);
    }
    try (FlightSqlExchangeClient.PreparedStatement insert =
            exchange.prepare("INSERT INTO intTable (keyName, value) VALUES ('no-params', 1)");
        FlightSqlExchangeClient.PreparedStatement delete =
            exchange.prepare("DELETE FROM intTable WHERE keyName = 'no-params'")) {
      assertThat(insert.executeUpdate(null)).isEqualTo(1);
      assertThat(delete.executeUpdate(null)).isEqualTo(1);
    }
    // prepare + execute + close for the query, then 2 x (prepare + execute + close).
    assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 9));
  }

  @Test
  public void testCatalogMetadata() throws Exception {
    final FlightInfo info = classic.getTables(null, null, "INTTABLE", null, false);
    final List<List<String>> expected;
    try (FlightStream stream = classic.getStream(info.getEndpoints().get(0).getTicket())) {
      expected = rows(stream);
    }
    assertThat(rpcs.reset()).isEqualTo(calls(GET_FLIGHT_INFO, 1, DO_GET, 1));

    try (FlightStream stream =
        exchange.executeCommand(
            CommandGetTables.newBuilder().setTableNameFilterPattern("INTTABLE").build())) {
      assertThat(rows(stream)).isEqualTo(expected).hasSize(1);
    }
    assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 1));
  }

  @Test
  public void testBulkIngest() throws Exception {
    final ExecuteIngestOptions options =
        new ExecuteIngestOptions(
            "INTTABLE",
            TableDefinitionOptions.newBuilder()
                .setIfExists(TableExistsOption.TABLE_EXISTS_OPTION_APPEND)
                .setIfNotExist(TableNotExistOption.TABLE_NOT_EXIST_OPTION_FAIL)
                .build(),
            null,
            null,
            null);
    // Derby's import procedure needs upper case column names.
    final VarCharVector keys = new VarCharVector("KEYNAME", allocator);
    final IntVector values = new IntVector("VALUE", allocator);
    fill(keys, values, "ingest-", 4, 1);
    try (VectorSchemaRoot rows = VectorSchemaRoot.of(keys, values)) {
      // The example server does not report how many rows it ingested.
      assertThat(exchange.executeIngest(rows, options)).isEqualTo(-1);
    }
    assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 1));
    assertThat(exchange.executeUpdate("DELETE FROM intTable WHERE keyName LIKE 'ingest-%'"))
        .isEqualTo(4);
  }

  @Test
  public void testErrorsHaveTheSameStatusAsClassicFlightSql() throws Exception {
    final String badQuery = "SELECT * FROM no_such_table";
    final FlightRuntimeException classicQueryError =
        assertThrows(FlightRuntimeException.class, () -> classic.execute(badQuery));
    final FlightRuntimeException queryError =
        assertThrows(
            FlightRuntimeException.class,
            () -> {
              try (FlightStream stream = exchange.execute(badQuery)) {
                stream.next();
              }
            });
    assertThat(queryError.status().code()).isEqualTo(classicQueryError.status().code());

    final String badUpdate = "INSERT INTO no_such_table VALUES (1)";
    final FlightRuntimeException classicUpdateError =
        assertThrows(FlightRuntimeException.class, () -> classic.executeUpdate(badUpdate));
    final FlightRuntimeException updateError =
        assertThrows(FlightRuntimeException.class, () -> exchange.executeUpdate(badUpdate));
    assertThat(updateError.status().code()).isEqualTo(classicUpdateError.status().code());
    assertThat(updateError.status().code()).isEqualTo(FlightStatusCode.INVALID_ARGUMENT);
  }

  @Test
  public void testFailedStatementStillClosesItsPreparedStatement() throws Exception {
    final int created = producer.created.get();
    // 'not a number' cannot be stored in the INT column: execution fails on the server.
    final VarCharVector values = new VarCharVector("value", allocator);
    values.setSafe(0, "not a number".getBytes(StandardCharsets.UTF_8));
    values.setValueCount(1);
    final VarCharVector keys = new VarCharVector("keyName", allocator);
    keys.setSafe(0, "one".getBytes(StandardCharsets.UTF_8));
    keys.setValueCount(1);
    try (VectorSchemaRoot badRows = VectorSchemaRoot.of(values, keys)) {
      assertThrows(
          FlightRuntimeException.class,
          () -> exchange.executeUpdate("UPDATE intTable SET value = ? WHERE keyName = ?", badRows));
    }
    assertThat(producer.created.get()).isEqualTo(created + 1);
    assertThat(producer.openPreparedStatements()).isZero();
  }

  @Test
  public void testTransactionActionsAreRoutedToTheProducer() {
    // The example does not implement transactions: the producer's own error reaches the client.
    final FlightRuntimeException error =
        assertThrows(FlightRuntimeException.class, () -> exchange.beginTransaction());
    assertThat(error.status().code()).isEqualTo(FlightStatusCode.UNIMPLEMENTED);
    assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 1));
  }

  @Test
  public void testGenericActionOverExchange() {
    // Any action can travel as a Flight Action message, e.g. a Flight SQL one.
    final List<Result> created =
        exchange.doAction(
            new Action(
                "CreatePreparedStatement",
                Any.pack(
                        ActionCreatePreparedStatementRequest.newBuilder()
                            .setQuery("SELECT * FROM intTable WHERE id = ?")
                            .build())
                    .toByteArray()));
    assertThat(created).hasSize(1);
    final ByteString handle =
        FlightSqlUtils.unpackAndParseOrThrow(
                created.get(0).getBody(), ActionCreatePreparedStatementResult.class)
            .getPreparedStatementHandle();
    exchange.doAction(
        new Action(
            "ClosePreparedStatement",
            Any.pack(
                    ActionClosePreparedStatementRequest.newBuilder()
                        .setPreparedStatementHandle(handle)
                        .build())
                .toByteArray()));
    assertThat(rpcs.reset()).isEqualTo(calls(DO_EXCHANGE, 2));
    assertThat(producer.openPreparedStatements()).isZero();
  }

  @Test
  public void testOtherExchangesArePassedToTheProducer() throws Exception {
    try (FlightClient client =
            FlightClient.builder(allocator, Location.forGrpcInsecure("localhost", server.getPort()))
                .build();
        FlightClient.ExchangeReaderWriter call =
            client.doExchange(
                FlightDescriptor.command("not flight sql".getBytes(StandardCharsets.UTF_8)))) {
      call.getWriter().completed();
      final FlightRuntimeException error =
          assertThrows(FlightRuntimeException.class, () -> call.getReader().next());
      // FlightSqlExample#doExchange is not implemented.
      assertThat(error.status().code()).isEqualTo(FlightStatusCode.UNIMPLEMENTED);
    }
  }

  @Test
  public void testStatelessServer() throws Exception {
    final FlightSqlStatelessExample stateless =
        new FlightSqlStatelessExample(Location.forGrpcInsecure("localhost", 0), STATELESS_DB_NAME);
    try (FlightServer statelessServer =
            FlightServer.builder(
                    allocator,
                    Location.forGrpcInsecure("localhost", 0),
                    new FlightSqlExchangeProducer(stateless, allocator))
                .build()
                .start();
        FlightSqlExchangeClient client =
            new FlightSqlExchangeClient(
                FlightClient.builder(
                        allocator, Location.forGrpcInsecure("localhost", statelessServer.getPort()))
                    .build())) {
      // Binding returns a new handle that carries the parameters; the server executes with it.
      try (FlightSqlExchangeClient.PreparedStatement select =
              client.prepare("SELECT * FROM intTable WHERE id = ?");
          VectorSchemaRoot parameters = ints(2);
          FlightStream stream = select.execute(parameters)) {
        assertThat(rows(stream)).isEqualTo(List.of(ALL_ROWS.get(1)));
      }
      // The stateless example runs the query once per parameter set and starts a new stream
      // each time; the exchange still delivers them as one result set.
      try (VectorSchemaRoot parameters = ints(1, 3);
          FlightStream stream = client.execute("SELECT * FROM intTable WHERE id = ?", parameters)) {
        assertThat(rows(stream)).isEqualTo(List.of(ALL_ROWS.get(0), ALL_ROWS.get(2)));
      }
    } finally {
      FlightSqlStatelessExample.removeDerbyDatabaseIfExists(STATELESS_DB_NAME);
    }
  }

  @Test
  public void testResultSetsFromSeveralEndpoints() throws Exception {
    try (BufferAllocator producerAllocator =
            allocator.newChildAllocator("endpoints", 0, Long.MAX_VALUE);
        BufferAllocator otherTree = new RootAllocator()) {
      // Odd endpoints come from another allocator tree, whose buffers have to be copied.
      try (EndpointsProducer endpoints = new EndpointsProducer(producerAllocator, otherTree);
          FlightServer endpointsServer =
              FlightServer.builder(
                      allocator,
                      Location.forGrpcInsecure("localhost", 0),
                      new FlightSqlExchangeProducer(endpoints, allocator))
                  .build()
                  .start();
          FlightSqlExchangeClient client =
              new FlightSqlExchangeClient(
                  FlightClient.builder(
                          allocator,
                          Location.forGrpcInsecure("localhost", endpointsServer.getPort()))
                      .build())) {
        // Three endpoints are streamed one after the other as a single result set.
        try (FlightStream stream = client.execute("3")) {
          assertThat(rows(stream)).isEqualTo(List.of(List.of("0"), List.of("1"), List.of("2")));
        }
        // Without endpoints the client still gets the schema.
        try (FlightStream stream = client.execute("0")) {
          assertThat(stream.getSchema()).isEqualTo(EndpointsProducer.SCHEMA);
          assertThat(stream.next()).isFalse();
        }
        // Closing the result early cancels the call, which the producer can observe.
        try (FlightStream stream = client.execute("block")) {
          assertThat(stream.next()).isTrue();
        }
        assertThat(endpoints.cancelled.await(10, TimeUnit.SECONDS)).isTrue();
      }
      // Every batch the forwarder borrowed from the producer must have been released.
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (producerAllocator.getAllocatedMemory() + otherTree.getAllocatedMemory() > 0
          && System.nanoTime() < deadline) {
        TimeUnit.MILLISECONDS.sleep(10);
      }
      assertThat(producerAllocator.getAllocatedMemory()).isZero();
      assertThat(otherTree.getAllocatedMemory()).isZero();
    }
  }

  /** Reads all the rows of a result set, whatever the number of batches, as strings. */
  private static List<List<String>> rows(FlightStream stream) {
    final List<List<String>> rows = new ArrayList<>();
    while (stream.next()) {
      final VectorSchemaRoot root = stream.getRoot();
      for (int row = 0; row < root.getRowCount(); row++) {
        final List<String> values = new ArrayList<>();
        for (FieldVector vector : root.getFieldVectors()) {
          final Object value = vector.getObject(row);
          values.add(value == null ? null : value.toString());
        }
        rows.add(values);
      }
    }
    return rows;
  }

  private static Map<FlightMethod, Integer> calls(Object... methodsAndCounts) {
    final Map<FlightMethod, Integer> calls = new EnumMap<>(FlightMethod.class);
    for (int i = 0; i < methodsAndCounts.length; i += 2) {
      calls.put((FlightMethod) methodsAndCounts[i], (Integer) methodsAndCounts[i + 1]);
    }
    return calls;
  }

  private static VectorSchemaRoot ints(int... values) {
    final IntVector vector = new IntVector("id", allocator);
    vector.allocateNew(values.length);
    for (int i = 0; i < values.length; i++) {
      vector.set(i, values[i]);
    }
    vector.setValueCount(values.length);
    return VectorSchemaRoot.of(vector);
  }

  private static VectorSchemaRoot keys(String prefix, int... suffixes) {
    final VarCharVector keys = new VarCharVector("keyName", allocator);
    for (int i = 0; i < suffixes.length; i++) {
      keys.setSafe(i, (prefix + suffixes[i]).getBytes(StandardCharsets.UTF_8));
    }
    keys.setValueCount(suffixes.length);
    return VectorSchemaRoot.of(keys);
  }

  /** Rows of (keyName = prefix + i, value = i * multiplier). */
  private static VectorSchemaRoot keysAndValues(String prefix, int count, int multiplier) {
    final VarCharVector keys = new VarCharVector("keyName", allocator);
    final IntVector values = new IntVector("value", allocator);
    fill(keys, values, prefix, count, multiplier);
    return VectorSchemaRoot.of(keys, values);
  }

  /** Rows of (value = i * multiplier, keyName = prefix + i). */
  private static VectorSchemaRoot valuesAndKeys(String prefix, int count, int multiplier) {
    final VarCharVector keys = new VarCharVector("keyName", allocator);
    final IntVector values = new IntVector("value", allocator);
    fill(keys, values, prefix, count, multiplier);
    return VectorSchemaRoot.of(values, keys);
  }

  private static void fill(
      VarCharVector keys, IntVector values, String prefix, int count, int multiplier) {
    for (int i = 0; i < count; i++) {
      keys.setSafe(i, (prefix + i).getBytes(StandardCharsets.UTF_8));
      values.setSafe(i, i * multiplier);
    }
    keys.setValueCount(count);
    values.setValueCount(count);
  }

  /** Counts the calls the server receives, by RPC method. */
  static final class RpcCounter implements FlightServerMiddleware.Factory<FlightServerMiddleware> {
    private final Map<FlightMethod, AtomicInteger> calls = new ConcurrentHashMap<>();

    @Override
    public FlightServerMiddleware onCallStarted(
        CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
      calls.computeIfAbsent(info.method(), method -> new AtomicInteger()).incrementAndGet();
      return new FlightServerMiddleware() {
        @Override
        public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {}

        @Override
        public void onCallCompleted(CallStatus status) {}

        @Override
        public void onCallErrored(Throwable err) {}
      };
    }

    /** Returns the calls counted since the previous reset. */
    Map<FlightMethod, Integer> reset() {
      final Map<FlightMethod, Integer> snapshot = new EnumMap<>(FlightMethod.class);
      calls.forEach(
          (method, count) -> {
            final int value = count.getAndSet(0);
            if (value > 0) {
              snapshot.put(method, value);
            }
          });
      return snapshot;
    }
  }

  /** The example server, counting how many prepared statements it creates and closes. */
  static final class CountingExample extends FlightSqlExample {
    final AtomicInteger created = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();

    CountingExample(Location location, String dbName) {
      super(location, dbName);
    }

    int openPreparedStatements() {
      return created.get() - closed.get();
    }

    @Override
    public void createPreparedStatement(
        ActionCreatePreparedStatementRequest request,
        CallContext context,
        StreamListener<Result> listener) {
      created.incrementAndGet();
      super.createPreparedStatement(request, context, listener);
    }

    @Override
    public void closePreparedStatement(
        ActionClosePreparedStatementRequest request,
        CallContext context,
        StreamListener<Result> listener) {
      closed.incrementAndGet();
      super.closePreparedStatement(request, context, listener);
    }
  }

  /**
   * Answers the query {@code "n"} with n endpoints holding one row each, and the query {@code
   * "block"} with a stream that only ends once the client cancels it.
   */
  static final class EndpointsProducer extends NoOpFlightSqlProducer {
    static final Schema SCHEMA = new Schema(List.of(Field.nullable("n", MinorType.INT.getType())));
    final CountDownLatch cancelled = new CountDownLatch(1);
    private final BufferAllocator allocator;
    private final BufferAllocator oddEndpointAllocator;

    EndpointsProducer(BufferAllocator allocator, BufferAllocator oddEndpointAllocator) {
      this.allocator = allocator;
      this.oddEndpointAllocator = oddEndpointAllocator;
    }

    @Override
    public FlightInfo getFlightInfoStatement(
        CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
      final List<String> handles =
          command.getQuery().equals("block")
              ? List.of("block")
              : Arrays.asList(new String[Integer.parseInt(command.getQuery())]);
      final List<FlightEndpoint> endpoints = new ArrayList<>();
      for (int i = 0; i < handles.size(); i++) {
        final String handle = handles.get(i) == null ? Integer.toString(i) : handles.get(i);
        final TicketStatementQuery ticket =
            TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8(handle))
                .build();
        endpoints.add(new FlightEndpoint(new Ticket(Any.pack(ticket).toByteArray())));
      }
      return new FlightInfo(SCHEMA, descriptor, endpoints, -1, -1);
    }

    @Override
    public void getStreamStatement(
        TicketStatementQuery ticket, CallContext context, ServerStreamListener listener) {
      final String handle = ticket.getStatementHandle().toStringUtf8();
      final int value = handle.equals("block") ? 42 : Integer.parseInt(handle);
      try (VectorSchemaRoot root =
          VectorSchemaRoot.create(SCHEMA, value % 2 == 1 ? oddEndpointAllocator : allocator)) {
        final IntVector vector = (IntVector) root.getVector(0);
        vector.allocateNew(1);
        vector.set(0, value);
        root.setRowCount(1);
        listener.start(root);
        listener.putNext();
        if (handle.equals("block")) {
          final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
          try {
            while (!listener.isCancelled() && System.nanoTime() < deadline) {
              TimeUnit.MILLISECONDS.sleep(5);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          if (listener.isCancelled()) {
            cancelled.countDown();
          }
        }
      }
      listener.completed();
    }
  }
}
