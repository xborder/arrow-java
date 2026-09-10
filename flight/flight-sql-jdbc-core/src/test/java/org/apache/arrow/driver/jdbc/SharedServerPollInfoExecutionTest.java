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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/** Black-box BDX-645 contract checks against the standalone shared conformance server. */
@EnabledIfSystemProperty(named = "pollinfo.shared.enabled", matches = "true")
@TestMethodOrder(OrderAnnotation.class)
public class SharedServerPollInfoExecutionTest {
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static final int FLIGHT_PORT = Integer.getInteger("pollinfo.shared.flightPort", 32347);
  private static final int CONTROL_PORT = Integer.getInteger("pollinfo.shared.controlPort", 32348);

  @BeforeEach
  public void resetServer() throws Exception {
    post("/reset", "");
  }

  @AfterEach
  public void printServerState(final TestInfo testInfo) throws Exception {
    System.out.println("SHARED_SERVER_STATE " + testInfo.getDisplayName());
    System.out.println(state());
  }

  @Test
  @Order(1)
  public void t1Immediate() throws Exception {
    try (Connection connection = connect(true)) {
      assertEquals(Arrays.asList(1L, 2L), directValues(connection, "immediate"));
    }
    assertCounters(1, 0, 0, 1, 0, 0, 0, 1);
  }

  @Test
  @Order(2)
  public void t2MultiStepContinuationsAndFinalCumulativeInfo() throws Exception {
    try (Connection connection = connect(true)) {
      assertEquals(Arrays.asList(1L, 2L, 3L), directValues(connection, "multi-step"));
    }
    final String state = state();
    assertCounters(state, 3, 0, 0, 1, 2, 0, 0, 3);
    assertTrue(state.contains("bdx-645-poll/v1/"));
    assertTrue(state.indexOf("/1\"") < state.indexOf("/2\""), state);
  }

  @Test
  @Order(3)
  public void t3PreparedParametersBoundExactlyOnce() throws Exception {
    try (Connection connection = connect(true);
        PreparedStatement statement = connection.prepareStatement("prepared-multi-step")) {
      statement.setLong(1, 41);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertEquals(Arrays.asList(41L, 42L), values(resultSet));
      }
    }
    assertCounters(3, 0, 1, 1, 2, 0, 0, 1);
  }

  @Test
  @Order(4)
  public void t4MetadataMultiStep() throws Exception {
    try (Connection connection = connect(true);
        ResultSet resultSet = connection.getMetaData().getCatalogs()) {
      assertTrue(resultSet.next());
      assertEquals("bdx_catalog", resultSet.getString("TABLE_CAT"));
      assertFalse(resultSet.next());
    }
    assertCounters(3, 0, 0, 1, 2, 0, 0, 1);
  }

  @Test
  @Order(5)
  public void t5InitialUnimplementedFallbackCacheIsFamilyIsolated() throws Exception {
    try (Connection connection = connect(true)) {
      assertEquals(Arrays.asList(1L, 2L), directValues(connection, "unimplemented"));
      assertEquals(Arrays.asList(1L, 2L), directValues(connection, "unimplemented"));
      try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
        assertTrue(resultSet.next());
        assertEquals("bdx_catalog", resultSet.getString("TABLE_CAT"));
      }
    }
    final String state = state();
    assertCounters(state, 4, 2, 0, 2, 2, 0, 0, 3);
    assertTrue(state.contains("\"direct\""), state);
    assertTrue(state.contains("\"metadata\""), state);
  }

  @Test
  @Order(6)
  public void t6ConnectionOptOut() throws Exception {
    try (Connection connection = connect(false)) {
      assertEquals(Arrays.asList(1L, 2L), directValues(connection, "immediate"));
    }
    final String state = state();
    assertCounters(state, 0, 1, 0, 0, 0, 0, 0, 1);
    assertTrue(
        state
            .replaceAll("\\s+", "")
            .contains("\"prepared\":{\"poll_flight_info\":0,\"get_flight_info\":1"),
        state);
  }

  @Test
  @Order(7)
  public void t7UnavailableDoesNotFallbackOrReexecute() throws Exception {
    try (Connection connection = connect(true);
        Statement statement = connection.createStatement()) {
      final Throwable failure =
          assertThrows(Throwable.class, () -> statement.executeQuery("unavailable"));
      assertTrue(hasFlightStatus(failure, FlightStatusCode.UNAVAILABLE), failure.toString());
    }
    assertCounters(1, 0, 0, 1, 0, 0, 0, 0);
  }

  @Test
  @Order(8)
  public void t8TimeoutEndsBlockedPollAtTheSingleStatementDeadline() throws Exception {
    final long startNanos = System.nanoTime();
    try (Connection connection = connect(true);
        Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(1);
      final Throwable failure =
          assertThrows(Throwable.class, () -> statement.executeQuery("blocked-poll"));
      assertTrue(hasFlightStatus(failure, FlightStatusCode.TIMED_OUT), failure.toString());
      assertJdbcTimeoutContract(failure, 1);
    }
    final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    System.out.println("SHARED_T8_ELAPSED_MILLIS " + elapsedMillis);
    assertTrue(elapsedMillis >= 800, "deadline fired too early: " + elapsedMillis);
    assertTrue(elapsedMillis < 1800, "deadline fired too late: " + elapsedMillis);
    assertCounters(1, 0, 0, 1, 0, 0, 1, 0);
  }

  @Test
  @Order(9)
  public void t9CancelBeforeResultSetCancelsPollAndFlightInfo() throws Exception {
    try (Connection connection = connect(true);
        Statement statement = connection.createStatement()) {
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicBoolean resultSetCreated = new AtomicBoolean();
      final Thread execution =
          new Thread(
              () -> {
                try (ResultSet ignored = statement.executeQuery("cancel-observable")) {
                  resultSetCreated.set(true);
                } catch (Throwable e) {
                  failure.set(e);
                }
              },
              "shared-server-jdbc-cancel");
      execution.start();
      awaitCounter("poll_flight_info", 2, Duration.ofSeconds(3));
      assertNull(statement.getResultSet());
      final long cancelStart = System.nanoTime();
      statement.cancel();
      execution.join(TimeUnit.SECONDS.toMillis(2));
      final long cancelMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - cancelStart);
      System.out.println("SHARED_T9_CANCEL_MILLIS " + cancelMillis);
      assertFalse(execution.isAlive());
      assertFalse(resultSetCreated.get());
      assertNotNull(failure.get());
      assertTrue(
          hasFlightStatus(failure.get(), FlightStatusCode.CANCELLED), failure.get().toString());
      assertTrue(cancelMillis < 1000, "cancellation was not prompt: " + cancelMillis);
    }
    awaitCounter("cancellation", 1, Duration.ofSeconds(2));
    assertCounters(2, 0, 0, 1, 1, 1, 1, 0);
  }

  private static Connection connect(final boolean usePollInfo) throws Exception {
    return DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://127.0.0.1:%d/?useEncryption=false&usePollInfo=%s",
            FLIGHT_PORT, usePollInfo));
  }

  private static List<Long> directValues(final Connection connection, final String query)
      throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {
      final List<Long> values = new ArrayList<>();
      while (resultSet.next()) {
        assertEquals(query, resultSet.getString("scenario"));
        values.add(resultSet.getLong("value"));
      }
      return values;
    }
  }

  private static List<Long> values(final ResultSet resultSet) throws Exception {
    final List<Long> values = new ArrayList<>();
    while (resultSet.next()) {
      values.add(resultSet.getLong("value"));
    }
    return values;
  }

  private static void assertCounters(
      final int poll,
      final int get,
      final int bind,
      final int original,
      final int continuation,
      final int cancellation,
      final int activeTermination,
      final int doGet)
      throws Exception {
    assertCounters(
        state(), poll, get, bind, original, continuation, cancellation, activeTermination, doGet);
  }

  private static void assertCounters(
      final String state,
      final int poll,
      final int get,
      final int bind,
      final int original,
      final int continuation,
      final int cancellation,
      final int activeTermination,
      final int doGet) {
    assertEquals(poll, counter(state, "poll_flight_info"), state);
    assertEquals(get, counter(state, "get_flight_info"), state);
    assertEquals(bind, counter(state, "parameter_binding"), state);
    assertEquals(original, counter(state, "original_descriptors"), state);
    assertEquals(continuation, counter(state, "continuation_descriptors"), state);
    assertEquals(cancellation, counter(state, "cancellation"), state);
    assertEquals(activeTermination, counter(state, "active_call_terminations"), state);
    assertEquals(doGet, counter(state, "do_get"), state);
  }

  private static void awaitCounter(final String name, final int expected, final Duration timeout)
      throws Exception {
    final long deadline = System.nanoTime() + timeout.toNanos();
    do {
      if (counter(state(), name) >= expected) {
        return;
      }
      Thread.sleep(20);
    } while (System.nanoTime() < deadline);
    assertEquals(expected, counter(state(), name), state());
  }

  private static int counter(final String state, final String name) {
    final Matcher matcher =
        Pattern.compile("\\\"" + Pattern.quote(name) + "\\\"\\s*:\\s*(\\d+)").matcher(state);
    assertTrue(matcher.find(), state);
    return Integer.parseInt(matcher.group(1));
  }

  private static String state() throws Exception {
    return request("GET", "/state", "");
  }

  private static void post(final String path, final String body) throws Exception {
    request("POST", path, body);
  }

  private static String request(final String method, final String path, final String body)
      throws Exception {
    final HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + CONTROL_PORT + path))
            .timeout(Duration.ofSeconds(2));
    if ("POST".equals(method)) {
      builder.POST(HttpRequest.BodyPublishers.ofString(body));
    } else {
      builder.GET();
    }
    final HttpResponse<String> response =
        HTTP_CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    if ("POST".equals(method)) {
      assertEquals(204, response.statusCode(), response.body());
    } else {
      assertEquals(200, response.statusCode(), response.body());
    }
    return response.body();
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
}
