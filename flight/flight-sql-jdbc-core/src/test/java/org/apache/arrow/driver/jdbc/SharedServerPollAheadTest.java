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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * End-to-end poll-ahead checks (BDX-645) against the standalone conformance server. The assertions
 * are on what the server observed: stream concurrency, the order of polls relative to held streams,
 * the gaps between polls, and which streams were cancelled.
 *
 * <p>Enable with {@code -Dpollinfo.shared.enabled=true -Dpollinfo.shared.flightPort=...
 * -Dpollinfo.shared.controlPort=...}. Connections use {@code threadPoolSize=4}: the reader pool
 * defaults to 1, and a held stream would starve the others.
 */
@EnabledIfSystemProperty(named = "pollinfo.shared.enabled", matches = "true")
public class SharedServerPollAheadTest {
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final int FLIGHT_PORT = Integer.getInteger("pollinfo.shared.flightPort", 32347);
  private static final int CONTROL_PORT = Integer.getInteger("pollinfo.shared.controlPort", 32348);
  private static final Duration WAIT = Duration.ofSeconds(10);
  // Negative assertions ("nothing happens") hold for this long while /state is polled.
  private static final Duration QUIET = Duration.ofMillis(400);
  private static final String CONTINUATION = "continuation";

  private ExecutorService background;

  @BeforeEach
  public void resetServer() throws Exception {
    background =
        Executors.newCachedThreadPool(
            runnable -> {
              final Thread thread = new Thread(runnable, "shared-server-poll-ahead");
              thread.setDaemon(true);
              return thread;
            });
    // The previous test closed its connections; wait until the server saw its streams end, because
    // /reset must not run with streams open.
    awaitUntil("no open streams before reset", () -> counter("do_get_active") == 0);
    post("/reset", "");
  }

  @AfterEach
  public void printServerState(final TestInfo testInfo) throws Exception {
    background.shutdownNow();
    System.out.println("SHARED_SERVER_STATE " + testInfo.getDisplayName());
    System.out.println(state());
  }

  @Test
  public void burstDeliversAllRowsWithinThePrefetchLimit() throws Exception {
    final List<Long> rows;
    try (Connection connection = connect(true, "pollInfoPrefetch=2");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("burst")) {
      rows = values(resultSet, "burst");
    }
    assertEquals(range(1, 8), sorted(rows));
    awaitUntil("streams ended", () -> counter("do_get_active") == 0);
    assertTrue(counter("do_get_max_concurrent") <= 2, state());
    assertEquals(4, counter("do_get"), state());
    assertEquals(List.of("1", "2"), continuationOrderSuffixes());
    assertEquals(2, continuationPolls().size());
    assertEquals(0, counter("get_flight_info"));
    assertEquals(0, counter("cancellation"));
  }

  @Test
  public void heldStreamsNeverExceedThePrefetchLimit() throws Exception {
    for (int endpoint = 1; endpoint <= 4; endpoint++) {
      post("/hold", "{\"endpoint\":" + endpoint + "}");
    }
    try (Connection connection = connect(true, "pollInfoPrefetch=2");
        Statement statement = connection.createStatement()) {
      final Future<ResultSet> execution = executeAsync(statement, "burst");
      awaitUntil("E1 held", () -> hasHold(1, "held"));
      awaitUntil("E2 held", () -> hasHold(2, "held"));
      // Four endpoints are known and three streams could be opened, but only two may be.
      awaitUntil("C1 polled", () -> continuationPolls().size() >= 1);
      assertQuiet(
          "a third stream opened at the limit",
          () -> counter("do_get") == 2 && counter("do_get_max_concurrent") == 2);
      for (int endpoint = 1; endpoint <= 4; endpoint++) {
        post("/release-endpoint", "{\"endpoint\":" + endpoint + "}");
      }
      try (ResultSet resultSet = result(execution)) {
        assertEquals(range(1, 8), sorted(values(resultSet, "burst")));
      }
    }
    awaitUntil("streams ended", () -> counter("do_get_active") == 0);
    assertEquals(2, counter("do_get_max_concurrent"), state());
    assertEquals(4, counter("do_get"), state());
  }

  @Test
  public void continuationIsPolledWhileTheFirstStreamIsStillHeld() throws Exception {
    post("/hold", "{\"endpoint\":1}");
    try (Connection connection = connect(true, "pollInfoPrefetch=2");
        Statement statement = connection.createStatement()) {
      final Future<ResultSet> execution = executeAsync(statement, "burst");
      awaitUntil("E1 held", () -> hasHold(1, "held"));
      awaitUntil("C1 polled", () -> continuationPolls().size() >= 1);
      assertTrue(
          events("DoGet.end").stream().noneMatch(event -> event.path("endpoint").asInt() == 1),
          "E1 must still be open: " + state());
      post("/release-endpoint", "{\"endpoint\":1}");
      try (ResultSet resultSet = result(execution)) {
        assertEquals(range(1, 8), sorted(values(resultSet, "burst")));
      }
    }
    final long firstContinuationPoll = continuationPolls().get(0).path("sequence").asLong();
    final JsonNode endOfFirstStream = single(events("DoGet.end"), 1);
    assertTrue(
        firstContinuationPoll < endOfFirstStream.path("sequence").asLong(),
        "C1 must be polled before E1 ends: " + state());
  }

  @Test
  public void trickleStopsPollingAtTheLimitAndResumesAfterTheStreamsAreReleased() throws Exception {
    post("/hold", "{\"endpoint\":1}");
    post("/hold", "{\"endpoint\":2}");
    final List<Long> rows;
    try (Connection connection = connect(true, "pollInfoPrefetch=2");
        Statement statement = connection.createStatement()) {
      final Future<ResultSet> execution = executeAsync(statement, "trickle");
      awaitUntil("E1 held", () -> hasHold(1, "held"));
      awaitUntil("E2 held", () -> hasHold(2, "held"));
      // E1 and E2 are queued and unread, so C2 must not be polled.
      assertQuiet(
          "polled past the limit",
          () -> continuationPolls().size() == 1 && counter("do_get_max_concurrent") == 2);
      post("/release-endpoint", "{\"endpoint\":1}");
      post("/release-endpoint", "{\"endpoint\":2}");
      try (ResultSet resultSet = result(execution)) {
        rows = values(resultSet, "trickle");
      }
    }
    assertEquals(range(1, 12), sorted(rows));
    assertEquals(List.of("1", "2", "3", "4", "5"), continuationOrderSuffixes());
    assertEquals(5, continuationPolls().size());
    final long lastRelease =
        events("control.release-endpoint").stream()
            .mapToLong(event -> event.path("sequence").asLong())
            .max()
            .orElseThrow();
    assertTrue(
        continuationPolls().get(1).path("sequence").asLong() > lastRelease,
        "C2 was polled before the held endpoints were released: " + state());
    assertTrue(counter("do_get_max_concurrent") <= 2, state());
  }

  @Test
  public void emptyAnswersBackOffExponentiallyUpToTheCap() throws Exception {
    final List<Long> rows;
    try (Connection connection = connect(true, "pollInfoMaxBackoffMs=300");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("empty-answers")) {
      rows = values(resultSet, "empty-answers");
    }
    assertEquals(range(1, 4), sorted(rows));
    final List<JsonNode> polls = continuationPolls();
    assertEquals(4, polls.size(), state());
    final long gap1 = gapMillis(polls.get(0), polls.get(1));
    final long gap2 = gapMillis(polls.get(1), polls.get(2));
    final long gap3 = gapMillis(polls.get(2), polls.get(3));
    System.out.printf("SHARED_BACKOFF_GAPS_MILLIS %d %d %d%n", gap1, gap2, gap3);
    // Lower bounds are the configured delays (with a little slack); the upper bounds are generous,
    // except that the third gap must show the cap: 400 ms uncapped, 300 ms capped.
    assertTrue(gap1 >= 90 && gap1 < 1000, "gap1=" + gap1);
    assertTrue(gap2 >= 180 && gap2 < 1000, "gap2=" + gap2);
    assertTrue(gap3 >= 270 && gap3 < 390, "gap3=" + gap3);
  }

  @Test
  public void lateFailureCancelsTheBufferedStreamsAndDeliversNoRows() throws Exception {
    post("/hold", "{\"endpoint\":1}");
    post("/hold", "{\"endpoint\":2}");
    try (Connection connection = connect(true, "");
        Statement statement = connection.createStatement()) {
      final Future<ResultSet> execution = executeAsync(statement, "late-error-buffered");
      awaitUntil("C2 polled", () -> continuationPolls().size() >= 2);
      // The failure cancels every buffered stream by itself, before anything is read or closed.
      awaitUntil("E1 cancelled", () -> hasEnd(1, "cancelled"));
      awaitUntil("streams ended", () -> counter("do_get_active") == 0);
      // E1 is held, so the first read may still be pending when the poller records the failure.
      // The failure then surfaces from executeQuery; otherwise from the first next(). Either way
      // it is the same exception and no row is delivered.
      final SQLException failure;
      final ResultSet resultSet = awaitResultOrFailure(execution);
      if (resultSet == null) {
        failure = executionFailure(execution);
      } else {
        try (ResultSet toClose = resultSet) {
          failure = assertThrows(SQLException.class, toClose::next);
          assertThrows(SQLException.class, toClose::next);
        }
      }
      assertEquals("Continuation PollFlightInfo failed", failureWithMessage(failure));
    }
    awaitUntil("streams ended", () -> counter("do_get_active") == 0);
    final List<JsonNode> ends = events("DoGet.end");
    assertTrue(
        ends.stream().noneMatch(event -> "completed".equals(event.path("outcome").asText())),
        "no stream may complete: " + state());
    // E2 was either cancelled or never started; it never sent a row.
    assertTrue(
        ends.stream()
            .filter(event -> event.path("endpoint").asInt() == 2)
            .allMatch(event -> "cancelled".equals(event.path("outcome").asText())),
        state());
    assertFalse(hasHold(2, "released"), state());
    assertEquals(2, continuationPolls().size(), state());
    // The server already ended the query, so the driver sends no CancelFlightInfo.
    assertEquals(0, counter("cancellation"), state());
  }

  @Test
  public void cancelWhileAPollIsOpenSendsNoMorePollsAndOneCancelFlightInfo() throws Exception {
    try (Connection connection = connect(true, "");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("cancel-observable")) {
      awaitUntil("C1 poll blocked on the server", () -> continuationPolls().size() >= 1);
      statement.cancel();
      final SQLException canceled = assertThrows(SQLException.class, resultSet::next);
      assertTrue(canceled.getMessage().contains("Statement canceled"), canceled.getMessage());
      awaitUntil("CancelFlightInfo", () -> counter("cancellation") >= 1);
    }
    assertEquals(1, counter("cancellation"), state());
    assertEquals(1, continuationPolls().size(), state());
    assertEquals(2, counter("poll_flight_info"), state());
  }

  @Test
  public void cancelWhileAStreamIsHeldCancelsItAndPollsNoMore() throws Exception {
    post("/hold", "{\"endpoint\":1}");
    try (Connection connection = connect(true, "pollInfoPrefetch=2");
        Statement statement = connection.createStatement()) {
      final Future<ResultSet> execution = executeAsync(statement, "burst");
      awaitUntil("E1 held", () -> hasHold(1, "held"));
      // E1..E4 are known and the poller is paused at the limit, so the poll count is settled.
      awaitUntil("C1 polled", () -> continuationPolls().size() >= 1);
      assertQuiet("polled past the limit", () -> continuationPolls().size() == 1);
      // E2 is readable while E1 is held, so execution has returned its result set.
      try (ResultSet resultSet = result(execution)) {
        statement.cancel();
        final SQLException canceled = assertThrows(SQLException.class, resultSet::next);
        assertTrue(canceled.getMessage().contains("Statement canceled"), canceled.getMessage());
        awaitUntil("E1 cancelled", () -> hasEnd(1, "cancelled"));
      }
      awaitUntil("streams ended", () -> counter("do_get_active") == 0);
    }
    assertEquals(2, counter("poll_flight_info"), state());
    assertTrue(counter("cancellation") <= 1, state());
    // The held stream was cancelled, never completed.
    assertFalse(hasEnd(1, "completed"), state());
  }

  @Test
  public void prefetchZeroKeepsThePocOrderOfPollsAndStreams() throws Exception {
    final List<Long> rows;
    try (Connection connection = connect(true, "pollInfoPrefetch=0");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("trickle")) {
      rows = values(resultSet, "trickle");
    }
    assertEquals(range(1, 12), rows);
    final List<JsonNode> polls = continuationPolls();
    assertEquals(5, polls.size(), state());
    final List<JsonNode> ends = events("DoGet.end");
    assertEquals(6, ends.size(), state());
    for (int k = 1; k <= 5; k++) {
      final long endOfPrevious = single(ends, k).path("sequence").asLong();
      assertTrue(
          polls.get(k - 1).path("sequence").asLong() > endOfPrevious,
          "C" + k + " was polled before E" + k + " ended: " + state());
    }
    assertEquals(1, counter("do_get_max_concurrent"), state());
  }

  @Test
  public void preparedStatementBindsOnceAndDeliversAllRows() throws Exception {
    final List<Long> rows;
    try (Connection connection = connect(true, "pollInfoPrefetch=2");
        PreparedStatement statement = connection.prepareStatement("burst")) {
      statement.setLong(1, 7);
      try (ResultSet resultSet = statement.executeQuery()) {
        rows = values(resultSet, "burst");
      }
    }
    assertEquals(range(1, 8), sorted(rows));
    assertEquals(1, counter("parameter_binding"), state());
    assertEquals(1, counter("original_descriptors"), state());
    assertEquals(2, counter("continuation_descriptors"), state());
    assertEquals(3, familyCounter("prepared", "poll_flight_info"), state());
    assertEquals(0, counter("get_flight_info"), state());
  }

  @Test
  public void optOutUsesGetFlightInfoOnly() throws Exception {
    final List<Long> rows;
    try (Connection connection = connect(false, "");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("burst")) {
      rows = values(resultSet, "burst");
    }
    assertEquals(range(1, 8), sorted(rows));
    assertEquals(1, counter("get_flight_info"), state());
    assertEquals(0, counter("poll_flight_info"), state());
    assertEquals(4, counter("do_get"), state());
    assertTrue(events("PollFlightInfo").isEmpty(), state());
  }

  private Future<ResultSet> executeAsync(final Statement statement, final String query) {
    return background.submit(() -> statement.executeQuery(query));
  }

  // Null if executeQuery failed; its exception is then available from executionFailure.
  private static ResultSet awaitResultOrFailure(final Future<ResultSet> execution)
      throws Exception {
    try {
      return result(execution);
    } catch (ExecutionException e) {
      return null;
    }
  }

  private static SQLException executionFailure(final Future<ResultSet> execution) throws Exception {
    final ExecutionException e = assertThrows(ExecutionException.class, execution::get);
    assertTrue(e.getCause() instanceof SQLException, e.toString());
    return (SQLException) e.getCause();
  }

  // The message of the first exception in the cause chain that is the driver's own failure.
  private static String failureWithMessage(final Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if ("Continuation PollFlightInfo failed".equals(current.getMessage())) {
        return current.getMessage();
      }
      current = current.getCause();
    }
    return failure.getMessage();
  }

  private static ResultSet result(final Future<ResultSet> execution) throws Exception {
    final ResultSet resultSet = execution.get(WAIT.toSeconds(), TimeUnit.SECONDS);
    assertNotNull(resultSet);
    return resultSet;
  }

  private static Connection connect(final boolean usePollInfo, final String extraProperties)
      throws Exception {
    return DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://127.0.0.1:%d/?useEncryption=false&usePollInfo=%s"
                + "&threadPoolSize=4%s",
            FLIGHT_PORT, usePollInfo, extraProperties.isEmpty() ? "" : "&" + extraProperties));
  }

  private static List<Long> values(final ResultSet resultSet, final String scenario)
      throws Exception {
    final List<Long> values = new ArrayList<>();
    while (resultSet.next()) {
      assertEquals(scenario, resultSet.getString("scenario"));
      values.add(resultSet.getLong("value"));
    }
    return values;
  }

  private static List<Long> range(final int firstInclusive, final int lastInclusive) {
    return IntStream.rangeClosed(firstInclusive, lastInclusive)
        .mapToObj(Long::valueOf)
        .collect(Collectors.toList());
  }

  private static List<Long> sorted(final List<Long> values) {
    final List<Long> copy = new ArrayList<>(values);
    Collections.sort(copy);
    return copy;
  }

  private static long gapMillis(final JsonNode earlier, final JsonNode later) {
    return TimeUnit.NANOSECONDS.toMillis(
        later.path("elapsed_ns").asLong() - earlier.path("elapsed_ns").asLong());
  }

  // Events, filtered by method; lifecycle events sit between requests, so never go by position.
  private static List<JsonNode> events(final String method) {
    final List<JsonNode> matching = new ArrayList<>();
    for (final JsonNode event : stateJson().path("events")) {
      if (method.equals(event.path("method").asText())) {
        matching.add(event);
      }
    }
    return matching;
  }

  private static List<JsonNode> continuationPolls() {
    return events("PollFlightInfo").stream()
        .filter(event -> CONTINUATION.equals(event.path("descriptor_kind").asText()))
        .collect(Collectors.toList());
  }

  private static List<String> continuationOrderSuffixes() {
    final List<String> suffixes = new ArrayList<>();
    for (final JsonNode descriptor : stateJson().path("continuation_order")) {
      final String text = descriptor.asText();
      suffixes.add(text.substring(text.lastIndexOf('/') + 1));
    }
    return suffixes;
  }

  private static boolean hasHold(final int endpoint, final String outcome) {
    return events("DoGet.hold").stream()
        .anyMatch(
            event ->
                event.path("endpoint").asInt() == endpoint
                    && outcome.equals(event.path("outcome").asText()));
  }

  private static boolean hasEnd(final int endpoint, final String outcome) {
    return events("DoGet.end").stream()
        .anyMatch(
            event ->
                event.path("endpoint").asInt() == endpoint
                    && outcome.equals(event.path("outcome").asText()));
  }

  private static JsonNode single(final List<JsonNode> events, final int endpoint) {
    final List<JsonNode> matching =
        events.stream()
            .filter(event -> event.path("endpoint").asInt() == endpoint)
            .collect(Collectors.toList());
    assertEquals(1, matching.size(), "endpoint " + endpoint + ": " + matching);
    return matching.get(0);
  }

  private static int counter(final String name) {
    return stateJson().path("counters").path(name).asInt();
  }

  private static int familyCounter(final String family, final String name) {
    return stateJson().path("counters_by_family").path(family).path(name).asInt();
  }

  private static void awaitUntil(final String description, final BooleanSupplier condition)
      throws Exception {
    final long deadline = System.nanoTime() + WAIT.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Timed out waiting for: " + description + "\n" + state());
      }
      Thread.sleep(10);
    }
  }

  // The condition must stay true for the whole quiet window.
  private static void assertQuiet(final String message, final BooleanSupplier condition)
      throws Exception {
    final long end = System.nanoTime() + QUIET.toNanos();
    do {
      assertTrue(condition.getAsBoolean(), message + "\n" + state());
      Thread.sleep(25);
    } while (System.nanoTime() < end);
  }

  private static JsonNode stateJson() {
    try {
      return JSON.readTree(state());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static String state() {
    return request("GET", "/state", "");
  }

  private static void post(final String path, final String body) {
    request("POST", path, body);
  }

  private static String request(final String method, final String path, final String body) {
    try {
      final HttpRequest.Builder builder =
          HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + CONTROL_PORT + path))
              .timeout(Duration.ofSeconds(2));
      if ("POST".equals(method)) {
        builder.header("Content-Type", "application/json");
        builder.POST(HttpRequest.BodyPublishers.ofString(body));
      } else {
        builder.GET();
      }
      final HttpResponse<String> response =
          HTTP_CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
      assertEquals("POST".equals(method) ? 204 : 200, response.statusCode(), response.body());
      return response.body();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
