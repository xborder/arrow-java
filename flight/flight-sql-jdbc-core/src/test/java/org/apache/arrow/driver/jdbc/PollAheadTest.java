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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.driver.jdbc.utils.PollingMockFlightSqlProducer;
import org.apache.arrow.driver.jdbc.utils.ScriptedPollQuery;
import org.apache.arrow.driver.jdbc.utils.ScriptedPollQuery.Answer;
import org.apache.arrow.driver.jdbc.utils.ScriptedPollQuery.EventType;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Poll-ahead behavior of progressive PollFlightInfo results, against a scripted in-process server.
 *
 * <p>Streams are held on the server with {@link ScriptedPollQuery#holdEnd} so that "open and
 * unread" is something the test decides, not a race. Rows are {@code id * 100 + row}. Waits for
 * something that must happen use {@link #WAIT_MS}; waits for something that must not happen use the
 * short {@link #QUIET_MS} window (they cost that long, and can only fail by letting a bug show).
 */
@Timeout(60)
public class PollAheadTest {
  private static final long WAIT_MS = 10_000;
  private static final long QUIET_MS = 300;

  private static final AtomicInteger QUERY_NUMBER = new AtomicInteger();

  private static BufferAllocator allocator;
  private static PollingMockFlightSqlProducer producer;
  private static FlightServer server;

  @BeforeAll
  public static void startServer() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
    producer = new PollingMockFlightSqlProducer();
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

  // 1. Poll-ahead: the poll for C1 arrives before E1's stream completes.
  @Test
  public void pollsTheContinuationWhileTheFirstStreamIsStillOpen() throws Exception {
    // Both streams are held after their batch, so no stream completes before C1 is polled. Which
    // endpoint's row comes first is up to the queue, so the test does not depend on it.
    final ScriptedPollQuery script = scripted(Answer.endpoints(1), Answer.done(1)).holdAllEnds(2);
    final List<Integer> rows = new ArrayList<>();
    try (Connection connection = connect("pollInfoPrefetch=2");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(resultSet.next());
      rows.add(resultSet.getInt(1));
      assertTrue(script.awaitPollCount(2, WAIT_MS), "C1 was not polled while a stream is open");
      assertEquals(0, script.count(EventType.DOGET_END));
      script.releaseEnd(rows.get(0) / 100);
      rows.addAll(readReleasing(resultSet, script));
    }
    assertEquals(List.of(0, 100), sorted(rows));
    assertEquals(List.of(0, 1), script.pollIndices());
    assertEquals(0, script.cancelFlightInfoCount());
  }

  // 2. Deferred open: one answer adds 4 endpoints, prefetch 2, never more than 2 streams open.
  @Test
  public void deferredOpenKeepsAtMostPrefetchStreamsOpen() throws Exception {
    final ScriptedPollQuery script = scripted(Answer.endpoints(4), Answer.done(0)).holdAllEnds(4);
    final List<Integer> rows;
    try (Connection connection = connect("pollInfoPrefetch=2");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(script.awaitDoGetStarted(2, WAIT_MS));
      assertFalse(script.awaitDoGetStarted(3, QUIET_MS), "a third stream opened at the limit");
      rows = readReleasing(resultSet, script);
    }
    assertEquals(List.of(0, 100, 200, 300), sorted(rows));
    assertEquals(4, script.doGetStarted());
    assertEquals(2, script.maxOpenStreams());
  }

  // 3. Backpressure: polling stops at the limit and resumes after reads.
  @Test
  public void pollingStopsAtTheLimitAndResumesAfterReads() throws Exception {
    // E0 has data and is held open; E1 is open but sends nothing; E2 and E3 stream freely.
    final ScriptedPollQuery script =
        scripted(
                Answer.endpoints(1),
                Answer.endpoints(1),
                Answer.endpoints(1),
                Answer.endpoints(1),
                Answer.done(0))
            .holdEnd(0)
            .holdStart(1);
    final List<Integer> rows = new ArrayList<>();
    try (Connection connection = connect("pollInfoPrefetch=2");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      // The initial answer and C1 fill the limit (E0 and E1 unread); C2 must wait.
      assertTrue(script.awaitPollCount(2, WAIT_MS));
      assertFalse(script.awaitPollCount(3, QUIET_MS), "polled past the limit");

      assertTrue(resultSet.next());
      assertEquals(0, resultSet.getInt(1));
      rows.add(0);
      script.releaseEnd(0);
      // E0 finished and E1 has no data yet, so this row comes from E2 (published by C2).
      assertTrue(resultSet.next());
      assertEquals(200, resultSet.getInt(1));
      rows.add(200);

      // Finishing E0 freed a slot: the poller resumed and polled C2 once...
      assertTrue(script.awaitPollCount(3, WAIT_MS), "poller did not resume after a read");
      // ...and the limit (E1 and E2 unread) stops it again.
      assertFalse(script.awaitPollCount(4, QUIET_MS), "polled past the limit after resuming");

      script.releaseStart(1);
      rows.addAll(readAll(resultSet));
    }
    assertEquals(List.of(0, 100, 200, 300), sorted(rows));
    assertEquals(List.of(0, 1, 2, 3, 4), script.pollIndices());
    assertEquals(2, script.maxOpenStreams());
  }

  // 4a. Backoff doubles from 100 ms and resets after an answer that adds endpoints.
  @Test
  public void backoffDoublesAndResetsAfterAnAnswerThatAddsEndpoints() throws Exception {
    final ScriptedPollQuery script =
        scripted(
            Answer.endpoints(1),
            Answer.empty(),
            Answer.empty(),
            Answer.endpoints(1),
            Answer.empty(),
            Answer.done(0));
    try (Connection connection = connect("pollInfoPrefetch=10");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertEquals(List.of(0, 100), sorted(readAll(resultSet)));
    }
    final List<Long> polls = script.pollNanos();
    assertEquals(6, polls.size());
    // Answers 1 and 2 are empty: waits of 100 ms, then 200 ms.
    assertGapBetween(polls, 1, 2, 100, 1500);
    assertGapBetween(polls, 2, 3, 200, 1500);
    // Answer 3 adds an endpoint: no wait is required, and the next empty answer waits 100 ms
    // again, not 400 ms.
    assertGapBetween(polls, 4, 5, 100, 350);
  }

  // 4b. The wait is capped by pollInfoMaxBackoffMs.
  @Test
  public void backoffIsCappedByPollInfoMaxBackoffMs() throws Exception {
    final ScriptedPollQuery script =
        scripted(
            Answer.endpoints(1), Answer.empty(), Answer.empty(), Answer.empty(), Answer.done(1));
    try (Connection connection = connect("pollInfoPrefetch=10&pollInfoMaxBackoffMs=150");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertEquals(List.of(0, 100), sorted(readAll(resultSet)));
    }
    final List<Long> polls = script.pollNanos();
    assertEquals(5, polls.size());
    // Waits after the three empty answers: 100 ms, then capped at 150 ms (not 200, not 400).
    assertGapBetween(polls, 1, 2, 100, 1500);
    assertGapBetween(polls, 2, 3, 150, 1500);
    assertGapBetween(polls, 3, 4, 150, 350);
  }

  // 5a. Fail fast: a continuation error while streams are held. next() throws, nothing more is
  // read, and the buffered streams are cancelled.
  @Test
  public void continuationFailureStopsReadsAndClosesStreams() throws Exception {
    final ScriptedPollQuery script =
        scripted(Answer.endpoints(1), Answer.endpoints(1), Answer.fail())
            .rowsPerEndpoint(2)
            .holdEnd(0)
            .holdStart(1);
    try (Connection connection = connect("pollInfoPrefetch=3");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(resultSet.next());
      assertEquals(0, resultSet.getInt(1));
      // fail() records the failure first and then cancels every open stream.
      assertTrue(script.awaitStream(EventType.DOGET_CANCELLED, 0, WAIT_MS));
      assertTrue(script.awaitStream(EventType.DOGET_CANCELLED, 1, WAIT_MS));

      // The current batch still has a row, but the failure wins, every time.
      final SQLException failure = assertThrows(SQLException.class, resultSet::next);
      assertEquals(SQLException.class, failure.getClass());
      assertEquals("Continuation PollFlightInfo failed", failure.getMessage());
      assertInstanceOf(FlightRuntimeException.class, failure.getCause());
      assertEquals(
          FlightStatusCode.INTERNAL, ((FlightRuntimeException) failure.getCause()).status().code());
      assertEquals(
          "Continuation PollFlightInfo failed",
          assertThrows(SQLException.class, resultSet::next).getMessage());
    }
    // The second endpoint never sent a row: its stream was held and then cancelled.
    assertFalse(script.hasEvent(EventType.DOGET_END, 1));
    assertEquals(0, script.cancelFlightInfoCount());
  }

  // 5b. The failure arrives while the reader is already blocked on a stream.
  @Test
  public void continuationFailureWakesAReaderBlockedOnAStream() throws Exception {
    final CountDownLatch failNow = new CountDownLatch(1);
    final ScriptedPollQuery script =
        scripted(Answer.endpoints(1), Answer.fail().gatedBy(failNow)).holdEnd(0);
    try (Connection connection = connect("pollInfoPrefetch=2");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(resultSet.next());
      assertEquals(0, resultSet.getInt(1));
      try (AsyncReader reader = new AsyncReader(resultSet::next)) {
        assertTrue(script.awaitBlockedPoll(WAIT_MS));
        reader.awaitBlocked();
        failNow.countDown();
        final SQLException failure = reader.failure();
        assertEquals(SQLException.class, failure.getClass());
        assertEquals("Continuation PollFlightInfo failed", failure.getMessage());
      }
    }
  }

  // 6. Cancel with a poll open: no further polls, one CancelFlightInfo, next() throws.
  @Test
  public void cancelEndsTheOpenPollAndNextThrows() throws Exception {
    final ScriptedPollQuery script =
        scripted(Answer.endpoints(1), Answer.hang()).rowsPerEndpoint(2);
    try (Connection connection = connect("pollInfoPrefetch=2");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(resultSet.next());
      assertTrue(script.awaitBlockedPoll(WAIT_MS));
      statement.cancel();
      assertTrue(script.awaitPollTerminated(WAIT_MS));
      assertTrue(script.awaitCancelFlightInfo(1, WAIT_MS));

      // The current batch still has a row, but next() fails right after cancel().
      final SQLException canceled = assertThrows(SQLException.class, resultSet::next);
      assertTrue(canceled.getMessage().contains("Statement canceled"), canceled.getMessage());
      assertFalse(script.awaitCancelFlightInfo(2, QUIET_MS));
      assertFalse(script.awaitPollCount(3, 0));
    }
    assertEquals(2, script.pollCount());
    assertEquals(1, script.cancelFlightInfoCount());
  }

  // 7. Closing an unfinished result stops the poller and sends one CancelFlightInfo.
  @Test
  public void closingAnUnfinishedResultStopsThePollerAndCancelsOnce() throws Exception {
    final ScriptedPollQuery script = scripted(Answer.endpoints(1), Answer.hang());
    try (Connection connection = connect("pollInfoPrefetch=2")) {
      try (Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery(script.query())) {
        assertTrue(resultSet.next());
        assertTrue(script.awaitBlockedPoll(WAIT_MS));
      }
      assertTrue(script.awaitPollTerminated(WAIT_MS));
      assertTrue(script.awaitCancelFlightInfo(1, WAIT_MS));
      // Closing the statement and the connection must not cancel again.
    }
    assertFalse(script.awaitCancelFlightInfo(2, QUIET_MS));
    assertEquals(2, script.pollCount());
    assertEquals(1, script.cancelFlightInfoCount());
  }

  // 8. No premature end: the final endpoints arrive in the done answer.
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void finalEndpointsInTheDoneAnswerAreAllDelivered(final boolean readerWaits)
      throws Exception {
    final CountDownLatch release = new CountDownLatch(1);
    final Answer done = readerWaits ? Answer.done(2).gatedBy(release) : Answer.done(2);
    final ScriptedPollQuery script = scripted(Answer.endpoints(1), done);
    try (Connection connection = connect("pollInfoPrefetch=2");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(resultSet.next());
      final int firstRow = resultSet.getInt(1);
      try (AsyncReader reader = new AsyncReader(() -> readAll(resultSet))) {
        if (readerWaits) {
          // The reader has consumed E0 and now waits for the done answer.
          assertTrue(script.awaitBlockedPoll(WAIT_MS));
          assertTrue(script.awaitStream(EventType.DOGET_END, 0, WAIT_MS));
          reader.awaitBlocked();
          release.countDown();
        }
        final List<Integer> rows = new ArrayList<>(reader.rows());
        rows.add(firstRow);
        assertEquals(List.of(0, 100, 200), sorted(rows));
      }
    }
    assertEquals(List.of(0, 1), script.pollIndices());
  }

  // 9. Continuations are sent once each and in order.
  @Test
  public void continuationsAreSentOnceEachAndInOrder() throws Exception {
    final ScriptedPollQuery script =
        scripted(
            Answer.endpoints(1),
            Answer.empty(),
            Answer.endpoints(1),
            Answer.empty(),
            Answer.done(1));
    try (Connection connection = connect("pollInfoPrefetch=2&pollInfoMaxBackoffMs=20");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertEquals(List.of(0, 100, 200), sorted(readAll(resultSet)));
    }
    assertEquals(List.of(0, 1, 2, 3, 4), script.pollIndices());
    assertEquals(1, script.maxInFlightPolls());
  }

  // 10. pollInfoPrefetch=0 keeps the exact POC order: poll, DoGet, poll, DoGet.
  @Test
  public void prefetchZeroKeepsThePocRequestOrder() throws Exception {
    final ScriptedPollQuery script =
        scripted(Answer.endpoints(1), Answer.endpoints(1), Answer.done(1)).holdEnd(0);
    try (Connection connection = connect("pollInfoPrefetch=0");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(resultSet.next());
      assertEquals(0, resultSet.getInt(1));
      // The reader polls only when its queue is empty, so C1 waits for E0 to finish.
      assertFalse(script.awaitPollCount(2, QUIET_MS), "polled ahead with pollInfoPrefetch=0");
      script.releaseEnd(0);
      assertEquals(List.of(100, 200), readAll(resultSet));
    }
    assertEquals(
        List.of("POLL:0", "DOGET_START:0", "POLL:1", "DOGET_START:1", "POLL:2", "DOGET_START:2"),
        script.eventNames(EventType.POLL, EventType.DOGET_START));
  }

  // Decision: answers with no continuation, and pollInfoPrefetch=0, open every endpoint at once.
  @Test
  public void completeFirstAnswerOpensEveryEndpointEagerly() throws Exception {
    final ScriptedPollQuery script = scripted(Answer.done(4)).holdAllEnds(4);
    final List<Integer> rows;
    try (Connection connection = connect("pollInfoPrefetch=2");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(script.awaitDoGetStarted(4, WAIT_MS), "not every endpoint opened eagerly");
      rows = readReleasing(resultSet, script);
    }
    assertEquals(List.of(0, 100, 200, 300), sorted(rows));
    assertEquals(4, script.maxOpenStreams());
  }

  @Test
  public void prefetchZeroOpensEveryEndpointEagerly() throws Exception {
    final ScriptedPollQuery script = scripted(Answer.endpoints(3), Answer.done(1)).holdAllEnds(4);
    final List<Integer> rows;
    try (Connection connection = connect("pollInfoPrefetch=0");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(script.awaitDoGetStarted(3, WAIT_MS), "not every endpoint opened eagerly");
      rows = readReleasing(resultSet, script);
    }
    assertEquals(List.of(0, 100, 200, 300), sorted(rows));
  }

  // 11. Poller pool busy: the second result falls back to polling in next().
  @Test
  public void busyPollerPoolMakesTheNextResultPollInNext() throws Exception {
    final CountDownLatch releaseA = new CountDownLatch(1);
    final ScriptedPollQuery scriptA =
        scripted(Answer.endpoints(1), Answer.endpoints(1).gatedBy(releaseA), Answer.done(1));
    final ScriptedPollQuery scriptB = scripted(Answer.endpoints(1), Answer.done(1)).holdEnd(0);
    try (Connection connection = connect("pollInfoPollerThreads=1&pollInfoPrefetch=2");
        Statement statementA = connection.createStatement();
        Statement statementB = connection.createStatement()) {
      try (ResultSet resultSetA = statementA.executeQuery(scriptA.query())) {
        // A's poller holds the only pool thread, blocked inside C1.
        assertTrue(scriptA.awaitBlockedPoll(WAIT_MS));

        try (ResultSet resultSetB = statementB.executeQuery(scriptB.query())) {
          assertTrue(resultSetB.next());
          assertEquals(0, resultSetB.getInt(1));
          // B has no poller: it polls inside next(), after E0 finished.
          assertFalse(scriptB.awaitPollCount(2, QUIET_MS), "B polled ahead without a poller");
          scriptB.releaseEnd(0);
          assertEquals(List.of(100), readAll(resultSetB));
        }
        assertEquals(List.of(0, 1), scriptB.pollIndices());
        assertEquals(2, scriptA.pollCount());

        releaseA.countDown();
        assertEquals(List.of(0, 100, 200), sorted(readAll(resultSetA)));
      }
    }
  }

  // 13. Orphaned poller: a resume finds the pool busy; the reader runs the poller inline.
  @Test
  public void anOrphanedPollerIsRunInlineByTheReader() throws Exception {
    final CountDownLatch releaseB = new CountDownLatch(1);
    final ScriptedPollQuery scriptA =
        scripted(Answer.endpoints(1), Answer.endpoints(1), Answer.done(1));
    final ScriptedPollQuery scriptB =
        scripted(Answer.endpoints(1), Answer.endpoints(1).gatedBy(releaseB), Answer.done(1));
    try (Connection connection = connect("pollInfoPollerThreads=1&pollInfoPrefetch=2");
        Statement statementA = connection.createStatement();
        Statement statementB = connection.createStatement();
        ResultSet resultSetA = statementA.executeQuery(scriptA.query())) {
      // A's poller polls C1, reaches the limit (E0 and E1 unread), pauses and frees its thread.
      assertTrue(scriptA.awaitPollCount(2, WAIT_MS));
      awaitPollerPoolIdle(connection);

      // B's poller takes the only pool thread and blocks inside its C1.
      try (ResultSet resultSetB = statementB.executeQuery(scriptB.query())) {
        assertTrue(scriptB.awaitBlockedPoll(WAIT_MS));

        // Reading A frees a slot, but the resume is rejected: the pool is busy with B. The reader
        // finishes A by polling C2 itself, while B is still blocked.
        final List<Integer> rowsA = readAllWithin(resultSetA);
        assertEquals(List.of(0, 100, 200), sorted(rowsA));
        assertEquals(List.of(0, 1, 2), scriptA.pollIndices());
        assertEquals(2, scriptB.pollCount());

        releaseB.countDown();
        assertEquals(List.of(0, 100, 200), sorted(readAll(resultSetB)));
      }
    }
  }

  // 14. A continuation that never answers ends at a read boundary with the JDBC timeout message.
  @ParameterizedTest
  @ValueSource(strings = {"pollInfoPrefetch=0", "pollInfoPrefetch=2"})
  public void queryTimeoutEndsAtAReadBoundary(final String property) throws Exception {
    final ScriptedPollQuery script = scripted(Answer.endpoints(1), Answer.hang());
    final long startNanos = System.nanoTime();
    try (Connection connection = connect(property);
        Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(1);
      try (ResultSet resultSet = statement.executeQuery(script.query())) {
        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getInt(1));
        final SQLTimeoutException timeout =
            assertThrows(SQLTimeoutException.class, resultSet::next);
        assertEquals("Query timed out after 1 SECONDS", timeout.getMessage());
        assertInstanceOf(FlightRuntimeException.class, timeout.getCause());
        assertEquals(
            FlightStatusCode.TIMED_OUT,
            ((FlightRuntimeException) timeout.getCause()).status().code());
      }
    }
    final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    assertTrue(elapsedMillis >= 850, "the deadline fired too early: " + elapsedMillis);
    assertTrue(script.awaitPollTerminated(WAIT_MS));
    assertEquals(2, script.pollCount());
  }

  // Decision: a schema change fails the queue as a plain SQLException.
  @Test
  public void schemaChangeFailsTheResultAsAPlainSqlException() throws Exception {
    final ScriptedPollQuery script =
        scripted(Answer.endpoints(1), Answer.schemaChange()).rowsPerEndpoint(2);
    try (Connection connection = connect("pollInfoPrefetch=2");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(resultSet.next());
      assertTrue(script.awaitPollCount(2, WAIT_MS));
      final SQLException failure = assertThrows(SQLException.class, () -> readAllWithin(resultSet));
      assertEquals(SQLException.class, failure.getClass());
      assertEquals("PollInfo changed the result schema", failure.getMessage());
    }
  }

  // F1. A finished endpoint frees its slot when the application finishes it, not when a reader
  // task later consumes the end of the stream.
  @Test
  public void steadyStateOverlapPollsWhileTheNextEndpointIsStillUnread() throws Exception {
    // E0 is held after its row, so E1's batch is ready (and its future completed) before the
    // application finishes E0. Completion order would hand E1 to the application first and keep
    // E0's slot until E1 was fully read.
    final ScriptedPollQuery script =
        scripted(Answer.endpoints(1), Answer.endpoints(1), Answer.endpoints(1), Answer.done(0))
            .holdEnd(0)
            .holdEnd(1)
            .holdStart(1);
    final List<Integer> rows = new ArrayList<>();
    try (Connection connection = connect("pollInfoPrefetch=2");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(resultSet.next());
      assertEquals(0, resultSet.getInt(1));
      rows.add(0);
      assertTrue(script.awaitPollCount(2, WAIT_MS));
      assertFalse(script.awaitPollCount(3, QUIET_MS), "polled past the limit");

      script.releaseStart(1);
      assertTrue(script.awaitStream(EventType.BATCH_SENT, 1, WAIT_MS));
      // Let the client's reader task receive E1's batch.
      Thread.sleep(QUIET_MS);

      try (AsyncReader reader =
          new AsyncReader(() -> List.of(resultSet.next() ? resultSet.getInt(1) : -1))) {
        script.releaseEnd(0);
        rows.addAll(reader.rows());
      }
      // The application is now reading E1 (unread, still held on the server) and has not asked for
      // more: finishing E0 alone must have resumed the poller.
      assertTrue(script.awaitPollCount(3, WAIT_MS), "C2 waited for the application to go idle");
      assertFalse(script.hasEvent(EventType.DOGET_END, 1));

      script.releaseEnd(1);
      rows.addAll(readAll(resultSet));
    }
    assertEquals(List.of(0, 100, 200), sorted(rows));
    assertEquals(List.of(0, 1, 2, 3), script.pollIndices());
  }

  @Test
  public void singleReaderThreadStillFreesAFinishedEndpointsSlot() throws Exception {
    // threadPoolSize=1: E1's reader task, held before its first batch, occupies the only reader
    // thread, so a marker task for the finished E0 could never run.
    final ScriptedPollQuery script =
        scripted(Answer.endpoints(1), Answer.endpoints(1), Answer.endpoints(1), Answer.done(0))
            .holdStart(1);
    final List<Integer> rows = new ArrayList<>();
    try (Connection connection = connect(1, "pollInfoPrefetch=2");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(resultSet.next());
      assertEquals(0, resultSet.getInt(1));
      rows.add(0);
      assertTrue(script.awaitPollCount(2, WAIT_MS));
      assertFalse(script.awaitPollCount(3, QUIET_MS), "polled past the limit");

      try (AsyncReader reader =
          new AsyncReader(() -> List.of(resultSet.next() ? resultSet.getInt(1) : -1))) {
        // E0 is done: its slot is freed on the application thread and the poller polls C2, while
        // the application waits for E1 and the only reader thread is stuck on E1.
        assertTrue(
            script.awaitPollCount(3, WAIT_MS), "the slot of a finished endpoint stayed held");
        reader.awaitBlocked();
        script.releaseStart(1);
        rows.addAll(reader.rows());
      }
      rows.addAll(readAll(resultSet));
    }
    assertEquals(List.of(0, 100, 200), sorted(rows));
    assertEquals(List.of(0, 1, 2, 3), script.pollIndices());
  }

  // F3. executeQuery returns once the first endpoint exists; it does not wait for later ones.
  @Test
  public void executeQueryDoesNotWaitForFutureEndpointsWhenTheFirstHasNoRows() throws Exception {
    final CountDownLatch gate = new CountDownLatch(1);
    final ScriptedPollQuery script =
        scripted(Answer.endpoints(1), Answer.done(1).gatedBy(gate)).emptyEndpoints(0);
    try (Connection connection = connect("pollInfoPrefetch=2");
        Statement statement = connection.createStatement()) {
      final AtomicReference<ResultSet> opened = new AtomicReference<>();
      try (AsyncReader execute =
          new AsyncReader(
              () -> {
                opened.set(statement.executeQuery(script.query()));
                return List.of();
              })) {
        execute.rows();
      }
      try (ResultSet resultSet = opened.get();
          AsyncReader reader = new AsyncReader(() -> readAll(resultSet))) {
        assertTrue(script.awaitBlockedPoll(WAIT_MS));
        reader.awaitBlocked();
        gate.countDown();
        assertEquals(List.of(100), reader.rows());
      }
    }
  }

  // F3, held first stream: the first batch is enough; executeQuery does not wait for the stream to
  // end or for the continuation.
  @Test
  public void executeQueryReturnsWhileTheFirstStreamIsHeldAfterItsFirstBatch() throws Exception {
    final ScriptedPollQuery script = scripted(Answer.endpoints(1), Answer.hang()).holdEnd(0);
    try (Connection connection = connect("pollInfoPrefetch=2");
        Statement statement = connection.createStatement()) {
      final AtomicReference<ResultSet> opened = new AtomicReference<>();
      try (AsyncReader execute =
          new AsyncReader(
              () -> {
                opened.set(statement.executeQuery(script.query()));
                return List.of();
              })) {
        execute.rows();
      }
      try (ResultSet resultSet = opened.get()) {
        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getInt(1));
      }
    }
  }

  // F4. The fallback path must not report the end while a continuation remains.
  @Test
  public void prefetchZeroDeliversRowsAfterAnEndpointWithoutRows() throws Exception {
    final ScriptedPollQuery script =
        scripted(Answer.endpoints(1), Answer.endpoints(1), Answer.done(1)).emptyEndpoints(1);
    try (Connection connection = connect("pollInfoPrefetch=0");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertEquals(List.of(0, 200), readAll(resultSet));
    }
    assertEquals(List.of(0, 1, 2), script.pollIndices());
  }

  // F5. A cancel that races a poll in flight reports "Statement canceled", not a poll failure.
  @Test
  public void cancelRacingAPollInFlightReportsStatementCanceled() throws Exception {
    // The server holds its CancelFlightInfo answer, so Statement.cancel() is still inside
    // operation.cancel() (before ResultSet.cancel() raises its flag) when the poll ends.
    final ScriptedPollQuery script =
        scripted(Answer.endpoints(1), Answer.hang()).holdCancelFlightInfo();
    try (Connection connection = connect("pollInfoPrefetch=2");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(script.query())) {
      assertTrue(resultSet.next());
      try (AsyncReader reader = new AsyncReader(() -> readAll(resultSet));
          AsyncReader canceller =
              new AsyncReader(
                  () -> {
                    statement.cancel();
                    return List.of();
                  },
                  false)) {
        assertTrue(script.awaitBlockedPoll(WAIT_MS));
        reader.awaitBlocked();
        canceller.start();
        final SQLException failure = reader.failure();
        assertTrue(failure.getMessage().contains("Statement canceled"), failure.getMessage());
        script.releaseCancelFlightInfo();
        canceller.rows();
      } finally {
        script.releaseCancelFlightInfo();
      }
    }
  }

  // ---- helpers ----------------------------------------------------------------------------

  private static ScriptedPollQuery scripted(final Answer... answers) {
    return producer.scripted("SELECT poll_ahead_" + QUERY_NUMBER.incrementAndGet(), answers);
  }

  // threadPoolSize=4: a stream held open must not starve the readers of the other streams, which
  // share one pool per connection (its default size is 1).
  private static Connection connect(final String properties) throws SQLException {
    return connect(4, properties);
  }

  private static Connection connect(final int threadPoolSize, final String properties)
      throws SQLException {
    return DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight-sql://localhost:%d?useEncryption=false&usePollInfo=true"
                + "&threadPoolSize=%d&%s",
            server.getPort(), threadPoolSize, properties));
  }

  private static List<Integer> readAll(final ResultSet resultSet) throws SQLException {
    final List<Integer> rows = new ArrayList<>();
    while (resultSet.next()) {
      rows.add(resultSet.getInt(1));
    }
    return rows;
  }

  /** Reads the rest of a result on another thread, so that a hang fails the test, not the run. */
  private static List<Integer> readAllWithin(final ResultSet resultSet) throws Exception {
    try (AsyncReader reader = new AsyncReader(() -> readAll(resultSet))) {
      return reader.rows();
    }
  }

  /**
   * Reads the rest of a result and releases each endpoint's held stream right after its row is
   * read, so the result can advance past that endpoint.
   */
  private static List<Integer> readReleasing(
      final ResultSet resultSet, final ScriptedPollQuery script) throws Exception {
    try (AsyncReader reader =
        new AsyncReader(
            () -> {
              final List<Integer> rows = new ArrayList<>();
              while (resultSet.next()) {
                final int row = resultSet.getInt(1);
                rows.add(row);
                script.releaseEnd(row / 100);
              }
              return rows;
            })) {
      return reader.rows();
    }
  }

  private static List<Integer> sorted(final List<Integer> rows) {
    final List<Integer> copy = new ArrayList<>(rows);
    Collections.sort(copy);
    return copy;
  }

  private static void assertGapBetween(
      final List<Long> polls,
      final int from,
      final int to,
      final long minMillis,
      final long maxMillis) {
    final long gap = TimeUnit.NANOSECONDS.toMillis(polls.get(to) - polls.get(from));
    assertTrue(gap >= minMillis, "gap " + from + "->" + to + " was " + gap + " ms");
    assertTrue(gap < maxMillis, "gap " + from + "->" + to + " was " + gap + " ms");
  }

  private static void awaitPollerPoolIdle(final Connection connection) throws Exception {
    final ThreadPoolExecutor pool =
        connection.unwrap(ArrowFlightConnection.class).getPollerExecutor();
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
    while (pool.getActiveCount() > 0) {
      assertTrue(System.nanoTime() < deadline, "the poller pool never became idle");
      Thread.onSpinWait();
    }
  }

  /** Runs a JDBC call on its own thread and reports its outcome with a bounded wait. */
  private static final class AsyncReader implements AutoCloseable {
    private final CompletableFuture<List<Integer>> result = new CompletableFuture<>();
    private final Thread thread;

    AsyncReader(final Callable<?> call) {
      this(call, true);
    }

    /** With {@code start} false, the thread runs only once {@link #start()} is called. */
    AsyncReader(final Callable<?> call, final boolean start) {
      thread =
          new Thread(
              () -> {
                try {
                  final Object value = call.call();
                  result.complete(value instanceof List ? castList(value) : List.of());
                } catch (final Throwable t) {
                  result.completeExceptionally(t);
                }
              },
              "poll-ahead-test-reader");
      thread.setDaemon(true);
      if (start) {
        thread.start();
      }
    }

    void start() {
      thread.start();
    }

    @SuppressWarnings("unchecked")
    private static List<Integer> castList(final Object value) {
      return (List<Integer>) value;
    }

    /** Waits until the thread is parked, i.e. blocked inside the driver. */
    void awaitBlocked() throws InterruptedException {
      final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
      while (thread.getState() != Thread.State.WAITING
          && thread.getState() != Thread.State.TIMED_WAITING) {
        assertTrue(System.nanoTime() < deadline, "the reader never blocked");
        assertFalse(result.isDone(), "the reader finished instead of blocking");
        Thread.onSpinWait();
      }
    }

    List<Integer> rows() throws Exception {
      try {
        return result.get(WAIT_MS, TimeUnit.MILLISECONDS);
      } catch (final ExecutionException e) {
        if (e.getCause() instanceof Exception) {
          throw (Exception) e.getCause();
        }
        throw e;
      } catch (final TimeoutException e) {
        throw new AssertionError("the reader hung", e);
      }
    }

    SQLException failure() throws Exception {
      try {
        result.get(WAIT_MS, TimeUnit.MILLISECONDS);
      } catch (final ExecutionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof SQLException) {
          return (SQLException) cause;
        }
        throw new AssertionError("expected a SQLException but got " + cause, cause);
      } catch (final TimeoutException e) {
        throw new AssertionError("the reader hung", e);
      }
      throw new AssertionError("expected the reader to fail");
    }

    @Override
    public void close() {
      thread.interrupt();
    }
  }
}
