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

import java.sql.SQLException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.arrow.driver.jdbc.client.PollBackoff;
import org.apache.arrow.driver.jdbc.client.PollInfoOperation;
import org.apache.arrow.driver.jdbc.client.PollInfoOperation.PollAnswer;
import org.apache.arrow.driver.jdbc.utils.FlightEndpointDataQueue;
import org.apache.arrow.flight.FlightRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chains PollFlightInfo continuations in the background while the application reads a progressive
 * result. It is the poller of the model in {@code formal/pollinfo-poll-ahead/PollAhead.tla}.
 *
 * <p>The poller runs one step at a time: pause if unread endpoints are at the limit, poll the held
 * continuation once, queue the appended endpoints (and mark the queue "no more expected" in the
 * same locked step when the query is complete), then back off if the answer added nothing. It runs
 * on the connection's poller pool, never on the stream reader pool. At the limit it pauses and
 * returns its thread; the reader resumes it when it frees a slot.
 *
 * <p>At most one party runs the poller at any moment (the {@code owned} flag). If a resume finds
 * the pool busy, the poller is orphaned and the reader runs it inline when it would otherwise wait
 * for more endpoints, which is the behavior of the polling-in-{@code next()} path.
 */
final class EndpointPoller {
  private static final Logger LOGGER = LoggerFactory.getLogger(EndpointPoller.class);
  private static final int SUBMIT_ATTEMPTS = 10;
  private static final long SUBMIT_RETRY_PARK_NANOS = 100_000L;

  /** The poller's lifecycle. */
  enum State {
    RUNNING,
    POLLING,
    PAUSED,
    BACKOFF,
    FINISHED,
    FAILED,
    STOPPED
  }

  /** Receives each successful answer: queues its appended endpoints and the continuation flag. */
  @FunctionalInterface
  interface AnswerSink {
    void accept(PollAnswer answer) throws SQLException;
  }

  private final FlightEndpointDataQueue queue;
  private final PollInfoOperation operation;
  private final Executor executor;
  private final AnswerSink sink;
  private final Function<FlightRuntimeException, SQLException> failureMapper;
  private final Consumer<FlightRuntimeException> afterFailure;
  private final Runnable onFinished;
  private final PollBackoff backoff;

  private final AtomicBoolean owned = new AtomicBoolean();
  private final AtomicBoolean orphaned = new AtomicBoolean();
  private volatile boolean stopped;
  private volatile State state = State.RUNNING;

  EndpointPoller(
      final FlightEndpointDataQueue queue,
      final PollInfoOperation operation,
      final Executor executor,
      final AnswerSink sink,
      final Function<FlightRuntimeException, SQLException> failureMapper,
      final Consumer<FlightRuntimeException> afterFailure,
      final Runnable onFinished,
      final long maxBackoffMillis) {
    this.queue = queue;
    this.operation = operation;
    this.executor = executor;
    this.sink = sink;
    this.failureMapper = failureMapper;
    this.afterFailure = afterFailure;
    this.onFinished = onFinished;
    this.backoff = new PollBackoff(maxBackoffMillis);
  }

  State state() {
    return state;
  }

  /**
   * Starts polling on the executor.
   *
   * @return false if the executor rejected the task; the poller is then unused.
   */
  boolean start() {
    owned.set(true);
    if (submit()) {
      return true;
    }
    owned.set(false);
    state = State.STOPPED;
    return false;
  }

  /** Stops the poller. Cancelling the operation ends an open poll and a backoff wait. */
  void stop() {
    stopped = true;
    state = State.STOPPED;
  }

  /**
   * Resumes a paused poller. Called, with no queue lock held, once the pause flag was cleared under
   * the queue lock.
   */
  void resume() {
    if (stopped || !owned.compareAndSet(false, true)) {
      return;
    }
    handBack();
  }

  /**
   * Called by the reader when it would otherwise wait for more endpoints. If the poller has no
   * thread (a resume was rejected), polls on the reader's thread until an answer adds endpoints.
   *
   * @return true if it polled, so the reader should look at the queue again.
   */
  boolean runInlineIfOrphaned() {
    if (stopped || !orphaned.compareAndSet(true, false)) {
      return false;
    }
    try {
      drive(true);
    } catch (final Throwable t) {
      fail(t);
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
    return true;
  }

  private boolean submit() {
    for (int attempt = 1; attempt <= SUBMIT_ATTEMPTS; attempt++) {
      try {
        executor.execute(this::runOnPool);
        return true;
      } catch (final RejectedExecutionException e) {
        // A pool thread that has just finished a task may not be waiting for work yet.
        if (attempt < SUBMIT_ATTEMPTS) {
          LockSupport.parkNanos(SUBMIT_RETRY_PARK_NANOS);
        }
      }
    }
    return false;
  }

  // Gives the owned poller to the pool, or leaves it for the reader to run inline.
  private void handBack() {
    if (!submit()) {
      orphaned.set(true);
      queue.wakeReader();
    }
  }

  private void runOnPool() {
    try {
      drive(false);
    } catch (final Throwable t) {
      // Whatever ends the poller must reach the reader, or next() would wait forever.
      fail(t);
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
  }

  private void drive(final boolean inline) {
    while (true) {
      if (stopped || queue.isClosed() || queue.failure() != null) {
        state = State.STOPPED;
        return;
      }
      state = State.RUNNING;
      // PollerPause: the limit check and the pause are one step under the queue lock.
      if (!inline
          && queue.pauseIfFull(
              () -> {
                state = State.PAUSED;
                owned.set(false);
              })) {
        return;
      }

      state = State.POLLING;
      final PollAnswer answer;
      try {
        answer = operation.pollOnce();
      } catch (final FlightRuntimeException e) {
        onPollFailed(e);
        return;
      }
      if (stopped || queue.isClosed()) {
        // PollerDrop: the answer to a poll that cancel interrupted is discarded.
        state = State.STOPPED;
        return;
      }
      if (answer == null) {
        queue.fail(new SQLException("Continuation PollFlightInfo has no continuation to poll"));
        state = State.FAILED;
        return;
      }

      // PollerApply: queue the appended endpoints and, if the query is complete, finish in the
      // same step under the queue lock.
      try {
        sink.accept(answer);
      } catch (final SQLException e) {
        queue.fail(e);
        state = State.FAILED;
        return;
      } catch (final RuntimeException e) {
        if (stopped || queue.isClosed()) {
          state = State.STOPPED;
        } else {
          fail(e);
        }
        return;
      }
      if (!answer.hasContinuation()) {
        state = State.FINISHED;
        onFinished.run();
        return;
      }
      if (answer.addedEndpoints()) {
        backoff.reset();
        if (inline) {
          // The reader has endpoints to read again; give the poller back to the pool.
          handBack();
          return;
        }
        continue;
      }
      state = State.BACKOFF;
      if (!operation.backoff(backoff.nextDelayMillis())) {
        if (stopped || queue.isClosed()) {
          state = State.STOPPED;
        } else {
          state = State.FAILED;
          queue.fail(new SQLException("Interrupted while waiting for the next PollFlightInfo"));
        }
        return;
      }
    }
  }

  private void onPollFailed(final FlightRuntimeException e) {
    if (stopped || queue.isClosed()) {
      // Cancel or close ended the open poll.
      state = State.STOPPED;
      return;
    }
    state = State.FAILED;
    // The queue fails first, so the reader sees the failure at once; the cleanup (a blocking
    // CancelFlightInfo RPC) follows.
    queue.fail(failureMapper.apply(e));
    afterFailure.accept(e);
  }

  private void fail(final Throwable e) {
    LOGGER.debug("PollFlightInfo poller failed unexpectedly", e);
    state = State.FAILED;
    queue.fail(new SQLException("Continuation PollFlightInfo failed", e));
  }
}
