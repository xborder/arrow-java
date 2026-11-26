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
package org.apache.arrow.driver.jdbc.utils;

import static java.lang.String.format;
import static org.apache.arrow.util.Preconditions.checkNotNull;
import static org.apache.arrow.util.Preconditions.checkState;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.apache.arrow.driver.jdbc.client.CloseableEndpointStreamPair;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.calcite.avatica.AvaticaConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auxiliary class used to handle consuming of multiple {@link FlightStream}.
 *
 * <p>The usage follows this routine:
 *
 * <ol>
 *   <li>Create a <code>FlightStreamQueue</code>;
 *   <li>Call <code>enqueue(FlightStream)</code> for all streams to be consumed;
 *   <li>Call <code>next()</code> to get a FlightStream that is ready to consume
 *   <li>Consume the given FlightStream and add it back to the queue - call <code>
 *       enqueue(FlightStream)</code>
 *   <li>Repeat from (3) until <code>next()</code> returns null.
 * </ol>
 *
 * <p>A queue created with an {@link EndpointOpener} additionally supports progressive results.
 * Endpoints are handed over with {@link #addEndpoints(List, boolean)} and stay pending until fewer
 * than {@code maxOpen} streams are open and unread; only then is their stream opened. While more
 * endpoints may still arrive, {@link #next()} waits instead of returning {@code null}.
 *
 * <p>All state shared between the reader (the thread calling {@link #next()}) and a background
 * poller is guarded by one lock. The lock is never held across {@code Future.get()}, {@code
 * CompletionService.take()/poll()}, or an RPC (opening a stream); a condition wait releases it.
 */
public class FlightEndpointDataQueue implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlightEndpointDataQueue.class);

  /** Opens the DoGet stream of one endpoint. Called without the queue lock held. */
  @FunctionalInterface
  public interface EndpointOpener {
    CloseableEndpointStreamPair open(FlightEndpoint endpoint) throws SQLException;
  }

  private final CompletionService<CloseableEndpointStreamPair> completionService;
  private final EndpointOpener opener;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition changed = lock.newCondition();
  private final Object closeMonitor = new Object();

  // Everything below is guarded by `lock`, unless it is volatile.
  private final Map<Future<CloseableEndpointStreamPair>, CloseableEndpointStreamPair> futures =
      new HashMap<>();
  private final Set<CloseableEndpointStreamPair> endpointsToClose = new HashSet<>();
  private final Deque<FlightEndpoint> pending = new ArrayDeque<>();
  private int maxOpen = Integer.MAX_VALUE;
  // Streams opened (or being opened) through the opener whose endpoint is not finished yet.
  private int openUnread;
  // The part of openUnread whose stream is being opened outside the lock.
  private int opening;
  private boolean moreExpected;
  private boolean paused;
  private Runnable resumeAction;
  private BooleanSupplier idleAction;
  private volatile boolean shutdown;
  private volatile SQLException failure;
  // Guarded by closeMonitor.
  private boolean teardownDone;

  /** Instantiate a new FlightStreamQueue. */
  protected FlightEndpointDataQueue(
      final CompletionService<CloseableEndpointStreamPair> executorService) {
    this(executorService, null, Integer.MAX_VALUE);
  }

  private FlightEndpointDataQueue(
      final CompletionService<CloseableEndpointStreamPair> executorService,
      final EndpointOpener opener,
      final int maxOpen) {
    completionService = checkNotNull(executorService);
    this.opener = opener;
    this.maxOpen = maxOpen;
  }

  /**
   * Creates a new {@link FlightEndpointDataQueue} from the provided {@link ExecutorService}.
   *
   * @param service the service from which to create a new queue.
   * @return a new queue.
   */
  public static FlightEndpointDataQueue createNewQueue(final ExecutorService service) {
    return new FlightEndpointDataQueue(new ExecutorCompletionService<>(service));
  }

  /**
   * Creates a queue that opens endpoint streams itself and keeps at most {@code maxOpen} of them
   * open and unread; the other endpoints wait as pending until a stream finishes.
   *
   * @param service the service that reads the streams.
   * @param maxOpen the most open, unread streams; {@link Integer#MAX_VALUE} opens eagerly.
   * @param opener opens one endpoint's stream.
   * @return a new queue.
   */
  public static FlightEndpointDataQueue createNewQueue(
      final ExecutorService service, final int maxOpen, final EndpointOpener opener) {
    checkState(maxOpen >= 1, "maxOpen must be at least 1");
    return new FlightEndpointDataQueue(
        new ExecutorCompletionService<>(service), checkNotNull(opener), maxOpen);
  }

  /**
   * Gets whether this queue is closed. This turns true as soon as {@link #close()} starts, so a
   * reader woken by a cancel sees it.
   *
   * @return a boolean indicating whether this resource is closed.
   */
  public boolean isClosed() {
    return shutdown;
  }

  /** Wakes a reader that is waiting for more endpoints so it re-checks the queue. */
  public void wakeReader() {
    lock.lock();
    try {
      changed.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /** The failure recorded by {@link #fail(SQLException)}, or null. */
  public SQLException failure() {
    return failure;
  }

  /** Called by the reader, with no lock held, when it is about to wait for more endpoints. */
  public void setIdleAction(final BooleanSupplier idleAction) {
    lock.lock();
    try {
      this.idleAction = idleAction;
    } finally {
      lock.unlock();
    }
  }

  /** Called, with no lock held, when the unread count drops below the limit of a paused poller. */
  public void setResumeAction(final Runnable resumeAction) {
    lock.lock();
    try {
      this.resumeAction = resumeAction;
    } finally {
      lock.unlock();
    }
  }

  /** The number of endpoints queued and not finished: pending plus open-unread. */
  public int unreadCount() {
    lock.lock();
    try {
      return unreadLocked();
    } finally {
      lock.unlock();
    }
  }

  /** The number of streams open (or being opened) and not finished. */
  public int openUnreadCount() {
    lock.lock();
    try {
      return openUnread;
    } finally {
      lock.unlock();
    }
  }

  /** Whether more endpoints may still be added. */
  public boolean isMoreExpected() {
    lock.lock();
    try {
      return moreExpected;
    } finally {
      lock.unlock();
    }
  }

  private int unreadLocked() {
    return pending.size() + openUnread;
  }

  /**
   * Atomically queues newly published endpoints and records whether more may follow. When {@code
   * moreExpected} is false this is the last call, so the reader can see the final endpoints and the
   * end of results in one step. Endpoints beyond the open limit stay pending; the others are opened
   * (without the lock) before this method returns.
   *
   * @throws IllegalStateException if the queue is closed.
   * @throws SQLException if a stream cannot be opened; the queue is then failed with it.
   */
  public void addEndpoints(final List<FlightEndpoint> endpoints, final boolean moreExpected)
      throws SQLException {
    checkState(opener != null, "This queue has no endpoint opener");
    final List<FlightEndpoint> toOpen;
    lock.lock();
    try {
      checkOpen();
      if (failure != null) {
        // Fail fast: nothing is queued or opened after a failure; the reader rethrows it.
        return;
      }
      pending.addAll(endpoints);
      this.moreExpected = moreExpected;
      toOpen = reserveSlotsLocked();
      changed.signalAll();
    } finally {
      lock.unlock();
    }
    openReserved(toOpen);
  }

  /**
   * Stops waiting for a poller and lifts the open limit: nothing more is expected through {@link
   * #addEndpoints}, and pending endpoints open now.
   */
  public void disablePollAhead() throws SQLException {
    final List<FlightEndpoint> toOpen;
    lock.lock();
    try {
      maxOpen = Integer.MAX_VALUE;
      moreExpected = false;
      paused = false;
      toOpen = reserveSlotsLocked();
      changed.signalAll();
    } finally {
      lock.unlock();
    }
    openReserved(toOpen);
  }

  /**
   * Atomically checks the limit and pauses the poller. Returns true, after running {@code onPaused}
   * under the lock, when more endpoints are expected but unread endpoints are at the limit. Because
   * the count check, the pause and {@code onPaused} are one step under the same lock the reader
   * uses to decrement the count and wake the poller, a wakeup cannot be lost.
   */
  public boolean pauseIfFull(final Runnable onPaused) {
    lock.lock();
    try {
      if (shutdown || failure != null || !moreExpected || unreadLocked() < maxOpen) {
        return false;
      }
      paused = true;
      onPaused.run();
      return true;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Fail fast: records the failure, drops pending endpoints, cancels every open stream (which drops
   * its buffered batches), and makes {@link #next()} rethrow {@code cause}. The first failure wins;
   * a closed queue ignores it.
   */
  public void fail(final SQLException cause) {
    checkNotNull(cause);
    final List<CloseableEndpointStreamPair> toCancel;
    lock.lock();
    try {
      if (shutdown || failure != null) {
        return;
      }
      failure = cause;
      pending.clear();
      moreExpected = false;
      toCancel = new ArrayList<>(endpointsToClose);
      changed.signalAll();
    } finally {
      lock.unlock();
    }
    for (final CloseableEndpointStreamPair endpoint : toCancel) {
      try {
        endpoint.getStream().cancel("Query failed.", null);
      } catch (final RuntimeException e) {
        LOGGER.debug("Failed to cancel a FlightStream after a failure.", e);
      }
    }
  }

  // Moves pending endpoints into the open-unread count while fewer than maxOpen are open.
  private List<FlightEndpoint> reserveSlotsLocked() {
    if (opener == null || pending.isEmpty() || openUnread >= maxOpen) {
      return Collections.emptyList();
    }
    final List<FlightEndpoint> reserved = new ArrayList<>();
    while (!pending.isEmpty() && openUnread < maxOpen) {
      reserved.add(pending.poll());
      openUnread++;
      opening++;
    }
    return reserved;
  }

  // Opens reserved endpoints without the lock. On failure, or once the queue failed or closed, the
  // reservations that were not handed to register() are released, even for an Error.
  private void openReserved(final List<FlightEndpoint> reserved) throws SQLException {
    int settled = 0;
    try {
      while (settled < reserved.size()) {
        if (shutdown || failure != null) {
          return;
        }
        final CloseableEndpointStreamPair stream;
        try {
          stream = opener.open(reserved.get(settled));
        } catch (final SQLException | RuntimeException e) {
          final SQLException cause =
              e instanceof SQLException ? (SQLException) e : new SQLException(e);
          fail(cause);
          throw cause;
        }
        // From here register() settles this endpoint's reservation, even when it throws.
        settled++;
        try {
          register(stream);
        } catch (final RuntimeException e) {
          fail(new SQLException("Failed to start reading an endpoint stream", e));
          throw e;
        }
      }
    } finally {
      if (settled < reserved.size()) {
        releaseReservations(reserved.size() - settled);
      }
    }
  }

  private void releaseReservations(final int count) {
    lock.lock();
    try {
      opening -= count;
      openUnread -= count;
      changed.signalAll();
    } finally {
      lock.unlock();
    }
  }

  // Adds an opened stream and starts reading it. Drops it if the queue was closed or failed.
  private void register(final CloseableEndpointStreamPair stream) {
    boolean registered = false;
    RuntimeException rejected = null;
    lock.lock();
    try {
      opening--;
      if (!shutdown && failure == null) {
        endpointsToClose.add(stream);
        try {
          futures.put(submitReader(stream), stream);
          registered = true;
        } catch (final RuntimeException e) {
          endpointsToClose.remove(stream);
          rejected = e;
        }
      }
      if (!registered) {
        openUnread--;
      }
      changed.signalAll();
    } finally {
      lock.unlock();
    }
    if (!registered) {
      closeQuietly(stream);
    }
    if (rejected != null) {
      throw rejected;
    }
  }

  private Future<CloseableEndpointStreamPair> submitReader(
      final CloseableEndpointStreamPair endpointRequest) {
    return completionService.submit(
        () -> {
          // `FlightStream#next` will block until new data can be read or stream is over.
          while (endpointRequest.getStream().next()) {
            if (endpointRequest.getStream().getRoot().getRowCount() > 0) {
              return endpointRequest;
            }
          }
          return null;
        });
  }

  private static void closeQuietly(final CloseableEndpointStreamPair endpoint) {
    try {
      endpoint.close();
    } catch (final Exception e) {
      LOGGER.debug("Failed to close a FlightStream.", e);
    }
  }

  /**
   * The application read an endpoint's stream to its end. Frees the endpoint's slot at once, on the
   * calling thread, under the queue lock: the open-unread count drops, the stream is closed,
   * pending endpoints open, and a paused poller resumes. Nothing is re-enqueued, so a slot does not
   * wait for a reader task or for the completion order of other endpoints.
   *
   * @param finished the stream the application just exhausted; it is closed here.
   * @throws SQLException if opening a pending endpoint fails; the queue is then failed with it.
   */
  public void endpointExhausted(final CloseableEndpointStreamPair finished) throws SQLException {
    checkNotNull(finished);
    if (opener == null) {
      // A queue without an opener has no slots; the reader task consumes the end of the stream.
      enqueue(finished);
      return;
    }
    onStreamFinished(finished);
  }

  // The reader finished an endpoint: free its slot, open pending endpoints, and wake the poller.
  private void onStreamFinished(final CloseableEndpointStreamPair finished) throws SQLException {
    final List<FlightEndpoint> toOpen;
    Runnable resume = null;
    lock.lock();
    try {
      if (opener != null) {
        endpointsToClose.remove(finished);
        openUnread--;
        toOpen = reserveSlotsLocked();
        if (paused && unreadLocked() < maxOpen) {
          paused = false;
          resume = resumeAction;
        }
      } else {
        toOpen = Collections.emptyList();
      }
      changed.signalAll();
    } finally {
      lock.unlock();
    }
    if (opener != null && finished != null) {
      closeQuietly(finished);
    }
    if (resume != null) {
      resume.run();
    }
    openReserved(toOpen);
  }

  private CloseableEndpointStreamPair awaitNext(
      final boolean timed,
      final long timeoutValue,
      final TimeUnit timeoutUnit,
      final boolean waitForMore)
      throws SQLException {
    final long deadlineNanos = timed ? System.nanoTime() + timeoutUnit.toNanos(timeoutValue) : 0L;
    boolean idleTried = false;
    while (true) {
      boolean runIdle = false;
      BooleanSupplier idle = null;
      lock.lock();
      try {
        checkOpen();
        throwIfFailed();
        if (futures.isEmpty()) {
          final boolean nothingQueued = pending.isEmpty() && opening == 0;
          if (nothingQueued && (!moreExpected || !waitForMore)) {
            return null;
          }
          if (nothingQueued && idleAction != null && !idleTried) {
            runIdle = true;
            idle = idleAction;
          } else {
            awaitChange(timed, deadlineNanos, timeoutValue, timeoutUnit);
            idleTried = false;
            continue;
          }
        }
      } finally {
        lock.unlock();
      }
      if (runIdle) {
        idleTried = !idle.getAsBoolean();
        continue;
      }

      final Future<CloseableEndpointStreamPair> future =
          timed ? pollFuture(deadlineNanos, timeoutValue, timeoutUnit) : takeFuture();
      final CloseableEndpointStreamPair source;
      lock.lock();
      try {
        source = futures.remove(future);
      } finally {
        lock.unlock();
      }
      final CloseableEndpointStreamPair ready;
      try {
        ready = future.get();
      } catch (final ExecutionException e) {
        throwIfFailed();
        // Unwrap one layer
        final Throwable cause = e.getCause();
        if (cause instanceof FlightRuntimeException) {
          throw (FlightRuntimeException) cause;
        }
        throw AvaticaConnection.HELPER.wrap(e.getMessage(), e);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw AvaticaConnection.HELPER.wrap(e.getMessage(), e);
      } catch (final CancellationException e) {
        throw AvaticaConnection.HELPER.wrap(e.getMessage(), e);
      }
      // Get the next FlightStream that has a root with content.
      if (ready != null) {
        throwIfFailed();
        return ready;
      }
      onStreamFinished(source);
      // A consumed future may have resumed a poller that the pool rejected: the idle hook must get
      // another chance, or the reader could wait for a wake-up that was already sent.
      idleTried = false;
    }
  }

  // Called with the lock held. Releases it while waiting.
  private void awaitChange(
      final boolean timed,
      final long deadlineNanos,
      final long timeoutValue,
      final TimeUnit timeoutUnit)
      throws SQLException {
    try {
      if (timed) {
        final long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
          throw new SQLTimeoutException(
              String.format("Query timed out after %d %s", timeoutValue, timeoutUnit));
        }
        changed.awaitNanos(remaining);
      } else {
        changed.await();
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      if (timed) {
        throw new SQLTimeoutException("Query was interrupted", e);
      }
      throw AvaticaConnection.HELPER.wrap(e.getMessage(), e);
    }
  }

  private Future<CloseableEndpointStreamPair> takeFuture() throws SQLException {
    try {
      return completionService.take();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw AvaticaConnection.HELPER.wrap(e.getMessage(), e);
    }
  }

  private Future<CloseableEndpointStreamPair> pollFuture(
      final long deadlineNanos, final long timeoutValue, final TimeUnit timeoutUnit)
      throws SQLException {
    try {
      final Future<CloseableEndpointStreamPair> future =
          completionService.poll(
              Math.max(0L, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
      if (future != null) {
        return future;
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLTimeoutException("Query was interrupted", e);
    }
    throw new SQLTimeoutException(
        String.format("Query timed out after %d %s", timeoutValue, timeoutUnit));
  }

  private void throwIfFailed() throws SQLException {
    final SQLException recorded = failure;
    if (recorded != null) {
      throw recorded;
    }
  }

  /**
   * Blocking request with timeout to get the next ready FlightStream in queue.
   *
   * @param timeoutValue the amount of time to be waited
   * @param timeoutUnit the timeoutValue time unit
   * @return a FlightStream that is ready to consume or null if all FlightStreams are ended.
   */
  public CloseableEndpointStreamPair next(final long timeoutValue, final TimeUnit timeoutUnit)
      throws SQLException {
    return awaitNext(true, timeoutValue, timeoutUnit, true);
  }

  /**
   * Blocking request to get the next ready FlightStream in queue.
   *
   * @return a FlightStream that is ready to consume or null if all FlightStreams are ended.
   */
  public CloseableEndpointStreamPair next() throws SQLException {
    return awaitNext(false, 0L, TimeUnit.NANOSECONDS, true);
  }

  /**
   * Like {@link #next(long, TimeUnit)}, but returns {@code null} instead of waiting for endpoints
   * that were not published yet: it waits only while a queued endpoint may still produce a batch.
   * The first load of a result uses it, so that {@code executeQuery} does not wait for the poller.
   *
   * @return a FlightStream that is ready to consume, or null if no queued endpoint is left.
   */
  public CloseableEndpointStreamPair nextAvailable(
      final long timeoutValue, final TimeUnit timeoutUnit) throws SQLException {
    return awaitNext(true, timeoutValue, timeoutUnit, false);
  }

  /** Untimed variant of {@link #nextAvailable(long, TimeUnit)}. */
  public CloseableEndpointStreamPair nextAvailable() throws SQLException {
    return awaitNext(false, 0L, TimeUnit.NANOSECONDS, false);
  }

  /** Checks if this queue is open. */
  public void checkOpen() {
    checkState(!isClosed(), format("%s closed", this.getClass().getSimpleName()));
  }

  /** Readily adds given {@link FlightStream}s to the queue. */
  public void enqueue(final Collection<CloseableEndpointStreamPair> endpointRequests) {
    endpointRequests.forEach(this::enqueue);
  }

  /** Adds given {@link FlightStream} to the queue. */
  public void enqueue(final CloseableEndpointStreamPair endpointRequest) {
    checkNotNull(endpointRequest);
    lock.lock();
    try {
      checkOpen();
      endpointsToClose.add(endpointRequest);
      futures.put(submitReader(endpointRequest), endpointRequest);
      changed.signalAll();
    } finally {
      lock.unlock();
    }
  }

  private static boolean isCallStatusCancelled(final Exception e) {
    return e.getCause() instanceof FlightRuntimeException
        && ((FlightRuntimeException) e.getCause()).status().code() == CallStatus.CANCELLED.code();
  }

  @Override
  public void close() throws SQLException {
    // Concurrent closers wait for the first one to finish; the queue lock is held only briefly.
    synchronized (closeMonitor) {
      if (teardownDone) {
        return;
      }
      final List<CloseableEndpointStreamPair> toClose;
      final List<Future<CloseableEndpointStreamPair>> toDrain;
      lock.lock();
      try {
        shutdown = true;
        pending.clear();
        moreExpected = false;
        toClose = new ArrayList<>(endpointsToClose);
        toDrain = new ArrayList<>(futures.keySet());
        changed.signalAll();
      } finally {
        lock.unlock();
      }

      final Set<SQLException> exceptions = new HashSet<>();
      try {
        for (final CloseableEndpointStreamPair endpointToClose : toClose) {
          try {
            endpointToClose.getStream().cancel("Cancelling this FlightStream.", null);
          } catch (final Exception e) {
            final String errorMsg = "Failed to cancel a FlightStream.";
            LOGGER.error(errorMsg, e);
            exceptions.add(new SQLException(errorMsg, e));
          }
        }
        toDrain.forEach(
            future -> {
              try {
                // TODO: Consider adding a hardcoded timeout?
                future.get();
              } catch (final InterruptedException | ExecutionException e) {
                // Ignore if future is already cancelled
                if (!isCallStatusCancelled(e)) {
                  final String errorMsg = "Failed consuming a future during close.";
                  LOGGER.error(errorMsg, e);
                  exceptions.add(new SQLException(errorMsg, e));
                }
              }
            });
        for (final CloseableEndpointStreamPair endpointToClose : toClose) {
          try {
            endpointToClose.close();
          } catch (final Exception e) {
            final String errorMsg = "Failed to close a FlightStream.";
            LOGGER.error(errorMsg, e);
            exceptions.add(new SQLException(errorMsg, e));
          }
        }
      } finally {
        lock.lock();
        try {
          endpointsToClose.clear();
          futures.clear();
          changed.signalAll();
        } finally {
          lock.unlock();
        }
        teardownDone = true;
      }
      if (!exceptions.isEmpty()) {
        final SQLException sqlException = new SQLException("Failed to close streams.");
        exceptions.forEach(sqlException::setNextException);
        throw sqlException;
      }
    }
  }
}
