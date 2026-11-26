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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.driver.jdbc.client.CloseableEndpointStreamPair;
import org.apache.arrow.driver.jdbc.client.PollInfoOperation;
import org.apache.arrow.driver.jdbc.utils.FlightEndpointDataQueue;
import org.apache.arrow.driver.jdbc.utils.VectorSchemaRootTransformer;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;

/**
 * {@link ResultSet} implementation for Arrow Flight used to access the results of multiple {@link
 * FlightStream} objects.
 */
public final class ArrowFlightJdbcFlightStreamResultSet
    extends ArrowFlightJdbcVectorSchemaRootResultSet {

  private final ArrowFlightConnection connection;
  private volatile FlightInfo flightInfo;
  private final PollInfoOperation pollInfoOperation;
  // Written by whichever party currently owns polling (the poller, or the reader inline).
  private volatile int consumedEndpointCount;
  private volatile EndpointPoller poller;
  private int pollerQueryTimeoutSeconds;
  private CloseableEndpointStreamPair currentEndpointData;
  private FlightEndpointDataQueue flightEndpointDataQueue;
  private volatile boolean cancelled;

  private VectorSchemaRootTransformer transformer;
  private VectorSchemaRoot currentVectorSchemaRoot;

  private Schema schema;
  private Integer id = null; // used for metadata result sets only

  /** Public constructor used by ArrowFlightJdbcFactory. */
  ArrowFlightJdbcFlightStreamResultSet(
      final AvaticaStatement statement,
      final QueryState state,
      final Meta.Signature signature,
      final ResultSetMetaData resultSetMetaData,
      final TimeZone timeZone,
      final Meta.Frame firstFrame)
      throws SQLException {
    super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    this.connection = (ArrowFlightConnection) statement.connection;
    try {
      this.flightInfo = ((ArrowFlightInfoStatement) statement).executeFlightInfoQuery();
      this.pollInfoOperation = activePollInfoOperation(statement);
    } catch (FlightRuntimeException e) {
      if (e.status().code() != FlightStatusCode.TIMED_OUT) {
        throw e;
      }
      final SQLTimeoutException jdbcTimeout =
          new SQLTimeoutException(
              String.format(
                  "Query timed out after %d %s", statement.getQueryTimeout(), TimeUnit.SECONDS));
      jdbcTimeout.initCause(e);
      throw jdbcTimeout;
    }
  }

  /** Private constructor for fromFlightInfo. */
  private ArrowFlightJdbcFlightStreamResultSet(
      final ArrowFlightConnection connection,
      final QueryState state,
      final Meta.Signature signature,
      final ResultSetMetaData resultSetMetaData,
      final TimeZone timeZone,
      final Meta.Frame firstFrame,
      final FlightInfo flightInfo)
      throws SQLException {
    super(null, state, signature, resultSetMetaData, timeZone, firstFrame);
    this.connection = connection;
    this.flightInfo = flightInfo;
    this.pollInfoOperation = null;
    this.id = connection.getNewMetadataResultSetId(this);
  }

  /**
   * Create a {@link ResultSet} which pulls data from given {@link FlightInfo}. This is used to
   * fetch result sets from DatabaseMetadata calls and skips the Avatica factory.
   *
   * @param connection The connection linked to the returned ResultSet.
   * @param flightInfo The FlightInfo from which data will be iterated by the returned ResultSet.
   * @param transformer Optional transformer for processing VectorSchemaRoot before access from
   *     ResultSet
   * @return A ResultSet which pulls data from given FlightInfo.
   */
  static ArrowFlightJdbcFlightStreamResultSet fromFlightInfo(
      final ArrowFlightConnection connection,
      final FlightInfo flightInfo,
      final VectorSchemaRootTransformer transformer)
      throws SQLException {
    // Similar to how org.apache.calcite.avatica.util.ArrayFactoryImpl does

    final TimeZone timeZone = TimeZone.getDefault();
    final QueryState state = new QueryState();

    final Meta.Signature signature = ArrowFlightMetaImpl.buildDefaultSignature();

    final AvaticaResultSetMetaData resultSetMetaData =
        new AvaticaResultSetMetaData(null, null, signature);
    final ArrowFlightJdbcFlightStreamResultSet resultSet =
        new ArrowFlightJdbcFlightStreamResultSet(
            connection, state, signature, resultSetMetaData, timeZone, null, flightInfo);

    resultSet.transformer = transformer;

    resultSet.populateData();
    return resultSet;
  }

  // A progressive result that still has a continuation is polled in the background.
  private boolean shouldPollAhead() {
    return pollInfoOperation != null && pollInfoOperation.hasContinuation();
  }

  private void loadNewQueue(final boolean pollAhead) {
    if (poller != null) {
      poller.stop();
      poller = null;
    }
    Optional.ofNullable(flightEndpointDataQueue).ifPresent(AutoCloseables::closeNoChecked);
    // Without a continuation the limit is lifted and every endpoint opens at once, as it always
    // did.
    flightEndpointDataQueue =
        FlightEndpointDataQueue.createNewQueue(
            connection.getExecutorService(),
            pollAhead ? connection.getPollInfoPrefetch() : Integer.MAX_VALUE,
            endpoint -> connection.getClientHandler().getStream(endpoint));
  }

  private void loadNewFlightStream() throws SQLException {
    if (currentEndpointData != null) {
      AutoCloseables.closeNoChecked(currentEndpointData);
    }
    // The first load must not wait for endpoints that were not published yet.
    this.currentEndpointData = getNextEndpointStream(true, false);
  }

  @Override
  protected AvaticaResultSet execute() throws SQLException {
    if (flightInfo != null) {
      schema = flightInfo.getSchemaOptional().orElse(null);
      populateData();
    }
    return this;
  }

  private void populateData() throws SQLException {
    final boolean pollAhead = shouldPollAhead();
    loadNewQueue(pollAhead);
    try {
      // The first answer goes through the queue too, so deferred opening applies to it.
      appendEndpoints(flightInfo, pollAhead);
      if (pollAhead) {
        startPoller();
      }
      loadNewFlightStream();
      // Ownership of the root will be passed onto the cursor. Without a stream yet, next() waits
      // for the first endpoint that has rows.
      if (currentEndpointData != null) {
        populateDataForCurrentFlightStream();
      }
    } catch (final SQLException | RuntimeException e) {
      if (poller != null) {
        poller.stop();
      }
      try {
        flightEndpointDataQueue.close();
      } catch (final Exception closeException) {
        e.addSuppressed(closeException);
      }
      // No ResultSet will be returned to close an unfinished operation: end it here.
      if (pollInfoOperation != null && !pollInfoOperation.isComplete()) {
        pollInfoOperation.cancel();
      }
      finishPollInfoOperation();
      throw e;
    }
  }

  private void populateDataForCurrentFlightStream() throws SQLException {
    final VectorSchemaRoot originalRoot = currentEndpointData.getStream().getRoot();

    if (transformer != null) {
      try {
        currentVectorSchemaRoot = transformer.transform(originalRoot, currentVectorSchemaRoot);
      } catch (final Exception e) {
        throw new SQLException("Failed to transform VectorSchemaRoot.", e);
      }
    } else {
      currentVectorSchemaRoot = originalRoot;
    }

    populateData(currentVectorSchemaRoot, schema);
  }

  /** Expose appMetadata associated with the underlying FlightInfo for this ResultSet. */
  public byte[] getAppMetadata() {
    return flightInfo.getAppMetadata();
  }

  @Override
  public boolean next() throws SQLException {
    // Fail fast: no row after a cancel, or after a background poll failed.
    if (cancelled) {
      throw AvaticaConnection.HELPER.createException("Statement canceled");
    }
    final FlightEndpointDataQueue queue = flightEndpointDataQueue;
    if (queue != null && queue.failure() != null) {
      throw queue.failure();
    }
    if (currentVectorSchemaRoot == null) {
      if (!loadNextPublishedEndpoint()) {
        return false;
      }
    }
    while (true) {
      final boolean hasNext = super.next();
      final int maxRows = statement != null ? statement.getMaxRows() : 0;
      if (maxRows != 0 && this.getRow() > maxRows) {
        if (statement.isCloseOnCompletion()) {
          statement.close();
        }
        return false;
      }

      if (hasNext) {
        return true;
      }

      try {
        if (currentEndpointData != null) {
          currentEndpointData.getStream().getRoot().clear();
          if (currentEndpointData.getStream().next()) {
            populateDataForCurrentFlightStream();
            continue;
          }

          // The slot is freed here, on the application thread, not when a reader task later
          // consumes the end of the stream.
          final CloseableEndpointStreamPair exhausted = currentEndpointData;
          currentEndpointData = null;
          endpointExhausted(exhausted);
        }

        currentEndpointData = getNextEndpointStream(false, true);
      } catch (final FlightRuntimeException e) {
        // A concurrent statement.cancel() (or close) cancels in-flight FlightStreams,
        // which surface here as CANCELLED. Normalize to Avatica's "Statement canceled". This wins
        // over a failure recorded by a poll that the cancel ended.
        if ((cancelled || flightEndpointDataQueue.isClosed())
            && e.status().code() == CallStatus.CANCELLED.code()) {
          throw AvaticaConnection.HELPER.createException("Statement canceled");
        }
        // A failed continuation poll cancels the open streams, which surface here as CANCELLED
        // while the reader is blocked on one: report the recorded failure, not the cancel.
        final SQLException recordedFailure = flightEndpointDataQueue.failure();
        if (recordedFailure != null) {
          throw recordedFailure;
        }
        throw e;
      }

      if (currentEndpointData != null) {
        populateDataForCurrentFlightStream();
        continue;
      }

      // Cancellation can wake the reader before queue cleanup completes.
      if (cancelled || flightEndpointDataQueue.isClosed()) {
        throw AvaticaConnection.HELPER.createException("Statement canceled");
      }

      if (loadNextPublishedEndpoint()) {
        continue;
      }

      if (statement != null && statement.isCloseOnCompletion()) {
        statement.close();
      }

      return false;
    }
  }

  @Override
  protected void cancel() {
    cancelled = true;
    if (poller != null) {
      poller.stop();
    }
    if (pollInfoOperation != null && !pollInfoOperation.isComplete()) {
      pollInfoOperation.cancel();
    }
    finishPollInfoOperation();
    super.cancel();
    final CloseableEndpointStreamPair currentEndpoint = this.currentEndpointData;
    if (currentEndpoint != null) {
      currentEndpoint.getStream().cancel("Cancel", null);
    }

    if (flightEndpointDataQueue != null) {
      try {
        flightEndpointDataQueue.close();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public synchronized void close() {

    try {
      if (isClosed()) {
        return;
      }
      this.connection.onResultSetClose(id);
      if (poller != null) {
        poller.stop();
      }
      if (flightEndpointDataQueue != null) {
        // flightStreamQueue should close currentFlightStream internally
        flightEndpointDataQueue.close();
      } else if (currentEndpointData != null) {
        // close is only called for currentFlightStream if there's no queue
        currentEndpointData.close();
      }

    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (pollInfoOperation != null && !pollInfoOperation.isComplete()) {
        pollInfoOperation.cancel();
      }
      finishPollInfoOperation();
      super.close();
    }
  }

  private CloseableEndpointStreamPair getNextEndpointStream(
      final boolean canTimeout, final boolean waitForMore) throws SQLException {
    try {
      if (canTimeout) {
        final long remainingTimeoutNanos = remainingQueryTimeoutNanos();
        if (remainingTimeoutNanos != Long.MAX_VALUE) {
          if (remainingTimeoutNanos <= 0) {
            throw new SQLTimeoutException("Query timed out before retrieving its first endpoint");
          }
          try {
            return waitForMore
                ? flightEndpointDataQueue.next(remainingTimeoutNanos, TimeUnit.NANOSECONDS)
                : flightEndpointDataQueue.nextAvailable(
                    remainingTimeoutNanos, TimeUnit.NANOSECONDS);
          } catch (SQLTimeoutException e) {
            if (statement != null && e.getMessage().startsWith("Query timed out after")) {
              final SQLTimeoutException jdbcTimeout =
                  new SQLTimeoutException(
                      String.format(
                          "Query timed out after %d %s",
                          statement.getQueryTimeout(), TimeUnit.SECONDS));
              jdbcTimeout.initCause(e);
              throw jdbcTimeout;
            }
            throw e;
          }
        }
      }
      return waitForMore ? flightEndpointDataQueue.next() : flightEndpointDataQueue.nextAvailable();
    } catch (final IllegalStateException e) {
      throw normalizeClosedQueueException(flightEndpointDataQueue, e);
    }
  }

  private long remainingQueryTimeoutNanos() throws SQLException {
    if (statement instanceof ArrowFlightStatement) {
      return ((ArrowFlightStatement) statement).remainingQueryTimeoutNanos();
    }
    if (statement instanceof ArrowFlightPreparedStatement) {
      return ((ArrowFlightPreparedStatement) statement).remainingQueryTimeoutNanos();
    }
    final int statementTimeout = statement != null ? statement.getQueryTimeout() : 0;
    return statementTimeout > 0 ? TimeUnit.SECONDS.toNanos(statementTimeout) : Long.MAX_VALUE;
  }

  static void enqueueEndpointData(
      final FlightEndpointDataQueue queue,
      final Collection<CloseableEndpointStreamPair> endpointRequests)
      throws SQLException {
    final Iterator<CloseableEndpointStreamPair> endpointIterator = endpointRequests.iterator();
    while (endpointIterator.hasNext()) {
      final CloseableEndpointStreamPair endpointRequest = endpointIterator.next();
      try {
        queue.enqueue(endpointRequest);
      } catch (final IllegalStateException e) {
        final SQLException exception = normalizeClosedQueueException(queue, e);
        closeRejectedEndpoint(endpointRequest, exception);
        endpointIterator.forEachRemaining(endpoint -> closeRejectedEndpoint(endpoint, exception));
        throw exception;
      }
    }
  }

  private void endpointExhausted(final CloseableEndpointStreamPair endpointRequest)
      throws SQLException {
    try {
      flightEndpointDataQueue.endpointExhausted(endpointRequest);
    } catch (final IllegalStateException e) {
      throw normalizeClosedQueueException(flightEndpointDataQueue, e);
    }
  }

  private static void closeRejectedEndpoint(
      final CloseableEndpointStreamPair endpoint, final SQLException exception) {
    try {
      endpoint.close();
    } catch (final Exception closeException) {
      exception.addSuppressed(closeException);
    }
  }

  private static SQLException normalizeClosedQueueException(
      final FlightEndpointDataQueue queue, final IllegalStateException exception) {
    if (queue.isClosed()) {
      return AvaticaConnection.HELPER.createException("Statement canceled");
    }
    throw exception;
  }

  private void startPoller() throws SQLException {
    pollerQueryTimeoutSeconds = statement != null ? statement.getQueryTimeout() : 0;
    final EndpointPoller newPoller =
        new EndpointPoller(
            flightEndpointDataQueue,
            pollInfoOperation,
            connection.getPollerExecutor(),
            answer -> appendEndpoints(answer.flightInfo(), answer.hasContinuation()),
            this::mapContinuationFailure,
            this::endPollInfoOperationAfterFailure,
            this::finishPollInfoOperation,
            connection.getPollInfoMaxBackoffMs());
    flightEndpointDataQueue.setResumeAction(newPoller::resume);
    flightEndpointDataQueue.setIdleAction(newPoller::runInlineIfOrphaned);
    poller = newPoller;
    // A busy poller pool leaves the poller orphaned: the reader runs it inline when it would wait.
    newPoller.start();
  }

  /** Records the endpoints a poll answer appended, and whether more may follow, in one step. */
  private void appendEndpoints(final FlightInfo updatedFlightInfo, final boolean moreExpected)
      throws SQLException {
    final int updatedEndpointCount = updatedFlightInfo.getEndpoints().size();
    if (updatedEndpointCount < consumedEndpointCount) {
      throw new SQLException("PollInfo removed previously published endpoints");
    }
    final Schema updatedSchema = updatedFlightInfo.getSchemaOptional().orElse(schema);
    if (schema != null && updatedSchema != null && !schema.equals(updatedSchema)) {
      throw new SQLException("PollInfo changed the result schema");
    }
    final List<FlightEndpoint> appendedEndpoints =
        new ArrayList<>(
            updatedFlightInfo.getEndpoints().subList(consumedEndpointCount, updatedEndpointCount));
    try {
      flightEndpointDataQueue.addEndpoints(appendedEndpoints, moreExpected);
    } catch (final IllegalStateException e) {
      throw normalizeClosedQueueException(flightEndpointDataQueue, e);
    }
    consumedEndpointCount = updatedEndpointCount;
    flightInfo = updatedFlightInfo;
  }

  /** Maps a failed background continuation poll to the exception the reader will throw. */
  private SQLException mapContinuationFailure(final FlightRuntimeException e) {
    // A cancel that ended this poll is not a failure: Statement.cancel() cancels the operation
    // before ResultSet.cancel() raises the flag, so the poller can get here first.
    if (pollInfoOperation.isCancelled()) {
      return AvaticaConnection.HELPER.createException("Statement canceled");
    }
    if (e.status().code() == FlightStatusCode.TIMED_OUT) {
      final SQLTimeoutException timeout =
          new SQLTimeoutException(
              String.format(
                  "Query timed out after %d %s", pollerQueryTimeoutSeconds, TimeUnit.SECONDS));
      timeout.initCause(e);
      return timeout;
    }
    return new SQLException("Continuation PollFlightInfo failed", e);
  }

  // Runs after the queue recorded the failure, because cancel() is a blocking RPC.
  private void endPollInfoOperationAfterFailure(final FlightRuntimeException e) {
    if (e.status().code() == FlightStatusCode.CANCELLED
        || e.status().code() == FlightStatusCode.TIMED_OUT) {
      pollInfoOperation.cancel();
    } else {
      pollInfoOperation.terminate();
    }
    finishPollInfoOperation();
  }

  private boolean loadNextPublishedEndpoint() throws SQLException {
    if (poller == null) {
      // No continuation was left after the first answer: every endpoint was read.
      finishPollInfoOperation();
      return false;
    }
    // The queue is the single source of truth. It waits while more may arrive, and null means every
    // endpoint was read and the poller finished.
    try {
      currentEndpointData = getNextEndpointStream(false, true);
    } catch (final FlightRuntimeException e) {
      if ((cancelled || flightEndpointDataQueue.isClosed())
          && e.status().code() == CallStatus.CANCELLED.code()) {
        throw AvaticaConnection.HELPER.createException("Statement canceled");
      }
      throw e;
    }
    if (currentEndpointData != null) {
      populateDataForCurrentFlightStream();
      return true;
    }
    if (cancelled || flightEndpointDataQueue.isClosed()) {
      throw AvaticaConnection.HELPER.createException("Statement canceled");
    }
    finishPollInfoOperation();
    return false;
  }

  private static PollInfoOperation activePollInfoOperation(final AvaticaStatement statement) {
    if (statement instanceof ArrowFlightStatement) {
      return ((ArrowFlightStatement) statement).activePollInfoOperation();
    }
    if (statement instanceof ArrowFlightPreparedStatement) {
      return ((ArrowFlightPreparedStatement) statement).activePollInfoOperation();
    }
    return null;
  }

  private void finishPollInfoOperation() {
    if (statement instanceof ArrowFlightStatement) {
      ((ArrowFlightStatement) statement).finishPollInfoOperation(pollInfoOperation);
    } else if (statement instanceof ArrowFlightPreparedStatement) {
      ((ArrowFlightPreparedStatement) statement).finishPollInfoOperation(pollInfoOperation);
    }
  }
}
