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

import static org.apache.arrow.driver.jdbc.utils.FlightEndpointDataQueue.createNewQueue;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.driver.jdbc.client.CloseableEndpointStreamPair;
import org.apache.arrow.driver.jdbc.client.PollInfoOperation;
import org.apache.arrow.driver.jdbc.utils.FlightEndpointDataQueue;
import org.apache.arrow.driver.jdbc.utils.VectorSchemaRootTransformer;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
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
  private FlightInfo flightInfo;
  private final PollInfoOperation pollInfoOperation;
  private int consumedEndpointCount;
  private CloseableEndpointStreamPair currentEndpointData;
  private FlightEndpointDataQueue flightEndpointDataQueue;

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
                  "Query timed out after %d %s",
                  statement.getQueryTimeout(), TimeUnit.SECONDS));
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

    final Meta.Signature signature = ArrowFlightMetaImpl.newSignature(null, null, null, null);

    final AvaticaResultSetMetaData resultSetMetaData =
        new AvaticaResultSetMetaData(null, null, signature);
    final ArrowFlightJdbcFlightStreamResultSet resultSet =
        new ArrowFlightJdbcFlightStreamResultSet(
            connection, state, signature, resultSetMetaData, timeZone, null, flightInfo);

    resultSet.transformer = transformer;

    resultSet.populateData();
    return resultSet;
  }

  private void loadNewQueue() {
    Optional.ofNullable(flightEndpointDataQueue).ifPresent(AutoCloseables::closeNoChecked);
    flightEndpointDataQueue = createNewQueue(connection.getExecutorService());
  }

  private void loadNewFlightStream() throws SQLException {
    if (currentEndpointData != null) {
      AutoCloseables.closeNoChecked(currentEndpointData);
    }
    this.currentEndpointData = getNextEndpointStream(true);
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
    loadNewQueue();
    enqueueNewEndpoints(flightInfo);
    loadNewFlightStream();

    // Ownership of the root will be passed onto the cursor.
    if (currentEndpointData != null) {
      populateDataForCurrentFlightStream();
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

      if (currentEndpointData != null) {
        currentEndpointData.getStream().getRoot().clear();
        if (currentEndpointData.getStream().next()) {
          populateDataForCurrentFlightStream();
          continue;
        }

        flightEndpointDataQueue.enqueue(currentEndpointData);
      }

      currentEndpointData = getNextEndpointStream(false);

      if (currentEndpointData != null) {
        populateDataForCurrentFlightStream();
        continue;
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

  private CloseableEndpointStreamPair getNextEndpointStream(final boolean canTimeout)
      throws SQLException {
    if (canTimeout) {
      final long remainingTimeoutNanos = remainingQueryTimeoutNanos();
      if (remainingTimeoutNanos != Long.MAX_VALUE) {
        if (remainingTimeoutNanos <= 0) {
          throw new SQLTimeoutException("Query timed out before retrieving its first endpoint");
        }
        try {
          return flightEndpointDataQueue.next(remainingTimeoutNanos, TimeUnit.NANOSECONDS);
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
    } else {
      return flightEndpointDataQueue.next();
    }
    return flightEndpointDataQueue.next();
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

  private void enqueueNewEndpoints(final FlightInfo updatedFlightInfo) throws SQLException {
    final int updatedEndpointCount = updatedFlightInfo.getEndpoints().size();
    if (updatedEndpointCount < consumedEndpointCount) {
      throw new SQLException("PollInfo removed previously published endpoints");
    }
    final Schema updatedSchema = updatedFlightInfo.getSchemaOptional().orElse(schema);
    if (schema != null && updatedSchema != null && !schema.equals(updatedSchema)) {
      throw new SQLException("PollInfo changed the result schema");
    }
    if (updatedEndpointCount == consumedEndpointCount) {
      flightInfo = updatedFlightInfo;
      return;
    }
    final List<FlightEndpoint> appendedEndpoints =
        new ArrayList<>(
            updatedFlightInfo
                .getEndpoints()
                .subList(consumedEndpointCount, updatedEndpointCount));
    final FlightInfo appendedInfo =
        new FlightInfo(
            updatedSchema,
            updatedFlightInfo.getDescriptor(),
            appendedEndpoints,
            updatedFlightInfo.getBytes(),
            updatedFlightInfo.getRecords());
    flightEndpointDataQueue.enqueue(connection.getClientHandler().getStreams(appendedInfo));
    consumedEndpointCount = updatedEndpointCount;
    flightInfo = updatedFlightInfo;
  }

  private boolean loadNextPublishedEndpoint() throws SQLException {
    if (pollInfoOperation == null || !pollInfoOperation.hasContinuation()) {
      finishPollInfoOperation();
      return false;
    }
    try {
      enqueueNewEndpoints(pollInfoOperation.pollNextAvailable());
      currentEndpointData = getNextEndpointStream(false);
      if (currentEndpointData != null) {
        populateDataForCurrentFlightStream();
        return true;
      }
      finishPollInfoOperation();
      return false;
    } catch (FlightRuntimeException e) {
      if (e.status().code() == FlightStatusCode.CANCELLED
          || e.status().code() == FlightStatusCode.TIMED_OUT) {
        pollInfoOperation.cancel();
      } else {
        pollInfoOperation.terminate();
      }
      finishPollInfoOperation();
      if (e.status().code() == FlightStatusCode.TIMED_OUT) {
        final SQLTimeoutException timeout =
            new SQLTimeoutException(
                String.format(
                    "Query timed out after %d %s",
                    statement.getQueryTimeout(), TimeUnit.SECONDS));
        timeout.initCause(e);
        throw timeout;
      }
      throw new SQLException("Continuation PollFlightInfo failed", e);
    }
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
