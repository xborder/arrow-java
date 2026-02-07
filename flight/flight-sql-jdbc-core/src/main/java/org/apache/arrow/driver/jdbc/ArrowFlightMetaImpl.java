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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.PreparedStatement;
import org.apache.arrow.driver.jdbc.utils.AvaticaParameterBinder;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;

/** Metadata handler for Arrow Flight. */
public class ArrowFlightMetaImpl extends MetaImpl {
  enum StatementExecutionMode {
    QUERY_ONLY,
    UPDATE_ONLY,
    QUERY_WITH_FALLBACK
  }

  private final Map<StatementHandleKey, PreparedStatement> statementHandlePreparedStatementMap;
  private final Map<StatementHandleKey, FlightInfo> statementHandleFlightInfoMap;
  private final Map<StatementHandleKey, StatementExecutionMode> statementHandleExecutionModeMap;

  /**
   * Constructs a {@link MetaImpl} object specific for Arrow Flight.
   *
   * @param connection A {@link AvaticaConnection}.
   */
  public ArrowFlightMetaImpl(final AvaticaConnection connection) {
    super(connection);
    this.statementHandlePreparedStatementMap = new ConcurrentHashMap<>();
    this.statementHandleFlightInfoMap = new ConcurrentHashMap<>();
    this.statementHandleExecutionModeMap = new ConcurrentHashMap<>();
    setDefaultConnectionProperties();
  }

  /** Construct a signature. */
  static Signature newSignature(final String sql, Schema resultSetSchema, Schema parameterSchema) {
    StatementType statementType =
        resultSetSchema == null || resultSetSchema.getFields().isEmpty()
            ? StatementType.IS_DML
            : StatementType.SELECT;
    return newSignature(sql, resultSetSchema, parameterSchema, statementType);
  }

  /** Construct a signature with an explicit statement type. */
  static Signature newSignature(
      final String sql,
      Schema resultSetSchema,
      Schema parameterSchema,
      StatementType statementType) {
    List<ColumnMetaData> columnMetaData =
        resultSetSchema == null
            ? new ArrayList<>()
            : ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields());
    List<AvaticaParameter> parameters =
        parameterSchema == null
            ? new ArrayList<>()
            : ConvertUtils.convertArrowFieldsToAvaticaParameters(parameterSchema.getFields());
    return new Signature(
        columnMetaData,
        sql,
        parameters,
        Collections.emptyMap(),
        null, // unnecessary, as SQL requests use ArrowFlightJdbcCursor
        statementType);
  }

  @Override
  public void closeStatement(final StatementHandle statementHandle) {
    PreparedStatement preparedStatement =
        statementHandlePreparedStatementMap.remove(new StatementHandleKey(statementHandle));
    statementHandleFlightInfoMap.remove(new StatementHandleKey(statementHandle));
    statementHandleExecutionModeMap.remove(new StatementHandleKey(statementHandle));
    // Testing if the prepared statement was created because the statement can be
    // not created until
    // this moment
    if (preparedStatement != null) {
      preparedStatement.close();
    }
  }

  @Override
  public void commit(final ConnectionHandle connectionHandle) {
    // TODO Fill this stub.
  }

  @Override
  public ExecuteResult execute(
      final StatementHandle statementHandle,
      final List<TypedValue> typedValues,
      final long maxRowCount) {
    Preconditions.checkArgument(
        connection.id.equals(statementHandle.connectionId), "Connection IDs are not consistent");
    PreparedStatement preparedStatement = getPreparedStatement(statementHandle);

    if (preparedStatement == null) {
      throw new IllegalStateException("Prepared statement not found: " + statementHandle);
    }

    new AvaticaParameterBinder(
            preparedStatement, ((ArrowFlightConnection) connection).getBufferAllocator())
        .bind(typedValues);

    if (statementHandle.signature == null
        || statementHandle.signature.statementType == StatementType.IS_DML) {
      // Update query
      long updatedCount = preparedStatement.executeUpdate();
      return new ExecuteResult(
          Collections.singletonList(
              MetaResultSet.count(statementHandle.connectionId, statementHandle.id, updatedCount)));
    } else {
      // TODO Why is maxRowCount ignored?
      return new ExecuteResult(
          Collections.singletonList(
              MetaResultSet.create(
                  statementHandle.connectionId,
                  statementHandle.id,
                  true,
                  statementHandle.signature,
                  null)));
    }
  }

  @Override
  public ExecuteResult execute(
      final StatementHandle statementHandle,
      final List<TypedValue> typedValues,
      final int maxRowsInFirstFrame) {
    return execute(statementHandle, typedValues, (long) maxRowsInFirstFrame);
  }

  @Override
  public ExecuteBatchResult executeBatch(
      final StatementHandle statementHandle, final List<List<TypedValue>> parameterValuesList)
      throws IllegalStateException {
    Preconditions.checkArgument(
        connection.id.equals(statementHandle.connectionId), "Connection IDs are not consistent");
    PreparedStatement preparedStatement = getPreparedStatement(statementHandle);

    if (preparedStatement == null) {
      throw new IllegalStateException("Prepared statement not found: " + statementHandle);
    }

    final AvaticaParameterBinder binder =
        new AvaticaParameterBinder(
            preparedStatement, ((ArrowFlightConnection) connection).getBufferAllocator());
    for (int i = 0; i < parameterValuesList.size(); i++) {
      binder.bind(parameterValuesList.get(i), i);
    }

    // Update query
    long[] updatedCounts = {preparedStatement.executeUpdate()};
    return new ExecuteBatchResult(updatedCounts);
  }

  @Override
  public Frame fetch(
      final StatementHandle statementHandle, final long offset, final int fetchMaxRowCount) {
    /*
     * ArrowFlightMetaImpl does not use frames.
     * Instead, we have accessors that contain a VectorSchemaRoot with
     * the results.
     */
    throw AvaticaConnection.HELPER.wrap(
        String.format("%s does not use frames.", this), AvaticaConnection.HELPER.unsupported());
  }

  private PreparedStatement prepareForHandle(final String query, StatementHandle handle) {
    final PreparedStatement preparedStatement =
        ((ArrowFlightConnection) connection).getClientHandler().prepare(query);
    handle.signature =
        newSignature(
            query, preparedStatement.getDataSetSchema(), preparedStatement.getParameterSchema());
    statementHandlePreparedStatementMap.put(new StatementHandleKey(handle), preparedStatement);
    return preparedStatement;
  }

  @Override
  public StatementHandle prepare(
      final ConnectionHandle connectionHandle, final String query, final long maxRowCount) {
    final StatementHandle handle = super.createStatement(connectionHandle);
    prepareForHandle(query, handle);
    return handle;
  }

  @Override
  public ExecuteResult prepareAndExecute(
      final StatementHandle statementHandle,
      final String query,
      final long maxRowCount,
      final PrepareCallback prepareCallback)
      throws NoSuchStatementException {
    return prepareAndExecute(
        statementHandle, query, maxRowCount, -1 /* Not used */, prepareCallback);
  }

  @Override
  public ExecuteResult prepareAndExecute(
      final StatementHandle handle,
      final String query,
      final long maxRowCount,
      final int maxRowsInFirstFrame,
      final PrepareCallback callback)
      throws NoSuchStatementException {
    try {
      final ArrowFlightConnection flightConnection = (ArrowFlightConnection) connection;
      final ArrowFlightSqlClientHandler clientHandler = flightConnection.getClientHandler();

      Schema resultSetSchema = null;
      FlightInfo statementFlightInfo = null;
      StatementType statementType;
      long updateCount = -1;
      FlightRuntimeException updateFallbackException = null;
      final StatementExecutionMode executionMode = getStatementExecutionMode(handle);

      if (executionMode == StatementExecutionMode.UPDATE_ONLY) {
        statementType = StatementType.IS_DML;
      } else {
        try {
          statementFlightInfo = clientHandler.getInfo(query);
          resultSetSchema = statementFlightInfo.getSchemaOptional().orElse(null);
          statementType = StatementType.SELECT;
        } catch (FlightRuntimeException infoException) {
          if (executionMode == StatementExecutionMode.QUERY_ONLY) {
            throw infoException;
          }
          if (shouldFallbackToUpdate(infoException)) {
            statementType = StatementType.IS_DML;
            updateFallbackException = infoException;
          } else {
            throw infoException;
          }
        }
      }

      if (statementType == StatementType.IS_DML) {
        try {
          updateCount = clientHandler.executeUpdate(query);
        } catch (FlightRuntimeException updateException) {
          if (updateFallbackException != null) {
            updateFallbackException.addSuppressed(updateException);
            throw updateFallbackException;
          }
          throw updateException;
        }
      }

      handle.signature = newSignature(query, resultSetSchema, null, statementType);
      if (statementFlightInfo != null) {
        statementHandleFlightInfoMap.put(new StatementHandleKey(handle), statementFlightInfo);
      }
      synchronized (callback.getMonitor()) {
        callback.clear();
        callback.assign(handle.signature, null, updateCount);
      }
      callback.execute();
      final MetaResultSet metaResultSet =
          MetaResultSet.create(handle.connectionId, handle.id, false, handle.signature, null);
      return new ExecuteResult(Collections.singletonList(metaResultSet));
    } catch (SQLTimeoutException e) {
      // So far AvaticaStatement(executeInternal) only handles NoSuchStatement and
      // Runtime
      // Exceptions.
      throw new RuntimeException(e);
    } catch (SQLException e) {
      throw new NoSuchStatementException(handle);
    }
  }

  @Override
  public ExecuteBatchResult prepareAndExecuteBatch(
      final StatementHandle statementHandle, final List<String> queries)
      throws NoSuchStatementException {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public void rollback(final ConnectionHandle connectionHandle) {
    // TODO Fill this stub.
  }

  @Override
  public boolean syncResults(
      final StatementHandle statementHandle, final QueryState queryState, final long offset)
      throws NoSuchStatementException {
    // TODO Fill this stub.
    return false;
  }

  @Override
  public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
    final ConnectionProperties result = super.connectionSync(ch, connProps);
    final String newCatalog = this.connProps.getCatalog();
    if (newCatalog != null) {
      try {
        ((ArrowFlightConnection) connection).getClientHandler().setCatalog(newCatalog);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  void setDefaultConnectionProperties() {
    // TODO Double-check this.
    connProps
        .setDirty(false)
        .setAutoCommit(true)
        .setReadOnly(true)
        .setCatalog(null)
        .setSchema(null)
        .setTransactionIsolation(Connection.TRANSACTION_NONE);
  }

  PreparedStatement getPreparedStatement(StatementHandle statementHandle) {
    return statementHandlePreparedStatementMap.get(new StatementHandleKey(statementHandle));
  }

  void setStatementExecutionMode(StatementHandle statementHandle, StatementExecutionMode mode) {
    statementHandleExecutionModeMap.put(new StatementHandleKey(statementHandle), mode);
  }

  void clearStatementExecutionMode(StatementHandle statementHandle) {
    statementHandleExecutionModeMap.remove(new StatementHandleKey(statementHandle));
  }

  FlightInfo removeStatementFlightInfo(StatementHandle statementHandle) {
    return statementHandleFlightInfoMap.remove(new StatementHandleKey(statementHandle));
  }

  private StatementExecutionMode getStatementExecutionMode(StatementHandle statementHandle) {
    final StatementExecutionMode mode =
        statementHandleExecutionModeMap.get(new StatementHandleKey(statementHandle));
    return mode == null ? StatementExecutionMode.QUERY_WITH_FALLBACK : mode;
  }

  private boolean shouldFallbackToUpdate(FlightRuntimeException exception) {
    final FlightStatusCode code = exception.status().code();
    if (code == FlightStatusCode.INVALID_ARGUMENT || code == FlightStatusCode.UNIMPLEMENTED) {
      return true;
    }
    if (code == FlightStatusCode.INTERNAL) {
      final String message = exception.getMessage();
      return message != null
          && (message.contains("Query not registered") || message.contains("Query not found"));
    }
    return false;
  }

  // Helper used to look up prepared statement instances later. Avatica doesn't
  // give us the
  // signature in
  // an UPDATE code path so we can't directly use StatementHandle as a map key.
  private static final class StatementHandleKey {
    public final String connectionId;
    public final int id;

    StatementHandleKey(StatementHandle statementHandle) {
      this.connectionId = statementHandle.connectionId;
      this.id = statementHandle.id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      StatementHandleKey that = (StatementHandleKey) o;

      if (id != that.id) {
        return false;
      }
      return connectionId.equals(that.connectionId);
    }

    @Override
    public int hashCode() {
      int result = connectionId.hashCode();
      result = 31 * result + id;
      return result;
    }
  }
}
