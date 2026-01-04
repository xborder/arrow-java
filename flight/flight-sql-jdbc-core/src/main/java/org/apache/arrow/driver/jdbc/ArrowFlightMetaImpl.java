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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.PreparedStatement;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.StatementExecution;
import org.apache.arrow.driver.jdbc.utils.AvaticaParameterBinder;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
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
  private final Map<StatementHandleKey, StatementExecution> statementExecutionMap;

  /**
   * Constructs a {@link MetaImpl} object specific for Arrow Flight.
   *
   * @param connection A {@link AvaticaConnection}.
   */
  public ArrowFlightMetaImpl(final AvaticaConnection connection) {
    super(connection);
    this.statementExecutionMap = new ConcurrentHashMap<>();
    setDefaultConnectionProperties();
  }

  /** Construct a signature. */
  static Signature newSignature(final String sql, Schema resultSetSchema, Schema parameterSchema) {
    List<ColumnMetaData> columnMetaData =
        resultSetSchema == null
            ? new ArrayList<>()
            : ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields());
    List<AvaticaParameter> parameters =
        parameterSchema == null
            ? new ArrayList<>()
            : ConvertUtils.convertArrowFieldsToAvaticaParameters(parameterSchema.getFields());
    StatementType statementType =
        resultSetSchema == null || resultSetSchema.getFields().isEmpty()
            ? StatementType.IS_DML
            : StatementType.SELECT;
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
    StatementExecution statementExecution =
        statementExecutionMap.remove(new StatementHandleKey(statementHandle));
    // Close the statement execution if it exists.
    // For simple statements, close() is a no-op.
    // For prepared statements, close() sends ClosePreparedStatement to the server.
    if (statementExecution != null) {
      statementExecution.close();
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
    StatementExecution statementExecution = getStatementExecution(statementHandle);

    if (statementExecution == null) {
      throw new IllegalStateException("Statement execution not found: " + statementHandle);
    }

    // Only bind parameters for prepared statements (simple statements don't support parameters)
    if (statementExecution.isPreparedStatement() && typedValues != null && !typedValues.isEmpty()) {
      new AvaticaParameterBinder(
              (PreparedStatement) statementExecution,
              ((ArrowFlightConnection) connection).getBufferAllocator())
          .bind(typedValues);
    }

    if (statementHandle.signature == null
        || statementHandle.signature.statementType == StatementType.IS_DML) {
      // Update query
      long updatedCount = statementExecution.executeUpdate();
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
    StatementExecution statementExecution = getStatementExecution(statementHandle);

    if (statementExecution == null) {
      throw new IllegalStateException("Statement execution not found: " + statementHandle);
    }

    // Batch execution requires a prepared statement for parameter binding
    if (!statementExecution.isPreparedStatement()) {
      throw new IllegalStateException("Batch execution requires a prepared statement");
    }

    final AvaticaParameterBinder binder =
        new AvaticaParameterBinder(
            (PreparedStatement) statementExecution,
            ((ArrowFlightConnection) connection).getBufferAllocator());
    for (int i = 0; i < parameterValuesList.size(); i++) {
      binder.bind(parameterValuesList.get(i), i);
    }

    // Update query
    long[] updatedCounts = {statementExecution.executeUpdate()};
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

  /**
   * Creates a server-side prepared statement for the given query. This method sends a
   * CreatePreparedStatement action to the server and should be used when the application explicitly
   * creates a PreparedStatement via {@code Connection.prepareStatement()}.
   */
  private PreparedStatement prepareForHandle(final String query, StatementHandle handle) {
    final PreparedStatement preparedStatement =
        ((ArrowFlightConnection) connection).getClientHandler().prepare(query);
    handle.signature =
        newSignature(
            query, preparedStatement.getDataSetSchema(), preparedStatement.getParameterSchema());
    statementExecutionMap.put(new StatementHandleKey(handle), preparedStatement);
    return preparedStatement;
  }

  /**
   * Creates a simple statement execution for the given query. This method uses
   * CommandStatementQuery/CommandStatementUpdate instead of creating a server-side prepared
   * statement, which is more efficient for one-time statement executions.
   */
  private StatementExecution executeSimpleForHandle(final String query, StatementHandle handle) {
    final StatementExecution statementExecution =
        ((ArrowFlightConnection) connection).getClientHandler().executeSimple(query);
    // For simple statements, we create a signature based on the statement type heuristic.
    // The actual schema will be populated after execution.
    handle.signature = newSignatureForSimpleStatement(query, statementExecution.getType());
    statementExecutionMap.put(new StatementHandleKey(handle), statementExecution);
    return statementExecution;
  }

  /**
   * Creates a signature for a simple statement. Unlike prepared statements, simple statements don't
   * have schema information until after execution, so we use a heuristic to determine the statement
   * type.
   */
  private static Signature newSignatureForSimpleStatement(String sql, StatementType statementType) {
    return new Signature(
        new ArrayList<>(), // columns will be populated after execution
        sql,
        new ArrayList<>(), // simple statements don't support parameters
        Collections.emptyMap(),
        null, // unnecessary, as SQL requests use ArrowFlightJdbcCursor
        statementType);
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
    // Use simple statement execution (CommandStatementQuery/CommandStatementUpdate)
    // instead of creating a server-side prepared statement.
    // This avoids unnecessary CreatePreparedStatement and ClosePreparedStatement calls.
    StatementExecution statementExecution = executeSimpleForHandle(query, handle);
    final StatementType statementType = statementExecution.getType();

    final long updateCount =
        statementType.equals(StatementType.UPDATE) ? statementExecution.executeUpdate() : -1;
    try {
      synchronized (callback.getMonitor()) {
        callback.clear();
        callback.assign(handle.signature, null, updateCount);
      }
      callback.execute();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    final MetaResultSet metaResultSet =
        MetaResultSet.create(handle.connectionId, handle.id, false, handle.signature, null);
    return new ExecuteResult(Collections.singletonList(metaResultSet));
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

  /**
   * Gets the statement execution for the given statement handle.
   *
   * @param statementHandle the statement handle.
   * @return the statement execution, or null if not found.
   */
  StatementExecution getStatementExecution(StatementHandle statementHandle) {
    return statementExecutionMap.get(new StatementHandleKey(statementHandle));
  }

  /**
   * Gets the prepared statement for the given statement handle. This method is used by
   * ArrowFlightStatement to access the prepared statement for query execution.
   *
   * @param statementHandle the statement handle.
   * @return the prepared statement, or null if not found or if it's a simple statement.
   * @deprecated Use {@link #getStatementExecution(StatementHandle)} instead.
   */
  @Deprecated
  PreparedStatement getPreparedStatement(StatementHandle statementHandle) {
    StatementExecution execution =
        statementExecutionMap.get(new StatementHandleKey(statementHandle));
    if (execution instanceof PreparedStatement) {
      return (PreparedStatement) execution;
    }
    return null;
  }

  // Helper used to look up prepared statement instances later. Avatica doesn't give us the
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
