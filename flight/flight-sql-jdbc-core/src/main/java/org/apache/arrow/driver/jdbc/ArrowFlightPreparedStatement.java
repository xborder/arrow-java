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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.client.PollInfoOperation;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;

/** Arrow Flight JBCS's implementation {@link PreparedStatement}. */
public class ArrowFlightPreparedStatement extends AvaticaPreparedStatement
    implements ArrowFlightInfoStatement {

  private final ArrowFlightSqlClientHandler.PreparedStatement preparedStatement;
  private final AtomicReference<PollInfoOperation> activeOperation = new AtomicReference<>();
  private volatile PollInfoOperation lastOperation;

  private ArrowFlightPreparedStatement(
      final ArrowFlightConnection connection,
      final ArrowFlightSqlClientHandler.PreparedStatement preparedStatement,
      final StatementHandle handle,
      final Signature signature,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    super(connection, handle, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
    this.preparedStatement = Preconditions.checkNotNull(preparedStatement);
  }

  static ArrowFlightPreparedStatement newPreparedStatement(
      final ArrowFlightConnection connection,
      final ArrowFlightSqlClientHandler.PreparedStatement preparedStmt,
      final StatementHandle statementHandle,
      final Signature signature,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    return new ArrowFlightPreparedStatement(
        connection,
        preparedStmt,
        statementHandle,
        signature,
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return (ArrowFlightConnection) super.getConnection();
  }

  @Override
  public synchronized void close() throws SQLException {
    this.preparedStatement.close();
    super.close();
  }

  @Override
  public FlightInfo executeFlightInfoQuery() throws SQLException {
    final PollInfoOperation operation = new PollInfoOperation(getQueryTimeout(), true);
    activeOperation.set(operation);
    lastOperation = operation;
    try {
      final FlightInfo flightInfo = preparedStatement.executeQuery(operation);
      if (!operation.hasContinuation()) {
        finishPollInfoOperation(operation);
      }
      return flightInfo;
    } catch (RuntimeException | SQLException e) {
      finishPollInfoOperation(operation);
      throw e;
    }
  }

  @Override
  public void cancel() throws SQLException {
    final PollInfoOperation operation = activeOperation.get();
    if (operation != null) {
      operation.cancel();
    }
    super.cancel();
  }

  long remainingQueryTimeoutNanos() {
    final PollInfoOperation operation = lastOperation;
    return operation != null && operation.hasDeadline()
        ? operation.remainingTimeoutNanos()
        : Long.MAX_VALUE;
  }

  PollInfoOperation activePollInfoOperation() {
    return activeOperation.get();
  }

  void finishPollInfoOperation(final PollInfoOperation operation) {
    if (operation != null && activeOperation.compareAndSet(operation, null)) {
      operation.close();
    }
  }
}
