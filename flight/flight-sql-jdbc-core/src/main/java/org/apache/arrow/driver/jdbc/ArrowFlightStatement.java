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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.PreparedStatement;
import org.apache.arrow.driver.jdbc.client.PollInfoOperation;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.StatementHandle;

/** A SQL statement for querying data from an Arrow Flight server. */
public class ArrowFlightStatement extends AvaticaStatement implements ArrowFlightInfoStatement {
  private final AtomicReference<PollInfoOperation> activeOperation = new AtomicReference<>();
  private volatile PollInfoOperation lastOperation;

  ArrowFlightStatement(
      final ArrowFlightConnection connection,
      final StatementHandle handle,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability) {
    super(connection, handle, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return (ArrowFlightConnection) super.getConnection();
  }

  @Override
  public FlightInfo executeFlightInfoQuery() throws SQLException {
    final PreparedStatement preparedStatement =
        getConnection().getMeta().getPreparedStatement(handle);
    final Meta.Signature signature = getSignature();
    if (signature == null) {
      return null;
    }

    final Schema resultSetSchema = preparedStatement.getDataSetSchema();
    signature.columns.clear();
    signature.columns.addAll(
        ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields()));
    setSignature(signature);

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
