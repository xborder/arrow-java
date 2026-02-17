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
import java.sql.SQLException;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.PreparedStatement;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.StatementHandle;

/** A SQL statement for querying data from an Arrow Flight server. */
public class ArrowFlightStatement extends AvaticaStatement implements ArrowFlightInfoStatement {
  private FlightInfo cachedStatementFlightInfo;

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
  public ResultSet executeQuery(final String sql) throws SQLException {
    try {
      return executeStatementQuery(sql);
    } catch (FlightRuntimeException exception) {
      throw toSqlException(sql, exception);
    }
  }

  @Override
  public boolean execute(final String sql) throws SQLException {
    cachedStatementFlightInfo = null;
    return super.execute(sql);
  }

  @Override
  public long executeLargeUpdate(final String sql) throws SQLException {
    return executeStatementUpdate(sql, null);
  }

  @Override
  public FlightInfo executeFlightInfoQuery() throws SQLException {
    final Meta.Signature signature = getSignature();
    if (signature == null) {
      return null;
    }

    final ArrowFlightMetaImpl meta = getConnection().getMeta();
    final PreparedStatement preparedStatement = meta.getPreparedStatement(handle);
    final Schema resultSetSchema;
    final FlightInfo flightInfo;

    if (preparedStatement != null) {
      resultSetSchema = preparedStatement.getDataSetSchema();
      flightInfo = preparedStatement.executeQuery();
    } else {
      final FlightInfo cachedFlightInfo = cachedStatementFlightInfo;
      cachedStatementFlightInfo = null;
      flightInfo =
          cachedFlightInfo != null
              ? cachedFlightInfo
              : getConnection().getClientHandler().getInfo(signature.sql);
      resultSetSchema = flightInfo.getSchemaOptional().orElse(null);
    }

    if (resultSetSchema != null && signature.columns.isEmpty()) {
      signature.columns.addAll(
          ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields()));
      setSignature(signature);
    }

    return flightInfo;
  }

  private ResultSet executeStatementQuery(final String sql) throws SQLException {
    cachedStatementFlightInfo = null;
    closeOpenResultSet();
    final ArrowFlightSqlClientHandler clientHandler = getConnection().getClientHandler();
    final FlightInfo flightInfo = clientHandler.getInfo(sql);
    cachedStatementFlightInfo = flightInfo;

    final Schema resultSetSchema = flightInfo.getSchemaOptional().orElse(null);
    final Meta.Signature signature =
        ArrowFlightMetaImpl.newSignature(sql, resultSetSchema, null, Meta.StatementType.SELECT);
    setSignature(signature);
    updateCount = -1;

    try {
      return getConnection().executeQueryInternal(this, signature);
    } catch (SQLException exception) {
      cachedStatementFlightInfo = null;
      throw exception;
    }
  }

  private long executeStatementUpdate(final String sql, FlightRuntimeException queryException)
      throws SQLException {
    cachedStatementFlightInfo = null;
    closeOpenResultSet();
    final ArrowFlightSqlClientHandler clientHandler = getConnection().getClientHandler();
    try {
      final long count = clientHandler.executeUpdate(sql);
      updateCount = count;
      setSignature(
          ArrowFlightMetaImpl.newSignature(sql, null, null, Meta.StatementType.IS_DML));
      return count;
    } catch (FlightRuntimeException updateException) {
      if (queryException != null) {
        queryException.addSuppressed(updateException);
        throw toSqlException(sql, queryException);
      }
      throw toSqlException(sql, updateException);
    }
  }

  private void closeOpenResultSet() throws SQLException {
    if (openResultSet == null) {
      return;
    }
    final AvaticaResultSet resultSet = openResultSet;
    openResultSet = null;
    try {
      resultSet.close();
    } catch (Exception exception) {
      throw AvaticaConnection.HELPER.createException(
          "Error while closing previous result set", exception);
    }
  }

  private SQLException toSqlException(String sql, RuntimeException exception) {
    return AvaticaConnection.HELPER.createException(
        "Error while executing SQL \"" + sql + "\": " + exception.getMessage(), exception);
  }
}
