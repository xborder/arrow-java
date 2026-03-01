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
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.PreparedStatement;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.SqlStatement;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.StatementHandle;

/** A SQL statement for querying data from an Arrow Flight server. */
public class ArrowFlightStatement extends AvaticaStatement implements ArrowFlightInfoStatement {

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
    checkOpen();
    updateCount = -1;
    resetAdhocHandle();
    try {
      final Meta.Signature signature = ArrowFlightMetaImpl.newStatementSignature(sql);
      setSignature(signature);
      return executeQueryInternal(signature, false);
    } catch (RuntimeException exception) {
      throw AvaticaConnection.HELPER.createException(
          "Error while executing SQL \"" + sql + "\": " + exception.getMessage(),
          exception);
    }
  }

  @Override
  public long executeLargeUpdate(final String sql) throws SQLException {
    checkOpen();
    clearOpenResultSet();
    updateCount = -1;

    resetAdhocHandle();
    try {
      final ArrowFlightMetaImpl meta = getConnection().getMeta();
      final SqlStatement statementHandle = meta.getStatement(handle);
      final long updatedCount = statementHandle.executeUpdate(sql);
      setSignature(ArrowFlightMetaImpl.newUpdateSignature(sql));
      updateCount = updatedCount;
      return updatedCount;
    } catch (RuntimeException exception) {
      throw AvaticaConnection.HELPER.createException(
          "Error while executing SQL \"" + sql + "\": " + exception.getMessage(),
          exception);
    }
  }

  @Override
  public FlightInfo executeFlightInfoQuery() throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final ArrowFlightMetaImpl meta = connection.getMeta();
    final Meta.Signature signature = getSignature();
    if (signature == null) {
      return null;
    }

    final SqlStatement statement = meta.getStatement(handle);
    // Statement.execute(String) goes through Meta.prepareAndExecute, which stores a prepared
    // handle even though the JDBC object is still an ArrowFlightStatement. Therefore,
    // executeFlightInfoQuery must handle both direct and prepared handles here.
    if (statement instanceof PreparedStatement) {
      final Schema resultSetSchema = statement.getDataSetSchema();
      signature.columns.addAll(
          ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields()));
      setSignature(signature);
      return statement.executeQuery();
    }

    final FlightInfo flightInfo =
        statement != null
            ? statement.executeQuery(signature.sql)
            : connection.getClientHandler().getInfo(signature.sql);
    final Schema resultSetSchema = flightInfo.getSchemaOptional().orElse(null);
    if (resultSetSchema != null) {
      signature.columns.addAll(
          ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields()));
      setSignature(signature);
    }
    return flightInfo;
  }

  private void clearOpenResultSet() throws SQLException {
    synchronized (this) {
      if (openResultSet != null) {
        final AvaticaResultSet resultSet = openResultSet;
        openResultSet = null;
        try {
          resultSet.close();
        } catch (Exception exception) {
          throw AvaticaConnection.HELPER.createException(
              "Error while closing previous result set", exception);
        }
      }
    }
  }

  private void resetAdhocHandle() throws SQLException {
    final ArrowFlightConnection conn = getConnection();
    final ArrowFlightMetaImpl meta = conn.getMeta();
    final SqlStatement statementHandle = meta.getStatement(handle);
    if (statementHandle == null || statementHandle instanceof PreparedStatement) {
      final SqlStatement newStatementHandle = conn.getClientHandler().createAdhocStatement();
      meta.updateStatementHandle(handle, newStatementHandle);
    }
  }
}
