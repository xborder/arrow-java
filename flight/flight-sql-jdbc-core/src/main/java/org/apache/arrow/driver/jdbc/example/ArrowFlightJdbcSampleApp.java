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
package org.apache.arrow.driver.jdbc.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public final class ArrowFlightJdbcSampleApp {
  private static final String DEFAULT_URL =
      "jdbc:arrow-flight-sql://localhost:32010/?useEncryption=false";

  private static final String SELECT_SQL = "SELECT 1";
  private static final String UPDATE_SQL = "UPDATE intTable SET value = value WHERE id = 1";
  private static final String PREPARED_SELECT_SQL =
      "SELECT id, keyName, value FROM intTable WHERE id = ?";
  private static final String PREPARED_UPDATE_SQL =
      "UPDATE intTable SET value = ? WHERE id = ?";

  private ArrowFlightJdbcSampleApp() {}

  public static void main(String[] args) throws Exception {
    final String url = args.length > 0 ? args[0] : DEFAULT_URL;
    final Properties properties = new Properties();
      properties.setProperty("user", "dremio");
      properties.setProperty("password", "dremio123");

    System.out.println("Connecting to: " + url);
    try (Connection connection = DriverManager.getConnection(url, properties)) {
      runStatementExamples(connection);
//      runPreparedStatementExamples(connection);
    }
  }

  private static void runStatementExamples(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      System.out.println("\nStatement.execute (query): " + SELECT_SQL);
//      boolean hasResultSet = statement.execute(SELECT_SQL);
//      if (hasResultSet) {
//        try (ResultSet resultSet = statement.getResultSet()) {
//          printResultSet(resultSet);
//        }
//      } else {
//        System.out.println("Update count: " + statement.getUpdateCount());
//      }
//
//      System.out.println("\nStatement.execute (update): " + UPDATE_SQL);
//      boolean hasResultSet = statement.execute(UPDATE_SQL);
//      if (!hasResultSet) {
//        System.out.println("Update count: " + statement.getUpdateCount());
//      }

//      System.out.println("\nStatement.executeQuery: " + SELECT_SQL);
//      try (ResultSet resultSet = statement.executeQuery(SELECT_SQL)) {
//        printResultSet(resultSet);
//      }

//      System.out.println("\nStatement.executeUpdate: " + UPDATE_SQL);
//      int updated = statement.executeUpdate(UPDATE_SQL);
//      System.out.println("Updated rows: " + updated);
    }
  }

  private static void runPreparedStatementExamples(Connection connection) throws SQLException {
    System.out.println("\nPreparedStatement.execute (query): " + PREPARED_SELECT_SQL);
    try (PreparedStatement preparedStatement = connection.prepareStatement(PREPARED_SELECT_SQL)) {
      preparedStatement.setInt(1, 1);
      boolean hasResultSet = preparedStatement.execute();
      if (hasResultSet) {
        try (ResultSet resultSet = preparedStatement.getResultSet()) {
          printResultSet(resultSet);
        }
      } else {
        System.out.println("Update count: " + preparedStatement.getUpdateCount());
      }
    }

    System.out.println("\nPreparedStatement.executeQuery: " + PREPARED_SELECT_SQL);
    try (PreparedStatement preparedStatement = connection.prepareStatement(PREPARED_SELECT_SQL)) {
      preparedStatement.setInt(1, 1);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        printResultSet(resultSet);
      }
    }

    System.out.println("\nPreparedStatement.executeUpdate: " + PREPARED_UPDATE_SQL);
    try (PreparedStatement preparedStatement = connection.prepareStatement(PREPARED_UPDATE_SQL)) {
      preparedStatement.setInt(1, 1);
      preparedStatement.setInt(2, 1);
      int updated = preparedStatement.executeUpdate();
      System.out.println("Updated rows: " + updated);
    }

    System.out.println("\nPreparedStatement.execute (update): " + PREPARED_UPDATE_SQL);
    try (PreparedStatement preparedStatement = connection.prepareStatement(PREPARED_UPDATE_SQL)) {
      preparedStatement.setInt(1, 1);
      preparedStatement.setInt(2, 1);
      boolean hasResultSet = preparedStatement.execute();
      if (!hasResultSet) {
        System.out.println("Update count: " + preparedStatement.getUpdateCount());
      }
    }
  }

  private static void printResultSet(ResultSet resultSet) throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    int rowCount = 0;
    while (resultSet.next()) {
      StringBuilder row = new StringBuilder();
      for (int i = 1; i <= columnCount; i++) {
        if (i > 1) {
          row.append(" | ");
        }
        row.append(metaData.getColumnLabel(i)).append("=").append(resultSet.getObject(i));
      }
      System.out.println(row);
      rowCount++;
    }
    if (rowCount == 0) {
      System.out.println("(no rows)");
    }
  }
}
