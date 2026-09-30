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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Runs a Flight SQL query with optional negotiated IPC compression. */
public final class FlightSqlIpcCompressionExample {
  private FlightSqlIpcCompressionExample() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println(
          "Usage: FlightSqlIpcCompressionExample <jdbc-url> <sql> <none|lz4_frame|zstd>");
      System.exit(2);
    }

    final Properties properties = new Properties();
    if (!"none".equalsIgnoreCase(args[2])) {
      properties.setProperty("ipcCompression", args[2]);
    }

    final long started = System.nanoTime();
    long rows = 0;
    int columns;
    try (Connection connection = DriverManager.getConnection(args[0], properties);
        Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(args[1])) {
      final ResultSetMetaData metadata = results.getMetaData();
      columns = metadata.getColumnCount();
      while (results.next()) {
        rows++;
        for (int column = 1; column <= columns; column++) {
          results.getObject(column);
        }
      }
    }
    final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    System.out.printf(
        "compression=%s rows=%d columns=%d elapsed_ms=%d%n", args[2], rows, columns, elapsedMillis);
  }
}
