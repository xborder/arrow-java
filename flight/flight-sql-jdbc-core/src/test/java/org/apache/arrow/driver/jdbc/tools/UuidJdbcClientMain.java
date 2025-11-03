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
package org.apache.arrow.driver.jdbc.tools;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;

/** Simple CLI to query the mock UUID server via JDBC. */
public final class UuidJdbcClientMain {
  public static void main(String[] args) throws Exception {
//    if (args.length < 1) {
//      System.err.println("Usage: UuidJdbcClientMain <port>");
//      System.exit(2);
//    }
//    int port = Integer.parseInt(args[0]);

    // Ensure driver class is loaded and static init runs
//    Class.forName(ArrowFlightJdbcDriver.class.getName());

//    String url = String.format("jdbc:arrow-flight-sql://localhost:%d", 32010);
//    String url = String.format("jdbc:dremio:direct=localhost:%d", 31010);
//    String url = String.format("jdbc:dremio:direct=dremio-clients-demo.test.drem.io:%d", 31010);

//    String url = String.format("jdbc:arrow-flight-sql://data.dev.dremio.site:%d", 443);
//    String url = "jdbc:arrow-flight-sql://dremio-clients-demo.test.drem.io:32010/";
    String url = "jdbc:arrow-flight-sql://172.25.1.64:32010/";
    Properties props = new Properties();
    props.setProperty("user", "dremio");
    props.setProperty("password", "dremio123");
    props.setProperty("useEncryption", "false");
//    props.setProperty("token", "RZ2tY%2BW0SIKjXh8imqL15WccJ2Xnm3%2Fnp8dxuSSTbU5WS1cDvgDUElwrmj9QUg%3D%3D");
//    props.setProperty("catalog", "7480f90b-b3bb-48bc-8fdf-e9bb507688e3");// server is insecure
//    props.setProperty("user", "clients_team");
//    props.setProperty("password", "m6w8b7ustRMrkf7");

    try (Connection conn = DriverManager.getConnection(url, props);
        Statement stmt = conn.createStatement();
//        ResultSet rs = stmt.executeQuery("SELECT state, loc AS \"_loc\" FROM \"Samples.samples.dremio.com\".\"zips.json\"")
        ResultSet rs = stmt.executeQuery("COPY INTO   hdfscopyinto.tmp.copy_into_transformation_binary_target_csv_5664 FROM        (SELECT id, BIT_LENGTH(100) AS int_col1, BIN(100) AS int_col2, bigint_col1, bigint_col2, decimal_col1, decimal_col2, double_col1, double_col2, float_col1, float_col2 FROM   '@s3/\"qa1.dremio.com\"/testdata/\"copy_into_select\"/csv/\"numeric_type_columns\"/\"dremio_numbers_10rows.csv\"') FILE_FORMAT 'csv' (ON_ERROR 'abort');")

    ) {
      int idx = 0;

      var m = rs.getMetaData();
      System.out.println("col count: " + m.getColumnCount());
      System.out.println("col 1: " + m.getColumnLabel(1));
      System.out.println("col 2: " + m.getColumnLabel(2));
      while (rs.next()) {
//        Array arr = rs.getArray(2);
        Object arr = rs.getObject(2);
        System.out.println(arr instanceof Array);
//        var r = arr.getResultSet();
//        var rmd = r.getMetaData();
//        System.out.println(rmd.getColumnCount());
//        System.out.println(rmd.getColumnLabel(1));
//        UUID u = rs.getObject(2, UUID.class);
//        System.out.printf("row %d: %s%n", idx, u);
//        System.out.printf("row %d: %s%n", idx, arr.toString());
        idx++;
      }
    }
  }
}
