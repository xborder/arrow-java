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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for reading compressed IPC data through the Flight JDBC driver.
 *
 * <p>This test verifies that the Flight JDBC driver can properly decompress compressed Arrow IPC
 * streams sent by a Flight SQL server. This addresses GitHub issue #926 where the JDBC driver was
 * missing compression support.
 */
public class ZstdCompressedParquetDataTest {

  private static final String TABLE_NAME = "ZSTD_COMPRESSED_PARQUET_DATA";
  private static final String PARQUET_DATA_QUERY = "SELECT * FROM " + TABLE_NAME;

  private ParquetFlightSqlServer server;
  private Connection connection;

  // Expected test data
  private static final int ROW_COUNT = 100;
  private static final long[] EXPECTED_IDS = new long[ROW_COUNT];
  private static final String[] EXPECTED_NAMES = new String[ROW_COUNT];
  private static final int[] EXPECTED_VALUES = new int[ROW_COUNT];
  private static final double[] EXPECTED_SCORES = new double[ROW_COUNT];
  private static final byte[][] EXPECTED_BINARY_DATA = new byte[ROW_COUNT][];

  static {
    for (int i = 0; i < ROW_COUNT; i++) {
      EXPECTED_IDS[i] = 1000L + i;
      EXPECTED_NAMES[i] = "Record_" + i;
      EXPECTED_VALUES[i] = i * 10;
      EXPECTED_SCORES[i] = i * 1.5;
      EXPECTED_BINARY_DATA[i] = ("binary_" + i).getBytes();
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    // Start embedded server with ZSTD compression enabled (default)
    server = new ParquetFlightSqlServer(0, null);
    server.start();

    String jdbcUrl =
        String.format(
            "jdbc:arrow-flight-sql://localhost:%d/?useEncryption=false", server.getPort());
    connection = DriverManager.getConnection(jdbcUrl);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (connection != null) {
      connection.close();
    }
    if (server != null) {
      server.close();
    }
  }

  private void setUpWithCodec(CompressionUtil.CodecType codecType) throws Exception {
    tearDown();
    server = new ParquetFlightSqlServer(0, null, codecType);
    server.start();

    String jdbcUrl =
        String.format(
            "jdbc:arrow-flight-sql://localhost:%d/?useEncryption=false", server.getPort());
    connection = DriverManager.getConnection(jdbcUrl);
  }

  @Test
  public void testReadZstdCompressedParquetData() throws SQLException {
    // Test reading data that simulates ZSTD-compressed Parquet data through Flight SQL
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(PARQUET_DATA_QUERY)) {

      // Verify metadata
      assertEquals(5, resultSet.getMetaData().getColumnCount());
      assertEquals("id", resultSet.getMetaData().getColumnName(1));
      assertEquals("name", resultSet.getMetaData().getColumnName(2));
      assertEquals("value", resultSet.getMetaData().getColumnName(3));
      assertEquals("score", resultSet.getMetaData().getColumnName(4));
      assertEquals("binary_data", resultSet.getMetaData().getColumnName(5));

      // Collect and verify all rows
      List<Long> actualIds = new ArrayList<>();
      List<String> actualNames = new ArrayList<>();
      List<Integer> actualValues = new ArrayList<>();
      List<Double> actualScores = new ArrayList<>();
      List<byte[]> actualBinaryData = new ArrayList<>();

      while (resultSet.next()) {
        actualIds.add(resultSet.getLong("id"));
        actualNames.add(resultSet.getString("name"));
        actualValues.add(resultSet.getInt("value"));
        actualScores.add(resultSet.getDouble("score"));
        actualBinaryData.add(resultSet.getBytes("binary_data"));
      }

      // Verify row count
      assertEquals(ROW_COUNT, actualIds.size(), "Row count mismatch");

      // Verify all data matches expected values
      for (int i = 0; i < ROW_COUNT; i++) {
        assertEquals(EXPECTED_IDS[i], actualIds.get(i), "ID mismatch at row " + i);
        assertEquals(EXPECTED_NAMES[i], actualNames.get(i), "Name mismatch at row " + i);
        assertEquals(EXPECTED_VALUES[i], actualValues.get(i), "Value mismatch at row " + i);
        assertEquals(EXPECTED_SCORES[i], actualScores.get(i), 0.001, "Score mismatch at row " + i);
        assertArrayEquals(
            EXPECTED_BINARY_DATA[i], actualBinaryData.get(i), "Binary data mismatch at row " + i);
      }
    }
  }

  @Test
  public void testReadZstdCompressedParquetDataWithColumnSelection() throws SQLException {
    // Test reading specific columns (common pattern when reading Parquet data)
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(PARQUET_DATA_QUERY)) {

      int rowCount = 0;
      while (resultSet.next()) {
        // Read only id and name columns
        long id = resultSet.getLong(1);
        String name = resultSet.getString(2);

        assertEquals(EXPECTED_IDS[rowCount], id);
        assertEquals(EXPECTED_NAMES[rowCount], name);
        rowCount++;
      }

      assertEquals(ROW_COUNT, rowCount);
    }
  }

  @Test
  public void testReadZstdCompressedParquetDataWithMaxRows() throws SQLException {
    // Test reading with max rows limit
    final int maxRows = 10;
    try (Statement statement = connection.createStatement()) {
      statement.setMaxRows(maxRows);

      try (ResultSet resultSet = statement.executeQuery(PARQUET_DATA_QUERY)) {
        int rowCount = 0;
        while (resultSet.next()) {
          assertTrue(rowCount < maxRows, "Should not exceed max rows");
          assertEquals(EXPECTED_IDS[rowCount], resultSet.getLong("id"));
          rowCount++;
        }

        assertEquals(maxRows, rowCount, "Should return exactly max rows");
      }
    }
  }

  @Test
  public void testReadLz4CompressedData() throws Exception {
    // Test reading LZ4_FRAME compressed data
    setUpWithCodec(CompressionUtil.CodecType.LZ4_FRAME);

    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(PARQUET_DATA_QUERY)) {

      List<Long> actualIds = new ArrayList<>();
      List<String> actualNames = new ArrayList<>();

      while (resultSet.next()) {
        actualIds.add(resultSet.getLong("id"));
        actualNames.add(resultSet.getString("name"));
      }

      assertEquals(ROW_COUNT, actualIds.size(), "Row count mismatch for LZ4 compressed data");

      for (int i = 0; i < ROW_COUNT; i++) {
        assertEquals(EXPECTED_IDS[i], actualIds.get(i), "ID mismatch at row " + i);
        assertEquals(EXPECTED_NAMES[i], actualNames.get(i), "Name mismatch at row " + i);
      }
    }
  }

  @Test
  public void testReadUncompressedData() throws Exception {
    // Test reading uncompressed data (no compression codec)
    setUpWithCodec(null);

    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(PARQUET_DATA_QUERY)) {

      List<Long> actualIds = new ArrayList<>();
      List<String> actualNames = new ArrayList<>();

      while (resultSet.next()) {
        actualIds.add(resultSet.getLong("id"));
        actualNames.add(resultSet.getString("name"));
      }

      assertEquals(ROW_COUNT, actualIds.size(), "Row count mismatch for uncompressed data");

      for (int i = 0; i < ROW_COUNT; i++) {
        assertEquals(EXPECTED_IDS[i], actualIds.get(i), "ID mismatch at row " + i);
        assertEquals(EXPECTED_NAMES[i], actualNames.get(i), "Name mismatch at row " + i);
      }
    }
  }

  @Test
  public void testMultipleDataSourcesWithDifferentCompression() throws Exception {
    // Test a server with multiple data sources using different compression types
    tearDown();

    // Create server with multiple data sources
    server =
        ParquetFlightSqlServer.builder()
            .port(0)
            .addGeneratedData("ZSTD_TABLE", createTestSchema(), CompressionUtil.CodecType.ZSTD)
            .addGeneratedData("LZ4_TABLE", createTestSchema(), CompressionUtil.CodecType.LZ4_FRAME)
            .addGeneratedData("UNCOMPRESSED_TABLE", createTestSchema(), null)
            .build();
    server.start();

    String jdbcUrl =
        String.format(
            "jdbc:arrow-flight-sql://localhost:%d/?useEncryption=false", server.getPort());
    connection = DriverManager.getConnection(jdbcUrl);

    // Test ZSTD table
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM ZSTD_TABLE")) {
      int count = 0;
      while (resultSet.next()) {
        assertEquals(EXPECTED_IDS[count], resultSet.getLong("id"));
        count++;
      }
      assertEquals(ROW_COUNT, count, "ZSTD table row count mismatch");
    }

    // Test LZ4 table
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM LZ4_TABLE")) {
      int count = 0;
      while (resultSet.next()) {
        assertEquals(EXPECTED_IDS[count], resultSet.getLong("id"));
        count++;
      }
      assertEquals(ROW_COUNT, count, "LZ4 table row count mismatch");
    }

    // Test uncompressed table
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM UNCOMPRESSED_TABLE")) {
      int count = 0;
      while (resultSet.next()) {
        assertEquals(EXPECTED_IDS[count], resultSet.getLong("id"));
        count++;
      }
      assertEquals(ROW_COUNT, count, "Uncompressed table row count mismatch");
    }
  }

  private static org.apache.arrow.vector.types.pojo.Schema createTestSchema() {
    return new org.apache.arrow.vector.types.pojo.Schema(
        java.util.Arrays.asList(
            new org.apache.arrow.vector.types.pojo.Field(
                "id",
                new org.apache.arrow.vector.types.pojo.FieldType(
                    false, new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true), null),
                null),
            new org.apache.arrow.vector.types.pojo.Field(
                "name",
                new org.apache.arrow.vector.types.pojo.FieldType(
                    true, new org.apache.arrow.vector.types.pojo.ArrowType.Utf8(), null),
                null),
            new org.apache.arrow.vector.types.pojo.Field(
                "value",
                new org.apache.arrow.vector.types.pojo.FieldType(
                    true, new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true), null),
                null),
            new org.apache.arrow.vector.types.pojo.Field(
                "score",
                new org.apache.arrow.vector.types.pojo.FieldType(
                    true,
                    new org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(
                        org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE),
                    null),
                null),
            new org.apache.arrow.vector.types.pojo.Field(
                "binary_data",
                new org.apache.arrow.vector.types.pojo.FieldType(
                    true, new org.apache.arrow.vector.types.pojo.ArrowType.Binary(), null),
                null)));
  }
}
