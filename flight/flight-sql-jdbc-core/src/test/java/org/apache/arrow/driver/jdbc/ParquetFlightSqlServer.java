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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A standalone Flight SQL server that serves data from Parquet files. This server can be run as a
 * separate process and accessed by external clients.
 *
 * <p>Usage: java ParquetFlightSqlServer [port] [parquet_file_path]
 *
 * <p>If no parquet file is provided, the server will generate sample data matching the test schema.
 *
 * <p>External clients can connect using:
 * jdbc:arrow-flight-sql://localhost:{port}/?useEncryption=false
 */
public class ParquetFlightSqlServer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFlightSqlServer.class);
  private static final int DEFAULT_PORT = 31337;
  private static final int ROW_COUNT = 100;

  private final FlightServer server;
  private final BufferAllocator allocator;
  private final ParquetFlightSqlProducer producer;

  /** Creates a server with a single data source using the specified compression. */
  public ParquetFlightSqlServer(int port, File parquetFile) throws Exception {
    this(port, parquetFile, CompressionUtil.CodecType.ZSTD);
  }

  /** Creates a server with a single data source using the specified compression. */
  public ParquetFlightSqlServer(int port, File parquetFile, CompressionUtil.CodecType codecType)
      throws Exception {
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    Location location = Location.forGrpcInsecure("localhost", port);
    this.producer = new ParquetFlightSqlProducer(allocator, location);

    // Register the data source with compression
    Schema schema = createTestSchema();
    CompressionCodec codec =
        codecType != null ? CompressionCodec.Factory.INSTANCE.createCodec(codecType) : null;

    if (parquetFile != null && parquetFile.exists()) {
      producer.registerParquetFile("ZSTD_COMPRESSED_PARQUET_DATA", parquetFile, schema, codec);
      LOGGER.info("Registered Parquet file: {}", parquetFile.getAbsolutePath());
    } else {
      byte[] arrowData = generateTestArrowData(schema);
      producer.registerArrowData("ZSTD_COMPRESSED_PARQUET_DATA", arrowData, schema, codec);
      LOGGER.info("Registered generated test data with {} compression", codecType);
    }

    this.server = FlightServer.builder(allocator, location, producer).build();
  }

  /** Creates a server using the Builder configuration. */
  private ParquetFlightSqlServer(Builder builder) throws Exception {
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    Location location = Location.forGrpcInsecure("localhost", builder.port);
    this.producer = new ParquetFlightSqlProducer(allocator, location);

    // Register all data sources
    for (DataSourceConfig config : builder.dataSources) {
      CompressionCodec codec =
          config.codecType != null
              ? CompressionCodec.Factory.INSTANCE.createCodec(config.codecType)
              : null;
      if (config.parquetFile != null && config.parquetFile.exists()) {
        producer.registerParquetFile(config.tableName, config.parquetFile, config.schema, codec);
        LOGGER.info(
            "Registered Parquet file '{}' as table '{}' with {} compression",
            config.parquetFile.getName(),
            config.tableName,
            config.codecType);
      } else if (config.arrowData != null) {
        producer.registerArrowData(config.tableName, config.arrowData, config.schema, codec);
        LOGGER.info(
            "Registered Arrow data as table '{}' with {} compression",
            config.tableName,
            config.codecType);
      } else {
        byte[] arrowData = generateTestArrowData(config.schema);
        producer.registerArrowData(config.tableName, arrowData, config.schema, codec);
        LOGGER.info(
            "Registered generated data as table '{}' with {} compression",
            config.tableName,
            config.codecType);
      }
    }

    this.server = FlightServer.builder(allocator, location, producer).build();
  }

  /** Returns a new Builder for configuring the server. */
  public static Builder builder() {
    return new Builder();
  }

  public void start() throws Exception {
    server.start();
    LOGGER.info("Flight SQL Server started on port {}", server.getPort());
    LOGGER.info(
        "Connect using: jdbc:arrow-flight-sql://localhost:{}/?useEncryption=false",
        server.getPort());
  }

  public int getPort() {
    return server.getPort();
  }

  public void awaitTermination() throws InterruptedException {
    server.awaitTermination();
  }

  @Override
  public void close() throws Exception {
    server.close();
    producer.close();
    allocator.close();
    LOGGER.info("Flight SQL Server stopped");
  }

  private static Schema createTestSchema() {
    return new Schema(
        java.util.Arrays.asList(
            new Field("id", new FieldType(false, new ArrowType.Int(64, true), null), null),
            new Field("name", new FieldType(true, new ArrowType.Utf8(), null), null),
            new Field("value", new FieldType(true, new ArrowType.Int(32, true), null), null),
            new Field(
                "score",
                new FieldType(
                    true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null),
                null),
            new Field("binary_data", new FieldType(true, new ArrowType.Binary(), null), null)));
  }

  private byte[] generateTestArrowData(Schema schema) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.allocateNew();

      BigIntVector idVector = (BigIntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");
      IntVector valueVector = (IntVector) root.getVector("value");
      Float8Vector scoreVector = (Float8Vector) root.getVector("score");
      VarBinaryVector binaryVector = (VarBinaryVector) root.getVector("binary_data");

      for (int i = 0; i < ROW_COUNT; i++) {
        idVector.setSafe(i, 1000L + i);
        nameVector.setSafe(i, new Text("Record_" + i));
        valueVector.setSafe(i, i * 10);
        scoreVector.setSafe(i, i * 1.5);
        binaryVector.setSafe(i, ("binary_" + i).getBytes());
      }
      root.setRowCount(ROW_COUNT);

      try (ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out))) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
    }
    return out.toByteArray();
  }

  /**
   * Main method demonstrating a server with multiple data sources using different compression.
   *
   * <p>Usage: java ParquetFlightSqlServer [port]
   *
   * <p>This creates two tables: - ZSTD_DATA: Uses ZSTD compression - LZ4_DATA: Uses LZ4_FRAME
   * compression
   */
  public static void main(String[] args) throws Exception {
    int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;

    // Create server with multiple data sources using different compression
    try (ParquetFlightSqlServer server =
        ParquetFlightSqlServer.builder()
            .port(port)
            .addGeneratedData("ZSTD_DATA", createTestSchema(), CompressionUtil.CodecType.ZSTD)
            .addGeneratedData("LZ4_DATA", createTestSchema(), CompressionUtil.CodecType.LZ4_FRAME)
            .addGeneratedData("UNCOMPRESSED_DATA", createTestSchema(), null)
            .build()) {
      server.start();
      LOGGER.info("Available tables:");
      LOGGER.info("  - ZSTD_DATA (ZSTD compression)");
      LOGGER.info("  - LZ4_DATA (LZ4_FRAME compression)");
      LOGGER.info("  - UNCOMPRESSED_DATA (no compression)");

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      server.close();
                    } catch (Exception e) {
                      LOGGER.error("Error closing server", e);
                    }
                  }));
      server.awaitTermination();
    }
  }

  /** Builder for configuring ParquetFlightSqlServer with multiple data sources. */
  public static class Builder {
    private int port = DEFAULT_PORT;
    private final List<DataSourceConfig> dataSources = new ArrayList<>();

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    /** Add a Parquet file as a data source with the specified compression for streaming. */
    public Builder addParquetFile(
        String tableName, File parquetFile, Schema schema, CompressionUtil.CodecType codecType) {
      dataSources.add(new DataSourceConfig(tableName, parquetFile, null, schema, codecType));
      return this;
    }

    /** Add generated test data as a data source with the specified compression for streaming. */
    public Builder addGeneratedData(
        String tableName, Schema schema, CompressionUtil.CodecType codecType) {
      dataSources.add(new DataSourceConfig(tableName, null, null, schema, codecType));
      return this;
    }

    /** Add Arrow IPC data as a data source with the specified compression for streaming. */
    public Builder addArrowData(
        String tableName, byte[] arrowData, Schema schema, CompressionUtil.CodecType codecType) {
      dataSources.add(new DataSourceConfig(tableName, null, arrowData, schema, codecType));
      return this;
    }

    public ParquetFlightSqlServer build() throws Exception {
      if (dataSources.isEmpty()) {
        // Default: add a single ZSTD-compressed data source
        dataSources.add(
            new DataSourceConfig(
                "ZSTD_COMPRESSED_PARQUET_DATA",
                null,
                null,
                createTestSchema(),
                CompressionUtil.CodecType.ZSTD));
      }
      return new ParquetFlightSqlServer(this);
    }
  }

  /** Configuration for a data source. */
  private static class DataSourceConfig {
    final String tableName;
    final File parquetFile;
    final byte[] arrowData;
    final Schema schema;
    final CompressionUtil.CodecType codecType;

    DataSourceConfig(
        String tableName,
        File parquetFile,
        byte[] arrowData,
        Schema schema,
        CompressionUtil.CodecType codecType) {
      this.tableName = tableName;
      this.parquetFile = parquetFile;
      this.arrowData = arrowData;
      this.schema = schema;
      this.codecType = codecType;
    }
  }
}
