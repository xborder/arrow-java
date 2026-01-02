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

import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.util.UUID.randomUUID;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.BasicFlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flight SQL producer that serves data from Parquet files. This producer reads Parquet files
 * using Arrow's native readers and serves the data via Flight SQL protocol.
 *
 * <p>Note: This implementation uses pre-converted Arrow IPC files for simplicity. In a production
 * scenario, you would use the Arrow Dataset API with JNI to read Parquet files directly.
 */
public class ParquetFlightSqlProducer extends BasicFlightSqlProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFlightSqlProducer.class);

  private final BufferAllocator allocator;
  private final Location location;
  private final Map<String, ParquetDataSource> dataSources = new ConcurrentHashMap<>();
  private final Map<ByteString, StatementInfo> statements = new ConcurrentHashMap<>();
  private final Map<ByteString, String> preparedStatements = new ConcurrentHashMap<>();
  private CompressionCodec defaultCompressionCodec;

  public ParquetFlightSqlProducer(BufferAllocator allocator, Location location) {
    this.allocator = allocator;
    this.location = location;
  }

  /** Set the default compression codec to use when streaming data. */
  public void setCompressionCodec(CompressionCodec codec) {
    this.defaultCompressionCodec = codec;
  }

  /** Register a Parquet file as a data source accessible via SQL query. */
  public void registerParquetFile(String tableName, File parquetFile, Schema schema) {
    registerParquetFile(tableName, parquetFile, schema, null);
  }

  /** Register a Parquet file with a specific compression codec for streaming. */
  public void registerParquetFile(
      String tableName, File parquetFile, Schema schema, CompressionCodec codec) {
    dataSources.put(tableName.toUpperCase(), new ParquetDataSource(parquetFile, schema, codec));
    LOGGER.info(
        "Registered Parquet file '{}' as table '{}' with {} compression",
        parquetFile.getName(),
        tableName,
        codec != null ? codec.getCodecType() : "default");
  }

  /** Register Arrow IPC data as a data source (for testing without native Parquet reader). */
  public void registerArrowData(String tableName, byte[] arrowIpcData, Schema schema) {
    registerArrowData(tableName, arrowIpcData, schema, null);
  }

  /** Register Arrow IPC data with a specific compression codec for streaming. */
  public void registerArrowData(
      String tableName, byte[] arrowIpcData, Schema schema, CompressionCodec codec) {
    dataSources.put(tableName.toUpperCase(), new ParquetDataSource(arrowIpcData, schema, codec));
    LOGGER.info(
        "Registered Arrow data as table '{}' with {} compression",
        tableName,
        codec != null ? codec.getCodecType() : "default");
  }

  @Override
  public FlightInfo getFlightInfoStatement(
      CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
    String query = command.getQuery();
    String tableName = extractTableName(query);

    ParquetDataSource dataSource = dataSources.get(tableName.toUpperCase());
    if (dataSource == null) {
      throw CallStatus.NOT_FOUND
          .withDescription("Table not found: " + tableName)
          .toRuntimeException();
    }

    ByteString handle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
    statements.put(handle, new StatementInfo(tableName, dataSource));

    TicketStatementQuery ticket =
        TicketStatementQuery.newBuilder().setStatementHandle(handle).build();

    return new FlightInfo(
        dataSource.schema,
        descriptor,
        Collections.singletonList(new FlightEndpoint(new Ticket(pack(ticket).toByteArray()))),
        -1,
        -1);
  }

  @Override
  public void getStreamStatement(
      TicketStatementQuery ticket, CallContext context, ServerStreamListener listener) {
    ByteString handle = ticket.getStatementHandle();
    StatementInfo info = statements.get(handle);

    if (info == null) {
      listener.error(
          CallStatus.NOT_FOUND.withDescription("Statement not found").toRuntimeException());
      return;
    }

    try {
      streamData(info.dataSource, listener);
    } catch (Exception e) {
      listener.error(CallStatus.INTERNAL.withCause(e).toRuntimeException());
    }
  }

  private void streamData(ParquetDataSource dataSource, ServerStreamListener listener)
      throws Exception {
    if (dataSource.arrowIpcData != null) {
      // Use data source specific codec, or fall back to default
      CompressionCodec codec =
          dataSource.compressionCodec != null
              ? dataSource.compressionCodec
              : defaultCompressionCodec;
      streamArrowIpcData(dataSource.arrowIpcData, listener, codec);
    } else {
      throw CallStatus.UNIMPLEMENTED
          .withDescription("Direct Parquet reading requires native libraries")
          .toRuntimeException();
    }
  }

  private void streamArrowIpcData(
      byte[] arrowIpcData, ServerStreamListener listener, CompressionCodec codec) throws Exception {
    try (SeekableReadChannel channel =
            new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(arrowIpcData));
        ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      listener.start(root, null, IpcOption.DEFAULT, codec);

      while (reader.loadNextBatch()) {
        listener.putNext();
      }
    } finally {
      listener.completed();
    }
  }

  @Override
  public void createPreparedStatement(
      ActionCreatePreparedStatementRequest request,
      CallContext context,
      StreamListener<Result> listener) {
    String query = request.getQuery();
    String tableName = extractTableName(query);

    ParquetDataSource dataSource = dataSources.get(tableName.toUpperCase());
    if (dataSource == null) {
      listener.onError(
          CallStatus.NOT_FOUND
              .withDescription("Table not found: " + tableName)
              .toRuntimeException());
      return;
    }

    ByteString handle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
    preparedStatements.put(handle, query);
    statements.put(handle, new StatementInfo(tableName, dataSource));

    ActionCreatePreparedStatementResult.Builder resultBuilder =
        ActionCreatePreparedStatementResult.newBuilder()
            .setPreparedStatementHandle(handle)
            .setDatasetSchema(ByteString.copyFrom(dataSource.schema.serializeAsMessage()));

    listener.onNext(new Result(Any.pack(resultBuilder.build()).toByteArray()));
    listener.onCompleted();
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(
      CommandPreparedStatementQuery command, CallContext context, FlightDescriptor descriptor) {
    ByteString handle = command.getPreparedStatementHandle();
    StatementInfo info = statements.get(handle);

    if (info == null) {
      throw CallStatus.NOT_FOUND
          .withDescription("Prepared statement not found")
          .toRuntimeException();
    }

    // Use CommandPreparedStatementQuery as the ticket so getStream routes to
    // getStreamPreparedStatement
    return new FlightInfo(
        info.dataSource.schema,
        descriptor,
        Collections.singletonList(
            new FlightEndpoint(
                new Ticket(
                    pack(CommandPreparedStatementQuery.newBuilder()
                            .setPreparedStatementHandle(handle)
                            .build())
                        .toByteArray()))),
        -1,
        -1);
  }

  @Override
  public void getStreamPreparedStatement(
      CommandPreparedStatementQuery command, CallContext context, ServerStreamListener listener) {
    ByteString handle = command.getPreparedStatementHandle();
    StatementInfo info = statements.get(handle);

    if (info == null) {
      listener.error(
          CallStatus.NOT_FOUND
              .withDescription("Prepared statement not found")
              .toRuntimeException());
      return;
    }

    try {
      streamData(info.dataSource, listener);
    } catch (Exception e) {
      listener.error(CallStatus.INTERNAL.withCause(e).toRuntimeException());
    }
  }

  @Override
  public void closePreparedStatement(
      ActionClosePreparedStatementRequest request,
      CallContext context,
      StreamListener<Result> listener) {
    ByteString handle = request.getPreparedStatementHandle();
    preparedStatements.remove(handle);
    statements.remove(handle);
    listener.onCompleted();
  }

  private String extractTableName(String query) {
    String upperQuery = query.toUpperCase().trim();
    int fromIndex = upperQuery.indexOf("FROM");
    if (fromIndex == -1) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("Invalid query: " + query)
          .toRuntimeException();
    }
    String afterFrom = query.substring(fromIndex + 4).trim();
    String[] parts = afterFrom.split("\\s+");
    return parts[0];
  }

  @Override
  protected <T extends Message> List<FlightEndpoint> determineEndpoints(
      T request, FlightDescriptor flightDescriptor, Schema schema) {
    return Collections.singletonList(
        new FlightEndpoint(new Ticket(flightDescriptor.getCommand()), location));
  }

  @Override
  public void close() throws Exception {
    statements.clear();
    dataSources.clear();
  }

  private static class ParquetDataSource {
    final File parquetFile;
    final byte[] arrowIpcData;
    final Schema schema;
    final CompressionCodec compressionCodec;

    ParquetDataSource(File parquetFile, Schema schema, CompressionCodec codec) {
      this.parquetFile = parquetFile;
      this.arrowIpcData = null;
      this.schema = schema;
      this.compressionCodec = codec;
    }

    ParquetDataSource(byte[] arrowIpcData, Schema schema, CompressionCodec codec) {
      this.parquetFile = null;
      this.arrowIpcData = arrowIpcData;
      this.schema = schema;
      this.compressionCodec = codec;
    }
  }

  private static class StatementInfo {
    final String tableName;
    final ParquetDataSource dataSource;

    StatementInfo(String tableName, ParquetDataSource dataSource) {
      this.tableName = tableName;
      this.dataSource = dataSource;
    }
  }
}
