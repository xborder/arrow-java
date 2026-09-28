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
package org.apache.arrow.flight.sql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.FlightClient.ExchangeReaderWriter;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.sql.FlightSqlClient.ExecuteIngestOptions;
import org.apache.arrow.flight.sql.FlightSqlClient.Transaction;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginTransactionRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginTransactionResult;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionEndTransactionRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.DoPutPreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.DoPutUpdateResult;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * <b>Experimental.</b> A Flight SQL client that runs every statement with exactly one {@code
 * DoExchange} call.
 *
 * <p>The server must serve Flight SQL over DoExchange, for example by wrapping its producer in a
 * {@link FlightSqlExchangeProducer}. Compared to {@link FlightSqlClient}:
 *
 * <ul>
 *   <li>queries return their results on the call that submitted them (no {@code GetFlightInfo}
 *       followed by {@code DoGet});
 *   <li>parameterized statements do not need an explicit prepared statement: the parameters are
 *       streamed on the same call and the server prepares, binds, executes and closes the statement
 *       before the call ends;
 *   <li>creating, executing and closing a reusable prepared statement take one call each.
 * </ul>
 *
 * <p>Methods that return a {@link FlightStream} hand over the call's reader: the caller iterates it
 * with {@link FlightStream#next()} and must close it.
 */
public class FlightSqlExchangeClient implements AutoCloseable {
  private final FlightClient client;

  /**
   * Creates a client that talks to the server behind {@code client}.
   *
   * @param client the Flight client to issue DoExchange calls with; closed by {@link #close()}.
   */
  public FlightSqlExchangeClient(FlightClient client) {
    this.client = Objects.requireNonNull(client, "client");
  }

  /**
   * Executes a query and returns its results, which arrive on the same call.
   *
   * @param query the SQL query.
   * @param options RPC-layer hints for this call.
   * @return the result set; the caller must close it.
   */
  public FlightStream execute(String query, CallOption... options) {
    return execute(query, null, null, options);
  }

  /**
   * Executes a parameterized query within one call: the server prepares it, binds {@code
   * parameters}, executes it and closes it again.
   *
   * @param query the SQL query, with parameter markers.
   * @param parameters the parameter values, one row per parameter set; may be null.
   * @param options RPC-layer hints for this call.
   * @return the result set; the caller must close it.
   */
  public FlightStream execute(String query, VectorSchemaRoot parameters, CallOption... options) {
    return execute(query, parameters, null, options);
  }

  /**
   * Executes a (possibly parameterized) query as part of a transaction, within one call.
   *
   * @param query the SQL query.
   * @param parameters the parameter values, one row per parameter set; may be null.
   * @param transaction the transaction the query belongs to; may be null for auto-commit.
   * @param options RPC-layer hints for this call.
   * @return the result set; the caller must close it.
   */
  public FlightStream execute(
      String query, VectorSchemaRoot parameters, Transaction transaction, CallOption... options) {
    final CommandStatementQuery.Builder command =
        CommandStatementQuery.newBuilder().setQuery(query);
    if (transaction != null) {
      command.setTransactionId(ByteString.copyFrom(transaction.getTransactionId()));
    }
    return start(command.build(), parametersOrNull(parameters), options).getReader();
  }

  /**
   * Executes any Flight SQL command that produces a result set, such as {@code CommandGetTables} or
   * {@code CommandGetSqlInfo}, and returns its results.
   *
   * @param command the Flight SQL command.
   * @param options RPC-layer hints for this call.
   * @return the result set; the caller must close it.
   */
  public FlightStream executeCommand(Message command, CallOption... options) {
    return start(command, null, options).getReader();
  }

  /**
   * Executes an update statement (INSERT, UPDATE, DELETE, DDL, ...) within one call.
   *
   * @param query the SQL statement.
   * @param options RPC-layer hints for this call.
   * @return the number of affected rows, or -1 if unknown.
   */
  public long executeUpdate(String query, CallOption... options) {
    return executeUpdate(query, null, null, options);
  }

  /**
   * Executes a parameterized update statement within one call: the server prepares it, executes it
   * once per row of {@code parameters} and closes it again.
   *
   * @param query the SQL statement, with parameter markers.
   * @param parameters the parameter values, one row per execution; may be null.
   * @param options RPC-layer hints for this call.
   * @return the total number of affected rows, or -1 if unknown.
   */
  public long executeUpdate(String query, VectorSchemaRoot parameters, CallOption... options) {
    return executeUpdate(query, parameters, null, options);
  }

  /**
   * Executes a (possibly parameterized) update statement as part of a transaction, within one call.
   *
   * @param query the SQL statement.
   * @param parameters the parameter values, one row per execution; may be null.
   * @param transaction the transaction the statement belongs to; may be null for auto-commit.
   * @param options RPC-layer hints for this call.
   * @return the total number of affected rows, or -1 if unknown.
   */
  public long executeUpdate(
      String query, VectorSchemaRoot parameters, Transaction transaction, CallOption... options) {
    final CommandStatementUpdate.Builder command =
        CommandStatementUpdate.newBuilder().setQuery(query);
    if (transaction != null) {
      command.setTransactionId(ByteString.copyFrom(transaction.getTransactionId()));
    }
    return readUpdateCount(start(command.build(), parametersOrNull(parameters), options));
  }

  /**
   * Bulk-ingests {@code data} into a table within one call.
   *
   * @param data the rows to ingest.
   * @param ingestOptions the target table and how to handle its definition.
   * @param options RPC-layer hints for this call.
   * @return the number of ingested rows, or -1 if unknown.
   */
  public long executeIngest(
      VectorSchemaRoot data, ExecuteIngestOptions ingestOptions, CallOption... options) {
    return executeIngest(data, ingestOptions, null, options);
  }

  /**
   * Bulk-ingests {@code data} into a table as part of a transaction, within one call.
   *
   * @param data the rows to ingest.
   * @param ingestOptions the target table and how to handle its definition.
   * @param transaction the transaction the ingestion belongs to; may be null.
   * @param options RPC-layer hints for this call.
   * @return the number of ingested rows, or -1 if unknown.
   */
  public long executeIngest(
      VectorSchemaRoot data,
      ExecuteIngestOptions ingestOptions,
      Transaction transaction,
      CallOption... options) {
    final CommandStatementIngest.Builder command = CommandStatementIngest.newBuilder();
    if (transaction != null) {
      command.setTransactionId(ByteString.copyFrom(transaction.getTransactionId()));
    }
    ingestOptions.updateCommandBuilder(command);
    return readUpdateCount(start(command.build(), data, options));
  }

  /**
   * Creates a reusable prepared statement.
   *
   * @param query the SQL statement, with parameter markers.
   * @param options RPC-layer hints for this call.
   * @return the prepared statement; close it to release it on the server.
   */
  public PreparedStatement prepare(String query, CallOption... options) {
    return prepare(query, null, options);
  }

  /**
   * Creates a reusable prepared statement whose executions belong to a transaction.
   *
   * @param query the SQL statement, with parameter markers.
   * @param transaction the transaction the executions belong to; may be null for auto-commit.
   * @param options RPC-layer hints for this call.
   * @return the prepared statement; close it to release it on the server.
   */
  public PreparedStatement prepare(String query, Transaction transaction, CallOption... options) {
    final ActionCreatePreparedStatementRequest.Builder request =
        ActionCreatePreparedStatementRequest.newBuilder().setQuery(query);
    if (transaction != null) {
      request.setTransactionId(ByteString.copyFrom(transaction.getTransactionId()));
    }
    final ActionCreatePreparedStatementResult result =
        single(
            readResponses(start(request.build(), null, options)),
            ActionCreatePreparedStatementResult.class);
    return new PreparedStatement(result);
  }

  /**
   * Begins a transaction.
   *
   * @param options RPC-layer hints for this call.
   * @return the new transaction.
   */
  public Transaction beginTransaction(CallOption... options) {
    final ActionBeginTransactionResult result =
        single(
            readResponses(start(ActionBeginTransactionRequest.getDefaultInstance(), null, options)),
            ActionBeginTransactionResult.class);
    return new Transaction(result.getTransactionId().toByteArray());
  }

  /**
   * Commits a transaction.
   *
   * @param transaction the transaction to commit.
   * @param options RPC-layer hints for this call.
   */
  public void commit(Transaction transaction, CallOption... options) {
    endTransaction(
        transaction, ActionEndTransactionRequest.EndTransaction.END_TRANSACTION_COMMIT, options);
  }

  /**
   * Rolls back a transaction.
   *
   * @param transaction the transaction to roll back.
   * @param options RPC-layer hints for this call.
   */
  public void rollback(Transaction transaction, CallOption... options) {
    endTransaction(
        transaction, ActionEndTransactionRequest.EndTransaction.END_TRANSACTION_ROLLBACK, options);
  }

  /**
   * Runs an arbitrary Flight action, such as {@code SetSessionOptions}, within one DoExchange call.
   *
   * @param action the action to run.
   * @param options RPC-layer hints for this call.
   * @return the results of the action; empty results are omitted.
   */
  public List<Result> doAction(Action action, CallOption... options) {
    final Flight.Action.Builder request = Flight.Action.newBuilder().setType(action.getType());
    if (action.getBody() != null) {
      request.setBody(ByteString.copyFrom(action.getBody()));
    }
    final List<Result> results = new ArrayList<>();
    for (byte[] body : readMetadata(start(request.build(), null, options))) {
      results.add(new Result(body));
    }
    return results;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(client);
  }

  private void endTransaction(
      Transaction transaction,
      ActionEndTransactionRequest.EndTransaction action,
      CallOption... options) {
    final ActionEndTransactionRequest request =
        ActionEndTransactionRequest.newBuilder()
            .setTransactionId(ByteString.copyFrom(transaction.getTransactionId()))
            .setAction(action)
            .build();
    readResponses(start(request, null, options));
  }

  /**
   * Opens a DoExchange call for {@code request}, streams {@code input} (if any) and half-closes, so
   * that the server knows the whole statement right away.
   */
  private ExchangeReaderWriter start(
      Message request, VectorSchemaRoot input, CallOption... options) {
    final ExchangeReaderWriter exchange =
        client.doExchange(FlightDescriptor.command(Any.pack(request).toByteArray()), options);
    try {
      final ClientStreamListener writer = exchange.getWriter();
      if (input != null) {
        writer.start(input);
        writer.putNext();
      }
      writer.completed();
      return exchange;
    } catch (RuntimeException e) {
      AutoCloseables.close(e, exchange);
      throw e;
    }
  }

  /** Parameters are only sent when there are some; the server looks for a non-empty schema. */
  private static VectorSchemaRoot parametersOrNull(VectorSchemaRoot parameters) {
    return parameters == null || parameters.getSchema().getFields().isEmpty() ? null : parameters;
  }

  /** Reads every metadata message until the server ends the call, then closes the call. */
  private static List<byte[]> readMetadata(ExchangeReaderWriter exchange) {
    final FlightStream reader = exchange.getReader();
    try {
      final List<byte[]> messages = new ArrayList<>();
      while (reader.next()) {
        if (reader.getLatestMetadata() != null) {
          messages.add(toBytes(reader.getLatestMetadata()));
        }
      }
      return messages;
    } finally {
      AutoCloseables.closeNoChecked(reader);
    }
  }

  /** Reads the {@link Any}-packed Flight SQL responses of a call that returns no result set. */
  private static List<Any> readResponses(ExchangeReaderWriter exchange) {
    final List<Any> responses = new ArrayList<>();
    for (byte[] message : readMetadata(exchange)) {
      responses.add(FlightSqlUtils.parseOrThrow(message));
    }
    return responses;
  }

  /** Sums the {@code DoPutUpdateResult}s the server sent; -1 if any count (or all) is unknown. */
  private static long readUpdateCount(ExchangeReaderWriter exchange) {
    long total = 0;
    boolean known = false;
    for (Any response : readResponses(exchange)) {
      if (response.is(DoPutUpdateResult.class)) {
        final long count =
            FlightSqlUtils.unpackOrThrow(response, DoPutUpdateResult.class).getRecordCount();
        if (count < 0) {
          return -1;
        }
        total += count;
        known = true;
      }
    }
    return known ? total : -1;
  }

  /** Reads the one message a server sends before the result set when it binds parameters. */
  private static Any readBindResponse(FlightStream reader) {
    if (!reader.next() || reader.getLatestMetadata() == null) {
      throw CallStatus.INTERNAL
          .withDescription("Expected a DoPutPreparedStatementResult before the result set")
          .toRuntimeException();
    }
    return FlightSqlUtils.parseOrThrow(toBytes(reader.getLatestMetadata()));
  }

  private static byte[] toBytes(ArrowBuf buffer) {
    final byte[] bytes = new byte[(int) buffer.readableBytes()];
    buffer.getBytes(buffer.readerIndex(), bytes);
    return bytes;
  }

  private static <T extends Message> T single(List<Any> responses, Class<T> type) {
    for (Any response : responses) {
      if (response.is(type)) {
        return FlightSqlUtils.unpackOrThrow(response, type);
      }
    }
    throw CallStatus.INTERNAL
        .withDescription("The server did not send a " + type.getSimpleName())
        .toRuntimeException();
  }

  private static Schema deserializeSchema(ByteString bytes) {
    if (bytes.isEmpty()) {
      return new Schema(Collections.emptyList());
    }
    try {
      return MessageSerializer.deserializeSchema(
          new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes.toByteArray()))));
    } catch (IOException e) {
      throw CallStatus.INTERNAL
          .withDescription("Failed to deserialize schema")
          .withCause(e)
          .toRuntimeException();
    }
  }

  /**
   * A reusable prepared statement. Each execution takes one DoExchange call that binds the
   * parameters, executes the statement and returns its results.
   */
  public class PreparedStatement implements AutoCloseable {
    private final ActionCreatePreparedStatementResult prepared;
    private ByteString handle;
    private boolean closed;

    PreparedStatement(ActionCreatePreparedStatementResult prepared) {
      this.prepared = prepared;
      this.handle = prepared.getPreparedStatementHandle();
    }

    /**
     * Returns the schema of the result set, as described by the server when preparing.
     *
     * @return the result set schema; empty if the server did not provide one.
     */
    public Schema getResultSetSchema() {
      return deserializeSchema(prepared.getDatasetSchema());
    }

    /**
     * Returns the schema of the parameters, as described by the server when preparing.
     *
     * @return the parameter schema; empty if the statement has no parameters.
     */
    public Schema getParameterSchema() {
      return deserializeSchema(prepared.getParameterSchema());
    }

    /**
     * Returns whether the server said this statement is an update.
     *
     * @return true for an update, false for a query, null if the server did not say.
     */
    public Boolean isUpdate() {
      return prepared.hasIsUpdate() ? prepared.getIsUpdate() : null;
    }

    /**
     * Binds {@code parameters} and executes the query in one call.
     *
     * @param parameters the parameter values, one row per parameter set; may be null.
     * @param options RPC-layer hints for this call.
     * @return the result set; the caller must close it.
     */
    public FlightStream execute(VectorSchemaRoot parameters, CallOption... options) {
      checkOpen();
      final VectorSchemaRoot input = parametersOrNull(parameters);
      final ExchangeReaderWriter exchange =
          start(
              CommandPreparedStatementQuery.newBuilder().setPreparedStatementHandle(handle).build(),
              input,
              options);
      final FlightStream reader = exchange.getReader();
      if (input != null) {
        try {
          final DoPutPreparedStatementResult bound =
              FlightSqlUtils.unpackOrThrow(
                  readBindResponse(reader), DoPutPreparedStatementResult.class);
          // A stateless server may hand out a new handle that encodes the bound parameters.
          if (bound.hasPreparedStatementHandle() && !bound.getPreparedStatementHandle().isEmpty()) {
            handle = bound.getPreparedStatementHandle();
          }
        } catch (RuntimeException e) {
          AutoCloseables.close(e, reader);
          throw e;
        }
      }
      return reader;
    }

    /**
     * Executes the statement as an update, once per row of {@code parameters}, in one call.
     *
     * @param parameters the parameter values, one row per execution; may be null.
     * @param options RPC-layer hints for this call.
     * @return the total number of affected rows, or -1 if unknown.
     */
    public long executeUpdate(VectorSchemaRoot parameters, CallOption... options) {
      checkOpen();
      final CommandPreparedStatementUpdate command =
          CommandPreparedStatementUpdate.newBuilder().setPreparedStatementHandle(handle).build();
      if (parameters != null) {
        return readUpdateCount(start(command, parameters, options));
      }
      // Like FlightSqlClient, send an empty batch so that the server executes the statement once.
      try (VectorSchemaRoot noParameters = VectorSchemaRoot.of()) {
        return readUpdateCount(start(command, noParameters, options));
      }
    }

    /**
     * Returns whether the prepared statement was closed.
     *
     * @return true once {@link #close()} was called.
     */
    public boolean isClosed() {
      return closed;
    }

    /**
     * Releases the prepared statement on the server, in one call.
     *
     * @param options RPC-layer hints for this call.
     */
    public void close(CallOption... options) {
      if (closed) {
        return;
      }
      closed = true;
      readResponses(
          start(
              ActionClosePreparedStatementRequest.newBuilder()
                  .setPreparedStatementHandle(handle)
                  .build(),
              null,
              options));
    }

    @Override
    public void close() {
      close(new CallOption[0]);
    }

    private void checkOpen() {
      Preconditions.checkState(!closed, "Statement closed");
    }
  }
}
