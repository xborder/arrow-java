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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PollInfo;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginSavepointRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginTransactionRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedSubstraitPlanRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionEndSavepointRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionEndTransactionRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCrossReference;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetXdbcTypeInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementSubstraitPlan;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.DoPutPreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.DoPutUpdateResult;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * <b>Experimental.</b> Serves Flight SQL over a single {@code DoExchange} call per statement.
 *
 * <p>Classic Flight SQL spreads a statement over several RPCs: {@code GetFlightInfo} and then
 * {@code DoGet} for queries, {@code DoPut} for updates and for binding parameters, and {@code
 * DoAction} to create and close prepared statements. This decorator lets a client run the same
 * Flight SQL commands with one {@code DoExchange} call instead:
 *
 * <ol>
 *   <li>The client opens the exchange with a command {@link FlightDescriptor} holding the {@link
 *       Any}-packed Flight SQL request: a {@code Command*} message, a Flight SQL {@code
 *       Action*Request} message, or a Flight {@code Action}.
 *   <li>The client optionally streams one Arrow stream (parameter values, one row per parameter
 *       set, or data to ingest) and then half-closes its side of the call.
 *   <li>The server answers on the same call with metadata-only messages carrying {@link Any}-packed
 *       Flight SQL results ({@code DoPutUpdateResult}, {@code DoPutPreparedStatementResult}, action
 *       results) and/or a single Arrow stream with the result set. The call's status reports
 *       success or failure of the statement.
 * </ol>
 *
 * <p>{@code CommandStatementQuery} and {@code CommandStatementUpdate} also accept parameters: the
 * server then prepares the statement, binds the parameters, executes it and closes it within the
 * call, which replaces five sequential RPCs with one.
 *
 * <p>No new producer methods are needed: each exchange is translated into in-process calls to the
 * delegate's existing {@code getFlightInfo}, {@code getStream}, {@code acceptPut} and {@code
 * doAction} handlers. Exchanges whose descriptor is not a Flight SQL request are handed to the
 * delegate's {@code doExchange} unchanged. See {@link FlightSqlExchangeClient} for the client side.
 */
public class FlightSqlExchangeProducer implements FlightProducer, AutoCloseable {
  private static final String FLIGHT_SQL_PACKAGE = "arrow.flight.protocol.sql.";

  /** Flight SQL action request messages, mapped to the type of the action they are sent with. */
  private static final Map<String, String> ACTION_TYPES =
      Map.of(
          name(ActionCreatePreparedStatementRequest.getDescriptor()),
          FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(),
          name(ActionCreatePreparedSubstraitPlanRequest.getDescriptor()),
          FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_SUBSTRAIT_PLAN.getType(),
          name(ActionClosePreparedStatementRequest.getDescriptor()),
          FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT.getType(),
          name(ActionBeginTransactionRequest.getDescriptor()),
          FlightSqlUtils.FLIGHT_SQL_BEGIN_TRANSACTION.getType(),
          name(ActionEndTransactionRequest.getDescriptor()),
          FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION.getType(),
          name(ActionBeginSavepointRequest.getDescriptor()),
          FlightSqlUtils.FLIGHT_SQL_BEGIN_SAVEPOINT.getType(),
          name(ActionEndSavepointRequest.getDescriptor()),
          FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT.getType());

  /** Flight SQL commands that classic Flight SQL executes with GetFlightInfo and DoGet. */
  private static final Set<String> RESULT_SET_COMMANDS =
      Set.of(
          name(CommandStatementSubstraitPlan.getDescriptor()),
          name(CommandGetCatalogs.getDescriptor()),
          name(CommandGetDbSchemas.getDescriptor()),
          name(CommandGetTables.getDescriptor()),
          name(CommandGetTableTypes.getDescriptor()),
          name(CommandGetSqlInfo.getDescriptor()),
          name(CommandGetXdbcTypeInfo.getDescriptor()),
          name(CommandGetPrimaryKeys.getDescriptor()),
          name(CommandGetExportedKeys.getDescriptor()),
          name(CommandGetImportedKeys.getDescriptor()),
          name(CommandGetCrossReference.getDescriptor()));

  private final FlightProducer delegate;
  private final BufferAllocator allocator;

  /**
   * Creates a producer that serves Flight SQL over DoExchange on top of {@code delegate}.
   *
   * @param delegate the Flight SQL producer whose handlers execute the commands; every other RPC is
   *     forwarded to it unchanged.
   * @param allocator the allocator for the metadata messages written to the client.
   */
  public FlightSqlExchangeProducer(FlightProducer delegate, BufferAllocator allocator) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.allocator = Objects.requireNonNull(allocator, "allocator");
  }

  @Override
  public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
    final FlightDescriptor descriptor = reader.getDescriptor();
    final Any request = parseRequest(descriptor);
    if (request == null) {
      delegate.doExchange(context, reader, writer);
      return;
    }
    try {
      new Exchange(context, descriptor, reader, writer).execute(request);
    } catch (Throwable t) {
      // Report anything the handlers throw (including assertion errors): nothing else would end
      // the call.
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      writer.error(StatusUtils.fromThrowable(t));
      return;
    }
    writer.completed();
  }

  @Override
  public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
    delegate.getStream(context, ticket, listener);
  }

  @Override
  public void listFlights(
      CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
    delegate.listFlights(context, criteria, listener);
  }

  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    return delegate.getFlightInfo(context, descriptor);
  }

  @Override
  public PollInfo pollFlightInfo(CallContext context, FlightDescriptor descriptor) {
    return delegate.pollFlightInfo(context, descriptor);
  }

  @Override
  public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
    return delegate.getSchema(context, descriptor);
  }

  @Override
  public Runnable acceptPut(
      CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
    return delegate.acceptPut(context, flightStream, ackStream);
  }

  @Override
  public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
    delegate.doAction(context, action, listener);
  }

  @Override
  public void listActions(CallContext context, StreamListener<ActionType> listener) {
    delegate.listActions(context, listener);
  }

  @Override
  public void close() throws Exception {
    if (delegate instanceof AutoCloseable closeable) {
      closeable.close();
    }
  }

  private static String name(Descriptor descriptor) {
    return descriptor.getFullName();
  }

  /** The full name of the message packed in {@code any} (the type URL without its prefix). */
  private static String typeName(Any any) {
    final String typeUrl = any.getTypeUrl();
    return typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
  }

  /** Returns the Flight SQL request carried by {@code descriptor}, or null if it has none. */
  private static Any parseRequest(FlightDescriptor descriptor) {
    if (!descriptor.isCommand()) {
      return null;
    }
    final Any request;
    try {
      request = Any.parseFrom(descriptor.getCommand());
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
    if (typeName(request).startsWith(FLIGHT_SQL_PACKAGE) || request.is(Flight.Action.class)) {
      return request;
    }
    return null;
  }

  private static FlightDescriptor commandDescriptor(Message command) {
    return FlightDescriptor.command(Any.pack(command).toByteArray());
  }

  private static FlightDescriptor preparedQuery(ByteString handle) {
    return commandDescriptor(
        CommandPreparedStatementQuery.newBuilder().setPreparedStatementHandle(handle).build());
  }

  private static FlightDescriptor preparedUpdate(ByteString handle) {
    return commandDescriptor(
        CommandPreparedStatementUpdate.newBuilder().setPreparedStatementHandle(handle).build());
  }

  private static FlightRuntimeException unwrap(ExecutionException e) {
    return StatusUtils.fromThrowable(e.getCause() == null ? e : e.getCause());
  }

  private static byte[] toBytes(ArrowBuf buffer) {
    if (buffer == null) {
      return new byte[0];
    }
    final byte[] bytes = new byte[(int) buffer.readableBytes()];
    buffer.getBytes(buffer.readerIndex(), bytes);
    return bytes;
  }

  /** The state of one DoExchange call. */
  private final class Exchange {
    private final CallContext context;
    private final FlightDescriptor descriptor;
    private final FlightStream reader;
    private final ServerStreamListener writer;

    /** The result of reading the client's first message ahead of time, or null if not read. */
    private Boolean peekedNext;

    Exchange(
        CallContext context,
        FlightDescriptor descriptor,
        FlightStream reader,
        ServerStreamListener writer) {
      this.context = context;
      this.descriptor = descriptor;
      this.reader = reader;
      this.writer = writer;
    }

    void execute(Any request) throws Exception {
      final String type = typeName(request);
      if (request.is(CommandStatementQuery.class)) {
        final CommandStatementQuery command =
            FlightSqlUtils.unpackOrThrow(request, CommandStatementQuery.class);
        if (hasParameters()) {
          executeOnce(
              command.getQuery(),
              command.hasTransactionId() ? command.getTransactionId() : null,
              /* isQuery= */ true);
        } else {
          streamResultSet(descriptor);
        }
      } else if (request.is(CommandStatementUpdate.class)) {
        final CommandStatementUpdate command =
            FlightSqlUtils.unpackOrThrow(request, CommandStatementUpdate.class);
        if (hasParameters()) {
          executeOnce(
              command.getQuery(),
              command.hasTransactionId() ? command.getTransactionId() : null,
              /* isQuery= */ false);
        } else {
          executeUpdate(descriptor);
        }
      } else if (request.is(CommandPreparedStatementQuery.class)) {
        ByteString handle =
            FlightSqlUtils.unpackOrThrow(request, CommandPreparedStatementQuery.class)
                .getPreparedStatementHandle();
        if (hasParameters()) {
          final ByteString boundHandle = bindParameters(handle);
          // Always answer a bind with exactly one DoPutPreparedStatementResult, so that the client
          // knows how many metadata messages precede the result set.
          final DoPutPreparedStatementResult.Builder bound =
              DoPutPreparedStatementResult.newBuilder();
          if (!boundHandle.equals(handle)) {
            bound.setPreparedStatementHandle(boundHandle);
          }
          sendMetadata(bound.build());
          handle = boundHandle;
        }
        streamResultSet(preparedQuery(handle));
      } else if (request.is(CommandPreparedStatementUpdate.class)
          || request.is(CommandStatementIngest.class)) {
        executeUpdate(descriptor);
      } else if (RESULT_SET_COMMANDS.contains(type)) {
        streamResultSet(descriptor);
      } else if (ACTION_TYPES.containsKey(type)) {
        sendResults(doAction(new Action(ACTION_TYPES.get(type), request.toByteArray())));
      } else if (request.is(Flight.Action.class)) {
        final Flight.Action action = FlightSqlUtils.unpackOrThrow(request, Flight.Action.class);
        sendResults(doAction(new Action(action.getType(), action.getBody().toByteArray())));
      } else {
        throw CallStatus.INVALID_ARGUMENT
            .withDescription("Unsupported Flight SQL request for DoExchange: " + type)
            .toRuntimeException();
      }
    }

    /**
     * Waits until the client sends its first message or half-closes, and reports whether it is
     * streaming parameter values, i.e. whether it sent a schema with at least one field.
     */
    private boolean hasParameters() {
      if (peekedNext == null) {
        peekedNext = reader.next();
      }
      return reader.hasRoot() && !reader.getRoot().getSchema().getFields().isEmpty();
    }

    /** The client's data, as seen by an in-process DoPut-style call for {@code callDescriptor}. */
    private FlightStream input(FlightDescriptor callDescriptor) {
      final Boolean replay = peekedNext;
      peekedNext = null;
      return new InboundStream(reader, callDescriptor, replay, allocator);
    }

    /**
     * Prepares {@code query}, binds the parameters the client is streaming, executes the statement
     * and closes it again, all within this call. The prepared statement is closed even if the call
     * fails or is cancelled.
     */
    private void executeOnce(String query, ByteString transactionId, boolean isQuery)
        throws Exception {
      final ActionCreatePreparedStatementRequest.Builder create =
          ActionCreatePreparedStatementRequest.newBuilder().setQuery(query);
      if (transactionId != null) {
        create.setTransactionId(transactionId);
      }
      final List<Result> created =
          doAction(
              new Action(
                  FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(),
                  Any.pack(create.build()).toByteArray()));
      if (created.isEmpty()) {
        throw CallStatus.INTERNAL
            .withDescription("CreatePreparedStatement did not return a result")
            .toRuntimeException();
      }
      ByteString handle =
          FlightSqlUtils.unpackAndParseOrThrow(
                  created.get(0).getBody(), ActionCreatePreparedStatementResult.class)
              .getPreparedStatementHandle();
      try {
        if (isQuery) {
          handle = bindParameters(handle);
          streamResultSet(preparedQuery(handle));
        } else {
          executeUpdate(preparedUpdate(handle));
        }
      } catch (Throwable t) {
        try {
          closePreparedStatement(handle);
        } catch (Exception closeFailure) {
          t.addSuppressed(closeFailure);
        }
        throw t;
      }
      closePreparedStatement(handle);
    }

    private void closePreparedStatement(ByteString handle) throws Exception {
      doAction(
          new Action(
              FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT.getType(),
              Any.pack(
                      ActionClosePreparedStatementRequest.newBuilder()
                          .setPreparedStatementHandle(handle)
                          .build())
                  .toByteArray()));
    }

    /**
     * Binds the client's parameter stream to a prepared statement, like DoPut with {@code
     * CommandPreparedStatementQuery}, and returns the handle to use from now on.
     */
    private ByteString bindParameters(ByteString handle) throws Exception {
      ByteString current = handle;
      for (byte[] ack : acceptPut(preparedQuery(handle))) {
        final DoPutPreparedStatementResult result = DoPutPreparedStatementResult.parseFrom(ack);
        if (result.hasPreparedStatementHandle() && !result.getPreparedStatementHandle().isEmpty()) {
          current = result.getPreparedStatementHandle();
        }
      }
      return current;
    }

    /** Runs a DoPut-style command and relays each {@code DoPutUpdateResult} to the client. */
    private void executeUpdate(FlightDescriptor callDescriptor) throws Exception {
      for (byte[] ack : acceptPut(callDescriptor)) {
        if (ack.length > 0) {
          sendMetadata(DoPutUpdateResult.parseFrom(ack));
        }
      }
    }

    /** Runs the delegate's DoPut handler against the client's data; returns the acks' metadata. */
    private List<byte[]> acceptPut(FlightDescriptor callDescriptor) {
      final PutResults results = new PutResults();
      delegate.acceptPut(context, input(callDescriptor), results).run();
      return results.get();
    }

    /** Runs the delegate's DoAction handler and waits for all of its results. */
    private List<Result> doAction(Action action) throws Exception {
      final ActionResults results = new ActionResults();
      delegate.doAction(context, action, results);
      return results.get();
    }

    /**
     * Runs a GetFlightInfo-style command and streams the data of all its endpoints to the client as
     * a single result set.
     */
    private void streamResultSet(FlightDescriptor callDescriptor) throws Exception {
      final FlightInfo info = delegate.getFlightInfo(context, callDescriptor);
      try (ResultSetForwarder forwarder = new ResultSetForwarder(writer)) {
        for (FlightEndpoint endpoint : info.getEndpoints()) {
          if (writer.isCancelled()) {
            throw CallStatus.CANCELLED.withDescription("Cancelled by client").toRuntimeException();
          }
          forwarder.forward(
              listener -> delegate.getStream(context, endpoint.getTicket(), listener));
        }
        forwarder.finish(info.getSchemaOptional().orElseGet(() -> new Schema(List.of())));
      }
    }

    private void sendResults(List<Result> results) {
      for (Result result : results) {
        if (result.getBody().length > 0) {
          sendMetadata(result.getBody());
        }
      }
    }

    private void sendMetadata(Message message) {
      sendMetadata(Any.pack(message).toByteArray());
    }

    private void sendMetadata(byte[] payload) {
      final ArrowBuf buffer = allocator.buffer(payload.length);
      buffer.writeBytes(payload);
      // The listener takes ownership of the buffer.
      writer.putMetadata(buffer);
    }
  }

  /**
   * The client's inbound stream as seen by one in-process DoPut-style call: it reports that call's
   * descriptor, replays a message that was read ahead and never closes the exchange's stream.
   */
  private static final class InboundStream extends FlightStream {
    private final FlightStream exchange;
    private final FlightDescriptor descriptor;
    private Boolean replay;

    InboundStream(
        FlightStream exchange,
        FlightDescriptor descriptor,
        Boolean replay,
        BufferAllocator allocator) {
      super(allocator, 0, null, count -> {});
      this.exchange = exchange;
      this.descriptor = descriptor;
      this.replay = replay;
    }

    @Override
    public FlightDescriptor getDescriptor() {
      return descriptor;
    }

    @Override
    public boolean next() {
      if (replay != null) {
        final boolean result = replay;
        replay = null;
        return result;
      }
      return exchange.next();
    }

    @Override
    public VectorSchemaRoot getRoot() {
      return exchange.getRoot();
    }

    @Override
    public Schema getSchema() {
      return exchange.getSchema();
    }

    @Override
    public boolean hasRoot() {
      return exchange.hasRoot();
    }

    @Override
    public ArrowBuf getLatestMetadata() {
      return exchange.getLatestMetadata();
    }

    @Override
    public DictionaryProvider getDictionaryProvider() {
      return exchange.getDictionaryProvider();
    }

    @Override
    public DictionaryProvider takeDictionaryOwnership() {
      return exchange.takeDictionaryOwnership();
    }

    @Override
    public void cancel(String message, Throwable exception) {
      exchange.cancel(message, exception);
    }

    @Override
    public void close() {
      // The exchange's stream is closed when the DoExchange call completes.
    }
  }

  /** Collects the acks of an in-process DoPut call. */
  private static final class PutResults implements StreamListener<PutResult> {
    private final List<byte[]> metadata = Collections.synchronizedList(new ArrayList<>());
    private volatile Throwable error;

    @Override
    public void onNext(PutResult val) {
      // The handler keeps ownership of the result, so copy its metadata right away.
      metadata.add(toBytes(val.getApplicationMetadata()));
    }

    @Override
    public void onError(Throwable t) {
      error = t;
    }

    @Override
    public void onCompleted() {}

    /**
     * Returns the collected metadata. Like DoPut, the call is over once the handler's runnable
     * returns, whether or not it called {@link #onCompleted()}.
     */
    List<byte[]> get() {
      if (error != null) {
        throw StatusUtils.fromThrowable(error);
      }
      return metadata;
    }
  }

  /** Collects the results of an in-process DoAction call, which may complete asynchronously. */
  private static final class ActionResults implements StreamListener<Result> {
    private final List<Result> results = Collections.synchronizedList(new ArrayList<>());
    private final CompletableFuture<List<Result>> done = new CompletableFuture<>();

    @Override
    public void onNext(Result val) {
      results.add(val);
    }

    @Override
    public void onError(Throwable t) {
      done.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
      done.complete(results);
    }

    List<Result> get() throws InterruptedException {
      try {
        return done.get();
      } catch (ExecutionException e) {
        throw unwrap(e);
      }
    }
  }

  /**
   * Relays the output of one or more in-process DoGet calls to the exchange as a single Arrow
   * stream. A DoExchange stream carries one schema per direction, and a DoGet handler may call
   * {@code start()} more than once or return several endpoints, so the forwarder owns the root that
   * is bound to the exchange writer and loads every incoming batch into it.
   */
  private final class ResultSetForwarder implements AutoCloseable {
    private final ServerStreamListener out;
    private BufferAllocator outAllocator;
    private VectorSchemaRoot outRoot;
    private VectorLoader outLoader;

    ResultSetForwarder(ServerStreamListener out) {
      this.out = out;
    }

    /** Runs one DoGet-style call and waits until it completes. */
    void forward(Consumer<ServerStreamListener> call) throws Exception {
      final Segment segment = new Segment();
      try {
        call.accept(segment);
      } catch (RuntimeException e) {
        segment.error(e);
      }
      segment.await();
    }

    /** Makes sure the client received a schema, even if no endpoint produced any data. */
    void finish(Schema schema) {
      if (outRoot == null) {
        outRoot = VectorSchemaRoot.create(schema, allocator);
        out.start(outRoot);
      }
    }

    @Override
    public void close() {
      if (outRoot != null) {
        outRoot.close();
      }
    }

    /**
     * Whether the buffers of {@code root} can be loaded into the relayed root without copying.
     * Allocator trees are identified by their root instance, as in {@code AllocationManager}.
     */
    @SuppressWarnings("ReferenceEquality")
    private boolean sharesAllocatorTree(VectorSchemaRoot root) {
      for (FieldVector vector : root.getFieldVectors()) {
        if (vector.getAllocator().getRoot() != outAllocator.getRoot()) {
          return false;
        }
      }
      return true;
    }

    /** Receives the output of one DoGet-style call. */
    private final class Segment implements ServerStreamListener {
      private final CompletableFuture<Void> done = new CompletableFuture<>();
      private VectorSchemaRoot source;
      private VectorUnloader unloader;

      void await() throws InterruptedException {
        try {
          done.get();
        } catch (ExecutionException e) {
          throw unwrap(e);
        }
      }

      @Override
      public boolean isCancelled() {
        return out.isCancelled();
      }

      @Override
      public void setOnCancelHandler(Runnable handler) {
        out.setOnCancelHandler(handler);
      }

      @Override
      public boolean isReady() {
        return out.isReady();
      }

      @Override
      public void setOnReadyHandler(Runnable handler) {
        out.setOnReadyHandler(handler);
      }

      @Override
      public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
        if (outRoot == null) {
          // Buffers can only be shared within one allocator tree: allocate in the handler's tree,
          // at its root, which outlives any per-stream child allocator.
          outAllocator =
              root.getFieldVectors().isEmpty()
                  ? allocator
                  : root.getFieldVectors().get(0).getAllocator().getRoot();
          outRoot = VectorSchemaRoot.create(root.getSchema(), outAllocator);
          outLoader = new VectorLoader(outRoot);
          out.start(outRoot, dictionaries, option);
        } else if (!outRoot.getSchema().equals(root.getSchema())) {
          throw CallStatus.INTERNAL
              .withDescription(
                  "A result set sent over DoExchange must have a single schema, but it changed from "
                      + outRoot.getSchema()
                      + " to "
                      + root.getSchema())
              .toRuntimeException();
        } else if (dictionaries != null && !dictionaries.getDictionaryIds().isEmpty()) {
          throw CallStatus.UNIMPLEMENTED
              .withDescription(
                  "Dictionary-encoded result sets made of several streams are not supported")
              .toRuntimeException();
        }
        source = root;
        unloader = new VectorUnloader(root, /* includeNullCount= */ true, /* alignBuffers= */ true);
      }

      @Override
      public void putNext() {
        putNext(null);
      }

      @Override
      public void putNext(ArrowBuf metadata) {
        if (unloader == null) {
          throw CallStatus.INTERNAL
              .withDescription("Stream was not started, call start()")
              .toRuntimeException();
        }
        if (sharesAllocatorTree(source)) {
          try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
            outLoader.load(batch);
          }
        } else {
          // Another allocator tree: the values have to be copied.
          final int rowCount = source.getRowCount();
          for (int i = 0; i < source.getFieldVectors().size(); i++) {
            final FieldVector target = outRoot.getVector(i);
            for (int row = 0; row < rowCount; row++) {
              target.copyFromSafe(row, row, source.getVector(i));
            }
            target.setValueCount(rowCount);
          }
          outRoot.setRowCount(rowCount);
        }
        try {
          out.putNext(metadata);
        } finally {
          // Release the batch's buffers, which still belong to the handler.
          outRoot.clear();
        }
      }

      @Override
      public void putMetadata(ArrowBuf metadata) {
        out.putMetadata(metadata);
      }

      @Override
      public void error(Throwable ex) {
        done.completeExceptionally(ex);
      }

      @Override
      public void completed() {
        done.complete(null);
      }

      @Override
      public void setUseZeroCopy(boolean enabled) {
        out.setUseZeroCopy(enabled);
      }
    }
  }
}
