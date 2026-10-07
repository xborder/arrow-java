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
package org.apache.arrow.flight.integration.tests;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.SyncPutListener;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A runnable sample that spins up a local mock Flight server and verifies compressed IPC batches
 * round-trip correctly through DoGet, DoPut, DoExchange, and dictionary messages.
 */
public final class FlightIpcCompressionSample {
  private static final String HOST = "localhost";
  private static final byte[] GET_TICKET = "compressed-get".getBytes(StandardCharsets.UTF_8);
  private static final byte[] DICTIONARY_TICKET =
      "compressed-dictionary".getBytes(StandardCharsets.UTF_8);
  private static final byte[] PUT_COMMAND = "compressed-put".getBytes(StandardCharsets.UTF_8);
  private static final byte[] EXCHANGE_COMMAND =
      "compressed-exchange".getBytes(StandardCharsets.UTF_8);
  private static final byte[] ZERO = "zero".getBytes(StandardCharsets.UTF_8);
  private static final byte[] ONE = "one".getBytes(StandardCharsets.UTF_8);
  private static final byte[] TWO = "two".getBytes(StandardCharsets.UTF_8);

  private static final Schema SCHEMA =
      new Schema(Collections.singletonList(Field.nullable("foo", new ArrowType.Int(32, true))));
  private static final Schema DICTIONARY_SCHEMA =
      new Schema(
          Collections.singletonList(
              new Field(
                  "encoded",
                  new FieldType(
                      true,
                      new ArrowType.Int(32, true),
                      new DictionaryEncoding(1L, false, null)),
                  null)));

  private FlightIpcCompressionSample() {}

  public static void main(String[] args) throws Exception {
    final List<CompressionUtil.CodecType> codecs = parseCodecs(args);
    try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      for (final CompressionUtil.CodecType codec : codecs) {
        runCodecRoundTrip(allocator, codec);
      }
    }
    System.out.println("All Flight IPC compression checks passed.");
  }

  private static List<CompressionUtil.CodecType> parseCodecs(String[] args) {
    if (args.length == 0) {
      return Arrays.asList(CompressionUtil.CodecType.LZ4_FRAME, CompressionUtil.CodecType.ZSTD);
    }
    return Arrays.stream(args)
        .map(String::trim)
        .map(value -> value.toUpperCase(Locale.ROOT))
        .map(CompressionUtil.CodecType::valueOf)
        .collect(Collectors.toList());
  }

  private static void runCodecRoundTrip(
      BufferAllocator allocator, CompressionUtil.CodecType codec) throws Exception {
    final IpcOption option = IpcOption.DEFAULT.withBodyCompression(codec);
    System.out.println("Testing codec: " + codec);
    try (final FlightServer server = startServer(allocator, option);
        final FlightClient client = connect(allocator, server)) {
      verifyDoGet(client);
      verifyDoPut(allocator, client, option);
      verifyDoExchange(allocator, client, option);
      verifyDictionaryDoGet(client);
    }
    System.out.println("PASS codec=" + codec);
  }

  private static FlightServer startServer(BufferAllocator allocator, IpcOption option)
      throws Exception {
    final FlightServer server =
        FlightServer.builder(allocator, Location.forGrpcInsecure(HOST, 0), new MockProducer(allocator, option))
            .build();
    server.start();
    return server;
  }

  private static FlightClient connect(BufferAllocator allocator, FlightServer server) {
    return FlightClient.builder(allocator, Location.forGrpcInsecure(HOST, server.getPort())).build();
  }

  private static void verifyDoGet(FlightClient client) throws Exception {
    try (final FlightStream stream = client.getStream(new Ticket(GET_TICKET))) {
      IntegrationAssertions.assertTrue("Expected one DoGet batch", stream.next());
      validateNumericRoot(stream.getRoot());
      IntegrationAssertions.assertFalse("Expected end of DoGet stream", stream.next());
    }
  }

  private static void verifyDoPut(BufferAllocator allocator, FlightClient client, IpcOption option)
      throws Exception {
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
        final SyncPutListener putListener = new SyncPutListener()) {
      populateNumericRoot(root);
      final FlightClient.ClientStreamListener writer =
          client.startPut(FlightDescriptor.command(PUT_COMMAND), putListener);
      writer.start(root, null, option);
      writer.putNext();
      writer.completed();
      putListener.getResult();
    }
  }

  private static void verifyDoExchange(
      BufferAllocator allocator, FlightClient client, IpcOption option) throws Exception {
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
        final FlightClient.ExchangeReaderWriter stream =
            client.doExchange(FlightDescriptor.command(EXCHANGE_COMMAND))) {
      populateNumericRoot(root);
      stream.getWriter().start(root, null, option);
      stream.getWriter().putNext();
      stream.getWriter().completed();

      IntegrationAssertions.assertTrue("Expected one DoExchange batch", stream.getReader().next());
      validateNumericRoot(stream.getReader().getRoot());
      IntegrationAssertions.assertFalse(
          "Expected end of DoExchange stream", stream.getReader().next());
    }
  }

  private static void verifyDictionaryDoGet(FlightClient client) throws Exception {
    try (final FlightStream stream = client.getStream(new Ticket(DICTIONARY_TICKET))) {
      IntegrationAssertions.assertTrue("Expected one dictionary batch", stream.next());
      IntegrationAssertions.assertEquals(DICTIONARY_SCHEMA, stream.getRoot().getSchema());
      IntegrationAssertions.assertNotNull(stream.getDictionaryProvider().lookup(1L));
      try (final ValueVector decoded =
          DictionaryEncoder.decode(
              stream.getRoot().getVector("encoded"), stream.getDictionaryProvider().lookup(1L))) {
        IntegrationAssertions.assertTrue(
            "Expected a decoded VarCharVector", decoded instanceof VarCharVector);
        final VarCharVector vector = (VarCharVector) decoded;
        IntegrationAssertions.assertEquals(ONE, vector.get(0));
        IntegrationAssertions.assertEquals(TWO, vector.get(1));
        IntegrationAssertions.assertEquals(ZERO, vector.get(2));
        IntegrationAssertions.assertEquals(TWO, vector.get(3));
      }
      IntegrationAssertions.assertFalse("Expected end of dictionary stream", stream.next());
    }
  }

  private static void populateNumericRoot(VectorSchemaRoot root) {
    IntegrationAssertions.assertEquals(SCHEMA, root.getSchema());
    final IntVector vector = (IntVector) root.getVector("foo");
    vector.allocateNew(3);
    vector.setSafe(0, 0);
    vector.setSafe(1, 1);
    vector.setSafe(2, 4);
    vector.setValueCount(3);
    root.setRowCount(3);
  }

  private static void validateNumericRoot(VectorSchemaRoot root) {
    IntegrationAssertions.assertEquals(SCHEMA, root.getSchema());
    IntegrationAssertions.assertEquals(3, root.getRowCount());
    final IntVector vector = (IntVector) root.getVector("foo");
    IntegrationAssertions.assertEquals(0, vector.get(0));
    IntegrationAssertions.assertEquals(1, vector.get(1));
    IntegrationAssertions.assertEquals(4, vector.get(2));
  }

  private static FlightRuntimeException asFlightException(String action, Throwable cause) {
    return CallStatus.INTERNAL
        .withCause(cause)
        .withDescription(action + " failed: " + cause)
        .toRuntimeException();
  }

  private static VarCharVector newVarCharVector(String name, BufferAllocator allocator) {
    return (VarCharVector)
        FieldType.nullable(new ArrowType.Utf8()).createNewSingleVector(name, allocator, null);
  }

  private static final class MockProducer extends NoOpFlightProducer {
    private final BufferAllocator allocator;
    private final IpcOption option;

    private MockProducer(BufferAllocator allocator, IpcOption option) {
      this.allocator = allocator;
      this.option = option;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      try {
        if (Arrays.equals(ticket.getBytes(), GET_TICKET)) {
          try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
            listener.start(root, null, option);
            populateNumericRoot(root);
            listener.putNext();
            listener.completed();
          }
          return;
        }

        if (Arrays.equals(ticket.getBytes(), DICTIONARY_TICKET)) {
          sendDictionaryStream(listener);
          return;
        }

        listener.error(
            CallStatus.NOT_FOUND
                .withDescription("Unknown ticket: " + new String(ticket.getBytes(), StandardCharsets.UTF_8))
                .toRuntimeException());
      } catch (Throwable t) {
        listener.error(asFlightException("DoGet", t));
      }
    }

    @Override
    public Runnable acceptPut(
        CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
      return () -> {
        try {
          final FlightDescriptor descriptor = flightStream.getDescriptor();
          IntegrationAssertions.assertTrue("Expected DoPut command descriptor", descriptor.isCommand());
          IntegrationAssertions.assertEquals(PUT_COMMAND, descriptor.getCommand());
          IntegrationAssertions.assertTrue("Expected one DoPut batch", flightStream.next());
          validateNumericRoot(flightStream.getRoot());
          IntegrationAssertions.assertFalse("Expected end of DoPut stream", flightStream.next());
          ackStream.onCompleted();
        } catch (Throwable t) {
          ackStream.onError(asFlightException("DoPut", t));
        }
      };
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
        final FlightDescriptor descriptor = reader.getDescriptor();
        IntegrationAssertions.assertTrue("Expected DoExchange command descriptor", descriptor.isCommand());
        IntegrationAssertions.assertEquals(EXCHANGE_COMMAND, descriptor.getCommand());
        IntegrationAssertions.assertTrue("Expected one inbound DoExchange batch", reader.next());
        validateNumericRoot(reader.getRoot());
        IntegrationAssertions.assertFalse("Expected end of inbound DoExchange stream", reader.next());

        writer.start(root, null, option);
        populateNumericRoot(root);
        writer.putNext();
        writer.completed();
      } catch (Throwable t) {
        writer.error(asFlightException("DoExchange", t));
      }
    }

    private void sendDictionaryStream(ServerStreamListener listener) throws Exception {
      try (final VarCharVector dictionaryVector = newVarCharVector("dictionary", allocator)) {
        final DictionaryProvider.MapDictionaryProvider provider =
            new DictionaryProvider.MapDictionaryProvider();
        dictionaryVector.allocateNew(256, 3);
        dictionaryVector.setSafe(0, ZERO, 0, ZERO.length);
        dictionaryVector.setSafe(1, ONE, 0, ONE.length);
        dictionaryVector.setSafe(2, TWO, 0, TWO.length);
        dictionaryVector.setValueCount(3);

        final Dictionary dictionary =
            new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
        provider.put(dictionary);

        try (final VarCharVector unencoded = newVarCharVector("encoded", allocator)) {
          unencoded.allocateNew(256, 4);
          unencoded.setSafe(0, ONE, 0, ONE.length);
          unencoded.setSafe(1, TWO, 0, TWO.length);
          unencoded.setSafe(2, ZERO, 0, ZERO.length);
          unencoded.setSafe(3, TWO, 0, TWO.length);
          unencoded.setValueCount(4);

          final FieldVector encodedVector =
              (FieldVector) DictionaryEncoder.encode(unencoded, dictionary);
          final List<Field> fields = Collections.singletonList(encodedVector.getField());
          final List<FieldVector> vectors = Collections.singletonList(encodedVector);
          try (final VectorSchemaRoot root =
              new VectorSchemaRoot(fields, vectors, encodedVector.getValueCount())) {
            listener.start(root, provider, option);
            listener.putNext();
            listener.completed();
          }
        }
      }
    }
  }
}
