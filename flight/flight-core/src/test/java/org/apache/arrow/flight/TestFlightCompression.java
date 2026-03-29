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
package org.apache.arrow.flight;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestFlightCompression {
  private static final byte[] DICTIONARY_TICKET = "dictionary".getBytes(StandardCharsets.UTF_8);

  private static BufferAllocator allocator;
  private static Schema schema;
  private static Schema encodedSchema;

  @BeforeAll
  public static void setUpClass() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
    schema =
        new Schema(Collections.singletonList(Field.nullable("foo", new ArrowType.Int(32, true))));
    encodedSchema =
        new Schema(
            Collections.singletonList(
                new Field(
                    "encoded",
                    new FieldType(
                        true,
                        new ArrowType.Int(32, true),
                        new DictionaryEncoding(1L, false, null)),
                    null)));
  }

  @AfterAll
  public static void tearDownClass() {
    allocator.close();
  }

  @Test
  public void testDefaultIpcOptionRemainsUncompressed() {
    assertEquals(CompressionUtil.CodecType.NO_COMPRESSION, IpcOption.DEFAULT.codecType);
    assertEquals(Optional.empty(), IpcOption.DEFAULT.compressionLevel);
  }

  @Test
  public void testDoGetRoundTripWithDefaultOption() throws Exception {
    try (final FlightServer server = startServer(IpcOption.DEFAULT);
        final FlightClient client = connect(server);
        final FlightStream stream = client.getStream(new Ticket(new byte[0]))) {
      assertTrue(stream.next());
      validateRoot(stream.getRoot());
      assertFalse(stream.next());
    }
  }

  @ParameterizedTest(name = "codec = {0}")
  @EnumSource(
      value = CompressionUtil.CodecType.class,
      names = {"LZ4_FRAME", "ZSTD"})
  public void testDoGetRoundTrip(CompressionUtil.CodecType codec) throws Exception {
    final IpcOption option = IpcOption.DEFAULT.withBodyCompression(codec);
    try (final FlightServer server = startServer(option);
        final FlightClient client = connect(server);
        final FlightStream stream = client.getStream(new Ticket(new byte[0]))) {
      assertTrue(stream.next());
      validateRoot(stream.getRoot());
      assertFalse(stream.next());
    }
  }

  @ParameterizedTest(name = "codec = {0}")
  @EnumSource(
      value = CompressionUtil.CodecType.class,
      names = {"LZ4_FRAME", "ZSTD"})
  public void testDoPutRoundTrip(CompressionUtil.CodecType codec) throws Exception {
    final IpcOption option = IpcOption.DEFAULT.withBodyCompression(codec);
    try (final FlightServer server = startServer(option);
        final FlightClient client = connect(server);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        final SyncPutListener putListener = new SyncPutListener()) {
      generateData(root);
      final FlightClient.ClientStreamListener listener =
          client.startPut(FlightDescriptor.command(new byte[0]), putListener);
      listener.start(root, null, option);
      listener.putNext();
      listener.completed();
      putListener.getResult();
    }
  }

  @ParameterizedTest(name = "codec = {0}")
  @EnumSource(
      value = CompressionUtil.CodecType.class,
      names = {"LZ4_FRAME", "ZSTD"})
  public void testDoExchangeRoundTrip(CompressionUtil.CodecType codec) throws Exception {
    final IpcOption option = IpcOption.DEFAULT.withBodyCompression(codec);
    try (final FlightServer server = startServer(option);
        final FlightClient client = connect(server);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        final FlightClient.ExchangeReaderWriter stream =
            client.doExchange(FlightDescriptor.command(new byte[0]))) {
      generateData(root);
      stream.getWriter().start(root, null, option);
      stream.getWriter().putNext();
      stream.getWriter().completed();

      assertTrue(stream.getReader().next());
      validateRoot(stream.getReader().getRoot());
      assertFalse(stream.getReader().next());
    }
  }

  @ParameterizedTest(name = "codec = {0}")
  @EnumSource(
      value = CompressionUtil.CodecType.class,
      names = {"LZ4_FRAME", "ZSTD"})
  public void testDoGetDictionaryRoundTrip(CompressionUtil.CodecType codec) throws Exception {
    final IpcOption option = IpcOption.DEFAULT.withBodyCompression(codec);
    try (final FlightServer server = startServer(option);
        final FlightClient client = connect(server);
        final FlightStream stream = client.getStream(new Ticket(DICTIONARY_TICKET))) {
      assertTrue(stream.next());
      assertNotNull(stream.getDictionaryProvider().lookup(1L));
      assertEquals(encodedSchema, stream.getRoot().getSchema());
      try (final ValueVector decoded =
          DictionaryEncoder.decode(
              stream.getRoot().getVector("encoded"), stream.getDictionaryProvider().lookup(1L))) {
        assertTrue(decoded instanceof VarCharVector);
        assertArrayEquals(
            "one".getBytes(StandardCharsets.UTF_8), ((VarCharVector) decoded).get(1));
        assertArrayEquals(
            "two".getBytes(StandardCharsets.UTF_8), ((VarCharVector) decoded).get(2));
        assertArrayEquals(
            "zero".getBytes(StandardCharsets.UTF_8), ((VarCharVector) decoded).get(3));
        assertArrayEquals(
            "two".getBytes(StandardCharsets.UTF_8), ((VarCharVector) decoded).get(4));
      }
      assertFalse(stream.next());
    }
  }

  private static void generateData(VectorSchemaRoot root) {
    assertEquals(schema, root.getSchema());
    final IntVector vector = (IntVector) root.getVector("foo");
    vector.allocateNew(3);
    vector.setSafe(0, 0);
    vector.setSafe(1, 1);
    vector.setSafe(2, 4);
    vector.setValueCount(3);
    root.setRowCount(3);
  }

  private static void validateRoot(VectorSchemaRoot root) {
    assertEquals(schema, root.getSchema());
    assertEquals(3, root.getRowCount());
    final IntVector vector = (IntVector) root.getVector("foo");
    assertEquals(0, vector.get(0));
    assertEquals(1, vector.get(1));
    assertEquals(4, vector.get(2));
  }

  private static FlightServer startServer(IpcOption option) throws Exception {
    final Location location = Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, 0);
    final CompressionFlightProducer producer = new CompressionFlightProducer(allocator, option);
    final FlightServer server = FlightServer.builder(allocator, location, producer).build();
    server.start();
    return server;
  }

  private static FlightClient connect(FlightServer server) {
    final Location location =
        Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, server.getPort());
    return FlightClient.builder(allocator, location).build();
  }

  static final class CompressionFlightProducer extends NoOpFlightProducer {
    private final BufferAllocator allocator;
    private final IpcOption option;

    CompressionFlightProducer(BufferAllocator allocator, IpcOption option) {
      this.allocator = allocator;
      this.option = option;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      if (Arrays.equals(ticket.getBytes(), DICTIONARY_TICKET)) {
        sendDictionaryStream(listener);
        return;
      }

      try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        listener.start(root, null, option);
        generateData(root);
        listener.putNext();
        listener.completed();
      }
    }

    @Override
    public Runnable acceptPut(
        CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
      return () -> {
        try {
          assertTrue(flightStream.next());
          validateRoot(flightStream.getRoot());
          assertFalse(flightStream.next());
          ackStream.onCompleted();
        } catch (AssertionError | RuntimeException ex) {
          ex.printStackTrace();
          ackStream.onError(
              CallStatus.INTERNAL
                  .withCause(ex)
                  .withDescription("Server assertion failed: " + ex)
                  .toRuntimeException());
        }
      };
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        assertTrue(reader.next());
        validateRoot(reader.getRoot());
        assertFalse(reader.next());

        writer.start(root, null, option);
        generateData(root);
        writer.putNext();
        writer.completed();
      } catch (AssertionError | RuntimeException ex) {
        ex.printStackTrace();
        writer.error(
            CallStatus.INTERNAL
                .withCause(ex)
                .withDescription("Server assertion failed: " + ex)
                .toRuntimeException());
      }
    }

    private void sendDictionaryStream(ServerStreamListener listener) {
      final byte[] zero = "zero".getBytes(StandardCharsets.UTF_8);
      final byte[] one = "one".getBytes(StandardCharsets.UTF_8);
      final byte[] two = "two".getBytes(StandardCharsets.UTF_8);
      try (final VarCharVector dictionaryVector = newVarCharVector("dictionary", allocator)) {
        final DictionaryProvider.MapDictionaryProvider provider =
            new DictionaryProvider.MapDictionaryProvider();
        dictionaryVector.allocateNew(512, 3);
        dictionaryVector.setSafe(0, zero, 0, zero.length);
        dictionaryVector.setSafe(1, one, 0, one.length);
        dictionaryVector.setSafe(2, two, 0, two.length);
        dictionaryVector.setValueCount(3);

        final Dictionary dictionary =
            new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
        provider.put(dictionary);

        final FieldVector encodedVector;
        try (final VarCharVector unencoded = newVarCharVector("encoded", allocator)) {
          unencoded.allocateNewSafe();
          unencoded.set(1, one);
          unencoded.set(2, two);
          unencoded.set(3, zero);
          unencoded.set(4, two);
          unencoded.setValueCount(6);
          encodedVector = (FieldVector) DictionaryEncoder.encode(unencoded, dictionary);
        }

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

    private static VarCharVector newVarCharVector(String name, BufferAllocator allocator) {
      return (VarCharVector)
          FieldType.nullable(new ArrowType.Utf8()).createNewSingleVector(name, allocator, null);
    }
  }
}
