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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests dictionary updates received as framed Flight messages. */
public class TestFlightStreamDictionaryDelta {
  private static final DictionaryEncoding ENCODING =
      new DictionaryEncoding(7, false, new ArrowType.Int(32, true));
  private static final Schema WIRE_SCHEMA = schema(ArrowType.Utf8.INSTANCE, ENCODING);
  private static final Schema MEMORY_SCHEMA = schema(new ArrowType.Int(32, true), ENCODING);

  @Test
  public void nullDictionaryDeltaAndReplacement() throws Exception {
    try (TestStream incoming = new TestStream(schema(ArrowType.Null.INSTANCE, ENCODING), false)) {
      final DictionaryProvider provider = incoming.stream.getDictionaryProvider();
      assertEquals(MEMORY_SCHEMA, incoming.stream.getSchema());
      incoming.nullDictionary(false, 1);
      incoming.batch(0);
      assertTrue(incoming.stream.next());
      final List<String> captured = incoming.decode();
      assertEquals(Collections.singletonList(null), captured);
      assertEquals(Collections.singletonList(null), values(provider.lookup(7).getVector()));

      incoming.nullDictionary(true, 0);
      incoming.nullDictionary(true, 1);
      incoming.batch(1);
      assertTrue(incoming.stream.next());
      assertEquals(Collections.singletonList(null), incoming.decode());
      assertEquals(Arrays.asList(null, null), values(provider.lookup(7).getVector()));
      assertSame(provider, incoming.stream.getDictionaryProvider());

      incoming.nullDictionary(false, 1);
      incoming.batch(0);
      assertTrue(incoming.stream.next());
      assertEquals(Collections.singletonList(null), incoming.decode());
      assertEquals(Collections.singletonList(null), values(provider.lookup(7).getVector()));
      incoming.batch(1);
      assertTrue(incoming.stream.next());
      assertThrows(IllegalArgumentException.class, incoming::decode);
      assertEquals(Collections.singletonList(null), captured);
      incoming.observer.onCompleted();
      assertFalse(incoming.stream.next());
    }
  }

  @Test
  public void dictionaryDeltaAndReplacement() throws Exception {
    try (TestStream incoming = new TestStream(WIRE_SCHEMA, false)) {
      final DictionaryProvider provider = incoming.stream.getDictionaryProvider();
      assertEquals(MEMORY_SCHEMA, incoming.stream.getSchema());
      assertEquals(Collections.emptyList(), values(provider.lookup(7).getVector()));
      incoming.dictionary(7, false, "alpha", "beta");
      incoming.batch(0, 1);
      assertTrue(incoming.stream.next());
      final List<String> captured = incoming.decode();
      assertEquals(Arrays.asList("alpha", "beta"), captured);

      incoming.dictionary(7, true);
      incoming.dictionary(7, true, "gamma");
      incoming.batch(2, 0, null);
      assertTrue(incoming.stream.next());
      assertSame(provider, incoming.stream.getDictionaryProvider());
      assertEquals(Arrays.asList("alpha", "beta", "gamma"), values(provider.lookup(7).getVector()));
      assertEquals(Arrays.asList("gamma", "alpha", null), incoming.decode());

      incoming.dictionary(7, false, "delta", "epsilon");
      incoming.batch(1, 0);
      assertTrue(incoming.stream.next());
      assertEquals(Arrays.asList("epsilon", "delta"), incoming.decode());
      assertEquals(Arrays.asList("delta", "epsilon"), values(provider.lookup(7).getVector()));
      assertEquals(Arrays.asList("alpha", "beta"), captured);
      incoming.observer.onCompleted();
      assertFalse(incoming.stream.next());
    }
  }

  @Test
  public void multipleDeltasAppendToEmptyDictionary() throws Exception {
    try (TestStream incoming = new TestStream(WIRE_SCHEMA, false)) {
      incoming.dictionary(7, false);
      incoming.batch((Integer) null);
      assertTrue(incoming.stream.next());
      assertEquals(Collections.singletonList(null), incoming.decode());
      incoming.dictionary(7, true, "alpha", null);
      incoming.dictionary(7, true);
      incoming.dictionary(7, true, "beta");
      incoming.batch(2, 0, 1, null);
      assertTrue(incoming.stream.next());
      assertEquals(Arrays.asList("beta", "alpha", null, null), incoming.decode());
      assertEquals(
          Arrays.asList("alpha", null, "beta"),
          values(incoming.stream.getDictionaryProvider().lookup(7).getVector()));
      incoming.observer.onCompleted();
      assertFalse(incoming.stream.next());
    }
  }

  @Test
  public void deltaIntoEmptySchemaDictionaryUsesSharedAppender() throws Exception {
    try (TestStream incoming = new TestStream(WIRE_SCHEMA, false)) {
      incoming.dictionary(7, true, "alpha");
      incoming.dictionary(7, true, "beta");
      incoming.batch(0, 1);
      incoming.observer.onCompleted();
      assertTrue(incoming.stream.next());
      assertEquals(Arrays.asList("alpha", "beta"), incoming.decode());
      assertFalse(incoming.stream.next());
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {-1, 2, 4294967296L, Long.MAX_VALUE, Long.MIN_VALUE})
  public void invalidLongDictionaryIndicesReleaseBuffers(long index) throws Exception {
    DictionaryEncoding encoding = new DictionaryEncoding(7, false, new ArrowType.Int(64, true));
    try (TestStream incoming = new TestStream(schema(ArrowType.Utf8.INSTANCE, encoding), false)) {
      incoming.dictionary(7, false, "alpha", "beta");
      incoming.longBatch(1, 0);
      assertTrue(incoming.stream.next());
      List<String> captured = incoming.decode();
      assertEquals(Arrays.asList("beta", "alpha"), captured);
      incoming.longBatch(index);
      assertTrue(incoming.stream.next());
      assertEquals(
          "Provided dictionary does not contain value for index " + index,
          assertThrows(IllegalArgumentException.class, incoming::decode).getMessage());
      assertEquals(Arrays.asList("beta", "alpha"), captured);
      assertEquals(
          Arrays.asList("alpha", "beta"),
          values(incoming.stream.getDictionaryProvider().lookup(7).getVector()));
    }
  }

  @Test
  public void malformedDeltaPreservesDictionaryAndReleasesBuffers() throws Exception {
    try (TestStream incoming = new TestStream(WIRE_SCHEMA, false)) {
      incoming.dictionary(7, false, "alpha");
      incoming.batch(0);
      assertTrue(incoming.stream.next());
      assertEquals(Collections.singletonList("alpha"), incoming.decode());
      try (IntVector vector = new IntVector("wrong-type", incoming.allocator);
          VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
        vector.allocateNew(1);
        vector.setSafe(0, 42);
        root.setRowCount(1);
        try (ArrowDictionaryBatch batch =
            new ArrowDictionaryBatch(7, new VectorUnloader(root).getRecordBatch(), true)) {
          incoming.push(new ArrowMessage(batch, IpcOption.DEFAULT));
        }
      }
      incoming.batch(0);
      assertThrows(RuntimeException.class, incoming.stream::next);
      assertEquals(
          Collections.singletonList("alpha"),
          values(incoming.stream.getDictionaryProvider().lookup(7).getVector()));
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = Types.MinorType.class,
      names = {"VARCHAR", "LARGEVARCHAR", "LIST", "LARGELIST"})
  public void slicedDictionaryDeltas(Types.MinorType type) throws Exception {
    try (RootAllocator allocator = new RootAllocator(1 << 20)) {
      try (FieldVector source =
              type.getNewVector(
                  new Field(
                      "dictionary", FieldType.nullable(type.getType()), Collections.emptyList()),
                  allocator,
                  null);
          ArrowBuf validity = allocator.buffer(1)) {
        switch (type) {
          case VARCHAR:
            ValueVectorDataPopulator.setVector(
                (VarCharVector) source, "skip", "beta", null, "gamma", "tail");
            break;
          case LARGEVARCHAR:
            ValueVectorDataPopulator.setVector(
                (LargeVarCharVector) source, "skip", "beta", null, "gamma", "tail");
            break;
          case LIST:
            ValueVectorDataPopulator.setVector(
                (ListVector) source,
                List.of(-1, -2),
                List.of(10, 11),
                null,
                List.of(),
                List.of(12, 13),
                List.of(99));
            break;
          case LARGELIST:
            ValueVectorDataPopulator.setVector(
                (LargeListVector) source,
                List.of(-1, -2),
                List.of(10, 11),
                null,
                List.of(),
                List.of(12, 13),
                List.of(99));
            break;
          default:
            throw new AssertionError(type);
        }
        boolean list = type == Types.MinorType.LIST || type == Types.MinorType.LARGELIST;
        int count = list ? 4 : 3;
        int width =
            type == Types.MinorType.LARGEVARCHAR || type == Types.MinorType.LARGELIST ? 8 : 4;
        validity.setByte(0, list ? 13 : 5);
        Schema wireSchema =
            new Schema(
                Collections.singletonList(
                    new Field(
                        "word",
                        new FieldType(true, type.getType(), ENCODING, null),
                        source.getField().getChildren())));
        try (FieldVector delta = source.getField().createVector(allocator);
            TestStream incoming = new TestStream(wireSchema, false)) {
          for (int i = 0; i < source.getChildrenFromFields().size(); i++) {
            FieldVector child = source.getChildrenFromFields().get(i);
            delta
                .getChildrenFromFields()
                .get(i)
                .loadFieldBuffers(
                    new ArrowFieldNode(child.getValueCount(), child.getNullCount()),
                    child.getFieldBuffers());
          }
          List<ArrowBuf> buffers = new ArrayList<>(source.getFieldBuffers());
          buffers.set(0, validity);
          buffers.set(1, source.getOffsetBuffer().slice(width, (long) (count + 1) * width));
          delta.loadFieldBuffers(new ArrowFieldNode(count, 1), buffers);
          try (FieldVector empty = source.getField().createVector(allocator)) {
            empty.allocateNew();
            incoming.dictionary(empty, false);
          }
          incoming.dictionary(delta, true);
          incoming.dictionary(delta, true);
          incoming.batch(count * 2 - 1, 0, count + 1, null);
          assertTrue(incoming.stream.next());
          assertEquals(
              list
                  ? Arrays.asList("[12,13]", "[10,11]", null, null)
                  : Arrays.asList("gamma", "beta", null, null),
              incoming.decode());
          assertEquals(
              list
                  ? Arrays.asList(
                      "[10,11]", null, "[]", "[12,13]", "[10,11]", null, "[]", "[12,13]")
                  : Arrays.asList("beta", null, "gamma", "beta", null, "gamma"),
              values(incoming.stream.getDictionaryProvider().lookup(7).getVector()));
          incoming.observer.onCompleted();
          assertFalse(incoming.stream.next());
        }
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  public void undefinedDictionaryFailsAndClosesMessages() throws Exception {
    try (TestStream incoming = new TestStream(WIRE_SCHEMA, false)) {
      incoming.dictionary(7, false, "alpha", "beta");
      incoming.batch(1, 0);
      assertTrue(incoming.stream.next());
      assertEquals(Arrays.asList("beta", "alpha"), incoming.decode());
      incoming.dictionary(8, true, "gamma");
      incoming.batch(0);
      assertEquals(
          "Dictionary not defined in schema: ID 8",
          assertThrows(IllegalArgumentException.class, incoming.stream::next).getMessage());
    }
  }

  @Test
  public void invalidDictionaryIndexFailsAfterDelta() throws Exception {
    try (TestStream incoming = new TestStream(WIRE_SCHEMA, false)) {
      incoming.dictionary(7, false, "alpha", "beta");
      incoming.dictionary(7, true, "gamma");
      incoming.batch(2, 0, null);
      assertTrue(incoming.stream.next());
      assertEquals(Arrays.asList("gamma", "alpha", null), incoming.decode());
      incoming.batch(3);
      assertTrue(incoming.stream.next());
      assertThrows(IllegalArgumentException.class, incoming::decode);
    }
  }

  @Test
  public void cancellationDrainsDeltaAndReleasesBuffers() throws Exception {
    try (TestStream incoming = new TestStream(WIRE_SCHEMA, true)) {
      incoming.dictionary(7, false, "alpha", "beta");
      incoming.batch(0, 1);
      assertTrue(incoming.stream.next());
      assertEquals(Arrays.asList("alpha", "beta"), incoming.decode());
      incoming.dictionary(7, true, "gamma");
      incoming.batch(2, null);
      incoming.stream.cancel("cancel dictionary stream", null);
      assertTrue(incoming.stream.next());
      assertEquals(Arrays.asList("gamma", null), incoming.decode());
      final FlightRuntimeException failure =
          assertThrows(FlightRuntimeException.class, incoming.stream::next);
      assertEquals(FlightStatusCode.CANCELLED, failure.status().code());
      assertEquals("cancel dictionary stream", failure.status().description());
      assertEquals(1, incoming.cancellations);
    }
  }

  @Test
  public void lateErrorPreservesCapturedValuesAndReleasesBuffers() throws Exception {
    try (TestStream incoming = new TestStream(WIRE_SCHEMA, false)) {
      incoming.dictionary(7, false, "alpha", "beta");
      incoming.batch(0, 1);
      assertTrue(incoming.stream.next());
      final List<String> captured = incoming.decode();
      assertEquals(Arrays.asList("alpha", "beta"), captured);
      incoming.dictionary(7, true, "gamma");
      incoming.batch(2);
      incoming.observer.onError(
          CallStatus.INTERNAL.withDescription("late failure").toRuntimeException());
      assertTrue(incoming.stream.next());
      assertEquals(Collections.singletonList("gamma"), incoming.decode());
      final FlightRuntimeException failure =
          assertThrows(FlightRuntimeException.class, incoming.stream::next);
      assertEquals(FlightStatusCode.INTERNAL, failure.status().code());
      assertEquals("late failure", failure.status().description());
      assertEquals(Arrays.asList("alpha", "beta"), captured);
    }
  }

  @Test
  public void closeReleasesQueuedDictionaryAndRecordMessages() throws Exception {
    try (TestStream incoming = new TestStream(WIRE_SCHEMA, false)) {
      incoming.dictionary(7, false, "alpha", "beta");
      incoming.dictionary(7, true, "gamma");
      incoming.batch(2, 0, null);
      assertTrue(incoming.stream.next());
      assertEquals(Arrays.asList("gamma", "alpha", null), incoming.decode());
      incoming.dictionary(7, true, "delta");
      incoming.batch(3);
    }
  }

  @Test
  public void dictionaryOwnershipSurvivesStreamClose() throws Exception {
    try (TestStream incoming = new TestStream(WIRE_SCHEMA, false)) {
      incoming.dictionary(7, false, "alpha", "beta");
      incoming.dictionary(7, true, "gamma");
      incoming.batch(2, 0);
      incoming.observer.onCompleted();
      assertTrue(incoming.stream.next());
      assertEquals(Arrays.asList("gamma", "alpha"), incoming.decode());
      assertFalse(incoming.stream.next());
      final DictionaryProvider borrowed = incoming.stream.getDictionaryProvider();
      try (DictionaryProvider.MapDictionaryProvider owned =
          (DictionaryProvider.MapDictionaryProvider) incoming.stream.takeDictionaryOwnership()) {
        assertSame(borrowed, owned);
        assertThrows(IllegalStateException.class, incoming.stream::takeDictionaryOwnership);
        assertThrows(IllegalStateException.class, incoming.stream::getDictionaryProvider);
        incoming.stream.close();
        assertEquals(Arrays.asList("alpha", "beta", "gamma"), values(owned.lookup(7).getVector()));
      }
      assertEquals(0, incoming.allocator.getAllocatedMemory());
    }
  }

  @Test
  public void zeroRowBatchAndSchemaOnlyStream() throws Exception {
    try (TestStream incoming = new TestStream(WIRE_SCHEMA, false)) {
      incoming.dictionary(7, false, "alpha", "beta");
      incoming.dictionary(7, true);
      incoming.batch();
      incoming.observer.onCompleted();
      assertTrue(incoming.stream.next());
      assertEquals(MEMORY_SCHEMA, incoming.stream.getSchema());
      assertEquals(Collections.emptyList(), incoming.decode());
      assertEquals(
          Arrays.asList("alpha", "beta"),
          values(incoming.stream.getDictionaryProvider().lookup(7).getVector()));
      assertFalse(incoming.stream.next());
    }
    try (TestStream incoming = new TestStream(WIRE_SCHEMA, false)) {
      incoming.observer.onCompleted();
      assertEquals(MEMORY_SCHEMA, incoming.stream.getSchema());
      assertFalse(incoming.stream.next());
      assertEquals(
          Collections.emptyList(),
          values(incoming.stream.getDictionaryProvider().lookup(7).getVector()));
    }
  }

  @Test
  public void nonDictionaryStreamKeepsSchemaAndValues() throws Exception {
    final Schema plainSchema = schema(new ArrowType.Int(32, true), null);
    try (TestStream incoming = new TestStream(plainSchema, false)) {
      incoming.batch(2, 0, null);
      incoming.observer.onCompleted();
      assertTrue(incoming.stream.next());
      assertEquals(plainSchema, incoming.stream.getSchema());
      assertEquals(Arrays.asList("2", "0", null), values(incoming.stream.getRoot().getVector(0)));
      assertEquals(
          Collections.emptySet(), incoming.stream.getDictionaryProvider().getDictionaryIds());
      assertFalse(incoming.stream.next());
    }
  }

  private static Schema schema(ArrowType type, DictionaryEncoding encoding) {
    return new Schema(
        Collections.singletonList(
            new Field(
                "word",
                new FieldType(true, type, encoding, Map.of("field", "words")),
                Collections.emptyList())),
        Map.of("stream", "dictionary-delta"));
  }

  private static List<String> values(ValueVector vector) {
    final List<String> values = new ArrayList<>();
    for (int i = 0; i < vector.getValueCount(); i++) {
      final Object value = vector.getObject(i);
      values.add(value == null ? null : value.toString());
    }
    return values;
  }

  private static final class TestStream implements AutoCloseable {
    private final RootAllocator allocator = new RootAllocator(1 << 20);
    private final FlightStream stream;
    private final StreamObserver<ArrowMessage> observer;
    private final List<ArrowBuf> receivedBuffers = new ArrayList<>();
    private int cancellations;

    private TestStream(Schema schema, boolean cancellable) throws Exception {
      stream = new FlightStream(allocator, 2, cancellable ? this::cancel : null, count -> {});
      observer = stream.asObserver();
      push(new ArrowMessage(null, schema, IpcOption.DEFAULT));
    }

    private void cancel(String message, Throwable exception) {
      cancellations++;
      observer.onError(
          CallStatus.CANCELLED.withDescription(message).withCause(exception).toRuntimeException());
    }

    private void dictionary(long id, boolean delta, String... words) throws Exception {
      try (VarCharVector vector = new VarCharVector("dictionary", allocator);
          VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
        vector.allocateNew();
        for (int i = 0; i < words.length; i++) {
          if (words[i] == null) {
            vector.setNull(i);
          } else {
            vector.setSafe(i, words[i].getBytes(StandardCharsets.UTF_8));
          }
        }
        root.setRowCount(words.length);
        try (ArrowDictionaryBatch batch =
            new ArrowDictionaryBatch(id, new VectorUnloader(root).getRecordBatch(), delta)) {
          push(new ArrowMessage(batch, IpcOption.DEFAULT));
        }
      }
    }

    private void batch(Integer... indices) throws Exception {
      try (IntVector vector = new IntVector("word", allocator);
          VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
        vector.allocateNew(indices.length);
        for (int i = 0; i < indices.length; i++) {
          if (indices[i] == null) {
            vector.setNull(i);
          } else {
            vector.setSafe(i, indices[i]);
          }
        }
        root.setRowCount(indices.length);
        push(
            new ArrowMessage(
                new VectorUnloader(root).getRecordBatch(), null, false, IpcOption.DEFAULT));
      }
    }

    private void nullDictionary(boolean delta, int count) throws Exception {
      try (VectorSchemaRoot root = VectorSchemaRoot.of(new NullVector("dictionary"))) {
        root.setRowCount(count);
        try (ArrowDictionaryBatch batch =
            new ArrowDictionaryBatch(7, new VectorUnloader(root).getRecordBatch(), delta)) {
          push(new ArrowMessage(batch, IpcOption.DEFAULT));
        }
      }
    }

    private void longBatch(long... indices) throws Exception {
      try (BigIntVector vector = new BigIntVector("word", allocator);
          VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
        vector.allocateNew(indices.length);
        for (int i = 0; i < indices.length; i++) {
          vector.setSafe(i, indices[i]);
        }
        root.setRowCount(indices.length);
        push(
            new ArrowMessage(
                new VectorUnloader(root).getRecordBatch(), null, false, IpcOption.DEFAULT));
      }
    }

    private void dictionary(FieldVector vector, boolean delta) throws Exception {
      VectorSchemaRoot root = VectorSchemaRoot.of(vector);
      root.setRowCount(vector.getValueCount());
      try (ArrowDictionaryBatch batch =
          new ArrowDictionaryBatch(7, new VectorUnloader(root).getRecordBatch(), delta)) {
        push(new ArrowMessage(batch, IpcOption.DEFAULT));
      }
    }

    private void push(ArrowMessage message) throws Exception {
      final MethodDescriptor.Marshaller<ArrowMessage> marshaller =
          ArrowMessage.createMarshaller(allocator);
      try (message;
          InputStream wire = marshaller.stream(message)) {
        final ArrowMessage received = marshaller.parse(wire);
        received.getBufs().forEach(receivedBuffers::add);
        observer.onNext(received);
      }
    }

    private List<String> decode() {
      try (ValueVector decoded =
          DictionaryEncoder.decode(
              stream.getRoot().getVector(0), stream.getDictionaryProvider().lookup(7))) {
        return values(decoded);
      }
    }

    @Override
    public void close() throws Exception {
      try (allocator) {
        stream.close();
        assertEquals(
            receivedBuffers.stream()
                .map(buffer -> buffer.capacity() == 0 ? 1 : 0)
                .collect(Collectors.toList()),
            receivedBuffers.stream()
                .map(buffer -> buffer.getReferenceManager().getRefCount())
                .collect(Collectors.toList()));
        assertEquals(0, allocator.getAllocatedMemory());
      }
    }
  }
}
