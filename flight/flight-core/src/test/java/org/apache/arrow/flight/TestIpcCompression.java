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

import static org.apache.arrow.flight.FlightTestUtil.LOCALHOST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestIpcCompression {
  private static final byte[] VALUE = "compressible-value".getBytes(StandardCharsets.UTF_8);

  @ParameterizedTest
  @EnumSource(
      value = CompressionUtil.CodecType.class,
      names = {"LZ4_FRAME", "ZSTD"})
  public void negotiatesCompression(CompressionUtil.CodecType codec) throws Exception {
    assertRoundTrip(codec, true, codec);
  }

  @ParameterizedTest
  @EnumSource(
      value = CompressionUtil.CodecType.class,
      names = {"LZ4_FRAME", "ZSTD"})
  public void fallsBackForServersWithoutCompression(CompressionUtil.CodecType codec)
      throws Exception {
    assertRoundTrip(codec, false, CompressionUtil.CodecType.NO_COMPRESSION);
  }

  private static void assertRoundTrip(
      CompressionUtil.CodecType requested,
      boolean enableServerCompression,
      CompressionUtil.CodecType expected)
      throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      final FlightServer.Builder serverBuilder =
          FlightServer.builder(
              allocator,
              Location.forGrpcInsecure(LOCALHOST, 0),
              new NoOpFlightProducer() {
                @Override
                public void getStream(
                    CallContext context, Ticket ticket, ServerStreamListener listener) {
                  try (VarCharVector vector = new VarCharVector("value", allocator);
                      VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
                    vector.allocateNew();
                    vector.setSafe(0, VALUE);
                    vector.setValueCount(1);
                    root.setRowCount(1);
                    listener.start(root);
                    listener.putNext();
                    listener.completed();
                  }
                }
              });
      if (enableServerCompression) {
        serverBuilder.ipcCompression(
            CompressionUtil.CodecType.LZ4_FRAME, CompressionUtil.CodecType.ZSTD);
      }
      try (FlightServer server = serverBuilder.build().start();
          FlightClient client = FlightClient.builder(allocator, server.getLocation()).build();
          FlightStream stream =
              client.getStream(
                  new Ticket(new byte[0]), CallOptions.acceptIpcCompression(requested))) {
        assertTrue(stream.next());
        assertEquals("compressible-value", stream.getRoot().getVector(0).getObject(0).toString());
        assertEquals(expected, stream.compressionType);
      }
    }
  }
}
