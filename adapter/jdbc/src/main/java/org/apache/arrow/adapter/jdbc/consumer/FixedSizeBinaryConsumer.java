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
package org.apache.arrow.adapter.jdbc.consumer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FixedSizeBinaryVector;

/**
 * Consumer for FixedSizeBinary(16) values, commonly used for UUID.
 *
 * <p>Attempts to read values as UUID, byte[], Blob, or String, in that order. If a String is
 * returned, it must be a canonical UUID string.
 */
public abstract class FixedSizeBinaryConsumer extends BaseConsumer<FixedSizeBinaryVector> {

  private static final int UUID_BYTES = 16;

  /**
   * Creates a consumer for {@link FixedSizeBinaryVector}.
   *
   * @param vector the vector to write to
   * @param index the column index
   * @param nullable whether the column is nullable
   * @return a consumer
   */
  public static FixedSizeBinaryConsumer createConsumer(
      FixedSizeBinaryVector vector, int index, boolean nullable) {
    if (nullable) {
      return new Nullable(vector, index);
    } else {
      return new NonNullable(vector, index);
    }
  }

  /**
   * Instantiate a FixedSizeBinaryConsumer.
   *
   * @param vector the vector to write to
   * @param index the column index
   */
  public FixedSizeBinaryConsumer(FixedSizeBinaryVector vector, int index) {
    super(vector, index);
    if (vector != null) {
      vector.allocateNewSafe();
    }
    Preconditions.checkArgument(
        vector == null || vector.getByteWidth() == UUID_BYTES,
        "FixedSizeBinaryConsumer expects byteWidth=16, but got %s",
        vector == null ? -1 : vector.getByteWidth());
  }

  protected void setBytesOrNull(byte[] bytes) {
    if (bytes == null) {
      // Leave null by advancing index only; vector validity stays unset.
    } else {
      Preconditions.checkArgument(
          bytes.length >= UUID_BYTES,
          "Expected at least 16 bytes to write FixedSizeBinary(16), got %s",
          bytes.length);
      vector.setSafe(currentIndex, bytes);
    }
  }

  private static byte[] toBytes(UUID uuid) {
    ByteBuffer bb = ByteBuffer.allocate(UUID_BYTES);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }

  private static byte[] readAll(InputStream is) throws IOException {
    if (is == null) {
      return null;
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream(UUID_BYTES);
    byte[] buf = new byte[64];
    int r;
    while ((r = is.read(buf)) != -1) {
      baos.write(buf, 0, r);
    }
    return baos.toByteArray();
  }

  protected void consumeInternal(ResultSet rs) throws SQLException, IOException {
    Object obj = rs.getObject(columnIndexInResultSet);
    byte[] bytes = null;
    if (rs.wasNull()) {
      // leave bytes = null
    } else if (obj instanceof UUID) {
      bytes = toBytes((UUID) obj);
    } else if (obj instanceof byte[]) {
      bytes = (byte[]) obj;
    } else if (obj instanceof Blob) {
      Blob blob = (Blob) obj;
      try (InputStream is = blob.getBinaryStream()) {
        bytes = readAll(is);
      }
    } else {
      // Fallbacks
      try {
        String s = rs.getString(columnIndexInResultSet);
        if (s != null) {
          bytes = toBytes(UUID.fromString(s));
        }
      } catch (Exception ignore) {
        // Try binary stream next
        try (InputStream is = rs.getBinaryStream(columnIndexInResultSet)) {
          bytes = readAll(is);
        }
      }
    }

    if (bytes != null && bytes.length != UUID_BYTES) {
      // If longer, accept the first 16 bytes; if shorter, reject.
      Preconditions.checkArgument(
          bytes.length >= UUID_BYTES,
          "Binary value length %s is shorter than 16 for FixedSizeBinary(16)",
          bytes.length);
      byte[] truncated = new byte[UUID_BYTES];
      System.arraycopy(bytes, 0, truncated, 0, UUID_BYTES);
      bytes = truncated;
    }

    setBytesOrNull(bytes);
  }

  public static class Nullable extends FixedSizeBinaryConsumer {
    public Nullable(FixedSizeBinaryVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException, IOException {
      consumeInternal(resultSet);
      currentIndex++;
    }
  }

  public static class NonNullable extends FixedSizeBinaryConsumer {
    public NonNullable(FixedSizeBinaryVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException, IOException {
      consumeInternal(resultSet);
      // If null, ensure we still advance; vector remains null at this index
      currentIndex++;
    }
  }

  @Override
  public void resetValueVector(FixedSizeBinaryVector vector) {
    super.resetValueVector(vector);
    if (vector != null) {
      Preconditions.checkArgument(
          vector.getByteWidth() == UUID_BYTES,
          "FixedSizeBinaryConsumer expects byteWidth=16, but got %s",
          vector.getByteWidth());
      vector.allocateNewSafe();
    }
  }
}
