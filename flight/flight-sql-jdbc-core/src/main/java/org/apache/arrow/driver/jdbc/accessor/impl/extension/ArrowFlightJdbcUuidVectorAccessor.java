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
package org.apache.arrow.driver.jdbc.accessor.impl.extension;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.IntSupplier;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.UuidType;

/** Accessor for UUID extension-typed vectors. */
public class ArrowFlightJdbcUuidVectorAccessor extends ArrowFlightJdbcAccessor {

  private final ValueVector vector;

  public ArrowFlightJdbcUuidVectorAccessor(
      ValueVector vector,
      IntSupplier currentRowSupplier,
      ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.vector = vector;
  }

  @Override
  public Object getObject() {
    int row = getCurrentRow();
    // vector is expected to be ExtensionTypeVector backed by FixedSizeBinary(16)
    final byte[] bytes;
    if (vector instanceof ExtensionTypeVector) {
      final ExtensionTypeVector ext = (ExtensionTypeVector) vector;
      bytes = ((FixedSizeBinaryVector) ext.getUnderlyingVector()).get(row);
    } else if (vector instanceof FixedSizeBinaryVector) {
      bytes = ((FixedSizeBinaryVector) vector).get(row);
    } else {
      bytes = null;
    }
    this.wasNull = bytes == null;
    this.wasNullConsumer.setWasNull(this.wasNull);
    if (bytes == null) {
      return null;
    }

    final ByteBuffer bb = ByteBuffer.wrap(bytes);
    return new UUID(bb.getLong(), bb.getLong());
  }

  @Override
  public Class<?> getObjectClass() {
    return UUID.class;
  }

  /** Ensure the UUID extension is registered so schemas deserialize correctly. */
  public static void ensureUuidTypeRegistered() {
    if (ExtensionTypeRegistry.lookup("uuid") == null) {
      ExtensionTypeRegistry.register(new UuidType());
    }
  }
}
