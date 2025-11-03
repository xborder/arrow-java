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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.UUID;
import java.util.function.IntSupplier;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.UuidType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ArrowFlightJdbcUuidVectorAccessorTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  private UuidVector uuidVector;

  @BeforeEach
  public void setUp() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    uuidVector =
        (UuidVector)
            new UuidType()
                .getNewVector("arrow.uuid", FieldType.nullable(new UuidType()), allocator);
    uuidVector.allocateNew();
    UUID u1 = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");
    uuidVector.set(0, u1);
    uuidVector.setNull(1);
    uuidVector.setValueCount(2);
  }

  @AfterEach
  public void tearDown() {
    if (uuidVector != null) {
      uuidVector.close();
    }
  }

  @Test
  public void testAccessorReturnsUuidObjects() throws Exception {
    IntSupplier row = () -> 0;

    ArrowFlightJdbcAccessor accessor =
        ArrowFlightJdbcAccessorFactory.createAccessor(uuidVector, row, (boolean ignored) -> {});

    assertThat(accessor.getObjectClass(), equalTo(UUID.class));

    // row 0
    assertThat(accessor.getObject(UUID.class), is(uuidVector.getObject(0)));
    // advance
    // simulate cursor advancing by changing supplier backing field
    // not strictly needed for single-row checks in this test
  }
}
