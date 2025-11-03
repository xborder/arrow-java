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
package org.apache.arrow.adapter.jdbc.h2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JdbcToArrowUuidTest {
  private Connection conn;

  @BeforeEach
  public void setUp() throws Exception {
    String url = "jdbc:h2:mem:JdbcToArrowUuidTest";
    Class.forName("org.h2.Driver");
    conn = DriverManager.getConnection(url);
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate("CREATE TABLE t (id UUID)");
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (conn != null) {
      conn.close();
    }
  }

  @Test
  public void testUuidColumnToFixedSizeBinary16() throws Exception {
    UUID u1 = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");
    UUID u2 = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");

    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate("INSERT INTO t VALUES ('" + u1 + "')");
      stmt.executeUpdate("INSERT INTO t VALUES ('" + u2 + "')");
    }

    try (ResultSet rs = conn.createStatement().executeQuery("SELECT id FROM t ORDER BY id");
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      JdbcToArrowConfig config =
          new JdbcToArrowConfigBuilder(allocator, JdbcToArrowUtils.getUtcCalendar())
              .setTargetBatchSize(2)
              .build();
      try (ArrowVectorIterator it =
          org.apache.arrow.adapter.jdbc.JdbcToArrow.sqlToArrowVectorIterator(rs, config)) {
        try (VectorSchemaRoot root = it.next()) {
          assertEquals(1, root.getFieldVectors().size());

          Field field = root.getFieldVectors().get(0).getField();
          ArrowType type = field.getType();
          // Expect FixedSizeBinary(16)
          ArrowType.FixedSizeBinary fsb = (ArrowType.FixedSizeBinary) type;
          assertEquals(16, fsb.getByteWidth());

          FixedSizeBinaryVector vec = (FixedSizeBinaryVector) root.getVector(0);
          assertEquals(2, vec.getValueCount());

          byte[] b0 = vec.get(0);
          byte[] b1 = vec.get(1);
          assertArrayEquals(toBytes(u1), b0);
          assertArrayEquals(toBytes(u2), b1);
        }
      }
    }
  }

  private static byte[] toBytes(UUID uuid) {
    ByteBuffer bb = ByteBuffer.allocate(16);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }
}
