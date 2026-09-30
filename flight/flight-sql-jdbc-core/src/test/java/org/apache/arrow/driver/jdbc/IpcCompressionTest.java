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
package org.apache.arrow.driver.jdbc;

import static org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD;
import static org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers.assertLegacyRegularSqlResultSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightMethod;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class IpcCompressionTest {
  @RegisterExtension
  public static final FlightServerTestExtension FLIGHT_SERVER =
      new FlightServerTestExtension.Builder()
          .authentication(
              new UserPasswordAuthentication.Builder()
                  .user(
                      FlightServerTestExtension.DEFAULT_USER,
                      FlightServerTestExtension.DEFAULT_PASSWORD)
                  .build())
          .producer(CoreMockedSqlProducers.getLegacyProducer())
          .ipcCompression(CompressionUtil.CodecType.LZ4_FRAME, CompressionUtil.CodecType.ZSTD)
          .build();

  @BeforeAll
  public static void configureCompression() {
    FLIGHT_SERVER.setIpcCompression("zstd,lz4_frame");
  }

  @Test
  public void readsCompressedResults() throws Exception {
    try (Connection connection = FLIGHT_SERVER.getConnection(false);
        Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(LEGACY_REGULAR_SQL_CMD)) {
      assertLegacyRegularSqlResultSet(results);
    }
    assertEquals(
        "zstd,lz4_frame",
        FLIGHT_SERVER
            .getInterceptorFactory()
            .getHeader(FlightMethod.DO_GET, FlightConstants.IPC_ACCEPT_COMPRESSION_HEADER));
  }
}
