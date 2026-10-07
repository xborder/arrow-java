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
package org.apache.arrow.flight.sql.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightMethod;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlExchangeClient;
import org.apache.arrow.flight.sql.FlightSqlExchangeProducer;
import org.apache.arrow.flight.sql.example.FlightSqlExample;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Manual benchmark: the latency of classic Flight SQL and of Flight SQL over DoExchange when every
 * network round trip costs {@code rttMs}. The client connects through a TCP proxy that delays each
 * direction by half of it. Run with:
 *
 * <pre>
 * mvn -pl flight/flight-sql test -Dtest=TestFlightSqlExchangeLatency \
 *     -Darrow.flight.sql.exchange.benchmark=true -DrttMs=20 -Druns=15
 * </pre>
 */
@EnabledIfSystemProperty(named = "arrow.flight.sql.exchange.benchmark", matches = "true")
public class TestFlightSqlExchangeLatency {
  private static final String DB_NAME = "derbyExchangeLatencyDB";
  private static final String PARAMETER_QUERY = "SELECT * FROM intTable WHERE id = ?";
  private static final String INSERT = "INSERT INTO intTable (keyName, value) VALUES (?, ?)";

  interface Flow {
    void run() throws Exception;
  }

  @Test
  public void compareLatency() throws Exception {
    final int rttMs = Integer.getInteger("rttMs", 20);
    final int runs = Integer.getInteger("runs", 15);
    final TestFlightSqlExchange.RpcCounter rpcs = new TestFlightSqlExchange.RpcCounter();
    final Location anyPort = Location.forGrpcInsecure("localhost", 0);
    try (BufferAllocator allocator = new RootAllocator();
        FlightServer server =
            FlightServer.builder(
                    allocator,
                    anyPort,
                    new FlightSqlExchangeProducer(
                        new FlightSqlExample(anyPort, DB_NAME), allocator))
                .middleware(FlightServerMiddleware.Key.of("rpc-counter"), rpcs)
                .build()
                .start();
        DelayProxy proxy = new DelayProxy(server.getPort(), rttMs / 2);
        FlightSqlClient classic =
            new FlightSqlClient(
                FlightClient.builder(
                        allocator, Location.forGrpcInsecure("localhost", proxy.getPort()))
                    .build());
        FlightSqlExchangeClient exchange =
            new FlightSqlExchangeClient(
                FlightClient.builder(
                        allocator, Location.forGrpcInsecure("localhost", proxy.getPort()))
                    .build())) {
      // The example server uses the SQL text as the handle, so keep reusable statements distinct.
      try (FlightSqlClient.PreparedStatement classicPrepared =
              classic.prepare("SELECT * FROM intTable WHERE id = ? AND 1 = 1");
          FlightSqlExchangeClient.PreparedStatement exchangePrepared =
              exchange.prepare("SELECT * FROM intTable WHERE id = ? AND 2 = 2")) {
        final Flow cleanup =
            () -> exchange.executeUpdate("DELETE FROM intTable WHERE keyName LIKE 'bench-%'");
        final Object[][] scenarios = {
          {
            "SELECT, statement",
            (Flow)
                () -> {
                  final FlightInfo info = classic.execute("SELECT * FROM intTable");
                  drain(classic.getStream(info.getEndpoints().get(0).getTicket()));
                },
            (Flow) () -> drain(exchange.execute("SELECT * FROM intTable")),
            null
          },
          {
            "SELECT with a parameter",
            (Flow)
                () -> {
                  try (FlightSqlClient.PreparedStatement statement =
                      classic.prepare(PARAMETER_QUERY)) {
                    statement.setParameters(ids(allocator, 1));
                    final FlightInfo info = statement.execute();
                    drain(classic.getStream(info.getEndpoints().get(0).getTicket()));
                  }
                },
            (Flow)
                () -> {
                  try (VectorSchemaRoot parameters = ids(allocator, 1)) {
                    drain(exchange.execute(PARAMETER_QUERY, parameters));
                  }
                },
            null
          },
          {
            "re-execute prepared SELECT",
            (Flow)
                () -> {
                  classicPrepared.setParameters(ids(allocator, 2));
                  final FlightInfo info = classicPrepared.execute();
                  drain(classic.getStream(info.getEndpoints().get(0).getTicket()));
                },
            (Flow)
                () -> {
                  try (VectorSchemaRoot parameters = ids(allocator, 2)) {
                    drain(exchangePrepared.execute(parameters));
                  }
                },
            null
          },
          {
            "INSERT 10 parameter sets",
            (Flow)
                () -> {
                  try (FlightSqlClient.PreparedStatement statement = classic.prepare(INSERT)) {
                    statement.setParameters(rows(allocator, 10));
                    statement.executeUpdate();
                  }
                },
            (Flow)
                () -> {
                  try (VectorSchemaRoot rows = rows(allocator, 10)) {
                    exchange.executeUpdate(INSERT, rows);
                  }
                },
            cleanup
          },
          {
            "UPDATE, statement",
            (Flow) () -> classic.executeUpdate("UPDATE intTable SET value = value WHERE id = 1"),
            (Flow) () -> exchange.executeUpdate("UPDATE intTable SET value = value WHERE id = 1"),
            null
          },
          {
            "GetTables",
            (Flow)
                () -> {
                  final FlightInfo info = classic.getTables(null, null, null, null, false);
                  drain(classic.getStream(info.getEndpoints().get(0).getTicket()));
                },
            (Flow) () -> drain(exchange.executeCommand(CommandGetTables.getDefaultInstance())),
            null
          },
        };

        System.out.printf("RTT %d ms, median of %d runs%n", rttMs, runs);
        System.out.printf(
            "| %-26s | %-58s | %10s | %11s |%n",
            "scenario", "classic RPCs", "classic ms", "exchange ms");
        for (Object[] scenario : scenarios) {
          final Flow classicFlow = (Flow) scenario[1];
          final Flow exchangeFlow = (Flow) scenario[2];
          final Flow reset = (Flow) scenario[3];
          final Map<FlightMethod, Integer> classicCalls = countCalls(rpcs, classicFlow, reset);
          final double classicMs = medianMillis(classicFlow, reset, runs);
          assertThat(countCalls(rpcs, exchangeFlow, reset))
              .isEqualTo(Map.of(FlightMethod.DO_EXCHANGE, 1));
          final double exchangeMs = medianMillis(exchangeFlow, reset, runs);
          System.out.printf(
              "| %-26s | %-58s | %10.1f | %11.1f |%n",
              scenario[0], classicCalls, classicMs, exchangeMs);
        }
      }
    } finally {
      FlightSqlExample.removeDerbyDatabaseIfExists(DB_NAME);
    }
  }

  private static Map<FlightMethod, Integer> countCalls(
      TestFlightSqlExchange.RpcCounter rpcs, Flow flow, Flow reset) throws Exception {
    rpcs.reset();
    flow.run();
    final Map<FlightMethod, Integer> calls = rpcs.reset();
    if (reset != null) {
      reset.run();
    }
    return calls;
  }

  private static double medianMillis(Flow flow, Flow reset, int runs) throws Exception {
    final double[] millis = new double[runs];
    for (int i = -3; i < runs; i++) {
      final long start = System.nanoTime();
      flow.run();
      if (i >= 0) {
        millis[i] = (System.nanoTime() - start) / 1e6;
      }
      if (reset != null) {
        reset.run();
      }
    }
    Arrays.sort(millis);
    return millis[runs / 2];
  }

  private static void drain(FlightStream stream) throws Exception {
    try (stream) {
      while (stream.next()) {
        // Consume the whole result set.
      }
    }
  }

  private static VectorSchemaRoot ids(BufferAllocator allocator, int id) {
    final IntVector vector = new IntVector("id", allocator);
    vector.allocateNew(1);
    vector.set(0, id);
    vector.setValueCount(1);
    return VectorSchemaRoot.of(vector);
  }

  private static VectorSchemaRoot rows(BufferAllocator allocator, int count) {
    final VarCharVector keys = new VarCharVector("keyName", allocator);
    final IntVector values = new IntVector("value", allocator);
    for (int i = 0; i < count; i++) {
      keys.setSafe(i, ("bench-" + i).getBytes(StandardCharsets.UTF_8));
      values.setSafe(i, i);
    }
    keys.setValueCount(count);
    values.setValueCount(count);
    return VectorSchemaRoot.of(keys, values);
  }

  /** Forwards TCP connections to a local port, delaying the bytes in each direction. */
  static final class DelayProxy implements AutoCloseable {
    private final ServerSocket listener;
    private final int targetPort;
    private final long delayNanos;

    DelayProxy(int targetPort, long oneWayDelayMillis) throws IOException {
      this.listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
      this.targetPort = targetPort;
      this.delayNanos = TimeUnit.MILLISECONDS.toNanos(oneWayDelayMillis);
      daemon(this::acceptLoop);
    }

    int getPort() {
      return listener.getLocalPort();
    }

    private void acceptLoop() {
      while (!listener.isClosed()) {
        try {
          final Socket client = listener.accept();
          final Socket server = new Socket(InetAddress.getLoopbackAddress(), targetPort);
          client.setTcpNoDelay(true);
          server.setTcpNoDelay(true);
          pipe(client, server);
          pipe(server, client);
        } catch (IOException e) {
          return;
        }
      }
    }

    /**
     * Copies from one socket to the other, delivering each chunk {@code delay} after reading it.
     */
    private void pipe(Socket from, Socket to) {
      final BlockingQueue<Object[]> chunks = new LinkedBlockingQueue<>();
      daemon(
          () -> {
            try {
              final InputStream in = from.getInputStream();
              final byte[] buffer = new byte[65536];
              int read;
              while ((read = in.read(buffer)) >= 0) {
                chunks.add(
                    new Object[] {System.nanoTime() + delayNanos, Arrays.copyOf(buffer, read)});
              }
            } catch (IOException e) {
              // The connection was closed.
            }
            chunks.add(new Object[] {System.nanoTime() + delayNanos, null});
          });
      daemon(
          () -> {
            try {
              final OutputStream out = to.getOutputStream();
              while (true) {
                final Object[] chunk = chunks.take();
                final long wait = (long) chunk[0] - System.nanoTime();
                if (wait > 0) {
                  TimeUnit.NANOSECONDS.sleep(wait);
                }
                if (chunk[1] == null) {
                  to.shutdownOutput();
                  return;
                }
                out.write((byte[]) chunk[1]);
                out.flush();
              }
            } catch (IOException e) {
              // The connection was closed.
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
    }

    private static void daemon(Runnable runnable) {
      final Thread thread = new Thread(runnable, "delay-proxy");
      thread.setDaemon(true);
      thread.start();
    }

    @Override
    public void close() throws IOException {
      listener.close();
    }
  }
}
