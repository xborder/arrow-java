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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SyncPutListener;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.example.FlightSqlExample;
import org.apache.arrow.flight.sql.example.FlightSqlStatelessExample;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.DoPutPreparedStatementResult;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Differential conformance test for the Flight SQL prepared-statement lifecycle.
 *
 * <p>The expected behaviour comes from a Bend 2 model of the protocol whose laws are
 * machine-checked ({@code dev/bend2/prepared_statement/}). The model's trace generator enumerates
 * request sequences and records the response the spec-conforming server gives to each request,
 * under both bind policies the spec allows: a server that returns an updated handle when parameters
 * are bound ("rotate") and one that keeps the handle ("keep"). This test replays every trace
 * against a real server through the raw Flight client and compares the success-or-error outcome of
 * each request with the model's.
 *
 * <p>Deviations are grouped by category (request kind, expected outcome, observed outcome). Each
 * server has a set of known, documented deviations; the test fails if a new category appears or a
 * known one disappears, so a fix to the example server shows up as a test change.
 *
 * <p>See {@code dev/bend2/DIFFERENTIAL_TESTING.md} for the design.
 */
public class TestFlightSqlBendConformance {

  private static final String TRACES = "/bend2/prepared_statement_traces.txt";
  private static final String LOCALHOST = "localhost";

  /** The stateful example keeps the handle on bind; the model's keep column applies. */
  @Test
  public void statefulExampleConformsUnderKeepPolicy() throws Exception {
    final Report report = replayAll("derbyBendKeepDB", false);
    assertThat(report.stepsCompared).isGreaterThan(1000);
    assertThat(report.categories()).isEqualTo(Collections.emptySet());
  }

  /**
   * The stateless example rotates the handle on bind; the model's rotate column applies. Its known
   * deviations from the model are listed in {@link #KNOWN_STATELESS_DEVIATIONS}.
   */
  @Test
  public void statelessExampleConformsUnderRotatePolicyExceptKnownDeviations() throws Exception {
    final Report report = replayAll("derbyBendRotateDB", true);
    assertThat(report.stepsCompared).isGreaterThan(1000);
    assertThat(report.categories()).isEqualTo(KNOWN_STATELESS_DEVIATIONS);
  }

  /**
   * Deviations of {@link FlightSqlStatelessExample} from the model, by category. The stateless
   * example encodes the query text (and, after a bind, the bound parameters) into the handle and
   * validates nothing on the server, so:
   *
   * <ul>
   *   <li>{@code E expected err observed ok}: executing a closed handle or a stale (pre-bind)
   *       handle succeeds, because the handle still carries a valid query. The spec says the server
   *       "should return an error" for a stale handle and that close "closes server resources".
   *   <li>{@code B expected err observed ok}: binding a closed or stale handle succeeds for the
   *       same reason.
   *   <li>{@code B expected ok observed err}: binding the rotated handle a second time fails,
   *       because the server reads the rotated handle as query text. The spec allows chaining a
   *       DoPut handle into another DoPut.
   * </ul>
   */
  private static final Set<String> KNOWN_STATELESS_DEVIATIONS =
      new TreeSet<>(
          List.of(
              "E expected err observed ok",
              "B expected err observed ok",
              "B expected ok observed err"));

  // Trace file
  // ----------

  /** One generated trace: the actions and the expected responses under each policy. */
  static final class Trace {
    final String[] actions;
    final String[] expectRotate;
    final String[] expectKeep;

    Trace(String line) {
      final String[] parts = line.split("\\|");
      actions = parts[0].trim().split(" ");
      expectRotate = parts[1].trim().split(" ");
      expectKeep = parts[2].trim().split(" ");
    }

    String[] expected(boolean rotate) {
      return rotate ? expectRotate : expectKeep;
    }
  }

  static List<Trace> loadTraces() throws IOException {
    final List<Trace> traces = new ArrayList<>();
    try (InputStream in = TestFlightSqlBendConformance.class.getResourceAsStream(TRACES);
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        traces.add(new Trace(line));
      }
    }
    return traces;
  }

  // Replay
  // ------

  /** What the replay found: the number of steps compared and every deviation. */
  static final class Report {
    int stepsCompared;
    final Map<String, Integer> deviationCounts = new TreeMap<>();
    final Map<String, String> firstExample = new TreeMap<>();

    void deviation(String category, String example) {
      deviationCounts.merge(category, 1, Integer::sum);
      firstExample.putIfAbsent(category, example);
    }

    Set<String> categories() {
      return new TreeSet<>(deviationCounts.keySet());
    }

    @Override
    public String toString() {
      return deviationCounts.entrySet().stream()
          .map(e -> e.getKey() + " x" + e.getValue() + " e.g. " + firstExample.get(e.getKey()))
          .collect(Collectors.joining("\n"));
    }
  }

  private static Report replayAll(String dbName, boolean rotate) throws Exception {
    final List<Trace> traces = loadTraces();
    final Report report = new Report();
    final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 0);
    final FlightSqlProducer producer =
        rotate
            ? new FlightSqlStatelessExample(serverLocation, dbName)
            : new FlightSqlExample(serverLocation, dbName);
    try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        FlightServer server =
            FlightServer.builder(allocator, serverLocation, producer).build().start();
        FlightClient client =
            FlightClient.builder(allocator, Location.forGrpcInsecure(LOCALHOST, server.getPort()))
                .build();
        VectorSchemaRoot parameters = VectorSchemaRoot.create(PARAMETER_SCHEMA, allocator)) {
      final Replayer replayer = new Replayer(client, parameters, rotate, report);
      for (Trace trace : traces) {
        replayer.replay(trace);
      }
    } finally {
      AutoCloseables.close(producer);
      FlightSqlExample.removeDerbyDatabaseIfExists(dbName);
    }
    System.out.println(
        "Bend conformance ("
            + (rotate ? "rotate" : "keep")
            + "): "
            + report.stepsCompared
            + " steps compared, deviations:\n"
            + report);
    return report;
  }

  private static final Schema PARAMETER_SCHEMA =
      new Schema(List.of(Field.nullable("p0", new ArrowType.Int(32, true))));

  /** Replays traces against one server, mapping the model's handle ids to real handles. */
  static final class Replayer {
    private final FlightClient client;
    private final VectorSchemaRoot parameters;
    private final boolean rotate;
    private final Report report;

    Replayer(FlightClient client, VectorSchemaRoot parameters, boolean rotate, Report report) {
      this.client = client;
      this.parameters = parameters;
      this.rotate = rotate;
      this.report = report;
    }

    void replay(Trace trace) {
      // model handle id -> the bytes the server actually issued for it
      final Map<Integer, ByteString> handles = new HashMap<>();
      final String[] expected = trace.expected(rotate);
      int created = 0;
      for (int i = 0; i < trace.actions.length; i++) {
        final String action = trace.actions[i];
        final String want = expected[i];
        final Outcome got;
        switch (action.charAt(0)) {
          case 'C':
            // a distinct query per Create: the example servers use the query text as the handle
            created++;
            got = create("SELECT * FROM intTable WHERE id = ? AND " + created + " = " + created);
            break;
          case 'B':
            got = bind(handleFor(handles, action));
            break;
          case 'E':
            got = execute(handleFor(handles, action));
            break;
          case 'X':
            got = close(handleFor(handles, action));
            break;
          default:
            throw new IllegalArgumentException("unknown action " + action);
        }
        // the model told us which id the server's Ok carries: remember its real bytes
        if (got.ok && want.startsWith("ok") && got.handle != null) {
          handles.put(Integer.parseInt(want.substring(2)), got.handle);
        }
        report.stepsCompared++;
        final String wantKind = want.startsWith("ok") ? "ok" : "err";
        final String gotKind = got.ok ? "ok" : "err";
        if (!wantKind.equals(gotKind)) {
          report.deviation(
              action.charAt(0) + " expected " + wantKind + " observed " + gotKind,
              String.join(" ", trace.actions) + " @" + (i + 1) + " (" + got.detail + ")");
        }
      }
      // leave no statement behind between traces
      for (ByteString handle : handles.values()) {
        close(handle);
      }
    }

    private static ByteString handleFor(Map<Integer, ByteString> handles, String action) {
      final int id = Integer.parseInt(action.substring(1));
      // a handle the model never issued in this trace: bytes no server has seen
      return handles.getOrDefault(id, ByteString.copyFromUtf8("bend2-unknown-handle-" + id));
    }

    private Outcome create(String query) {
      try {
        final Action action =
            new Action(
                FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(),
                Any.pack(ActionCreatePreparedStatementRequest.newBuilder().setQuery(query).build())
                    .toByteArray());
        final Result result = client.doAction(action).next();
        final ActionCreatePreparedStatementResult parsed =
            FlightSqlUtils.unpackAndParseOrThrow(
                result.getBody(), ActionCreatePreparedStatementResult.class);
        return Outcome.ok(parsed.getPreparedStatementHandle());
      } catch (RuntimeException e) {
        return Outcome.err(e);
      }
    }

    private Outcome bind(ByteString handle) {
      try (SyncPutListener listener = new SyncPutListener()) {
        parameters.allocateNew();
        ((IntVector) parameters.getVector(0)).setSafe(0, 1);
        parameters.setRowCount(1);
        final FlightClient.ClientStreamListener writer =
            client.startPut(descriptor(handle), parameters, listener);
        writer.putNext();
        writer.completed();
        writer.getResult();
        ByteString updated = null;
        final PutResult putResult = listener.read();
        if (putResult != null) {
          try (ArrowBuf metadata = putResult.getApplicationMetadata()) {
            if (metadata != null) {
              final DoPutPreparedStatementResult parsed =
                  DoPutPreparedStatementResult.parseFrom(metadata.nioBuffer());
              if (!parsed.getPreparedStatementHandle().isEmpty()) {
                updated = parsed.getPreparedStatementHandle();
              }
            }
          }
        }
        // no updated handle: the server kept the one we sent
        return Outcome.ok(updated != null ? updated : handle);
      } catch (RuntimeException
          | InterruptedException
          | java.util.concurrent.ExecutionException
          | com.google.protobuf.InvalidProtocolBufferException e) {
        return Outcome.err(e);
      }
    }

    private Outcome execute(ByteString handle) {
      try {
        client.getInfo(descriptor(handle));
        return Outcome.ok(handle);
      } catch (RuntimeException e) {
        return Outcome.err(e);
      }
    }

    private Outcome close(ByteString handle) {
      try {
        final Action action =
            new Action(
                FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT.getType(),
                Any.pack(
                        ActionClosePreparedStatementRequest.newBuilder()
                            .setPreparedStatementHandle(handle)
                            .build())
                    .toByteArray());
        client.doAction(action).forEachRemaining(result -> {});
        return Outcome.ok(handle);
      } catch (RuntimeException e) {
        return Outcome.err(e);
      }
    }

    private static FlightDescriptor descriptor(ByteString handle) {
      return FlightDescriptor.command(
          Any.pack(
                  CommandPreparedStatementQuery.newBuilder()
                      .setPreparedStatementHandle(handle)
                      .build())
              .toByteArray());
    }
  }

  /** The outcome of one request: success with the handle to use next, or an error. */
  static final class Outcome {
    final boolean ok;
    final ByteString handle;
    final String detail;

    private Outcome(boolean ok, ByteString handle, String detail) {
      this.ok = ok;
      this.handle = handle;
      this.detail = detail;
    }

    static Outcome ok(ByteString handle) {
      return new Outcome(true, handle, "ok");
    }

    static Outcome err(Exception e) {
      final String message = e.getMessage() == null ? "" : e.getMessage();
      return new Outcome(
          false,
          null,
          e.getClass().getSimpleName() + ": " + message.lines().findFirst().orElse(""));
    }
  }
}
