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
package org.apache.arrow.driver.jdbc.utils;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.PollInfo;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * One scripted progressive query served by {@link PollingMockFlightSqlProducer}.
 *
 * <p>The script is a list of {@link Answer}s: answer 0 answers the initial PollFlightInfo and
 * answer {@code i} answers the continuation {@code i}. Endpoints are numbered from 0 in the order
 * the answers publish them. Endpoint {@code id} streams {@code rowsPerEndpoint} rows in one batch,
 * row {@code r} holding {@code id * 100 + r}. Every DoGet stream can be held before it sends
 * anything (start gate) or after it sent its batch but before it completes (end gate), so a test
 * decides when a stream is "open and unread" from the server's point of view.
 *
 * <p>The query records every poll, DoGet start/end/cancel and CancelFlightInfo with a nanosecond
 * timestamp, and counts concurrently open streams and in-flight polls.
 */
public final class ScriptedPollQuery {
  /** Schema of every endpoint stream. */
  public static final Schema SCHEMA =
      new Schema(Collections.singletonList(Field.nullable("answer", new ArrowType.Int(32, true))));

  /** A schema a continuation answer can switch to by mistake. */
  static final Schema CHANGED_SCHEMA =
      new Schema(
          List.of(
              Field.nullable("answer", new ArrowType.Int(32, true)),
              Field.nullable("extra", new ArrowType.Int(32, true))));

  private static final AtomicInteger KEYS = new AtomicInteger();

  /** What the server records. */
  public enum EventType {
    POLL,
    POLL_CANCELLED,
    DOGET_START,
    DOGET_END,
    DOGET_CANCELLED,
    CANCEL_FLIGHT_INFO
  }

  /** One recorded server-side event. */
  public static final class Event {
    private final EventType type;
    private final int index;
    private final long nanos;

    Event(final EventType type, final int index, final long nanos) {
      this.type = type;
      this.index = index;
      this.nanos = nanos;
    }

    public EventType type() {
      return type;
    }

    /** The answer index of a poll, or the endpoint id of a stream. */
    public int index() {
      return index;
    }

    public long nanos() {
      return nanos;
    }

    @Override
    public String toString() {
      return type + ":" + index;
    }
  }

  /** The server's answer to one poll. */
  public static final class Answer {
    private final int newEndpoints;
    private final boolean done;
    private final boolean failure;
    private final boolean schemaChange;
    private final CountDownLatch gate;

    private Answer(
        final int newEndpoints,
        final boolean done,
        final boolean failure,
        final boolean schemaChange,
        final CountDownLatch gate) {
      this.newEndpoints = newEndpoints;
      this.done = done;
      this.failure = failure;
      this.schemaChange = schemaChange;
      this.gate = gate;
    }

    /** Publishes {@code count} more endpoints; the query keeps running. */
    public static Answer endpoints(final int count) {
      return new Answer(count, false, false, false, null);
    }

    /** Publishes {@code count} more endpoints and completes the query. */
    public static Answer done(final int count) {
      return new Answer(count, true, false, false, null);
    }

    /** Publishes nothing; the query keeps running. */
    public static Answer empty() {
      return endpoints(0);
    }

    /** Fails the poll with INTERNAL. */
    public static Answer fail() {
      return new Answer(0, false, true, false, null);
    }

    /** Publishes one more endpoint but announces a different schema. */
    public static Answer schemaChange() {
      return new Answer(1, false, false, true, null);
    }

    /** Never answers; the poll ends only when the client cancels it or its deadline expires. */
    public static Answer hang() {
      return empty().gatedBy(new CountDownLatch(1));
    }

    /** Holds the answer until {@code gate} is released, or the client cancels the poll. */
    public Answer gatedBy(final CountDownLatch gate) {
      return new Answer(newEndpoints, done, failure, schemaChange, gate);
    }
  }

  private final int key = KEYS.incrementAndGet();
  private final String query;
  private final List<Answer> answers;
  private final FlightDescriptor descriptor;
  private volatile int rowsPerEndpoint = 1;

  private final List<Event> events = new ArrayList<>();
  private final Map<Integer, CountDownLatch> startGates = new HashMap<>();
  private final Map<Integer, CountDownLatch> endGates = new HashMap<>();
  private final CountDownLatch blockedPoll = new CountDownLatch(1);
  private final CountDownLatch pollTerminated = new CountDownLatch(1);
  private int openStreams;
  private int maxOpenStreams;
  private int inFlightPolls;
  private int maxInFlightPolls;

  ScriptedPollQuery(final String query, final List<Answer> answers) {
    this.query = query;
    this.answers = new ArrayList<>(answers);
    this.descriptor =
        FlightDescriptor.command(
            Any.pack(CommandStatementQuery.newBuilder().setQuery(query).build()).toByteArray());
  }

  public String query() {
    return query;
  }

  /** Streams {@code rows} rows per endpoint, in one batch. */
  public ScriptedPollQuery rowsPerEndpoint(final int rows) {
    this.rowsPerEndpoint = rows;
    return this;
  }

  // ---- hold and release -------------------------------------------------------------------

  /** Holds the listed streams before they send anything. */
  public ScriptedPollQuery holdStart(final int... ids) {
    synchronized (this) {
      for (final int id : ids) {
        startGates.put(id, new CountDownLatch(1));
      }
    }
    return this;
  }

  /** Holds the listed streams after they sent their batch, before they complete. */
  public ScriptedPollQuery holdEnd(final int... ids) {
    synchronized (this) {
      for (final int id : ids) {
        endGates.put(id, new CountDownLatch(1));
      }
    }
    return this;
  }

  /** Holds the end of streams {@code 0..count-1}. */
  public ScriptedPollQuery holdAllEnds(final int count) {
    for (int id = 0; id < count; id++) {
      holdEnd(id);
    }
    return this;
  }

  public void releaseStart(final int id) {
    final CountDownLatch gate;
    synchronized (this) {
      gate = startGates.get(id);
    }
    if (gate != null) {
      gate.countDown();
    }
  }

  public void releaseEnd(final int id) {
    final CountDownLatch gate;
    synchronized (this) {
      gate = endGates.get(id);
    }
    if (gate != null) {
      gate.countDown();
    }
  }

  /** Releases every held stream. */
  public void releaseAll() {
    synchronized (this) {
      startGates.values().forEach(CountDownLatch::countDown);
      endGates.values().forEach(CountDownLatch::countDown);
    }
  }

  // ---- observations -----------------------------------------------------------------------

  public synchronized List<Event> events() {
    return new ArrayList<>(events);
  }

  /** The events of the given types, as {@code TYPE:index} strings, in arrival order. */
  public List<String> eventNames(final EventType... types) {
    final List<EventType> wanted = List.of(types);
    final List<String> names = new ArrayList<>();
    for (final Event event : events()) {
      if (wanted.contains(event.type)) {
        names.add(event.toString());
      }
    }
    return names;
  }

  public synchronized int count(final EventType type) {
    int count = 0;
    for (final Event event : events) {
      if (event.type == type) {
        count++;
      }
    }
    return count;
  }

  public synchronized boolean hasEvent(final EventType type, final int index) {
    return events.stream().anyMatch(event -> event.type == type && event.index == index);
  }

  public int pollCount() {
    return count(EventType.POLL);
  }

  public int doGetStarted() {
    return count(EventType.DOGET_START);
  }

  public int cancelFlightInfoCount() {
    return count(EventType.CANCEL_FLIGHT_INFO);
  }

  /** The answer indices of the polls, in arrival order. */
  public List<Integer> pollIndices() {
    final List<Integer> indices = new ArrayList<>();
    for (final Event event : events()) {
      if (event.type == EventType.POLL) {
        indices.add(event.index);
      }
    }
    return indices;
  }

  /** The arrival times, in nanoseconds, of the polls, in arrival order. */
  public List<Long> pollNanos() {
    final List<Long> nanos = new ArrayList<>();
    for (final Event event : events()) {
      if (event.type == EventType.POLL) {
        nanos.add(event.nanos);
      }
    }
    return nanos;
  }

  public synchronized int maxOpenStreams() {
    return maxOpenStreams;
  }

  public synchronized int maxInFlightPolls() {
    return maxInFlightPolls;
  }

  public boolean awaitPollCount(final int count, final long millis) throws InterruptedException {
    return awaitEvents(events -> countOf(events, EventType.POLL) >= count, millis);
  }

  public boolean awaitDoGetStarted(final int count, final long millis) throws InterruptedException {
    return awaitEvents(events -> countOf(events, EventType.DOGET_START) >= count, millis);
  }

  public boolean awaitStream(final EventType type, final int id, final long millis)
      throws InterruptedException {
    return awaitEvents(
        events -> events.stream().anyMatch(e -> e.type == type && e.index == id), millis);
  }

  public boolean awaitCancelFlightInfo(final int count, final long millis)
      throws InterruptedException {
    return awaitEvents(events -> countOf(events, EventType.CANCEL_FLIGHT_INFO) >= count, millis);
  }

  /** Waits until a poll is blocked on a gate (or a hang). */
  public boolean awaitBlockedPoll(final long millis) throws InterruptedException {
    return blockedPoll.await(millis, TimeUnit.MILLISECONDS);
  }

  /** Waits until a blocked poll ended because the client cancelled it. */
  public boolean awaitPollTerminated(final long millis) throws InterruptedException {
    return pollTerminated.await(millis, TimeUnit.MILLISECONDS);
  }

  private static int countOf(final List<Event> events, final EventType type) {
    int count = 0;
    for (final Event event : events) {
      if (event.type == type) {
        count++;
      }
    }
    return count;
  }

  private synchronized boolean awaitEvents(
      final Predicate<List<Event>> condition, final long millis) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
    while (!condition.test(events)) {
      final long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        return false;
      }
      TimeUnit.NANOSECONDS.timedWait(this, remaining);
    }
    return true;
  }

  private synchronized void record(final EventType type, final int index) {
    events.add(new Event(type, index, System.nanoTime()));
    notifyAll();
  }

  // ---- server side ------------------------------------------------------------------------

  FlightDescriptor descriptor() {
    return descriptor;
  }

  /** Whether {@code candidate} is this query's statement command (the initial poll). */
  boolean matches(final FlightDescriptor candidate) {
    if (!candidate.isCommand()) {
      return false;
    }
    try {
      final Any command = Any.parseFrom(candidate.getCommand());
      return command.is(CommandStatementQuery.class)
          && query.equals(command.unpack(CommandStatementQuery.class).getQuery());
    } catch (final InvalidProtocolBufferException e) {
      return false;
    }
  }

  void recordCancelFlightInfo() {
    record(EventType.CANCEL_FLIGHT_INFO, 0);
  }

  private FlightDescriptor continuation(final int index) {
    return FlightDescriptor.command(
        ("jdbc-continuation-s" + key + "-" + index).getBytes(StandardCharsets.UTF_8));
  }

  /** The answer index a continuation descriptor stands for, or -1 for another query's. */
  int continuationIndex(final FlightDescriptor candidate) {
    if (!candidate.isCommand()) {
      return -1;
    }
    final String prefix = "jdbc-continuation-s" + key + "-";
    final String text = new String(candidate.getCommand(), StandardCharsets.UTF_8);
    if (!text.startsWith(prefix)) {
      return -1;
    }
    return Integer.parseInt(text.substring(prefix.length()));
  }

  boolean ownsTicket(final Ticket ticket) {
    return new String(ticket.getBytes(), StandardCharsets.UTF_8).startsWith(ticketPrefix());
  }

  private String ticketPrefix() {
    return "jdbc-script-" + key + "-";
  }

  PollInfo poll(final CallContext context, final int index) {
    if (index >= answers.size()) {
      throw CallStatus.INTERNAL
          .withDescription("Script exhausted at poll " + index + " of " + query)
          .toRuntimeException();
    }
    final Answer answer = answers.get(index);
    record(EventType.POLL, index);
    synchronized (this) {
      inFlightPolls++;
      maxInFlightPolls = Math.max(maxInFlightPolls, inFlightPolls);
    }
    try {
      if (answer.gate != null) {
        blockedPoll.countDown();
        while (!awaitGate(answer.gate)) {
          if (context.isCancelled()) {
            record(EventType.POLL_CANCELLED, index);
            pollTerminated.countDown();
            throw CallStatus.CANCELLED.withDescription("poll cancelled").toRuntimeException();
          }
        }
      }
      if (answer.failure) {
        throw CallStatus.INTERNAL.withDescription("scripted failure").toRuntimeException();
      }
      int published = 0;
      boolean schemaChanged = false;
      for (int i = 0; i <= index; i++) {
        published += answers.get(i).newEndpoints;
        schemaChanged |= answers.get(i).schemaChange;
      }
      final List<FlightEndpoint> endpoints = new ArrayList<>();
      for (int id = 0; id < published; id++) {
        endpoints.add(
            new FlightEndpoint(new Ticket((ticketPrefix() + id).getBytes(StandardCharsets.UTF_8))));
      }
      final FlightInfo info =
          new FlightInfo(schemaChanged ? CHANGED_SCHEMA : SCHEMA, descriptor, endpoints, -1, -1);
      final FlightDescriptor next = answer.done ? null : continuation(index + 1);
      return new PollInfo(info, next, answer.done ? 1.0 : 0.5, null);
    } finally {
      synchronized (this) {
        inFlightPolls--;
      }
    }
  }

  private static boolean awaitGate(final CountDownLatch gate) {
    try {
      return gate.await(10, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw CallStatus.CANCELLED.withCause(e).toRuntimeException();
    }
  }

  void doGet(final Ticket ticket, final ServerStreamListener listener) {
    final int id =
        Integer.parseInt(
            new String(ticket.getBytes(), StandardCharsets.UTF_8)
                .substring(ticketPrefix().length()));
    record(EventType.DOGET_START, id);
    synchronized (this) {
      openStreams++;
      maxOpenStreams = Math.max(maxOpenStreams, openStreams);
    }
    boolean open = true;
    try {
      if (!awaitStreamGate(startGates, id, listener)) {
        return;
      }
      sendBatch(id, listener);
      if (!awaitStreamGate(endGates, id, listener)) {
        return;
      }
      // The stream stops being open before the client can see it complete.
      synchronized (this) {
        openStreams--;
      }
      open = false;
      record(EventType.DOGET_END, id);
      listener.completed();
    } finally {
      if (open) {
        synchronized (this) {
          openStreams--;
        }
      }
    }
  }

  // Returns false (after recording it) if the client cancelled the stream while it was held.
  private boolean awaitStreamGate(
      final Map<Integer, CountDownLatch> gates, final int id, final ServerStreamListener listener) {
    final CountDownLatch gate;
    synchronized (this) {
      gate = gates.get(id);
    }
    if (gate == null) {
      return true;
    }
    while (!awaitGate(gate)) {
      if (listener.isCancelled()) {
        record(EventType.DOGET_CANCELLED, id);
        return false;
      }
    }
    return true;
  }

  private void sendBatch(final int id, final ServerStreamListener listener) {
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
      root.allocateNew();
      final IntVector vector = (IntVector) root.getVector("answer");
      for (int row = 0; row < rowsPerEndpoint; row++) {
        vector.setSafe(row, id * 100 + row);
      }
      root.setRowCount(rowsPerEndpoint);
      listener.start(root);
      listener.putNext();
    }
  }
}
