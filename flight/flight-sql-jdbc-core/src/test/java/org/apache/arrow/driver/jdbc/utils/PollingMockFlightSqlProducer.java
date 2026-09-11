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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.CancelStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PollInfo;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;

/** Controllable in-process PollInfo producer used by the JDBC POC conformance tests. */
public final class PollingMockFlightSqlProducer extends MockFlightSqlProducer {
  public enum Scenario {
    IMMEDIATE,
    MULTI_STEP,
    UNIMPLEMENTED,
    CONTINUATION_UNIMPLEMENTED,
    UNAVAILABLE,
    TIMEOUT,
    BIND_TIMEOUT,
    BIND_CANCEL,
    CANCEL_OBSERVABLE
  }

  private static final String PREPARED_FAMILY =
      Any.pack(CommandPreparedStatementQuery.getDefaultInstance()).getTypeUrl();

  private static final FlightDescriptor CONTINUATION_1 =
      FlightDescriptor.command("jdbc-continuation-1".getBytes(StandardCharsets.UTF_8));
  private static final FlightDescriptor CONTINUATION_2 =
      FlightDescriptor.command("jdbc-continuation-2".getBytes(StandardCharsets.UTF_8));

  private final List<FlightDescriptor> pollDescriptors =
      Collections.synchronizedList(new ArrayList<>());
  private final AtomicInteger getFlightInfoCount = new AtomicInteger();
  private final AtomicInteger parameterBindCount = new AtomicInteger();
  private final AtomicInteger cancelFlightInfoCount = new AtomicInteger();
  private volatile String lastGetFlightInfoFamily;
  private volatile Scenario scenario = Scenario.IMMEDIATE;
  private volatile String scenarioFamily;
  private volatile boolean scenarioSelectedForExecution;
  private volatile FlightInfo finalFlightInfo;
  private volatile CountDownLatch activePoll = new CountDownLatch(1);
  private volatile CountDownLatch pollTerminated = new CountDownLatch(1);

  public void configure(final Scenario scenario, final String scenarioFamily) {
    this.scenario = scenario;
    this.scenarioFamily = scenarioFamily;
    pollDescriptors.clear();
    getFlightInfoCount.set(0);
    parameterBindCount.set(0);
    cancelFlightInfoCount.set(0);
    lastGetFlightInfoFamily = null;
    scenarioSelectedForExecution = false;
    finalFlightInfo = null;
    activePoll = new CountDownLatch(1);
    pollTerminated = new CountDownLatch(1);
  }

  @Override
  public FlightInfo getFlightInfo(final CallContext context, final FlightDescriptor descriptor) {
    getFlightInfoCount.incrementAndGet();
    lastGetFlightInfoFamily = commandFamily(descriptor);
    return super.getFlightInfo(context, descriptor);
  }

  @Override
  public PollInfo pollFlightInfo(final CallContext context, final FlightDescriptor descriptor) {
    pollDescriptors.add(descriptor);
    final boolean initial = isFlightSqlCommand(descriptor);
    if (initial) {
      finalFlightInfo = super.getFlightInfo(context, descriptor);
      scenarioSelectedForExecution =
          scenarioFamily == null || scenarioFamily.equals(commandFamily(descriptor));
    }

    final boolean selectedFamily = scenarioSelectedForExecution;
    if (initial && selectedFamily && scenario == Scenario.UNIMPLEMENTED) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }
    if (initial && selectedFamily && scenario == Scenario.UNAVAILABLE) {
      throw CallStatus.UNAVAILABLE.toRuntimeException();
    }
    if (selectedFamily && scenario == Scenario.CONTINUATION_UNIMPLEMENTED) {
      if (initial) {
        return new PollInfo(partialInfo(finalFlightInfo), CONTINUATION_1, 0.25, null);
      }
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }
    if (selectedFamily && scenario == Scenario.TIMEOUT) {
      if (initial) {
        sleep(700);
        return new PollInfo(partialInfo(finalFlightInfo), CONTINUATION_1, 0.25, null);
      }
      return blockUntilCancelled(context);
    }
    if (selectedFamily && scenario == Scenario.CANCEL_OBSERVABLE) {
      if (initial) {
        return new PollInfo(partialInfo(finalFlightInfo), CONTINUATION_1, 0.25, null);
      }
      return blockUntilCancelled(context);
    }
    if (selectedFamily && scenario == Scenario.MULTI_STEP) {
      if (initial) {
        return new PollInfo(partialInfo(finalFlightInfo), CONTINUATION_1, 0.25, null);
      }
      if (descriptor.equals(CONTINUATION_1)) {
        return new PollInfo(partialInfo(finalFlightInfo), CONTINUATION_2, 0.75, null);
      }
    }
    return new PollInfo(finalFlightInfo, null, 1.0, null);
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(
      final CommandPreparedStatementQuery command,
      final CallContext context,
      final FlightStream stream,
      final StreamListener<PutResult> listener) {
    parameterBindCount.incrementAndGet();
    final Runnable delegate =
        super.acceptPutPreparedStatementQuery(command, context, stream, listener);
    if (PREPARED_FAMILY.equals(scenarioFamily)
        && (scenario == Scenario.BIND_TIMEOUT || scenario == Scenario.BIND_CANCEL)) {
      return () -> {
        activePoll.countDown();
        try {
          while (!context.isCancelled()) {
            sleep(10);
          }
          throw CallStatus.CANCELLED.toRuntimeException();
        } finally {
          pollTerminated.countDown();
        }
      };
    }
    return delegate;
  }

  @Override
  public void cancelFlightInfo(
      final CancelFlightInfoRequest request,
      final CallContext context,
      final StreamListener<CancelStatus> listener) {
    cancelFlightInfoCount.incrementAndGet();
    listener.onNext(CancelStatus.CANCELLED);
    listener.onCompleted();
  }

  public List<FlightDescriptor> pollDescriptors() {
    synchronized (pollDescriptors) {
      return new ArrayList<>(pollDescriptors);
    }
  }

  public int getFlightInfoCount() {
    return getFlightInfoCount.get();
  }

  public String lastGetFlightInfoFamily() {
    return lastGetFlightInfoFamily;
  }

  public int parameterBindCount() {
    return parameterBindCount.get();
  }

  public int cancelFlightInfoCount() {
    return cancelFlightInfoCount.get();
  }

  public boolean awaitActivePoll(final long timeout, final TimeUnit unit)
      throws InterruptedException {
    return activePoll.await(timeout, unit);
  }

  public boolean awaitPollTerminated(final long timeout, final TimeUnit unit)
      throws InterruptedException {
    return pollTerminated.await(timeout, unit);
  }

  public static String commandFamily(final FlightDescriptor descriptor) {
    if (!descriptor.isCommand()) {
      return "path";
    }
    try {
      return Any.parseFrom(descriptor.getCommand()).getTypeUrl();
    } catch (Exception e) {
      return "continuation";
    }
  }

  private static boolean isFlightSqlCommand(final FlightDescriptor descriptor) {
    return !"continuation".equals(commandFamily(descriptor));
  }

  private PollInfo blockUntilCancelled(final CallContext context) {
    activePoll.countDown();
    try {
      while (!context.isCancelled()) {
        sleep(10);
      }
      throw CallStatus.CANCELLED.toRuntimeException();
    } finally {
      pollTerminated.countDown();
    }
  }

  private static FlightInfo partialInfo(final FlightInfo finalInfo) {
    return new FlightInfo(
        finalInfo.getSchemaOptional().orElse(null),
        finalInfo.getDescriptor(),
        new ArrayList<>(finalInfo.getEndpoints()),
        -1,
        -1);
  }

  private static void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw CallStatus.CANCELLED.withCause(e).toRuntimeException();
    }
  }
}
