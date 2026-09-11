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
package org.apache.arrow.driver.jdbc.client;

import io.grpc.Context;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.PollInfo;

/**
 * State shared by every PollFlightInfo call in one JDBC query execution.
 *
 * <p>The absolute deadline is deliberately created once. Each RPC receives only the remaining
 * budget, while the cancellable gRPC context lets {@code Statement.cancel()} terminate an active
 * poll before or after a ResultSet exists. Progressive operations retain their continuation until
 * the ResultSet consumes all published endpoints.
 */
public final class PollInfoOperation implements AutoCloseable {
  private static final long NO_DEADLINE = Long.MAX_VALUE;

  private final long deadlineNanos;
  private final boolean progressive;
  private final Context.CancellableContext context;
  private final AtomicBoolean cancelled = new AtomicBoolean();
  private final AtomicBoolean cancelFlightInfoAttempted = new AtomicBoolean();
  private volatile FlightInfo latestFlightInfo;
  private volatile FlightDescriptor continuationDescriptor;
  private volatile Function<FlightDescriptor, PollInfo> poller;
  private volatile Runnable cancelFlightInfo;

  /** Create an operation whose timeout is expressed in JDBC query-timeout seconds. */
  public PollInfoOperation(final int timeoutSeconds) {
    this(timeoutSeconds, false);
  }

  /** Create an operation that may hand continuation polling to the JDBC ResultSet. */
  public PollInfoOperation(final int timeoutSeconds, final boolean progressive) {
    deadlineNanos =
        timeoutSeconds > 0
            ? saturatingAdd(System.nanoTime(), TimeUnit.SECONDS.toNanos(timeoutSeconds))
            : NO_DEADLINE;
    this.progressive = progressive;
    context = Context.current().withCancellation();
  }

  /** Run one RPC in the operation's cancellable context. */
  <T> T call(final Callable<T> callable) {
    if (cancelled.get()) {
      throw CallStatus.CANCELLED.withDescription("Statement canceled").toRuntimeException();
    }
    try {
      return context.call(callable);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Return the base options plus this operation's remaining deadline. */
  CallOption[] options(final CallOption[] baseOptions) {
    if (deadlineNanos == NO_DEADLINE) {
      return baseOptions;
    }
    final long remaining = remainingTimeoutNanos();
    if (remaining <= 0) {
      throw CallStatus.TIMED_OUT.withDescription("JDBC query timeout expired").toRuntimeException();
    }
    final CallOption[] options = Arrays.copyOf(baseOptions, baseOptions.length + 1);
    options[baseOptions.length] = CallOptions.timeout(remaining, TimeUnit.NANOSECONDS);
    return options;
  }

  /** Cancel the active PollFlightInfo call, if any. */
  public void cancel() {
    cancelled.set(true);
    context.cancel(null);
    final Runnable cleanup = cancelFlightInfo;
    if (cleanup != null) {
      cleanup.run();
    }
  }

  boolean isCancelled() {
    return cancelled.get();
  }

  void configure(
      final Function<FlightDescriptor, PollInfo> poller,
      final Runnable cancelFlightInfo) {
    this.poller = poller;
    this.cancelFlightInfo = cancelFlightInfo;
  }

  PollInfo poll(final FlightDescriptor descriptor) {
    final Function<FlightDescriptor, PollInfo> currentPoller = poller;
    if (currentPoller == null) {
      throw new IllegalStateException("PollInfo operation has no poller");
    }
    return call(() -> currentPoller.apply(descriptor));
  }

  synchronized void remember(final PollInfo pollInfo) {
    if (pollInfo == null || pollInfo.getFlightInfo() == null) {
      throw CallStatus.INTERNAL
          .withDescription("Server returned PollInfo without FlightInfo")
          .toRuntimeException();
    }
    final FlightInfo nextFlightInfo = pollInfo.getFlightInfo();
    validateAppendOnly(latestFlightInfo, nextFlightInfo);
    latestFlightInfo = nextFlightInfo;
    continuationDescriptor = pollInfo.getFlightDescriptor().orElse(null);
  }

  synchronized void complete(final FlightInfo flightInfo) {
    latestFlightInfo = flightInfo;
    continuationDescriptor = null;
  }

  /** Poll until another endpoint is appended, the query completes, or the poll fails. */
  public synchronized FlightInfo pollNextAvailable() {
    final int previousEndpointCount = endpointCount(latestFlightInfo);
    while (continuationDescriptor != null) {
      remember(poll(continuationDescriptor));
      if (endpointCount(latestFlightInfo) > previousEndpointCount
          || continuationDescriptor == null) {
        return latestFlightInfo;
      }
    }
    return latestFlightInfo;
  }

  FlightInfo latestFlightInfo() {
    return latestFlightInfo;
  }

  FlightDescriptor continuationDescriptor() {
    return continuationDescriptor;
  }

  /** Whether this operation can return before PollFlightInfo reaches completion. */
  public boolean isProgressive() {
    return progressive;
  }

  /** Whether another continuation poll is required. */
  public boolean hasContinuation() {
    return continuationDescriptor != null;
  }

  /** Whether PollFlightInfo has reached a response without a continuation descriptor. */
  public boolean isComplete() {
    return latestFlightInfo != null && continuationDescriptor == null;
  }

  /** Mark a failed continuation as terminal without discarding already published endpoints. */
  public synchronized void terminate() {
    continuationDescriptor = null;
  }

  boolean beginCancelFlightInfoAttempt() {
    return latestFlightInfo != null && cancelFlightInfoAttempted.compareAndSet(false, true);
  }

  /** Return the remaining operation-wide timeout, in nanoseconds. */
  public long remainingTimeoutNanos() {
    if (deadlineNanos == NO_DEADLINE) {
      return NO_DEADLINE;
    }
    return deadlineNanos - System.nanoTime();
  }

  /** Whether this operation has a finite deadline. */
  public boolean hasDeadline() {
    return deadlineNanos != NO_DEADLINE;
  }

  @Override
  public void close() {
    context.close();
  }

  private static long saturatingAdd(final long left, final long right) {
    final long result = left + right;
    if (((left ^ result) & (right ^ result)) < 0) {
      return Long.MAX_VALUE;
    }
    return result;
  }

  private static int endpointCount(final FlightInfo flightInfo) {
    return flightInfo == null ? 0 : flightInfo.getEndpoints().size();
  }

  private static void validateAppendOnly(
      final FlightInfo previous, final FlightInfo current) {
    if (previous == null) {
      return;
    }
    final List<FlightEndpoint> previousEndpoints = previous.getEndpoints();
    final List<FlightEndpoint> currentEndpoints = current.getEndpoints();
    if (currentEndpoints.size() < previousEndpoints.size()) {
      throw CallStatus.INTERNAL
          .withDescription("PollInfo removed previously published endpoints")
          .toRuntimeException();
    }
    for (int index = 0; index < previousEndpoints.size(); index++) {
      if (!previousEndpoints.get(index).equals(currentEndpoints.get(index))) {
        throw CallStatus.INTERNAL
            .withDescription("PollInfo mutated previously published endpoint " + index)
            .toRuntimeException();
      }
    }
  }
}
