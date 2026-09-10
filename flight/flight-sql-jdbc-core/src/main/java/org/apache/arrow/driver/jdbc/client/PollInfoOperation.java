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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightInfo;

/**
 * State shared by every PollFlightInfo call in one synchronous JDBC query execution.
 *
 * <p>The absolute deadline is deliberately created once. Each RPC receives only the remaining
 * budget, while the cancellable gRPC context lets {@code Statement.cancel()} terminate a poll that
 * is active before a ResultSet exists.
 */
public final class PollInfoOperation implements AutoCloseable {
  private static final long NO_DEADLINE = Long.MAX_VALUE;

  private final long deadlineNanos;
  private final Context.CancellableContext context;
  private final AtomicBoolean cancelled = new AtomicBoolean();
  private final AtomicBoolean cancelFlightInfoAttempted = new AtomicBoolean();
  private volatile FlightInfo latestFlightInfo;

  /** Create an operation whose timeout is expressed in JDBC query-timeout seconds. */
  public PollInfoOperation(final int timeoutSeconds) {
    deadlineNanos =
        timeoutSeconds > 0
            ? saturatingAdd(System.nanoTime(), TimeUnit.SECONDS.toNanos(timeoutSeconds))
            : NO_DEADLINE;
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
  }

  boolean isCancelled() {
    return cancelled.get();
  }

  void remember(final FlightInfo flightInfo) {
    latestFlightInfo = flightInfo;
  }

  FlightInfo latestFlightInfo() {
    return latestFlightInfo;
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
}
