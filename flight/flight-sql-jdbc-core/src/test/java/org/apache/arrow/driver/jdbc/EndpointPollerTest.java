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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.arrow.driver.jdbc.client.PollInfoOperation;
import org.apache.arrow.driver.jdbc.utils.FlightEndpointDataQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Tests how {@link EndpointPoller} ends: whatever stops the poller must reach the reader. */
@Timeout(60)
public class EndpointPollerTest {
  private static final long WAIT_MS = 10_000;

  private ExecutorService readers;
  private ExecutorService pollerPool;

  @BeforeEach
  public void setUp() {
    readers = Executors.newCachedThreadPool(daemonThreads());
    // An Error escapes the pool task by design; keep it out of the test output.
    pollerPool = Executors.newCachedThreadPool(daemonThreads());
  }

  @AfterEach
  public void tearDown() {
    readers.shutdownNow();
    pollerPool.shutdownNow();
  }

  private static ThreadFactory daemonThreads() {
    return runnable -> {
      final Thread thread = new Thread(runnable, "endpoint-poller-test");
      thread.setDaemon(true);
      thread.setUncaughtExceptionHandler((t, e) -> {});
      return thread;
    };
  }

  // F2. An Error on the poller thread used to leave next() waiting forever.
  @Test
  public void anErrorOnThePollerThreadFailsTheQueueInsteadOfHangingTheReader() throws Exception {
    final StackOverflowError error = new StackOverflowError("scripted");
    final PollInfoOperation operation = mock(PollInfoOperation.class);
    when(operation.pollOnce()).thenThrow(error);
    try (FlightEndpointDataQueue queue =
        FlightEndpointDataQueue.createNewQueue(
            readers,
            2,
            endpoint -> {
              throw new AssertionError("no endpoint is expected");
            })) {
      queue.addEndpoints(Collections.emptyList(), true);
      final EndpointPoller poller =
          new EndpointPoller(
              queue,
              operation,
              pollerPool,
              answer -> {},
              exception -> new SQLException(exception),
              exception -> {},
              () -> {},
              100);
      assertTrue(poller.start());

      final CompletableFuture<Object> next = new CompletableFuture<>();
      final Thread reader =
          new Thread(
              () -> {
                try {
                  next.complete(queue.next());
                } catch (final Throwable t) {
                  next.completeExceptionally(t);
                }
              });
      reader.setDaemon(true);
      reader.start();

      final ExecutionException thrown;
      try {
        next.get(WAIT_MS, TimeUnit.MILLISECONDS);
        throw new AssertionError("next() returned instead of failing");
      } catch (final TimeoutException e) {
        throw new AssertionError("next() hung after an Error on the poller thread", e);
      } catch (final ExecutionException e) {
        thrown = e;
      }
      final SQLException failure = assertInstanceOf(SQLException.class, thrown.getCause());
      assertEquals("Continuation PollFlightInfo failed", failure.getMessage());
      assertSame(error, failure.getCause());
      assertEquals(EndpointPoller.State.FAILED, poller.state());
    }
  }
}
