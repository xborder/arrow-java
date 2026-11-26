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

/**
 * Exponential backoff between PollFlightInfo answers that add no endpoints: 100 ms, doubling up to
 * a maximum. Not thread safe; each poll loop owns one instance.
 */
public final class PollBackoff {
  /** Default longest wait, in milliseconds. */
  public static final long DEFAULT_MAX_MILLIS = 5000L;

  private static final long INITIAL_MILLIS = 100L;

  private final long maxMillis;
  private long nextMillis = INITIAL_MILLIS;

  /** Create a backoff capped at {@code maxMillis} (at least 1). */
  public PollBackoff(final long maxMillis) {
    this.maxMillis = Math.max(1L, maxMillis);
  }

  /** The delay to wait now; the following delay doubles, up to the cap. */
  public long nextDelayMillis() {
    final long delay = Math.min(nextMillis, maxMillis);
    nextMillis = Math.min(maxMillis, nextMillis * 2);
    return delay;
  }

  /** Start again at the initial delay, after an answer that added endpoints. */
  public void reset() {
    nextMillis = INITIAL_MILLIS;
  }
}
