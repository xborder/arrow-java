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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class PollBackoffTest {
  private static List<Long> delays(final PollBackoff backoff, final int count) {
    final List<Long> delays = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      delays.add(backoff.nextDelayMillis());
    }
    return delays;
  }

  @Test
  public void doublesFromOneHundredMillisecondsUpToTheCap() {
    assertEquals(
        List.of(100L, 200L, 400L, 800L, 1600L, 3200L, 5000L, 5000L),
        delays(new PollBackoff(PollBackoff.DEFAULT_MAX_MILLIS), 8));
  }

  @Test
  public void capBetweenTwoStepsCutsTheLaterStepAndStaysThere() {
    assertEquals(List.of(100L, 150L, 150L, 150L), delays(new PollBackoff(150), 4));
  }

  @Test
  public void capBelowTheInitialDelayIsTheOnlyDelay() {
    assertEquals(List.of(50L, 50L, 50L), delays(new PollBackoff(50), 3));
  }

  @Test
  public void capBelowOneMillisecondIsRaisedToOne() {
    assertEquals(List.of(1L, 1L), delays(new PollBackoff(0), 2));
    assertEquals(List.of(1L, 1L), delays(new PollBackoff(-5), 2));
  }

  @Test
  public void resetStartsAgainFromTheInitialDelay() {
    final PollBackoff backoff = new PollBackoff(5000);
    assertEquals(List.of(100L, 200L, 400L), delays(backoff, 3));
    backoff.reset();
    assertEquals(List.of(100L, 200L), delays(backoff, 2));
  }

  @Test
  public void resetAfterReachingTheCapStartsAgainFromTheInitialDelay() {
    final PollBackoff backoff = new PollBackoff(300);
    assertEquals(List.of(100L, 200L, 300L, 300L), delays(backoff, 4));
    backoff.reset();
    assertEquals(List.of(100L, 200L, 300L), delays(backoff, 3));
  }
}
