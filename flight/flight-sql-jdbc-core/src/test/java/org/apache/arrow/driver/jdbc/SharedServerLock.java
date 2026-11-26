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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A cross-JVM lock that serializes the test classes sharing the one standalone conformance server.
 *
 * <p>The root POM runs Surefire with several reused forks, and every shared-server class resets the
 * server and asserts its global counters, so two classes running in two forks would corrupt each
 * other. A class takes the lock in {@code @BeforeAll} and releases it in {@code @AfterAll}. The
 * lock file lives in {@code java.io.tmpdir} (Surefire points it at {@code target/}), named after
 * the control port, so every fork of one build agrees on it.
 */
final class SharedServerLock implements AutoCloseable {
  private static final long TIMEOUT_MINUTES = 10;
  private static final long RETRY_PARK_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

  private final FileChannel channel;
  private final FileLock lock;

  private SharedServerLock(final FileChannel channel, final FileLock lock) {
    this.channel = channel;
    this.lock = lock;
  }

  /** Blocks until no other class, in this JVM or another, holds the lock for {@code port}. */
  static SharedServerLock acquire(final int controlPort) throws IOException {
    final Path file =
        Paths.get(System.getProperty("java.io.tmpdir"), "pollinfo-shared-server-" + controlPort)
            .toAbsolutePath();
    Files.createDirectories(file.getParent());
    final FileChannel channel =
        FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    final long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(TIMEOUT_MINUTES);
    try {
      while (true) {
        try {
          final FileLock lock = channel.tryLock();
          if (lock != null) {
            return new SharedServerLock(channel, lock);
          }
        } catch (final OverlappingFileLockException e) {
          // Held by another class of this JVM: wait like for another process.
        }
        if (System.nanoTime() > deadline) {
          throw new IOException(
              "Timed out waiting for the shared conformance server lock: " + file);
        }
        LockSupport.parkNanos(RETRY_PARK_NANOS);
      }
    } catch (final IOException | RuntimeException e) {
      channel.close();
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      lock.release();
    } finally {
      channel.close();
    }
  }
}
