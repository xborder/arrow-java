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

import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.replaceSemiColons;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.client.utils.FlightClientCache;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.DriverVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Connection to the Arrow Flight server. */
public final class ArrowFlightConnection extends AvaticaConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFlightConnection.class);

  private final BufferAllocator allocator;
  private final ArrowFlightSqlClientHandler clientHandler;
  private final ArrowFlightConnectionConfigImpl config;
  private ExecutorService executorService;
  private ThreadPoolExecutor pollerExecutor;
  private int metadataResultSetCount;
  // Track metadata result sets so connection close can release them.
  private Map<Integer, ArrowFlightJdbcFlightStreamResultSet> metadataResultSetMap = new HashMap<>();
  // Track original statement owners when execution replaces their statement-map entries.
  private final Set<AvaticaStatement> statementOwners =
      Collections.newSetFromMap(new IdentityHashMap<>());

  /**
   * Creates a new {@link ArrowFlightConnection}.
   *
   * @param driver the {@link ArrowFlightJdbcDriver} to use.
   * @param factory the {@link AvaticaFactory} to use.
   * @param url the URL to use.
   * @param properties the {@link Properties} to use.
   * @param config the {@link ArrowFlightConnectionConfigImpl} to use.
   * @param allocator the {@link BufferAllocator} to use.
   * @param clientHandler the {@link ArrowFlightSqlClientHandler} to use.
   */
  private ArrowFlightConnection(
      final ArrowFlightJdbcDriver driver,
      final AvaticaFactory factory,
      final String url,
      final Properties properties,
      final ArrowFlightConnectionConfigImpl config,
      final BufferAllocator allocator,
      final ArrowFlightSqlClientHandler clientHandler) {
    super(driver, factory, url, properties);
    this.config = Preconditions.checkNotNull(config, "Config cannot be null.");
    this.allocator = Preconditions.checkNotNull(allocator, "Allocator cannot be null.");
    this.clientHandler = Preconditions.checkNotNull(clientHandler, "Handler cannot be null.");
    this.metadataResultSetCount = 0;
  }

  /**
   * Creates a new {@link ArrowFlightConnection} to a {@link FlightClient}.
   *
   * @param driver the {@link ArrowFlightJdbcDriver} to use.
   * @param factory the {@link AvaticaFactory} to use.
   * @param url the URL to establish the connection to.
   * @param properties the {@link Properties} to use for this session.
   * @param allocator the {@link BufferAllocator} to use.
   * @return a new {@link ArrowFlightConnection}.
   * @throws SQLException on error.
   */
  static ArrowFlightConnection createNewConnection(
      final ArrowFlightJdbcDriver driver,
      final AvaticaFactory factory,
      String url,
      final Properties properties,
      final BufferAllocator allocator)
      throws SQLException {
    ArrowFlightSqlClientHandler clientHandler = null;
    try {
      url = replaceSemiColons(url);
      final ArrowFlightConnectionConfigImpl config =
          new ArrowFlightConnectionConfigImpl(properties);
      config.validatePollInfoPrefetch();
      clientHandler = createNewClientHandler(config, allocator, driver.getDriverVersion());
      return new ArrowFlightConnection(
          driver, factory, url, properties, config, allocator, clientHandler);
    } catch (SQLException | RuntimeException | Error e) {
      AutoCloseables.close(e, clientHandler, allocator);
      throw e;
    }
  }

  private static ArrowFlightSqlClientHandler createNewClientHandler(
      final ArrowFlightConnectionConfigImpl config,
      final BufferAllocator allocator,
      final DriverVersion driverVersion)
      throws SQLException {
    return new ArrowFlightSqlClientHandler.Builder()
        .withHost(config.getHost())
        .withPort(config.getPort())
        .withUsername(config.getUser())
        .withPassword(config.getPassword())
        .withTrustStorePath(config.getTrustStorePath())
        .withTrustStorePassword(config.getTrustStorePassword())
        .withSystemTrustStore(config.useSystemTrustStore())
        .withTlsRootCertificates(config.getTlsRootCertificatesPath())
        .withClientCertificate(config.getClientCertificatePath())
        .withClientKey(config.getClientKeyPath())
        .withBufferAllocator(allocator)
        .withEncryption(config.useEncryption())
        .withDisableCertificateVerification(config.getDisableCertificateVerification())
        .withHostnameOverride(config.getHostnameOverride())
        .withToken(config.getToken())
        .withCallOptions(config.toCallOption())
        .withRetainCookies(config.retainCookies())
        .withRetainAuth(config.retainAuth())
        .withCatalog(config.getCatalog())
        .withClientCache(config.useClientCache() ? new FlightClientCache() : null)
        .withConnectTimeout(config.getConnectTimeout())
        .withPollInfo(config.usePollInfo())
        .withDriverVersion(driverVersion)
        .withOAuthConfiguration(config.getOauthConfiguration())
        .build();
  }

  void reset() throws SQLException {
    // Clean up any open Statements
    try {
      AutoCloseables.close(getStatementsToClose());
    } catch (final Exception e) {
      throw AvaticaConnection.HELPER.createException(e.getMessage(), e);
    }

    statementMap.clear();

    // Reset Holdability
    this.setHoldability(this.metaData.getResultSetHoldability());

    // Reset Meta
    ((ArrowFlightMetaImpl) this.meta).setDefaultConnectionProperties();
  }

  /**
   * Gets the client {@link #clientHandler} backing this connection.
   *
   * @return the handler.
   */
  ArrowFlightSqlClientHandler getClientHandler() {
    return clientHandler;
  }

  /**
   * Gets the {@link ExecutorService} of this connection.
   *
   * @return the {@link #executorService}.
   */
  synchronized ExecutorService getExecutorService() {
    return executorService =
        executorService == null
            ? Executors.newFixedThreadPool(
                config.threadPoolSize(), new DefaultThreadFactory(getClass().getSimpleName()))
            : executorService;
  }

  /**
   * Gets the pool that runs background PollFlightInfo pollers, creating it on first use. The pool
   * is bounded and has no queue: when every thread is busy, submitting a task is rejected and the
   * poller is run by the result set's reader inside {@code next()} instead.
   */
  synchronized ThreadPoolExecutor getPollerExecutor() {
    if (pollerExecutor == null) {
      final int threads = config.pollInfoPollerThreads();
      pollerExecutor =
          new ThreadPoolExecutor(
              threads,
              threads,
              0L,
              TimeUnit.MILLISECONDS,
              new SynchronousQueue<>(),
              new DefaultThreadFactory(getClass().getSimpleName() + "-poller", true),
              new ThreadPoolExecutor.AbortPolicy());
    }
    return pollerExecutor;
  }

  /** The effective most queued, unread endpoints of a progressive result; at least 1. */
  int getPollInfoPrefetch() {
    return config.pollInfoPrefetch();
  }

  /** The longest wait between PollFlightInfo answers that add no endpoints. */
  long getPollInfoMaxBackoffMs() {
    return config.pollInfoMaxBackoffMs();
  }

  private void shutdownPollerExecutor() throws InterruptedException {
    final ThreadPoolExecutor executor;
    synchronized (this) {
      executor = pollerExecutor;
    }
    if (executor != null) {
      // The statements were closed, so every poller is ending. One that failed the result on a
      // deadline is still sending its CancelFlightInfo (bounded to 1 s): interrupting it now would
      // drop the cancel, so give the pollers a moment before interrupting them.
      executor.shutdown();
      if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
        executor.shutdownNow();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          LOGGER.warn("PollFlightInfo poller threads did not terminate within 5 seconds");
        }
      }
    }
  }

  /**
   * Registers a new metadata ResultSet and assigns it a unique ID. Metadata ResultSets are those
   * created without an associated Statement.
   *
   * @param resultSet the ResultSet to register
   * @return the assigned ID
   */
  int getNewMetadataResultSetId(ArrowFlightJdbcFlightStreamResultSet resultSet) {
    metadataResultSetMap.put(metadataResultSetCount, resultSet);
    return metadataResultSetCount++;
  }

  /**
   * Unregisters a metadata ResultSet when it is closed. This method is called by metadata
   * ResultSets during their close operation to remove themselves from the tracking map.
   *
   * @param id the ID of the ResultSet to unregister, or null if not a metadata ResultSet
   */
  void onResultSetClose(Integer id) {
    if (id == null) {
      return;
    }
    metadataResultSetMap.remove(id);
  }

  synchronized void registerStatementOwner(final AvaticaStatement statement) {
    statementOwners.add(statement);
  }

  synchronized void unregisterStatementOwner(final AvaticaStatement statement) {
    statementOwners.remove(statement);
  }

  @VisibleForTesting
  synchronized boolean isStatementOwner(final Statement statement) {
    return statementOwners.contains(statement);
  }

  private synchronized ArrayList<AutoCloseable> getStatementsToClose() {
    final ArrayList<AutoCloseable> statements = new ArrayList<>(statementOwners);
    final Set<Integer> ownedHandles = new HashSet<>();
    for (AvaticaStatement statement : statementOwners) {
      ownedHandles.add(statement.handle.id);
    }
    for (Map.Entry<Integer, AvaticaStatement> entry : statementMap.entrySet()) {
      if (!ownedHandles.contains(entry.getKey())) {
        statements.add(entry.getValue());
      }
    }
    return statements;
  }

  @Override
  public Properties getClientInfo() {
    final Properties copy = new Properties();
    copy.putAll(info);
    return copy;
  }

  @Override
  public void close() throws SQLException {
    Exception topLevelException = null;
    try {
      if (executorService != null) {
        executorService.shutdown();
      }
    } catch (final Exception e) {
      topLevelException = e;
    }
    // copies of the collections are used to avoid concurrent modification problems
    ArrayList<AutoCloseable> closeables = getStatementsToClose();
    closeables.addAll(new ArrayList<>(metadataResultSetMap.values()));
    // Statements stop their pollers first; then no poller can outlive the client or allocator.
    closeables.add(this::shutdownPollerExecutor);
    closeables.add(clientHandler);
    closeables.addAll(allocator.getChildAllocators());
    closeables.add(allocator);
    try {
      AutoCloseables.close(closeables);
    } catch (final Exception e) {
      if (topLevelException == null) {
        topLevelException = e;
      } else {
        topLevelException.addSuppressed(e);
      }
    }
    try {
      super.close();
    } catch (final Exception e) {
      if (topLevelException == null) {
        topLevelException = e;
      } else {
        topLevelException.addSuppressed(e);
      }
    }
    if (topLevelException != null) {
      throw AvaticaConnection.HELPER.createException(
          topLevelException.getMessage(), topLevelException);
    }
  }

  BufferAllocator getBufferAllocator() {
    return allocator;
  }

  public ArrowFlightMetaImpl getMeta() {
    return (ArrowFlightMetaImpl) this.meta;
  }

  @Override
  public PreparedStatement prepareStatement(final String sql) throws SQLException {
    return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public PreparedStatement prepareStatement(
      final String sql, final int resultSetType, final int resultSetConcurrency)
      throws SQLException {
    return prepareStatement(sql, resultSetType, resultSetConcurrency, getHoldability());
  }

  @Override
  public PreparedStatement prepareStatement(
      final String sql,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    checkOpen();
    try {
      return ArrowFlightPreparedStatement.builder(this)
          .withQuery(sql)
          .withGeneratedHandle()
          .withResultSetType(resultSetType)
          .withResultSetConcurrency(resultSetConcurrency)
          .withResultSetHoldability(resultSetHoldability)
          .build();
    } catch (final RuntimeException e) {
      throw HELPER.createException("while preparing SQL:" + sql, e);
    }
  }
}
