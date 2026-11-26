# BDX-645 JDBC: production go/no-go and follow-ups

Driver: Arrow Flight SQL JDBC (`apache/arrow-java`, modules `flight-sql` and `flight-sql-jdbc-core`).
POC branch: `bdx-645-pollinfo-poc` ([xborder/arrow-java#21](https://github.com/xborder/arrow-java/pull/21)).
Detailed evidence: [`jdbc-report.md`](jdbc-report.md), [`call-site-inventory.md`](call-site-inventory.md), [`evidence.jsonl`](evidence.jsonl).

## Recommendation: GO, as a production change built from this POC; not mergeable as-is

PollInfo-by-default works in JDBC without changing any `java.sql` signature or result type. Direct, prepared and metadata queries all went through `PollFlightInfo` against both in-process producers and the shared Go conformance server. The contract held: send the original descriptor once, bind once, fall back only on an initial `UNIMPLEMENTED`, cache per family, offer an opt-out, use one deadline, and let cancel work before the `ResultSet` exists. Progressive endpoint consumption works, and since 2026-11-26 a background poller fetches continuations while the application reads (see "Poll-ahead" below).

The branch has been rebased onto current upstream `main` (see "Rebase onto upstream main" below). It is still not ready to merge, because of the public API:

1. **The public API surface is accidental.** The POC adds a protected `FlightSqlClient.getInfo(FlightDescriptor, CallOption...)` in the published `flight-sql` artifact. That method can collide with subclasses that already exist downstream. It also adds a public `PollInfoOperation` and public handler and builder methods in `flight-sql-jdbc-core`. All of this needs an intentional design and an upstream Apache discussion before it lands.

Contract item 12 (a delay between answers that add nothing) was missing from the first POC. It is now implemented: both the background poller and the reader's own polling wait 100 ms after such an answer, doubling up to `pollInfoMaxBackoffMs` (default 5000), never past the deadline. Cancel and close end the wait.

## Documented limitations (behavioral)

- **Late errors move to `ResultSet.next()`.** In progressive mode, `executeQuery` returns once the first endpoint is published. A query that fails afterwards raises `SQLException` from `next()`, not from `executeQuery`. JDBC allows this, but applications that treat a successful `executeQuery` as a successful query will see different behavior. With poll-ahead the failure is found in the background, so it can also be thrown before the first row is read: buffered rows are dropped and the next `next()` throws. Production needs an explicit decision here, because the ticket rules out an opt-in: either progressive by default, or final-only by default with progressive behind a property.
- **The deadline excludes schema preparation.** The single deadline and cancellable context cover the parameter `DoPut`, every poll and the first endpoint wait. They do not cover the earlier schema/prepare action.
- **CancelFlightInfo needs a payload.** Best-effort `CancelFlightInfo` is only possible after at least one `PollInfo` has returned a `FlightInfo`. Cancelling a blocked initial poll terminates the RPC but sends no server-side cancel.
- **Cleanup is synchronous.** The cancel cleanup call is bounded to one second, but it runs on the caller's thread. A slow server can delay the delivery of a timeout or cancel by up to that second.
- **The capability cache is scoped to the handler.** Unsupported families are cached per `ArrowFlightSqlClientHandler`, keyed by Flight SQL command `type_url`. Pooled or reused physical connections have not been tested.
- **Poll-ahead uses threads per connection.** Each connection can start up to `pollInfoPollerThreads` (default 4) daemon poller threads. When they are all busy, a new result falls back to polling inside `next()`. Several results sharing one pool are covered by unit tests, not by the model.
- **The opt-out name differs from the ticket.** The ticket suggested `usePollFlightInfo=false`; the POC implements `usePollInfo=false`. Pick one name before release and document it.

## GetFlightInfo call-site coverage

All 13 Flight SQL command families (14 call shapes) go through the shared hook. Coverage differs by family:

| Coverage | Families |
| --- | --- |
| Black-box tested against the shared server | `CommandStatementQuery` (direct), `CommandPreparedStatementQuery` (prepared), `CommandGetCatalogs` |
| Exposed through JDBC; covered only by existing JDBC regressions, with no PollInfo-specific scenario | `CommandGetDbSchemas`, `CommandGetSqlInfo`, `CommandGetTables`, `CommandGetPrimaryKeys`, `CommandGetExportedKeys`, `CommandGetImportedKeys`, `CommandGetCrossReference`, `CommandGetTableTypes` |
| Routed through the hook but not reachable from JDBC (not exercised) | `CommandStatementSubstraitPlan`, `CommandGetXdbcTypeInfo` (both overloads) |
| Not applicable (no `FlightInfo`) | updates and ingest (`DoPut` / update actions), schema-only `GetSchema`, `DoGet` |

After the rebase, the JDBC entry points map to wire commands as follows:

| JDBC call | Wire command (upstream after GH-62) | Polling hook |
| --- | --- | --- |
| `Statement.executeQuery(String)` | `CommandStatementQuery`, with no prepare | `ArrowFlightSqlClientHandler.getInfo(String, PollInfoOperation)` (new overload of the upstream direct path) |
| `Statement.execute(String)` | `CommandPreparedStatementQuery` (GH-62 deliberately keeps this prepared) | `ArrowFlightPreparedStatement.executeFlightInfoQuery`, reached through the statement's prepared delegate |
| `PreparedStatement.executeQuery()` | `CommandPreparedStatementQuery` after a single parameter `DoPut` | `ArrowFlightPreparedStatement.executeFlightInfoQuery` |
| `DatabaseMetaData.*` | metadata commands | `PollingFlightSqlClient.getInfo` hook, with a per-call operation |

The POC's `prepareDirect` path was removed in the rebase, because GH-62 already made direct `executeQuery` send `CommandStatementQuery` without a prepare. One consequence: with `usePollInfo=false`, direct `executeQuery` now issues `GetFlightInfo` on `CommandStatementQuery` (upstream's new baseline), no longer on `CommandPreparedStatementQuery`. The two T6 assertions were updated to match.

## Minimal follow-up list

### Shared library (`flight-sql`), needs upstream design agreement
1. Replace the protected `getInfo` hook with an intentional descriptor-resolution abstraction, or with a package-private or `@InternalApi` seam, and run a compatibility review.
2. Move the poll loop into a reusable component: continuation handling, append-only endpoint validation, initial-`UNIMPLEMENTED` fallback and the family cache. Unit-test it against every command constructor and against the default producer (which returns an already-completed `PollInfo`).
3. ~~Add a small bounded delay or backoff for consecutive successful responses that add no endpoints (contract item 12).~~ Done with poll-ahead (`PollBackoff`).

### JDBC (`flight-sql-jdbc-core`)
4. Replace the POC's `ArrowFlightStatement` → prepared-delegate lookup through `statementMap` (used for cancel, timeout and operation lookup on the `Statement.execute(String)` path) with an explicit owner reference.
5. Make `PollInfoOperation` and the handler and builder additions package-private. Keep only the documented connection property.
6. Decide the default behavior for late errors (progressive or final-only). Decide whether the operation deadline should start at `prepareAndExecute` entry so that it includes schema preparation.
7. Run best-effort `CancelFlightInfo` off the caller's thread, or with a tighter bound, so it can never delay a timeout or cancel.
8. Add tests for pooled connections and the family cache. Races between cancel, close and an in-flight poll are now covered by the poll-ahead tests and the model.
9. Add PollInfo black-box scenarios for at least one more metadata family (for example `getTables`) once the fixture supports it.
10. Keep the POC's regression tests: direct select and update, OAuth on `POLL_FLIGHT_INFO`, the exact timeout message, prepared single-bind, pre-`ResultSet` cancel, and close of an incomplete result.

## Poll-ahead (2026-11-26)

The first POC polled the next continuation only after the application had read every published endpoint, so endpoints were found and downloaded one at a time. Poll-ahead removes that wait without changing the JDBC API. The design was agreed in review, model-checked before it was written, and implemented from [`poll-ahead-implementation-plan.md`](poll-ahead-implementation-plan.md).

- `executeQuery` is unchanged: it returns once the first endpoint exists.
- An `EndpointPoller` on the connection's poller pool then polls each continuation once, in the same operation (one deadline, one cancel), checks append-only, and queues only the new endpoints.
- Backpressure: the poller pauses while unread endpoints reach `pollInfoPrefetch`, and holds no thread while paused. Streams open only while fewer than the limit are open and unread. The reader frees a slot and resumes the poller when it finishes an endpoint.
- Backoff as described above.
- Fail fast: a failed continuation closes every stream, drops buffered batches, and makes the next `next()` throw, with the existing exception mapping.
- Cancel, the deadline, and closing an unfinished result stop the poller and send `CancelFlightInfo` once.
- `pollInfoPrefetch=0`, or a busy poller pool, keeps the first POC's behavior (with backoff). A first answer that is already complete opens every endpoint at once, as before.

| Property | Default | Meaning |
| --- | --- | --- |
| `pollInfoPrefetch` | `-1` = `max(2, threadPoolSize)` | Most queued, unread endpoints. `0` keeps the first POC's behavior. |
| `pollInfoPollerThreads` | `4` | Size of the connection's poller pool. |
| `pollInfoMaxBackoffMs` | `5000` | Longest wait between answers that add nothing. |

The review set the prefetch default to `threadPoolSize`, but `threadPoolSize` defaults to 1, and a limit of 1 never polls while an endpoint is being read.

Verification:

- **Model.** `formal/pollinfo-poll-ahead/PollAhead.tla`, checked with TLC 2.19 by `run.sh`: 22 checks match at 3 and 4 endpoints, including a broken variant for each rule. How to recheck after a change: `.claude/skills/verify-jdbc-poll-ahead/SKILL.md`.
- **Bugs found before release.** The model found a hang after a rejected poller resume (`dadd45d8a`). A code review found a late slot release that made poll-ahead fall back to the first POC's timing, plus error, fallback and cancel edge cases (`4b831553c`). The tests found a raw `CANCELLED` error leaking from `next()` (`dadd45d8a`) and a lost `CancelFlightInfo` when the connection closed right after a deadline failure (`436156f29`).
- **Tests.** `PollAheadTest` (30), `FlightEndpointDataQueueDeferredOpenTest` (24), `EndpointPollerTest` (1) and `PollBackoffTest` (6) against scripted mock servers, and `SharedServerPollAheadTest` (13) against the conformance server's poll-ahead scenarios (`burst`, `trickle`, `empty-answers`, `late-error-buffered`), which check the server's maximum concurrent streams, poll timing against held streams, backoff gaps and cancellation. Four existing tests that assert the first POC's exact request order now run with `pollInfoPrefetch=0`.

Not covered by the model: `executeQuery`'s first poll, the fallback path, and several results sharing one pool.

## Rebase onto upstream main (2026-11-25)

The branch was rebased from base `91b4a2ca4` onto `apache/arrow-java` `main` at `10c55b9c8`, which is 164 upstream commits. All five POC commits were kept. Six JDBC files conflicted, with 15 hunks in total, mostly against GH-62 (`054c7b8b7`). The rest were against GH-1268, GH-1499/GH-1500, GH-1232 and GH-1319/GH-1442. Resolutions:

- `ArrowFlightMetaImpl`: took upstream's version. The POC's `prepareForHandle(..., directExecution)` and `prepareDirect` usage are gone.
- `ArrowFlightConnection` and the handler imports: took upstream and re-added `.withPollInfo(config.usePollInfo())`.
- `ArrowFlightSqlClientHandler`: removed `prepareDirect` and the `directExecution` branching. Added `getInfo(String, PollInfoOperation)` for upstream's direct path. Made `PreparedStatement.executeQuery(PollInfoOperation)` a `default` method, because a test stub added upstream implements the interface.
- `ArrowFlightStatement`: the direct `executeFlightInfoQuery` branch now runs inside a progressive `PollInfoOperation`. `cancel()`, `remainingQueryTimeoutNanos()`, `activePollInfoOperation()` and `finishPollInfoOperation()` delegate to the prepared statement that shares the handle when `Statement.execute(String)` used the prepared path.
- `ArrowFlightPreparedStatement`: kept upstream's `ensurePrepared()`, closable resources and builder, and added the POC's operation lifecycle.
- `ArrowFlightJdbcFlightStreamResultSet`: merged upstream's cancel normalization (`cancelled` flag, the closed-queue exception becomes "Statement canceled", `enqueueEndpointData`) with the POC's progressive append-only enqueue and remaining-deadline handling. The cancelled/closed-queue check runs before a continuation poll. A `CANCELLED` continuation after cancel now also surfaces as "Statement canceled".
- Tests: the two T6 opt-out assertions now expect `CommandStatementQuery`, which is upstream's new legacy path.

Only the final commit was compiled. The intermediate rebased commits were not built individually.

## Re-validation (2026-11-25)

Environment: macOS arm64, Java 21.0.10, Maven 3.9.10. Maven 3.10.0 fails on the repo's Develocity extension, and the old `git-commit-id-plugin` fails inside a git worktree, so `-Dmaven.gitcommitid.skip=true` is needed there. Conformance server: branch `bdx-645-pollinfo-conformance-server` (unchanged server code), private instance on Flight `127.0.0.1:32447` and control `127.0.0.1:32448`.

```sh
# conformance server (Go 1.25.5)
make build && make test
./bin/conformance-server -flight-addr 127.0.0.1:32447 -control-addr 127.0.0.1:32448

# focused + shared T1-T9 (run from the repo root)
mvn -pl flight/flight-sql,flight/flight-sql-jdbc-core -am test \
  -Dtest='PollInfoExecutionTest,SharedServerPollInfoExecutionTest' -Dsurefire.failIfNoSpecifiedTests=false \
  -Dpollinfo.shared.enabled=true -Dpollinfo.shared.flightPort=32447 -Dpollinfo.shared.controlPort=32448 \
  -Dmaven.gitcommitid.skip=true

# T10 regression
mvn -pl flight/flight-sql,flight/flight-sql-jdbc-core -am test \
  -Dtest='org.apache.arrow.flight.sql.**.*,org.apache.arrow.driver.jdbc.**.*' -Dsurefire.failIfNoSpecifiedTests=false \
  -Dpollinfo.shared.enabled=true -Dpollinfo.shared.flightPort=32447 -Dpollinfo.shared.controlPort=32448 \
  -Dmaven.gitcommitid.skip=true
```

| Run | Before rebase (base `91b4a2ca4`) | After rebase (`10c55b9c8`) |
| --- | --- | --- |
| Conformance server `go test ./...` | 13/13 pass | n/a (server unchanged) |
| `PollInfoExecutionTest` (focused T1–T9) | 12/12 pass | 12/12 pass |
| `SharedServerPollInfoExecutionTest` (shared T1–T9) | 11/11 pass | 11/11 pass |
| T10: Flight SQL + JDBC core full suites | not rerun (Sept: 97 + 1,269, 56 skipped) | Flight SQL 97/97; JDBC core 1,365 run, 0 failures/errors, 46 skipped (includes the PollInfo tests) |

The post-rebase runs had Spotless and Checkstyle enabled. The pre-rebase runs skipped them.

## Re-validation with poll-ahead (2026-11-26)

Same environment. Conformance server at commit `5d0d11c` (adds the poll-ahead scenarios, holds and counters) on Flight `127.0.0.1:31337` and control `127.0.0.1:31338`. Use path-style `-Dtest` patterns so top-level test classes are included:

```sh
mvn -pl flight/flight-sql,flight/flight-sql-jdbc-core -am install \
  -Dtest='org/apache/arrow/flight/sql/**/*.java,org/apache/arrow/driver/jdbc/**/*.java' -Dsurefire.failIfNoSpecifiedTests=false \
  -Dpollinfo.shared.enabled=true -Dpollinfo.shared.flightPort=31337 -Dpollinfo.shared.controlPort=31338 \
  -Dmaven.gitcommitid.skip=true
formal/pollinfo-poll-ahead/run.sh && N=4 formal/pollinfo-poll-ahead/run.sh
```

| Run | Result |
| --- | --- |
| Flight SQL suite | 97/97 |
| JDBC core suite, shared-server tests enabled | 1,458 run, 0 failures, 49 skipped |
| `SharedServerPollAheadTest` / `SharedServerPollInfoExecutionTest` | 13/13 and 11/11 |
| `install` (Spotless, Checkstyle, `dependency:analyze`) | passes |
| Model checks | all 22 match at N=3 and N=4 |
