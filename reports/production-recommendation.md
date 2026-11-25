# BDX-645 JDBC: production go/no-go and follow-ups

Driver: Arrow Flight SQL JDBC (`apache/arrow-java`, modules `flight-sql` and `flight-sql-jdbc-core`).
POC branch: `bdx-645-pollinfo-poc` ([xborder/arrow-java#21](https://github.com/xborder/arrow-java/pull/21)).
Detailed evidence: [`jdbc-report.md`](jdbc-report.md), [`call-site-inventory.md`](call-site-inventory.md), [`evidence.jsonl`](evidence.jsonl).

## Recommendation: GO, as a production change built from this POC; not mergeable as-is

PollInfo-by-default works in JDBC without changing any `java.sql` signature or result type. Direct, prepared and metadata queries all went through `PollFlightInfo` against both in-process producers and the shared Go conformance server. The contract held: send the original descriptor once, bind once, fall back only on an initial `UNIMPLEMENTED`, cache per family, offer an opt-out, use one deadline, and let cancel work before the `ResultSet` exists. Progressive endpoint consumption at `ResultSet.next()` also works.

The branch has been rebased onto current upstream `main` (see "Rebase onto upstream main" below). It is still not ready to merge, for two reasons:

1. **The public API surface is accidental.** The POC adds a protected `FlightSqlClient.getInfo(FlightDescriptor, CallOption...)` in the published `flight-sql` artifact. That method can collide with subclasses that already exist downstream. It also adds a public `PollInfoOperation` and public handler and builder methods in `flight-sql-jdbc-core`. All of this needs an intentional design and an upstream Apache discussion before it lands.
2. **Contract item 12 was not implemented.** `PollInfoOperation.pollNextAvailable()` loops with no delay while the server keeps returning responses that add no endpoints. It relies entirely on the server to long-poll. A server that answers immediately makes the loop spin, and with no statement timeout set, nothing bounds it.

## Documented limitations (behavioral)

- **Late errors move to `ResultSet.next()`.** In progressive mode, `executeQuery` returns once the first endpoint is published. A query that fails afterwards raises `SQLException` from `next()`, not from `executeQuery`. JDBC allows this, but applications that treat a successful `executeQuery` as a successful query will see different behavior. Production needs an explicit decision here, because the ticket rules out an opt-in: either progressive by default, or final-only by default with progressive behind a property.
- **The deadline excludes schema preparation.** The single deadline and cancellable context cover the parameter `DoPut`, every poll and the first endpoint wait. They do not cover the earlier schema/prepare action.
- **CancelFlightInfo needs a payload.** Best-effort `CancelFlightInfo` is only possible after at least one `PollInfo` has returned a `FlightInfo`. Cancelling a blocked initial poll terminates the RPC but sends no server-side cancel.
- **Cleanup is synchronous.** The cancel cleanup call is bounded to one second, but it runs on the caller's thread. A slow server can delay the delivery of a timeout or cancel by up to that second.
- **The capability cache is scoped to the handler.** Unsupported families are cached per `ArrowFlightSqlClientHandler`, keyed by Flight SQL command `type_url`. Pooled or reused physical connections have not been tested.
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
3. Add a small bounded delay or backoff for consecutive successful responses that add no endpoints (contract item 12).

### JDBC (`flight-sql-jdbc-core`)
4. Replace the POC's `ArrowFlightStatement` → prepared-delegate lookup through `statementMap` (used for cancel, timeout and operation lookup on the `Statement.execute(String)` path) with an explicit owner reference.
5. Make `PollInfoOperation` and the handler and builder additions package-private. Keep only the documented connection property.
6. Decide the default behavior for late errors (progressive or final-only). Decide whether the operation deadline should start at `prepareAndExecute` entry so that it includes schema preparation.
7. Run best-effort `CancelFlightInfo` off the caller's thread, or with a tighter bound, so it can never delay a timeout or cancel.
8. Add tests for pooled connections and the family cache, and for races between cancel, close and an in-flight poll.
9. Add PollInfo black-box scenarios for at least one more metadata family (for example `getTables`) once the fixture supports it.
10. Keep the POC's regression tests: direct select and update, OAuth on `POLL_FLIGHT_INFO`, the exact timeout message, prepared single-bind, pre-`ResultSet` cancel, and close of an incomplete result.

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
