# BDX-645 JDBC poll-ahead: implementation plan

Goal: during progressive `PollFlightInfo` execution, poll continuations in the background while the application reads, without changing the JDBC API. The design was agreed in review and model-checked in `formal/pollinfo-poll-ahead/` (skill: `.claude/skills/verify-jdbc-poll-ahead/SKILL.md`). The implementation must match the model's **deferred-open** configuration (`EagerOpen = FALSE`).

All paths below are relative to `flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/` unless shown in full.

## Behavior

1. `executeQuery` is unchanged. It polls until the first endpoint exists and returns the `ResultSet`.
2. If a continuation is pending, the prefetch limit is above 0, and the poller pool accepts the task, the `ResultSet` hands the continuation to an `EndpointPoller`. Otherwise it keeps today's behavior: the reader polls inside `next()` when its queue is empty (the POC path, plus backoff).
3. The poller polls the held continuation once, in the execution's `PollInfoOperation` context (one deadline, one cancel). It checks append-only, queues only the appended endpoints, and continues with the next continuation.
4. Backpressure:
   - The poller does not poll while queued, unread endpoints (opened-unread plus not-yet-opened) are at or above the limit. It pauses and holds no thread.
   - A queued endpoint opens its `DoGet` stream only while fewer than the limit are open and unread.
   - When the reader finishes an endpoint, under the same lock, it opens pending endpoints and resumes a paused poller.
5. Backoff: after an answer that adds no endpoints while the query is still running, wait 100 ms, doubling up to `pollInfoMaxBackoffMs` (default 5000). Reset after any answer that adds endpoints. Never wait past the operation deadline; cancel and close interrupt the wait. The wait may block the poller thread.
6. End of results: `next()` returns false only when the queue is empty, nothing is pending, and the poller has finished. The poller must mark itself finished in the same locked step that queues the final endpoints.
7. Fail fast: a failed continuation poll records the failure, closes every open stream, drops pending endpoints and buffered batches, and makes the next `next()` throw. No `CancelFlightInfo` is needed, because the server already ended the query. The exception keeps today's mapping: `TIMED_OUT` becomes `SQLTimeoutException("Query timed out after N SECONDS")`; anything else becomes `SQLException("Continuation PollFlightInfo failed", cause)`.
8. Cancel and close stop the poller. `Statement.cancel()`, the deadline and `ResultSet.close()` on an incomplete result cancel the operation context, end any open poll, send best-effort `CancelFlightInfo` once (existing `PollInfoOperation.cancel`), close the queue, and make the next `next()` throw "Statement canceled".
9. Unlimited eager queue: a first answer that is already complete (no continuation) and `pollInfoPrefetch=0` both use the queue without an open limit, so every endpoint opens at once, as it always did. The limit applies only while a poller is running.
10. Metadata calls, `executeQuery`'s first poll, the `UNIMPLEMENTED` fallback, and `usePollInfo=false` are unchanged.

## Configuration (connection properties)

| Property | Type | Default | Meaning |
| --- | --- | --- | --- |
| `pollInfoPrefetch` | number | `-1` = auto: `max(2, threadPoolSize)` | Most queued, unread endpoints. The poller polls only below it, and streams open only below it. `0` keeps the POC behavior (the reader polls when its queue is empty). |
| `pollInfoPollerThreads` | number | `4` | Size of the connection's poller pool. When all threads are busy, a new result uses the `0` behavior. |
| `pollInfoMaxBackoffMs` | number | `5000` | Longest wait between answers that add nothing. |

Deviation from the review decision: the review set the prefetch default to `threadPoolSize`, but `threadPoolSize` defaults to 1. A limit of 1 never polls while an endpoint is being read, so the default is `max(2, threadPoolSize)`. Follow the naming and type conventions of the existing properties in `utils/ArrowFlightConnectionConfigImpl.java`. If they use a different suffix for durations, use theirs and record the name here.

## Work units

### U1. Core implementation (one subagent; tightly coupled files)

- `utils/ArrowFlightConnectionConfigImpl.java`: the three properties and their accessors.
- `ArrowFlightConnection.java`: a lazily created poller executor.
  - Bounded `ThreadPoolExecutor` with core = max = `pollInfoPollerThreads`, a `SynchronousQueue`, daemon threads named after the connection, and an `AbortPolicy`, so a busy pool rejects and the result falls back.
  - Shut it down on `close()`, next to the stream executor. Expose the effective prefetch and backoff to result sets.
- `client/ArrowFlightSqlClientHandler.java`: split `getStreams(FlightInfo)` so a single endpoint can be opened (`getStream(FlightEndpoint)` or similar), reusing the existing location and TLS logic. Keep `getStreams` working for existing callers.
- `utils/FlightEndpointDataQueue.java`, extended without breaking existing callers (metadata result sets, existing tests):
  - A pending list of `FlightEndpoint`s not yet opened, an opener function, `maxOpen`, and an open-unread count.
  - Opened-unread goes down when a stream's future returns `null` (finished) or the stream is closed.
  - "More may arrive": while set, `next()` waits instead of returning `null` when nothing is queued. Honor the timeout variant and wake on new endpoints, finish, fail or close.
  - `fail(Throwable)`: closes all streams, drops pending, and makes `next()` rethrow.
  - An atomic `pauseIfFull()` for the poller and a resume callback fired when the unread count drops below the limit, all under one lock. Never hold that lock across a blocking `future.get()`, `take()`, or a network call.
- `client/PollInfoOperation.java`:
  - A single-poll method for the poller: poll the held continuation once, `remember` the answer, and report whether it added endpoints.
  - Keep `pollNextAvailable` for the fallback path, but add the same backoff between empty answers.
  - Keep `validateAppendOnly`.
- New `EndpointPoller` (package `org.apache.arrow.driver.jdbc`, package-private), started by the result set.
  - States: running, polling, paused, backoff, finished, failed, stopped.
  - Each step is guarded by the queue's lock where it touches queue state.
  - Stop on cancel or close. Do not leak tasks after the result closes.
- `ArrowFlightJdbcFlightStreamResultSet.java`:
  - Create the queue with the limit and opener, and queue the first answer's endpoints through it, so deferred opening applies to the first answer too.
  - Start the poller, or fall back.
  - In poller mode, `next()` relies on the queue: `null` means done. Map a failure to the exceptions above.
  - Keep the POC path for prefetch `0` or a rejected submit. Keep cancel and close semantics.
- Keep every existing test green. If an existing test asserts the POC request order (`Poll, DoGet, Poll, DoGet`), run it with `pollInfoPrefetch=0` rather than deleting the assertion.
- Do not change the TLA+ model.

### U2. Unit tests (after U1)

Add to `flight/flight-sql-jdbc-core/src/test/java/org/apache/arrow/driver/jdbc/` (extend `utils/PollingMockFlightSqlProducer.java` as needed: multi-endpoint answers, held `DoGet` streams, immediate empty answers, failing continuations, timestamps):

1. Poll-ahead: the poll for C1 arrives before E1's stream completes.
2. Deferred open: one answer adds 4 endpoints with prefetch 2, and at most 2 `DoGet` streams are ever open at once. All rows are delivered.
3. Backpressure: with held streams, polling stops at the limit and resumes after reads.
4. Backoff: gaps of at least 100 ms, then 200 ms, between consecutive empty answers (generous upper tolerance); the gap resets after an answer that adds endpoints; capped by `pollInfoMaxBackoffMs`.
5. Fail fast: a continuation error while E1 is held. `next()` throws, no rows from E2, and E2's stream is closed or never opened.
6. Cancel: `Statement.cancel()` with a poll open sends no further polls and exactly one `CancelFlightInfo`; `next()` throws.
7. Close of an unfinished result stops the poller and sends one `CancelFlightInfo`.
8. No premature end: the final endpoints arrive in the done answer, and all rows are delivered.
9. Continuations are sent once each and in order.
10. `pollInfoPrefetch=0` keeps the exact POC order.
11. Poller pool busy (`pollInfoPollerThreads=1`, two concurrent results): the second falls back, and both return correct rows.
12. `FlightEndpointDataQueue` unit tests for deferred open, wait-for-more, fail, and close.

### U3. End-to-end tests against the conformance server (after U2 and the server additions)

Extend `SharedServerPollInfoExecutionTest` (enabled by `-Dpollinfo.shared.enabled=true` with `-Dpollinfo.shared.flightPort` and `-Dpollinfo.shared.controlPort`). The scenarios, events and control endpoints come from the conformance-server session (statement texts `burst`, `trickle`, `empty-answers`, `late-error-buffered`, plus hold/release and `DoGet.end` / `do_get_max_concurrent` / `elapsed_ns`). Assert server-side observations: maximum concurrent streams, poll-ahead ordering relative to held streams, backoff gaps, fail-fast cancellation of buffered streams, and continuation order.

### U4. Model sync and verification (after U1; can run alongside U2)

- Update the coverage record in `.claude/skills/verify-jdbc-poll-ahead/SKILL.md` to map each model action to the new code: `EndpointPoller`, the queue methods, the result set.
- Check the implementation against `PollAhead.tla` (`EagerOpen = FALSE`). Mirror any difference into the model, or report it as a finding.
- Run `formal/pollinfo-poll-ahead/run.sh` and `N=4 formal/pollinfo-poll-ahead/run.sh`. Move the sync commit only after confirming that the model matches.

### U5. Review and regression (after U3)

Correctness and test review of the full diff. Fix confirmed issues. Then run:

- the full Flight SQL and JDBC core suites
- the shared-server suite against a fresh conformance server
- the verification skill

## Build and run conventions (all units)

- Worktree: `/Users/helder/repos/arrow-java-fork/.claude/worktrees/bdx-645-jdbc`, branch `bdx-645-pollinfo-poc`. Never push.
- Maven: `/Users/helder/.m2/wrapper/dists/apache-maven-3.9.10-bin/53h08a94dg6djh6umvruv7q564/apache-maven-3.9.10/bin/mvn`. Maven 3.10 fails on this repo. Always pass `-Dmaven.gitcommitid.skip=true`, and run from the repo root with `-pl flight/flight-sql,flight/flight-sql-jdbc-core -am`.
- Focused tests: add `-Dtest='<Classes>' -Dsurefire.failIfNoSpecifiedTests=false`. Full suites: `-Dtest='org/apache/arrow/flight/sql/**/*.java,org/apache/arrow/driver/jdbc/**/*.java'`.
- Format with `spotless:apply` on both modules before committing. Spotless and Checkstyle must pass.
- One local commit per unit, ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Only one Maven build may run in this worktree at a time.
