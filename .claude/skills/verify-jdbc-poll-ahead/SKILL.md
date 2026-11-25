---
name: verify-jdbc-poll-ahead
description: Model-check the Flight SQL JDBC progressive PollInfo result path (BDX-645) and its proposed bounded poll-ahead with TLA+. Use before merging any change to ArrowFlightJdbcFlightStreamResultSet, FlightEndpointDataQueue, PollInfoOperation, or the PollingFlightSqlClient poll loop, and when implementing or changing the poll-ahead poller.
---

# Verify the JDBC poll-ahead design

The JDBC driver reads a progressive query in two concurrent roles. The **reader** serves `ResultSet.next()` and opens `DoGet` streams. The **poller** sends `PollFlightInfo` continuations. A TLA+ model, `formal/pollinfo-poll-ahead/PollAhead.tla`, checks every interleaving of reader, poller and server, within small bounds. Use this skill to tell whether a change still keeps the promises below.

## What is verified

Each rule is an INVARIANT or PROPERTY in `PollAhead.tla`.

| Rule | Promise | Harm if broken |
| --- | --- | --- |
| `NoPrematureEnd` | `next()` returns false only after every published endpoint was read and the query finished. | The application silently gets a truncated result. |
| `EachEndpointOnce` | Each endpoint is queued for download once, even though every PollInfo repeats all earlier endpoints. | Duplicate rows. |
| `ContinuationsOnce` | Each continuation descriptor is sent at most once. | The server rejects the reused descriptor (`FAILED_PRECONDITION` in the conformance server), or the query re-runs. |
| `FailFast` | No row reaches the application after the driver saw the query fail or was cancelled. | The application processes data from a failed query. |
| `CancelStopsPolling` | No poll is sent after `Statement.cancel()`, the deadline, or `close()`. | Server work continues after the user cancelled. |
| `Backpressure` | The poller does not poll while unread endpoints are at `pollInfoPrefetch`. | The driver runs ahead of a slow reader without bound. |
| `OpenWithinLimit` | Open, unread `DoGet` streams never exceed the limit. | More open streams, each buffering up to 5 batches, than the setting promises. |
| `NoSpin` | No poll is sent straight after an answer that added nothing. | A busy loop against servers that answer at once. |
| `Terminates` | Every result eventually ends or throws, if nobody cancels. | `next()` hangs forever. |

## Scope, bounds, assumptions

- Bounds: up to `N` endpoints per query (default 3; also checked with `N=4`), `Limit=2`. One `ResultSet`. Two or more results sharing the poller pool are not modeled.
- Rows are abstracted: reading an endpoint is one step. PollInfo is modeled as a count of endpoints so far, so the append-only rule shows up as "never fewer than before".
- Time is abstracted: a backoff is one step. Its length (100 ms doubling up to `pollInfoMaxBackoff`) is not checked.
- The server may answer a poll at any moment, including at once with nothing new.
- The reader's end check ("queue empty and poller finished") is assumed to be one atomic read under the queue's lock. The poller's count check and its pause are also one step. The `pauseNotAtomic` variant shows what breaks when they are not.
- The model covers the result path only. It does not cover `executeQuery`'s first poll, the `UNIMPLEMENTED` fallback, metadata calls, or parameter upload.

## Coverage record

Sync commit: `b444a256006897ebf079dd47596ad978cb4701cc` (branch `bdx-645-pollinfo-poc`, rebased on apache/arrow-java `10c55b9c8`). At that commit the POC exists and the poll-ahead poller does not.

| Model action | Code at the sync commit (`flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/`) |
| --- | --- |
| `PollerSend`, `PollerTake` | `client/PollInfoOperation.java`: `poll`, `pollNextAvailable`. In the POC these run on the reader thread from `ArrowFlightJdbcFlightStreamResultSet.loadNextPublishedEndpoint`. |
| `PollerApply` | `client/PollInfoOperation.java`: `remember`, `validateAppendOnly`. `ArrowFlightJdbcFlightStreamResultSet.java`: `enqueueNewEndpoints`. |
| `AppRead` | `ArrowFlightJdbcFlightStreamResultSet.java`: `next`, `getNextEndpointStream`. `utils/FlightEndpointDataQueue.java`: `next`, `enqueue`. |
| `AppEnd` | `ArrowFlightJdbcFlightStreamResultSet.next` returning false. |
| `AppCancel` | `ArrowFlightStatement.cancel`, `ArrowFlightPreparedStatement.cancel`, `ArrowFlightJdbcFlightStreamResultSet.cancel` and `close`, `client/PollInfoOperation.cancel`. |
| `SrvAnswer` failure branch, `PollerTake` error branch | `ArrowFlightJdbcFlightStreamResultSet.loadNextPublishedEndpoint` catch block. |
| `PollerPause`, `BackoffElapses`, the wake in `AppRead`, the poller pool, deferred stream opening (`EagerOpen = FALSE`) | Proposed only. No code yet. |

## When to use it

Run it when a diff touches any covered file:

```sh
git diff --name-only b444a256006897ebf079dd47596ad978cb4701cc..HEAD -- \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightJdbcFlightStreamResultSet.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/utils/FlightEndpointDataQueue.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/client/PollInfoOperation.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/client/ArrowFlightSqlClientHandler.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightStatement.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightPreparedStatement.java \
  formal/pollinfo-poll-ahead/
```

Any output means the model may no longer describe the code. Also use this skill for any new poller class, for example `EndpointPoller`.

## How to check a change

1. **Mirror the change in the model.** The model is a hand-written design model, so TLC cannot read Java. Use the coverage record to find the action that corresponds to the code you changed, and edit that action so it does what the new code does. Keep each atomic step in the model atomic in the code: a step under one lock, or one RPC answer, is one action.
2. **Run every check** (about 15 seconds; needs Java 11+ and `tla2tools.jar` v1.7.4, found through `TLA2TOOLS` or the mise install path):

   ```sh
   formal/pollinfo-poll-ahead/run.sh
   N=4 formal/pollinfo-poll-ahead/run.sh
   ```

   The script exits 0 only when every line says `ok`. The design checks must pass. `finding-open-streams` must still fail on `OpenWithinLimit` until the finding below is resolved. Every broken variant must fail on its rule. A variant that suddenly passes means the model lost the ability to catch that bug: fix the model before you trust any pass.
3. **Read any failure with the pstack `/statechart` skill** (type `/pstack:statechart`). Give it a copy of `PollAhead.tla` and a config with the failing rule, in a scratch folder outside the repo. It prints the shortest path to the violation as numbered steps in plain words. When the model passes, it can draw how a result moves between states.
4. **Decide whether it is a bug or a modeling error.** Replay the path against the real code, one model action per step: as a unit test with a scripted Flight SQL producer (see `PollingMockFlightSqlProducer` in the JDBC tests), or against the Go conformance server (branch `bdx-645-pollinfo-conformance-server` of `dremio-employees/helder.gregorio`). If the code does the same thing, it is a real bug: report it with the path in the code's words, and do not weaken the rule. If the code cannot take that path, the model is wrong: fix the action, and say what you fixed.
5. **When a behavior change is intended**, change the action and, if a rule no longer applies, change that rule in a separate commit that explains why. Add a broken variant (a new `Mutation` value plus a `check` line in `run.sh`) for any new rule, and confirm it fails.
6. **Move the sync commit** in the coverage record only after you have confirmed that every action still matches the code at the new commit. Update the coverage table at the same time, for example when proposed actions get real code.

## Known finding

`OpenWithinLimit` fails for the proposal as written (`EagerOpen = TRUE`). The limit is checked before a poll, but one answer can publish several endpoints, and their streams all open at once. Shortest path, with a limit of 2:

1. The server publishes a second and a third endpoint while the application is still reading the first.
2. One stream is open, which is below the limit, so the poller sends the continuation.
3. The answer carries all three endpoints. The two new streams open at once, leaving three open streams against a limit of 2.

With deferred opening (`EagerOpen = FALSE`), streams open only while fewer than `Limit` are unread, and every rule holds. The implementation should keep queued endpoints unopened until the reader frees a slot, or the setting should be documented as "polls ahead while fewer than N are unread", not as a cap on open streams.

## Files

- `formal/pollinfo-poll-ahead/PollAhead.tla`: the model, with the code map in its header.
- `formal/pollinfo-poll-ahead/run.sh`: all design checks and broken variants, with their expected results.
