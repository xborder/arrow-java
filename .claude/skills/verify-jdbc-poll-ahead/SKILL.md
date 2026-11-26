---
name: verify-jdbc-poll-ahead
description: Model-check the Flight SQL JDBC progressive PollInfo result path (BDX-645) and its bounded poll-ahead with TLA+. Use before merging any change to ArrowFlightJdbcFlightStreamResultSet, FlightEndpointDataQueue, EndpointPoller, PollInfoOperation, or the PollingFlightSqlClient poll loop, and when changing the poll-ahead poller.
---

# Verify the JDBC poll-ahead design

The JDBC driver reads a progressive query in concurrent roles. The **reader** serves `ResultSet.next()` and opens `DoGet` streams. The **poller** (`EndpointPoller`) sends `PollFlightInfo` continuations from a pool thread. A TLA+ model, `formal/pollinfo-poll-ahead/PollAhead.tla`, checks every interleaving of reader, poller, server and a `cancel()` caller, within small bounds. Use this skill to tell whether a change still keeps the promises below.

## What is verified

Each rule is an INVARIANT or PROPERTY in `PollAhead.tla`.

| Rule | Promise | Harm if broken |
| --- | --- | --- |
| `NoPrematureEnd` | `next()` returns false only after every published endpoint was read and the query finished. | The application silently gets a truncated result. |
| `EachEndpointOnce` | Each endpoint is queued for download once, even though every PollInfo repeats all earlier endpoints. | Duplicate rows. |
| `ContinuationsOnce` | Each continuation descriptor is sent at most once. | The server rejects the reused descriptor (`FAILED_PRECONDITION` in the conformance server), or the query re-runs. |
| `FailFast` | No row reaches the application after the driver saw the query fail or was cancelled. | The application processes data from a failed query. |
| `CancelStopsPolling` | No poll is sent after `Statement.cancel()`, the deadline, or `close()` has finished stopping the poller. | Server work continues after the user cancelled. |
| `Backpressure` | The poller does not poll while unread endpoints are at `pollInfoPrefetch`. | The driver runs ahead of a slow reader without bound. |
| `OpenWithinLimit` | Open, opening and unread `DoGet` streams never exceed the limit. | More open streams, each buffering up to 5 batches, than the setting promises. |
| `NoSpin` | No poll is sent straight after an answer that added nothing. | A busy loop against servers that answer at once. |
| `Terminates` | Every result eventually ends or throws, if nobody cancels. | `next()` hangs forever. |
| `InlineMeansPoller` | Structural: when the reader runs the poller inline, the poller is not paused or orphaned. | Two threads drive one continuation chain. |

## Scope, bounds, assumptions

- Bounds: up to `N` endpoints per query (default 3; also checked with `N=4`), `Limit=2`. One `ResultSet`. Two or more results sharing the poller pool are not modeled, except that a rejected resume is a free choice (`ReaderResumeRejected`).
- Rows are abstracted: reading an endpoint is one step, and an endpoint is either empty (its future yields `null`) or not. PollInfo is modeled as a count of endpoints so far, so the append-only rule shows up as "never fewer than before".
- Time is abstracted: a backoff is one step. Its length (100 ms doubling up to `pollInfoMaxBackoff`) is not checked, and neither are the 10 x 100 us resubmit retries (only their outcome).
- The server may answer a poll at any moment, including at once with nothing new.
- Atomic steps in the model, each one a single locked step or RPC answer in the code:
  - `Q.addEndpoints`: append, set `moreExpected`, reserve slots, signal (`PollerApply*`).
  - `Q.pauseIfFull`: count check, paused flag, and the poller's `owned = false` (`PollerPause`).
  - `Q.onStreamFinished`, the part under the lock: free the slot, reserve pending endpoints, clear the paused flag (`AppRead`, `AppReadEmpty`).
  - `Q.awaitNext`'s lock-protected check and the start of the wait (`AppEnd`, `AppIdle`, `ReaderWait`).
- Steps the model splits because the code does not do them atomically:
  - the resume after unlocking (`ReaderResume`, `ReaderResumeRejected`), against the poller's `owned` flag;
  - stream opening outside the lock into reserved slots (`OpenReserved`, `OpenFails`);
  - the orphaned poller run inline by the reader (`AppIdle`, `InlineExit`, `PollerApplyHandBack`);
  - `cancel()`: the `cancelled` flag, then stop, cancel the operation and close the queue (`AppCancel`, `CancelStop`);
  - the reader's idle hook and its wait (`idleTried`).
- Over-approximations (safe, they only add behaviors): any step may open a reserved stream (the code opens it on the thread that reserved it, before that thread goes on); a stream registered after a failure or cancel still counts as open (the code closes it at once).
- Not modeled: a row already buffered when cancel or a failure starts can still reach the application from a `next()` call that was already running (the next call throws).
- The model covers the poll-ahead path only. It does not cover `executeQuery`'s first poll, the `UNIMPLEMENTED` fallback, metadata calls, parameter upload, protocol violations in an answer (`validateAppendOnly`, schema change), the poller pool failing to start (`start()` rejected, then `disablePollAhead`), the timed first wait, or an `Error` (not `RuntimeException`) killing the poller thread.
- **The implementation corresponds to `EagerOpen = FALSE`** for continuations: a queue built with `maxOpen = pollInfoPrefetch`. **Complete answers** (no continuation) **and `pollInfoPrefetch = 0` use the unlimited eager queue** (`maxOpen = Integer.MAX_VALUE`, no poller, the reader polls in `next()`); the model does not cover that path. `EagerOpen = TRUE` is kept only to show the old finding.

## Coverage record

Sync commit: `d7c3ed4ab4d30601447bc0518ac05294462b37d3` (branch `bdx-645-pollinfo-poc`, rebased on apache/arrow-java `10c55b9c8`). At that commit the poll-ahead poller exists, and the model matches it, including the finding below.

Abbreviations, all under `flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/`: **P** = `EndpointPoller.java`, **Q** = `utils/FlightEndpointDataQueue.java`, **R** = `ArrowFlightJdbcFlightStreamResultSet.java`, **O** = `client/PollInfoOperation.java`.

| Model action | Code at the sync commit |
| --- | --- |
| `PollerSend`, `PollerTake` | P `drive` -> O `pollOnce` -> `poll`. The poll runs on a pool thread, or on the reader's thread when run inline. |
| `PollerApplyBackoff`, `PollerApplyGo` | O `remember` (inside `pollOnce`: `validateAppendOnly`, new continuation), then R `appendEndpoints` -> Q `addEndpoints`; then P `drive`: `operation.backoff` (backoff) or the next loop iteration (go). |
| `PollerApplyHandBack` | The same, run inline: P `drive(true)` after an answer that added endpoints calls `handBack` (`submit`, or orphaned again). |
| `PollerApplyFinish` | Q `addEndpoints(list, false)` and P `drive`: `FINISHED`, `onFinished`. |
| `PollerPause`, `PollerPauseEnd`, `PollerCheck` | Q `pauseIfFull` with P `drive`'s `onPaused` (paused flag and `owned.set(false)` under the lock). `PollerPauseEnd` and `PollerCheck` exist only for the broken variants. |
| `PollerSeesFail`, `PollerDrop` | P `drive`: the loop-top check and the check after `pollOnce`. |
| `BackoffElapses` | O `backoff` (cancellable wait), P `drive` `BACKOFF` state. |
| `OpenReserved`, `OpenFails` | Q `openReserved`, `register`, `releaseReservations`, `fail`. The `opening` count is `resv`. |
| `AppRead`, `AppReadEmpty` | R `next` and `loadNextPublishedEndpoint` -> Q `awaitNext` taking a ready future, or a `null` future -> `onStreamFinished` (slot freed, `reserveSlotsLocked`, paused flag cleared). |
| `ReaderResume`, `ReaderResumeRejected` | The resume action `onStreamFinished` runs after `lock.unlock()`: P `resume` (`owned` compare-and-set), `handBack` (`submit`, 10 attempts, then `orphaned.set(true)`, `wakeReader`). |
| `AppIdle`, `InlineExit` | Q `awaitNext`'s idle hook -> P `runInlineIfOrphaned` (`drive(true)`). `idleTried` is the local variable in `awaitNext`. |
| `ReaderWait` and the wakes (`WakeRd`) | Q `awaitNext` -> `awaitChange`; `signalAll` in `addEndpoints`, `register`, `releaseReservations`, `fail`, `close`, `wakeReader`. |
| `AppEnd` | Q `awaitNext` returning `null` (futures empty, pending empty, `opening == 0`, `!moreExpected`, under the lock), R `next` returning false. |
| `AppThrow` | Q `awaitNext` -> `checkOpen`, `throwIfFailed`; R `next` top-of-call checks of `cancelled` and `queue.failure()`. |
| `AppCancel`, `CancelStop` | R `cancel` and `close`: `cancelled = true`; then P `stop`, O `cancel`, Q `close`. Also `ArrowFlightStatement.cancel`, `ArrowFlightPreparedStatement.cancel`. |
| `SrvAnswer` failure branch, `PollerTake` error branch | P `onPollFailed` -> Q `fail` (R `mapContinuationFailure`). |

## When to use it

Run it when a diff touches any covered file:

```sh
git diff --name-only d7c3ed4ab4d30601447bc0518ac05294462b37d3..HEAD -- \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightJdbcFlightStreamResultSet.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/EndpointPoller.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightConnection.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/utils/FlightEndpointDataQueue.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/client/PollInfoOperation.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/client/PollBackoff.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/client/ArrowFlightSqlClientHandler.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightStatement.java \
  flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightPreparedStatement.java \
  formal/pollinfo-poll-ahead/
```

Any output means the model may no longer describe the code. Also use this skill for any new class that polls or opens streams on a background thread.

## How to check a change

1. **Mirror the change in the model.** The model is a hand-written design model, so TLC cannot read Java. Use the coverage record to find the action that corresponds to the code you changed, and edit that action so it does what the new code does. Keep each atomic step in the model atomic in the code: a step under one lock, or one RPC answer, is one action.
2. **Run every check** (about 15 seconds; needs Java 11+ and `tla2tools.jar` v1.7.4, found through `TLA2TOOLS` or the mise install path):

   ```sh
   formal/pollinfo-poll-ahead/run.sh
   N=4 formal/pollinfo-poll-ahead/run.sh
   ```

   The script exits 0 only when every line says `ok`. The design checks must pass. `finding-open-streams` fails on `OpenWithinLimit` by design: it is the old `EagerOpen = TRUE` proposal, which the implementation does not follow. `finding-stale-idle` must fail on `Terminates` until the finding below is fixed in the code and `IdleFix` becomes the only behavior (then delete the constant, the finding row and the `idleReset` wording, and run the liveness check on the main row). Every broken variant must fail on its rule; they run with `IdleFix = TRUE` so the open finding cannot hide a variant that no longer fails. `openNotReserved` only fails from `N=4` on. A variant that suddenly passes means the model lost the ability to catch that bug: fix the model before you trust any pass.
3. **Read any failure with the pstack `/statechart` skill** (type `/pstack:statechart`). Give it a copy of `PollAhead.tla` and a config with the failing rule, in a scratch folder outside the repo. It prints the shortest path to the violation as numbered steps in plain words. When the model passes, it can draw how a result moves between states.
4. **Decide whether it is a bug or a modeling error.** Replay the path against the real code, one model action per step: as a unit test with a scripted Flight SQL producer (see `PollingMockFlightSqlProducer` in the JDBC tests), or against the Go conformance server (branch `bdx-645-pollinfo-conformance-server` of `dremio-employees/helder.gregorio`). If the code does the same thing, it is a real bug: report it with the path in the code's words, and do not weaken the rule. If the code cannot take that path, the model is wrong: fix the action, and say what you fixed.
5. **When a behavior change is intended**, change the action and, if a rule no longer applies, change that rule in a separate commit that explains why. Add a broken variant (a new `Mutation` value plus a `check` line in `run.sh`) for any new rule, and confirm it fails.
6. **Move the sync commit** in the coverage record only after you have confirmed that every action still matches the code at the new commit. Update the coverage table at the same time. Check the non-atomic steps in particular: the resume after unlocking against `owned`, opening outside the lock through reserved slots, the orphaned inline poller and the `idleTried` flag, and the two steps of `cancel()`.

## Known findings

### 1. `finding-stale-idle`: `next()` can hang after a rejected resume (open in the code)

`Terminates` fails on the model of the code at `d7c3ed4ab`. `FlightEndpointDataQueue.awaitNext` keeps a local `idleTried` flag (line 464). It sets it when the idle hook returns false (line 490) and clears it only after a wait (line 482). If an endpoint finishes between those two, the flag stays set. Shortest path, with a limit of 2 (`finding-stale-idle`, 16 steps):

1. The application has read the first endpoint. `awaitNext` finds nothing queued, `moreExpected` true. It calls the idle hook (`EndpointPoller.runInlineIfOrphaned`), which returns false because the poller is not orphaned, so `idleTried = true` (line 490).
2. Before the reader takes the lock again (line 468), the poller's open poll answers with two new endpoints. `addEndpoints` reserves both slots, `openReserved` registers the streams (futures not empty), and the poller pauses at the limit (`pauseIfFull`).
3. The reader takes the first new stream's future. It yields `null` (an endpoint with no rows). `onStreamFinished` frees a slot, clears the paused flag and runs the resume (`EndpointPoller.resume` -> `handBack`). The pool rejects the submit 10 times, so the poller is orphaned (`orphaned.set(true)`, `wakeReader()` on the reader's own thread, so nobody is waiting).
4. The second new stream also yields `null`. The loop reaches the empty-futures branch with nothing queued and `moreExpected` true. `!idleTried` is false (line 477), so the reader does not run the orphaned poller. It waits (line 481) for a signal nobody will send.

The code can take this path, but it is narrow: the poller must answer and register a stream inside the reader's few instructions between lines 490 and 468, the connection's poller pool must reject a resume (all `pollInfoPollerThreads` busy with other results, or the pool shut down; a fresh pool below its core size creates a thread instead), and the new endpoints must have no rows. When it happens, `next()` hangs until `cancel()` or `close()`. It was not replayed as a test.

Fix (checked as `IdleFix = TRUE`, `deferred-open-fixed`): clear `idleTried` whenever the loop consumes a future, for example right after `onStreamFinished(source)` (line 522). Another fix: test the `orphaned` flag inside the locked check at lines 472-483.

### 2. `finding-open-streams`: the first proposal (`EagerOpen = TRUE`, resolved)

`OpenWithinLimit` fails for the proposal as first written. The limit is checked before a poll, but one answer can publish several endpoints, and their streams all open at once. With a limit of 2: one stream is open, the poller sends the continuation, and an answer with three endpoints opens two more, leaving three open streams. The implementation does not do this: `reserveSlotsLocked` opens only while fewer than `maxOpen` streams are open or opening (`EagerOpen = FALSE`, check `deferred-open`).

## Files

- `formal/pollinfo-poll-ahead/PollAhead.tla`: the model, with the code map in its header.
- `formal/pollinfo-poll-ahead/run.sh`: all design checks and broken variants, with their expected results. Variants: `endWhenQueueEmpty`, `endIgnoresOpening`, `finishBeforeEnqueue`, `enqueueWholeList`, `reuseContinuation`, `readAfterFail`, `cancelIgnoresPoller`, `noLimit`, `openNotReserved`, `noBackoff`, `noWake`, `pauseNotAtomic`, `ownedClearedLate`, `noIdleHook`.
