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
| `FailedQueueInert` | After the queue recorded a failure, `addEndpoints` queues, reserves and opens nothing. | Endpoints and reservations pile up on a dead queue, and `moreExpected` could be set again after `fail()` cleared it. |
| `CancelStopsPolling` | No poll is sent after `Statement.cancel()`, the deadline, or `close()` has finished stopping the poller. | Server work continues after the user cancelled. |
| `Backpressure` | The poller does not poll while unread endpoints are at `pollInfoPrefetch`. | The driver runs ahead of a slow reader without bound. |
| `OpenWithinLimit` | Open, opening and unread `DoGet` streams never exceed the limit. The stream the application is reading counts until it is read to the end. | More open streams, each buffering up to 5 batches, than the setting promises. |
| `SlotFreedOnExhaust` | A stream's slot is freed in the step where the application's own `stream.next()` returns false. No finished stream still holds a slot while the application reads another one. | The poller stays paused, and pending endpoints stay unopened, behind a stream nobody reads. This is a progress rule, not a safety rule: see finding 3. |
| `NoSpin` | No poll is sent straight after an answer that added nothing. | A busy loop against servers that answer at once. |
| `Terminates` | Every result eventually ends or throws, if nobody cancels. | `next()` hangs forever. |
| `InlineMeansPoller` | Structural: when the reader runs the poller inline, the poller is not paused or orphaned. | Two threads drive one continuation chain. |

## Scope, bounds, assumptions

- Bounds: up to `N` endpoints per query (default 3; also checked with `N=4`), `Limit=2`. One `ResultSet`. Two or more results sharing the poller pool are not modeled, except that a rejected resume is a free choice (`ReaderResumeRejected`).
- Rows are abstracted: an endpoint is taken (`AppTake`) and later read to the end (`AppExhaust`), and nothing between is tracked. An endpoint is either empty (its future yields `null`, `AppReadEmpty`) or not. PollInfo is modeled as a count of endpoints so far, so the append-only rule shows up as "never fewer than before".
- Time is abstracted: a backoff is one step. Its length (100 ms doubling up to `pollInfoMaxBackoff`) is not checked, and neither are the 10 x 100 us resubmit retries (only their outcome).
- The server may answer a poll at any moment, including at once with nothing new.
- Atomic steps in the model, each one a single locked step or RPC answer in the code:
  - `Q.addEndpoints`: append, set `moreExpected`, reserve slots, signal (`PollerApply*`).
  - `Q.pauseIfFull`: count check, paused flag, and the poller's `owned = false` (`PollerPause`).
  - `Q.onStreamFinished`, the part under the lock: free the slot, reserve pending endpoints, clear the paused flag. The application thread runs it in two places: `Q.endpointExhausted` when its own `stream.next()` returned false (`AppExhaust`, read to the end and free in one step), and `awaitNext` consuming a null future (`AppReadEmpty`). Closing the finished stream and opening the reserved endpoints happen after the unlock and are not part of the step.
  - `Q.awaitNext`'s lock-protected check and the start of the wait (`AppEnd`, `AppIdle`, `ReaderWait`).
- Steps the model splits because the code does not do them atomically:
  - taking a ready stream (`AppTake`, no slot change) and reading it to the end (`AppExhaust`, slot freed): the stream keeps its slot in between, as `openUnread` does;
  - the resume after unlocking (`ReaderResume`, `ReaderResumeRejected`), against the poller's `owned` flag;
  - stream opening outside the lock into reserved slots (`OpenReserved`, `OpenFails`);
  - the orphaned poller run inline by the reader (`AppIdle`, `InlineExit`, `PollerApplyHandBack`);
  - `Statement.cancel()`: `PollInfoOperation.cancel()` first (`CancelOp`: an open poll ends `CANCELLED`, so the poller can record "Statement canceled" as the queue failure before `cancelled` is set), then `ResultSet.cancel()`: the `cancelled` flag, then stop, cancel the operation and close the queue (`AppCancel`, `CancelStop`). `CancelOp` is optional: `close()` and a direct `ResultSet.cancel()` start at `AppCancel`;
  - the reader's idle hook and its wait (`idleTried`).
- Over-approximations (safe, they only add behaviors): any step may open a reserved stream (the code opens it on the thread that reserved it, before that thread goes on); `AppExhaust` is allowed after a failure or cancel (the stream's `next()` would usually throw `CANCELLED` first); `PollerCrash` may hit the poller in any of `running`, `polling`, `received`, `backoff`; a started `Statement.cancel()` always reaches `ResultSet.cancel()` (fairness on `AppCancel`).
- Not tracked: the rows between `AppTake` and `AppExhaust`. Each `next()` call starts with the `cancelled` and `queue.failure()` checks, which the model treats as `AppThrow`, so `FailFast` only watches the hand-over of a stream. A row already buffered when cancel or a failure starts can still reach the application from a `next()` call that was already running (the next call throws).
- Not modeled, by design: the cleanup RPC that follows a failed poll (`afterFailure`: `cancel()` or `terminate()`, then `finishPollInfoOperation`; `CancelFlightInfo` has a 1 s timeout). The model has no time, so it cannot say how long a failure takes to reach the reader. It records the failure in the step that takes the answer, which is what `onPollFailed` now does.
- The model covers the poll-ahead path only. It does not cover `executeQuery`'s first poll, the `UNIMPLEMENTED` fallback, metadata calls, parameter upload, the poller pool failing to start (`start()` rejected, then `disablePollAhead`), the timed first wait (`SQLTimeoutException`), the reader pool's size and the other results that share it, `endpointExhausted` on a queue without an opener (it re-enqueues), a cancel during `executeQuery` before the `ResultSet` exists, or **the fallback path** (`pollInfoPrefetch = 0` or a busy poller pool): `loadNextPublishedEndpoint` polls inside `next()` on the unlimited eager queue, and since `4b831553c` it loops while a continuation remains. Before that it returned false when the new endpoints had no rows, which truncates the result (the class of `NoPrematureEnd`, on a path the model does not describe; found by review, covered only by Java tests).
- Answer validation (`validateAppendOnly`, schema change, no continuation to poll) and an interrupted backoff are not modeled as such. They all end the poller with a queue failure, which is `PollerCrash`.
- **The implementation corresponds to `EagerOpen = FALSE`** for continuations: a queue built with `maxOpen = pollInfoPrefetch`. **Complete answers** (no continuation) **and `pollInfoPrefetch = 0` use the unlimited eager queue** (`maxOpen = Integer.MAX_VALUE`, no poller, the reader polls in `next()`); the model does not cover that path. `EagerOpen = TRUE` is kept only to show the old finding.
- The first load (`populateData` -> `getNextEndpointStream(true, false)` -> `Q.nextAvailable`) is not a separate action. It runs the same `awaitNext` loop and only differs by returning null where `next()` would idle, wait or end. The model starts when the poller has started, with the reader at the top of that loop and `idleTried` false, which is also the state of a fresh `next()` call; every state after the first load is one the model reaches from there.

## Coverage record

Abbreviations, all under `flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/`: **P** = `EndpointPoller.java`, **Q** = `utils/FlightEndpointDataQueue.java`, **R** = `ArrowFlightJdbcFlightStreamResultSet.java`, **O** = `client/PollInfoOperation.java`.

Sync commit: `4b831553c` (branch `bdx-645-pollinfo-poc`, rebased on apache/arrow-java `10c55b9c8`). The model matches the main code there, including the idle-flag fix (`IdleFix = TRUE`) and the slot release on exhaustion.

Earlier syncs: the diff `d7c3ed4ab..dadd45d8a` of the main code has two changes: the `idleTried = false` line in `awaitNext` (mirrored, see finding 1) and a rethrow of `queue.failure()` in `ResultSet.next()`'s `FlightRuntimeException` catch (not a model action: it only picks the exception thrown while the reader is blocked in a stream).

The diff `dadd45d8a..4b831553c` of the main code (four files: **R**, **P**, **O**, **Q**) changed these behaviors. Findings 3 and 4 say what each one did to the model:

1. A finished endpoint's slot is freed by `Q.endpointExhausted` when the application's own `stream.next()` returns false (model: `AppRead` split into `AppTake` and `AppExhaust`).
2. The first load uses `Q.nextAvailable` (waitForMore = false), so `executeQuery` does not wait for unpublished endpoints (no model change).
3. The queue is failed before the operation's blocking `cancel()` / `terminate()` (no model change).
4. A cancel that races a poll is recorded as the queue failure "Statement canceled" (model: `CancelOp`).
5. A failed queue is inert: `addEndpoints` is a no-op and `openReserved` stops opening (model: `ApplyQ`, `OpenReserved`, `ReadFree`).
6. Any `Throwable` on the poller fails the queue (model: `PollerCrash`).
7. The fallback path loops while a continuation remains (not modeled).

Not a model action: `Thread.currentThread().interrupt()` before wrapping an `InterruptedException`, `volatile` on `R.poller`, and the new public `O.isCancelled()` (it is read by `mapContinuationFailure`, see `CancelOp`).

Reviewed after the sync, not a model action: `436156f29` (`ArrowFlightConnection` shuts the poller pool down with `shutdown()` and a 2 s grace before `shutdownNow()`, so a poller's `CancelFlightInfo` is not interrupted). A rejected submit after the shutdown is already the free choice `ReaderResumeRejected`, and, per the commit message, the statements are closed (pollers stopped) before the pool goes. The sync commit stays at `4b831553c`: move it to a later commit once its main-code diff has been checked the same way.

| Model action | Code at the sync commit |
| --- | --- |
| `PollerSend`, `PollerTake` | P `drive` -> O `pollOnce` -> `poll`. The poll runs on a pool thread, or on the reader's thread when run inline. |
| `PollerApplyBackoff`, `PollerApplyGo` | O `remember` (inside `pollOnce`: `validateAppendOnly`, new continuation), then R `appendEndpoints` -> Q `addEndpoints`; then P `drive`: `operation.backoff` (backoff) or the next loop iteration (go). On a failed queue Q `addEndpoints` returns at its `failure != null` check and changes nothing (`ApplyQ`, `FailedQueueInert`); P `drive` then stops at its loop-top check (`PollerSeesFail`). |
| `PollerApplyHandBack` | The same, run inline: P `drive(true)` after an answer that added endpoints calls `handBack` (`submit`, or orphaned again). |
| `PollerApplyFinish` | Q `addEndpoints(list, false)` and P `drive`: `FINISHED`, `onFinished`. |
| `PollerPause`, `PollerPauseEnd`, `PollerCheck` | Q `pauseIfFull` with P `drive`'s `onPaused` (paused flag and `owned.set(false)` under the lock). `PollerPauseEnd` and `PollerCheck` exist only for the broken variants. |
| `PollerSeesFail`, `PollerDrop` | P `drive`: the loop-top check and the check after `pollOnce`. |
| `BackoffElapses` | O `backoff` (cancellable wait), P `drive` `BACKOFF` state. |
| `OpenReserved`, `OpenFails` | Q `openReserved`, `register`, `releaseReservations`, `fail`. The `opening` count is `resv`. `openReserved` stops at `shutdown \|\| failure != null` and releases the rest, and `register` drops a stream opened after a failure or close: in both cases the reservation is released and no stream is registered (`OpenReserved` leaves `opened` unchanged once `failSeen` or `gated`). |
| `AppTake` | R `populateData` (first load: Q `nextAvailable`), `next` and `loadNextPublishedEndpoint` -> Q `awaitNext` returning a ready future (`ready != null`, after `throwIfFailed`). No slot changes: the stream stays in `openUnread` and `endpointsToClose`. |
| `AppExhaust` | R `next`: `currentEndpointData.getStream().next()` returned false, then R `endpointExhausted` -> Q `endpointExhausted` -> `onStreamFinished(finished)`: under the lock `endpointsToClose.remove`, `openUnread--`, `reserveSlotsLocked`, paused flag cleared (`ReadFree`); after the unlock `closeQuietly`, the resume action (`ReaderResume`), `openReserved`. Only R `next` calls it, on the application thread. |
| `AppReadEmpty` | Q `awaitNext` taking a `null` future (a stream that ended inside the pool task without rows) -> `onStreamFinished` (same locked step as `AppExhaust`). |
| `ReadFree` (the shared locked step) | Q `onStreamFinished`. On a failed or closed queue `pending` is empty (`fail()` and `close()` clear it), so it reserves nothing. |
| `ReaderResume`, `ReaderResumeRejected` | The resume action `onStreamFinished` runs after `lock.unlock()`: P `resume` (`owned` compare-and-set), `handBack` (`submit`, 10 attempts, then `orphaned.set(true)`, `wakeReader`). |
| `AppIdle`, `InlineExit` | Q `awaitNext`'s idle hook -> P `runInlineIfOrphaned` (`drive(true)`). `idleTried` is the local variable in `awaitNext`; it is cleared after a wait and after every consumed future (`AppReadEmpty`, `IdleFix`). |
| `ReaderWait` and the wakes (`WakeRd`) | Q `awaitNext` -> `awaitChange`; `signalAll` in `addEndpoints`, `register`, `releaseReservations`, `fail`, `close`, `wakeReader`. |
| `AppEnd` | Q `awaitNext` returning `null` (futures empty, pending empty, `opening == 0`, `!moreExpected`, under the lock), R `next` returning false. |
| `AppThrow` | Q `awaitNext` -> `checkOpen`, `throwIfFailed`; R `next` top-of-call checks of `cancelled` and `queue.failure()`. |
| `CancelOp` | `ArrowFlightStatement.cancel` and `ArrowFlightPreparedStatement.cancel`: `operation.cancel()` runs before `super.cancel()` reaches `R.cancel`. O `cancel` sets `cancelled`, cancels the gRPC context (an open poll ends `CANCELLED`, O `call` refuses new RPCs). |
| `AppCancel`, `CancelStop` | R `cancel` and `close`: `cancelled = true`; then P `stop`, O `cancel`, Q `close`. |
| `SrvAnswer` failure branch, `PollerTake` error branch | P `onPollFailed` -> `queue.fail(failureMapper.apply(e))` (R `mapContinuationFailure`), then `afterFailure` (not modeled). If the poller is not stopped and the queue is not closed yet, a poll that `CancelOp` ended is recorded as the failure "Statement canceled" (O `isCancelled`). If `stopped` or the queue is closed, `onPollFailed` returns without a failure (`PollerDrop`). |
| `PollerCrash` | P `runOnPool` and `runInlineIfOrphaned`: `catch (Throwable)` -> P `fail(Throwable)` -> Q `fail` (an `Error` is rethrown afterwards). Also P `drive`'s `queue.fail` for a sink `SQLException`, a missing continuation, and an interrupted backoff. |

## When to use it

Run it when a diff touches any covered file:

```sh
git --no-pager diff --name-only 4b831553c..HEAD -- \
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

Any output means the model may no longer describe the code. Also use this skill for any new class that polls or opens streams on a background thread. Read the code diff itself with `git --no-pager diff 4b831553c..HEAD -- <paths>` (or `git show`, or `rtk proxy git diff`): a hook may summarize plain `git diff` output.

## How to check a change

1. **Mirror the change in the model.** The model is a hand-written design model, so TLC cannot read Java. Use the coverage record to find the action that corresponds to the code you changed, and edit that action so it does what the new code does. Keep each atomic step in the model atomic in the code: a step under one lock, or one RPC answer, is one action.
2. **Run every check** (about 35 seconds each; needs Java 11+ and `tla2tools.jar` v1.7.4, found through `TLA2TOOLS` or the mise install path):

   ```sh
   formal/pollinfo-poll-ahead/run.sh
   N=4 formal/pollinfo-poll-ahead/run.sh
   ```

   The script exits 0 only when every line says `ok`. The design checks must pass. `finding-open-streams` fails on `OpenWithinLimit` by design: it is the old `EagerOpen = TRUE` proposal, which the implementation does not follow. `deferred-open` is the code as written, with `IdleFix = TRUE`, every safety rule and `Terminates`; it must pass. Every broken variant must fail on its rule; they run with `IdleFix = TRUE` so the stale-idle bug cannot hide a variant that no longer fails. `staleIdle` is the exception: it runs with `IdleFix = FALSE` and must fail on `Terminates`. `openNotReserved` only fails from `N=4` on. `lateRelease-old-rules` is the other exception, on purpose: it runs `lateRelease` against the rules that existed before `SlotFreedOnExhaust` and must pass, which records that the late release is invisible to them (finding 3). A variant that suddenly passes means the model lost the ability to catch that bug: fix the model before you trust any pass.
3. **Read any failure with the pstack `/statechart` skill** (type `/pstack:statechart`). Give it a copy of `PollAhead.tla` and a config with the failing rule, in a scratch folder outside the repo. It prints the shortest path to the violation as numbered steps in plain words. When the model passes, it can draw how a result moves between states.
4. **Decide whether it is a bug or a modeling error.** Replay the path against the real code, one model action per step: as a unit test with a scripted Flight SQL producer (see `PollingMockFlightSqlProducer` in the JDBC tests), or against the Go conformance server (branch `bdx-645-pollinfo-conformance-server` of `dremio-employees/helder.gregorio`). If the code does the same thing, it is a real bug: report it with the path in the code's words, and do not weaken the rule. If the code cannot take that path, the model is wrong: fix the action, and say what you fixed.
5. **When a behavior change is intended**, change the action and, if a rule no longer applies, change that rule in a separate commit that explains why. Add a broken variant (a new `Mutation` value plus a `check` line in `run.sh`) for any new rule, and confirm it fails.
6. **Move the sync commit** in the coverage record only after you have confirmed that every action still matches the code at the new commit. Update the coverage table at the same time. Check the non-atomic steps in particular: the resume after unlocking against `owned`, opening outside the lock through reserved slots, the orphaned inline poller and the `idleTried` flag, and the two steps of `cancel()`.

## Known findings

### 1. Stale `idleTried`: `next()` could hang after a rejected resume (found by the model, fixed in `dadd45d8a`)

`Terminates` failed on the model of the code at `d7c3ed4ab` (check `finding-stale-idle`, now the broken variant `staleIdle` with `IdleFix = FALSE`, which still fails). `FlightEndpointDataQueue.awaitNext` keeps a local `idleTried` flag. It set it when the idle hook returned false and cleared it only after a wait. If an endpoint finished between those two, the flag stayed set. Shortest path, with a limit of 2 (16 steps):

1. The application has read the first endpoint. `awaitNext` finds nothing queued, `moreExpected` true. It calls the idle hook (`EndpointPoller.runInlineIfOrphaned`), which returns false because the poller is not orphaned, so `idleTried = true`.
2. Before the reader takes the lock again, the poller's open poll answers with two new endpoints. `addEndpoints` reserves both slots, `openReserved` registers the streams (futures not empty), and the poller pauses at the limit (`pauseIfFull`).
3. The reader takes the first new stream's future. It yields `null` (an endpoint with no rows). `onStreamFinished` frees a slot, clears the paused flag and runs the resume (`EndpointPoller.resume` -> `handBack`). The pool rejects the submit 10 times, so the poller is orphaned (`orphaned.set(true)`, `wakeReader()` on the reader's own thread, so nobody is waiting).
4. The second new stream also yields `null`. The loop reaches the empty-futures branch with nothing queued and `moreExpected` true. `!idleTried` is false, so the reader does not run the orphaned poller. It waits for a signal nobody will send.

The path was narrow: the poller had to answer and register a stream inside the reader's few instructions between the idle hook and the next lock, the connection's poller pool had to reject a resume (all `pollInfoPollerThreads` busy with other results, or the pool shut down), and the new endpoints had to have no rows. When it happened, `next()` hung until `cancel()` or `close()`.

Fix, in `dadd45d8a`: `awaitNext` sets `idleTried = false` right after `onStreamFinished(source)`. This is exactly `IdleFix = TRUE` (`AppReadEmpty` clears `idleTried`; `AppTake` returns to the application, where the local flag ends). Regression test: `rejectedResumeDuringTheIdleHookDoesNotStrandTheReader` in `FlightEndpointDataQueueDeferredOpenTest`. Commit `dadd45d8a` (`next()` rethrows `queue.failure()` before the CANCELLED mapping) changes only which exception a blocked reader throws, not any model action.

### 2. `finding-open-streams`: the first proposal (`EagerOpen = TRUE`, resolved)

`OpenWithinLimit` fails for the proposal as first written. The limit is checked before a poll, but one answer can publish several endpoints, and their streams all open at once. With a limit of 2: one stream is open, the poller sends the continuation, and an answer with three endpoints opens two more, leaving three open streams. The implementation does not do this: `reserveSlotsLocked` opens only while fewer than `maxOpen` streams are open or opening (`EagerOpen = FALSE`, check `deferred-open`).

### 3. Late slot release: found by a reviewer, not by the model checker (fixed in `4b831553c`)

**What the code did** (`dadd45d8a`). When the application read a stream to its end, `ResultSet.next()` re-enqueued it (`enqueueEndpointData` -> `Q.enqueue`). A reader-pool task then ran `stream.next()`, got false and returned `null`, and the slot (`openUnread`) was freed only when a later `awaitNext` took that `null` future (`onStreamFinished`). Three consequences:

- The slot of a finished stream stayed taken while the application read another one, because `awaitNext` takes the first completed future, whichever stream it belongs to.
- So the poller stayed paused, and pending endpoints stayed unopened, behind a stream nobody reads. With `pollInfoPrefetch = 2`, a finished stream plus the stream being read fill the window.
- The release needed a free thread in the connection's fixed-size reader pool, which every statement of the connection shares. A pool full of tasks blocked on slow streams of other results delays the release of an unrelated stream.

**Why the model did not see it.** The old `AppRead` took a ready future and freed its slot in the same step, at hand-over, before the application read anything. That is not an over-approximation of the old code, it is a different point in time, and it was wrong in both directions:

- Too tight: no state had "the application finished a stream, its slot is still taken", so the late release could not occur.
- Too loose: while the application held a stream the model had already freed its slot, so `Unread`, `Backpressure` and `OpenWithinLimit` ignored it. The model's poller could run one endpoint further ahead than the code's (`pauseIfFull` counts `openUnread`, which includes the stream being read), and the model's number of open streams, the one being read included, could reach `Limit + 1` without any rule noticing.

No rule could have caught it, and none does: `lateRelease-old-rules` shows that the late-release model passes every rule that existed before this change, `Terminates` included. It is a progress problem (the poller is blocked by a stream that is already finished), not a safety or termination problem.

**What the model does now.** `AppRead` is split into `AppTake` (hand-over, no slot change) and `AppExhaust` (the application's own `stream.next()` returned false, and the slot is freed under the lock in the same step, on the application thread). The held stream counts as unread and open, as `openUnread` does. The new rule `SlotFreedOnExhaust` says "no finished stream holds a slot while the application reads another one", which is the "poller is not blocked by an unrelated stream" property. The broken variant `lateRelease` reproduces the old code: `AppExhaust` only marks the stream as spent, and the slot is freed when `AppReadEmpty` takes its null future. It fails `SlotFreedOnExhaust` in 9 steps, with a limit of 2:

1. The server publishes a second endpoint. The poller polls and the answer lists two endpoints.
2. The poller applies the answer, the second endpoint is reserved, and its stream is opened. Two streams are open: the limit.
3. The application takes the first stream (`AppTake`), reads it to the end (`AppExhaust`): its slot stays taken.
4. The reader takes the second stream's ready future first (`AppTake`, completion order) while the first stream's slot is still taken. The poller cannot prefetch anything while the application reads the second stream.

**Limits of this check.** `SlotFreedOnExhaust` holds in the faithful model by the construction of `AppExhaust`: it guards the action against being split again, it does not look at Java. The code is guarded by Java tests such as `FlightEndpointDataQueueDeferredOpenTest.endpointExhaustedFreesTheSlotOnTheCallingThread`: after `endpointExhausted`, `openUnreadCount()` has dropped on the calling thread. The third consequence (the reader pool) is not modeled: pool threads and other results are not in the model, so no rule can say that a release depends on a free thread.

### 4. The other review fixes in `4b831553c`

None of these was found by TLC. Each row says what the model did about it.

| Change | Model | Rule or variant |
| --- | --- | --- |
| 2. The first load does not wait for unpublished endpoints | None. The first load is the same `awaitNext` loop (see the scope section), and the model starts at its top. A first endpoint without rows leaves no stream, and the following `next()` does the waiting: states the model already has. | None. A "`executeQuery` does not block on the poller" rule would need time or an uncooperative server. |
| 3. The queue is failed before the blocking `cancel()` / `terminate()` | None. The model already records the failure in the step that takes the answer, so the old code was the one that did not match. The delay was bounded by the 1 s timeout of `cancelFlightInfoBestEffort`, a latency problem. | None, for the same reason: no time in the model. |
| 4. A cancel that races a poll is recorded as the failure "Statement canceled" | New action `CancelOp` (`PollInfoOperation.cancel()` before `ResultSet.cancel()`), `SrvAnswer` answers `CANCELLED` once `opCancelled`, and `PollerTake` records it as `failSeen` while `cancelled` is still false. Reachable (checked), and every rule holds: the reader throws either way, only the exception differs. | No new rule. `FailFast` and `CancelStopsPolling` hold with the extra step. |
| 5. A failed queue is inert | `ApplyQ` is a no-op once `failSeen`; `OpenReserved` registers nothing once `failSeen` or `gated`; `ReadFree` finds no pending endpoint after a failure or close (the model derives pending from `known - opened - resv`, so it is masked by `failSeen \/ gated`). | New rule `FailedQueueInert`, variant `applyAfterFail`. The check in `openReserved` is a `volatile` read outside the lock, so "no `DoGet` starts after a failure" is not a rule: a race window remains and only the locked part is atomic. |
| 6. Any `Throwable` on the poller fails the queue | New action `PollerCrash`. Before this, the model listed an `Error` killing the poller as not modeled. | Variant `errorNotReported`: an `Error` leaves the poller dead and the queue not failed, so `next()` waits forever (`Terminates`). The old code did exactly this for an `Error`. |
| 7. The fallback path loops while a continuation remains | None. The path is outside the model (see the scope section). | None. Its old defect belongs to the class of `NoPrematureEnd`. Extending the model to the fallback path is the open follow-up. |

No rule failed on the model of `4b831553c`: every line of `run.sh` says `ok` at `N=3` and `N=4`, and the design checks also pass with a limit of 1 and of 3.

## Files

- `formal/pollinfo-poll-ahead/PollAhead.tla`: the model, with the code map in its header.
- `formal/pollinfo-poll-ahead/run.sh`: all design checks and broken variants, with their expected results. Variants: `staleIdle`, `endWhenQueueEmpty`, `endIgnoresOpening`, `finishBeforeEnqueue`, `enqueueWholeList`, `reuseContinuation`, `readAfterFail`, `cancelIgnoresPoller`, `noLimit`, `openNotReserved`, `noBackoff`, `noWake`, `pauseNotAtomic`, `ownedClearedLate`, `noIdleHook`, `lateRelease`, `applyAfterFail`, `errorNotReported`, plus the gap check `lateRelease-old-rules`.
