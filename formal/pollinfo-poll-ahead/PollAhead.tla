----------------------------- MODULE PollAhead -----------------------------
(***************************************************************************)
(* Bounded poll-ahead for the Arrow Flight SQL JDBC driver (BDX-645).      *)
(*                                                                         *)
(* One progressive JDBC ResultSet, from the moment its poller has started: *)
(* the first PollInfo published endpoint E1 and continuation C1, and C1    *)
(* has been handed to the poller. The application thread is in             *)
(* executeQuery's first load (Q.nextAvailable) or in ResultSet.next(); both*)
(* run the same awaitNext loop, and the first load only differs by         *)
(* returning null instead of waiting, so the model does not tell them apart.*)
(*                                                                         *)
(* Parties that run concurrently:                                          *)
(*   - the Flight SQL server, which publishes endpoints and finishes or    *)
(*     fails the query, and answers PollFlightInfo at any moment (a server *)
(*     that does not long-poll may answer at once with nothing new);       *)
(*   - the poller (EndpointPoller), which chains continuations. It runs on *)
(*     a pool thread, or, if the pool rejected a resume, inline on the     *)
(*     reader's thread;                                                    *)
(*   - the reader: the application thread inside ResultSet.next(), which   *)
(*     is inside FlightEndpointDataQueue.awaitNext, takes a ready stream,  *)
(*     reads it to the end (and frees its slot), ends the results, or      *)
(*     throws;                                                             *)
(*   - another application thread that may call Statement.cancel().        *)
(*                                                                         *)
(* Code map (sync commit in .claude/skills/verify-jdbc-poll-ahead/SKILL.md;*)
(* "Q" = utils/FlightEndpointDataQueue, "P" = EndpointPoller, "R" =        *)
(* ArrowFlightJdbcFlightStreamResultSet, "O" = client/PollInfoOperation):  *)
(*   PollerSend / PollerTake    P.drive -> O.pollOnce -> O.poll            *)
(*   PollerApply*               O.remember (inside pollOnce) and          *)
(*                              R.appendEndpoints -> Q.addEndpoints, one   *)
(*                              locked step: append, set moreExpected,     *)
(*                              reserve slots, signal. P.drive then backs  *)
(*                              off, continues, or finishes                *)
(*   PollerPause                Q.pauseIfFull (count check, paused flag    *)
(*                              and P's owned=false in one locked step)    *)
(*   OpenReserved, OpenFails    Q.openReserved / Q.register: the stream    *)
(*                              opens OUTSIDE the lock, into a slot that   *)
(*                              was reserved under it (the "opening"       *)
(*                              count)                                     *)
(*   AppTake                    Q.awaitNext returning a ready stream to R. *)
(*                              The stream keeps its slot while the        *)
(*                              application reads it                       *)
(*   AppExhaust                 R.next: the application's own stream.next()*)
(*                              returned false -> Q.endpointExhausted ->   *)
(*                              Q.onStreamFinished: under the lock free    *)
(*                              the slot, reserve pending endpoints, clear *)
(*                              the paused flag (one step on the reader)   *)
(*   AppReadEmpty               Q.awaitNext taking a null future (a stream *)
(*                              that ended inside the pool task without    *)
(*                              rows) -> Q.onStreamFinished, same locked   *)
(*                              step                                       *)
(*   ReaderResume               the resume action Q.onStreamFinished runs  *)
(*                              AFTER unlocking: P.resume (claim owned,    *)
(*                              resubmit; 10 rejected submits orphan P)    *)
(*   AppIdle, ReaderWait,       Q.awaitNext, empty-futures branch: end,    *)
(*   AppEnd, AppThrow           idle hook (P.runInlineIfOrphaned), or wait *)
(*   InlineExit                 P.runInlineIfOrphaned returning            *)
(*   CancelOp, AppCancel,       Statement.cancel: O.cancel first (an open  *)
(*   CancelStop                 poll ends CANCELLED), then R.cancel: the   *)
(*                              cancelled flag, then P.stop + O.cancel +   *)
(*                              Q.close                                    *)
(*   PollerCrash                P.runOnPool / P.runInlineIfOrphaned catch  *)
(*                              any Throwable -> P.fail -> Q.fail; also    *)
(*                              the answer-validation failures             *)
(*   PollerSeesFail, PollerDrop P.drive loop top and the check after the   *)
(*                              poll                                       *)
(*                                                                         *)
(* The implementation corresponds to EagerOpen = FALSE (a Q built with     *)
(* maxOpen = pollInfoPrefetch). Answers with no continuation, and          *)
(* pollInfoPrefetch = 0, use an unlimited eager queue and never start a    *)
(* poller; the model does not cover them.                                  *)
(*                                                                         *)
(* Rows are abstracted: an endpoint is taken (AppTake, its first batch     *)
(* reaches the application) and later read to the end (AppExhaust, which   *)
(* frees its slot); nothing in between is tracked. Endpoint i is the i-th  *)
(* endpoint the server publishes; PollInfo is cumulative, so an answer is  *)
(* just how many endpoints exist so far. An endpoint is either empty (no   *)
(* rows: its future yields null, AppReadEmpty) or not.                     *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    N,          \* most endpoints the query can publish
    Limit,      \* pollInfoPrefetch: the poller polls only while queued, unread endpoints < Limit
    EagerOpen,  \* TRUE: every enqueued endpoint takes a stream slot at once (the proposal as first written)
    IdleFix,    \* TRUE: the code (awaitNext clears idleTried after a wait and after every consumed
                \* future, dadd45d8a); FALSE: the stale-idle bug, kept to show that the model catches it
    Mutation    \* "none", or the name of a deliberately broken variant (see run.sh)

VARIABLES
    published,      \* endpoints the server has published
    server,         \* "running" | "done" | "failed"
    known,          \* endpoints handed to the queue by addEndpoints (cumulative prefix)
    resv,           \* slots reserved under the lock whose stream is not registered yet ("opening")
    opened,         \* streams registered with the queue (future submitted), cumulative
    read,           \* endpoints finished and freed (onStreamFinished done)
    enq,            \* enq[i]: how many times endpoint i was handed to the queue (capped at 2)
    more,           \* the queue's moreExpected flag
    pflag,          \* the queue's paused flag
    contHeld,       \* the driver holds a continuation descriptor
    contUsed,       \* the held continuation has already been sent
    inflight,       \* a PollFlightInfo RPC is open
    resp,           \* the server's answer, not yet taken by the poller
    got,            \* the answer the poller took, not yet applied
    poller,         \* "running" (at the top of its loop) | "polling" | "received" | "backoff"
                    \* | "paused" | "pausing" | "orphaned" | "finished" | "failed" | "stopped"
    lastEmpty,      \* the last applied answer added no endpoints while the query was running
    waited,         \* a backoff has elapsed since that empty answer
    sawFull,        \* pauseNotAtomic only: the poller saw the limit reached and has not paused yet
    rd,             \* the reader: "run" (top of the awaitNext loop) | "resume" | "waiting" | "inline"
    idleTried,      \* awaitNext's local idleTried flag
    held,           \* the application holds a stream (currentEndpointData) that still has its slot
    spent,          \* lateRelease only: streams the application finished, slot not freed yet
    app,            \* "reading" | "ended" | "threw"
    failSeen,       \* the queue recorded a failure
    opCancelled,    \* Statement.cancel called PollInfoOperation.cancel: an open poll ends CANCELLED
    cancelled,      \* ResultSet.cancelled: next() throws from now on
    gated,          \* cancel finished: poller stopped, operation cancelled, queue closed
    badApply,       \* addEndpoints queued endpoints on a queue that had already failed
    badRow,         \* a row was handed to the application after a failure or cancel
    badPoll,        \* a poll was sent after cancel finished
    polledAtLimit,  \* a poll was sent while queued, unread endpoints were at the limit
    resent,         \* a continuation was sent twice
    spun            \* a poll was sent right after an empty answer, without backoff

srvV  == <<published, server>>
qV    == <<known, resv, opened, read, enq, more, pflag>>
rpcV  == <<contHeld, contUsed, inflight, resp, got>>
polV  == <<poller, lastEmpty, waited, sawFull>>
rdV   == <<rd, idleTried, held, spent>>
appV  == <<app, failSeen, opCancelled, cancelled, gated>>
flag5 == <<badRow, badPoll, polledAtLimit, resent, spun>>
flagV == <<badApply, flag5>>

vars == <<srvV, qV, rpcV, polV, rdV, appV, flagV>>

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

Answer == [has : BOOLEAN, count : 0..N, more : BOOLEAN, err : BOOLEAN]
NoAnswer == [has |-> FALSE, count |-> 0, more |-> FALSE, err |-> FALSE]

Unread == known - read                  \* pending + open-unread (a held stream is still open-unread)
Futs == opened - read - (IF held THEN 1 ELSE 0)   \* futures in the completion queue
NoFutures == Futs = 0                   \* the queue's `futures` map is empty
NothingQueued == known = opened /\ resv = 0   \* pending empty and opening == 0

\* The reader is woken (signalAll) only if it is waiting; then it clears its idle flag.
WakeRd == /\ rd' = IF rd = "waiting" THEN "run" ELSE rd
          /\ idleTried' = IF rd = "waiting" THEN FALSE ELSE idleTried
          /\ UNCHANGED <<held, spent>>

-----------------------------------------------------------------------------
Init ==
    /\ published = 1
    /\ server = "running"
    /\ known = 1
    /\ resv = 0
    /\ opened = 1
    /\ read = 0
    /\ enq = [i \in 1..N |-> IF i = 1 THEN 1 ELSE 0]
    /\ more = TRUE
    /\ pflag = FALSE
    /\ contHeld = TRUE
    /\ contUsed = FALSE
    /\ inflight = FALSE
    /\ resp = NoAnswer
    /\ got = NoAnswer
    /\ poller = "running"
    /\ lastEmpty = FALSE
    /\ waited = FALSE
    /\ sawFull = FALSE
    /\ rd = "run"
    /\ idleTried = FALSE
    /\ held = FALSE
    /\ spent = 0
    /\ app = "reading"
    /\ failSeen = FALSE
    /\ opCancelled = FALSE
    /\ cancelled = FALSE
    /\ gated = FALSE
    /\ badApply = FALSE
    /\ badRow = FALSE
    /\ badPoll = FALSE
    /\ polledAtLimit = FALSE
    /\ resent = FALSE
    /\ spun = FALSE

-----------------------------------------------------------------------------
(* Server *)

SrvPublish ==
    /\ server = "running"
    /\ published < N
    /\ published' = published + 1
    /\ UNCHANGED <<server, qV, rpcV, polV, rdV, appV, flagV>>

SrvComplete ==
    /\ server = "running"
    /\ server' = "done"
    /\ UNCHANGED <<published, qV, rpcV, polV, rdV, appV, flagV>>

SrvFail ==
    /\ server = "running"
    /\ server' = "failed"
    /\ UNCHANGED <<published, qV, rpcV, polV, rdV, appV, flagV>>

\* The server answers the open poll. It may answer at once with nothing new.
\* Once Statement.cancel has cancelled the operation's context (opCancelled, which comes before
\* R.cancel sets `cancelled`), the open poll ends with CANCELLED.
SrvAnswer ==
    /\ inflight
    /\ ~resp.has
    /\ resp' = IF opCancelled \/ gated \/ server = "failed"
                 THEN [has |-> TRUE, count |-> 0, more |-> FALSE, err |-> TRUE]
                 ELSE [has |-> TRUE, count |-> published, more |-> server = "running", err |-> FALSE]
    /\ inflight' = FALSE
    /\ UNCHANGED <<srvV, qV, contHeld, contUsed, got, polV, rdV, appV, flagV>>

-----------------------------------------------------------------------------
(* Poller (EndpointPoller.drive, on the poller pool or inline on the reader) *)

\* Poll the held continuation, after pauseIfFull said no.
PollerSend ==
    /\ poller = "running"
    /\ contHeld
    /\ ~inflight
    /\ Mutation = "noLimit" \/ Unread < Limit
    /\ inflight' = TRUE
    /\ poller' = "polling"
    /\ contUsed' = TRUE
    /\ resent' = (resent \/ contUsed)
    /\ spun' = (spun \/ (lastEmpty /\ ~waited))
    /\ badPoll' = (badPoll \/ gated)
    /\ polledAtLimit' = (polledAtLimit \/ Unread >= Limit)
    /\ waited' = FALSE
    /\ UNCHANGED <<srvV, qV, contHeld, resp, got, lastEmpty, sawFull, rdV, appV, badRow, badApply>>

\* Q.pauseIfFull: at the limit, the poller sets the paused flag and gives its thread back.
\* The count check, the flag and P's owned=false are one step under the queue lock.
\* The inline poller never pauses (drive(true) skips pauseIfFull).
PollerPause ==
    /\ poller = "running"
    /\ rd # "inline"
    /\ contHeld
    /\ more /\ ~failSeen /\ ~gated
    /\ Mutation # "noLimit"
    /\ IF Mutation = "pauseNotAtomic" THEN sawFull ELSE Unread >= Limit
    /\ pflag' = TRUE
    /\ poller' = IF Mutation = "ownedClearedLate" THEN "pausing" ELSE "paused"
    /\ sawFull' = FALSE
    /\ UNCHANGED <<srvV, known, resv, opened, read, enq, more, rpcV, lastEmpty, waited,
                   rdV, appV, flagV>>

\* Broken variant only: the poller reads the count, and pauses in a later step.
PollerCheck ==
    /\ Mutation = "pauseNotAtomic"
    /\ poller = "running"
    /\ rd # "inline"
    /\ contHeld
    /\ ~sawFull
    /\ Unread >= Limit
    /\ sawFull' = TRUE
    /\ UNCHANGED <<srvV, qV, rpcV, poller, lastEmpty, waited, rdV, appV, flagV>>

\* Broken variant only: the poller sets the paused flag under the lock but clears
\* `owned` after unlocking.
PollerPauseEnd ==
    /\ poller = "pausing"
    /\ poller' = "paused"
    /\ UNCHANGED <<srvV, qV, rpcV, lastEmpty, waited, sawFull, rdV, appV, flagV>>

\* The top of drive(): the queue has failed, so the poller stops.
PollerSeesFail ==
    /\ poller = "running"
    /\ failSeen
    /\ poller' = "stopped"
    /\ UNCHANGED <<srvV, qV, rpcV, lastEmpty, waited, sawFull, rdV, appV, flagV>>

\* The poller takes the answer. A failure fails the queue at once (onPollFailed -> Q.fail; the
\* operation's own cancel/terminate RPC runs afterwards and is not modeled). An answer to a poll
\* that Statement.cancel ended (opCancelled, but P.stop and Q.close have not run yet) is recorded
\* as a failure too: the failure mapper turns it into "Statement canceled".
PollerTake ==
    /\ poller = "polling"
    /\ resp.has
    /\ resp' = NoAnswer
    /\ IF resp.err
         THEN /\ poller' = "failed"
              /\ failSeen' = TRUE
              /\ contHeld' = FALSE
              /\ UNCHANGED <<got, more, held, spent>>
              /\ rd' = IF rd \in {"waiting", "inline"} THEN "run" ELSE rd
              /\ idleTried' = IF rd \in {"waiting", "inline"} THEN FALSE ELSE idleTried
         ELSE /\ got' = resp
              /\ poller' = "received"
              /\ more' = IF Mutation = "finishBeforeEnqueue" /\ ~resp.more THEN FALSE ELSE more
              /\ UNCHANGED <<failSeen, contHeld, rdV>>
    /\ UNCHANGED <<srvV, known, resv, opened, read, enq, pflag, contUsed, inflight, lastEmpty,
                   waited, sawFull, app, opCancelled, cancelled, gated, flagV>>

\* Anything unexpected ends the poller (a RuntimeException or an Error from the RPC layer or the
\* sink, a protocol violation in an answer, an interrupted backoff): P.runOnPool and
\* P.runInlineIfOrphaned catch every Throwable and fail the queue (P.fail -> Q.fail). The poller
\* never keeps a half-applied answer. Broken variant errorNotReported: only a RuntimeException
\* failed the queue, an Error silently killed the pool thread (the poller is "dead").
PollerCrash ==
    /\ poller \in {"running", "polling", "received", "backoff"}
    /\ IF Mutation = "errorNotReported"
         THEN /\ rd # "inline"
              /\ poller' = "dead"
              /\ UNCHANGED <<failSeen, rd, idleTried>>
         ELSE /\ poller' = "failed"
              /\ failSeen' = TRUE
              /\ rd' = IF rd \in {"waiting", "inline"} THEN "run" ELSE rd
              /\ idleTried' = IF rd \in {"waiting", "inline"} THEN FALSE ELSE idleTried
    /\ contHeld' = FALSE
    /\ inflight' = FALSE
    /\ resp' = NoAnswer
    /\ got' = NoAnswer
    /\ UNCHANGED <<srvV, qV, contUsed, lastEmpty, waited, sawFull, held, spent, app, opCancelled,
                   cancelled, gated, flagV>>

\* Slots addEndpoints can reserve: fewer than Limit streams open or opening and unread.
\* EagerOpen: every new endpoint takes a slot at once.
ApplyRsv ==
    LET pendNew == got.count - opened - resv
        free == Max(0, Limit - (opened - read)
                       - (IF Mutation = "openNotReserved" THEN 0 ELSE resv))
    IN IF EagerOpen THEN pendNew ELSE Min(pendNew, free)

\* addEndpoints: in one locked step append only the new suffix, set moreExpected, reserve slots
\* (their streams open later, outside the lock), and signal the reader. On a failed queue it is a
\* no-op: nothing is queued or opened, the reader rethrows the failure (broken variant
\* applyAfterFail: the old code queued and reserved anyway).
ApplyQ ==
    /\ got.has
    /\ poller = "received"
    /\ IF failSeen /\ Mutation # "applyAfterFail"
         THEN /\ UNCHANGED <<enq, known, resv, more, badApply>>
         ELSE /\ enq' = [i \in 1..N |->
                           IF i >= (IF Mutation = "enqueueWholeList" THEN 1 ELSE known + 1)
                              /\ i <= got.count
                             THEN Min(enq[i] + 1, 2) ELSE enq[i]]
              /\ known' = got.count
              /\ resv' = resv + ApplyRsv
              /\ more' = got.more
              /\ badApply' = (badApply \/ failSeen)
    /\ got' = NoAnswer

\* An answer that adds nothing while the query runs: back off, then poll again.
PollerApplyBackoff ==
    /\ ApplyQ
    /\ got.more
    /\ got.count = known
    /\ contHeld' = TRUE
    /\ contUsed' = (Mutation = "reuseContinuation")
    /\ lastEmpty' = TRUE
    /\ poller' = IF Mutation = "noBackoff" THEN "running" ELSE "backoff"
    /\ WakeRd
    /\ UNCHANGED <<srvV, opened, read, pflag, inflight, resp, waited, sawFull, appV, flag5>>

\* An answer that adds endpoints, applied by a pool thread: go on to the next iteration.
PollerApplyGo ==
    /\ ApplyQ
    /\ got.more
    /\ got.count > known
    /\ rd # "inline"
    /\ contHeld' = TRUE
    /\ contUsed' = FALSE
    /\ lastEmpty' = FALSE
    /\ poller' = "running"
    /\ WakeRd
    /\ UNCHANGED <<srvV, opened, read, pflag, inflight, resp, waited, sawFull, appV, flag5>>

\* An answer that adds endpoints, applied inline on the reader: hand the poller back to the
\* pool (handBack). If the pool rejects it 10 times, the poller stays orphaned.
PollerApplyHandBack ==
    /\ ApplyQ
    /\ got.more
    /\ got.count > known
    /\ rd = "inline"
    /\ contHeld' = TRUE
    /\ contUsed' = FALSE
    /\ lastEmpty' = FALSE
    /\ poller' \in {"running", "orphaned"}
    /\ rd' = "run"
    /\ idleTried' = FALSE
    /\ UNCHANGED <<srvV, opened, read, pflag, inflight, resp, waited, sawFull, held, spent,
                   appV, flag5>>

\* The final answer: the queue sees the last endpoints and moreExpected = FALSE together.
PollerApplyFinish ==
    /\ ApplyQ
    /\ ~got.more
    /\ contHeld' = FALSE
    /\ lastEmpty' = FALSE
    /\ poller' = "finished"
    /\ rd' = IF rd = "inline" \/ rd = "waiting" THEN "run" ELSE rd
    /\ idleTried' = IF rd = "inline" \/ rd = "waiting" THEN FALSE ELSE idleTried
    /\ UNCHANGED <<srvV, opened, read, pflag, inflight, resp, contUsed, waited, sawFull,
                   held, spent, appV, flag5>>

\* 100 ms doubling up to pollInfoMaxBackoff; time is abstracted to one step.
BackoffElapses ==
    /\ poller = "backoff"
    /\ poller' = "running"
    /\ waited' = TRUE
    /\ UNCHANGED <<srvV, qV, rpcV, lastEmpty, sawFull, rdV, appV, flagV>>

\* A stopped poller discards the answer to the poll that cancel interrupted.
PollerDrop ==
    /\ poller = "stopped"
    /\ resp.has
    /\ resp' = NoAnswer
    /\ UNCHANGED <<srvV, qV, contHeld, contUsed, inflight, got, polV, rdV, appV, flagV>>

-----------------------------------------------------------------------------
(* Opening streams, outside the lock, into reserved slots. Whoever reserved the slot opens it;
   the model lets any step do it, which only adds behaviors. On a failed or closed queue
   openReserved stops opening and register() drops a stream that was already open: either way the
   reservation is released and no stream is registered. *)

OpenReserved ==
    /\ resv > 0
    /\ resv' = resv - 1
    /\ opened' = IF failSeen \/ gated THEN opened ELSE opened + 1
    /\ WakeRd
    /\ UNCHANGED <<srvV, known, read, enq, more, pflag, rpcV, polV, appV, flagV>>

\* opener.open throws: the remaining reservations are released and the queue fails.
OpenFails ==
    /\ resv > 0
    /\ resv' = resv - 1
    /\ failSeen' = TRUE
    /\ WakeRd
    /\ UNCHANGED <<srvV, known, opened, read, enq, more, pflag, rpcV, polV, app, opCancelled,
                   cancelled, gated, flagV>>

-----------------------------------------------------------------------------
(* The reader: ResultSet.next() inside Q.awaitNext, and the application between calls *)

\* onStreamFinished, the part under the lock: free the slot, reserve pending endpoints for
\* opening, and clear the paused flag if unread endpoints are below the limit. The resume
\* action runs after unlocking (ReaderResume). A failed or closed queue has no pending endpoints
\* (fail() and close() clear them).
ReadFree ==
    LET read1 == read + 1
        pend == IF failSeen \/ gated THEN 0 ELSE known - opened - resv
        free == Max(0, Limit - (opened - read1)
                       - (IF Mutation = "openNotReserved" THEN 0 ELSE resv))
        rsv == IF EagerOpen THEN 0 ELSE Min(pend, free)
        wake == pflag /\ known - read1 < Limit /\ Mutation # "noWake"
    IN /\ read' = read1
       /\ resv' = resv + rsv
       /\ pflag' = IF wake THEN FALSE ELSE pflag
       /\ rd' = IF wake THEN "resume" ELSE "run"

\* awaitNext returns a ready stream (a future with rows) to the application. The stream keeps its
\* slot (openUnread) until the application has read it to the end (AppExhaust). Rows reach the
\* application from here on; later next() calls start with the cancelled and failure checks.
AppTake ==
    /\ app = "reading"
    /\ rd = "run"
    /\ ~held
    /\ Futs - spent > 0
    /\ (Mutation = "readAfterFail" \/ (~failSeen /\ ~cancelled))
    /\ held' = TRUE
    /\ badRow' = (badRow \/ failSeen \/ cancelled)
    /\ idleTried' = FALSE
    /\ UNCHANGED <<srvV, qV, rpcV, polV, rd, spent, appV, badApply, badPoll, polledAtLimit,
                   resent, spun>>

\* R.next: the application's own stream.next() returned false. It frees the slot at once, on the
\* application thread, under the queue lock (Q.endpointExhausted -> onStreamFinished): read and
\* free are one step. The resume action runs after unlocking (ReaderResume). Broken variant
\* lateRelease is the old code: the stream was re-enqueued and the slot was freed only when a
\* later awaitNext consumed its null future, in completion order.
AppExhaust ==
    /\ app = "reading"
    /\ rd = "run"
    /\ held
    /\ held' = FALSE
    /\ IF Mutation = "lateRelease"
         THEN /\ spent' = spent + 1
              /\ UNCHANGED <<read, resv, pflag, rd>>
         ELSE /\ spent' = spent
              /\ ReadFree
    /\ UNCHANGED <<srvV, known, opened, enq, more, rpcV, polV, idleTried, appV, flagV>>

\* Take a null future (a stream that ended inside the pool task without rows) and finish it.
\* awaitNext loops without returning to the application. The code clears idleTried right after
\* onStreamFinished (IdleFix); without it the flag would stay set (the bug fixed in dadd45d8a).
\* In the lateRelease variant the null future may be the one of a stream the application finished.
AppReadEmpty ==
    /\ app = "reading"
    /\ rd = "run"
    /\ ~held
    /\ Futs > 0
    /\ ~failSeen /\ ~cancelled
    /\ \/ spent > 0 /\ spent' = spent - 1
       \/ Futs > spent /\ spent' = spent
    /\ ReadFree
    /\ idleTried' = IF IdleFix THEN FALSE ELSE idleTried
    /\ UNCHANGED <<srvV, known, opened, enq, more, rpcV, polV, held, appV, flagV>>

\* The resume action of onStreamFinished, run on the reader's thread after unlocking:
\* P.resume claims `owned` (true only if the poller gave it up) and resubmits. A rejected
\* submit, 10 times, orphans the poller.
ReaderResume ==
    /\ rd = "resume"
    /\ rd' = "run"
    /\ poller' = IF poller = "paused" THEN "running" ELSE poller
    /\ UNCHANGED <<srvV, qV, rpcV, lastEmpty, waited, sawFull, idleTried, held, spent, appV,
                   flagV>>

\* The resume was rejected: the poller is orphaned, owned stays true, the reader is woken.
ReaderResumeRejected ==
    /\ rd = "resume"
    /\ poller = "paused"
    /\ rd' = "run"
    /\ poller' = "orphaned"
    /\ UNCHANGED <<srvV, qV, rpcV, lastEmpty, waited, sawFull, idleTried, held, spent, appV,
                   flagV>>

\* Nothing is queued and more may come: call the idle hook (P.runInlineIfOrphaned). If the poller
\* is orphaned, it polls on the reader's thread; otherwise the hook returns false and
\* idleTried is set.
AppIdle ==
    /\ app = "reading"
    /\ rd = "run"
    /\ ~held
    /\ ~failSeen /\ ~cancelled
    /\ NoFutures /\ NothingQueued
    /\ more
    /\ ~idleTried
    /\ IF poller = "orphaned" /\ Mutation # "noIdleHook"
         THEN /\ poller' = "running"
              /\ rd' = "inline"
              /\ UNCHANGED idleTried
         ELSE /\ idleTried' = TRUE
              /\ UNCHANGED <<poller, rd>>
    /\ UNCHANGED <<srvV, qV, rpcV, lastEmpty, waited, sawFull, held, spent, appV, flagV>>

\* The inline run ended because the poller was stopped; the idle hook returns true.
InlineExit ==
    /\ rd = "inline"
    /\ poller = "stopped"
    /\ ~inflight
    /\ ~resp.has
    /\ rd' = "run"
    /\ idleTried' = FALSE
    /\ UNCHANGED <<srvV, qV, rpcV, polV, held, spent, appV, flagV>>

\* Futures empty and no endpoint queued or opening, but more may come (or the idle hook said no):
\* wait on the condition, which the lock-protected check makes atomic.
ReaderWait ==
    /\ app = "reading"
    /\ rd = "run"
    /\ ~held
    /\ ~failSeen /\ ~cancelled
    /\ NoFutures
    /\ ~(NothingQueued /\ ~more)
    /\ ~(NothingQueued /\ ~idleTried)
    /\ rd' = "waiting"
    /\ UNCHANGED <<srvV, qV, rpcV, polV, idleTried, held, spent, appV, flagV>>

\* next() returns false: futures empty, pending empty, opening == 0, and !moreExpected,
\* all read under the lock.
AppEnd ==
    /\ app = "reading"
    /\ rd = "run"
    /\ ~held
    /\ ~failSeen
    /\ ~cancelled
    /\ NoFutures
    /\ known = opened + resv
    /\ (resv = 0 \/ Mutation = "endIgnoresOpening")
    /\ (~more \/ Mutation = "endWhenQueueEmpty")
    /\ app' = "ended"
    /\ UNCHANGED <<srvV, qV, rpcV, polV, rdV, failSeen, opCancelled, cancelled, gated, flagV>>

\* next() throws: the top-of-call checks of `cancelled` and queue.failure(), or awaitNext's
\* checkOpen and throwIfFailed. The application may hold a stream while it does.
AppThrow ==
    /\ app = "reading"
    /\ rd = "run"
    /\ failSeen \/ cancelled
    /\ app' = "threw"
    /\ UNCHANGED <<srvV, qV, rpcV, polV, rdV, failSeen, opCancelled, cancelled, gated, flagV>>

\* Statement.cancel, step 0: PollInfoOperation.cancel. An open poll ends with CANCELLED and no
\* new RPC starts. This runs before R.cancel sets `cancelled`, so the poller can record the ended
\* poll as the queue failure "Statement canceled" first.
CancelOp ==
    /\ app = "reading"
    /\ ~opCancelled
    /\ opCancelled' = TRUE
    /\ UNCHANGED <<srvV, qV, rpcV, polV, rdV, app, failSeen, cancelled, gated, flagV>>

\* R.cancel, step 1: the cancelled flag. next() throws from the next call on.
AppCancel ==
    /\ app = "reading"
    /\ ~cancelled
    /\ cancelled' = TRUE
    /\ UNCHANGED <<srvV, qV, rpcV, polV, rdV, app, failSeen, opCancelled, gated, flagV>>

\* R.cancel, step 2: P.stop (stopped = true), O.cancel (an open poll ends with CANCELLED, no new
\* RPC starts), Q.close (wakes the reader).
CancelStop ==
    /\ cancelled
    /\ ~gated
    /\ gated' = TRUE
    /\ opCancelled' = TRUE
    /\ poller' = IF poller \in {"finished", "failed"} \/ Mutation = "cancelIgnoresPoller"
                   THEN poller ELSE "stopped"
    /\ contHeld' = IF Mutation = "cancelIgnoresPoller" THEN contHeld ELSE FALSE
    /\ WakeRd
    /\ UNCHANGED <<srvV, qV, contUsed, inflight, resp, got, lastEmpty, waited, sawFull, app,
                   failSeen, cancelled, flagV>>

\* The application is done with the result; nothing more happens to it.
Done ==
    /\ app \in {"ended", "threw"}
    /\ UNCHANGED vars

Next ==
    \/ SrvPublish \/ SrvComplete \/ SrvFail \/ SrvAnswer
    \/ PollerSend \/ PollerCheck \/ PollerPause \/ PollerPauseEnd \/ PollerSeesFail
    \/ PollerTake \/ PollerCrash \/ PollerApplyBackoff \/ PollerApplyGo \/ PollerApplyHandBack
    \/ PollerApplyFinish \/ BackoffElapses \/ PollerDrop
    \/ OpenReserved \/ OpenFails
    \/ AppTake \/ AppExhaust \/ AppReadEmpty \/ ReaderResume \/ ReaderResumeRejected \/ AppIdle
    \/ InlineExit \/ ReaderWait \/ AppEnd \/ AppThrow \/ CancelOp \/ AppCancel \/ CancelStop
    \/ Done

\* The server eventually finishes; the driver, the cancel call and the application keep going.
\* A started Statement.cancel always reaches R.cancel.
Fairness ==
    /\ WF_vars(SrvComplete) /\ WF_vars(SrvAnswer)
    /\ WF_vars(PollerSend) /\ WF_vars(PollerPause) /\ WF_vars(PollerPauseEnd)
    /\ WF_vars(PollerSeesFail) /\ WF_vars(PollerTake)
    /\ WF_vars(PollerApplyBackoff) /\ WF_vars(PollerApplyGo) /\ WF_vars(PollerApplyHandBack)
    /\ WF_vars(PollerApplyFinish) /\ WF_vars(BackoffElapses) /\ WF_vars(PollerDrop)
    /\ WF_vars(OpenReserved)
    /\ WF_vars(AppTake \/ AppReadEmpty) /\ WF_vars(AppExhaust)
    /\ WF_vars(ReaderResume \/ ReaderResumeRejected)
    /\ WF_vars(AppIdle) /\ WF_vars(InlineExit) /\ WF_vars(ReaderWait)
    /\ WF_vars(AppEnd) /\ WF_vars(AppThrow) /\ WF_vars(AppCancel /\ opCancelled)
    /\ WF_vars(CancelStop)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Rules *)

TypeOK ==
    /\ published \in 1..N
    /\ server \in {"running", "done", "failed"}
    /\ read + (IF held THEN 1 ELSE 0) <= opened /\ opened + resv <= known /\ known <= published
    /\ held \in BOOLEAN /\ spent \in 0..N /\ spent <= Futs
    /\ gated => opCancelled
    /\ enq \in [1..N -> 0..2]
    /\ resp \in Answer /\ got \in Answer
    /\ poller \in {"running", "polling", "received", "backoff", "paused", "pausing",
                   "orphaned", "finished", "failed", "stopped", "dead"}
    /\ rd \in {"run", "resume", "waiting", "inline"}
    /\ app \in {"reading", "ended", "threw"}
    /\ sawFull \in BOOLEAN

\* Only one party runs the poller: an inline run means the poller is on the reader's thread.
InlineMeansPoller ==
    rd = "inline" => poller \in {"running", "polling", "received", "backoff", "stopped"}

\* Each endpoint is queued for download exactly once (append-only suffix).
EachEndpointOnce == \A i \in 1..N : enq[i] <= 1

\* Each continuation descriptor is sent at most once.
ContinuationsOnce == ~resent

\* next() returns false only after every published endpoint was read and the query finished.
NoPrematureEnd == app = "ended" => (server = "done" /\ read = published)

\* No row reaches the application after the driver saw a failure or a cancel.
FailFast == ~badRow

\* No poll is sent after cancel has finished (stopped, operation cancelled, queue closed).
CancelStopsPolling == ~badPoll

\* The poller never polls while queued, unread endpoints are at the limit.
Backpressure == ~polledAtLimit

\* No slot stays held by a stream the application has read to the end while the application reads
\* another one: the poller and the pending endpoints must not wait for a finished stream.
SlotFreedOnExhaust == ~(held /\ spent > 0)

\* A failed queue takes nothing more: addEndpoints queues and opens nothing after a failure.
FailedQueueInert == ~badApply

\* Open, opening and unread DoGet streams never exceed the limit.
OpenWithinLimit == opened + resv - read <= Limit

\* No poll is sent straight after an empty answer.
NoSpin == ~spun

\* Every result eventually ends or throws.
Terminates == <>(app \in {"ended", "threw"})
=============================================================================
