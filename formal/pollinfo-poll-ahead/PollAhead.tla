----------------------------- MODULE PollAhead -----------------------------
(***************************************************************************)
(* Bounded poll-ahead for the Arrow Flight SQL JDBC driver (BDX-645).      *)
(*                                                                         *)
(* One progressive JDBC ResultSet, from the moment executeQuery has        *)
(* returned it: the first PollInfo published endpoint E1 and continuation  *)
(* C1, and C1 has been handed to the poller.                               *)
(*                                                                         *)
(* Parties that run concurrently:                                          *)
(*   - the Flight SQL server, which publishes endpoints and finishes or    *)
(*     fails the query, and answers PollFlightInfo at any moment (a server *)
(*     that does not long-poll may answer at once with nothing new);       *)
(*   - the poller (EndpointPoller), which chains continuations. It runs on *)
(*     a pool thread, or, if the pool rejected a resume, inline on the     *)
(*     reader's thread;                                                    *)
(*   - the reader: the application thread inside ResultSet.next(), which   *)
(*     is inside FlightEndpointDataQueue.awaitNext, reads endpoints, ends  *)
(*     the results, or throws;                                             *)
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
(*   AppRead, AppReadEmpty      Q.awaitNext taking a ready future, or a    *)
(*                              null future -> Q.onStreamFinished: under   *)
(*                              the lock free the slot, reserve pending    *)
(*                              endpoints, clear the paused flag           *)
(*   ReaderResume               the resume action Q.onStreamFinished runs  *)
(*                              AFTER unlocking: P.resume (claim owned,    *)
(*                              resubmit; 10 rejected submits orphan P)    *)
(*   AppIdle, ReaderWait,       Q.awaitNext, empty-futures branch: end,    *)
(*   AppEnd, AppThrow           idle hook (P.runInlineIfOrphaned), or wait *)
(*   InlineExit                 P.runInlineIfOrphaned returning            *)
(*   AppCancel / CancelStop     R.cancel: the cancelled flag first, then   *)
(*                              P.stop + O.cancel + Q.close                *)
(*   PollerSeesFail, PollerDrop P.drive loop top and the check after the   *)
(*                              poll                                       *)
(*                                                                         *)
(* The implementation corresponds to EagerOpen = FALSE (a Q built with     *)
(* maxOpen = pollInfoPrefetch). Answers with no continuation, and          *)
(* pollInfoPrefetch = 0, use an unlimited eager queue and never start a    *)
(* poller; the model does not cover them.                                  *)
(*                                                                         *)
(* Rows are abstracted: reading an endpoint is one step. Endpoint i is the *)
(* i-th endpoint the server publishes; PollInfo is cumulative, so an       *)
(* answer is just how many endpoints exist so far. An endpoint is either   *)
(* empty (no rows: its future yields null) or not.                         *)
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
    app,            \* "reading" | "ended" | "threw"
    failSeen,       \* the queue recorded a failure
    cancelled,      \* ResultSet.cancelled: next() throws from now on
    gated,          \* cancel finished: poller stopped, operation cancelled, queue closed
    badRow,         \* a row was handed to the application after a failure or cancel
    badPoll,        \* a poll was sent after cancel finished
    polledAtLimit,  \* a poll was sent while queued, unread endpoints were at the limit
    resent,         \* a continuation was sent twice
    spun            \* a poll was sent right after an empty answer, without backoff

srvV  == <<published, server>>
qV    == <<known, resv, opened, read, enq, more, pflag>>
rpcV  == <<contHeld, contUsed, inflight, resp, got>>
polV  == <<poller, lastEmpty, waited, sawFull>>
rdV   == <<rd, idleTried>>
appV  == <<app, failSeen, cancelled, gated>>
flagV == <<badRow, badPoll, polledAtLimit, resent, spun>>

vars == <<srvV, qV, rpcV, polV, rdV, appV, flagV>>

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

Answer == [has : BOOLEAN, count : 0..N, more : BOOLEAN, err : BOOLEAN]
NoAnswer == [has |-> FALSE, count |-> 0, more |-> FALSE, err |-> FALSE]

Unread == known - read                  \* pending + open-unread
NoFutures == opened = read              \* every registered stream was finished
NothingQueued == known = opened /\ resv = 0   \* pending empty and opening == 0

\* The reader is woken (signalAll) only if it is waiting; then it clears its idle flag.
WakeRd == /\ rd' = IF rd = "waiting" THEN "run" ELSE rd
          /\ idleTried' = IF rd = "waiting" THEN FALSE ELSE idleTried

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
    /\ app = "reading"
    /\ failSeen = FALSE
    /\ cancelled = FALSE
    /\ gated = FALSE
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
\* Once cancel has cancelled the operation's context, the open poll ends with CANCELLED.
SrvAnswer ==
    /\ inflight
    /\ ~resp.has
    /\ resp' = IF gated \/ server = "failed"
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
    /\ UNCHANGED <<srvV, qV, contHeld, resp, got, lastEmpty, sawFull, rdV, appV, badRow>>

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

\* The poller takes the answer. A failure fails the queue at once (onPollFailed -> Q.fail).
PollerTake ==
    /\ poller = "polling"
    /\ resp.has
    /\ resp' = NoAnswer
    /\ IF resp.err
         THEN /\ poller' = "failed"
              /\ failSeen' = TRUE
              /\ contHeld' = FALSE
              /\ UNCHANGED <<got, more>>
              /\ rd' = IF rd \in {"waiting", "inline"} THEN "run" ELSE rd
              /\ idleTried' = IF rd \in {"waiting", "inline"} THEN FALSE ELSE idleTried
         ELSE /\ got' = resp
              /\ poller' = "received"
              /\ more' = IF Mutation = "finishBeforeEnqueue" /\ ~resp.more THEN FALSE ELSE more
              /\ UNCHANGED <<failSeen, contHeld, rdV>>
    /\ UNCHANGED <<srvV, known, resv, opened, read, enq, pflag, contUsed, inflight, lastEmpty,
                   waited, sawFull, app, cancelled, gated, flagV>>

\* Slots addEndpoints can reserve: fewer than Limit streams open or opening and unread.
\* EagerOpen: every new endpoint takes a slot at once.
ApplyRsv ==
    LET pendNew == got.count - opened - resv
        free == Max(0, Limit - (opened - read)
                       - (IF Mutation = "openNotReserved" THEN 0 ELSE resv))
    IN IF EagerOpen THEN pendNew ELSE Min(pendNew, free)

\* addEndpoints: in one locked step append only the new suffix, set moreExpected, reserve slots
\* (their streams open later, outside the lock), and signal the reader.
ApplyQ ==
    /\ got.has
    /\ poller = "received"
    /\ enq' = [i \in 1..N |->
                 IF i >= (IF Mutation = "enqueueWholeList" THEN 1 ELSE known + 1) /\ i <= got.count
                   THEN Min(enq[i] + 1, 2) ELSE enq[i]]
    /\ known' = got.count
    /\ resv' = resv + ApplyRsv
    /\ more' = got.more
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
    /\ UNCHANGED <<srvV, opened, read, pflag, inflight, resp, waited, sawFull, appV, flagV>>

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
    /\ UNCHANGED <<srvV, opened, read, pflag, inflight, resp, waited, sawFull, appV, flagV>>

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
    /\ UNCHANGED <<srvV, opened, read, pflag, inflight, resp, waited, sawFull, appV, flagV>>

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
                   appV, flagV>>

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
   the model lets any step do it, which only adds behaviors. A stream registered after a failure
   or cancel is closed at once in the code; here it still counts as open, which also only adds
   behaviors. *)

OpenReserved ==
    /\ resv > 0
    /\ resv' = resv - 1
    /\ opened' = opened + 1
    /\ WakeRd
    /\ UNCHANGED <<srvV, known, read, enq, more, pflag, rpcV, polV, appV, flagV>>

\* opener.open throws: the remaining reservations are released and the queue fails.
OpenFails ==
    /\ resv > 0
    /\ resv' = resv - 1
    /\ failSeen' = TRUE
    /\ WakeRd
    /\ UNCHANGED <<srvV, known, opened, read, enq, more, pflag, rpcV, polV, app, cancelled,
                   gated, flagV>>

-----------------------------------------------------------------------------
(* The reader: ResultSet.next() inside Q.awaitNext *)

\* onStreamFinished, the part under the lock: free the slot, reserve pending endpoints for
\* opening, and clear the paused flag if unread endpoints are below the limit. The resume
\* action runs after unlocking (ReaderResume).
ReadFree ==
    LET read1 == read + 1
        pend == known - opened - resv
        free == Max(0, Limit - (opened - read1)
                       - (IF Mutation = "openNotReserved" THEN 0 ELSE resv))
        rsv == IF EagerOpen THEN 0 ELSE Min(pend, free)
        wake == pflag /\ known - read1 < Limit /\ Mutation # "noWake"
    IN /\ read' = read1
       /\ resv' = resv + rsv
       /\ pflag' = IF wake THEN FALSE ELSE pflag
       /\ rd' = IF wake THEN "resume" ELSE "run"

\* Take a ready stream, read it, and finish it: rows reach the application.
AppRead ==
    /\ app = "reading"
    /\ rd = "run"
    /\ opened > read
    /\ (Mutation = "readAfterFail" \/ (~failSeen /\ ~cancelled))
    /\ ReadFree
    /\ badRow' = (badRow \/ failSeen \/ cancelled)
    /\ idleTried' = FALSE
    /\ UNCHANGED <<srvV, known, opened, enq, more, rpcV, polV, app, failSeen, cancelled, gated,
                   badPoll, polledAtLimit, resent, spun>>

\* Take a null future (an endpoint with no rows) and finish it. awaitNext loops without
\* returning to the application. The code clears idleTried right after onStreamFinished
\* (IdleFix); without it the flag would stay set (the bug fixed in dadd45d8a).
AppReadEmpty ==
    /\ app = "reading"
    /\ rd = "run"
    /\ opened > read
    /\ ~failSeen /\ ~cancelled
    /\ ReadFree
    /\ idleTried' = IF IdleFix THEN FALSE ELSE idleTried
    /\ UNCHANGED <<srvV, known, opened, enq, more, rpcV, polV, appV, flagV>>

\* The resume action of onStreamFinished, run on the reader's thread after unlocking:
\* P.resume claims `owned` (true only if the poller gave it up) and resubmits. A rejected
\* submit, 10 times, orphans the poller.
ReaderResume ==
    /\ rd = "resume"
    /\ rd' = "run"
    /\ poller' = IF poller = "paused" THEN "running" ELSE poller
    /\ UNCHANGED <<srvV, qV, rpcV, lastEmpty, waited, sawFull, idleTried, appV, flagV>>

\* The resume was rejected: the poller is orphaned, owned stays true, the reader is woken.
ReaderResumeRejected ==
    /\ rd = "resume"
    /\ poller = "paused"
    /\ rd' = "run"
    /\ poller' = "orphaned"
    /\ UNCHANGED <<srvV, qV, rpcV, lastEmpty, waited, sawFull, idleTried, appV, flagV>>

\* Nothing is queued and more may come: call the idle hook (P.runInlineIfOrphaned). If the poller
\* is orphaned, it polls on the reader's thread; otherwise the hook returns false and
\* idleTried is set.
AppIdle ==
    /\ app = "reading"
    /\ rd = "run"
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
    /\ UNCHANGED <<srvV, qV, rpcV, lastEmpty, waited, sawFull, appV, flagV>>

\* The inline run ended because the poller was stopped; the idle hook returns true.
InlineExit ==
    /\ rd = "inline"
    /\ poller = "stopped"
    /\ ~inflight
    /\ ~resp.has
    /\ rd' = "run"
    /\ idleTried' = FALSE
    /\ UNCHANGED <<srvV, qV, rpcV, polV, appV, flagV>>

\* Futures empty and no endpoint queued or opening, but more may come (or the idle hook said no):
\* wait on the condition, which the lock-protected check makes atomic.
ReaderWait ==
    /\ app = "reading"
    /\ rd = "run"
    /\ ~failSeen /\ ~cancelled
    /\ NoFutures
    /\ ~(NothingQueued /\ ~more)
    /\ ~(NothingQueued /\ ~idleTried)
    /\ rd' = "waiting"
    /\ UNCHANGED <<srvV, qV, rpcV, polV, idleTried, appV, flagV>>

\* next() returns false: futures empty, pending empty, opening == 0, and !moreExpected,
\* all read under the lock.
AppEnd ==
    /\ app = "reading"
    /\ rd = "run"
    /\ ~failSeen
    /\ ~cancelled
    /\ NoFutures
    /\ known = opened + resv
    /\ (resv = 0 \/ Mutation = "endIgnoresOpening")
    /\ (~more \/ Mutation = "endWhenQueueEmpty")
    /\ app' = "ended"
    /\ UNCHANGED <<srvV, qV, rpcV, polV, rdV, failSeen, cancelled, gated, flagV>>

AppThrow ==
    /\ app = "reading"
    /\ rd = "run"
    /\ failSeen \/ cancelled
    /\ app' = "threw"
    /\ UNCHANGED <<srvV, qV, rpcV, polV, rdV, failSeen, cancelled, gated, flagV>>

\* R.cancel, step 1: the cancelled flag. next() throws from the next call on.
AppCancel ==
    /\ app = "reading"
    /\ ~cancelled
    /\ cancelled' = TRUE
    /\ UNCHANGED <<srvV, qV, rpcV, polV, rdV, app, failSeen, gated, flagV>>

\* R.cancel, step 2: P.stop (stopped = true), O.cancel (an open poll ends with CANCELLED, no new
\* RPC starts), Q.close (wakes the reader).
CancelStop ==
    /\ cancelled
    /\ ~gated
    /\ gated' = TRUE
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
    \/ PollerTake \/ PollerApplyBackoff \/ PollerApplyGo \/ PollerApplyHandBack
    \/ PollerApplyFinish \/ BackoffElapses \/ PollerDrop
    \/ OpenReserved \/ OpenFails
    \/ AppRead \/ AppReadEmpty \/ ReaderResume \/ ReaderResumeRejected \/ AppIdle \/ InlineExit
    \/ ReaderWait \/ AppEnd \/ AppThrow \/ AppCancel \/ CancelStop
    \/ Done

\* The server eventually finishes; the driver, the cancel call and the application keep going.
Fairness ==
    /\ WF_vars(SrvComplete) /\ WF_vars(SrvAnswer)
    /\ WF_vars(PollerSend) /\ WF_vars(PollerPause) /\ WF_vars(PollerPauseEnd)
    /\ WF_vars(PollerSeesFail) /\ WF_vars(PollerTake)
    /\ WF_vars(PollerApplyBackoff) /\ WF_vars(PollerApplyGo) /\ WF_vars(PollerApplyHandBack)
    /\ WF_vars(PollerApplyFinish) /\ WF_vars(BackoffElapses) /\ WF_vars(PollerDrop)
    /\ WF_vars(OpenReserved)
    /\ WF_vars(AppRead \/ AppReadEmpty) /\ WF_vars(ReaderResume \/ ReaderResumeRejected)
    /\ WF_vars(AppIdle) /\ WF_vars(InlineExit) /\ WF_vars(ReaderWait)
    /\ WF_vars(AppEnd) /\ WF_vars(AppThrow) /\ WF_vars(CancelStop)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Rules *)

TypeOK ==
    /\ published \in 1..N
    /\ server \in {"running", "done", "failed"}
    /\ read <= opened /\ opened + resv <= known /\ known <= published
    /\ enq \in [1..N -> 0..2]
    /\ resp \in Answer /\ got \in Answer
    /\ poller \in {"running", "polling", "received", "backoff", "paused", "pausing",
                   "orphaned", "finished", "failed", "stopped"}
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

\* Open, opening and unread DoGet streams never exceed the limit.
OpenWithinLimit == opened + resv - read <= Limit

\* No poll is sent straight after an empty answer.
NoSpin == ~spun

\* Every result eventually ends or throws.
Terminates == <>(app \in {"ended", "threw"})
=============================================================================
