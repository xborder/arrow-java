----------------------------- MODULE PollAhead -----------------------------
(***************************************************************************)
(* Bounded poll-ahead for the Arrow Flight SQL JDBC driver (BDX-645).      *)
(*                                                                         *)
(* One progressive JDBC ResultSet, from the moment executeQuery has        *)
(* returned it: the first PollInfo published endpoint E1 and continuation  *)
(* C1, and C1 has been handed to the poller.                               *)
(*                                                                         *)
(* Three parties run concurrently:                                         *)
(*   - the Flight SQL server, which publishes endpoints and finishes or    *)
(*     fails the query, and answers PollFlightInfo at any moment (a server *)
(*     that does not long-poll may answer at once with nothing new);       *)
(*   - the poller (proposed EndpointPoller), which chains continuations;   *)
(*   - the application thread inside ResultSet.next(), which reads         *)
(*     endpoints, ends the results, or throws.                             *)
(*                                                                         *)
(* Code map (sync commit in .claude/skills/verify-jdbc-poll-ahead/SKILL.md):*)
(*   PollerSend / PollerTake    PollInfoOperation.poll / pollNextAvailable *)
(*   PollerApply                PollInfoOperation.remember +               *)
(*                              ArrowFlightJdbcFlightStreamResultSet       *)
(*                              .enqueueNewEndpoints (append-only suffix)  *)
(*   AppRead                    ArrowFlightJdbcFlightStreamResultSet.next  *)
(*                              + FlightEndpointDataQueue.next             *)
(*   AppEnd                     next() returning false                     *)
(*   AppCancel                  Statement.cancel / deadline / close        *)
(*                              -> PollInfoOperation.cancel                *)
(*   PollerPause, BackoffElapses, the wake in AppRead, and running the     *)
(*   poll loop off the application thread are the proposal; they have no   *)
(*   code yet (the POC polls inside next() when the queue is empty).       *)
(*                                                                         *)
(* Rows are abstracted: reading an endpoint is one step. Endpoint i is the *)
(* i-th endpoint the server publishes; PollInfo is cumulative, so an       *)
(* answer is just how many endpoints exist so far.                         *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    N,          \* most endpoints the query can publish
    Limit,      \* pollInfoPrefetch: the poller polls only while open, unread endpoints < Limit
    EagerOpen,  \* TRUE: a DoGet stream opens as soon as its endpoint is enqueued (the proposal as written)
    Mutation    \* "none", or the name of a deliberately broken variant (see run.sh)

VARIABLES
    published,      \* endpoints the server has published
    server,         \* "running" | "done" | "failed"
    known,          \* endpoints enqueued in FlightEndpointDataQueue (cumulative prefix)
    opened,         \* endpoints whose DoGet stream has been opened
    read,           \* endpoints the application has fully read
    enq,            \* enq[i]: how many times endpoint i was enqueued (capped at 2)
    contHeld,       \* the driver holds a continuation descriptor
    contUsed,       \* the held continuation has already been sent
    inflight,       \* a PollFlightInfo RPC is open
    resp,           \* the server's answer, not yet taken by the poller
    got,            \* the answer the poller took, not yet applied
    poller,         \* "running" | "polling" | "received" | "backoff" | "paused" | "finished" | "failed" | "stopped"
    lastEmpty,      \* the last applied answer added no endpoints while the query was running
    waited,         \* a backoff has elapsed since that empty answer
    app,            \* "reading" | "ended" | "threw"
    failSeen,       \* the driver has seen the query fail
    cancelled,      \* Statement.cancel(), the deadline, or close() happened
    badRow,         \* a row was handed to the application after a failure or cancel
    badPoll,        \* a poll was sent after cancel
    polledAtLimit,  \* a poll was sent while open, unread endpoints were at the limit
    resent,         \* a continuation was sent twice
    spun,           \* a poll was sent right after an empty answer, without backoff
    sawFull         \* pauseNotAtomic only: the poller saw the limit reached and has not paused yet

vars == <<published, server, known, opened, read, enq, contHeld, contUsed,
          inflight, resp, got, poller, lastEmpty, waited, app, failSeen,
          cancelled, badRow, badPoll, polledAtLimit, resent, spun, sawFull>>

Min(a, b) == IF a < b THEN a ELSE b

Answer == [has : BOOLEAN, count : 0..N, more : BOOLEAN, err : BOOLEAN]
NoAnswer == [has |-> FALSE, count |-> 0, more |-> FALSE, err |-> FALSE]

Unread == known - read

-----------------------------------------------------------------------------
Init ==
    /\ published = 1
    /\ server = "running"
    /\ known = 1
    /\ opened = 1
    /\ read = 0
    /\ enq = [i \in 1..N |-> IF i = 1 THEN 1 ELSE 0]
    /\ contHeld = TRUE
    /\ contUsed = FALSE
    /\ inflight = FALSE
    /\ resp = NoAnswer
    /\ got = NoAnswer
    /\ poller = "running"
    /\ lastEmpty = FALSE
    /\ waited = FALSE
    /\ app = "reading"
    /\ failSeen = FALSE
    /\ cancelled = FALSE
    /\ badRow = FALSE
    /\ badPoll = FALSE
    /\ polledAtLimit = FALSE
    /\ resent = FALSE
    /\ spun = FALSE
    /\ sawFull = FALSE

-----------------------------------------------------------------------------
(* Server *)

SrvPublish ==
    /\ server = "running"
    /\ published < N
    /\ published' = published + 1
    /\ UNCHANGED <<server, known, opened, read, enq, contHeld, contUsed, inflight,
                   resp, got, poller, lastEmpty, waited, app, failSeen, cancelled,
                   badRow, badPoll, polledAtLimit, resent, spun, sawFull>>

SrvComplete ==
    /\ server = "running"
    /\ server' = "done"
    /\ UNCHANGED <<published, known, opened, read, enq, contHeld, contUsed, inflight,
                   resp, got, poller, lastEmpty, waited, app, failSeen, cancelled,
                   badRow, badPoll, polledAtLimit, resent, spun, sawFull>>

SrvFail ==
    /\ server = "running"
    /\ server' = "failed"
    /\ UNCHANGED <<published, known, opened, read, enq, contHeld, contUsed, inflight,
                   resp, got, poller, lastEmpty, waited, app, failSeen, cancelled,
                   badRow, badPoll, polledAtLimit, resent, spun, sawFull>>

\* The server answers the open poll. It may answer at once with nothing new.
SrvAnswer ==
    /\ inflight
    /\ ~resp.has
    /\ resp' = IF cancelled \/ server = "failed"
                 THEN [has |-> TRUE, count |-> 0, more |-> FALSE, err |-> TRUE]
                 ELSE [has |-> TRUE, count |-> published, more |-> server = "running", err |-> FALSE]
    /\ inflight' = FALSE
    /\ UNCHANGED <<published, server, known, opened, read, enq, contHeld, contUsed,
                   got, poller, lastEmpty, waited, app, failSeen, cancelled,
                   badRow, badPoll, polledAtLimit, resent, spun, sawFull>>

-----------------------------------------------------------------------------
(* Poller (proposed EndpointPoller, on its own pool) *)

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
    /\ badPoll' = (badPoll \/ cancelled)
    /\ polledAtLimit' = (polledAtLimit \/ Unread >= Limit)
    /\ waited' = FALSE
    /\ UNCHANGED <<published, server, known, opened, read, enq, contHeld, resp, got,
                   lastEmpty, app, failSeen, cancelled, badRow, sawFull>>

\* At the limit the poller pauses and gives its thread back.
PollerPause ==
    /\ poller = "running"
    /\ contHeld
    /\ Mutation # "noLimit"
    /\ IF Mutation = "pauseNotAtomic" THEN sawFull ELSE Unread >= Limit
    /\ poller' = "paused"
    /\ sawFull' = FALSE
    /\ UNCHANGED <<published, server, known, opened, read, enq, contHeld, contUsed,
                   inflight, resp, got, lastEmpty, waited, app, failSeen, cancelled,
                   badRow, badPoll, polledAtLimit, resent, spun>>

\* Broken variant only: the poller reads the count, and pauses in a later step.
PollerCheck ==
    /\ Mutation = "pauseNotAtomic"
    /\ poller = "running"
    /\ contHeld
    /\ ~sawFull
    /\ Unread >= Limit
    /\ sawFull' = TRUE
    /\ UNCHANGED <<published, server, known, opened, read, enq, contHeld, contUsed,
                   inflight, resp, got, poller, lastEmpty, waited, app, failSeen, cancelled,
                   badRow, badPoll, polledAtLimit, resent, spun>>

\* The poller takes the answer. A failure fails the execution at once.
PollerTake ==
    /\ poller = "polling"
    /\ resp.has
    /\ resp' = NoAnswer
    /\ IF resp.err
         THEN /\ poller' = "failed"
              /\ failSeen' = TRUE
              /\ contHeld' = FALSE
              /\ UNCHANGED got
         ELSE /\ got' = resp
              /\ poller' = IF Mutation = "finishBeforeEnqueue" /\ ~resp.more
                             THEN "finished" ELSE "received"
              /\ UNCHANGED <<failSeen, contHeld, sawFull>>
    /\ UNCHANGED <<published, server, known, opened, read, enq, contUsed, inflight,
                   lastEmpty, waited, app, cancelled, badRow, badPoll, polledAtLimit,
                   resent, spun, sawFull>>

\* Apply a successful answer: enqueue only the appended endpoints, take the
\* next continuation, and mark the poller finished in the same step when the
\* query is complete.
PollerApply ==
    /\ got.has
    /\ poller \in {"received", "finished"}
    /\ LET new  == got.count
           from == IF Mutation = "enqueueWholeList" THEN 1 ELSE known + 1
           empty == new = known
       IN
       /\ enq' = [i \in 1..N |-> IF i >= from /\ i <= new THEN Min(enq[i] + 1, 2) ELSE enq[i]]
       /\ known' = new
       /\ opened' = IF EagerOpen THEN new ELSE Min(new, read + Limit)
       /\ got' = NoAnswer
       /\ IF got.more
            THEN /\ contHeld' = TRUE
                 /\ contUsed' = (Mutation = "reuseContinuation" /\ empty)
                 /\ lastEmpty' = empty
                 /\ poller' = IF empty /\ Mutation # "noBackoff" THEN "backoff" ELSE "running"
            ELSE /\ contHeld' = FALSE
                 /\ lastEmpty' = FALSE
                 /\ poller' = "finished"
                 /\ UNCHANGED contUsed
    /\ UNCHANGED <<published, server, read, inflight, resp, waited, app, failSeen,
                   cancelled, badRow, badPoll, polledAtLimit, resent, spun, sawFull>>

\* 100 ms doubling up to pollInfoMaxBackoff; time is abstracted to one step.
BackoffElapses ==
    /\ poller = "backoff"
    /\ poller' = "running"
    /\ waited' = TRUE
    /\ UNCHANGED <<published, server, known, opened, read, enq, contHeld, contUsed,
                   inflight, resp, got, lastEmpty, app, failSeen, cancelled,
                   badRow, badPoll, polledAtLimit, resent, spun, sawFull>>

\* A stopped poller discards the answer to the poll that cancel interrupted.
PollerDrop ==
    /\ poller = "stopped"
    /\ resp.has
    /\ resp' = NoAnswer
    /\ UNCHANGED <<published, server, known, opened, read, enq, contHeld, contUsed,
                   inflight, got, poller, lastEmpty, waited, app, failSeen, cancelled,
                   badRow, badPoll, polledAtLimit, resent, spun, sawFull>>

-----------------------------------------------------------------------------
(* Application thread inside ResultSet.next() *)

\* Read one opened endpoint. Finishing it may wake a paused poller.
AppRead ==
    /\ app = "reading"
    /\ opened > read
    /\ Mutation = "readAfterFail" \/ (~failSeen /\ ~cancelled)
    /\ read' = read + 1
    /\ badRow' = (badRow \/ failSeen \/ cancelled)
    /\ opened' = IF EagerOpen THEN opened ELSE Min(known, read + 1 + Limit)
    /\ poller' = IF poller = "paused" /\ known - (read + 1) < Limit /\ Mutation # "noWake"
                   THEN "running" ELSE poller
    /\ UNCHANGED <<published, server, known, enq, contHeld, contUsed, inflight, resp,
                   got, lastEmpty, waited, app, failSeen, cancelled, badPoll,
                   polledAtLimit, resent, spun, sawFull>>

\* next() returns false only when the queue is empty and the poller finished.
AppEnd ==
    /\ app = "reading"
    /\ ~failSeen
    /\ ~cancelled
    /\ read = known
    /\ Mutation = "endWhenQueueEmpty" \/ poller = "finished"
    /\ app' = "ended"
    /\ UNCHANGED <<published, server, known, opened, read, enq, contHeld, contUsed,
                   inflight, resp, got, poller, lastEmpty, waited, failSeen, cancelled,
                   badRow, badPoll, polledAtLimit, resent, spun, sawFull>>

AppThrow ==
    /\ app = "reading"
    /\ failSeen \/ cancelled
    /\ app' = "threw"
    /\ UNCHANGED <<published, server, known, opened, read, enq, contHeld, contUsed,
                   inflight, resp, got, poller, lastEmpty, waited, failSeen, cancelled,
                   badRow, badPoll, polledAtLimit, resent, spun, sawFull>>

\* Cancel stops the poller; its open poll, if any, ends with CANCELLED.
AppCancel ==
    /\ app = "reading"
    /\ ~cancelled
    /\ cancelled' = TRUE
    /\ poller' = IF poller \in {"finished", "failed"} \/ Mutation = "cancelIgnoresPoller"
                   THEN poller ELSE "stopped"
    /\ contHeld' = IF Mutation = "cancelIgnoresPoller" THEN contHeld ELSE FALSE
    /\ UNCHANGED <<published, server, known, opened, read, enq, contUsed, inflight,
                   resp, got, lastEmpty, waited, app, failSeen, badRow, badPoll,
                   polledAtLimit, resent, spun, sawFull>>

\* The application is done with the result; nothing more happens to it.
Done ==
    /\ app \in {"ended", "threw"}
    /\ UNCHANGED vars

Next ==
    \/ SrvPublish \/ SrvComplete \/ SrvFail \/ SrvAnswer
    \/ PollerSend \/ PollerCheck \/ PollerPause \/ PollerTake \/ PollerApply \/ BackoffElapses \/ PollerDrop
    \/ AppRead \/ AppEnd \/ AppThrow \/ AppCancel
    \/ Done

\* The server eventually finishes; the driver and the application keep going.
Fairness ==
    /\ WF_vars(SrvComplete) /\ WF_vars(SrvAnswer)
    /\ WF_vars(PollerSend) /\ WF_vars(PollerPause) /\ WF_vars(PollerTake)
    /\ WF_vars(PollerApply) /\ WF_vars(BackoffElapses) /\ WF_vars(PollerDrop)
    /\ WF_vars(AppRead) /\ WF_vars(AppEnd) /\ WF_vars(AppThrow)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Rules *)

TypeOK ==
    /\ published \in 1..N
    /\ server \in {"running", "done", "failed"}
    /\ read <= opened /\ opened <= known /\ known <= published
    /\ enq \in [1..N -> 0..2]
    /\ resp \in Answer /\ got \in Answer
    /\ poller \in {"running", "polling", "received", "backoff", "paused", "finished", "failed", "stopped"}
    /\ app \in {"reading", "ended", "threw"}
    /\ sawFull \in BOOLEAN

\* Each endpoint is queued for download exactly once (append-only suffix).
EachEndpointOnce == \A i \in 1..N : enq[i] <= 1

\* Each continuation descriptor is sent at most once.
ContinuationsOnce == ~resent

\* next() returns false only after every published endpoint was read and the query finished.
NoPrematureEnd == app = "ended" => (server = "done" /\ read = published)

\* No row reaches the application after the driver saw a failure or a cancel.
FailFast == ~badRow

\* No poll is sent after cancel.
CancelStopsPolling == ~badPoll

\* The poller never polls while unread endpoints are at the limit.
Backpressure == ~polledAtLimit

\* Open, unread DoGet streams never exceed the limit.
OpenWithinLimit == opened - read <= Limit

\* No poll is sent straight after an empty answer.
NoSpin == ~spun

\* Every result eventually ends or throws.
Terminates == <>(app \in {"ended", "threw"})
=============================================================================
