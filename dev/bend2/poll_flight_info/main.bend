# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# A model of the Arrow Flight long-running query protocol: the
# PollFlightInfo RPC, FlightEndpoint expiration, and the
# RenewFlightEndpoint and CancelFlightInfo actions (Flight.proto,
# docs/source/format/Flight.rst). Written in Bend 2 so that LAWS.bend
# can state the protocol's "must" and "should" sentences and
# PROOF.bend can prove the model obeys them.
#
# Abstractions (Bend has no U64, no F64 and no timestamps):
#   - time is a Nat tick that advances on an explicit ATick request;
#   - progress is a Nat out of FULL() = 100 instead of a double in [0, 1];
#   - the poll descriptor of a query is its id, as PollFlightInfoProducer
#     encodes it into the command bytes; a ticket is (query id, index).
#
#   bend main.bend         # checks the model and runs the sample trace in main
#   bend PROOF.bend        # checks LAWS.bend against this model

import Base

# Constants
# ---------

# progress is reported out of FULL; a tick of work adds STEP
def FULL() -> Nat:
  100n

def STEP() -> Nat:
  25n

# how many ticks a poll descriptor stays valid after a poll
def PTTL() -> Nat:
  3n

# how many ticks an endpoint stays valid after it is produced
def ETTL() -> Nat:
  2n

# Protocol
# --------

# CancelStatus of CancelFlightInfoResult. The model answers CANCELLED
# and NOT_CANCELLABLE; CANCELLING needs asynchronous cancellation and
# UNSPECIFIED is discouraged by the proto ("servers should avoid").
type CancelSt is Data:
  CsUnspecified{}
  CsCancelled{}
  CsCancelling{}
  CsNotCancellable{}

# one client request or server-side event, by Flight RPC / action
type Act is Data:
  AStart{}                 # PollFlightInfo with an original descriptor: starts a query
  APoll{q: Nat}            # PollFlightInfo with PollInfo.flight_descriptor q
  ACancel{q: Nat}          # DoAction CancelFlightInfo on the query's FlightInfo
  ADoGet{q: Nat, tk: Nat}  # DoGet with the ticket (q, tk)
  ARenew{q: Nat, tk: Nat}  # DoAction RenewFlightEndpoint on endpoint (q, tk)
  ATick{}                  # the clock advances one tick; running queries make progress
  AFail{q: Nat}            # the query's execution fails inside the server

# the server's answer
type Resp is Data:
  RPoll{progress: Nat, desc: Maybe<&2, Nat>, info: List<&2, Nat>}  # PollInfo
  RCancel{status: CancelSt}                                        # CancelFlightInfoResult
  RData{}                                                          # a DoGet stream
  REndpoint{tk: Nat, exp: Nat}                                     # the renewed FlightEndpoint
  RAck{}                                                           # a server-side event
  RErr{}                                                           # an error status

# State
# -----

# an endpoint of a query's FlightInfo: its ticket index and expiry tick
type Endpoint is Data:
  Endpoint{tk: Nat, exp: Nat}

type QState is Data:
  QRunning{progress: Nat}
  QDone{}
  QCancelled{}
  QFailed{}

# the mutable part of a query: state, poll-descriptor expiry, endpoints
# in creation order
type QBody is Data:
  QBody{st: QState, pexp: Nat, eps: List<&2, Endpoint>}

type Query is Data:
  Query{id: Nat, body: QBody}

# the queries, the next id to issue, the clock
type Server is Data:
  Server{qs: List<&2, Query>, next: Nat, now: Nat}

# List helpers
# ------------

def pick_q(c: Bool, x: QBody, rest: Maybe<&2, QBody>) -> Maybe<&2, QBody>:
  match c:
    case True{}:
      Some{x}
    case False{}:
      rest

# the body of the first query with id q
def find(+q: Nat, qs: List<&2, Query>) -> Maybe<&2, QBody>:
  match qs:
    case Nil{}:
      None{}
    case Con{Query{+id, body}, t}:
      pick_q(Nat.is_eq(q, id), body, find(q, t))

def pick_n(c: Bool, x: Nat, rest: Maybe<&2, Nat>) -> Maybe<&2, Nat>:
  match c:
    case True{}:
      Some{x}
    case False{}:
      rest

# the expiry of the first endpoint with ticket tk
def find_ep(+tk: Nat, eps: List<&2, Endpoint>) -> Maybe<&2, Nat>:
  match eps:
    case Nil{}:
      None{}
    case Con{Endpoint{+etk, exp}, t}:
      pick_n(Nat.is_eq(tk, etk), exp, find_ep(tk, t))

# the tickets of the endpoints, in order: the info of a PollInfo
def tickets(eps: List<&2, Endpoint>) -> List<&2, Nat>:
  match eps:
    case Nil{}:
      Nil{}
    case Con{Endpoint{tk, exp}, t}:
      tk <> tickets(t)

def snoc(eps: List<&2, Endpoint>, e: Endpoint) -> List<&2, Endpoint>:
  match eps:
    case Nil{}:
      [e]
    case Con{h, t}:
      h <> snoc(t, e)

def length(eps: List<&2, Endpoint>) -> Nat:
  match eps:
    case Nil{}:
      0n
    case Con{h, t}:
      1n+length(t)

# Per-query transitions
# ---------------------

# a tick of work: advance progress, or complete when it would reach FULL
def advance(p: Nat, more: Bool) -> QState:
  match more:
    case True{}:
      QRunning{p}
    case False{}:
      QDone{}

def tick_st(st: QState) -> QState:
  match st:
    case QRunning{+p}:
      advance(Nat.add(p, STEP()), Nat.is_lt(Nat.add(p, STEP()), FULL()))
    case QDone{}:
      QDone{}
    case QCancelled{}:
      QCancelled{}
    case QFailed{}:
      QFailed{}

# a running query also produces one more endpoint per tick, valid for ETTL
def tick_eps(st: QState, +now: Nat, +eps: List<&2, Endpoint>) -> List<&2, Endpoint>:
  match st:
    case QRunning{p}:
      snoc(eps, Endpoint{length(eps), Nat.add(ETTL(), now)})
    case QDone{}:
      eps
    case QCancelled{}:
      eps
    case QFailed{}:
      eps

def tick_body(+now: Nat, body: QBody) -> QBody:
  QBody{+st, pexp, eps} = body
  QBody{tick_st(st), pexp, tick_eps(st, now, eps)}

# a successful poll keeps the descriptor alive for another PTTL
def touch_body(now: Nat, body: QBody) -> QBody:
  QBody{st, pexp, eps} = body
  QBody{st, Nat.add(PTTL(), now), eps}

# CancelFlightInfo: a running or complete query (and its results) is
# cancelled; a failed one has nothing left to cancel
def cancel_st(st: QState) -> QState:
  match st:
    case QRunning{p}:
      QCancelled{}
    case QDone{}:
      QCancelled{}
    case QCancelled{}:
      QCancelled{}
    case QFailed{}:
      QFailed{}

def cancel_body(body: QBody) -> QBody:
  QBody{st, pexp, eps} = body
  QBody{cancel_st(st), pexp, eps}

def cancel_status(st: QState) -> CancelSt:
  match st:
    case QRunning{p}:
      CsCancelled{}
    case QDone{}:
      CsCancelled{}
    case QCancelled{}:
      CsCancelled{}
    case QFailed{}:
      CsNotCancellable{}

# only a running query can fail
def fail_st(st: QState) -> QState:
  match st:
    case QRunning{p}:
      QFailed{}
    case QDone{}:
      QDone{}
    case QCancelled{}:
      QCancelled{}
    case QFailed{}:
      QFailed{}

def fail_body(body: QBody) -> QBody:
  QBody{st, pexp, eps} = body
  QBody{fail_st(st), pexp, eps}

# RenewFlightEndpoint: the new expiry is strictly later than the old
# one and at least a full ETTL from now
def renewed(+exp: Nat, now: Nat) -> Nat:
  1n+Nat.max(exp, Nat.add(ETTL(), now))

def when_n(c: Bool, a: Nat, b: Nat) -> Nat:
  match c:
    case True{}:
      a
    case False{}:
      b

def renew_ep(+tk: Nat, +now: Nat, eps: List<&2, Endpoint>) -> List<&2, Endpoint>:
  match eps:
    case Nil{}:
      Nil{}
    case Con{Endpoint{+etk, +exp}, t}:
      Endpoint{etk, when_n(Nat.is_eq(tk, etk), renewed(exp, now), exp)} <> renew_ep(tk, now, t)

def renew_body(tk: Nat, now: Nat, body: QBody) -> QBody:
  QBody{st, pexp, eps} = body
  QBody{st, pexp, renew_ep(tk, now, eps)}

# Updates over the query list
# ---------------------------

# a state change, applied to every query (OpTick) or to the query with
# the given id (the others)
type Op is Data:
  OpTick{}
  OpTouch{q: Nat}
  OpCancel{q: Nat}
  OpFail{q: Nat}
  OpRenew{q: Nat, tk: Nat}

def when_b(c: Bool, a: QBody, b: QBody) -> QBody:
  match c:
    case True{}:
      a
    case False{}:
      b

# the new body of the query with id `id`
def apply(op: Op, +now: Nat, +id: Nat, +body: QBody) -> QBody:
  match op:
    case OpTick{}:
      tick_body(now, body)
    case OpTouch{q}:
      when_b(Nat.is_eq(q, id), touch_body(now, body), body)
    case OpCancel{q}:
      when_b(Nat.is_eq(q, id), cancel_body(body), body)
    case OpFail{q}:
      when_b(Nat.is_eq(q, id), fail_body(body), body)
    case OpRenew{q, tk}:
      when_b(Nat.is_eq(q, id), renew_body(tk, now, body), body)

def upd(+op: Op, +now: Nat, qs: List<&2, Query>) -> List<&2, Query>:
  match qs:
    case Nil{}:
      Nil{}
    case Con{Query{+id, body}, t}:
      Query{id, apply(op, now, id, body)} <> upd(op, now, t)

# Requests
# --------

# PollFlightInfo with a poll descriptor. A running query answers its
# progress, its descriptor and the endpoints so far, unless the
# descriptor expired, in which case the query is cancelled ("the query
# may be cancelled"). A complete query answers the complete FlightInfo
# with the descriptor unset. A cancelled or failed query is an error.
def poll_run(+q: Nat, +qs: List<&2, Query>, +next: Nat, +now: Nat, p: Nat, eps: List<&2, Endpoint>, alive: Bool) -> Server & Resp:
  match alive:
    case True{}:
      (Server{upd(OpTouch{q}, now, qs), next, now}, RPoll{p, Some{q}, tickets(eps)})
    case False{}:
      (Server{upd(OpCancel{q}, now, qs), next, now}, RErr{})

def poll.fin(+q: Nat, +qs: List<&2, Query>, +next: Nat, +now: Nat, m: Maybe<&2, QBody>) -> Server & Resp:
  match m:
    case None{}:
      (Server{qs, next, now}, RErr{})
    case Some{QBody{st, pexp, eps}}:
      match st:
        case QRunning{p}:
          poll_run(q, qs, next, now, p, eps, Nat.is_lt(now, pexp))
        case QDone{}:
          (Server{qs, next, now}, RPoll{FULL(), None{}, tickets(eps)})
        case QCancelled{}:
          (Server{qs, next, now}, RErr{})
        case QFailed{}:
          (Server{qs, next, now}, RErr{})

def poll(+q: Nat, +qs: List<&2, Query>, +next: Nat, +now: Nat) -> Server & Resp:
  poll.fin(q, qs, next, now, find(q, qs))

# CancelFlightInfo: NOT_FOUND on an unknown query, else the status of
# the query's state, and the query becomes cancelled
def cancel.fin(+q: Nat, +qs: List<&2, Query>, +next: Nat, +now: Nat, m: Maybe<&2, QBody>) -> Server & Resp:
  match m:
    case None{}:
      (Server{qs, next, now}, RErr{})
    case Some{QBody{st, pexp, eps}}:
      (Server{upd(OpCancel{q}, now, qs), next, now}, RCancel{cancel_status(st)})

def cancel(+q: Nat, +qs: List<&2, Query>, +next: Nat, +now: Nat) -> Server & Resp:
  cancel.fin(q, qs, next, now, find(q, qs))

# DoGet: the endpoint of a running or complete query streams while
# now < expiration; a cancelled or failed query's endpoints are gone
def doget_ep(valid: Bool) -> Resp:
  match valid:
    case True{}:
      RData{}
    case False{}:
      RErr{}

def doget_run(+now: Nat, e: Maybe<&2, Nat>) -> Resp:
  match e:
    case None{}:
      RErr{}
    case Some{exp}:
      doget_ep(Nat.is_lt(now, exp))

def doget.fin(+tk: Nat, +now: Nat, m: Maybe<&2, QBody>) -> Resp:
  match m:
    case None{}:
      RErr{}
    case Some{QBody{st, pexp, eps}}:
      match st:
        case QRunning{p}:
          doget_run(now, find_ep(tk, eps))
        case QDone{}:
          doget_run(now, find_ep(tk, eps))
        case QCancelled{}:
          RErr{}
        case QFailed{}:
          RErr{}

def doget(+q: Nat, +tk: Nat, +qs: List<&2, Query>, +next: Nat, +now: Nat) -> Server & Resp:
  (Server{qs, next, now}, doget.fin(tk, now, find(q, qs)))

# RenewFlightEndpoint: a still-valid endpoint of a running or complete
# query gets a later expiry; an expired endpoint, or one of a cancelled
# or failed query, is an error
def renew_ep_r(+q: Nat, +tk: Nat, +qs: List<&2, Query>, +next: Nat, +now: Nat, exp: Nat, valid: Bool) -> Server & Resp:
  match valid:
    case True{}:
      (Server{upd(OpRenew{q, tk}, now, qs), next, now}, REndpoint{tk, renewed(exp, now)})
    case False{}:
      (Server{qs, next, now}, RErr{})

def renew_run(+q: Nat, +tk: Nat, +qs: List<&2, Query>, +next: Nat, +now: Nat, e: Maybe<&2, Nat>) -> Server & Resp:
  match e:
    case None{}:
      (Server{qs, next, now}, RErr{})
    case Some{+exp}:
      renew_ep_r(q, tk, qs, next, now, exp, Nat.is_lt(now, exp))

def renew.fin(+q: Nat, +tk: Nat, +qs: List<&2, Query>, +next: Nat, +now: Nat, m: Maybe<&2, QBody>) -> Server & Resp:
  match m:
    case None{}:
      (Server{qs, next, now}, RErr{})
    case Some{QBody{st, pexp, eps}}:
      match st:
        case QRunning{p}:
          renew_run(q, tk, qs, next, now, find_ep(tk, eps))
        case QDone{}:
          renew_run(q, tk, qs, next, now, find_ep(tk, eps))
        case QCancelled{}:
          (Server{qs, next, now}, RErr{})
        case QFailed{}:
          (Server{qs, next, now}, RErr{})

def renew(+q: Nat, +tk: Nat, +qs: List<&2, Query>, +next: Nat, +now: Nat) -> Server & Resp:
  renew.fin(q, tk, qs, next, now, find(q, qs))

# a new query: running, no progress, no endpoints, descriptor valid for PTTL
def fresh(now: Nat) -> QBody:
  QBody{QRunning{0n}, Nat.add(PTTL(), now), Nil{}}

# one request against one state
def step(a: Act, +qs: List<&2, Query>, +next: Nat, +now: Nat) -> Server & Resp:
  match a:
    case AStart{}:
      (Server{Query{next, fresh(now)} <> qs, 1n+next, now}, RPoll{0n, Some{next}, Nil{}})
    case APoll{q}:
      poll(q, qs, next, now)
    case ACancel{q}:
      cancel(q, qs, next, now)
    case ADoGet{q, tk}:
      doget(q, tk, qs, next, now)
    case ARenew{q, tk}:
      renew(q, tk, qs, next, now)
    case ATick{}:
      (Server{upd(OpTick{}, 1n+now, qs), next, 1n+now}, RAck{})
    case AFail{q}:
      (Server{upd(OpFail{q}, now, qs), next, now}, RAck{})

# projections of a step
def state(sr: Server & Resp) -> Server:
  (s, r) = sr
  s

def resp(sr: Server & Resp) -> Resp:
  (s, r) = sr
  r

# a request against a whole server
def step_s(a: Act, s: Server) -> Server & Resp:
  Server{qs, next, now} = s
  step(a, qs, next, now)

def start() -> Server:
  Server{Nil{}, 0n, 0n}

# a whole trace, threading the state and the last response
def run(acts: List<Act>, sr: Server & Resp) -> Server & Resp:
  match acts:
    case Nil{}:
      sr
    case a <> rest:
      (s, last) = sr
      run(rest, step_s(a, s))

def replay(acts: List<Act>) -> Server & Resp:
  run(acts, (start(), RErr{}))

# Show
# ----

def CancelSt.show(c: CancelSt) -> String:
  match c:
    case CsUnspecified{}:
      "UNSPECIFIED"
    case CsCancelled{}:
      "CANCELLED"
    case CsCancelling{}:
      "CANCELLING"
    case CsNotCancellable{}:
      "NOT_CANCELLABLE"

def Desc.show(d: Maybe<&2, Nat>) -> String:
  match d:
    case None{}:
      "unset"
    case Some{q}:
      Nat.show(q)

def Info.show(ts: List<&2, Nat>) -> String:
  match ts:
    case Nil{}:
      ""
    case Con{t, rest}:
      Nat.show(t) ++ " " ++ Info.show(rest)

def Resp.show(r: Resp) -> String:
  match r:
    case RPoll{p, d, info}:
      "PollInfo(progress=" ++ Nat.show(p) ++ "/100, descriptor=" ++ Desc.show(d) ++ ", tickets=[" ++ Info.show(info) ++ "])"
    case RCancel{c}:
      "CancelFlightInfoResult(" ++ CancelSt.show(c) ++ ")"
    case RData{}:
      "Data"
    case REndpoint{tk, exp}:
      "FlightEndpoint(ticket=" ++ Nat.show(tk) ++ ", expires=" ++ Nat.show(exp) ++ ")"
    case RAck{}:
      "Ack"
    case RErr{}:
      "Err"

# print the response to each request of a trace, in order
def show_trace(acts: List<Act>, +s: Server) -> IO(Unit):
  match acts:
    case Nil{}:
      IO.pure(Unit, Unit{})
    case Con{+a, rest}:
      do IO<Unit>:
        IO.print(Resp.show(resp(step_s(a, s))))
        show_trace(rest, state(step_s(a, s)))

# start a query (id 0); poll; tick; poll (progress 25, ticket 0);
# DoGet ticket 0 before its expiry; renew it; tick twice past the
# original expiry; DoGet again (renewed, so still valid); cancel;
# DoGet after cancel (Err); cancel again (CANCELLED); poll (Err)
def main() -> IO(Unit):
  show_trace([AStart{}, APoll{0n}, ATick{}, APoll{0n}, ADoGet{0n, 0n}, ARenew{0n, 0n},
              ATick{}, ATick{}, ADoGet{0n, 0n}, ACancel{0n}, ADoGet{0n, 0n}, ACancel{0n}, APoll{0n}],
             start())
