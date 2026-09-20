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

# The laws of the PollFlightInfo model. The human states them;
# PROOF.bend must prove them. Each law quotes the sentence of
# Flight.proto or docs/source/format/Flight.rst it comes from, or is a
# sanity check that keeps the others from being satisfied vacuously.
# Laws the model cannot express or the session could not prove are
# kept here commented out, with the reason.

import Base
import ./main.bend as PF

# Predicates
# ----------

# the truth of a Bool as a type: Unit when True, Empty when False
def T(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

# a running query's progress is within FULL
def st_ok(st: PF.QState) -> Bool:
  match st:
    case PF.QRunning{p}:
      Nat.is_le(p, PF.FULL())
    case PF.QDone{}:
      True{}
    case PF.QCancelled{}:
      True{}
    case PF.QFailed{}:
      True{}

# every query id is below the counter (so no id is ever reissued) and
# every progress is within FULL
def all_ok(qs: List<&2, PF.Query>, +next: Nat) -> Bool:
  match qs:
    case Nil{}:
      True{}
    case Con{PF.Query{id, PF.QBody{st, pexp, eps}}, t}:
      (Nat.is_lt(id, next) && st_ok(st)) && all_ok(t, next)

def Inv(qs: List<&2, PF.Query>, +next: Nat) -> Data:
  T(all_ok(qs, next))

def Inv_s(s: PF.Server) -> Data:
  PF.Server{qs, next, now} = s
  Inv(qs, next)

def queries(s: PF.Server) -> List<&2, PF.Query>:
  PF.Server{qs, next, now} = s
  qs

# observations of a looked-up query
def mq_running(m: Maybe<&2, PF.QBody>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{PF.QBody{st, pexp, eps}}:
      match st:
        case PF.QRunning{p}:
          True{}
        case PF.QDone{}:
          False{}
        case PF.QCancelled{}:
          False{}
        case PF.QFailed{}:
          False{}

def mq_done(m: Maybe<&2, PF.QBody>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{PF.QBody{st, pexp, eps}}:
      match st:
        case PF.QRunning{p}:
          False{}
        case PF.QDone{}:
          True{}
        case PF.QCancelled{}:
          False{}
        case PF.QFailed{}:
          False{}

def mq_cancelled(m: Maybe<&2, PF.QBody>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{PF.QBody{st, pexp, eps}}:
      match st:
        case PF.QRunning{p}:
          False{}
        case PF.QDone{}:
          False{}
        case PF.QCancelled{}:
          True{}
        case PF.QFailed{}:
          False{}

def mq_failed(m: Maybe<&2, PF.QBody>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{PF.QBody{st, pexp, eps}}:
      match st:
        case PF.QRunning{p}:
          False{}
        case PF.QDone{}:
          False{}
        case PF.QCancelled{}:
          False{}
        case PF.QFailed{}:
          True{}

# a running query whose poll descriptor has expired
def poll_expired(+now: Nat, m: Maybe<&2, PF.QBody>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{PF.QBody{st, pexp, eps}}:
      match st:
        case PF.QRunning{p}:
          Bool.not(Nat.is_lt(now, pexp))
        case PF.QDone{}:
          False{}
        case PF.QCancelled{}:
          False{}
        case PF.QFailed{}:
          False{}

# the endpoints of a looked-up query, as tickets
def tickets_of(m: Maybe<&2, PF.QBody>) -> List<&2, Nat>:
  match m:
    case None{}:
      Nil{}
    case Some{PF.QBody{st, pexp, eps}}:
      PF.tickets(eps)

# the expiry of endpoint tk of a looked-up query
def exp_of(+tk: Nat, m: Maybe<&2, PF.QBody>) -> Maybe<&2, Nat>:
  match m:
    case None{}:
      None{}
    case Some{PF.QBody{st, pexp, eps}}:
      PF.find_ep(tk, eps)

# whether the query's results are still served: running or complete
def st_live(st: PF.QState) -> Bool:
  match st:
    case PF.QRunning{p}:
      True{}
    case PF.QDone{}:
      True{}
    case PF.QCancelled{}:
      False{}
    case PF.QFailed{}:
      False{}

def ep_before(+now: Nat, e: Maybe<&2, Nat>) -> Bool:
  match e:
    case None{}:
      False{}
    case Some{exp}:
      Nat.is_lt(now, exp)

def ep_after(+now: Nat, e: Maybe<&2, Nat>) -> Bool:
  match e:
    case None{}:
      False{}
    case Some{exp}:
      Bool.not(Nat.is_lt(now, exp))

# endpoint tk of a live query exists and now < expiration
def ep_valid(+now: Nat, +tk: Nat, m: Maybe<&2, PF.QBody>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{PF.QBody{st, pexp, eps}}:
      st_live(st) && ep_before(now, PF.find_ep(tk, eps))

# endpoint tk of a live query exists and its expiration has passed
def ep_expired(+now: Nat, +tk: Nat, m: Maybe<&2, PF.QBody>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{PF.QBody{st, pexp, eps}}:
      st_live(st) && ep_after(now, PF.find_ep(tk, eps))

# observations of responses
def prog_ok(r: PF.Resp) -> Bool:
  match r:
    case PF.RPoll{p, d, info}:
      Nat.is_le(p, PF.FULL())
    case _:
      True{}

# xs is a prefix of ys
def is_prefix(xs: List<&2, Nat>, ys: List<&2, Nat>) -> Bool:
  match xs ys:
    case Nil{} _:
      True{}
    case Con{x, xt} Nil{}:
      False{}
    case Con{x, xt} Con{y, yt}:
      Nat.is_eq(x, y) && is_prefix(xt, yt)

# a later PollInfo reports at least the progress and extends the
# endpoints of an earlier one; vacuous unless both are PollInfos
def Resp.mono(r1: PF.Resp, r2: PF.Resp) -> Bool:
  match r1 r2:
    case PF.RPoll{p1, d1, i1} PF.RPoll{p2, d2, i2}:
      Nat.is_le(p1, p2) && is_prefix(i1, i2)
    case _ _:
      True{}

# the renewed endpoint's expiry is strictly later than the old one
def ren_later(old: Maybe<&2, Nat>, r: PF.Resp) -> Bool:
  match old r:
    case Some{exp} PF.REndpoint{tk, exp2}:
      Nat.is_lt(exp, exp2)
    case _ _:
      False{}

# Invariant
# ---------

# LAW: the initial state satisfies the invariant
law inv_start:
  Inv_s(PF.start())

# LAW: every request keeps the invariant: ids stay below the counter
# and progress stays within FULL
law inv_kept:
  for  a    : PF.Act
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : Inv(qs, next)
  Inv_s(PF.state(PF.step(a, qs, next, now)))

# Progress
# --------

# LAW (Flight.proto, PollInfo.progress): "If known, must be in
# [0.0, 1.0]". In the Nat model: a PollInfo's progress is at most FULL.
law progress_bounded:
  for +q    : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : Inv(qs, next)
  T(prog_ok(PF.resp(PF.poll(q, qs, next, now))))

# LAW (Flight.proto, PollInfo.info): "Subsequent PollInfo responses may
# only append new endpoints to info." And, stronger than the proto,
# which says progress "need not be monotonic or nondecreasing", this
# server's progress never decreases: polling a query, letting any one
# request happen, and polling it again yields at least the progress and
# a superlist of the endpoints of the first poll.
law poll_monotone:
  for  a    : PF.Act
  for +q    : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : Inv(qs, next)
  T(Resp.mono(PF.resp(PF.poll(q, qs, next, now)), PF.resp(PF.step_s(PF.APoll{q}, PF.state(PF.step(a, qs, next, now))))))

# Descriptors
# -----------

# LAW (Flight.rst): "The client should use the descriptor (not the
# original FlightDescriptor) to call the next PollFlightInfo()." A poll
# descriptor the server never issued (an id at or above the counter) is
# an error. Re-sending the original descriptor is AStart, which starts a
# new query rather than polling the old one.
law unknown_descriptor_rejected:
  for +q    : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : Inv(qs, next)
  for  u    : T(Nat.is_le(next, q))
  {PF.resp(PF.poll(q, qs, next, now)) == PF.RErr{} : PF.Resp}

# LAW (Flight.proto, PollFlightInfo): "A client can't use
# PollInfo.flight_descriptor after PollInfo.expiration_time passes. A
# server might not accept the retry descriptor anymore and the query
# may be cancelled." Polling an expired descriptor is an error, and the
# query is cancelled afterwards.
law expired_descriptor_rejected:
  for +q    : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : T(poll_expired(now, PF.find(q, qs)))
  {PF.resp(PF.poll(q, qs, next, now)) == PF.RErr{} : PF.Resp} & T(mq_cancelled(PF.find(q, queries(PF.state(PF.poll(q, qs, next, now))))))

# LAW (Flight.proto, PollInfo.flight_descriptor): "If unset, the query
# is complete." and PollInfo.info: "If 'flight_descriptor' is not
# specified, the query is complete and 'info' specifies all results."
# Polling a complete query answers progress FULL, no descriptor and
# every endpoint of the query, and leaves the server unchanged.
law done_poll_complete:
  for +q    : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : T(mq_done(PF.find(q, qs)))
  {PF.poll(q, qs, next, now) == (PF.Server{qs, next, now}, PF.RPoll{PF.FULL(), None{}, tickets_of(PF.find(q, qs))}) : PF.Server & PF.Resp}

# LAW (anti-vacuity): the descriptor a start returns polls
# successfully, with progress 0 and no endpoints yet.
law fresh_query_polls:
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  {PF.resp(PF.step_s(PF.APoll{next}, PF.state(PF.step(PF.AStart{}, qs, next, now)))) == PF.RPoll{0n, Some{next}, Nil{}} : PF.Resp}

# Cancellation
# ------------

# LAW (Flight.proto, CancelStatus): "CANCEL_STATUS_CANCELLED = 1; The
# cancellation request is complete." Cancelling a running query answers
# CANCELLED and the query is cancelled.
law cancel_running:
  for +q    : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : T(mq_running(PF.find(q, qs)))
  {PF.resp(PF.cancel(q, qs, next, now)) == PF.RCancel{PF.CsCancelled{}} : PF.Resp} & T(mq_cancelled(PF.find(q, queries(PF.state(PF.cancel(q, qs, next, now))))))

# LAW (Flight.proto, PollFlightInfo): "A client may use the
# CancelFlightInfo action with PollInfo.info to cancel the running
# query." Once cancelled, polling the query is an error, from any state.
law cancelled_poll_rejected:
  for +q    : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  {PF.resp(PF.step_s(PF.APoll{q}, PF.state(PF.cancel(q, qs, next, now)))) == PF.RErr{} : PF.Resp}

# LAW (ExpirationTimeProducer, Java integration test): "The client can't
# read data from endpoints even within 6 seconds after the action."
# Once cancelled, DoGet on any of the query's endpoints is an error.
law cancelled_doget_rejected:
  for +q    : Nat
  for +tk   : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  {PF.resp(PF.step_s(PF.ADoGet{q, tk}, PF.state(PF.cancel(q, qs, next, now)))) == PF.RErr{} : PF.Resp}

# LAW (Flight.proto, CancelStatus): "Subsequent requests with the same
# payload may return CANCELLED or a NOT_FOUND error." This server
# repeats its first answer: cancelling twice answers the same as once.
law cancel_idempotent:
  for +q    : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  {PF.resp(PF.step_s(PF.ACancel{q}, PF.state(PF.cancel(q, qs, next, now)))) == PF.resp(PF.cancel(q, qs, next, now)) : PF.Resp}

# LAW (Flight.proto, CancelStatus): "CANCEL_STATUS_NOT_CANCELLABLE = 3;
# The query is not cancellable." A failed query has nothing left to
# cancel and answers NOT_CANCELLABLE.
law failed_not_cancellable:
  for +q    : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : T(mq_failed(PF.find(q, qs)))
  {PF.resp(PF.cancel(q, qs, next, now)) == PF.RCancel{PF.CsNotCancellable{}} : PF.Resp}

# LAW (Flight.rst): "A server should return an error status instead of a
# response if the query fails." Polling a query after it failed is an
# error.
law failed_poll_rejected:
  for +q    : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : T(mq_running(PF.find(q, qs)))
  {PF.resp(PF.step_s(PF.APoll{q}, PF.state(PF.step(PF.AFail{q}, qs, next, now)))) == PF.RErr{} : PF.Resp}

# Endpoint expiration
# -------------------

# LAW (Flight.rst): "If an endpoint has expiration time, the client can
# get data multiple times by DoGet until the expiration time is
# reached." Before the expiry tick, DoGet streams (and, since DoGet does
# not change the state, does so again on the next call).
law doget_before_expiry:
  for +q    : Nat
  for +tk   : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : T(ep_valid(now, tk, PF.find(q, qs)))
  {PF.resp(PF.doget(q, tk, qs, next, now)) == PF.RData{} : PF.Resp}

# LAW (Flight.rst, same sentence): once the expiration time is reached,
# DoGet is an error.
law doget_after_expiry:
  for +q    : Nat
  for +tk   : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : T(ep_expired(now, tk, PF.find(q, qs)))
  {PF.resp(PF.doget(q, tk, qs, next, now)) == PF.RErr{} : PF.Resp}

# LAW (Flight.rst): "the client may be able to extend the expiration
# time by RenewFlightEndpoint action", and the Java integration test:
# "Renewed FlightEndpoint must have newer expiration time". Renewing a
# valid endpoint answers an endpoint whose expiry is strictly later.
law renew_extends:
  for +q    : Nat
  for +tk   : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : T(ep_valid(now, tk, PF.find(q, qs)))
  T(ren_later(exp_of(tk, PF.find(q, qs)), PF.resp(PF.renew(q, tk, qs, next, now))))

# LAW (model decision): an endpoint whose expiration has passed cannot
# be renewed; the server may already have released its data.
law renew_expired_rejected:
  for +q    : Nat
  for +tk   : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : T(ep_expired(now, tk, PF.find(q, qs)))
  {PF.resp(PF.renew(q, tk, qs, next, now)) == PF.RErr{} : PF.Resp}

# LAW (ExpirationTimeProducer): "The client can read data from endpoints
# multiple times within more 10 seconds after the action." After a
# renewal, DoGet on the renewed endpoint streams.
law renewed_doget_ok:
  for +q    : Nat
  for +tk   : Nat
  for +qs   : List<&2, PF.Query>
  for +next : Nat
  for +now  : Nat
  for  w    : T(ep_valid(now, tk, PF.find(q, qs)))
  {PF.resp(PF.step_s(PF.ADoGet{q, tk}, PF.state(PF.renew(q, tk, qs, next, now)))) == PF.RData{} : PF.Resp}

# Sample traces
# -------------

# LAW (anti-vacuity, closed trace): start, poll, tick, poll, then DoGet
# on the fresh endpoint before its expiry streams data.
law trace_doget_fresh:
  {PF.resp(PF.replay([PF.AStart{}, PF.APoll{0n}, PF.ATick{}, PF.APoll{0n}, PF.ADoGet{0n, 0n}])) == PF.RData{} : PF.Resp}

# LAW (closed trace): the same endpoint, ETTL ticks later, is expired.
law trace_doget_expired:
  {PF.resp(PF.replay([PF.AStart{}, PF.APoll{0n}, PF.ATick{}, PF.APoll{0n}, PF.ATick{}, PF.ATick{}, PF.ADoGet{0n, 0n}])) == PF.RErr{} : PF.Resp}

# LAW (closed trace): four ticks complete the query; the poll then has
# no descriptor and lists the four endpoints.
law trace_completes:
  {PF.resp(PF.replay([PF.AStart{}, PF.ATick{}, PF.APoll{0n}, PF.ATick{}, PF.APoll{0n}, PF.ATick{}, PF.APoll{0n}, PF.ATick{}, PF.APoll{0n}])) == PF.RPoll{PF.FULL(), None{}, [0n, 1n, 2n, 3n]} : PF.Resp}

# Not modelled
# ------------

# NOT EXPRESSIBLE: Flight.proto, PollFlightInfo: "A server should not
# respond until the result would be different from last time." This is
# a timing (liveness) property of when the RPC returns, not of what it
# returns. The model is a pure function from state and request to
# response; it has no notion of blocking, so the law cannot be stated.
#
# NOT EXPRESSIBLE: Flight.proto, PollFlightInfo: "The first
# PollFlightInfo call should return as quickly as possible." Same
# reason: there is no time-to-respond in the model.
#
# NOT EXPRESSIBLE: Flight.proto, PollInfo.progress: "optional double
# progress = 3; ... must be in [0.0, 1.0]". Bend has no F64 and its F32
# is axiomatic (nothing about floats can be proven), so progress is a
# Nat out of FULL() = 100; progress_bounded is that law's Nat form.
#
# NOT EXPRESSIBLE: expiration_time as a google.protobuf.Timestamp
# (int64 seconds + int32 nanos) against wall-clock time. Bend has no
# 64-bit integers and no clock the checker can reason about, so time is
# a Nat tick advanced by the ATick request.
#
# NOT PROVEN: the trace form of poll_monotone ("for any list of requests
# between two polls"). It follows from poll_monotone by induction once
# two more invariants are proven: an issued id is never forgotten, and
# QCancelled / QFailed are absorbing (so an error between two PollInfos
# cannot happen). Both are true of the model but were not proven in
# this session; the one-step law with an arbitrary intermediate request
# is what is checked.
#
# law poll_monotone_trace:
#   for  acts : List<PF.Act>
#   for +q    : Nat
#   for +qs   : List<&2, PF.Query>
#   for +next : Nat
#   for +now  : Nat
#   for  w    : Inv(qs, next)
#   T(Resp.mono(PF.resp(PF.poll(q, qs, next, now)), PF.resp(PF.step_s(PF.APoll{q}, PF.state(PF.run(acts, (PF.Server{qs, next, now}, PF.RErr{})))))))
