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

# The laws of the transaction and savepoint model. The human states
# them; PROOF.bend must prove them. Each law quotes the sentence of
# FlightSql.proto it comes from, or is a sanity check that keeps the
# others from being satisfied vacuously. Laws are stated over
# arbitrary server states; the ones that need the id invariant say so
# with `for w: Inv(...)`, and the two laws at the end show that every
# reachable state satisfies the invariant.

import Base
import ./main.bend as TX

# the truth of a Bool as a type: Unit when True, Empty when False
def T(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

# Invariant
# ---------

# every id in xs is below n and the list is strictly descending, so ids
# are never reissued and never repeated
def desc(xs: List<&2, Nat>, +n: Nat) -> Bool:
  match xs:
    case Nil{}:
      True{}
    case Con{+x, t}:
      Nat.is_lt(x, n) && desc(t, x)

# the same for savepoint ids
def desc_sp(sps: List<&2, TX.Sp>, +n: Nat) -> Bool:
  match sps:
    case Nil{}:
      True{}
    case Con{TX.Sp{+id, tx}, t}:
      Nat.is_lt(id, n) && desc_sp(t, id)

def Inv(txs: List<&2, Nat>, sps: List<&2, TX.Sp>, +next: Nat) -> Type:
  T(desc(txs, next)) & T(desc_sp(sps, next))

def Inv_s(s: TX.Server) -> Type:
  TX.Server{txs, sps, next} = s
  Inv(txs, sps, next)

# whether request a names t as its transaction_id
def refers_tx(a: TX.Act, +t: Nat) -> Bool:
  match a:
    case TX.ABeginTx{}:
      False{}
    case TX.ABeginSp{tx}:
      Nat.is_eq(t, tx)
    case TX.AEndTx{tx, how}:
      Nat.is_eq(t, tx)
    case TX.AEndSp{sp, how}:
      False{}
    case TX.AStmt{TX.SAuto{}}:
      False{}
    case TX.AStmt{TX.STx{tx}}:
      Nat.is_eq(t, tx)

# Ending a transaction
# --------------------

# LAW: "If the action completes successfully, the transaction handle is
# invalidated" (ActionEndTransactionRequest). After EndTransaction on
# t, commit or rollback, every request that names t as its
# transaction_id (BeginSavepoint, EndTransaction, a statement) is an
# error. Holds from any state: if t was not live the EndTransaction
# itself failed and t is still not live.
law ended_tx_rejected:
  for  a    : TX.Act
  for +t    : Nat
  for  how  : TX.EndTx
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  for  u    : T(refers_tx(a, t))
  {TX.resp(TX.step_s(a, TX.state(TX.step(TX.AEndTx{t, how}, txs, sps, next)))) == TX.RErr{} : TX.Resp}

# LAW: "...as are all associated savepoints" (ActionEndTransactionRequest),
# and "If the associated transaction is committed, rolled back, or
# times out, then the savepoint is also invalidated"
# (ActionBeginSavepointResult). After EndTransaction on a live t, any
# EndSavepoint on a savepoint s that belonged to t is an error. Needs
# the invariant: without unique ids another transaction could hold a
# savepoint with the same id.
law ended_tx_kills_savepoints:
  for +t    : Nat
  for +s    : Nat
  for  how  : TX.EndTx
  for  how2 : TX.EndSp
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  for  w    : Inv(txs, sps, next)
  for  live : T(TX.has(t, txs))
  for  own  : {Some{t} == TX.owner(s, sps) : Maybe<&2, Nat>}
  {TX.resp(TX.step_s(TX.AEndSp{s, how2}, TX.state(TX.step(TX.AEndTx{t, how}, txs, sps, next)))) == TX.RErr{} : TX.Resp}

# LAW: the lifecycle does not distinguish commit from rollback: "Commit
# (COMMIT) or rollback (ROLLBACK) the transaction" lead to the same
# server state (what happens to the data is outside this model).
law commit_rollback_same_state:
  for +t    : Nat
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  {TX.state(TX.step(TX.AEndTx{t, TX.TCommit{}}, txs, sps, next)) == TX.state(TX.step(TX.AEndTx{t, TX.TRollback{}}, txs, sps, next)) : TX.Server}

# Beginning a savepoint
# ---------------------

# LAW: "Creates a savepoint within a transaction" / "The transaction to
# which a savepoint belongs" (ActionBeginSavepointRequest). A
# BeginSavepoint naming a transaction that is not live is an error.
law savepoint_needs_live_tx:
  for +t    : Nat
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  for  dead : {False{} == TX.has(t, txs) : Bool}
  {TX.resp(TX.step(TX.ABeginSp{t}, txs, sps, next)) == TX.RErr{} : TX.Resp}

# LAW (anti-vacuity): BeginSavepoint on a live transaction succeeds and
# answers the counter as the savepoint id.
law savepoint_in_live_tx:
  for +t    : Nat
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  for  live : T(TX.has(t, txs))
  {TX.resp(TX.step(TX.ABeginSp{t}, txs, sps, next)) == TX.RId{next} : TX.Resp}

# LAW: the savepoint id a BeginSavepoint returns is bound to the
# transaction it was created in: the server records it as owned by t.
law savepoint_bound_to_tx:
  for +t    : Nat
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  for  live : T(TX.has(t, txs))
  {Some{t} == TX.owner(next, TX.sps_of(TX.state(TX.step(TX.ABeginSp{t}, txs, sps, next)))) : Maybe<&2, Nat>}

# Ending a savepoint
# ------------------

# LAW: EndSavepoint (release or rollback, on a live savepoint or not)
# never changes which transactions are live: "Roll back to a savepoint"
# keeps the transaction open, unlike EndTransaction.
law end_savepoint_keeps_txs:
  for +s    : Nat
  for  how  : TX.EndSp
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  {txs == TX.txs_of(TX.state(TX.step(TX.AEndSp{s, how}, txs, sps, next))) : List<&2, Nat>}

# LAW: "Releasing a savepoint invalidates that savepoint"
# (ActionEndSavepointRequest). After a release of s, any EndSavepoint
# on s is an error. Holds from any state, as for ended_tx_rejected.
law release_kills_savepoint:
  for +s    : Nat
  for  how  : TX.EndSp
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  {TX.resp(TX.step_s(TX.AEndSp{s, how}, TX.state(TX.step(TX.AEndSp{s, TX.SRelease{}}, txs, sps, next)))) == TX.RErr{} : TX.Resp}

# LAW (chosen reading): the proto says release "invalidates that
# savepoint" and nothing about the others, so this model keeps every
# other live savepoint, including the later ones of the same
# transaction. (JDBC's releaseSavepoint and PostgreSQL's RELEASE
# SAVEPOINT also drop the later ones; a server following them would
# violate this law and satisfy the previous one.)
law release_keeps_other_savepoints:
  for +s    : Nat
  for +s2   : Nat
  for +t    : Nat
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  for  ne   : {False{} == Nat.is_eq(s2, s) : Bool}
  for  own  : {Some{t} == TX.owner(s2, sps) : Maybe<&2, Nat>}
  {Some{t} == TX.owner(s2, TX.sps_of(TX.state(TX.step(TX.AEndSp{s, TX.SRelease{}}, txs, sps, next)))) : Maybe<&2, Nat>}

# LAW: "Rolling back to a savepoint does not invalidate the savepoint"
# (ActionEndSavepointRequest). After a rollback to live s, another
# EndSavepoint on s succeeds.
law rollback_keeps_savepoint:
  for +s    : Nat
  for +t    : Nat
  for  how  : TX.EndSp
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  for +own  : {Some{t} == TX.owner(s, sps) : Maybe<&2, Nat>}
  {TX.resp(TX.step_s(TX.AEndSp{s, how}, TX.state(TX.step(TX.AEndSp{s, TX.SRollback{}}, txs, sps, next)))) == TX.RDone{} : TX.Resp}

# LAW: "...but invalidates all savepoints created after the current
# savepoint" (ActionEndSavepointRequest). After a rollback to live s
# of t, any EndSavepoint on a savepoint s2 of t created after s (a
# larger id) is an error. Needs the invariant for unique ids.
law rollback_kills_later_savepoints:
  for +s    : Nat
  for +s2   : Nat
  for +t    : Nat
  for  how  : TX.EndSp
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  for  w    : Inv(txs, sps, next)
  for  own  : {Some{t} == TX.owner(s, sps) : Maybe<&2, Nat>}
  for  own2 : {Some{t} == TX.owner(s2, sps) : Maybe<&2, Nat>}
  for  aft  : T(Nat.is_lt(s, s2))
  {TX.resp(TX.step_s(TX.AEndSp{s2, how}, TX.state(TX.step(TX.AEndSp{s, TX.SRollback{}}, txs, sps, next)))) == TX.RErr{} : TX.Resp}

# LAW: a rollback to s keeps the savepoints of the same transaction
# created before s (a smaller id).
law rollback_keeps_earlier_savepoints:
  for +s    : Nat
  for +s2   : Nat
  for +t    : Nat
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  for  own  : {Some{t} == TX.owner(s, sps) : Maybe<&2, Nat>}
  for  own2 : {Some{t} == TX.owner(s2, sps) : Maybe<&2, Nat>}
  for  bef  : T(Nat.is_lt(s2, s))
  {Some{t} == TX.owner(s2, TX.sps_of(TX.state(TX.step(TX.AEndSp{s, TX.SRollback{}}, txs, sps, next)))) : Maybe<&2, Nat>}

# Statements
# ----------

# LAW: "Include the query as part of this transaction"
# (CommandStatementQuery.transaction_id). A statement whose
# transaction_id is not a live transaction is an error.
law statement_needs_live_tx:
  for +t    : Nat
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  for  dead : {False{} == TX.has(t, txs) : Bool}
  {TX.resp(TX.step(TX.AStmt{TX.STx{t}}, txs, sps, next)) == TX.RErr{} : TX.Resp}

# LAW: "(if unset, the query is auto-committed)"
# (CommandStatementQuery.transaction_id). A statement without a
# transaction_id runs from any state.
law statement_autocommit:
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  {TX.resp(TX.step(TX.AStmt{TX.SAuto{}}, txs, sps, next)) == TX.RDone{} : TX.Resp}

# LAW (anti-vacuity): a statement inside a live transaction runs.
law statement_in_live_tx:
  for +t    : Nat
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  for  live : T(TX.has(t, txs))
  {TX.resp(TX.step(TX.AStmt{TX.STx{t}}, txs, sps, next)) == TX.RDone{} : TX.Resp}

# LAW (anti-vacuity): the id a BeginTransaction returns is usable: a
# statement in it runs, and a savepoint can be created in it.
law fresh_tx_runs_statement:
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  {TX.resp(TX.step_s(TX.AStmt{TX.STx{next}}, TX.state(TX.step(TX.ABeginTx{}, txs, sps, next)))) == TX.RDone{} : TX.Resp}

law fresh_tx_takes_savepoint:
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  {TX.resp(TX.step_s(TX.ABeginSp{next}, TX.state(TX.step(TX.ABeginTx{}, txs, sps, next)))) == TX.RId{1n+next} : TX.Resp}

# Invariant
# ---------

# LAW: the initial state satisfies the invariant
law inv_start:
  Inv_s(TX.start())

# LAW: every request preserves the invariant, so every reachable state
# satisfies it and the laws stated `for w: Inv(...)` cover every trace
law inv_kept:
  for  a    : TX.Act
  for +txs  : List<&2, Nat>
  for +sps  : List<&2, TX.Sp>
  for +next : Nat
  for  w    : Inv(txs, sps, next)
  Inv_s(TX.state(TX.step(a, txs, sps, next)))

# Not modelled
# ------------

# NOT EXPRESSIBLE: "The transaction can be manipulated with the
# EndTransaction action, or automatically via server timeout. If the
# transaction times out, then it is automatically rolled back"
# (ActionBeginTransactionResult). The model has no clock: a Bend law
# ranges over pure terms, and a timeout is an event the server raises
# on its own between requests, not a request. It could be added as an
# explicit ATimeout{tx} request whose effect equals AEndTx{tx,
# TRollback{}}, but that would only restate commit_rollback_same_state,
# not the timing.
# law timeout_rolls_back:
#   for +t : Nat
#   ...

# NOT EXPRESSIBLE: transaction_id and savepoint_id are `bytes` chosen
# by the server ("Opaque handle for the transaction on the server").
# The model issues Nats from one counter, so laws about the handle's
# encoding (length, opacity, that the client does not interpret it)
# are not stated; Bend has no byte or 64-bit integer type to state
# them over. inv_kept covers the one property that matters for the
# lifecycle: an id is never reissued.

# NOT EXPRESSIBLE: "Only supported if FLIGHT_SQL_TRANSACTION is
# FLIGHT_SQL_TRANSACTION_SUPPORT_SAVEPOINT" (ActionBeginSavepointRequest).
# Feature negotiation through GetSqlInfo is a separate RPC with its own
# state; this model assumes a server that supports savepoints.
