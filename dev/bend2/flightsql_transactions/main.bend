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

# A model of the Arrow Flight SQL transaction and savepoint lifecycle
# (FlightSql.proto: ActionBeginTransaction, ActionBeginSavepoint,
# ActionEndTransaction, ActionEndSavepoint, and the optional
# transaction_id on statements), written in Bend 2 so that LAWS.bend
# can state the spec's sentences about it and PROOF.bend can prove them.
#
# Ids for transactions and savepoints are Nats issued by one counter.
# The server keeps the live transaction ids and the live savepoints,
# each tagged with the transaction it belongs to; both lists are
# newest first. The savepoints of one transaction are the entries of
# `sps` tagged with it, so "created after" is "has a larger id".
#
#   bend main.bend         # checks the model and runs the sample trace in main
#   bend PROOF.bend        # checks LAWS.bend against this model

import Base

# Protocol
# --------

# ActionEndTransactionRequest.EndTransaction
type EndTx is Data:
  TCommit{}
  TRollback{}

# ActionEndSavepointRequest.EndSavepoint
type EndSp is Data:
  SRelease{}
  SRollback{}

# the optional transaction_id of CommandStatementQuery,
# CommandStatementUpdate and ActionCreatePreparedStatementRequest
type TxRef is Data:
  SAuto{}        # unset: the statement is auto-committed
  STx{tx: Nat}   # set: the statement runs inside transaction tx

# one client request, by Flight SQL action or command
type Act is Data:
  ABeginTx{}                       # DoAction BeginTransaction
  ABeginSp{tx: Nat}                # DoAction BeginSavepoint{transaction_id}
  AEndTx{tx: Nat, how: EndTx}      # DoAction EndTransaction{transaction_id, action}
  AEndSp{sp: Nat, how: EndSp}      # DoAction EndSavepoint{savepoint_id, action}
  AStmt{ref: TxRef}                # GetFlightInfo / DoPut of a statement

# the server's answer
type Resp is Data:
  RId{id: Nat}    # ActionBeginTransactionResult / ActionBeginSavepointResult
  RDone{}         # completed: no body for End*, a FlightInfo for a statement
  RErr{}          # an error status

# a live savepoint and the transaction it belongs to
type Sp is Data:
  Sp{id: Nat, tx: Nat}

# server state: live transactions, live savepoints, the next id to issue
type Server is Data:
  Server{txs: List<&2, Nat>, sps: List<&2, Sp>, next: Nat}

# List helpers on transaction ids
# -------------------------------

# keep x unless drop
def put(x: Nat, r: List<&2, Nat>, drop: Bool) -> List<&2, Nat>:
  match drop:
    case True{}:
      r
    case False{}:
      x <> r

# xs without every occurrence of h
def remove(+h: Nat, xs: List<&2, Nat>) -> List<&2, Nat>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{+x, t}:
      put(x, remove(h, t), Nat.is_eq(x, h))

# whether h occurs in xs
def has(+h: Nat, xs: List<&2, Nat>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case x <> t:
      Nat.is_eq(x, h) || has(h, t)

# List helpers on savepoints
# --------------------------

# which savepoints an EndTransaction or EndSavepoint invalidates
type Pred is Data:
  OfTx{tx: Nat}               # every savepoint of transaction tx
  AfterIn{sp: Nat, tx: Nat}   # the savepoints of tx created after sp
  IsId{sp: Nat}               # the savepoint sp itself

# whether the savepoint Sp{id, tx} is selected by p
def hit(p: Pred, +id: Nat, +tx: Nat) -> Bool:
  match p:
    case OfTx{t}:
      Nat.is_eq(tx, t)
    case AfterIn{s, t}:
      Nat.is_eq(tx, t) && Nat.is_lt(s, id)
    case IsId{s}:
      Nat.is_eq(id, s)

# keep x unless drop
def put_sp(x: Sp, r: List<&2, Sp>, drop: Bool) -> List<&2, Sp>:
  match drop:
    case True{}:
      r
    case False{}:
      x <> r

# sps without the savepoints selected by p
def cut(+p: Pred, sps: List<&2, Sp>) -> List<&2, Sp>:
  match sps:
    case Nil{}:
      Nil{}
    case Con{Sp{+id, +tx}, t}:
      put_sp(Sp{id, tx}, cut(p, t), hit(p, id, tx))

# the first of two candidates when found
def found(tx: Nat, r: Maybe<&2, Nat>, c: Bool) -> Maybe<&2, Nat>:
  match c:
    case True{}:
      Some{tx}
    case False{}:
      r

# the transaction that live savepoint s belongs to, if s is live
def owner(+s: Nat, sps: List<&2, Sp>) -> Maybe<&2, Nat>:
  match sps:
    case Nil{}:
      None{}
    case Con{Sp{+id, tx}, t}:
      found(tx, owner(s, t), Nat.is_eq(s, id))

# Transitions
# -----------

# BeginSavepoint: inside a live transaction, issue the counter as the
# savepoint id and record it as the newest savepoint of t; else Err
def begin_sp(+t: Nat, txs: List<&2, Nat>, sps: List<&2, Sp>, +next: Nat, ok: Bool) -> Server & Resp:
  match ok:
    case True{}:
      (Server{txs, Sp{next, t} <> sps, 1n+next}, RId{next})
    case False{}:
      (Server{txs, sps, next}, RErr{})

# EndTransaction: on a live transaction, retire it and every savepoint
# it owns; commit and rollback are the same transition of the
# lifecycle (what happens to the data is outside this model); else Err
def end_tx(+t: Nat, txs: List<&2, Nat>, sps: List<&2, Sp>, next: Nat, ok: Bool) -> Server & Resp:
  match ok:
    case True{}:
      (Server{remove(t, txs), cut(OfTx{t}, sps), next}, RDone{})
    case False{}:
      (Server{txs, sps, next}, RErr{})

# what EndSavepoint on savepoint s of transaction t invalidates:
# release invalidates s itself, rollback invalidates the savepoints of
# t created after s and keeps s
def end_sp.go(+s: Nat, how: EndSp, t: Nat, sps: List<&2, Sp>) -> List<&2, Sp>:
  match how:
    case SRelease{}:
      cut(IsId{s}, sps)
    case SRollback{}:
      cut(AfterIn{s, t}, sps)

# EndSavepoint: on a live savepoint, apply how to its transaction's
# savepoints; the transactions are untouched; else Err
def end_sp(+s: Nat, how: EndSp, txs: List<&2, Nat>, sps: List<&2, Sp>, next: Nat, o: Maybe<&2, Nat>) -> Server & Resp:
  match o:
    case None{}:
      (Server{txs, sps, next}, RErr{})
    case Some{t}:
      (Server{txs, end_sp.go(s, how, t, sps), next}, RDone{})

# a statement inside a transaction: runs when the transaction is live
def stmt(txs: List<&2, Nat>, sps: List<&2, Sp>, next: Nat, ok: Bool) -> Server & Resp:
  match ok:
    case True{}:
      (Server{txs, sps, next}, RDone{})
    case False{}:
      (Server{txs, sps, next}, RErr{})

# a statement with or without a transaction_id; auto-commit always runs
def stmt_ref(ref: TxRef, +txs: List<&2, Nat>, sps: List<&2, Sp>, next: Nat) -> Server & Resp:
  match ref:
    case SAuto{}:
      (Server{txs, sps, next}, RDone{})
    case STx{+t}:
      stmt(txs, sps, next, has(t, txs))

# one request against one state
def step(a: Act, +txs: List<&2, Nat>, +sps: List<&2, Sp>, +next: Nat) -> Server & Resp:
  match a:
    case ABeginTx{}:
      (Server{next <> txs, sps, 1n+next}, RId{next})
    case ABeginSp{+t}:
      begin_sp(t, txs, sps, next, has(t, txs))
    case AEndTx{+t, how}:
      end_tx(t, txs, sps, next, has(t, txs))
    case AEndSp{+s, how}:
      end_sp(s, how, txs, sps, next, owner(s, sps))
    case AStmt{ref}:
      stmt_ref(ref, txs, sps, next)

# projections of a step
def state(sr: Server & Resp) -> Server:
  (s, r) = sr
  s

def resp(sr: Server & Resp) -> Resp:
  (s, r) = sr
  r

def txs_of(s: Server) -> List<&2, Nat>:
  Server{txs, sps, next} = s
  txs

def sps_of(s: Server) -> List<&2, Sp>:
  Server{txs, sps, next} = s
  sps

# a request against a whole server
def step_s(a: Act, s: Server) -> Server & Resp:
  Server{txs, sps, next} = s
  step(a, txs, sps, next)

def start() -> Server:
  Server{Nil{}, Nil{}, 0n}

# a whole trace, collecting every response (newest first); the step
# result arrives as a parameter, since a computed pair cannot be
# destructured in place
def run.go(sr: Server & Resp, acc: List<&2, Resp>, rec: Server -> List<&2, Resp> -> List<&2, Resp>) -> List<&2, Resp>:
  (s, r) = sr
  rec(s, r <> acc)

def run(acts: List<Act>) -> Server -> List<&2, Resp> -> List<&2, Resp>:
  match acts:
    case Nil{}:
      s => acc => acc
    case a <> rest:
      s => acc => run.go(step_s(a, s), acc, run(rest))

# the responses of a trace from the initial state, in order
def replay(acts: List<Act>) -> List<&2, Resp>:
  List.reverse(&2, Resp, run(acts)(start(), Nil{}))

# Show
# ----

def Resp.show(r: Resp) -> String:
  match r:
    case RId{id}:
      "Id(" ++ Nat.show(id) ++ ")"
    case RDone{}:
      "Done"
    case RErr{}:
      "Err"

# The trace: begin transaction 0; savepoints 1 and 2 in it; roll back
# to 1 (retires 2); release 2: Err; a statement in 0: Done; commit 0;
# a statement in 0: Err; release 1: Err; an auto-commit statement: Done.
#   [Id(0), Id(1), Id(2), Done, Err, Done, Done, Err, Err, Done]
def main() -> IO(Unit):
  IO.print(List.show(~&2, ~Resp, ~Resp.show, replay([
    ABeginTx{},
    ABeginSp{0n},
    ABeginSp{0n},
    AEndSp{1n, SRollback{}},
    AEndSp{2n, SRelease{}},
    AStmt{STx{0n}},
    AEndTx{0n, TCommit{}},
    AStmt{STx{0n}},
    AEndSp{1n, SRelease{}},
    AStmt{SAuto{}}])))
