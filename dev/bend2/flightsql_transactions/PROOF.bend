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

# The proofs. Imports the model (as TX) and the claims (as Laws) and
# fills every law they state; `bend PROOF.bend` is the whole check.
#
# Conventions, as in dev/bend2/prepared_statement/PROOF.bend: a lemma
# whose step case matches on a computed Boolean has a `.fin` half that
# takes the verdict c as a parameter together with an equation about
# it (the "inspect" idiom); the induction hypothesis arrives as `rec`.

import Base
import ./main.bend as TX
import ./LAWS.bend as Laws

# Proof kit
# ---------

# T(True{}) from an equation
def true_T(b: Bool, e: {True{} == b : Bool}) -> Laws.T(b):
  %e : Laws.T(_)
  Unit{}

# an equation from T(b)
def T_true(b: Bool, w: Laws.T(b)) -> {True{} == b : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      Empty.absurd({True{} == False{} : Bool}, w)

def disc(b: Bool) -> Type:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

# True and False clash
def true_ne_false(e: {True{} == False{} : Bool}) -> Empty:
  %e : disc(_)
  Unit{}

# T(c && b) splits into T(c) and T(b); c is a parameter so it can be matched
def split.fin(c: Bool, -b: Bool, w: Laws.T(c && b), -P: Type, k: Laws.T(c) -> Laws.T(b) -> P) -> P:
  match c:
    case True{}:
      k(Unit{}, w)
    case False{}:
      Empty.absurd(P, w)

# T(c) and T(b) join into T(c && b)
def join.fin(c: Bool, -b: Bool, wc: Laws.T(c), wb: Laws.T(b)) -> Laws.T(c && b):
  match c:
    case True{}:
      wb
    case False{}:
      Empty.absurd(Laws.T(False{} && b), wc)

# Some{a} == Some{b} gives a == b
def some_inj(a: Nat, b: Nat, e: {Some{a} == Some{b} : Maybe<&2, Nat>}) -> {a == b : Nat}:
  Equal.cong(Maybe<&2, Nat>, Nat, m => Maybe.default(&2, Nat, m, 0n), Some{a}, Some{b}, e)

# Lemmas on Nat
# -------------

# a nat equals itself
def eq_refl(n: Nat) -> {True{} == Nat.is_eq(n, n) : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      eq_refl(p)

# a true Nat.is_eq is an equality
def eq_sound(x: Nat, h: Nat, e: {True{} == Nat.is_eq(x, h) : Bool}) -> {x == h : Nat}:
  match x h:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      Empty.absurd({0n == 1n+q : Nat}, true_ne_false(e))
    case 1n+p 0n:
      Empty.absurd({1n+p == 0n : Nat}, true_ne_false(e))
    case 1n+p 1n+q:
      %eq_sound(p, q, e) : {1n+p == 1n+_ : Nat}
      {==}

# n < n + 1
def lt_succ(n: Nat) -> Laws.T(Nat.is_lt(n, 1n+n)):
  match n:
    case 0n:
      Unit{}
    case 1n+p:
      lt_succ(p)

# x < n implies x < n + 1
def lt_mono(x: Nat, n: Nat, w: Laws.T(Nat.is_lt(x, n))) -> Laws.T(Nat.is_lt(x, 1n+n)):
  match x n:
    case 0n 0n:
      Empty.absurd(Laws.T(Nat.is_lt(0n, 1n)), w)
    case 0n 1n+q:
      Unit{}
    case 1n+p 0n:
      Empty.absurd(Laws.T(Nat.is_lt(1n+p, 1n)), w)
    case 1n+p 1n+q:
      lt_mono(p, q, w)

# a < b and b < c give a < c
def lt_trans(a: Nat, b: Nat, c: Nat, ab: Laws.T(Nat.is_lt(a, b)), bc: Laws.T(Nat.is_lt(b, c))) -> Laws.T(Nat.is_lt(a, c)):
  match a b c:
    case 0n 0n c0:
      Empty.absurd(Laws.T(Nat.is_lt(0n, c0)), ab)
    case 0n 1n+q 0n:
      Empty.absurd(Laws.T(Nat.is_lt(0n, 0n)), bc)
    case 0n 1n+q 1n+r:
      Unit{}
    case 1n+p 0n c0:
      Empty.absurd(Laws.T(Nat.is_lt(1n+p, c0)), ab)
    case 1n+p 1n+q 0n:
      Empty.absurd(Laws.T(Nat.is_lt(1n+p, 0n)), bc)
    case 1n+p 1n+q 1n+r:
      lt_trans(p, q, r, ab, bc)

# n < n never holds
def lt_irrefl(n: Nat) -> {False{} == Nat.is_lt(n, n) : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      lt_irrefl(p)

# a < b rules out b < a
def lt_asym(a: Nat, b: Nat, w: Laws.T(Nat.is_lt(a, b))) -> {False{} == Nat.is_lt(b, a) : Bool}:
  match a b:
    case 0n 0n:
      Empty.absurd({False{} == Nat.is_lt(0n, 0n) : Bool}, w)
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      Empty.absurd({False{} == Nat.is_lt(0n, 1n+p) : Bool}, w)
    case 1n+p 1n+q:
      lt_asym(p, q, w)

# a < b rules out a == b (in either argument order of is_eq)
def lt_ne(a: Nat, b: Nat, w: Laws.T(Nat.is_lt(a, b))) -> {False{} == Nat.is_eq(a, b) : Bool}:
  match a b:
    case 0n 0n:
      Empty.absurd({False{} == Nat.is_eq(0n, 0n) : Bool}, w)
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      Empty.absurd({False{} == Nat.is_eq(1n+p, 0n) : Bool}, w)
    case 1n+p 1n+q:
      lt_ne(p, q, w)

def lt_ne_r(a: Nat, b: Nat, w: Laws.T(Nat.is_lt(a, b))) -> {False{} == Nat.is_eq(b, a) : Bool}:
  match a b:
    case 0n 0n:
      Empty.absurd({False{} == Nat.is_eq(0n, 0n) : Bool}, w)
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      Empty.absurd({False{} == Nat.is_eq(0n, 1n+p) : Bool}, w)
    case 1n+p 1n+q:
      lt_ne_r(p, q, w)

# Lemmas on has and remove
# ------------------------

# removing h leaves no h: the step case, over the verdict c of
# Nat.is_eq(x, h)
def has_remove.fin(+h: Nat, +x: Nat, -t: List<&2, Nat>,
  rec: {False{} == TX.has(h, TX.remove(h, t)) : Bool},
  c: Bool, e: {c == Nat.is_eq(x, h) : Bool})
  -> {False{} == TX.has(h, TX.put(x, TX.remove(h, t), c)) : Bool}:
  match c:
    case True{}:
      rec
    case False{}:
      %e : {False{} == (_ || TX.has(h, TX.remove(h, t))) : Bool}
      rec

def has_remove(+h: Nat, xs: List<&2, Nat>) -> {False{} == TX.has(h, TX.remove(h, xs)) : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{+x, t}:
      has_remove.fin(h, x, t, has_remove(h, t), Nat.is_eq(x, h), {==})

# Statements and savepoint creation, from any state
# -------------------------------------------------

def Laws.statement_autocommit(txs, sps, next):
  {==}

def Laws.statement_needs_live_tx(t, txs, sps, next, dead):
  %dead : {TX.resp(TX.stmt(txs, sps, next, _)) == TX.RErr{} : TX.Resp}
  {==}

def Laws.statement_in_live_tx(t, txs, sps, next, live):
  %T_true(TX.has(t, txs), live) : {TX.resp(TX.stmt(txs, sps, next, _)) == TX.RDone{} : TX.Resp}
  {==}

def Laws.savepoint_needs_live_tx(t, txs, sps, next, dead):
  %dead : {TX.resp(TX.begin_sp(t, txs, sps, next, _)) == TX.RErr{} : TX.Resp}
  {==}

def Laws.savepoint_in_live_tx(t, txs, sps, next, live):
  %T_true(TX.has(t, txs), live) : {TX.resp(TX.begin_sp(t, txs, sps, next, _)) == TX.RId{next} : TX.Resp}
  {==}

def Laws.savepoint_bound_to_tx(t, txs, sps, next, live):
  %T_true(TX.has(t, txs), live) : {Some{t} == TX.owner(next, TX.sps_of(TX.state(TX.begin_sp(t, txs, sps, next, _)))) : Maybe<&2, Nat>}
  %eq_refl(next) : {Some{t} == TX.found(t, TX.owner(next, sps), _) : Maybe<&2, Nat>}
  {==}

def Laws.fresh_tx_runs_statement(txs, sps, next):
  %eq_refl(next) : {TX.resp(TX.stmt(next <> txs, sps, 1n+next, (_ || TX.has(next, txs)))) == TX.RDone{} : TX.Resp}
  {==}

def Laws.fresh_tx_takes_savepoint(txs, sps, next):
  %eq_refl(next) : {TX.resp(TX.begin_sp(next, next <> txs, sps, 1n+next, (_ || TX.has(next, txs)))) == TX.RId{1n+next} : TX.Resp}
  {==}

def Laws.commit_rollback_same_state(t, txs, sps, next):
  {==}

# Ended transactions are rejected
# --------------------------------

# every request naming a transaction that is not live is an error
def dead_tx_rejected(a: TX.Act, +t: Nat, +txs: List<&2, Nat>, +sps: List<&2, TX.Sp>, +next: Nat,
  dead: {False{} == TX.has(t, txs) : Bool}, u: Laws.T(Laws.refers_tx(a, t)))
  -> {TX.resp(TX.step(a, txs, sps, next)) == TX.RErr{} : TX.Resp}:
  match a:
    case TX.ABeginTx{}:
      Empty.absurd({TX.resp(TX.step(TX.ABeginTx{}, txs, sps, next)) == TX.RErr{} : TX.Resp}, u)
    case TX.ABeginSp{+tx}:
      %eq_sound(t, tx, T_true(Nat.is_eq(t, tx), u)) : {TX.resp(TX.begin_sp(_, txs, sps, next, TX.has(_, txs))) == TX.RErr{} : TX.Resp}
      %dead : {TX.resp(TX.begin_sp(t, txs, sps, next, _)) == TX.RErr{} : TX.Resp}
      {==}
    case TX.AEndTx{+tx, how}:
      %eq_sound(t, tx, T_true(Nat.is_eq(t, tx), u)) : {TX.resp(TX.end_tx(_, txs, sps, next, TX.has(_, txs))) == TX.RErr{} : TX.Resp}
      %dead : {TX.resp(TX.end_tx(t, txs, sps, next, _)) == TX.RErr{} : TX.Resp}
      {==}
    case TX.AEndSp{+sp, how}:
      Empty.absurd({TX.resp(TX.step(TX.AEndSp{sp, how}, txs, sps, next)) == TX.RErr{} : TX.Resp}, u)
    case TX.AStmt{TX.SAuto{}}:
      Empty.absurd({TX.resp(TX.step(TX.AStmt{TX.SAuto{}}, txs, sps, next)) == TX.RErr{} : TX.Resp}, u)
    case TX.AStmt{TX.STx{+tx}}:
      %eq_sound(t, tx, T_true(Nat.is_eq(t, tx), u)) : {TX.resp(TX.stmt(txs, sps, next, TX.has(_, txs))) == TX.RErr{} : TX.Resp}
      %dead : {TX.resp(TX.stmt(txs, sps, next, _)) == TX.RErr{} : TX.Resp}
      {==}

# over the verdict c of has(t, txs): after end_tx, t is not live either way
def ended.fin(a: TX.Act, +t: Nat, +txs: List<&2, Nat>, +sps: List<&2, TX.Sp>, +next: Nat,
  c: Bool, e: {c == TX.has(t, txs) : Bool}, u: Laws.T(Laws.refers_tx(a, t)))
  -> {TX.resp(TX.step_s(a, TX.state(TX.end_tx(t, txs, sps, next, c)))) == TX.RErr{} : TX.Resp}:
  match c:
    case True{}:
      dead_tx_rejected(a, t, TX.remove(t, txs), TX.cut(TX.OfTx{t}, sps), next, has_remove(t, txs), u)
    case False{}:
      dead_tx_rejected(a, t, txs, sps, next, e, u)

def Laws.ended_tx_rejected(a, t, how, txs, sps, next, u):
  ended.fin(a, t, txs, sps, next, TX.has(t, txs), {==}, u)

# EndSavepoint and the transaction list
# -------------------------------------

# over the lookup verdict o: the transactions are untouched either way
def keeps_txs.fin(+s: Nat, how: TX.EndSp, +txs: List<&2, Nat>, +sps: List<&2, TX.Sp>, +next: Nat, o: Maybe<&2, Nat>)
  -> {txs == TX.txs_of(TX.state(TX.end_sp(s, how, txs, sps, next, o))) : List<&2, Nat>}:
  match o:
    case None{}:
      {==}
    case Some{t}:
      {==}

def Laws.end_savepoint_keeps_txs(s, how, txs, sps, next):
  keeps_txs.fin(s, how, txs, sps, next, TX.owner(s, sps))

# Lemmas on owner and cut
# -----------------------

def disc_m(m: Maybe<&2, Nat>) -> Type:
  match m:
    case None{}:
      Empty
    case Some{x}:
      Unit

# None and Some clash
def none_ne_some(t: Nat, e: {Some{t} == None{} : Maybe<&2, Nat>}) -> Empty:
  %e : disc_m(_)
  Unit{}

# cutting the savepoint s itself leaves no s: the step case, over the
# verdict c of Nat.is_eq(id, s)
def owner_cut_id.fin(+s: Nat, +id: Nat, +tx: Nat, -t: List<&2, TX.Sp>,
  rec: {None{} == TX.owner(s, TX.cut(TX.IsId{s}, t)) : Maybe<&2, Nat>},
  c: Bool, e: {c == Nat.is_eq(id, s) : Bool}, e2: {c == Nat.is_eq(s, id) : Bool})
  -> {None{} == TX.owner(s, TX.put_sp(TX.Sp{id, tx}, TX.cut(TX.IsId{s}, t), c)) : Maybe<&2, Nat>}:
  match c:
    case True{}:
      rec
    case False{}:
      %e2 : {None{} == TX.found(tx, TX.owner(s, TX.cut(TX.IsId{s}, t)), _) : Maybe<&2, Nat>}
      rec

# Nat.is_eq is symmetric
def eq_sym(x: Nat, y: Nat) -> {Nat.is_eq(x, y) == Nat.is_eq(y, x) : Bool}:
  match x y:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      eq_sym(p, q)

def owner_cut_id(+s: Nat, sps: List<&2, TX.Sp>) -> {None{} == TX.owner(s, TX.cut(TX.IsId{s}, sps)) : Maybe<&2, Nat>}:
  match sps:
    case Nil{}:
      {==}
    case Con{TX.Sp{+id, +tx}, t}:
      owner_cut_id.fin(s, id, tx, t, owner_cut_id(s, t), Nat.is_eq(id, s), {==}, eq_sym(id, s))

# a savepoint not selected by p keeps its owner through cut: the step
# case, over the verdict c of Nat.is_eq(s, id) and then d of hit(p, id, tx)
def owner_cut_keep.fin2(+p: TX.Pred, +s: Nat, +t: Nat, +id: Nat, +tx: Nat, -r: List<&2, TX.Sp>,
  rec: {Some{t} == TX.owner(s, TX.cut(p, r)) : Maybe<&2, Nat>},
  e: {False{} == Nat.is_eq(s, id) : Bool}, d: Bool)
  -> {Some{t} == TX.owner(s, TX.put_sp(TX.Sp{id, tx}, TX.cut(p, r), d)) : Maybe<&2, Nat>}:
  match d:
    case True{}:
      rec
    case False{}:
      %e : {Some{t} == TX.found(tx, TX.owner(s, TX.cut(p, r)), _) : Maybe<&2, Nat>}
      rec

# hit(p, s, t) restated for Sp{id, tx} when s == id and t == tx
def cast_miss(+p: TX.Pred, +s: Nat, +t: Nat, +id: Nat, +tx: Nat, es: {s == id : Nat}, et: {t == tx : Nat},
  m: {False{} == TX.hit(p, s, t) : Bool}) -> {False{} == TX.hit(p, id, tx) : Bool}:
  %es : {False{} == TX.hit(p, _, tx) : Bool}
  %et : {False{} == TX.hit(p, s, _) : Bool}
  m

def cast_hit(+p: TX.Pred, +s: Nat, +t: Nat, +id: Nat, +tx: Nat, es: {s == id : Nat}, et: {t == tx : Nat},
  h: Laws.T(TX.hit(p, s, t))) -> Laws.T(TX.hit(p, id, tx)):
  %es : Laws.T(TX.hit(p, _, tx))
  %et : Laws.T(TX.hit(p, s, _))
  h

def owner_cut_keep.fin(+p: TX.Pred, +s: Nat, +t: Nat, +id: Nat, +tx: Nat, -r: List<&2, TX.Sp>,
  rec: {Some{t} == TX.owner(s, r) : Maybe<&2, Nat>} -> {False{} == TX.hit(p, s, t) : Bool} -> {Some{t} == TX.owner(s, TX.cut(p, r)) : Maybe<&2, Nat>},
  c: Bool, e: {c == Nat.is_eq(s, id) : Bool},
  +own: {Some{t} == TX.found(tx, TX.owner(s, r), c) : Maybe<&2, Nat>},
  miss: {False{} == TX.hit(p, s, t) : Bool})
  -> {Some{t} == TX.owner(s, TX.put_sp(TX.Sp{id, tx}, TX.cut(p, r), TX.hit(p, id, tx))) : Maybe<&2, Nat>}:
  match c:
    case True{}:
      +es = eq_sound(s, id, e)
      %Equal.sym(Nat, s, id, es) : {Some{t} == TX.owner(_, TX.put_sp(TX.Sp{id, tx}, TX.cut(p, r), TX.hit(p, id, tx))) : Maybe<&2, Nat>}
      %cast_miss(p, s, t, id, tx, es, some_inj(t, tx, own), miss) : {Some{t} == TX.owner(id, TX.put_sp(TX.Sp{id, tx}, TX.cut(p, r), _)) : Maybe<&2, Nat>}
      %eq_refl(id) : {Some{t} == TX.found(tx, TX.owner(id, TX.cut(p, r)), _) : Maybe<&2, Nat>}
      own
    case False{}:
      owner_cut_keep.fin2(p, s, t, id, tx, r, rec(own, miss), e, TX.hit(p, id, tx))

def owner_cut_keep(+p: TX.Pred, +s: Nat, +t: Nat, sps: List<&2, TX.Sp>)
  -> {Some{t} == TX.owner(s, sps) : Maybe<&2, Nat>} -> {False{} == TX.hit(p, s, t) : Bool} -> {Some{t} == TX.owner(s, TX.cut(p, sps)) : Maybe<&2, Nat>}:
  match sps:
    case Nil{}:
      own => miss => Empty.absurd({Some{t} == TX.owner(s, TX.cut(p, Nil{})) : Maybe<&2, Nat>}, none_ne_some(t, own))
    case Con{TX.Sp{+id, +tx}, r}:
      own => miss => owner_cut_keep.fin(p, s, t, id, tx, r, owner_cut_keep(p, s, t, r), Nat.is_eq(s, id), {==}, own, miss)

# Release and rollback keep what they should
# ------------------------------------------

# over the lookup verdict o of owner(s, sps)
def release_kills.fin(+s: Nat, how: TX.EndSp, +txs: List<&2, Nat>, +sps: List<&2, TX.Sp>, +next: Nat,
  o: Maybe<&2, Nat>, e: {o == TX.owner(s, sps) : Maybe<&2, Nat>})
  -> {TX.resp(TX.step_s(TX.AEndSp{s, how}, TX.state(TX.end_sp(s, TX.SRelease{}, txs, sps, next, o)))) == TX.RErr{} : TX.Resp}:
  match o:
    case None{}:
      %e : {TX.resp(TX.end_sp(s, how, txs, sps, next, _)) == TX.RErr{} : TX.Resp}
      {==}
    case Some{t}:
      %owner_cut_id(s, sps) : {TX.resp(TX.end_sp(s, how, txs, TX.cut(TX.IsId{s}, sps), next, _)) == TX.RErr{} : TX.Resp}
      {==}

def Laws.release_kills_savepoint(s, how, txs, sps, next):
  release_kills.fin(s, how, txs, sps, next, TX.owner(s, sps), {==})

# over the lookup verdict o of owner(s, sps); s2 is untouched either way
def release_keeps.fin(+s: Nat, +s2: Nat, +t: Nat, +txs: List<&2, Nat>, +sps: List<&2, TX.Sp>, +next: Nat,
  o: Maybe<&2, Nat>, ne: {False{} == Nat.is_eq(s2, s) : Bool}, own: {Some{t} == TX.owner(s2, sps) : Maybe<&2, Nat>})
  -> {Some{t} == TX.owner(s2, TX.sps_of(TX.state(TX.end_sp(s, TX.SRelease{}, txs, sps, next, o)))) : Maybe<&2, Nat>}:
  match o:
    case None{}:
      own
    case Some{t2}:
      owner_cut_keep(TX.IsId{s}, s2, t, sps)(own, ne)

def Laws.release_keeps_other_savepoints(s, s2, t, txs, sps, next, ne, own):
  release_keeps.fin(s, s2, t, txs, sps, next, TX.owner(s, sps), ne, own)

# a rollback to s does not select s: is_lt(s, s) fails
def miss_self(+s: Nat, +t: Nat) -> {False{} == (Nat.is_eq(t, t) && Nat.is_lt(s, s)) : Bool}:
  %eq_refl(t) : {False{} == (_ && Nat.is_lt(s, s)) : Bool}
  lt_irrefl(s)

# a rollback to s does not select an earlier s2: is_lt(s, s2) fails
def miss_before(+s: Nat, +s2: Nat, +t: Nat, bef: Laws.T(Nat.is_lt(s2, s))) -> {False{} == (Nat.is_eq(t, t) && Nat.is_lt(s, s2)) : Bool}:
  %eq_refl(t) : {False{} == (_ && Nat.is_lt(s, s2)) : Bool}
  lt_asym(s2, s, bef)

def Laws.rollback_keeps_savepoint(s, t, how, txs, sps, next, own):
  %own : {TX.resp(TX.step_s(TX.AEndSp{s, how}, TX.state(TX.end_sp(s, TX.SRollback{}, txs, sps, next, _)))) == TX.RDone{} : TX.Resp}
  %owner_cut_keep(TX.AfterIn{s, t}, s, t, sps)(own, miss_self(s, t)) : {TX.resp(TX.end_sp(s, how, txs, TX.cut(TX.AfterIn{s, t}, sps), next, _)) == TX.RDone{} : TX.Resp}
  {==}

def Laws.rollback_keeps_earlier_savepoints(s, s2, t, txs, sps, next, own, own2, bef):
  %own : {Some{t} == TX.owner(s2, TX.sps_of(TX.state(TX.end_sp(s, TX.SRollback{}, txs, sps, next, _)))) : Maybe<&2, Nat>}
  owner_cut_keep(TX.AfterIn{s, t}, s2, t, sps)(own2, miss_before(s, s2, t, bef))

# Lemmas on desc and desc_sp
# --------------------------

# a bound a is a bound b when a < b
def weaken(xs: List<&2, Nat>, +a: Nat, +b: Nat, w: Laws.T(Laws.desc(xs, a)), ab: Laws.T(Nat.is_lt(a, b))) -> Laws.T(Laws.desc(xs, b)):
  match xs:
    case Nil{}:
      Unit{}
    case Con{+x, +t}:
      split.fin(Nat.is_lt(x, a), Laws.desc(t, x), w, Laws.T(Laws.desc(x <> t, b)),
        wx => wt => join.fin(Nat.is_lt(x, b), Laws.desc(t, x), lt_trans(x, a, b, wx, ab), wt))

def weaken_sp(sps: List<&2, TX.Sp>, +a: Nat, +b: Nat, w: Laws.T(Laws.desc_sp(sps, a)), ab: Laws.T(Nat.is_lt(a, b))) -> Laws.T(Laws.desc_sp(sps, b)):
  match sps:
    case Nil{}:
      Unit{}
    case Con{TX.Sp{+id, +tx}, +t}:
      split.fin(Nat.is_lt(id, a), Laws.desc_sp(t, id), w, Laws.T(Laws.desc_sp(TX.Sp{id, tx} <> t, b)),
        wx => wt => join.fin(Nat.is_lt(id, b), Laws.desc_sp(t, id), lt_trans(id, a, b, wx, ab), wt))

# a bound below n is a bound below n + 1
def mono(xs: List<&2, Nat>, +n: Nat, w: Laws.T(Laws.desc(xs, n))) -> Laws.T(Laws.desc(xs, 1n+n)):
  weaken(xs, n, 1n+n, w, lt_succ(n))

def mono_sp(sps: List<&2, TX.Sp>, +n: Nat, w: Laws.T(Laws.desc_sp(sps, n))) -> Laws.T(Laws.desc_sp(sps, 1n+n)):
  weaken_sp(sps, n, 1n+n, w, lt_succ(n))

# removing keeps the order: the step case, over the verdict c of the removal
def desc_remove.fin(c: Bool, +h: Nat, +x: Nat, t: List<&2, Nat>, +n: Nat,
  wx: Laws.T(Nat.is_lt(x, n)), wr: Laws.T(Laws.desc(TX.remove(h, t), x)))
  -> Laws.T(Laws.desc(TX.put(x, TX.remove(h, t), c), n)):
  match c:
    case True{}:
      weaken(TX.remove(h, t), x, n, wr, wx)
    case False{}:
      join.fin(Nat.is_lt(x, n), Laws.desc(TX.remove(h, t), x), wx, wr)

def desc_remove(+h: Nat, xs: List<&2, Nat>, +n: Nat, w: Laws.T(Laws.desc(xs, n))) -> Laws.T(Laws.desc(TX.remove(h, xs), n)):
  match xs:
    case Nil{}:
      Unit{}
    case Con{+x, +t}:
      split.fin(Nat.is_lt(x, n), Laws.desc(t, x), w, Laws.T(Laws.desc(TX.remove(h, x <> t), n)),
        wx => wt => desc_remove.fin(Nat.is_eq(x, h), h, x, t, n, wx, desc_remove(h, t, x, wt)))

# cutting keeps the order: the step case, over the verdict c of hit
def desc_cut.fin(c: Bool, +p: TX.Pred, +id: Nat, +tx: Nat, t: List<&2, TX.Sp>, +n: Nat,
  wx: Laws.T(Nat.is_lt(id, n)), wr: Laws.T(Laws.desc_sp(TX.cut(p, t), id)))
  -> Laws.T(Laws.desc_sp(TX.put_sp(TX.Sp{id, tx}, TX.cut(p, t), c), n)):
  match c:
    case True{}:
      weaken_sp(TX.cut(p, t), id, n, wr, wx)
    case False{}:
      join.fin(Nat.is_lt(id, n), Laws.desc_sp(TX.cut(p, t), id), wx, wr)

def desc_cut(+p: TX.Pred, sps: List<&2, TX.Sp>, +n: Nat, w: Laws.T(Laws.desc_sp(sps, n))) -> Laws.T(Laws.desc_sp(TX.cut(p, sps), n)):
  match sps:
    case Nil{}:
      Unit{}
    case Con{TX.Sp{+id, +tx}, +t}:
      split.fin(Nat.is_lt(id, n), Laws.desc_sp(t, id), w, Laws.T(Laws.desc_sp(TX.cut(p, TX.Sp{id, tx} <> t), n)),
        wx => wt => desc_cut.fin(TX.hit(p, id, tx), p, id, tx, t, n, wx, desc_cut(p, t, id, wt)))

# Invariant laws
# --------------

def Laws.inv_start():
  (Unit{}, Unit{})

# the BeginSavepoint arm, over the verdict c of has(t, txs)
def kept_begin_sp.fin(c: Bool, +t: Nat, +txs: List<&2, Nat>, +sps: List<&2, TX.Sp>, +next: Nat, w: Laws.Inv(txs, sps, next))
  -> Laws.Inv_s(TX.state(TX.begin_sp(t, txs, sps, next, c))):
  match c:
    case True{}:
      (wt, ws) = w
      (mono(txs, next, wt), join.fin(Nat.is_lt(next, 1n+next), Laws.desc_sp(sps, next), lt_succ(next), ws))
    case False{}:
      w

# the EndTransaction arm, over the verdict c of has(t, txs)
def kept_end_tx.fin(c: Bool, +t: Nat, +txs: List<&2, Nat>, +sps: List<&2, TX.Sp>, +next: Nat, w: Laws.Inv(txs, sps, next))
  -> Laws.Inv_s(TX.state(TX.end_tx(t, txs, sps, next, c))):
  match c:
    case True{}:
      (wt, ws) = w
      (desc_remove(t, txs, next, wt), desc_cut(TX.OfTx{t}, sps, next, ws))
    case False{}:
      w

# the EndSavepoint arm, over the lookup verdict o and the action
def kept_end_sp.fin(o: Maybe<&2, Nat>, how: TX.EndSp, +s: Nat, +txs: List<&2, Nat>, +sps: List<&2, TX.Sp>, +next: Nat, w: Laws.Inv(txs, sps, next))
  -> Laws.Inv_s(TX.state(TX.end_sp(s, how, txs, sps, next, o))):
  match o how:
    case None{} TX.SRelease{}:
      w
    case None{} TX.SRollback{}:
      w
    case Some{t} TX.SRelease{}:
      (wt, ws) = w
      (wt, desc_cut(TX.IsId{s}, sps, next, ws))
    case Some{t} TX.SRollback{}:
      (wt, ws) = w
      (wt, desc_cut(TX.AfterIn{s, t}, sps, next, ws))

# the statement arm: the state is unchanged either way
def kept_stmt.fin(c: Bool, +txs: List<&2, Nat>, +sps: List<&2, TX.Sp>, +next: Nat, w: Laws.Inv(txs, sps, next))
  -> Laws.Inv_s(TX.state(TX.stmt(txs, sps, next, c))):
  match c:
    case True{}:
      w
    case False{}:
      w

def Laws.inv_kept(a, txs, sps, next, w):
  match a:
    case TX.ABeginTx{}:
      (wt, ws) = w
      (join.fin(Nat.is_lt(next, 1n+next), Laws.desc(txs, next), lt_succ(next), wt), mono_sp(sps, next, ws))
    case TX.ABeginSp{+t}:
      kept_begin_sp.fin(TX.has(t, txs), t, txs, sps, next, w)
    case TX.AEndTx{+t, how}:
      kept_end_tx.fin(TX.has(t, txs), t, txs, sps, next, w)
    case TX.AEndSp{+s, how}:
      kept_end_sp.fin(TX.owner(s, sps), how, s, txs, sps, next, w)
    case TX.AStmt{TX.SAuto{}}:
      w
    case TX.AStmt{TX.STx{+t}}:
      kept_stmt.fin(TX.has(t, txs), txs, sps, next, w)

# Unique ids: a cut savepoint is gone
# -----------------------------------

# below a bound s, no savepoint has id s, before or after a cut: the
# step case, over the verdict d of hit(p, id, tx)
def below_none_cut.fin(d: Bool, +p: TX.Pred, +s: Nat, +id: Nat, +tx: Nat, r: List<&2, TX.Sp>,
  +wx: Laws.T(Nat.is_lt(id, s)), wr: Laws.T(Laws.desc_sp(r, id)),
  rec: Laws.T(Laws.desc_sp(r, s)) -> {None{} == TX.owner(s, TX.cut(p, r)) : Maybe<&2, Nat>})
  -> {None{} == TX.owner(s, TX.put_sp(TX.Sp{id, tx}, TX.cut(p, r), d)) : Maybe<&2, Nat>}:
  match d:
    case True{}:
      rec(weaken_sp(r, id, s, wr, wx))
    case False{}:
      %lt_ne_r(id, s, wx) : {None{} == TX.found(tx, TX.owner(s, TX.cut(p, r)), _) : Maybe<&2, Nat>}
      rec(weaken_sp(r, id, s, wr, wx))

def below_none_cut(+p: TX.Pred, +s: Nat, r: List<&2, TX.Sp>) -> Laws.T(Laws.desc_sp(r, s)) -> {None{} == TX.owner(s, TX.cut(p, r)) : Maybe<&2, Nat>}:
  match r:
    case Nil{}:
      w => {==}
    case Con{TX.Sp{+id, +tx}, +r2}:
      w => split.fin(Nat.is_lt(id, s), Laws.desc_sp(r2, id), w, {None{} == TX.owner(s, TX.cut(p, TX.Sp{id, tx} <> r2)) : Maybe<&2, Nat>},
        wx => wr => below_none_cut.fin(TX.hit(p, id, tx), p, s, id, tx, r2, wx, wr, below_none_cut(p, s, r2)))

# then over the verdict d of hit(p, id, tx), when id is not s
def owner_cut_hit.fin2(+p: TX.Pred, +s: Nat, +t: Nat, +id: Nat, +tx: Nat, -r: List<&2, TX.Sp>,
  rec: {None{} == TX.owner(s, TX.cut(p, r)) : Maybe<&2, Nat>},
  e: {False{} == Nat.is_eq(s, id) : Bool}, d: Bool)
  -> {None{} == TX.owner(s, TX.put_sp(TX.Sp{id, tx}, TX.cut(p, r), d)) : Maybe<&2, Nat>}:
  match d:
    case True{}:
      rec
    case False{}:
      %e : {None{} == TX.found(tx, TX.owner(s, TX.cut(p, r)), _) : Maybe<&2, Nat>}
      rec

# a savepoint selected by p is gone after cut, given unique ids: the
# step case, over the verdict c of Nat.is_eq(s, id)
def owner_cut_hit.fin(+p: TX.Pred, +s: Nat, +t: Nat, +id: Nat, +tx: Nat, r: List<&2, TX.Sp>,
  rec: Laws.T(Laws.desc_sp(r, id)) -> {Some{t} == TX.owner(s, r) : Maybe<&2, Nat>} -> Laws.T(TX.hit(p, s, t)) -> {None{} == TX.owner(s, TX.cut(p, r)) : Maybe<&2, Nat>},
  c: Bool, e: {c == Nat.is_eq(s, id) : Bool},
  wr: Laws.T(Laws.desc_sp(r, id)),
  own: {Some{t} == TX.found(tx, TX.owner(s, r), c) : Maybe<&2, Nat>},
  h: Laws.T(TX.hit(p, s, t)))
  -> {None{} == TX.owner(s, TX.put_sp(TX.Sp{id, tx}, TX.cut(p, r), TX.hit(p, id, tx))) : Maybe<&2, Nat>}:
  match c:
    case True{}:
      +es = eq_sound(s, id, e)
      %Equal.sym(Nat, s, id, es) : {None{} == TX.owner(_, TX.put_sp(TX.Sp{id, tx}, TX.cut(p, r), TX.hit(p, id, tx))) : Maybe<&2, Nat>}
      %T_true(TX.hit(p, id, tx), cast_hit(p, s, t, id, tx, es, some_inj(t, tx, own), h)) : {None{} == TX.owner(id, TX.put_sp(TX.Sp{id, tx}, TX.cut(p, r), _)) : Maybe<&2, Nat>}
      below_none_cut(p, id, r)(wr)
    case False{}:
      owner_cut_hit.fin2(p, s, t, id, tx, r, rec(wr, own, h), e, TX.hit(p, id, tx))

def owner_cut_hit(+p: TX.Pred, +s: Nat, +t: Nat, sps: List<&2, TX.Sp>, +n: Nat)
  -> Laws.T(Laws.desc_sp(sps, n)) -> {Some{t} == TX.owner(s, sps) : Maybe<&2, Nat>} -> Laws.T(TX.hit(p, s, t)) -> {None{} == TX.owner(s, TX.cut(p, sps)) : Maybe<&2, Nat>}:
  match sps:
    case Nil{}:
      w => own => h => Empty.absurd({None{} == TX.owner(s, TX.cut(p, Nil{})) : Maybe<&2, Nat>}, none_ne_some(t, own))
    case Con{TX.Sp{+id, +tx}, +r}:
      w => own => h => split.fin(Nat.is_lt(id, n), Laws.desc_sp(r, id), w, {None{} == TX.owner(s, TX.cut(p, TX.Sp{id, tx} <> r)) : Maybe<&2, Nat>},
        wx => wr => owner_cut_hit.fin(p, s, t, id, tx, r, owner_cut_hit(p, s, t, r, id), Nat.is_eq(s, id), {==}, wr, own, h))

# Ended transactions and rollbacks kill savepoints
# ------------------------------------------------

def Laws.ended_tx_kills_savepoints(t, s, how, how2, txs, sps, next, w, live, own):
  (wt, ws) = w
  %T_true(TX.has(t, txs), live) : {TX.resp(TX.step_s(TX.AEndSp{s, how2}, TX.state(TX.end_tx(t, txs, sps, next, _)))) == TX.RErr{} : TX.Resp}
  %owner_cut_hit(TX.OfTx{t}, s, t, sps, next)(ws, own, true_T(Nat.is_eq(t, t), eq_refl(t))) : {TX.resp(TX.end_sp(s, how2, TX.remove(t, txs), TX.cut(TX.OfTx{t}, sps), next, _)) == TX.RErr{} : TX.Resp}
  {==}

def Laws.rollback_kills_later_savepoints(s, s2, t, how, txs, sps, next, w, own, own2, aft):
  (wt, ws) = w
  %own : {TX.resp(TX.step_s(TX.AEndSp{s2, how}, TX.state(TX.end_sp(s, TX.SRollback{}, txs, sps, next, _)))) == TX.RErr{} : TX.Resp}
  %owner_cut_hit(TX.AfterIn{s, t}, s2, t, sps, next)(ws, own2, join.fin(Nat.is_eq(t, t), Nat.is_lt(s, s2), true_T(Nat.is_eq(t, t), eq_refl(t)), aft)) : {TX.resp(TX.end_sp(s2, how, txs, TX.cut(TX.AfterIn{s, t}, sps), next, _)) == TX.RErr{} : TX.Resp}
  {==}
