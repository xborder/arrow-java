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

# The proofs. Imports the model (as PF) and the claims (as Laws) and
# fills every law they state; `bend PROOF.bend` is the whole check.
#
# Two idioms recur. A computed Bool or Maybe cannot be matched in
# place, so a `.fin` helper takes it as a parameter c together with an
# equation e about it, matches c, and rewrites the goal with e inside
# each case ("inspect"). And a rewrite may not precede a match on a
# parameter, so every helper matches first and rewrites inside the
# cases.

import Base
import ./main.bend as PF
import ./LAWS.bend as Laws

# Proof kit
# ---------

# T(True{}) from an equation
def true_T(b: Bool, e: {True{} == b : Bool}) -> Laws.T(b):
  %e : Laws.T(_)
  Unit{}

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

# an equation from T(b)
def T_true(b: Bool, w: Laws.T(b)) -> {True{} == b : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      Empty.absurd({True{} == False{} : Bool}, w)

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

# T(Bool.not(c)) refutes T(c)
def not_both.fin(c: Bool, w: Laws.T(c), n: Laws.T(Bool.not(c)), -P: Type) -> P:
  match c:
    case True{}:
      Empty.absurd(P, n)
    case False{}:
      Empty.absurd(P, w)

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

# x < n implies x <= n
def lt_le(x: Nat, n: Nat, w: Laws.T(Nat.is_lt(x, n))) -> Laws.T(Nat.is_le(x, n)):
  match x n:
    case 0n 0n:
      Empty.absurd(Laws.T(Nat.is_le(0n, 0n)), w)
    case 0n 1n+q:
      Unit{}
    case 1n+p 0n:
      Empty.absurd(Laws.T(Nat.is_le(1n+p, 0n)), w)
    case 1n+p 1n+q:
      lt_le(p, q, w)

# n <= n
def le_refl(n: Nat) -> Laws.T(Nat.is_le(n, n)):
  match n:
    case 0n:
      Unit{}
    case 1n+p:
      le_refl(p)

# p <= p + k
def le_add(p: Nat, k: Nat) -> Laws.T(Nat.is_le(p, Nat.add(p, k))):
  match p k:
    case 0n 0n:
      Unit{}
    case 0n 1n+q:
      Unit{}
    case 1n+p2 k2:
      le_add(p2, k2)

# m >= e implies e < m + 1
def ge_lt_succ(e: Nat, m: Nat, w: Laws.T(Nat.is_ge(m, e))) -> Laws.T(Nat.is_lt(e, 1n+m)):
  match e m:
    case 0n 0n:
      Unit{}
    case 0n 1n+q:
      Unit{}
    case 1n+p 0n:
      Empty.absurd(Laws.T(Nat.is_lt(1n+p, 1n)), w)
    case 1n+p 1n+q:
      ge_lt_succ(p, q, w)

# m >= a + 1 implies m >= a
def ge_pred(a: Nat, m: Nat, w: Laws.T(Nat.is_ge(m, 1n+a))) -> Laws.T(Nat.is_ge(m, a)):
  match a m:
    case 0n 0n:
      Empty.absurd(Laws.T(Nat.is_ge(0n, 0n)), w)
    case 0n 1n+q:
      Unit{}
    case 1n+p 0n:
      Empty.absurd(Laws.T(Nat.is_ge(0n, 1n+p)), w)
    case 1n+p 1n+q:
      ge_pred(p, q, w)

# Base's max law as a T fact
def max_ge_l(+a: Nat, +b: Nat) -> Laws.T(Nat.is_ge(Nat.max(a, b), a)):
  true_T(Nat.is_ge(Nat.max(a, b), a), Equal.sym(Bool, Nat.is_ge(Nat.max(a, b), a), True{}, Nat.max_ge_l(a, b)))

def max_ge_r(+a: Nat, +b: Nat) -> Laws.T(Nat.is_ge(Nat.max(a, b), b)):
  true_T(Nat.is_ge(Nat.max(a, b), b), Equal.sym(Bool, Nat.is_ge(Nat.max(a, b), b), True{}, Nat.max_ge_r(a, b)))

# the renewed expiry is strictly later than the old one
def renewed_later(+exp: Nat, +now: Nat) -> Laws.T(Nat.is_lt(exp, PF.renewed(exp, now))):
  ge_lt_succ(exp, Nat.max(exp, Nat.add(PF.ETTL(), now)), max_ge_l(exp, Nat.add(PF.ETTL(), now)))

# the renewed expiry is still ahead of the clock
def renewed_valid(+exp: Nat, +now: Nat) -> Laws.T(Nat.is_lt(now, PF.renewed(exp, now))):
  ge_lt_succ(now, Nat.max(exp, Nat.add(PF.ETTL(), now)),
    ge_pred(now, Nat.max(exp, Nat.add(PF.ETTL(), now)),
      ge_pred(1n+now, Nat.max(exp, Nat.add(PF.ETTL(), now)), max_ge_r(exp, Nat.add(PF.ETTL(), now)))))

# a touched descriptor is valid now
def pttl_valid(now: Nat) -> {True{} == Nat.is_lt(now, Nat.add(PF.PTTL(), now)) : Bool}:
  match now:
    case 0n:
      {==}
    case 1n+p:
      pttl_valid(p)

# id < next and next <= q imply q != id
def lt_le_ne(id: Nat, next: Nat, q: Nat, w: Laws.T(Nat.is_lt(id, next)), u: Laws.T(Nat.is_le(next, q))) -> {False{} == Nat.is_eq(q, id) : Bool}:
  match id next q:
    case 0n 0n c:
      Empty.absurd({False{} == Nat.is_eq(c, 0n) : Bool}, w)
    case 1n+i 0n c:
      Empty.absurd({False{} == Nat.is_eq(c, 1n+i) : Bool}, w)
    case i 1n+n 0n:
      Empty.absurd({False{} == Nat.is_eq(0n, i) : Bool}, u)
    case 0n 1n+n 1n+c:
      {==}
    case 1n+i 1n+n 1n+c:
      lt_le_ne(i, n, c, w, u)

# Lemmas on find and upd
# ----------------------

# a lookup after an update is the update of the lookup
def upd_m(+op: PF.Op, +now: Nat, +q: Nat, m: Maybe<&2, PF.QBody>) -> Maybe<&2, PF.QBody>:
  match m:
    case None{}:
      None{}
    case Some{body}:
      Some{PF.apply(op, now, q, body)}

def find_upd.fin(+q: Nat, +op: PF.Op, +now: Nat, +id: Nat, +body: PF.QBody, -t: List<&2, PF.Query>,
  rec: {upd_m(op, now, q, PF.find(q, t)) == PF.find(q, PF.upd(op, now, t)) : Maybe<&2, PF.QBody>},
  c: Bool, e: {c == Nat.is_eq(q, id) : Bool})
  -> {upd_m(op, now, q, PF.pick_q(c, body, PF.find(q, t))) == PF.pick_q(c, PF.apply(op, now, id, body), PF.find(q, PF.upd(op, now, t))) : Maybe<&2, PF.QBody>}:
  match c:
    case True{}:
      %eq_sound(q, id, e) : {Some{PF.apply(op, now, q, body)} == Some{PF.apply(op, now, _, body)} : Maybe<&2, PF.QBody>}
      {==}
    case False{}:
      rec

def find_upd(+q: Nat, +op: PF.Op, +now: Nat, qs: List<&2, PF.Query>) -> {upd_m(op, now, q, PF.find(q, qs)) == PF.find(q, PF.upd(op, now, qs)) : Maybe<&2, PF.QBody>}:
  match qs:
    case Nil{}:
      {==}
    case Con{PF.Query{+id, +body}, t}:
      find_upd.fin(q, op, now, id, body, t, find_upd(q, op, now, t), Nat.is_eq(q, id), {==})

# a lookup of an id at or above the counter finds nothing
def find_unknown.fin(+q: Nat, +next: Nat, +id: Nat, -body: PF.QBody, -t: List<&2, PF.Query>,
  rec: Laws.T(Laws.all_ok(t, next)) -> {None{} == PF.find(q, t) : Maybe<&2, PF.QBody>},
  u: Laws.T(Nat.is_le(next, q)), c: Bool, e: {c == Nat.is_eq(q, id) : Bool}, wid: Laws.T(Nat.is_lt(id, next)), wt: Laws.T(Laws.all_ok(t, next)))
  -> {None{} == PF.pick_q(c, body, PF.find(q, t)) : Maybe<&2, PF.QBody>}:
  match c:
    case True{}:
      Empty.absurd({None{} == Some{body} : Maybe<&2, PF.QBody>},
        true_ne_false(Equal.trans(Bool, True{}, Nat.is_eq(q, id), False{}, e, Equal.sym(Bool, False{}, Nat.is_eq(q, id), lt_le_ne(id, next, q, wid, u)))))
    case False{}:
      rec(wt)

def find_unknown(+q: Nat, qs: List<&2, PF.Query>, +next: Nat, +u: Laws.T(Nat.is_le(next, q))) -> Laws.T(Laws.all_ok(qs, next)) -> {None{} == PF.find(q, qs) : Maybe<&2, PF.QBody>}:
  match qs:
    case Nil{}:
      w => {==}
    case Con{PF.Query{+id, PF.QBody{st, pexp, eps}}, +t}:
      w => split.fin(Nat.is_lt(id, next) && Laws.st_ok(st), Laws.all_ok(t, next), w, {None{} == PF.find(q, PF.Query{id, PF.QBody{st, pexp, eps}} <> t) : Maybe<&2, PF.QBody>},
        wh => wt => split.fin(Nat.is_lt(id, next), Laws.st_ok(st), wh, {None{} == PF.find(q, PF.Query{id, PF.QBody{st, pexp, eps}} <> t) : Maybe<&2, PF.QBody>},
          wid => wst => find_unknown.fin(q, next, id, PF.QBody{st, pexp, eps}, t, find_unknown(q, t, next, u), u, Nat.is_eq(q, id), {==}, wid, wt)))

# the invariant, seen through a lookup: a found running query's progress is within FULL
def mst_ok(m: Maybe<&2, PF.QBody>) -> Bool:
  match m:
    case None{}:
      True{}
    case Some{PF.QBody{st, pexp, eps}}:
      Laws.st_ok(st)

def find_ok.fin(c: Bool, -st: PF.QState, -pexp: Nat, -eps: List<&2, PF.Endpoint>, -t: List<&2, PF.Query>, +q: Nat,
  wst: Laws.T(Laws.st_ok(st)), rec: Laws.T(mst_ok(PF.find(q, t))))
  -> Laws.T(mst_ok(PF.pick_q(c, PF.QBody{st, pexp, eps}, PF.find(q, t)))):
  match c:
    case True{}:
      wst
    case False{}:
      rec

def find_ok(+q: Nat, qs: List<&2, PF.Query>, +next: Nat) -> Laws.T(Laws.all_ok(qs, next)) -> Laws.T(mst_ok(PF.find(q, qs))):
  match qs:
    case Nil{}:
      w => Unit{}
    case Con{PF.Query{+id, PF.QBody{st, pexp, eps}}, +t}:
      w => split.fin(Nat.is_lt(id, next) && Laws.st_ok(st), Laws.all_ok(t, next), w, Laws.T(mst_ok(PF.find(q, PF.Query{id, PF.QBody{st, pexp, eps}} <> t))),
        wh => wt => split.fin(Nat.is_lt(id, next), Laws.st_ok(st), wh, Laws.T(mst_ok(PF.find(q, PF.Query{id, PF.QBody{st, pexp, eps}} <> t))),
          wid => wst => find_ok.fin(Nat.is_eq(q, id), st, pexp, eps, t, q, wst, find_ok(q, t, next)(wt))))

# Invariant preservation
# ----------------------

# a bound below next is a bound below next + 1
def mono(qs: List<&2, PF.Query>, +next: Nat, w: Laws.T(Laws.all_ok(qs, next))) -> Laws.T(Laws.all_ok(qs, 1n+next)):
  match qs:
    case Nil{}:
      Unit{}
    case Con{PF.Query{+id, PF.QBody{+st, pexp, eps}}, +t}:
      split.fin(Nat.is_lt(id, next) && Laws.st_ok(st), Laws.all_ok(t, next), w, Laws.T(Laws.all_ok(PF.Query{id, PF.QBody{st, pexp, eps}} <> t, 1n+next)),
        wh => wt => split.fin(Nat.is_lt(id, next), Laws.st_ok(st), wh, Laws.T(Laws.all_ok(PF.Query{id, PF.QBody{st, pexp, eps}} <> t, 1n+next)),
          wid => wst => join.fin(Nat.is_lt(id, 1n+next) && Laws.st_ok(st), Laws.all_ok(t, 1n+next),
            join.fin(Nat.is_lt(id, 1n+next), Laws.st_ok(st), lt_mono(id, next, wid), wst), mono(t, next, wt))))

# a tick keeps progress within FULL: over the verdict c of is_lt(p + STEP, FULL)
def tick_ok.fin(c: Bool, +p: Nat, e: {c == Nat.is_lt(Nat.add(p, PF.STEP()), PF.FULL()) : Bool}) -> Laws.T(Laws.st_ok(PF.advance(Nat.add(p, PF.STEP()), c))):
  match c:
    case True{}:
      lt_le(Nat.add(p, PF.STEP()), PF.FULL(), true_T(Nat.is_lt(Nat.add(p, PF.STEP()), PF.FULL()), e))
    case False{}:
      Unit{}

def tick_st_ok(st: PF.QState, w: Laws.T(Laws.st_ok(st))) -> Laws.T(Laws.st_ok(PF.tick_st(st))):
  match st:
    case PF.QRunning{+p}:
      tick_ok.fin(Nat.is_lt(Nat.add(p, PF.STEP()), PF.FULL()), p, {==})
    case PF.QDone{}:
      Unit{}
    case PF.QCancelled{}:
      Unit{}
    case PF.QFailed{}:
      Unit{}

def cancel_st_ok(st: PF.QState) -> Laws.T(Laws.st_ok(PF.cancel_st(st))):
  match st:
    case PF.QRunning{p}:
      Unit{}
    case PF.QDone{}:
      Unit{}
    case PF.QCancelled{}:
      Unit{}
    case PF.QFailed{}:
      Unit{}

def fail_st_ok(st: PF.QState) -> Laws.T(Laws.st_ok(PF.fail_st(st))):
  match st:
    case PF.QRunning{p}:
      Unit{}
    case PF.QDone{}:
      Unit{}
    case PF.QCancelled{}:
      Unit{}
    case PF.QFailed{}:
      Unit{}

# the verdict c of a targeted op's id test picks the new or the old body
def when_ok.fin(c: Bool, -a: PF.QBody, -b: PF.QBody, wa: Laws.T(mst_ok(Some{a})), wb: Laws.T(mst_ok(Some{b}))) -> Laws.T(mst_ok(Some{PF.when_b(c, a, b)})):
  match c:
    case True{}:
      wa
    case False{}:
      wb

# every op keeps a body's progress within FULL
def apply_ok(op: PF.Op, +now: Nat, +id: Nat, body: PF.QBody) -> Laws.T(mst_ok(Some{body})) -> Laws.T(mst_ok(Some{PF.apply(op, now, id, body)})):
  match op body:
    case PF.OpTick{} PF.QBody{+st, pexp, eps}:
      w => tick_st_ok(st, w)
    case PF.OpTouch{q} PF.QBody{+st, +pexp, +eps}:
      +w => when_ok.fin(Nat.is_eq(q, id), PF.touch_body(now, PF.QBody{st, pexp, eps}), PF.QBody{st, pexp, eps}, w, w)
    case PF.OpCancel{q} PF.QBody{+st, +pexp, +eps}:
      w => when_ok.fin(Nat.is_eq(q, id), PF.cancel_body(PF.QBody{st, pexp, eps}), PF.QBody{st, pexp, eps}, cancel_st_ok(st), w)
    case PF.OpFail{q} PF.QBody{+st, +pexp, +eps}:
      w => when_ok.fin(Nat.is_eq(q, id), PF.fail_body(PF.QBody{st, pexp, eps}), PF.QBody{st, pexp, eps}, fail_st_ok(st), w)
    case PF.OpRenew{q, tk} PF.QBody{+st, +pexp, +eps}:
      +w => when_ok.fin(Nat.is_eq(q, id), PF.renew_body(tk, now, PF.QBody{st, pexp, eps}), PF.QBody{st, pexp, eps}, w, w)

def all_ok_cons(+id: Nat, body: PF.QBody, -rest: List<&2, PF.Query>, +next: Nat,
  wid: Laws.T(Nat.is_lt(id, next)), wb: Laws.T(mst_ok(Some{body})), wr: Laws.T(Laws.all_ok(rest, next)))
  -> Laws.T(Laws.all_ok(PF.Query{id, body} <> rest, next)):
  match body:
    case PF.QBody{+st, pexp, eps}:
      join.fin(Nat.is_lt(id, next) && Laws.st_ok(st), Laws.all_ok(rest, next), join.fin(Nat.is_lt(id, next), Laws.st_ok(st), wid, wb), wr)

# the head of an updated list is ok: the body after apply, as a pattern
def upd_body_ok(op: PF.Op, +now: Nat, +id: Nat, body: PF.QBody, -rest: List<&2, PF.Query>, +next: Nat,
  wid: Laws.T(Nat.is_lt(id, next)), wa: Laws.T(mst_ok(Some{PF.apply(op, now, id, body)})), wr: Laws.T(Laws.all_ok(rest, next)))
  -> Laws.T(Laws.all_ok(PF.Query{id, PF.apply(op, now, id, body)} <> rest, next)):
  all_ok_cons(id, PF.apply(op, now, id, body), rest, next, wid, wa, wr)

# an update keeps the invariant
def upd_ok(+op: PF.Op, +now: Nat, qs: List<&2, PF.Query>, +next: Nat) -> Laws.T(Laws.all_ok(qs, next)) -> Laws.T(Laws.all_ok(PF.upd(op, now, qs), next)):
  match qs:
    case Nil{}:
      w => Unit{}
    case Con{PF.Query{+id, PF.QBody{+st, +pexp, +eps}}, +t}:
      w => split.fin(Nat.is_lt(id, next) && Laws.st_ok(st), Laws.all_ok(t, next), w, Laws.T(Laws.all_ok(PF.upd(op, now, PF.Query{id, PF.QBody{st, pexp, eps}} <> t), next)),
        wh => wt => split.fin(Nat.is_lt(id, next), Laws.st_ok(st), wh, Laws.T(Laws.all_ok(PF.upd(op, now, PF.Query{id, PF.QBody{st, pexp, eps}} <> t), next)),
          wid => wst => upd_body_ok(op, now, id, PF.QBody{st, pexp, eps}, PF.upd(op, now, t), next, wid, apply_ok(op, now, id, PF.QBody{st, pexp, eps})(wst), upd_ok(op, now, t, next)(wt))))

# Invariant laws
# --------------

def Laws.inv_start():
  Unit{}

# the state after a poll, over the lookup m and its verdicts
def kept_poll_run.fin(alive: Bool, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, -p: Nat, -eps: List<&2, PF.Endpoint>, w: Laws.Inv(qs, next))
  -> Laws.Inv_s(PF.state(PF.poll_run(q, qs, next, now, p, eps, alive))):
  match alive:
    case True{}:
      upd_ok(PF.OpTouch{q}, now, qs, next)(w)
    case False{}:
      upd_ok(PF.OpCancel{q}, now, qs, next)(w)

def kept_poll.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.Inv(qs, next))
  -> Laws.Inv_s(PF.state(PF.poll.fin(q, qs, next, now, m))):
  match m:
    case None{}:
      w
    case Some{PF.QBody{st, +pexp, eps}}:
      match st:
        case PF.QRunning{p}:
          kept_poll_run.fin(Nat.is_lt(now, pexp), q, qs, next, now, p, eps, w)
        case PF.QDone{}:
          w
        case PF.QCancelled{}:
          w
        case PF.QFailed{}:
          w

def kept_cancel.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.Inv(qs, next))
  -> Laws.Inv_s(PF.state(PF.cancel.fin(q, qs, next, now, m))):
  match m:
    case None{}:
      w
    case Some{PF.QBody{st, pexp, eps}}:
      upd_ok(PF.OpCancel{q}, now, qs, next)(w)

def kept_renew_ep.fin(valid: Bool, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, -exp: Nat, w: Laws.Inv(qs, next))
  -> Laws.Inv_s(PF.state(PF.renew_ep_r(q, tk, qs, next, now, exp, valid))):
  match valid:
    case True{}:
      upd_ok(PF.OpRenew{q, tk}, now, qs, next)(w)
    case False{}:
      w

def kept_renew_run.fin(e: Maybe<&2, Nat>, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.Inv(qs, next))
  -> Laws.Inv_s(PF.state(PF.renew_run(q, tk, qs, next, now, e))):
  match e:
    case None{}:
      w
    case Some{+exp}:
      kept_renew_ep.fin(Nat.is_lt(now, exp), q, tk, qs, next, now, exp, w)

def kept_renew.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.Inv(qs, next))
  -> Laws.Inv_s(PF.state(PF.renew.fin(q, tk, qs, next, now, m))):
  match m:
    case None{}:
      w
    case Some{PF.QBody{st, pexp, eps}}:
      match st:
        case PF.QRunning{p}:
          kept_renew_run.fin(PF.find_ep(tk, eps), q, tk, qs, next, now, w)
        case PF.QDone{}:
          kept_renew_run.fin(PF.find_ep(tk, eps), q, tk, qs, next, now, w)
        case PF.QCancelled{}:
          w
        case PF.QFailed{}:
          w

def Laws.inv_kept(a, qs, next, now, w):
  match a:
    case PF.AStart{}:
      join.fin(Nat.is_lt(next, 1n+next) && Laws.st_ok(PF.QRunning{0n}), Laws.all_ok(qs, 1n+next),
        join.fin(Nat.is_lt(next, 1n+next), Laws.st_ok(PF.QRunning{0n}), lt_succ(next), Unit{}), mono(qs, next, w))
    case PF.APoll{+q}:
      kept_poll.fin(PF.find(q, qs), q, qs, next, now, w)
    case PF.ACancel{+q}:
      kept_cancel.fin(PF.find(q, qs), q, qs, next, now, w)
    case PF.ADoGet{q, tk}:
      w
    case PF.ARenew{+q, +tk}:
      kept_renew.fin(PF.find(q, qs), q, tk, qs, next, now, w)
    case PF.ATick{}:
      upd_ok(PF.OpTick{}, 1n+now, qs, next)(w)
    case PF.AFail{+q}:
      upd_ok(PF.OpFail{q}, now, qs, next)(w)

# Self-targeted ops
# -----------------
# an op aimed at q, applied to q's own body, is the plain body change

def touch_self(+q: Nat, +now: Nat, +body: PF.QBody) -> {PF.touch_body(now, body) == PF.apply(PF.OpTouch{q}, now, q, body) : PF.QBody}:
  %eq_refl(q) : {PF.touch_body(now, body) == PF.when_b(_, PF.touch_body(now, body), body) : PF.QBody}
  {==}

def cancel_self(+q: Nat, +now: Nat, +body: PF.QBody) -> {PF.cancel_body(body) == PF.apply(PF.OpCancel{q}, now, q, body) : PF.QBody}:
  %eq_refl(q) : {PF.cancel_body(body) == PF.when_b(_, PF.cancel_body(body), body) : PF.QBody}
  {==}

def fail_self(+q: Nat, +now: Nat, +body: PF.QBody) -> {PF.fail_body(body) == PF.apply(PF.OpFail{q}, now, q, body) : PF.QBody}:
  %eq_refl(q) : {PF.fail_body(body) == PF.when_b(_, PF.fail_body(body), body) : PF.QBody}
  {==}

def renew_self(+q: Nat, +tk: Nat, +now: Nat, +body: PF.QBody) -> {PF.renew_body(tk, now, body) == PF.apply(PF.OpRenew{q, tk}, now, q, body) : PF.QBody}:
  %eq_refl(q) : {PF.renew_body(tk, now, body) == PF.when_b(_, PF.renew_body(tk, now, body), body) : PF.QBody}
  {==}

# Lemmas on endpoints
# -------------------

# a list is a prefix of itself
def prefix_refl(xs: List<&2, Nat>) -> Laws.T(Laws.is_prefix(xs, xs)):
  match xs:
    case Nil{}:
      Unit{}
    case Con{+x, t}:
      join.fin(Nat.is_eq(x, x), Laws.is_prefix(t, t), true_T(Nat.is_eq(x, x), eq_refl(x)), prefix_refl(t))

# appending an endpoint keeps the old tickets as a prefix
def prefix_snoc(eps: List<&2, PF.Endpoint>, e: PF.Endpoint) -> Laws.T(Laws.is_prefix(PF.tickets(eps), PF.tickets(PF.snoc(eps, e)))):
  match eps:
    case Nil{}:
      Unit{}
    case Con{PF.Endpoint{+tk, exp}, t}:
      join.fin(Nat.is_eq(tk, tk), Laws.is_prefix(PF.tickets(t), PF.tickets(PF.snoc(t, e))), true_T(Nat.is_eq(tk, tk), eq_refl(tk)), prefix_snoc(t, e))

# renewing keeps the tickets
def tickets_renew(+tk: Nat, +now: Nat, eps: List<&2, PF.Endpoint>) -> {PF.tickets(eps) == PF.tickets(PF.renew_ep(tk, now, eps)) : List<&2, Nat>}:
  match eps:
    case Nil{}:
      {==}
    case Con{PF.Endpoint{+etk, +exp}, t}:
      %tickets_renew(tk, now, t) : {etk <> PF.tickets(t) == etk <> _ : List<&2, Nat>}
      {==}

def prefix_renew(+tk: Nat, +now: Nat, +eps: List<&2, PF.Endpoint>) -> Laws.T(Laws.is_prefix(PF.tickets(eps), PF.tickets(PF.renew_ep(tk, now, eps)))):
  %tickets_renew(tk, now, eps) : Laws.T(Laws.is_prefix(PF.tickets(eps), _))
  prefix_refl(PF.tickets(eps))

# an endpoint lookup after a renewal is the renewal of the lookup
def ren_m(+now: Nat, m: Maybe<&2, Nat>) -> Maybe<&2, Nat>:
  match m:
    case None{}:
      None{}
    case Some{exp}:
      Some{PF.renewed(exp, now)}

def find_ep_renew.fin(c: Bool, +tk: Nat, +now: Nat, +exp: Nat, -t: List<&2, PF.Endpoint>,
  rec: {ren_m(now, PF.find_ep(tk, t)) == PF.find_ep(tk, PF.renew_ep(tk, now, t)) : Maybe<&2, Nat>})
  -> {ren_m(now, PF.pick_n(c, exp, PF.find_ep(tk, t))) == PF.pick_n(c, PF.when_n(c, PF.renewed(exp, now), exp), PF.find_ep(tk, PF.renew_ep(tk, now, t))) : Maybe<&2, Nat>}:
  match c:
    case True{}:
      {==}
    case False{}:
      rec

def find_ep_renew(+tk: Nat, +now: Nat, eps: List<&2, PF.Endpoint>) -> {ren_m(now, PF.find_ep(tk, eps)) == PF.find_ep(tk, PF.renew_ep(tk, now, eps)) : Maybe<&2, Nat>}:
  match eps:
    case Nil{}:
      {==}
    case Con{PF.Endpoint{+etk, +exp}, t}:
      find_ep_renew.fin(Nat.is_eq(tk, etk), tk, now, exp, t, find_ep_renew(tk, now, t))

# Progress laws
# -------------

def bounded_run.fin(alive: Bool, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, +p: Nat, +eps: List<&2, PF.Endpoint>, wp: Laws.T(Nat.is_le(p, PF.FULL())))
  -> Laws.T(Laws.prog_ok(PF.resp(PF.poll_run(q, qs, next, now, p, eps, alive)))):
  match alive:
    case True{}:
      wp
    case False{}:
      Unit{}

def bounded.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, wm: Laws.T(mst_ok(m)))
  -> Laws.T(Laws.prog_ok(PF.resp(PF.poll.fin(q, qs, next, now, m)))):
  match m:
    case None{}:
      Unit{}
    case Some{PF.QBody{st, +pexp, +eps}}:
      match st:
        case PF.QRunning{+p}:
          bounded_run.fin(Nat.is_lt(now, pexp), q, qs, next, now, p, eps, wm)
        case PF.QDone{}:
          Unit{}
        case PF.QCancelled{}:
          Unit{}
        case PF.QFailed{}:
          Unit{}

def Laws.progress_bounded(q, qs, next, now, w):
  bounded.fin(PF.find(q, qs), q, qs, next, now, find_ok(q, qs, next)(w))

# Descriptor laws
# ---------------

def Laws.unknown_descriptor_rejected(q, qs, next, now, w, u):
  %find_unknown(q, qs, next, u)(w) : {PF.resp(PF.poll.fin(q, qs, next, now, _)) == PF.RErr{} : PF.Resp}
  {==}

# after a cancel aimed at q, q's running body is cancelled
def cancelled_after(+q: Nat, +qs: List<&2, PF.Query>, +now: Nat, +p: Nat, +pexp: Nat, +eps: List<&2, PF.Endpoint>,
  e: {Some{PF.QBody{PF.QRunning{p}, pexp, eps}} == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> Laws.T(Laws.mq_cancelled(PF.find(q, PF.upd(PF.OpCancel{q}, now, qs)))):
  %find_upd(q, PF.OpCancel{q}, now, qs) : Laws.T(Laws.mq_cancelled(_))
  %e : Laws.T(Laws.mq_cancelled(upd_m(PF.OpCancel{q}, now, q, _)))
  %cancel_self(q, now, PF.QBody{PF.QRunning{p}, pexp, eps}) : Laws.T(Laws.mq_cancelled(Some{_}))
  Unit{}

def expired_run.fin(alive: Bool, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, +p: Nat, +pexp: Nat, +eps: List<&2, PF.Endpoint>,
  w: Laws.T(Bool.not(alive)), e: {Some{PF.QBody{PF.QRunning{p}, pexp, eps}} == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> {PF.resp(PF.poll_run(q, qs, next, now, p, eps, alive)) == PF.RErr{} : PF.Resp} & Laws.T(Laws.mq_cancelled(PF.find(q, Laws.queries(PF.state(PF.poll_run(q, qs, next, now, p, eps, alive)))))):
  match alive:
    case True{}:
      Empty.absurd({PF.RPoll{p, Some{q}, PF.tickets(eps)} == PF.RErr{} : PF.Resp} & Laws.T(Laws.mq_cancelled(PF.find(q, PF.upd(PF.OpTouch{q}, now, qs)))), w)
    case False{}:
      ({==}, cancelled_after(q, qs, now, p, pexp, eps, e))

def expired.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat,
  w: Laws.T(Laws.poll_expired(now, m)), e: {m == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> {PF.resp(PF.poll.fin(q, qs, next, now, m)) == PF.RErr{} : PF.Resp} & Laws.T(Laws.mq_cancelled(PF.find(q, Laws.queries(PF.state(PF.poll.fin(q, qs, next, now, m)))))):
  match m:
    case None{}:
      Empty.absurd({PF.RErr{} == PF.RErr{} : PF.Resp} & Laws.T(Laws.mq_cancelled(PF.find(q, qs))), w)
    case Some{PF.QBody{st, +pexp, +eps}}:
      match st:
        case PF.QRunning{+p}:
          expired_run.fin(Nat.is_lt(now, pexp), q, qs, next, now, p, pexp, eps, w, e)
        case PF.QDone{}:
          Empty.absurd({PF.RPoll{PF.FULL(), None{}, PF.tickets(eps)} == PF.RErr{} : PF.Resp} & Laws.T(Laws.mq_cancelled(PF.find(q, qs))), w)
        case PF.QCancelled{}:
          Empty.absurd({PF.RErr{} == PF.RErr{} : PF.Resp} & Laws.T(Laws.mq_cancelled(PF.find(q, qs))), w)
        case PF.QFailed{}:
          Empty.absurd({PF.RErr{} == PF.RErr{} : PF.Resp} & Laws.T(Laws.mq_cancelled(PF.find(q, qs))), w)

def Laws.expired_descriptor_rejected(q, qs, next, now, w):
  expired.fin(PF.find(q, qs), q, qs, next, now, w, {==})

def done.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat,
  w: Laws.T(Laws.mq_done(m)), e: {m == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> {PF.poll.fin(q, qs, next, now, m) == (PF.Server{qs, next, now}, PF.RPoll{PF.FULL(), None{}, Laws.tickets_of(PF.find(q, qs))}) : PF.Server & PF.Resp}:
  match m:
    case None{}:
      Empty.absurd({(PF.Server{qs, next, now}, PF.RErr{}) == (PF.Server{qs, next, now}, PF.RPoll{PF.FULL(), None{}, Laws.tickets_of(PF.find(q, qs))}) : PF.Server & PF.Resp}, w)
    case Some{PF.QBody{st, +pexp, +eps}}:
      match st:
        case PF.QRunning{+p}:
          Empty.absurd({PF.poll_run(q, qs, next, now, p, eps, Nat.is_lt(now, pexp)) == (PF.Server{qs, next, now}, PF.RPoll{PF.FULL(), None{}, Laws.tickets_of(PF.find(q, qs))}) : PF.Server & PF.Resp}, w)
        case PF.QDone{}:
          %e : {(PF.Server{qs, next, now}, PF.RPoll{PF.FULL(), None{}, PF.tickets(eps)}) == (PF.Server{qs, next, now}, PF.RPoll{PF.FULL(), None{}, Laws.tickets_of(_)}) : PF.Server & PF.Resp}
          {==}
        case PF.QCancelled{}:
          Empty.absurd({(PF.Server{qs, next, now}, PF.RErr{}) == (PF.Server{qs, next, now}, PF.RPoll{PF.FULL(), None{}, Laws.tickets_of(PF.find(q, qs))}) : PF.Server & PF.Resp}, w)
        case PF.QFailed{}:
          Empty.absurd({(PF.Server{qs, next, now}, PF.RErr{}) == (PF.Server{qs, next, now}, PF.RPoll{PF.FULL(), None{}, Laws.tickets_of(PF.find(q, qs))}) : PF.Server & PF.Resp}, w)

def Laws.done_poll_complete(q, qs, next, now, w):
  done.fin(PF.find(q, qs), q, qs, next, now, w, {==})

def Laws.fresh_query_polls(qs, next, now):
  %eq_refl(next) : {PF.resp(PF.poll.fin(next, PF.Query{next, PF.fresh(now)} <> qs, 1n+next, now, PF.pick_q(_, PF.fresh(now), PF.find(next, qs)))) == PF.RPoll{0n, Some{next}, Nil{}} : PF.Resp}
  %pttl_valid(now) : {PF.resp(PF.poll_run(next, PF.Query{next, PF.fresh(now)} <> qs, 1n+next, now, 0n, Nil{}, _)) == PF.RPoll{0n, Some{next}, Nil{}} : PF.Resp}
  {==}

# Cancellation laws
# -----------------

def cancel_run.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat,
  w: Laws.T(Laws.mq_running(m)), e: {m == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> {PF.resp(PF.cancel.fin(q, qs, next, now, m)) == PF.RCancel{PF.CsCancelled{}} : PF.Resp} & Laws.T(Laws.mq_cancelled(PF.find(q, Laws.queries(PF.state(PF.cancel.fin(q, qs, next, now, m)))))):
  match m:
    case None{}:
      Empty.absurd({PF.RErr{} == PF.RCancel{PF.CsCancelled{}} : PF.Resp} & Laws.T(Laws.mq_cancelled(PF.find(q, qs))), w)
    case Some{PF.QBody{st, +pexp, +eps}}:
      match st:
        case PF.QRunning{+p}:
          ({==}, cancelled_after(q, qs, now, p, pexp, eps, e))
        case PF.QDone{}:
          Empty.absurd({PF.RCancel{PF.CsCancelled{}} == PF.RCancel{PF.CsCancelled{}} : PF.Resp} & Laws.T(Laws.mq_cancelled(PF.find(q, PF.upd(PF.OpCancel{q}, now, qs)))), w)
        case PF.QCancelled{}:
          Empty.absurd({PF.RCancel{PF.CsCancelled{}} == PF.RCancel{PF.CsCancelled{}} : PF.Resp} & Laws.T(Laws.mq_cancelled(PF.find(q, PF.upd(PF.OpCancel{q}, now, qs)))), w)
        case PF.QFailed{}:
          Empty.absurd({PF.RCancel{PF.CsNotCancellable{}} == PF.RCancel{PF.CsCancelled{}} : PF.Resp} & Laws.T(Laws.mq_cancelled(PF.find(q, PF.upd(PF.OpCancel{q}, now, qs)))), w)

def Laws.cancel_running(q, qs, next, now, w):
  cancel_run.fin(PF.find(q, qs), q, qs, next, now, w, {==})

# polling a cancelled body is an error, whatever it was before
def cpr_st(st: PF.QState, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, +pexp: Nat, +eps: List<&2, PF.Endpoint>)
  -> {PF.resp(PF.poll.fin(q, PF.upd(PF.OpCancel{q}, now, qs), next, now, Some{PF.cancel_body(PF.QBody{st, pexp, eps})})) == PF.RErr{} : PF.Resp}:
  match st:
    case PF.QRunning{p}:
      {==}
    case PF.QDone{}:
      {==}
    case PF.QCancelled{}:
      {==}
    case PF.QFailed{}:
      {==}

def cpr.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, e: {m == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> {PF.resp(PF.step_s(PF.APoll{q}, PF.state(PF.cancel.fin(q, qs, next, now, m)))) == PF.RErr{} : PF.Resp}:
  match m:
    case None{}:
      %e : {PF.resp(PF.poll.fin(q, qs, next, now, _)) == PF.RErr{} : PF.Resp}
      {==}
    case Some{PF.QBody{+st, +pexp, +eps}}:
      %find_upd(q, PF.OpCancel{q}, now, qs) : {PF.resp(PF.poll.fin(q, PF.upd(PF.OpCancel{q}, now, qs), next, now, _)) == PF.RErr{} : PF.Resp}
      %e : {PF.resp(PF.poll.fin(q, PF.upd(PF.OpCancel{q}, now, qs), next, now, upd_m(PF.OpCancel{q}, now, q, _))) == PF.RErr{} : PF.Resp}
      %cancel_self(q, now, PF.QBody{st, pexp, eps}) : {PF.resp(PF.poll.fin(q, PF.upd(PF.OpCancel{q}, now, qs), next, now, Some{_})) == PF.RErr{} : PF.Resp}
      cpr_st(st, q, qs, next, now, pexp, eps)

def Laws.cancelled_poll_rejected(q, qs, next, now):
  cpr.fin(PF.find(q, qs), q, qs, next, now, {==})

def cdr_st(st: PF.QState, +tk: Nat, +now: Nat, +pexp: Nat, +eps: List<&2, PF.Endpoint>)
  -> {PF.doget.fin(tk, now, Some{PF.cancel_body(PF.QBody{st, pexp, eps})}) == PF.RErr{} : PF.Resp}:
  match st:
    case PF.QRunning{p}:
      {==}
    case PF.QDone{}:
      {==}
    case PF.QCancelled{}:
      {==}
    case PF.QFailed{}:
      {==}

def cdr.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, e: {m == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> {PF.resp(PF.step_s(PF.ADoGet{q, tk}, PF.state(PF.cancel.fin(q, qs, next, now, m)))) == PF.RErr{} : PF.Resp}:
  match m:
    case None{}:
      %e : {PF.doget.fin(tk, now, _) == PF.RErr{} : PF.Resp}
      {==}
    case Some{PF.QBody{+st, +pexp, +eps}}:
      %find_upd(q, PF.OpCancel{q}, now, qs) : {PF.doget.fin(tk, now, _) == PF.RErr{} : PF.Resp}
      %e : {PF.doget.fin(tk, now, upd_m(PF.OpCancel{q}, now, q, _)) == PF.RErr{} : PF.Resp}
      %cancel_self(q, now, PF.QBody{st, pexp, eps}) : {PF.doget.fin(tk, now, Some{_}) == PF.RErr{} : PF.Resp}
      cdr_st(st, tk, now, pexp, eps)

def Laws.cancelled_doget_rejected(q, tk, qs, next, now):
  cdr.fin(PF.find(q, qs), q, tk, qs, next, now, {==})

def ci_st(st: PF.QState, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, +pexp: Nat, +eps: List<&2, PF.Endpoint>)
  -> {PF.resp(PF.cancel.fin(q, PF.upd(PF.OpCancel{q}, now, qs), next, now, Some{PF.cancel_body(PF.QBody{st, pexp, eps})})) == PF.resp(PF.cancel.fin(q, qs, next, now, Some{PF.QBody{st, pexp, eps}})) : PF.Resp}:
  match st:
    case PF.QRunning{p}:
      {==}
    case PF.QDone{}:
      {==}
    case PF.QCancelled{}:
      {==}
    case PF.QFailed{}:
      {==}

def ci.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, e: {m == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> {PF.resp(PF.step_s(PF.ACancel{q}, PF.state(PF.cancel.fin(q, qs, next, now, m)))) == PF.resp(PF.cancel.fin(q, qs, next, now, m)) : PF.Resp}:
  match m:
    case None{}:
      %e : {PF.resp(PF.cancel.fin(q, qs, next, now, _)) == PF.RErr{} : PF.Resp}
      {==}
    case Some{PF.QBody{+st, +pexp, +eps}}:
      %find_upd(q, PF.OpCancel{q}, now, qs) : {PF.resp(PF.cancel.fin(q, PF.upd(PF.OpCancel{q}, now, qs), next, now, _)) == PF.resp(PF.cancel.fin(q, qs, next, now, Some{PF.QBody{st, pexp, eps}})) : PF.Resp}
      %e : {PF.resp(PF.cancel.fin(q, PF.upd(PF.OpCancel{q}, now, qs), next, now, upd_m(PF.OpCancel{q}, now, q, _))) == PF.resp(PF.cancel.fin(q, qs, next, now, Some{PF.QBody{st, pexp, eps}})) : PF.Resp}
      %cancel_self(q, now, PF.QBody{st, pexp, eps}) : {PF.resp(PF.cancel.fin(q, PF.upd(PF.OpCancel{q}, now, qs), next, now, Some{_})) == PF.resp(PF.cancel.fin(q, qs, next, now, Some{PF.QBody{st, pexp, eps}})) : PF.Resp}
      ci_st(st, q, qs, next, now, pexp, eps)

def Laws.cancel_idempotent(q, qs, next, now):
  ci.fin(PF.find(q, qs), q, qs, next, now, {==})

def fnc.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.T(Laws.mq_failed(m)))
  -> {PF.resp(PF.cancel.fin(q, qs, next, now, m)) == PF.RCancel{PF.CsNotCancellable{}} : PF.Resp}:
  match m:
    case None{}:
      Empty.absurd({PF.RErr{} == PF.RCancel{PF.CsNotCancellable{}} : PF.Resp}, w)
    case Some{PF.QBody{st, pexp, eps}}:
      match st:
        case PF.QRunning{p}:
          Empty.absurd({PF.RCancel{PF.CsCancelled{}} == PF.RCancel{PF.CsNotCancellable{}} : PF.Resp}, w)
        case PF.QDone{}:
          Empty.absurd({PF.RCancel{PF.CsCancelled{}} == PF.RCancel{PF.CsNotCancellable{}} : PF.Resp}, w)
        case PF.QCancelled{}:
          Empty.absurd({PF.RCancel{PF.CsCancelled{}} == PF.RCancel{PF.CsNotCancellable{}} : PF.Resp}, w)
        case PF.QFailed{}:
          {==}

def Laws.failed_not_cancellable(q, qs, next, now, w):
  fnc.fin(PF.find(q, qs), q, qs, next, now, w)

def fpr.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat,
  w: Laws.T(Laws.mq_running(m)), e: {m == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> {PF.resp(PF.poll.fin(q, PF.upd(PF.OpFail{q}, now, qs), next, now, PF.find(q, PF.upd(PF.OpFail{q}, now, qs)))) == PF.RErr{} : PF.Resp}:
  match m:
    case None{}:
      Empty.absurd({PF.resp(PF.poll.fin(q, PF.upd(PF.OpFail{q}, now, qs), next, now, PF.find(q, PF.upd(PF.OpFail{q}, now, qs)))) == PF.RErr{} : PF.Resp}, w)
    case Some{PF.QBody{st, +pexp, +eps}}:
      match st:
        case PF.QRunning{+p}:
          %find_upd(q, PF.OpFail{q}, now, qs) : {PF.resp(PF.poll.fin(q, PF.upd(PF.OpFail{q}, now, qs), next, now, _)) == PF.RErr{} : PF.Resp}
          %e : {PF.resp(PF.poll.fin(q, PF.upd(PF.OpFail{q}, now, qs), next, now, upd_m(PF.OpFail{q}, now, q, _))) == PF.RErr{} : PF.Resp}
          %fail_self(q, now, PF.QBody{PF.QRunning{p}, pexp, eps}) : {PF.resp(PF.poll.fin(q, PF.upd(PF.OpFail{q}, now, qs), next, now, Some{_})) == PF.RErr{} : PF.Resp}
          {==}
        case PF.QDone{}:
          Empty.absurd({PF.resp(PF.poll.fin(q, PF.upd(PF.OpFail{q}, now, qs), next, now, PF.find(q, PF.upd(PF.OpFail{q}, now, qs)))) == PF.RErr{} : PF.Resp}, w)
        case PF.QCancelled{}:
          Empty.absurd({PF.resp(PF.poll.fin(q, PF.upd(PF.OpFail{q}, now, qs), next, now, PF.find(q, PF.upd(PF.OpFail{q}, now, qs)))) == PF.RErr{} : PF.Resp}, w)
        case PF.QFailed{}:
          Empty.absurd({PF.resp(PF.poll.fin(q, PF.upd(PF.OpFail{q}, now, qs), next, now, PF.find(q, PF.upd(PF.OpFail{q}, now, qs)))) == PF.RErr{} : PF.Resp}, w)

def Laws.failed_poll_rejected(q, qs, next, now, w):
  fpr.fin(PF.find(q, qs), q, qs, next, now, w, {==})

# Endpoint laws
# -------------

def dbe_v.fin(valid: Bool, w: Laws.T(valid)) -> {PF.doget_ep(valid) == PF.RData{} : PF.Resp}:
  match valid:
    case True{}:
      {==}
    case False{}:
      Empty.absurd({PF.RErr{} == PF.RData{} : PF.Resp}, w)

def dbe_ep.fin(e: Maybe<&2, Nat>, +now: Nat, w: Laws.T(Laws.ep_before(now, e))) -> {PF.doget_run(now, e) == PF.RData{} : PF.Resp}:
  match e:
    case None{}:
      Empty.absurd({PF.RErr{} == PF.RData{} : PF.Resp}, w)
    case Some{+exp}:
      dbe_v.fin(Nat.is_lt(now, exp), w)

def dbe.fin(m: Maybe<&2, PF.QBody>, +tk: Nat, +now: Nat, w: Laws.T(Laws.ep_valid(now, tk, m))) -> {PF.doget.fin(tk, now, m) == PF.RData{} : PF.Resp}:
  match m:
    case None{}:
      Empty.absurd({PF.RErr{} == PF.RData{} : PF.Resp}, w)
    case Some{PF.QBody{st, pexp, +eps}}:
      match st:
        case PF.QRunning{p}:
          dbe_ep.fin(PF.find_ep(tk, eps), now, w)
        case PF.QDone{}:
          dbe_ep.fin(PF.find_ep(tk, eps), now, w)
        case PF.QCancelled{}:
          Empty.absurd({PF.RErr{} == PF.RData{} : PF.Resp}, w)
        case PF.QFailed{}:
          Empty.absurd({PF.RErr{} == PF.RData{} : PF.Resp}, w)

def Laws.doget_before_expiry(q, tk, qs, next, now, w):
  dbe.fin(PF.find(q, qs), tk, now, w)

def dae_v.fin(valid: Bool, n: Laws.T(Bool.not(valid))) -> {PF.doget_ep(valid) == PF.RErr{} : PF.Resp}:
  match valid:
    case True{}:
      Empty.absurd({PF.RData{} == PF.RErr{} : PF.Resp}, n)
    case False{}:
      {==}

def dae_ep.fin(e: Maybe<&2, Nat>, +now: Nat, w: Laws.T(Laws.ep_after(now, e))) -> {PF.doget_run(now, e) == PF.RErr{} : PF.Resp}:
  match e:
    case None{}:
      Empty.absurd({PF.RErr{} == PF.RErr{} : PF.Resp}, w)
    case Some{+exp}:
      dae_v.fin(Nat.is_lt(now, exp), w)

def dae.fin(m: Maybe<&2, PF.QBody>, +tk: Nat, +now: Nat, w: Laws.T(Laws.ep_expired(now, tk, m))) -> {PF.doget.fin(tk, now, m) == PF.RErr{} : PF.Resp}:
  match m:
    case None{}:
      Empty.absurd({PF.RErr{} == PF.RErr{} : PF.Resp}, w)
    case Some{PF.QBody{st, pexp, +eps}}:
      match st:
        case PF.QRunning{p}:
          dae_ep.fin(PF.find_ep(tk, eps), now, w)
        case PF.QDone{}:
          dae_ep.fin(PF.find_ep(tk, eps), now, w)
        case PF.QCancelled{}:
          {==}
        case PF.QFailed{}:
          {==}

def Laws.doget_after_expiry(q, tk, qs, next, now, w):
  dae.fin(PF.find(q, qs), tk, now, w)

def rx_v.fin(valid: Bool, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, +exp: Nat, w: Laws.T(valid))
  -> Laws.T(Laws.ren_later(Some{exp}, PF.resp(PF.renew_ep_r(q, tk, qs, next, now, exp, valid)))):
  match valid:
    case True{}:
      renewed_later(exp, now)
    case False{}:
      Empty.absurd(Laws.T(Laws.ren_later(Some{exp}, PF.RErr{})), w)

def rx_ep.fin(e: Maybe<&2, Nat>, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.T(Laws.ep_before(now, e)))
  -> Laws.T(Laws.ren_later(e, PF.resp(PF.renew_run(q, tk, qs, next, now, e)))):
  match e:
    case None{}:
      Empty.absurd(Laws.T(Laws.ren_later(None{}, PF.RErr{})), w)
    case Some{+exp}:
      rx_v.fin(Nat.is_lt(now, exp), q, tk, qs, next, now, exp, w)

def rx.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.T(Laws.ep_valid(now, tk, m)))
  -> Laws.T(Laws.ren_later(Laws.exp_of(tk, m), PF.resp(PF.renew.fin(q, tk, qs, next, now, m)))):
  match m:
    case None{}:
      Empty.absurd(Laws.T(Laws.ren_later(None{}, PF.RErr{})), w)
    case Some{PF.QBody{st, pexp, +eps}}:
      match st:
        case PF.QRunning{p}:
          rx_ep.fin(PF.find_ep(tk, eps), q, tk, qs, next, now, w)
        case PF.QDone{}:
          rx_ep.fin(PF.find_ep(tk, eps), q, tk, qs, next, now, w)
        case PF.QCancelled{}:
          Empty.absurd(Laws.T(Laws.ren_later(PF.find_ep(tk, eps), PF.RErr{})), w)
        case PF.QFailed{}:
          Empty.absurd(Laws.T(Laws.ren_later(PF.find_ep(tk, eps), PF.RErr{})), w)

def Laws.renew_extends(q, tk, qs, next, now, w):
  rx.fin(PF.find(q, qs), q, tk, qs, next, now, w)

def rr_v.fin(valid: Bool, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, +exp: Nat, n: Laws.T(Bool.not(valid)))
  -> {PF.resp(PF.renew_ep_r(q, tk, qs, next, now, exp, valid)) == PF.RErr{} : PF.Resp}:
  match valid:
    case True{}:
      Empty.absurd({PF.REndpoint{tk, PF.renewed(exp, now)} == PF.RErr{} : PF.Resp}, n)
    case False{}:
      {==}

def rr_ep.fin(e: Maybe<&2, Nat>, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.T(Laws.ep_after(now, e)))
  -> {PF.resp(PF.renew_run(q, tk, qs, next, now, e)) == PF.RErr{} : PF.Resp}:
  match e:
    case None{}:
      Empty.absurd({PF.RErr{} == PF.RErr{} : PF.Resp}, w)
    case Some{+exp}:
      rr_v.fin(Nat.is_lt(now, exp), q, tk, qs, next, now, exp, w)

def rr.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.T(Laws.ep_expired(now, tk, m)))
  -> {PF.resp(PF.renew.fin(q, tk, qs, next, now, m)) == PF.RErr{} : PF.Resp}:
  match m:
    case None{}:
      Empty.absurd({PF.RErr{} == PF.RErr{} : PF.Resp}, w)
    case Some{PF.QBody{st, pexp, +eps}}:
      match st:
        case PF.QRunning{p}:
          rr_ep.fin(PF.find_ep(tk, eps), q, tk, qs, next, now, w)
        case PF.QDone{}:
          rr_ep.fin(PF.find_ep(tk, eps), q, tk, qs, next, now, w)
        case PF.QCancelled{}:
          {==}
        case PF.QFailed{}:
          {==}

def Laws.renew_expired_rejected(q, tk, qs, next, now, w):
  rr.fin(PF.find(q, qs), q, tk, qs, next, now, w)

# DoGet on the renewed body, over the query's state
def rdo_st(st: PF.QState, +tk: Nat, +now: Nat, +pexp: Nat, +eps: List<&2, PF.Endpoint>, +exp: Nat,
  wl: Laws.T(Laws.st_live(st)), ee: {Some{exp} == PF.find_ep(tk, eps) : Maybe<&2, Nat>})
  -> {PF.doget.fin(tk, now, Some{PF.QBody{st, pexp, PF.renew_ep(tk, now, eps)}}) == PF.RData{} : PF.Resp}:
  match st:
    case PF.QRunning{p}:
      %find_ep_renew(tk, now, eps) : {PF.doget_run(now, _) == PF.RData{} : PF.Resp}
      %ee : {PF.doget_run(now, ren_m(now, _)) == PF.RData{} : PF.Resp}
      %T_true(Nat.is_lt(now, PF.renewed(exp, now)), renewed_valid(exp, now)) : {PF.doget_ep(_) == PF.RData{} : PF.Resp}
      {==}
    case PF.QDone{}:
      %find_ep_renew(tk, now, eps) : {PF.doget_run(now, _) == PF.RData{} : PF.Resp}
      %ee : {PF.doget_run(now, ren_m(now, _)) == PF.RData{} : PF.Resp}
      %T_true(Nat.is_lt(now, PF.renewed(exp, now)), renewed_valid(exp, now)) : {PF.doget_ep(_) == PF.RData{} : PF.Resp}
      {==}
    case PF.QCancelled{}:
      Empty.absurd({PF.RErr{} == PF.RData{} : PF.Resp}, wl)
    case PF.QFailed{}:
      Empty.absurd({PF.RErr{} == PF.RData{} : PF.Resp}, wl)

def rdo_v.fin(valid: Bool, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, +st: PF.QState, +pexp: Nat, +eps: List<&2, PF.Endpoint>, +exp: Nat,
  w: Laws.T(valid), wl: Laws.T(Laws.st_live(st)), ee: {Some{exp} == PF.find_ep(tk, eps) : Maybe<&2, Nat>}, e: {Some{PF.QBody{st, pexp, eps}} == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> {PF.resp(PF.step_s(PF.ADoGet{q, tk}, PF.state(PF.renew_ep_r(q, tk, qs, next, now, exp, valid)))) == PF.RData{} : PF.Resp}:
  match valid:
    case True{}:
      %find_upd(q, PF.OpRenew{q, tk}, now, qs) : {PF.doget.fin(tk, now, _) == PF.RData{} : PF.Resp}
      %e : {PF.doget.fin(tk, now, upd_m(PF.OpRenew{q, tk}, now, q, _)) == PF.RData{} : PF.Resp}
      %renew_self(q, tk, now, PF.QBody{st, pexp, eps}) : {PF.doget.fin(tk, now, Some{_}) == PF.RData{} : PF.Resp}
      rdo_st(st, tk, now, pexp, eps, exp, wl, ee)
    case False{}:
      Empty.absurd({PF.doget.fin(tk, now, PF.find(q, qs)) == PF.RData{} : PF.Resp}, w)

def rdo_ep.fin(ep: Maybe<&2, Nat>, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, +st: PF.QState, +pexp: Nat, +eps: List<&2, PF.Endpoint>,
  w: Laws.T(Laws.ep_before(now, ep)), wl: Laws.T(Laws.st_live(st)), ee: {ep == PF.find_ep(tk, eps) : Maybe<&2, Nat>}, e: {Some{PF.QBody{st, pexp, eps}} == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> {PF.resp(PF.step_s(PF.ADoGet{q, tk}, PF.state(PF.renew_run(q, tk, qs, next, now, ep)))) == PF.RData{} : PF.Resp}:
  match ep:
    case None{}:
      Empty.absurd({PF.doget.fin(tk, now, PF.find(q, qs)) == PF.RData{} : PF.Resp}, w)
    case Some{+exp}:
      rdo_v.fin(Nat.is_lt(now, exp), q, tk, qs, next, now, st, pexp, eps, exp, w, wl, ee, e)

def rdo.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +tk: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat,
  w: Laws.T(Laws.ep_valid(now, tk, m)), e: {m == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> {PF.resp(PF.step_s(PF.ADoGet{q, tk}, PF.state(PF.renew.fin(q, tk, qs, next, now, m)))) == PF.RData{} : PF.Resp}:
  match m:
    case None{}:
      Empty.absurd({PF.doget.fin(tk, now, PF.find(q, qs)) == PF.RData{} : PF.Resp}, w)
    case Some{PF.QBody{st, +pexp, +eps}}:
      match st:
        case PF.QRunning{+p}:
          rdo_ep.fin(PF.find_ep(tk, eps), q, tk, qs, next, now, PF.QRunning{p}, pexp, eps, w, Unit{}, {==}, e)
        case PF.QDone{}:
          rdo_ep.fin(PF.find_ep(tk, eps), q, tk, qs, next, now, PF.QDone{}, pexp, eps, w, Unit{}, {==}, e)
        case PF.QCancelled{}:
          Empty.absurd({PF.doget.fin(tk, now, PF.find(q, qs)) == PF.RData{} : PF.Resp}, w)
        case PF.QFailed{}:
          Empty.absurd({PF.doget.fin(tk, now, PF.find(q, qs)) == PF.RData{} : PF.Resp}, w)

def Laws.renewed_doget_ok(q, tk, qs, next, now, w):
  rdo.fin(PF.find(q, qs), q, tk, qs, next, now, w, {==})

# Sample traces
# -------------

def Laws.trace_doget_fresh():
  {==}

def Laws.trace_doget_expired():
  {==}

def Laws.trace_completes():
  {==}

# Monotonicity
# ------------
# poll_monotone: a poll of q, any one request, and a poll of q again.
# The second poll sees either the same list (DoGet, a poll or cancel or
# renew that found nothing), the list with a new query in front
# (Start), or the list after one upd (everything else). For an upd,
# find_upd turns the second lookup into apply on the first lookup's
# body, and mono_apply is a case analysis on the op and the state.

# a PollInfo is at least itself
def mono_refl(r: PF.Resp) -> Laws.T(Laws.Resp.mono(r, r)):
  match r:
    case PF.RPoll{+p, d, +i}:
      join.fin(Nat.is_le(p, p), Laws.is_prefix(i, i), le_refl(p), prefix_refl(i))
    case PF.RCancel{c}:
      Unit{}
    case PF.RData{}:
      Unit{}
    case PF.REndpoint{tk, exp}:
      Unit{}
    case PF.RAck{}:
      Unit{}
    case PF.RErr{}:
      Unit{}

# two polls of running bodies, over both liveness verdicts
def mono_run2.fin(a1: Bool, a2: Bool, +q: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +qs2: List<&2, PF.Query>, +next2: Nat, +now2: Nat,
  +p1: Nat, +p2: Nat, +eps1: List<&2, PF.Endpoint>, +eps2: List<&2, PF.Endpoint>,
  wp: Laws.T(Nat.is_le(p1, p2)), wi: Laws.T(Laws.is_prefix(PF.tickets(eps1), PF.tickets(eps2))))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll_run(q, qs1, next1, now1, p1, eps1, a1)), PF.resp(PF.poll_run(q, qs2, next2, now2, p2, eps2, a2)))):
  match a1 a2:
    case True{} True{}:
      join.fin(Nat.is_le(p1, p2), Laws.is_prefix(PF.tickets(eps1), PF.tickets(eps2)), wp, wi)
    case True{} False{}:
      Unit{}
    case False{} True{}:
      Unit{}
    case False{} False{}:
      Unit{}

# a poll of a running body against an error
def mono_run_err.fin(a1: Bool, +q: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +p1: Nat, +eps1: List<&2, PF.Endpoint>)
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll_run(q, qs1, next1, now1, p1, eps1, a1)), PF.RErr{})):
  match a1:
    case True{}:
      Unit{}
    case False{}:
      Unit{}

# a poll of a running body against a complete one
def mono_run_done.fin(a1: Bool, +q: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +p1: Nat, +eps1: List<&2, PF.Endpoint>, +eps2: List<&2, PF.Endpoint>,
  wp: Laws.T(Nat.is_le(p1, PF.FULL())), wi: Laws.T(Laws.is_prefix(PF.tickets(eps1), PF.tickets(eps2))))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll_run(q, qs1, next1, now1, p1, eps1, a1)), PF.RPoll{PF.FULL(), None{}, PF.tickets(eps2)})):
  match a1:
    case True{}:
      join.fin(Nat.is_le(p1, PF.FULL()), Laws.is_prefix(PF.tickets(eps1), PF.tickets(eps2)), wp, wi)
    case False{}:
      Unit{}

# the same body polled twice, from any two states
def mono_same(+q: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +qs2: List<&2, PF.Query>, +next2: Nat, +now2: Nat, st: PF.QState, +pexp: Nat, +eps: List<&2, PF.Endpoint>)
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs1, next1, now1, Some{PF.QBody{st, pexp, eps}})), PF.resp(PF.poll.fin(q, qs2, next2, now2, Some{PF.QBody{st, pexp, eps}})))):
  match st:
    case PF.QRunning{+p}:
      mono_run2.fin(Nat.is_lt(now1, pexp), Nat.is_lt(now2, pexp), q, qs1, next1, now1, qs2, next2, now2, p, p, eps, eps, le_refl(p), prefix_refl(PF.tickets(eps)))
    case PF.QDone{}:
      prefix_refl(PF.tickets(eps))
    case PF.QCancelled{}:
      Unit{}
    case PF.QFailed{}:
      Unit{}

def mono_same_m(+q: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +qs2: List<&2, PF.Query>, +next2: Nat, +now2: Nat, m: Maybe<&2, PF.QBody>)
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs1, next1, now1, m)), PF.resp(PF.poll.fin(q, qs2, next2, now2, m)))):
  match m:
    case None{}:
      Unit{}
    case Some{PF.QBody{+st, +pexp, +eps}}:
      mono_same(q, qs1, next1, now1, qs2, next2, now2, st, pexp, eps)

# the body after a tick, over the verdict c of is_lt(p + STEP, FULL)
def mono_tick_run.fin(c: Bool, +q: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +qs2: List<&2, PF.Query>, +next2: Nat, +now2: Nat,
  +p: Nat, +pexp: Nat, +eps: List<&2, PF.Endpoint>, wm: Laws.T(Nat.is_le(p, PF.FULL())))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll_run(q, qs1, next1, now1, p, eps, Nat.is_lt(now1, pexp))),
       PF.resp(PF.poll.fin(q, qs2, next2, now2, Some{PF.QBody{PF.advance(Nat.add(p, PF.STEP()), c), pexp, PF.snoc(eps, PF.Endpoint{PF.length(eps), Nat.add(PF.ETTL(), now2)})}})))):
  match c:
    case True{}:
      mono_run2.fin(Nat.is_lt(now1, pexp), Nat.is_lt(now2, pexp), q, qs1, next1, now1, qs2, next2, now2, p, Nat.add(p, PF.STEP()),
        eps, PF.snoc(eps, PF.Endpoint{PF.length(eps), Nat.add(PF.ETTL(), now2)}), le_add(p, PF.STEP()), prefix_snoc(eps, PF.Endpoint{PF.length(eps), Nat.add(PF.ETTL(), now2)}))
    case False{}:
      mono_run_done.fin(Nat.is_lt(now1, pexp), q, qs1, next1, now1, p, eps, PF.snoc(eps, PF.Endpoint{PF.length(eps), Nat.add(PF.ETTL(), now2)}),
        wm, prefix_snoc(eps, PF.Endpoint{PF.length(eps), Nat.add(PF.ETTL(), now2)}))

def mono_tick(+q: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +qs2: List<&2, PF.Query>, +next2: Nat, +now2: Nat, st: PF.QState, +pexp: Nat, +eps: List<&2, PF.Endpoint>, wm: Laws.T(Laws.st_ok(st)))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs1, next1, now1, Some{PF.QBody{st, pexp, eps}})), PF.resp(PF.poll.fin(q, qs2, next2, now2, Some{PF.tick_body(now2, PF.QBody{st, pexp, eps})})))):
  match st:
    case PF.QRunning{+p}:
      mono_tick_run.fin(Nat.is_lt(Nat.add(p, PF.STEP()), PF.FULL()), q, qs1, next1, now1, qs2, next2, now2, p, pexp, eps, wm)
    case PF.QDone{}:
      prefix_refl(PF.tickets(eps))
    case PF.QCancelled{}:
      Unit{}
    case PF.QFailed{}:
      Unit{}

# the body after a touch: its descriptor is valid again
def mono_touch(+q: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +qs2: List<&2, PF.Query>, +next2: Nat, +now2: Nat, st: PF.QState, +pexp: Nat, +eps: List<&2, PF.Endpoint>)
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs1, next1, now1, Some{PF.QBody{st, pexp, eps}})), PF.resp(PF.poll.fin(q, qs2, next2, now2, Some{PF.touch_body(now2, PF.QBody{st, pexp, eps})})))):
  match st:
    case PF.QRunning{+p}:
      %pttl_valid(now2) : Laws.T(Laws.Resp.mono(PF.resp(PF.poll_run(q, qs1, next1, now1, p, eps, Nat.is_lt(now1, pexp))), PF.resp(PF.poll_run(q, qs2, next2, now2, p, eps, _))))
      mono_run2.fin(Nat.is_lt(now1, pexp), True{}, q, qs1, next1, now1, qs2, next2, now2, p, p, eps, eps, le_refl(p), prefix_refl(PF.tickets(eps)))
    case PF.QDone{}:
      prefix_refl(PF.tickets(eps))
    case PF.QCancelled{}:
      Unit{}
    case PF.QFailed{}:
      Unit{}

# the body after a cancel: an error, or unchanged
def mono_cancel_b(+q: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +qs2: List<&2, PF.Query>, +next2: Nat, +now2: Nat, st: PF.QState, +pexp: Nat, +eps: List<&2, PF.Endpoint>)
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs1, next1, now1, Some{PF.QBody{st, pexp, eps}})), PF.resp(PF.poll.fin(q, qs2, next2, now2, Some{PF.cancel_body(PF.QBody{st, pexp, eps})})))):
  match st:
    case PF.QRunning{+p}:
      mono_run_err.fin(Nat.is_lt(now1, pexp), q, qs1, next1, now1, p, eps)
    case PF.QDone{}:
      Unit{}
    case PF.QCancelled{}:
      Unit{}
    case PF.QFailed{}:
      Unit{}

# the body after a failure: an error, or unchanged
def mono_fail_b(+q: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +qs2: List<&2, PF.Query>, +next2: Nat, +now2: Nat, st: PF.QState, +pexp: Nat, +eps: List<&2, PF.Endpoint>)
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs1, next1, now1, Some{PF.QBody{st, pexp, eps}})), PF.resp(PF.poll.fin(q, qs2, next2, now2, Some{PF.fail_body(PF.QBody{st, pexp, eps})})))):
  match st:
    case PF.QRunning{+p}:
      mono_run_err.fin(Nat.is_lt(now1, pexp), q, qs1, next1, now1, p, eps)
    case PF.QDone{}:
      prefix_refl(PF.tickets(eps))
    case PF.QCancelled{}:
      Unit{}
    case PF.QFailed{}:
      Unit{}

# the body after a renewal: the same tickets
def mono_renew_b(+q: Nat, +tk: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +qs2: List<&2, PF.Query>, +next2: Nat, +now2: Nat, st: PF.QState, +pexp: Nat, +eps: List<&2, PF.Endpoint>)
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs1, next1, now1, Some{PF.QBody{st, pexp, eps}})), PF.resp(PF.poll.fin(q, qs2, next2, now2, Some{PF.renew_body(tk, now2, PF.QBody{st, pexp, eps})})))):
  match st:
    case PF.QRunning{+p}:
      mono_run2.fin(Nat.is_lt(now1, pexp), Nat.is_lt(now2, pexp), q, qs1, next1, now1, qs2, next2, now2, p, p, eps, PF.renew_ep(tk, now2, eps), le_refl(p), prefix_renew(tk, now2, eps))
    case PF.QDone{}:
      prefix_renew(tk, now2, eps)
    case PF.QCancelled{}:
      Unit{}
    case PF.QFailed{}:
      Unit{}

# a targeted op changed q's body or left it alone, over the verdict c of its id test
def mono_when.fin(c: Bool, +q: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +qs2: List<&2, PF.Query>, +next2: Nat, +now2: Nat, +st: PF.QState, +pexp: Nat, +eps: List<&2, PF.Endpoint>, +new: PF.QBody,
  wn: Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs1, next1, now1, Some{PF.QBody{st, pexp, eps}})), PF.resp(PF.poll.fin(q, qs2, next2, now2, Some{new})))))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs1, next1, now1, Some{PF.QBody{st, pexp, eps}})), PF.resp(PF.poll.fin(q, qs2, next2, now2, Some{PF.when_b(c, new, PF.QBody{st, pexp, eps})})))):
  match c:
    case True{}:
      wn
    case False{}:
      mono_same(q, qs1, next1, now1, qs2, next2, now2, st, pexp, eps)

# every op: the second poll is at least the first
def mono_apply(op: PF.Op, +q: Nat, +qs1: List<&2, PF.Query>, +next1: Nat, +now1: Nat, +qs2: List<&2, PF.Query>, +next2: Nat, +now2: Nat, +st: PF.QState, +pexp: Nat, +eps: List<&2, PF.Endpoint>, wm: Laws.T(Laws.st_ok(st)))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs1, next1, now1, Some{PF.QBody{st, pexp, eps}})), PF.resp(PF.poll.fin(q, qs2, next2, now2, Some{PF.apply(op, now2, q, PF.QBody{st, pexp, eps})})))):
  match op:
    case PF.OpTick{}:
      mono_tick(q, qs1, next1, now1, qs2, next2, now2, st, pexp, eps, wm)
    case PF.OpTouch{q2}:
      mono_when.fin(Nat.is_eq(q2, q), q, qs1, next1, now1, qs2, next2, now2, st, pexp, eps, PF.touch_body(now2, PF.QBody{st, pexp, eps}),
        mono_touch(q, qs1, next1, now1, qs2, next2, now2, st, pexp, eps))
    case PF.OpCancel{q2}:
      mono_when.fin(Nat.is_eq(q2, q), q, qs1, next1, now1, qs2, next2, now2, st, pexp, eps, PF.cancel_body(PF.QBody{st, pexp, eps}),
        mono_cancel_b(q, qs1, next1, now1, qs2, next2, now2, st, pexp, eps))
    case PF.OpFail{q2}:
      mono_when.fin(Nat.is_eq(q2, q), q, qs1, next1, now1, qs2, next2, now2, st, pexp, eps, PF.fail_body(PF.QBody{st, pexp, eps}),
        mono_fail_b(q, qs1, next1, now1, qs2, next2, now2, st, pexp, eps))
    case PF.OpRenew{q2, +tk}:
      mono_when.fin(Nat.is_eq(q2, q), q, qs1, next1, now1, qs2, next2, now2, st, pexp, eps, PF.renew_body(tk, now2, PF.QBody{st, pexp, eps}),
        mono_renew_b(q, tk, qs1, next1, now1, qs2, next2, now2, st, pexp, eps))

# a poll before and after an update of the query list
def mono_upd.fin(m: Maybe<&2, PF.QBody>, +q: Nat, +op: PF.Op, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, +now2: Nat,
  wm: Laws.T(mst_ok(m)), e: {m == PF.find(q, qs) : Maybe<&2, PF.QBody>})
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs, next, now, m)), PF.resp(PF.poll.fin(q, PF.upd(op, now2, qs), next, now2, PF.find(q, PF.upd(op, now2, qs)))))):
  match m:
    case None{}:
      Unit{}
    case Some{PF.QBody{+st, +pexp, +eps}}:
      %find_upd(q, op, now2, qs) : Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs, next, now, Some{PF.QBody{st, pexp, eps}})), PF.resp(PF.poll.fin(q, PF.upd(op, now2, qs), next, now2, _))))
      %e : Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs, next, now, Some{PF.QBody{st, pexp, eps}})), PF.resp(PF.poll.fin(q, PF.upd(op, now2, qs), next, now2, upd_m(op, now2, q, _)))))
      mono_apply(op, q, qs, next, now, PF.upd(op, now2, qs), next, now2, st, pexp, eps, wm)

def mono_upd(+q: Nat, +op: PF.Op, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, +now2: Nat, w: Laws.Inv(qs, next))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll(q, qs, next, now)), PF.resp(PF.poll(q, PF.upd(op, now2, qs), next, now2)))):
  mono_upd.fin(PF.find(q, qs), q, op, qs, next, now, now2, find_ok(q, qs, next)(w), {==})

# the intermediate request is a poll of q2
def mono_poll_run.fin(alive: Bool, +q2: Nat, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, +p2: Nat, +eps2: List<&2, PF.Endpoint>, w: Laws.Inv(qs, next))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll(q, qs, next, now)), PF.resp(PF.step_s(PF.APoll{q}, PF.state(PF.poll_run(q2, qs, next, now, p2, eps2, alive)))))):
  match alive:
    case True{}:
      mono_upd(q, PF.OpTouch{q2}, qs, next, now, now, w)
    case False{}:
      mono_upd(q, PF.OpCancel{q2}, qs, next, now, now, w)

def mono_poll.fin(m2: Maybe<&2, PF.QBody>, +q2: Nat, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.Inv(qs, next))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll(q, qs, next, now)), PF.resp(PF.step_s(PF.APoll{q}, PF.state(PF.poll.fin(q2, qs, next, now, m2)))))):
  match m2:
    case None{}:
      mono_refl(PF.resp(PF.poll(q, qs, next, now)))
    case Some{PF.QBody{st2, +pexp2, +eps2}}:
      match st2:
        case PF.QRunning{+p2}:
          mono_poll_run.fin(Nat.is_lt(now, pexp2), q2, q, qs, next, now, p2, eps2, w)
        case PF.QDone{}:
          mono_refl(PF.resp(PF.poll(q, qs, next, now)))
        case PF.QCancelled{}:
          mono_refl(PF.resp(PF.poll(q, qs, next, now)))
        case PF.QFailed{}:
          mono_refl(PF.resp(PF.poll(q, qs, next, now)))

# the intermediate request is a cancel of q2
def mono_cancel.fin(m2: Maybe<&2, PF.QBody>, +q2: Nat, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.Inv(qs, next))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll(q, qs, next, now)), PF.resp(PF.step_s(PF.APoll{q}, PF.state(PF.cancel.fin(q2, qs, next, now, m2)))))):
  match m2:
    case None{}:
      mono_refl(PF.resp(PF.poll(q, qs, next, now)))
    case Some{PF.QBody{st2, pexp2, eps2}}:
      mono_upd(q, PF.OpCancel{q2}, qs, next, now, now, w)

# the intermediate request is a renewal of (q2, tk)
def mono_renew_v.fin(valid: Bool, +q2: Nat, +tk: Nat, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, +exp: Nat, w: Laws.Inv(qs, next))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll(q, qs, next, now)), PF.resp(PF.step_s(PF.APoll{q}, PF.state(PF.renew_ep_r(q2, tk, qs, next, now, exp, valid)))))):
  match valid:
    case True{}:
      mono_upd(q, PF.OpRenew{q2, tk}, qs, next, now, now, w)
    case False{}:
      mono_refl(PF.resp(PF.poll(q, qs, next, now)))

def mono_renew_ep.fin(ep: Maybe<&2, Nat>, +q2: Nat, +tk: Nat, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.Inv(qs, next))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll(q, qs, next, now)), PF.resp(PF.step_s(PF.APoll{q}, PF.state(PF.renew_run(q2, tk, qs, next, now, ep)))))):
  match ep:
    case None{}:
      mono_refl(PF.resp(PF.poll(q, qs, next, now)))
    case Some{+exp}:
      mono_renew_v.fin(Nat.is_lt(now, exp), q2, tk, q, qs, next, now, exp, w)

def mono_renew.fin(m2: Maybe<&2, PF.QBody>, +q2: Nat, +tk: Nat, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, w: Laws.Inv(qs, next))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll(q, qs, next, now)), PF.resp(PF.step_s(PF.APoll{q}, PF.state(PF.renew.fin(q2, tk, qs, next, now, m2)))))):
  match m2:
    case None{}:
      mono_refl(PF.resp(PF.poll(q, qs, next, now)))
    case Some{PF.QBody{st2, pexp2, +eps2}}:
      match st2:
        case PF.QRunning{p2}:
          mono_renew_ep.fin(PF.find_ep(tk, eps2), q2, tk, q, qs, next, now, w)
        case PF.QDone{}:
          mono_renew_ep.fin(PF.find_ep(tk, eps2), q2, tk, q, qs, next, now, w)
        case PF.QCancelled{}:
          mono_refl(PF.resp(PF.poll(q, qs, next, now)))
        case PF.QFailed{}:
          mono_refl(PF.resp(PF.poll(q, qs, next, now)))

# next <= q from q == next
def le_of_eq(+q: Nat, +next: Nat, eqn: {q == next : Nat}) -> Laws.T(Nat.is_le(next, q)):
  %eqn : Laws.T(Nat.is_le(_, q))
  le_refl(q)

# the intermediate request is a start: the new query is q itself (then
# the first poll found nothing, by freshness) or another one
def mono_start.fin(c: Bool, +q: Nat, +qs: List<&2, PF.Query>, +next: Nat, +now: Nat, e: {c == Nat.is_eq(q, next) : Bool}, w: Laws.Inv(qs, next))
  -> Laws.T(Laws.Resp.mono(PF.resp(PF.poll(q, qs, next, now)), PF.resp(PF.poll.fin(q, PF.Query{next, PF.fresh(now)} <> qs, 1n+next, now, PF.pick_q(c, PF.fresh(now), PF.find(q, qs)))))):
  match c:
    case True{}:
      %find_unknown(q, qs, next, le_of_eq(q, next, eq_sound(q, next, e)))(w) : Laws.T(Laws.Resp.mono(PF.resp(PF.poll.fin(q, qs, next, now, _)), PF.resp(PF.poll.fin(q, PF.Query{next, PF.fresh(now)} <> qs, 1n+next, now, Some{PF.fresh(now)}))))
      Unit{}
    case False{}:
      mono_same_m(q, qs, next, now, PF.Query{next, PF.fresh(now)} <> qs, 1n+next, now, PF.find(q, qs))

def Laws.poll_monotone(a, q, qs, next, now, w):
  match a:
    case PF.AStart{}:
      mono_start.fin(Nat.is_eq(q, next), q, qs, next, now, {==}, w)
    case PF.APoll{+q2}:
      mono_poll.fin(PF.find(q2, qs), q2, q, qs, next, now, w)
    case PF.ACancel{+q2}:
      mono_cancel.fin(PF.find(q2, qs), q2, q, qs, next, now, w)
    case PF.ADoGet{q2, tk}:
      mono_refl(PF.resp(PF.poll(q, qs, next, now)))
    case PF.ARenew{+q2, +tk}:
      mono_renew.fin(PF.find(q2, qs), q2, tk, q, qs, next, now, w)
    case PF.ATick{}:
      mono_upd(q, PF.OpTick{}, qs, next, now, 1n+now, w)
    case PF.AFail{+q2}:
      mono_upd(q, PF.OpFail{q2}, qs, next, now, now, w)
