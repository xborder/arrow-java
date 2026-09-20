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

# The proofs. Imports the model (as PS) and the claims (as Laws) and
# fills every law they state; `bend PROOF.bend` is the whole check.

import Base
import ./main.bend as PS
import ./LAWS.bend as Laws

# Lemmas on has and remove
# ------------------------

# removing h leaves no h: the step case, with the verdict c of
# Nat.is_eq(x, h) as a parameter, since a computed value cannot be
# matched in place
def has_remove.fin(+h: Nat, +x: Nat, -t: List<&2, Nat>,
  rec: {False{} == PS.has(h, PS.remove(h, t)) : Bool},
  c: Bool, e: {c == Nat.is_eq(x, h) : Bool})
  -> {False{} == PS.has(h, PS.put(x, PS.remove(h, t), c)) : Bool}:
  match c:
    case True{}:
      rec
    case False{}:
      %e : {False{} == (_ || PS.has(h, PS.remove(h, t))) : Bool}
      rec

def has_remove(+h: Nat, xs: List<&2, Nat>) -> {False{} == PS.has(h, PS.remove(h, xs)) : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{+x, t}:
      has_remove.fin(h, x, t, has_remove(h, t), Nat.is_eq(x, h), {==})

# a nat equals itself
def eq_refl(n: Nat) -> {True{} == Nat.is_eq(n, n) : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      eq_refl(p)

# Laws
# ----

def Laws.closed_never_executes(h, live, next):
  %has_remove(h, live) : {PS.resp(PS.exec(h, PS.remove(h, live), next, _)) == PS.RErr{} : PS.Resp}
  {==}

def Laws.created_handle_executes(live, next):
  %eq_refl(next) : {PS.resp(PS.exec(next, next <> live, 1n+next, (_ || PS.has(next, live)))) == PS.ROk{next} : PS.Resp}
  {==}

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

# Lemmas on Nat
# -------------

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

# h < n implies n != h
def lt_ne(h: Nat, n: Nat, w: Laws.T(Nat.is_lt(h, n))) -> {False{} == Nat.is_eq(n, h) : Bool}:
  match h n:
    case 0n 0n:
      Empty.absurd({False{} == Nat.is_eq(0n, 0n) : Bool}, w)
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      Empty.absurd({False{} == Nat.is_eq(0n, 1n+p) : Bool}, w)
    case 1n+p 1n+q:
      lt_ne(p, q, w)

# Lemmas on all_lt
# ----------------

# a bound below n is a bound below n + 1
def mono(xs: List<&2, Nat>, +n: Nat, w: Laws.T(PS.all_lt(xs, n))) -> Laws.T(PS.all_lt(xs, 1n+n)):
  match xs:
    case Nil{}:
      Unit{}
    case Con{+x, +t}:
      split.fin(Nat.is_lt(x, n), PS.all_lt(t, n), w, Laws.T(PS.all_lt(x <> t, 1n+n)),
        wx => wt => join.fin(Nat.is_lt(x, 1n+n), PS.all_lt(t, 1n+n), lt_mono(x, n, wx), mono(t, n, wt)))

# removing keeps a bound: the step case, over the verdict c of the removal
def remove_lt.fin(c: Bool, +x: Nat, -r: List<&2, Nat>, +n: Nat, wx: Laws.T(Nat.is_lt(x, n)), wr: Laws.T(PS.all_lt(r, n)))
  -> Laws.T(PS.all_lt(PS.put(x, r, c), n)):
  match c:
    case True{}:
      wr
    case False{}:
      join.fin(Nat.is_lt(x, n), PS.all_lt(r, n), wx, wr)

def remove_lt(+h: Nat, xs: List<&2, Nat>, +n: Nat, w: Laws.T(PS.all_lt(xs, n))) -> Laws.T(PS.all_lt(PS.remove(h, xs), n)):
  match xs:
    case Nil{}:
      Unit{}
    case Con{+x, +t}:
      split.fin(Nat.is_lt(x, n), PS.all_lt(t, n), w, Laws.T(PS.all_lt(PS.remove(h, x <> t), n)),
        wx => wt => remove_lt.fin(Nat.is_eq(x, h), x, PS.remove(h, t), n, wx, remove_lt(h, t, n, wt)))

# a live handle is below the bound: the step case, over the verdict c
# of Nat.is_eq(x, h); rec is the induction hypothesis on the tail
def has_lt.fin(+h: Nat, +x: Nat, -t: List<&2, Nat>, +n: Nat,
  rec: Laws.T(PS.has(h, t)) -> Laws.T(PS.all_lt(t, n)) -> Laws.T(Nat.is_lt(h, n)),
  c: Bool, e: {c == Nat.is_eq(x, h) : Bool}, hw: Laws.T(c || PS.has(h, t)), w: Laws.T(PS.all_lt(x <> t, n)))
  -> Laws.T(Nat.is_lt(h, n)):
  match c:
    case True{}:
      split.fin(Nat.is_lt(x, n), PS.all_lt(t, n), w, Laws.T(Nat.is_lt(h, n)),
        wx => wt =>
          %eq_sound(x, h, e) : Laws.T(Nat.is_lt(_, n))
          wx)
    case False{}:
      split.fin(Nat.is_lt(x, n), PS.all_lt(t, n), w, Laws.T(Nat.is_lt(h, n)),
        wx => wt => rec(hw, wt))

def has_lt(+h: Nat, xs: List<&2, Nat>, +n: Nat) -> Laws.T(PS.has(h, xs)) -> Laws.T(PS.all_lt(xs, n)) -> Laws.T(Nat.is_lt(h, n)):
  match xs:
    case Nil{}:
      hw => w => Empty.absurd(Laws.T(Nat.is_lt(h, n)), hw)
    case Con{+x, +t}:
      hw => w => has_lt.fin(h, x, t, n, has_lt(h, t, n), Nat.is_eq(x, h), {==}, hw, w)

# Invariant laws
# --------------

def Laws.fresh_start():
  Unit{}

# the Bind arm, over the verdict c of has(h, live)
def kept_bind.fin(c: Bool, +h: Nat, +live: List<&2, Nat>, +next: Nat, w: Laws.Fresh(live, next))
  -> Laws.Fresh_s(PS.state(PS.bind(h, live, next, c))):
  match c:
    case True{}:
      join.fin(Nat.is_lt(next, 1n+next), PS.all_lt(PS.remove(h, live), 1n+next),
        lt_succ(next), mono(PS.remove(h, live), next, remove_lt(h, live, next, w)))
    case False{}:
      w

# the Exec arm: the state is unchanged either way
def kept_exec.fin(c: Bool, +h: Nat, +live: List<&2, Nat>, +next: Nat, w: Laws.Fresh(live, next))
  -> Laws.Fresh_s(PS.state(PS.exec(h, live, next, c))):
  match c:
    case True{}:
      w
    case False{}:
      w

def Laws.fresh_kept(a, live, next, w):
  match a:
    case PS.ACreate{}:
      join.fin(Nat.is_lt(next, 1n+next), PS.all_lt(live, 1n+next), lt_succ(next), mono(live, next, w))
    case PS.ABind{+h}:
      kept_bind.fin(PS.has(h, live), h, live, next, w)
    case PS.AExec{+h}:
      kept_exec.fin(PS.has(h, live), h, live, next, w)
    case PS.AClose{+h}:
      remove_lt(h, live, next, w)

# Stale handles
# -------------

# over the verdict c of has(h, live), with its equation
def stale.fin(+h: Nat, +live: List<&2, Nat>, +next: Nat, c: Bool, e: {c == PS.has(h, live) : Bool}, w: Laws.Fresh(live, next))
  -> {PS.resp(PS.step_s(PS.AExec{h}, PS.state(PS.bind(h, live, next, c)))) == PS.RErr{} : PS.Resp}:
  match c:
    case True{}:
      ne = lt_ne(h, next, has_lt(h, live, next)(true_T(PS.has(h, live), e), w))
      %has_remove(h, live) : {PS.resp(PS.exec(h, next <> PS.remove(h, live), 1n+next, (Nat.is_eq(next, h) || _))) == PS.RErr{} : PS.Resp}
      %ne : {PS.resp(PS.exec(h, next <> PS.remove(h, live), 1n+next, (_ || False{}))) == PS.RErr{} : PS.Resp}
      {==}
    case False{}:
      %e : {PS.resp(PS.exec(h, live, next, _)) == PS.RErr{} : PS.Resp}
      {==}

def Laws.stale_handle_rejected(h, live, next, w):
  stale.fin(h, live, next, PS.has(h, live), {==}, w)
