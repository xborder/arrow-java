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

# The proofs. Imports the model (as M) and the claims (as Laws) and
# fills every law they state; `bend PROOF.bend` is the whole check.
#
# Conventions. A rewrite `%e : P` with `e : {a == b : T}` turns a goal
# matching P[b] into P[a], so equations are oriented "new == old". A
# computed value cannot be matched, so each case split on one goes
# through a `.fin` helper that takes the value as a parameter together
# with an equation tying it to the computation (the "inspect" idiom).

import Base
import ./main.bend as M
import ./LAWS.bend as Laws

# Proof kit
# ---------

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

def false_ne_true(e: {False{} == True{} : Bool}) -> Empty:
  true_ne_false(Equal.sym(Bool, False{}, True{}, e))

def zero_ne_succ.motive(n: Nat) -> Type:
  match n:
    case 0n:
      Unit
    case 1n+p:
      Empty

# 0 and a successor clash
def zero_ne_succ(-p: Nat, e: {0n == 1n+p : Nat}) -> Empty:
  %e : zero_ne_succ.motive(_)
  Unit{}

# T(True{}) from an equation, and back
def true_T(b: Bool, e: {True{} == b : Bool}) -> Laws.T(b):
  %e : Laws.T(_)
  Unit{}

def T_true(b: Bool, w: Laws.T(b)) -> {True{} == b : Bool}:
  match b:
    case True{}:
      {==}
    case False{}:
      Empty.absurd({True{} == False{} : Bool}, w)

# T(b) refutes b == False
def T_not_false(b: Bool, w: Laws.T(b), e: {b == False{} : Bool}) -> Empty:
  match b:
    case True{}:
      true_ne_false(e)
    case False{}:
      w

# Lemmas on Nat
# -------------

def add_zero(+a: Nat) -> {a == Nat.add(a, 0n) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %add_zero(p) : {1n+p == 1n+_ : Nat}
      {==}

def add_succ(+a: Nat, +b: Nat) -> {1n+Nat.add(a, b) == Nat.add(a, 1n+b) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %add_succ(p, b) : {2n+Nat.add(p, b) == 1n+_ : Nat}
      {==}

def add_comm(+a: Nat, +b: Nat) -> {Nat.add(a, b) == Nat.add(b, a) : Nat}:
  match a:
    case 0n:
      add_zero(b)
    case 1n+p:
      %add_succ(b, p) : {1n+Nat.add(p, b) == _ : Nat}
      %Equal.sym(Nat, Nat.add(p, b), Nat.add(b, p), add_comm(p, b)) : {1n+_ == 1n+Nat.add(b, p) : Nat}
      {==}

# a + (b + c) == (a + b) + c
def add_assoc(+a: Nat, +b: Nat, +c: Nat) -> {Nat.add(a, Nat.add(b, c)) == Nat.add(Nat.add(a, b), c) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      %add_assoc(p, b, c) : {1n+Nat.add(p, Nat.add(b, c)) == 1n+_ : Nat}
      {==}

def sub_zero(+a: Nat) -> {a == Nat.sub(a, 0n) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      {==}

# (a + b) - b == a
def sub_add_cancel(+a: Nat, +b: Nat) -> {a == Nat.sub(Nat.add(a, b), b) : Nat}:
  match b:
    case 0n:
      %add_zero(a) : {a == Nat.sub(_, 0n) : Nat}
      sub_zero(a)
    case 1n+q:
      %add_succ(a, q) : {a == Nat.sub(_, 1n+q) : Nat}
      sub_add_cancel(a, q)

def sub_self(+a: Nat) -> {0n == Nat.sub(a, a) : Nat}:
  match a:
    case 0n:
      {==}
    case 1n+p:
      sub_self(p)

def pred(n: Nat) -> Nat:
  match n:
    case 0n:
      0n
    case 1n+p:
      p

def succ_inj(+a: Nat, +b: Nat, e: {1n+a == 1n+b : Nat}) -> {a == b : Nat}:
  Equal.cong(Nat, Nat, pred, 1n+a, 1n+b, e)

def add_succ_both(+a: Nat, +b: Nat, +q: Nat, e: {Nat.add(a, 1n+q) == Nat.add(b, 1n+q) : Nat}) -> {1n+Nat.add(a, q) == 1n+Nat.add(b, q) : Nat}:
  %Equal.sym(Nat, 1n+Nat.add(a, q), Nat.add(a, 1n+q), add_succ(a, q)) : {_ == 1n+Nat.add(b, q) : Nat}
  %Equal.sym(Nat, 1n+Nat.add(b, q), Nat.add(b, 1n+q), add_succ(b, q)) : {Nat.add(a, 1n+q) == _ : Nat}
  e

# a + c == b + c implies a == b
def add_cancel_r(+a: Nat, +b: Nat, +c: Nat, e: {Nat.add(a, c) == Nat.add(b, c) : Nat}) -> {a == b : Nat}:
  match c:
    case 0n:
      %Equal.sym(Nat, a, Nat.add(a, 0n), add_zero(a)) : {_ == b : Nat}
      %Equal.sym(Nat, b, Nat.add(b, 0n), add_zero(b)) : {Nat.add(a, 0n) == _ : Nat}
      e
    case 1n+q:
      add_cancel_r(a, b, q, succ_inj(Nat.add(a, q), Nat.add(b, q), add_succ_both(a, b, q, e)))

def eq_refl(+n: Nat) -> {True{} == Nat.is_eq(n, n) : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      eq_refl(p)

def lt_irrefl(+n: Nat) -> {False{} == Nat.is_lt(n, n) : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      lt_irrefl(p)

# a < b implies not b <= a
def lt_not_le(+a: Nat, +b: Nat, w: Laws.T(Nat.is_lt(a, b))) -> {False{} == Nat.is_le(b, a) : Bool}:
  match a b:
    case 0n 0n:
      Empty.absurd({False{} == Nat.is_le(0n, 0n) : Bool}, w)
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      Empty.absurd({False{} == Nat.is_le(0n, 1n+p) : Bool}, w)
    case 1n+p 1n+q:
      lt_not_le(p, q, w)

# a < b implies not b < a
def lt_asym(+a: Nat, +b: Nat, w: Laws.T(Nat.is_lt(a, b))) -> {False{} == Nat.is_lt(b, a) : Bool}:
  match a b:
    case 0n 0n:
      Empty.absurd({False{} == Nat.is_lt(0n, 0n) : Bool}, w)
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      Empty.absurd({False{} == Nat.is_lt(0n, 1n+p) : Bool}, w)
    case 1n+p 1n+q:
      lt_asym(p, q, w)

# a < b implies b - a is positive: b - a == 1 + (b - (1 + a))
def lt_sub_succ(+a: Nat, +b: Nat, w: Laws.T(Nat.is_lt(a, b))) -> {1n+Nat.sub(b, 1n+a) == Nat.sub(b, a) : Nat}:
  match a b:
    case 0n 0n:
      Empty.absurd({1n+Nat.sub(0n, 1n) == Nat.sub(0n, 0n) : Nat}, w)
    case 0n 1n+q:
      %sub_zero(q) : {1n+_ == 1n+q : Nat}
      {==}
    case 1n+p 0n:
      Empty.absurd({1n+Nat.sub(0n, 2n+p) == Nat.sub(0n, 1n+p) : Nat}, w)
    case 1n+p 1n+q:
      lt_sub_succ(p, q, w)

# a - b == 1 + q implies a is positive, so is_eq(a, 0) is False
def sub_pos_pos(+a: Nat, +b: Nat, -q: Nat, e: {1n+q == Nat.sub(a, b) : Nat}) -> {Nat.is_eq(a, 0n) == False{} : Bool}:
  match a b:
    case 0n 0n:
      Empty.absurd({Nat.is_eq(0n, 0n) == False{} : Bool}, zero_ne_succ(q, Equal.sym(Nat, 1n+q, 0n, e)))
    case 0n 1n+bp:
      Empty.absurd({Nat.is_eq(0n, 0n) == False{} : Bool}, zero_ne_succ(q, Equal.sym(Nat, 1n+q, 0n, e)))
    case 1n+ap 0n:
      {==}
    case 1n+ap 1n+bp:
      {==}

# a true Nat.is_eq(refs, 0n) means refs is 0
def refs_zero(+refs: Nat, e: {True{} == Nat.is_eq(refs, 0n) : Bool}) -> {0n == refs : Nat}:
  match refs:
    case 0n:
      {==}
    case 1n+r:
      Empty.absurd({0n == 1n+r : Nat}, true_ne_false(e))

# not refs < n with n >= 1 means refs is positive
def ge_succ_pos(+refs: Nat, -m: Nat, e: {False{} == Nat.is_lt(refs, 1n+m) : Bool}) -> {Nat.is_eq(refs, 0n) == False{} : Bool}:
  match refs:
    case 0n:
      Empty.absurd({Nat.is_eq(0n, 0n) == False{} : Bool}, false_ne_true(e))
    case 1n+r:
      {==}

# n < refs means refs is positive
def lt_pos(-m: Nat, +refs: Nat, w: Laws.T(Nat.is_lt(1n+m, refs))) -> {Nat.is_eq(refs, 0n) == False{} : Bool}:
  match refs:
    case 0n:
      Empty.absurd({Nat.is_eq(0n, 0n) == False{} : Bool}, w)
    case 1n+r:
      {==}

# transport along an equation
def cast(-A: Type, -P: A -> Type, -a: A, -b: A, e: {a == b : A}, w: P(a)) -> P(b):
  %e : P(_)
  w

# Lemmas on the allocation list
# -----------------------------

# the consistency of a lookup result
def AllocOkM(m: Maybe<&2, M.Alloc>) -> Data:
  match m:
    case None{}:
      Unit
    case Some{a}:
      Laws.AllocOk(a)

# a lookup in a consistent list finds a consistent allocation: the step
# case, over the verdict c of Nat.is_eq(aid, id)
def lookup_ok.fin(+id: Nat, +aid: Nat, +size: Nat, +refs: Nat, +freed: Bool, -t: List<&2, M.Alloc>,
  w: Laws.Consistent(M.Alloc{aid, size, refs, freed} <> t),
  rec: Laws.Consistent(t) -> AllocOkM(M.lookup(id, t)), c: Bool)
  -> AllocOkM(M.lookup.at(M.Alloc{aid, size, refs, freed}, M.lookup(id, t), c)):
  Laws.Conj{ok, wt} = w
  match c:
    case True{}:
      ok
    case False{}:
      rec(wt)

def lookup_ok(+id: Nat, xs: List<&2, M.Alloc>) -> Laws.Consistent(xs) -> AllocOkM(M.lookup(id, xs)):
  match xs:
    case Nil{}:
      w => Unit{}
    case Con{M.Alloc{+aid, +size, +refs, +freed}, +t}:
      w => lookup_ok.fin(id, aid, size, refs, freed, t, w, lookup_ok(id, t), Nat.is_eq(aid, id))

# replacing an allocation by a consistent one keeps the list consistent
def replace_ok.fin(+id: Nat, +a2: M.Alloc, +aid: Nat, +size: Nat, +refs: Nat, +freed: Bool, +t: List<&2, M.Alloc>,
  w: Laws.Consistent(M.Alloc{aid, size, refs, freed} <> t), w2: Laws.AllocOk(a2),
  rec: Laws.Consistent(t) -> Laws.AllocOk(a2) -> Laws.Consistent(M.replace(id, a2, t)), c: Bool)
  -> Laws.Consistent(M.replace.at(M.Alloc{aid, size, refs, freed}, a2, t, M.replace(id, a2, t), c)):
  Laws.Conj{ok, wt} = w
  match c:
    case True{}:
      Laws.Conj{w2, wt}
    case False{}:
      Laws.Conj{ok, rec(wt, w2)}

def replace_ok(+id: Nat, +a2: M.Alloc, xs: List<&2, M.Alloc>) -> Laws.Consistent(xs) -> Laws.AllocOk(a2) -> Laws.Consistent(M.replace(id, a2, xs)):
  match xs:
    case Nil{}:
      w => w2 => Unit{}
    case Con{M.Alloc{+aid, +size, +refs, +freed}, +t}:
      w => w2 => replace_ok.fin(id, a2, aid, size, refs, freed, t, w, w2, replace_ok(id, a2, t), Nat.is_eq(aid, id))

# the bytes of a lookup result, and the bytes of its replacement
def mbytes(m: Maybe<&2, M.Alloc>) -> Nat:
  match m:
    case None{}:
      0n
    case Some{a}:
      M.bytes(a)

def pick(m: Maybe<&2, M.Alloc>, a2: M.Alloc) -> Nat:
  match m:
    case None{}:
      0n
    case Some{a}:
      M.bytes(a2)

# (x + y) + z == (z + y) + x
def add_rot(+x: Nat, +y: Nat, +z: Nat) -> {Nat.add(Nat.add(x, y), z) == Nat.add(Nat.add(z, y), x) : Nat}:
  %add_assoc(x, y, z) : {_ == Nat.add(Nat.add(z, y), x) : Nat}
  %add_comm(x, Nat.add(z, y)) : {Nat.add(x, Nat.add(y, z)) == _ : Nat}
  %add_comm(y, z) : {Nat.add(x, Nat.add(y, z)) == Nat.add(x, _) : Nat}
  {==}

# replacing the found allocation moves its bytes out and a2's bytes in:
# live_bytes(xs) + bytes(a2) == live_bytes(replace(id, a2, xs)) + bytes(found)
def replace_bytes.fin(+id: Nat, +a2: M.Alloc, +aid: Nat, +size: Nat, +refs: Nat, +freed: Bool, +t: List<&2, M.Alloc>,
  rec: {Nat.add(M.live_bytes(t), pick(M.lookup(id, t), a2)) == Nat.add(M.live_bytes(M.replace(id, a2, t)), mbytes(M.lookup(id, t))) : Nat},
  c: Bool, e: {c == Nat.is_eq(aid, id) : Bool})
  -> {Nat.add(M.live_bytes(M.Alloc{aid, size, refs, freed} <> t), pick(M.lookup.at(M.Alloc{aid, size, refs, freed}, M.lookup(id, t), Nat.is_eq(aid, id)), a2))
      == Nat.add(M.live_bytes(M.replace.at(M.Alloc{aid, size, refs, freed}, a2, t, M.replace(id, a2, t), Nat.is_eq(aid, id))), mbytes(M.lookup.at(M.Alloc{aid, size, refs, freed}, M.lookup(id, t), Nat.is_eq(aid, id)))) : Nat}:
  match c:
    case True{}:
      %e : {Nat.add(M.live_bytes(M.Alloc{aid, size, refs, freed} <> t), pick(M.lookup.at(M.Alloc{aid, size, refs, freed}, M.lookup(id, t), _), a2))
            == Nat.add(M.live_bytes(M.replace.at(M.Alloc{aid, size, refs, freed}, a2, t, M.replace(id, a2, t), _)), mbytes(M.lookup.at(M.Alloc{aid, size, refs, freed}, M.lookup(id, t), _))) : Nat}
      add_rot(M.bytes.of(freed, size), M.live_bytes(t), M.bytes(a2))
    case False{}:
      %e : {Nat.add(M.live_bytes(M.Alloc{aid, size, refs, freed} <> t), pick(M.lookup.at(M.Alloc{aid, size, refs, freed}, M.lookup(id, t), _), a2))
            == Nat.add(M.live_bytes(M.replace.at(M.Alloc{aid, size, refs, freed}, a2, t, M.replace(id, a2, t), _)), mbytes(M.lookup.at(M.Alloc{aid, size, refs, freed}, M.lookup(id, t), _))) : Nat}
      %add_assoc(M.bytes.of(freed, size), M.live_bytes(t), pick(M.lookup(id, t), a2)) : {_ == Nat.add(Nat.add(M.bytes.of(freed, size), M.live_bytes(M.replace(id, a2, t))), mbytes(M.lookup(id, t))) : Nat}
      %add_assoc(M.bytes.of(freed, size), M.live_bytes(M.replace(id, a2, t)), mbytes(M.lookup(id, t))) : {Nat.add(M.bytes.of(freed, size), Nat.add(M.live_bytes(t), pick(M.lookup(id, t), a2))) == _ : Nat}
      %rec : {Nat.add(M.bytes.of(freed, size), Nat.add(M.live_bytes(t), pick(M.lookup(id, t), a2))) == Nat.add(M.bytes.of(freed, size), _) : Nat}
      {==}

def replace_bytes(+id: Nat, +a2: M.Alloc, xs: List<&2, M.Alloc>)
  -> {Nat.add(M.live_bytes(xs), pick(M.lookup(id, xs), a2)) == Nat.add(M.live_bytes(M.replace(id, a2, xs)), mbytes(M.lookup(id, xs))) : Nat}:
  match xs:
    case Nil{}:
      {==}
    case Con{M.Alloc{+aid, +size, +refs, +freed}, +t}:
      replace_bytes.fin(id, a2, aid, size, refs, freed, t, replace_bytes(id, a2, t), Nat.is_eq(aid, id), {==})

# the same, with the lookup result named
def replace_bytes_at(+id: Nat, +a2: M.Alloc, +xs: List<&2, M.Alloc>, -m0: Maybe<&2, M.Alloc>, e: {M.lookup(id, xs) == m0 : Maybe<&2, M.Alloc>})
  -> {Nat.add(M.live_bytes(xs), pick(m0, a2)) == Nat.add(M.live_bytes(M.replace(id, a2, xs)), mbytes(m0)) : Nat}:
  %e : {Nat.add(M.live_bytes(xs), pick(_, a2)) == Nat.add(M.live_bytes(M.replace(id, a2, xs)), mbytes(_)) : Nat}
  replace_bytes(id, a2, xs)

def none_ne_some.motive(m: Maybe<&2, M.Alloc>) -> Type:
  match m:
    case None{}:
      Unit
    case Some{a}:
      Empty

def none_ne_some(-a: M.Alloc, e: {None{} == Some{a} : Maybe<&2, M.Alloc>}) -> Empty:
  %e : none_ne_some.motive(_)
  Unit{}

# after replacing the found allocation by one with the same id, the lookup finds the new one
def lookup_replace.fin(+id: Nat, +size: Nat, +refs: Nat, +freed: Bool, -a: M.Alloc, +aid: Nat, +asize: Nat, +arefs: Nat, +afreed: Bool, +t: List<&2, M.Alloc>,
  rec: {M.lookup(id, t) == Some{a} : Maybe<&2, M.Alloc>} -> {M.lookup(id, M.replace(id, M.Alloc{id, size, refs, freed}, t)) == Some{M.Alloc{id, size, refs, freed}} : Maybe<&2, M.Alloc>},
  c: Bool, +e: {c == Nat.is_eq(aid, id) : Bool},
  el: {M.lookup.at(M.Alloc{aid, asize, arefs, afreed}, M.lookup(id, t), c) == Some{a} : Maybe<&2, M.Alloc>})
  -> {M.lookup(id, M.replace.at(M.Alloc{aid, asize, arefs, afreed}, M.Alloc{id, size, refs, freed}, t, M.replace(id, M.Alloc{id, size, refs, freed}, t), Nat.is_eq(aid, id))) == Some{M.Alloc{id, size, refs, freed}} : Maybe<&2, M.Alloc>}:
  match c:
    case True{}:
      %e : {M.lookup(id, M.replace.at(M.Alloc{aid, asize, arefs, afreed}, M.Alloc{id, size, refs, freed}, t, M.replace(id, M.Alloc{id, size, refs, freed}, t), _)) == Some{M.Alloc{id, size, refs, freed}} : Maybe<&2, M.Alloc>}
      %eq_refl(id) : {M.lookup.at(M.Alloc{id, size, refs, freed}, M.lookup(id, t), _) == Some{M.Alloc{id, size, refs, freed}} : Maybe<&2, M.Alloc>}
      {==}
    case False{}:
      %e : {M.lookup(id, M.replace.at(M.Alloc{aid, asize, arefs, afreed}, M.Alloc{id, size, refs, freed}, t, M.replace(id, M.Alloc{id, size, refs, freed}, t), _)) == Some{M.Alloc{id, size, refs, freed}} : Maybe<&2, M.Alloc>}
      %e : {M.lookup.at(M.Alloc{aid, asize, arefs, afreed}, M.lookup(id, M.replace(id, M.Alloc{id, size, refs, freed}, t)), _) == Some{M.Alloc{id, size, refs, freed}} : Maybe<&2, M.Alloc>}
      rec(el)

def lookup_replace(+id: Nat, +size: Nat, +refs: Nat, +freed: Bool, -a: M.Alloc, xs: List<&2, M.Alloc>)
  -> {M.lookup(id, xs) == Some{a} : Maybe<&2, M.Alloc>} -> {M.lookup(id, M.replace(id, M.Alloc{id, size, refs, freed}, xs)) == Some{M.Alloc{id, size, refs, freed}} : Maybe<&2, M.Alloc>}:
  match xs:
    case Nil{}:
      el => Empty.absurd({M.lookup(id, Nil{}) == Some{M.Alloc{id, size, refs, freed}} : Maybe<&2, M.Alloc>}, none_ne_some(a, el))
    case Con{M.Alloc{+aid, +asize, +arefs, +afreed}, +t}:
      el => lookup_replace.fin(id, size, refs, freed, a, aid, asize, arefs, afreed, t, lookup_replace(id, size, refs, freed, a, t), Nat.is_eq(aid, id), {==}, el)

# a live allocation means there is an outstanding buffer
def live_has_live.fin(c: Bool, +id: Nat, +aid: Nat, +size: Nat, +refs: Nat, +freed: Bool, +t: List<&2, M.Alloc>,
  rec: Laws.T(M.is_live(id, t)) -> {True{} == M.has_live(t) : Bool},
  l: Laws.T(M.is_live.m(M.lookup.at(M.Alloc{aid, size, refs, freed}, M.lookup(id, t), c))))
  -> {True{} == M.has_live(M.Alloc{aid, size, refs, freed} <> t) : Bool}:
  match c:
    case True{}:
      match freed:
        case True{}:
          match l:
        case False{}:
          {==}
    case False{}:
      match freed:
        case True{}:
          rec(l)
        case False{}:
          {==}

def live_has_live(+id: Nat, xs: List<&2, M.Alloc>) -> Laws.T(M.is_live(id, xs)) -> {True{} == M.has_live(xs) : Bool}:
  match xs:
    case Nil{}:
      l => Empty.absurd({True{} == M.has_live(Nil{}) : Bool}, l)
    case Con{M.Alloc{+aid, +size, +refs, +freed}, +t}:
      l => live_has_live.fin(Nat.is_eq(aid, id), id, aid, size, refs, freed, t, live_has_live(id, t), l)

# no outstanding buffer means no accounted bytes
def no_live_no_bytes.fin(+size: Nat, +freed: Bool, +t: List<&2, M.Alloc>,
  c: {False{} == (Bool.not(freed) || M.has_live(t)) : Bool},
  rec: {False{} == M.has_live(t) : Bool} -> {M.live_bytes(t) == 0n : Nat})
  -> {Nat.add(M.bytes.of(freed, size), M.live_bytes(t)) == 0n : Nat}:
  match freed:
    case True{}:
      rec(c)
    case False{}:
      Empty.absurd({Nat.add(size, M.live_bytes(t)) == 0n : Nat}, false_ne_true(c))

def no_live_no_bytes(xs: List<&2, M.Alloc>) -> {False{} == M.has_live(xs) : Bool} -> {M.live_bytes(xs) == 0n : Nat}:
  match xs:
    case Nil{}:
      c => {==}
    case Con{M.Alloc{+aid, +size, +refs, +freed}, +t}:
      c => no_live_no_bytes.fin(size, freed, t, c, no_live_no_bytes(t))

# Invariant laws
# --------------

def Laws.inv_start(limit):
  Laws.Conj{{==}, Unit{}}

# the consistency of the allocation a lookup finds
def ok_of_lookup(+id: Nat, +allocs: List<&2, M.Alloc>, -allocated: Nat, w: Laws.Inv(allocs, allocated)) -> AllocOkM(M.lookup(id, allocs)):
  Laws.Conj{e, c} = w
  lookup_ok(id, allocs)(c)

# the closed allocator answers every request without touching its state
def kept_closed(r: M.Req, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, w: Laws.Inv(allocs, allocated))
  -> Laws.Inv_s(M.state(M.step_closed(r, allocs, allocated, limit, next))):
  match r:
    case M.RAlloc{size}:
      w
    case M.RRetain{id, n}:
      w
    case M.RRelease{id, n}:
      w
    case M.RClose{}:
      w

# buffer(size): the counter grows by the size of the new live allocation
def alloc_bytes(+size: Nat, -allocs: List<&2, M.Alloc>, +allocated: Nat, e: {allocated == M.live_bytes(allocs) : Nat})
  -> {Nat.add(allocated, size) == Nat.add(size, M.live_bytes(allocs)) : Nat}:
  %e : {Nat.add(allocated, size) == Nat.add(size, _) : Nat}
  add_comm(allocated, size)

def kept_alloc.fin(ok: Bool, +size: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, w: Laws.Inv(allocs, allocated))
  -> Laws.Inv_s(M.state(M.alloc.fits(size, allocs, allocated, limit, next, ok))):
  match ok:
    case True{}:
      Laws.Conj{e, c} = w
      Laws.Conj{alloc_bytes(size, allocs, allocated, e), Laws.Conj{{==}, c}}
    case False{}:
      w

def kept_alloc(size: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, w: Laws.Inv(allocs, allocated))
  -> Laws.Inv_s(M.state(M.alloc(size, allocs, allocated, limit, next))):
  match size:
    case 0n:
      w
    case 1n+p:
      +size2 = {1n+p : Nat}
      kept_alloc.fin(Nat.is_le(Nat.add(allocated, size2), limit), size2, allocs, allocated, limit, next, w)

# a replacement with the same bytes as the allocation it replaces keeps the counter right
def same_bytes.fix(-L: Nat, -L2: Nat, -B2: Nat, -B1: Nat, -B: Nat, rb: {Nat.add(L, B2) == Nat.add(L2, B1) : Nat}, b2: {B2 == B : Nat}, b1: {B1 == B : Nat})
  -> {Nat.add(L, B) == Nat.add(L2, B) : Nat}:
  %b2 : {Nat.add(L, _) == Nat.add(L2, B) : Nat}
  %b1 : {Nat.add(L, B2) == Nat.add(L2, _) : Nat}
  rb

def same_bytes(+id: Nat, +a2: M.Alloc, +allocs: List<&2, M.Alloc>, +allocated: Nat, +B: Nat, -a: M.Alloc,
  el: {M.lookup(id, allocs) == Some{a} : Maybe<&2, M.Alloc>},
  e: {allocated == M.live_bytes(allocs) : Nat},
  b2: {M.bytes(a2) == B : Nat},
  b1: {M.bytes(a) == B : Nat})
  -> {allocated == M.live_bytes(M.replace(id, a2, allocs)) : Nat}:
  rb = replace_bytes_at(id, a2, allocs, Some{a}, el)
  %add_cancel_r(M.live_bytes(allocs), M.live_bytes(M.replace(id, a2, allocs)), B, same_bytes.fix(M.live_bytes(allocs), M.live_bytes(M.replace(id, a2, allocs)), M.bytes(a2), M.bytes(a), B, rb, b2, b1)) : {allocated == _ : Nat}
  e

# retain(n) on a live allocation: the count grows, the bytes do not move
def kept_retain.live(+id: Nat, +n: Nat, +aid: Nat, +size: Nat, refs: Nat, +freed: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  w: Laws.Inv(allocs, allocated), el: {M.lookup(id, allocs) == Some{M.Alloc{aid, size, refs, freed}} : Maybe<&2, M.Alloc>},
  ok: Laws.AllocOk(M.Alloc{aid, size, refs, freed}))
  -> Laws.Inv_s(M.state(M.retain.live(id, n, size, refs, freed, allocs, allocated, limit, next))):
  match refs:
    case 0n:
      w
    case 1n+q:
      Laws.Conj{e, c} = w
      +refs2 = {1n+q : Nat}
      Laws.Conj{same_bytes(id, M.Alloc{id, size, Nat.add(refs2, n), freed}, allocs, allocated, M.bytes.of(freed, size), M.Alloc{aid, size, refs2, freed}, el, e, {==}, {==}),
        replace_ok(id, M.Alloc{id, size, Nat.add(refs2, n), freed}, allocs)(c, ok)}

def kept_retain.found(+id: Nat, +n: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  w: Laws.Inv(allocs, allocated), m0: Maybe<&2, M.Alloc>, el: {M.lookup(id, allocs) == m0 : Maybe<&2, M.Alloc>}, ok: AllocOkM(m0))
  -> Laws.Inv_s(M.state(M.retain.found(id, n, m0, allocs, allocated, limit, next))):
  match m0:
    case None{}:
      w
    case Some{M.Alloc{+aid, +size, refs, freed}}:
      kept_retain.live(id, n, aid, size, refs, freed, allocs, allocated, limit, next, w, el, ok)

def kept_retain(+id: Nat, n: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, +w: Laws.Inv(allocs, allocated))
  -> Laws.Inv_s(M.state(M.retain(id, n, allocs, allocated, limit, next))):
  match n:
    case 0n:
      w
    case 1n+m:
      kept_retain.found(id, 1n+m, allocs, allocated, limit, next, w, M.lookup(id, allocs), {==}, ok_of_lookup(id, allocs, allocated, w))

# release(n) that frees: the counter loses the size of the allocation,
# whose bytes were its size (it was live: refs >= n >= 1)
def freed_bytes(+L: Nat, -L2: Nat, -B: Nat, -size: Nat, rb: {Nat.add(L, 0n) == Nat.add(L2, B) : Nat}, bl: {B == size : Nat}) -> {L == Nat.add(L2, size) : Nat}:
  %bl : {L == Nat.add(L2, _) : Nat}
  %Equal.sym(Nat, L, Nat.add(L, 0n), add_zero(L)) : {_ == Nat.add(L2, B) : Nat}
  rb

def live_bytes_of(-freed: Bool, -refs: Nat, -size: Nat, ok: {freed == Nat.is_eq(refs, 0n) : Bool}, pos: {Nat.is_eq(refs, 0n) == False{} : Bool})
  -> {M.bytes.of(freed, size) == size : Nat}:
  %Equal.sym(Bool, freed, False{}, Equal.trans(Bool, freed, Nat.is_eq(refs, 0n), False{}, ok, pos)) : {M.bytes.of(_, size) == size : Nat}
  {==}

def kept_release.freed(+id: Nat, +size: Nat, -aid: Nat, -refs: Nat, -freed: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat,
  el: {M.lookup(id, allocs) == Some{M.Alloc{aid, size, refs, freed}} : Maybe<&2, M.Alloc>},
  e: {allocated == M.live_bytes(allocs) : Nat},
  bl: {M.bytes.of(freed, size) == size : Nat})
  -> {Nat.sub(allocated, size) == M.live_bytes(M.replace(id, M.Alloc{id, size, 0n, True{}}, allocs)) : Nat}:
  fb = freed_bytes(M.live_bytes(allocs), M.live_bytes(M.replace(id, M.Alloc{id, size, 0n, True{}}, allocs)), M.bytes.of(freed, size), size,
         replace_bytes_at(id, M.Alloc{id, size, 0n, True{}}, allocs, Some{M.Alloc{aid, size, refs, freed}}, el), bl)
  %Equal.sym(Nat, allocated, M.live_bytes(allocs), e) : {Nat.sub(_, size) == M.live_bytes(M.replace(id, M.Alloc{id, size, 0n, True{}}, allocs)) : Nat}
  %Equal.sym(Nat, M.live_bytes(allocs), Nat.add(M.live_bytes(M.replace(id, M.Alloc{id, size, 0n, True{}}, allocs)), size), fb) : {Nat.sub(_, size) == M.live_bytes(M.replace(id, M.Alloc{id, size, 0n, True{}}, allocs)) : Nat}
  Equal.sym(Nat, M.live_bytes(M.replace(id, M.Alloc{id, size, 0n, True{}}, allocs)), Nat.sub(Nat.add(M.live_bytes(M.replace(id, M.Alloc{id, size, 0n, True{}}, allocs)), size), size),
    sub_add_cancel(M.live_bytes(M.replace(id, M.Alloc{id, size, 0n, True{}}, allocs)), size))

# release(n) after the decrement, over the new count refs2 = refs - n
def kept_release.new(refs2: Nat, +id: Nat, +n: Nat, +aid: Nat, +size: Nat, +refs: Nat, +freed: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  w: Laws.Inv(allocs, allocated), el: {M.lookup(id, allocs) == Some{M.Alloc{aid, size, refs, freed}} : Maybe<&2, M.Alloc>},
  ok: Laws.AllocOk(M.Alloc{aid, size, refs, freed}),
  e2: {refs2 == Nat.sub(refs, n) : Nat}, pos: {Nat.is_eq(refs, 0n) == False{} : Bool})
  -> Laws.Inv_s(M.state(M.release.new(id, size, refs2, freed, allocs, allocated, limit, next))):
  match refs2:
    case 0n:
      Laws.Conj{e, c} = w
      Laws.Conj{kept_release.freed(id, size, aid, refs, freed, allocs, allocated, el, e, live_bytes_of(freed, refs, size, ok, pos)),
        replace_ok(id, M.Alloc{id, size, 0n, True{}}, allocs)(c, {==})}
    case 1n+q:
      Laws.Conj{e, c} = w
      +refs3 = {1n+q : Nat}
      Laws.Conj{same_bytes(id, M.Alloc{id, size, refs3, freed}, allocs, allocated, M.bytes.of(freed, size), M.Alloc{aid, size, refs, freed}, el, e, {==}, {==}),
        replace_ok(id, M.Alloc{id, size, refs3, freed}, allocs)(c, Equal.trans(Bool, freed, Nat.is_eq(refs, 0n), False{}, ok, pos))}

def kept_release.checked(negative: Bool, +id: Nat, +n: Nat, +aid: Nat, +size: Nat, +refs: Nat, +freed: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  w: Laws.Inv(allocs, allocated), el: {M.lookup(id, allocs) == Some{M.Alloc{aid, size, refs, freed}} : Maybe<&2, M.Alloc>},
  ok: Laws.AllocOk(M.Alloc{aid, size, refs, freed}), e3: {negative == Nat.is_lt(refs, 1n+n) : Bool})
  -> Laws.Inv_s(M.state(M.release.checked(id, 1n+n, size, refs, freed, allocs, allocated, limit, next, negative))):
  match negative:
    case True{}:
      w
    case False{}:
      kept_release.new(Nat.sub(refs, 1n+n), id, 1n+n, aid, size, refs, freed, allocs, allocated, limit, next, w, el, ok, {==}, ge_succ_pos(refs, n, e3))

def kept_release.found(+id: Nat, +n: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  w: Laws.Inv(allocs, allocated), m0: Maybe<&2, M.Alloc>, el: {M.lookup(id, allocs) == m0 : Maybe<&2, M.Alloc>}, ok: AllocOkM(m0))
  -> Laws.Inv_s(M.state(M.release.found(id, 1n+n, m0, allocs, allocated, limit, next))):
  match m0:
    case None{}:
      w
    case Some{M.Alloc{+aid, +size, +refs, +freed}}:
      kept_release.checked(Nat.is_lt(refs, 1n+n), id, n, aid, size, refs, freed, allocs, allocated, limit, next, w, el, ok, {==})

def kept_release(+id: Nat, n: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, +w: Laws.Inv(allocs, allocated))
  -> Laws.Inv_s(M.state(M.release(id, n, allocs, allocated, limit, next))):
  match n:
    case 0n:
      w
    case 1n+m:
      kept_release.found(id, m, allocs, allocated, limit, next, w, M.lookup(id, allocs), {==}, ok_of_lookup(id, allocs, allocated, w))

def kept_close(leak: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, w: Laws.Inv(allocs, allocated))
  -> Laws.Inv_s(M.state(M.close(allocs, allocated, limit, next, leak))):
  match leak:
    case True{}:
      w
    case False{}:
      w

def kept_step(closed: Bool, r: M.Req, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, w: Laws.Inv(allocs, allocated))
  -> Laws.Inv_s(M.state(M.step(r, allocs, allocated, limit, next, closed))):
  match closed:
    case True{}:
      kept_closed(r, allocs, allocated, limit, next, w)
    case False{}:
      match r:
        case M.RAlloc{size}:
          kept_alloc(size, allocs, allocated, limit, next, w)
        case M.RRetain{+id, n}:
          kept_retain(id, n, allocs, allocated, limit, next, w)
        case M.RRelease{+id, n}:
          kept_release(id, n, allocs, allocated, limit, next, w)
        case M.RClose{}:
          kept_close(M.has_live(allocs), allocs, allocated, limit, next, w)

def Laws.inv_kept(r, allocs, allocated, limit, next, closed, w):
  kept_step(closed, r, allocs, allocated, limit, next, w)

# Response laws
# -------------

# the unchanged state beside a response
def Unchanged(+allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, closed: Bool, r: M.Resp) -> M.Allocator & M.Resp:
  (M.Allocator{allocs, allocated, limit, next, closed}, r)

# Release

def Laws.release_zero_is_error(id, allocs, allocated, limit, next):
  {==}

# over the lookup result m0
def rel_below.fin(m0: Maybe<&2, M.Alloc>, +id: Nat, +m: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  lt: Laws.T(Nat.is_lt(M.refs_of.m(m0), 1n+m)))
  -> {M.release.found(id, 1n+m, m0, allocs, allocated, limit, next) == Unchanged(allocs, allocated, limit, next, False{}, M.RErr{}) : M.Allocator & M.Resp}:
  match m0:
    case None{}:
      {==}
    case Some{M.Alloc{aid, +size, +refs, +freed}}:
      %T_true(Nat.is_lt(refs, 1n+m), lt) : {M.release.checked(id, 1n+m, size, refs, freed, allocs, allocated, limit, next, _) == Unchanged(allocs, allocated, limit, next, False{}, M.RErr{}) : M.Allocator & M.Resp}
      {==}

def Laws.release_below_zero_is_error(id, n, allocs, allocated, limit, next, lt):
  match n:
    case 0n:
      {==}
    case 1n+m:
      rel_below.fin(M.lookup(id, allocs), id, m, allocs, allocated, limit, next, lt)

# a found allocation that is not live has count 0, so any release overdraws
def rel_dead.fix(refs: Nat, freed: Bool, +id: Nat, +m: Nat, +size: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  ok: {freed == Nat.is_eq(refs, 0n) : Bool}, d: Laws.T(Bool.not(Bool.not(freed))))
  -> {M.release.checked(id, 1n+m, size, refs, freed, allocs, allocated, limit, next, Nat.is_lt(refs, 1n+m)) == Unchanged(allocs, allocated, limit, next, False{}, M.RErr{}) : M.Allocator & M.Resp}:
  match refs:
    case 0n:
      {==}
    case 1n+r:
      match freed:
        case True{}:
          Empty.absurd({M.release.checked(id, 1n+m, size, 1n+r, True{}, allocs, allocated, limit, next, Nat.is_lt(1n+r, 1n+m)) == Unchanged(allocs, allocated, limit, next, False{}, M.RErr{}) : M.Allocator & M.Resp}, true_ne_false(ok))
        case False{}:
          match d:

def rel_dead.fin(m0: Maybe<&2, M.Alloc>, +id: Nat, +m: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  ok: AllocOkM(m0), d: Laws.T(Bool.not(M.is_live.m(m0))))
  -> {M.release.found(id, 1n+m, m0, allocs, allocated, limit, next) == Unchanged(allocs, allocated, limit, next, False{}, M.RErr{}) : M.Allocator & M.Resp}:
  match m0:
    case None{}:
      {==}
    case Some{M.Alloc{aid, +size, refs, freed}}:
      rel_dead.fix(refs, freed, id, m, size, allocs, allocated, limit, next, ok, d)

def Laws.release_dead_is_error(id, n, allocs, allocated, limit, next, w, d):
  match n:
    case 0n:
      {==}
    case 1n+m:
      rel_dead.fin(M.lookup(id, allocs), id, m, allocs, allocated, limit, next, ok_of_lookup(id, allocs, allocated, w), d)

# after replacing the found allocation by a freed one, the id is freed
def freed_after(+id: Nat, +size: Nat, +allocs: List<&2, M.Alloc>, -a: M.Alloc, el: {M.lookup(id, allocs) == Some{a} : Maybe<&2, M.Alloc>})
  -> Laws.T(M.is_freed(id, M.replace(id, M.Alloc{id, size, 0n, True{}}, allocs))):
  %Equal.sym(Maybe<&2, M.Alloc>, M.lookup(id, M.replace(id, M.Alloc{id, size, 0n, True{}}, allocs)), Some{M.Alloc{id, size, 0n, True{}}}, lookup_replace(id, size, 0n, True{}, a, allocs)(el)) : Laws.T(M.is_freed.m(_))
  Unit{}

# after replacing the found allocation by a live one, the id is live with that count
def live_after(+id: Nat, +size: Nat, +refs: Nat, +allocs: List<&2, M.Alloc>, -a: M.Alloc, el: {M.lookup(id, allocs) == Some{a} : Maybe<&2, M.Alloc>})
  -> Laws.T(M.is_live(id, M.replace(id, M.Alloc{id, size, refs, False{}}, allocs))):
  %Equal.sym(Maybe<&2, M.Alloc>, M.lookup(id, M.replace(id, M.Alloc{id, size, refs, False{}}, allocs)), Some{M.Alloc{id, size, refs, False{}}}, lookup_replace(id, size, refs, False{}, a, allocs)(el)) : Laws.T(M.is_live.m(_))
  Unit{}

def count_after(+id: Nat, +size: Nat, +refs: Nat, +allocs: List<&2, M.Alloc>, -a: M.Alloc, el: {M.lookup(id, allocs) == Some{a} : Maybe<&2, M.Alloc>},
  -target: Nat, e: {refs == target : Nat})
  -> {M.refs_of(id, M.replace(id, M.Alloc{id, size, refs, False{}}, allocs)) == target : Nat}:
  %Equal.sym(Maybe<&2, M.Alloc>, M.lookup(id, M.replace(id, M.Alloc{id, size, refs, False{}}, allocs)), Some{M.Alloc{id, size, refs, False{}}}, lookup_replace(id, size, refs, False{}, a, allocs)(el)) : {M.refs_of.m(_) == target : Nat}
  e

# release to zero: the goal, over the lookup result, the verdict, and the new count
def FreesGoal(m0: Maybe<&2, M.Alloc>, +id: Nat, +m: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Type:
  {M.resp(M.release.found(id, 1n+m, m0, allocs, allocated, limit, next)) == M.RFreed{} : M.Resp}
    & Laws.T(M.is_freed(id, M.allocs_of(M.state(M.release.found(id, 1n+m, m0, allocs, allocated, limit, next)))))
    & {M.allocated_of(M.state(M.release.found(id, 1n+m, m0, allocs, allocated, limit, next))) == Nat.sub(allocated, M.size_of.m(m0)) : Nat}

def FreesGoal2(neg: Bool, +id: Nat, +m: Nat, +size: Nat, +freed: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Type:
  {M.resp(M.release.checked(id, 1n+m, size, 1n+m, freed, allocs, allocated, limit, next, neg)) == M.RFreed{} : M.Resp}
    & Laws.T(M.is_freed(id, M.allocs_of(M.state(M.release.checked(id, 1n+m, size, 1n+m, freed, allocs, allocated, limit, next, neg)))))
    & {M.allocated_of(M.state(M.release.checked(id, 1n+m, size, 1n+m, freed, allocs, allocated, limit, next, neg))) == Nat.sub(allocated, size) : Nat}

def FreesGoal3(refs2: Nat, +id: Nat, +size: Nat, +freed: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Type:
  {M.resp(M.release.new(id, size, refs2, freed, allocs, allocated, limit, next)) == M.RFreed{} : M.Resp}
    & Laws.T(M.is_freed(id, M.allocs_of(M.state(M.release.new(id, size, refs2, freed, allocs, allocated, limit, next)))))
    & {M.allocated_of(M.state(M.release.new(id, size, refs2, freed, allocs, allocated, limit, next))) == Nat.sub(allocated, size) : Nat}

def frees.new(refs2: Nat, +id: Nat, +m: Nat, +size: Nat, +freed: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  -a: M.Alloc, el: {M.lookup(id, allocs) == Some{a} : Maybe<&2, M.Alloc>}, e4: {refs2 == Nat.sub(1n+m, 1n+m) : Nat})
  -> FreesGoal3(refs2, id, size, freed, allocs, allocated, limit, next):
  match refs2:
    case 0n:
      ({==}, (freed_after(id, size, allocs, a, el), {==}))
    case 1n+q:
      Empty.absurd(FreesGoal3(1n+q, id, size, freed, allocs, allocated, limit, next),
        zero_ne_succ(q, Equal.sym(Nat, 1n+q, 0n, Equal.trans(Nat, 1n+q, Nat.sub(1n+m, 1n+m), 0n, e4, Equal.sym(Nat, 0n, Nat.sub(1n+m, 1n+m), sub_self(1n+m))))))

def frees.checked(neg: Bool, +id: Nat, +m: Nat, +size: Nat, +freed: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  -a: M.Alloc, el: {M.lookup(id, allocs) == Some{a} : Maybe<&2, M.Alloc>}, e3: {neg == Nat.is_lt(1n+m, 1n+m) : Bool})
  -> FreesGoal2(neg, id, m, size, freed, allocs, allocated, limit, next):
  match neg:
    case True{}:
      Empty.absurd(FreesGoal2(True{}, id, m, size, freed, allocs, allocated, limit, next),
        true_ne_false(Equal.trans(Bool, True{}, Nat.is_lt(1n+m, 1n+m), False{}, e3, Equal.sym(Bool, False{}, Nat.is_lt(1n+m, 1n+m), lt_irrefl(1n+m)))))
    case False{}:
      frees.new(Nat.sub(1n+m, 1n+m), id, m, size, freed, allocs, allocated, limit, next, a, el, {==})

def frees.fin(m0: Maybe<&2, M.Alloc>, +id: Nat, +m: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  el: {M.lookup(id, allocs) == m0 : Maybe<&2, M.Alloc>}, e: {M.refs_of.m(m0) == 1n+m : Nat})
  -> FreesGoal(m0, id, m, allocs, allocated, limit, next):
  match m0:
    case None{}:
      Empty.absurd(FreesGoal(None{}, id, m, allocs, allocated, limit, next), zero_ne_succ(m, e))
    case Some{M.Alloc{+aid, +size, +refs, +freed}}:
      %Equal.sym(Nat, refs, 1n+m, e) : FreesGoal(Some{M.Alloc{aid, size, _, freed}}, id, m, allocs, allocated, limit, next)
      frees.checked(Nat.is_lt(1n+m, 1n+m), id, m, size, freed, allocs, allocated, limit, next, M.Alloc{aid, size, refs, freed}, el, {==})

def Laws.release_to_zero_frees(id, m, allocs, allocated, limit, next, e):
  frees.fin(M.lookup(id, allocs), id, m, allocs, allocated, limit, next, {==}, e)

# release that keeps the memory: the goal, over the lookup result, the verdict, and the new count
def KeepsGoal(m0: Maybe<&2, M.Alloc>, +id: Nat, +m: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Type:
  {M.resp(M.release.found(id, 1n+m, m0, allocs, allocated, limit, next)) == M.ROk{} : M.Resp}
    & Laws.T(M.is_live(id, M.allocs_of(M.state(M.release.found(id, 1n+m, m0, allocs, allocated, limit, next)))))
    & {M.refs_of(id, M.allocs_of(M.state(M.release.found(id, 1n+m, m0, allocs, allocated, limit, next)))) == Nat.sub(M.refs_of.m(m0), 1n+m) : Nat}
    & {M.allocated_of(M.state(M.release.found(id, 1n+m, m0, allocs, allocated, limit, next))) == allocated : Nat}

def KeepsGoal2(neg: Bool, +id: Nat, +m: Nat, +size: Nat, +refs: Nat, +freed: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Type:
  {M.resp(M.release.checked(id, 1n+m, size, refs, freed, allocs, allocated, limit, next, neg)) == M.ROk{} : M.Resp}
    & Laws.T(M.is_live(id, M.allocs_of(M.state(M.release.checked(id, 1n+m, size, refs, freed, allocs, allocated, limit, next, neg)))))
    & {M.refs_of(id, M.allocs_of(M.state(M.release.checked(id, 1n+m, size, refs, freed, allocs, allocated, limit, next, neg)))) == Nat.sub(refs, 1n+m) : Nat}
    & {M.allocated_of(M.state(M.release.checked(id, 1n+m, size, refs, freed, allocs, allocated, limit, next, neg))) == allocated : Nat}

def KeepsGoal3(refs2: Nat, +id: Nat, +m: Nat, +size: Nat, +refs: Nat, +freed: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Type:
  {M.resp(M.release.new(id, size, refs2, freed, allocs, allocated, limit, next)) == M.ROk{} : M.Resp}
    & Laws.T(M.is_live(id, M.allocs_of(M.state(M.release.new(id, size, refs2, freed, allocs, allocated, limit, next)))))
    & {M.refs_of(id, M.allocs_of(M.state(M.release.new(id, size, refs2, freed, allocs, allocated, limit, next)))) == Nat.sub(refs, 1n+m) : Nat}
    & {M.allocated_of(M.state(M.release.new(id, size, refs2, freed, allocs, allocated, limit, next))) == allocated : Nat}

def keeps.new(refs2: Nat, +id: Nat, +m: Nat, +size: Nat, +refs: Nat, +freed: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  -a: M.Alloc, +el: {M.lookup(id, allocs) == Some{a} : Maybe<&2, M.Alloc>}, fl: {freed == False{} : Bool},
  lt: Laws.T(Nat.is_lt(1n+m, refs)), e4: {refs2 == Nat.sub(refs, 1n+m) : Nat})
  -> KeepsGoal3(refs2, id, m, size, refs, freed, allocs, allocated, limit, next):
  match refs2:
    case 0n:
      Empty.absurd(KeepsGoal3(0n, id, m, size, refs, freed, allocs, allocated, limit, next),
        zero_ne_succ(Nat.sub(refs, 2n+m), Equal.trans(Nat, 0n, Nat.sub(refs, 1n+m), 1n+Nat.sub(refs, 2n+m), e4, Equal.sym(Nat, 1n+Nat.sub(refs, 2n+m), Nat.sub(refs, 1n+m), lt_sub_succ(1n+m, refs, lt)))))
    case 1n+q:
      +refs3 = {1n+q : Nat}
      %Equal.sym(Bool, freed, False{}, fl) : KeepsGoal3(refs3, id, m, size, refs, _, allocs, allocated, limit, next)
      ({==}, (live_after(id, size, refs3, allocs, a, el), (count_after(id, size, refs3, allocs, a, el, Nat.sub(refs, 1n+m), e4), {==})))

def keeps.checked(neg: Bool, +id: Nat, +m: Nat, +size: Nat, +refs: Nat, +freed: Bool, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  -a: M.Alloc, el: {M.lookup(id, allocs) == Some{a} : Maybe<&2, M.Alloc>}, fl: {freed == False{} : Bool},
  +lt: Laws.T(Nat.is_lt(1n+m, refs)), e3: {neg == Nat.is_lt(refs, 1n+m) : Bool})
  -> KeepsGoal2(neg, id, m, size, refs, freed, allocs, allocated, limit, next):
  match neg:
    case True{}:
      Empty.absurd(KeepsGoal2(True{}, id, m, size, refs, freed, allocs, allocated, limit, next),
        true_ne_false(Equal.trans(Bool, True{}, Nat.is_lt(refs, 1n+m), False{}, e3, Equal.sym(Bool, False{}, Nat.is_lt(refs, 1n+m), lt_asym(1n+m, refs, lt)))))
    case False{}:
      keeps.new(Nat.sub(refs, 1n+m), id, m, size, refs, freed, allocs, allocated, limit, next, a, el, fl, lt, {==})

def keeps.fin(m0: Maybe<&2, M.Alloc>, +id: Nat, +m: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  el: {M.lookup(id, allocs) == m0 : Maybe<&2, M.Alloc>}, ok: AllocOkM(m0), +lt: Laws.T(Nat.is_lt(1n+m, M.refs_of.m(m0))))
  -> KeepsGoal(m0, id, m, allocs, allocated, limit, next):
  match m0:
    case None{}:
      match lt:
    case Some{M.Alloc{+aid, +size, +refs, +freed}}:
      keeps.checked(Nat.is_lt(refs, 1n+m), id, m, size, refs, freed, allocs, allocated, limit, next, M.Alloc{aid, size, refs, freed}, el,
        Equal.trans(Bool, freed, Nat.is_eq(refs, 0n), False{}, ok, lt_pos(m, refs, lt)), lt, {==})

def Laws.release_keeps_live(id, m, allocs, allocated, limit, next, w, lt):
  keeps.fin(M.lookup(id, allocs), id, m, allocs, allocated, limit, next, {==}, ok_of_lookup(id, allocs, allocated, w), lt)

# Retain

def Laws.retain_zero_is_error(id, allocs, allocated, limit, next):
  {==}

def ret_dead.fix(refs: Nat, freed: Bool, +id: Nat, +m: Nat, +size: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  ok: {freed == Nat.is_eq(refs, 0n) : Bool}, d: Laws.T(Bool.not(Bool.not(freed))))
  -> {M.retain.live(id, 1n+m, size, refs, freed, allocs, allocated, limit, next) == Unchanged(allocs, allocated, limit, next, False{}, M.RErr{}) : M.Allocator & M.Resp}:
  match refs:
    case 0n:
      {==}
    case 1n+r:
      match freed:
        case True{}:
          Empty.absurd({M.retain.live(id, 1n+m, size, 1n+r, True{}, allocs, allocated, limit, next) == Unchanged(allocs, allocated, limit, next, False{}, M.RErr{}) : M.Allocator & M.Resp}, true_ne_false(ok))
        case False{}:
          match d:

def ret_dead.fin(m0: Maybe<&2, M.Alloc>, +id: Nat, +m: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  ok: AllocOkM(m0), d: Laws.T(Bool.not(M.is_live.m(m0))))
  -> {M.retain.found(id, 1n+m, m0, allocs, allocated, limit, next) == Unchanged(allocs, allocated, limit, next, False{}, M.RErr{}) : M.Allocator & M.Resp}:
  match m0:
    case None{}:
      {==}
    case Some{M.Alloc{aid, +size, refs, freed}}:
      ret_dead.fix(refs, freed, id, m, size, allocs, allocated, limit, next, ok, d)

def Laws.retain_dead_is_error(id, n, allocs, allocated, limit, next, w, d):
  match n:
    case 0n:
      {==}
    case 1n+m:
      ret_dead.fin(M.lookup(id, allocs), id, m, allocs, allocated, limit, next, ok_of_lookup(id, allocs, allocated, w), d)

def AddsGoal(m0: Maybe<&2, M.Alloc>, +id: Nat, +m: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Type:
  {M.resp(M.retain.found(id, 1n+m, m0, allocs, allocated, limit, next)) == M.ROk{} : M.Resp}
    & Laws.T(M.is_live(id, M.allocs_of(M.state(M.retain.found(id, 1n+m, m0, allocs, allocated, limit, next)))))
    & {M.refs_of(id, M.allocs_of(M.state(M.retain.found(id, 1n+m, m0, allocs, allocated, limit, next)))) == Nat.add(M.refs_of.m(m0), 1n+m) : Nat}
    & {M.allocated_of(M.state(M.retain.found(id, 1n+m, m0, allocs, allocated, limit, next))) == allocated : Nat}

def AddsGoal2(refs: Nat, freed: Bool, +id: Nat, +m: Nat, +size: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Type:
  {M.resp(M.retain.live(id, 1n+m, size, refs, freed, allocs, allocated, limit, next)) == M.ROk{} : M.Resp}
    & Laws.T(M.is_live(id, M.allocs_of(M.state(M.retain.live(id, 1n+m, size, refs, freed, allocs, allocated, limit, next)))))
    & {M.refs_of(id, M.allocs_of(M.state(M.retain.live(id, 1n+m, size, refs, freed, allocs, allocated, limit, next)))) == Nat.add(refs, 1n+m) : Nat}
    & {M.allocated_of(M.state(M.retain.live(id, 1n+m, size, refs, freed, allocs, allocated, limit, next))) == allocated : Nat}

def adds.fix(refs: Nat, freed: Bool, +id: Nat, +m: Nat, +aid: Nat, +size: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  +el: {M.lookup(id, allocs) == Some{M.Alloc{aid, size, refs, freed}} : Maybe<&2, M.Alloc>},
  ok: {freed == Nat.is_eq(refs, 0n) : Bool}, l: Laws.T(Bool.not(freed)))
  -> AddsGoal2(refs, freed, id, m, size, allocs, allocated, limit, next):
  match refs:
    case 0n:
      match freed:
        case True{}:
          match l:
        case False{}:
          Empty.absurd(AddsGoal2(0n, False{}, id, m, size, allocs, allocated, limit, next), false_ne_true(ok))
    case 1n+q:
      match freed:
        case True{}:
          match l:
        case False{}:
          +refs3 = {1n+q : Nat}
          ({==}, (live_after(id, size, Nat.add(refs3, 1n+m), allocs, M.Alloc{aid, size, refs3, False{}}, el),
            (count_after(id, size, Nat.add(refs3, 1n+m), allocs, M.Alloc{aid, size, refs3, False{}}, el, Nat.add(refs3, 1n+m), {==}), {==})))

def adds.fin(m0: Maybe<&2, M.Alloc>, +id: Nat, +m: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat,
  el: {M.lookup(id, allocs) == m0 : Maybe<&2, M.Alloc>}, ok: AllocOkM(m0), l: Laws.T(M.is_live.m(m0)))
  -> AddsGoal(m0, id, m, allocs, allocated, limit, next):
  match m0:
    case None{}:
      match l:
    case Some{M.Alloc{+aid, +size, refs, freed}}:
      adds.fix(refs, freed, id, m, aid, size, allocs, allocated, limit, next, el, ok, l)

def Laws.retain_live_adds(id, m, allocs, allocated, limit, next, w, l):
  adds.fin(M.lookup(id, allocs), id, m, allocs, allocated, limit, next, {==}, ok_of_lookup(id, allocs, allocated, w), l)

# Allocation

def Laws.alloc_over_limit_rejected(p, allocs, allocated, limit, next, over):
  %lt_not_le(limit, Nat.add(allocated, 1n+p), over) : {M.alloc.fits(1n+p, allocs, allocated, limit, next, _) == Unchanged(allocs, allocated, limit, next, False{}, M.ROom{}) : M.Allocator & M.Resp}
  {==}

def ServedGoal(ok: Bool, +p: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Type:
  {M.resp(M.alloc.fits(1n+p, allocs, allocated, limit, next, ok)) == M.RBuf{next} : M.Resp}
    & {M.refs_of(next, M.allocs_of(M.state(M.alloc.fits(1n+p, allocs, allocated, limit, next, ok)))) == 1n : Nat}
    & {M.allocated_of(M.state(M.alloc.fits(1n+p, allocs, allocated, limit, next, ok))) == Nat.add(allocated, 1n+p) : Nat}

# the new buffer is found first, with a count of 1
def served_refs(+p: Nat, +allocs: List<&2, M.Alloc>, +next: Nat) -> {M.refs_of(next, M.Alloc{next, 1n+p, 1n, False{}} <> allocs) == 1n : Nat}:
  %eq_refl(next) : {M.refs_of.m(M.lookup.at(M.Alloc{next, 1n+p, 1n, False{}}, M.lookup(next, allocs), _)) == 1n : Nat}
  {==}

def served.fin(ok: Bool, +p: Nat, +allocs: List<&2, M.Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, fits: Laws.T(ok))
  -> ServedGoal(ok, p, allocs, allocated, limit, next):
  match ok:
    case True{}:
      ({==}, (served_refs(p, allocs, next), {==}))
    case False{}:
      match fits:

def Laws.alloc_within_limit_served(p, allocs, allocated, limit, next, fits):
  served.fin(Nat.is_le(Nat.add(allocated, 1n+p), limit), p, allocs, allocated, limit, next, fits)

def Laws.alloc_zero_is_empty(allocs, allocated, limit, next):
  {==}

# Close

def Laws.close_with_live_buffer_is_error(id, allocs, allocated, limit, next, l):
  %live_has_live(id, allocs)(l) : {M.resp(M.close(allocs, allocated, limit, next, _)) == M.RErr{} : M.Resp}
  {==}

def Laws.close_clean_succeeds(allocs, allocated, limit, next, c):
  %c : {M.close(allocs, allocated, limit, next, _) == Unchanged(allocs, allocated, limit, next, True{}, M.ROk{}) : M.Allocator & M.Resp}
  {==}

def Laws.clean_means_zero_bytes(allocs, allocated, w, c):
  Laws.Conj{e, cons} = w
  %Equal.sym(Nat, allocated, M.live_bytes(allocs), e) : {_ == 0n : Nat}
  no_live_no_bytes(allocs)(c)

def Laws.closed_rejects(r, allocs, allocated, limit, next, nc):
  match r:
    case M.RAlloc{size}:
      {==}
    case M.RRetain{id, n}:
      {==}
    case M.RRelease{id, n}:
      {==}
    case M.RClose{}:
      Empty.absurd({M.step(M.RClose{}, allocs, allocated, limit, next, True{}) == Unchanged(allocs, allocated, limit, next, True{}, M.RErr{}) : M.Allocator & M.Resp}, false_ne_true(nc))

def Laws.close_twice_is_noop(allocs, allocated, limit, next):
  {==}

# Traces
# ------

# each trace starts with buffer(1 + p) on a fresh allocator; the verdict
# of the limit check is the only non-literal computation in it

def Trace1(ok: Bool, +p: Nat, +limit: Nat) -> Type:
  {M.resp(M.run([M.RRelease{0n, 1n}, M.RClose{}], M.alloc.fits(1n+p, Nil{}, 0n, limit, 0n, ok))) == M.ROk{} : M.Resp}
    & {M.allocated_of(M.state(M.run([M.RRelease{0n, 1n}, M.RClose{}], M.alloc.fits(1n+p, Nil{}, 0n, limit, 0n, ok)))) == 0n : Nat}

def trace1.fin(ok: Bool, +p: Nat, +limit: Nat, fits: Laws.T(ok)) -> Trace1(ok, p, limit):
  match ok:
    case True{}:
      ({==}, Equal.sym(Nat, 0n, Nat.sub(p, p), sub_self(p)))
    case False{}:
      match fits:

def Laws.trace_alloc_release_frees(p, limit, fits):
  trace1.fin(Nat.is_le(Nat.add(0n, 1n+p), limit), p, limit, fits)

def Trace2(ok: Bool, +p: Nat, +limit: Nat) -> Type:
  {M.resp(M.run([M.RRetain{0n, 1n}, M.RRelease{0n, 1n}, M.RClose{}], M.alloc.fits(1n+p, Nil{}, 0n, limit, 0n, ok))) == M.RErr{} : M.Resp}
    & {M.refs_of(0n, M.allocs_of(M.state(M.run([M.RRetain{0n, 1n}, M.RRelease{0n, 1n}, M.RClose{}], M.alloc.fits(1n+p, Nil{}, 0n, limit, 0n, ok))))) == 1n : Nat}
    & {M.allocated_of(M.state(M.run([M.RRetain{0n, 1n}, M.RRelease{0n, 1n}, M.RClose{}], M.alloc.fits(1n+p, Nil{}, 0n, limit, 0n, ok)))) == 1n+p : Nat}

def trace2.fin(ok: Bool, +p: Nat, +limit: Nat, fits: Laws.T(ok)) -> Trace2(ok, p, limit):
  match ok:
    case True{}:
      ({==}, ({==}, {==}))
    case False{}:
      match fits:

def Laws.trace_retain_keeps_live(p, limit, fits):
  trace2.fin(Nat.is_le(Nat.add(0n, 1n+p), limit), p, limit, fits)

def Trace3(ok: Bool, +p: Nat, +limit: Nat) -> Type:
  {M.resp(M.run([M.RRelease{0n, 1n}, M.RRelease{0n, 1n}], M.alloc.fits(1n+p, Nil{}, 0n, limit, 0n, ok))) == M.RErr{} : M.Resp}

def trace3.fin(ok: Bool, +p: Nat, +limit: Nat, fits: Laws.T(ok)) -> Trace3(ok, p, limit):
  match ok:
    case True{}:
      {==}
    case False{}:
      match fits:

def Laws.trace_double_release_is_error(p, limit, fits):
  trace3.fin(Nat.is_le(Nat.add(0n, 1n+p), limit), p, limit, fits)
