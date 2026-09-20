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

# The proofs. Imports the model (as R) and the claims (as Laws) and
# fills every law they state; `bend PROOF.bend` is the whole check.
#
# Two idioms recur. A computed Bool cannot be matched in place, so every
# case split is a `.fin` helper that takes the verdict `c` as a
# parameter, usually next to an equation `e : {c == <the computation>}`
# or a truth `w : T(c)` that the match refines. And a rewrite `%e : P`
# with `e : {a == b}` replaces `b` by `a` in the goal, so lemmas put the
# simpler side on the left.

import Base
import ./main.bend as R
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

def ndisc(b: Bool) -> Type:
  match b:
    case True{}:
      Empty
    case False{}:
      Unit

# True and False clash
def true_ne_false(e: {True{} == False{} : Bool}) -> Empty:
  %e : disc(_)
  Unit{}

def false_ne_true(e: {False{} == True{} : Bool}) -> Empty:
  %e : ndisc(_)
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

# T(a) gives T(a || b)
def or_l(a: Bool, -b: Bool, wa: Laws.T(a)) -> Laws.T(a || b):
  match a:
    case True{}:
      Unit{}
    case False{}:
      Empty.absurd(Laws.T(False{} || b), wa)

# T(b) gives T(a || b)
def or_r(a: Bool, -b: Bool, wb: Laws.T(b)) -> Laws.T(a || b):
  match a:
    case True{}:
      Unit{}
    case False{}:
      wb

# (a || b) || c gives b || (a || c)
def or_rot(a: Bool, b: Bool, -c: Bool, w: Laws.T((a || b) || c)) -> Laws.T(b || (a || c)):
  match a b:
    case True{} True{}:
      Unit{}
    case True{} False{}:
      Unit{}
    case False{} True{}:
      Unit{}
    case False{} False{}:
      w

# a && False is False
def and_false(a: Bool) -> {False{} == (a && False{}) : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

# T(not(a)) is a == False
def not_T(a: Bool, w: Laws.T(Bool.not(a))) -> {False{} == a : Bool}:
  match a:
    case True{}:
      Empty.absurd({False{} == True{} : Bool}, w)
    case False{}:
      {==}

# not(a || b) splits into not(a) and not(b)
def not_or_split(a: Bool, -b: Bool, w: Laws.T(Bool.not(a || b)), -P: Type,
  k: Laws.T(Bool.not(a)) -> Laws.T(Bool.not(b)) -> P) -> P:
  match a:
    case True{}:
      Empty.absurd(P, w)
    case False{}:
      k(Unit{}, w)

# not(a) and not(b) join into not(a || b)
def not_or_join(a: Bool, -b: Bool, wa: Laws.T(Bool.not(a)), wb: Laws.T(Bool.not(b))) -> Laws.T(Bool.not(a || b)):
  match a:
    case True{}:
      Empty.absurd(Laws.T(Bool.not(True{} || b)), wa)
    case False{}:
      wb

# Lemmas on Nat
# -------------

# a nat equals itself
def eq_refl(n: Nat) -> {True{} == Nat.is_eq(n, n) : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      eq_refl(p)

# a true Nat.is_eq is an equality, oriented to replace x by h
def eq_sound(x: Nat, h: Nat, e: {True{} == Nat.is_eq(x, h) : Bool}) -> {h == x : Nat}:
  match x h:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      Empty.absurd({1n+q == 0n : Nat}, false_ne_true(Equal.sym(Bool, True{}, False{}, e)))
    case 1n+p 0n:
      Empty.absurd({0n == 1n+p : Nat}, false_ne_true(Equal.sym(Bool, True{}, False{}, e)))
    case 1n+p 1n+q:
      %eq_sound(p, q, e) : {1n+q == 1n+_ : Nat}
      {==}

# Nat.is_eq is symmetric
def eq_comm(a: Nat, b: Nat) -> {Nat.is_eq(a, b) == Nat.is_eq(b, a) : Bool}:
  match a b:
    case 0n 0n:
      {==}
    case 0n 1n+q:
      {==}
    case 1n+p 0n:
      {==}
    case 1n+p 1n+q:
      eq_comm(p, q)

# not(x == id) gives not(id == x)
def not_eq_comm(x: Nat, id: Nat, w: Laws.T(Bool.not(Nat.is_eq(x, id)))) -> Laws.T(Bool.not(Nat.is_eq(id, x))):
  %eq_comm(x, id) : Laws.T(Bool.not(_))
  w

# 1. Schema first, schema once
# ----------------------------

def Laws.schema_first(fmt, m, e):
  match m:
    case R.MSchema{ids}:
      Empty.absurd({R.verdict(R.MSchema{ids}, R.start(fmt)) == R.VReject{} : R.Verdict}, false_ne_true(e))
    case R.MDict{id, delta}:
      {==}
    case R.MBatch{refs}:
      {==}
    case R.MEos{}:
      {==}

def Laws.schema_accepted(fmt, ids):
  {==}

def Laws.schema_once(fmt, ids, declared, dicts):
  {==}

# 2. Dictionaries before use
# --------------------------

# a batch using an undefined id fails all_defined: the step case, over
# the verdict c of Nat.is_eq(x, id) for the head x of refs
def missing.fin(+id: Nat, +x: Nat, -t: List<&2, Nat>, +dicts: List<&2, R.DEntry>,
  rec: Laws.T(R.has(id, t)) -> {False{} == R.all_defined(t, dicts) : Bool},
  c: Bool, ec: {c == Nat.is_eq(x, id) : Bool}, w: Laws.T(c || R.has(id, t)),
  e: {False{} == R.tbl_has(id, dicts) : Bool})
  -> {False{} == (R.tbl_has(x, dicts) && R.all_defined(t, dicts)) : Bool}:
  match c:
    case True{}:
      %eq_sound(x, id, ec) : {False{} == (R.tbl_has(_, dicts) && R.all_defined(t, dicts)) : Bool}
      %e : {False{} == (_ && R.all_defined(t, dicts)) : Bool}
      {==}
    case False{}:
      %rec(w) : {False{} == (R.tbl_has(x, dicts) && _) : Bool}
      and_false(R.tbl_has(x, dicts))

def missing(+id: Nat, refs: List<&2, Nat>, +dicts: List<&2, R.DEntry>, w: Laws.T(R.has(id, refs)),
  +e: {False{} == R.tbl_has(id, dicts) : Bool}) -> {False{} == R.all_defined(refs, dicts) : Bool}:
  match refs:
    case Nil{}:
      Empty.absurd({False{} == R.all_defined(Nil{}, dicts) : Bool}, w)
    case Con{+x, t}:
      missing.fin(id, x, t, dicts, wt => missing(id, t, dicts, wt, e), Nat.is_eq(x, id), {==}, w, e)

def Laws.batch_needs_dictionary(fmt, declared, dicts, refs, id, w, e):
  %missing(id, refs, dicts, w, e) : {R.resp(R.step_batch(fmt, declared, dicts, _)) == R.VReject{} : R.Verdict}
  {==}

# the batch arm, over the verdict c of all_defined
def batch_acc.fin(-fmt: R.Fmt, -declared: List<&2, Nat>, -dicts: List<&2, R.DEntry>, c: Bool, w: Laws.T(c))
  -> {R.step_batch(fmt, declared, dicts, c) == (R.RState{fmt, R.PBody{}, declared, dicts}, R.VAccept{}) : R.RState & R.Verdict}:
  match c:
    case True{}:
      {==}
    case False{}:
      Empty.absurd({R.step_batch(fmt, declared, dicts, False{}) == (R.RState{fmt, R.PBody{}, declared, dicts}, R.VAccept{}) : R.RState & R.Verdict}, w)

def Laws.batch_accepted(fmt, declared, dicts, refs, w):
  batch_acc.fin(fmt, declared, dicts, R.all_defined(refs, dicts), w)

def Laws.all_null_batch_needs_no_dictionary(fmt, declared):
  {==}

# 3. Delta dictionaries
# ---------------------

# a delta with no base is rejected whether or not the id is declared
def delta_base.fin(-fmt: R.Fmt, -declared: List<&2, Nat>, -dicts: List<&2, R.DEntry>, -id: Nat, c: Bool)
  -> {R.resp(R.step_dict(fmt, declared, dicts, id, True{}, c, False{})) == R.VReject{} : R.Verdict}:
  match c:
    case True{}:
      {==}
    case False{}:
      {==}

def Laws.delta_needs_base(fmt, declared, dicts, id, e):
  %e : {R.resp(R.step_dict(fmt, declared, dicts, id, True{}, R.has(id, declared), _)) == R.VReject{} : R.Verdict}
  delta_base.fin(fmt, declared, dicts, id, R.has(id, declared))

# a delta adds one segment to the visible dictionary: the step case,
# over the verdict c of Nat.is_eq(eid, id) for the head entry
def segs_bump.fin(+id: Nat, +eid: Nat, +segs: Nat, +rest: List<&2, R.DEntry>,
  rec: Laws.T(R.tbl_has(id, rest)) -> {R.tbl_segs(id, R.tbl_bump(id, rest)) == 1n+R.tbl_segs(id, rest) : Nat},
  c: Bool, e: {c == Nat.is_eq(eid, id) : Bool}, w: Laws.T(c || R.tbl_has(id, rest)))
  -> {R.tbl_segs(id, R.bump_put(eid, segs, R.tbl_bump(id, rest), c)) == 1n+R.pick_nat(Nat.is_eq(eid, id), segs, R.tbl_segs(id, rest)) : Nat}:
  match c:
    case True{}:
      %e : {R.pick_nat(_, 1n+segs, R.tbl_segs(id, R.tbl_bump(id, rest))) == 1n+R.pick_nat(_, segs, R.tbl_segs(id, rest)) : Nat}
      {==}
    case False{}:
      %e : {R.pick_nat(_, segs, R.tbl_segs(id, R.tbl_bump(id, rest))) == 1n+R.pick_nat(_, segs, R.tbl_segs(id, rest)) : Nat}
      rec(w)

def segs_bump(+id: Nat, dicts: List<&2, R.DEntry>, w: Laws.T(R.tbl_has(id, dicts)))
  -> {R.tbl_segs(id, R.tbl_bump(id, dicts)) == 1n+R.tbl_segs(id, dicts) : Nat}:
  match dicts:
    case Nil{}:
      Empty.absurd({R.tbl_segs(id, R.tbl_bump(id, Nil{})) == 1n+R.tbl_segs(id, Nil{}) : Nat}, w)
    case Con{R.DEntry{+eid, +segs}, +rest}:
      segs_bump.fin(id, eid, segs, rest, wr => segs_bump(id, rest, wr), Nat.is_eq(eid, id), {==}, w)

# the claim of delta_appends, over the two verdicts
def DeltaGoal(+fmt: R.Fmt, +declared: List<&2, Nat>, +dicts: List<&2, R.DEntry>, +id: Nat, +c1: Bool, +c2: Bool) -> Type:
  {R.resp(R.step_dict(fmt, declared, dicts, id, True{}, c1, c2)) == R.VAccept{} : R.Verdict}
  & {R.tbl_segs(id, R.dicts_of(R.state(R.step_dict(fmt, declared, dicts, id, True{}, c1, c2)))) == 1n+R.tbl_segs(id, dicts) : Nat}

def delta_app.fin(-fmt: R.Fmt, -declared: List<&2, Nat>, +dicts: List<&2, R.DEntry>, +id: Nat,
  c1: Bool, wd: Laws.T(c1), c2: Bool, e2: {c2 == R.tbl_has(id, dicts) : Bool}, wt: Laws.T(c2))
  -> DeltaGoal(fmt, declared, dicts, id, c1, c2):
  match c1 c2:
    case True{} True{}:
      ({==}, segs_bump(id, dicts, true_T(R.tbl_has(id, dicts), e2)))
    case True{} False{}:
      Empty.absurd(DeltaGoal(fmt, declared, dicts, id, True{}, False{}), wt)
    case False{} True{}:
      Empty.absurd(DeltaGoal(fmt, declared, dicts, id, False{}, True{}), wd)
    case False{} False{}:
      Empty.absurd(DeltaGoal(fmt, declared, dicts, id, False{}, False{}), wd)

def Laws.delta_appends(fmt, declared, dicts, id, wd, wt):
  delta_app.fin(fmt, declared, dicts, id, R.has(id, declared), wd, R.tbl_has(id, dicts), {==}, wt)

# 4. Replacement dictionaries
# ---------------------------

# the claim of dictionary_defined and replacement_in_stream, over the
# two verdicts: accepted, and the id has one segment afterwards
def DefGoal(+fmt: R.Fmt, +declared: List<&2, Nat>, +dicts: List<&2, R.DEntry>, +id: Nat, +c1: Bool, +c2: Bool) -> Type:
  {R.resp(R.step_dict(fmt, declared, dicts, id, False{}, c1, c2)) == R.VAccept{} : R.Verdict}
  & {R.tbl_segs(id, R.dicts_of(R.state(R.step_dict(fmt, declared, dicts, id, False{}, c1, c2)))) == 1n : Nat}

# a freshly defined id has one segment
def segs_define(+id: Nat, -dicts: List<&2, R.DEntry>) -> {R.tbl_segs(id, R.tbl_define(id, dicts)) == 1n : Nat}:
  %eq_refl(id) : {R.pick_nat(_, 1n, R.tbl_segs(id, dicts)) == 1n : Nat}
  {==}

def defined.fin(-fmt: R.Fmt, -declared: List<&2, Nat>, -dicts: List<&2, R.DEntry>, +id: Nat, c1: Bool, wd: Laws.T(c1))
  -> DefGoal(fmt, declared, dicts, id, c1, False{}):
  match c1:
    case True{}:
      ({==}, segs_define(id, dicts))
    case False{}:
      Empty.absurd(DefGoal(fmt, declared, dicts, id, False{}, False{}), wd)

def Laws.dictionary_defined(fmt, declared, dicts, id, wd, e):
  %e : DefGoal(fmt, declared, dicts, id, R.has(id, declared), _)
  defined.fin(fmt, declared, dicts, id, R.has(id, declared), wd)

def replace.fin(-declared: List<&2, Nat>, -dicts: List<&2, R.DEntry>, +id: Nat, c1: Bool, wd: Laws.T(c1), c2: Bool, wt: Laws.T(c2))
  -> DefGoal(R.FStream{}, declared, dicts, id, c1, c2):
  match c1 c2:
    case True{} True{}:
      ({==}, segs_define(id, dicts))
    case True{} False{}:
      Empty.absurd(DefGoal(R.FStream{}, declared, dicts, id, True{}, False{}), wt)
    case False{} True{}:
      Empty.absurd(DefGoal(R.FStream{}, declared, dicts, id, False{}, True{}), wd)
    case False{} False{}:
      Empty.absurd(DefGoal(R.FStream{}, declared, dicts, id, False{}, False{}), wd)

def Laws.replacement_in_stream(declared, dicts, id, wd, wt):
  replace.fin(declared, dicts, id, R.has(id, declared), wd, R.tbl_has(id, dicts), wt)

# the claim of no_replacement_in_file, over the two verdicts
def FileGoal(+declared: List<&2, Nat>, +dicts: List<&2, R.DEntry>, +id: Nat, +c1: Bool, +c2: Bool) -> Type:
  {R.step_dict(R.FFile{}, declared, dicts, id, False{}, c1, c2) == (R.RState{R.FFile{}, R.PBody{}, declared, dicts}, R.VReject{}) : R.RState & R.Verdict}

def no_replace.fin(-declared: List<&2, Nat>, -dicts: List<&2, R.DEntry>, -id: Nat, c1: Bool, c2: Bool, wt: Laws.T(c2))
  -> FileGoal(declared, dicts, id, c1, c2):
  match c1 c2:
    case True{} True{}:
      {==}
    case True{} False{}:
      Empty.absurd(FileGoal(declared, dicts, id, True{}, False{}), wt)
    case False{} True{}:
      {==}
    case False{} False{}:
      Empty.absurd(FileGoal(declared, dicts, id, False{}, False{}), wt)

def Laws.no_replacement_in_file(declared, dicts, id, wt):
  no_replace.fin(declared, dicts, id, R.has(id, declared), R.tbl_has(id, dicts), wt)

def Laws.undeclared_dictionary_rejected(fmt, declared, dicts, id, delta, e):
  %e : {R.resp(R.step_dict(fmt, declared, dicts, id, delta, _, R.tbl_has(id, dicts))) == R.VReject{} : R.Verdict}
  {==}

# Invariant: the table only holds declared ids
# --------------------------------------------

def Laws.declared_start(fmt):
  Unit{}

# a bump keeps every entry's id: the step case, over the verdict c
def bump_decl.fin(+eid: Nat, -segs: Nat, -id: Nat, -rest: List<&2, R.DEntry>, +declared: List<&2, Nat>, c: Bool,
  we: Laws.T(R.has(eid, declared)), wr: Laws.T(R.tbl_declared(R.tbl_bump(id, rest), declared)))
  -> Laws.T(R.tbl_declared(R.bump_put(eid, segs, R.tbl_bump(id, rest), c), declared)):
  match c:
    case True{}:
      join.fin(R.has(eid, declared), R.tbl_declared(R.tbl_bump(id, rest), declared), we, wr)
    case False{}:
      join.fin(R.has(eid, declared), R.tbl_declared(R.tbl_bump(id, rest), declared), we, wr)

def bump_declared(+id: Nat, dicts: List<&2, R.DEntry>, +declared: List<&2, Nat>, w: Laws.T(R.tbl_declared(dicts, declared)))
  -> Laws.T(R.tbl_declared(R.tbl_bump(id, dicts), declared)):
  match dicts:
    case Nil{}:
      Unit{}
    case Con{R.DEntry{+eid, segs}, +rest}:
      split.fin(R.has(eid, declared), R.tbl_declared(rest, declared), w,
        Laws.T(R.tbl_declared(R.tbl_bump(id, R.DEntry{eid, segs} <> rest), declared)),
        we => wr => bump_decl.fin(eid, segs, id, rest, declared, Nat.is_eq(eid, id), we, bump_declared(id, rest, declared, wr)))

# the claim of declared_kept after one step of step_dict
def KeptGoal(+fmt: R.Fmt, +declared: List<&2, Nat>, +dicts: List<&2, R.DEntry>, +id: Nat, +delta: Bool, +c1: Bool, +c2: Bool) -> Type:
  Laws.T(R.tbl_declared(R.dicts_of(R.state(R.step_dict(fmt, declared, dicts, id, delta, c1, c2))), R.declared_of(R.state(R.step_dict(fmt, declared, dicts, id, delta, c1, c2)))))

# a replacement: the stream adds a declared id, the file changes nothing
def kept_replace.fin(fmt: R.Fmt, +declared: List<&2, Nat>, +dicts: List<&2, R.DEntry>, +id: Nat,
  e1: {True{} == R.has(id, declared) : Bool}, w: Laws.T(R.tbl_declared(dicts, declared)))
  -> KeptGoal(fmt, declared, dicts, id, False{}, True{}, True{}):
  match fmt:
    case R.FStream{}:
      join.fin(R.has(id, declared), R.tbl_declared(dicts, declared), true_T(R.has(id, declared), e1), w)
    case R.FFile{}:
      w

# the dictionary arm, over the declared verdict c1, isDelta and the exists verdict c2
def kept_dict.fin(fmt: R.Fmt, +declared: List<&2, Nat>, +dicts: List<&2, R.DEntry>, +id: Nat,
  c1: Bool, e1: {c1 == R.has(id, declared) : Bool}, delta: Bool, c2: Bool, w: Laws.T(R.tbl_declared(dicts, declared)))
  -> KeptGoal(fmt, declared, dicts, id, delta, c1, c2):
  match c1:
    case False{}:
      w
    case True{}:
      match delta c2:
        case True{} True{}:
          bump_declared(id, dicts, declared, w)
        case True{} False{}:
          w
        case False{} False{}:
          join.fin(R.has(id, declared), R.tbl_declared(dicts, declared), true_T(R.has(id, declared), e1), w)
        case False{} True{}:
          kept_replace.fin(fmt, declared, dicts, id, e1, w)

# the batch arm: the table is unchanged either way
def kept_batch.fin(-fmt: R.Fmt, -declared: List<&2, Nat>, -dicts: List<&2, R.DEntry>, c: Bool, w: Laws.T(R.tbl_declared(dicts, declared)))
  -> Laws.T(R.tbl_declared(R.dicts_of(R.state(R.step_batch(fmt, declared, dicts, c))), R.declared_of(R.state(R.step_batch(fmt, declared, dicts, c))))):
  match c:
    case True{}:
      w
    case False{}:
      w

def Laws.declared_kept(fmt, phase, declared, dicts, m, w):
  match phase:
    case R.PStart{}:
      match m:
        case R.MSchema{ids}:
          Unit{}
        case R.MDict{id, delta}:
          w
        case R.MBatch{refs}:
          w
        case R.MEos{}:
          w
    case R.PBody{}:
      match m:
        case R.MSchema{ids}:
          w
        case R.MDict{+id, delta}:
          kept_dict.fin(fmt, declared, dicts, id, R.has(id, declared), {==}, delta, R.tbl_has(id, dicts), w)
        case R.MBatch{refs}:
          kept_batch.fin(fmt, declared, dicts, R.all_defined(refs, dicts), w)
        case R.MEos{}:
          w
    case R.PEnd{}:
      w

# 5. End of stream
# ----------------

def Laws.eos_accepted(fmt, declared, dicts):
  {==}

def Laws.nothing_after_eos(fmt, declared, dicts, m):
  {==}

def Laws.eos_is_final(fmt, declared, dicts, msgs):
  match msgs:
    case Nil{}:
      Unit{}
    case Con{m, rest}:
      Laws.eos_is_final(fmt, declared, dicts, rest)

# 6. Anti-vacuity: the spec's own examples
# ----------------------------------------

def Laws.spec_delta_example(fmt):
  (Unit{}, {==})

def Laws.spec_replacement_example_stream():
  (Unit{}, {==})

def Laws.spec_replacement_example_file():
  {==}

# 7. Writer / reader round trip
# -----------------------------
# The writer emits one dictionary per id in `rem`, and the reader's table
# grows by one entry per message. The induction carries: every remaining
# id is declared (sub), every declared id is remaining or defined
# (covers), the remaining ids are distinct (nodup) and none of them is
# defined yet (disj). The last two make every dictionary a first
# definition, which is what a file reader needs.

# every declared id is still to be written or already defined
def covers(ids: List<&2, Nat>, +rem: List<&2, Nat>, +dicts: List<&2, R.DEntry>) -> Bool:
  match ids:
    case Nil{}:
      True{}
    case Con{+x, t}:
      (R.has(x, rem) || R.tbl_has(x, dicts)) && covers(t, rem, dicts)

# no remaining id is defined
def disj(rem: List<&2, Nat>, +dicts: List<&2, R.DEntry>) -> Bool:
  match rem:
    case Nil{}:
      True{}
    case Con{+x, t}:
      Bool.not(R.tbl_has(x, dicts)) && disj(t, dicts)

# Lemmas on sub
def sub_cons(xs: List<&2, Nat>, +y: Nat, +ys: List<&2, Nat>, w: Laws.T(R.sub(xs, ys))) -> Laws.T(R.sub(xs, y <> ys)):
  match xs:
    case Nil{}:
      Unit{}
    case Con{+x, t}:
      split.fin(R.has(x, ys), R.sub(t, ys), w, Laws.T(R.sub(x <> t, y <> ys)),
        wx => wt => join.fin(R.has(x, y <> ys), R.sub(t, y <> ys), or_r(Nat.is_eq(y, x), R.has(x, ys), wx), sub_cons(t, y, ys, wt)))

def sub_refl(xs: List<&2, Nat>) -> Laws.T(R.sub(xs, xs)):
  match xs:
    case Nil{}:
      Unit{}
    case Con{+x, +t}:
      join.fin(R.has(x, x <> t), R.sub(t, x <> t),
        or_l(Nat.is_eq(x, x), R.has(x, t), true_T(Nat.is_eq(x, x), eq_refl(x))),
        sub_cons(t, x, t, sub_refl(t)))

# Lemmas on covers
def covers_init(ids: List<&2, Nat>, +all: List<&2, Nat>, +dicts: List<&2, R.DEntry>, w: Laws.T(R.sub(ids, all))) -> Laws.T(covers(ids, all, dicts)):
  match ids:
    case Nil{}:
      Unit{}
    case Con{+x, t}:
      split.fin(R.has(x, all), R.sub(t, all), w, Laws.T(covers(x <> t, all, dicts)),
        wx => wt => join.fin(R.has(x, all) || R.tbl_has(x, dicts), covers(t, all, dicts), or_l(R.has(x, all), R.tbl_has(x, dicts), wx), covers_init(t, all, dicts, wt)))

def covers_step(ids: List<&2, Nat>, +id: Nat, +t: List<&2, Nat>, +dicts: List<&2, R.DEntry>, w: Laws.T(covers(ids, id <> t, dicts)))
  -> Laws.T(covers(ids, t, R.DEntry{id, 1n} <> dicts)):
  match ids:
    case Nil{}:
      Unit{}
    case Con{+x, rest}:
      split.fin((Nat.is_eq(id, x) || R.has(x, t)) || R.tbl_has(x, dicts), covers(rest, id <> t, dicts), w,
        Laws.T(covers(x <> rest, t, R.DEntry{id, 1n} <> dicts)),
        wx => wr => join.fin(R.has(x, t) || (Nat.is_eq(id, x) || R.tbl_has(x, dicts)), covers(rest, t, R.DEntry{id, 1n} <> dicts),
          or_rot(Nat.is_eq(id, x), R.has(x, t), R.tbl_has(x, dicts), wx), covers_step(rest, id, t, dicts, wr)))

def covers_nil(ids: List<&2, Nat>, +dicts: List<&2, R.DEntry>, w: Laws.T(covers(ids, Nil{}, dicts))) -> Laws.T(R.all_defined(ids, dicts)):
  match ids:
    case Nil{}:
      Unit{}
    case Con{+x, t}:
      split.fin(R.tbl_has(x, dicts), covers(t, Nil{}, dicts), w, Laws.T(R.all_defined(x <> t, dicts)),
        wx => wt => join.fin(R.tbl_has(x, dicts), R.all_defined(t, dicts), wx, covers_nil(t, dicts, wt)))

# Lemmas on disj
def disj_nil(rem: List<&2, Nat>) -> Laws.T(disj(rem, Nil{})):
  match rem:
    case Nil{}:
      Unit{}
    case Con{x, t}:
      disj_nil(t)

def disj_step(t: List<&2, Nat>, +id: Nat, +dicts: List<&2, R.DEntry>, wn: Laws.T(Bool.not(R.has(id, t))), wd: Laws.T(disj(t, dicts)))
  -> Laws.T(disj(t, R.DEntry{id, 1n} <> dicts)):
  match t:
    case Nil{}:
      Unit{}
    case Con{+x, rest}:
      not_or_split(Nat.is_eq(x, id), R.has(id, rest), wn, Laws.T(disj(x <> rest, R.DEntry{id, 1n} <> dicts)),
        wnx => wnr => split.fin(Bool.not(R.tbl_has(x, dicts)), disj(rest, dicts), wd, Laws.T(disj(x <> rest, R.DEntry{id, 1n} <> dicts)),
          wdx => wdr => join.fin(Bool.not(Nat.is_eq(id, x) || R.tbl_has(x, dicts)), disj(rest, R.DEntry{id, 1n} <> dicts),
            not_or_join(Nat.is_eq(id, x), R.tbl_has(x, dicts), not_eq_comm(x, id, wnx), wdx), disj_step(rest, id, dicts, wnr, wdr))))

# The invariant and its step
def Inv(+ids: List<&2, Nat>, +rem: List<&2, Nat>, +dicts: List<&2, R.DEntry>) -> Type:
  Laws.T(R.sub(rem, ids)) & Laws.T(covers(ids, rem, dicts)) & Laws.T(R.nodup(rem)) & Laws.T(disj(rem, dicts))

def InvStep(+ids: List<&2, Nat>, +id: Nat, +t: List<&2, Nat>, +dicts: List<&2, R.DEntry>) -> Type:
  Laws.T(R.has(id, ids)) & Laws.T(Bool.not(R.tbl_has(id, dicts))) & Inv(ids, t, R.DEntry{id, 1n} <> dicts)

def inv_step(+ids: List<&2, Nat>, +id: Nat, +t: List<&2, Nat>, +dicts: List<&2, R.DEntry>, inv: Inv(ids, id <> t, dicts)) -> InvStep(ids, id, t, dicts):
  (wsub, wcov, wnd, wdis) = inv
  split.fin(R.has(id, ids), R.sub(t, ids), wsub, InvStep(ids, id, t, dicts),
    wh => wsub2 => split.fin(Bool.not(R.has(id, t)), R.nodup(t), wnd, InvStep(ids, id, t, dicts),
      wnh => wnd2 => split.fin(Bool.not(R.tbl_has(id, dicts)), disj(t, dicts), wdis, InvStep(ids, id, t, dicts),
        wnt => wdis2 => (wh, wnt, wsub2, covers_step(ids, id, t, dicts, wcov), wnd2, disj_step(t, id, dicts, wnh, wdis2)))))

def inv_init(+ids: List<&2, Nat>, w: Laws.T(R.nodup(ids))) -> Inv(ids, ids, Nil{}):
  (sub_refl(ids), covers_init(ids, ids, Nil{}, sub_refl(ids)), w, disj_nil(ids))

# The batches: accepted while every id is defined, and the state is unchanged
def batches_ok.fin(-fmt: R.Fmt, -ids: List<&2, Nat>, -dicts: List<&2, R.DEntry>, -p: Nat, c: Bool, w: Laws.T(c),
  rec: Laws.T(R.all_accepted(R.verdicts(R.emit_batches(p, ids), R.RState{fmt, R.PBody{}, ids, dicts}))))
  -> Laws.T(R.accepted(R.resp(R.step_batch(fmt, ids, dicts, c))) && R.all_accepted(R.verdicts(R.emit_batches(p, ids), R.state(R.step_batch(fmt, ids, dicts, c))))):
  match c:
    case True{}:
      rec
    case False{}:
      Empty.absurd(Laws.T(R.accepted(R.resp(R.step_batch(fmt, ids, dicts, False{}))) && R.all_accepted(R.verdicts(R.emit_batches(p, ids), R.state(R.step_batch(fmt, ids, dicts, False{}))))), w)

def batches_ok(n: Nat, +ids: List<&2, Nat>, +fmt: R.Fmt, +dicts: List<&2, R.DEntry>, +w: Laws.T(R.all_defined(ids, dicts)))
  -> Laws.T(R.all_accepted(R.verdicts(R.emit_batches(n, ids), R.RState{fmt, R.PBody{}, ids, dicts}))):
  match n:
    case 0n:
      Unit{}
    case 1n+p:
      batches_ok.fin(fmt, ids, dicts, p, R.all_defined(ids, dicts), w, batches_ok(p, ids, fmt, dicts, w))

# The dictionaries: each is a first definition of a declared id
def dicts_ok.fin(-fmt: R.Fmt, -ids: List<&2, Nat>, -dicts: List<&2, R.DEntry>, -id: Nat, -t: List<&2, Nat>, -n: Nat,
  c1: Bool, wh: Laws.T(c1), c2: Bool, wn: Laws.T(Bool.not(c2)),
  rec: Laws.T(R.all_accepted(R.verdicts(R.emit_dicts(t, ids, n), R.RState{fmt, R.PBody{}, ids, R.DEntry{id, 1n} <> dicts}))))
  -> Laws.T(R.accepted(R.resp(R.step_dict(fmt, ids, dicts, id, False{}, c1, c2))) && R.all_accepted(R.verdicts(R.emit_dicts(t, ids, n), R.state(R.step_dict(fmt, ids, dicts, id, False{}, c1, c2))))):
  match c1:
    case False{}:
      Empty.absurd(Laws.T(R.accepted(R.resp(R.step_dict(fmt, ids, dicts, id, False{}, False{}, c2))) && R.all_accepted(R.verdicts(R.emit_dicts(t, ids, n), R.state(R.step_dict(fmt, ids, dicts, id, False{}, False{}, c2))))), wh)
    case True{}:
      match c2:
        case True{}:
          Empty.absurd(Laws.T(R.accepted(R.resp(R.step_dict(fmt, ids, dicts, id, False{}, True{}, True{}))) && R.all_accepted(R.verdicts(R.emit_dicts(t, ids, n), R.state(R.step_dict(fmt, ids, dicts, id, False{}, True{}, True{}))))), wn)
        case False{}:
          rec

# the step, with the stepped invariant as a parameter so it can be destructured
def dicts_ok.go(-fmt: R.Fmt, +ids: List<&2, Nat>, +dicts: List<&2, R.DEntry>, +id: Nat, -t: List<&2, Nat>, -n: Nat,
  st: InvStep(ids, id, t, dicts),
  rec: Inv(ids, t, R.DEntry{id, 1n} <> dicts) -> Laws.T(R.all_accepted(R.verdicts(R.emit_dicts(t, ids, n), R.RState{fmt, R.PBody{}, ids, R.DEntry{id, 1n} <> dicts}))))
  -> Laws.T(R.accepted(R.resp(R.step_dict(fmt, ids, dicts, id, False{}, R.has(id, ids), R.tbl_has(id, dicts)))) && R.all_accepted(R.verdicts(R.emit_dicts(t, ids, n), R.state(R.step_dict(fmt, ids, dicts, id, False{}, R.has(id, ids), R.tbl_has(id, dicts)))))):
  (wh, wnt, inv2) = st
  dicts_ok.fin(fmt, ids, dicts, id, t, n, R.has(id, ids), wh, R.tbl_has(id, dicts), wnt, rec(inv2))

def dicts_ok(rem: List<&2, Nat>, +ids: List<&2, Nat>, +n: Nat, +fmt: R.Fmt, +dicts: List<&2, R.DEntry>, inv: Inv(ids, rem, dicts))
  -> Laws.T(R.all_accepted(R.verdicts(R.emit_dicts(rem, ids, n), R.RState{fmt, R.PBody{}, ids, dicts}))):
  match rem:
    case Nil{}:
      (wsub, wcov, wnd, wdis) = inv
      batches_ok(n, ids, fmt, dicts, covers_nil(ids, dicts, wcov))
    case Con{+id, +t}:
      dicts_ok.go(fmt, ids, dicts, id, t, n, inv_step(ids, id, t, dicts, inv), inv2 => dicts_ok(t, ids, n, fmt, R.DEntry{id, 1n} <> dicts, inv2))

def Laws.writer_round_trip(fmt, ids, n, w):
  dicts_ok(ids, ids, n, fmt, Nil{}, inv_init(ids, w))

# The final state: the batches leave the table alone and EOS ends the stream
def final_batches.fin(-fmt: R.Fmt, -ids: List<&2, Nat>, -dicts: List<&2, R.DEntry>, -p: Nat, c: Bool, w: Laws.T(c),
  rec: {R.RState{fmt, R.PEnd{}, ids, dicts} == R.final(R.emit_batches(p, ids), R.RState{fmt, R.PBody{}, ids, dicts}) : R.RState})
  -> {R.RState{fmt, R.PEnd{}, ids, dicts} == R.final(R.emit_batches(p, ids), R.state(R.step_batch(fmt, ids, dicts, c))) : R.RState}:
  match c:
    case True{}:
      rec
    case False{}:
      Empty.absurd({R.RState{fmt, R.PEnd{}, ids, dicts} == R.final(R.emit_batches(p, ids), R.state(R.step_batch(fmt, ids, dicts, False{}))) : R.RState}, w)

def final_batches(n: Nat, +ids: List<&2, Nat>, +fmt: R.Fmt, +dicts: List<&2, R.DEntry>, +w: Laws.T(R.all_defined(ids, dicts)))
  -> {R.RState{fmt, R.PEnd{}, ids, dicts} == R.final(R.emit_batches(n, ids), R.RState{fmt, R.PBody{}, ids, dicts}) : R.RState}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      final_batches.fin(fmt, ids, dicts, p, R.all_defined(ids, dicts), w, final_batches(p, ids, fmt, dicts, w))

# the claim of writer_round_trip_state for a reader state r
def FinalGoal(+ids: List<&2, Nat>, +r: R.RState) -> Type:
  Laws.T(R.all_defined(ids, R.dicts_of(r))) & {R.phase_of(r) == R.PEnd{} : R.Phase}

def final_ok.fin(-fmt: R.Fmt, -ids: List<&2, Nat>, -dicts: List<&2, R.DEntry>, -id: Nat, -t: List<&2, Nat>, -n: Nat,
  c1: Bool, wh: Laws.T(c1), c2: Bool, wn: Laws.T(Bool.not(c2)),
  rec: FinalGoal(ids, R.final(R.emit_dicts(t, ids, n), R.RState{fmt, R.PBody{}, ids, R.DEntry{id, 1n} <> dicts})))
  -> FinalGoal(ids, R.final(R.emit_dicts(t, ids, n), R.state(R.step_dict(fmt, ids, dicts, id, False{}, c1, c2)))):
  match c1:
    case False{}:
      Empty.absurd(FinalGoal(ids, R.final(R.emit_dicts(t, ids, n), R.state(R.step_dict(fmt, ids, dicts, id, False{}, False{}, c2)))), wh)
    case True{}:
      match c2:
        case True{}:
          Empty.absurd(FinalGoal(ids, R.final(R.emit_dicts(t, ids, n), R.state(R.step_dict(fmt, ids, dicts, id, False{}, True{}, True{})))), wn)
        case False{}:
          rec

def final_ok.go(-fmt: R.Fmt, +ids: List<&2, Nat>, +dicts: List<&2, R.DEntry>, +id: Nat, -t: List<&2, Nat>, -n: Nat,
  st: InvStep(ids, id, t, dicts),
  rec: Inv(ids, t, R.DEntry{id, 1n} <> dicts) -> FinalGoal(ids, R.final(R.emit_dicts(t, ids, n), R.RState{fmt, R.PBody{}, ids, R.DEntry{id, 1n} <> dicts})))
  -> FinalGoal(ids, R.final(R.emit_dicts(t, ids, n), R.state(R.step_dict(fmt, ids, dicts, id, False{}, R.has(id, ids), R.tbl_has(id, dicts))))):
  (wh, wnt, inv2) = st
  final_ok.fin(fmt, ids, dicts, id, t, n, R.has(id, ids), wh, R.tbl_has(id, dicts), wnt, rec(inv2))

# the Nil case: the batches leave the table, EOS ends the stream
def final_ok.nil(+fmt: R.Fmt, +ids: List<&2, Nat>, +n: Nat, +dicts: List<&2, R.DEntry>, +wall: Laws.T(R.all_defined(ids, dicts)))
  -> FinalGoal(ids, R.final(R.emit_batches(n, ids), R.RState{fmt, R.PBody{}, ids, dicts})):
  %final_batches(n, ids, fmt, dicts, wall) : FinalGoal(ids, _)
  (wall, {==})

def final_ok(rem: List<&2, Nat>, +ids: List<&2, Nat>, +n: Nat, +fmt: R.Fmt, +dicts: List<&2, R.DEntry>, inv: Inv(ids, rem, dicts))
  -> FinalGoal(ids, R.final(R.emit_dicts(rem, ids, n), R.RState{fmt, R.PBody{}, ids, dicts})):
  match rem:
    case Nil{}:
      (wsub, wcov, wnd, wdis) = inv
      final_ok.nil(fmt, ids, n, dicts, covers_nil(ids, dicts, wcov))
    case Con{+id, +t}:
      final_ok.go(fmt, ids, dicts, id, t, n, inv_step(ids, id, t, dicts, inv), inv2 => final_ok(t, ids, n, fmt, R.DEntry{id, 1n} <> dicts, inv2))

def Laws.writer_round_trip_state(fmt, ids, n, w):
  final_ok(ids, ids, n, fmt, Nil{}, inv_init(ids, w))
