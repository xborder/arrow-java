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

# A model of the message-ordering rules of the Arrow IPC streaming and
# file formats, written in Bend 2. Messages are abstract: a schema that
# declares which dictionary ids exist, dictionary batches (id, isDelta),
# record batches (the dictionary ids they actually reference) and
# end-of-stream. Bytes, flatbuffers, alignment and the file footer are
# not modelled.
#
# The reader is a state machine: one message in, one verdict out, and a
# dictionary table (id -> number of segments) that accumulates. The
# table is an association list where a new definition is pushed in front
# of the old one, so a replacement shadows what it replaces and a delta
# bumps the segment count of the visible entry.
#
#   bend main.bend         # checks the model and runs the sample traces in main
#   bend PROOF.bend        # checks LAWS.bend against this model

import Base

# Protocol
# --------

# the two IPC containers; they differ only in dictionary replacement
type Fmt is Data:
  FStream{}
  FFile{}

# one encapsulated message, reduced to what the ordering rules look at
type Msg is Data:
  MSchema{ids: List<&2, Nat>}    # Schema: the dictionary ids its fields declare
  MDict{id: Nat, delta: Bool}    # DictionaryBatch{id, isDelta}
  MBatch{refs: List<&2, Nat>}    # RecordBatch: the dictionary ids it uses
  MEos{}                         # 0xFFFFFFFF 0x00000000

# the reader's answer to one message
type Verdict is Data:
  VAccept{}
  VReject{}

# where the reader is in the stream
type Phase is Data:
  PStart{}    # nothing read yet: only a schema is legal
  PBody{}     # schema read: dictionaries, batches and EOS are legal
  PEnd{}      # EOS read: nothing is legal

# one visible dictionary: its id and how many batches make it up
type DEntry is Data:
  DEntry{id: Nat, segs: Nat}

# reader state: format, phase, ids declared by the schema, dictionary table
type RState is Data:
  RState{fmt: Fmt, phase: Phase, declared: List<&2, Nat>, dicts: List<&2, DEntry>}

# Lists of ids
# ------------

# whether h occurs in xs
def has(+h: Nat, xs: List<&2, Nat>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{+x, t}:
      Nat.is_eq(x, h) || has(h, t)

# whether every x in xs occurs in ys
def sub(xs: List<&2, Nat>, +ys: List<&2, Nat>) -> Bool:
  match xs:
    case Nil{}:
      True{}
    case Con{+x, t}:
      has(x, ys) && sub(t, ys)

# whether no id occurs twice
def nodup(xs: List<&2, Nat>) -> Bool:
  match xs:
    case Nil{}:
      True{}
    case Con{+x, +t}:
      Bool.not(has(x, t)) && nodup(t)

# Dictionary table
# ----------------

# whether id has a dictionary
def tbl_has(+id: Nat, t: List<&2, DEntry>) -> Bool:
  match t:
    case Nil{}:
      False{}
    case Con{DEntry{+eid, segs}, rest}:
      Nat.is_eq(eid, id) || tbl_has(id, rest)

def pick_nat(c: Bool, a: Nat, b: Nat) -> Nat:
  match c:
    case True{}:
      a
    case False{}:
      b

# how many segments the visible dictionary for id has; 0 when none
def tbl_segs(+id: Nat, t: List<&2, DEntry>) -> Nat:
  match t:
    case Nil{}:
      0n
    case Con{DEntry{+eid, segs}, rest}:
      pick_nat(Nat.is_eq(eid, id), segs, tbl_segs(id, rest))

# the entry, with one more segment when hit
def bump_put(eid: Nat, segs: Nat, rest: List<&2, DEntry>, hit: Bool) -> List<&2, DEntry>:
  match hit:
    case True{}:
      DEntry{eid, 1n+segs} <> rest
    case False{}:
      DEntry{eid, segs} <> rest

# a delta batch for id: one more segment on every entry for id
def tbl_bump(+id: Nat, t: List<&2, DEntry>) -> List<&2, DEntry>:
  match t:
    case Nil{}:
      Nil{}
    case Con{DEntry{+eid, segs}, rest}:
      bump_put(eid, segs, tbl_bump(id, rest), Nat.is_eq(eid, id))

# a non-delta batch for id: a fresh one-segment dictionary, shadowing any older one
def tbl_define(id: Nat, t: List<&2, DEntry>) -> List<&2, DEntry>:
  DEntry{id, 1n} <> t

# whether every referenced id has a dictionary
def all_defined(refs: List<&2, Nat>, +t: List<&2, DEntry>) -> Bool:
  match refs:
    case Nil{}:
      True{}
    case Con{+x, rest}:
      tbl_has(x, t) && all_defined(rest, t)

# whether every dictionary in the table was declared by the schema
def tbl_declared(t: List<&2, DEntry>, +declared: List<&2, Nat>) -> Bool:
  match t:
    case Nil{}:
      True{}
    case Con{DEntry{+eid, segs}, rest}:
      has(eid, declared) && tbl_declared(rest, declared)

# Transitions
# -----------

# before the schema: only a schema is accepted
def step_start(fmt: Fmt, declared: List<&2, Nat>, dicts: List<&2, DEntry>, m: Msg) -> RState & Verdict:
  match m:
    case MSchema{ids}:
      (RState{fmt, PBody{}, ids, Nil{}}, VAccept{})
    case MDict{id, delta}:
      (RState{fmt, PStart{}, declared, dicts}, VReject{})
    case MBatch{refs}:
      (RState{fmt, PStart{}, declared, dicts}, VReject{})
    case MEos{}:
      (RState{fmt, PStart{}, declared, dicts}, VReject{})

# a non-delta batch for an id that already has a dictionary: a
# replacement, legal in a stream and illegal in a file
def step_replace(fmt: Fmt, declared: List<&2, Nat>, dicts: List<&2, DEntry>, id: Nat) -> RState & Verdict:
  match fmt:
    case FStream{}:
      (RState{FStream{}, PBody{}, declared, tbl_define(id, dicts)}, VAccept{})
    case FFile{}:
      (RState{FFile{}, PBody{}, declared, dicts}, VReject{})

# a dictionary batch for a declared id, by isDelta and by whether a
# dictionary for the id exists
def step_dict_ok(fmt: Fmt, declared: List<&2, Nat>, dicts: List<&2, DEntry>, id: Nat, delta: Bool, exists: Bool) -> RState & Verdict:
  match delta exists:
    case True{} True{}:
      (RState{fmt, PBody{}, declared, tbl_bump(id, dicts)}, VAccept{})
    case True{} False{}:
      (RState{fmt, PBody{}, declared, dicts}, VReject{})
    case False{} False{}:
      (RState{fmt, PBody{}, declared, tbl_define(id, dicts)}, VAccept{})
    case False{} True{}:
      step_replace(fmt, declared, dicts, id)

# a dictionary batch: rejected unless the schema declared the id
def step_dict(fmt: Fmt, declared: List<&2, Nat>, dicts: List<&2, DEntry>, id: Nat, delta: Bool, in_schema: Bool, exists: Bool) -> RState & Verdict:
  match in_schema:
    case True{}:
      step_dict_ok(fmt, declared, dicts, id, delta, exists)
    case False{}:
      (RState{fmt, PBody{}, declared, dicts}, VReject{})

# a record batch: accepted when every dictionary it uses is defined
def step_batch(fmt: Fmt, declared: List<&2, Nat>, dicts: List<&2, DEntry>, ok: Bool) -> RState & Verdict:
  match ok:
    case True{}:
      (RState{fmt, PBody{}, declared, dicts}, VAccept{})
    case False{}:
      (RState{fmt, PBody{}, declared, dicts}, VReject{})

# after the schema
def step_body(fmt: Fmt, +declared: List<&2, Nat>, +dicts: List<&2, DEntry>, m: Msg) -> RState & Verdict:
  match m:
    case MSchema{ids}:
      (RState{fmt, PBody{}, declared, dicts}, VReject{})
    case MDict{+id, delta}:
      step_dict(fmt, declared, dicts, id, delta, has(id, declared), tbl_has(id, dicts))
    case MBatch{refs}:
      step_batch(fmt, declared, dicts, all_defined(refs, dicts))
    case MEos{}:
      (RState{fmt, PEnd{}, declared, dicts}, VAccept{})

# one message against one state, by phase
def step(fmt: Fmt, phase: Phase, +declared: List<&2, Nat>, +dicts: List<&2, DEntry>, m: Msg) -> RState & Verdict:
  match phase:
    case PStart{}:
      step_start(fmt, declared, dicts, m)
    case PBody{}:
      step_body(fmt, declared, dicts, m)
    case PEnd{}:
      (RState{fmt, PEnd{}, declared, dicts}, VReject{})

def step_r(m: Msg, r: RState) -> RState & Verdict:
  RState{fmt, phase, declared, dicts} = r
  step(fmt, phase, declared, dicts, m)

# projections of a step
def state(sv: RState & Verdict) -> RState:
  (s, v) = sv
  s

def resp(sv: RState & Verdict) -> Verdict:
  (s, v) = sv
  v

def next(m: Msg, r: RState) -> RState:
  state(step_r(m, r))

def verdict(m: Msg, r: RState) -> Verdict:
  resp(step_r(m, r))

def start(fmt: Fmt) -> RState:
  RState{fmt, PStart{}, Nil{}, Nil{}}

# Traces
# ------

# the verdict on every message of a stream, in order
def verdicts(msgs: List<&2, Msg>, +r: RState) -> List<&2, Verdict>:
  match msgs:
    case Nil{}:
      Nil{}
    case Con{+m, rest}:
      verdict(m, r) <> verdicts(rest, next(m, r))

# the reader after a whole stream
def final(msgs: List<&2, Msg>, r: RState) -> RState:
  match msgs:
    case Nil{}:
      r
    case Con{m, rest}:
      final(rest, next(m, r))

def accepted(v: Verdict) -> Bool:
  match v:
    case VAccept{}:
      True{}
    case VReject{}:
      False{}

def all_accepted(vs: List<&2, Verdict>) -> Bool:
  match vs:
    case Nil{}:
      True{}
    case Con{v, rest}:
      accepted(v) && all_accepted(rest)

def all_rejected(vs: List<&2, Verdict>) -> Bool:
  match vs:
    case Nil{}:
      True{}
    case Con{v, rest}:
      Bool.not(accepted(v)) && all_rejected(rest)

# Observers
# ---------

def is_schema(m: Msg) -> Bool:
  match m:
    case MSchema{ids}:
      True{}
    case MDict{id, delta}:
      False{}
    case MBatch{refs}:
      False{}
    case MEos{}:
      False{}

def phase_of(r: RState) -> Phase:
  RState{fmt, phase, declared, dicts} = r
  phase

def dicts_of(r: RState) -> List<&2, DEntry>:
  RState{fmt, phase, declared, dicts} = r
  dicts

def declared_of(r: RState) -> List<&2, Nat>:
  RState{fmt, phase, declared, dicts} = r
  declared

# Writer
# ------

# What ArrowWriter does for one VectorSchemaRoot: the schema, then one
# non-delta dictionary batch per dictionary id in use, then the record
# batches, then EOS. The Java stream and file writers emit the same
# sequence; the file writer adds a footer that is not modelled.

# n record batches that use every id, then EOS
def emit_batches(n: Nat, +ids: List<&2, Nat>) -> List<&2, Msg>:
  match n:
    case 0n:
      [MEos{}]
    case 1n+p:
      MBatch{ids} <> emit_batches(p, ids)

# one dictionary batch per remaining id, then the batches
def emit_dicts(rem: List<&2, Nat>, ids: List<&2, Nat>, n: Nat) -> List<&2, Msg>:
  match rem:
    case Nil{}:
      emit_batches(n, ids)
    case Con{id, t}:
      MDict{id, False{}} <> emit_dicts(t, ids, n)

def write(+ids: List<&2, Nat>, n: Nat) -> List<&2, Msg>:
  MSchema{ids} <> emit_dicts(ids, ids, n)

# Show
# ----

def Verdict.show(v: Verdict) -> String:
  match v:
    case VAccept{}:
      "accept"
    case VReject{}:
      "reject"

def verdicts_show(vs: List<&2, Verdict>) -> String:
  match vs:
    case Nil{}:
      ""
    case Con{v, rest}:
      " " ++ Verdict.show(v) ++ verdicts_show(rest)

def Fmt.show(fmt: Fmt) -> String:
  match fmt:
    case FStream{}:
      "stream"
    case FFile{}:
      "file  "

# "id:segments" for every declared id, in the reader's final table
def table_show(ids: List<&2, Nat>, +dicts: List<&2, DEntry>) -> String:
  match ids:
    case Nil{}:
      ""
    case Con{+id, rest}:
      " " ++ Nat.show(id) ++ ":" ++ Nat.show(tbl_segs(id, dicts)) ++ table_show(rest, dicts)

def final_show(r: RState) -> String:
  RState{fmt, phase, declared, dicts} = r
  table_show(declared, dicts)

def trace_show(+fmt: Fmt, +msgs: List<&2, Msg>) -> String:
  Fmt.show(fmt) ++ " |" ++ verdicts_show(verdicts(msgs, start(fmt))) ++ " | table" ++ final_show(final(msgs, start(fmt)))

# the spec's delta example: schema, dict 0, batch, dict 0 delta, batch, EOS
def delta_example() -> List<&2, Msg>:
  [MSchema{[0n]}, MDict{0n, False{}}, MBatch{[0n]}, MDict{0n, True{}}, MBatch{[0n]}, MEos{}]

# the spec's replacement example: legal in a stream, illegal in a file
def replacement_example() -> List<&2, Msg>:
  [MSchema{[0n]}, MDict{0n, False{}}, MBatch{[0n]}, MDict{0n, False{}}, MBatch{[0n]}, MEos{}]

# a batch that uses dictionary 1 before it is defined, and a message after EOS
def bad_example() -> List<&2, Msg>:
  [MSchema{[0n, 1n]}, MDict{0n, False{}}, MBatch{[0n, 1n]}, MDict{1n, False{}}, MBatch{[0n, 1n]}, MEos{}, MBatch{[0n]}]

def main() -> IO(Unit):
  do IO<Unit>:
    IO.print("delta        " ++ trace_show(FStream{}, delta_example()))
    IO.print("delta        " ++ trace_show(FFile{}, delta_example()))
    IO.print("replacement  " ++ trace_show(FStream{}, replacement_example()))
    IO.print("replacement  " ++ trace_show(FFile{}, replacement_example()))
    IO.print("bad          " ++ trace_show(FStream{}, bad_example()))
    IO.print("writer       " ++ trace_show(FFile{}, write([3n, 7n], 2n)))
