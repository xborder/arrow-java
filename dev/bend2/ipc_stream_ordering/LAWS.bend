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

# The laws of the IPC ordering model. The human states them; PROOF.bend
# must prove them. Each law quotes the sentence of the Arrow columnar
# format specification (docs/source/format/Columnar.rst, sections "IPC
# Streaming Format", "IPC File Format" and "Dictionary Messages") or the
# behaviour of this repository's ArrowReader / ArrowWriter that it
# formalises. Laws that cannot be proven or expressed are kept at the
# end, commented out, with the reason.

import Base
import ./main.bend as R

# the truth of a Bool as a type: Unit when True, Empty when False
def T(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

# a reader that has read the schema, in any format, with any table
def body(fmt: R.Fmt, declared: List<&2, Nat>, dicts: List<&2, R.DEntry>) -> R.RState:
  R.RState{fmt, R.PBody{}, declared, dicts}

# 1. Schema first, schema once
# ----------------------------

# LAW: "The schema comes first in the stream." A stream whose first
# message is not a schema is rejected at that message.
# (ArrowStreamReader.readSchema: "Expected schema but header was ...")
law schema_first:
  for +fmt : R.Fmt
  for  m   : R.Msg
  for  e   : {False{} == R.is_schema(m) : Bool}
  {R.verdict(m, R.start(fmt)) == R.VReject{} : R.Verdict}

# LAW (anti-vacuity): a schema as the first message is accepted, and the
# reader then knows exactly the ids the schema declared.
law schema_accepted:
  for +fmt : R.Fmt
  for +ids : List<&2, Nat>
  {R.step_r(R.MSchema{ids}, R.start(fmt)) == (body(fmt, ids, Nil{}), R.VAccept{}) : R.RState & R.Verdict}

# LAW: "it is the same for all of the record batches that follow": a
# second schema is rejected.
# (ArrowStreamReader.loadNextBatch: "Expected RecordBatch or
# DictionaryBatch but header was Schema")
law schema_once:
  for +fmt      : R.Fmt
  for +ids      : List<&2, Nat>
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  {R.verdict(R.MSchema{ids}, body(fmt, declared, dicts)) == R.VReject{} : R.Verdict}

# 2. Dictionaries before use
# --------------------------

# LAW: "before any dictionary key is used in a RecordBatch it should be
# defined in a DictionaryBatch." A record batch that uses an id with no
# dictionary is rejected.
law batch_needs_dictionary:
  for +fmt      : R.Fmt
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  for +refs     : List<&2, Nat>
  for +id       : Nat
  for  w        : T(R.has(id, refs))
  for  e        : {False{} == R.tbl_has(id, dicts) : Bool}
  {R.verdict(R.MBatch{refs}, body(fmt, declared, dicts)) == R.VReject{} : R.Verdict}

# LAW (anti-vacuity): a record batch whose dictionaries are all defined
# is accepted and leaves the reader unchanged.
law batch_accepted:
  for +fmt      : R.Fmt
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  for +refs     : List<&2, Nat>
  for  w        : T(R.all_defined(refs, dicts))
  {R.step_r(R.MBatch{refs}, body(fmt, declared, dicts)) == (body(fmt, declared, dicts), R.VAccept{}) : R.RState & R.Verdict}

# LAW: "An edge-case for interleaved dictionary and record batches occurs
# when the record batches contain dictionary encoded arrays that are
# completely null. In this case, the dictionary for the encoded column
# might appear after the first record batch." A batch that uses no
# dictionary key is accepted with an empty table.
# (ArrowStreamReader.checkDictionaries: "vector.getNullCount() <
# vector.getValueCount()")
law all_null_batch_needs_no_dictionary:
  for +fmt      : R.Fmt
  for +declared : List<&2, Nat>
  {R.verdict(R.MBatch{Nil{}}, body(fmt, declared, Nil{})) == R.VAccept{} : R.Verdict}

# 3. Delta dictionaries
# ---------------------

# LAW: "The dictionary isDelta flag allows existing dictionaries to be
# expanded": a delta batch for an id with no dictionary is rejected.
# This is the spec's rule; ArrowReader.loadDictionary is more lenient and
# appends the delta to the empty vector it created from the schema. The
# model follows the spec.
law delta_needs_base:
  for +fmt      : R.Fmt
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  for +id       : Nat
  for  e        : {False{} == R.tbl_has(id, dicts) : Bool}
  {R.verdict(R.MDict{id, True{}}, body(fmt, declared, dicts)) == R.VReject{} : R.Verdict}

# LAW: "A dictionary batch with isDelta set indicates that its vector
# should be concatenated with those of any previous batches with the same
# id." A delta on an existing, declared id is accepted in both formats
# and the dictionary gains one segment.
# (ArrowReader.loadDictionary: VectorBatchAppender.batchAppend)
law delta_appends:
  for +fmt      : R.Fmt
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  for +id       : Nat
  for  wd       : T(R.has(id, declared))
  for  wt       : T(R.tbl_has(id, dicts))
  {R.verdict(R.MDict{id, True{}}, body(fmt, declared, dicts)) == R.VAccept{} : R.Verdict}
  & {R.tbl_segs(id, R.dicts_of(R.next(R.MDict{id, True{}}, body(fmt, declared, dicts)))) == 1n+R.tbl_segs(id, dicts) : Nat}

# 4. Replacement dictionaries
# ---------------------------

# LAW: a first, non-delta dictionary batch for a declared id is accepted
# in both formats and defines a one-segment dictionary.
law dictionary_defined:
  for +fmt      : R.Fmt
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  for +id       : Nat
  for  wd       : T(R.has(id, declared))
  for  e        : {False{} == R.tbl_has(id, dicts) : Bool}
  {R.verdict(R.MDict{id, False{}}, body(fmt, declared, dicts)) == R.VAccept{} : R.Verdict}
  & {R.tbl_segs(id, R.dicts_of(R.next(R.MDict{id, False{}}, body(fmt, declared, dicts)))) == 1n : Nat}

# LAW: "if isDelta is set to false, then the dictionary replaces the
# existing dictionary for the same ID." In a stream the replacement is
# accepted and the dictionary is back to one segment.
law replacement_in_stream:
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  for +id       : Nat
  for  wd       : T(R.has(id, declared))
  for  wt       : T(R.tbl_has(id, dicts))
  {R.verdict(R.MDict{id, False{}}, body(R.FStream{}, declared, dicts)) == R.VAccept{} : R.Verdict}
  & {R.tbl_segs(id, R.dicts_of(R.next(R.MDict{id, False{}}, body(R.FStream{}, declared, dicts)))) == 1n : Nat}

# LAW: "The IPC File format does not support dictionary replacement,
# i.e. only one non-delta dictionary batch can be emitted for a given
# dictionary ID." In a file the replacement is rejected and the table is
# unchanged.
# (ArrowFileWriter.ensureDictionariesWritten: "Replacement dictionaries
# are not supported in the IPC file format.")
law no_replacement_in_file:
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  for +id       : Nat
  for  wt       : T(R.tbl_has(id, dicts))
  {R.step_r(R.MDict{id, False{}}, body(R.FFile{}, declared, dicts)) == (body(R.FFile{}, declared, dicts), R.VReject{}) : R.RState & R.Verdict}

# LAW: "The dictionary types are found in the schema": a dictionary batch
# for an id the schema did not declare is rejected, delta or not.
# (ArrowReader.loadDictionary: "Dictionary ID ... not defined in schema")
law undeclared_dictionary_rejected:
  for +fmt      : R.Fmt
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  for +id       : Nat
  for  delta    : Bool
  for  e        : {False{} == R.has(id, declared) : Bool}
  {R.verdict(R.MDict{id, delta}, body(fmt, declared, dicts)) == R.VReject{} : R.Verdict}

# LAW (invariant): every dictionary in the table was declared by the
# schema. It holds at the start and every message keeps it, so it holds
# in every reachable state.
law declared_start:
  for +fmt : R.Fmt
  T(R.tbl_declared(R.dicts_of(R.start(fmt)), R.declared_of(R.start(fmt))))

law declared_kept:
  for +fmt      : R.Fmt
  for  phase    : R.Phase
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  for  m        : R.Msg
  for  w        : T(R.tbl_declared(dicts, declared))
  T(R.tbl_declared(R.dicts_of(R.next(m, R.RState{fmt, phase, declared, dicts})), R.declared_of(R.next(m, R.RState{fmt, phase, declared, dicts}))))

# 5. End of stream
# ----------------

# LAW: "The stream writer can signal end-of-stream (EOS)": EOS after the
# schema is accepted and ends the stream.
law eos_accepted:
  for +fmt      : R.Fmt
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  {R.step_r(R.MEos{}, body(fmt, declared, dicts)) == (R.RState{fmt, R.PEnd{}, declared, dicts}, R.VAccept{}) : R.RState & R.Verdict}

# LAW: no message is accepted after EOS, whatever it is.
# (MessageChannelReader.readNext returns null at EOS and
# ArrowStreamReader.loadNextBatch then answers false forever)
law nothing_after_eos:
  for +fmt      : R.Fmt
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  for  m        : R.Msg
  {R.verdict(m, R.RState{fmt, R.PEnd{}, declared, dicts}) == R.VReject{} : R.Verdict}

# LAW (trace form): every message of any stream that follows an EOS is
# rejected.
law eos_is_final:
  for +fmt      : R.Fmt
  for +declared : List<&2, Nat>
  for +dicts    : List<&2, R.DEntry>
  for +msgs     : List<&2, R.Msg>
  T(R.all_rejected(R.verdicts(msgs, R.RState{fmt, R.PEnd{}, declared, dicts})))

# 6. Anti-vacuity: the spec's own examples
# ----------------------------------------

# LAW: the delta example of "Dictionary Messages" (schema, dictionary 0,
# batch, dictionary 0 delta, batch, EOS) is accepted in full by both
# formats and leaves dictionary 0 with two segments.
law spec_delta_example:
  for +fmt : R.Fmt
  T(R.all_accepted(R.verdicts(R.delta_example(), R.start(fmt))))
  & {R.tbl_segs(0n, R.dicts_of(R.final(R.delta_example(), R.start(fmt)))) == 2n : Nat}

# LAW: the replacement example of "Dictionary Messages" is accepted in
# full by a stream reader and leaves dictionary 0 with one segment.
law spec_replacement_example_stream:
  T(R.all_accepted(R.verdicts(R.replacement_example(), R.start(R.FStream{}))))
  & {R.tbl_segs(0n, R.dicts_of(R.final(R.replacement_example(), R.start(R.FStream{})))) == 1n : Nat}

# LAW: the same replacement example is not accepted in full by a file
# reader: exactly the fourth message, the replacement, is rejected.
law spec_replacement_example_file:
  {R.verdicts(R.replacement_example(), R.start(R.FFile{})) == [R.VAccept{}, R.VAccept{}, R.VAccept{}, R.VReject{}, R.VAccept{}, R.VAccept{}] : List<&2, R.Verdict>}

# 7. Writer / reader round trip
# -----------------------------

# LAW: "<SCHEMA> <DICTIONARY 0> ... <DICTIONARY k - 1> <RECORD BATCH 0>
# ... <RECORD BATCH n - 1> <EOS>": what ArrowWriter emits for a schema
# with distinct dictionary ids (ArrowWriter.dictionaryIdsUsed is a Set)
# and any number of record batches is accepted in full by the reader of
# either format.
law writer_round_trip:
  for +fmt : R.Fmt
  for +ids : List<&2, Nat>
  for +n   : Nat
  for  w   : T(R.nodup(ids))
  T(R.all_accepted(R.verdicts(R.write(ids, n), R.start(fmt))))

# LAW: after the round trip every declared dictionary is defined and the
# reader has seen EOS.
law writer_round_trip_state:
  for +fmt : R.Fmt
  for +ids : List<&2, Nat>
  for +n   : Nat
  for  w   : T(R.nodup(ids))
  T(R.all_defined(ids, R.dicts_of(R.final(R.write(ids, n), R.start(fmt)))))
  & {R.phase_of(R.final(R.write(ids, n), R.start(fmt))) == R.PEnd{} : R.Phase}

# Not proven or not expressible
# -----------------------------

# NOT EXPRESSIBLE: "each message ... is padded to an 8-byte boundary"
# (Encapsulated message format). The model has no bytes: a message is
# an abstract constructor with no length, and Bend's Base ships no
# theory of Nat.mod, so even with lengths modelled as Nat the proof of
# `Nat.mod(padded(n), 8n) == 0n` would need a divisibility library
# written from scratch first.
# law metadata_padded_to_8:
#   for +n : Nat
#   {Nat.mod(R.padded(n), 8n) == 0n : Nat}

# NOT EXPRESSIBLE: "bodyLength: long" (Message.fbs) and Block.offset /
# metaDataLength / bodyLength (File.fbs) are int64 / int32. Bend has no
# 64-bit integer type (only Nat, U32 and F32), so overflow and sign rules
# of the wire format cannot be stated; lengths would be unbounded Nats.
# law body_length_fits_int64:
#   for m : R.Msg
#   {Nat.is_lt(R.body_length(m), 9223372036854775808n) == True{} : Bool}

# NOT MODELLED: "The metadata version, schema and custom metadata
# serialized in the IPC file footer MUST be identical to ... the embedded
# IPC stream" and "The dictionaries and record batches serialized in the
# IPC file footer SHOULD be listed in the same order as they appear in
# the embedded IPC stream". The footer is not part of this model: the
# reader here is the sequential stream reader that the file format
# embeds, and ArrowFileReader reads the footer instead of the EOS. A
# footer model would add a list of blocks per message kind and a law
# equating it to the filtered stream; it is left for a follow-up.
# law footer_matches_stream:
#   for +ids : List<&2, Nat>
#   for +n   : Nat
#   {R.footer_dicts(R.write_file(ids, n)) == ids : List<&2, Nat>}

# NOT MODELLED: "Delta dictionary batches in an IPC File are applied in
# the order they appear in the file footer." Random access over the
# footer is not modelled; the reader here consumes messages in stream
# order, which by the SHOULD above is the footer order.
