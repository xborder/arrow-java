<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Bend 2 proof of concept: IPC stream ordering laws

A checked Bend 2 model of the message-ordering rules of the Arrow IPC
streaming and file formats: schema first, dictionaries before use,
delta and replacement dictionaries, end of stream. The rules come from
the columnar format specification (`docs/source/format/Columnar.rst`,
sections "IPC Streaming Format", "IPC File Format" and "Dictionary
Messages") and from how this repository's `ArrowStreamReader`,
`ArrowReader`, `ArrowFileWriter` and `ArrowStreamWriter` enforce them.

Written against Bend 2.0.21, commit `6018e28` of
[bendlang/bend](https://github.com/bendlang/bend), on 2026-09-20.
The research notes and the first model (the Flight SQL
prepared-statement lifecycle) are on a sibling branch under
`dev/bend2/`; this directory is self-contained and copies the proof
helpers it needs.

## 1. What is modelled

`main.bend` models a validating reader as a state machine over a list
of abstract messages. Bytes, flatbuffers, alignment, the file magic and
the file footer are not modelled.

| Concept | Model |
| --- | --- |
| Format | `Fmt`: `FStream{}` or `FFile{}`. They differ only in dictionary replacement. |
| Message | `Msg`: `MSchema{ids}` (the dictionary ids the schema's fields declare), `MDict{id, delta}` (`DictionaryBatch.id`, `isDelta`), `MBatch{refs}` (the dictionary ids a record batch actually uses), `MEos{}`. |
| Reader | `RState{fmt, phase, declared, dicts}`: the format, a phase (`PStart`, `PBody`, `PEnd`), the ids the schema declared, and the dictionary table. |
| Dictionary table | `List<DEntry{id, segs}>`, an association list. A non-delta batch pushes a fresh one-segment entry in front (so a replacement shadows the old dictionary); a delta bumps the segment count of the visible entry. `tbl_has` and `tbl_segs` read the table. |
| Verdict | `VAccept{}` or `VReject{}` per message. |
| Trace | `verdicts(msgs, r)` gives one verdict per message; `final(msgs, r)` the reader afterwards. |
| Writer | `write(ids, n)`: schema, one non-delta dictionary per id, `n` record batches using every id, EOS. This is the sequence `ArrowWriter.writeBatch` / `end` produces for one `VectorSchemaRoot` in either format. |

The reader's rules, as `step` implements them:

- `PStart`: a schema is accepted and moves to `PBody`; anything else is
  rejected.
- `PBody`, schema: rejected. Dictionary batch: rejected unless the id
  was declared; a delta is accepted only if a dictionary for the id
  exists and then adds a segment; a non-delta on a new id defines it;
  a non-delta on an existing id is a replacement, accepted in a stream
  and rejected in a file. Record batch: accepted iff every id it uses
  has a dictionary. EOS: accepted, moves to `PEnd`.
- `PEnd`: everything is rejected.

Two modelling choices worth stating:

- **Record batch references.** `MBatch{refs}` carries the ids whose
  keys the batch actually uses. A dictionary-encoded column that is
  entirely null uses no key, so `refs` omits it. This is the spec's
  all-null edge case and matches `ArrowStreamReader.checkDictionaries`,
  which only complains when `nullCount < valueCount`.
- **Delta with no base.** The spec says `isDelta` "allows existing
  dictionaries to be expanded" and that a delta "should be concatenated
  with those of any previous batches with the same id". The model
  rejects a delta for an id with no dictionary. `ArrowReader.loadDictionary`
  is more lenient: it pre-creates an empty vector per declared id in
  `initialize()` and appends the delta to it, so Java accepts this case.
  The law records the choice; mutation M4 below shows that switching to
  Java's rule is caught.

One observation about the Java reader fell out of writing the model.
`ArrowStreamReader.checkDictionaries` tests
`!dictionaries.containsKey(encoding.getId())`, but `dictionaries` is
populated for every declared id in `ArrowReader.initialize()` before
any batch is read, so the test is always false and a record batch whose
dictionary was never sent is loaded against an empty dictionary rather
than rejected. The model follows the spec here (law
`batch_needs_dictionary`). No Java code is changed in this PR.

## 2. The laws

`LAWS.bend` states 22 laws; `PROOF.bend` proves all of them. Each law
quotes its source. `T(b)` is the truth of a Bool as a type.

### Schema first, schema once

| Law | Spec / Java sentence | Claim |
| --- | --- | --- |
| `schema_first` | "The schema comes first in the stream." (`readSchema`: "Expected schema but header was ...") | Any non-schema first message is rejected. |
| `schema_accepted` | anti-vacuity | A first schema is accepted and the reader records exactly its ids. |
| `schema_once` | "it is the same for all of the record batches that follow" | A second schema is rejected. |

### Dictionaries before use

| Law | Spec / Java sentence | Claim |
| --- | --- | --- |
| `batch_needs_dictionary` | "before any dictionary key is used in a RecordBatch it should be defined in a DictionaryBatch" | A batch using an id with no dictionary is rejected. |
| `batch_accepted` | anti-vacuity | A batch whose ids are all defined is accepted and leaves the reader unchanged. |
| `all_null_batch_needs_no_dictionary` | the all-null edge-case note; `checkDictionaries` | A batch that uses no key is accepted with an empty table. |

### Delta dictionaries

| Law | Spec / Java sentence | Claim |
| --- | --- | --- |
| `delta_needs_base` | "allows existing dictionaries to be expanded" | A delta for an id with no dictionary is rejected (spec's rule, see above). |
| `delta_appends` | "its vector should be concatenated with those of any previous batches with the same id" (`VectorBatchAppender.batchAppend`) | A delta on a declared, existing id is accepted in both formats and the segment count grows by one. |

### Replacement dictionaries

| Law | Spec / Java sentence | Claim |
| --- | --- | --- |
| `dictionary_defined` | first `<DICTIONARY k>` | A first non-delta batch for a declared id is accepted in both formats and yields one segment. |
| `replacement_in_stream` | "if isDelta is set to false, then the dictionary replaces the existing dictionary for the same ID" | In a stream the replacement is accepted and the id is back to one segment. |
| `no_replacement_in_file` | "The IPC File format does not support dictionary replacement" (`ArrowFileWriter`: "Replacement dictionaries are not supported") | In a file the replacement is rejected and the reader is unchanged. |
| `undeclared_dictionary_rejected` | "The dictionary types are found in the schema" (`loadDictionary`: "Dictionary ID ... not defined in schema") | A dictionary batch for an undeclared id is rejected, delta or not. |
| `declared_start`, `declared_kept` | invariant | Every dictionary in the table was declared by the schema, at the start and after every message. |

### End of stream

| Law | Spec / Java sentence | Claim |
| --- | --- | --- |
| `eos_accepted` | "The stream writer can signal end-of-stream (EOS)" | EOS after the schema is accepted and ends the stream. |
| `nothing_after_eos` | `MessageChannelReader.readNext` returns null at EOS | No message is accepted after EOS. |
| `eos_is_final` | trace form of the above | Every message of any list that follows EOS is rejected. |

### Anti-vacuity: the spec's own examples

| Law | Claim |
| --- | --- |
| `spec_delta_example` | The delta example (schema, dict 0, batch, dict 0 delta, batch, EOS) is accepted in full by both formats and leaves dictionary 0 with two segments. |
| `spec_replacement_example_stream` | The replacement example is accepted in full by a stream reader, one segment at the end. |
| `spec_replacement_example_file` | A file reader answers exactly `accept accept accept reject accept accept` on it: only the replacement is refused. |

### Writer / reader round trip

| Law | Claim |
| --- | --- |
| `writer_round_trip` | For any format, any list of distinct dictionary ids (`ArrowWriter.dictionaryIdsUsed` is a `Set`) and any number of batches, every message the writer emits is accepted. |
| `writer_round_trip_state` | Afterwards every declared dictionary is defined and the reader is in `PEnd`. |

The round trip is the one real induction. The invariant carried over
the writer's remaining ids is: every remaining id is declared, every
declared id is either remaining or already defined, the remaining ids
are distinct, and none of them is defined yet. The last two make every
dictionary a first definition, which is what the file reader needs.

### Not proven or not expressible

These stay in `LAWS.bend`, commented out, with the reason above each:

| Law | Status | Why |
| --- | --- | --- |
| `metadata_padded_to_8` | NOT EXPRESSIBLE | The model has no bytes or lengths, and Bend's Base has no `Nat.mod` theory; the divisibility library would have to be written first. |
| `body_length_fits_int64` | NOT EXPRESSIBLE | Bend has only `Nat`, `U32` and `F32`; `bodyLength: long` and the footer's `int64` offsets cannot be typed, so overflow and sign rules cannot be stated. |
| `footer_matches_stream` | NOT MODELLED | The footer is outside the model; the reader is the sequential stream reader that a file embeds. A footer model (a block list per message kind, equated with the filtered stream) is a natural follow-up. |
| footer-order application of deltas | NOT MODELLED | Random access over the footer is not modelled; the sequential reader applies deltas in stream order, which the spec's SHOULD makes the footer order. |

## 3. How to run

The installer host was blocked from this environment, so the checker
runs from a clone with bun:

```sh
git clone --depth 1 https://github.com/bendlang/bend.git /tmp/bend
BEND="bun /tmp/bend/bend2/main.ts"
cd dev/bend2/ipc_stream_ordering
$BEND PROOF.bend            # All terms check.
$BEND main.bend             # runs the six sample traces
$BEND main.bend -o out.js   # JavaScript; node out.js
$BEND main.bend -o out      # native, via clang
```

`main` prints one line per trace: the format, the verdict per message,
and `id:segments` for every declared id in the final table:

```
delta        stream | accept accept accept accept accept accept | table 0:2
delta        file   | accept accept accept accept accept accept | table 0:2
replacement  stream | accept accept accept accept accept accept | table 0:1
replacement  file   | accept accept accept reject accept accept | table 0:1
bad          stream | accept accept reject accept accept accept reject | table 0:1 1:1
writer       file   | accept accept accept accept accept accept | table 3:1 7:1
```

The `bad` trace uses dictionary 1 before defining it (third message
rejected) and sends a batch after EOS (last message rejected).

## 4. Results

| Item | Lines |
| --- | ---: |
| Model (`main.bend`) | 407 |
| Laws (`LAWS.bend`, 22 laws + 4 commented) | 323 |
| Proofs (`PROOF.bend`) | 717 |

`bend PROOF.bend` prints `All terms check.` in 0.29 s wall time. The JS
build is 26 KB and the native binary 1.1 MB; both print the six traces
above. A deliberately false law added to a copy (`MEos` accepted as the
first message) was rejected with the expected/observed verdicts, so the
gate is live.

### Mutation tests

Each bug was introduced into a copy of `main.bend` alone, `bend
PROOF.bend` was run, and the copy discarded. The committed model is
unchanged.

| # | Mutation in `main.bend` | Real-world bug | Checker result |
| --- | --- | --- | --- |
| M1 | `step_batch` accepts when `all_defined` is false | reader loads a batch whose dictionary was never sent | rejected at `Laws.batch_needs_dictionary`: expected `VAccept`, observed `VReject` |
| M2 | `step_replace` accepts in `FFile` | file reader allows dictionary replacement | rejected at `no_replace.fin`: state `(RState{FFile, PBody, declared, DEntry{id,1n} <> dicts}, VAccept)` vs `VReject` |
| M3 | `step` in `PEnd` delegates to `step_body` | reader keeps accepting after EOS | rejected at `Laws.declared_kept` (first failure reported), `nothing_after_eos` and `eos_is_final` also fail |
| M4 | delta with no base defines a dictionary | Java's lenient `loadDictionary` rule | rejected at `delta_base.fin`: expected `VAccept`, observed `VReject` |
| M5 | `bump_put` does not increment `segs` | delta batch ignored | rejected at `segs_bump.fin`: `pick_nat(.., segs, ..)` vs `pick_nat(.., 1n+segs, ..)` |
| M6 | `write` omits the schema | writer forgets the schema message | rejected at `Laws.writer_round_trip`: reader still in `PStart` with `[]` declared |
| M7 | `step_body` accepts a second schema and adopts its ids | reader lets the schema change mid-stream | rejected at `Laws.schema_once` |

M3 shows the brittleness noted in the research notes: the mutation
breaks three laws, but the first error the checker prints is at the
invariant proof, whose `PEnd` arm relied on the state being unchanged.
Either way the build is blocked.

### What it cost

Roughly 1.8 lines of proof per line of model. About a third of
`PROOF.bend` is the generic kit (splitting and joining `T(a && b)`,
`T(a || b)` introductions, `Nat.is_eq` reflexivity, soundness and
symmetry) and the `.fin` helpers that let a computed Bool be matched.
The single-step laws (sections 1 to 5) are each a few lines once the kit
exists; the round trip took the invariant above and about 150 lines.

Two Bend restrictions shaped the proofs: a computed value cannot be
matched or destructured in place (hence every `.fin` and `.go` helper
takes the verdict or the tuple as a parameter), and a `match` on a
parameter must respect binder order (the replacement arm of
`declared_kept` had to move into its own def to match on `fmt` after
`c1`). Type-returning goal defs also count usages, so their parameters
are marked `+`.

## 5. Sources

- Arrow columnar format, IPC and dictionary sections:
  https://github.com/apache/arrow/blob/main/docs/source/format/Columnar.rst
- `arrow-format/Message.fbs` (`DictionaryBatch.isDelta`),
  `arrow-format/Schema.fbs` (`DictionaryEncoding.id`),
  `arrow-format/File.fbs` (`Footer.dictionaries`, `recordBatches`)
- `vector/src/main/java/org/apache/arrow/vector/ipc/ArrowStreamReader.java`
  (`readSchema`, `loadNextBatch`, `checkDictionaries`),
  `ArrowReader.java` (`initialize`, `loadDictionary`),
  `ArrowWriter.java`, `ArrowStreamWriter.java`
  (`ensureDictionariesWritten` rewrites changed dictionaries),
  `ArrowFileWriter.java` (dictionaries written once, no replacement),
  `ArrowFileReader.java`, `message/MessageChannelReader.java`
- Bend 2 repository, `guide/GUIDE.md`, `bend2/base.bend`, demos
  `proof_insertion_sort` and `app_win_is_bug_2d`:
  https://github.com/bendlang/bend
