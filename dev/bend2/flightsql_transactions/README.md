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

# Flight SQL transactions and savepoints, as Bend 2 laws

A checked model of the Arrow Flight SQL transaction and savepoint
lifecycle, written in [Bend 2](https://github.com/bendlang/bend). The
model (`main.bend`) is an executable state machine for the server side
of `BeginTransaction`, `BeginSavepoint`, `EndTransaction`,
`EndSavepoint` and statements that carry an optional `transaction_id`.
`LAWS.bend` states the sentences of `arrow-format/FlightSql.proto` that
describe these actions as laws over that machine, and `PROOF.bend`
proves them. `bend PROOF.bend` refuses to pass while any law is
unproven or false.

This is a proof of concept for using Bend as a specification tool
around Arrow. It follows the prepared-statement prototype and the
research notes in `dev/bend2/README.md` on the
`claude/bend2-arrow-integration-pgexut` branch of this repository, and
reuses that prototype's proof helpers (the `T` predicate, `split.fin`
and `join.fin`, the Nat lemmas, the "inspect" idiom for matching a
computed Boolean). Checked against Bend 2.0.21, commit `6018e28e` of
bendlang/bend, on 2026-09-20.

## 1. What is modelled

Sources read: the messages `ActionBeginTransactionRequest`,
`ActionBeginSavepointRequest`, `ActionBeginTransactionResult`,
`ActionBeginSavepointResult`, `ActionEndTransactionRequest` (enum
`EndTransaction`: COMMIT, ROLLBACK), `ActionEndSavepointRequest` (enum
`EndSavepoint`: RELEASE, ROLLBACK) and the `optional bytes
transaction_id` of `CommandStatementQuery`, `CommandStatementUpdate` and
`ActionCreatePreparedStatementRequest` in `arrow-format/FlightSql.proto`;
`FlightSqlProducer.beginTransaction`, `beginSavepoint`, `endTransaction`,
`endSavepoint` and `FlightSqlClient.Transaction`, `Savepoint`,
`beginTransaction`, `beginSavepoint`, `commit`, `rollback`, `release`
in `flight/flight-sql`; the example servers under
`flight/flight-sql/src/test/.../example/` (both advertise
`SQL_SUPPORTED_TRANSACTION_NONE` and implement none of the four
actions, so they contributed nothing beyond the interface). The format
document `docs/source/format/FlightSql.rst` in apache/arrow does not
mention transactions; the proto comments are the whole written spec.

### 1.1 The state machine

```
type Sp is Data:
  Sp{id: Nat, tx: Nat}                 # a live savepoint and its transaction

type Server is Data:
  Server{txs: List<&2, Nat>,           # live transaction ids, newest first
         sps: List<&2, Sp>,            # live savepoints, newest first
         next: Nat}                    # the next id to issue
```

One counter issues both transaction and savepoint ids, so a savepoint
id can never be mistaken for a transaction id. The "ordered list of
savepoints of a transaction" is the sub-list of `sps` tagged with that
transaction; since ids come from a counter, "created after savepoint
s" is "has an id greater than s".

Requests and responses:

| Request | Proto message | Effect when it succeeds | Error when |
| --- | --- | --- | --- |
| `ABeginTx{}` | `ActionBeginTransactionRequest` | adds `next` to `txs`, answers `RId{next}` | never |
| `ABeginSp{tx}` | `ActionBeginSavepointRequest` | adds `Sp{next, tx}` to `sps`, answers `RId{next}` | `tx` not live |
| `AEndTx{tx, how}` | `ActionEndTransactionRequest` | removes `tx` and every `Sp{_, tx}`; `how` is COMMIT or ROLLBACK | `tx` not live |
| `AEndSp{sp, SRelease{}}` | `ActionEndSavepointRequest` RELEASE | removes `Sp{sp, _}` | `sp` not live |
| `AEndSp{sp, SRollback{}}` | `ActionEndSavepointRequest` ROLLBACK | removes the `Sp{id, t}` of the same transaction `t` with `id > sp`; keeps `sp` | `sp` not live |
| `AStmt{STx{tx}}` | statement with `transaction_id` | no state change, answers `RDone{}` | `tx` not live |
| `AStmt{SAuto{}}` | statement without `transaction_id` | no state change, answers `RDone{}` (auto-commit) | never |

`step` is one request against one state, `replay` runs a trace from
the empty server. `main` prints the responses of a ten-step trace:
begin transaction 0, savepoints 1 and 2 in it, roll back to 1 (retires
2), release 2 (error), a statement in 0, commit 0, a statement in 0
(error), release 1 (error), an auto-commit statement:

```
[Id(0), Id(1), Id(2), Done, Err, Done, Done, Err, Err, Done]
```

### 1.2 Readings chosen where the proto is silent

- **Release keeps the other savepoints.** The proto says "Releasing a
  savepoint invalidates that savepoint" and nothing else. The model
  removes exactly that savepoint. JDBC's `Connection.releaseSavepoint`
  and PostgreSQL's `RELEASE SAVEPOINT` also drop the savepoints created
  after it; a server with that behaviour satisfies
  `release_kills_savepoint` but violates
  `release_keeps_other_savepoints`. The law is there so that the choice
  is explicit and a reviewer can flip it.
- **Commit and rollback are the same lifecycle transition.** Both
  retire the transaction and its savepoints ("If the action completes
  successfully, the transaction handle is invalidated, as are all
  associated savepoints"). What happens to the data is outside the
  model, and `commit_rollback_same_state` records that the model does
  not distinguish them.
- **Ending a transaction that is not live is an error**, and so is any
  `EndSavepoint` on an id that is not live. The proto does not say what
  a server answers to an unknown handle; every Flight SQL producer in
  practice returns an error status, and the Java client turns a missing
  result into an exception.
- **Ids are `Nat`s from one counter**, standing in for the proto's
  opaque `bytes` handles. The only property of a handle the lifecycle
  depends on is that it is never reissued, which is the invariant.

## 2. The laws

Every law is stated for arbitrary `txs`, `sps` and `next`. The two
laws that need unique ids take the invariant `Inv(txs, sps, next)` as
a hypothesis; `inv_start` and `inv_kept` show every reachable state
satisfies it. Bend's rewrite direction is the reason every equation is
written with the constant on the left (`{False{} == has(t, txs)}`).

| Law | Spec sentence (FlightSql.proto) | Statement |
| --- | --- | --- |
| `ended_tx_rejected` | "If the action completes successfully, the transaction handle is invalidated" | After `AEndTx{t, how}`, any request that names `t` as its transaction_id (`ABeginSp`, `AEndTx`, `AStmt{STx{t}}`) is `RErr`. Unconditional: if `t` was not live the `EndTransaction` failed and `t` is still not live. |
| `ended_tx_kills_savepoints` | "...as are all associated savepoints"; "If the associated transaction is committed, rolled back, or times out, then the savepoint is also invalidated" | After `AEndTx{t, how}` on a live `t`, `AEndSp{s, how2}` on any `s` owned by `t` is `RErr`. Needs `Inv`. |
| `commit_rollback_same_state` | "Commit (COMMIT) or rollback (ROLLBACK) the transaction" | `AEndTx{t, TCommit{}}` and `AEndTx{t, TRollback{}}` produce the same state. |
| `savepoint_needs_live_tx` | "Creates a savepoint within a transaction"; "The transaction to which a savepoint belongs" | `ABeginSp{t}` with `t` not live is `RErr`. |
| `savepoint_in_live_tx` | anti-vacuity | `ABeginSp{t}` with `t` live answers `RId{next}`. |
| `savepoint_bound_to_tx` | "The transaction to which a savepoint belongs" | after `ABeginSp{t}` on live `t`, the server records `next` as owned by `t`. |
| `end_savepoint_keeps_txs` | "Roll back to a savepoint" (as opposed to the transaction) | any `AEndSp` leaves `txs` unchanged, so the transaction stays live. |
| `release_kills_savepoint` | "Releasing a savepoint invalidates that savepoint" | after `AEndSp{s, SRelease{}}`, any `AEndSp{s, how}` is `RErr`. Unconditional. |
| `release_keeps_other_savepoints` | chosen reading, see 1.2 | after `AEndSp{s, SRelease{}}`, every other live savepoint keeps its owner. |
| `rollback_keeps_savepoint` | "Rolling back to a savepoint does not invalidate the savepoint" | after `AEndSp{s, SRollback{}}` on live `s`, another `AEndSp{s, how}` answers `RDone{}`. |
| `rollback_kills_later_savepoints` | "...but invalidates all savepoints created after the current savepoint" | after `AEndSp{s, SRollback{}}` on live `s` of `t`, `AEndSp{s2, how}` is `RErr` for every live `s2` of `t` with `s < s2`. Needs `Inv`. |
| `rollback_keeps_earlier_savepoints` | complement of the sentence above | the savepoints of `t` with `s2 < s` keep their owner. |
| `statement_needs_live_tx` | "Include the query as part of this transaction" | `AStmt{STx{t}}` with `t` not live is `RErr`. |
| `statement_autocommit` | "(if unset, the query is auto-committed)" | `AStmt{SAuto{}}` answers `RDone{}` from any state. |
| `statement_in_live_tx` | anti-vacuity | `AStmt{STx{t}}` with `t` live answers `RDone{}`. |
| `fresh_tx_runs_statement` | anti-vacuity | the id `ABeginTx{}` returns runs a statement. |
| `fresh_tx_takes_savepoint` | anti-vacuity | the id `ABeginTx{}` returns takes a savepoint, whose id is `1n+next`. |
| `inv_start` | freshness | the empty server satisfies `Inv`. |
| `inv_kept` | freshness | every request preserves `Inv`. |

The invariant is stronger than the prepared-statement prototype's "all
handles below the counter": `desc(xs, n)` says the list is strictly
descending with head below `n`. It is what the server naturally
maintains (new ids go to the front), it implies freshness, and it gives
uniqueness of ids for free, which `ended_tx_kills_savepoints` and
`rollback_kills_later_savepoints` need. Without uniqueness a second
transaction could hold a savepoint with the same id as the one just
retired, and the later `EndSavepoint` would find it.

### 2.1 Laws kept but not proven

Kept in `LAWS.bend` as comments with the reason above each:

- `NOT EXPRESSIBLE` timeout: "If the transaction times out, then it is
  automatically rolled back." The model has no clock; a timeout is an
  event the server raises between requests, not a request. It could be
  added as an explicit `ATimeout{tx}` request equal to
  `AEndTx{tx, TRollback{}}`, but that only restates
  `commit_rollback_same_state`, not the timing.
- `NOT EXPRESSIBLE` opaque `bytes` handles: nothing about handle
  encoding is stated; Bend has no byte or 64-bit integer type and the
  lifecycle depends only on ids not being reissued, which `inv_kept`
  covers.
- `NOT EXPRESSIBLE` feature negotiation: "Only supported if
  FLIGHT_SQL_TRANSACTION is FLIGHT_SQL_TRANSACTION_SUPPORT_SAVEPOINT" is
  a `GetSqlInfo` property with its own RPC; the model assumes a server
  that supports savepoints.

Every law that was attempted was proven, so there is no `NOT PROVEN`
entry.

## 3. How to run

The installer host `bend-lang.com` was blocked in the environment this
was written in, so the checker ran from a clone with bun, which is
what the installed binary wraps:

```sh
git clone --depth 1 https://github.com/bendlang/bend.git /tmp/bend
BEND="bun /tmp/bend/bend2/main.ts"
cd dev/bend2/flightsql_transactions
$BEND PROOF.bend            # All terms check.
$BEND main.bend             # runs the trace in main
$BEND main.bend -o out.js   # JavaScript target; node out.js
$BEND main.bend -o out      # native binary via clang; ./out
```

## 4. Results

| Item | Value |
| --- | ---: |
| `main.bend` | 295 lines |
| `LAWS.bend` (19 proven laws, 3 not-expressible ones as comments) | 340 lines |
| `PROOF.bend` | 649 lines |
| `bend PROOF.bend` wall time | 0.32 s |
| JavaScript output | 21 KB, runs under node |
| Native output | 1.1 MB, builds in 4 s with clang 18, runs |

Roughly two lines of proof per line of model, as in the
prepared-statement prototype. About a third of `PROOF.bend` is the
generic kit (`T` splitting and joining, `Nat` order lemmas:
`lt_trans`, `lt_irrefl`, `lt_asym`, `lt_ne`, soundness of `is_eq`,
injectivity of `Some`). The model-specific part is four lemma
families: `owner_cut_keep` (a savepoint the cut does not select keeps
its owner), `owner_cut_hit` (a selected savepoint is gone, given
unique ids), `below_none_cut` (no savepoint below a bound has that
bound as id) and the `desc` preservation lemmas for `remove` and `cut`.

### 4.1 Mutation tests

Each mutation was applied to a copy of `main.bend`, `bend PROOF.bend`
was run, and the copy discarded. The committed files are unmodified.

| # | Mutation of `main.bend` | Spec bug it corresponds to | Law that is false | Where the checker first fails |
| --- | --- | --- | --- | --- |
| 1 | `end_tx` keeps `sps` instead of `cut(OfTx{t}, sps)` | commit does not retire the transaction's savepoints | `ended_tx_kills_savepoints` | `ended.fin`: the rewritten state no longer has the `cut` |
| 2 | `AfterIn` selects `is_le(s, id)` instead of `is_lt(s, id)` | rollback to a savepoint drops the savepoint itself | `rollback_keeps_savepoint` | `Laws.rollback_keeps_savepoint`: `miss_self` proves `is_lt(s, s)` false, the goal now has `is_le(s, s)` |
| 3 | `stmt` answers `RDone{}` when the transaction is not live | a statement on an ended transaction succeeds | `statement_needs_live_tx`, `ended_tx_rejected` | `Laws.statement_needs_live_tx`: expected `RDone{}`, observed `RErr{}` |
| 4 | `ABeginTx` does not advance `next` | transaction ids reissued | `fresh_tx_takes_savepoint`, `inv_kept` | `Laws.fresh_tx_runs_statement`: the proof spelled out `1n+next` |
| 5 | `IsId` selects `is_le(s, id)` (the JDBC reading of release) | release also drops later savepoints | `release_keeps_other_savepoints` | `owner_cut_id`: the verdict passed to `.fin` no longer matches |
| 6 | `AfterIn` selects nothing (`False{}`) | rollback keeps the later savepoints | `rollback_kills_later_savepoints` | `Laws.rollback_keeps_savepoint`: `miss_self` has the wrong shape |
| 7 | `end_sp` also removes the owning transaction | ending a savepoint ends the transaction | `end_savepoint_keeps_txs` | `keeps_txs.fin`: expected `txs`, observed `remove(t, txs)` |

All seven are rejected. The last column shows the brittleness noted in
the research notes: the checker reports the first proof that no longer
type-checks, which is often a lemma whose statement spelled out the
old shape of the model, not the law that became false. In rows 1, 2,
5 and 6 the failing location is a proof step, and the law that is
genuinely violated is in the column before. Both readings block the
build, which is what `LAWS.bend` promises; telling "the law is false"
from "the proof needs updating" is left to the reader.

### 4.2 An observation on the Java client

While reading `FlightSqlClient` for the semantics: `rollback(Savepoint)`
(documented "Rollback to a savepoint") sets `END_SAVEPOINT_RELEASE` as
its action, the same value as `release(Savepoint)`. Under this model a
client calling `rollback(savepoint)` would in fact perform the
`SRelease{}` transition, so the savepoint would be gone afterwards
(`release_kills_savepoint`) instead of staying usable
(`rollback_keeps_savepoint`). This is exactly the kind of mismatch a
model of the lifecycle makes visible; no Java code was changed in this
proof of concept.

## 5. What this shows about Bend for Arrow

The lifecycle sentences of the proto, including the ones about
interaction between actions ("as are all associated savepoints",
"invalidates all savepoints created after"), fit in a few hundred
lines of laws and proofs and check in a third of a second. The parts
that resisted were not the semantics but the language mechanics: the
rewrite direction forces a convention on how equations are written,
computed values need a helper to be matched, and a change to the model
breaks proofs that mention its shape. The parts that Bend cannot say
are the ones outside a pure request/response machine: time (timeouts),
bytes (handles), and negotiation state that lives in another RPC.
