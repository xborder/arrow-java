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

# Bend 2 proof of concept: PollFlightInfo and endpoint expiry

A checked [Bend 2](https://github.com/bendlang/bend) model of the Arrow
Flight long-running query protocol: the `PollFlightInfo` RPC and its
`PollInfo` message, `FlightEndpoint.expiration_time`, and the
`RenewFlightEndpoint` and `CancelFlightInfo` actions. The protocol's
"must" and "should" sentences from `arrow-format/Flight.proto` and
`docs/source/format/Flight.rst` are stated as laws in `LAWS.bend` and
proven against the model in `PROOF.bend`; `bend PROOF.bend` refuses to
pass while any law is unproven or false.

This follows the layout and proof kit of the prepared-statement model
in `dev/bend2/prepared_statement/` (research branch). Written against
Bend 2.0.21, commit `6018e28` of bendlang/bend, on 2026-09-20.

| File | Lines | Content |
| --- | ---: | --- |
| `main.bend` | 540 | the model, plus a runnable sample trace in `main` |
| `LAWS.bend` | 522 | 22 laws, each with the spec sentence it comes from, and the laws that cannot be stated |
| `PROOF.bend` | 1321 | the proofs and the lemma library they need |

## 1. What is modelled

The server is `Server{qs, next, now}`: a list of queries, the next
query id to issue, and a clock. A query is `Query{id, body}` with
`QBody{st, pexp, eps}`: its state, the tick at which its poll
descriptor expires, and its endpoints in creation order. States are
`QRunning{progress}`, `QDone{}`, `QCancelled{}` and `QFailed{}`. An
endpoint is `Endpoint{tk, exp}`: a ticket index and an expiry tick.

Requests (`Act`) map onto the Flight RPCs and actions:

| Request | Flight operation | Effect in the model |
| --- | --- | --- |
| `AStart{}` | `PollFlightInfo` with an original descriptor | starts query `next`, running at progress 0, descriptor valid for `PTTL` ticks; answers `PollInfo` with descriptor `next` |
| `APoll{q}` | `PollFlightInfo` with `PollInfo.flight_descriptor` | running: answers progress, descriptor and endpoints, refreshes the descriptor expiry; expired descriptor: error and the query is cancelled; complete: complete info, descriptor unset; cancelled or failed: error |
| `ACancel{q}` | `DoAction CancelFlightInfo` | running or complete: `CANCELLED`, query becomes cancelled; failed: `NOT_CANCELLABLE`; unknown: error |
| `ADoGet{q, tk}` | `DoGet` | streams while the query is running or complete and `now < exp`; error otherwise; the state is unchanged |
| `ARenew{q, tk}` | `DoAction RenewFlightEndpoint` | a still-valid endpoint of a running or complete query gets expiry `1 + max(exp, now + ETTL)`; expired, unknown, cancelled or failed: error |
| `ATick{}` | the clock | `now + 1`; every running query gains `STEP` progress (completing when it would reach `FULL`) and one new endpoint valid for `ETTL` ticks |
| `AFail{q}` | the query's execution fails | running becomes failed |

Abstractions forced by Bend (no U64, no F64, no timestamps, F32
axiomatic): time is a `Nat` tick advanced by `ATick`; progress is a
`Nat` out of `FULL() = 100` with `STEP() = 25`; the poll descriptor of a
query is its id, as `PollFlightInfoProducer` encodes it into command
bytes; a ticket is the pair (query id, endpoint index), as
`ExpirationTimeProducer` encodes the index into the ticket.

Decisions taken where the spec leaves room, and why:

- **Cancelling a complete query answers `CANCELLED`** and its endpoints
  stop streaming. Flight.rst says of a complete `FlightInfo` that "the
  client may be able to cancel the returned FlightInfo by
  CancelFlightInfo action", and the Java integration scenario
  `ExpirationTimeCancelFlightInfoScenario` cancels the info returned by
  `GetFlightInfo`, expects `CANCELLED`, and then expects every `DoGet`
  to fail. `NOT_CANCELLABLE` is answered for a failed query, which has
  nothing left to cancel. `CANCELLING` needs asynchronous cancellation
  and is not produced; `UNSPECIFIED` is discouraged by the proto.
- **Cancelling twice answers `CANCELLED` again.** The proto allows
  "CANCELLED or a NOT_FOUND error"; the model picks the idempotent
  answer. (`ExpirationTimeProducer` answers `NOT_CANCELLABLE` here,
  which the proto text does not list.)
- **Re-sending the original descriptor starts a new query** rather than
  polling the old one, because the proto says `PollFlightInfo` with a
  descriptor "start[s] a query"; that is `AStart`. The law about using
  the wrong descriptor is therefore about descriptors the server never
  issued (`unknown_descriptor_rejected`) and expired ones
  (`expired_descriptor_rejected`).
- **An endpoint is valid while `now < exp`**, reading "until the
  expiration time is reached" as exclusive. (The Java producer uses
  `Instant.now().isAfter(expirationTime)`, which is inclusive; the
  difference is one tick and the laws would go through either way.)
- **Renewal requires a still-valid endpoint**, since the server may have
  released an expired endpoint's data. The Java producer renews
  unconditionally; that reading would drop `renew_expired_rejected`.
- **Polling an expired descriptor cancels the query**, following "the
  query may be cancelled" in the proto.

## 2. The laws

All 22 laws below are proven; `bend PROOF.bend` prints
`All terms check.` in 0.3 s. Laws over arbitrary states quantify over
every `qs`, `next` and `now`, and where they need the invariant they
assume it (`Inv`: every id is below the counter and every running
progress is at most `FULL`); `inv_start` and `inv_kept` show every
reachable state satisfies it.

| Law | Source sentence | Claim |
| --- | --- | --- |
| `inv_start`, `inv_kept` | (invariant, task item 7) | the initial state is fresh and bounded, and every request keeps it so |
| `progress_bounded` | proto `PollInfo.progress`: "If known, must be in [0.0, 1.0]" | a `PollInfo`'s progress is at most `FULL` |
| `poll_monotone` | proto `PollInfo.info`: "Subsequent PollInfo responses may only append new endpoints to info"; and, stronger than the proto ("need not be monotonic or nondecreasing"), this server's progress never decreases | poll `q`, let any one request happen, poll `q` again: the second `PollInfo` has at least the progress of the first and its endpoints as a prefix; vacuous when either poll is an error |
| `unknown_descriptor_rejected` | Flight.rst: "The client should use the descriptor (not the original FlightDescriptor)" | a descriptor at or above the counter is an error |
| `expired_descriptor_rejected` | proto `PollFlightInfo`: "A client can't use PollInfo.flight_descriptor after PollInfo.expiration_time passes ... the query may be cancelled" | polling an expired descriptor is an error and the query is cancelled |
| `done_poll_complete` | proto `PollInfo.flight_descriptor`: "If unset, the query is complete"; `PollInfo.info`: "'info' specifies all results" | polling a complete query answers `FULL`, no descriptor and every endpoint, and leaves the server unchanged |
| `fresh_query_polls` | (anti-vacuity) | the descriptor a start returns polls successfully with progress 0 and no endpoints |
| `cancel_running` | proto `CancelStatus`: "CANCELLED ... The cancellation request is complete" | cancelling a running query answers `CANCELLED` and the query is cancelled |
| `cancelled_poll_rejected` | proto `PollFlightInfo`: "A client may use the CancelFlightInfo action ... to cancel the running query" | after a cancel, polling the query is an error, from any state |
| `cancelled_doget_rejected` | `ExpirationTimeProducer`: "The client can't read data from endpoints even within 6 seconds after the action" | after a cancel, `DoGet` on any endpoint of the query is an error |
| `cancel_idempotent` | proto `CancelStatus`: "Subsequent requests with the same payload may return CANCELLED" | cancelling twice answers the same as once, from any state |
| `failed_not_cancellable` | proto `CancelStatus`: "NOT_CANCELLABLE ... The query is not cancellable" | cancelling a failed query answers `NOT_CANCELLABLE` |
| `failed_poll_rejected` | Flight.rst: "A server should return an error status instead of a response if the query fails" | after a running query fails, polling it is an error |
| `doget_before_expiry` | Flight.rst: "the client can get data multiple times by DoGet until the expiration time is reached" | before the expiry tick, `DoGet` streams (and does not change the state, so it streams again) |
| `doget_after_expiry` | same sentence | at or after the expiry tick, `DoGet` is an error |
| `renew_extends` | Flight.rst: "the client may be able to extend the expiration time by RenewFlightEndpoint"; Java scenario: "Renewed FlightEndpoint must have newer expiration time" | renewing a valid endpoint answers an endpoint whose expiry is strictly later |
| `renew_expired_rejected` | (model decision, above) | renewing an expired endpoint is an error |
| `renewed_doget_ok` | `ExpirationTimeProducer`: "The client can read data from endpoints multiple times within more 10 seconds after the action" | after a renewal, `DoGet` on the renewed endpoint streams |
| `trace_doget_fresh` | (anti-vacuity, task item 6) | start, poll, tick, poll, `DoGet` on the fresh endpoint before expiry streams |
| `trace_doget_expired` | | the same endpoint two ticks later is expired |
| `trace_completes` | | four ticks complete the query; the poll then has no descriptor and lists four endpoints |

The last three are closed traces, proven by computation (`{==}`); they
double as executable tests of the model.

### Not expressible or not proven

Kept in `LAWS.bend` as comments with the reason:

- **NOT EXPRESSIBLE** "A server should not respond until the result
  would be different from last time" and "The first PollFlightInfo call
  should return as quickly as possible" (proto). Both are about *when*
  the RPC returns. The model is a pure function from state and request
  to response, with no notion of blocking or elapsed time within a
  call, so neither sentence has a statement.
- **NOT EXPRESSIBLE** `progress` as a `double` in `[0.0, 1.0]`. Bend has
  no F64 and its F32 is axiomatic, so nothing about floats can be
  proven. `progress_bounded` is the `Nat`-out-of-100 form.
- **NOT EXPRESSIBLE** `expiration_time` as a `google.protobuf.Timestamp`
  (int64 seconds and int32 nanos) against a wall clock. Bend has no
  64-bit integers and no clock the checker can reason about. Time is a
  tick advanced by an explicit request.
- **NOT PROVEN** `poll_monotone_trace`, the form of `poll_monotone` with
  an arbitrary list of requests between the two polls. It follows from
  the one-step law by induction once two more invariants are proven:
  an issued id is never forgotten, and `QCancelled`/`QFailed` are
  absorbing, so an error between two `PollInfo`s cannot occur. Both are
  true of the model; they were not proven in this session, so the
  one-step law with an arbitrary intermediate request is what is
  checked.

## 3. How to run

The Bend 2 installer host was not reachable from the environment, so
the checker was run from a clone of the repository with bun:

```sh
git clone --depth 1 https://github.com/bendlang/bend.git /tmp/bend
BEND="bun /tmp/bend/bend2/main.ts"
cd dev/bend2/poll_flight_info
$BEND PROOF.bend          # checks LAWS.bend against main.bend: "All terms check."
$BEND main.bend           # runs the sample trace in main
$BEND main.bend -o out.js # JavaScript; node out.js
$BEND main.bend -o out    # native binary via clang; ./out
```

## 4. Results

`bend PROOF.bend` prints `All terms check.` in 0.34 s wall time. The
JavaScript target builds to a 32 KB file and the native target to a
1.1 MB binary in 6.7 s; both run the sample trace in `main` and print,
one line per request:

```
PollInfo(progress=0/100, descriptor=0, tickets=[])        AStart
PollInfo(progress=0/100, descriptor=0, tickets=[])        APoll 0
Ack                                                       ATick
PollInfo(progress=25/100, descriptor=0, tickets=[0 ])     APoll 0
Data                                                      ADoGet (0, 0)   now=1 < exp=3
FlightEndpoint(ticket=0, expires=4)                       ARenew (0, 0)   1 + max(3, 1 + 2)
Ack                                                       ATick
Ack                                                       ATick
Data                                                      ADoGet (0, 0)   now=3 < exp=4, renewed
CancelFlightInfoResult(CANCELLED)                         ACancel 0
Err                                                       ADoGet (0, 0)   cancelled
CancelFlightInfoResult(CANCELLED)                         ACancel 0       idempotent
Err                                                       APoll 0         cancelled
```

### Mutation tests

Seven bugs were introduced into `main.bend` one at a time, the check
was run, and the file restored (`git diff` clean and
`All terms check.` again after each). Every mutation was rejected.

| Mutation | Protocol bug | Law it falsifies | Where the checker stops first |
| --- | --- | --- | --- |
| `tick_st` advances with `Nat.sub(p, STEP())` | progress regresses on every tick | `poll_monotone` (`le_add`) | `tick_st_ok`, an invariant proof whose rewrite spells out `Nat.add(p, 25n)` |
| complete query answers `RPoll{0n, ...}` | a finished query reports progress 0 | `done_poll_complete` | an `Empty.absurd` annotation in `expired.fin` that spells out `RPoll{100n, ...}`; with that one annotation updated, `done.fin`: expected `RPoll{0n, ...}`, observed `RPoll{100n, ...}` |
| `doget_run` answers `RData{}` for any found endpoint | `DoGet` ignores `expiration_time` | `doget_after_expiry` | `dbe_ep.fin`: observed `doget_ep(is_lt(now, exp))`, expected `RData{}` |
| `poll_run`'s expired branch answers a `PollInfo` and touches the descriptor | server accepts a descriptor after `PollInfo.expiration_time` | `expired_descriptor_rejected` | `kept_poll_run.fin`: `upd(OpTouch)` where `upd(OpCancel)` was proven |
| `AStart` does not advance `next` | query ids reissued | `inv_kept` | `inv_kept`'s start case: `is_lt(next, next)` |
| `cancel_st(QDone{}) = QDone{}` | cancelling a complete `FlightInfo` leaves its endpoints readable | `cancelled_poll_rejected`, `cancelled_doget_rejected` | `cpr_st`'s `QDone` case: observed `RErr{}`, expected `RPoll{100n, ...}` |
| `renewed(exp, now) = exp` | `RenewFlightEndpoint` does not extend | `renew_extends` | `renewed_later`: expected `is_lt(exp, exp)` |

Two rows show the brittleness the prepared-statement notes describe:
the mutation genuinely falsifies a law, but the first failure the
checker reports is in a proof or annotation that merely mentions the
changed shape. Either way `bend PROOF.bend` fails, which is what
`LAWS.bend` promises; telling "the law is false" from "the proof needs
re-shaping" is left to the reader. The `AStart` row is the counterpart
of the same row in the prepared-statement model.

### What it cost

The proof is 2.4 lines per line of model, against 1.7 for the
prepared-statement model. The extra cost is one law, `poll_monotone`,
whose proof is about 300 lines: a case analysis over the intermediate
request, then over the update it applies to the polled query's body,
then over that body's state and the two liveness verdicts. Everything
else is 10 to 40 lines per law once the two commutation lemmas
(`find_upd`: a lookup after an update is the update of the lookup;
`find_ep_renew`: the same for endpoints) and the `Nat` lemma library
are in place. Bend's Base still ships almost no arithmetic: `n < n+1`,
`x < n ⇒ x < n+1`, `x < n ⇒ x ≤ n`, `p ≤ p + k`, `m ≥ e ⇒ e < m+1`, and
the soundness of `Nat.is_eq` are all proven here by hand.

Two Bend rules shaped every proof and are worth knowing before writing
another one. A `match` may only scrutinise a parameter or a
pattern-bound variable, so every computed `Bool` or `Maybe` goes
through a helper (`.fin`) that takes it as a parameter together with an
equation about it. And a rewrite (`%e : P`) may not precede a `match`
on a parameter, so every helper matches first and rewrites inside each
case; the equation's type is refined by the match, which is what makes
the rewrite land. Constructor names are global, so `Done{}` and
`Fail{}` collide with Base and everything is prefixed.

## 5. Sources

- `arrow-format/Flight.proto`: `PollFlightInfo`, `PollInfo`,
  `CancelFlightInfoRequest`, `CancelStatus`, `CancelFlightInfoResult`,
  `FlightEndpoint.expiration_time`, `RenewFlightEndpointRequest`.
- Arrow format docs, `docs/source/format/Flight.rst`, sections
  "Downloading Data" (endpoint expiration, renewal, cancellation) and
  "Downloading Data by Running a Heavy Query":
  https://github.com/apache/arrow/blob/main/docs/source/format/Flight.rst
- Java: `flight/flight-core/.../PollInfo.java`, `FlightEndpoint.java`,
  `CancelStatus.java`, `FlightProducer.pollFlightInfo`,
  `FlightClient.pollInfo`, `cancelFlightInfo`, `renewFlightEndpoint`;
  integration scenarios `PollFlightInfoScenario`,
  `PollFlightInfoProducer`, `ExpirationTimeProducer`,
  `ExpirationTimeCancelFlightInfoScenario`,
  `ExpirationTimeRenewFlightEndpointScenario` under
  `flight/flight-integration-tests/`.
- Bend 2: https://github.com/bendlang/bend (`guide/GUIDE.md`,
  `bend2/base.bend`, `demos/proof_insertion_sort`,
  `demos/app_win_is_bug_2d`), and the research notes in
  `dev/bend2/README.md` on the research branch.
