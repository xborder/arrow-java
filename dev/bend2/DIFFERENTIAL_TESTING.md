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

# Validating Java against a proven Bend 2 model: differential testing

This document explains how the Bend 2 model of the Flight SQL
prepared-statement lifecycle in `prepared_statement/` is used to
validate the Java servers in `flight-sql`, what that validation does
and does not establish, and how to extend it.

## 1. The problem it solves

A Bend 2 model with proven laws says nothing about Java code by itself.
The laws are theorems about the Bend program `main.bend`; the checker
never sees `FlightSqlExample` or `FlightSqlClient`. To validate the
Java implementation, the model has to be connected to it. The
connection used here is differential testing: the model is the oracle
that says what a conforming server answers to every request in a
trace, and a JUnit test replays the same traces against a real server
and compares.

The chain of trust is:

1. **Spec sentences become laws.** `LAWS.bend` states, for every
   server state and every handle, what the spec's "must" and "should"
   sentences require (a closed handle never executes, a stale handle is
   rejected, a created handle does execute, and so on).
2. **The model is proven to obey the laws.** `bend PROOF.bend` checks
   `PROOF.bend` against `LAWS.bend` and the model. If the model
   violated a law, there would be no proof and the check would fail.
   This is what makes the model trustworthy as an oracle: its answers
   are not hand-written expectations, they are the output of a program
   that is proven to respect the spec.
3. **The model generates expectations.** `traces.bend` enumerates
   request sequences, runs each through the model, and writes the
   response to every request into a text file.
4. **A JUnit test replays the traces against Java.** For each trace and
   each request, the test issues the real RPC and compares the
   success-or-error outcome with the model's.
5. **Deviations are classified, not hidden.** Every mismatch is
   bucketed by request kind and expected versus observed outcome. Each
   server has a documented set of known deviations, and the test fails
   if a category appears or disappears.

## 2. The pieces

| File | Role |
| --- | --- |
| `dev/bend2/prepared_statement/main.bend` | The model: server state, four requests, transitions, trace replay. |
| `dev/bend2/prepared_statement/LAWS.bend` | Six laws, each tied to a spec sentence or a sanity requirement. |
| `dev/bend2/prepared_statement/PROOF.bend` | Proofs of the six laws. `bend PROOF.bend` is the gate. |
| `dev/bend2/prepared_statement/traces.bend` | The trace generator. Prints the trace file. |
| `flight/flight-sql/src/test/resources/bend2/prepared_statement_traces.txt` | Generated expectations, checked in. |
| `flight/flight-sql/src/test/java/.../test/TestFlightSqlBendConformance.java` | The replayer and the known-deviation lists. |

### 2.1 The model and its two policies

The Flight SQL spec says a server "may return an updated handle" when
parameters are bound with `DoPut`, and that if it does, the client must
use the new handle and the server "should return an error" if the old
one is used. Both example servers in this repository are valid under
that sentence but behave differently: `FlightSqlExample` keeps the
handle, `FlightSqlStatelessExample` returns a new one that encodes the
bound parameters.

The model therefore takes a policy flag, `rot`. Under `rot = True` a
bind retires the old handle and issues a fresh one; under `rot = False`
the handle is kept. The laws are stated for both policies where they
apply to both (close, create, freshness), for the rotating policy
where the spec sentence is about rotation (`stale_handle_rejected`),
and a sixth law, `kept_handle_executes`, pins down the keeping policy:
after a bind, the same handle still executes.

### 2.2 The trace file

`traces.bend` builds every sequence of one to three requests over the
alphabet `C` (create), `B0`/`B1` (bind handle 0 or 1), `E0`/`E1`
(execute), `X0`/`X1` (close), then every length-four sequence starting
with a create and every length-five sequence starting with create and
bind. That is 1085 traces and 4221 request steps. Each line holds the
actions and the expected responses under both policies:

```
C B0 E0 | ok0 ok1 err | ok0 ok0 ok0
```

Read: create returns handle 0; bind handle 0 returns handle 1 under
rotation or 0 under keeping; execute handle 0 is then an error under
rotation (stale) and a success under keeping. The `okN` form carries
the model's handle id, which the Java side needs (next section).

Handle ids are small naturals in the model and opaque bytes on the
wire, so the alphabet uses ids 0 and 1 and lets the traces reach ids up
to 4 through creates and rotations. Requests on ids the trace never
issued exercise the "unknown handle" paths.

### 2.3 The replayer

`TestFlightSqlBendConformance` starts a `FlightServer` with one of the
two example producers over Derby, opens a raw `FlightClient`, and for
each trace:

- keeps a map from model handle id to the `ByteString` the server
  actually issued;
- for `C`, calls the `CreatePreparedStatement` action with a query that
  differs per create (`... AND 1 = 1`, `... AND 2 = 2`), because both
  example servers use the query text as the handle and two creates of
  the same text would alias;
- for `B`, runs a `DoPut` with a one-row parameter batch and reads the
  `DoPutPreparedStatementResult` metadata for an updated handle;
- for `E`, calls `GetFlightInfo` with `CommandPreparedStatementQuery`;
- for `X`, calls the `ClosePreparedStatement` action;
- maps unknown ids to bytes no server has seen
  (`bend2-unknown-handle-N`);
- after every successful request whose expectation is `okN`, records
  the returned bytes as the real handle for id `N`, so the model's
  handle bookkeeping and the server's stay aligned even when the server
  rotates;
- compares only the kind of outcome, success or error, per request,
  and closes every handle it created before the next trace.

Expectations are read from the column matching the server's policy:
the stateful example is compared against the keep column, the
stateless one against the rotate column.

### 2.4 Known deviations

A conformance test that fails on day one is not mergeable, and a test
that silently ignores mismatches is worthless. The middle ground used
here: deviations are bucketed by category, each server carries an
explicit, commented set of known categories, and the assertion is
equality of category sets. Fixing the server makes a category
disappear and the test fail, which is the prompt to delete the entry.
A regression adds a category and fails the test.

## 3. Results

Run with:

```sh
mvn -pl flight/flight-sql test -Dtest=TestFlightSqlBendConformance
```

| Server | Policy column | Steps compared | Deviation categories |
| --- | --- | ---: | --- |
| `FlightSqlExample` (stateful) | keep | 4221 | none |
| `FlightSqlStatelessExample` | rotate | 4221 | three, below |

The stateless example deviates from the model in three ways, all
consequences of the same design: the handle is the query text, or a
serialised (query, parameters) pair, and the server validates nothing
against its own state.

| Category | Count | Example | Spec sentence at stake |
| --- | ---: | --- | --- |
| `E expected err observed ok` | 229 | `C B0 E0`, step 3 | Executing the pre-bind handle succeeds; the spec says the server "should return an error". Executing a closed handle also succeeds. |
| `B expected err observed ok` | 189 | `C B0 B0`, step 3 | Binding a stale or closed handle succeeds. |
| `B expected ok observed err` | 126 | `C B0 B1`, step 3 | Binding the rotated handle fails, because the server parses the rotated bytes as SQL. The spec allows chaining a `DoPut` handle into another `DoPut`. |

The third row is a real limitation of the example rather than a
laxness: a client that binds parameters twice against the stateless
example gets a lexical error. The first two are the example choosing
statelessness over validation, which the spec's "should" permits but
the model, taking the sentence at face value, does not.

The stateful example's server threads log `AssertionError` during the
run. That is its `assert statementContext != null` firing on requests
with unknown handles, which is exactly the error path the model
expects; the assertion is converted into an error status for the
client.

## 4. What this validates and what it does not

It validates that, for every enumerated trace, each Java server answers
each request with the same success-or-error verdict as a model that is
proven to satisfy the spec's lifecycle laws, except for the documented
deviations.

It does not validate:

- **Traces beyond the enumeration.** The model's proofs cover all
  states; the replay covers 1085 traces. Longer or differently shaped
  sequences are untested unless the generator is extended.
- **Anything but the verdict.** Result schemas, row contents, error
  codes and messages are not compared. The model has no notion of them.
- **Servers other than the two examples.** Running the same replayer
  against another Flight SQL server is a matter of pointing the test at
  it; it is not done here.
- **The client wrapper.** `FlightSqlClient.PreparedStatement` is
  bypassed on purpose so that server behaviour is observed directly;
  its own client-side checks (for example refusing to execute a closed
  statement locally) are not under test.
- **The model's fidelity.** If the model misreads the spec, the oracle
  is wrong and the proofs faithfully guarantee the wrong thing. The
  laws in `LAWS.bend` quote the sentence each one encodes so that a
  reviewer can check the reading.

## 5. Extending it

- **New laws or a changed model.** Edit `main.bend` and `LAWS.bend`,
  make `bend PROOF.bend` pass again, regenerate the trace file with
  `bend traces.bend > flight/flight-sql/src/test/resources/bend2/prepared_statement_traces.txt`,
  and rerun the test. If the model's behaviour changed, the deviation
  sets may change with it.
- **Wider traces.** Change `alphabet` or `all_traces` in `traces.bend`.
  Replay time is roughly linear in steps; 4221 steps against Derby take
  about eight seconds per server here.
- **Another server.** Add a test method that constructs the producer
  and calls `replayAll` with the right policy column and its own known
  deviations.
- **Other protocols.** The same shape works for any lifecycle the model
  can express: a model with a `responses` function, a generator that
  prints one line per trace, and a replayer that maps model ids to wire
  values. The transactions and savepoints, IPC ordering, buffer
  reference counting and PollFlightInfo proofs of concept are the
  natural next candidates.

## 6. Running Bend here

The installer host may be unreachable; the checker runs from a clone
of the Bend repository with bun:

```sh
git clone --depth 1 https://github.com/bendlang/bend.git /tmp/bend
cd dev/bend2/prepared_statement
bun /tmp/bend/bend2/main.ts PROOF.bend      # All terms check.
bun /tmp/bend/bend2/main.ts main.bend       # prints Err, then Ok(0)
bun /tmp/bend/bend2/main.ts traces.bend > ../../../flight/flight-sql/src/test/resources/bend2/prepared_statement_traces.txt
```
