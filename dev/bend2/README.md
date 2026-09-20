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

# Bend 2 and Apache Arrow: research notes

Research notes on the Bend 2 programming language (released 2026-09-17)
and where it could be used around Arrow, Arrow Flight and Flight SQL.
Written against Bend 2.0.21, commit `c15a75f8` of
[bendlang/bend](https://github.com/bendlang/bend), on 2026-09-20.

The `prepared_statement/` directory next to this file holds a small,
checked Bend 2 model of the Flight SQL prepared-statement lifecycle that
was built while writing these notes. It is the concrete evidence behind
the assessment in section 4.

## 1. Summary

Bend 2 is a dependently typed, affine, pure functional language whose
type checker doubles as a proof checker. Its headline feature is
`LAWS.bend`: a file of properties the program must satisfy, which the
compiler re-checks against a `PROOF.bend` on every build and refuses
to compile while any law is unproven or false. It compiles one C file
per program that runs on CPU threads and on GPUs (CUDA, Metal), and it
also emits single-threaded JavaScript.

For Arrow the realistic fit today is **executable, machine-checked
specification** of protocol semantics: the lifecycles and invariants
that the Flight and Flight SQL specs express in prose ("the server
should return an error if the client does not use the updated
handle"). The prototype in this directory states five such laws about
prepared statements, proves them, checks in 0.2 seconds, and rejects
three injected bugs. Bend has no Java target, no stable foreign ABI,
no 64-bit integers and no float64, so it is not a candidate for
production code inside `arrow-java`, and its `LAWS.bend` mechanism
cannot constrain Java code, only Bend code.

Recommendation: treat Bend 2 as a modelling and specification tool in
the same family as TLA+ or Alloy, with the differences that its models
are proven rather than model-checked and can be compiled and run as
test oracles. Do not plan any runtime integration. Revisit if the
promised Python target or a stable FFI lands. Details in section 5.

## 2. What Bend 2 is

### 2.1 Origin and positioning

Bend 2 was released on 2026-09-17 by Higher Order Company (Victor
Taelin), under Apache-2.0. It is a new language and does not accept
Bend 1 programs or the HVM runtime. The README states the goal
plainly: "an ambiguity-free language to communicate our intents to the
AIs building the world around us", where laws make intents precise and
proofs let humans verify AI output mechanically. The tagline is
"`LAWS.bend` is `AGENTS.md` backed by proof".

The project also describes NeoGen, a program and proof synthesiser
that can fill holes in programs; it is announced material rather than a
documented, shipped command in the 2.0.21 checkout, and these notes do
not rely on it.

### 2.2 The language in one page

Syntax is Python-like with mandatory type annotations. Nothing is
inferred.

```python
import Base

type Shape is Data:
  Circle{r: U32}
  Square{s: U32}

def area(s: Shape) -> U32:
  match s:
    case Circle{+r}:
      (3 * r * r : U32)
    case Square{+s}:
      (s * s : U32)
```

The pieces that matter for specification work:

- **Quantities.** Every variable is affine by default (used at most
  once). `+x` marks a reusable value, allowed only for `Data` kinds
  (constructors, numbers, lists of Data). `-x` marks an erased value
  that exists only for the checker. Functions, arrays and IO handles
  are `Type`-kinded and can never be copied.
- **Dependent types.** A `def` may return a `Type`, so predicates are
  ordinary functions: `def Sorted(xs: List<Nat>) -> Type`. Datatypes
  can be indexed, which allows intrinsically typed ASTs (the
  `proof_typed_eval` demo).
- **Propositions as types.** `{a == b : T}` is an equality type,
  `{==}` proves it when both sides normalise to the same term, and
  `%e : P` rewrites the goal with an equation. A `match` refines the
  goal per case and a recursive call is the induction hypothesis.
  There are no tactics and no proof search.
- **Laws.** `law name:` states a claim with `for` binders and an
  optional `exs` witness; `def Laws.name(...)` in another file proves
  it. `bend PROOF.bend` fails while any law is open or false and prints
  `All terms check.` otherwise. Base itself uses laws for axioms such
  as the handle types and the F32 operations.
- **Termination is mandatory.** Recursion must structurally decrease
  on an argument, read left to right, and mutual recursion is
  forbidden. A `match` may only scrutinise a parameter or a
  pattern-bound variable, never a computed value; the idiom is a helper
  `def` that takes the computed value as a parameter. `@unsafe` opts a
  def out of the termination check and out of the proof guarantees.
- **Parallelism.** `a b = f(x) g(y)` forks two calls; `f!(x)` runs a
  call on the GPU. The programmer promises the two halves are
  independent and balanced.
- **Effects.** `IO(R)` with `do` blocks. Built-in effects are print,
  env, time, sleep, spawn, channels, files, TCP and UDP. There is no
  TLS, HTTP, JSON or regex. New effects are "foreigns": a `def` with an
  `IO` return type that imports a `.c` and a `.js` implementation.

### 2.3 Type theory and trust model

The core, BendTT, is described in `paper/BendTT.pdf` and its abstract:
one sort with `Type : Type`, no universe hierarchy, datatypes without
a positivity restriction, and consistency recovered by the affine
usage discipline plus a wall between "live" code (runs, must
terminate) and "dead" code (types, erased arguments, equations, may
diverge but never counts as live evidence). The theory is mechanised
in `bend2/bend.lean`, but the README says the Lean formalisation and
the checker "mismatch" and "early consistency bugs may occur". The
checker (`bend2/bend.ts`) is human-written; the compiler and runtimes
(`bend2/comp.ts`) are, per the README, "99% AI-written and not fully
audited yet". The changelog shows the pace: three releases on
2026-09-19 and 2026-09-20, one of them closing a way to inhabit
`Empty` through templates (#902).

Foreign effects are trusted invisibly: the checker accepts whatever
type the `.c`/`.js` side claims to return (bendlang/bend#874 tracks
this). Proofs therefore say nothing about code behind an FFI boundary.

### 2.4 Runtime and targets

BendRT emits one C file per program. A term is a 64-bit word, small
values inline and everything else a pointer into one heap shared by
every core and the GPU; there is no garbage collector, a `match` frees
the node it consumes, and only `+` values carry a reference count.
There is no C call stack: each def is a segment of a flat state
machine. Targets are C (clang only, 14+; 19+ for GPU calls), CUDA 12,
Metal and JavaScript (single core). Lua, Luau and Python are listed as
planned. No Windows, no separate compilation, and native compilation is
slow; the guide recommends the JS target for development.

### 2.5 Tooling

One binary: `bend file.bend` checks and runs, `-o out` builds a
native binary, `-o out.c` and `-o out.js` emit sources, `bend guide`
and `bend base` print the docs and the prelude, `--publish` pushes to
a hub whose packages are content hashes with no names, versions or
search. The LSP formats only. There is no debugger, profiler, REPL,
test framework or diagnostics.

### 2.6 Reception in the first days

Hacker News threads tested the `LAWS.bend` mechanic and found that it
does what it claims, and also surfaced the central weakness: an
under-specified law is satisfied by implementations that violate its
intent (the example reported was a movement scheme that preserved a
stated game law while breaking the game). Taelin agreed that writing
complete, well-scoped laws for a non-trivial system is hard. The
prototype here runs into the same issue and handles it with an
explicit anti-vacuity law (section 4.3).

### 2.7 Stated limitations that matter here

From the README's own list, the ones that shape the Arrow assessment:
numbers are `Nat`, `U32` and `F32` only (no U64, I64 or F64; F32 is
axiomatic and nothing about floats can be proven); strings are linked
lists of characters; Base is small and ships almost no arithmetic
lemmas (`Nat.ge_refl`, `Nat.max_ge_*`, `U32.add_comm` and the `Equal`
combinators); no type classes or macros; parallelism requires balanced
calls; one GPU and one event loop per program; the compiler has known
blind spots.

## 3. How to use it

### 3.1 Installing and running

The official installer is `curl -fsSL https://bend-lang.com/install.sh | sh`.
In this sandbox that host was blocked, so the checker was run from a
clone of the repository with bun, which is exactly what the installed
binary wraps:

```sh
git clone --depth 1 https://github.com/bendlang/bend.git
bun bend/bend2/main.ts path/to/PROOF.bend      # check laws and proofs
bun bend/bend2/main.ts path/to/main.bend       # check and run main
bun bend/bend2/main.ts main.bend -o out.js     # JavaScript
bun bend/bend2/main.ts main.bend -o out        # native, needs clang 14+
```

All four were exercised on the prototype in this directory; the JS
and native builds both run and print the expected result.

### 3.2 The intended workflow with coding agents

The README's recipe, verbatim in spirit: add to `AGENTS.md` that the
agent must run `bend guide` to learn the language, keep the important
rules in `LAWS.bend`, and run `bend PROOF.bend` before committing. The
human writes and owns `LAWS.bend`; the agent writes `main.bend` and
`PROOF.bend`; the checker is the gate. `bend` refuses a `PROOF.bend`
that sits beside a `LAWS.bend` without importing it.

### 3.3 What proving costs in practice

Writing the prototype gave a feel for the effort:

| Item | Lines |
| --- | ---: |
| Model (`main.bend`) | 139 |
| Laws (`LAWS.bend`, 5 laws) | 63 |
| Proofs (`PROOF.bend`) | 240 |

Roughly two lines of proof per line of model, for a model whose
invariant is a simple list bound. Most of the proof file is generic
plumbing that Base does not provide: splitting and joining `T(a && b)`,
`n < n + 1`, monotonicity of `<`, soundness of `Nat.is_eq`, and the
"inspect" idiom of passing a computed Boolean plus an equation about
it into a helper so that it can be matched. The checker ran the whole
thing in 0.2 seconds wall time, which does match the project's claim
that checking is fast.

Two frictions recur. Constructor names are global, so `Close{}` and
`Ok{}` collide with Base and everything needs a prefix. And proofs are
brittle: changing an implementation detail that does not affect a
law's truth (the exact shape of a state) still breaks proofs that
mention that shape, and the failure is reported at the proof, not as
"law is false". An agent would then have to re-prove rather than fix
code, and it needs to tell those two situations apart.

## 4. Prototype: a Flight SQL prepared-statement model

### 4.1 What is modelled

The Flight SQL spec describes prepared statements as: create a handle
with `CreatePreparedStatement`; bind parameters with `DoPut`, after
which the server "may return an updated handle" that the client must
use from then on; execute with `GetFlightInfo`; close with
`ClosePreparedStatement`. On the handle-rotation rule the spec says:
"The server is responsible for detecting the case where the client
does not use the updated handle and should return an error." The Java
client implements the client side of this in
`FlightSqlClient.PreparedStatement.execute`, which adopts the handle
returned in `DoPutPreparedStatementResult` when it is non-empty, and
`FlightSqlStatelessExample` implements a server that encodes the query
and bound parameters into the rotated handle.

`main.bend` models the server as a list of live handles plus a
counter, with four requests (`ACreate`, `ABind`, `AExec`, `AClose`)
and two responses (`ROk{h}`, `RErr`). `ABind` on a live handle retires
it and issues the counter as the new handle. `replay` runs a trace.

### 4.2 The laws

`LAWS.bend` states five laws over arbitrary server states:

1. `closed_never_executes`: after `AClose{h}`, `AExec{h}` answers `RErr`.
2. `created_handle_executes`: the handle returned by `ACreate` executes
   with `ROk`. This is the anti-vacuity law; without it a server that
   rejects every request satisfies law 1.
3. `stale_handle_rejected`: after `ABind{h}` rotates the handle,
   `AExec{h}` on the old handle answers `RErr`. This is the spec
   sentence quoted above.
4. `fresh_start` and 5. `fresh_kept`: the initial state is fresh (all
   live handles are below the counter) and every request preserves
   freshness. Law 3 is stated for fresh states, and laws 4 and 5 show
   every reachable state is fresh, so together they cover every trace.

### 4.3 Results

`bend PROOF.bend` prints `All terms check.` in 0.22 seconds. Three
mutations of `main.bend` were then tried, each restored afterwards:

| Mutation | Spec bug it corresponds to | Result |
| --- | --- | --- |
| `ABind` keeps the old handle live | Server accepts a stale handle | Rejected at `fresh_kept`'s bind case |
| `AClose` does not remove the handle | Closed statement still executes | Rejected at `closed_never_executes` |
| `ACreate` does not advance the counter | Handles reissued | Rejected at `created_handle_executes` |

The third row illustrates the brittleness noted in 3.3: the mutation
genuinely breaks `fresh_kept` (the new handle is not below an
unchanged counter), but the first failure the checker reports is in a
proof whose rewrite spelled out `1n+next`. Both readings lead to "the
build is blocked", which is the property `LAWS.bend` promises; the
diagnosis of why is left to the human or agent.

The model compiles to a 16 KB JavaScript file and a 1.1 MB native
binary, both of which run the sample trace (create, bind, execute the
stale handle) and print `Err`.

## 5. Where Bend 2 could fit around Arrow

### 5.1 Executable protocol specifications (good fit)

Flight and Flight SQL carry a lot of lifecycle semantics in prose:
prepared-statement handle rotation, transactions and savepoints
(`ActionBeginTransaction`, `ActionEndSavepoint` with release or
rollback, statements that may or may not carry a `transaction_id`),
`PollFlightInfo` (reuse the returned descriptor, `progress` in
`[0, 1]`, expiration, cancel via `CancelFlightInfo`), endpoint
expiration and `RenewFlightEndpoint`, session options
(`SetSessionOptions` "may require these options be set exactly once
and prior to any other activity"), and the `ordered` flag on
`FlightInfo`. Each of these is a small state machine with a handful of
"must" and "should" sentences, which is exactly what the prototype
shows Bend can state as laws and prove in minutes of checker time.

Value for Arrow: a machine-checked reference of what a conforming
server does, next to the `.proto` and `.rst` files. It would settle
questions such as the one in apache/arrow#37720 (stateless prepared
statements with parameters) by making the intended state transitions
explicit and checked. This is comparable to writing a TLA+ spec, with
two practical differences: the proof is total rather than bounded by a
model checker, and the model compiles to JS or C so it can be run.

Cost: the proofs are hand-written, and the first model of each area
pays for its own lemma library. Expect the proof to be two to three
times the size of the model, as in section 3.3.

### 5.2 Test oracles for the Java implementation (good fit, some work)

Because the model compiles to JavaScript, and `bend2/main.ts` doubles
as a bun and node loader (`import Model from "./main.bend"` exposes
every non-IO def with constructors as `{$: "Name", ...}` and `Nat` as
`BigInt`), a Bend model can drive or check traces. A differential
test would generate request traces, run them through the model to get
the expected responses, and replay them against `FlightSqlClient` and
a producer such as `FlightSqlStatelessExample`, comparing the
`Ok`/error outcome per step. `TestFlightSqlStateless` is the natural
home. This needs a small bridge (a JSON trace format, a node step in
the test, or committing generated expectations), and the oracle is
only as good as the model, but it turns the prose rules into a
regression test.

### 5.3 Columnar and IPC structural invariants (feasible, expensive)

The columnar spec has invariants that are natural laws: offsets have
`length + 1` entries and are monotonically non-decreasing; run-end
arrays have strictly increasing positive run ends; `null_count`
matches the validity bitmap; every buffer is padded to 8 bytes; an
encapsulated IPC message is `0xFFFFFFFF`, an int32 metadata length,
the flatbuffer, padding to 8 bytes and a body whose total is a multiple
of 8 (`MessageSerializer` enforces this with `checkArgument` calls);
streams put the schema first and define a dictionary before a batch
uses it.

These can be modelled, but the arithmetic ones are costly today: Base
ships no lemmas about `Nat.mod`, `Nat.div` or multiplication, so a
proof that a padded length is a multiple of 8 means building that
theory first, and there is no 64-bit integer type, so `int64` lengths
and offsets become `Nat`. A round-trip law for the message prefix
(`decode(encode(m)) == m`) needs little-endian byte splitting and the
same divmod lemmas. The list-shaped invariants (offsets monotonic,
dictionaries defined before use, schema first) are cheap and look like
the prototype. Value is moderate: these invariants are already checked
at runtime by every implementation, and the spec prose is precise.

### 5.4 Verified reference implementations (not now)

Compiling a verified Bend routine and calling it from `arrow-java` is
not practical. There is no Java or JVM target. The C output is one
whole program with its own heap and flat state machine, not a library
with a stable ABI: the effects guide says outright there is "no ABI
promise" and effects must be rebuilt with every compiler release.
Values live as 64-bit words in Bend's heap, so Arrow buffers would be
copied and re-encoded on the way in and out, and strings are linked
lists. A Panama or JNI bridge would be fighting the runtime.

### 5.5 GPU kernels over Arrow data (not a fit)

Bend's parallelism is real but shaped for divide-and-conquer over its
own data structures, with arrays limited to power-of-two sizes and
balanced forks. Arrow's columnar buffers would have to be marshalled
into that heap, and `arrow-java` has no GPU story to plug into. Where
GPU work on Arrow data is wanted, existing Arrow-native engines are the
comparison, and Bend does not offer zero-copy against the C Data
Interface.

### 5.6 Governing AI-written Java (not applicable)

`LAWS.bend` constrains Bend code only. It cannot state or enforce
anything about `FlightSqlProducer` implementations written in Java.
The useful transfer is indirect: a Bend model pins down what the Java
code should do, and section 5.2 turns that into tests.

## 6. Risks and open questions

- **Maturity.** Three days old at time of writing, releases daily,
  a consistency fix in the newest one, the compiler unaudited, the
  Lean model behind the checker. Fine for specification work whose
  output is understanding and tests; not fine for anything that ships.
- **Trusted computing base.** Proofs are checked by `bend.ts`, which
  is not the formalised kernel. Anything behind a foreign effect is
  trusted on its declared type.
- **Under-specified laws.** A law can be true of the wrong program.
  Every law set needs positive (liveness-style) laws next to the
  negative ones, as the prototype's `created_handle_executes` shows.
- **No 64-bit integers or float64.** Arrow is full of both. Models
  must abstract them to `Nat`, which is fine for semantics and wrong
  for overflow behaviour.
- **Proof brittleness without tactics.** Refactoring a model breaks
  proofs even when laws still hold. Budget for re-proving.
- **Network egress.** The docs site (`bend-lang.com`, `bend2.dev`) was
  unreachable from this environment; the GitHub repository carried
  everything needed, including the guide (`bend guide` prints it).

## 7. Suggested next steps

1. Extend the prototype with transactions and savepoints
   (`ActionBeginTransaction`, `ActionBeginSavepoint`,
   `ActionEndTransaction`, `ActionEndSavepoint`) and with statements
   that carry a `transaction_id`, and state the legality of each
   action per state as laws.
2. Model `PollFlightInfo` (descriptor reuse, monotone progress,
   expiration, cancel) the same way.
3. Build the differential harness in 5.2 against
   `FlightSqlStatelessExample` and `FlightSqlExample`, starting with
   the prepared-statement traces the model already generates.
4. Revisit runtime integration only if a Python or JVM target or a
   stable FFI appears in the Bend changelog.

## 8. Sources

Primary, read directly:

- Bend repository, README, `guide/GUIDE.md`, `guide/EFFECTS.md`,
  `WONTFIX.txt`, `AGENTS.md`, `CHANGELOG.md`, `bend2/base.bend`,
  paper abstracts in `bend2/docs/BendTT` and `bend2/docs/BendRT`,
  demos `proof_insertion_sort`, `proof_typed_eval`,
  `app_win_is_bug_2d`, `io_tcp_echos`, `io_http_server`:
  https://github.com/bendlang/bend (commit c15a75f8, 2026-09-20)
- Foreign effects trusted on declared type: https://github.com/bendlang/bend/issues/874
- Apache Arrow format docs, Flight, Flight SQL and Columnar:
  https://github.com/apache/arrow/tree/main/docs/source/format
- Stateless prepared statements with parameters: https://github.com/apache/arrow/issues/37720
- This repository: `arrow-format/Flight.proto`, `arrow-format/FlightSql.proto`,
  `flight/flight-sql/.../FlightSqlClient.java`,
  `flight/flight-sql/.../FlightSqlProducer.java`,
  `flight/flight-sql/src/test/.../FlightSqlStatelessExample.java`,
  `vector/.../ipc/message/MessageSerializer.java`

Secondary, via search results (the sites themselves were not reachable
from this environment, so figures quoted from them are vendor claims):

- Bend 2 launch coverage and Hacker News discussion of under-specified
  laws: https://news.ycombinator.com/item?id=49746163 and
  https://news.ycombinator.com/item?id=49753179
- bend2.dev notes ("What is Bend2?", "Bend2 vs Lean", reporting a
  checker benchmark of 0.295 s for Bend against 36.177 s for Lean on a
  12,800-definition fixture): https://bend2.dev/notes/what-is-bend2/
  and https://bend2.dev/notes/bend2-vs-lean/
- Taelin's launch and NeoGen posts: https://x.com/VictorTaelin/status/2100681226143092875
  and https://x.com/VictorTaelin/status/1957775213053022614
- Higher Order Company fundraising page (older material; lists
  Python, JavaScript and Go export, which the release does not):
  https://wefunder.com/higherorderco/
- Vow language issue proposing an audit of Bend 2's verification model:
  https://github.com/vow-lang/vow/issues/1298
