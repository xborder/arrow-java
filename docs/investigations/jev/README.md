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

# What could Jev (TypeSafe's structured decision model) make possible in Arrow Java?

Status: exploration only. Nothing in this directory changes library behaviour.
Investigated 2026-09-21 against commit `91b4a2c` of `xborder/arrow-java`
(identical to upstream `apache/arrow-java` at that point; no fork-specific patches).

## 0. Bottom line

**For this repository, Jev is not a big deal.** Arrow Java is a deterministic,
latency-critical columnar library. It has no natural-language inputs on its data
path, no model calls anywhere, no text it generates only to parse back into a
decision, and every judgment it makes at runtime must be bit-exact and reproducible
across seven language implementations. A model that answers in 70 to 500 ms is
between six and nine orders of magnitude too slow for anything on the value, batch,
or allocation path, and a JDBC driver that phones a third-party SaaS to classify an
exception would be rejected by every serious deployer of it.

What survives scrutiny is peripheral to the library and lives in the **development
workflow**, where the inputs actually are text: CI failure logs, pull request
titles and bodies, and issue reports. Three opportunities hold up, one of them
strongly. None reduces an existing model bill, because there is none. Two would
modestly improve maintainer throughput; one might change how flaky tests are
handled. The economics are not the deciding factor at this project's volume
(tens of CI failures a week); structured, calibrated output without a parsing step
is.

The genuinely interesting "semantic judgments as a routine operation" ideas
(classifying columns by meaning, spotting PII in schemas, ranking candidate type
mappings by sample values) belong to systems built **on top of** Arrow, such as a
query engine or catalog, not inside the columnar library. This document says so
rather than proposing them here.

## 1. What was inspected, and what Jev is

### 1.1 Sources on Jev

The environment used for this investigation blocks `docs.typesafe.ai` and
`archerhume.com` at the network proxy, so the five sources named in the brief could
not be read directly. Facts below are taken from secondary sources that quote them,
and are labelled accordingly. Anything marked *unverified* should be checked against
the vendor docs before an experiment is budgeted.

| Fact | Value | Source class |
| --- | --- | --- |
| Interface | One `state` string plus a map of named `questions`; each question is `choice`, `score`, or `noul`; questions are evaluated independently against the same state, in one parallel pass | Vendor docs, as quoted by an independent reference gist and by the open-source Kev clone that mirrors the API |
| `choice` | Returns the chosen option, a probability per option, and a confidence in 0..1; up to 255 options | Vendor docs (quoted) |
| `score` | Ordered rubric of 2 to 10 levels; returns the probability-weighted mean of the level indices `0..n-1` (not normalised to 0..1), per-level probabilities, confidence | Vendor docs (quoted); confirmed by independent measurement |
| `noul` | A yes/no probability 0..1; **no confidence field**, so distance from 0.5 is the only certainty signal | Independent measurement |
| Price | US$0.042 per million input tokens; output tokens not billed | Vendor claim, restated by measurement repo |
| Latency | Vendor: 70 to 500 ms. Independent, one machine: wall-clock median 458 to 563 ms, of which ~199 ms was TCP connect; server time 70 to 81 ms whether 1 or 5 questions were asked | Vendor claim vs. independent measurement |
| Accuracy | 27/27 on a small support-ticket routing set, tying `mistral-small-3.2-24b` exactly. On a held-out set the self-hosted Kev-9B clone reports 0.852 accuracy vs 0.857 for Jev | Independent measurement; clone's own benchmark |
| Calibration | Trained with RLCD (Reinforcement Learning for Calibrated Decisions), so confidence is claimed to track accuracy in aggregate. One independent article headline reports 62.6% correct when a task was asked as a single question vs 95% when decomposed into five, which signals strong sensitivity to decomposition and wording | Vendor claim; independent article (headline only, body unreachable) |
| Context limit | Not verifiable here. The Kev clone serves up to 8,192 tokens of state plus questions | *Unverified for Jev* |
| Batching | One request evaluates many questions against one state; there is no documented way to evaluate many states in one request | Vendor docs (quoted); *check* |
| Architecture (hypothesis) | Prefill-only forward pass over shared state, questions isolated from each other, probabilities read from the model's own representations, sparse MoE backbone suspected | Black-box latency probing, ~10,000 calls, explicitly labelled deduction by its author |
| SDKs | JavaScript and Python are referenced; no Java SDK found. The API is plain HTTPS+JSON, so a Java client is a trivial wrapper | Secondary sources |

The wire shape, as mirrored by the Kev clone (which states it matches TypeSafe's
field names):

```json
POST /v1/systemone
{
  "state": "<text to judge>",
  "questions": {
    "kind":     {"type": "choice", "instructions": "...", "criteria": {"a": "...", "b": "..."}},
    "urgent":   {"type": "noul",   "instructions": "..."},
    "severity": {"type": "score",  "instructions": "...", "criteria": ["low", "medium", "high"]}
  }
}
→
{
  "answers": {
    "kind":     {"type": "choice", "choice": "a", "confidence": 0.61, "probabilities": {"a": 0.78, "b": 0.22}},
    "urgent":   {"type": "noul",   "noul": 0.12},
    "severity": {"type": "score",  "score": 1.44, "confidence": 0.78, "legend": {"0": "low", "1": "medium", "2": "high"},
                 "probabilities": {"0": 0.0, "1": 0.56, "2": 0.44}}
  },
  "usage": {"input_tokens": 101, "output_tokens": 0},
  "latency_ms": 495
}
```

### 1.2 The project

Arrow Java is the JVM implementation of the Apache Arrow columnar format. It is a
library, not a service. Core modules: `memory` (off-heap allocator with reference
counting and size-rounding policies), `vector` (typed `ValueVector`s,
`VectorSchemaRoot`, IPC readers and writers), `format` (FlatBuffers wire metadata).
Around it: `compression`, `algorithm`, `c` (C Data Interface), `adapter/jdbc`,
`adapter/avro`, `adapter/orc`, `dataset` and `gandiva` (JNI to Arrow C++), and the
largest functional area, `flight` (gRPC transport, Flight SQL, and an Avatica-based
Flight SQL JDBC driver). Tests run under Maven on JDK 17/21/25 across three OSes,
plus cross-language integration via Archery against six other Arrow implementations.
Releases follow the ASF process with a human vote.

A repository-wide search found **no** LLM, embedding, or ML usage in code, CI, or
scripts.

### 1.3 Where the brief's checklist lands

| Brief asks where we... | Finding in this repo |
| --- | --- |
| Spend money or time on model calls | Nowhere. |
| Generate text only to parse it into a decision | Nowhere in code. Humans do it in PR review and CI triage. |
| Repeatedly process the same context | The allocator and vectors re-derive sizes, but numerically, not semantically. CI re-runs the full matrix on every push. |
| Serialize judgments that could be independent | PR checks in `.github/workflows/dev_pr.js` run title, label, and linked-issue checks as separate rule steps; they are already independent. |
| Use brittle rules because semantic understanding seemed impractical | Yes, in a handful of places, listed in §2. Most exist because determinism is required, not because semantics were expensive. |
| Rely on manual review, coarse categories, sampling, or delayed batch processing | Yes: manual PR labelling into five fixed categories, manual `@Disabled` for flaky tests (26 `@Disabled` annotations, 9 explicit "flaky" comments), manual CI triage, human release vote. |
| Discard information or limit coverage to stay within a budget | `reportUnsupportedTypesAsOpaque` keeps only the vendor type name as a string; gRPC codes without a Flight equivalent collapse to `UNKNOWN`; but these are protocol budgets, not compute budgets. |

## 2. Concrete decision points found in the code

Grouped by whether a semantic judgment could plausibly help. Paths are relative to
the repo root.

**A. Text-classifying rules (the only places Jev's input type matches)**

- `flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/client/ArrowFlightSqlClientHandler.java:333-338`
  `isBenignCloseException` suppresses an error if the Flight code is `UNAVAILABLE`,
  or `INTERNAL` **and** the message contains the literal `"Connection closed after GOAWAY"`.
  This is a semantic judgment ("is this a shutdown artefact?") implemented as a substring match on gRPC's human-readable text.
- `flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightJdbcDriver.java:95,240,253,262,265`
  and `ArrowDatabaseMetadata.java:495,707,805`: failures wrapped as bare
  `SQLException` with no SQLState and no vendor code, so callers can only classify by message text.
- `flight/flight-core/src/main/java/org/apache/arrow/flight/grpc/StatusUtils.java:82-118`
  gRPC `ABORTED`, `OUT_OF_RANGE`, `DATA_LOSS`, `FAILED_PRECONDITION` all collapse to `FlightStatusCode.UNKNOWN`.
- `adapter/jdbc/src/main/java/org/apache/arrow/adapter/jdbc/JdbcToArrowUtils.java:221-241`
  Unmapped `java.sql.Types` throw, or with `reportUnsupportedTypesAsOpaque` become
  `OpaqueType(NULL, typeName, vendorName)`: the vendor's type **name** is the only
  information retained.

**B. Type-mapping tables (deterministic by design; a model must not decide these at runtime)**

- `JdbcToArrowUtils.java:164-222` JDBC to Arrow switch; `:180-184` `precision > 38` chooses Decimal256 over Decimal128; `:200-209` TIME and TIMESTAMP always millisecond; `:215` ARRAY needs an out-of-band element type.
- `flight/flight-sql-jdbc-core/.../utils/SqlTypes.java:95-170` Arrow to `java.sql.Types`; `Struct`, `Duration`, `Interval`, `Map`, `Union` all become `JAVA_OBJECT`.
- `adapter/avro/.../AvroToArrowUtils.java:183-196, 592-600` two-branch union with null is treated as nullable field unless legacy mode.
- `adapter/jdbc/.../binder/ColumnBinderArrowTypeVisitor.java` four switches with default fallbacks.

**C. Numerical heuristics (not semantic; Jev is the wrong tool)**

- `vector/.../BaseVariableWidthVector.java:43-46` `DEFAULT_RECORD_BYTE_COUNT = 8` assumed average bytes per string before any data is seen.
- `memory/memory-core/.../rounding/DefaultRoundingPolicy.java:37-61,106` power-of-two rounding, up to 2x over-allocation; bad config silently falls back.
- `vector/.../compression/AbstractCompressionCodec.java:49-55` compress, then discard if the result is larger.
- `adapter/jdbc/.../JdbcToArrowConfig.java:51` `DEFAULT_TARGET_BATCH_SIZE = 1024`.

**D. Human process (text inputs, low volume, no latency budget)**

- `.github/workflows/dev_pr.js:195-198` PR title must match `MINOR: ` or `GH-NNN: `.
- `:208-212` `breaking-change` label applied only if a body line starts with the exact string `**This contains breaking changes.**`.
- `:225-229` PR must carry one of `bug-fix`, `chore`, `dependencies`, `documentation`, `enhancement`; otherwise the bot posts a nag comment and fails the check.
- `.github/workflows/dev_pr_milestone.sh:37-45` milestone = first one that looks like a semver.
- `.github/release.yml` release notes bucketed by those labels.
- Flaky tests: `flight-core/src/test/.../TestApplicationMetadata.java:53-54` ("consistently flaky on CI"), `TestFlightClient.java:83,130,158`, `TestBasicOperation.java:339-341`, `TestBackPressure.java`, `TestDoExchange.java:375`, `TestCallOptions.java`, `TestCookieHandling.java`, and others: handled by `@Disabled`, with no surefire retry configuration anywhere.
- `.github/dependabot.yml` weekly bumps dominate the log (about 25 of the last 40 commits).
- `.github/CODEOWNERS` routes every path to the same four or five committers.

## 3. First-principles reframe

> If many useful semantic judgments were affordable within our response-time budget, what would we design differently?

For the library's runtime, the honest answer is **nothing**. The response-time
budget on the data path is nanoseconds per value and microseconds per batch. The
budget at connection setup in the JDBC driver is milliseconds, and the driver runs
inside customers' JVMs, often air-gapped, under security review; an outbound call to
a model vendor from a database driver is disqualifying regardless of price. The
type-mapping tables are not brittle because semantics were expensive; they are
tables because two Arrow implementations must agree bit-for-bit on the answer.

Assumptions in the architecture that exist because semantic computation was
expensive, which the brief asks for explicitly:

1. **"An unknown vendor type is opaque."** (`reportUnsupportedTypesAsOpaque`.) This
   exists because nobody has written the mapping, not because judging it is costly.
   A cheap classifier could *propose* mappings for the long tail of vendor type
   names, offline, to be reviewed and frozen into the table. It must not decide at
   runtime: a wrong mapping is silent data corruption, and the cost asymmetry is
   extreme.
2. **"Shutdown noise is recognised by one magic string."** This exists because
   gRPC does not expose the distinction structurally. A semantic classifier could
   recognise the whole family of benign-close messages. But the correct fix is
   still deterministic (match on gRPC status and a curated set of causes), and a
   model can help *build* that set from logs, offline.
3. **"Flaky tests are disabled by hand, after a human notices."** This one really is
   a coverage decision made because judging each red CI run is expensive human
   time. It is the strongest candidate, below.
4. **"PR category is whatever the author labelled."** Human labelling is cheap
   enough that the project simply nags. A model changes the UX from "fail and nag"
   to "suggest and confirm", a small but real improvement.

The three opportunity kinds the brief names:

- **Direct savings:** none available; there is no model spend and no text-parsing pipeline to replace.
- **Better outcomes:** CI triage, PR/issue labelling.
- **New capabilities:** per-failure flake probability with a calibrated abstain band, enabling an automatic "quarantine and open tracking issue" path instead of manual `@Disabled`; risk scoring of dependency bumps before a human looks.

## 4. Ranked opportunity table

| Rank | Opportunity | Kind | Where | Benefit | Effort | Worst failure | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | CI failure triage: flake vs regression vs infra, per failed job | New capability | New GitHub Action reading job logs; no library change | Replaces manual triage and blanket `@Disabled`; gives a calibrated flake probability per test | Medium (1 to 2 weeks incl. labelled set) | A real regression scored as a flake and auto-rerun to green | **Pursue, gated on §6 experiment** |
| 2 | PR/issue triage: suggest one of the 5 category labels, `breaking-change` likelihood, template routing | Better outcome | Extend `dev_pr.js` step; comment with suggestions, never auto-apply blocking labels | Fewer nag cycles; consistent release-note buckets | Low (days) | Wrong `breaking-change` suggestion accepted without reading | Pursue, low ceiling |
| 3 | Dependency-bump risk score for dependabot PRs | Better outcome | Same action as 2, state = PR body + changelog excerpt | Prioritise review of ~25 bumps/month | Low | Score treated as approval; supply-chain risk is not visible in text | Pursue only as a sort key |
| 4 | Offline: propose Arrow type families for the OpaqueType long tail of vendor type names | Tooling | One-off script; output reviewed, frozen into `JdbcToArrowUtils` | Shrinks the opaque fallback set | Low | Wrong mapping merged | Marginal; a human with vendor docs is as fast |
| 5 | Offline: mine driver/Flight error messages into a SQLState mapping table | Tooling | One-off script over logs | Real SQLStates on `SQLException`s | Low | None at runtime (table is static) | Marginal; the deterministic fix is the point, not the model |
| 6 | Runtime classification of `FlightRuntimeException` as benign | Runtime | `ArrowFlightSqlClientHandler` | Replaces substring match | — | Network call in a driver close path; cannot ship | **Rejected** |
| 7 | Runtime type mapping / schema inference with a model | Runtime | adapters | — | — | Non-deterministic schemas across runs; silent corruption | **Rejected** |
| 8 | Allocator, batch-size, compression heuristics | Runtime | `memory`, `vector` | — | — | Not a semantic problem | **Rejected** |
| 9 | Semantic column classification (PII, meaning) over schemas and samples | New capability | Not this repo | Large, for a catalog or engine | — | — | **Out of scope**: belongs above Arrow |

## 5. Detailed designs for the three that survive

### 5.1 CI failure triage

**User problem and current behaviour.** A red CI job on a PR produces a wall of
Maven/surefire output. A maintainer reads it, decides whether it is the PR's fault,
a known flaky test, or a runner/network problem, and either re-runs, comments, or
eventually adds `@Disabled` with a comment such as "This test is consistently flaky
on CI, unfortunately." Nothing records the decision; the same judgment is repeated
across PRs.

**Integration point and input state.** A `workflow_run` triggered action (on
`conclusion == failure` of `test.yml` or `integration.yml`) fetches the failed jobs'
logs via the Actions API. The state is *not* the whole log. Deterministic code
extracts: the failing test class and method names from surefire summary lines, the
first exception per failure with up to ~40 lines of stack trace, the last ~60 lines
of the job, the OS/JDK matrix cell, and the PR's changed file list. Target: 2,000 to
4,000 tokens. This extraction is where most of the engineering is.

**Questions, one request per failed job.** All independent; all share one request.

```json
{
  "state": "<extracted failure digest>",
  "questions": {
    "cause": {
      "type": "choice",
      "instructions": "What is the most likely cause of this CI job failure?",
      "criteria": {
        "regression":  "The failing assertion or exception is in code this PR changed or directly exercises",
        "flaky_test":  "A timing, ordering, port, or resource-race failure in a test unrelated to the PR's changes",
        "infra":       "Runner, network, download, Docker, disk, or dependency-resolution failure before or outside any test body",
        "build_tooling": "Compilation, checkstyle, spotless, errorprone, or enforcer failure",
        "unclear":     "The digest does not contain enough evidence to decide"
      }
    },
    "touches_changed_code": {
      "type": "noul",
      "instructions": "Does the failing test or stack trace reference any file in the PR's changed-files list?"
    },
    "known_flaky_family": {
      "type": "noul",
      "instructions": "Does the failure match a Flight/gRPC shutdown, port-binding, or back-pressure timing pattern?"
    },
    "actionability": {
      "type": "score",
      "instructions": "How much human attention does this failure need right now?",
      "criteria": ["none, safe to re-run once", "look within the day", "blocks merge, investigate now"]
    }
  }
}
```

Note that `touches_changed_code` is a question deterministic code can also answer
by string matching; asking it anyway is a cheap **consistency check** on the model,
and disagreement should lower trust in `cause` for that job.

**Consumer pseudocode.**

```js
const a = answers;
const pReg = a.cause.probabilities.regression;
const pFlake = a.cause.probabilities.flaky_test + a.cause.probabilities.infra;

if (pReg >= 0.6 || a.touches_changed_code.noul >= 0.7) {
  comment("Likely caused by this PR: ...digest...");        // never re-run
} else if (pFlake >= 0.85 && a.cause.confidence >= 0.5 && rerunsSoFar === 0) {
  rerunFailedJobs(); recordFlakeObservation(testName, runId);  // at most once
} else {
  comment("Unclear failure; needs a human. Model said: " + summary(a));
}
if (flakeObservations(testName) >= 3 in 30 days && noOpenIssue(testName)) {
  openIssue("Flaky test: " + testName, evidenceLinks);       // replaces silent @Disabled
}
```

**What still needs other machinery.** Log extraction (deterministic). Flake history
(a small JSON file or issue labels). Re-run (Actions API). Any *fix* (a human).
Nothing here needs generation.

**Failure mode that matters.** False "flake" on a real regression, followed by a
green re-run masking it. Mitigations: the re-run cap of one, the hard
`touches_changed_code` override, and requiring the regression probability to be
low *and* confidence high before re-running. The asymmetric cost is why the
go/no-go criterion in §6 is precision on `regression`, not overall accuracy.

**Economics.** Volume: the test matrix is 5 cells per push; assume 10 to 40 failed
jobs per week. At 3,000 tokens per digest plus ~250 tokens of questions, the model
cost is about US$0.00014 per job, so well under a dollar a year. Latency is
irrelevant (the action already runs after a 10 to 30 minute job). The real costs are
the action's runtime minutes and the labelled dataset. Compared with alternatives:
a regex list of known flaky test names costs nothing and catches the *known* flakes
today; a general LLM at 3k tokens costs roughly 10 to 100 times more per call but at
this volume is still cents per month, and can also write the explanatory comment.
**Jev's edge here is not price; it is a calibrated probability per option without a
parsing step, which is exactly what the threshold logic above consumes.** If
calibration turns out poor on our logs (see §6), a small generative model with
JSON output is an equally good choice.

### 5.2 PR and issue triage

**Current behaviour.** `check_labels` fails the PR and posts a comment until an
author picks one of five labels; `apply_labels` looks for one exact bold string
to add `breaking-change`; issue templates route bug/feature/question at filing time
with no check afterwards.

**Integration.** Add a non-blocking step to `dev_pr.yml` that runs on
`opened`/`edited`. State: PR title + body + list of changed paths + (optionally) the
first 200 lines of the diff, capped at ~3,000 tokens. Questions, in one request:

- `category` choice over the five existing labels, plus `mixed` and `unclear`.
- `breaking` noul: "Does this change remove, rename, or change the behaviour of a public Java API or wire format?"
- `needs_issue` noul: "Is this more than a trivial fix, so that it should be linked to a GitHub issue rather than titled MINOR?"
- `api_surface` score: `["none", "internal only", "public API additive", "public API breaking"]`.

**Consumer.** Post one suggestion comment ("Suggested label: enhancement (0.81).
Breaking-change probability 0.12."). Apply the category label automatically only
when `probabilities[top] >= 0.8 && confidence >= 0.6`; never auto-apply
`breaking-change`, only suggest it, because a false negative there is a release-note
omission a human must own. Keep the existing hard checks unchanged.

**Effort and ceiling.** A day or two. The upside is bounded: the label set is five
categories, the volume is a few PRs a day, and dependabot already labels its own
PRs. This is worth doing as a companion to 5.1 in the same action, not on its own.

### 5.3 Dependency-bump risk as a review sort key

**Current behaviour.** Roughly 25 dependabot bumps a month, all titled `MINOR:`,
reviewed in arrival order.

**Integration.** In the same action, for PRs by `dependabot[bot]`: state = PR body
(dependabot includes release notes and changelog excerpts) + which modules depend on
the artifact (from `pom.xml`). Questions: `bump_kind` choice
`{patch, minor, major, unclear}`; `mentions_breaking` noul; `security_fix` noul;
`blast_radius` score `["test only", "one module", "core memory/vector", "flight or driver"]`.

**Consumer.** Sort the review queue by `security_fix` desc, then `blast_radius`
desc. Nothing is auto-merged. The failure mode is treating a low score as safety;
supply-chain risk (a compromised upstream) is invisible in text, so this must be
framed as ordering, not gating.

## 6. An evaluation that could prove us wrong

Focused on 5.1, since 5.2 and 5.3 are cheap enough to try and revert.

**Dataset.** Pull the last 12 months of failed `test.yml` and `integration.yml`
jobs from `apache/arrow-java` (upstream has far more history than this fork).
Label each by what actually happened next: a re-run that passed with no code change
→ `flaky_test` or `infra` (split by whether a test body ran); a follow-up commit on
the PR that made it green → `regression`; checkstyle/spotless/compile output →
`build_tooling`. Hold out 30% by *test class*, not by run, so the model is judged on
flaky families it has not seen. Target 150 to 300 labelled jobs; 50 is enough for
the first go/no-go.

**Baselines.** (a) A regex list of the test classes currently carrying `@Disabled`
or a "flaky" comment. (b) `touches_changed_code` string matching alone. (c) One
small generative model asked the same four questions with JSON output, same digest.

**Metrics.** Precision and recall per class, with the go/no-go on
**precision for `regression` ≥ 0.90 at recall ≥ 0.70** and on flake-rerun
decisions: **false re-run rate (regression re-run as flake) ≤ 2%**. Calibration:
Brier score per question and a reliability diagram over the `cause` top-probability
in 0.1 bins; expected calibration error ≤ 0.10 or the thresholds above are
meaningless. Coverage: share of jobs decided automatically at those thresholds
(want ≥ 60%; below that the human still triages most failures and the feature is
mostly noise).

**Sensitivity tests.**
- Digest construction: head vs tail vs error-window extraction; ± the changed-files list.
- Question wording: three paraphrases of `cause`; report the spread of accuracy. The independent report of 62.6% vs 95% under different decompositions makes this mandatory.
- Batch composition: `cause` alone vs all four questions; probabilities should not move.
- Adversarial: a real regression whose stack trace happens to pass through gRPC shutdown code; a flake in a file the PR touched for formatting only.
- Missing evidence: truncate the digest to the last 20 lines; `unclear` should rise, not `flaky_test`.

**Cost and latency under load.** Irrelevant to go/no-go here; record them anyway
(p50/p95 wall-clock from a GitHub runner, tokens per request, cost per job).

**Go/no-go.** Go if the precision, false re-run and calibration bars are met on the
held-out split *and* the model beats baseline (a) on recall for flakes by a
meaningful margin (else the regex list is enough). No-go otherwise, in which case
the cheapest good outcome is still to ship the deterministic parts: the digest
extractor, the flake-observation log, and the "open a tracking issue after three
observations" rule.

**Runnable harness.** `run_eval.py` in this directory reads a JSONL of
`{id, state, questions, gold}` records, calls the endpoint, and reports accuracy,
Brier, MAE for score questions, latency percentiles, and cost at the published
price. `--dry-run` prints request shapes and token estimates without network access.
`examples/ci_failure_triage.jsonl` holds six **synthetic** digests modelled on the
flaky patterns documented in this repo's own test comments; they illustrate the
input format and are not evidence. No TypeSafe credentials or reachable endpoint
were available in this environment, so **no results were produced and none are
claimed.**

**Smallest experiment that resolves the biggest uncertainty.** Fifty labelled
historical failures, four questions, one afternoon. Total model cost well under one
US dollar. It answers the only question that matters for 5.1: is the `regression`
probability calibrated well enough on Maven/gRPC logs to gate an automatic re-run?

## 7. A first-principles sketch of the relevant parts, given this capability

If Arrow Java's CI were designed today with cheap calibrated judgments available:

- Every red job produces a structured **failure record** (deterministic digest +
  model probabilities + human outcome), stored as an artifact and indexed by test
  class. Flakiness becomes a measured rate per test, not a comment in the source.
- `@Disabled` is replaced by a **quarantine list** maintained by the action: a test
  enters quarantine after N model-plus-human-confirmed flake observations, runs in a
  non-blocking job while quarantined, and exits when it passes M consecutive times.
  The library code never changes for CI reasons.
- PR checks **suggest, then verify**: labels and breaking-change flags are proposed
  on open, confirmed by the author with one click, and only then enforced.
- The **library itself is unchanged**. Its correctness contract is deterministic
  interoperability, and no part of that contract benefits from probabilistic
  judgment.

## 8. Rejected ideas and why

- **Classify Flight/gRPC exceptions at runtime in the JDBC driver.** A network call
  to a third party from a driver's close path, in customers' JVMs, is unacceptable
  on security, offline-operation, and licensing grounds; also the 70 to 500 ms
  latency lands on connection close, where users notice. The right fix for
  `isBenignCloseException` is structural (gRPC status + cause type), possibly
  informed offline by mining logs.
- **Model-assisted schema/type mapping at runtime.** Two runs must produce the same
  schema; two Arrow implementations must agree. A probabilistic mapper violates
  both. Offline suggestion for the OpaqueType long tail is fine but is a one-off
  that a person with vendor documentation does as quickly.
- **Allocator sizing, batch size, compression choice.** These are numerical
  decisions over numbers already in hand. Sampling the first batch and measuring
  beats asking anything, and costs microseconds.
- **Column-level semantic classification (PII, units, meaning).** Real and
  valuable, and a natural fit for `choice` over a schema plus sample values, but it
  is a catalog or query-engine feature. Arrow Java carries no policy layer to act on
  the answer. Recommend raising it in whichever downstream system consumes these
  vectors.
- **Release-note generation or bucketing.** Already fully determined by labels via
  `.github/release.yml`; fixing labelling (5.2) fixes this for free.
- **Self-hosting the open-source Kev clone inside CI to avoid a vendor.** Possible
  (Apache-2.0, Qwen3.5-based, 0.8B to 9B), but a 9B model on a GitHub-hosted runner
  is CPU-only and slow; only worth it if the vendor is disallowed for policy reasons.
  Keep as a fallback plan, not the first move.

## 9. What was inspected, measured, and remains hypothetical

- **Inspected:** every module's `pom.xml`, the full `.github/` tree, `dev/`, `ci/`,
  the JDBC and Avro adapters' type mapping, Flight `StatusUtils`, the Flight SQL JDBC
  driver's client handler and driver classes, memory rounding policies,
  variable-width vector sizing, compression codec, and all `@Disabled`/flaky test
  annotations. Repository-wide grep for any AI/ML dependency (none).
- **Measured:** nothing against Jev. The endpoint and its documentation were
  unreachable from this environment. All Jev numbers are quoted from secondary
  sources and labelled as vendor claim or independent measurement.
- **Hypothetical:** every accuracy, calibration, and coverage figure in §6 is a
  target, not a result. The architecture description of Jev is a third party's
  deduction. The Jev context limit is unknown here; the digest size budget in 5.1
  assumes at least 8k tokens and must be re-checked.

## Sources consulted

Vendor documentation (blocked here; verify first): docs.typesafe.ai/introduction,
/primitives, /api, /llms.txt. Secondary: the independent measurement repository
`WallerChen/jev-measured`; the API-compatible open-source clone `jaredpalmer/kev`
(request/response shape, limits); a community reference gist on Jev primitives and
confidence; press coverage of the launch (Tom's Hardware, TypeSafe blog) for the
headline speed and price claims; the archerhume.com architecture post via its
abstract and citations only.
