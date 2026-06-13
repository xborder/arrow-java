# Plan: Reduce GitHub Actions time for the `Test` workflow (`.github/workflows/test.yml`)

> Handoff document. The executing agent has **no prior context** — everything needed is here.
> Work on branch `claude/arrow-java-workflows-99cxw7`. Commit per phase. Do **not** open a PR unless explicitly asked.

## 0. Background & goal

`arrow-java` is the Apache Arrow Java implementation (Maven, 17 modules, compiles to
`maven.compiler.release=17` — see `pom.xml:123-125`). The `Test` workflow is the slowest and most
expensive CI workflow. Goal: **cut GitHub Actions minutes for `test.yml` without losing meaningful
test coverage.**

### How the workflow runs today (`.github/workflows/test.yml`)

10 jobs per run, **each compiles from scratch then tests**:

| # | Job | Runner (billing multiplier) | Work |
|---|---|---|---|
| 1–3 | `ubuntu` / JDK {17,21,23} / image `ubuntu` | Linux (1×) | compile reactor + pure-Java tests |
| 4–6 | `ubuntu` / JDK {17,21,23} / image `conda-jni-cdata` | Linux (1×) | build native C-Data `.so` + compile + JNI tests |
| 7 | `macos` Intel / JDK 17 (`macos-15-intel`) | macOS (**10×**) | compile reactor + pure-Java tests |
| 8 | `macos` ARM / JDK 17 (`macos-latest`) | macOS (**10×**) | compile reactor + pure-Java tests |
| 9 | `windows` / JDK 17 | Windows (**2×**) | compile reactor + pure-Java tests |
| 10 | `integration` | Linux (1×) | builds 6 sibling languages + cross-language tests |

### Key facts established during analysis (do not re-derive — verify if changing)

- **Pure-Java vs JNI:** the `ubuntu`-image jobs, macOS jobs, and Windows job build **pure Java only** —
  they do NOT set `ARROW_JAVA_JNI` / `ARROW_JAVA_CDATA`, which default to `OFF` (`ci/scripts/build.sh:71,75`;
  gated in `ci/scripts/test.sh:43,58`). Only `conda-jni-cdata` builds native code (the C-Data Interface),
  and only on Linux x86_64.
- **Compile-twice bug:** `compose.yaml` runs `build.sh && test.sh`. `build.sh:80` does `mvn -T 2C clean install`,
  then `test.sh:40` does `mvn ... clean test` — the `clean` wipes what `build.sh` produced and recompiles.
  `test.sh:49,59` add two more `mvn clean test` invocations. So a job compiles 2–4×.
- **Bytecode portability:** `release=17` bytecode runs unchanged on JDK 17/21/23 and is identical across
  OS/arch. So one compile is valid everywhere at the JDK-17 baseline.
- **JNI ABI is JDK-stable:** the native `.so` is JDK-agnostic. Releases (`rc.yml`) build JNI **once with JDK 17**
  (`rc.yml:276` macOS uses `openjdk@17`, `rc.yml:320` Windows pins `java-version: '17'`) and ship that single
  binary for all JDKs. So per-JDK native rebuilds in `test.yml` are unnecessary.
- **Cache key over-invalidation:** `test.yml:69` keys the Docker-volume cache on `**/*.java`, so any code edit
  busts the Maven dependency cache.
- **Triggers:** `test.yml:20-27` fires on `push` (all branches) AND `pull_request`. The concurrency group
  (`test.yml:30`) keys on `head_ref || sha`, so push and PR runs do not cancel each other → duplicate runs on
  PR branches.
- **`integration` job** (`test.yml:149-216`) checks out 7 repos, builds 6 languages from `main` (no pins),
  has a 60-min timeout, **no `WIP` guard, no path filter**. Its harness lives in `apache/arrow`
  (`dev/archery`, `ci/scripts/integration_arrow*.sh`, the `conda-integration` compose service) — not in this repo.
  Verified upstream: `integration_arrow_build.sh` builds every language from source with no version pinning and
  no release-binary path; backward-compat uses static "gold" data files (`--gold-dirs`), not rebuilt binaries.

---

## Execution phases

Phases are ordered by increasing risk/effort and are independent unless noted. **Commit after each phase.**
Validate by running `actionlint` (or `pre-commit run --all-files`, see `.github/workflows/dev.yml`) and,
where possible, by triggering the workflow on the fork before relying on it.

---

### Phase 0 — (Optional, recommended) Size the payoff

Before the big restructure (Phase 4), pull the compile-vs-test time split from a recent run to confirm it's
worth it. Two sources:
- GitHub Actions run logs for the `macos` / `windows` jobs (look at the `Build` step vs `Test` step durations).
- Develocity build scans (the workflow passes `DEVELOCITY_ACCESS_KEY`, `test.yml:74,110,141`).

**Decision gate:** if the `Build` (compile) step is a small fraction of each macOS/Windows job, Phase 4 has low
ROI — stop after Phase 3. If compile is significant, proceed to Phase 4.

---

### Phase 1 — Safe quick wins (low risk, independent)

**1a. Fix the Maven/Docker-volume cache key** — `test.yml:69`
- Change:
  ```
  key: maven-${{ matrix.jdk }}-${{ matrix.maven }}-${{ hashFiles('compose.yaml', '**/pom.xml', '**/*.java') }}
  ```
  to:
  ```
  key: maven-${{ matrix.jdk }}-${{ matrix.maven }}-${{ hashFiles('compose.yaml', '**/pom.xml') }}
  ```
- Why: dependencies only change when POMs change; `**/*.java` makes the cache miss on every code edit.
- Risk: none.

**1b. Cache `~/.m2` on macOS and Windows** — `test.yml:97-101` and `test.yml:128-132`
- Add `cache: 'maven'` to both `actions/setup-java@v5` steps:
  ```yaml
  - uses: actions/setup-java@v5
    with:
      distribution: 'temurin'
      java-version: ${{ matrix.jdk }}
      cache: 'maven'
  ```
- Why: these premium runners currently re-download all dependencies every run.
- Risk: none.

**1c. Add the `WIP` guard to the `integration` job** — `test.yml:149-152`
- The other jobs have `if: ${{ !contains(github.event.pull_request.title, 'WIP') }}` (`test.yml:43,84,121`);
  `integration` does not. Add the same `if:` to the `integration` job.
- Risk: none (only skips WIP PRs).

**1d. Stop push+PR double-triggering** — `test.yml:20-27`
- Change the `push` trigger to long-lived branches only:
  ```yaml
  on:
    push:
      branches: [main, 'maintenance-**']
      tags: ['**']
    pull_request:
  ```
- Why: removes the duplicate full-matrix run on every PR-branch push (concurrency groups don't dedupe them).
- Risk: low. PR coverage is unchanged; `main` and tags still build on push.
- NOTE: verify the maintenance-branch naming convention in this repo (`git branch -a`); adjust the glob if
  the convention differs (e.g. `release-**`). If unsure, ask the requester.

---

### Phase 2 — Remove the compile-twice waste (low risk, one validation run)

Eliminate the redundant `clean` recompilation. Two equivalent options — prefer **2a**.

**2a. Remove `clean` from `ci/scripts/test.sh`** so test reuses `build.sh` output:
- `test.sh:40`: `${mvn} -Darrow.test.dataRoot="..." clean test` → drop `clean` → `... test`
- `test.sh:49`: `${mvn} clean test -Parrow-jni ...` → `${mvn} test -Parrow-jni ...`
- `test.sh:59`: `${mvn} clean test -Parrow-c-data ...` → `${mvn} test -Parrow-c-data ...`
- Rationale: `build.sh:80` already did `clean install`; Maven incremental compilation reuses up-to-date classes.

**2b. (Alternative)** Collapse `build.sh` + `test.sh` into one `mvn install` reactor pass in `compose.yaml`
(`compose.yaml:48-51` and `:82-85`). More invasive; only if 2a proves insufficient.

- Validation: run the full suite once (locally via `docker compose run ubuntu`, or on the fork) and confirm
  test counts/results match the pre-change baseline. Watch for any test that depended on a clean `target/`.
- Risk: low. Keep `rc.yml` (release/packaging) untouched — it legitimately needs the separate skip-tests install.

---

### Phase 3 — Spend fewer jobs per PR (medium effort)

**3a. Event-aware JDK matrix** — `test.yml:45-55`
- On `pull_request`, test LTS only (JDK 17, 21); on `push`/main + nightly, test full set (17, 21, 23).
- Implement with a `setup` job that emits the matrix as JSON, consumed via `fromJson` in `strategy.matrix`,
  OR with `include`/`exclude` guarded by `github.event_name`. Example setup-job pattern:
  ```yaml
  setup:
    runs-on: ubuntu-latest
    outputs:
      jdk: ${{ steps.m.outputs.jdk }}
    steps:
      - id: m
        run: |
          if [ "${{ github.event_name }}" = "pull_request" ]; then
            echo 'jdk=[17,21]' >> "$GITHUB_OUTPUT"
          else
            echo 'jdk=[17,21,23]' >> "$GITHUB_OUTPUT"
          fi
  ```
  Then `ubuntu` and `conda-jni-cdata` matrices use `jdk: ${{ fromJson(needs.setup.outputs.jdk) }}`.
- Optionally reduce macOS to one arch on PRs, both on push.
- Risk: low — a JDK-23-only break surfaces on merge/nightly instead of PR. Acceptable for non-LTS.

**3b. Gate the `integration` job** — `test.yml:149`
- Restrict to interop-relevant changes + main/nightly. Add `paths`-based gating (interop lives in
  `format/`, `vector/`, `c/`, `flight/`, plus build files `**/pom.xml`, `compose.yaml`), and/or run it only on
  `push` to `main` and via the nightly `rc.yml` cron rather than every PR.
- `pull_request` path-filtering options: a job-level `if` checking changed paths (needs a changed-files action),
  or split `integration` into its own workflow file with top-level `on.pull_request.paths`.
- Why: it's the single longest job (60-min timeout) and runs even on docs-only PRs.
- Risk: low-medium — IPC/C-Data/Flight regressions caught at merge/nightly if a PR skips it. Confirm with the
  requester whether `integration` is a required status check (if so, a skipped job must still report success —
  use a path filter that keeps the check green, not one that removes the job).

---

### Phase 4 — Build-once / test-many restructure (the big change)

> Only proceed if Phase 0 shows compilation is a meaningful fraction of job time.
> This is the largest change. Land it as its own commit, ideally after Phases 1–3 are validated.

**Target shape: a two-stage DAG.**

**Stage 1 — Build (the only place anything compiles):**
- `build-java` (Linux / JDK 17 / x86_64): compile the full reactor incl. test-classes; upload an artifact
  containing the `target/` trees (jars + `target/test-classes`) and/or a primed `~/.m2`. Bytecode is
  `release=17` → valid on all JDKs and all OS/arch.
- `build-jni-cdata` (Linux / x86_64 / JDK 17): build the native C-Data `.so` once (via `ci/scripts/jni_build.sh`);
  upload it. JDK-agnostic, matches how `rc.yml` ships it. (May be folded into the same Linux build job.)

**Stage 2 — Test (no compilation; `mvn surefire:test -o` against prebuilt classes):**
- Pure-Java test jobs reuse `build-java`:
  - Linux JDK 17, 21, 23
  - macOS Intel (17), macOS ARM (17)
  - Windows (17)
- JNI / C-Data test jobs reuse `build-java` + `build-jni-cdata`:
  - Linux JDK 17, 21, 23

**Retain compile-coverage:** add a cheap `mvn -DskipTests test-compile` (or `compile`) check on JDK 21 and 23.
Do NOT drop compilation on newer JDKs entirely — Arrow uses `sun.misc.Unsafe`/internal APIs that newer JDKs
restrict, so a full skip risks missing compile breakage. The cheap check is a fraction of a full build.

**Implementation notes / gotchas:**
- Maven does not natively "run tests against prebuilt classes" cleanly. Two mechanisms:
  1. Ship the whole `target/` tree + primed `~/.m2` as an artifact; on test jobs run `mvn surefire:test -o`
     (offline). Fragile but explicit.
  2. **Preferred:** lean on the Develocity remote build cache already wired in (`DEVELOCITY_ACCESS_KEY`).
     Ensure the cache is *shared/remote* across matrix jobs (compilation outputs served from cache; test-execution
     tasks re-run because the cache key includes the OS). Verify current cache config before assuming reuse works.
- `needs:` adds a serialization point: every test job waits for Stage 1, so per-PR **wall-clock** gains a build
  prefix even though total **machine-minutes** drop. Acceptable, but note it.
- **Compilation collapses from ~9 reactor compiles + 3 native builds → 1 + 1** (+2 cheap compile-checks).
  Test *execution* is unchanged (that's the coverage being preserved); savings come from removing `javac`/CMake
  on every job, especially the 10× macOS and 2× Windows runners.
- Native is Linux-x86_64-only in `test.yml`, so just one native build (the OS/arch fan-out lives in `rc.yml`,
  which is out of scope here).

**Validation:** compare test counts and pass/fail per platform against the pre-Phase-4 baseline. Confirm the
JNI test jobs actually load the reused `.so` (check for `UnsatisfiedLinkError`). Confirm offline Maven runs don't
silently skip modules.

---

## Out of scope / do not touch
- `rc.yml`, `release.yml` (release/packaging pipelines) — they legitimately build per-platform and per-arch.
- The `apache/arrow` integration harness internals.
- Changing `maven.compiler.release` or the set of supported JDKs/platforms.

## Suggested commit sequence (each its own commit on `claude/arrow-java-workflows-99cxw7`)
1. `Phase 1: cache key + mac/win .m2 cache + integration WIP guard + push-branch scope`
2. `Phase 2: drop redundant clean recompile in ci/scripts/test.sh`
3. `Phase 3: event-aware JDK matrix + gate integration job`
4. `Phase 4: two-stage build-once / test-many restructure`

## Open questions to confirm with the requester before/while implementing
1. Maintenance-branch naming for the `push` filter (Phase 1d).
2. Is `integration` a required status check? (affects how Phase 3b gates it).
3. Appetite for the Phase 4 wall-clock-vs-machine-minutes trade-off, and whether Develocity remote caching is
   already enabled/shared (determines Phase 4 mechanism).
4. Whether to trial each phase on the fork's Actions before relying on it (fork PRs to apache require maintainer
   approval to run; pushes to the fork's own branches run without approval).
