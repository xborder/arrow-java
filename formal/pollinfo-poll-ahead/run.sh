#!/usr/bin/env bash
# Model-check the JDBC poll-ahead design (BDX-645) with TLC.
#
#   formal/pollinfo-poll-ahead/run.sh            # all checks
#   N=4 LIMIT=2 formal/pollinfo-poll-ahead/run.sh  # larger bound (about 30 s)
#
# Each check states what it expects. The script exits 0 only when every
# check matches its expectation, including the broken variants, which must
# fail on the rule they target. Needs Java 11+ and tla2tools.jar v1.7.4.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
JAR=${TLA2TOOLS:-}
if [ -z "$JAR" ] && [ -f "$HOME/.local/share/mise/installs/http-tla2tools/1.7.4/tla2tools.jar" ]; then
  JAR="$HOME/.local/share/mise/installs/http-tla2tools/1.7.4/tla2tools.jar"
fi
if [ -z "$JAR" ] || [ ! -f "$JAR" ]; then
  echo "tla2tools.jar not found. Download v1.7.4 from https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4 and set TLA2TOOLS=/path/to/tla2tools.jar" >&2
  exit 2
fi

N=${N:-3}
LIMIT=${LIMIT:-2}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
cp "$HERE/PollAhead.tla" "$WORK/"

# The rules before the slot-release change (4b831553c): no rule says when a finished stream's slot is freed.
OLD_RULES="TypeOK InlineMeansPoller EachEndpointOnce ContinuationsOnce NoPrematureEnd FailFast CancelStopsPolling Backpressure NoSpin OpenWithinLimit"
SAFE="TypeOK InlineMeansPoller EachEndpointOnce ContinuationsOnce NoPrematureEnd FailFast CancelStopsPolling Backpressure NoSpin SlotFreedOnExhaust FailedQueueInert"
ALL="$SAFE OpenWithinLimit"
unexpected=0

# check NAME EAGER_OPEN IDLE_FIX MUTATION "INVARIANTS" LIVENESS(yes|no) EXPECT(pass|invariant:Name|deadlock|liveness) NOTE
check() {
  local name=$1 eager=$2 idlefix=$3 mutation=$4 invariants=$5 liveness=$6 expect=$7 note=$8
  local cfg="$WORK/$name.cfg" out="$WORK/$name.out" got
  {
    echo "SPECIFICATION Spec"
    echo "CONSTANTS N = $N  Limit = $LIMIT  EagerOpen = $eager  IdleFix = $idlefix  Mutation = \"$mutation\""
    for inv in $invariants; do echo "INVARIANT $inv"; done
    if [ "$liveness" = yes ]; then echo "PROPERTY Terminates"; fi
  } > "$cfg"
  (cd "$WORK" && java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -workers auto \
      -metadir "$WORK/meta-$name" -config "$cfg" PollAhead.tla) > "$out" 2>&1 || true
  if grep -q "No error has been found" "$out"; then got=pass
  elif inv=$(grep -o "Invariant [A-Za-z]* is violated" "$out" | head -1 | awk '{print $2}') && [ -n "$inv" ]; then got="invariant:$inv"
  elif grep -q "Deadlock reached" "$out"; then got=deadlock
  elif grep -q "Temporal properties were violated" "$out"; then got=liveness
  else got=error
  fi
  local states
  states=$(grep -o "[0-9,]* distinct states found" "$out" | tail -1 | awk '{print $1}')
  if [ "$got" = "$expect" ]; then
    printf 'ok    %-26s %-28s %s (%s distinct states)\n' "$name" "$got" "$note" "${states:-?}"
  else
    printf 'FAIL  %-26s expected %-19s got %s\n' "$name" "$expect" "$got"
    sed -n '/^Error:/,$p' "$out" | head -80
    unexpected=1
  fi
}

echo "PollAhead model check  N=$N  Limit=$LIMIT  ($(java -cp "$JAR" tlc2.TLC 2>&1 | head -1))"
echo
echo "Design checks (IdleFix = TRUE is the code: awaitNext clears idleTried after every consumed future)"
echo "(every check starts the poller either accepted by the pool or rejected, which leaves it orphaned)"
check proposal-as-written     TRUE  TRUE  none "$SAFE"          yes pass                      "streams open as soon as endpoints are enqueued"
check finding-open-streams    TRUE  TRUE  none "OpenWithinLimit" no invariant:OpenWithinLimit "KNOWN FINDING: one answer can add more streams than the limit"
check deferred-open           FALSE TRUE  none "$ALL"           yes pass                      "the code as written: every safety rule and Terminates hold"
echo
echo "Broken variants (each must fail on the rule it targets; the idle fix is on, so only the variant is at fault,"
echo "except staleIdle, which turns the idle fix off: the bug fixed in dadd45d8a)"
check endWhenQueueEmpty       FALSE TRUE  endWhenQueueEmpty   "$ALL" no invariant:NoPrematureEnd     "next() ends when the queue is empty"
check endIgnoresOpening       FALSE TRUE  endIgnoresOpening   "$ALL" no invariant:NoPrematureEnd     "end test forgets streams still opening"
check finishBeforeEnqueue     FALSE TRUE  finishBeforeEnqueue "$ALL" no invariant:NoPrematureEnd     "moreExpected cleared before the final endpoints are queued"
check enqueueWholeList        FALSE TRUE  enqueueWholeList    "$ALL" no invariant:EachEndpointOnce   "cumulative list enqueued, not the suffix"
check reuseContinuation       FALSE TRUE  reuseContinuation   "$ALL" no invariant:ContinuationsOnce  "old continuation kept after empty answer"
check readAfterFail           FALSE TRUE  readAfterFail       "$ALL" no invariant:FailFast           "reader ignores the failure flag"
check cancelIgnoresPoller     FALSE TRUE  cancelIgnoresPoller "$ALL" no invariant:CancelStopsPolling "cancel does not stop the poller"
check noLimit                 FALSE TRUE  noLimit             "$ALL" no invariant:Backpressure       "poller ignores the limit"
# The poller never has two batches reserved at once (it stops polling at the limit), so the
# reader's and the poller's reservations only collide from 4 endpoints on.
if [ "$N" -ge 4 ]; then OPEN_NOT_RESERVED=invariant:OpenWithinLimit; else OPEN_NOT_RESERVED=pass; fi
check openNotReserved         FALSE TRUE  openNotReserved     "$ALL" no "$OPEN_NOT_RESERVED"         "a stream takes its slot only once opened, not when reserved (needs N>=4)"
check noBackoff               FALSE TRUE  noBackoff           "$ALL" no invariant:NoSpin             "no wait after an empty answer"
check noWake                  FALSE TRUE  noWake              "$ALL" yes liveness                    "reader never resumes a paused poller"
check pauseNotAtomic          FALSE TRUE  pauseNotAtomic      "$ALL" yes liveness                    "limit check and pause in separate steps"
check ownedClearedLate        FALSE TRUE  ownedClearedLate    "$ALL" yes liveness                    "pause clears owned after the lock: resume finds it set"
check noIdleHook              FALSE TRUE  noIdleHook          "$ALL" yes liveness                    "an orphaned poller is never run inline"
check staleIdle               FALSE FALSE none                "$SAFE" yes liveness                   "idleTried not cleared after a consumed future (rejected resume strands next())"
check lateRelease             FALSE TRUE  lateRelease         "$ALL" no invariant:SlotFreedOnExhaust "slot freed only when a later awaitNext consumes the null future, not when the application finishes the stream"
check applyAfterFail          FALSE TRUE  applyAfterFail      "$ALL" no invariant:FailedQueueInert   "addEndpoints queues and reserves on a queue that already failed"
check errorNotReported        FALSE TRUE  errorNotReported    "$ALL" yes liveness                    "an Error kills the poller thread without failing the queue (next() waits forever)"
check rejectedStartUnused     FALSE TRUE  rejectedStartUnused "$ALL" yes liveness                    "a rejected poller start leaves the poller unused instead of orphaned (next() waits forever)"
echo
echo "Gap check: lateRelease passes every rule that existed before SlotFreedOnExhaust. The late release is a progress"
echo "problem (a finished stream blocks the poller), not a safety or termination one, and only the new rule sees it."
check lateRelease-old-rules   FALSE TRUE  lateRelease         "$OLD_RULES" yes pass                  "NOT a bug in the model: shows what the old rule set could not express"

echo
if [ "$unexpected" -eq 0 ]; then echo "All checks matched their expectations."; else echo "Some checks did not match. See above."; fi
exit "$unexpected"
