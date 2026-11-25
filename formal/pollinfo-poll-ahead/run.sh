#!/usr/bin/env bash
# Model-check the JDBC poll-ahead design (BDX-645) with TLC.
#
#   formal/pollinfo-poll-ahead/run.sh            # all checks
#   N=4 LIMIT=2 formal/pollinfo-poll-ahead/run.sh  # larger bound (slower)
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

SAFE="TypeOK EachEndpointOnce ContinuationsOnce NoPrematureEnd FailFast CancelStopsPolling Backpressure NoSpin"
ALL="$SAFE OpenWithinLimit"
unexpected=0

# check NAME EAGER_OPEN MUTATION "INVARIANTS" LIVENESS(yes|no) EXPECT(pass|invariant:Name|deadlock|liveness) NOTE
check() {
  local name=$1 eager=$2 mutation=$3 invariants=$4 liveness=$5 expect=$6 note=$7
  local cfg="$WORK/$name.cfg" out="$WORK/$name.out" got
  {
    echo "SPECIFICATION Spec"
    echo "CONSTANTS N = $N  Limit = $LIMIT  EagerOpen = $eager  Mutation = \"$mutation\""
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
echo "Design checks"
check proposal-as-written     TRUE  none "$SAFE"          yes pass                      "streams open as soon as endpoints are enqueued"
check finding-open-streams    TRUE  none "OpenWithinLimit" no invariant:OpenWithinLimit "KNOWN FINDING: one answer can add more streams than the limit"
check deferred-open           FALSE none "$ALL"           yes pass                      "streams open only while open < limit"
echo
echo "Broken variants (each must fail on the rule it targets)"
check endWhenQueueEmpty       FALSE endWhenQueueEmpty   "$ALL" no invariant:NoPrematureEnd     "next() ends when the queue is empty"
check finishBeforeEnqueue     FALSE finishBeforeEnqueue "$ALL" no invariant:NoPrematureEnd     "poller marked finished before enqueueing"
check enqueueWholeList        FALSE enqueueWholeList    "$ALL" no invariant:EachEndpointOnce   "cumulative list enqueued, not the suffix"
check reuseContinuation       FALSE reuseContinuation   "$ALL" no invariant:ContinuationsOnce  "old continuation kept after empty answer"
check readAfterFail           FALSE readAfterFail       "$ALL" no invariant:FailFast           "reader ignores the failure flag"
check cancelIgnoresPoller     FALSE cancelIgnoresPoller "$ALL" no invariant:CancelStopsPolling "cancel does not stop the poller"
check noLimit                 FALSE noLimit             "$ALL" no invariant:Backpressure       "poller ignores the limit"
check noBackoff               FALSE noBackoff           "$ALL" no invariant:NoSpin             "no wait after an empty answer"
check noWake                  FALSE noWake              "$ALL" yes liveness                    "reader never wakes a paused poller"
check pauseNotAtomic          FALSE pauseNotAtomic      "$ALL" yes liveness                    "limit check and pause in separate steps"

echo
if [ "$unexpected" -eq 0 ]; then echo "All checks matched their expectations."; else echo "Some checks did not match. See above."; fi
exit "$unexpected"
