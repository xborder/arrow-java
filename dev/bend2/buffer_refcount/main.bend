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

# A model of reference counting and allocator accounting in the
# arrow-java memory module (ArrowBuf, ReferenceManager / BufferLedger,
# AllocationManager, BufferAllocator / BaseAllocator / Accountant),
# written in Bend 2 so that its laws can be stated and proven.
#
# One allocator owns a list of allocations. An allocation is one
# AllocationManager with its owning BufferLedger: an id (the ArrowBuf
# handle), the chunk size, the ledger's reference count and whether the
# chunk has been released (release0 was called). The allocator keeps the
# Accountant's counters: allocated bytes, the limit, and whether it has
# been closed. Sizes and counts are Nat; Java's long/int are unbounded
# here.
#
#   bend main.bend         # checks the model and runs the sample trace in main
#   bend PROOF.bend        # checks LAWS.bend against this model

import Base

# Requests and responses
# ----------------------

# one call against the allocator or one of its buffers
type Req is Data:
  RAlloc{size: Nat}           # BufferAllocator.buffer(size)
  RRetain{id: Nat, n: Nat}    # buf.getReferenceManager().retain(n)
  RRelease{id: Nat, n: Nat}   # buf.getReferenceManager().release(n); release() is n = 1
  RClose{}                    # BufferAllocator.close()

# the outcome of a call
type Resp is Data:
  RBuf{id: Nat}     # buffer(size): a new ArrowBuf, named by its allocation id
  REmpty{}          # buffer(0): the shared empty buffer, not accounted
  ROk{}             # retain(n), release(n) that keeps the memory (false), close()
  RFreed{}          # release(n) that dropped the count to 0 (true): memory released
  ROom{}            # OutOfMemoryException: the request exceeds the limit
  RErr{}            # IllegalStateException / IllegalArgumentException

# State
# -----

# one AllocationManager and its owning BufferLedger
type Alloc is Data:
  Alloc{id: Nat, size: Nat, refs: Nat, freed: Bool}

# one BaseAllocator: its allocations and its Accountant
type Allocator is Data:
  Allocator{allocs: List<&2, Alloc>, allocated: Nat, limit: Nat, next: Nat, closed: Bool}

def Alloc.id(a: Alloc) -> Nat:
  Alloc{id, size, refs, freed} = a
  id

def Alloc.size(a: Alloc) -> Nat:
  Alloc{id, size, refs, freed} = a
  size

def Alloc.refs(a: Alloc) -> Nat:
  Alloc{id, size, refs, freed} = a
  refs

def Alloc.freed(a: Alloc) -> Bool:
  Alloc{id, size, refs, freed} = a
  freed

# the bytes an allocation accounts for: its size while live, 0 once freed
def bytes.of(freed: Bool, size: Nat) -> Nat:
  match freed:
    case True{}:
      0n
    case False{}:
      size

def bytes(a: Alloc) -> Nat:
  Alloc{id, size, refs, freed} = a
  bytes.of(freed, size)

# the bytes the allocator should be accounting for
def live_bytes(xs: List<&2, Alloc>) -> Nat:
  match xs:
    case Nil{}:
      0n
    case Con{a, t}:
      Nat.add(bytes(a), live_bytes(t))

# whether any allocation is still live (an outstanding buffer)
def has_live(xs: List<&2, Alloc>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{a, t}:
      Bool.not(Alloc.freed(a)) || has_live(t)

# List helpers
# ------------

# the first allocation with the given id
def lookup.at(a: Alloc, rec: Maybe<&2, Alloc>, hit: Bool) -> Maybe<&2, Alloc>:
  match hit:
    case True{}:
      Some{a}
    case False{}:
      rec

def lookup(+id: Nat, xs: List<&2, Alloc>) -> Maybe<&2, Alloc>:
  match xs:
    case Nil{}:
      None{}
    case Con{+a, t}:
      lookup.at(a, lookup(id, t), Nat.is_eq(Alloc.id(a), id))

# xs with the first allocation of the given id replaced by a2
def replace.at(a: Alloc, a2: Alloc, t: List<&2, Alloc>, rec: List<&2, Alloc>, hit: Bool) -> List<&2, Alloc>:
  match hit:
    case True{}:
      a2 <> t
    case False{}:
      a <> rec

def replace(+id: Nat, +a2: Alloc, xs: List<&2, Alloc>) -> List<&2, Alloc>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{+a, +t}:
      replace.at(a, a2, t, replace(id, a2, t), Nat.is_eq(Alloc.id(a), id))

# Observations of a lookup
# ------------------------

def refs_of.m(m: Maybe<&2, Alloc>) -> Nat:
  match m:
    case None{}:
      0n
    case Some{a}:
      Alloc.refs(a)

def size_of.m(m: Maybe<&2, Alloc>) -> Nat:
  match m:
    case None{}:
      0n
    case Some{a}:
      Alloc.size(a)

def is_live.m(m: Maybe<&2, Alloc>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{a}:
      Bool.not(Alloc.freed(a))

def is_freed.m(m: Maybe<&2, Alloc>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{a}:
      Alloc.freed(a)

# the reference count of the buffer with the given id (0 if unknown)
def refs_of(+id: Nat, xs: List<&2, Alloc>) -> Nat:
  refs_of.m(lookup(id, xs))

# the size of the buffer with the given id (0 if unknown)
def size_of(+id: Nat, xs: List<&2, Alloc>) -> Nat:
  size_of.m(lookup(id, xs))

# whether the id names an allocation whose memory is still held
def is_live(+id: Nat, xs: List<&2, Alloc>) -> Bool:
  is_live.m(lookup(id, xs))

# whether the id names an allocation whose memory was released
def is_freed(+id: Nat, xs: List<&2, Alloc>) -> Bool:
  is_freed.m(lookup(id, xs))

# Transitions
# -----------

# the state with nothing changed
def same(+allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, closed: Bool) -> Allocator:
  Allocator{allocs, allocated, limit, next, closed}

# buffer(size), once the request is known to fit (Accountant.allocate:
# newLocal > allocationLimit fails): a new AllocationManager, its ledger
# associated with a count of 1, and the bytes added to the accountant
def alloc.fits(+size: Nat, +allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, ok: Bool) -> Allocator & Resp:
  match ok:
    case True{}:
      (Allocator{Alloc{next, size, 1n, False{}} <> allocs, Nat.add(allocated, size), limit, 1n+next, False{}}, RBuf{next})
    case False{}:
      (same(allocs, allocated, limit, next, False{}), ROom{})

# buffer(size): size 0 answers the shared empty buffer and accounts nothing
def alloc(size: Nat, +allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Allocator & Resp:
  match size:
    case 0n:
      (same(allocs, allocated, limit, next, False{}), REmpty{})
    case 1n+p:
      +size2 = {1n+p : Nat}
      alloc.fits(size2, allocs, allocated, limit, next, Nat.is_le(Nat.add(allocated, size2), limit))

# retain(n) on a found ledger: checkArgument(originalReferenceCount > 0)
def retain.live(+id: Nat, +n: Nat, +size: Nat, refs: Nat, freed: Bool, +allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Allocator & Resp:
  match refs:
    case 0n:
      (same(allocs, allocated, limit, next, False{}), RErr{})
    case 1n+q:
      (Allocator{replace(id, Alloc{id, size, Nat.add(1n+q, n), freed}, allocs), allocated, limit, next, False{}}, ROk{})

def retain.found(+id: Nat, +n: Nat, m: Maybe<&2, Alloc>, +allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Allocator & Resp:
  match m:
    case None{}:
      (same(allocs, allocated, limit, next, False{}), RErr{})
    case Some{Alloc{aid, +size, refs, freed}}:
      retain.live(id, n, size, refs, freed, allocs, allocated, limit, next)

# retain(n): checkArgument(increment > 0), then bump the ledger's count
def retain(+id: Nat, n: Nat, +allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Allocator & Resp:
  match n:
    case 0n:
      (same(allocs, allocated, limit, next, False{}), RErr{})
    case 1n+m:
      retain.found(id, 1n+m, lookup(id, allocs), allocs, allocated, limit, next)

# release(n) after the decrement: a count of 0 releases the chunk
# (AllocationManager.release: releaseBytes(getSize()); release0()),
# any other count keeps it
def release.new(+id: Nat, +size: Nat, refs2: Nat, freed: Bool, +allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Allocator & Resp:
  match refs2:
    case 0n:
      (Allocator{replace(id, Alloc{id, size, 0n, True{}}, allocs), Nat.sub(allocated, size), limit, next, False{}}, RFreed{})
    case 1n+q:
      (Allocator{replace(id, Alloc{id, size, 1n+q, freed}, allocs), allocated, limit, next, False{}}, ROk{})

# release(n) on a found ledger: checkState(refCnt >= 0, "RefCnt has gone negative")
def release.checked(+id: Nat, +n: Nat, +size: Nat, +refs: Nat, freed: Bool, +allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, negative: Bool) -> Allocator & Resp:
  match negative:
    case True{}:
      (same(allocs, allocated, limit, next, False{}), RErr{})
    case False{}:
      release.new(id, size, Nat.sub(refs, n), freed, allocs, allocated, limit, next)

def release.found(+id: Nat, +n: Nat, m: Maybe<&2, Alloc>, +allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Allocator & Resp:
  match m:
    case None{}:
      (same(allocs, allocated, limit, next, False{}), RErr{})
    case Some{Alloc{aid, +size, +refs, freed}}:
      release.checked(id, n, size, refs, freed, allocs, allocated, limit, next, Nat.is_lt(refs, n))

# release(n): checkState(decrement >= 1), then decrement the ledger's count
def release(+id: Nat, n: Nat, +allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Allocator & Resp:
  match n:
    case 0n:
      (same(allocs, allocated, limit, next, False{}), RErr{})
    case 1n+m:
      release.found(id, 1n+m, lookup(id, allocs), allocs, allocated, limit, next)

# close(): the allocator is closed either way (isClosed = true comes
# first), and outstanding buffers make it an error ("Memory was leaked")
def close(+allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, leak: Bool) -> Allocator & Resp:
  match leak:
    case True{}:
      (same(allocs, allocated, limit, next, True{}), RErr{})
    case False{}:
      (same(allocs, allocated, limit, next, True{}), ROk{})

# a request against an open allocator
def step_open(r: Req, +allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Allocator & Resp:
  match r:
    case RAlloc{size}:
      alloc(size, allocs, allocated, limit, next)
    case RRetain{+id, n}:
      retain(id, n, allocs, allocated, limit, next)
    case RRelease{+id, n}:
      release(id, n, allocs, allocated, limit, next)
    case RClose{}:
      close(allocs, allocated, limit, next, has_live(allocs))

# a request against a closed allocator: assertOpen() throws, except
# that a second close() is a no-op
def step_closed(r: Req, +allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat) -> Allocator & Resp:
  match r:
    case RAlloc{size}:
      (same(allocs, allocated, limit, next, True{}), RErr{})
    case RRetain{id, n}:
      (same(allocs, allocated, limit, next, True{}), RErr{})
    case RRelease{id, n}:
      (same(allocs, allocated, limit, next, True{}), RErr{})
    case RClose{}:
      (same(allocs, allocated, limit, next, True{}), ROk{})

# whether a request is close()
def is_close(r: Req) -> Bool:
  match r:
    case RAlloc{size}:
      False{}
    case RRetain{id, n}:
      False{}
    case RRelease{id, n}:
      False{}
    case RClose{}:
      True{}

# one request against one state
def step(r: Req, +allocs: List<&2, Alloc>, +allocated: Nat, +limit: Nat, +next: Nat, closed: Bool) -> Allocator & Resp:
  match closed:
    case True{}:
      step_closed(r, allocs, allocated, limit, next)
    case False{}:
      step_open(r, allocs, allocated, limit, next)

# projections of a step
def state(sr: Allocator & Resp) -> Allocator:
  (s, r) = sr
  s

def resp(sr: Allocator & Resp) -> Resp:
  (s, r) = sr
  r

def allocs_of(s: Allocator) -> List<&2, Alloc>:
  Allocator{allocs, allocated, limit, next, closed} = s
  allocs

def allocated_of(s: Allocator) -> Nat:
  Allocator{allocs, allocated, limit, next, closed} = s
  allocated

def closed_of(s: Allocator) -> Bool:
  Allocator{allocs, allocated, limit, next, closed} = s
  closed

# a request against a whole allocator
def step_s(r: Req, s: Allocator) -> Allocator & Resp:
  Allocator{allocs, allocated, limit, next, closed} = s
  step(r, allocs, allocated, limit, next, closed)

# new RootAllocator(limit)
def start(limit: Nat) -> Allocator:
  Allocator{Nil{}, 0n, limit, 0n, False{}}

# a whole trace, threading the state and the last response
def run(reqs: List<Req>, sr: Allocator & Resp) -> Allocator & Resp:
  match reqs:
    case Nil{}:
      sr
    case Con{r, rest}:
      (s, last) = sr
      run(rest, step_s(r, s))

def replay(reqs: List<Req>, limit: Nat) -> Allocator & Resp:
  run(reqs, (start(limit), ROk{}))

# Show
# ----

def Req.show(r: Req) -> String:
  match r:
    case RAlloc{size}:
      "buffer(" ++ Nat.show(size) ++ ")"
    case RRetain{id, n}:
      "buf" ++ Nat.show(id) ++ ".retain(" ++ Nat.show(n) ++ ")"
    case RRelease{id, n}:
      "buf" ++ Nat.show(id) ++ ".release(" ++ Nat.show(n) ++ ")"
    case RClose{}:
      "close()"

def Resp.show(r: Resp) -> String:
  match r:
    case RBuf{id}:
      "buf" ++ Nat.show(id)
    case REmpty{}:
      "empty buffer"
    case ROk{}:
      "ok"
    case RFreed{}:
      "true (memory released)"
    case ROom{}:
      "OutOfMemoryException"
    case RErr{}:
      "IllegalStateException"

def Allocator.show(s: Allocator) -> String:
  Allocator{allocs, allocated, limit, next, closed} = s
  "allocated=" ++ Nat.show(allocated) ++ "/" ++ Nat.show(limit) ++ " live_bytes=" ++ Nat.show(live_bytes(allocs)) ++ " closed=" ++ Bool.show(closed)

# one line per request, then the final state
def trace.fin(r: Req, sr: Allocator & Resp, rec: Allocator -> String) -> String:
  (s, answer) = sr
  "  " ++ Req.show(r) ++ " -> " ++ Resp.show(answer) ++ "\n" ++ rec(s)

def trace(reqs: List<Req>) -> Allocator -> String:
  match reqs:
    case Nil{}:
      s => "  " ++ Allocator.show(s)
    case Con{+r, rest}:
      s => trace.fin(r, step_s(r, s), trace(rest))

# a RootAllocator of 1024 bytes: allocate, share, release twice, over-release,
# allocate past the limit, leak on close, then close cleanly
def main() -> IO(Unit):
  IO.print(trace([
    RAlloc{512n}, RRetain{0n, 1n}, RRelease{0n, 1n}, RRelease{0n, 1n}, RRelease{0n, 1n},
    RAlloc{768n}, RAlloc{256n}, RAlloc{768n}, RRelease{1n, 2n}, RClose{}, RRelease{1n, 1n}
  ])(start(1024n)))
