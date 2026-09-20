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

# The laws of the buffer reference-counting model. The human states
# them; PROOF.bend must prove them. Each law quotes the Javadoc or
# Preconditions message in arrow-java's memory-core it comes from, or is
# a sanity check that keeps the others from being satisfied vacuously.
#
# Laws are stated over arbitrary allocator states, so the ones that need
# it take the invariant Inv as a hypothesis; inv_start and inv_kept show
# every reachable state satisfies Inv.

import Base
import ./main.bend as M

# the truth of a Bool as a type: Unit when True, Empty when False
def T(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

# a conjunction of two copyable facts (A & B is Type-kinded, so a proof
# of it could be used only once)
type And<-A: Data, -B: Data> is Data:
  Conj{l: A, r: B}

# Invariant
# ---------

# an allocation's freed flag agrees with its reference count:
# "If the reference count drops to 0, it implies that ArrowBufs managed
# by this reference manager no longer need access to the underlying
# memory" (ReferenceManager.release), and the chunk is released exactly
# then (AllocationManager.release: release0())
def AllocOk(a: M.Alloc) -> Data:
  M.Alloc{id, size, refs, freed} = a
  {freed == Nat.is_eq(refs, 0n) : Bool}

def Consistent(xs: List<&2, M.Alloc>) -> Data:
  match xs:
    case Nil{}:
      Unit
    case Con{a, t}:
      And<AllocOk(a), Consistent(t)>

# the accountant's counter is the sum of the live allocations' sizes
# (BaseAllocator.verifyAllocator: "bufferTotal + reservedTotal + childTotal
# != getAllocatedMemory()" is the failure it looks for), and every
# allocation is consistent
def Inv(allocs: List<&2, M.Alloc>, allocated: Nat) -> Data:
  And<{allocated == M.live_bytes(allocs) : Nat}, Consistent(allocs)>

def Inv_s(s: M.Allocator) -> Data:
  M.Allocator{allocs, allocated, limit, next, closed} = s
  Inv(allocs, allocated)

# LAW (accounting, start): a fresh RootAllocator accounts for nothing
law inv_start:
  for limit : Nat
  Inv_s(M.start(limit))

# LAW (accounting, step): every request keeps the allocated-bytes counter
# equal to the live bytes and every freed flag equal to "refcount is 0".
# This is law 3 (the counter) and law 2 (freed exactly at zero) of the
# spec, as one inductive invariant.
law inv_kept:
  for  r         : M.Req
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  for  closed    : Bool
  for  w         : Inv(allocs, allocated)
  Inv_s(M.state(M.step(r, allocs, allocated, limit, next, closed)))

# Release
# -------

# LAW: "ref count decrement should be greater than or equal to 1"
# (BufferLedger.release: Preconditions.checkState). release(0) is an
# error and changes nothing.
law release_zero_is_error:
  for +id        : Nat
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  {M.step(M.RRelease{id, 0n}, allocs, allocated, limit, next, False{}) == (M.Allocator{allocs, allocated, limit, next, False{}}, M.RErr{}) : M.Allocator & M.Resp}

# LAW: "RefCnt has gone negative" (BufferLedger.release:
# Preconditions.checkState(refCnt >= 0)). Releasing more references than
# are held is an error and leaves the state unchanged; an unknown id
# holds 0 references, so this covers it too.
law release_below_zero_is_error:
  for +id        : Nat
  for +n         : Nat
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  for  lt        : T(Nat.is_lt(M.refs_of(id, allocs), n))
  {M.step(M.RRelease{id, n}, allocs, allocated, limit, next, False{}) == (M.Allocator{allocs, allocated, limit, next, False{}}, M.RErr{}) : M.Allocator & M.Resp}

# LAW: a release on a buffer whose memory is already released, or on an
# unknown buffer, is an error and not a silent no-op: memory is never
# freed twice. Needs the invariant (freed means the count is 0).
law release_dead_is_error:
  for +id        : Nat
  for +n         : Nat
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  for  w         : Inv(allocs, allocated)
  for  d         : T(Bool.not(M.is_live(id, allocs)))
  {M.step(M.RRelease{id, n}, allocs, allocated, limit, next, False{}) == (M.Allocator{allocs, allocated, limit, next, False{}}, M.RErr{}) : M.Allocator & M.Resp}

# LAW: "@return true if ref count has dropped to 0" (ReferenceManager.release),
# and then "oldAllocator.releaseBytes(getSize()); release0()"
# (AllocationManager.release). Releasing exactly the references held
# answers true, marks the memory freed and takes its size off the counter.
law release_to_zero_frees:
  for +id        : Nat
  for +m         : Nat
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  for  e         : {M.refs_of(id, allocs) == 1n+m : Nat}
  after = M.state(M.step(M.RRelease{id, 1n+m}, allocs, allocated, limit, next, False{}))
  {M.resp(M.step(M.RRelease{id, 1n+m}, allocs, allocated, limit, next, False{})) == M.RFreed{} : M.Resp}
    & T(M.is_freed(id, M.allocs_of(after)))
    & {M.allocated_of(after) == Nat.sub(allocated, M.size_of(id, allocs)) : Nat}

# LAW: "@return ... false otherwise" (ReferenceManager.release). Releasing
# fewer references than are held answers false, keeps the buffer live,
# lowers its count by exactly n and leaves the accountant alone. Needs
# the invariant: a positive count means the memory is held.
law release_keeps_live:
  for +id        : Nat
  for +m         : Nat
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  for  w         : Inv(allocs, allocated)
  for  lt        : T(Nat.is_lt(1n+m, M.refs_of(id, allocs)))
  after = M.state(M.step(M.RRelease{id, 1n+m}, allocs, allocated, limit, next, False{}))
  {M.resp(M.step(M.RRelease{id, 1n+m}, allocs, allocated, limit, next, False{})) == M.ROk{} : M.Resp}
    & T(M.is_live(id, M.allocs_of(after)))
    & {M.refs_of(id, M.allocs_of(after)) == Nat.sub(M.refs_of(id, allocs), 1n+m) : Nat}
    & {M.allocated_of(after) == allocated : Nat}

# Retain
# ------

# LAW: "retain(%s) argument is not positive" (BufferLedger.retain:
# Preconditions.checkArgument(increment > 0)). retain(0) is an error and
# changes nothing.
law retain_zero_is_error:
  for +id        : Nat
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  {M.step(M.RRetain{id, 0n}, allocs, allocated, limit, next, False{}) == (M.Allocator{allocs, allocated, limit, next, False{}}, M.RErr{}) : M.Allocator & M.Resp}

# LAW: "Preconditions.checkArgument(originalReferenceCount > 0)"
# (BufferLedger.retain). A retain on a buffer whose memory is released,
# or on an unknown buffer, is an error and does not revive it.
law retain_dead_is_error:
  for +id        : Nat
  for +n         : Nat
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  for  w         : Inv(allocs, allocated)
  for  d         : T(Bool.not(M.is_live(id, allocs)))
  {M.step(M.RRetain{id, n}, allocs, allocated, limit, next, False{}) == (M.Allocator{allocs, allocated, limit, next, False{}}, M.RErr{}) : M.Allocator & M.Resp}

# LAW: "Increment this reference manager's reference count by a given
# amount for the associated underlying memory" (ReferenceManager.retain).
# On a live buffer, retain(n) adds exactly n, keeps the buffer live and
# does not touch the accountant.
law retain_live_adds:
  for +id        : Nat
  for +m         : Nat
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  for  w         : Inv(allocs, allocated)
  for  l         : T(M.is_live(id, allocs))
  after = M.state(M.step(M.RRetain{id, 1n+m}, allocs, allocated, limit, next, False{}))
  {M.resp(M.step(M.RRetain{id, 1n+m}, allocs, allocated, limit, next, False{})) == M.ROk{} : M.Resp}
    & T(M.is_live(id, M.allocs_of(after)))
    & {M.refs_of(id, M.allocs_of(after)) == Nat.add(M.refs_of(id, allocs), 1n+m) : Nat}
    & {M.allocated_of(after) == allocated : Nat}

# Allocation
# ----------

# LAW: "Unable to allocate buffer of size %d due to memory limit"
# (BaseAllocator.buffer throws OutOfMemoryException when
# Accountant.allocate finds newLocal > allocationLimit). "Either
# completely succeeds or completely fails. If it fails, no changes are
# made to accounting." (Accountant.allocateBytes)
law alloc_over_limit_rejected:
  for +p         : Nat
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  for  over      : T(Nat.is_lt(limit, Nat.add(allocated, 1n+p)))
  {M.step(M.RAlloc{1n+p}, allocs, allocated, limit, next, False{}) == (M.Allocator{allocs, allocated, limit, next, False{}}, M.ROom{}) : M.Allocator & M.Resp}

# LAW (anti-vacuity): a request that fits is served: a new buffer with
# "a ref count of 1" (BufferLedger.retain Javadoc; associate(this) in
# bufferWithoutReservation), and the counter grows by its size.
law alloc_within_limit_served:
  for +p         : Nat
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  for  fits      : T(Nat.is_le(Nat.add(allocated, 1n+p), limit))
  after = M.state(M.step(M.RAlloc{1n+p}, allocs, allocated, limit, next, False{}))
  {M.resp(M.step(M.RAlloc{1n+p}, allocs, allocated, limit, next, False{})) == M.RBuf{next} : M.Resp}
    & {M.refs_of(next, M.allocs_of(after)) == 1n : Nat}
    & {M.allocated_of(after) == Nat.add(allocated, 1n+p) : Nat}

# LAW: "if (initialRequestSize == 0) return getEmpty();" (BaseAllocator.buffer).
# buffer(0) is the shared empty buffer and accounts nothing.
law alloc_zero_is_empty:
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  {M.step(M.RAlloc{0n}, allocs, allocated, limit, next, False{}) == (M.Allocator{allocs, allocated, limit, next, False{}}, M.REmpty{}) : M.Allocator & M.Resp}

# Close
# -----

# LAW: "Allocator[%s] closed with outstanding buffers allocated" /
# "Memory was leaked by query" (BaseAllocator.close). Closing while any
# buffer is live is an error.
law close_with_live_buffer_is_error:
  for +id        : Nat
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  for  l         : T(M.is_live(id, allocs))
  {M.resp(M.step(M.RClose{}, allocs, allocated, limit, next, False{})) == M.RErr{} : M.Resp}

# LAW: with no outstanding buffer, close succeeds and the allocator is closed.
law close_clean_succeeds:
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  for  c         : {False{} == M.has_live(allocs) : Bool}
  {M.step(M.RClose{}, allocs, allocated, limit, next, False{}) == (M.Allocator{allocs, allocated, limit, next, True{}}, M.ROk{}) : M.Allocator & M.Resp}

# LAW: the two leak checks agree. BaseAllocator.close tests
# getAllocatedMemory() > 0 always and the outstanding-ledger count in
# DEBUG mode; under the invariant, no live buffer means no accounted bytes.
law clean_means_zero_bytes:
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for  w         : Inv(allocs, allocated)
  for  c         : {False{} == M.has_live(allocs) : Bool}
  {allocated == 0n : Nat}

# LAW: "Attempting operation on allocator when allocator is closed"
# (BaseAllocator.assertOpen). After close, buffer(), retain() and
# release() fail and change nothing.
law closed_rejects:
  for  r         : M.Req
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  for  nc        : {False{} == M.is_close(r) : Bool}
  {M.step(r, allocs, allocated, limit, next, True{}) == (M.Allocator{allocs, allocated, limit, next, True{}}, M.RErr{}) : M.Allocator & M.Resp}

# LAW: "Some owners may close more than once" (BaseAllocator.close).
# A second close is a no-op.
law close_twice_is_noop:
  for +allocs    : List<&2, M.Alloc>
  for +allocated : Nat
  for +limit     : Nat
  for +next      : Nat
  {M.step(M.RClose{}, allocs, allocated, limit, next, True{}) == (M.Allocator{allocs, allocated, limit, next, True{}}, M.ROk{}) : M.Allocator & M.Resp}

# Traces (anti-vacuity)
# ---------------------

# LAW: buffer(size); release() frees the memory and the allocator closes cleanly
# (TestBaseAllocator.testRootAllocator_createChildAndUse).
law trace_alloc_release_frees:
  for +p     : Nat
  for +limit : Nat
  for  fits  : T(Nat.is_le(1n+p, limit))
  run = M.replay([M.RAlloc{1n+p}, M.RRelease{0n, 1n}, M.RClose{}], limit)
  {M.resp(run) == M.ROk{} : M.Resp} & {M.allocated_of(M.state(run)) == 0n : Nat}

# LAW: buffer(size); retain(); release() keeps the buffer live with one
# reference, and close then reports the leak
# (TestBaseAllocator.testRootAllocator_closeWithOutstanding).
law trace_retain_keeps_live:
  for +p     : Nat
  for +limit : Nat
  for  fits  : T(Nat.is_le(1n+p, limit))
  run = M.replay([M.RAlloc{1n+p}, M.RRetain{0n, 1n}, M.RRelease{0n, 1n}, M.RClose{}], limit)
  {M.resp(run) == M.RErr{} : M.Resp}
    & {M.refs_of(0n, M.allocs_of(M.state(run))) == 1n : Nat}
    & {M.allocated_of(M.state(run)) == 1n+p : Nat}

# LAW: buffer(size); release(); release() is an error: memory is never freed twice.
law trace_double_release_is_error:
  for +p     : Nat
  for +limit : Nat
  for  fits  : T(Nat.is_le(1n+p, limit))
  {M.resp(M.replay([M.RAlloc{1n+p}, M.RRelease{0n, 1n}, M.RRelease{0n, 1n}], limit)) == M.RErr{} : M.Resp}

# Not modelled
# ------------

# NOT PROVEN: ownership transfer between two allocators.
# "Transfer the memory accounting ownership of this ArrowBuf to another
# allocator ... Transfers will always succeed, even if that puts the
# other allocator into an overlimit situation." (BufferLedger.transferOwnership)
# The law would be: the sum of allocated bytes over both allocators is
# unchanged by a transfer. This model has one allocator and one ledger
# per allocation; a faithful transfer needs the AllocationManager's map
# from allocators to ledgers, an owning ledger per chunk, and
# forceAllocate/releaseBytes on two accountants. That is a second model,
# not a hypothesis on this one, so it is left as follow-up work.
#
# law transfer_preserves_total:
#   for src : M.Allocator
#   for dst : M.Allocator
#   for +id : Nat
#   {total(transfer(id, src, dst)) == Nat.add(allocated_of(src), allocated_of(dst)) : Nat}

# NOT EXPRESSIBLE: thread safety of the reference count.
# "Operations within the context of a single BufferLedger are lockless in
# nature and can be leveraged by multiple threads." (AllocationManager)
# BufferLedger uses AtomicIntegerFieldUpdater and synchronized
# (allocationManager). Bend is pure and its model of a step is a
# function, so there is no interleaving to quantify over; stating
# linearizability would need an explicit scheduler model, which is not
# what LAWS.bend checks.
#
# law release_is_atomic: ...

# NOT EXPRESSIBLE: 64-bit overflow of the accountant.
# Accountant.allocate detects "overflow = ((oldLocal ^ newLocal) & (size ^
# newLocal)) < 0" on a long. Bend has Nat, U32 and F32 only; sizes here
# are unbounded Nat, so the overflow branch has no model.
#
# law alloc_overflow_rejected: ...

# NOT MODELLED: bounds checking of reads and writes.
# "Will throw an exception if the memory is not readable" (ArrowBuf.checkBytes).
# Index arithmetic against capacity is expressible in Bend, but this model
# has no reader/writer indexes or capacity per ArrowBuf (slices), only the
# chunk size; the check belongs to a model of ArrowBuf views.
#
# law read_within_capacity: ...
