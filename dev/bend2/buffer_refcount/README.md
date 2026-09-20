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

# Bend 2 model of ArrowBuf reference counting and allocator accounting

A proof of concept that uses the Bend 2 language to state and enforce
the reference-counting and ownership protocol of buffers in the
`memory-core` module: `ArrowBuf`, `ReferenceManager` / `BufferLedger`,
`AllocationManager` and `BufferAllocator` / `BaseAllocator` /
`Accountant`. The protocol is written as a small executable model in
Bend, the rules the Javadoc and `Preconditions` messages state are
written as laws, and `bend PROOF.bend` refuses to pass until every law
is proven against the model.

Written against Bend 2.0.21, commit `6018e28` of
[bendlang/bend](https://github.com/bendlang/bend), on 2026-09-20. The
research notes on Bend 2 itself, and a first model of the Flight SQL
prepared-statement lifecycle, are on the `claude/bend2-arrow-integration-pgexut`
branch under `dev/bend2/`; this directory reuses its proof-kit idioms.

| File | Lines | Contents |
| --- | ---: | --- |
| `main.bend` | 419 | the model and a runnable trace (`main`) |
| `LAWS.bend` | 388 | 21 laws, plus 4 commented-out laws that are not proven or not expressible |
| `PROOF.bend` | 1001 | the proofs and the lemma library they need |

## 1. What is modelled

One `BaseAllocator` and the allocations it owns. Each allocation is one
`AllocationManager` together with its owning `BufferLedger`:

```python
type Alloc is Data:
  Alloc{id: Nat, size: Nat, refs: Nat, freed: Bool}

type Allocator is Data:
  Allocator{allocs: List<&2, Alloc>, allocated: Nat, limit: Nat, next: Nat, closed: Bool}
```

- `id` is the handle the `ArrowBuf` carries; `buffer(size)` issues them
  from the counter `next`.
- `size` is `AllocationManager.getSize()`, `refs` is
  `BufferLedger.getRefCount()`, and `freed` records that
  `AllocationManager.release0()` has run for the chunk.
- `allocated` is `Accountant.getAllocatedMemory()` (`locallyHeldMemory`),
  `limit` is `Accountant.getLimit()` (`allocationLimit`), and `closed` is
  `BaseAllocator.isClosed`.

Four requests are modelled, each with the checks the Java code makes,
in the order it makes them:

| Request | Java | Model |
| --- | --- | --- |
| `RAlloc{size}` | `BufferAllocator.buffer(size)` | `size == 0` answers the shared empty buffer (`REmpty`), else `allocated + size <= limit` or `OutOfMemoryException` (`ROom`); on success a new allocation with `refs = 1`, `allocated += size` |
| `RRetain{id, n}` | `buf.getReferenceManager().retain(n)` | `n >= 1` (`checkArgument(increment > 0)`), the ledger exists and `refs > 0` (`checkArgument(originalReferenceCount > 0)`), then `refs += n` |
| `RRelease{id, n}` | `buf.getReferenceManager().release(n)`; `ArrowBuf.close()` is `n = 1` | `n >= 1` (`checkState(decrement >= 1)`), the ledger exists and `refs >= n` (`checkState(refCnt >= 0, "RefCnt has gone negative")`), then `refs -= n`; at 0, `releaseBytes(getSize())` and `release0()` (`RFreed`), else `ROk` |
| `RClose{}` | `BufferAllocator.close()` | `isClosed = true` first; an outstanding buffer is the "Memory was leaked" `IllegalStateException` (`RErr`); a second `close()` is a no-op |

Every request against a closed allocator except `close()` fails, as
`assertOpen()` does with assertions enabled.

Sizes and counts are `Nat`, so Java's `long` and `int` are unbounded
here (see section 5). Not modelled: child allocators and reservations
(the `Accountant` parent chain), the rounding policy, slices and
reader/writer indexes, the multi-ledger side of `AllocationManager`
(`retain(ArrowBuf, BufferAllocator)`, `transferOwnership`), and the
`BufferManager` hook.

`main.bend` runs a trace on a 1024-byte root allocator and prints:

```
  buffer(512) -> buf0
  buf0.retain(1) -> ok
  buf0.release(1) -> ok
  buf0.release(1) -> true (memory released)
  buf0.release(1) -> IllegalStateException
  buffer(768) -> buf1
  buffer(256) -> buf2
  buffer(768) -> OutOfMemoryException
  buf1.release(2) -> IllegalStateException
  close() -> IllegalStateException
  buf1.release(1) -> IllegalStateException
  allocated=1024/1024 live_bytes=1024 closed=True
```

## 2. The laws

`LAWS.bend` states 21 laws. Laws hold for arbitrary allocator states;
the ones that need it take the invariant `Inv` as a hypothesis, and the
two invariant laws show every reachable state satisfies it. Each law
below quotes the sentence in `memory-core` it comes from.

### The invariant (laws 2 and 3 of the brief)

```python
def Inv(allocs, allocated) -> Data:
  And<{allocated == live_bytes(allocs) : Nat}, Consistent(allocs)>
```

`live_bytes` sums the sizes of the allocations whose `freed` flag is
false, and `Consistent` says that for every allocation
`freed == (refs == 0)`.

| Law | Source sentence |
| --- | --- |
| `inv_start` | a fresh `RootAllocator` accounts for nothing |
| `inv_kept` | `BaseAllocator.verifyAllocator`: it fails when `bufferTotal + reservedTotal + childTotal != getAllocatedMemory()`. `ReferenceManager.release`: "If the reference count drops to 0, it implies that ArrowBufs managed by this reference manager no longer need access to the underlying memory"; `AllocationManager.release` then calls `releaseBytes(getSize())` and `release0()`. Every request keeps the counter equal to the live bytes and the freed flag equal to "count is 0". |

### Release (law 1 and law 2)

| Law | Source sentence |
| --- | --- |
| `release_zero_is_error` | "ref count decrement should be greater than or equal to 1" (`BufferLedger.release`) |
| `release_below_zero_is_error` | "RefCnt has gone negative" (`BufferLedger.release`): releasing more than is held is an error and changes nothing; an unknown id holds 0 |
| `release_dead_is_error` | a release on a freed or unknown buffer is an error, not a silent no-op: memory is never freed twice (needs `Inv`) |
| `release_to_zero_frees` | "@return true if ref count has dropped to 0" (`ReferenceManager.release`), then `releaseBytes(getSize()); release0()`: the response is `true`, the id is freed afterwards, and the counter drops by the buffer's size |
| `release_keeps_live` | "@return ... false otherwise": the buffer stays live, its count drops by exactly `n`, the counter is untouched (needs `Inv`) |

### Retain

| Law | Source sentence |
| --- | --- |
| `retain_zero_is_error` | "retain(%s) argument is not positive" (`BufferLedger.retain`) |
| `retain_dead_is_error` | `Preconditions.checkArgument(originalReferenceCount > 0)` (`BufferLedger.retain`): a retain on a freed or unknown buffer is an error and does not revive it (needs `Inv`) |
| `retain_live_adds` | "Increment this reference manager's reference count by a given amount" (`ReferenceManager.retain`): exactly `n` is added, the buffer stays live, the counter is untouched (needs `Inv`) |

### Allocation (law 4 and law 6)

| Law | Source sentence |
| --- | --- |
| `alloc_over_limit_rejected` | "Unable to allocate buffer of size %d due to memory limit" (`BaseAllocator.buffer`); "Either completely succeeds or completely fails. If it fails, no changes are made to accounting." (`Accountant.allocateBytes`) |
| `alloc_within_limit_served` | anti-vacuity: a request that fits answers a new buffer with "a ref count of 1" (`BufferLedger` Javadoc, `associate(this)` in `bufferWithoutReservation`) and the counter grows by its size |
| `alloc_zero_is_empty` | `if (initialRequestSize == 0) return getEmpty();` (`BaseAllocator.buffer`) |

### Close (law 5)

| Law | Source sentence |
| --- | --- |
| `close_with_live_buffer_is_error` | "Allocator[%s] closed with outstanding buffers allocated" / "Memory was leaked by query" (`BaseAllocator.close`) |
| `close_clean_succeeds` | with no outstanding buffer, `close()` succeeds and the allocator is closed |
| `clean_means_zero_bytes` | the two leak checks in `BaseAllocator.close` (ledger count in DEBUG mode, `getAllocatedMemory() > 0` always) agree under `Inv` |
| `closed_rejects` | "Attempting operation on allocator when allocator is closed" (`BaseAllocator.assertOpen`): after close, `buffer`, `retain` and `release` fail and change nothing |
| `close_twice_is_noop` | "Some owners may close more than once" (`BaseAllocator.close`) |

### Traces (law 6, anti-vacuity)

| Law | Trace |
| --- | --- |
| `trace_alloc_release_frees` | `buffer(1+p); release(); close()` answers `ok` with 0 bytes accounted (`TestBaseAllocator.testRootAllocator_createChildAndUse`) |
| `trace_retain_keeps_live` | `buffer(1+p); retain(); release(); close()` leaves one reference and `1+p` bytes, and `close()` reports the leak (`testRootAllocator_closeWithOutstanding`) |
| `trace_double_release_is_error` | `buffer(1+p); release(); release()` is an error |

### Not proven or not expressible (kept as comments in `LAWS.bend`)

| Law | Status | Why |
| --- | --- | --- |
| `transfer_preserves_total` (law 7) | `NOT PROVEN` | `BufferLedger.transferOwnership`: "Transfers will always succeed, even if that puts the other allocator into an overlimit situation." The model has one allocator and one ledger per chunk. A faithful transfer needs `AllocationManager`'s map from allocators to ledgers, an owning ledger per chunk, and `forceAllocate`/`releaseBytes` on two accountants; that is a second model, not a hypothesis on this one. |
| `release_is_atomic` | `NOT EXPRESSIBLE` | `BufferLedger` uses `AtomicIntegerFieldUpdater` and `synchronized (allocationManager)`. Bend is pure; a step is a function, so there is no interleaving to quantify over without an explicit scheduler model. |
| `alloc_overflow_rejected` | `NOT EXPRESSIBLE` | `Accountant.allocate` detects `long` overflow with `((oldLocal ^ newLocal) & (size ^ newLocal)) < 0`. Bend has `Nat`, `U32` and `F32` only; sizes here are unbounded. |
| `read_within_capacity` | `NOT MODELLED` | `ArrowBuf.checkBytes` bounds checking is expressible, but this model has no per-buffer capacity or indexes (slices), only the chunk size. |

## 3. How to run

The installer host (`bend-lang.com`) and the docs sites were not
reachable from the sandbox, so the checker is run from a clone with
`bun`, which is what the installed binary wraps:

```sh
git clone --depth 1 https://github.com/bendlang/bend.git /tmp/bend
BEND="bun /tmp/bend/bend2/main.ts"
cd dev/bend2/buffer_refcount
$BEND PROOF.bend            # checks LAWS.bend against main.bend; prints "All terms check."
$BEND main.bend             # checks the model and runs the trace above
$BEND main.bend -o out.js   # JavaScript target; node out.js
$BEND main.bend -o out      # native binary via clang; ./out
```

## 4. Results

`bend PROOF.bend` prints `All terms check.` in 0.30 s wall time. The
JavaScript build is 26 KB and the native binary 1.1 MB; both run the
trace and print the output in section 1.

### Mutation tests

Six bugs were introduced into `main.bend` one at a time, the checker was
run, and the file restored (it is byte-identical to the committed
version). Every mutation is rejected. The table gives the first error
the checker reports; where that is a proof whose shape broke rather
than the law that became false, the false law is named too.

| # | Bug injected | First failure reported | Law that is false |
| --- | --- | --- | --- |
| M1 | release past zero answers `ok` instead of throwing (`release.checked`) | `rel_below.fin`: expected `ROk`, observed `RErr` | `release_below_zero_is_error` (and `release_dead_is_error`) |
| M2 | freeing forgets `releaseBytes` (counter not decremented) | `kept_release.new`: `allocated` vs `Nat.sub(allocated, size)` | `inv_kept` (accounting) and `release_to_zero_frees` |
| M3 | `retain` on a freed buffer revives it (no `originalReferenceCount > 0` check) | `kept_retain.live`: the new state is not `Inv` | `inv_kept` (bytes appear without the counter moving) and `retain_dead_is_error` |
| M4 | freeing does not mark the chunk released (`release0` forgotten) | `kept_release.new`: replacement shape differs | `inv_kept` (consistency: `False == is_eq(0n, 0n)`) and `release_to_zero_frees` |
| M5 | `buffer(size)` ignores the limit | `kept_alloc`: the proof passes the limit verdict the model no longer computes | `alloc_over_limit_rejected`; confirmed by adapting `kept_alloc` under the mutation, after which the checker stops at exactly that law (expected `RBuf`, observed `ROom`) |
| M6 | `close()` never reports a leak | `LAWS.close_with_live_buffer_is_error`: expected `ROk`, observed `RErr` | `close_with_live_buffer_is_error` |

M4 and M5 show the brittleness noted in the research notes: a mutation
that changes the shape of a state breaks the proofs that spell out that
shape before the checker reaches the law that is false. Both readings
block the build, which is what `LAWS.bend` promises; telling them apart
is left to the reader of the error.

### What the model says about the Java

Writing the laws against the code surfaced three details of
`BufferLedger` worth knowing. None is a bug in normal use, and the Java
was not changed.

1. `BufferLedger.retain(int)` does not call `allocator.assertOpen()`;
   `release` and `newArrowBuf` do. The model's `closed_rejects` law is
   therefore stricter than the code for `retain`: after a failed
   `close()` (which sets `isClosed` before throwing), Java still accepts
   `retain` on the leaked buffers, and rejects `release`.
2. `retain(int)` does `getAndAdd(increment)` and then
   `checkArgument(originalReferenceCount > 0)`, so a retain on a ledger
   whose count is 0 bumps the count and then throws. `release(int)`
   likewise does `addAndGet(-decrement)` and then
   `checkState(refCnt >= 0)`. The model states the intended contract,
   that a failed call leaves the state unchanged; the Java mutates and
   throws. The affected ledger is already dissociated from its
   `AllocationManager` (count 0 means `release(this)` ran), so the
   difference is not observable through a live buffer.
3. `BaseAllocator.close()` sets `isClosed = true` before it checks for
   leaks, so a leaked buffer can never be released once its allocator's
   `close()` has thrown (with assertions on). The model reproduces this
   (`close` marks the allocator closed either way), and the sample trace
   in `main` shows it.

## 5. Limitations

- Sizes and counts are `Nat`. The `long` overflow branch of
  `Accountant.allocate` and any `int` wraparound of `bufRefCnt` are
  outside the model.
- One allocator, one ledger per chunk. Ownership transfer, shared
  ownership through `retain(ArrowBuf, BufferAllocator)`, the owning-ledger
  hand-off in `AllocationManager.release`, child allocators and
  reservations are follow-up models.
- Proofs are hand-written: about 2.4 lines of proof per line of model,
  most of it a lemma library that Base does not ship (`Nat.add`
  commutativity and associativity, `(a + b) - b == a`, cancellation,
  `<`/`<=` facts, and the lookup/replace lemmas over the allocation
  list).
- The checker is `bend.ts`, not the Lean formalisation, and Bend 2 is
  days old; treat the result as specification work, not as a
  verification of `memory-core`.
