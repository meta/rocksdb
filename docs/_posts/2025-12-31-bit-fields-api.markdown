---
title: "BitFields API: Type-Safe Bit Packing for Lock-Free Data Structures"
layout: post
author: pdillinger
category: blog
---

Modern concurrent data structures increasingly rely on [atomic operations](https://en.cppreference.com/w/cpp/atomic/atomic) to avoid the overhead of locking. A valuable but under-utilized technique for maximizing the effectiveness of atomic operations is [bit packing](https://en.wikipedia.org/wiki/Bit_field)---fitting multiple logical fields into a single atomic variable for algorithmic simplicity and efficiency. However, language support for bit packing does not guarantee dense packing, and manually managing bit manipulation quickly becomes error-prone, especially when dealing with complex state machines.

To address this in RocksDB, we have developed a reusable **BitFields API**, a type-safe, zero-overhead abstraction for bit packing in C++. This works in conjunction with clean wrappers for `std::atomic` for powerful and relatively safe bit-packing of atomic data. For broader use, a [variant of the code](https://github.com/facebook/folly/pull/2549) has been proposed for adding to folly.

## The Problem: Managing Packed Bit Fields

Consider HyperClockCache, an essentially lock-free cache implementation in RocksDB, which was [refactored to use this BitFields API](https://github.com/meta/rocksdb/pull/14154). It is a hash table built on *slots* that can each hold a cache entry and relevant metadata. For atomic simplicity and efficiency, all the essential metadata for each slot is packed into a single 64-bit value:
- The reference count and eviction metadata are together encoded into *acquire* and *release* counters, 30 bits each.
- The possible states of {*empty*, *under construction/destruction*, *occupied+visible*, and *occupied+invisible*} are encoded into three state bits (instead of two, for easier decoding and manipulation).
- A *hit* bit is used for secondary cache integration.

Traditionally, you might write code like this:

```cpp
// Old approach: manual bit manipulation
constexpr uint64_t kAcquireCounterShift = 0;
constexpr uint64_t kReleaseCounterShift = 30;
constexpr uint64_t kCounterMask = 0x3FFFFFFF;
constexpr uint64_t kHitBitShift = 60;
constexpr uint64_t kOccupiedShift = 61;
constexpr uint64_t kShareableShift = 62;
constexpr uint64_t kVisibleShift = 63;
constexpr uint64_t kStateShift = kOccupiedShift;

std::atomic<uint64_t> meta_;

bool IsUnderConstruction(uint64_t meta) const {
    return (meta & (uint64_t{1} << kOccupiedShift)) && !(meta & (uint64_t{1} << kShareableShift));
}

// Getting fields
uint64_t meta = meta_.load(std::memory_order_acquire);
if (IsUnderConstruction(meta)) {
  // ...
} else if ((meta >> kVisibleShift) & 1) {
  uint32_t refcount =
      static_cast<uint32_t>(((meta >> kAcquireCounterShift) -
                             (meta >> kReleaseCounterShift)) & kCounterMask);
  // ...
}


// Setting fields

// Set the hit bit (relaxed)
meta_.fetch_or(uint64_t{1} << kHitBitShift, std::memory_order_relaxed);

// Set both counters to `new_count` (as in eviction processing)
uint64_t meta = meta_.load(std::memory_order_relaxed);
uint64_t new_meta =
    (meta & ((uint64_t{1} << kHitBitShift) | (uint64_t{7} << kStateShift))) |
    (new_count << kReleaseCounterShift) |
    (new_count << kAcquireCounterShift);
bool success = meta_.compare_exchange_strong(meta, new_meta,
                                             std::memory_order_acq_rel);

// Increment acquire counter by initial_countdown
old_meta = meta_.fetch_add((uint64_t{1} << kAcquireCounterShift) * initial_countdown,
                           std::memory_order_acq_rel);
```

This approach has several problems:
1. **Error-prone**: Easy to get masks and shifts wrong
2. **Maintenance burden**: Changes to field sizes require updating multiple constants
3. **Abstraction challenges**: Even if writing a full set of well-tested getters and setters to hide all the details, details can leak in to do things like update multiple fields in one non-CAS (compare-and-swap) atomic operation.

## New Solution: BitFields API

The BitFields API provides a declarative, type-safe way to define bit-packed structures. Here's how the same example looks with BitFields:

```cpp
// New approach: declarative bit fields. (Each field must reference the
// previous, so that the declaration machinery is simply stateless.)
struct SlotMeta : public BitFields<uint64_t, SlotMeta> {
  using AcquireCounter = UnsignedBitField<SlotMeta, 30, NoPrevBitField>;
  using ReleaseCounter = UnsignedBitField<SlotMeta, 30, AcquireCounter>;
  using HitFlag = BoolBitField<SlotMeta, ReleaseCounter>;
  using OccupiedFlag = BoolBitField<SlotMeta, HitFlag>;
  using ShareableFlag = BoolBitField<SlotMeta, OccupiedFlag>;
  using VisibleFlag = BoolBitField<SlotMeta, ShareableFlag>;

  // Convenience helpers
  bool IsUnderConstruction() const {
    return Get<OccupiedFlag>() && !Get<ShareableFlag>();
  }
};

BitFieldsAtomic<SlotMeta> meta_;

// Getting fields
SlotMeta state = meta_.Load();
if (state.IsUnderConstruction()) {
  // ...
} else if (state.Get<SlotMeta::VisibleFlag>()) {
  uint32_t refcount = state.Get<SlotMeta::AcquireCounter>() -
                      state.Get<SlotMeta::ReleaseCounter>();
  // ...
}

// Setting fields

// Set the hit bit (relaxed)
meta_.ApplyRelaxed(SlotMeta::HitFlag::SetTransform());

// Set both counters to `new_count` (as in eviction processing)
SlotMeta meta = meta_.LoadRelaxed();
SlotMeta new_meta = meta;
new_meta.Set<SlotMeta::ReleaseCounter>(new_count);
new_meta.Set<SlotMeta::AcquireCounter>(new_count);
meta_.CasStrongRelaxed(meta, new_meta);

// Increment acquire counter by initial_countdown
auto add_acquire =
    AcquireCounter::PlusTransformPromiseNoOverflow(initial_countdown);
meta_.Apply(add_acquire, &old_meta);

// Bonus: Atomic multi-field updates without compare-exchange
auto transform = AcquireCounter::PlusTransformPromiseNoOverflow(1) +
                 ReleaseCounter::PlusTransformPromiseNoOverflow(1);
meta_.Apply(transform);
```

## Key Features

### Type Safety and Self-Documentation

Each field has a specific type (`bool` for `BoolBitField`, appropriately-sized unsigned int for `UnsignedBitField`) and clear semantic meaning. The field definitions are self-documenting: you can immediately see how many bits each field occupies and in what order.

### [Zero Overhead](https://en.cppreference.com/w/cpp/language/Zero-overhead_principle)

Because of heavy use of templates and constexpr operations and the ability to satisfy multiple field reads or writes from a single atomic operation, we have seen no runtime overhead vs. hand-written bit manipulation, in RocksDB. In one case, we verified the assembly code was identical.

[For folly's LifoSem](https://github.com/facebook/folly/pull/2550), there was one case where an optimization hack with detected overflow from one field to another couldn't be replicated as efficiently with the BitFields API because it would violate overflow checking. For that case I dove into the underlying representation to bypass the BitFields overflow check.

### Atomic Operations with Transforms

One of the most powerful features is the ability to combine multiple field updates into a single atomic operation using "transforms", if they are all either (a) some combination of addition and subtraction, (b) bitwise-and, or (c) bitwise-or. For example:

```cpp
// Clear several but not all fields atomically
auto and_transform = Field1::AndTransform(0) +
                 Field2::ClearTransform() +
                 Field4::ClearTransform();
atomic_bitfields.Apply(and_transform, &old_state, &new_state);
...
// Set more than one boolean field atomically
auto or_transform = Field2::SetTransform() +
                 Field4::SetTransform();
atomic_bitfields.Apply(or_transform, &old_state, &new_state);
...
auto add_transform = Field1::PlusTransformPromiseNoOverflow(1) +
                     Field3::MinusTransformPromiseNoUnderflow(1);
atomic_bitfields.Apply(add_transform, &old_state, &new_state);
```

Each `Apply()` generates a single atomic operation (e.g., `fetch_add` or `fetch_or`) that updates all the specified fields, and optionally returns both the old and new values. This enables a number of hacks for atomic updates without CAS.

### Overflow Protection

The API includes built-in overflow detection in debug builds:

```cpp
// An assertion will fail in debug builds if the counter overflows
auto transform = Counter::PlusTransformPromiseNoOverflow(value);
atomic.Apply(transform);
```

For fields at the top of the underlying representation (where overflow doesn't affect other fields), overflow is explicitly ignored. (A compile time error is generated if you try to use `PlusTransformPromiseNoOverflow` on a field at the top of the representation or `PlusTransformIgnoreOverflow` on a field not at the top of the representation.)

```cpp
// For wraparound counters
auto transform = Counter::PlusTransformIgnoreOverflow(value);
```

This capability is used in a folly data structure called LifoSem, which [I have proposed to refactor](https://github.com/facebook/folly/pull/2550) to a proposed BitFields API variant for folly.

### Compare-and-Swap (CAS) Support

The atomic wrappers provide full CAS support for lock-free algorithms:

```cpp
SlotMeta expected = current_state;
SlotMeta desired = expected.With<Field1>(new_value).With<Field2>(true);
if (meta_.CasStrong(expected, desired)) {
  // Successfully updated
  ...
}
```

### Atomic wrappers

The BitFields API includes two atomic wrappers: `RelaxedBitFieldsAtomic` and `BitFieldsAtomic`. However, RocksDB also has versions of these wrappers for regular `std::atomic` variables that help with memory ordering discipline: `RelaxedAtomic` and `Atomic` in `util/atomic.h`.

These wrappers help in a couple of ways:
* **Self-document intended memory order**: An atomic field generally has a single memory order that all or most operations should use, typically either `std::memory_order_relaxed` or `std::memory_order_acq_rel`.
* **More intentional memory orders and atomic operations**: The standard library's implicit conversions and default memory ordering (`memory_order_seq_cst`) make it easy to accidentally use sequential consistency with acquire/release ordering or even relaxed, which could hurt performance, and tend to hide where atomic operations are actually happening (e.g. implicit vs. explicit load).

For example, instead of writing:
```cpp
std::atomic<uint64_t> stat_counter;
stat_counter++;  // Uses memory_order_seq_cst implicitly - maybe inefficient
```

You write:
```cpp
RelaxedAtomic<uint64_t> stat_counter;
stat_counter.FetchAddRelaxed(1);  // Explicitly relaxed - appropriate for a diagnostic counter
```

Or for data providing synchronization:
```cpp
Atomic<size_t> refcount;
refcount.FetchAdd(1);  // Standard acquire-release semantics for coordinating with other threads
```

These wrappers complement the BitFields atomic wrappers by providing the same ordering discipline for non-packed atomic variables throughout much of RocksDB, creating a more readable and less clunky approach to concurrent programming. Migrating remaining uses of `std::atomic` is an ongoing effort.

## Real-World Usage in RocksDB

The BitFields API was developed along with the revamped parallel compression in RocksDB, but with the intention to also clean up the HyperClockCache (HCC) implementation. With that migration complete, we can see the benefits. Specifically, **by packing more of the state machine into a single atomic value, the parallel algorithms became both simpler and more efficient.** Concurrent algorithms that could have blown up in their state space with elaborate interleavings between threads trying not to block each other, e.g. because of multi-step consensus on work assignments, were instead able to quickly and more easily make progress, e.g. with atomically clear work assignments.

### Before: Manual Bit Manipulation

The old HCC code was difficult to read and maintain. Many of the common read and update operations had manually written helper functions, but it was not practical to develop the full set of functions needed for rare cases. Consider this code that clears the "visible" flag on a slot when an entry is erased from subsequent lookups but might still be referenced:

```cpp
// Old HCC code, without atomic wrappers
uint64_t old_meta =
        h->meta.fetch_and(~(uint64_t{ClockHandle::kStateVisibleBit}
                                   << ClockHandle::kStateShift), std::memory_order_acq_rel);
// Apply update to local copy
uint64_t new_meta = old_meta & ~(uint64_t{ClockHandle::kStateVisibleBit}
                            << ClockHandle::kStateShift);

// New HCC code
SlotMeta old_meta, new_meta;
h->meta.Apply(SlotMeta::VisibleFlag::ClearTransform(), &old_meta, &new_meta);
```

Or this assertion that the acquire and release counters are different:

```cpp
// Old HCC code
uint64_t old_meta = ...;
assert(((old_meta >> ClockHandle::kAcquireCounterShift) &
        ClockHandle::kCounterMask) !=
        ((old_meta >> ClockHandle::kReleaseCounterShift) &
        ClockHandle::kCounterMask));

// New HCC code without single-purpose helper functions
SlotMeta old_meta = ...;
assert(old_meta.Get<SlotMeta::AcquireCounter>() !=
       old_meta.Get<SlotMeta::ReleaseCounter>());

// New HCC code, with single-purpose helper functions
SlotMeta old_meta = ...;
assert(old_meta.GetAcquireCounter() != old_meta.GetReleaseCounter());
```

Some hand-written helper functions or using directives are still useful for brevity, but even without them all the bit manipulation details are hidden in the BitFields implementation.

## Future Directions

We hope the proposed folly version is accepted to make the BitFields API available for broader usage. Additionally, some quality-of-life improvements are likely possible, perhaps including easier declaration and usage syntax, hopefully without delving into boost-like macro hell. Better runtime and compile time checks might also be possible.

## Conclusion

The BitFields API demonstrates that zero-overhead abstractions can significantly improve code quality without sacrificing performance. By providing type safety, self-documentation, and convenience features around bit manipulation and atomic operations, it makes lock-free programming more accessible and maintainable. Bit-packed atomics are arguably essential for *slaying the complexity dragon* of efficient lock-free and low-lock algorithms, because they reduce explosion in algorithm states.

For RocksDB specifically, the migration to BitFields has made the HyperClockCache implementation substantially easier to understand and modify, while maintaining the same high-performance characteristics. Combined with the recent [parallel compression revamp](/blog/2025/10/08/parallel-compression-revamp.html), these improvements showcase our ongoing commitment to writing clean, efficient, and maintainable code.

The BitFields API is available in RocksDB's util/bit_fields.h and can be adapted for use in other projects requiring efficient, type-safe bit packing. For those building high-performance concurrent systems, it offers a compelling alternative to manual bit manipulation—proving that safe abstractions and peak performance are not mutually exclusive.
