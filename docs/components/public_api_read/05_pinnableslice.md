# PinnableSlice and Zero-Copy

**Files:** `include/rocksdb/slice.h`, `include/rocksdb/cleanable.h`

## Overview

`PinnableSlice` (see `PinnableSlice` in `include/rocksdb/slice.h`) is a `Slice` subclass that avoids copying values by pinning the underlying storage (typically a block cache entry). It inherits from both `Slice` (for data access) and `Cleanable` (for deferred cleanup).

## Pinning Mechanics

A `PinnableSlice` operates in two modes:

### Pinned Mode (IsPinned() == true)

The `data()` pointer references memory owned by the block cache or another external source. The block cache entry is held alive via cleanup functions registered through the `Cleanable` interface. No copy occurs -- the application reads directly from block cache memory.

This mode is used when:
- The value is found in an SST file and the block is in the block cache
- `PinSlice()` is called with a `Slice` and a cleanup function or `Cleanable` delegate

### Unpinned Mode (IsPinned() == false)

The value is copied into `PinnableSlice`'s internal `self_space_` buffer (a `std::string`). The `data()` pointer references this internal buffer.

This mode is used when:
- The value is found in a memtable (copied via `PinSelf()`)
- The block cache is not used or the entry cannot be pinned
- The constructor takes an external `std::string*` buffer

## Construction

| Constructor | Behavior |
|------------|----------|
| `PinnableSlice()` | Uses internal `self_space_` buffer |
| `PinnableSlice(std::string* buf)` | Uses external buffer; data is written to `*buf` in unpinned mode |

The external-buffer constructor is used internally by the `std::string*` `Get()` overloads. This allows the `Get()` implementation to write directly into the caller's string without an extra copy.

## Key Methods

### PinSlice()

Pins the slice to external memory with a cleanup function. Called by the SST read path when the value is found in a cached block. The cleanup function typically decrements the block cache entry's reference count.

### PinSelf()

Copies data into the internal buffer and sets the `Slice` to point at it. Called when the value is found in a memtable (which does not use the block cache).

### Reset()

Runs all registered cleanup functions (releasing block cache references), clears the pinned flag, and resets the size to 0. Must be called or the destructor must run before reusing the `PinnableSlice` in another `Get()` or `MultiGet()` call.

## Move Semantics

`PinnableSlice` supports move construction and move assignment. Copy construction and copy assignment are deleted.

When moved, the source loses ownership of the pinned data and cleanup functions. The destination takes over the block cache reference.

## Lifetime Rules

- A pinned `PinnableSlice` holds a reference to a block cache entry. This reference is released when `Reset()` is called or the `PinnableSlice` is destroyed.
- The `data()` pointer remains valid until `Reset()`, destruction, or reuse in another read call.
- All `PinnableSlice` objects returned by `Get()`/`MultiGet()` must be destroyed or `Reset()` before the DB is closed.
- `remove_prefix()` and `remove_suffix()` work differently in pinned vs. unpinned mode: in pinned mode they adjust the pointer/size directly; in unpinned mode they erase from the internal buffer.

## Performance Impact

Using `PinnableSlice*` with `Get()` instead of `std::string*` avoids one `memcpy` when the value is in the block cache. For workloads where most reads hit the block cache and values are large, this can meaningfully reduce CPU usage.

The `std::string*` overloads create a `PinnableSlice` wrapping the caller's string. If the result ends up pinned (SST read), the data must be copied from block cache memory into the string via `assign()`. The `PinnableSlice*` overload avoids this copy.
