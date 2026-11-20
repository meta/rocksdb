# Merge Operator

**Files:** `include/rocksdb/merge_operator.h`, `db/merge_operator.cc`, `db/merge_context.h`

## Overview

The merge operator enables read-modify-write semantics without requiring a separate read before each write. Instead of fetching the current value, modifying it, and writing it back, the application calls `DB::Merge()` to record an incremental update. RocksDB defers the actual computation until the value is needed -- during `Get()`, iteration, or compaction -- at which point it resolves all accumulated merge operands into a single result.

This design eliminates the read step from the write path, reducing latency and avoiding read-write contention. Common use cases include counters, list append operations, and incremental updates to complex data structures.

## MergeOperator Interface Hierarchy

RocksDB provides two merge operator interfaces, forming a two-level hierarchy:

| Class | Purpose | When to Use |
|-------|---------|-------------|
| `AssociativeMergeOperator` | Simplified interface for associative operations | When the merge is always binary (two inputs, one output) and associative |
| `MergeOperator` | Full-featured interface with partial merge support | When merges can be optimized by combining operands before a base value is available |

Both are defined in `include/rocksdb/merge_operator.h`. `AssociativeMergeOperator` inherits from `MergeOperator` and provides default implementations of `FullMergeV2` and `PartialMerge` that delegate to a single `Merge()` method.

## AssociativeMergeOperator

The simpler interface requires implementing a single method: `Merge()` in `AssociativeMergeOperator` (see `include/rocksdb/merge_operator.h`).

The `Merge()` method receives:
- `key`: the key being merged
- `existing_value`: pointer to the current value, or `nullptr` if the key does not yet exist
- `value`: the new merge operand
- `new_value`: output buffer for the merged result

The `AssociativeMergeOperator` default implementation of `FullMergeV2` loops through all operands in order, calling `Merge()` for each one. Its default `PartialMerge` implementation also delegates directly to `Merge()`.

This interface is appropriate for operations where the merge function is associative, such as integer addition or string concatenation. RocksDB preserves operand order, so commutativity is not required.

## MergeOperator: Full Merge

The full merge interface has evolved through three versions:

| Version | Method | Status |
|---------|--------|--------|
| V1 | `FullMerge()` | Deprecated; uses `std::deque<std::string>` |
| V2 | `FullMergeV2()` | Stable; uses `MergeOperationInput` / `MergeOperationOutput` |
| V3 | `FullMergeV3()` | Current; adds wide-column support via `std::variant` |

### FullMergeV3

`FullMergeV3()` is the recommended method to override. It receives a `MergeOperationInputV3` and produces a `MergeOperationOutputV3`.

**Input (`MergeOperationInputV3`):**
- `key`: the user key (includes user-defined timestamp if enabled)
- `existing_value`: a `std::variant` of three states -- `std::monostate` (no base value), `Slice` (plain value), or `WideColumns` (wide-column entity)
- `operand_list`: vector of `Slice` operands in chronological order (oldest first)
- `logger`: for error logging

**Output (`MergeOperationOutputV3`):**
- `new_value`: a `std::variant` that can be a `std::string` (new plain value), `NewColumns` (new wide-column entity), or `Slice` (reference to an existing operand)
- `op_failure_scope`: controls failure blast radius when returning `false`

The method returns `true` on success and `false` on failure.

### Backward Compatibility Chain

The default `FullMergeV3()` implementation in `db/merge_operator.cc` falls back to `FullMergeV2()`:
- If the existing value is `std::monostate` or `Slice`, it calls `FullMergeV2()` directly
- If the existing value is `WideColumns`, it extracts the default column value (if present), calls `FullMergeV2()` on that, and reconstructs the wide-column output with the merged default column and all other columns unchanged

The default `FullMergeV2()` implementation in turn falls back to the deprecated `FullMerge()` by converting the operand list from `std::vector<Slice>` to `std::deque<std::string>`.

### OpFailureScope

When a merge operator returns `false`, the `op_failure_scope` field controls how broadly the failure propagates:

| Scope | Effect |
|-------|--------|
| `kDefault` | Falls back to `kTryMerge` |
| `kTryMerge` | Fails flush and compaction (puts DB in read-only mode) |
| `kMustMerge` | Only fails operations that must resolve the merge (`Get()`, iteration); flush and compaction copy operands through unchanged |

`kMustMerge` is useful when a merge failure is data-dependent and the operands should be preserved for later retry or manual inspection, rather than making the entire database read-only.

## MergeOperator: Partial Merge

Partial merge combines two or more merge operands together without a base value. This is an optimization: if multiple operands can be combined into one before encountering a `Put` or `Delete`, the total work during the eventual full merge is reduced.

Two methods are available:

**`PartialMerge()`** combines exactly two operands (`left_operand` and `right_operand`) into `new_value`. The default implementation returns `false` (no partial merge support).

**`PartialMergeMulti()`** combines a list of operands into a single result. The default implementation calls `PartialMerge()` pairwise from front to back. Override `PartialMergeMulti()` when the multi-way merge is more efficient than repeated pairwise merges.

Important: returning `false` from partial merge is not an error. It simply means the operands cannot be combined without a base value, and they will be kept as separate entries until a full merge is triggered.

## AllowSingleOperand

By default, `PartialMerge` and `PartialMergeMulti` are only called when there are two or more operands. If `AllowSingleOperand()` returns `true`, they may also be called with a single operand. This enables transformations like format normalization or validation on individual operands during compaction.

## ShouldMerge

`ShouldMerge()` (see `MergeOperator` in `include/rocksdb/merge_operator.h`) provides a way to trigger an early full merge during `Get()` operations. When the method returns `true`, RocksDB performs a full merge with no base value immediately, without searching deeper levels for a `Put` or `Delete`.

Note: the operands are passed in reverse order (newest first) for performance reasons. This optimization was introduced in version 5.16 (see `HISTORY.md`).

Important: `ShouldMerge` only affects point lookups. It does not affect iterators.

## MergeContext

`MergeContext` (see `db/merge_context.h`) is the container that accumulates merge operands during a lookup or compaction. It maintains operands in a `std::vector<Slice>` and can present them in either forward (chronological) or backward (reverse chronological) order.

Key behaviors:
- `PushOperand()`: adds an operand in backward (newest-first) order, used during point lookups that traverse from newest to oldest
- `PushOperandBack()`: adds an operand in forward (oldest-first) order
- `GetOperands()` / `GetOperandsDirectionForward()`: returns operands in the order expected by `FullMerge` / `FullMergeV2` / `FullMergeV3` (oldest first)
- `GetOperandsDirectionBackward()`: returns operands in reverse order, used by `ShouldMerge()`
- Direction changes are implemented by reversing the internal vector in place

Operands that are not pinned by their source are copied into owned `std::string` objects to ensure they remain valid for the lifetime of the merge context.

## Merge Operator Naming

The `Name()` method (pure virtual in `MergeOperator`) returns a string identifier for the merge operator. The client must ensure that the merge operator supplied during `DB::Open()` has the same name and semantics as the merge operator used in previous opens of the same database. Mismatching merge operators can lead to data corruption because the operand format and merge semantics are operator-specific.

Note: the merge operator name is stored persistently in the OPTIONS file (via `ImmutableCFOptions` serialization) and verified by name during `DB::Open()`. It is also stored per SST file in the table properties (as `rocksdb.merge.operator`) for informational purposes, though this per-file value is not used for DB-open consistency checks. If the OPTIONS file is lost or ignored, enforcement depends on the client providing a consistent operator.
