# Filter Types and Selection

**Files:** `include/rocksdb/filter_policy.h`, `table/block_based/filter_policy.cc`, `table/block_based/filter_policy_internal.h`

## FilterPolicy API

The `FilterPolicy` class in `include/rocksdb/filter_policy.h` is the entry point for filter configuration. It defines three key methods:

- `CompatibilityName()` -- returns a shared family name used to identify whether a filter on disk is readable by this policy. All built-in policies return `"rocksdb.BuiltinBloomFilter"`.
- `GetBuilderWithContext(FilterBuildingContext&)` -- creates a `FilterBitsBuilder` for constructing filters during SST creation. Returns `nullptr` to indicate "no filter."
- `GetFilterBitsReader(Slice& contents)` -- creates a `FilterBitsReader` for querying filters during reads.

Users configure filters via `BlockBasedTableOptions::filter_policy`:

```
table_options.filter_policy.reset(NewBloomFilterPolicy(10.0));   // Bloom
table_options.filter_policy.reset(NewRibbonFilterPolicy(10.0));  // Ribbon
```

## Available Filter Implementations

| Implementation | Builder | Reader | Introduced | Notes |
|---|---|---|---|---|
| LegacyBloom | `LegacyBloomBitsBuilder` | `LegacyBloomBitsReader` | Original | Used when `format_version < 5`; 32-bit hash only |
| FastLocalBloom | `FastLocalBloomBitsBuilder` | `FastLocalBloomBitsReader` | format_version 5 | Default for `NewBloomFilterPolicy`; 64-bit hash, AVX2 SIMD |
| Standard128Ribbon | `Standard128RibbonBitsBuilder` | `Standard128RibbonBitsReader` | 6.15.0 | 30% space savings; fallback to Bloom on failure |
| ReadOnlyBuiltin | (none -- returns nullptr) | All readers | 7.0 | Backward-compat policy for old OPTIONS files |

## NewBloomFilterPolicy

`NewBloomFilterPolicy(double bits_per_key)` in `include/rocksdb/filter_policy.h` creates a `BloomFilterPolicy` that selects the implementation based on `format_version`:

- `format_version < 5` -- Uses `LegacyBloomBitsBuilder` (32-bit hash, locality-based probing)
- `format_version >= 5` -- Uses `FastLocalBloomBitsBuilder` (64-bit hash, cache-local SIMD probing)

**Bits-per-key sanitization:**
- `< 0.5` -- rounds down to 0.0, meaning "generate no filter"
- `0.5` to `< 1.0` -- rounds up to 1.0 (62% FP rate)
- `>= 100.0` or NaN -- capped at 100.0

Internally, bits-per-key is stored as `millibits_per_key` (thousandths of a bit) for fractional precision with `format_version >= 5`. Legacy format rounds to whole bits.

## NewRibbonFilterPolicy

`NewRibbonFilterPolicy(double bloom_equivalent_bits_per_key, int bloom_before_level)` creates a `RibbonFilterPolicy` that chooses between Ribbon and FastLocalBloom based on the `FilterBuildingContext`.

### bloom_before_level Logic

The `bloom_before_level` parameter controls the level threshold. The selection logic in `RibbonFilterPolicy::GetBuilderWithContext()` works as follows:

Step 1 -- Determine `levelish` from context:
- Flush (`TableFileCreationReason::kFlush`) -- `levelish = -1`
- Known level -- `levelish = context.level_at_creation`
- Unknown level or FIFO/None compaction style -- `levelish = INT_MAX - 1` (treated as bottommost)

Step 2 -- Compare with threshold:
- `levelish < bloom_before_level` -- Use FastLocalBloom
- `levelish >= bloom_before_level` -- Use Ribbon

| bloom_before_level | Behavior |
|---|---|
| `-1` | Always Ribbon (except some extreme/exceptional cases) |
| `0` (default) | Bloom for flushes only; Ribbon for all compaction output |
| `1` | Bloom for L0 (flush + intra-L0); Ribbon for L1+ |
| `INT_MAX` | Always Bloom |

This parameter is mutable at runtime via `SetOptions()`:
`db->SetOptions({{"table_factory.filter_policy.bloom_before_level", "3"}});`

## FilterBuildingContext

`FilterBuildingContext` in `include/rocksdb/filter_policy.h` carries contextual information to `GetBuilderWithContext()`:

| Field | Type | Purpose |
|---|---|---|
| `table_options` | `BlockBasedTableOptions&` | Current table options |
| `compaction_style` | `CompactionStyle` | Level, Universal, FIFO, or None |
| `num_levels` | `int` | Total LSM levels, or -1 if unknown |
| `column_family_name` | `string` | CF name for the table |
| `level_at_creation` | `int` | Target level for this SST, or -1 if unknown |
| `is_bottommost` | `bool` | Whether this SST is in the bottommost sorted run |
| `reason` | `TableFileCreationReason` | Flush, compaction, misc, etc. |

Custom `FilterPolicy` implementations can use this context to make per-SST decisions (e.g., different FP rates for different levels).

## Class Hierarchy

The internal class hierarchy in `filter_policy_internal.h`:

- `FilterPolicy` (public API)
  - `BuiltinFilterPolicy` -- base for all built-in policies; implements `GetFilterBitsReader()` dispatch based on trailer byte
    - `ReadOnlyBuiltinFilterPolicy` -- backward compatibility; does not build filters
    - `BloomLikeFilterPolicy` -- shared base for Bloom and Ribbon; stores `millibits_per_key_`, `desired_one_in_fp_rate_`, and `aggregate_rounding_balance_`
      - `BloomFilterPolicy` -- dispatches to Legacy or FastLocalBloom based on `format_version`
      - `RibbonFilterPolicy` -- dispatches to Ribbon or FastLocalBloom based on `bloom_before_level`

The reader dispatch is handled by `BuiltinFilterPolicy::GetBuiltinFilterBitsReader()`, which reads the metadata trailer byte to select the appropriate reader class. This means any built-in filter can be read regardless of which policy created it.
