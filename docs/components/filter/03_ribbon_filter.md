# Ribbon Filter Implementation

**Files:** `table/block_based/filter_policy.cc`, `util/ribbon_config.h`, `util/ribbon_impl.h`, `util/ribbon_alg.h`

## Overview

Ribbon (Rapid Incremental Boolean Banding ON-the-fly) is a space-efficient alternative to Bloom filters. It saves ~30% space compared to Bloom at similar FP rates, at the cost of 3-4x CPU during construction and ~3x temporary memory usage. Query speed is comparable to Bloom.

## How Ribbon Works

Ribbon is fundamentally different from Bloom filters. Instead of setting bits in a bit array, it solves a linear system over GF(2) (binary field):

1. Each key is hashed to a 128-bit coefficient row and a result value
2. The system of equations is solved using Gaussian elimination (banding)
3. The solution is stored as an interleaved array of coefficient blocks

A query re-hashes the key and checks if the stored solution matches the expected result. If it does, the key "may be present." The FP rate is determined by the number of result bits per slot.

## Construction: Standard128RibbonBitsBuilder

The Ribbon builder in `filter_policy.cc` (`Standard128RibbonBitsBuilder`) uses 128-bit coefficient rows (`Unsigned128`). Construction proceeds:

Step 1 -- Accumulate 64-bit hashes in the `XXPH3FilterBitsBuilder` base class (same hash accumulation as FastLocalBloom).

Step 2 -- Calculate space: `CalculateSpaceAndSlots()` determines `num_slots` from `num_entries` using `BandingConfigHelper` lookup tables. The number of result bits per slot is derived from `desired_one_in_fp_rate_`.

Step 3 -- Attempt banding: `banding.ResetAndFindSeedToSolve()` tries up to 256 different hash seeds (0-255) to find one where the linear system can be solved. Each seed has roughly a 1-in-20 chance of failure (`kOneIn20`).

Step 4 -- Back-substitution: `soln.BackSubstFrom(banding)` computes the interleaved solution from the banding.

Step 5 -- Write metadata trailer: seed (1 byte) + num_blocks (3 bytes, little-endian).

## Bloom Fallback Conditions

`Standard128RibbonBitsBuilder` contains an embedded `FastLocalBloomBitsBuilder bloom_fallback_` and falls back to Bloom in three cases:

1. **Too many keys** -- More than `kMaxRibbonEntries` (950 million). The Ribbon implementation uses 32-bit slot indices.
2. **Small filter** -- Fewer than 1024 slots, where Bloom is more space-efficient than Ribbon.
3. **Construction failure** -- All 256 seed attempts fail to solve the linear system (~extremely rare, probability roughly (1/20)^256).
4. **Cache reservation failure** -- If the block cache cannot accommodate the temporary banding memory (`status_banding_cache_res.IsMemoryLimit()`).

Fallback is transparent: the entries are swapped to `bloom_fallback_` via `SwapEntriesWith()`, and the resulting filter is a valid FastLocalBloom. Readers handle it automatically via the metadata trailer byte discriminator.

## Space Efficiency

Ribbon achieves ~30% space savings by using near-information-theoretic-minimum bits per key:

| Target FP Rate | Bloom bits/key | Ribbon bits/key | Savings |
|---|---|---|---|
| 1% | 10 | ~7 | 30% |
| 0.1% | 14.3 | ~10 | 30% |

The space saving comes from Ribbon's ability to use fractional result bits. The actual bits per slot varies: `GetBytesForOneInFpRate()` computes the exact byte count needed for a given number of slots and target FP rate, mixing `b` and `b+1` result bits per slot.

## Query: Standard128RibbonBitsReader

The reader stores a `SerializableInterleavedSolution` and a `StandardHasher`. Query proceeds:

Step 1 -- Hash the key to a 64-bit value with `GetSliceHash64()`.
Step 2 -- Apply the stored seed to produce a seeded hash.
Step 3 -- Compute the coefficient row and expected result.
Step 4 -- Check against the stored solution.

**Batch query optimization:** `MayMatch(num_keys, ...)` separates the prepare phase (computing segment_num, num_columns, start_bits) from the query phase, similar to FastLocalBloom's prefetching strategy. This is implemented via `ribbon::InterleavedPrepareQuery()` and `ribbon::InterleavedFilterQuery()`.

## Configuration

Ribbon is configured indirectly through `bloom_equivalent_bits_per_key`:

```
// 10.0 bloom-equivalent bits/key -> ~1% FP rate, ~7 actual bits/key
auto policy = NewRibbonFilterPolicy(10.0, /*bloom_before_level=*/1);
```

The `desired_one_in_fp_rate_` is computed from the Bloom FP rate at the specified bits/key:
`desired_one_in_fp_rate_ = 1.0 / CacheLocalFpRate(bits_per_key, num_probes, 512)`

This means specifying the same `bits_per_key` for Bloom and Ribbon gives the same FP rate, but Ribbon uses fewer actual bits.

## Interleaved Solution Format

The on-disk format uses interleaved solution blocks. Each block contains 128 slots (matching the 128-bit coefficient row width). The number of blocks is stored in the 3-byte metadata trailer field.

**Alignment:** Solution data is rounded down to multiples of 16 bytes (segment size) by `RoundDownUsableSpace()`.

**Maximum entries:** ~950 million (`kMaxRibbonEntries`), limited by 32-bit `num_slots` and 24-bit `num_blocks` in the metadata trailer.

## When to Use Ribbon

**Cost model:** The break-even point for Ribbon vs. Bloom is roughly when a filter lives in memory for ~1 hour. Filters living longer than that save more in memory cost than they spend in extra construction CPU. Since L0 data typically lives minutes and deeper levels live days to weeks, the hybrid strategy is almost always profitable. A DB would need to sustain 20+ DWPD for data to have an average lifetime short enough to make Ribbon unprofitable.

**Recommended for:**
- Lower LSM levels (L1+) where files are larger and longer-lived, amortizing the construction cost
- Space-constrained deployments where 30% filter space savings matter
- Using the hybrid strategy: `NewRibbonFilterPolicy(10.0, 1)` for Bloom in L0, Ribbon in L1+

**Not recommended for:**
- Flush-heavy workloads where construction CPU is the bottleneck
- Very small SST files where Ribbon falls back to Bloom anyway (< 1024 slots)
- Environments where RocksDB < 6.15.0 might read the data (Ribbon is not forward-compatible)
