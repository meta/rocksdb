# FastLocalBloom Implementation

**Files:** `util/bloom_impl.h`, `table/block_based/filter_policy.cc`

## Overview

FastLocalBloom is a cache-local Bloom filter where all probes for a single key land in one 64-byte cache line. This gives near-optimal accuracy for a cache-local design while enabling SIMD-accelerated queries via AVX2. It is the default SST filter implementation for `format_version >= 5`.

## Design Principles

**Cache locality:** Each key maps to a single 64-byte (512-bit) cache line via `FastRange32(h1, num_cache_lines)`. All probes for that key occur within that cache line, meaning a filter query touches exactly one cache line regardless of `num_probes`.

**Two-hash scheme:** A 64-bit hash (from `GetSliceHash64` / XXH3) is split into two 32-bit halves:
- `h1` (lower 32 bits) -- selects the cache line
- `h2` (upper 32 bits) -- generates probe positions within the cache line via repeated multiplication by the golden ratio `0x9e3779b9`

**FastRange32:** Uses fast integer multiplication `(uint64_t(h) * uint64_t(range)) >> 32` instead of modulo for mapping h1 to a cache line index. This is faster than `%` on modern CPUs and produces near-uniform distribution.

## Probe Generation

For each probe `i` (0 to `num_probes - 1`), the bit position within the 512-bit cache line is:

```
h *= 0x9e3779b9;  // golden ratio multiplication
bitpos = h >> (32 - 9);  // top 9 bits = position 0-511
```

The top 9 bits are used because they have the best mixing from the multiplication. Each successive multiplication by the golden ratio produces a new quasi-independent probe position.

## AVX2 SIMD Query Path

The AVX2 query path in `FastLocalBloomImpl::HashMayMatchPrepared()` processes up to 8 probes simultaneously:

Step 1 -- Load 8 copies of `h2` into a 256-bit vector.
Step 2 -- Multiply by precomputed powers of the golden ratio (`0x9e3779b9^0` through `0x9e3779b9^7`) to generate 8 probe hashes simultaneously.
Step 3 -- Extract 4-bit word addresses (top 4 bits of each 32-bit element) and 5-bit bit-within-word addresses.
Step 4 -- Use `_mm256_permutevar8x32_epi32` + blend to gather the 8 probed 32-bit words from the cache line.
Step 5 -- Build a bit mask from the bit addresses and apply a probe-count selector mask.
Step 6 -- Check all probes with `_mm256_testc_si256`.

For `num_probes > 8`, the process repeats in groups of 8, advancing h by `h *= 0xab25f4c1` (golden ratio to the 8th power).

The non-SIMD fallback path uses a simple loop checking each probe sequentially, which is still fast due to cache locality.

## Num Probes Selection

`FastLocalBloomImpl::ChooseNumProbes()` selects the optimal number of probes based on `millibits_per_key`. The mapping is empirically tuned for the 512-bit cache-local design:

| millibits_per_key range | num_probes |
|---|---|
| <= 2080 | 1 |
| 2081 - 3580 | 2 |
| 3581 - 5100 | 3 |
| 5101 - 6640 | 4 |
| 6641 - 8300 | 5 |
| 8301 - 10070 | 6 |
| 10071 - 11720 | 7 |
| 11721 - 14001 | 8 |
| 14002 - 16050 | 9 |
| 16051 - 18300 | 10 |
| 18301 - 22001 | 11 |
| 22002 - 25501 | 12 |
| 25502 - 50000 | (millibits_per_key - 1) / 2000 - 1 (yields 13-23) |
| > 50000 | 24 (maximum) |

Note: These thresholds differ from standard Bloom filter theory because cache-local filters have different optimal probe counts. For example, at 16 bits/key, the optimal is 9 probes (vs. 11 for standard Bloom).

## FP Rate Characteristics

The FP rate model combines two independent sources of false positives:

1. **Filter FP rate** -- from the cache-local Bloom structure, modeled by `BloomMath::CacheLocalFpRate()` which accounts for variance in per-cache-line key counts (uses +/- 1 standard deviation approximation)
2. **Fingerprint FP rate** -- from hash collisions in the 64-bit hash space, modeled by `BloomMath::FingerprintFpRate()`

These are combined via `IndependentProbabilitySum(rate1, rate2) = rate1 + rate2 - rate1 * rate2`.

At 10 bits/key with 6 probes and 512-bit blocks: ~0.957% FP rate (vs. 0.9535% theoretical optimum for cache-local Bloom, vs. 0.953% for standard Bloom).

**Important:** The 64-bit hash avoids the accuracy degradation seen with LegacyBloom's 32-bit hash. With a 32-bit hash, the fingerprint FP rate becomes significant at ~40 million keys (at 10 bits/key). FastLocalBloom's 64-bit hash pushes this inflection point far beyond practical filter sizes.

## Filter Size Alignment

Filter payload size is always a multiple of 64 bytes (cache line size). The total encoded filter is `aligned_payload + 5` bytes (5-byte metadata trailer). This alignment is enforced by `FastLocalBloomBitsBuilder::CalculateSpace()` and `RoundDownUsableSpace()`.

**Maximum supported filter size:** `0xffffffc0` bytes (~4 GB minus 64 bytes), limited by 32-bit `FastRange32` for cache line selection. Beyond 2^32 cache lines (256 GB), accuracy degrades abruptly.

## Prefetching Strategy

Both `AddAllEntries()` (construction) and batch `MayMatch()` (query) use software prefetching with a sliding window:

- Construction uses an 8-entry ring buffer: prefetch the cache line for entry `i+8` while processing entry `i`
- Batch query prefetches all cache lines first (`PrepareHash`), then checks all probes -- separating the prefetch from the check maximizes memory-level parallelism

## LegacyBloom (Deprecated)

`LegacyLocalityBloomImpl` in `util/bloom_impl.h` is the pre-format_version-5 implementation. It uses a 32-bit hash with locality-based block probing. Key differences:

- **32-bit hash only** -- at 10 bits/key, the fingerprint FP rate becomes significant around ~40 million keys. A warning is logged at >= 3 million keys when the estimated FP rate exceeds 1.5x the expected rate
- **Double-hashing probes** -- h += delta where delta = (h >> 17) | (h << 15), with a 1/512 chance of the increment being zero within the 512-bit cache-line address space, causing all probes to hit the same bit position
- **No SIMD acceleration**
- **Odd number of cache lines** -- forces odd `num_lines` to improve hash distribution

LegacyBloom is still used when `NewBloomFilterPolicy` is configured with `format_version < 5`. At high bits/key (>= 14), a warning is logged recommending upgrade to `format_version >= 5`.
