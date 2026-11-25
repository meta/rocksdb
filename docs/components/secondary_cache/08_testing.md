# Testing and Fault Injection

**Files:** `utilities/fault_injection_secondary_cache.h`, `utilities/fault_injection_secondary_cache.cc`, `test_util/secondary_cache_test_util.h`, `test_util/secondary_cache_test_util.cc`

## FaultInjectionSecondaryCache

`FaultInjectionSecondaryCache` (see `utilities/fault_injection_secondary_cache.h`) is a `SecondaryCache` wrapper used by `db_stress` to verify correctness under secondary cache errors. It randomly injects failures into `Insert` and `Lookup` operations based on a configurable probability.

### Behavior

- Wraps any `SecondaryCache` implementation (typically `CompressedSecondaryCache`)
- Uses a per-thread `Random` instance (from `util/random.h`) with a configurable seed
- On `Insert`: probabilistically returns an error status instead of forwarding to the base cache
- On `Lookup`: probabilistically returns `nullptr` (miss) or triggers errors in `Wait`/`Value` via the `ResultHandle` wrapper
- `InsertSaved` is a no-op (always returns `Status::OK()`)
- `Erase`, `SetCapacity`, `GetCapacity`, and `SupportForceErase` forward directly to the base cache

### Limitations

`FaultInjectionSecondaryCache` extends `SecondaryCache` directly (not `SecondaryCacheWrapper`), so it does not override `Deflate()` or `Inflate()`. These methods return `Status::NotSupported()`, which means fault injection cannot be used with `distribute_cache_res=true` (proportional reservation) without breaking. Developers wrapping secondary caches for testing should be aware of this limitation.

The fault injection is thread-local, using `ThreadLocalPtr` for the error context, ensuring no contention between threads.

### Special Handling

When the base cache is `CompressedSecondaryCache` (detected by name comparison in the constructor), `base_is_compressed_sec_cache_` is set to true. This flag adjusts fault injection behavior to account for the compressed cache's specific semantics.

## Test Utilities

`secondary_cache_test_util.h` provides helper classes for testing secondary cache implementations:

- `WithCacheType`: a test mixin that configures different cache types (LRU, HCC) and secondary cache combinations for parameterized testing
- Common test patterns include creating a tiered cache, inserting entries, and verifying lookup behavior with various admission policies

## Statistics for Monitoring

The secondary cache subsystem exposes several statistics for monitoring in production and testing:

### Tickers (via `Statistics`)

| Ticker | Description |
|--------|-------------|
| `SECONDARY_CACHE_HITS` | Total secondary cache hits |
| `SECONDARY_CACHE_FILTER_HITS` | Filter block hits from secondary cache |
| `SECONDARY_CACHE_INDEX_HITS` | Index block hits from secondary cache |
| `SECONDARY_CACHE_DATA_HITS` | Data block hits from secondary cache |
| `COMPRESSED_SECONDARY_CACHE_HITS` | Hits on real entries in compressed secondary cache |
| `COMPRESSED_SECONDARY_CACHE_DUMMY_HITS` | Hits on dummy entries (two-hit protocol) |
| `COMPRESSED_SECONDARY_CACHE_PROMOTIONS` | Blocks promoted from NVM to compressed secondary |
| `COMPRESSED_SECONDARY_CACHE_PROMOTION_SKIPS` | Promotions skipped |

### Perf Context Counters

| Counter | Description |
|---------|-------------|
| `secondary_cache_hit_count` | Number of secondary cache hits in current operation |
| `block_cache_standalone_handle_count` | Standalone handles created (first-access path) |
| `block_cache_real_handle_count` | Full inserts into primary cache (confirmed-access path) |
| `compressed_sec_cache_insert_dummy_count` | Dummy entries inserted (first touch) |
| `compressed_sec_cache_insert_real_count` | Real entries inserted (second touch) |
| `compressed_sec_cache_uncompressed_bytes` | Total uncompressed bytes before compression |
| `compressed_sec_cache_compressed_bytes` | Total bytes after compression |

## Stress Test Integration

The `db_stress` tool can be configured to use a secondary cache with fault injection. This exercises the secondary cache code paths under concurrent workloads and verifies that:

- Secondary cache errors do not cause data corruption
- The primary cache correctly handles secondary cache failures by falling back to storage reads
- The admission protocol remains consistent under error conditions
- Memory accounting remains correct even when insertions fail
