# SecondaryCache Interface

**Files:** `include/rocksdb/secondary_cache.h`, `include/rocksdb/advanced_cache.h`, `cache/cache.cc`

## Overview

`SecondaryCache` is an abstract interface (extending `Customizable`) for second-tier caching in RocksDB. It stores serialized representations of cache entries that have been evicted from the primary cache, and reconstructs them on demand via callbacks. The interface supports both synchronous and asynchronous lookups, dynamic capacity management, and cache warming.

## SecondaryCache API

`SecondaryCache` (see `include/rocksdb/secondary_cache.h`) defines the following virtual methods:

| Method | Description |
|--------|-------------|
| `Insert(key, obj, helper, force_insert)` | Serialize and store an evicted entry. Uses `helper->size_cb` and `helper->saveto_cb` to extract persistable data. `force_insert` bypasses the secondary cache's own admission policy. |
| `InsertSaved(key, saved, type, source)` | Insert from pre-serialized data (e.g., compressed blocks from SSTs). Used for cache warming. |
| `Lookup(key, helper, create_context, wait, advise_erase, stats, kept_in_sec_cache)` | Look up and reconstruct an entry using `helper->create_cb`. Supports async mode (`wait=false`). |
| `Erase(key)` | Remove an entry from the cache. |
| `WaitAll(handles)` | Batch-wait for multiple async lookup results. |
| `SupportForceErase()` | Whether this cache supports the `advise_erase` hint (required pure virtual). |
| `SetCapacity(capacity)` / `GetCapacity(capacity)` | Dynamically adjust or query the cache capacity. |
| `Deflate(decrease)` / `Inflate(increase)` | Lightweight temporary capacity adjustments targeting a single shard, used by `CacheWithSecondaryAdapter` for proportional reservation distribution. |

Important: exceptions must NOT propagate out of overridden `SecondaryCache` methods into RocksDB, as RocksDB is not exception-safe.

## SecondaryCacheResultHandle

`SecondaryCacheResultHandle` (see `include/rocksdb/secondary_cache.h`) represents a lookup result that may or may not be ready yet. It has three possible states:

| State | `IsReady()` | `Value()` | Meaning |
|-------|------------|-----------|---------|
| Pending | `false` | Must not call | Lookup in progress; `Wait()` or `WaitAll()` required |
| Ready + not found | `true` | `nullptr` | Lookup completed, no match found (or error) |
| Ready + found | `true` | Non-null | Lookup completed, entry reconstructed; caller owns the object |

After a handle is known ready, calling `Value()` and taking ownership is required to avoid memory leaks. The `Size()` method returns the charge from `helper->create_cb`.

Note: depending on the implementation, `IsReady()` might never return true without explicitly calling `Wait()` or `WaitAll()`.

## CacheItemHelper Compatibility (Packing/Unpacking)

Insertion into the secondary cache requires that the cached object be serialized ("unpacked") into a persistable form, and on lookup, the stored data must be used to reconstruct ("pack") the original object. For RocksDB block cache entries such as data blocks, index blocks, filter blocks, and compression dictionaries, unpacking copies out the raw uncompressed `BlockContents`, and packing constructs the corresponding block/index/filter/dictionary object from that raw data.

For an entry to be eligible for secondary caching, its `CacheItemHelper` (see `include/rocksdb/advanced_cache.h`) must provide three callbacks:

- **`size_cb`**: returns the serialized size of the cached object
- **`saveto_cb`**: serializes the object into a buffer
- **`create_cb`**: reconstructs the object from serialized data, a compression type, and a source tier

The method `IsSecondaryCacheCompatible()` returns `true` when `size_cb` is non-null (all three are always set or unset together).

Each secondary-cache-compatible helper has a companion `without_secondary_compat` pointer that references an equivalent helper without secondary cache support. This is used to prevent re-insertion loops: when an entry promoted from secondary is inserted into primary while still retained in secondary, the `without_secondary_compat` helper ensures it will not be demoted back on eviction.

## SecondaryCacheWrapper

`SecondaryCacheWrapper` (see `include/rocksdb/secondary_cache.h`) provides a delegation pattern for extending `SecondaryCache`. It wraps a `target_` secondary cache and forwards all virtual methods. This is analogous to `CacheWrapper` for the primary `Cache`.

`TieredSecondaryCache` extends `SecondaryCacheWrapper` to stack a compressed cache on top of an NVM cache.

## kSliceCacheItemHelper

`kSliceCacheItemHelper` (declared in `include/rocksdb/secondary_cache.h`, defined in `cache/cache.cc`) is a pre-built helper for cache entries that are simple byte slices (e.g., compressed blocks). It provides no-op delete, slice-based size/save callbacks, and a no-op create callback. It is used by `CompressedSecondaryCache::InsertSaved()` to handle pre-serialized compressed blocks from the tiered cache path.

## Factory Method

`SecondaryCache::CreateFromString()` constructs a secondary cache from a serialized configuration string using the `ObjectRegistry` and `Customizable` framework. This enables configuration-driven setup via `DBOptions` or option strings.
