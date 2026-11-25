# Prefix Filtering

**Files:** table/block_based/full_filter_block.cc, table/block_based/block_based_table_reader.cc, table/block_based/filter_block_reader_common.cc, include/rocksdb/slice_transform.h

## Overview

Prefix filtering uses a `SliceTransform` to extract key prefixes and add them to the filter alongside (or instead of) whole keys. This enables filtering for range queries (`Seek`, `SeekForPrev`) that target a specific prefix, in addition to point queries (`Get`).

## Configuration

```
Options options;
options.prefix_extractor.reset(NewFixedPrefixTransform(8));  // Extract first 8 bytes

BlockBasedTableOptions table_options;
table_options.filter_policy.reset(NewBloomFilterPolicy(10.0));
table_options.whole_key_filtering = true;   // Add both whole keys and prefixes (default)
// table_options.whole_key_filtering = false;  // Add prefixes only
```

## What Gets Added to the Filter

The addition logic in `FullFilterBlockBuilder::Add()`:

| `whole_key_filtering` | `prefix_extractor` set | Key in domain | What's added |
|---|---|---|---|
| true | yes | yes | Both key and prefix via `AddKeyAndAlt(key, prefix)` |
| true | yes | no | Key only via `AddKey(key)` |
| true | no | N/A | Key only via `AddKey(key)` |
| false | yes | yes | Prefix only via `AddKey(prefix)` |
| false | yes | no | Nothing (key is not in the extractor's domain) |
| false | no | N/A | Nothing added |

## Query Path Dispatch

### Point Query (Get)

When `Get(key)` is called, the filter check in `FullFilterKeyMayMatch()` uses an if-else chain -- only one type of check is performed:

Step 1 -- If `whole_key_filtering == true`: call `KeyMayMatch(key)`. This checks the whole key hash against the filter. **Prefix is NOT checked**, even if both whole keys and prefixes were added during construction.

Step 2 -- Else if `whole_key_filtering == false` and `prefix_extractor` is compatible and key is in-domain: call `PrefixMayMatch(prefix)`. This checks the prefix hash.

Step 3 -- If neither applies: no filter check, proceed to read the SST data blocks.

Note: When `whole_key_filtering == true` and `prefix_extractor` is set, both whole keys and prefixes are added to the filter during construction (via `AddKeyAndAlt`), but during point queries, only the whole-key check is performed. The prefix entries are still useful for `Seek`/`SeekForPrev` operations.

### Range Query (Seek/SeekForPrev)

`Seek(target)` uses `PrefixMayMatch(prefix_of_target)` when `prefix_extractor` is configured and the target is in-domain. If the prefix is not in the filter, the entire SST file can be skipped.

## Prefix Extractor Compatibility

### The Problem

Filters are built with a specific `prefix_extractor`. If the extractor changes between SST creation and read time, prefix filtering could produce false negatives (incorrectly skipping keys).

### The Solution

RocksDB preserves correctness when prefix_extractor changes, but the behavior differs between point lookups and iterator/seek operations:

**Point queries (Get/MultiGet):** During table open, the current prefix_extractor is compared with the stored name. If incompatible, prefix filtering is disabled for point queries on that SST. Queries fall through without prefix filter checks until compaction rebuilds the filter.

**Iterator/Seek queries:** The reader stores the SST-time prefix extractor (reconstructed from TableProperties::prefix_extractor_name) in table_prefix_extractor. When the current options extractor differs, PrefixMayMatch() uses the SST-local extractor for filter checks. The decision to apply the filter depends on RangeMayExist(), which checks whether the seek key and upper bound are compatible with the SST-local extractor. This means old SST filters can still be used for iterator prefix filtering even after the extractor changes, as long as the query bounds are compatible.

Step 1 -- During SST file creation, the prefix extractor's name is stored in TableProperties::prefix_extractor_name.

Step 2 -- During table open (BlockBasedTable::Open()), the current prefix_extractor is compared with the stored name, and the SST-time extractor is saved in table_prefix_extractor.

Step 3 -- For point queries: if incompatible, prefix filtering is skipped. For iterator queries: the SST-local extractor may still be used if RangeMayExist() determines the query bounds are compatible.

Step 4 -- Compaction rebuilds filters with the current extractor.

This means changing `prefix_extractor` is safe (no data corruption or incorrect results) but degrades performance for existing SST files until they are compacted.

### Compatible Changes

Some extractor changes are recognized as compatible:
- A `nullptr` current extractor disables prefix filtering for all SSTs
- The same extractor name is always compatible

### Practical Implications

- Changing from `NewFixedPrefixTransform(8)` to `NewFixedPrefixTransform(4)` is safe but loses prefix filter benefit on older SSTs
- Removing `prefix_extractor` entirely is safe but disables all prefix filtering
- Adding a `prefix_extractor` where none existed before only benefits new SSTs

## Whole-Key Filtering with prefix_extractor

When `whole_key_filtering == true` and `prefix_extractor` is set, both whole keys and prefixes coexist in the same filter. This is the recommended configuration for workloads that use both point queries and prefix-based range queries.

The `AddKeyAndAlt()` deduplication ensures that when a key equals its own prefix (e.g., the key is exactly the prefix length), only one hash is added. This prevents inflating the filter with duplicate entries.

## Out-of-Domain Keys

Keys that are not in the prefix extractor's domain (i.e., `prefix_extractor->InDomain(key)` returns false) get special treatment:

- With `whole_key_filtering == true`: the whole key is still added and can be filtered
- With `whole_key_filtering == false`: the key is not added to the filter at all, and queries for such keys bypass filtering entirely

This is a common pitfall: if most keys are out-of-domain, `whole_key_filtering = false` effectively disables filtering for them.
