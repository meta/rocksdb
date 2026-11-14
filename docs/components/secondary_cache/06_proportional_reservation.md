# Proportional Reservation Distribution

**Files:** `cache/secondary_cache_adapter.h`, `cache/secondary_cache_adapter.cc`, `cache/cache_reservation_manager.h`

## Overview

When `CacheWithSecondaryAdapter` is constructed with `distribute_cache_res = true` (as done by `NewTieredCache()`), it manages the entire memory budget across primary and secondary caches. Memory reservations from external consumers (e.g., `WriteBufferManager`) are distributed proportionally between tiers, ensuring neither tier is unfairly burdened.

## Memory Layout

The primary cache is initially sized to the **combined** budget (primary + secondary):

```
|------------ Primary Cache Configured Capacity -------------|
|--- Secondary Cache Budget ---|---- Primary Cache Budget ---|
```

A `ConcurrentCacheReservationManager` (`pri_cache_res_`) reserves the secondary cache's share against the primary cache via dummy entries, effectively limiting usable primary capacity:

```
|-- Reservation for Sec Cache --|-- Pri Cache Usable Capacity --|
```

The secondary cache has its own independent capacity set to `total_capacity * compressed_secondary_ratio`.

## Reservation Distribution Workflow

### When a Placeholder is Inserted

External components like `WriteBufferManager` insert placeholder entries (value = `nullptr`, non-zero charge) to reserve memory against the block cache.

Step 1: The placeholder is inserted into the primary cache at full charge.

Step 2: Track the cumulative placeholder usage (`placeholder_usage_`).

Step 3: If the new total exceeds `reserved_usage_` by at least `kReservationChunkSize` (1MB) and `placeholder_usage_` does not exceed total capacity:
- Round `reserved_usage_` down to the nearest 1MB boundary
- Compute the secondary cache's share: `new_sec_reserved = reserved_usage_ * sec_cache_res_ratio_`
- Call `secondary_cache_->Deflate(delta)` to shrink the secondary cache
- Release the same amount from `pri_cache_res_` to give the primary more usable capacity

Step 4: Result: the secondary cache absorbs its proportional share of the reservation.

### When a Placeholder is Released

The reverse process occurs in `Release()` with `erase_if_last_ref = true`:

Step 1: Decrease `placeholder_usage_` by the released charge.

Step 2: If `placeholder_usage_ < reserved_usage_`:
- Recompute `reserved_usage_` and `new_sec_reserved`
- Call `secondary_cache_->Inflate(delta)` to restore secondary capacity
- Increase `pri_cache_res_` reservation to re-protect the secondary budget

### Example: 70/30 Split with 10MB Reservation

For a 100MB total budget with a 70/30 primary/secondary split:

| State | Primary Usable | Sec Cache Reserve | Placeholder | Notes |
|-------|---------------|-------------------|-------------|-------|
| Initial | 70MB | 30MB | 0MB | Primary sized to 100MB, 30MB reserved |
| After inserting 10MB placeholder | 60MB | 30MB | 10MB | Placeholder in primary at full charge |
| After deflate/adjust | 63MB | 27MB | 10MB | 3MB (10 * 0.3) moved from sec to pri |

## Capacity Resizing

`SetCapacity()` carefully orders operations to avoid temporary capacity spikes:

### Shrinking

Step 1: Lower the secondary cache capacity first.

Step 2: Credit the freed amount to the primary (decrease `pri_cache_res_`).

Step 3: Decrease the primary cache capacity to the new total budget.

### Expanding

Step 1: Expand the primary cache capacity first (to absorb the additional budget).

Step 2: Reserve additional capacity for the secondary (increase `pri_cache_res_`).

Step 3: Expand the secondary cache capacity.

## Ratio Updates

`UpdateCacheReservationRatio()` changes the primary/secondary split at runtime. Similar ordering logic applies:

### Increasing the ratio (larger secondary)

Step 1: Deflate the secondary to account for its larger share of existing reservations.

Step 2: Increase `pri_cache_res_` to reserve the additional secondary budget.

Step 3: Increase the secondary cache capacity.

### Decreasing the ratio (smaller secondary)

Step 1: Lower the secondary cache capacity.

Step 2: Decrease `pri_cache_res_` to free the surplus reservation.

Step 3: Inflate the secondary cache to reduce its share of existing reservations.

Important: once the ratio is lowered to 0.0, the secondary cache is effectively disabled and `pri_cache_res_` total memory goes to 0. Due to implementation constraints, increasing the ratio back from 0.0 is not supported.

## Thread Safety

All reservation distribution operations are protected by `cache_res_mutex_`. This mutex is separate from any cache-internal locks to avoid deadlocks. The chunking mechanism (`kReservationChunkSize = 1MB`) reduces the frequency of lock acquisitions.

The `sec_cache_res_ratio_` is computed once during construction and updated only via `UpdateCacheReservationRatio()` under the mutex.
