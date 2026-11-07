# FIFO Compaction Strategy

This document describes the FIFO compaction style in RocksDB, covering the
file dropping strategies and both the old and new intra-L0 compaction
picking strategies.

## Overview

FIFO compaction is designed for time-series and log-like workloads where data
has a natural expiration. All data lives at L0. When total data exceeds a
configured size limit, the oldest SST files are dropped — no merge, no rewrite,
just deletion. This gives near-zero write amplification for the compaction layer.

```
L0 (all data lives here):
  Newest                                                           Oldest
    |                                                                |
    v                                                                v
  [SST_N] [SST_N-1] ... [SST_3] [SST_2] [SST_1]
    ^                                       ^
    |                                       |
  new flushes added here          oldest files dropped here
                                  (when over size limit)
```

Without intra-L0 compaction, every memtable flush creates a new small SST file.
Over time, the number of L0 files grows, increasing read amplification (each
point lookup must check every L0 file). Intra-L0 compaction addresses this by
merging small files into fewer larger files.

## Compaction Picking Priority Chain

When compaction is triggered (score >= 1.0), the picker tries these strategies
in order, returning the first non-null result:

```
PickCompaction():
    |
    |-- 1. PickTTLCompaction()               [File Dropping]
    |       Drop files older than TTL.
    |
    |-- 2. PickSizeCompaction()              [File Dropping]
    |       Drop oldest files when over size limit.
    |
    |-- 3. PickIntraL0Compaction()           [Intra-L0]
    |       Dispatcher: merges small L0 files to reduce file count.
    |       Requires allow_compaction=true. Dispatches to:
    |         - PickRatioBasedIntraL0Compaction (use_kv_ratio_compaction=true)
    |         - PickCostBasedIntraL0Compaction  (use_kv_ratio_compaction=false)
    |
    |-- 4. PickTemperatureChangeCompaction() [Temperature Migration]
            Rewrite one file to change its temperature tier.
            Lowest priority — runs only if nothing else needs to be done.
```

Steps 1 and 2 are **file dropping** — they delete old files to enforce size
or TTL limits. Step 3 is **intra-L0 compaction** — it merges small files into
fewer larger ones. `PickIntraL0Compaction` is the dispatcher that selects
between the two strategies based on `use_kv_ratio_compaction`.

Step 4 is **temperature migration** — it rewrites a single file to change its
storage temperature (e.g., moving cold data to cheaper storage). It picks one
file at a time, checking if the file's age exceeds a configured threshold but
its current temperature doesn't match the target. It runs last because it's
the lowest priority: disk space management (dropping) and read amplification
(intra-L0) are more important than storage tiering. Since FIFO only allows
one compaction at a time, running temperature change last ensures it never
blocks more critical operations.

Note: Intra-L0 compaction runs after size-based dropping. If `PickSizeCompaction`
dropped files (returned non-null), `PickIntraL0Compaction` is skipped. This
means intra-L0 only runs when the DB is under the size limit or when
size-based compaction is already in progress.

## Score Computation

The compaction score determines when compaction should be triggered. For FIFO:

```
score = effective_total_size / effective_max_size
```

Where:
- `effective_total_size` = total SST size (or SST + blob when
  `max_data_files_size > 0`)
- `effective_max_size` = `max_table_files_size` (or `max_data_files_size`
  when set)

Additional score contributions:
- When `allow_compaction` is true (enables intra-L0 compaction):
  `score = max(score, num_sorted_runs / level0_file_num_compaction_trigger)`
- When `ttl > 0`: score is boosted by expired file count
- When temperature thresholds are set: score is boosted if files need
  temperature change

---

# Part 1: File Dropping Strategies

These strategies delete old files to enforce data size or TTL limits.
No data is rewritten — files are simply removed.

## TTL-Based Dropping (`PickTTLCompaction`)

**When**: `ttl > 0`

Drops L0 files whose data is older than the TTL threshold. Iterates from
oldest to newest, checking `newest_key_time` or `oldest_ancestor_time` from table
properties against `current_time - ttl`.

```
Before TTL compaction (ttl = 3600s, files older than 1 hour):

  L0: [F6:10m] [F5:20m] [F4:40m] [F3:50m] [F2:70m] [F1:80m]
                                              ^^^^     ^^^^
                                            older than TTL --> DROP

After:
  L0: [F6:10m] [F5:20m] [F4:40m] [F3:50m]
```

Returns `nullptr` if deleting expired files would still leave the total size
above the size limit — in that case, size-based dropping handles it instead.

**Config**: `MutableCFOptions::ttl` (in seconds)

## Size-Based Dropping (`PickSizeCompaction`)

**When**: Total size exceeds the configured limit.

### SST-Only Mode (default)

Compares sum of SST file sizes against `max_table_files_size`:

```
Before (total 1.2GB > max_table_files_size 1GB):

  L0: [F8:200MB] [F7:200MB] [F6:200MB] [F5:200MB] [F4:200MB] [F3:200MB]
                                                       total = 1.2GB

  Drop oldest files until under limit:
  Drop F3 (200MB) --> remaining = 1.0GB <= 1GB limit --> STOP

After:
  L0: [F8:200MB] [F7:200MB] [F6:200MB] [F5:200MB] [F4:200MB]
                                                       total = 1.0GB
```

### Blob-Aware Mode (`max_data_files_size > 0`)

When BlobDB is enabled, SST files are small (keys + blob references) and blob
files hold the actual values. The total disk usage is dominated by blob files,
so `max_table_files_size` (SST-only) cannot control total disk usage.

`max_data_files_size` accounts for both SST and blob files:

```
effective_size = total_sst + total_blob

Example: total_sst = 10MB, total_blob = 9.99GB
  max_table_files_size = 1GB  --> sees 10MB, no dropping (WRONG!)
  max_data_files_size = 10GB  --> sees 10GB, drops when exceeded (CORRECT)
```

When selecting files to drop, actual SST bytes are subtracted. Remaining blob
bytes are estimated in proportion to the remaining SST bytes, using SST sizes
across all levels as the reference:

```
estimated_remaining_data = remaining_sst + (remaining_sst / total_sst) * total_blob
```

Blob files are automatically cleaned up when their linked SSTs are deleted
(via `BlobFileMetaData::GetLinkedSsts()` reference counting).
The blob estimate is approximate: SSTs can reference different amounts of blob
data, and shared blob files remain until all their linked SSTs are deleted.

**Config**:
- `CompactionOptionsFIFO::max_table_files_size` (default: 1GB)
- `CompactionOptionsFIFO::max_data_files_size` (default: 0, disabled)

## Temperature Migration (`PickTemperatureChangeCompaction`)

**When**: `file_temperature_age_thresholds` is non-empty

This is NOT file dropping — it **rewrites** a single SST file to assign it a
new storage temperature (e.g., kWarm, kCold). This allows tiered storage
systems to move aging data to cheaper/slower media. The file content is
unchanged; only the temperature metadata is updated.

Picks one file at a time, scanning from oldest to newest. For each file,
checks if its age exceeds a configured threshold AND its current temperature
doesn't match the target. Only one file is migrated per compaction to minimize
impact on other operations. Only works with single-level FIFO
(`num_levels == 1`).

This runs as the **lowest priority** in the picking chain (step 4) because
storage tiering is less urgent than disk space management (dropping) or read
amplification (intra-L0 compaction). Since FIFO allows only one compaction at
a time, this ensures temperature migration never blocks critical operations.

```
Config: file_temperature_age_thresholds = [{kWarm, 3600}, {kCold, 86400}]

  [F6:5m,kUnk] [F5:30m,kUnk] [F4:2h,kUnk] [F3:5h,kUnk] [F2:2d,kUnk]
                                                            ^^^^^^^^
                                                            age > 86400s
                                                            --> compact to kCold

After:
  [F6:5m,kUnk] [F5:30m,kUnk] [F4:2h,kUnk] [F3:5h,kUnk] [F2:2d,kCold]
```

**Config**: `CompactionOptionsFIFO::file_temperature_age_thresholds`

---

# Part 2: Intra-L0 Compaction

Intra-L0 compaction merges multiple small L0 files into fewer larger files
to reduce file count and read amplification. Unlike file dropping, this
rewrites data — but only SST data (blob files are never rewritten).

`allow_compaction = true` is the **master switch** for intra-L0 compaction.
When enabled, `use_kv_ratio_compaction` selects which picking strategy to use:

```
  allow_compaction = true          (master switch for intra-L0)
          |
          +-- use_kv_ratio_compaction = false   (default)
          |     Old Strategy: PickCostBasedIntraL0Compaction
          |     Guard: 1.1 * write_buffer_size
          |     Works when SST ~= write_buffer_size (non-BlobDB)
          |
          +-- use_kv_ratio_compaction = true
                New Strategy: PickRatioBasedIntraL0Compaction
                Guard: capacity-derived target from SST/blob ratio
                Works when SST << write_buffer_size (BlobDB)
                Requires: max_data_files_size > 0
```

## Old Strategy: `PickCostBasedIntraL0Compaction`

**When**: `allow_compaction = true` AND `use_kv_ratio_compaction = false`.
Called from `PickIntraL0Compaction` (which only runs when `PickSizeCompaction`
returned nullptr, meaning the DB is under the size limit).

This is the original intra-L0 compaction, implemented in
`PickCostBasedIntraL0Compaction()`. It uses a greedy algorithm to pick files,
with a `write_buffer_size`-based guard to prevent re-compacting large files.

### Algorithm

```
1. Start from the newest L0 file (index 0)
2. Greedily add older files while compact_bytes_per_del_file decreases
3. Stop when:
   - A file is being_compacted
   - compact_bytes_per_del_file starts increasing (diminishing returns)
   - Total exceeds max_compaction_bytes
4. Check: enough files (>= trigger) AND per_del < 1.1 * write_buffer_size
5. Output: always a single file
```

### Understanding `compact_bytes_per_del_file`

`compact_bytes_per_del_file` measures the **cost per file eliminated**. When
we compact N files into 1 output, we eliminate (N-1) files but must read and
rewrite all N files' data. The metric is:

```
compact_bytes_per_del_file = total_input_bytes / (num_files - 1)
```

The algorithm greedily adds files as long as this ratio keeps **decreasing**
(meaning each additional file is "cheap" to include). When adding a file
causes the ratio to **increase**, we stop — it signals diminishing returns.

```
Example: scanning files from newest (left) to oldest (right)

  Files:    [F5:32KB] [F4:64KB] [F3:48KB] [F2:96KB] [F1:128KB]

  Step 1: Start with F5 (32KB). compact_bytes = 32KB.
  Step 2: Add F4.  compact_bytes = 96KB.  per_del = 96/1 = 96KB.
  Step 3: Add F3.  compact_bytes = 144KB. per_del = 144/2 = 72KB. (72 < 96, improving)
  Step 4: Add F2.  compact_bytes = 240KB. per_del = 240/3 = 80KB. (80 > 72, WORSE!)
          --> STOP. Adding F2 makes the ratio increase.

  Result: pick [F5, F4, F3] (3 files), per_del = 72KB.
```

The ratio increases when a file is significantly larger than the average of
files already selected. This naturally prevents including already-compacted
files (which are larger than flush files) — IF the size gap is significant.

### Example (uniform flush files)

```
Before (4 flush files of 64KB each, trigger=4):

  L0: [F4:64KB] [F3:64KB] [F2:64KB] [F1:64KB]
       newest                          oldest

  PickCostBasedIntraL0Compaction:
    Add F4: compact_bytes = 64KB
    Add F3: compact_bytes = 128KB, per_del = 128/1 = 128KB
    Add F2: compact_bytes = 192KB, per_del = 192/2 = 96KB  (96 < 128, better)
    Add F1: compact_bytes = 256KB, per_del = 256/3 = 85KB  (85 < 96, better)
    No more files. Check: 4 >= trigger(4) and 85KB < 70MB. OK.

After:
  L0: [C1:256KB]    (single compacted output)
```

### Example (flush + compacted, ratio detects size gap)

```
  L0: [F8:64KB] [F7:64KB] [F6:64KB] [F5:64KB] [C1:256KB]
       newest                                     oldest (compacted)

  PickCostBasedIntraL0Compaction:
    Add F8: compact_bytes = 64KB
    Add F7: compact_bytes = 128KB, per_del = 128/1 = 128KB
    Add F6: compact_bytes = 192KB, per_del = 192/2 = 96KB  (improving)
    Add F5: compact_bytes = 256KB, per_del = 256/3 = 85KB  (improving)
    Add C1: compact_bytes = 512KB, per_del = 512/4 = 128KB (128 > 85, WORSE!)
    --> STOP before C1.

  Result: pick [F8, F7, F6, F5] — compacted file C1 is excluded.
  This works because C1 (256KB) is 4x larger than flush files (64KB).
```

### Anti-Re-Compaction Guard

The guard `compact_bytes_per_del_file < 1.1 * write_buffer_size` prevents
picking files that are already near memtable size. The idea: compacted files
should be ~write_buffer_size, so they'd push `per_del` above the guard.

```
Guard works when SST ~= write_buffer_size:

  Files: [64MB, 64MB, 64MB, 64MB]   (SST ~= WBS = 64MB)
  per_del = 256MB/3 = 85MB > 70MB   --> guard rejects --> no re-compaction
```

### Known Limitation with BlobDB

With BlobDB, SST files are ~1000x smaller than `write_buffer_size`. The guard
threshold (e.g., 70MB) is never reached by any L0 file. ALL files pass the
guard, including previously compacted files:

```
Guard FAILS when SST << write_buffer_size (BlobDB):

  write_buffer_size = 64MB, SST files ~64KB (1000x smaller)
  Guard threshold: 1.1 * 64MB = 70.4MB

  10 compacted files of 256KB each:
    per_del = 2560KB/9 = 284KB << 70.4MB --> guard passes!
    ALL 10 files re-compacted into 1 file of 2.56MB

  Result: cascading re-compaction creates "monster files"

  Round 1: [64KB, 64KB, 64KB, 64KB] --> compact --> [256KB]
  Round 2: [64KB, 64KB, 64KB, 256KB] --> compact ALL --> [448KB]
  Round 3: [64KB, 64KB, 64KB, 448KB] --> compact ALL --> [640KB]
  ... files grow unboundedly
```

Use the KV-ratio strategy instead for BlobDB workloads.

### Config

- `CompactionOptionsFIFO::allow_compaction` (default: false)
- Anti-re-compaction guard: `1.1 * write_buffer_size`
- Min files: `level0_file_num_compaction_trigger`

## New Strategy: `use_kv_ratio_compaction` (`PickRatioBasedIntraL0Compaction`)

**When**: `allow_compaction = true` AND `use_kv_ratio_compaction = true`
AND `max_data_files_size > 0`

This strategy replaces the `write_buffer_size`-based guard with a
**capacity-derived target** and uses **tiered size-based merging** to achieve
logarithmic write amplification. It observes the actual SST/blob size ratio,
computes a target graduated file size, and merges files incrementally through
size tiers rather than directly to target.

### Why a New Strategy?

```
Without BlobDB:  SST ~= write_buffer_size     --> old guard works
With BlobDB:     SST ~= write_buffer_size/1000 --> old guard is useless
```

The new strategy derives the target from the **data capacity** and
**observed key/value ratio**, not from `write_buffer_size`.

### Algorithm

**Step 1: Target Computation**

The target graduated file size can be determined in two ways:

```
If max_compaction_bytes > 0 (explicitly set by user):
  target = max_compaction_bytes      // user override

If max_compaction_bytes == 0 (default, auto-calculate):
  sst_ratio = total_l0_sst / (total_l0_sst + total_blob)
  total_sst_at_cap = max_data_files_size * sst_ratio
  target = total_sst_at_cap / level0_file_num_compaction_trigger
```

```
Example (auto-calculated):
  max_data_files_size = 10GB, sst_ratio = 0.001 (64KB SST / 64MB total)
  total_sst_at_cap = 10GB * 0.001 = 10MB
  trigger = 10
  target = 10MB / 10 = 1MB
```

The `sst_ratio` is **recomputed on every `PickCompaction` call**. The
computation is trivial (sum file sizes + arithmetic) and `PickCompaction`
is only called once per flush or compaction completion, so no caching is
needed. This also means the ratio naturally adapts when `SetOptions()`
changes configuration.

**Step 2: Tier Boundaries**

Tier boundaries form a geometric sequence descending from the target,
using `trigger` as the growth factor:

```
..., target/trigger^2, target/trigger, target
```

Example with target=1MB, trigger=10:
  boundaries = [10KB, 100KB, 1MB]

Boundaries below 10KB are not generated (SST files of most workloads
are larger than this). If target itself is below 10KB, it is used as
the sole boundary.

Files >= target are "graduated" and never compacted again. They sit
in L0 until FIFO drops them.

**Step 3: Tiered File Selection**

For each tier boundary (smallest first), scan L0 from oldest to newest:

```
For each boundary B (from smallest to largest):
  1. Skip files >= B (they belong to higher tiers) and being_compacted files
  2. Collect contiguous files < B
  3. Stop when accumulated >= B (cap at 2*B to prevent tier-skipping)
  4. If >= 2 files and accumulated >= B: merge them
  5. Output (~B bytes) lands at the next tier
```

Processing boundaries smallest-first ensures bottom-up build: flush outputs
are merged first, and higher-tier merges happen naturally as lower-tier
outputs accumulate.

```
Example (target=1MB, trigger=10, flush~10KB):

  Tier boundaries: [10KB, 100KB, 1MB]

  L0: [1MB_grad] [1MB_grad] [100KB] [100KB] [10KB] [10KB] [F] [F] [F] [F]

  Scan at boundary=10KB:
    F,F,F,F (all < 10KB) --> accumulated >= 10KB? If yes, merge → ~10KB output

  Scan at boundary=100KB:
    10KB,10KB,... (all < 100KB) --> accumulated >= 100KB? merge → ~100KB

  Scan at boundary=1MB:
    100KB,100KB,... (all < 1MB) --> accumulated >= 1MB? merge → graduated!
```

### Trade-Off: Write Amp vs L0 File Count

The tiered approach trades higher L0 file count for logarithmic write amp:

```
Write amp per byte:
  k + 1 = ceil(log(target/flush) / log(trigger)) + 1
  Each byte is rewritten once per tier crossing.

L0 file count at steady state:
  trigger + k * (trigger - 1)
  More than the original trigger target, but bounded logarithmically.

Example (target=1MB, flush=1KB, trigger=10):
  k = 3 tiers, write amp = 4, file count ≈ 37
  vs flat merging: write amp ≈ 57
```

### Anti-Re-Compaction Guard

The guard is implicit in the tier boundaries:

```
Graduated files (>= target) are skipped at EVERY tier boundary.
  1MB >= 1MB   --> skipped at 1MB boundary
  1MB >= 100KB --> skipped at 100KB boundary
  1MB >= 10KB  --> skipped at 10KB boundary

Intermediate tier files are only merged at HIGHER tier boundaries.
  A 100KB file (output of tier-0 merge) is:
    >= 100KB --> skipped at 100KB boundary (won't be re-merged at same tier)
    < 1MB    --> eligible at 1MB boundary (merges into graduated file)
```

Compare with the old strategy's guard:

```
Old: guard = 1.1 * write_buffer_size (breaks when SST << WBS)
New: graduated files >= target always excluded; intermediate files
     progress through tiers without cascading re-compaction
```

### Steady State

```
Steady state L0 (target=64MB, trigger=4, flush~1MB):

  [64MB_grad, 64MB_grad, 64MB_grad, 64MB_grad,
   16MB, 16MB, 16MB,
   4MB, 4MB,
   1MB, 1MB, 1MB]

  - 4 graduated files at target size (frozen until FIFO drops them)
  - Intermediate files at tier sizes (accumulating for next merge)
  - Flush outputs (accumulating for first tier merge)

When FIFO drops the oldest graduated file, it removes exactly
1/trigger of the total SST data (predictable).
```

### Write Amplification

```
With BlobDB (SST ~1KB, blob ~1MB per flush, target=1MB, trigger=10):
  - k = 3 tiers (1KB → 10KB → 100KB → 1MB)
  - SST write amp: k+1 = 4x (flush + 3 tier crossings)
  - Blob write amp: ~1x (never rewritten)
  - Total write amp: ~1 + 1KB*4/(1KB+1MB) ≈ 1.004x

Without BlobDB (SST ~64MB per flush):
  - target = large, ratio = 1, k = 1 typically
  - SST write amp: ~2x
```

### File Uniformity

At steady state, all graduated files are close to the target size.
Output is in [boundary, 2*boundary) at each tier. Variable flush sizes
are handled naturally — the size-based merge rule produces consistent
output regardless of individual file sizes.

### Config

- `CompactionOptionsFIFO::allow_compaction` (required: true)
- `CompactionOptionsFIFO::use_kv_ratio_compaction` (default: false)
- `CompactionOptionsFIFO::max_data_files_size` (required, > 0)
- `level0_file_num_compaction_trigger` (target max L0 file count)
- `max_compaction_bytes` (default: 0 = auto-calculate target from capacity;
  when > 0, overrides auto-calculated target with this value)

## Choosing Between Old and New Intra-L0 Strategies

Both strategies require `allow_compaction = true`. The choice of strategy
depends on whether BlobDB is used:

```
Decision tree:

  Want intra-L0 compaction?
    |
    +-- NO:  allow_compaction = false (default)
    |        No file merging, only dropping.
    |
    +-- YES: allow_compaction = true
             |
             +-- Using BlobDB (SST << write_buffer_size)?
             |     |
             |     +-- YES: use_kv_ratio_compaction = true
             |     |        (also requires max_data_files_size > 0)
             |     |
             |     +-- NO:  use_kv_ratio_compaction = false (default)
             |              Old strategy works fine.
```

| Criteria | Old (default) | New (`use_kv_ratio_compaction`) |
|----------|------------------------|-------------------------------|
| Guard mechanism | `1.1 * write_buffer_size` | capacity-derived target |
| Works with BlobDB? | No (guard broken) | Yes (designed for it) |
| File uniformity | Poor with BlobDB | Good (+/-25%) |
| Re-compaction risk | High with BlobDB | None (tiered boundaries prevent it) |
| Write amp (BlobDB) | Unpredictable | Logarithmic: (k+1)x SST, ~1x total |
| Requires | `allow_compaction=true` | `allow_compaction=true` + `use_kv_ratio_compaction=true` + `max_data_files_size>0` |

---

# Configuration Examples

## Basic FIFO (no intra-L0 compaction)

```cpp
options.compaction_style = kCompactionStyleFIFO;
options.compaction_options_fifo.max_table_files_size = 1ULL * 1024 * 1024 * 1024;  // 1GB
```

## FIFO with old intra-L0 (non-BlobDB)

```cpp
options.compaction_style = kCompactionStyleFIFO;
options.compaction_options_fifo.max_table_files_size = 1ULL * 1024 * 1024 * 1024;
options.compaction_options_fifo.allow_compaction = true;
options.level0_file_num_compaction_trigger = 4;
```

## FIFO with BlobDB and KV-ratio compaction

```cpp
options.compaction_style = kCompactionStyleFIFO;
options.compaction_options_fifo.max_data_files_size = 10ULL * 1024 * 1024 * 1024;  // 10GB
options.compaction_options_fifo.allow_compaction = true;   // master switch
options.compaction_options_fifo.use_kv_ratio_compaction = true;  // select new strategy
options.level0_file_num_compaction_trigger = 10;
options.enable_blob_files = true;
options.min_blob_size = 1024;
```

## FIFO with TTL + BlobDB

```cpp
options.compaction_style = kCompactionStyleFIFO;
options.compaction_options_fifo.max_data_files_size = 10ULL * 1024 * 1024 * 1024;
options.compaction_options_fifo.allow_compaction = true;
options.compaction_options_fifo.use_kv_ratio_compaction = true;
options.level0_file_num_compaction_trigger = 10;
options.ttl = 86400;  // 24 hours
options.enable_blob_files = true;
options.min_blob_size = 1024;
```

---

# Configuration Reference

## CompactionOptionsFIFO

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_table_files_size` | uint64_t | 1GB | SST-only size limit for FIFO dropping |
| `max_data_files_size` | uint64_t | 0 | Combined SST+blob size limit (0=disabled) |
| `allow_compaction` | bool | false | Master switch for intra-L0 compaction (required for both old and new strategies) |
| `use_kv_ratio_compaction` | bool | false | Select capacity-derived intra-L0 strategy (requires allow_compaction=true AND max_data_files_size>0) |
| `age_for_warm` | uint64_t | 0 | DEPRECATED |
| `file_temperature_age_thresholds` | vector | empty | Age-based temperature migration |
| `allow_trivial_copy_when_change_temperature` | bool | false | Allow trivial copy for temp change |
| `trivial_copy_buffer_size` | uint64_t | 4096 | Buffer size for trivial copy |

## Related CF Options

| Option | Relevance to FIFO |
|--------|-------------------|
| `level0_file_num_compaction_trigger` | Target max L0 file count for KV-ratio; min files for old intra-L0 |
| `ttl` | TTL-based file expiration (seconds) |
| `write_buffer_size` | Guard threshold for old-style intra-L0 (1.1x) |
| `max_compaction_bytes` | For KV-ratio: 0 = auto-calculate target from capacity; > 0 = use as target directly. For old intra-L0: cap on total input size. Default sanitized to target_file_size_base * 25 (except when use_kv_ratio_compaction=true, where 0 is preserved) |
