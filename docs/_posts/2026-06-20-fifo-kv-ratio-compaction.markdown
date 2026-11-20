---
title: "FIFO KV-Ratio Compaction for BlobDB-Backed TTL Workloads"
layout: post
author: xbw
category: blog
---

RocksDB 11.0 added `CompactionOptionsFIFO::max_data_files_size` and `CompactionOptionsFIFO::use_kv_ratio_compaction` for a specific but important shape of workload: FIFO compaction, integrated BlobDB, large values, point lookups, and data that naturally expires by TTL or by a bounded data-size budget. The implementation was added in [pull request #14326](https://github.com/meta/rocksdb/pull/14326).

The goal is to keep FIFO's low write amplification while reducing the read overhead caused by many small L0 files. The new picker uses the observed ratio between SST bytes and blob bytes to choose a stable target SST size, then moves L0 files through size tiers until they reach that target.

## Background: FIFO and BlobDB

FIFO compaction is designed for time-ordered or log-like data. All files remain in L0. When files become old enough for `ttl`, or when the configured size limit is exceeded, RocksDB drops the oldest files instead of rewriting them into lower levels. That is what keeps FIFO write amplification low.

Integrated BlobDB changes the file-size picture. Large values are stored in blob files, while SST files mostly contain keys, metadata, filters, indexes, and blob references. For point lookup workloads with large values, this can be a good fit: the SST portion can stay small and cached, and the read can fetch the large value from the blob file.

However, FIFO without intra-L0 compaction can accumulate many small L0 SST files. A point lookup may then need to probe many L0 files and many filters before finding the key. FIFO's optional intra-L0 compaction, enabled with `CompactionOptionsFIFO::allow_compaction`, addresses that by merging several small L0 SST files into fewer larger SST files. Intra-L0 compaction rewrites SST metadata only; it does not rewrite blob files.

## Why the old intra-L0 picker is not enough

The existing FIFO intra-L0 picker is cost based. It tries to reduce L0 file count while limiting how many bytes are rewritten for each file removed:

```
compact_bytes_per_del_file = total_input_bytes / (num_input_files - 1)
```

It keeps expanding the input set while that ratio improves, and it has a guard based on `write_buffer_size`:

```
compact_bytes_per_del_file < 1.1 * write_buffer_size
```

That guard assumes SST output size is roughly related to the memtable flush size. With BlobDB, that assumption often breaks. If values are large enough to be stored in blob files, a memtable flush can produce a large blob file and a much smaller SST file.

![The old picker compares compacted SST files with write_buffer_size, so BlobDB SST files can keep looking small enough to compact again](/static/images/fifo-kv-ratio-compaction/old-picker.svg)
{: style="display: block; margin-left: auto; margin-right: auto; width: 92%"}

*With large values in blob files, compacted SST files can remain far below `write_buffer_size`, so the old guard may keep allowing them into later intra-L0 compactions.*
{: style="text-align: center"}

For example, if `write_buffer_size` is 64 MB, the old guard is about 70 MB. In a BlobDB-heavy workload, a fresh SST might be tens of KB, and an already compacted SST might still be only a few MB. From the old picker's point of view, that file can still look cheap to compact even after it has already grown to a useful size.

This has two practical consequences:

* SST files can keep growing without a workload-appropriate target.
* FIFO dropping becomes less predictable, because TTL and size-based reclamation happen at file granularity.

The problem is not that an SST can be compacted more than once. The problem is that the old picker has no BlobDB-aware definition of when an L0 SST file should stop growing.

## The KV-ratio target

The new picker starts from a different question: given the current ratio of SST bytes to blob bytes, how large should a mature L0 SST file be when the database is near its configured data-size budget?

When `max_compaction_bytes` is left at `0`, RocksDB computes the target as:

```
sst_ratio = total_l0_sst / (total_l0_sst + total_blob)
target_sst_size = max_data_files_size * sst_ratio /
                  level0_file_num_compaction_trigger
```

If `max_compaction_bytes` is set to a non-zero value, RocksDB uses it directly as the target SST size. That is an advanced override for users who want to choose the graduated file size explicitly.

An SST file at or above the target is considered graduated. Graduated files stay in L0 until FIFO drops them; they are not repeatedly pulled into future KV-ratio intra-L0 compactions.

## Tiered merging

The picker does not try to merge every small SST directly to the final target size. Instead, it builds geometric size tiers using `level0_file_num_compaction_trigger` as the growth factor.

Suppose the target SST size is 1 MB and `level0_file_num_compaction_trigger` is 10. The tiers look like:

```
10 KB -> 100 KB -> 1 MB
```

Small flush outputs are merged into the first useful tier. Once enough files accumulate in that tier, they are merged into the next tier. Once a file reaches the target, it graduates.

![The KV-ratio picker moves files through size tiers until they reach a BlobDB-aware target SST size](/static/images/fifo-kv-ratio-compaction/kv-ratio-tiering.svg)
{: style="display: block; margin-left: auto; margin-right: auto; width: 92%"}

*The target comes from the observed SST/blob ratio and the configured data-size budget, not from `write_buffer_size`. Files move through bounded tiers and then graduate.*
{: style="text-align: center"}

Tiering is the trade-off. It allows some intermediate L0 files to exist so that write amplification for SST metadata grows logarithmically with the ratio between the target size and the flush SST size, rather than forcing a large merge every time. Blob files are not rewritten by this intra-L0 compaction path, so when blob bytes dominate the database size, total write amplification remains close to FIFO's original design goal.

## Configuration

Use KV-ratio compaction when all of these are true:

* The column family uses FIFO compaction.
* Integrated BlobDB stores a large fraction of total data.
* SST files are much smaller than `write_buffer_size`.
* Point lookups are sensitive to the number of L0 files.
* Data expires through TTL or through a bounded data-size budget.

A typical configuration looks like this:

```cpp
Options options;
options.compaction_style = kCompactionStyleFIFO;

// Use a combined SST + blob size budget for FIFO trimming.
options.compaction_options_fifo.max_data_files_size =
    10ULL * 1024 * 1024 * 1024;  // 10 GB

// Enable FIFO intra-L0 compaction and select the KV-ratio picker.
options.compaction_options_fifo.allow_compaction = true;
options.compaction_options_fifo.use_kv_ratio_compaction = true;

// Controls both the L0 file-count trigger and the tier growth factor.
options.level0_file_num_compaction_trigger = 10;

// BlobDB stores values at or above min_blob_size in blob files.
options.enable_blob_files = true;
options.min_blob_size = 1024;

// Optional: expire old files by age.
options.ttl = 24 * 60 * 60;
```

`max_data_files_size` is important for BlobDB-backed FIFO because it counts SST files and blob files together. When it is zero, FIFO uses the older `max_table_files_size` behavior, which only accounts for SST files. For KV-ratio compaction, `max_data_files_size` must be non-zero and should be at least as large as `max_table_files_size`; otherwise RocksDB falls back to the old cost-based intra-L0 picker.

Leave `max_compaction_bytes` at `0` unless you want to override the target SST size directly. With KV-ratio compaction, `0` means "derive the target from `max_data_files_size` and the observed SST/blob ratio."

## What to expect

In steady state, L0 should look less like a long tail of tiny flush SSTs and less like a few oversized SSTs. Instead, it should contain a bounded set of tier files plus graduated SST files near the target size.

The expected benefits are:

* Fewer tiny L0 SST files to probe during point lookup.
* More predictable graduated SST sizes.
* Smoother TTL and size-based FIFO dropping, since files are closer to a known target size.
* Bounded SST metadata rewrite cost, while blob files are left untouched.

There are also trade-offs:

* The picker can leave intermediate tier files in L0, so steady-state L0 file count can be higher than a strategy that always merges directly to the final target.
* SST metadata can still be rewritten across tiers.
* This is intended for BlobDB-heavy FIFO workloads. If SST files are already close to `write_buffer_size`, the default cost-based FIFO intra-L0 picker may be simpler and sufficient.

KV-ratio compaction does not change FIFO's reclamation model. FIFO still reclaims space by dropping whole files. The new picker only shapes the SST side of L0 so reads do less work and file dropping is easier to reason about for BlobDB-backed TTL workloads.
