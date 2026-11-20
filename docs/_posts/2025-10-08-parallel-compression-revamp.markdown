---
title: "Parallel Compression Revamp: Dramatically Reduced CPU Overhead"
layout: post
author: pdillinger
category: blog
---

The upcoming RocksDB 10.7 release includes a major revamp of parallel compression that **dramatically reduces the feature's CPU overhead by up to 65%** while maintaining or improving throughput for compression-heavy workloads. We expect this to broaden the set of workloads that could benefit from parallel compression, especially for **bulk SST generation and remote compaction use cases** that are less sensitive to CPU responsiveness.

## Background

Parallel compression in RocksDB (`CompressionOptions::parallel_threads > 1`) allows multiple threads to compress different blocks simultaneously during SST file generation, which can significantly improve compaction throughput for workloads where compression is a bottleneck. However, the original implementation had substantial CPU overhead that often outweighed the benefits, limiting its practical adoption.

## What's New: A Complete Reimplementation

The parallel compression framework has been completely rewritten from the ground up in [pull request #13910](https://github.com/meta/rocksdb/pull/13910) to address the core inefficiencies:

### Ring Buffer Architecture
Instead of separate compression and write queues with complex thread coordination, the new implementation uses a ring buffer of blocks-in-progress that enables efficient work distribution across threads. This bounds working memory while enabling high throughput with minimal cross-thread synchronization.

![Ring Buffer Architecture](/static/images/parallel-compression/ring-buffer-architecture.svg)

### Work-Stealing Design
Previously, the calling thread could only generate uncompressed blocks, dedicated compression threads could only compress, and a writer thread could only write the SST file to storage. Now, all threads can participate in compression work in a quasi-work-stealing manner, dramatically reducing the need for threads to block waiting for work. While only one thread (the calling thread or "emit thread") can generate uncompressed SST blocks in the new implementation, feeding compression work to other threads and itself, all other threads are compatible with writing compressed blocks to storage.

### Auto-Scaling Thread Management
The ring buffer enables another key feature: auto-scaling of active threads based on ring buffer utilization. The framework intelligently wakes up idle worker threads only when there's sufficient work to justify the overhead, achieving near-maximum throughput while minimizing CPU waste from unnecessary thread wake-ups.

### Lock-Free Synchronization
The entire framework is now lock-free (and wait-free as long as compatible work units are available for each thread), based primarily on atomic operations. To cleanly pack and leverage many data fields into a single atomic value, I've developed a new `BitFields` utility API. This is proving useful for cleaning up the HyperClockCache implementation as well, and will be the topic of a later blog post.

Semaphores are used for lock-free management of idle threads (assuming a lock-free semaphore implementation, which is likely the case with `ROCKSDB_USE_STD_SEMAPHORES` but that is untrustworthy; see below).

## Performance Improvements

The results speak for themselves. Here's a comparison using `db_bench` fillseq benchmarks with various compression configurations:

### ZSTD Compression (Default Level)
Note:
* "throughput" = how quickly a given CPU-bound flush or compaction can complete
* "CPU increase" = total CPU usage in amount of time that each core was used
* "PT" = parallel_threads setting.

**Before:**
- PT=3: ~38% throughput increase for ~73% CPU increase
- PT=6: No throughput increase for ~70% CPU increase

**After:**
- PT=3: ~58% throughput increase for ~25% CPU increase
- PT=6: ~58% throughput increase for ~28% CPU increase

### High Compression Scenarios
For ZSTD compression level 8, the improvements are even more dramatic:

**Before:**
- PT=4: 2.6x throughput increase for 139% CPU increase
- PT=8: 3.6x throughput increase for 135% CPU increase

**After:**
- PT=4: 2.8x throughput increase for 114% CPU increase
- PT=8: 3.7x throughput increase for 116% CPU increase

## Compression Algorithm Optimizations

Alongside the parallel compression revamp, some optimizations have gone into the underlying compression implementations/integrations. Most notably, **LZ4HC received dramatic performance improvements** through better reuse of internal data structures between compression calls (detailed in [pull request #13805](https://github.com/meta/rocksdb/pull/13805)). A small regression in LZ4 performance from that change was fixed in [pull request #14017](https://github.com/meta/rocksdb/pull/14017).

While **ZSTD remains the gold standard** for medium-to-high compression ratios in RocksDB, these LZ4HC optimizations make it an increasingly attractive option for read-heavy workloads where LZ4's faster decompression can provide overall performance benefits.

## Production Ready

With these efficiency improvements, parallel compression is now considered **production-ready**. The feature has been thoroughly tested in both unit tests and stress testing, including validation on high-load scenarios with hundreds of concurrent compression jobs and thousands of threads.

Some notes on current limitations:
- Parallel compression is currently incompatible with `UserDefinedIndex` and with the deprecated `decouple_partitioned_filters=false` setting
- Maximum performance is available with `-DROCKSDB_USE_STD_SEMAPHORES` at compile time, though this is not currently recommended due to reported bugs in some implementations of C++20 semaphores

## Configuration Recommendations

The dramatically reduced CPU overhead means parallel compression is now viable for a broader range of workloads, particularly those using higher compression levels or compression-heavy scenarios like time-series data. However, simply enabling parallel compression could result in more *spiky* CPU loads for hosts serving live DB data. **Parallel compression might be most useful for bulk SST file generation and/or remote compaction workloads** because they are less sensitive to CPU responsiveness. In these scenarios there is little danger in setting `parallel_threads=8` even with the possibility of over-subscribing CPU cores, though the potentially safer "sweet spot" is typically around `parallel_threads=3`, depending on compression level, etc.

## Limitations and Future

Although this offers a great improvement in the implementation of an existing option, we recognize that this setup is suboptimal in a number of ways:
* There is no work sharing / thread pooling for these SST compression/writer threads among compactions in the same process, so not well able to fit the workload to available CPU cores and not able to use other SST file compression work to avoid a worker thread going to sleep.
* We are not (yet) using a framework that would allow micro-work sharing with things other than SST generation on a set of threads. That would be a good direction for effective sharing of CPU resources without spikes in usage, but might incur intolerable CPU overhead in managing work. With this "hand optimized" and specialized framework, we can at least evaluate such future endeavors against a perhaps ideal framework in terms of parallelizing with minimal overhead.

## Try It Out

Parallel compression revamp will be available in RocksDB 10.7. As always, we recommend testing in your specific environment to determine the optimal configuration for your workload.
