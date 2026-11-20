---
title: Resumable Remote Compaction
layout: post
author: hx235
category: blog
---

## Background

RocksDB can offload compaction work to remote workers through the `CompactionService` API. In this model, the **primary RocksDB instance** selects the input files and sends a serialized `CompactionServiceInput` to a worker; the **remote worker** runs `DB::OpenAndCompact()`, writes output SSTs to `output_directory`, and returns a serialized `CompactionServiceResult` that the primary RocksDB instance installs into its LSM tree. See the [Remote Compaction wiki](https://github.com/meta/rocksdb/wiki/Remote-Compaction) for the full architecture. This lets operators scale compaction throughput with stateless workers while keeping the primary RocksDB instance's CPU and I/O available for serving reads and writes. However, remote compaction jobs can be long-running—sometimes processing hundreds of gigabytes of input. When a worker crashes, gets preempted, or times out, the entire compaction must restart from scratch, wasting all output produced before the interruption and increasing compaction debt on the primary RocksDB instance.

## How Resumable Remote Compaction Works

Resumable remote compaction introduces a **checkpoint-and-resume** mechanism. During a compaction, the worker periodically saves its progress to the `output_directory`. If the compaction is interrupted, a subsequent call to `OpenAndCompact()` with the same output directory can pick up from the last checkpoint rather than starting over.

### Checkpointing

After each output SST file is completed, the worker persists a progress checkpoint to a **compaction progress file** in the output directory `output_directory`. The checkpoint records which internal key to resume from and the metadata of all completed output files. Progress records use **delta encoding**—each record only contains files completed since the last checkpoint—to keep serialization cost linear.

![Checkpointing overview](/static/images/resumable-remote-compaction/checkpointing-overview.svg)

The worker skips checkpointing at boundaries where resuming could be unsafe or requires complicated handling: when range deletions span the file boundary or when adjacent output files share the same user key. These constraints ensure that resuming produces the same results as if the compaction was not interrupted.

### Resuming

When `OpenAndCompact()` is called with `allow_resumption = true`, it scans the output directory for a valid progress file. If one is found, it loads the checkpointed state, seeks the input iterator to the recorded resume key, restores the output file state, and continues compaction from that point. If the progress file is corrupted or missing, the system falls back to a fresh compaction by cleaning the directory.

![Resume flow](/static/images/resumable-remote-compaction/resume-flow.svg)

## How to Enable It

On the primary RocksDB instance, set a `CompactionService` implementation on the DB options. On the remote worker, pass `allow_resumption = true` in `OpenAndCompactOptions` when calling `DB::OpenAndCompact()`. The `output_directory` must be the same across retries for resumption to work—each retry call with the same directory will automatically detect and resume from the previous checkpoint. The `REMOTE_COMPACT_RESUMED_BYTES` statistics ticker tracks the total bytes of output files reused from a previous interrupted run, giving visibility into how much work resumption saved.

```cpp
// Primary RocksDB instance
DBOptions db_options;
db_options.compaction_service = std::make_shared<MyCompactionService>();

// Remote worker
OpenAndCompactOptions options;
options.allow_resumption = true;

std::string result;
Status s = DB::OpenAndCompact(
    options,
    db_path,             // source database path
    output_directory,    // where output SSTs and progress are stored
    compaction_input,    // serialized CompactionServiceInput
    &result,             // serialized CompactionServiceResult
    override_options);
```

## Future Work

Today this feature targets remote compaction. The same checkpoint-and-resume mechanism could also support **local compaction** after a crash. The core persistence and resume logic is already in `CompactionJob`; the remaining work is to integrate it with local compaction scheduling and recovery.
