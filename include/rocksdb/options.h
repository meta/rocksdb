// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/advanced_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/customizable.h"
#include "rocksdb/data_structure.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/listener.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/types.h"
#include "rocksdb/universal_compaction.h"
#include "rocksdb/version.h"
#include "rocksdb/write_buffer_manager.h"

#ifdef max  // ODR-SAFE
#undef max
#endif

namespace ROCKSDB_NAMESPACE {

class Cache;
class CompactionFilter;
class CompactionFilterFactory;
class Comparator;
class ConcurrentTaskLimiter;
class Env;
enum InfoLogLevel : unsigned char;
class SstFileManager;
class FilterPolicy;
class Logger;
class MergeOperator;
class Snapshot;
class MemTableRepFactory;
class RateLimiter;
class ReadScopedBlockBufferProvider;
class Slice;
class Statistics;
class InternalKeyComparator;
class WalFilter;
class FileSystem;
class IndexFactory;
using UserDefinedIndexFactory = IndexFactory;
class IODispatcher;

struct Options;
struct DbPath;

using FileTypeSet = SmallEnumSet<FileType, FileType::kBlobFile>;
using CompactionStyleSet =
    SmallEnumSet<CompactionStyle, CompactionStyle::kCompactionStyleNone>;

struct ColumnFamilyOptions : public AdvancedColumnFamilyOptions {
  // The function recovers options to a previous version. Only 4.6 or later
  // versions are supported.
  // NOT MAINTAINED: This function has not been and is not maintained.
  // DEPRECATED: This function might be removed in a future release.
  // In general, defaults are changed to suit broad interests. Opting
  // out of a change on upgrade should be deliberate and considered.
  ColumnFamilyOptions* OldDefaults(int rocksdb_major_version = 4,
                                   int rocksdb_minor_version = 6);

  // Some functions that make it easier to optimize RocksDB
  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  // An optional cache object is passed in to be used as the block cache
  ColumnFamilyOptions* OptimizeForSmallDb(
      std::shared_ptr<Cache>* cache = nullptr);

  // Use this if you don't need to keep the data sorted, i.e. you'll never use
  // an iterator, only Put() and Get() API calls
  //
  ColumnFamilyOptions* OptimizeForPointLookup(uint64_t block_cache_size_mb);

  // Default values for some parameters in ColumnFamilyOptions are not
  // optimized for heavy workloads and big datasets, which means you might
  // observe write stalls under some conditions. As a starting point for tuning
  // RocksDB options, use the following two functions:
  // * OptimizeLevelStyleCompaction -- optimizes level style compaction
  // * OptimizeUniversalStyleCompaction -- optimizes universal style compaction
  // Universal style compaction is focused on reducing Write Amplification
  // Factor for big data sets, but increases Space Amplification. You can learn
  // more about the different styles here:
  // https://github.com/meta/rocksdb/wiki/Rocksdb-Architecture-Guide
  // Make sure to also call IncreaseParallelism(), which will provide the
  // biggest performance gains.
  // Note: we might use more memory than memtable_memory_budget during high
  // write rate period
  ColumnFamilyOptions* OptimizeLevelStyleCompaction(
      uint64_t memtable_memory_budget = 512 * 1024 * 1024);
  ColumnFamilyOptions* OptimizeUniversalStyleCompaction(
      uint64_t memtable_memory_budget = 512 * 1024 * 1024);

  // -------------------
  // Parameters that affect behavior

  // Comparator used to define the order of keys in the table.
  // Default: a comparator that uses lexicographic byte-wise ordering
  //
  // REQUIRES: The client must ensure that the comparator supplied
  // here has the same name and orders keys *exactly* the same as the
  // comparator provided to previous open calls on the same DB.
  const Comparator* comparator = BytewiseComparator();

  // REQUIRES: The client must provide a merge operator if Merge operation
  // needs to be accessed. Calling Merge on a DB without a merge operator
  // would result in Status::NotSupported. The client must ensure that the
  // merge operator supplied here has the same name and *exactly* the same
  // semantics as the merge operator provided to previous open calls on
  // the same DB. The only exception is reserved for upgrade, where a DB
  // previously without a merge operator is introduced to Merge operation
  // for the first time. It's necessary to specify a merge operator when
  // opening the DB in this case.
  // Default: nullptr
  std::shared_ptr<MergeOperator> merge_operator = nullptr;

  // A single CompactionFilter instance to call into during compaction.
  // Allows an application to modify/delete a key-value during background
  // compaction.
  //
  // If the client requires a new `CompactionFilter` to be used for different
  // compaction runs and/or requires a `CompactionFilter` for table file
  // creations outside of compaction, it can specify compaction_filter_factory
  // instead of this option.  The client should specify only one of the two.
  // compaction_filter takes precedence over compaction_filter_factory if
  // client specifies both.
  //
  // If multithreaded compaction is being used, the supplied CompactionFilter
  // instance may be used from different threads concurrently and so should be
  // thread-safe.
  //
  // Default: nullptr
  const CompactionFilter* compaction_filter = nullptr;

  // This is a factory that provides `CompactionFilter` objects which allow
  // an application to modify/delete a key-value during table file creation.
  //
  // Unlike the `compaction_filter` option, which is used when compaction
  // creates a table file, this factory allows using a `CompactionFilter` when a
  // table file is created for various reasons. The factory can decide what
  // `TableFileCreationReason`s use a `CompactionFilter`. For compatibility, by
  // default the decision is to use a `CompactionFilter` for
  // `TableFileCreationReason::kCompaction` only.
  //
  // Each thread of work involving creating table files will create a new
  // `CompactionFilter` when it will be used according to the above
  // `TableFileCreationReason`-based decision. This allows the application to
  // know about the different ongoing threads of work and makes it unnecessary
  // for `CompactionFilter` to provide thread-safety.
  //
  // Default: nullptr
  std::shared_ptr<CompactionFilterFactory> compaction_filter_factory = nullptr;

  // -------------------
  // Parameters that affect performance

  // Amount of data to build up in memory (backed by an unsorted log
  // on disk) before converting to a sorted on-disk file.
  //
  // Larger values increase performance, especially during bulk loads.
  // Up to max_write_buffer_number write buffers may be held in memory
  // at the same time,
  // so you may wish to adjust this parameter to control memory usage.
  // Also, a larger write buffer will result in a longer recovery time
  // the next time the database is opened.
  //
  // Note that write_buffer_size is enforced per column family.
  // See db_write_buffer_size for sharing memory across column families.
  //
  // Default: 64MB
  //
  // Dynamically changeable through SetOptions() API
  size_t write_buffer_size = 64 << 20;

  // Compress blocks using the specified compression algorithm.
  //
  // Default: kLZ4Compression if support is compiled in, else kSnappyCompression
  // if support is compiled in, else kNoCompression
  //
  // Typical single-core speed of kLZ4Compression on an AMD EPYC-Genoa
  //     ~800 -  1200 MB/s compression
  //    ~8000 - 16000 MB/s decompression
  // and with a 160 threads to saturate the cores:
  //      ~60 -    90 GB/s compression
  //     ~900 -  1000 GB/s decompression
  // using db_bench compress/uncompress benchmarks.
  //
  // Note that these speeds are significantly faster than most
  // persistent storage speeds, and therefore it is typically never
  // worth switching to kNoCompression.  Even if the input data is
  // incompressible, the compression implementation will
  // efficiently detect that and fall back on no compression.
  //
  // See CompressionOptions for more options.
  //
  // Dynamically changeable through SetOptions() API
  CompressionType compression;

  // Compression algorithm that will be used for the bottommost level that
  // contain files. The behavior for num_levels = 1 is not well defined.
  // Right now, with num_levels = 1,  all compaction outputs will use
  // bottommost_compression and all flush outputs still use options.compression,
  // but the behavior is subject to change.
  //
  // Default: kDisableCompressionOption (Disabled)
  CompressionType bottommost_compression = kDisableCompressionOption;

  // different options for compression algorithms used by bottommost_compression
  // if it is enabled. To enable it, please see the definition of
  // CompressionOptions. Behavior for num_levels = 1 is the same as
  // options.bottommost_compression.
  CompressionOptions bottommost_compression_opts;

  // different options for compression algorithms
  CompressionOptions compression_opts;

  // EXPERIMENTAL
  // Customized compression through a callback interface. When non-nullptr,
  // supersedes the above compression options, except that the above options are
  // still processed as they historically would be and passed to
  // CompressionManager::GetCompressorForSST as hints or suggestions. See
  // advanced_compression.h
  std::shared_ptr<CompressionManager> compression_manager;

  // Number of files to trigger level-0 compaction. A value <0 means that
  // level-0 compaction will not be triggered by number of files at all.
  //
  // Universal compaction: RocksDB will try to keep the number of sorted runs
  //   no more than this number. If CompactionOptionsUniversal::max_read_amp is
  //   set, then this option will be used only as a trigger to look for
  //   compaction. CompactionOptionsUniversal::max_read_amp will be the limit
  //   on the number of sorted runs.
  //
  // Default: 4
  //
  // Dynamically changeable through SetOptions() API
  int level0_file_num_compaction_trigger = 4;

  // If non-nullptr, use the specified function to put keys in contiguous
  // groups called "prefixes". These prefixes are used to place one
  // representative entry for the group into the Bloom filter
  // rather than an entry for each key (see whole_key_filtering).
  // Under certain conditions, this enables optimizing some range queries
  // (Iterators) in addition to some point lookups (Get/MultiGet).
  //
  // Together `prefix_extractor` and `comparator` must satisfy one essential
  // property for valid prefix filtering of range queries:
  //   If Compare(k1, k2) <= 0 and Compare(k2, k3) <= 0 and
  //      InDomain(k1) and InDomain(k3) and prefix(k1) == prefix(k3),
  //   Then InDomain(k2) and prefix(k2) == prefix(k1)
  //
  // In other words, all keys with the same prefix must be in a contiguous
  // group by comparator order, and cannot be interrupted by keys with no
  // prefix ("out of domain"). (This makes it valid to conclude that no
  // entries within some bounds are present if the upper and lower bounds
  // have a common prefix and no entries with that same prefix are present.)
  //
  // Some other properties are recommended but not strictly required. Under
  // most sensible comparators, the following will need to hold true to
  // satisfy the essential property above:
  // * "Prefix is a prefix": key.starts_with(prefix(key))
  // * "Prefixes preserve ordering": If Compare(k1, k2) <= 0, then
  //   Compare(prefix(k1), prefix(k2)) <= 0
  //
  // The next two properties ensure that seeking to a prefix allows
  // enumerating all entries with that prefix:
  // * "Prefix starts the group": Compare(prefix(key), key) <= 0
  // * "Prefix idempotent": prefix(prefix(key)) == prefix(key)
  //
  // Default: nullptr
  std::shared_ptr<const SliceTransform> prefix_extractor = nullptr;

  // Control maximum total data size for a level.
  // max_bytes_for_level_base is the max total for level-1.
  // Maximum number of bytes for level L can be calculated as
  // (max_bytes_for_level_base) * (max_bytes_for_level_multiplier ^ (L-1))
  // For example, if max_bytes_for_level_base is 200MB, and if
  // max_bytes_for_level_multiplier is 10, total data size for level-1
  // will be 200MB, total file size for level-2 will be 2GB,
  // and total file size for level-3 will be 20GB.
  //
  // Default: 256MB.
  //
  // Dynamically changeable through SetOptions() API
  uint64_t max_bytes_for_level_base = 256 * 1048576;

  // Disable automatic compactions. Manual compactions can still
  // be issued on this column family
  //
  // Dynamically changeable through SetOptions() API
  bool disable_auto_compactions = false;

  // This is a factory that provides TableFactory objects.
  // Default: a block-based table factory that provides a default
  // implementation of TableBuilder and TableReader with default
  // BlockBasedTableOptions.
  std::shared_ptr<TableFactory> table_factory;

  // A list of paths where SST files for this column family
  // can be put into, with its target size. Similar to db_paths,
  // newer data is placed into paths specified earlier in the
  // vector while older data gradually moves to paths specified
  // later in the vector.
  // Note that, if a path is supplied to multiple column
  // families, it would have files and total size from all
  // the column families combined. User should provision for the
  // total size(from all the column families) in such cases.
  //
  // If left empty, db_paths will be used.
  // Default: empty
  std::vector<DbPath> cf_paths;

  // Compaction concurrent thread limiter for the column family.
  // If non-nullptr, use given concurrent thread limiter to control
  // the max outstanding compaction tasks. Limiter can be shared with
  // multiple column families across db instances.
  //
  // Default: nullptr
  std::shared_ptr<ConcurrentTaskLimiter> compaction_thread_limiter = nullptr;

  // If non-nullptr, use the specified factory for a function to determine the
  // partitioning of sst files. This helps compaction to split the files
  // on interesting boundaries (key prefixes) to make propagation of sst
  // files less write amplifying (covering the whole key space).
  // THE FEATURE IS STILL EXPERIMENTAL
  //
  // Default: nullptr
  std::shared_ptr<SstPartitionerFactory> sst_partitioner_factory = nullptr;

  // RocksDB will try to flush the current memtable after the number of range
  // deletions is >= this limit. For workloads with many range
  // deletions, limiting the number of range deletions in memtable can help
  // prevent performance degradation and/or OOM caused by too many range
  // tombstones in a single memtable.
  //
  // Default: 0 (disabled)
  //
  // Dynamically changeable through SetOptions() API
  uint32_t memtable_max_range_deletions = 0;

  // EXPERIMENTAL
  // When > 0, RocksDB attempts to erase some block cache entries for files
  // that have become obsolete, which means they are about to be deleted.
  // To avoid excessive tracking, this "uncaching" process is iterative and
  // speculative, meaning it could incur extra background CPU effort if the
  // file's blocks are generally not cached. A larger number indicates more
  // willingness to spend CPU time to maximize block cache hit rates by
  // erasing known-obsolete entries.
  //
  // When uncache_aggressiveness=1, block cache entries for an obsolete file
  // are only erased until any attempted erase operation fails because the
  // block is not cached. Then no further attempts are made to erase cached
  // blocks for that file.
  //
  // For larger values, erasure is attempted until evidence incidates that the
  // chance of success is < 0.99^(a-1), where a = uncache_aggressiveness. For
  // example:
  // 2 -> Attempt only while expecting >= 99% successful/useful erasure
  // 11 -> 90%
  // 69 -> 50%
  // 110 -> 33%
  // 230 -> 10%
  // 460 -> 1%
  // 690 -> 0.1%
  // 1000 -> 1 in 23000
  // 10000 -> Always (for all practical purposes)
  // NOTE: UINT32_MAX and nearby values could take additional special meanings
  // in the future.
  //
  // Pinned cache entries (guaranteed present) are always erased if
  // uncache_aggressiveness > 0, but are not used in predicting the chances of
  // successful erasure of non-pinned entries.
  //
  // NOTE: In the case of copied DBs (such as Checkpoints) sharing a block
  // cache, it is possible that a file becoming obsolete doesn't mean its
  // block cache entries (shared among copies) are obsolete. Such a scenerio
  // is the best case for uncache_aggressiveness = 0.
  //
  // When using allow_mmap_reads=true, this option is ignored (no un-caching).
  //
  // Once validated in production, the default will likely change to something
  // around 300.
  uint32_t uncache_aggressiveness = 0;

  // Create ColumnFamilyOptions with default values for all fields
  ColumnFamilyOptions();
  // Create ColumnFamilyOptions from Options
  explicit ColumnFamilyOptions(const Options& options);

  void Dump(Logger* log) const;
};

enum class WALRecoveryMode : char {
  // Original levelDB recovery
  //
  // We tolerate the last record in any log to be incomplete due to a crash
  // while writing it. Zeroed bytes from preallocation are also tolerated in the
  // trailing data of any log.
  //
  // Use case: Applications for which updates, once applied, must not be rolled
  // back even after a crash-recovery. In this recovery mode, RocksDB guarantees
  // this as long as `WritableFile::Append()` writes are durable. In case the
  // user needs the guarantee in more situations (e.g., when
  // `WritableFile::Append()` writes to page cache, but the user desires this
  // guarantee in face of power-loss crash-recovery), RocksDB offers various
  // mechanisms to additionally invoke `WritableFile::Sync()` in order to
  // strengthen the guarantee.
  //
  // This differs from `kPointInTimeRecovery` in that, in case a corruption is
  // detected during recovery, this mode will refuse to open the DB. Whereas,
  // `kPointInTimeRecovery` will stop recovery just before the corruption since
  // that is a valid point-in-time to which to recover.
  kTolerateCorruptedTailRecords = 0x00,
  // Recover from clean shutdown
  // We don't expect to find any corruption in the WAL
  // Use case : This is ideal for unit tests and rare applications that
  // can require high consistency guarantee
  kAbsoluteConsistency = 0x01,
  // Recover to point-in-time consistency (default)
  // We stop the WAL playback on discovering WAL inconsistency
  // Use case : Ideal for systems that have disk controller cache like
  // hard disk, SSD without super capacitor that store related data
  kPointInTimeRecovery = 0x02,
  // Recovery after a disaster
  // We ignore any corruption in the WAL and try to salvage as much data as
  // possible
  // Use case : Ideal for last ditch effort to recover data or systems that
  // operate with low grade unrelated data
  kSkipAnyCorruptedRecords = 0x03,
};

struct DbPath {
  std::string path;
  uint64_t target_size;  // Target size of total files under the path, in byte.

  DbPath() : target_size(0) {}
  DbPath(const std::string& p, uint64_t t) : path(p), target_size(t) {}
};

extern const char* const kHostnameForDbHostId;

enum class CompactionServiceJobStatus : char {
  kSuccess,
  kFailure,
  kAborted,
  kUseLocal,
};

struct CompactionServiceJobInfo {
  std::string db_name;
  std::string db_id;
  std::string db_session_id;

  // the id of the column family where the compaction happened.
  uint32_t cf_id;
  // the name of the column family where the compaction happened.
  std::string cf_name;

  uint64_t job_id;  // job_id is only unique within the current DB and session,
                    // restart DB will reset the job_id. `db_id` and
                    // `db_session_id` could help you build unique id across
                    // different DBs and sessions.

  Env::Priority priority;

  // Additional Compaction Details that can be useful in the CompactionService
  CompactionReason compaction_reason;
  bool is_full_compaction;
  bool is_manual_compaction;
  bool bottommost_level;

  // the smallest input level of the compaction.
  // (same as Compaction::start_level and CompactionJobInfo::base_input_level)
  int base_input_level;
  // the output level of the compaction.
  int output_level;

  // The scheduled request requires merge-before-filter worker support.
  bool merge_before_filter = false;

  CompactionServiceJobInfo() {}
  CompactionServiceJobInfo(std::string db_name_, std::string db_id_,
                           std::string db_session_id_, uint32_t cf_id_,
                           std::string cf_name_, uint64_t job_id_,
                           Env::Priority priority_,
                           CompactionReason compaction_reason_,
                           bool is_full_compaction_, bool is_manual_compaction_,
                           bool bottommost_level_, int base_input_level_,
                           int output_level_)
      : db_name(std::move(db_name_)),
        db_id(std::move(db_id_)),
        db_session_id(std::move(db_session_id_)),
        cf_id(cf_id_),
        cf_name(std::move(cf_name_)),
        job_id(job_id_),
        priority(priority_),
        compaction_reason(compaction_reason_),
        is_full_compaction(is_full_compaction_),
        is_manual_compaction(is_manual_compaction_),
        bottommost_level(bottommost_level_),
        base_input_level(base_input_level_),
        output_level(output_level_) {}
};

struct CompactionServiceScheduleResponse {
  std::string scheduled_job_id;  // Generated outside of primary host, unique
                                 // across different DBs and sessions
  CompactionServiceJobStatus status;
  CompactionServiceScheduleResponse(std::string scheduled_job_id_,
                                    CompactionServiceJobStatus status_)
      : scheduled_job_id(scheduled_job_id_), status(status_) {}
  explicit CompactionServiceScheduleResponse(CompactionServiceJobStatus status_)
      : status(status_) {}
};

// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class CompactionService : public Customizable {
 public:
  static const char* Type() { return "CompactionService"; }

  // Returns the name of this compaction service.
  const char* Name() const override = 0;

  // Declares support for transporting merge-before-filter requests and their
  // capability acknowledgments. Called without the DB mutex; must be bounded,
  // thread-safe, non-reentrant, and perform no I/O or waits. Old workers ignore
  // the request field and return no applied-order acknowledgment; RocksDB then
  // falls back to local compaction before importing output. Services can also
  // decline incompatible workers with kUseLocal before execution.
  // The default routes new-order jobs locally.
  virtual bool SupportsMergeBeforeFilter() const { return false; }

  // Schedule compaction to be processed remotely.
  virtual CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& /*info*/,
      const std::string& /*compaction_service_input*/) {
    CompactionServiceScheduleResponse response(
        CompactionServiceJobStatus::kUseLocal);
    return response;
  }

  // Wait for the scheduled compaction to finish from the remote worker and
  // return its status; `result` is the serialized CompactionServiceResult from
  // the worker's DB::OpenAndCompact.
  //
  // DB::OpenAndCompact may return Status::Incomplete to *decline* a scheduled
  // job rather than fail it -- for example, when the MANIFEST floor check finds
  // a stale worker view, the reserved BlobDB file-number range is exhausted, or
  // the worker's BlobDB capability/options do not match the primary's. Whether
  // to map that to kUseLocal (fall back to a local compaction) or kFailure
  // (surface it) is up to the integrator; both are valid. (Cancellation also
  // surfaces as Status::Incomplete, distinguishable by SubCode
  // kManualCompactionPaused.)
  virtual CompactionServiceJobStatus Wait(
      const std::string& /*scheduled_job_id*/, std::string* /*result*/) {
    return CompactionServiceJobStatus::kUseLocal;
  }

  // Cancel awaiting jobs. Called by CancelAllBackgroundWork()
  virtual void CancelAwaitingJobs() {}

  // Optional callback function upon Installation.
  virtual void OnInstallation(const std::string& /*scheduled_job_id*/,
                              CompactionServiceJobStatus /*status*/) {}

  ~CompactionService() override = default;
};

// Canonical name for the DB session temporary directory, which can be used as
// the remote compaction output root when enabled.
inline constexpr char kSessionTmpDirName[] = "session_tmp";

struct DBOptions {
  // The function recovers options to the option as in version 4.6.
  // NOT MAINTAINED: This function has not been and is not maintained.
  // DEPRECATED: This function might be removed in a future release.
  // In general, defaults are changed to suit broad interests. Opting
  // out of a change on upgrade should be deliberate and considered.
  DBOptions* OldDefaults(int rocksdb_major_version = 4,
                         int rocksdb_minor_version = 6);

  // Some functions that make it easier to optimize RocksDB

  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  // An optional cache object is passed in for the memory of the
  // memtable to cost to
  DBOptions* OptimizeForSmallDb(std::shared_ptr<Cache>* cache = nullptr);

  // By default, RocksDB uses only one background thread for flush and
  // compaction. Calling this function will set it up such that total of
  // `total_threads` is used. Good value for `total_threads` is the number of
  // cores. You almost definitely want to call this function if your system is
  // bottlenecked by RocksDB.
  DBOptions* IncreaseParallelism(int total_threads = 16);

  // If true, the database will be created if it is missing.
  // Default: false
  bool create_if_missing = false;

  // If true, missing column families will be automatically created on
  // DB::Open().
  // Default: false
  bool create_missing_column_families = false;

  // If true, an error is raised if the database already exists.
  // Default: false
  bool error_if_exists = false;

  // If true, RocksDB does some pro-active and generally inexpensive checks
  // for DB or data corruption, on top of usual protections such as block
  // checksums. True also enters a read-only mode when a DB write fails;
  // see DB::Resume().
  //
  // When set to true, the DB will fail to open if any SST files fail to open
  // e.g. due to incorrect file size or corrupted footer.
  //
  // When set to false, when there are files corrupted, the DB will still be
  // opened, and the healthy ones could still be accessed, while corrupted one
  // will not
  //
  // As most workloads value data correctness over availability, this option
  // is on by default. Note that the name of this old option is potentially
  // misleading, and other options and operations go further in proactive
  // checking for corruption, including
  // * paranoid_file_checks
  // * paranoid_memory_checks
  // * memtable_verify_per_key_checksum_on_seek
  // * DB::VerifyChecksum()
  //
  // Default: true
  bool paranoid_checks = true;

  // DEPRECATED: This option might be removed in a future release.
  //
  // If true, during memtable flush, RocksDB will validate total entries
  // read in flush, total entries written in the SST and compare them with
  // counter of keys added.
  //
  // The option is here to turn the feature off in case this new validation
  // feature has a bug. The option may be removed in the future once the
  // feature is stable.
  //
  // Default: true
  bool flush_verify_memtable_count = true;

  // DEPRECATED: This option might be removed in a future release.
  //
  // If true, during compaction, RocksDB will count the number of entries
  // read and compare it against the number of entries in the compaction
  // input files. This is intended to add protection against corruption
  // during compaction. Note that
  // - this verification is not done for compactions during which a compaction
  // filter returns kRemoveAndSkipUntil, and
  // - the number of range deletions is not verified.
  //
  // The option is here to turn the feature off in case this new validation
  // feature has a bug. The option may be removed in the future once the
  // feature is stable.
  //
  // Default: true
  bool compaction_verify_record_count = true;

  // If true, the log numbers and sizes of the synced WALs are tracked
  // in MANIFEST. During DB recovery, if a synced WAL is missing
  // from disk, or the WAL's size does not match the recorded size in
  // MANIFEST, an error will be reported and the recovery will be aborted.
  //
  // This is one additional protection against WAL corruption besides the
  // per-WAL-entry checksum.
  //
  // Note that this option does not work with secondary instance.
  // Currently, only syncing closed WALs are tracked. Calling `DB::SyncWAL()`,
  // etc. or writing with `WriteOptions::sync=true` to sync the live WAL is not
  // tracked for performance/efficiency reasons.
  //
  // Default: false
  bool track_and_verify_wals_in_manifest = false;

  // EXPERIMENTAL
  //
  // If true, each new WAL will record various information about its predecessor
  // WAL for verification on the predecessor WAL during WAL recovery.
  //
  // It verifies the following:
  // 1. There exists at least some WAL in the DB
  // - It's not compatible with `RepairDB()` since this option imposes a
  // stricter requirement on WAL than the DB went through `RepariDB()` can
  // normally meet
  // 2. There exists no WAL hole where new WAL data presents while some old WAL
  // data not yet obsolete is missing. The DB manifest indicates which WALs are
  // obsolete.
  //
  // This is intended to be a better replacement to
  // `track_and_verify_wals_in_manifest`.
  //
  // Default: false
  bool track_and_verify_wals = false;

  // If true, verifies the SST unique id between MANIFEST and actual file
  // each time an SST file is opened. This check ensures an SST file is not
  // overwritten or misplaced. A corruption error will be reported if mismatch
  // detected, but only when MANIFEST tracks the unique id, which starts from
  // RocksDB version 7.3. Although the tracked internal unique id is related
  // to the one returned by GetUniqueIdFromTableProperties, that is subject to
  // change.
  // NOTE: verification is currently only done on SST files using block-based
  // table format.
  //
  // Setting to false should only be needed in case of unexpected problems.
  //
  // Although an early version of this option opened all SST files for
  // verification on DB::Open, that is no longer guaranteed. However, as
  // documented in an above option, if max_open_files is -1, DB will open all
  // files on DB::Open().
  //
  // Default: true
  bool verify_sst_unique_id_in_manifest = true;

  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  Env* env = Env::Default();

  // Limits internal file read/write bandwidth:
  //
  // - Flush requests write bandwidth at `Env::IOPriority::IO_HIGH`
  // - Compaction requests read and write bandwidth at
  //   `Env::IOPriority::IO_LOW`
  // - Reads associated with a `ReadOptions` can be charged at
  //   `ReadOptions::rate_limiter_priority` (see that option's API doc for usage
  //   and limitations).
  // - Writes associated with a `WriteOptions` can be charged at
  //   `WriteOptions::rate_limiter_priority` (see that option's API doc for
  //   usage and limitations).
  //
  // Rate limiting is disabled if nullptr. If rate limiter is enabled,
  // bytes_per_sync is set to 1MB by default.
  //
  // Default: nullptr
  std::shared_ptr<RateLimiter> rate_limiter = nullptr;

  // Use to track SST files and control their file deletion rate.
  //
  // Features:
  //  - Throttle the deletion rate of the SST files.
  //  - Keep track the total size of all SST files.
  //  - Set a maximum allowed space limit for SST files that when reached
  //    the DB wont do any further flushes or compactions and will set the
  //    background error.
  //  - Can be shared between multiple dbs.
  // Limitations:
  //  - Only track and throttle deletes of SST files in
  //    first db_path (db_name if db_paths is empty).
  //
  // Default: nullptr
  std::shared_ptr<SstFileManager> sst_file_manager = nullptr;

  // Any internal progress/error information generated by the db will
  // be written to info_log if it is non-nullptr, or to a file stored
  // in the same directory as the DB contents if info_log is nullptr.
  // Default: nullptr
  std::shared_ptr<Logger> info_log = nullptr;

  // Minimum level for sending log messages to info_log. The default is
  // INFO_LEVEL when RocksDB is compiled in release mode, and DEBUG_LEVEL
  // when it is compiled in debug mode.
  InfoLogLevel info_log_level = Logger::kDefaultLogLevel;

  // Number of open files that can be used by the DB.  You may need to
  // increase this if your database has a large working set. Value -1 means
  // files opened are always kept open. You can estimate number of files based
  // on target_file_size_base and target_file_size_multiplier for level-based
  // compaction. For universal-style compaction, you can usually set it to -1.
  //
  // A high value or -1 for this option can cause high memory usage.
  // See BlockBasedTableOptions::cache_usage_options to constrain
  // memory usage in case of block based table format.
  //
  // Default: -1
  //
  // Dynamically changeable through SetDBOptions() API.
  int max_open_files = -1;

  // If max_open_files is -1, DB will open all files on DB::Open(). You can
  // use this option to increase the number of threads used to open the files.
  // Default: 16
  int max_file_opening_threads = 16;

  // Requested maximum number of threads in the shared read I/O executor.
  // Opening a DB or SstFileReader can increase the executor to this size but
  // cannot reduce it. Used exclusively for asynchronous read requests (e.g.
  // GetAsync, MultiGetAsync).
  int read_io_executor_threads = 1;

  // If true, SST files are opened and validated asynchronously in the
  // background after DB::Open or DB::OpenAsSecondary returns. This reduces DB
  // open time for databases with many SST files and high latency file systems.
  // Mostly useful when max_open_files = -1, as max_open_files != -1 usually has
  // fast open times. See also `max_file_opening_threads` and
  // `skip_stats_update_on_db_open` to improve file open latency.
  //
  // A secondary with this option can no longer rely on max_open_files = -1 to
  // pin every current SST before OpenAsSecondary returns. If the primary
  // deletes a file before the background job opens it, the background job or
  // later reads can return an I/O error. TryCatchUpWithPrimary() does not clear
  // a latched asynchronous-open background error; fix the underlying problem
  // and reopen the secondary instead.
  //
  // Note: This option is currently not compatible with FIFO compaction and
  // requires skip_stats_update_on_db_open=true.
  //
  // Errors will no longer show up in the DB open call, but instead can show up
  // as either background errors and/or operations that access the file (e.g.
  // reads, compactions).
  //
  // When false (default), SST files are opened and validated during DB open.
  //
  // Default: false
  bool open_files_async = false;

  // Once write-ahead logs exceed this size, we will start forcing the flush of
  // column families whose memtables are backed by the oldest live WAL file
  // (i.e. the ones that are causing all the space amplification). If set to 0
  // (default), we will dynamically choose the WAL size limit to be
  // [sum of all write_buffer_size * max_write_buffer_number] * 4
  //
  // For example, with 15 column families, each with
  // write_buffer_size = 128 MB
  // max_write_buffer_number = 6
  // max_total_wal_size will be calculated to be [15 * 128MB * 6] * 4 = 45GB
  //
  // The RocksDB wiki has some discussion about how the WAL interacts
  // with memtables and flushing of column families.
  // https://github.com/meta/rocksdb/wiki/Column-Families
  //
  // This option takes effect only when there are more than one column
  // family as otherwise the wal size is dictated by the write_buffer_size.
  //
  // Default: 0
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t max_total_wal_size = 0;

  // If non-null, then we should collect metrics about database operations
  std::shared_ptr<Statistics> statistics = nullptr;

  // By default, writes to stable storage use fdatasync (on platforms
  // where this function is available). If this option is true,
  // fsync is used instead.
  //
  // fsync and fdatasync are equally safe for our purposes and fdatasync is
  // faster, so it is rarely necessary to set this option. It is provided
  // as a workaround for kernel/filesystem bugs, such as one that affected
  // fdatasync with ext4 in kernel versions prior to 3.7.
  bool use_fsync = false;

  // A list of paths where SST files can be put into, with its target size.
  // Newer data is placed into paths specified earlier in the vector while
  // older data gradually moves to paths specified later in the vector.
  //
  // For example, you have a flash device with 10GB allocated for the DB,
  // as well as a hard drive of 2TB, you should config it to be:
  //   [{"/flash_path", 10GB}, {"/hard_drive", 2TB}]
  //
  // The system will try to guarantee data under each path is close to but
  // not larger than the target size. But current and future file sizes used
  // by determining where to place a file are based on best-effort estimation,
  // which means there is a chance that the actual size under the directory
  // is slightly more than target size under some workloads. User should give
  // some buffer room for those cases.
  //
  // If none of the paths has sufficient room to place a file, the file will
  // be placed to the last path anyway, despite to the target size.
  //
  // Placing newer data to earlier paths is also best-efforts. User should
  // expect user files to be placed in higher levels in some extreme cases.
  //
  // If left empty, only one path will be used, which is db_name passed when
  // opening the DB.
  // Default: empty
  std::vector<DbPath> db_paths;

  // This specifies the info LOG dir.
  // If it is empty, the log files will be in the same dir as data.
  // If it is non empty, the log files will be in the specified dir,
  // and the db data dir's absolute path will be used as the log file
  // name's prefix.
  // NOTE: not for WALs
  std::string db_log_dir;

  // This specifies the absolute dir path for write-ahead logs (WAL).
  // If it is empty, the log files will be in the same dir as data,
  //   dbname is used as the data dir by default
  // If it is non empty, the log files will be in kept the specified dir.
  // When destroying the db,
  //   all log files in wal_dir and the dir itself is deleted
  std::string wal_dir;

  // The periodicity when obsolete files get deleted. The default
  // value is 6 hours. The files that get out of scope by compaction
  // process will still get automatically delete on every compaction,
  // regardless of this setting
  //
  // Default: 6 hours
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t delete_obsolete_files_period_micros = 6ULL * 60 * 60 * 1000000;

  // Maximum number of concurrent background jobs (compactions and flushes).
  //
  // Default: 2
  //
  // Dynamically changeable through SetDBOptions() API.
  int max_background_jobs = 2;

  // DEPRECATED: RocksDB automatically decides this based on the
  // value of max_background_jobs. For backwards compatibility we will set
  // `max_background_jobs = max_background_compactions + max_background_flushes`
  // in the case where user sets at least one of `max_background_compactions` or
  // `max_background_flushes` (we replace -1 by 1 in case one option is unset).
  //
  // Maximum number of concurrent background compaction jobs, submitted to
  // the default LOW priority thread pool.
  //
  // If you're increasing this, also consider increasing number of threads in
  // LOW priority thread pool. For more information, see
  // Env::SetBackgroundThreads
  //
  // Default: -1
  //
  // Dynamically changeable through SetDBOptions() API.
  int max_background_compactions = -1;

  // This value represents the maximum number of threads that will
  // concurrently perform a compaction job by breaking it into multiple,
  // smaller ones that are run simultaneously.
  // Default: 1 (i.e. no subcompactions)
  //
  // Dynamically changeable through SetDBOptions() API.
  uint32_t max_subcompactions = 1;

  // DEPRECATED: RocksDB automatically decides this based on the
  // value of max_background_jobs. For backwards compatibility we will set
  // `max_background_jobs = max_background_compactions + max_background_flushes`
  // in the case where user sets at least one of `max_background_compactions` or
  // `max_background_flushes`.
  //
  // Maximum number of concurrent background memtable flush jobs, submitted by
  // default to the HIGH priority thread pool. If the HIGH priority thread pool
  // is configured to have zero threads, flush jobs will share the LOW priority
  // thread pool with compaction jobs.
  //
  // It is important to use both thread pools when the same Env is shared by
  // multiple db instances. Without a separate pool, long running compaction
  // jobs could potentially block memtable flush jobs of other db instances,
  // leading to unnecessary Put stalls.
  //
  // If you're increasing this, also consider increasing number of threads in
  // HIGH priority thread pool. For more information, see
  // Env::SetBackgroundThreads
  // Default: -1
  int max_background_flushes = -1;

  // Specify the maximal size of the info log file. If the log file
  // is larger than `max_log_file_size`, a new info log file will
  // be created.
  // If max_log_file_size == 0, all logs will be written to one
  // log file.
  // NOTE: not for WALs
  size_t max_log_file_size = 0;

  // Time for the info log file to roll (in seconds).
  // If specified with non-zero value, log file will be rolled
  // if it has been active longer than `log_file_time_to_roll`.
  // Default: 0 (disabled)
  // NOTE: not for WALs
  size_t log_file_time_to_roll = 0;

  // Maximal info log files to be kept.
  // Default: 1000
  // NOTE: not for WALs
  size_t keep_log_file_num = 1000;

  // Recycle WAL files.
  // If non-zero, we will reuse previously written WAL files for new
  // WALs, overwriting the old data.  The value indicates how many
  // such files we will keep around at any point in time for later
  // use.  This is more efficient because the blocks are already
  // allocated and fdatasync does not need to update the inode after
  // each write.
  // Default: 0
  size_t recycle_log_file_num = 0;

  // EXPERIMENTAL: If true, RocksDB asynchronously precreates the next WAL file
  // so foreground memtable switching can usually avoid the filesystem latency
  // of creating a new WAL. The precreated file is only reserved empty storage;
  // it does not become a logical WAL and is not added to WAL tracking until it
  // is consumed by a foreground WAL rotation.
  //
  // The option is sanitized to false when recycle_log_file_num is non-zero.
  //
  // Default: false
  bool async_wal_precreate = false;

  // The manifest file is rolled over on reaching this limit AND the
  // space amp limit described in max_manifest_space_amp_pct. More trade-off
  // details there.
  //
  // NOTE: this option used to be a hard limit, but that made this a dangerous
  // tuning parameter for optimizing manifest file size because the best
  // size really depends on the DB size and average SST file size (and other
  // settings). Now it is essentially a minimum for the auto-tuned max manifest
  // file size.
  //
  // Until the max_manifest_space_amp_pct feature is fully validated to show a
  // smaller default here like 1MB is appropriate, the default value is 1GB to
  // match historical behavior (without it being a hard limit in case of giant
  // compacted manifest size).
  //
  // This option is mutable with SetDBOptions(), taking effect on the next
  // manifest write (e.g. completed DB compaction or flush).
  //
  // For MANIFEST write batches containing foreground operations like external
  // file ingestion/import, DeleteFilesInRange, CreateColumnFamily, and
  // DropColumnFamily, the effective limit is relaxed by 25% to reduce the
  // likelihood of user operations blocking on MANIFEST rotation.
  // Background-only batches (flush and compaction) use the configured or
  // auto-tuned limit directly.
  uint64_t max_manifest_file_size = 1024 * 1024 * 1024;

  // If true, on DB close, read back the entire MANIFEST file and validate
  // CRC checksums and logical record content. If corruption is detected,
  // a fresh MANIFEST is written from in-memory state before closing.
  //
  // This option is mutable with SetDBOptions().
  bool verify_manifest_content_on_close = false;

  // EXPERIMENTAL: If true, RocksDB can reduce recovery work after a clean
  // shutdown, which may reduce DB::Open latency on warm reopens, especially on
  // storage where metadata appends are expensive.
  //
  // Best-effort optimization: if it is disabled or unavailable, RocksDB falls
  // back to the standard recovery path.
  //
  // Temporary rollout / kill switch for an optimization that is intended to be
  // correct and eventually always enabled. Mutable via SetDBOptions().
  bool optimize_manifest_for_recovery = false;

  // EXPERIMENTAL: If true, DB::Open can try to reuse the existing MANIFEST
  // for the first post-open metadata update instead of creating a fresh one.
  // This can reduce warm-open latency for DBs whose MANIFEST is expensive to
  // rebuild.
  //
  // Best-effort optimization: even when enabled, RocksDB may still create a
  // fresh MANIFEST if the FileSystem does not support reopening the existing
  // MANIFEST for append, or if RocksDB decides reuse is unsafe. That fallback
  // is normal behavior.
  //
  // With very small `max_manifest_file_size` settings, the reused MANIFEST can
  // still rotate earlier than expected after open, because RocksDB may keep a
  // conservative auto-tuned rotation threshold until it later refreshes its
  // compacted-size estimate.
  //
  // Temporary rollout / kill switch while this optimization is being
  // validated.
  bool reuse_manifest_on_open = false;

  // This option mostly replaces max_manifest_file_size to control an auto-tuned
  // balance of manifest write amplification and space amplification. A new
  // manifest file is created with the "compacted" contents of the old one when
  //  current_manifest_size
  //    >
  //  max(max_manifest_file_size,
  //      est_compacted_manifest_size * (1 + max_manifest_space_amp_pct/100))
  //
  // where est_compacted_manifest_size is an estimate of how big a new compacted
  // version of the current manifest would be. Currently, the estimate used is
  // the last newly-written manifest, in its "compacted" form.
  //
  // Space amplification in the manifest file might be less of a concern for
  // primary storage space and more of a concern for DB recover time and size of
  // backup files that aren't incremental between backups. To minimize manifest
  // churn on initial DB population, setting max_manifest_file_size to something
  // not too small, like 1MB, should suffice. Similarly, write amp on the
  // manifest file is likely not a direct concern but completed compactions and
  // flushes cannot (currently) be committed while the (relatively small)
  // manifest file is being compacted. Manifest compactions should not
  // interfere with user write latency or throughput unless the DB is
  // chronically stalling or close to stalling writes already.
  //
  // For this option to have a meaningful effect, it is recommended to set
  // max_manifest_file_size to something modest like 1MB. Then we can interpret
  // values for this option as follows, starting with minimum space amp and
  // maximum write amp:
  // * 0 - Every manifest write (flush, compaction, etc.) generates a whole new
  // manifest. Only useful for testing.
  // * very small - Doesn't take many manifest writes to generate a whole new
  // manifest.
  // * 100 - In a DB with pretty consistent number of SST files, etc., achieves
  // about 1.0 write amp (writing about 2x the theoretical minimum) and a max of
  // about 1.0 space amp (manifest up to 2x the compacted size).
  // * 500 - Recommended and default: 0.2 write amp and up to roughly 5.0 space
  // amp.
  // * 10000 - 0.01 write amp and up to 100 space amp on the manifest.
  //
  // This option is mutable with SetDBOptions(), taking effect on the next
  // manifest write (e.g. completed DB compaction or flush).
  int max_manifest_space_amp_pct = 500;

  // Number of shards used for table cache.
  int table_cache_numshardbits = 6;

  // The following two fields affect when WALs will be archived and deleted.
  //
  // When both are zero, obsolete WALs will not be archived and will be deleted
  // immediately. Otherwise, obsolete WALs will be archived prior to deletion.
  //
  // When `WAL_size_limit_MB` is nonzero, archived WALs starting with the
  // earliest will be deleted until the total size of the archive falls below
  // this limit. All empty WALs will be deleted.
  //
  // When `WAL_ttl_seconds` is nonzero, archived WALs older than
  // `WAL_ttl_seconds` will be deleted.
  //
  // When only `WAL_ttl_seconds` is nonzero, the frequency at which archived
  // WALs are deleted is every `WAL_ttl_seconds / 2` seconds. When only
  // `WAL_size_limit_MB` is nonzero, the deletion frequency is every ten
  // minutes. When both are nonzero, the deletion frequency is the minimum of
  // those two values.
  uint64_t WAL_ttl_seconds = 0;
  uint64_t WAL_size_limit_MB = 0;

  // Number of bytes to preallocate (via fallocate) the manifest
  // files.  Default is 4mb, which is reasonable to reduce random IO
  // as well as prevent overallocation for mounts that preallocate
  // large amounts of data (such as xfs's allocsize option).
  size_t manifest_preallocation_size = 4 * 1024 * 1024;

  // Allow the OS to mmap file for reading sst tables.
  // Not recommended for 32-bit OS.
  // When the option is set to true and compression is disabled, the blocks
  // will not be copied and will be read directly from the mmap-ed memory
  // area, and the block will not be inserted into the block cache. However,
  // checksums will still be checked if ReadOptions.verify_checksums is set
  // to be true. It means a checksum check every time a block is read, more
  // than the setup where the option is set to false and the block cache is
  // used. The common use of the options is to run RocksDB on ramfs, where
  // checksum verification is usually not needed.
  // Default: false
  bool allow_mmap_reads = false;

  // Allow the OS to mmap file for writing.
  // DB::SyncWAL() only works if this is set to false.
  // Default: false
  bool allow_mmap_writes = false;

  // Enable direct I/O mode for read/write
  // they may or may not improve performance depending on the use case
  //
  // Files will be opened in "direct I/O" mode
  // which means that data r/w from the disk will not be cached or
  // buffered. The hardware buffer of the devices may however still
  // be used. Memory mapped files are not impacted by these parameters.

  // Use O_DIRECT for user and compaction reads.
  // Default: false
  bool use_direct_reads = false;

  // Use O_DIRECT for compaction-input SST reads only, leaving user reads
  // buffered. Useful when sequential compaction reads would otherwise evict
  // the hot user-read working set from the OS page cache. When this is true
  // and use_direct_reads is false, compaction opens short-lived O_DIRECT
  // readers for its input files instead of reusing the buffered readers cached
  // for user reads. This is the read-side analogue of
  // use_direct_io_for_flush_and_compaction, and the two are often paired on
  // write-heavy workloads.
  //
  // Scope and limits:
  //   * DBOption scope (applies to all column families); no per-CF setting.
  //   * Covers compaction inputs only. Blob-file reads and compaction-output
  //     verification (paranoid_file_checks) still use the buffered path.
  //   * The ephemeral readers bypass the TableCache and are not counted against
  //     max_open_files. Non-L0 levels keep one reader open at a time; L0 opens
  //     all of a subcompaction's overlapping inputs at once, so with large L0
  //     fan-in and many subcompactions, watch RLIMIT_NOFILE.
  //   * Every input file is reopened per compaction, so NO_FILE_OPENS and
  //     TABLE_OPEN_IO_MICROS rise while this is enabled.
  //
  // The same SST can be open through both a buffered handle (user reads) and an
  // O_DIRECT handle (the compaction scan) at once; modern Linux handles this
  // fine. The flag is neutral or slightly negative for in-memory DBs or
  // uniform random reads, so measure before enabling.
  //
  // Has no effect when use_direct_reads is true (all reads are already
  // O_DIRECT). Rejected at DB::Open when allow_mmap_reads is set.
  //
  // On a filesystem without O_DIRECT support (e.g. tmpfs), DB::Open fails: it
  // probes by opening the MANIFEST with O_DIRECT. The probe only checks the
  // filesystem holding the DB directory, so if SST files live elsewhere (via
  // db_paths/cf_paths) without O_DIRECT, Open succeeds and the first compaction
  // fails instead.
  //
  // Default: false
  bool use_direct_io_for_compaction_reads = false;

  // Use O_DIRECT for writes in background flush and compactions. See also
  // use_direct_io_for_compaction_reads, the read-side analogue often paired
  // with this on write-heavy workloads.
  // Default: false
  bool use_direct_io_for_flush_and_compaction = false;

  // If false, fallocate() calls are bypassed, which disables file
  // preallocation. The file space preallocation is used to increase the file
  // write/append performance. By default, RocksDB preallocates space for WAL,
  // SST, Manifest files, the extra space is truncated when the file is written.
  // Warning: if you're using btrfs, we would recommend setting
  // `allow_fallocate=false` to disable preallocation. As on btrfs, the extra
  // allocated space cannot be freed, which could be significant if you have
  // lots of files. More details about this limitation:
  // https://github.com/btrfs/btrfs-dev-docs/blob/471c5699336e043114d4bca02adcd57d9dab9c44/data-extent-reference-counts.md
  bool allow_fallocate = true;

  // Disable child process inherit open files. Default: true
  bool is_fd_close_on_exec = true;

  // if not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
  //
  // Default: 600 (10 min)
  //
  // Dynamically changeable through SetDBOptions() API.
  unsigned int stats_dump_period_sec = 600;

  // Minimum completed DB-wide mutex hold duration for sampled INFO logging,
  // in microseconds. Requires Statistics::kAll and an info logger. Zero
  // disables these hold probes without disabling existing statistics timers.
  // Updates apply when a tracked ownership interval ends; previously untracked
  // intervals cannot be reconstructed. Default: 0 (disabled). Set to 100000
  // for a 100 ms investigation threshold.
  // Dynamically changeable through SetDBOptions().
  uint64_t mutex_hold_log_threshold_us = 0;

  // if not zero, dump rocksdb.stats to RocksDB every stats_persist_period_sec
  // Default: 600
  unsigned int stats_persist_period_sec = 600;

  // If true, automatically persist stats to a hidden column family (column
  // family name: ___rocksdb_stats_history___) every
  // stats_persist_period_sec seconds; otherwise, write to an in-memory
  // struct. User can query through `GetStatsHistory` API.
  // If user attempts to create a column family with the same name on a DB
  // which have previously set persist_stats_to_disk to true, the column family
  // creation will fail, but the hidden column family will survive, as well as
  // the previously persisted statistics.
  // When peristing stats to disk, the stat name will be limited at 100 bytes.
  // Default: false
  bool persist_stats_to_disk = false;

  // if not zero, periodically take stats snapshots and store in memory, the
  // memory size for stats snapshots is capped at stats_history_buffer_size
  // Default: 1MB
  size_t stats_history_buffer_size = 1024 * 1024;

  // If set true, will hint the underlying file system that the file
  // access pattern is random, when a sst file is opened.
  // Default: true
  bool advise_random_on_open = true;

  // Amount of data to build up in memtables across all column
  // families before writing to disk.
  //
  // This is distinct from write_buffer_size, which enforces a limit
  // for a single memtable.
  //
  // This feature is disabled by default. Specify a non-zero value
  // to enable it.
  //
  // Default: 0 (disabled)
  size_t db_write_buffer_size = 0;

  // The memory usage of memtable will report to this object. The same object
  // can be passed into multiple DBs and it will track the sum of size of all
  // the DBs. If the total size of all live memtables of all the DBs exceeds
  // a limit, a flush will be triggered in the next DB to which the next write
  // is issued, as long as there is one or more column family not already
  // flushing.
  //
  // If the object is only passed to one DB, the behavior is the same as
  // db_write_buffer_size. When write_buffer_manager is set, the value set will
  // override db_write_buffer_size.
  //
  // This feature is disabled by default. Specify a non-zero value
  // to enable it.
  //
  // Default: null
  std::shared_ptr<WriteBufferManager> write_buffer_manager = nullptr;

  // If non-zero, we perform bigger reads when doing compaction. If you're
  // running RocksDB on spinning disks, you should set this to at least 2MB.
  // That way RocksDB's compaction is doing sequential instead of random reads.
  //
  // Default: 2MB
  //
  // Dynamically changeable through SetDBOptions() API.
  size_t compaction_readahead_size = 2 * 1024 * 1024;

  // This is the maximum buffer size that is used by WritableFileWriter.
  // With direct IO, we need to maintain an aligned buffer for writes.
  // We allow the buffer to grow until it's size hits the limit in buffered
  // IO and fix the buffer size when using direct IO to ensure alignment of
  // write requests if the logical sector size is unusual
  //
  // Default: 1024 * 1024 (1 MB)
  //
  // Dynamically changeable through SetDBOptions() API.
  size_t writable_file_max_buffer_size = 1024 * 1024;

  // Use adaptive mutex, which spins in the user space before resorting
  // to kernel. This could reduce context switch when the mutex is not
  // heavily contended. However, if the mutex is hot, we could end up
  // wasting spin time.
  // Default: false
  bool use_adaptive_mutex = false;

  // Create DBOptions with default values for all fields
  DBOptions();
  // Create DBOptions from Options
  explicit DBOptions(const Options& options);

  void Dump(Logger* log) const;

  // Allows OS to incrementally sync files to disk while they are being
  // written, asynchronously, in the background. This operation can be used
  // to smooth out write I/Os over time. Users shouldn't rely on it for
  // persistence guarantee.
  // Issue one request for every bytes_per_sync written. 0 turns it off.
  //
  // You may consider using rate_limiter to regulate write rate to device.
  // When rate limiter is enabled, it automatically enables bytes_per_sync
  // to 1MB.
  //
  // This option applies to table files
  //
  // Default: 0, turned off
  //
  // Note: DOES NOT apply to WAL files. See wal_bytes_per_sync instead
  // Dynamically changeable through SetDBOptions() API.
  uint64_t bytes_per_sync = 0;

  // Same as bytes_per_sync, but applies to WAL files
  // This does not gaurantee the WALs are synced in the order of creation. New
  // WAL can be synced while an older WAL doesn't. Therefore upon system crash,
  // this hole in the WAL data can create partial data loss.
  //
  // Default: 0, turned off
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t wal_bytes_per_sync = 0;

  // When true, guarantees WAL files have at most `wal_bytes_per_sync`
  // bytes submitted for writeback at any given time, and SST files have at most
  // `bytes_per_sync` bytes pending writeback at any given time. This can be
  // used to handle cases where processing speed exceeds I/O speed during file
  // generation, which can lead to a huge sync when the file is finished, even
  // with `bytes_per_sync` / `wal_bytes_per_sync` properly configured.
  //
  //  - If `sync_file_range` is supported it achieves this by waiting for any
  //    prior `sync_file_range`s to finish before proceeding. In this way,
  //    processing (compression, etc.) can proceed uninhibited in the gap
  //    between `sync_file_range`s, and we block only when I/O falls behind.
  //  - Otherwise the `WritableFile::Sync` method is used. Note this mechanism
  //    always blocks, thus preventing the interleaving of I/O and processing.
  //
  // Note: Enabling this option does not provide any additional persistence
  // guarantees, as it may use `sync_file_range`, which does not write out
  // metadata.
  //
  // Default: false
  bool strict_bytes_per_sync = false;

  // A vector of EventListeners whose callback functions will be called
  // when specific RocksDB event happens.
  std::vector<std::shared_ptr<EventListener>> listeners;

  // If true, then the status of the threads involved in this DB will
  // be tracked and available via GetThreadList() API.
  //
  // Default: false
  bool enable_thread_tracking = false;

  // The limited write rate to DB if soft_pending_compaction_bytes_limit or
  // level0_slowdown_writes_trigger is triggered, or we are writing to the
  // last mem table allowed and we allow more than 3 mem tables. It is
  // calculated using size of user write requests before compression.
  // RocksDB may decide to slow down more if the compaction still
  // gets behind further.
  // If the value is 0, we will infer a value from `rater_limiter` value
  // if it is not empty, or 16MB if `rater_limiter` is empty. Note that
  // if users change the rate in `rate_limiter` after DB is opened,
  // `delayed_write_rate` won't be adjusted.
  //
  // Unit: byte per second.
  //
  // Default: 0
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t delayed_write_rate = 0;

  // By default, a single write thread queue is maintained. The thread gets
  // to the head of the queue becomes write batch group leader and responsible
  // for writing to WAL and memtable for the batch group.
  //
  // If enable_pipelined_write is true, separate write thread queue is
  // maintained for WAL write and memtable write. A write thread first enter WAL
  // writer queue and then memtable writer queue. Pending thread on the WAL
  // writer queue thus only have to wait for previous writers to finish their
  // WAL writing but not the memtable writing. Enabling the feature may improve
  // write throughput and reduce latency of the prepare phase of two-phase
  // commit.
  //
  // Default: false
  bool enable_pipelined_write = false;

  // Setting unordered_write to true trades higher write throughput with
  // relaxing the immutability guarantee of snapshots. This violates the
  // repeatability one expects from ::Get from a snapshot, as well as
  // ::MultiGet and Iterator's consistent-point-in-time view property.
  // If the application cannot tolerate the relaxed guarantees, it can implement
  // its own mechanisms to work around that and yet benefit from the higher
  // throughput. Using TransactionDB with WRITE_PREPARED write policy and
  // two_write_queues=true is one way to achieve immutable snapshots despite
  // unordered_write.
  //
  // By default, i.e., when it is false, rocksdb does not advance the sequence
  // number for new snapshots unless all the writes with lower sequence numbers
  // are already finished. This provides the immutability that we expect from
  // snapshots. Moreover, since Iterator and MultiGet internally depend on
  // snapshots, the snapshot immutability results into Iterator and MultiGet
  // offering consistent-point-in-time view. If set to true, although
  // Read-Your-Own-Write property is still provided, the snapshot immutability
  // property is relaxed: the writes issued after the snapshot is obtained (with
  // larger sequence numbers) will be still not visible to the reads from that
  // snapshot, however, there still might be pending writes (with lower sequence
  // number) that will change the state visible to the snapshot after they are
  // landed to the memtable.
  //
  // Default: false
  bool unordered_write = false;

  // If true, allow multi-writers to update mem tables in parallel.
  // Only some memtable_factory-s support concurrent writes; currently it
  // is implemented only for SkipListFactory.  Concurrent memtable writes
  // are not compatible with inplace_update_support or filter_deletes.
  // It is strongly recommended to set enable_write_thread_adaptive_yield
  // if you are going to use this feature.
  //
  // Default: true
  bool allow_concurrent_memtable_write = true;

  // If true, threads synchronizing with the write batch group leader will
  // wait for up to write_thread_max_yield_usec before blocking on a mutex.
  // This can substantially improve throughput for concurrent workloads,
  // regardless of whether allow_concurrent_memtable_write is enabled.
  //
  // Default: true
  bool enable_write_thread_adaptive_yield = true;

  // The maximum limit of number of bytes that are written in a single batch
  // of WAL or memtable write. It is followed when the leader write size
  // is larger than 1/8 of this limit.
  //
  // Default: 1 MB
  uint64_t max_write_batch_group_size_bytes = 1 << 20;

  // The maximum number of microseconds that a write operation will use
  // a yielding spin loop to coordinate with other write threads before
  // blocking on a mutex.  (Assuming write_thread_slow_yield_usec is
  // set properly) increasing this value is likely to increase RocksDB
  // throughput at the expense of increased CPU usage.
  //
  // Default: 100
  uint64_t write_thread_max_yield_usec = 100;

  // The latency in microseconds after which a std::this_thread::yield
  // call (sched_yield on Linux) is considered to be a signal that
  // other processes or threads would like to use the current core.
  // Increasing this makes writer threads more likely to take CPU
  // by spinning, which will show up as an increase in the number of
  // involuntary context switches.
  //
  // Default: 3
  uint64_t write_thread_slow_yield_usec = 3;

  // If true, then DB::Open() will not update the statistics used to optimize
  // compaction decision by loading table properties from many files.
  // Turning off this feature will improve DBOpen time especially in
  // disk environment.
  //
  // Default: false
  bool skip_stats_update_on_db_open = false;

  // Recovery mode to control the consistency while replaying WAL
  // Default: kPointInTimeRecovery
  WALRecoveryMode wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  // if set to false then recovery will fail when a prepared
  // transaction is encountered in the WAL
  bool allow_2pc = false;

  // A global cache for table-level rows.
  // Used to speed up Get() queries.
  // NOTE: does not work with DeleteRange() yet.
  // Default: nullptr (disabled)
  std::shared_ptr<RowCache> row_cache = nullptr;

  // A filter object supplied to be invoked while processing write-ahead-logs
  // (WALs) during recovery. The filter provides a way to inspect log
  // records, ignoring a particular record or skipping replay.
  // The filter is invoked at startup and is invoked from a single-thread
  // currently.
  WalFilter* wal_filter = nullptr;

  // If true, then print malloc stats together with rocksdb.stats
  // when printing to LOG.
  // DEFAULT: false
  bool dump_malloc_stats = false;

  // By default RocksDB replay WAL logs and flush them on DB open, which may
  // create very small SST files. If this option is enabled, RocksDB will try
  // to avoid (but not guarantee not to) flush during recovery. Also, existing
  // WAL logs will be kept, so that if crash happened before flush, we still
  // have logs to recover from.
  //
  // Note: when `enforce_write_buffer_manager_during_recovery` is also enabled,
  // flushes may still occur during recovery to respect the
  // WriteBufferManager's global memory limit, even if this option is true.
  // Once any such WBM-triggered flush happens, all remaining memtables will
  // also be flushed at the end of recovery (similar to the behavior when this
  // option is false).
  //
  // DEFAULT: false
  bool avoid_flush_during_recovery = false;

  // If true and a WriteBufferManager is configured, RocksDB will check
  // WriteBufferManager::ShouldFlush() during WAL recovery and schedule
  // flushes when needed. This prevents OOM when multiple RocksDB instances
  // share a WriteBufferManager and one instance is recovering from WAL.
  //
  // When triggered, all column families with non-empty memtables are scheduled
  // for flush, which may produce smaller L0 files in some column families.
  // This also overrides `avoid_flush_during_recovery`: once a WBM-triggered
  // flush occurs mid-recovery, all remaining non-empty memtables will be
  // flushed at the end of recovery as well.
  //
  // DEFAULT: true
  bool enforce_write_buffer_manager_during_recovery = true;

  // By default RocksDB will flush all memtables on DB close if there are
  // unpersisted data (i.e. with WAL disabled) The flush can be skip to speedup
  // DB close. Unpersisted data WILL BE LOST.
  //
  // DEFAULT: false
  //
  // Dynamically changeable through SetDBOptions() API.
  bool avoid_flush_during_shutdown = false;

  // DEPRECATED: use ColumnFamilyOptions::cf_allow_ingest_behind instead.
  // This option might be removed in a future release.
  //
  // See comment for `ColumnFamilyOptions::cf_allow_ingest_behind` for
  // detail about the option's functionality and use cases.
  //
  // DEFAULT: false
  // Immutable.
  bool allow_ingest_behind = false;

  // If enabled it uses two queues for writes, one for the ones with
  // disable_memtable and one for the ones that also write to memtable. This
  // allows the memtable writes not to lag behind other writes. It can be used
  // to optimize MySQL 2PC in which only the commits, which are serial, write to
  // memtable.
  bool two_write_queues = false;

  // If true WAL is not flushed automatically after each write. Instead it
  // relies on manual invocation of FlushWAL to write the WAL buffer to its
  // file.
  bool manual_wal_flush = false;

  // If enabled WAL records will be compressed before they are written. Only
  // ZSTD (= kZSTD) is supported (until streaming support is adapted for other
  // compression types). Compressed WAL records will be read in supported
  // versions (>= RocksDB 7.4.0 for ZSTD) regardless of this setting when
  // the WAL is read.
  CompressionType wal_compression = kNoCompression;

  // Set to true to re-instate an old behavior of keeping complete, synced WAL
  // files open for write until they are collected for deletion by a
  // background thread. This should not be needed unless there is a
  // performance issue with file Close(), but setting it to true means that
  // Checkpoint might call LinkFile on a WAL still open for write, which might
  // be unsupported on some FileSystem implementations. As this is intended as
  // a temporary kill switch, it is already DEPRECATED.
  bool background_close_inactive_wals = false;

  // If true, RocksDB supports flushing multiple column families and committing
  // their results atomically to MANIFEST. Note that it is not
  // necessary to set atomic_flush to true if WAL is always enabled since WAL
  // allows the database to be restored to the last persistent state in WAL.
  // This option is useful when there are column families with writes NOT
  // protected by WAL.
  // For manual flush, application has to specify which column families to
  // flush atomically in DB::Flush.
  // For auto-triggered flush, RocksDB atomically flushes ALL column families.
  //
  // Currently, any WAL-enabled writes after atomic flush may be replayed
  // independently if the process crashes later and tries to recover.
  bool atomic_flush = false;

  // If true, working thread may avoid doing unnecessary and long-latency
  // operation (such as deleting obsolete files directly or deleting memtable)
  // and will instead schedule a background job to do it.
  // Use it if you're latency-sensitive.
  // If set to true, takes precedence over
  // ReadOptions::background_purge_on_iterator_cleanup.
  bool avoid_unnecessary_blocking_io = false;

  // The DB unique ID can be saved in the DB manifest (preferred, this option)
  // or an IDENTITY file (historical, deprecated), or both. If this option is
  // set to false (old behavior), then write_identity_file must be set to true.
  // The manifest is preferred because
  // 1. The IDENTITY file is not checksummed, so it is not as safe against
  //    corruption.
  // 2. The IDENTITY file may or may not be copied with the DB (e.g. not
  //    copied by BackupEngine), so is not reliable for the provenance of a DB.
  // This option might eventually be obsolete and removed as Identity files
  // are phased out.
  bool write_dbid_to_manifest = true;

  // EXPERIMENTAL: If true, each durable OPTIONS file is followed by a MANIFEST
  // record committing its file number. For a column-family create or drop, the
  // commit and membership edit are the same MANIFEST record.
  //
  // This value is persisted in OPTIONS files so readers can distinguish a
  // tracked file that was durably renamed but not committed from a file written
  // by a legacy or disabled writer. A higher file with this value false
  // supersedes an older MANIFEST commit. A higher file with this value true is
  // ignored until its number is committed.
  //
  // Older binaries safely ignore the MANIFEST field and retain their legacy
  // highest-numbered-OPTIONS behavior. Rollback binaries must know this option
  // before the feature is enabled so they can parse newly written OPTIONS
  // files.
  //
  // The multi-column-family create/drop APIs preserve their existing partial
  // success behavior while writing one complete OPTIONS snapshot per batch.
  // A partial batch can leave the committed snapshot with extra column
  // families; LoadLatestOptions and CheckOptionsCompatibility filter those
  // entries against the live MANIFEST membership.
  //
  // Default: false
  bool track_options_file_number_in_manifest = false;

  // It is expected that the Identity file will be obsoleted by recording
  // DB ID in the manifest (see write_dbid_to_manifest). Setting this to true
  // maintains the historical behavior of writing an Identity file, while
  // setting to false is expected to be the future default. This option might
  // eventually be obsolete and removed as Identity files are phased out.
  bool write_identity_file = true;

  // Historically, when prefix_extractor != nullptr, iterators have an
  // unfortunate default semantics of *possibly* only returning data
  // within the same prefix. To avoid "spooky action at a distance," iterator
  // bounds should come from the instantiation or seeking of the iterator,
  // not from a mutable column family option.
  //
  // When set to true, it is as if every iterator is created with
  // total_order_seek=true and only auto_prefix_mode=true and
  // prefix_same_as_start=true can take advantage of prefix seek optimizations.
  bool prefix_seek_opt_in_only = false;

  // The number of bytes to prefetch when reading the DB manifest and WAL files
  // during DB::Open (and variants). This is mostly useful for reading a
  // remotely located log, as it can save the number of round-trips. If 0, then
  // the prefetching is disabled.
  //
  // Default: 0
  size_t log_readahead_size = 0;

  // If user does NOT provide the checksum generator factory, the file checksum
  // will NOT be used. A new file checksum generator object will be created
  // when a SST file is created. Therefore, each created FileChecksumGenerator
  // will only be used from a single thread and so does not need to be
  // thread-safe.
  //
  // Default: nullptr
  std::shared_ptr<FileChecksumGenFactory> file_checksum_gen_factory = nullptr;

  // By default, RocksDB will attempt to detect any data losses or corruptions
  // in DB files and return an error to the user, either at DB::Open time or
  // later during DB operation. The exception to this policy is the WAL file,
  // whose recovery is controlled by the wal_recovery_mode option.
  //
  // Best-efforts recovery (this option set to true) signals a preference for
  // opening the DB to any point-in-time valid state for each column family,
  // including the empty/new state, versus the default of returning non-WAL
  // data losses to the user as errors. In terms of RocksDB user data, this
  // is like applying WALRecoveryMode::kPointInTimeRecovery to each column
  // family rather than just the WAL.
  //
  // The behavior changes in the presence of "AtomicGroup"s in the MANIFEST,
  // which is currently only the case when `atomic_flush == true`. In that
  // case, all pre-existing CFs must recover the atomic group in order for
  // that group to be applied in an all-or-nothing manner. This means that
  // unused/inactive CF(s) with invalid filesystem state can block recovery of
  // all other CFs at an atomic group.
  //
  // Best-efforts recovery (BER) is specifically designed to recover a DB with
  // files that are missing or truncated to some smaller size, such as the
  // result of an incomplete DB "physical" (FileSystem) copy. BER can also
  // detect when an SST file has been replaced with a different one of the
  // same size (assuming SST unique IDs are tracked in DB manifest).
  // BER is not yet designed to produce a usable DB from other corruptions to
  // DB files (which should generally be detectable by DB::VerifyChecksum()),
  // and BER does not yet attempt to recover any WAL files.
  //
  // For example, if an SST or blob file referenced by the MANIFEST is missing,
  // BER might be able to find a set of files corresponding to an old "point in
  // time" version of the column family, possibly from an older MANIFEST
  // file.
  // Besides complete "point in time" version, an incomplete version with
  // only a suffix of L0 files missing can also be recovered to if the
  // versioning history doesn't include an atomic flush.  From the users'
  // perspective, missing a suffix of L0 files means missing the
  // user's most recently written data. So the remaining available files still
  // presents a valid point in time view, although for some previous time. It's
  // not done for atomic flush because that guarantees a consistent view across
  // column families. We cannot guarantee that if recovering an incomplete
  // version.
  // Some other kinds of DB files (e.g. CURRENT, LOCK, IDENTITY) are
  // either ignored or replaced with BER, or quietly fixed regardless of BER
  // setting. BER does require at least one valid MANIFEST to recover to a
  // non-trivial DB state, unlike `ldb repair`.
  //
  // Default: false
  bool best_efforts_recovery = false;

  // It defines how many times DB::Resume() is called by a separate thread when
  // background retryable IO Error happens. When background retryable IO
  // Error happens, SetBGError is called to deal with the error. If the error
  // can be auto-recovered (e.g., retryable IO Error during Flush or WAL write),
  // then db resume is called in background to recover from the error. If this
  // value is 0 or negative, DB::Resume() will not be called automatically, and
  // any background retryable IO error (including one during compaction, which
  // would otherwise be retried by rescheduling the compaction) stops writes
  // until the user calls DB::Resume().
  //
  // Default: INT_MAX
  int max_bgerror_resume_count = INT_MAX;

  // If max_bgerror_resume_count is >= 2, db resume is called multiple times.
  // This option decides how long to wait to retry the next resume if the
  // previous resume fails and satisfy redo resume conditions.
  //
  // Default: 1000000 (microseconds).
  uint64_t bgerror_resume_retry_interval = 1000000;

  // It allows user to opt-in to get error messages containing corrupted
  // keys/values. Corrupt keys, values will be logged in the
  // messages/logs/status that will help users with the useful information
  // regarding affected data. By default value is set false to prevent users
  // data to be exposed in the logs/messages etc.
  //
  // Default: false
  bool allow_data_in_errors = false;

  // A string identifying the machine hosting the DB. This
  // will be written as a property in every SST file written by the DB (or
  // by offline writers such as SstFileWriter and RepairDB). It can be useful
  // for troubleshooting in memory corruption caused by a failing host when
  // writing a file, by tracing back to the writing host. These corruptions
  // may not be caught by the checksum since they happen before checksumming.
  // If left as default, the table writer will substitute it with the actual
  // hostname when writing the SST file. If set to an empty string, the
  // property will not be written to the SST file.
  //
  // Default: hostname
  std::string db_host_id = kHostnameForDbHostId;

  // Use this if your DB want to enable checksum handoff for specific file
  // types writes. Make sure that the File_system you use support the
  // crc32c checksum verification
  // Currently supported file tyes: kWALFile, kTableFile, kDescriptorFile.
  // NOTE: currently RocksDB only generates crc32c based checksum for the
  // handoff. If the storage layer has different checksum support, user
  // should enble this set as empty. Otherwise,it may cause unexpected
  // write failures.
  FileTypeSet checksum_handoff_file_types;

  // EXPERIMENTAL
  // CompactionService is a feature allows the user to run compactions on a
  // different host or process, which offloads the background load from the
  // primary host.
  // It's an experimental feature, the interface will be changed without
  // backward/forward compatibility support for now. Some known issues are still
  // under development.
  std::shared_ptr<CompactionService> compaction_service = nullptr;

  // Hardening for remote compaction (CompactionService / DB::OpenAndCompact).
  // When true (default), the primary includes in each remote compaction request
  // the MANIFEST position (file number and size) it scheduled the compaction
  // from. The remote worker refuses to reconstruct the compaction against an
  // older MANIFEST view than that -- e.g. an eventually-consistent filesystem
  // returning a stale CURRENT, or a truncated MANIFEST -- and falls back to a
  // local compaction rather than risk an incorrect result computed against the
  // wrong (older) LSM shape.
  //
  // The presence of this position on the request also enables the worker's
  // "trust the MANIFEST" recovery, which avoids failing a job over a
  // transiently-unavailable file that is not one of the compaction's inputs.
  //
  // Setting this to false is a kill switch for disabling both behaviors (for
  // subsequently scheduled compactions). Dynamically changeable through
  // SetDBOptions() API. DEPRECATED because this kill switch should not be
  // needed long term.
  //
  // Only affects a primary that schedules remote compactions; it has no effect
  // when compaction_service is not configured.
  bool remote_compaction_manifest_floor = true;

  // When true, remote compaction uses the DB session temporary directory.
  // An `output_directory` argument of `job_1` to `DB::OpenAndCompact()` is
  // resolved to `<dbname>/session_tmp/job_1`. DB::Open() creates the directory
  // and deletes contents left by the previous DB session before scheduling new
  // compactions. Cleanup is best effort and never fails open.
  //
  // When false, output is written directly to `<dbname>/<output_directory>`
  // and DB::Open() performs no remote compaction output cleanup, preserving
  // the legacy behavior.
  //
  // Default: false
  bool use_session_tmp_dir_for_remote_compaction = false;

  // It indicates, which lowest cache tier we want to
  // use for a certain DB. Currently we support volatile_tier and
  // non_volatile_tier. They are layered. By setting it to kVolatileTier, only
  // the block cache (current implemented volatile_tier) is used. So
  // cache entries will not spill to secondary cache (current
  // implemented non_volatile_tier), and block cache lookup misses will not
  // lookup in the secondary cache. When kNonVolatileBlockTier is used, we use
  // both block cache and secondary cache.
  //
  // Default: kNonVolatileBlockTier
  CacheTier lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier;

  // DEPRECATED: This option might be removed in a future release.
  //
  // If set to false, when compaction or flush sees a SingleDelete followed by
  // a Delete for the same user key, compaction job will not fail.
  // Otherwise, compaction job will fail.
  // This is a temporary option to help existing use cases migrate, and
  // will be removed in a future release.
  // Warning: do not set to false unless you are trying to migrate existing
  // data in which the contract of single delete
  // (https://github.com/meta/rocksdb/wiki/Single-Delete) is not enforced,
  // thus has Delete mixed with SingleDelete for the same user key. Violation
  // of the contract leads to undefined behaviors with high possibility of data
  // inconsistency, e.g. deleted old data become visible again, etc.
  bool enforce_single_del_contracts = true;

  // Implementing off-peak duration awareness in RocksDB. In this context,
  // "off-peak time" signifies periods characterized by significantly less read
  // and write activity compared to other times. By leveraging this knowledge,
  // we can prevent low-priority tasks, such as TTL-based compactions, from
  // competing with read and write operations during peak hours. Essentially, we
  // preprocess these tasks during the preceding off-peak period, just before
  // the next peak cycle begins. For example, if the TTL is configured for 25
  // days, we may compact the files during the off-peak hours of the 24th day.
  //
  // Time of the day in UTC, start_time-end_time inclusive.
  // Format - HH:mm-HH:mm (00:00-23:59)
  // If the start time > end time, it will be considered that the time period
  // spans to the next day (e.g., 23:30-04:00). To make an entire day off-peak,
  // use "0:00-23:59". To make an entire day have no offpeak period, leave
  // this field blank. Default: Empty string (no offpeak).
  std::string daily_offpeak_time_utc;

  // Maximum interval in seconds between periodic compaction trigger checks.
  // The periodic trigger re-evaluates compaction scores for all column
  // families, which is necessary for features like read-triggered compaction
  // and time-based compaction to work on a "quiet" DB with no writes.
  //
  // This is an upper bound: the actual check interval may be reduced to
  // align with stats_dump_period_sec, stats_persist_period_sec, or per-CF
  // time-based compaction intervals (periodic_compaction_seconds, ttl, etc.).
  //
  // Note: this option controls how often RocksDB *checks* whether compaction
  // is needed. It is different from the CF option `periodic_compaction_seconds`
  // which controls the *age threshold* at which SST files become eligible for
  // periodic compaction.
  //
  // The minimum effective period is 1 second (values below 1 are clamped to 1).
  // Setting this to 0 results in the most aggressive 1-second polling.
  //
  // Default: 43200 (12 hours)
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t max_compaction_trigger_wakeup_seconds = 43200;

  // EXPERIMENTAL
  // Enables and tunes preferred-phase scheduling of periodic (time-based)
  // compaction, which de-herds periodic_compaction_seconds compactions across a
  // fleet of similarly-configured DBs (a recurring "thundering herd" is
  // especially costly with remote compaction). Each (DB, column family) is
  // given a stable-but-well-spread preferred phase within
  // periodic_compaction_seconds and compactions are steered toward it. The
  // phase is derived from the DB ID -- stable across re-open and physical
  // cloning/migration (at least when write_dbid_to_manifest=true), but distinct
  // across DBs -- and each column family is spread quasi-uniformly around the
  // DB's base phase by a golden-ratio recurrence, so different CFs of one DB
  // get well-separated phases. No user tuning of the phase itself is needed.
  //
  // This value is the percent (0-100) of the gap between a file's unphased
  // trigger time and its preferred-phase time that is closed on each
  // periodic-compaction trigger, always by triggering *earlier* -- never later
  // than periodic_compaction_seconds, which remains a soft upper bound on data
  // age. Because each compaction re-stamps the file, the phase error then
  // decays geometrically toward the preferred phase over successive cycles.
  //
  // "Never later" holds in steady state. It can be exceeded only for files
  // already past due when phasing (re)anchors -- which it does on DB open, on
  // enabling phasing, and on any change to periodic_compaction_seconds. The
  // cases that matter are reopening a DB whose files aged out while it was
  // down (e.g. DC power-outage recovery) and turning the interval *down*
  // (which retroactively makes older files past due). Rather than compact that
  // whole cohort at once (a herd), phasing spreads it over the first quarter
  // of the interval after the anchor, on a stable per-(DB, CF) grid --
  // expedited but herd-avoiding -- so such a file may be rewritten up to a
  // quarter-interval past its (revised) deadline. To enforce the age limit
  // immediately at such a transition, trigger a manual compaction.
  //
  // Files not yet past due at a (re)anchor are steered toward their phase more
  // gently for up to about two intervals afterward, so that many DBs
  // anchoring at once (e.g. enabling phasing across a fleet) do not
  // concentrate periodic compactions shortly after the change. Because DB open
  // is also a (re)anchor, a DB reopened much more often than once per interval
  // converges toward its phase more slowly; such re-anchoring is appropriate to
  // reduce load spikes after some period with the DB closed, without
  // compromising the periodic_compaction_seconds contract.
  //
  // 0 disables phasing entirely (exact legacy behavior; this is the kill
  // switch). 100 snaps to the preferred phase in a single cycle, which can
  // recompact some files well before their normal interval and is generally not
  // recommended. Values are clamped to [0, 100].
  //
  // Lower values converge more slowly but incur less extra compaction work,
  // especially under write-driven or bursty workloads that keep re-randomizing
  // a file's phase (where a high recovery rate wastefully "chases" the moving
  // phase). ~33 is a good balance of spreading speed vs. extra work: almost
  // always widely spread after a few periods while almost always below 5%
  // extra compaction work overall, trending toward 0% in the long term for
  // most workloads.
  //
  // This feature might become obsolete if a future feature is able to have
  // compaction pressure, whether local or remote, feed back into compaction
  // scheduling.
  //
  // Recommended setting: 33
  // Default: feature disabled
  //
  // Dynamically changeable through SetDBOptions() API.
  int periodic_compaction_phase_recovery_percent = 0;

  // EXPERIMENTAL

  // When a RocksDB database is opened in follower mode, this option
  // is set by the user to request the frequency of the follower
  // attempting to refresh its view of the leader. RocksDB may choose to
  // trigger catch ups more frequently if it detects any changes in the
  // database state.
  // Default every 10s.
  uint64_t follower_refresh_catchup_period_ms = 10000;

  // For a given catch up attempt, this option specifies the number of times
  // to tail the MANIFEST and try to install a new, consistent  version before
  // giving up. Though it should be extremely rare, the catch up may fail if
  // the leader is mutating the LSM at a very high rate and the follower is
  // unable to get a consistent view.
  // Default to 10 attempts
  uint64_t follower_catchup_retry_count = 10;

  // Time to wait between consecutive catch up attempts
  // Default 100ms
  uint64_t follower_catchup_retry_wait_ms = 100;

  // When DB files other than SST, blob and WAL files are created, use this
  // filesystem temperature. (See also `wal_write_temperature` and various
  // `*_temperature` CF options.) When not `kUnknown`, this overrides any
  // temperature set by OptimizeForManifestWrite functions.
  Temperature metadata_write_temperature = Temperature::kUnknown;

  // Use this filesystem temperature when creating WAL files. When not
  // `kUnknown`, this overrides any temperature set by OptimizeForLogWrite
  // functions.
  Temperature wal_write_temperature = Temperature::kUnknown;

  // Enum set indicative of which compaction styles SST write lifetime hint
  // calculation is allowed on. Today, RocksDB provides native support for
  // kCompactionStyleLevel and kCompactionStyleUniversal (experimental version).
  // Other compaction styles, even when enabled in the set, won't have any
  // effect in the default PosixWritableFile file implementation. There are
  // numerous benefits coming from employing the hints including reduction in
  // write amplification caused by OS file movement during garbage collection,
  // and reduction in wear-leveling (SSDs). However, as currently implemented,
  // SST write lifetime hints are calculated in a static way and solely based on
  // the level, which might not be suitable for non-uniform workloads with
  // dynamic / high-variance lifespan of data within the same level. In those
  // cases (or when the performance is not satisfactory), it's recommended to
  // disable the hints by assigning the setting to the empty set (= {});
  //
  // Default: Enabled in kCompactionStyleLevel mode.
  CompactionStyleSet calculate_sst_write_lifetime_hint_set = {
      CompactionStyle::kCompactionStyleLevel};

  // EXPERIMENTAL
  // When this is true, save file system metadata (if supported by the FS) for
  // SST files added to the DB in the MANIFEST, and use it to accelerate
  // re-opening of those files on DB open. This will help cut down DB open
  // latency on remote storage systems.
  bool fast_sst_open = false;

  // End EXPERIMENTAL
};

// Options to control the behavior of a database (passed to DB::Open)
struct Options : public DBOptions, public ColumnFamilyOptions {
  // Create an Options object with default values for all fields.
  Options() : DBOptions(), ColumnFamilyOptions() {}

  Options(const DBOptions& db_options,
          const ColumnFamilyOptions& column_family_options)
      : DBOptions(db_options), ColumnFamilyOptions(column_family_options) {}

  // Change to some default settings from an older version.
  // NOT MAINTAINED: This function has not been and is not maintained.
  // DEPRECATED: This function might be removed in a future release.
  // In general, defaults are changed to suit broad interests. Opting
  // out of a change on upgrade should be deliberate and considered.
  Options* OldDefaults(int rocksdb_major_version = 4,
                       int rocksdb_minor_version = 6);

  void Dump(Logger* log) const;

  void DumpCFOptions(Logger* log) const;

  // Some functions that make it easier to optimize RocksDB

  // Set appropriate parameters for bulk loading.
  // The reason that this is a function that returns "this" instead of a
  // constructor is to enable chaining of multiple similar calls in the future.
  //

  // All data will be in level 0 without any automatic compaction.
  // It's recommended to manually call CompactRange(NULL, NULL) before reading
  // from the database, because otherwise the read can be very slow.
  Options* PrepareForBulkLoad();

  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  Options* OptimizeForSmallDb();

  // Disable some checks that should not be necessary in the absence of
  // software logic errors or CPU+memory hardware errors. This can improve
  // write speeds but is only recommended for temporary use. Does not
  // change protection against corrupt storage (e.g. verify_checksums).
  Options* DisableExtraChecks();
};

// An application can issue a read request (via Get/Iterators) and specify
// if that read should process data that ALREADY resides on a specified cache
// level. For example, if an application specifies kBlockCacheTier then the
// Get call will process data that is already processed in the memtable or
// the block cache. It will not page in data from the OS cache or data that
// resides in storage.
enum ReadTier {
  kReadAllTier = 0x0,     // data in memtable, block cache, OS cache or storage
  kBlockCacheTier = 0x1,  // data in memtable or block cache
  kPersistedTier = 0x2,   // persisted data.  When WAL is disabled, this option
                          // will skip data in memtable.
                          // Note that this ReadTier currently only supports
                          // Get and MultiGet and does not support iterators.
  kMemtableTier = 0x3     // data in memtable. used for memtable-only iterators.
};

// A range of keys. In case of user_defined timestamp, if enabled, `start` and
// `limit` should point to key without timestamp part.
struct Range {
  Slice start;
  Slice limit;

  Range() {}
  Range(const Slice& s, const Slice& l) : start(s), limit(l) {}
};

// A key range with optional endpoints. In case of user_defined timestamp, if
// enabled, `start` and `limit` should point to key without timestamp part.
struct RangeOpt {
  // When start.has_value() == false, refers to starting before every key
  OptSlice start;
  // When limit.has_value() == false, refers to ending after every key
  OptSlice limit;

  RangeOpt() {}
  RangeOpt(const OptSlice& s, const OptSlice& l) : start(s), limit(l) {}
};

// EXPERIMENTAL
//
// Options for a RocksDB scan request. Only forward scans for now.
// We may add other options such as prefix scan in the future.
struct ScanOptions {
  // The scan range. Mandatory for start to be set, limit is optional
  RangeOpt range;

  // A map of name,value pairs that can be passed by the user to an
  // external table reader. This is completely opaque to RocksDB and is
  // ignored by the natively supported table readers like block based and plain
  // table. This is only useful for Iterator.
  std::optional<std::unordered_map<std::string, std::string>> property_bag;

  // An unbounded scan with a start key
  explicit ScanOptions(const Slice& _start) : range(_start, OptSlice()) {}

  // A bounded scan with a start key and upper bound
  ScanOptions(const Slice& _start, const Slice& _upper_bound)
      : range(_start, _upper_bound) {}
};

// Container for multiple scan ranges that can be used with MultiScan.
// This replaces std::vector<ScanOptions> with a more efficient implementation
// that can merge overlapping ranges.
class MultiScanArgs {
 public:
  // Constructor that takes a comparator
  explicit MultiScanArgs(const Comparator* comparator) : comp_(comparator) {}

  // Copy Constructor
  MultiScanArgs(const MultiScanArgs& other) {
    comp_ = other.comp_;
    original_ranges_ = other.original_ranges_;
    io_coalesce_threshold = other.io_coalesce_threshold;
    max_prefetch_size = other.max_prefetch_size;
    rolling_prefetch = other.rolling_prefetch;
    rolling_prefetch_window_size = other.rolling_prefetch_window_size;
    use_async_io = other.use_async_io;
    reverse = other.reverse;
    io_dispatcher = other.io_dispatcher;
  }
  MultiScanArgs(MultiScanArgs&& other) noexcept
      : io_coalesce_threshold(other.io_coalesce_threshold),
        max_prefetch_size(other.max_prefetch_size),
        rolling_prefetch(other.rolling_prefetch),
        rolling_prefetch_window_size(other.rolling_prefetch_window_size),
        use_async_io(other.use_async_io),
        reverse(other.reverse),
        io_dispatcher(std::move(other.io_dispatcher)),
        comp_(other.comp_),
        original_ranges_(std::move(other.original_ranges_)) {}

  MultiScanArgs& operator=(const MultiScanArgs& other) {
    comp_ = other.comp_;
    original_ranges_ = other.original_ranges_;
    io_coalesce_threshold = other.io_coalesce_threshold;
    max_prefetch_size = other.max_prefetch_size;
    rolling_prefetch = other.rolling_prefetch;
    rolling_prefetch_window_size = other.rolling_prefetch_window_size;
    use_async_io = other.use_async_io;
    reverse = other.reverse;
    io_dispatcher = other.io_dispatcher;
    return *this;
  }

  MultiScanArgs& operator=(MultiScanArgs&& other) noexcept {
    if (this != &other) {
      comp_ = other.comp_;
      original_ranges_ = std::move(other.original_ranges_);
      io_coalesce_threshold = other.io_coalesce_threshold;
      max_prefetch_size = other.max_prefetch_size;
      rolling_prefetch = other.rolling_prefetch;
      rolling_prefetch_window_size = other.rolling_prefetch_window_size;
      use_async_io = other.use_async_io;
      reverse = other.reverse;
      io_dispatcher = std::move(other.io_dispatcher);
    }
    return *this;
  }

  void insert(const Slice& s, const Slice& b) {
    original_ranges_.emplace_back(s, b);
  }

  void insert(const Slice& s, const Slice& b,
              const std::optional<std::unordered_map<std::string, std::string>>&
                  property_bag) {
    original_ranges_.emplace_back(s, b);
    original_ranges_.back().property_bag = property_bag;
  }

  void insert(const Slice& s) { original_ranges_.emplace_back(s); }

  void insert(const Slice& s,
              const std::optional<std::unordered_map<std::string, std::string>>&
                  property_bag) {
    original_ranges_.emplace_back(s);
    original_ranges_.back().property_bag = property_bag;
  }

  size_t size() const { return original_ranges_.size(); }
  bool empty() const { return original_ranges_.empty(); }

  bool HasBoundedScanRanges() const {
    if (empty()) {
      return false;
    }

    for (const ScanOptions& scan_range : original_ranges_) {
      if (!scan_range.range.start.has_value() ||
          !scan_range.range.limit.has_value()) {
        return false;
      }
    }

    return true;
  }

  void reserve(size_t size) { original_ranges_.reserve(size); }

  operator std::vector<ScanOptions>*() { return &original_ranges_; }

  operator const std::vector<ScanOptions>*() const { return &original_ranges_; }

  ~MultiScanArgs() {}

  const std::vector<ScanOptions>& GetScanRanges() const {
    return original_ranges_;
  }

  const Comparator* GetComparator() const { return comp_; }

  // Copies the configurations (excluding actual scan ranges) from another
  // MultiScanArgs.
  void CopyConfigFrom(const MultiScanArgs& other) {
    io_coalesce_threshold = other.io_coalesce_threshold;
    max_prefetch_size = other.max_prefetch_size;
    rolling_prefetch = other.rolling_prefetch;
    rolling_prefetch_window_size = other.rolling_prefetch_window_size;
    use_async_io = other.use_async_io;
    reverse = other.reverse;
    io_dispatcher = other.io_dispatcher;
  }

  uint64_t io_coalesce_threshold = 16 << 10;  // 16KB by default

  // Maximum size (in bytes) for the data blocks loaded by a MultiScan.
  // This limits the amount of I/O and memory usage by pinned data blocks.
  //
  // When set to 0 (the default), there is no limit. When the limit is reached,
  // the iterator will start returning Status::PrefetchLimitReached().
  //
  // Note that prefetching happens only once in Prepare(), which is different
  // from ReadOptions::readahead_size, which applies any time the iterator does
  // I/O.
  // Note that this limit is per file and applies to compressed block size.
  uint64_t max_prefetch_size = 0;

  // Use max_prefetch_size as the maximum adaptive rolling batch size. The
  // first batch is submitted by Prepare(), and touching a block in that batch
  // submits the next batch. Iteration waits for submitted I/O at its boundary
  // rather than falling back to an individual block read. Applied only to
  // forward scans with use_async_io and a nonzero rolling window. Otherwise it
  // is ignored, leaving max_prefetch_size with its ordinary one-shot semantics.
  bool rolling_prefetch = false;

  // Maximum requested physical span of one rolling prefetch window and the
  // initial rolling batch budget. Must be nonzero. When max_prefetch_size is
  // nonzero, the effective window is the smaller of the two and a foreground
  // wait may grow the batch up to max_prefetch_size. Otherwise the batch stays
  // fixed at this size.
  uint64_t rolling_prefetch_window_size = 0;

  // Enable async I/O for multi-scan operations
  // When true, BlockBasedTableIterator will use ReadAsync() for reading blocks
  // When false, it will use synchronous MultiRead().
  bool use_async_io = false;

  // Scan ranges in reverse order. Ranges are still specified in comparator
  // order as [start, limit), and reverse scans require bounded ranges.
  bool reverse = false;

  // Optional IODispatcher for multi-scan operations.
  // If nullptr (default), a new IODispatcher is created internally.
  // Users can provide their own IODispatcher for custom IO scheduling
  // or for testing/monitoring purposes (e.g., to check IO statistics).
  std::shared_ptr<IODispatcher> io_dispatcher = nullptr;

 private:
  // The comparator used for ordering ranges
  const Comparator* comp_;
  std::vector<ScanOptions> original_ranges_;
};

// Options that control read operations
struct ReadOptions {
  // NOTE: for all pointer members of ReadOptions (e.g. snapshot, timestamp,
  // iterate_lower_bound, iterate_upper_bound, table_filter, ...), when the
  // pointer is non-nullptr the caller retains ownership of the pointed-to
  // object and, unless documented otherwise, is responsible for keeping it
  // alive and unchanged for as long as the read operation using these options
  // is in progress (which, for an iterator, is the lifetime of the iterator).
  // This keeps ReadOptions cheap to copy (internal to RocksDB).

  // *** BEGIN options relevant to point lookups as well as scans ***

  // If "snapshot" is non-nullptr, read as of the supplied snapshot
  // (which must belong to the DB that is being read and which must
  // not have been released).  If "snapshot" is nullptr, use an implicit
  // snapshot of the state at the beginning of this read operation.
  const Snapshot* snapshot = nullptr;

  // Timestamp of operation. Read should return the latest data visible to the
  // specified timestamp. All timestamps of the same database must be of the
  // same length and format. The user is responsible for providing a customized
  // compare function via Comparator to order <key, timestamp> tuples.
  // For iterator, iter_start_ts is the lower bound (older) and timestamp
  // serves as the upper bound. Versions of the same record that fall in
  // the timestamp range will be returned. If iter_start_ts is nullptr,
  // only the most recent version visible to timestamp is returned.
  // The user-specified timestamp feature is still under active development,
  // and the API is subject to change.
  const Slice* timestamp = nullptr;
  const Slice* iter_start_ts = nullptr;

  // Deadline for completing an API call (Get/MultiGet/Seek/Next for now)
  // in microseconds.
  // It should be set to microseconds since epoch, i.e, gettimeofday or
  // equivalent plus allowed duration in microseconds. The best way is to use
  // env->NowMicros() + some timeout.
  // This is best efforts. The call may exceed the deadline if there is IO
  // involved and the file system doesn't support deadlines, or due to
  // checking for deadline periodically rather than for every key if
  // processing a batch
  std::chrono::microseconds deadline = std::chrono::microseconds::zero();

  // A timeout in microseconds to be passed to the underlying FileSystem for
  // reads. As opposed to deadline, this determines the timeout for each
  // individual file read request. If a MultiGet/Get/Seek/Next etc call
  // results in multiple reads, each read can last up to io_timeout us.
  std::chrono::microseconds io_timeout = std::chrono::microseconds::zero();

  // Specify if this read request should process data that ALREADY
  // resides on a particular cache. If the required data is not
  // found at the specified cache, then Status::Incomplete is returned.
  ReadTier read_tier = kReadAllTier;

  // For file reads associated with this option, charge the internal rate
  // limiter (see `DBOptions::rate_limiter`) at the specified priority. The
  // special value `Env::IO_TOTAL` disables charging the rate limiter.
  //
  // The rate limiting is bypassed no matter this option's value for file reads
  // on plain tables (these can exist when `ColumnFamilyOptions::table_factory`
  // is a `PlainTableFactory`) and cuckoo tables (these can exist when
  // `ColumnFamilyOptions::table_factory` is a `CuckooTableFactory`).
  //
  // The bytes charged to rate limiter may not exactly match the file read bytes
  // since there are some seemingly insignificant reads, like for file
  // headers/footers, that we currently do not charge to rate limiter.
  Env::IOPriority rate_limiter_priority = Env::IO_TOTAL;

  // Soft limit on the cumulative value size read by a single MultiGet, to bound
  // how much it buffers. It always makes progress: at least one key is read
  // even if its value alone exceeds the limit. Once the returned size exceeds
  // the limit, subsequent keys get status Aborted (so a caller can retry them,
  // and cannot loop forever on a single value that by itself exceeds the
  // limit).
  uint64_t value_size_soft_limit = std::numeric_limits<uint64_t>::max();

  // When the number of merge operands applied exceeds this threshold
  // during a successful query, the operation will return a special OK
  // Status with subcode kMergeOperandThresholdExceeded. Currently only applies
  // to point lookups and is disabled by default.
  std::optional<size_t> merge_operand_count_threshold;

  // If true, all data read from underlying storage will be
  // verified against corresponding checksums.
  bool verify_checksums = true;

  // Should the "data block"/"index block" read for this iteration be placed in
  // block cache?
  // Callers may wish to set this field to false for bulk scans.
  // This would help not to the change eviction order of existing items in the
  // block cache.
  bool fill_cache = true;

  // DEPRECATED: This option might be removed in a future release.
  // There should be no noticeable performance difference whether this option
  // is turned on or off when a DB does not use DeleteRange().
  //
  // If true, range tombstones handling will be skipped in key lookup paths.
  // For DB instances that don't use DeleteRange() calls, this setting can
  // be used to optimize the read performance.
  // Note that, if this assumption (of no previous DeleteRange() calls) is
  // broken, stale keys could be served in read paths.
  bool ignore_range_deletions = false;

  // If async_io is enabled, RocksDB will prefetch some of data asynchronously.
  // RocksDB apply it if reads are sequential and its internal automatic
  // prefetching.
  bool async_io = false;

  // Experimental
  //
  // If async_io is set, then this flag controls whether we read SST files
  // in multiple levels asynchronously. Enabling this flag can help reduce
  // MultiGet latency by maximizing the number of SST files read in
  // parallel if the keys in the MultiGet batch are in different levels. It
  // comes at the expense of slightly higher CPU overhead.
  bool optimize_multiget_for_io = true;

  // EXPERIMENTAL
  //
  // An opaque, non-owning context passed to all table implementations for this
  // read. RocksDB does not interpret, manage, or synchronize access to this
  // context. It is intended to let external table implementations consume
  // application-defined read options. The caller is responsible for keeping the
  // context alive for the duration of the read operation (or iterator lifetime)
  // and for any required synchronization.
  void* custom_context = nullptr;

  // *** END options relevant to point lookups (as well as scans) ***
  // *** BEGIN options only relevant to iterators or scans ***

  // RocksDB does auto-readahead for iterators on noticing more than two reads
  // for a table file. The readahead starts at 8KB and doubles on every
  // additional read up to 256KB.
  // This option can help if most of the range scans are large, and if it is
  // determined that a larger readahead than that enabled by auto-readahead is
  // needed.
  // Using a large readahead size (> 2MB) can typically improve the performance
  // of forward iteration on spinning disks.
  size_t readahead_size = 0;

  // A threshold for the number of keys that can be skipped before failing an
  // iterator seek as incomplete. The default value of 0 should be used to
  // never fail a request as incomplete, even on skipping too many keys.
  uint64_t max_skippable_internal_keys = 0;

  // `iterate_lower_bound` defines the smallest key at which the backward
  // iterator can return an entry. Once the bound is passed, Valid() will be
  // false. `iterate_lower_bound` is inclusive ie the bound value is a valid
  // entry.
  //
  // If prefix_extractor is not null, the Seek target and `iterate_lower_bound`
  // need to have the same prefix. This is because ordering is not guaranteed
  // outside of prefix domain.
  //
  // In case of user_defined timestamp, if enabled, iterate_lower_bound should
  // point to key without timestamp part.
  const Slice* iterate_lower_bound = nullptr;

  // "iterate_upper_bound" defines the extent up to which the forward iterator
  // can return entries. Once the bound is reached, Valid() will be false.
  // "iterate_upper_bound" is exclusive ie the bound value is
  // not a valid entry. If prefix_extractor is not null:
  // 1. If options.auto_prefix_mode = true, iterate_upper_bound will be used
  //    to infer whether prefix iterating (e.g. applying prefix bloom filter)
  //    can be used within RocksDB. This is done by comparing
  //    iterate_upper_bound with the seek key.
  // 2. If options.auto_prefix_mode = false, iterate_upper_bound only takes
  //    effect if it shares the same prefix as the seek key. If
  //    iterate_upper_bound is outside the prefix of the seek key, then keys
  //    returned outside the prefix range will be undefined, just as if
  //    iterate_upper_bound = null.
  // If iterate_upper_bound is not null, SeekToLast() will position the iterator
  // at the first key smaller than iterate_upper_bound.
  //
  // In case of user_defined timestamp, if enabled, iterate_upper_bound should
  // point to key without timestamp part.
  const Slice* iterate_upper_bound = nullptr;

  // Specify to create a tailing iterator -- a special iterator that has a
  // view of the complete database (i.e. it can also be used to read newly
  // added data) and is optimized for sequential reads. It will return records
  // that were inserted into the database after the creation of the iterator.
  bool tailing = false;

  // Enable a total order seek regardless of index format (e.g. hash index)
  // used in the table. Some table format (e.g. plain table) may not support
  // this option.
  // If true when calling Get(), we also skip prefix bloom when reading from
  // block based table, which only affects Get() performance.
  bool total_order_seek = false;

  // When true, by default use total_order_seek = true, and RocksDB can
  // selectively enable prefix seek mode if won't generate a different result
  // from total_order_seek, based on seek key, and iterator upper bound.
  // BUG: Using Comparator::IsSameLengthImmediateSuccessor and
  // SliceTransform::FullLengthEnabled to enable prefix mode in cases where
  // prefix of upper bound differs from prefix of seek key has a flaw.
  // If present in the DB, "short keys" (shorter than "full length" prefix)
  // can be omitted from auto_prefix_mode iteration when they would be present
  // in total_order_seek iteration, regardless of whether the short keys are
  // "in domain" of the prefix extractor. This is not an issue if no short
  // keys are added to DB or are not expected to be returned by such
  // iterators. (We are also assuming the new condition on
  // IsSameLengthImmediateSuccessor is satisfied; see its BUG section).
  // A bug example is in DBTest2::AutoPrefixMode1, search for "BUG".
  bool auto_prefix_mode = false;

  // Enforce that the iterator only iterates over the same prefix as the seek.
  // This makes the iterator bounds dependent on the column family's current
  // prefix_extractor, which is mutable. When SST files have been built with
  // the same prefix extractor, prefix filtering optimizations will be used
  // for both Seek and SeekForPrev.
  bool prefix_same_as_start = false;

  // Keep the blocks loaded by the iterator pinned in memory as long as the
  // iterator is not deleted, If used when reading from tables created with
  // BlockBasedTableOptions::use_delta_encoding = false,
  // Iterator's property "rocksdb.iterator.is-key-pinned" is guaranteed to
  // return 1.
  bool pin_data = false;

  // For iterators, RocksDB does auto-readahead on noticing more than two
  // sequential reads for a table file if user doesn't provide readahead_size.
  // The readahead starts at 8KB and doubles on every additional read upto
  // max_auto_readahead_size only when reads are sequential. However at each
  // level, if iterator moves over next file, readahead_size starts again from
  // 8KB.
  //
  // By enabling this option, RocksDB will do some enhancements for
  // prefetching the data.
  bool adaptive_readahead = false;

  // If true, when PurgeObsoleteFile is called in CleanupIteratorState, we
  // schedule a background job in the flush job queue and delete obsolete files
  // in background.
  bool background_purge_on_iterator_cleanup = false;

  // A callback to determine whether relevant keys for this scan exist in a
  // given table based on the table's properties. The callback is passed the
  // properties of each table during iteration. If the callback returns false,
  // the table will not be scanned. This option only affects Iterators and has
  // no impact on point lookups.
  //
  // Iterator creation on read-write DB variants returns InvalidArgument for
  // safety when the target column family's min_tombstones_for_range_conversion
  // is non-zero. The reasoning is that a fully visible iterator may create a
  // range tombstone from tombstones that can be later converted to a range
  // tombstone. If another iterator tries to filter out the table with the
  // tombstones, the reader would expect the deletes to no longer apply, but it
  // actually still does because of the range tombstone that was inserted.
  // IMPORTANT: min_tombstones_for_range_conversion is a dynamic option, so
  // disabling it may allow you to use table_filters again, you must account for
  // the possibility that range tombstones have already been inserted into
  // either the memtable or other sst files.
  //
  // A nullptr, or a pointer to an empty std::function, means no filtering
  // (every table will be scanned).
  //
  // Default: nullptr (every table will be scanned)
  const std::function<bool(const TableProperties&)>* table_filter = nullptr;

  // If auto_readahead_size is set to true, it will auto tune the readahead_size
  // during scans internally based on block cache data when block cache is
  // enabled, iteration upper bound when `iterate_upper_bound != nullptr` and
  // prefix when `prefix_same_as_start == true`
  //
  // Besides enabling block cache, it
  // also requires `iterate_upper_bound != nullptr` or  `prefix_same_as_start ==
  // true` for this option to take effect
  //
  // To be specific, it does the following:
  // (1) When `iterate_upper_bound`
  // is specified, trim the readahead so the readahead does not exceed iteration
  // upper bound
  // (2) When `prefix_same_as_start` is set to true, trim the
  // readahead so data blocks containing keys that are not in the same prefix as
  // the seek key in `Seek()` are not prefetched
  //  - Limition: `Seek(key)` instead of `SeekToFirst()` needs to be called in
  //  order for this trimming to take effect
  //
  // NOTE: - Used for forward Scans only.
  //       - If there is a backward scans, this option will be
  //          disabled internally and won't be enabled again if the forward scan
  //          is issued again.
  //
  // Default: true
  bool auto_readahead_size = true;

  // When set, the iterator may defer loading and/or preparing the value when
  // moving to a different entry (i.e. during SeekToFirst/SeekToLast/Seek/
  // SeekForPrev/Next/Prev operations). This can be used to save on I/O and/or
  // CPU when the values associated with certain keys may not be used by the
  // application. See also IteratorBase::PrepareValue().
  //
  // Note: this option currently only applies to 1) large values stored in blob
  // files using BlobDB and 2) multi-column-family iterators (CoalescingIterator
  // and AttributeGroupIterator). Otherwise, it has no effect.
  //
  // Default: false
  bool allow_unprepared_value = false;

  // EXPERIMENTAL
  //
  // Long-running iterators are holding onto memory and storage resources long
  // after they are obsolete. This setting (when enabled) will fix that problem
  // for as long as iterator periodically makes some progress and its supplied
  // `read_options` was configured with non-nullptr `snapshot` value.
  // The feature is engineered so that the performance impact should be
  // negligible. We expect the default value to be true some time in the future.
  //
  // NOTE 1: Does not have effect on TransactionDB with WRITE_PREPARED or
  //         WRITE_UNPREPARED policies (currently incompatible).
  //
  // NOTE 2: True is not recommended if using user-defined timestamp with
  //         persist_user_defined_timestamps=false and non-nullptr
  //         ReadOptions::timestamp or ReadOptions::iter_start_ts, because
  //         auto-refreshing iterator will not prevent user timestamp
  //         information from being dropped during iteration. Auto-refresh might
  //         be disabled for this combination in the future.
  //
  // Default: false
  bool auto_refresh_iterator_with_snapshot = false;

  // EXPERIMENTAL
  //
  // Per-read index selection. Overrides the default read routing determined
  // by BlockBasedTableOptions::index_mode.
  //
  //   kDefault: use whatever index_mode says.
  //     kStandardOnly/kStandardDefault/kStandardRequired -> standard index.
  //     kCustomDefault/kCustomOnly                       -> custom
  //                                                        IndexFactory index.
  //
  //   kBuiltin: force the standard index for this read.
  //     Useful for debugging, comparing results between indexes, or
  //     temporary fallback. In kCustomOnly mode, the standard index
  //     is a minimal stub -- reads fail loudly instead of returning an
  //     OK empty result.
  //
  //   kPreferCustom: select the custom IndexFactory index when the SST has
  //     one; fall back to the standard index for SSTs that don't (e.g.,
  //     pre-UDI files still on disk during migration). Once the custom
  //     reader is loaded for an SST the per-read selection is strict --
  //     iterator-creation failures surface as Status::Corruption rather
  //     than silently falling back.
  //
  // ReadIndex is a two-way selector because BlockBasedTable supports
  // exactly two index read targets per SST: the standard index (selected
  // by BlockBasedTableOptions::index_type) and at most one custom index
  // from user_defined_index_factory.
  enum class ReadIndex : uint8_t {
    kDefault = 0,
    kBuiltin = 1,
    kPreferCustom = 2,
  };
  ReadIndex read_index = ReadIndex::kDefault;

  // EXPERIMENTAL
  //
  // Legacy selector for a secondary custom index. With read_index=kDefault,
  // the table's custom index takes precedence when it is primary or the only
  // usable index.
  // Otherwise, except with kBuiltin, this factory's Name() must match the SST's
  // custom index name; a mismatch fails reads with InvalidArgument. New code
  // can leave this null and use read_index directly.
  const IndexFactory* table_index_factory = nullptr;

  // EXPERIMENTAL: Optional non-owning provider for data-block storage pinned by
  // scans using this ReadOptions. Applications that set it are attempting
  // advanced performance optimizations and are responsible for ensuring the
  // provider outlives the iterator/read scope and any provider-backed data that
  // can remain pinned by that scope.
  //
  // This is a raw pointer rather than a shared_ptr because ReadOptions is
  // copied through stack frames and iterator internals; a shared_ptr would add
  // refcount overhead to those copies. An internal shadow ReadOptions that
  // strips ownership would add maintenance overhead for this advanced option.
  //
  // Current support is limited to block-based table iterators and MultiScan
  // data-block reads, and is ignored when mmap reads are enabled. When set,
  // supported scan reads bypass the data-block cache and use provider-backed
  // final data-block memory. RocksDB may still use ordinary temporary scratch
  // for serialized block bytes, such as when a block may be compressed. When
  // unset, scans use the normal RocksDB data-block backing for the table: use
  // the configured block cache when present, otherwise use RocksDB-owned block
  // memory. Index/filter blocks keep their normal block-cache behavior.
  //
  // TODO: Extend support to point lookups (Get/MultiGet) once those paths can
  // preserve provider-backed block ownership.
  ReadScopedBlockBufferProvider* read_scoped_block_buffer_provider = nullptr;

  // *** END options only relevant to iterators or scans ***

  // *** BEGIN options for RocksDB internal use only ***

  // EXPERIMENTAL
  Env::IOActivity io_activity = Env::IOActivity::kUnknown;

  // *** END options for RocksDB internal use only ***

  // *** BEGIN per-request settings for internal team use only ***

  // TODO: create a new struct for per-request options, potentially including
  // timestamps in point lookups/scans

  // request_id is a unique id assigned by the application. It is used to allow
  // us to link file system metrics/logs to rocksDB and application logs. This
  // request_id may not be unique to each RocksDB api call - it could refer to
  // an application level request that results in multiple RocksDB api calls
  const std::string* request_id = nullptr;

  // *** END per-request settings for internal team use only ***

  ReadOptions() {}
  ReadOptions(bool _verify_checksums, bool _fill_cache);
  explicit ReadOptions(Env::IOActivity _io_activity);
};

// Options that control write operations
struct WriteOptions {
  // If true, the write will be flushed from the operating system
  // buffer cache (by calling WritableFile::Sync()) before the write
  // is considered complete.  If this flag is true, writes will be
  // slower.
  //
  // If this flag is false, and the machine crashes, some recent
  // writes may be lost.  Note that if it is just the process that
  // crashes (i.e., the machine does not reboot), no writes will be
  // lost even if sync==false.
  //
  // In other words, a DB write with sync==false has similar
  // crash semantics as the "write()" system call.  A DB write
  // with sync==true has similar crash semantics to a "write()"
  // system call followed by "fdatasync()".
  //
  // Default: false
  bool sync = false;

  // If true, writes will not first go to the write ahead log,
  // and the write may get lost after a crash. The backup engine
  // relies on write-ahead logs to back up the memtable, so if
  // you disable write-ahead logs, you must create backups with
  // flush_before_backup=true to avoid losing unflushed memtable data.
  // Default: false
  bool disableWAL = false;

  // If true and if user is trying to write to column families that don't exist
  // (they were dropped),  ignore the write (don't return an error). If there
  // are multiple writes in a WriteBatch, other writes will succeed.
  // Default: false
  bool ignore_missing_column_families = false;

  // If true and we need to wait or sleep for the write request, fails
  // immediately with Status::Incomplete().
  // Default: false
  bool no_slowdown = false;

  // If true, this write request is of lower priority if compaction is
  // behind. In this case, no_slowdown = true, the request will be canceled
  // immediately with Status::Incomplete() returned. Otherwise, it will be
  // slowed down. The slowdown value is determined by RocksDB to guarantee
  // it introduces minimum impacts to high priority writes.
  //
  // Default: false
  bool low_pri = false;

  // If true, this writebatch will maintain the last insert positions of each
  // memtable as hints in concurrent write. It can improve write performance
  // in concurrent writes if keys in one writebatch are sequential. In
  // non-concurrent writes (when concurrent_memtable_writes is false) this
  // option will be ignored.
  //
  // Default: false
  bool memtable_insert_hint_per_batch = false;

  // For writes associated with this option, charge the internal rate
  // limiter (see `DBOptions::rate_limiter`) at the specified priority. The
  // special value `Env::IO_TOTAL` disables charging the rate limiter.
  //
  // Currently the support covers automatic WAL flushes, which happen during
  // live updates (`Put()`, `Write()`, `Delete()`, etc.)
  // when `WriteOptions::disableWAL == false`
  // and `DBOptions::manual_wal_flush == false`.
  //
  // Only `Env::IO_USER` and `Env::IO_TOTAL` are allowed
  // due to implementation constraints.
  //
  // Default: `Env::IO_TOTAL`
  Env::IOPriority rate_limiter_priority = Env::IO_TOTAL;

  // `protection_bytes_per_key` is the number of bytes used to store
  // protection information for each key entry. Currently supported values are
  // zero (disabled) and eight.
  //
  // Default: zero (disabled).
  size_t protection_bytes_per_key = 0;

  // For RocksDB internal use only
  //
  // Default: Env::IOActivity::kUnknown.
  Env::IOActivity io_activity = Env::IOActivity::kUnknown;

  WriteOptions() {}
  explicit WriteOptions(Env::IOActivity _io_activity);
  explicit WriteOptions(
      Env::IOPriority _rate_limiter_priority,
      Env::IOActivity _io_activity = Env::IOActivity::kUnknown);
};

// Options that control flush operations
struct FlushOptions {
  // If true, the flush will wait until the flush is done.
  // Default: true
  bool wait;
  // If true, the flush would proceed immediately even it means writes will
  // stall for the duration of the flush; if false the operation will wait
  // until it's possible to do flush w/o causing stall or until required flush
  // is performed by someone else (foreground call or background thread).
  // Default: false
  bool allow_write_stall;
  // If true, use atomic flush to flush all column families atomically,
  // regardless of the DBOptions::atomic_flush setting. When used with
  // DB::Flush() or internally via GetLiveFilesStorageInfo(), this forces
  // all column families to be flushed in a single atomic operation.
  // Default: false (uses DBOptions::atomic_flush setting).
  bool force_atomic_flush;

  // If true (and `wait` is also true), Flush() will not return until the
  // registered EventListener::OnFlushCompleted callbacks for the flushed
  // memtables have finished running. By default (false), Flush(wait=true) may
  // return as soon as the flush result is committed, which can be before (or
  // while) the OnFlushCompleted callbacks execute on the background flush
  // thread. Set this to true when the caller needs to observe the effects of
  // its OnFlushCompleted listener(s) immediately after Flush() returns.
  // Has no effect when `wait == false`.
  // Default: false
  bool listener_wait;

  FlushOptions()
      : wait(true),
        allow_write_stall(false),
        force_atomic_flush(false),
        listener_wait(false) {}
};

struct FlushWALOptions {
  // If true, it calls `SyncWAL()` afterwards.
  // Default: false
  bool sync;

  // For IO operations associated with flushing the WAL, charge the internal
  // rate limiter (see `DBOptions::rate_limiter`) at the specified priority and
  // pass the priority down to the file system through
  // `IOOptions::rate_limiter_priority`. The special value `Env::IO_TOTAL`
  // disables charging the rate limiter.
  //
  // Default: `Env::IO_TOTAL`
  Env::IOPriority rate_limiter_priority;

  FlushWALOptions() : sync(false), rate_limiter_priority(Env::IO_TOTAL) {}
};

// Create a Logger from provided DBOptions
Status CreateLoggerFromOptions(const std::string& dbname,
                               const DBOptions& options,
                               std::shared_ptr<Logger>* logger);

// CompactionOptions are used in CompactFiles() call.
struct CompactionOptions {
  // DEPRECATED: this option is unsafe because it allows the user to set any
  // `CompressionType` while always using `CompressionOptions` from the
  // `ColumnFamilyOptions`. As a result the `CompressionType` and
  // `CompressionOptions` can easily be inconsistent.
  //
  // Compaction output compression type
  //
  // Default: `kDisableCompressionOption`
  //
  // If set to `kDisableCompressionOption`, RocksDB will choose compression type
  // according to the `ColumnFamilyOptions`. RocksDB takes into account the
  // output level in case the `ColumnFamilyOptions` has level-specific settings.
  CompressionType compression;

  // Compaction will create files of size `output_file_size_limit`.
  // Default: MAX, which means that compaction will create a single file
  uint64_t output_file_size_limit;

  // If > 0, it will replace the option in the DBOptions for this compaction.
  uint32_t max_subcompactions;

  // Allows cancellation of an in-progress manual compaction.
  //
  // Cancellation can be delayed waiting on automatic compactions when used
  // together with `exclusive_manual_compaction == true`.
  std::atomic<bool>* canceled;
  // NOTE: Calling DisableManualCompaction() will not override the
  // canceled variable in CompactionOptions, as it does for CompactRangeOptions
  // - this is because ManualCompactionState is not used

  // Create output compaction file using this file temperature. If unset, will
  // default to "last_level_temperature" if output level is last level otherwise
  // "default_write_temperature"
  Temperature output_temperature_override = Temperature::kUnknown;

  // Option to optimize the manual compaction by enabling trivial move for non
  // overlapping files.
  // Default: false
  bool allow_trivial_move;

  CompactionOptions()
      : compression(kDisableCompressionOption),
        output_file_size_limit(std::numeric_limits<uint64_t>::max()),
        max_subcompactions(0),
        canceled(nullptr),
        allow_trivial_move(false) {}
};

// For level based compaction, we can configure if we want to skip/force
// bottommost level compaction.
enum class BottommostLevelCompaction {
  // Skip bottommost level compaction.
  kSkip,
  // Only compact bottommost level if there is a compaction filter.
  // This is the default option.
  // Similar to kForceOptimized, when compacting bottommost level, avoid
  // double-compacting files
  // created in the same manual compaction.
  kIfHaveCompactionFilter,
  // Always compact bottommost level.
  kForce,
  // Always compact bottommost level but in bottommost level avoid
  // double-compacting files created in the same compaction.
  kForceOptimized,
};

// For manual compaction, we can configure if we want to skip/force garbage
// collection of blob files.
enum class BlobGarbageCollectionPolicy {
  // Force blob file garbage collection.
  kForce,
  // Skip blob file garbage collection.
  kDisable,
  // Inherit blob file garbage collection policy from ColumnFamilyOptions.
  kUseDefault,
};

// CompactRangeOptions is used by CompactRange() call.
struct CompactRangeOptions {
  // If true, no other compaction will run at the same time as this
  // manual compaction.
  //
  // Default: false
  bool exclusive_manual_compaction = false;

  // If true, compacted files will be moved to the minimum level capable
  // of holding the data or given level (specified non-negative target_level).
  bool change_level = false;
  // If change_level is true and target_level have non-negative value, compacted
  // files will be moved to target_level.
  int target_level = -1;
  // Compaction outputs will be placed in options.db_paths[target_path_id].
  // Behavior is undefined if target_path_id is out of range.
  uint32_t target_path_id = 0;
  // By default level based compaction will only compact the bottommost level
  // if there is a compaction filter
  BottommostLevelCompaction bottommost_level_compaction =
      BottommostLevelCompaction::kIfHaveCompactionFilter;
  // If true, will execute immediately even if doing so would cause the DB to
  // enter write stall mode. Otherwise, it'll sleep until load is low enough.
  bool allow_write_stall = false;
  // If > 0, it will replace the option in the DBOptions for this compaction.
  uint32_t max_subcompactions = 0;
  // Set user-defined timestamp low bound, the data with older timestamp than
  // low bound maybe GCed by compaction. Default: nullptr
  const Slice* full_history_ts_low = nullptr;

  // Allows cancellation of an in-progress manual compaction.
  //
  // Cancellation can be delayed waiting on automatic compactions when used
  // together with `exclusive_manual_compaction == true`.
  std::atomic<bool>* canceled = nullptr;
  // NOTE: Calling DisableManualCompaction() overwrites the user-provided
  // canceled variable in CompactRangeOptions.
  // Typically, when CompactRange is being called in one thread (t1) with
  // canceled = false, and DisableManualCompaction is being called in the
  // other thread (t2), manual compaction is disabled normally, even if the
  // compaction iterator may still scan a few items before *canceled is
  // set to true

  // If set to kForce, RocksDB will override enable_blob_file_garbage_collection
  // to true; if set to kDisable, RocksDB will override it to false, and
  // kUseDefault leaves the setting in effect. This enables customers to both
  // force-enable and force-disable GC when calling CompactRange.
  BlobGarbageCollectionPolicy blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kUseDefault;

  // If set to < 0 or > 1, RocksDB leaves blob_garbage_collection_age_cutoff
  // from ColumnFamilyOptions in effect. Otherwise, it will override the
  // user-provided setting. This enables customers to selectively override the
  // age cutoff.
  double blob_garbage_collection_age_cutoff = -1;
};

// IngestExternalFileOptions setting guide:
//
// The options in IngestExternalFileOptions interact in complex ways depending
// on the source and overlap of SST files. Below is a summary of recommended
// non-default settings for common use cases:
//
// 1. Ingesting only SST writer generated non-overlapping SSTs that are not
// expected to overlap with existing data:
//    - Optionally set fail_if_not_bottommost_level = true to enforce placement
//    in the last level. This is better paird with SST partitioner to guarantee
//    that there are no existing file with keys across the ingesting key range.
//    - Set allow_blocking_flush to false: Not expecting to overlap with
//    memtable and cause a flush.
//    - If snapshot consistency is not expected, set snapshot_consistency to
//    false and allow_global_seqno to false. allow_global_seqno = false will
//    fail ingestion if any input file overlap with each other.
//
// 2. Ingesting SST writer generated overlapping SSTs:
//    - order files with older updates first, newer overwrites later.
//    - Set allow_global_seqno = true since newer files need to be assigned
//    larger sequence numbers.
//
// 3. Ingesting DB generated SSTs: overlapping with target CF data is not
// allowed. Input files are allowed to contain both DB generated files and SST
// file writer generated files. They will all be treated as DB generated.
//    - Set allow_db_generated_files = true.
//    - Set snapshot_consistency = false: snapshot consistency requires
// assigning higher sequence number to ingested files. DB generated files
// don't support global seqno assignment yet.
//    - Set allow_blocking_flush to false: Not expecting to overlap with
//    memtable and cause a flush.
//    - If the source live DB is running, set link_files = true instead of
//    move_files.
// 3a) SST files are non-overlapping and all keys have seqno 0: e.g., a
// temporary RocksDB instance used to sort some data, and compacts all
// data into the last level before ingestion.
//    - Optionally set fail_if_not_bottommost_level = true to enforce placement
//    in the last level.
// 3b) SST files are overlapping, e.g. ingesting files from one CF to another.
//    - Ensure older updates are ordered first and newer updates are ordered
//    later. See more in option comment for allow_db_generated_files.
struct IngestExternalFileOptions {
  // Can be set to true to move the files instead of copying them.
  // The input files will be unlinked after successful ingestion.
  // The implementation depends on hard links (LinkFile) instead of traditional
  // move (RenameFile) to maximize the chances to restore to the original
  // state upon failure.
  bool move_files = false;
  // Same as move_files except that input files will NOT be unlinked.
  // Only one of `move_files` and `link_files` can be set at the same time.
  bool link_files = false;
  // If set to true, ingestion falls back to copy when hard linking fails.
  // This applies to both `move_files` and `link_files`.
  bool failed_move_fall_back_to_copy = true;
  // If set to false, an ingested file keys could appear in existing snapshots
  // that where created before the file was ingested.
  bool snapshot_consistency = true;
  // Enables assiging a global sequence number to each ingested file, i.e.,
  // all keys in the ingested file will be treated as having this seqno.
  // If set to false, we will use the sequence numbers in the ingested file
  // as is, and IngestExternalFile() will fail if the ingested key range
  // overlaps with existing keys or tombstones or output of ongoing compaction
  // in the CF (the conditions under which a global seqno must be assigned to
  // the ingested file).
  // If the ingested files overlap with each other, we need to assign global
  // sequence to the ingested files and this option needs to be enabled. One
  // exception to this is when ingesting DB generated SST files (see option
  // allow_db_generated_files below). DB generated files do not support
  // global seqno assignment and can be ingested even if they overlap with
  // each other. This option has no effect when allow_db_generated_files is
  // enabled.
  bool allow_global_seqno = true;
  // Normally (true), IngestExternalFile() will trigger and block for flushing
  // memtable(s) if there is overlap between ingested files and memtable(s). If
  // allow_blocking_flush is set to false, IngestExternalFile() will fail if the
  // file key range overlaps with the memtable key range (memtable flush
  // required).
  bool allow_blocking_flush = true;
  // Set to true if you would like duplicate keys in the file being ingested
  // to be skipped rather than overwriting existing data under that key.
  // Use case: back-fill of some historical data in the database without
  // over-writing existing newer version of data.
  // This option could only be used if the CF has been running
  // with cf_allow_ingest_behind=true since CF creation (or before any write).
  // All files will be ingested at the bottommost level with seqno=0.
  bool ingest_behind = false;
  // Set to true if you would like to verify the checksums of each block of the
  // external SST file before ingestion.
  // Warning: setting this to true causes slowdown in file ingestion because
  // the external SST file has to be read.
  bool verify_checksums_before_ingest = false;
  // When verify_checksums_before_ingest = true, RocksDB uses default
  // readahead setting to scan the file while verifying checksums before
  // ingestion.
  // Users can override the default value using this option.
  // Using a large readahead size (> 2MB) can typically improve the performance
  // of forward iteration on spinning disks.
  size_t verify_checksums_readahead_size = 0;
  // Set to TRUE if user wants to verify the sst file checksum of ingested
  // files. The DB checksum function will generate the checksum of each
  // ingested file (if file_checksum_gen_factory is set) and compare the
  // checksum function name and checksum with the ingested checksum information.
  //
  // If this option is set to True: 1) if DB does not enable checksum
  // (file_checksum_gen_factory == nullptr), the ingested checksum information
  // will be ignored; 2) If DB enable the checksum function, we calculate the
  // sst file checksum after the file is moved or copied and compare the
  // checksum and checksum name. If checksum or checksum function name does
  // not match, ingestion will be failed. If the verification is successful,
  // checksum and checksum function name will be stored in Manifest.
  // If this option is set to FALSE, 1) if DB does not enable checksum,
  // the ingested checksum information will be ignored; 2) if DB enable the
  // checksum, we only verify the ingested checksum function name and we
  // trust the ingested checksum. If the checksum function name matches, we
  // store the checksum in Manifest. DB does not calculate the checksum during
  // ingestion. However, if no checksum information is provided with the
  // ingested files, DB will generate the checksum and store in the Manifest.
  bool verify_file_checksum = true;
  // Set to TRUE if user wants file to be ingested to the last level. An
  // error of Status::TryAgain() will be returned if a file cannot fit in the
  // last level when calling
  // DB::IngestExternalFile()/DB::IngestExternalFiles(). The user should clear
  // the last level in the overlapping range before re-attempt.
  //
  // ingest_behind takes precedence over fail_if_not_bottommost_level.
  //
  // XXX: "bottommost" is obsolete/confusing terminology to refer to last level
  bool fail_if_not_bottommost_level = false;
  // EXPERIMENTAL, SUBJECT TO CHANGE
  //
  // Enables special mode of ingestion that allows files generated by a live DB,
  // instead of SstFileWriter. When true:
  // - Allows files to be ingested when their cf_id doesn't match the CF they
  //   are being ingested into.
  // - Allows files with any sequence numbers to be ingested.
  // - Original sequence numbers are preserved (no reassignment).
  //
  // REQUIREMENTS:
  // - Ingested files must NOT overlap with any existing data in the DB. Since
  //   no sequence number reassignment is performed on db generated files.
  //   Ingestion will fail if any overlap is detected. However, input files
  //   are allowed to overlap with each other when this option is enabled. This
  //   is useful when ingesting multiple levels of files from a CF, where
  //   levels naturally overlap with each other.
  // - CAUTION: If input files overlap with each other, then for any given user
  //   key appearing in multiple files, earlier files MUST have smaller sequence
  //   numbers than later files. Later files will be placed at a higher level
  //   (smaller level number). This is to ensure the LSM invariant where for
  //   the same key, recent updates are in higher levels. This means that
  //   if you are ingesting files from multiple levels of a CF, you should
  //   put files from lower levels first, and files from higher levels later.
  //   Example for getting files from a CF for ingestion:
  //
  // ColumnFamilyMetaData cf_meta;
  // from_db->GetColumnFamilyMetaData(from_cf, &cf_meta);
  // // iterate in reverse to start from lowest level
  // for (auto level_meta = cf_meta.levels.rbegin();
  //      level_meta != cf_meta.levels.rend(); ++level_meta) {
  //   // L0 files need to be added in reverse order so we iterate in reverse
  //   // within a level too
  //   for (auto file_meta = level_meta->files.rbegin();
  //        file_meta != level_meta->files.rend(); ++file_meta) {
  //     // Add file for ingestion
  //   }
  // }
  //
  //   WARNING: Violating the sequence number ordering requirement will cause
  //   LSM invariant violations and may lead to incorrect reads or data
  //   corruption.
  // - If you would like to enforce that the ingested files do not overlap
  //   with each other, you can set `fail_if_not_bottommost_level` to true.
  //   If ingested files overlap with each other, some file will be placed
  //   above Lmax, failing the ingestion if the option is set.
  bool allow_db_generated_files = false;

  // Controls whether data and metadata blocks (e.g. index, filter) read during
  // file ingestion will be added to block cache.
  // Users may wish to set this to false when bulk loading into a CF that is not
  // available for reads yet.
  // When ingesting to multiple families, this option should be the same across
  // ingestion options.
  bool fill_cache = true;

  // Controls whether external file ingestion should prefetch index and filter
  // blocks while opening table readers during commit. Setting this to false can
  // reduce commit latency for bulk loads into Lmax when
  // (BlockBasedTableOptions::cache_index_and_filter_blocks=true or partitioned
  // filters/indexes are enabled).
  bool prefetch_lmax_index_and_filter_blocks = true;

  // Maximum number of threads used to open table readers for the files being
  // ingested during commit, can speed up ingestion performance, when ingesting
  // multiple files at once.
  int file_opening_threads = 1;

  bool operator==(const IngestExternalFileOptions& rhs) const = default;
};

// Opaque per-file metadata accepted through IngestExternalFileArg::file_infos.
// Supplying it lets DB::IngestExternalFiles() and DB::PrepareFileIngestion()
// prepare ingestion without re-opening and scanning the SST to recompute file
// metadata. It is produced by SstFileWriter::Finish() for writer-created files
// or by DB::GetPreparedFileInfoForExternalSstIngestion() for live DB-generated
// files. SerializePreparedFileInfo() and DeserializePreparedFileInfo() in
// rocksdb/db.h can be used to persist it without exposing its contents.
struct PreparedFileInfo;

// It is valid that files_checksums and files_checksum_func_names are both
// empty (no checksum information is provided for ingestion). Otherwise,
// their sizes should be the same as external_files. The file order should
// be the same in three vectors and guaranteed by the caller.
// Note that, we assume the temperatures of this batch of files to be
// ingested are the same.
struct IngestExternalFileArg {
  ColumnFamilyHandle* column_family = nullptr;
  std::vector<std::string> external_files;
  IngestExternalFileOptions options;
  std::vector<std::string> files_checksums;
  std::vector<std::string> files_checksum_func_names;
  // A hint as to the temperature for *reading* the files to be ingested.
  Temperature file_temperature = Temperature::kUnknown;
  // EXPERIMENTAL: When specified, existing keys in the given range will be
  // cleared atomically as part of the ingestion, where the ingested files are
  // logically applied on top of the cleared key range.
  // * If both `start` and `limit` are nullptr, the entire column family is
  // cleared; however, setting just one bound to nullptr is not yet supported.
  // * When a range is specified, all the external files in this batch must
  //   be contained in that key range.
  // * Checks for memtable overlap and possible blocking flush will apply
  //   to this range (not just the file ranges).
  // * Not compatible with ingest_behind=true.
  // * When options.snapshot_consistency = false, the range is cleared
  // similarly to DeleteFilesInRange. With universal compaction and a table
  // factory that supports range deletion, a partial overlap is handled by
  // retaining the overlapping file and adding a range tombstone below the
  // ingested data. This requires allow_global_seqno=true,
  // allow_db_generated_files=false, and no user-defined timestamps. Other
  // configurations fail on partial overlap.
  //   * fail_if_not_bottommost_level=true rejects partial overlap because the
  //     retained file requires the tombstone and replacement files to be
  //     placed on higher levels.
  // * options.snapshot_consistency = true is not yet supported.
  // * The range is half-open: start is inclusive and limit is exclusive.
  std::optional<RangeOpt> atomic_replace_range;

  // Optimizes for prepare performance, see `PreparedFileInfo` for details.
  // The owning handle must stay alive until ingestion completes.
  // Because the file is not opened, ingestion does not read its storage
  // temperature and trusts the caller-supplied `file_temperature` hint as-is.
  std::vector<const PreparedFileInfo*> file_infos;
};

enum TraceFilterType : uint64_t {
  // Trace all the operations
  kTraceFilterNone = 0x0,
  // Do not trace the get operations
  kTraceFilterGet = 0x1 << 0,
  // Do not trace the write operations
  kTraceFilterWrite = 0x1 << 1,
  // Do not trace the `Iterator::Seek()` operations
  kTraceFilterIteratorSeek = 0x1 << 2,
  // Do not trace the `Iterator::SeekForPrev()` operations
  kTraceFilterIteratorSeekForPrev = 0x1 << 3,
  // Do not trace the `MultiGet()` operations
  kTraceFilterMultiGet = 0x1 << 4,
};

// TraceOptions is used for StartTrace
struct TraceOptions {
  // To avoid the trace file size grows large than the storage space,
  // user can set the max trace file size in Bytes. Default is 64GB
  uint64_t max_trace_file_size = uint64_t{64} * 1024 * 1024 * 1024;
  // Specify trace sampling option, i.e. capture one per how many requests.
  // Default to 1 (capture every request).
  uint64_t sampling_frequency = 1;
  // Note: The filtering happens before sampling.
  uint64_t filter = kTraceFilterNone;
  // When true, the order of write records in the trace will match the order of
  // the corresponding write records in the WAL and applied to the DB. There may
  // be a performance penalty associated with preserving this ordering.
  //
  // Default: false. This means write records in the trace may be in an order
  // different from the WAL's order.
  bool preserve_write_order = false;
  // When true, each query record also holds the options of its call, and
  // replay uses them instead of default options:
  // - writes: WriteOptions::sync, disableWAL, no_slowdown and
  //   rate_limiter_priority;
  // - Get, MultiGet and iterator seeks: ReadOptions::verify_checksums,
  //   fill_cache, ignore_range_deletions, async_io, optimize_multiget_for_io,
  //   tailing, total_order_seek, auto_prefix_mode, prefix_same_as_start,
  //   pin_data, adaptive_readahead, background_purge_on_iterator_cleanup,
  //   auto_readahead_size, allow_unprepared_value, read_tier,
  //   rate_limiter_priority, readahead_size, max_skippable_internal_keys and
  //   value_size_soft_limit.
  // Other fields (snapshots, timestamps, deadlines, callbacks and other
  // pointers) are not recorded. See WriteQueryTraceRecord::GetWriteOptions()
  // and GetQueryTraceRecord::GetReadOptions().
  //
  // Off by default because older RocksDB versions do not know these record
  // fields: their release builds ignore them, their debug builds assert.
  bool include_read_write_options = false;
};

// ImportColumnFamilyOptions is used by ImportColumnFamily()
struct ImportColumnFamilyOptions {
  // Can be set to true to move the files instead of copying them.
  bool move_files = false;
};

// Options used with DB::GetApproximateSizes()
struct SizeApproximationOptions {
  // Defines whether the returned size should include the recently written
  // data in the memtables. If set to false, at least one of include_files or
  // include_blob_files must be true.
  bool include_memtables = false;
  // Defines whether the returned size should include data serialized to disk.
  // If set to false, at least one of include_memtables or include_blob_files
  // must be true.
  bool include_files = true;
  // Defines whether the returned size should include an approximation of
  // blob file data in the key range. When enabled, the total blob file size
  // is prorated by the ratio of SST data in the range to the total SST data:
  //
  //   blob_size_in_range ~= total_blob_size * (sst_in_range / total_sst)
  //
  // Limitations of this approximation:
  // - Assumes blob data is distributed proportionally to SST data, which
  //   may not hold if blob value sizes vary significantly across keys.
  // - If there are no SST files (all data in memtables), the blob size
  //   contribution will be 0 even if blob files exist on disk.
  // Default: false (for backward compatibility).
  bool include_blob_files = false;
  // When approximating the files total size that is used to store a keys range
  // using DB::GetApproximateSizes, allow approximation with an error margin of
  // up to total_files_size * files_size_error_margin. This allows to take some
  // shortcuts in files size approximation, resulting in better performance,
  // while guaranteeing the resulting error is within a reasonable margin.
  // E.g., if the value is 0.1, then the error margin of the returned files size
  // approximation will be within 10%.
  // If the value is non-positive - a more precise yet more CPU intensive
  // estimation is performed.
  double files_size_error_margin = -1.0;
};

struct CompactionServiceOptionsOverride {
  Env* env = Env::Default();
  std::shared_ptr<FileChecksumGenFactory> file_checksum_gen_factory = nullptr;

  const Comparator* comparator = BytewiseComparator();
  std::shared_ptr<MergeOperator> merge_operator = nullptr;
  const CompactionFilter* compaction_filter = nullptr;
  std::shared_ptr<CompactionFilterFactory> compaction_filter_factory = nullptr;
  std::shared_ptr<const SliceTransform> prefix_extractor = nullptr;
  std::shared_ptr<TableFactory> table_factory;
  std::shared_ptr<SstPartitionerFactory> sst_partitioner_factory = nullptr;

  // Only subsets of events are triggered in remote compaction worker, like:
  // `OnTableFileCreated`, `OnTableFileCreationStarted`,
  // `ShouldBeNotifiedOnFileIO` `OnSubcompactionBegin`,
  // `OnSubcompactionCompleted`, etc. Worth mentioning, `OnCompactionBegin` and
  // `OnCompactionCompleted` won't be triggered. They will be triggered on the
  // primary DB side.
  std::vector<std::shared_ptr<EventListener>> listeners;

  // statistics is used to collect DB operation metrics, the metrics won't be
  // returned to CompactionService primary host, to collect that, the user needs
  // to set it here.
  std::shared_ptr<Statistics> statistics = nullptr;

  // Info Log. If not overriden, default one will be used.
  std::shared_ptr<Logger> info_log = nullptr;

  // Only compaction generated SST files use this user defined table properties
  // collector.
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      table_properties_collector_factories;

  // All other options to override. Unknown options will be ignored.
  std::unordered_map<std::string, std::string> options_map;
};

struct OpenAndCompactOptions {
  // Allows cancellation of an in-progress compaction.
  std::atomic<bool>* canceled = nullptr;

  // Maximum number of times to retry opening the source DB as a secondary
  // after the initial attempt fails because CURRENT or MANIFEST was replaced
  // concurrently. A value of zero disables retries.
  //
  // Default: 2
  uint32_t max_secondary_open_retries = 2;

  // EXPERIMENTAL
  //
  // Controls whether OpenAndCompact() should attempt to resume from previously
  // persisted compaction progress or start fresh.
  //
  // When `allow_resumption = true`:
  // - OpenAndCompact() attempts to resume from previously persisted compaction
  //   progress stored in `output_directory`
  // - During execution, it periodically persists new progress to the same
  //   directory, allowing future calls to continue from where the previous
  //   compaction left off.
  // - Fallback behavior: If resumption cannot be fulfilled (e.g., due to
  //   corrupted or missing resume state), the system will attempt to start a
  //   fresh compaction as a best-effort fallback by cleaning related files in
  //   the `output_directory` to achieve a clean state. If even the fresh
  //   compaction cannot be started, a non-OK status will be returned.
  // - Important: Resume attempts will be ineffective if the underlying
  //   conditions that caused the previous OpenAndCompact() failure still
  //   persist. The same non-OK status will likely be returned unless the root
  //   cause has been resolved.
  // - Progress persistence is sequential and best-effort, triggered upon
  //   completion of each new output file. If compaction is interrupted while
  //   creating an output file (before its completion), that partial work will
  //   need to be redone upon resumption.
  //
  // When `allow_resumption = false`:
  // - OpenAndCompact() starts a fresh compaction from scratch.
  // - No progress will be saved during execution, so interruptions require
  //   starting over completely.
  // - CRITICAL REQUIREMENT: The `output_directory` associated MUST be empty
  //   before calling OpenAndCompact(). Any existing files (including resume
  //   state or output files from previous runs) may cause correctness errors.
  //
  // Limitation: Currently incompatible with paranoid_file_checks=true. The
  // option is effectively disabled when `paranoid_file_checks` is enabled.
  bool allow_resumption = false;
};

struct LiveFilesStorageInfoOptions {
  // Whether to populate FileStorageInfo::file_checksum* or leave blank
  bool include_checksum_info = false;
  // Flushes memtables if total size in bytes of live WAL files is >= this
  // number (and DB is not read-only).
  // Default: always force a flush without checking sizes.
  uint64_t wal_size_for_flush = 0;
  // If true, use atomic flush to flush all column families atomically,
  // regardless of the DBOptions::atomic_flush setting. This ensures that the
  // checkpoint captures a consistent view across all column families.
  // Only takes effect when a flush is actually performed (i.e., not suppressed
  // by wal_size_for_flush).
  // Default: false (uses DBOptions::atomic_flush setting).
  bool atomic_flush = false;
};

struct WaitForCompactOptions {
  // A boolean to abort waiting in case of a pause (PauseBackgroundWork()
  // called) If true, Status::Aborted will be returned immediately. If false,
  // ContinueBackgroundWork() must be called to resume the background jobs.
  // Otherwise, jobs that were queued, but not scheduled yet may never finish
  // and WaitForCompact() may wait indefinitely (if timeout is set, it will
  // expire and return Status::TimedOut).
  bool abort_on_pause = false;

  // A boolean to flush all column families before starting to wait.
  bool flush = false;

  // A boolean to wait for purge to complete
  bool wait_for_purge = false;

  // A boolean to call Close() after waiting is done. By the time Close() is
  // called here, there should be no background jobs in progress and no new
  // background jobs should be added. DB may not have been closed if Close()
  // returned Aborted status due to unreleased snapshots in the system. See
  // comments in DB::Close() for details.
  bool close_db = false;

  // Timeout in microseconds for waiting for compaction to complete.
  // Status::TimedOut will be returned if timeout expires.
  // when timeout == 0, WaitForCompact() will wait as long as there's background
  // work to finish.
  std::chrono::microseconds timeout = std::chrono::microseconds::zero();
};

}  // namespace ROCKSDB_NAMESPACE
