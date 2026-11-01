// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <memory>

#include "rocksdb/blob_file_partition_strategy.h"
#include "rocksdb/cache.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/universal_compaction.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class SliceTransform;
class TablePropertiesCollectorFactory;
class TableFactory;
struct Options;

enum CompactionStyle : char {
  // level based compaction style
  kCompactionStyleLevel = 0x0,
  // Universal compaction style
  kCompactionStyleUniversal = 0x1,
  // FIFO compaction style
  kCompactionStyleFIFO = 0x2,
  // Disable background compaction. Compaction jobs are submitted
  // via CompactFiles().
  kCompactionStyleNone = 0x3,
};

// In Level-based compaction, it Determines which file from a level to be
// picked to merge to the next level. We suggest people try
// kMinOverlappingRatio first when you tune your database.
enum CompactionPri : char {
  // Slightly prioritize larger files by size compensated by #deletes
  kByCompensatedSize = 0x0,
  // First compact files whose data's latest update time is oldest.
  // Try this if you only update some hot keys in small ranges.
  kOldestLargestSeqFirst = 0x1,
  // First compact files whose range hasn't been compacted to the next level
  // for the longest. If your updates are random across the key space,
  // write amplification is slightly better with this option.
  kOldestSmallestSeqFirst = 0x2,
  // First compact files whose ratio between overlapping size in next level
  // and its size is the smallest. It in many cases can optimize write
  // amplification.
  // Files marked for compaction will be prioritized over files that are not
  // marked.
  kMinOverlappingRatio = 0x3,
  // Keeps a cursor(s) of the successor of the file (key range) was/were
  // compacted before, and always picks the next files (key range) in that
  // level. The file picking process will cycle through all the files in a
  // round-robin manner.
  kRoundRobin = 0x4,
};

struct FileTemperatureAge {
  Temperature temperature = Temperature::kUnknown;
  uint64_t age = 0;
  bool operator==(const FileTemperatureAge& rhs) const = default;
};

struct CompactionOptionsFIFO {
  // once the total sum of table files reaches this, we will delete the oldest
  // table file
  // Default: 1GB
  uint64_t max_table_files_size;

  // If true, try to do compaction to compact smaller files into larger ones.
  // Minimum files to compact follows options.level0_file_num_compaction_trigger
  // and compaction won't trigger if average compact bytes per del file is
  // larger than options.write_buffer_size. This is to protect large files
  // from being compacted again.
  // Default: false;
  bool allow_compaction = false;

  // DEPRECATED
  // When not 0, if the data in the file is older than this threshold, RocksDB
  // will soon move the file to warm temperature.
  uint64_t age_for_warm = 0;

  // EXPERIMENTAL
  // Age (in seconds) threshold for different file temperatures.
  // When not empty, each element specifies an age threshold `age` and a
  // temperature such that if all the data in a file is older than `age`,
  // RocksDB will compact the file to the specified `temperature`. Oldest file
  // will be considered first. Only one file is compacted at a time,
  // so multiple files qualifying to be compacted to be same temperature
  // won't be merged together.
  //
  // Note:
  // - Flushed files will always have temperature kUnknown.
  // - Compaction output files will have temperature kUnknown by default, so
  //   only temperatures other than kUnknown needs to be specified.
  // - The elements should be in increasing order with respect to `age` field.
  //
  // Dynamically changeable through SetOptions() API, e.g.,
  //   SetOptions("compaction_options_fifo",
  //   "{file_temperature_age_thresholds={
  //    {age=10;temperature=kWarm}:{age=20;temperature=kCold}}}")
  // In this example, all files that are at least 20 seconds old will be
  // compacted and output files will have temperature kCold. All files that are
  // at least 10 seconds old but younger than 20 seconds will be compacted to
  // files with temperature kWarm.
  //
  // Default: empty
  std::vector<FileTemperatureAge> file_temperature_age_thresholds{};

  // EXPERIMENTAL
  // If true, when compaction is picked for kChangeTemperature reason,
  // allow the trivia copy of the sst file from source FileSystem to
  // destination FileSystem. If false, the changeTemperature will be
  // the non-trivial copy by iterating/appending blocks by blocks of the
  // sst file.
  bool allow_trivial_copy_when_change_temperature = false;

  // EXPERIMENTAL
  // If 'allow_trivia_copy_op_when_change_temperature=true', the tmp buffer size
  // to copy the file from the source FileSystem to the destnation FileSystem.
  // If 'allow_trivia_copy_op_when_change_temperature=false', this field will
  // not be used. The minmum buffer size must be at least 4KiB
  uint64_t trivial_copy_buffer_size = 4096;

  // When non-zero, FIFO compaction uses the combined size of SST files and
  // blob files for size-based trimming decisions. When the total data size
  // (SST + blob) exceeds this limit, the oldest SST files are dropped along
  // with their associated blob files.
  //
  // When non-zero, this takes precedence over max_table_files_size for all
  // FIFO compaction decisions: size-based dropping, TTL threshold checks,
  // and compaction score computation. max_table_files_size is ignored.
  //
  // When zero (default), FIFO compaction uses max_table_files_size which
  // only considers SST file sizes, maintaining backward compatibility.
  //
  // This option is primarily intended for use with integrated BlobDB where
  // blob files can represent a significant portion of the total data.
  //
  // Dynamically changeable through SetOptions() API.
  // Default: 0 (use max_table_files_size behavior)
  uint64_t max_data_files_size = 0;

  // When true, enables a capacity-derived intra-L0 compaction strategy
  // optimized for BlobDB workloads where SST files are much smaller than
  // write_buffer_size. Uses the observed key/value size ratio (SST vs blob
  // file sizes) to compute a target compacted file size, producing uniform
  // files for predictable FIFO trimming.
  //
  // Uses level0_file_num_compaction_trigger as the target max L0 file count.
  //
  // When max_compaction_bytes is 0, the target is auto-calculated from the
  // data capacity and observed SST/blob ratio. When max_compaction_bytes is
  // explicitly set to a non-zero value, it overrides the auto-calculated
  // target.
  //
  // Recommends:
  //   - allow_compaction = true (master switch for intra-L0 compaction)
  //   - max_data_files_size > 0 (needed to compute the target file size)
  // If these are not met, kv_ratio compaction is skipped and the old
  // cost-based intra-L0 compaction algorithm is used as a fallback.
  //
  // When false, the old intra-L0 strategy is used if allow_compaction is
  // true (PickCostBasedIntraL0Compaction with 1.1 * write_buffer_size guard).
  //
  // Dynamically changeable through SetOptions() API.
  // Default: false
  bool use_kv_ratio_compaction = false;

  CompactionOptionsFIFO() : max_table_files_size(1 * 1024 * 1024 * 1024) {}
  CompactionOptionsFIFO(uint64_t _max_table_files_size, bool _allow_compaction)
      : max_table_files_size(_max_table_files_size),
        allow_compaction(_allow_compaction) {}

  bool operator==(const CompactionOptionsFIFO& rhs) const = default;
};

// The control option of how the cache tiers will be used. Currently rocksdb
// support block cache (volatile tier), secondary cache (non-volatile tier).
// In the future, we may add more caching layers.
enum class CacheTier : uint8_t {
  kVolatileTier = 0,
  kVolatileCompressedTier = 0x01,
  kNonVolatileBlockTier = 0x02,
};

enum UpdateStatus {     // Return status For inplace update callback
  UPDATE_FAILED = 0,    // Nothing to update
  UPDATED_INPLACE = 1,  // Value updated inplace
  UPDATED = 2,          // No inplace update. Merged value set
};

enum class PrepopulateBlobCache : uint8_t {
  kDisable = 0x0,    // Disable prepopulate blob cache
  kFlushOnly = 0x1,  // Prepopulate blobs during flush only
};

// Bitmask enum for verify output flags during compaction.
// This allows fine-grained control over what verification is performed
// on compaction output files and when it's enabled.
enum class VerifyOutputFlags : uint32_t {
  kVerifyNone = 0x0,  // No verification

  // First set of bits: type of verifications
  kVerifyBlockChecksum = 1 << 0,  // Verify block checksums
  kVerifyIteration = 1 << 1,      // Verify iteration and full key/value hash
                                  // by comparing the one inserted into a
                                  // file, and what is read back.

  kVerifyFileChecksum = 1 << 2,  // Verify file-level checksum

  // Second set of bits: when to enable verification
  kEnableForLocalCompaction = 1 << 10,   // Enable for local compaction
  kEnableForRemoteCompaction = 1 << 11,  // Enable for remote compaction

  // TODO - Implement
  // kEnableForFlush = 1 << 12,  // Enable for flush

  kVerifyAll = 0xFFFFFFFF,
};

inline VerifyOutputFlags operator|(VerifyOutputFlags lhs,
                                   VerifyOutputFlags rhs) {
  using T = std::underlying_type_t<VerifyOutputFlags>;
  return static_cast<VerifyOutputFlags>(static_cast<T>(lhs) |
                                        static_cast<T>(rhs));
}

inline VerifyOutputFlags& operator|=(VerifyOutputFlags& lhs,
                                     VerifyOutputFlags rhs) {
  lhs = lhs | rhs;
  return lhs;
}

inline VerifyOutputFlags operator&(VerifyOutputFlags lhs,
                                   VerifyOutputFlags rhs) {
  using T = std::underlying_type_t<VerifyOutputFlags>;
  return static_cast<VerifyOutputFlags>(static_cast<T>(lhs) &
                                        static_cast<T>(rhs));
}

inline VerifyOutputFlags& operator&=(VerifyOutputFlags& lhs,
                                     VerifyOutputFlags rhs) {
  lhs = lhs & rhs;
  return lhs;
}

inline bool operator!(VerifyOutputFlags flag) {
  return flag == VerifyOutputFlags::kVerifyNone;
}

struct AdvancedColumnFamilyOptions {
  // The maximum number of write buffers that are built up in memory.
  // The default and the minimum number is 2, so that when 1 write buffer
  // is being flushed to storage, new writes can continue to the other
  // write buffer.
  // If max_write_buffer_number > 3, writing will be slowed down to
  // options.delayed_write_rate if we are writing to the last write buffer
  // allowed.
  //
  // Default: 2
  //
  // Dynamically changeable through SetOptions() API
  int max_write_buffer_number = 2;

  // The minimum number of write buffers that will be merged together
  // before writing to storage.  If set to 1, then
  // all write buffers are flushed to L0 as individual files and this increases
  // read amplification because a get request has to check in all of these
  // files. Also, an in-memory merge may result in writing lesser
  // data to storage if there are duplicate records in each of these
  // individual write buffers.
  // If atomic flush is enabled (options.atomic_flush == true), then this
  // option will be sanitized to 1.
  // Default: 1
  int min_write_buffer_number_to_merge = 1;

  // The target number of write history bytes to hold in memory. Write history
  // comprises the latest write buffers (memtables). To reach the target, write
  // buffers that were most recently flushed to SST files may be retained in
  // memory.
  //
  // This controls the target amount of write history that will be available
  // in memory for conflict checking when Transactions are used.
  //
  // This target may be undershot when the CF first opens and has not recovered
  // or received enough writes to reach the target. After reaching the target
  // once, it is guaranteed to never undershoot again. That guarantee is
  // implemented by retaining flushed write buffers in-memory until the oldest
  // one can be trimmed without dropping below the target.
  //
  // Examples with `max_write_buffer_size_to_maintain` set to 32MB:
  //
  // - One mutable memtable of 64MB, one unflushed immutable memtable of 64MB,
  //   and zero flushed immutable memtables. Nothing trimmable exists.
  // - One mutable memtable of 16MB, zero unflushed immutable memtables, and
  //   one flushed immutable memtable of 64MB. Trimming is disallowed because
  //   dropping the earliest (only) flushed immutable memtable would result in
  //   write history of 16MB < 32MB.
  // - One mutable memtable of 24MB, one unflushed immutable memtable of 16MB,
  //   and one flushed immutable memtable of 16MB. The earliest (only) flushed
  //   immutable memtable is trimmed because without it we still have
  //   16MB + 24MB = 40MB > 32MB of write history.
  //
  // When using an OptimisticTransactionDB:
  // If this value is too low, some transactions may fail at commit time due
  // to not being able to determine whether there were any write conflicts.
  //
  // When using a TransactionDB:
  // If Transaction::SetSnapshot is used, TransactionDB will read either
  // in-memory write buffers or SST files to do write-conflict checking.
  // Increasing this value can reduce the number of reads to SST files
  // done for conflict detection.
  //
  // Setting this value to 0 will cause write buffers to be freed immediately
  // after they are flushed. If this value is set to -1,
  // 'max_write_buffer_number * write_buffer_size' will be used.
  //
  // Default:
  // If using a TransactionDB/OptimisticTransactionDB, the default value will
  // be set to the value of 'max_write_buffer_number * write_buffer_size'
  // if it is not explicitly set by the user.  Otherwise, the default is 0.
  int64_t max_write_buffer_size_to_maintain = 0;

  // Allows thread-safe inplace updates.
  //
  // If this is true, there is no way to
  // achieve point-in-time consistency using snapshot or iterator (assuming
  // concurrent updates). Hence iterator and multi-get will return results
  // which are not consistent as of any point-in-time.
  //
  // Backward iteration on memtables will not work either.
  //
  // It is intended to work or be compatible with a limited set of features:
  // (1) Non-snapshot Get()
  //
  // If inplace_callback function is not set,
  //   Put(key, new_value) will update inplace the existing_value iff
  //   * key exists in current memtable
  //   * new sizeof(new_value) <= sizeof(existing_value)
  //   * existing_value for that key is a put i.e. kTypeValue
  // If inplace_callback function is set, check doc for inplace_callback.
  // Default: false.
  bool inplace_update_support = false;

  // Number of locks used for inplace update
  // Default: 10000, if inplace_update_support = true, else 0.
  //
  // Dynamically changeable through SetOptions() API
  size_t inplace_update_num_locks = 10000;

  // [experimental]
  // Used to activate or deactive the Mempurge feature (memtable garbage
  // collection). (deactivated by default). At every flush, the total useful
  // payload (total entries minus garbage entries) is estimated as a ratio
  // [useful payload bytes]/[size of a memtable (in bytes)]. This ratio is then
  // compared to this `threshold` value:
  //     - if ratio<threshold: the flush is replaced by a mempurge operation
  //     - else: a regular flush operation takes place.
  // Threshold values:
  //   0.0: mempurge deactivated (default).
  //   1.0: recommended threshold value.
  //   >1.0 : aggressive mempurge.
  //   0 < threshold < 1.0: mempurge triggered only for very low useful payload
  //   ratios.
  // [experimental]
  double experimental_mempurge_threshold = 0.0;

  // existing_value - pointer to previous value (from both memtable and sst).
  //                  nullptr if key doesn't exist
  // existing_value_size - pointer to size of existing_value).
  //                       nullptr if key doesn't exist
  // delta_value - Delta value to be merged with the existing_value.
  //               Stored in transaction logs.
  // merged_value - Set when delta is applied on the previous value.
  //
  // Applicable only when inplace_update_support is true,
  // this callback function is called at the time of updating the memtable
  // as part of a Put operation, lets say Put(key, delta_value). It allows the
  // 'delta_value' specified as part of the Put operation to be merged with
  // an 'existing_value' of the key in the database.
  //
  // If the merged value is smaller in size that the 'existing_value',
  // then this function can update the 'existing_value' buffer inplace and
  // the corresponding 'existing_value'_size pointer, if it wishes to.
  // The callback should return UpdateStatus::UPDATED_INPLACE.
  // In this case. (In this case, the snapshot-semantics of the rocksdb
  // Iterator is not atomic anymore).
  //
  // If the merged value is larger in size than the 'existing_value' or the
  // application does not wish to modify the 'existing_value' buffer inplace,
  // then the merged value should be returned via *merge_value. It is set by
  // merging the 'existing_value' and the Put 'delta_value'. The callback should
  // return UpdateStatus::UPDATED in this case. This merged value will be added
  // to the memtable.
  //
  // If merging fails or the application does not wish to take any action,
  // then the callback should return UpdateStatus::UPDATE_FAILED.
  //
  // Please remember that the original call from the application is Put(key,
  // delta_value). So the transaction log (if enabled) will still contain (key,
  // delta_value). The 'merged_value' is not stored in the transaction log.
  // Hence the inplace_callback function should be consistent across db reopens.
  //
  // RocksDB callbacks are NOT exception-safe. A callback completing with an
  // exception can lead to undefined behavior in RocksDB, including data loss,
  // unreported corruption, deadlocks, and more.
  //
  // Default: nullptr
  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value) = nullptr;

  // Should really be called `memtable_bloom_size_ratio`. Enables a dynamic
  // Bloom filter in memtable to optimize many queries that must go beyond
  // the memtable. The size in bytes of the filter is
  // write_buffer_size * memtable_prefix_bloom_size_ratio.
  // * If prefix_extractor is set, the filter includes prefixes.
  // * If memtable_whole_key_filtering, the filter includes whole keys.
  // * If both, the filter includes both.
  // * If neither, the feature is disabled.
  //
  // If this value is larger than 0.25, it is sanitized to 0.25.
  //
  // Default: 0 (disabled)
  //
  // Dynamically changeable through SetOptions() API
  double memtable_prefix_bloom_size_ratio = 0.0;

  // Enable whole key bloom filter in memtable. Note this will only take effect
  // if memtable_prefix_bloom_size_ratio is not 0. Enabling whole key filtering
  // can potentially reduce CPU usage for point-look-ups.
  //
  // Default: false (disabled)
  //
  // Dynamically changeable through SetOptions() API
  bool memtable_whole_key_filtering = false;

  // Page size for huge page for the arena used by the memtable. If <=0, it
  // won't allocate from huge page but from malloc.
  // Users are responsible to reserve huge pages for it to be allocated. For
  // example:
  //      sysctl -w vm.nr_hugepages=20
  // See linux doc Documentation/vm/hugetlbpage.txt
  // If there isn't enough free huge page available, it will fall back to
  // malloc.
  //
  // Dynamically changeable through SetOptions() API
  size_t memtable_huge_page_size = 0;

  // If non-nullptr, memtable will use the specified function to extract
  // prefixes for keys, and for each prefix maintain a hint of insert location
  // to reduce CPU usage for inserting keys with the prefix. Keys out of
  // domain of the prefix extractor will be insert without using hints.
  //
  // Currently only the default skiplist based memtable implements the feature.
  // All other memtable implementation will ignore the option. It incurs ~250
  // additional bytes of memory overhead to store a hint for each prefix.
  // Also concurrent writes (when allow_concurrent_memtable_write is true) will
  // ignore the option.
  //
  // The option is best suited for workloads where keys will likely to insert
  // to a location close the last inserted key with the same prefix.
  // One example could be inserting keys of the form (prefix + timestamp),
  // and keys of the same prefix always comes in with time order. Another
  // example would be updating the same key over and over again, in which case
  // the prefix can be the key itself.
  //
  // Default: nullptr (disabled)
  std::shared_ptr<const SliceTransform>
      memtable_insert_with_hint_prefix_extractor = nullptr;

  // Control locality of bloom filter probes to improve CPU cache hit rate.
  // This option now only applies to plaintable prefix bloom. This
  // optimization is turned off when set to 0, and positive number to turn
  // it on.
  // Default: 0
  uint32_t bloom_locality = 0;

  // size of one block in arena memory allocation.
  // If <= 0, a proper value is automatically calculated (usually 1/8 of
  // writer_buffer_size, rounded up to a multiple of 4KB, or 1MB which ever is
  // smaller).
  //
  // There are two additional restriction of the specified size:
  // (1) size should be in the range of [4096, 2 << 30] and
  // (2) be the multiple of the CPU word (which helps with the memory
  // alignment).
  //
  // We'll automatically check and adjust the size number to make sure it
  // conforms to the restrictions.
  //
  // Default: 0
  //
  // Dynamically changeable through SetOptions() API
  size_t arena_block_size = 0;

  // Different levels can have different compression policies. There
  // are cases where most lower levels would like to use quick compression
  // algorithms while the higher levels (which have more data) use
  // compression algorithms that have better compression but could
  // be slower. This array, if non-empty, should have an entry for
  // each level of the database; these override the value specified in
  // the previous field 'compression'.
  //
  // NOTICE if level_compaction_dynamic_level_bytes=true,
  // compression_per_level[0] still determines L0, but other elements
  // of the array are based on base level (the level L0 files are merged
  // to), and may not match the level users see from info log for metadata.
  // If L0 files are merged to level-n, then, for i>0, compression_per_level[i]
  // determines compaction type for level n+i-1.
  // For example, if we have three 5 levels, and we determine to merge L0
  // data to L4 (which means L1..L3 will be empty), then the new files go to
  // L4 uses compression type compression_per_level[1].
  // If now L0 is merged to L2. Data goes to L2 will be compressed
  // according to compression_per_level[1], L3 using compression_per_level[2]
  // and L4 using compression_per_level[3]. Compaction for each level can
  // change when data grows.
  //
  // NOTE: if the vector size is smaller than the level number, the undefined
  // lower level uses the last option in the vector, for example, for 3 level
  // LSM tree the following settings are the same:
  // {kNoCompression, kSnappyCompression}
  // {kNoCompression, kSnappyCompression, kSnappyCompression}
  //
  // Dynamically changeable through SetOptions() API
  std::vector<CompressionType> compression_per_level;

  // Number of levels for this database
  int num_levels = 7;

  // Soft limit on number of level-0 files. We start slowing down writes at this
  // point. A value <0 means that no writing slow down will be triggered by
  // number of files in level-0.
  //
  // Default: 20
  //
  // Dynamically changeable through SetOptions() API
  int level0_slowdown_writes_trigger = 20;

  // Maximum number of level-0 files.  We stop writes at this point.
  //
  // Default: 36
  //
  // Dynamically changeable through SetOptions() API
  int level0_stop_writes_trigger = 36;

  // Target file size for compaction.
  // target_file_size_base is per-file size for level-1.
  // Target file size for level L can be calculated by
  // target_file_size_base * (target_file_size_multiplier ^ (L-1))
  // For example, if target_file_size_base is 2MB and
  // target_file_size_multiplier is 10, then each file on level-1 will
  // be 2MB, and each file on level 2 will be 20MB,
  // and each file on level-3 will be 200MB.
  //
  // Default: 64MB.
  //
  // Dynamically changeable through SetOptions() API
  uint64_t target_file_size_base = 64 * 1048576;

  // By default target_file_size_multiplier is 1, which means
  // by default files in different levels will have similar size.
  //
  // Dynamically changeable through SetOptions() API
  int target_file_size_multiplier = 1;

  // If true, RocksDB will consider the estimated tail size (filter + index +
  // meta blocks) when deciding whether to cut a compaction output file. This
  // helps prevent output files from exceeding the target_file_size_base due to
  // large tail blocks. When disabled, only the data block size is considered,
  // which may result in SST files exceeding the target_file_size_base.
  //
  // Default: false
  //
  // Dynamically changeable through SetOptions() API
  bool target_file_size_is_upper_bound = false;

  // If true, RocksDB will pick target size of each level dynamically.
  // We will pick a base level b >= 1. L0 will be directly merged into level b,
  // instead of always into level 1. Level 1 to b-1 need to be empty.
  // We try to pick b and its target size so that
  // 1. target size is in the range of
  //   (max_bytes_for_level_base / max_bytes_for_level_multiplier,
  //    max_bytes_for_level_base]
  // 2. target size of the last level (level num_levels-1) equals to the max
  //    size of a level in the LSM (typically the last level).
  // At the same time max_bytes_for_level_multiplier is still satisfied.
  // Note that max_bytes_for_level_multiplier_additional is ignored with this
  // flag on.
  //
  // With this option on, from an empty DB, we make last level the base level,
  // which means merging L0 data into the last level, until it exceeds
  // max_bytes_for_level_base. And then we make the second last level to be
  // base level, to start to merge L0 data to second last level, with its
  // target size to be 1/max_bytes_for_level_multiplier of the last level's
  // extra size. After the data accumulates more so that we need to move the
  // base level to the third last one, and so on.
  //
  // For example, assume max_bytes_for_level_multiplier=10, num_levels=6,
  // and max_bytes_for_level_base=10MB.
  // Target sizes of level 1 to 5 starts with:
  // [- - - - 10MB]
  // with base level is level 5. Target sizes of level 1 to 4 are not applicable
  // because they will not be used.
  // Until the size of Level 5 grows to more than 10MB, say 11MB, we make
  // base target to level 4 and now the targets looks like:
  // [- - - 1.1MB 11MB]
  // While data are accumulated, size targets are tuned based on actual data
  // of level 5. When level 5 has 50MB of data, the target is like:
  // [- - - 5MB 50MB]
  // Until level 5's actual size is more than 100MB, say 101MB. Now if we keep
  // level 4 to be the base level, its target size needs to be 10.1MB, which
  // doesn't satisfy the target size range. So now we make level 3 the target
  // size and the target sizes of the levels look like:
  // [- - 1.01MB 10.1MB 101MB]
  // In the same way, while level 5 further grows, all levels' targets grow,
  // like
  // [- - 5MB 50MB 500MB]
  // Until level 5 exceeds 1000MB and becomes 1001MB, we make level 2 the
  // base level and make levels' target sizes like this:
  // [- 1.001MB 10.01MB 100.1MB 1001MB]
  // and go on...
  //
  // By doing it, we give max_bytes_for_level_multiplier a priority against
  // max_bytes_for_level_base, for a more predictable LSM tree shape. It is
  // useful to limit worse case space amplification.
  // If `cf_allow_ingest_behind=true` or `preclude_last_level_data_seconds > 0`,
  // then the last level is reserved, and we will start filling LSM from the
  // second last level.
  //
  // With this option on, compaction is more adaptive to write traffic:
  // Compaction priority will take into account estimated bytes to be compacted
  // down to a level and favors compacting lower levels when there is a write
  // traffic spike (and hence more compaction debt). Refer to
  // https://github.com/meta/rocksdb/wiki/Leveled-Compactio#option-level_compaction_dynamic_level_bytes-and-levels-target-size
  // for more detailed description. See more implementation detail in:
  // VersionStorageInfo::ComputeCompactionScore().
  //
  // With this option on, unneeded levels will be drained automatically:
  // Note that there may be excessive levels (where target level size is 0 when
  // computed based on this feature) in the LSM. This can happen after a user
  // migrates to turn this feature on or deletes a lot of data. This is
  // especially likely when a user migrates from leveled compaction with a
  // smaller multiplier or from universal compaction. RocksDB will gradually
  // drain these unnecessary levels by compacting files down the LSM. Smaller
  // number of levels should help to reduce read amplification.
  //
  // Migration to turn on this option:
  // - Before RocksDB v8.2, users are expected to do a full manual compaction
  //   and then restart DB to turn on this option.
  // - Since RocksDB v8.2, users can just restart DB with this option on, as
  //   long as num_levels is no smaller than number of non-empty levels in the
  //   LSM. Migration will be done automatically by RocksDB. See more in
  //   https://github.com/meta/rocksdb/wiki/Leveled-Compaction#migrating-from-level_compaction_dynamic_level_bytesfalse-to-level_compaction_dynamic_level_bytestrue
  //
  // Default: true
  bool level_compaction_dynamic_level_bytes = true;

  // Default: 10.
  //
  // Dynamically changeable through SetOptions() API
  double max_bytes_for_level_multiplier = 10;

  // Different max-size multipliers for different levels.
  // These are multiplied by max_bytes_for_level_multiplier to arrive
  // at the max-size of each level.
  // This option only applies to leveled compaction with
  // `level_compaction_dynamic_level_bytes = false`.
  //
  // Default: 1
  //
  // Dynamically changeable through SetOptions() API
  std::vector<int> max_bytes_for_level_multiplier_additional =
      std::vector<int>(static_cast<size_t>(num_levels), 1);

  // We try to limit number of bytes in one compaction to be lower than this
  // threshold. But it's not guaranteed.
  // Value 0 will be sanitized.
  //
  // Default: target_file_size_base * 25
  //
  // For FIFO compaction with use_kv_ratio_compaction=true:
  // When set to 0 (and compaction_style is FIFO), the value is NOT sanitized
  // to the default. Instead, the target compacted file size is automatically
  // calculated from the data capacity (max_data_files_size) and observed
  // SST/blob ratio. When explicitly set to a non-zero value, it overrides
  // the auto-calculated target and is used directly as the max compaction
  // input size. Note: for FIFO, this controls the output file size target,
  // not a general compaction byte limit as in level/universal compaction.
  //
  // Dynamically changeable through SetOptions() API
  uint64_t max_compaction_bytes = 0;

  // All writes will be slowed down to at least delayed_write_rate if estimated
  // bytes needed to be compaction exceed this threshold.
  //
  // Default: 64GB
  //
  // Dynamically changeable through SetOptions() API
  uint64_t soft_pending_compaction_bytes_limit = 64 * 1073741824ull;

  // All writes are stopped if estimated bytes needed to be compaction exceed
  // this threshold.
  //
  // Default: 256GB
  //
  // Dynamically changeable through SetOptions() API
  uint64_t hard_pending_compaction_bytes_limit = 256 * 1073741824ull;

  // The compaction style. Default: kCompactionStyleLevel
  CompactionStyle compaction_style = kCompactionStyleLevel;

  // If level compaction_style = kCompactionStyleLevel, for each level,
  // which files are prioritized to be picked to compact.
  // Default: kMinOverlappingRatio
  CompactionPri compaction_pri = kMinOverlappingRatio;

  // The options needed to support Universal Style compactions
  //
  // Dynamically changeable through SetOptions() API
  // Dynamic change example:
  // SetOptions("compaction_options_universal", "{size_ratio=2;}")
  CompactionOptionsUniversal compaction_options_universal;

  // The options for FIFO compaction style
  //
  // Dynamically changeable through SetOptions() API
  // Dynamic change example:
  // SetOptions("compaction_options_fifo", "{max_table_files_size=100;}")
  CompactionOptionsFIFO compaction_options_fifo;

  // An iteration->Next() sequentially skips over keys with the same
  // user-key unless this option is set. This number specifies the number
  // of keys (with the same userkey) that will be sequentially
  // skipped before a reseek is issued.
  //
  // Default: 8
  //
  // Dynamically changeable through SetOptions() API
  uint64_t max_sequential_skip_in_iterations = 8;

  // This is a factory that provides MemTableRep objects.
  // Default: a factory that provides a skip-list-based implementation of
  // MemTableRep.
  std::shared_ptr<MemTableRepFactory> memtable_factory =
      std::shared_ptr<SkipListFactory>(new SkipListFactory);

  // Block-based table related options are moved to BlockBasedTableOptions.
  // Related options that were originally here but now moved include:
  //   no_block_cache
  //   block_cache
  //   block_cache_compressed (removed)
  //   block_size
  //   block_size_deviation
  //   block_restart_interval
  //   filter_policy
  //   whole_key_filtering
  // If you'd like to customize some of these options, you will need to
  // use NewBlockBasedTableFactory() to construct a new table factory.

  // This option allows user to collect their own interested statistics of
  // the tables.
  // Default: empty vector -- no user-defined statistics collection will be
  // performed.
  using TablePropertiesCollectorFactories =
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>;
  TablePropertiesCollectorFactories table_properties_collector_factories;

  // Maximum number of successive merge operations on a key in the memtable.
  // It may be violated when filesystem reads would be needed to stay under the
  // limit, unless `strict_max_successive_merges` is explicitly set.
  //
  // When a merge operation is added to the memtable and the maximum number of
  // successive merges is reached, RocksDB will attempt to read the value. Upon
  // success, the value will be inserted into the memtable instead of the merge
  // operation.
  //
  // Default: 0 (disabled)
  //
  // Dynamically changeable through SetOptions() API
  size_t max_successive_merges = 0;

  // Whether to allow filesystem reads to stay under the `max_successive_merges`
  // limit. When true, this can lead to merge writes blocking the write path
  // waiting on filesystem reads.
  //
  // This option is temporary in case the recent change to disallow filesystem
  // reads during merge writes has a problem and users need to undo it quickly.
  //
  // Default: false
  bool strict_max_successive_merges = false;

  // This flag specifies that the implementation should optimize the filters
  // mainly for cases where keys are found rather than also optimize for keys
  // missed. This would be used in cases where the application knows that
  // there are very few misses or the performance in the case of misses is not
  // important.
  //
  // For now, this flag allows us to not store filters for the last level i.e
  // the largest level which contains data of the LSM store. For keys which
  // are hits, the filters in this level are not useful because we will search
  // for the data anyway. NOTE: the filters in other levels are still useful
  // even for key hit because they tell us whether to look in that level or go
  // to the higher level.
  //
  // Default: false
  bool optimize_filters_for_hits = false;

  // After writing every SST file, reopen it and read all the keys.
  // Checks the hash of all of the keys and values written versus the
  // keys in the file and signals a corruption if they do not match
  //
  // Default: false
  //
  // Dynamically changeable through SetOptions() API
  bool paranoid_file_checks = false;

  // Bitmask enum for output verification option.
  //
  // Default: 0 (kVerifyNone)
  //
  // Dynamically changeable (as a uint32_t) through SetOptions() API.
  VerifyOutputFlags verify_output_flags = VerifyOutputFlags::kVerifyNone;

  // In debug mode, RocksDB runs consistency checks on the LSM every time the
  // LSM changes (Flush, Compaction, AddFile). When this option is true, these
  // checks are also enabled in release mode. These checks were historically
  // disabled in release mode, but are now enabled by default for proactive
  // corruption detection. The CPU overhead is negligible for normal mixed
  // operations but can slow down saturated writing. See
  // Options::DisableExtraChecks().
  // Default: true
  bool force_consistency_checks = true;

  // Measure IO stats in compactions and flushes, if true.
  //
  // Default: false
  //
  // Dynamically changeable through SetOptions() API
  bool report_bg_io_stats = false;

  // Setting this option to true disallows ordinary writes to the column family
  // and it can only be populated through import and ingestion. It is intended
  // to protect "ingestion only" column families. This option is not currently
  // supported on the default column family because of error handling challenges
  // analogous to https://github.com/meta/rocksdb/issues/13429
  //
  // This option is not mutable with SetOptions(). It can be changed between
  // DB::Open() calls, but open will fail if recovering WAL writes to a CF with
  // this option set.
  bool disallow_memtable_writes = false;

  // This option has different meanings for different compaction styles:
  //
  // Leveled: Non-bottom-level files with all keys older than TTL will go
  //    through the compaction process. This usually happens in a cascading
  //    way so that those entries will be compacted to bottommost level/file.
  //    The feature is used to remove stale entries that have been deleted or
  //    updated from the file system.
  //
  // FIFO: Files with all keys older than TTL will be deleted. TTL is only
  //    supported if option max_open_files is set to -1.
  //
  // Universal: users should only set the option `periodic_compaction_seconds`
  //    below instead. For backward compatibility, this option has the same
  //    meaning as `periodic_compaction_seconds`. See more in comments for
  //    `periodic_compaction_seconds` on the interaction between these two
  //    options.
  //
  // This option only supports block based table format for any compaction
  // style.
  //
  // unit: seconds. Ex: 1 day = 1 * 24 * 60 * 60
  // 0 means disabling.
  // UINT64_MAX - 1 (0xfffffffffffffffe) is special flag to allow RocksDB to
  // pick default.
  //
  // Default: 30 days if using block based table. 0 (disable) otherwise.
  //
  // Dynamically changeable through SetOptions() API
  // Note that dynamically changing this option only works for leveled and FIFO
  // compaction. For universal compaction, dynamically changing this option has
  // no effect, users should dynamically change `periodic_compaction_seconds`
  // instead.
  uint64_t ttl = 0xfffffffffffffffe;

  // This option has different meanings for different compaction styles:
  //
  // Leveled: files older than `periodic_compaction_seconds` will be picked up
  //    for compaction and will be re-written to the same level as they were
  //    before if level_compaction_dynamic_level_bytes is disabled. Otherwise,
  //    it will rewrite files to the next level except for the last level files
  //    to the same level.
  //
  // FIFO: not supported. Setting this option has no effect for FIFO compaction.
  //
  // Universal: when there are files older than `periodic_compaction_seconds`,
  //    rocksdb will try to do as large a compaction as possible including the
  //    last level. Such compaction is only skipped if only last level is to
  //    be compacted and no file in last level is older than
  //    `periodic_compaction_seconds`. See more in
  //    UniversalCompactionBuilder::PickPeriodicCompaction().
  //    For backward compatibility, the effective value of this option takes
  //    into account the value of option `ttl`. The logic is as follows:
  //    - both options are set to 30 days if they have the default value.
  //    - if both options are zero, zero is picked. Otherwise, we take the min
  //    value among non-zero options values (i.e. takes the stricter limit).
  //
  // One main use of the feature is to make sure a file goes through compaction
  // filters periodically. Users can also use the feature to clear up SST
  // files using old format.
  //
  // A file's age is computed by looking at file_creation_time or
  // oldest_ancestor_time table properties in order, if they have valid
  // non-zero values; if not, the age is based on the file's last modified time
  // (given by the underlying Env).
  //
  // This option only supports block based table format for any compaction
  // style.
  //
  // unit: seconds. Ex: 7 days = 7 * 24 * 60 * 60
  //
  // Values:
  // 0: Turn off Periodic compactions.
  // UINT64_MAX - 1 (0xfffffffffffffffe) is special flag to allow RocksDB to
  // pick default.
  //
  // Default: 30 days if using block based table format + compaction filter +
  //  leveled compaction or block based table format + universal compaction.
  //  0 (disabled) otherwise.
  //
  // Dynamically changeable through SetOptions() API
  uint64_t periodic_compaction_seconds = 0xfffffffffffffffe;

  // When set to a positive value, enables read-triggered compaction. An SST
  // file is marked for compaction when its estimated read frequency
  // (estimated_reads / file_size) exceeds this threshold. This helps reduce
  // read amplification for hot keys by compacting frequently-read files.
  //
  // Only "collapsible" reads are counted -- lookups that return NotFound
  // (bloom filter false positive), Delete/SingleDeletion (tombstone), or
  // Merge (partial result). These are reads where the file contributed no
  // final value and compaction would eliminate the wasted work.
  //
  // Choosing a value: the threshold balances read IO saved against the
  // write amplification (WA) of an extra compaction. This assumes the
  // block-based table format is being used,
  //
  // Break-even derivation (no block cache):
  //   Let r = estimated_reads / file_size  (the threshold)
  //       S = file_size
  //       B = block_size             (typically 4 KB)
  //       F = level fanout           (typically ~10)
  //
  //   Each collapsible read wastes one data-block read = B bytes of IO.
  //   Total wasted read IO for a file = r * S * B.
  //
  //   Compaction cost: one level-L file overlaps ~F files in level L+1,
  //   so we read (1 + F) files and write (1 + F) files.
  //   Total compaction IO = 2 * (1 + F) * S.
  //
  //   Break-even when wasted read IO equals compaction IO:
  //     r * S * B = 2 * (1 + F) * S
  //     r = 2 * (1 + F) / B
  //
  //   With F = 10, B = 4096:  r = 22 / 4096 ~= 0.005.
  //
  // With a block-cache hit rate h (0 <= h < 1), each collapsible read
  // only costs (1 - h) * B bytes of actual disk IO, so:
  //     r = 2 * (1 + F) / ((1 - h) * B)
  //
  //   h = 0   -> r ~= 0.005
  //   h = 0.5 -> r ~= 0.01
  //   h = 0.9 -> r ~= 0.05
  //
  // A recommended starting point is 0.01, which avoids triggering
  // compactions that cost more IO than they save for most cache-friendly
  // workloads, while still being responsive enough to compact files with
  // significant wasted reads.
  //
  // For this feature to take effect on a "quiet" DB (no writes), the DB-level
  // option `max_compaction_trigger_wakeup_seconds` must also be set to a
  // non-zero value so the periodic background job can re-evaluate files.
  //
  // Valid range: >= 0.0 (must be finite). Use 0.0 to disable.
  //
  // Dynamically changeable through SetOptions() API
  double read_triggered_compaction_threshold = 0.0;

  // If this option is set then 1 in N blocks are compressed
  // using a fast (lz4) and slow (zstd) compression algorithm.
  // The compressibility is reported as stats and the stored
  // data is left uncompressed (unless compression is also requested).
  uint64_t sample_for_compression = 0;

  // EXPERIMENTAL
  // If this option is set, when creating the last level files, pass this
  // temperature to FileSystem used. Should be no-op for default FileSystem
  // and users need to plug in their own FileSystem to take advantage of it.
  // Currently only compatible with universal compaction.
  //
  // Dynamically changeable through the SetOptions() API
  Temperature last_level_temperature = Temperature::kUnknown;

  // EXPERIMENTAL
  // When no other option such as last_level_temperature determines the
  // temperature of a new SST file, it will be written with this temperature,
  // which can be set differently for each column family.
  //
  // Dynamically changeable through the SetOptions() API
  Temperature default_write_temperature = Temperature::kUnknown;

  // EXPERIMENTAL
  // When this field is set, all SST files without an explicitly set temperature
  // will be treated as if they have this temperature for file reading
  // accounting purpose, such as io statistics, io perf context.
  //
  // Not dynamically changeable; change requires DB restart.
  Temperature default_temperature = Temperature::kUnknown;

  // EXPERIMENTAL
  // The feature is still in development and is incomplete.
  // If this option is set, when data insert time is within this time range, it
  // will be precluded from the last level.
  // 0 means no key will be precluded from the last level.
  //
  // Note: when enabled, universal size amplification (controlled by option
  //  `compaction_options_universal.max_size_amplification_percent`) calculation
  //  will exclude the last level. As the feature is designed for tiered storage
  //  and a typical setting is the last level is cold tier which is likely not
  //  size constrained, the size amp is going to be only for non-last levels.
  //
  // Default: 0 (disable the feature)
  //
  // Dynamically changeable through the SetOptions() API
  uint64_t preclude_last_level_data_seconds = 0;

  // EXPERIMENTAL
  // If this option is set, it will preserve the internal time information about
  // the data until it's older than the specified time here.
  // Internally the time information is a map between sequence number and time,
  // which is the same as `preclude_last_level_data_seconds`. But it won't
  // preclude the data from the last level and the data in the last level won't
  // have the sequence number zeroed out.
  // Internally, rocksdb would sample the sequence number to time pair and store
  // that in SST property "rocksdb.seqno.time.map". The information is currently
  // only used for tiered storage compaction (option
  // `preclude_last_level_data_seconds`).
  //
  // Note: if both `preclude_last_level_data_seconds` and this option is set, it
  //  will preserve the max time of the 2 options and compaction still preclude
  //  the data based on `preclude_last_level_data_seconds`.
  //  The higher the preserve_time is, the less the sampling frequency will be (
  //  which means less accuracy of the time estimation).
  //
  // Default: 0 (disable the feature)
  //
  // Dynamically changeable through the SetOptions() API
  uint64_t preserve_internal_time_seconds = 0;

  // When set, large values (blobs) are written to separate blob files, and
  // only pointers to them are stored in SST files. This can reduce write
  // amplification for large-value use cases at the cost of introducing a level
  // of indirection for reads. See also the options min_blob_size,
  // blob_file_size, blob_compression_type, enable_blob_garbage_collection,
  // blob_garbage_collection_age_cutoff,
  // blob_garbage_collection_force_threshold, and blob_compaction_readahead_size
  // below.
  //
  // Default: false
  //
  // Dynamically changeable through the SetOptions() API
  bool enable_blob_files = false;

  // The size of the smallest value to be stored separately in a blob file.
  // Values which have an uncompressed size smaller than this threshold are
  // stored alongside the keys in SST files in the usual fashion. A value of
  // zero for this option means that all values are stored in blob files. Note
  // that enable_blob_files has to be set in order for this option to have any
  // effect.
  //
  // Default: 0
  //
  // Dynamically changeable through the SetOptions() API
  uint64_t min_blob_size = 0;

  // The size limit for blob files. When writing blob files, a new file is
  // opened once this limit is reached. Note that enable_blob_files has to be
  // set in order for this option to have any effect.
  //
  // Default: 256 MB
  //
  // Dynamically changeable through the SetOptions() API
  uint64_t blob_file_size = 1ULL << 28;

  // Max size of the in-memory write buffer for blob files only, overriding
  // DBOptions::writable_file_max_buffer_size for blob writers.
  // 0 => inherit DBOptions::writable_file_max_buffer_size.
  //
  // Default: 0
  //
  // Dynamically changeable through the SetOptions() API
  uint64_t blob_file_writable_file_max_buffer_size = 0;

  // The compression algorithm to use for large values stored in blob files.
  // Note that enable_blob_files has to be set in order for this option to have
  // any effect.
  //
  // Default: no compression
  //
  // Dynamically changeable through the SetOptions() API
  CompressionType blob_compression_type = kNoCompression;

  // The compression options for blob files. This allows fine-tuning of
  // compression parameters (e.g. level, window_bits) independently of SST file
  // compression. For example, setting level=1 with ZSTD uses the "fast"
  // strategy (single hash table) vs the default level=3 "doubleFast" strategy.
  //
  // Default: default CompressionOptions (level = kDefaultCompressionLevel)
  //
  // Dynamically changeable through the SetOptions() API
  CompressionOptions blob_compression_opts;

  // Enables garbage collection of blobs. Blob GC is performed as part of
  // compaction. Valid blobs residing in blob files older than a cutoff get
  // relocated to new files as they are encountered during compaction, which
  // makes it possible to clean up blob files once they contain nothing but
  // obsolete/garbage blobs. See also blob_garbage_collection_age_cutoff and
  // blob_garbage_collection_force_threshold below.
  //
  // Default: false
  //
  // Dynamically changeable through the SetOptions() API
  bool enable_blob_garbage_collection = false;

  // The cutoff in terms of blob file age for garbage collection. Blobs in
  // the oldest N blob files will be relocated when encountered during
  // compaction, where N = garbage_collection_cutoff * number_of_blob_files.
  // Note that enable_blob_garbage_collection has to be set in order for this
  // option to have any effect.
  //
  // Default: 0.25
  //
  // Dynamically changeable through the SetOptions() API
  double blob_garbage_collection_age_cutoff = 0.25;

  // If the ratio of garbage in the blob files currently eligible for garbage
  // collection exceeds this threshold, targeted compactions are scheduled in
  // order to force garbage collecting the oldest blob files. This option is
  // currently only supported with leveled compactions. Note that
  // enable_blob_garbage_collection has to be set in order for this option to
  // have any effect.
  //
  // Default: 1.0
  //
  // Dynamically changeable through the SetOptions() API
  double blob_garbage_collection_force_threshold = 1.0;

  // Compaction readahead for blob files.
  //
  // Default: 0
  //
  // Dynamically changeable through the SetOptions() API
  uint64_t blob_compaction_readahead_size = 0;

  // Enable blob files starting from a certain LSM tree level.
  //
  // For certain use cases that have a mix of short-lived and long-lived values,
  // it might make sense to support extracting large values only during
  // compactions whose output level is greater than or equal to a specified LSM
  // tree level (e.g. compactions into L1/L2/... or above). This could reduce
  // the space amplification caused by large values that are turned into garbage
  // shortly after being written at the price of some write amplification
  // incurred by long-lived values whose extraction to blob files is delayed.
  //
  // Default: 0
  //
  // Dynamically changeable through the SetOptions() API
  int blob_file_starting_level = 0;

  // The Cache object to use for blobs. Using a dedicated object for blobs and
  // using the same object for the block and blob caches are both supported. In
  // the latter case, note that blobs are less valuable from a caching
  // perspective than SST blocks, and some cache implementations have
  // configuration options that can be used to prioritize items accordingly (see
  // Cache::Priority and LRUCacheOptions::{high,low}_pri_pool_ratio).
  //
  // Default: nullptr (disabled)
  std::shared_ptr<Cache> blob_cache = nullptr;

  // Enable/disable prepopulating the blob cache. When set to kFlushOnly, BlobDB
  // will insert newly written blobs into the blob cache during flush. This can
  // improve performance when reading back these blobs would otherwise be
  // expensive (e.g. when using direct I/O or remote storage), or when the
  // workload has a high temporal locality.
  //
  // Default: disabled
  //
  // Dynamically changeable through the SetOptions() API
  PrepopulateBlobCache prepopulate_blob_cache = PrepopulateBlobCache::kDisable;

  // When enabled, values >= min_blob_size are written directly to blob files
  // during the write path and replaced in WAL and memtable with BlobIndex
  // references.
  //
  // Requires enable_blob_files = true.
  // Experimental reduced-scope v1 restrictions. These limitations keep the v1
  // implementation intentionally small; follow-up PRs are expected to improve
  // feature compatibility over time:
  //  - only supports the ordered single-memtable-writer path; unordered,
  //    pipelined, two_write_queues, and allow_concurrent_memtable_write are
  //    not supported.
  //  - crash recovery only supports blob files that were already made
  //    manifest-visible by flush/SST creation; WAL replay of active
  //    direct-write blob files is not currently supported.
  //  - checkpoint/backup/live-files enumeration must flush pending
  //    direct-write state first; APIs that intentionally skip the flush, or
  //    run while WAL is locked, can return NotSupported.
  //  - not compatible with MemPurge or user-defined timestamps.
  //  - DB::IngestWriteBatchWithIndex() is not supported while any live column
  //    family enables this option.
  //  - read-only and secondary opens can read flushed/manifest-visible blob
  //    files, but do not resolve still-active direct-write blob files.
  //
  // Default: false
  //
  // Not dynamically changeable through the SetOptions() API.
  bool enable_blob_direct_write = false;

  // Number of direct-write blob partitions for this column family.
  // Requires enable_blob_direct_write = true.
  //
  // If blob_direct_write_partition_strategy is null, partition selection uses
  // the default round-robin strategy.
  //
  // Default: 1
  //
  // Not dynamically changeable through the SetOptions() API.
  uint32_t blob_direct_write_partitions = 1;

  // Custom partition strategy for blob direct writes.
  // If null, uses the default round-robin strategy.
  // Put()/Merge-style value separation uses SelectPartition(..., Slice value),
  // while PutEntity() wide-column separation calls
  // SelectPartition(..., WideColumns columns) once per entity and reuses the
  // selected partition for all blob-backed columns in that entity.
  // Requires enable_blob_direct_write = true.
  //
  // RocksDB treats this as an application-supplied callback rather than a
  // serialized OPTIONS object. Applications must provide it again on every DB
  // open when they rely on custom partitioning behavior.
  //
  // Default: nullptr
  //
  // Not dynamically changeable through the SetOptions() API.
  std::shared_ptr<BlobFilePartitionStrategy>
      blob_direct_write_partition_strategy = nullptr;

  // Enable memtable per key-value checksum protection.
  //
  // Each entry in memtable will be suffixed by a per key-value checksum.
  // This options determines the size of such checksums.
  //
  // It is suggested to turn on write batch per key-value
  // checksum protection together with this option, so that the checksum
  // computation is done outside of writer threads (memtable kv checksum can be
  // computed from write batch checksum) See
  // WriteOptions::protection_bytes_per_key for more detail.
  //
  // Default: 0 (no protection)
  // Supported values: 0, 1, 2, 4, 8.
  // Dynamically changeable through the SetOptions() API.
  uint32_t memtable_protection_bytes_per_key = 0;

  // UNDER CONSTRUCTION -- DO NOT USE
  // When the user-defined timestamp feature is enabled, this flag controls
  // whether the user-defined timestamps will be persisted.
  //
  // When it's false, the user-defined timestamps will be removed from the user
  // keys when data is flushed from memtables to SST files. Other places that
  // user keys can be persisted like file boundaries in file metadata and blob
  // files go through a similar process. There are two major motivations
  // for this flag:
  // 1) backward compatibility: if the user later decides to
  // disable the user-defined timestamp feature for the column family, these SST
  // files can be handled by a user comparator that is not aware of user-defined
  // timestamps.
  // 2) enable user-defined timestamp feature for an existing column family
  // while set this flag to be `false`: user keys in the newly generated SST
  // files are of the same format as the existing SST files.
  //
  // Currently only user comparator that formats user-defined timesamps as
  // uint64_t via using one of the RocksDB provided comparator
  // `ComparatorWithU64TsImpl` are supported.
  //
  // When setting this flag to `false`, users should also call
  // `DB::IncreaseFullHistoryTsLow` to set a cutoff timestamp for flush. RocksDB
  // refrains from flushing a memtable with data still above
  // the cutoff timestamp with best effort. One limitation of this best effort
  // is that when `max_write_buffer_number` is equal to or smaller than 2,
  // RocksDB will not attempt to retain user-defined timestamps, all flush jobs
  // continue normally.
  //
  // Users can do user-defined
  // multi-versioned read above the cutoff timestamp. When users try to read
  // below the cutoff timestamp, an error will be returned.
  //
  // Note that if WAL is enabled, unlike SST files, user-defined timestamps are
  // persisted to WAL even if this flag is set to `false`. The benefit of this
  // is that user-defined timestamps can be recovered with the caveat that users
  // should flush all memtables so there is no active WAL files before doing a
  // downgrade. In order to use WAL to recover user-defined timestamps, users of
  // this feature would want to set both `avoid_flush_during_shutdown` and
  // `avoid_flush_during_recovery` to be true.
  //
  // Note that setting this flag to false is not supported in combination with
  // atomic flush, or concurrent memtable write enabled by
  // `allow_concurrent_memtable_write`.
  //
  // Default: true (user-defined timestamps are persisted)
  // Not dynamically changeable, change it requires db restart and
  // only compatible changes are allowed.
  bool persist_user_defined_timestamps = true;

  // Enable/disable per key-value checksum protection for in memory blocks.
  //
  // Checksum is constructed when a block is loaded into memory and verification
  // is done for each key read from the block. This is useful for detecting
  // in-memory data corruption. Note that this feature has a non-trivial
  // negative impact on read performance. Different values of the
  // option have similar performance impact, but different memory cost and
  // corruption detection probability (e.g. 1 byte gives 255/256 chance for
  // detecting a corruption).
  //
  // Default: 0 (no protection)
  // Supported values: 0, 1, 2, 4, 8.
  // Dynamically changeable through the SetOptions() API.
  uint8_t block_protection_bytes_per_key = 0;

  // For leveled compaction, RocksDB may compact a file at the bottommost level
  // if it can compact away data that were protected by some snapshot.
  // The compaction reason in LOG for this kind of compactions is
  // "BottommostFiles". Usually such compaction can happen as soon as a
  // relevant snapshot is released. This option allows user to delay
  // such compactions. A file is qualified for "BottommostFiles" compaction
  // if it is at least "bottommost_file_compaction_delay" seconds old.
  //
  // Default: 0 (no delay)
  // Dynamically changeable through the SetOptions() API.
  uint32_t bottommost_file_compaction_delay = 0;

  // Enables additional integrity checks during reads/scans.
  // Specifically, for skiplist-based memtables, key ordering validation could
  // be enabled optionally. This is helpful to detect corrupted memtable keys
  // during reads. Enabling this feature incurs a performance overhead due to
  // additional comparison during memtable lookup.
  bool paranoid_memory_checks = false;

  // Enables additional integrity checks during seek.
  // Specifically, for skiplist-based memtables, key checksum validation could
  // be enabled during seek optionally. This is helpful to detect corrupted
  // memtable keys during reads. Enabling this feature incurs a performance
  // overhead due to additional key checksum validation during memtable seek
  // operation.
  // This option depends on memtable_protection_bytes_per_key to be non zero.
  // If memtable_protection_bytes_per_key is zero, no validation is performed.
  bool memtable_verify_per_key_checksum_on_seek = false;

  // When an iterator scans this number of invisible entries (tombstones or
  // hidden puts) from the active memtable during a single iterator operation,
  // we will attempt to flush the memtable. Currently only forward scans are
  // supported (SeekToFirst(), Seek() and Next()).
  // This option helps to reduce the overhead of scanning through a
  // large number of entries in memtable.
  // Users should consider enable deletion-triggered-compaction (see
  // CompactOnDeletionCollectorFactory) together with this option to compact
  // away tombstones after the memtable is flushed.
  //
  // Note that this option has no effect on tailing iterators yet.
  //
  // Default: 0 (disabled)
  // Dynamically changeable through the SetOptions() API.
  uint32_t memtable_op_scan_flush_trigger = 0;

  // Similar to `memtable_op_scan_flush_trigger`, but this option applies to
  // Next() calls between Seeks or until iterator destruction. If the average
  // of the number of invisible entries scanned from the active memtable, the
  // memtable will be marked for flush.
  // Note that to avoid the case where the window between Seeks is too small,
  // the option only takes effect if the total number of hidden entries scanned
  // within a window is at least `memtable_op_scan_flush_trigger`. So this
  // option is only effective when `memtable_op_scan_flush_trigger` is set.
  //
  // This option should be set to a lower value than
  // `memtable_op_scan_flush_trigger`. It covers the case where an iterator
  // scans through an expensive key range with many invisible entries from the
  // active memtable, but the number of invisible entries per operation does not
  // exceed `memtable_op_scan_flush_trigger`.
  //
  // Default: 0 (disabled)
  // Dynamically changeable through the SetOptions() API.
  uint32_t memtable_avg_op_scan_flush_trigger = 0;

  // EXPERIMENTAL
  //
  // During forward or reverse iteration, when this many or more strictly
  // contiguous point tombstones (kTypeDeletion, kTypeDeletionWithTimestamp,
  // kTypeSingleDeletion) are encountered with no live keys between them,
  // a range tombstone [first_tombstone_key, next_live_key) is inserted into
  // the current mutable memtable (only if memtable is not empty). This is a
  // logically redundant entry that does not change any data, but optimizes
  // future iterators by potentially skipping a large number of tombstone scans.
  //
  // This optimization is best-effort and is currently disabled for iterator
  // configurations that may not expose all interior live keys, including:
  // * user-defined timestamp reads without full visibility (for example,
  //   ReadOptions::iter_start_ts or a non-max ReadOptions::timestamp)
  // * prefix extractor reads that are neither total-order
  //   (ReadOptions::total_order_seek / ReadOptions::auto_prefix_mode) nor
  //   bounded by ReadOptions::prefix_same_as_start
  // * reads with ReadOptions::read_tier other than kReadAllTier
  //
  // Even if the above restrictions are met, there are still scenarios where a
  // converted range tombstone may be discarded:
  //   * The snapshot's active mutable memtable has already become immutable.
  //   * The iterator's snapshot seq is below the active memtable's earliest
  //     sequence number.
  //   * A range tombstone covering [first_tombstone_key, next_live_key) is
  //     already present in the memtable.
  //   * A WritePrepared/WriteUnprepared transaction read callback is in use
  //     and the snapshot seq is at or above its min uncommitted seq.
  //   * An IngestExternalFile call is currently in flight on this column
  //     family OR the inserted range tombstone seqno would be lower than the
  //     ingested file seqno OR the iterator's Version predates a file
  //     ingested while the active memtable was current.
  //
  // Read-write iterators using ReadOptions::table_filter are rejected while
  // this option is enabled, see more details in ReadOptions::table_filter
  // comments.
  //
  // Set to 0 to disable.
  //
  // Dynamically changeable through SetOptions() API
  uint32_t min_tombstones_for_range_conversion = 0;

  // If either DBOptions::allow_ingest_behind or this option is set to true,
  // this column family will prepare for ingesting files to the last level
  // (IngestExternalFiles() with ingest_behind=true). Users should set only
  // this option since DBOptions::allow_ingest_behind is deprecated.
  //
  // Specifically, preparing a column family for ingesting files to the last
  // level has the following effects:
  // 1) Disables some internal optimizations around SST file compression.
  // 2) Reserves the last level for ingested files only.
  // 3) Compaction will not include any file from the last level.
  // 4) Compaction will preserve necessary tombstones that can apply on
  // top of ingested files.
  //
  // Note that only Universal Compaction supports cf_allow_ingest_behind.
  // `num_levels` should be >= 3 if this option is turned on.
  //
  // Note that this option needs to be set to true before any write to the CF.
  // It's recommended to set the option to true since CF creation. Otherwise,
  // ingestion with ingest_behind = true might fail. Once file ingestions are
  // done, the option should be flipped to false. Flipping this option to false
  // allows the CF to disable the behavior changes detailed above and resume
  // more efficient operation.
  //
  // Default: false
  // Immutable.
  bool cf_allow_ingest_behind = false;

  // If true, use batch lookup optimization for memtable MultiGet. For skip
  // list memtables, after looking up each key, the search path is cached and
  // reused for the next key, reducing per-key cost from O(log N) to O(log d)
  // where d is the distance between consecutive keys.
  //
  // This optimization exploits the fact that MultiGet keys are sorted.
  // Non-skip-list memtable implementations fall back to per-key lookups.
  //
  // Default: false
  // Immutable.
  bool memtable_batch_lookup_optimization = false;

  // Create ColumnFamilyOptions with default values for all fields
  AdvancedColumnFamilyOptions();
  // Create ColumnFamilyOptions from Options
  explicit AdvancedColumnFamilyOptions(const Options& options);

  // ---------------- OPTIONS NOT SUPPORTED ANYMORE ----------------
};

}  // namespace ROCKSDB_NAMESPACE
