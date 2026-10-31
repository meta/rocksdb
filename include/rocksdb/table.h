// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Currently we support two types of tables: plain table and block-based table.
//   1. Block-based table: this is the default table type that we inherited from
//      LevelDB, which was designed for storing data in hard disk or flash
//      device.
//   2. Plain table: it is one of RocksDB's SST file format optimized
//      for low query latency on pure-memory or really low-latency media.
//
// A tutorial of rocksdb table formats is available here:
//   https://github.com/meta/rocksdb/wiki/A-Tutorial-of-RocksDB-SST-formats
//
// Example code is also available
//   https://github.com/meta/rocksdb/wiki/A-Tutorial-of-RocksDB-SST-formats#wiki-examples

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "rocksdb/cache.h"
#include "rocksdb/cleanable.h"
#include "rocksdb/customizable.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// -- Block-based Table
class Cache;
class FilterPolicy;
class FlushBlockPolicyFactory;
class PersistentCache;
class RandomAccessFile;
struct TableReaderOptions;
struct TableBuilderOptions;
class TableBuilder;
class TableFactory;
class TableReader;
class WritableFileWriter;
struct ConfigOptions;
struct EnvOptions;
class IndexFactory;

// Hook for providing read-scoped storage for data blocks read from SST files.
// This is configured through C++ options rather than OPTIONS files.
//
// Current support is limited to block-based table iterator scans and MultiScan
// data-block reads. Reads using mmap ignore this provider and use normal
// RocksDB block backing.
//
// The provider backs final data-block contents pinned by the scan. RocksDB may
// still use ordinary temporary scratch for serialized block bytes, such as when
// reading a block that may be compressed before decompressing or copying the
// final data block into provider-backed storage.
//
// This is separate from MemoryAllocator because each allocation needs a
// per-lease cleanup handle that RocksDB can attach to pinned blocks/slices, and
// direct-I/O reads that use provider-backed read buffers need the requested
// alignment to be passed to the provider.
//
// TODO: Extend support to point lookups (Get/MultiGet) once those paths can
// preserve provider-backed block ownership.
//
// Requirements:
// - If the same provider instance is shared by multiple concurrently active
//   readers, `Allocate()` must be safe to call concurrently.
// - `Lease::data` must point to at least `size` bytes of writable contiguous
//   memory that remains valid until every copy of `Lease::cleanup` is reset.
// - `Lease::data` must be aligned to `alignment` bytes, where `alignment` is a
//   power of two and `1` means no special alignment requirement.
// - When `alignment` is greater than 1, `Lease::size` must also be a multiple
//   of `alignment` so it can be used as direct-I/O backing storage.
// - `Lease::cleanup` must be non-null on success. Its cleanup may run on any
//   thread that releases the last RocksDB reference. If the cleanup touches
//   provider or other shared state, it must synchronize with Allocate() and
//   other provider cleanup callbacks.
// - After `Allocate()` succeeds, RocksDB releases `Lease::cleanup` on all
//   paths, including later I/O or decompression failure.
class ReadScopedBlockBufferProvider {
 public:
  // A Lease hands one writable backing allocation from the provider to
  // RocksDB. For a provider-backed block, the final BlockContents data points
  // into this allocation and RocksDB attaches `cleanup` to the resulting Blocks
  // and any slices pinned from them. File-read scratch, copying, and
  // decompression choices are implementation details outside this contract.
  //
  // The provider controls allocation reclamation. RocksDB keeps the data valid
  // by copying `cleanup`; the provider must not reuse or release `data` until
  // every copy of `cleanup` has been reset. `size` is the usable backing
  // allocation size and may be larger than the requested allocation size. For
  // direct-I/O reads, `size` must be a multiple of the requested alignment.
  struct Lease {
    // Writable contiguous memory for the loaded block.
    char* data = nullptr;
    size_t size = 0;
    // Reclaims `data` after all derived pinned key/value slices are released.
    SharedCleanablePtr cleanup;
  };

  virtual ~ReadScopedBlockBufferProvider() = default;

  virtual Status Allocate(size_t size, size_t alignment, Lease* out) = 0;
};

// Types of checksums to use for checking integrity of logical blocks within
// files. All checksums currently use 32 bits of checking power (1 in 4B
// chance of failing to detect random corruption). Traditionally, the actual
// checking power can be far from ideal if the corruption is due to misplaced
// data (e.g. physical blocks out of order in a file, or from another file),
// which is fixed in format_version=6 (see below).
enum ChecksumType : char {
  kNoChecksum = 0x0,
  kCRC32c = 0x1,
  kxxHash = 0x2,
  kxxHash64 = 0x3,
  kXXH3 = 0x4,  // Supported since RocksDB 6.27
};

// `PinningTier` is used to specify which tier of block-based tables should
// be affected by a block cache pinning setting (see
// `MetadataCacheOptions` below).
enum class PinningTier {
  // For compatibility, this value specifies to fallback to the behavior
  // indicated by the deprecated options,
  // `pin_l0_filter_and_index_blocks_in_cache` and
  // `pin_top_level_index_and_filter`.
  kFallback,

  // This tier contains no block-based tables.
  kNone,

  // This tier contains block-based tables that may have originated from a
  // memtable flush. In particular, it includes tables from L0 that are smaller
  // than 1.5 times the current `write_buffer_size`. Note these criteria imply
  // it can include intra-L0 compaction outputs and ingested files, as long as
  // they are not abnormally large compared to flushed files in L0.
  kFlushedAndSimilar,

  // This tier contains all block-based tables.
  kAll,
};

// `MetadataCacheOptions` contains members indicating the desired caching
// behavior for the different categories of metadata blocks.
struct MetadataCacheOptions {
  // The tier of block-based tables whose top-level index into metadata
  // partitions will be pinned. Currently indexes and filters may be
  // partitioned.
  //
  // Note `cache_index_and_filter_blocks` must be true for this option to have
  // any effect. Otherwise any top-level index into metadata partitions would be
  // held in table reader memory, outside the block cache.
  PinningTier top_level_index_pinning = PinningTier::kFallback;

  // The tier of block-based tables whose metadata partitions will be pinned.
  // Currently indexes and filters may be partitioned.
  PinningTier partition_pinning = PinningTier::kFallback;

  // The tier of block-based tables whose unpartitioned metadata blocks will be
  // pinned.
  //
  // Note `cache_index_and_filter_blocks` must be true for this option to have
  // any effect. Otherwise the unpartitioned meta-blocks would be held in table
  // reader memory, outside the block cache.
  PinningTier unpartitioned_pinning = PinningTier::kFallback;
};

struct CacheEntryRoleOptions {
  enum class Decision {
    kEnabled,
    kDisabled,
    kFallback,
  };
  Decision charged = Decision::kFallback;
  bool operator==(const CacheEntryRoleOptions& other) const {
    return charged == other.charged;
  }
};

struct CacheUsageOptions {
  CacheEntryRoleOptions options;
  std::map<CacheEntryRole, CacheEntryRoleOptions> options_overrides;
};

// Configures how SST files using the block-based table format (standard)
// are written and read. With few exceptions, each option only affects either
// (a) how new SST files are written, or (b) how SST files are read. If an
// option seems to affect how the SST file is constructed, e.g. format_version,
// that option *ONLY* has an effect at construction time. Contrast this with
// options like the various `cache` and `pin` options, that only affect
// in-memory and IO behavior at read time. In general, any version of RocksDB
// able to read the full key-value and indexing data in the SST file will read
// it as written regardless of current options for writing new files. See
// filter_policy regarding filters.
//
// Except as specifically noted, all options here are "mutable" using
// SetOptions(), with the caveat that only new table builders and new table
// readers will pick up new options. This is nearly immediate effect for
// SST building, but in the worst case, options affecting reads only take
// effect for new files. (Unless the DB is closed and re-opened, table readers
// can live as long as the SST file itself.)
//
// Examples (DB* db):
// db->SetOptions({{"block_based_table_factory",
//                  "{detect_filter_construct_corruption=true;}"}});
// db->SetOptions({{"block_based_table_factory",
//                  "{max_auto_readahead_size=0;block_size=8192;}"}}));
// db->SetOptions({{"block_based_table_factory",
//                  "{prepopulate_block_cache=kFlushOnly;}"}}));
// db->SetOptions({{"block_based_table_factory",
//                  "{filter_policy=ribbonfilter:10;}"}});
struct BlockBasedTableOptions {
  static const char* kName() { return "BlockTableOptions"; }
  // @flush_block_policy_factory creates the instances of flush block policy.
  // which provides a configurable way to determine when to flush a block in
  // the block based tables.  If not set, table builder will use the default
  // block flush policy, which cut blocks by block size (please refer to
  // `FlushBlockBySizePolicy`).
  std::shared_ptr<FlushBlockPolicyFactory> flush_block_policy_factory;

  // TODO(kailiu) Temporarily disable this feature by making the default value
  // to be false.
  //
  // TODO(ajkr) we need to update names of variables controlling meta-block
  // caching as they should now apply to range tombstone and compression
  // dictionary meta-blocks, in addition to index and filter meta-blocks.
  //
  // Whether to put index/filter blocks in the block cache. When false,
  // each "table reader" object will pre-load index/filter blocks during
  // table initialization. Index and filter partition blocks always use
  // block cache regardless of this option.
  bool cache_index_and_filter_blocks = false;

  // If cache_index_and_filter_blocks is enabled, cache index and filter
  // blocks with high priority. If set to true, depending on implementation of
  // block cache, index, filter, and other metadata blocks may be less likely
  // to be evicted than data blocks.
  bool cache_index_and_filter_blocks_with_high_priority = true;

  // DEPRECATED: This option will be removed in a future version. For now, this
  // option still takes effect by updating each of the following variables that
  // has the default value, `PinningTier::kFallback`:
  //
  // - `MetadataCacheOptions::partition_pinning`
  // - `MetadataCacheOptions::unpartitioned_pinning`
  //
  // The updated value is chosen as follows:
  //
  // - `pin_l0_filter_and_index_blocks_in_cache == false` ->
  //   `PinningTier::kNone`
  // - `pin_l0_filter_and_index_blocks_in_cache == true` ->
  //   `PinningTier::kFlushedAndSimilar`
  //
  // To migrate away from this flag, explicitly configure
  // `MetadataCacheOptions` as described above.
  //
  // if cache_index_and_filter_blocks is true and the below is true, then
  // filter and index blocks are stored in the cache, but a reference is
  // held in the "table reader" object so the blocks are pinned and only
  // evicted from cache when the table reader is freed.
  bool pin_l0_filter_and_index_blocks_in_cache = false;

  // DEPRECATED: This option will be removed in a future version. For now, this
  // option still takes effect by updating
  // `MetadataCacheOptions::top_level_index_pinning` when it has the
  // default value, `PinningTier::kFallback`.
  //
  // The updated value is chosen as follows:
  //
  // - `pin_top_level_index_and_filter == false` ->
  //   `PinningTier::kNone`
  // - `pin_top_level_index_and_filter == true` ->
  //   `PinningTier::kAll`
  //
  // To migrate away from this flag, explicitly configure
  // `MetadataCacheOptions` as described above.
  //
  // If cache_index_and_filter_blocks is true and the below is true, then
  // the top-level index of partitioned filter and index blocks are stored in
  // the cache, but a reference is held in the "table reader" object so the
  // blocks are pinned and only evicted from cache when the table reader is
  // freed. This is not limited to l0 in LSM tree.
  bool pin_top_level_index_and_filter = true;

  // The desired block cache pinning behavior for the different categories of
  // metadata blocks. While pinning can reduce block cache contention, users
  // must take care not to pin excessive amounts of data, which risks
  // overflowing block cache.
  MetadataCacheOptions metadata_cache_options;

  // The index type that will be used for this table.
  enum IndexType : char {
    // A space efficient index block that is optimized for
    // binary-search-based index.
    kBinarySearch = 0x00,

    // The hash index, if enabled, will do the hash lookup when
    // `Options.prefix_extractor` is provided.
    kHashSearch = 0x01,

    // A two-level index implementation. Both levels are binary search indexes.
    // Second level index blocks ("partitions") use block cache even when
    // cache_index_and_filter_blocks=false.
    kTwoLevelIndexSearch = 0x02,

    // Like kBinarySearch, but index also contains first key of each block.
    // This allows iterators to defer reading the block until it's actually
    // needed. May significantly reduce read amplification of short range scans.
    // Without it, iterator seek usually reads one block from each level-0 file
    // and from each level, which may be expensive.
    // Works best in combination with:
    //  - IndexShorteningMode::kNoShortening,
    //  - custom FlushBlockPolicy to cut blocks at some meaningful boundaries,
    //    e.g. when prefix changes.
    // Makes the index significantly bigger (2x or more), especially when keys
    // are long.
    kBinarySearchWithFirstKey = 0x03,
  };

  IndexType index_type = kBinarySearch;

  // The search algorithm used when seeking to entries in the index block.
  //
  // Note: This option is only used at read time and is compatible with any type
  // of block.
  enum BlockSearchType : char {
    // Standard binary search
    kBinary = 0x00,
    // Interpolation search, which may be better suited for uniformly
    // distributed keys. This will only be applicable if the comparator is the
    // byte-wise comparator. Avoid using
    // IndexShorteningMode::kShortenSeparatorsAndSuccessor as shortening the
    // succesor can skew the end key and make interpolation search significantly
    // less performant.
    kInterpolation = 0x01,
    // See `uniform_cv_threshold`. On the write path if `uniform_cv_threshold`
    // >= 0, then it is possible for a block to be marked as "is_uniform=true"
    // in the block footer via bit flag. On files from older versions or
    // produced via `uniform_cv_threshold` < 0, blocks are always marked as
    // "is_uniform=false".
    //
    // When kAuto is used, the search algorithm will use interpolation search if
    // "is_uniform" flag is set in the block footer, otherwise it will use
    // binary search.
    kAuto = 0x02,
  };

  BlockSearchType index_block_search_type = kBinary;

  // The index type that will be used for the data block.
  enum DataBlockIndexType : char {
    kDataBlockBinarySearch = 0,   // traditional block type
    kDataBlockBinaryAndHash = 1,  // additional hash index
  };

  DataBlockIndexType data_block_index_type = kDataBlockBinarySearch;

  // #entries/#buckets. It is valid only when data_block_hash_index_type is
  // kDataBlockBinaryAndHash.
  double data_block_hash_table_util_ratio = 0.75;

  // Option hash_index_allow_collision is now deleted.
  // It will behave as if hash_index_allow_collision=true.

  // Use the specified checksum type. Newly created table files will be
  // protected with this checksum type. Old table files will still be readable,
  // even though they have different checksum type.
  ChecksumType checksum = kXXH3;

  // Disable block cache. If this is set to true, then no block cache
  // will be configured (block_cache reset to nullptr).
  //
  // This option should not be used with SetOptions.
  bool no_block_cache = false;

  // If non-nullptr and no_block_cache == false, use the specified cache for
  // blocks. If nullptr and no_block_cache == false, a 32MB internal cache
  // will be created and used.
  //
  // This option should not be used with SetOptions, because (a) the code
  // to make it safe is incomplete, and (b) it is not clear when/if the
  // old block cache would go away. For now, dynamic changes to block cache
  // should be through the Cache object, e.g. Cache::SetCapacity().
  std::shared_ptr<Cache> block_cache = nullptr;

  // If non-NULL use the specified cache for pages read from device
  // IF NULL, no page cache is used
  std::shared_ptr<PersistentCache> persistent_cache = nullptr;

  // Approximate size of user data packed per block.  Note that the
  // block size specified here corresponds to uncompressed data.  The
  // actual size of the unit read from disk may be smaller if
  // compression is enabled.  This parameter can be changed dynamically.
  uint64_t block_size = 4 * 1024;

  // This is used to close a block before it reaches the configured
  // 'block_size'. If the percentage of free space in the current block is less
  // than this specified number and adding a new record to the block will
  // exceed the configured block size, then this block will be closed and the
  // new record will be written to the next block.
  int block_size_deviation = 10;

  // Number of keys between restart points for delta encoding of keys.
  // This parameter can be changed dynamically.  Most clients should
  // leave this parameter alone.  The minimum value allowed is 1.  Any smaller
  // value will be silently overwritten with 1.
  int block_restart_interval = 16;

  // Same as block_restart_interval but used for the index block.
  int index_block_restart_interval = 1;

  // Target block size for partitioned metadata. Currently applied to indexes
  // when kTwoLevelIndexSearch is used and to filters when partition_filters is
  // used. When decouple_partitioned_filters=false (original behavior), there is
  // much more deviation from this target size. See the comment on
  // decouple_partitioned_filters.
  uint64_t metadata_block_size = 4096;

  // `cache_usage_options` allows users to specify the default
  // options (`cache_usage_options.options`) and the overriding
  // options (`cache_usage_options.options_overrides`)
  // for different `CacheEntryRole` under various features related to cache
  // usage.
  //
  // For a certain `CacheEntryRole role` and a certain feature `f` of
  // `CacheEntryRoleOptions`:
  // 1. If `options_overrides` has an entry for `role` and
  // `options_overrides[role].f != kFallback`, we use
  // `options_overrides[role].f`
  // 2. Otherwise, if `options[role].f != kFallback`, we use `options[role].f`
  // 3. Otherwise, we follow the compatible existing behavior for `f` (see
  // each feature's comment for more)
  //
  // `cache_usage_options` currently supports specifying options for the
  // following features:
  //
  // 1. Memory charging to block cache (`CacheEntryRoleOptions::charged`)
  // Memory charging is a feature of accounting memory usage of specific area
  // (represented by `CacheEntryRole`) toward usage in block cache (if
  // available), by updating a dynamical charge to the block cache loosely based
  // on the actual memory usage of that area.
  //
  // (a) CacheEntryRole::kCompressionDictionaryBuildingBuffer
  // (i) If kEnabled:
  // Charge memory usage of the buffered data used as training samples for
  // dictionary compression.
  // If such memory usage exceeds the avaible space left in the block cache
  // at some point (i.e, causing a cache full under
  // `LRUCacheOptions::strict_capacity_limit` = true), the data will then be
  // unbuffered.
  // (ii) If kDisabled:
  // Does not charge the memory usage mentioned above.
  // (iii) Compatible existing behavior:
  // Same as kEnabled.
  //
  // (b) CacheEntryRole::kFilterConstruction
  // (i) If kEnabled:
  // Charge memory usage of Bloom Filter
  // (format_version >= 5) and Ribbon Filter construction.
  // If additional temporary memory of Ribbon Filter exceeds the avaible
  // space left in the block cache at some point (i.e, causing a cache full
  // under `LRUCacheOptions::strict_capacity_limit` = true),
  // construction will fall back to Bloom Filter.
  // (ii) If kDisabled:
  // Does not charge the memory usage mentioned above.
  // (iii) Compatible existing behavior:
  // Same as kDisabled.
  //
  // (c) CacheEntryRole::kBlockBasedTableReader
  // (i) If kEnabled:
  // Charge memory usage of table properties +
  // index block/filter block/uncompression dictionary (when stored in table
  // reader i.e, BlockBasedTableOptions::cache_index_and_filter_blocks ==
  // false) + some internal data structures during table reader creation.
  // If such a table reader exceeds
  // the avaible space left in the block cache at some point (i.e, causing
  // a cache full under `LRUCacheOptions::strict_capacity_limit` = true),
  // creation will fail with Status::MemoryLimit().
  // (ii) If kDisabled:
  // Does not charge the memory usage mentioned above.
  // (iii) Compatible existing behavior:
  // Same as kDisabled.
  //
  // (d) CacheEntryRole::kFileMetadata
  // (i) If kEnabled:
  // Charge memory usage of file metadata. RocksDB holds one file metadata
  // structure in-memory per on-disk table file.
  // If such file metadata's
  // memory exceeds the avaible space left in the block cache at some point
  // (i.e, causing a cache full under `LRUCacheOptions::strict_capacity_limit` =
  // true), creation will fail with Status::MemoryLimit().
  // (ii) If kDisabled:
  // Does not charge the memory usage mentioned above.
  // (iii) Compatible existing behavior:
  // Same as kDisabled.
  //
  // (e) Other CacheEntryRole
  // Not supported.
  // `Status::kNotSupported` will be returned if
  // `CacheEntryRoleOptions::charged` is set to {`kEnabled`, `kDisabled`}.
  //
  //
  // 2. More to come ...
  //
  CacheUsageOptions cache_usage_options;

  // Note: currently this option requires kTwoLevelIndexSearch to be set as
  // well.
  // TODO(myabandeh): remove the note above once the limitation is lifted
  // Use partitioned full filters for each SST file. This option is
  // incompatible with block-based filters. Filter partition blocks use
  // block cache even when cache_index_and_filter_blocks=false.
  bool partition_filters = false;

  // When both partitioned indexes and partitioned filters are enabled,
  // this enables independent partitioning boundaries between the two. Most
  // notably, this enables these metadata blocks to hit their target size much
  // more accurately, as there is often a disparity between index sizes and
  // filter sizes. This should reduce fragmentation and metadata overheads in
  // the block cache, as well as treat blocks more fairly for cache eviction
  // purposes.
  //
  // There are no SST format compatibility issues with this option. (All
  // versions of RocksDB able to read partitioned filters are able to read
  // decoupled partitioned filters.)
  //
  // decouple_partitioned_filters = true is the new default. This option is now
  // DEPRECATED and might be ignored and/or removed in a future release.
  //
  // NOTE: decouple_partitioned_filters = false with partition_filters = true
  // disables parallel compression (CompressionOptions::parallel_threads
  // sanitized to 1).
  bool decouple_partitioned_filters = true;

  // Option to generate Bloom/Ribbon filters that minimize memory
  // internal fragmentation.
  //
  // When false, malloc_usable_size is not available, or format_version < 5,
  // filters are generated without regard to internal fragmentation when
  // loaded into memory (historical behavior). When true (and
  // malloc_usable_size is available and format_version >= 5), then
  // filters are generated to "round up" and "round down" their sizes to
  // minimize internal fragmentation when loaded into memory, assuming the
  // reading DB has the same memory allocation characteristics as the
  // generating DB. This option does not break forward or backward
  // compatibility.
  //
  // While individual filters will vary in bits/key and false positive rate
  // when setting is true, the implementation attempts to maintain a weighted
  // average FP rate for filters consistent with this option set to false.
  //
  // With Jemalloc for example, this setting is expected to save about 10% of
  // the memory footprint and block cache charge of filters, while increasing
  // disk usage of filters by about 1-2% due to encoding efficiency losses
  // with variance in bits/key.
  //
  // NOTE: Because some memory counted by block cache might be unmapped pages
  // within internal fragmentation, this option can increase observed RSS
  // memory usage. With cache_index_and_filter_blocks=true, this option makes
  // the block cache better at using space it is allowed. (These issues
  // should not arise with partitioned filters.)
  //
  // NOTE: Set to false if you do not trust malloc_usable_size. When set to
  // true, RocksDB might access an allocated memory object beyond its original
  // size if malloc_usable_size says it is safe to do so. While this can be
  // considered bad practice, it should not produce undefined behavior unless
  // malloc_usable_size is buggy or broken.
  bool optimize_filters_for_memory = true;

  // Use delta encoding to compress keys in blocks.
  // ReadOptions::pin_data requires this option to be disabled.
  //
  // Default: true
  bool use_delta_encoding = true;

  // If non-nullptr, use the specified filter policy to reduce disk reads.
  // Many applications will benefit from passing the result of
  // NewBloomFilterPolicy() here.
  //
  // Because filters only impact performance and are not data-critical, an
  // SST file can be opened and used without filters if (a) the filter
  // policy name or schema is unrecognized, or (b) filter_policy is nullptr.
  // See filter_policy regarding filters.
  std::shared_ptr<const FilterPolicy> filter_policy = nullptr;

  // EXPERIMENTAL
  //
  // If non-nullptr, build a custom index using this factory in addition to
  // (or instead of) the standard index, controlled by index_mode below.
  // Parallel compression is supported per-builder: see
  // IndexFactoryBuilder::SupportsParallelAddEntry. If any configured
  // builder doesn't opt in, the table builder falls back to single-
  // threaded compression for the SST.
  std::shared_ptr<IndexFactory> user_defined_index_factory = nullptr;

  // EXPERIMENTAL
  //
  // Controls how the custom IndexFactory interacts with the standard
  // index (the index selected by index_type -- binary search by default,
  // hash search, or partitioned). kCustomDefault and kCustomOnly require
  // user_defined_index_factory to be set because reads route through the
  // custom index by default or exclusively.
  //
  //   kStandardOnly:
  //     Only the standard index is used.
  //     user_defined_index_factory is ignored if set. Use this mode to
  //     explicitly suppress UDI building while leaving a factory pointer
  //     configured.
  //
  //   kStandardDefault:
  //     Both indexes are built. Reads use the standard index by default.
  //     The custom index is accessible via ReadOptions::read_index
  //     for per-read override. When opening SSTs that lack the custom
  //     index block, falls back to the standard index with a warning
  //     (not a hard error).
  //
  //   kStandardRequired:
  //     Same read routing as kStandardDefault, but opening an SST that
  //     lacks the custom index block is a hard error when a factory is
  //     configured. For compatibility with legacy UDI SSTs, a present zero-size
  //     custom index block is treated as "no UDI" and read through the
  //     populated standard index. This preserves the legacy
  //     fail_if_no_udi_on_open=true behavior without changing default reads to
  //     the custom index. If no factory is configured, this behaves like
  //     kStandardDefault/no factory so OPTIONS files from older binaries remain
  //     readable when the non-serialized factory pointer is not restored.
  //
  //   kCustomDefault:
  //     Both indexes are built. All reads (including internal operations
  //     like compaction and VerifyChecksum) route through the custom
  //     index. The standard index serves as a safety fallback for
  //     backup/restore and rollback. A present zero-size legacy custom index
  //     block also falls back to the standard index for upgrade compatibility.
  //
  //   kCustomOnly:
  //     Only the custom index is built. The standard index is not
  //     populated (a minimal stub satisfies the SST footer format).
  //     Requires format_version >= 6 so the footer can mark the SST as
  //     requiring a custom index. Older readers reject that marker rather
  //     than treating the standard-index stub as an empty table.
  //     Maximum efficiency but no fallback -- rollback requires
  //     compacting with a mode that builds the standard index.
  //
  // Default: kStandardDefault. With no user_defined_index_factory, this
  // behaves exactly like kStandardOnly. With a factory, it preserves the
  // legacy behavior where setting only user_defined_index_factory builds a
  // secondary UDI while reads continue to use the standard index by default.
  //
  // Recommended migration path (each step yields SSTs readable by the
  // next): kStandardDefault -> kStandardRequired -> kCustomDefault ->
  // kCustomOnly
  //
  // Rollback:
  //   From kCustomDefault: switch to kStandardDefault or kStandardOnly.
  //     The standard index is fully populated, so no rewrite is needed.
  //     kStandardOnly silently ignores any factory pointer that remains set
  //     in the config.
  //   From kCustomOnly: switch to kCustomDefault and compact to rewrite
  //     all SSTs with both indexes before downgrading further.
  //   An SST's index routing is fixed when its table reader is opened. A
  //   change made through SetOptions() applies to SSTs opened afterwards;
  //   SSTs already open keep their routing until they are reopened, e.g. by
  //   reopening the DB.
  //
  // Backup/restore and other Options-serialization reopen paths:
  //   user_defined_index_factory (shared_ptr) does not survive Options
  //   serialization unless reconstructed through the object registry. If a
  //   factory is lost in kStandardDefault or kStandardRequired, RocksDB opens
  //   through the standard index. For kCustomDefault/kCustomOnly, callers must
  //   explicitly reattach the same factory before opening the DB, or change
  //   index_mode to kStandardOnly for a standard-index-only restore.
  //
  // Incompatible with:
  //   - Partitioned index (kTwoLevelIndexSearch) in kCustomDefault/kCustomOnly
  //   - Partitioned filters in kCustomDefault/kCustomOnly
  //   - User-defined timestamps with a factory in any mode except kStandardOnly
  //
  // OPTIONS file compatibility: the legacy boolean names
  // use_udi_as_primary_index, skip_standard_index, and
  // fail_if_no_udi_on_open are accepted as parse aliases when index_mode is
  // absent, or when an unmarked legacy encoding contains kStandardDefault.
  // New OPTIONS files also serialize index_mode_explicit=true,
  // making their mode authoritative regardless of legacy flags or parse order.
  // They also serialize equivalent legacy booleans for kStandardDefault,
  // kStandardRequired, and kCustomDefault so older binaries retain routing when
  // loading newer OPTIONS files with ignore_unknown_options=true. The other
  // modes have no legacy equivalent; custom-only SSTs reject older readers.
  // When constructing a factory from this C++ options struct, the default
  // kStandardDefault value is indistinguishable from an unset mode, so legacy
  // bools still apply. Using ConfigureOption() on the factory to set
  // index_mode=kStandardDefault selects it explicitly, overriding legacy bools.
  enum class IndexMode {
    kStandardOnly = 0,
    kStandardDefault = 1,
    kCustomDefault = 2,
    kCustomOnly = 3,
    // Keep the original numeric values for kCustomDefault/kCustomOnly so
    // existing numeric flags and old binaries do not silently change meaning.
    kStandardRequired = 4,
  };
  IndexMode index_mode = IndexMode::kStandardDefault;

  // EXPERIMENTAL
  //
  // Deprecated compatibility input for old UDI callers. Prefer index_mode.
  // GetEffectiveIndexMode() resolves this to kCustomDefault when
  // index_mode is still kStandardDefault.
  bool use_udi_as_primary_index = false;

  // EXPERIMENTAL
  //
  // Deprecated compatibility input for old UDI callers. Prefer index_mode.
  // GetEffectiveIndexMode() resolves this to kStandardRequired when
  // index_mode is still kStandardDefault and use_udi_as_primary_index is false.
  bool fail_if_no_udi_on_open = false;

  // Resolves deprecated inputs without changing index_mode. GetOptions()
  // preserves those inputs so copying the options and clearing a legacy bool
  // can still roll back its policy. Explicit modes configured through the
  // factory clear ignored legacy inputs, including for kStandardDefault.
  IndexMode GetEffectiveIndexMode() const {
    if (index_mode != IndexMode::kStandardDefault) {
      return index_mode;
    }
    if (use_udi_as_primary_index) {
      return IndexMode::kCustomDefault;
    }
    return fail_if_no_udi_on_open ? IndexMode::kStandardRequired
                                  : IndexMode::kStandardDefault;
  }

  // If true, place whole keys in the filter (not just prefixes).
  // This must generally be true for gets to be efficient.
  bool whole_key_filtering = true;

  // If true, detect corruption during Bloom Filter (format_version >= 5)
  // and Ribbon Filter construction.
  //
  // This is an extra check that is only
  // useful in detecting software bugs or CPU+memory malfunction.
  // Turning on this feature increases filter construction time by 30%.
  //
  // TODO: optimize this performance
  bool detect_filter_construct_corruption = false;

  // Verify that decompressing the compressed block gives back the input. This
  // is a verification mode that we use to detect bugs in compression
  // algorithms.
  bool verify_compression = false;

  // If used, For every data block we load into memory, we will create a bitmap
  // of size ((block_size / `read_amp_bytes_per_bit`) / 8) bytes. This bitmap
  // will be used to figure out the percentage we actually read of the blocks.
  //
  // When this feature is used Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES and
  // Tickers::READ_AMP_TOTAL_READ_BYTES can be used to calculate the
  // read amplification using this formula
  // (READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES)
  //
  // value  =>  memory usage (percentage of loaded blocks memory)
  // 1      =>  12.50 %
  // 2      =>  06.25 %
  // 4      =>  03.12 %
  // 8      =>  01.56 %
  // 16     =>  00.78 %
  //
  // Note: This number must be a power of 2, if not it will be sanitized
  // to be the next lowest power of 2, for example a value of 7 will be
  // treated as 4, a value of 19 will be treated as 16.
  //
  // Default: 0 (disabled)
  uint32_t read_amp_bytes_per_bit = 0;

  // We currently have these format versions:
  // 0 - 1 -- No longer supported. Attempting to read files with these format
  // versions will return an error. To upgrade, load the data with RocksDB
  // >= 4.6.0 and < 11.0.0, then run a full compaction.
  // 2 -- Can be read by RocksDB's versions since 3.10. Changes the way we
  // encode compressed blocks with LZ4, BZip2 and Zlib compression. If you
  // don't plan to run RocksDB before version 3.10, you should probably use
  // this.
  // 3 -- Can be read by RocksDB's versions since 5.15. Changes the way we
  // encode the keys in index blocks. If you don't plan to run RocksDB before
  // version 5.15, you should probably use this.
  // This option only affects newly written tables. When reading existing
  // tables, the information about version is read from the footer.
  // 4 -- Can be read by RocksDB's versions since 5.16. Changes the way we
  // encode the values in index blocks. If you don't plan to run RocksDB before
  // version 5.16 and you are using index_block_restart_interval > 1, you should
  // probably use this as it would reduce the index size.
  // This option only affects newly written tables. When reading existing
  // tables, the information about version is read from the footer.
  // 5 -- Can be read by RocksDB's versions since 6.6.0. Full and partitioned
  // filters use a generally faster and more accurate Bloom filter
  // implementation, with a different schema.
  // 6 -- Modified the file footer and checksum matching so that SST data
  // misplaced within or between files is as likely to fail checksum
  // verification as random corruption. Also checksum-protects SST footer.
  // Can be read by RocksDB versions >= 8.6.0.
  // 7 -- Support for custom compression algorithms with a CompressionManager
  // using a non-built-in CompatibilityName(). See `compression_manager` in
  // ColumnFamilyOptions. Also changes the format of TableProperties field
  // `compression_name`. Can be read by RocksDB versions >= 10.4.0.
  // 8 -- Support for optimize_key_common_prefix, cleaner support for some
  // features like super_block_alignment_size that impact index encoding, and
  // more future-proofing that might guard future opt-in features. Can be read
  // by RocksDB versions >= 11.10.0
  //
  // Using the default setting of format_version is strongly recommended, so
  // that available enhancements are adopted eventually and automatically. The
  // default setting will only update to the latest after thorough production
  // validation and sufficient time and number of releases have elapsed
  // (6 months recommended) to ensure a clean downgrade/revert path for users
  // who might only upgrade a few times per year.
  uint32_t format_version = 7;

  // Latest format_version supported by this version of RocksDB for both
  // reading and writing SST files. Setting format_version higher than this is
  // rejected as InvalidArgument (e.g. by DB::Open() or SetOptions()).
  static constexpr uint32_t kLatestSupportedFormatVersion = 8;

  // When true, data blocks store keys and values separately. Keys are stored
  // at the beginning of the block, followed by values at the end. This can
  // improve read performance at a cost of a varint per restart interval (~1 bit
  // per key by default), in addition to improving compression. Small values or
  // low block_restart_interval may prefer to set this as false.
  //
  // Default: false
  bool separate_key_value_in_data_block = false;

  // Coefficient of variation (CV) threshold used to determine if keys in an
  // index block are uniformly distributed. Lower CV means more "uniform", and
  // the more likely interpolation search will outperform binary search.
  //
  // On the write path, if the CV of key gaps in an index
  // block is less than this threshold, the "is_uniform" hint is set in that
  // block's footer. To disable (i.e. always have "is_uniform=false"), set value
  // to -1.
  //
  // On the read path, if `BlockSearchType::kAuto` is set, then it will use the
  // is_uniform hint to select an appropriate search algorithm for the block.
  //
  // NOTE: Currently only supports index blocks. May update to include data
  // blocks in the future.
  double uniform_cv_threshold = -1;

  // Controls the conditions under which the format_version >= 8 "common
  // user-key prefix" data-block optimization is enabled: the common prefix
  // shared by all keys in a data block is stored once at the start of the block
  // instead of at every restart point. This shrinks the block (and its
  // block-cache footprint) for prefix-heavy data and, for bytewise-ordered
  // keys, speeds up Seek by comparing prefix-stripped suffixes.
  //
  // Only takes effect at format_version >= 8, and requires use_delta_encoding
  // (it builds on delta encoding). When use_delta_encoding is false -- e.g. to
  // preserve zero-copy key pinning for ReadOptions::pin_data -- this is a
  // no-op.
  //
  // The Seek speedup ("fast seek") is only correct for the built-in bytewise
  // and reverse-bytewise comparators. Other comparators can still get the space
  // savings (the reader reconstructs full keys) at a small read-CPU cost.
  //
  // Expressed as a criteria so it need not be toggled per column family based
  // on the comparator (which can vary by CF).
  enum class OptimizeKeyCommonPrefix : char {
    // Never use the optimization.
    kDisabled = 0,
    // Use it only where it also enables the Seek speedup: format_version >= 8
    // with a (reverse-)bytewise comparator. Left off for other comparators.
    // This is the default.
    kIfFastSeek = 1,
    // Use it for all comparators at format_version >= 8. (Reverse-)bytewise get
    // space savings + fast seek; other comparators get the space savings only,
    // read via full-key reconstruction.
    kEnabled = 2,
  };
  OptimizeKeyCommonPrefix optimize_key_common_prefix =
      OptimizeKeyCommonPrefix::kIfFastSeek;

  // Store index blocks on disk in compressed format. Changing this option to
  // false  will avoid the overhead of decompression if index blocks are evicted
  // and read back
  bool enable_index_compression = true;

  // Align data blocks on lesser of page size and block size
  bool block_align = false;

  // Align data blocks on super block alignment. Avoid a data block split across
  // super block boundaries. Works with/without compression.
  //
  // Here a "super block" refers to an aligned unit of underlying Filesystem
  // storage for which there is an extra cost when a random read involves two
  // such super blocks instead of just one. Configuring that size here suggests
  // inserting padding in the SST file to avoid a single SST block splitting
  // across two super blocks. Only power-of-two sizes are supported. See also
  // super_block_alignment_space_overhead_ratio. Default to 0, which means super
  // block alignment is disabled.
  //
  // Super block alignment size. Default to 0, which means super block alignment
  // is disabled. If it is enabled, it needs to be a power of 2 and higher than
  // block size.
  size_t super_block_alignment_size = 0;

  // This option constrols the storage space overhead of super block alignment.
  // It is used to calculate the max padding size allowed for super block
  // alignment. It is calculated in this way. If super_block_alignment_size is
  // 2MB, and super_block_alignment_overhead_ratio is 128, then the max padding
  // size allowed for super block alignment is 2MB / 128 = 16KB.
  // Note that, when it is set to 0, super block alignment is disabled.
  size_t super_block_alignment_space_overhead_ratio = 128;

  // This enum allows trading off increased index size for improved iterator
  // seek performance in some situations, particularly when block cache is
  // disabled (ReadOptions::fill_cache = false) and direct IO is
  // enabled (DBOptions::use_direct_reads = true).
  // The default mode is the best tradeoff for most use cases.
  // This option only affects newly written tables.
  //
  // The index contains a key separating each pair of consecutive blocks.
  // Let A be the highest key in one block, B the lowest key in the next block,
  // and I the index entry separating these two blocks:
  // [ ... A] I [B ...]
  // I is allowed to be anywhere in [A, B).
  // If an iterator is seeked to a key in (A, I], we'll unnecessarily read the
  // first block, then immediately fall through to the second block.
  // However, if I=A, this can't happen, and we'll read only the second block.
  // In kNoShortening mode, we use I=A. In other modes, we use the shortest
  // key in [A, B), which usually significantly reduces index size.
  //
  // There's a similar story for the last index entry, which is an upper bound
  // of the highest key in the file. If it's shortened and therefore
  // overestimated, iterator is likely to unnecessarily read the last data block
  // from each file on each seek.
  enum class IndexShorteningMode : char {
    // Use full keys.
    kNoShortening,
    // Shorten index keys between blocks, but use full key for the last index
    // key, which is the upper bound of the whole file.
    kShortenSeparators,
    // Shorten both keys between blocks and key after last block.
    kShortenSeparatorsAndSuccessor,
  };

  IndexShorteningMode index_shortening =
      IndexShorteningMode::kShortenSeparators;

  // RocksDB does auto-readahead for iterators on noticing more than two reads
  // for a table file if user doesn't provide readahead_size. The readahead
  // starts at BlockBasedTableOptions.initial_auto_readahead_size (default: 8KB)
  // and doubles on every additional read upto max_auto_readahead_size and
  // max_auto_readahead_size can be configured.
  //
  // Special Value: 0 - If max_auto_readahead_size is set 0 then it will disable
  // the implicit auto prefetching.
  // If max_auto_readahead_size provided is less
  // than initial_auto_readahead_size, then RocksDB will sanitize the
  // initial_auto_readahead_size and set it to max_auto_readahead_size.
  //
  // Value should be provided along with KB i.e. 256 * 1024 as it will prefetch
  // the blocks.
  //
  // Found that 256 KB readahead size provides the best performance, based on
  // experiments, for auto readahead. Experiment data is in PR #3282.
  //
  // Default: 256 KB (256 * 1024).
  size_t max_auto_readahead_size = 256 * 1024;

  // If enabled, prepopulate warm/hot blocks (data, uncompressed dict, index and
  // filter blocks) which are already in memory into block cache at the time of
  // flush or compaction.
  //
  // On a flush, the data block that is in memory (in memtables) gets flushed to
  // the device. If using Direct IO, additional IO is incurred to read this data
  // back into memory again, which is avoided by enabling this option. This
  // further helps if the workload exhibits high temporal locality, where most
  // of the reads go to recently written data. This also helps in case of
  // Distributed FileSystem.
  //
  // On a compaction, output SST files are written to disk but not placed in the
  // block cache by default. With tiered or remote storage (e.g., HDFS, S3),
  // reading recently compacted data back incurs high latency.
  // Enabling compaction warming avoids these cold reads. However, unlike flush
  // output, it is hard to distinguish hot from cold blocks in compaction
  // output, so warming all of it risks polluting the cache. To mitigate this,
  // compaction-warmed blocks are inserted at BOTTOM priority (vs LOW for flush)
  // so they are evicted first under cache pressure. Even so,
  // kFlushAndCompaction is recommended only when most or all of the database is
  // expected to reside in cache. For workloads where only a fraction of the
  // data is hot, kFlushOnly is the safer choice.
  enum class PrepopulateBlockCache : char {
    // Disable prepopulate block cache.
    kDisable,
    // Prepopulate blocks during flush only.
    kFlushOnly,
    // Prepopulate blocks during flush and compaction. Flush-warmed blocks are
    // inserted at LOW priority, compaction-warmed blocks at BOTTOM priority.
    kFlushAndCompaction,
  };

  PrepopulateBlockCache prepopulate_block_cache =
      PrepopulateBlockCache::kDisable;

  // RocksDB does auto-readahead for iterators on noticing more than two reads
  // for a table file if user doesn't provide readahead_size. The readahead size
  // starts at initial_auto_readahead_size and doubles on every additional read
  // upto BlockBasedTableOptions.max_auto_readahead_size.
  // max_auto_readahead_size can also be configured.
  //
  // Scenarios:
  // - If initial_auto_readahead_size is set 0 then it will disabled the
  //   implicit auto prefetching irrespective of max_auto_readahead_size.
  // - If max_auto_readahead_size is set 0, it will disable the internal
  //    prefetching irrespective of initial_auto_readahead_size.
  // - If initial_auto_readahead_size > max_auto_readahead_size, then RocksDB
  //   will sanitize the value of initial_auto_readahead_size to
  //   max_auto_readahead_size and readahead_size will be
  //   max_auto_readahead_size.
  //
  // Value should be provided along with KB i.e. 8 * 1024 as it will prefetch
  // the blocks.
  //
  // Default: 8 KB (8 * 1024).
  size_t initial_auto_readahead_size = 8 * 1024;

  // RocksDB does auto-readahead for iterators on noticing more than two reads
  // for a table file if user doesn't provide readahead_size and reads are
  // sequential.
  // num_file_reads_for_auto_readahead indicates after how many
  // sequential reads internal auto prefetching should be start.
  //
  // For example, if value is 2 then after reading 2 sequential data blocks on
  // third data block prefetching will start.
  // If set 0, it will start prefetching from the first read.
  //
  // This parameter can be changed dynamically by
  // DB::SetOptions({{"block_based_table_factory",
  //                  "{num_file_reads_for_auto_readahead=0;}"}}));
  //
  // Changing the value dynamically will only affect files opened after the
  // change.
  //
  // Default: 2
  uint64_t num_file_reads_for_auto_readahead = 2;
};

// Table Properties that are specific to block-based table properties.
struct BlockBasedTablePropertyNames {
  // value of this properties is a fixed int32 number.
  static const std::string kIndexType;
  // value is "1" for true and "0" for false.
  static const std::string kWholeKeyFiltering;
  // value is "1" for true and "0" for false.
  static const std::string kPrefixFiltering;
  // Set to "1" when partitioned filters are decoupled from partitioned indexes.
  // This metadata is recorded in case a read-time optimization for coupled
  // filter+index partitioning is ever developed; that optimization/assumption
  // would be disabled when this is set.
  static const std::string kDecoupledPartitionedFilters;
};

// Create default block based table factory.
TableFactory* NewBlockBasedTableFactory(
    const BlockBasedTableOptions& table_options = BlockBasedTableOptions());

enum EncodingType : char {
  // Always write full keys without any special encoding.
  kPlain,
  // Find opportunity to write the same prefix once for multiple rows.
  // In some cases, when a key follows a previous key with the same prefix,
  // instead of writing out the full key, it just writes out the size of the
  // shared prefix, as well as other bytes, to save some bytes.
  //
  // When using this option, the user is required to use the same prefix
  // extractor to make sure the same prefix will be extracted from the same key.
  // The Name() value of the prefix extractor will be stored in the file. When
  // reopening the file, the name of the options.prefix_extractor given will be
  // bitwise compared to the prefix extractors stored in the file. An error
  // will be returned if the two don't match.
  kPrefix,
};

// Table Properties that are specific to plain table properties.
struct PlainTablePropertyNames {
  static const std::string kEncodingType;
  static const std::string kBloomVersion;
  static const std::string kNumBloomBlocks;
};

const uint32_t kPlainTableVariableLength = 0;

struct PlainTableOptions {
  static const char* kName() { return "PlainTableOptions"; }
  // @user_key_len: plain table has optimization for fix-sized keys, which can
  //                be specified via user_key_len.  Alternatively, you can pass
  //                `kPlainTableVariableLength` if your keys have variable
  //                lengths.
  uint32_t user_key_len = kPlainTableVariableLength;

  // @bloom_bits_per_key: the number of bits used for bloom filer per prefix.
  //                      You may disable it by passing a zero.
  int bloom_bits_per_key = 10;

  // @hash_table_ratio: the desired utilization of the hash table used for
  //                    prefix hashing.
  //                    hash_table_ratio = number of prefixes / #buckets in the
  //                    hash table
  double hash_table_ratio = 0.75;

  // @index_sparseness: inside each prefix, need to build one index record for
  //                    how many keys for binary search inside each hash bucket.
  //                    For encoding type kPrefix, the value will be used when
  //                    writing to determine an interval to rewrite the full
  //                    key. It will also be used as a suggestion and satisfied
  //                    when possible.
  size_t index_sparseness = 16;

  // @huge_page_tlb_size: if <=0, allocate hash indexes and blooms from malloc.
  //                      Otherwise from huge page TLB. The user needs to
  //                      reserve huge pages for it to be allocated, like:
  //                          sysctl -w vm.nr_hugepages=20
  //                      See linux doc Documentation/vm/hugetlbpage.txt
  size_t huge_page_tlb_size = 0;

  // @encoding_type: how to encode the keys. See enum EncodingType above for
  //                 the choices. The value will determine how to encode keys
  //                 when writing to a new SST file. This value will be stored
  //                 inside the SST file which will be used when reading from
  //                 the file, which makes it possible for users to choose
  //                 different encoding type when reopening a DB. Files with
  //                 different encoding types can co-exist in the same DB and
  //                 can be read.
  EncodingType encoding_type = kPlain;

  // @full_scan_mode: mode for reading the whole file one record by one without
  //                  using the index.
  bool full_scan_mode = false;

  // @store_index_in_file: compute plain table index and bloom filter during
  //                       file building and store it in file. When reading
  //                       file, index will be mapped instead of recomputation.
  bool store_index_in_file = false;
};

// -- Plain Table with prefix-only seek
// For this factory, you need to set Options.prefix_extractor properly to make
// it work. Look-up will starts with prefix hash lookup for key prefix. Inside
// the hash bucket found, a binary search is executed for hash conflicts.
// Finally, a linear search is used.

TableFactory* NewPlainTableFactory(
    const PlainTableOptions& options = PlainTableOptions());

struct CuckooTablePropertyNames {
  // The key that is used to fill empty buckets.
  static const std::string kEmptyKey;
  // Fixed length of value.
  static const std::string kValueLength;
  // Number of hash functions used in Cuckoo Hash.
  static const std::string kNumHashFunc;
  // It denotes the number of buckets in a Cuckoo Block. Given a key and a
  // particular hash function, a Cuckoo Block is a set of consecutive buckets,
  // where starting bucket id is given by the hash function on the key. In case
  // of a collision during inserting the key, the builder tries to insert the
  // key in other locations of the cuckoo block before using the next hash
  // function. This reduces cache miss during read operation in case of
  // collision.
  static const std::string kCuckooBlockSize;
  // Size of the hash table. Use this number to compute the modulo of hash
  // function. The actual number of buckets will be kMaxHashTableSize +
  // kCuckooBlockSize - 1. The last kCuckooBlockSize-1 buckets are used to
  // accommodate the Cuckoo Block from end of hash table, due to cache friendly
  // implementation.
  static const std::string kHashTableSize;
  // Denotes if the key sorted in the file is Internal Key (if false)
  // or User Key only (if true).
  static const std::string kIsLastLevel;
  // Indicate if using identity function for the first hash function.
  static const std::string kIdentityAsFirstHash;
  // Indicate if using module or bit and to calculate hash value
  static const std::string kUseModuleHash;
  // Fixed user key length
  static const std::string kUserKeyLength;
};

struct CuckooTableOptions {
  static const char* kName() { return "CuckooTableOptions"; }

  // Determines the utilization of hash tables. Smaller values
  // result in larger hash tables with fewer collisions.
  double hash_table_ratio = 0.9;
  // A property used by builder to determine the depth to go to
  // to search for a path to displace elements in case of
  // collision. See Builder.MakeSpaceForKey method. Higher
  // values result in more efficient hash tables with fewer
  // lookups but take more time to build.
  uint32_t max_search_depth = 100;
  // In case of collision while inserting, the builder
  // attempts to insert in the next cuckoo_block_size
  // locations before skipping over to the next Cuckoo hash
  // function. This makes lookups more cache friendly in case
  // of collisions.
  uint32_t cuckoo_block_size = 5;
  // If this option is enabled, user key is treated as uint64_t and its value
  // is used as hash value directly. This option changes builder's behavior.
  // Reader ignore this option and behave according to what specified in table
  // property.
  bool identity_as_first_hash = false;
  // If this option is set to true, module is used during hash calculation.
  // This often yields better space efficiency at the cost of performance.
  // If this option is set to false, # of entries in table is constrained to be
  // power of two, and bit and is used to calculate hash, which is faster in
  // general.
  bool use_module_hash = true;
};

// Cuckoo Table Factory for SST table format using Cache Friendly Cuckoo Hashing
TableFactory* NewCuckooTableFactory(
    const CuckooTableOptions& table_options = CuckooTableOptions());

class RandomAccessFileReader;

// A base class for table factories.
class TableFactory : public Customizable {
 public:
  static const char* kBlockCacheOpts() { return "BlockCache"; }
  static const char* kBlockBasedTableName() { return "BlockBasedTable"; }
  static const char* kPlainTableName() { return "PlainTable"; }
  static const char* kCuckooTableName() { return "CuckooTable"; }

  // Creates and configures a new TableFactory from the input options and id.
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& id,
                                 std::shared_ptr<TableFactory>* factory);

  static const char* Type() { return "TableFactory"; }

  // Returns a Table object table that can fetch data from file specified
  // in parameter file. It's the caller's responsibility to make sure
  // file is in the correct format.
  //
  // NewTableReader() is called in three places:
  // (1) TableCache::FindTable() calls the function when table cache miss
  //     and cache the table object returned.
  // (2) SstFileDumper (for SST Dump) opens the table and dump the table
  //     contents using the iterator of the table.
  // (3) DBImpl::IngestExternalFile() calls this function to read the contents
  //     of the sst file it's attempting to add
  //
  // table_reader_options is a TableReaderOptions which contain all the
  //    needed parameters and configuration to open the table.
  // file is a file handler to handle the file for the table.
  // file_size is the physical file size of the file.
  // table_reader is the output table reader.
  virtual Status NewTableReader(
      const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache = true) const {
    ReadOptions ro;
    return NewTableReader(ro, table_reader_options, std::move(file), file_size,
                          table_reader, prefetch_index_and_filter_in_cache);
  }

  // Overload of the above function that allows the caller to pass in a
  // ReadOptions
  virtual Status NewTableReader(
      const ReadOptions& ro, const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache) const = 0;

  // Return a table builder to write to a file for this table type.
  //
  // It is called in several places:
  // (1) When flushing memtable to a level-0 output file, it creates a table
  //     builder (In DBImpl::WriteLevel0Table(), by calling BuildTable())
  // (2) During compaction, it gets the builder for writing compaction output
  //     files in DBImpl::OpenCompactionOutputFile().
  // (3) When recovering from transaction logs, it creates a table builder to
  //     write to a level-0 output file (In DBImpl::WriteLevel0TableForRecovery,
  //     by calling BuildTable())
  // (4) When running Repairer, it creates a table builder to convert logs to
  //     SST files (In Repairer::ConvertLogToTable() by calling BuildTable())
  //
  // Multiple configured can be accessed from there, including and not limited
  // to compression options. file is a handle of a writable file.
  // It is the caller's responsibility to keep the file open and close the file
  // after closing the table builder. compression_type is the compression type
  // to use in this table.
  virtual TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      WritableFileWriter* file) const = 0;

  // Clone this TableFactory with the same options, ideally a "shallow" clone
  // in which shared_ptr members and hidden state are (safely) shared between
  // this original and the returned clone.
  virtual std::unique_ptr<TableFactory> Clone() const = 0;

  // Return is delete range supported
  virtual bool IsDeleteRangeSupported() const { return false; }
};

// Create a special table factory that can open either of the supported
// table formats, based on setting inside the SST files. It should be used to
// convert a DB from one table format to another.
// @table_factory_to_write: the table factory used when writing to new files.
// @block_based_table_factory:  block based table factory to use. If NULL, use
//                              a default one.
// @plain_table_factory: plain table factory to use. If NULL, use a default one.
// @cuckoo_table_factory: cuckoo table factory to use. If NULL, use a default
// one.
TableFactory* NewAdaptiveTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write = nullptr,
    std::shared_ptr<TableFactory> block_based_table_factory = nullptr,
    std::shared_ptr<TableFactory> plain_table_factory = nullptr,
    std::shared_ptr<TableFactory> cuckoo_table_factory = nullptr);

}  // namespace ROCKSDB_NAMESPACE
