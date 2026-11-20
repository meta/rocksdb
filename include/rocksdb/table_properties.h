// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#pragma once

#include <stdint.h>

#include <map>
#include <memory>
#include <optional>
#include <string>

#include "rocksdb/customizable.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class InternalTblPropColl;
class CompressionManager;

// -- Table Properties
// Other than basic table properties, each table may also have the user
// collected properties.
// The value of the user-collected properties are encoded as raw bytes --
// users have to interpret these values by themselves.
// Note: To do prefix seek/scan in `UserCollectedProperties`, you can do
// something similar to:
//
// UserCollectedProperties props = ...;
// for (auto pos = props.lower_bound(prefix);
//      pos != props.end() && pos->first.compare(0, prefix.size(), prefix) == 0;
//      ++pos) {
//   ...
// }
using UserCollectedProperties = std::map<std::string, std::string>;

// table properties' human-readable names in the property block.
//
// DEPRECATED: intended for internal use only and will be removed from the
// public API in a future release. Use the corresponding TableProperties
// fields instead.
struct TablePropertiesNames {
  static const std::string kDbId;
  static const std::string kDbSessionId;
  static const std::string kDbHostId;
  static const std::string kOriginalFileNumber;
  static const std::string kDataSize;
  static const std::string kIndexSize;
  static const std::string kIndexPartitions;
  static const std::string kTopLevelIndexSize;
  static const std::string kIndexKeyIsUserKey;
  static const std::string kIndexValueIsDeltaEncoded;
  static const std::string kUDIIsPrimaryIndex;
  static const std::string kStandardIndexIsStub;
  static const std::string kFilterSize;
  static const std::string kRawKeySize;
  static const std::string kRawValueSize;
  static const std::string kReferencedBlobBytes;
  static const std::string kNumDataBlocks;
  static const std::string kNumDataBlocksCompressionRejected;
  static const std::string kNumDataBlocksCompressionBypassed;
  static const std::string kNumUniformBlocks;
  static const std::string kNumEntries;
  static const std::string kNumFilterEntries;
  static const std::string kDeletedKeys;
  static const std::string kMergeOperands;
  static const std::string kNumRangeDeletions;
  static const std::string kFormatVersion;
  static const std::string kFixedKeyLen;
  static const std::string kFilterPolicy;
  static const std::string kColumnFamilyName;
  static const std::string kColumnFamilyId;
  static const std::string kComparator;
  static const std::string kMergeOperator;
  static const std::string kPrefixExtractorName;
  static const std::string kPropertyCollectors;
  static const std::string kCompression;
  static const std::string kCompressionOptions;
  // NOTE: set to "rocksdb.creation.time" because of unfortunate initial
  // mis-naming of the property. See kFileCreationTime and file_creation_time.
  static const std::string kOldestAncestorTime;
  // DEPRECATED: old name for kOldestAncestorTime. Will be removed in a future
  // release.
  static const std::string& kCreationTime;
  static const std::string kOldestKeyTime;
  static const std::string kNewestKeyTime;
  static const std::string kFileCreationTime;
  static const std::string kSlowCompressionEstimatedDataSize;
  static const std::string kFastCompressionEstimatedDataSize;
  static const std::string kSequenceNumberTimeMapping;
  static const std::string kTailStartOffset;
  static const std::string kUserDefinedTimestampsPersisted;
  static const std::string kKeyLargestSeqno;
  static const std::string kKeySmallestSeqno;
  static const std::string kDataBlockRestartInterval;
  static const std::string kIndexBlockRestartInterval;
  static const std::string kSeparateKeyValueInDataBlock;
  static const std::string kUncompressedDataSize;
  static const std::string kLsmInfoAtCreation;
};

// `TablePropertiesCollector` provides the mechanism for users to collect
// their own properties that they are interested in. This class is essentially
// a collection of callback functions that will be invoked during table
// building. It is constructed with TablePropertiesCollectorFactory. The methods
// don't need to be thread-safe, as we will create exactly one
// TablePropertiesCollector object per table and then call it sequentially.
//
// Statuses from these callbacks are currently logged when not OK, but
// otherwise ignored by RocksDB.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class TablePropertiesCollector {
 public:
  virtual ~TablePropertiesCollector() = default;

  // DEPRECATE User defined collector should implement AddUserKey(), though
  //           this old function still works for backward compatible reason.
  // Add() will be called when a new key/value pair is inserted into the table.
  // @params key    the user key that is inserted into the table.
  // @params value  the value that is inserted into the table.
  virtual Status Add(const Slice& /*key*/, const Slice& /*value*/) {
    return Status::InvalidArgument(
        "TablePropertiesCollector::Add() deprecated.");
  }

  // AddUserKey() will be called when a new key/value pair is inserted into the
  // table.
  // @params key    the user key that is inserted into the table.
  // @params value  the value that is inserted into the table.
  // @params file_size the current file size. For BlockBasedTable, this
  //         includes all the data blocks written so far, upto but not including
  //         the current block being built. With parallel compression, data
  //         blocks are written async so it depends on the compression progress.
  virtual Status AddUserKey(const Slice& key, const Slice& value,
                            EntryType /*type*/, SequenceNumber /*seq*/,
                            uint64_t /*file_size*/) {
    // For backwards-compatibility.
    return Add(key, value);
  }

  // Called after each new block is cut
  virtual void BlockAdd(uint64_t /* block_uncomp_bytes */,
                        uint64_t /* block_compressed_bytes_fast */,
                        uint64_t /* block_compressed_bytes_slow */) {
    // Nothing to do here. Callback registers can override.
    return;
  }

  // Finish() will be called when a table has already been built and is ready
  // for writing the properties block.
  // It will be called only once by RocksDB internal.
  // When the returned Status is not OK, the collected properties will not be
  // written to the file's property block.
  //
  // @params properties  User will add their collected statistics to
  // `properties`.
  virtual Status Finish(UserCollectedProperties* properties) = 0;

  // Return the human-readable properties, where the key is property name and
  // the value is the human-readable form of value.
  // Returned properties are used for logging.
  // It will only be called after Finish() has been called by RocksDB internal.
  virtual UserCollectedProperties GetReadableProperties() const = 0;

  // The name of the properties collector can be used for debugging purpose.
  virtual const char* Name() const = 0;

  // Return whether the output file should be further compacted
  virtual bool NeedCompact() const { return false; }

  // For internal use only.
  virtual InternalTblPropColl* AsInternal() { return nullptr; }
};

// Constructs TablePropertiesCollector instances for each table file creation.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class TablePropertiesCollectorFactory : public Customizable {
 public:
  struct Context {
    uint32_t column_family_id = kUnknownColumnFamily;
    // The level at creating the SST file (i.e, table), of which the
    // properties are being collected.
    int level_at_creation = kUnknownLevelAtCreation;
    int num_levels = kUnknownNumLevels;
    // In the tiering case, data with seqnos smaller than or equal to this
    // cutoff sequence number will be considered by a compaction job as eligible
    // to be placed on the last level. When this is the maximum sequence number,
    // it indicates tiering is disabled.
    SequenceNumber last_level_inclusive_max_seqno_threshold = 0;
    static const uint32_t kUnknownColumnFamily;
    static const int kUnknownLevelAtCreation = -1;
    static const int kUnknownNumLevels = -1;

    Context() {}

    Context(uint32_t _column_family_id, int _level_at_creation, int _num_levels,
            SequenceNumber _last_level_inclusive_max_seqno_threshold)
        : column_family_id(_column_family_id),
          level_at_creation(_level_at_creation),
          num_levels(_num_levels),
          last_level_inclusive_max_seqno_threshold(
              _last_level_inclusive_max_seqno_threshold) {}
  };

  ~TablePropertiesCollectorFactory() override = default;
  static const char* Type() { return "TablePropertiesCollectorFactory"; }
  static Status CreateFromString(
      const ConfigOptions& options, const std::string& value,
      std::shared_ptr<TablePropertiesCollectorFactory>* result);

  // To collect properties of a table with the given context, returns
  // a new object inheriting from TablePropertiesCollector. The caller
  // is responsible for deleting the object returned. Alternatively,
  // nullptr may be returned to decline collecting properties for the
  // file (and reduce callback overheads).
  // MUST be thread-safe.
  virtual TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) = 0;

  // The name of the properties collector can be used for debugging purpose.
  const char* Name() const override = 0;

  // Can be overridden by sub-classes to return the Name, followed by
  // configuration info that will // be logged to the info log when the
  // DB is opened
  virtual std::string ToString() const { return Name(); }
};

// Decoded form of TableProperties::lsm_info_at_creation. Encapsulates the
// packed uint64 encoding and its three overall states, so TableProperties
// itself can stay a bare data struct. Round-trips losslessly through
// Encode() / DecodeFrom().
struct LsmInfoAtCreation {
  // Sentinel for an unknown level (same value as
  // TablePropertiesCollectorFactory::Context::kUnknownLevelAtCreation).
  static constexpr int kUnknownLevel = -1;

  // Overall applicability of the LSM-at-creation info:
  //  - kUnknown: nothing recorded (e.g. a file written by an older RocksDB).
  //  - kApplicable: created at a known LSM position (flush / compaction /
  //    recovery); level_at_creation and is_bottommost are meaningful.
  //  - kNotApplicable: the file has no LSM position (e.g. SstFileWriter /
  //    external files, or other misc uses).
  enum class Applicability : uint8_t { kUnknown, kApplicable, kNotApplicable };
  Applicability applicability = Applicability::kUnknown;

  // Meaningful only when applicability == kApplicable. kUnknownLevel (-1) when
  // the level itself is unknown (e.g. files created by the DB repairer).
  int level_at_creation = kUnknownLevel;

  // Meaningful only when applicability == kApplicable.
  bool is_bottommost = false;

  static LsmInfoAtCreation Applicable(int level_at_creation,
                                      bool is_bottommost);
  static LsmInfoAtCreation NotApplicable();

  // Serialize to / deserialize from the packed
  // TableProperties::lsm_info_at_creation representation. DecodeFrom ignores
  // unrecognized (reserved) bits so the encoding can be extended in the future.
  uint64_t Encode() const;
  static LsmInfoAtCreation DecodeFrom(uint64_t encoded);

  bool operator==(const LsmInfoAtCreation& other) const {
    return applicability == other.applicability &&
           level_at_creation == other.level_at_creation &&
           is_bottommost == other.is_bottommost;
  }
};

// TableProperties contains a bunch of read-only properties of its associated
// table.
struct TableProperties {
 public:
  // the file number at creation time, or 0 for unknown. When known,
  // combining with db_session_id must uniquely identify an SST file.
  uint64_t orig_file_number = 0;
  // the total size of all data blocks.
  uint64_t data_size = 0;
  // the total uncompressed size of all data blocks (since RocksDB 10.7)
  uint64_t uncompressed_data_size = 0;
  // the size of index block.
  uint64_t index_size = 0;
  // Total number of index partitions if kTwoLevelIndexSearch is used
  uint64_t index_partitions = 0;
  // Size of the top-level index if kTwoLevelIndexSearch is used
  uint64_t top_level_index_size = 0;
  // Whether the index key is user key. Otherwise it includes 8 byte of sequence
  // number added by internal key format.
  uint64_t index_key_is_user_key = 0;
  // Whether delta encoding is used to encode the index values.
  uint64_t index_value_is_delta_encoded = 0;
  // Whether the UDI is the primary index for reads. The standard index is
  // fully populated alongside the UDI except in kCustomOnly mode, where it is
  // a minimal stub.
  uint64_t udi_is_primary_index = 0;
  // Whether the standard index is only a footer-satisfying stub and cannot
  // serve reads. This is set for kCustomOnly SSTs.
  uint64_t standard_index_is_stub = 0;
  // the size of filter block.
  uint64_t filter_size = 0;
  // total raw (uncompressed, undelineated) key size
  uint64_t raw_key_size = 0;
  // total raw (uncompressed, undelineated) value size
  uint64_t raw_value_size = 0;
  // the number of blocks in this table
  uint64_t num_data_blocks = 0;
  // Number of data blocks stored uncompressed because compression was
  // attempted but the compressed output exceeded the ratio limit set by
  // CompressionOptions::max_compressed_bytes_per_kb.
  uint64_t num_data_blocks_compression_rejected = 0;
  // Number of data blocks stored uncompressed because compression was
  // never attempted (e.g., kNoCompression, no compressor available).
  uint64_t num_data_blocks_compression_bypassed = 0;
  // the number of uniform blocks in this table
  uint64_t num_uniform_blocks = 0;
  // the number of entries in this table
  uint64_t num_entries = 0;
  // the number of unique entries (keys or prefixes) added to filters
  uint64_t num_filter_entries = 0;
  // the number of deletions in the table
  uint64_t num_deletions = 0;
  // the number of merge operands in the table
  uint64_t num_merge_operands = 0;
  // the number of range deletions in this table
  uint64_t num_range_deletions = 0;
  // format version, reserved for backward compatibility
  uint64_t format_version = 0;
  // If 0, key is variable length. Otherwise number of bytes for each key.
  uint64_t fixed_key_len = 0;
  // ID of column family for this SST file, corresponding to the CF identified
  // by column_family_name.
  uint64_t column_family_id = ROCKSDB_NAMESPACE::
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;

  // Oldest ancestor time. 0 means unknown.
  //
  // For flush output file, oldest ancestor time is the oldest key time in the
  // file.  If the oldest key time is not available, flush time is used.
  //
  // For compaction output file, oldest ancestor time is the oldest
  // among all the oldest key time of its input files, since the file could be
  // the compaction output from other SST files, which could in turn be outputs
  // for compact older SST files. If that's not available, creation time of this
  // compaction output file is used.
  union {
    uint64_t oldest_ancestor_time = 0;
    // DEPRECATED: old name for oldest_ancestor_time. Will be removed in a
    // future release.
    uint64_t creation_time;
  };

  // Timestamp of the earliest key. 0 means unknown.
  uint64_t oldest_key_time = 0;
  // Timestamp of the newest key. 0 means unknown.
  uint64_t newest_key_time = 0;
  // When the SST file was built (for block-based tables, when the builder
  // finished, which is no earlier than when any entry was added to it). 0
  // means unknown.
  uint64_t file_creation_time = 0;
  // Estimated size of data blocks if compressed using a relatively slower
  // compression algorithm (see `ColumnFamilyOptions::sample_for_compression`).
  // 0 means unknown.
  uint64_t slow_compression_estimated_data_size = 0;
  // Estimated size of data blocks if compressed using a relatively faster
  // compression algorithm (see `ColumnFamilyOptions::sample_for_compression`).
  // 0 means unknown.
  uint64_t fast_compression_estimated_data_size = 0;
  // Offset where the "tail" part of SST file starts
  // "Tail" refers to all blocks after data blocks till the end of the SST file
  uint64_t tail_start_offset = 0;

  // Value of the `AdvancedColumnFamilyOptions.persist_user_defined_timestamps`
  // when the file is created. Default to be true, only when this flag is false,
  // it's explicitly written to meta properties block.
  uint64_t user_defined_timestamps_persisted = 1;

  // The largest sequence number of keys in this file.
  // UINT64_MAX means unknown.
  // Only written to properties block if known (should be known unless the
  // table is empty).
  uint64_t key_largest_seqno = UINT64_MAX;

  bool HasKeyLargestSeqno() const { return key_largest_seqno != UINT64_MAX; }

  // The smallest sequence number of keys in this file.
  // UINT64_MAX means unknown.
  // Only written to properties block if known (should be known unless the
  // table is empty).
  uint64_t key_smallest_seqno = UINT64_MAX;

  bool HasKeySmallestSeqno() const { return key_smallest_seqno != UINT64_MAX; }

  // Block restart intervals used when building this SST file.
  // 0 means unknown (for backwards compatibility with older SST files).
  uint64_t data_block_restart_interval = 0;
  uint64_t index_block_restart_interval = 0;

  // Whether the SST file uses separated key/value storage in data blocks (0 =
  // false). The block footer stores the real source of truth of whether the
  // block has separated key values, but this table property is useful for
  // debugging/validation purposes. Consider removing this if we ever decide to
  // mix separation strategies for a sst.
  uint64_t separate_key_value_in_data_block = 0;

  // Packed, informational LSM metadata about how/where this file was created
  // (level and bottommost), for debugging and analysis (e.g. tiering,
  // write-amp). Decode/encode with LsmInfoAtCreation, e.g.
  // LsmInfoAtCreation::DecodeFrom(lsm_info_at_creation). A value of 0 means
  // "unknown" (nothing recorded, e.g. older files); it is only written to the
  // properties block when not "unknown". Distinguishes "unknown" from "not
  // applicable" (files with no LSM position, e.g. SstFileWriter). See
  // LsmInfoAtCreation.
  uint64_t lsm_info_at_creation = 0;

  // DB identity
  // db_id is an identifier generated the first time the DB is created
  // If DB identity is unset or unassigned, `db_id` will be an empty string.
  std::string db_id;

  // DB session identity
  // db_session_id is an identifier that gets reset every time the DB is opened
  // If DB session identity is unset or unassigned, `db_session_id` will be an
  // empty string.
  std::string db_session_id;

  // Location of the machine hosting the DB instance
  // db_host_id identifies the location of the host in some form
  // (hostname by default, but can also be any string of the user's choosing).
  // It can potentially change whenever the DB is opened
  std::string db_host_id;

  // Name of the column family with which this SST file is associated.
  // If column family is unknown, `column_family_name` will be an empty string.
  std::string column_family_name;

  // The name of the filter policy used in this table.
  // If no filter policy is used, `filter_policy_name` will be an empty string.
  std::string filter_policy_name;

  // The name of the comparator used in this table.
  std::string comparator_name;

  // The name of the merge operator used in this table.
  // If no merge operator is used, `merge_operator_name` will be "nullptr".
  std::string merge_operator_name;

  // The name of the prefix extractor used in this table
  // If no prefix extractor is used, `prefix_extractor_name` will be "nullptr".
  std::string prefix_extractor_name;

  // The names of the property collectors factories used in this table
  // separated by commas
  // {collector_name[1]},{collector_name[2]},{collector_name[3]} ..
  std::string property_collectors_names;

  // Identifies the compression algorithm or schema used in the file.
  // Specifically:
  // * For format_version < 7, it is one of several names for built-in
  // compression types. Because of how some previous versions of RocksDB
  // behave, this must be set to "ZSTD" if any blocks are compressed
  // with zstd and must NOT be set to "NoCompression" if any blocks are
  // compressed.
  // * For format_version >= 7, the format is
  //   <compatibility_name>;<hex-coded compression types>;<future use>
  // where <compatibility_name> is the CompatibilityName() of the
  // CompressionManager used for the file, or empty if compression was
  // disabled; <hex-coded compression types> represents a sorted set of
  // CompressionType values used in the file other than kNoCompression, each
  // as 2-digit hex, e.g. 04 for LZ$, 07 for ZSTD, etc.
  std::string compression_name;

  // Compression options used to compress the SST files.
  std::string compression_options;

  // Sequence number to time mapping, delta encoded.
  std::string seqno_to_time_mapping;

  // user collected properties
  UserCollectedProperties user_collected_properties;
  UserCollectedProperties readable_properties;

  // Total physical bytes referenced in separate blob files (excluding blobs
  // embedded in the SST): compressed payload plus each record's key and header
  // overhead. nullopt means the count was not recorded, the table format does
  // not track it, or tracking was unavailable; an engaged zero means no such
  // references. Add() sums recorded counts, so an aggregate with unknown inputs
  // can be a partial sum; it stays nullopt if every input is unknown.
  std::optional<uint64_t> referenced_blob_bytes;

  // convert this object to a human readable form
  //   @prop_delim: delimiter for each property.
  std::string ToString(const std::string& prop_delim = "; ",
                       const std::string& kv_delim = "=") const;

  // Aggregate the numerical member variables of the specified
  // TableProperties.
  void Add(const TableProperties& tp);

  // Subset of properties that make sense when added together
  // between tables. Keys match field names in this class instead
  // of using full property names.
  std::map<std::string, uint64_t> GetAggregatablePropertiesAsMap() const;

  // Return the approximated memory usage of this TableProperties object,
  // including memory used by the string properties and UserCollectedProperties
  std::size_t ApproximateMemoryUsage() const;

  // Serialize and deserialize Table Properties
  Status Serialize(const ConfigOptions& opts, std::string* output) const;
  static Status Parse(const ConfigOptions& opts, const std::string& serialized,
                      TableProperties* table_properties);
  bool AreEqual(const ConfigOptions& opts,
                const TableProperties* other_table_properties,
                std::string* mismatch) const;
};

// Parse TableProperties::compression_name into human-readable format.
// Thread-safe, but not purely local: the single-argument overload may consult
// globally registered CompressionManagers via ObjectLibrary using the stored
// compatibility name, and the two-argument overload consults the supplied
// CompressionManager first before falling back to that lookup.
// For format_version >= 7: "<compatibility_name>;<hex_codes>;" -> "ZSTD",
// "LZ4", etc. For older versions (no semicolon): returns as-is (e.g., "ZSTD",
// "Snappy", "kZSTD" are preserved with their original names). This means the
// single-argument overload can now return manager-specific names for custom
// compression types when a compatible CompressionManager is registered
// globally; otherwise, reserved/custom values fall back to generic names such
// as "Reserved7F" or "Custom8A". Returns "NoCompression" for an empty input,
// an empty hex field such as "BuiltinV2;;", or when all parsed entries are
// filtered out as NoCompression/DisableOption. Returns "Unknown" for malformed
// format_version >= 7 metadata, including a missing second semicolon,
// odd-length hex payload, or invalid hex characters. If multiple compression
// types are present, returns a comma-separated list in input order.
std::string ParseCompressionNameForDisplay(const std::string& compression_name);
std::string ParseCompressionNameForDisplay(
    const std::string& compression_name,
    std::shared_ptr<CompressionManager> compression_manager);

// Extra properties
// Below is a list of non-basic properties that are collected by database
// itself. Especially some properties regarding to the internal keys (which
// is unknown to `table`).
//
// DEPRECATED: these properties now belong as TableProperties members. Please
// use TableProperties::num_deletions and TableProperties::num_merge_operands,
// respectively.
uint64_t GetDeletedKeys(const UserCollectedProperties& props);
uint64_t GetMergeOperands(const UserCollectedProperties& props,
                          bool* property_present);

}  // namespace ROCKSDB_NAMESPACE
