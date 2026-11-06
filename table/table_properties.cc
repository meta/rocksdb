//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/table_properties.h"

#include "db/seqno_to_time_mapping.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/unique_id.h"
#include "rocksdb/utilities/options_type.h"
#include "table/table_properties_internal.h"
#include "table/unique_id_impl.h"
#include "util/compression.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

const uint32_t TablePropertiesCollectorFactory::Context::kUnknownColumnFamily =
    std::numeric_limits<int32_t>::max();
// Out-of-line definitions for these in-class-initialized constants, needed
// because they are ODR-used (e.g. bound to a const reference).
const int TablePropertiesCollectorFactory::Context::kUnknownLevelAtCreation;
const int TablePropertiesCollectorFactory::Context::kUnknownNumLevels;

namespace {
void AppendProperty(std::string& props, const std::string& key,
                    const std::string& value, const std::string& prop_delim,
                    const std::string& kv_delim) {
  props.append(key);
  props.append(kv_delim);
  props.append(value);
  props.append(prop_delim);
}

template <class TValue>
void AppendProperty(std::string& props, const std::string& key,
                    const TValue& value, const std::string& prop_delim,
                    const std::string& kv_delim) {
  AppendProperty(props, key, std::to_string(value), prop_delim, kv_delim);
}

std::shared_ptr<CompressionManager> ResolveCompressionManagerForDisplay(
    Slice compatibility_name,
    const std::shared_ptr<CompressionManager>& compression_manager) {
  std::shared_ptr<CompressionManager> mgr_to_use;
  if (!ResolveCompressionManagerByCompatibilityName(
           compatibility_name, compression_manager.get(), &mgr_to_use)
           .ok()) {
    mgr_to_use.reset();
  }
  return mgr_to_use;
}

std::string CompressionTypeDisplayName(
    CompressionType compression_type,
    const std::shared_ptr<CompressionManager>& compression_manager) {
  if (compression_manager) {
    std::string name =
        compression_manager->CompressionTypeToString(compression_type);
    if (!name.empty()) {
      return name;
    }
  }
  return CompressionTypeToString(compression_type);
}

// lsm_info_at_creation bit layout (see LsmInfoAtCreation / TableProperties::
// lsm_info_at_creation). LSB first; low bits are kept stable so the encoding
// can be extended by defining currently-reserved higher bits, and readers
// ignore bits they do not recognize.
//   bits [0..7]  level_at_creation natural value (0..254) when applicable;
//                0xff means "applicable but level unknown" (e.g. DB repairer)
//   bits [8..9]  0 = unknown, 1 = applicable & not bottommost,
//                2 = applicable & bottommost, 3 = not applicable
//   bits [10..]  reserved for future informational fields
constexpr int kLsmLevelBits = 8;
constexpr uint64_t kLsmLevelMask = (uint64_t{1} << kLsmLevelBits) - 1;
// Sentinel level value (0xff) meaning "applicable but level unknown".
constexpr uint64_t kLsmLevelUnknown = kLsmLevelMask;
constexpr int kLsmStateShift = kLsmLevelBits;
constexpr uint64_t kLsmStateMask = uint64_t{3} << kLsmStateShift;
// Values for the 2-bit state field (bits [8..9]).
constexpr uint64_t kLsmStateUnknown = 0;
constexpr uint64_t kLsmStateNotBottommost = 1;
constexpr uint64_t kLsmStateBottommost = 2;
constexpr uint64_t kLsmStateNotApplicable = 3;
}  // namespace

LsmInfoAtCreation LsmInfoAtCreation::Applicable(int level_at_creation,
                                                bool is_bottommost) {
  LsmInfoAtCreation info;
  info.applicability = Applicability::kApplicable;
  info.level_at_creation = level_at_creation;
  info.is_bottommost = is_bottommost;
  return info;
}

LsmInfoAtCreation LsmInfoAtCreation::NotApplicable() {
  LsmInfoAtCreation info;
  info.applicability = Applicability::kNotApplicable;
  return info;
}

uint64_t LsmInfoAtCreation::Encode() const {
  switch (applicability) {
    case Applicability::kUnknown:
      return kLsmStateUnknown << kLsmStateShift;
    case Applicability::kNotApplicable:
      return kLsmStateNotApplicable << kLsmStateShift;
    case Applicability::kApplicable: {
      // Store the level as its natural value; kLsmLevelUnknown (0xff) encodes
      // an unknown or out-of-range level.
      uint64_t level_field =
          (level_at_creation >= 0 &&
           static_cast<uint64_t>(level_at_creation) < kLsmLevelUnknown)
              ? static_cast<uint64_t>(level_at_creation)
              : kLsmLevelUnknown;
      uint64_t state =
          is_bottommost ? kLsmStateBottommost : kLsmStateNotBottommost;
      return level_field | (state << kLsmStateShift);
    }
  }
  return 0;
}

LsmInfoAtCreation LsmInfoAtCreation::DecodeFrom(uint64_t encoded) {
  uint64_t state = (encoded & kLsmStateMask) >> kLsmStateShift;
  switch (state) {
    case kLsmStateNotBottommost:
    case kLsmStateBottommost: {
      uint64_t level_field = encoded & kLsmLevelMask;
      int level = level_field == kLsmLevelUnknown
                      ? kUnknownLevel
                      : static_cast<int>(level_field);
      return Applicable(level, state == kLsmStateBottommost);
    }
    case kLsmStateNotApplicable:
      return NotApplicable();
    default:
      return LsmInfoAtCreation{};
  }
}

std::string TableProperties::ToString(const std::string& prop_delim,
                                      const std::string& kv_delim) const {
  std::string result;
  result.reserve(1024);

  // Basic Info
  AppendProperty(result, "# data blocks", num_data_blocks, prop_delim,
                 kv_delim);
  AppendProperty(result, "# data blocks compression rejected",
                 num_data_blocks_compression_rejected, prop_delim, kv_delim);
  AppendProperty(result, "# data blocks compression bypassed",
                 num_data_blocks_compression_bypassed, prop_delim, kv_delim);
  AppendProperty(result, "# uniform blocks", num_uniform_blocks, prop_delim,
                 kv_delim);
  AppendProperty(result, "# entries", num_entries, prop_delim, kv_delim);
  AppendProperty(result, "# deletions", num_deletions, prop_delim, kv_delim);
  AppendProperty(result, "# merge operands", num_merge_operands, prop_delim,
                 kv_delim);
  AppendProperty(result, "# range deletions", num_range_deletions, prop_delim,
                 kv_delim);

  AppendProperty(result, "raw key size", raw_key_size, prop_delim, kv_delim);
  AppendProperty(result, "raw average key size",
                 num_entries != 0 ? 1.0 * raw_key_size / num_entries : 0.0,
                 prop_delim, kv_delim);
  AppendProperty(result, "raw value size", raw_value_size, prop_delim,
                 kv_delim);
  AppendProperty(result, "raw average value size",
                 num_entries != 0 ? 1.0 * raw_value_size / num_entries : 0.0,
                 prop_delim, kv_delim);

  AppendProperty(result, "data block size", data_size, prop_delim, kv_delim);
  AppendProperty(result, "data uncompressed size", uncompressed_data_size,
                 prop_delim, kv_delim);
  char index_block_size_str[80];
  snprintf(index_block_size_str, sizeof(index_block_size_str),
           "index block size (user-key? %d, delta-value? %d)",
           static_cast<int>(index_key_is_user_key),
           static_cast<int>(index_value_is_delta_encoded));
  AppendProperty(result, index_block_size_str, index_size, prop_delim,
                 kv_delim);
  if (index_partitions != 0) {
    AppendProperty(result, "# index partitions", index_partitions, prop_delim,
                   kv_delim);
    AppendProperty(result, "top-level index size", top_level_index_size,
                   prop_delim, kv_delim);
  }
  AppendProperty(result, "filter block size", filter_size, prop_delim,
                 kv_delim);
  AppendProperty(result, "# entries for filter", num_filter_entries, prop_delim,
                 kv_delim);
  AppendProperty(result, "(estimated) table size",
                 data_size + index_size + filter_size, prop_delim, kv_delim);
  AppendProperty(result, "standard index is stub",
                 static_cast<int>(standard_index_is_stub), prop_delim,
                 kv_delim);

  AppendProperty(
      result, "filter policy name",
      filter_policy_name.empty() ? std::string("N/A") : filter_policy_name,
      prop_delim, kv_delim);

  AppendProperty(result, "prefix extractor name",
                 prefix_extractor_name.empty() ? std::string("N/A")
                                               : prefix_extractor_name,
                 prop_delim, kv_delim);

  AppendProperty(result, "column family ID",
                 column_family_id ==
                         ROCKSDB_NAMESPACE::TablePropertiesCollectorFactory::
                             Context::kUnknownColumnFamily
                     ? std::string("N/A")
                     : std::to_string(column_family_id),
                 prop_delim, kv_delim);
  AppendProperty(
      result, "column family name",
      column_family_name.empty() ? std::string("N/A") : column_family_name,
      prop_delim, kv_delim);

  AppendProperty(result, "comparator name",
                 comparator_name.empty() ? std::string("N/A") : comparator_name,
                 prop_delim, kv_delim);
  AppendProperty(result, "user defined timestamps persisted",
                 user_defined_timestamps_persisted ? std::string("true")
                                                   : std::string("false"),
                 prop_delim, kv_delim);
  AppendProperty(result, "largest sequence number in file", key_largest_seqno,
                 prop_delim, kv_delim);
  AppendProperty(result, "smallest sequence number in file", key_smallest_seqno,
                 prop_delim, kv_delim);

  AppendProperty(
      result, "merge operator name",
      merge_operator_name.empty() ? std::string("N/A") : merge_operator_name,
      prop_delim, kv_delim);

  AppendProperty(result, "property collectors names",
                 property_collectors_names.empty() ? std::string("N/A")
                                                   : property_collectors_names,
                 prop_delim, kv_delim);

  AppendProperty(
      result, "SST file compression algo",
      compression_name.empty() ? std::string("N/A") : compression_name,
      prop_delim, kv_delim);

  AppendProperty(
      result, "SST file compression options",
      compression_options.empty() ? std::string("N/A") : compression_options,
      prop_delim, kv_delim);

  AppendProperty(result, "creation time", oldest_ancestor_time, prop_delim,
                 kv_delim);

  AppendProperty(result, "time stamp of earliest key", oldest_key_time,
                 prop_delim, kv_delim);
  AppendProperty(result, "time stamp of newest key", newest_key_time,
                 prop_delim, kv_delim);

  AppendProperty(result, "file creation time", file_creation_time, prop_delim,
                 kv_delim);

  {
    LsmInfoAtCreation lsm_info =
        LsmInfoAtCreation::DecodeFrom(lsm_info_at_creation);
    std::string level_str;
    std::string bottommost_str;
    switch (lsm_info.applicability) {
      case LsmInfoAtCreation::Applicability::kUnknown:
        level_str = "unknown";
        bottommost_str = "unknown";
        break;
      case LsmInfoAtCreation::Applicability::kNotApplicable:
        level_str = "N/A";
        bottommost_str = "N/A";
        break;
      case LsmInfoAtCreation::Applicability::kApplicable:
        level_str =
            lsm_info.level_at_creation == LsmInfoAtCreation::kUnknownLevel
                ? std::string("unknown")
                : std::to_string(lsm_info.level_at_creation);
        bottommost_str =
            lsm_info.is_bottommost ? std::string("true") : std::string("false");
        break;
    }
    AppendProperty(result, "level at creation", level_str, prop_delim,
                   kv_delim);
    AppendProperty(result, "bottommost at creation", bottommost_str, prop_delim,
                   kv_delim);
  }

  AppendProperty(result, "slow compression estimated data size",
                 slow_compression_estimated_data_size, prop_delim, kv_delim);
  AppendProperty(result, "fast compression estimated data size",
                 fast_compression_estimated_data_size, prop_delim, kv_delim);

  // DB identity and DB session ID
  AppendProperty(result, "DB identity", db_id, prop_delim, kv_delim);
  AppendProperty(result, "DB session identity", db_session_id, prop_delim,
                 kv_delim);
  AppendProperty(result, "DB host id", db_host_id, prop_delim, kv_delim);
  AppendProperty(result, "original file number", orig_file_number, prop_delim,
                 kv_delim);

  // Unique ID, when available
  std::string id;
  Status s = GetUniqueIdFromTableProperties(*this, &id);
  AppendProperty(result, "unique ID",
                 s.ok() ? UniqueIdToHumanString(id) : "N/A", prop_delim,
                 kv_delim);

  SeqnoToTimeMapping seq_time_mapping;
  s = seq_time_mapping.DecodeFrom(seqno_to_time_mapping);
  AppendProperty(result, "Sequence number to time mapping",
                 s.ok() ? seq_time_mapping.ToHumanString() : "N/A", prop_delim,
                 kv_delim);

  return result;
}

void TableProperties::Add(const TableProperties& tp) {
  data_size += tp.data_size;
  uncompressed_data_size += tp.uncompressed_data_size;
  index_size += tp.index_size;
  index_partitions += tp.index_partitions;
  top_level_index_size += tp.top_level_index_size;
  index_key_is_user_key += tp.index_key_is_user_key;
  index_value_is_delta_encoded += tp.index_value_is_delta_encoded;
  standard_index_is_stub += tp.standard_index_is_stub;
  filter_size += tp.filter_size;
  raw_key_size += tp.raw_key_size;
  raw_value_size += tp.raw_value_size;
  num_data_blocks += tp.num_data_blocks;
  num_data_blocks_compression_rejected +=
      tp.num_data_blocks_compression_rejected;
  num_data_blocks_compression_bypassed +=
      tp.num_data_blocks_compression_bypassed;
  num_uniform_blocks += tp.num_uniform_blocks;
  num_entries += tp.num_entries;
  num_filter_entries += tp.num_filter_entries;
  num_deletions += tp.num_deletions;
  num_merge_operands += tp.num_merge_operands;
  num_range_deletions += tp.num_range_deletions;
  slow_compression_estimated_data_size +=
      tp.slow_compression_estimated_data_size;
  fast_compression_estimated_data_size +=
      tp.fast_compression_estimated_data_size;
}

std::map<std::string, uint64_t>
TableProperties::GetAggregatablePropertiesAsMap() const {
  std::map<std::string, uint64_t> rv;
  rv["data_size"] = data_size;
  rv["uncompressed_data_size"] = uncompressed_data_size;
  rv["index_size"] = index_size;
  rv["index_partitions"] = index_partitions;
  rv["top_level_index_size"] = top_level_index_size;
  rv["standard_index_is_stub"] = standard_index_is_stub;
  rv["filter_size"] = filter_size;
  rv["raw_key_size"] = raw_key_size;
  rv["raw_value_size"] = raw_value_size;
  rv["num_data_blocks"] = num_data_blocks;
  rv["num_data_blocks_compression_rejected"] =
      num_data_blocks_compression_rejected;
  rv["num_data_blocks_compression_bypassed"] =
      num_data_blocks_compression_bypassed;
  rv["num_uniform_blocks"] = num_uniform_blocks;
  rv["num_entries"] = num_entries;
  rv["num_filter_entries"] = num_filter_entries;
  rv["num_deletions"] = num_deletions;
  rv["num_merge_operands"] = num_merge_operands;
  rv["num_range_deletions"] = num_range_deletions;
  rv["slow_compression_estimated_data_size"] =
      slow_compression_estimated_data_size;
  rv["fast_compression_estimated_data_size"] =
      fast_compression_estimated_data_size;
  return rv;
}

// WARNING: manual update to this function is needed
// whenever a new string property is added to TableProperties
// to reduce approximation error.
//
// TODO: eliminate the need of manually updating this function
// for new string properties
std::size_t TableProperties::ApproximateMemoryUsage() const {
  std::size_t usage = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size((void*)this);
#else
  usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE

  std::size_t string_props_mem_usage =
      db_id.size() + db_session_id.size() + db_host_id.size() +
      column_family_name.size() + filter_policy_name.size() +
      comparator_name.size() + merge_operator_name.size() +
      prefix_extractor_name.size() + property_collectors_names.size() +
      compression_name.size() + compression_options.size();
  usage += string_props_mem_usage;

  for (auto iter = user_collected_properties.begin();
       iter != user_collected_properties.end(); ++iter) {
    usage += (iter->first.size() + iter->second.size());
  }

  return usage;
}

const std::string TablePropertiesNames::kDbId = "rocksdb.creating.db.identity";
const std::string TablePropertiesNames::kDbSessionId =
    "rocksdb.creating.session.identity";
const std::string TablePropertiesNames::kDbHostId =
    "rocksdb.creating.host.identity";
const std::string TablePropertiesNames::kOriginalFileNumber =
    "rocksdb.original.file.number";
const std::string TablePropertiesNames::kDataSize = "rocksdb.data.size";
const std::string TablePropertiesNames::kIndexSize = "rocksdb.index.size";
const std::string TablePropertiesNames::kIndexPartitions =
    "rocksdb.index.partitions";
const std::string TablePropertiesNames::kTopLevelIndexSize =
    "rocksdb.top-level.index.size";
const std::string TablePropertiesNames::kIndexKeyIsUserKey =
    "rocksdb.index.key.is.user.key";
const std::string TablePropertiesNames::kIndexValueIsDeltaEncoded =
    "rocksdb.index.value.is.delta.encoded";
const std::string TablePropertiesNames::kUDIIsPrimaryIndex =
    "rocksdb.udi.is.primary.index";
// NOLINTNEXTLINE(cert-err58-cpp)
const std::string TablePropertiesNames::kStandardIndexIsStub =
    "rocksdb.standard.index.is.stub";
const std::string TablePropertiesNames::kFilterSize = "rocksdb.filter.size";
const std::string TablePropertiesNames::kRawKeySize = "rocksdb.raw.key.size";
const std::string TablePropertiesNames::kRawValueSize =
    "rocksdb.raw.value.size";
const std::string TablePropertiesNames::kNumDataBlocks =
    "rocksdb.num.data.blocks";
const std::string TablePropertiesNames::kNumDataBlocksCompressionRejected =
    "rocksdb.num.data.blocks.compression.rejected";
const std::string TablePropertiesNames::kNumDataBlocksCompressionBypassed =
    "rocksdb.num.data.blocks.compression.bypassed";
const std::string TablePropertiesNames::kNumUniformBlocks =
    "rocksdb.num.uniform.blocks";
const std::string TablePropertiesNames::kNumEntries = "rocksdb.num.entries";
const std::string TablePropertiesNames::kNumFilterEntries =
    "rocksdb.num.filter_entries";
const std::string TablePropertiesNames::kDeletedKeys = "rocksdb.deleted.keys";
const std::string TablePropertiesNames::kMergeOperands =
    "rocksdb.merge.operands";
const std::string TablePropertiesNames::kNumRangeDeletions =
    "rocksdb.num.range-deletions";
const std::string TablePropertiesNames::kFilterPolicy = "rocksdb.filter.policy";
const std::string TablePropertiesNames::kFormatVersion =
    "rocksdb.format.version";
const std::string TablePropertiesNames::kFixedKeyLen =
    "rocksdb.fixed.key.length";
const std::string TablePropertiesNames::kColumnFamilyId =
    "rocksdb.column.family.id";
const std::string TablePropertiesNames::kColumnFamilyName =
    "rocksdb.column.family.name";
const std::string TablePropertiesNames::kComparator = "rocksdb.comparator";
const std::string TablePropertiesNames::kMergeOperator =
    "rocksdb.merge.operator";
const std::string TablePropertiesNames::kPrefixExtractorName =
    "rocksdb.prefix.extractor.name";
const std::string TablePropertiesNames::kPropertyCollectors =
    "rocksdb.property.collectors";
const std::string TablePropertiesNames::kCompression = "rocksdb.compression";
const std::string TablePropertiesNames::kCompressionOptions =
    "rocksdb.compression_options";
const std::string TablePropertiesNames::kOldestAncestorTime =
    "rocksdb.creation.time";
const std::string& TablePropertiesNames::kCreationTime =
    TablePropertiesNames::kOldestAncestorTime;
const std::string TablePropertiesNames::kOldestKeyTime =
    "rocksdb.oldest.key.time";
const std::string TablePropertiesNames::kNewestKeyTime =
    "rocksdb.newest.key.time";
const std::string TablePropertiesNames::kFileCreationTime =
    "rocksdb.file.creation.time";
const std::string TablePropertiesNames::kSlowCompressionEstimatedDataSize =
    "rocksdb.sample_for_compression.slow.data.size";
const std::string TablePropertiesNames::kFastCompressionEstimatedDataSize =
    "rocksdb.sample_for_compression.fast.data.size";
const std::string TablePropertiesNames::kSequenceNumberTimeMapping =
    "rocksdb.seqno.time.map";
const std::string TablePropertiesNames::kTailStartOffset =
    "rocksdb.tail.start.offset";
const std::string TablePropertiesNames::kUserDefinedTimestampsPersisted =
    "rocksdb.user.defined.timestamps.persisted";
const std::string TablePropertiesNames::kKeyLargestSeqno =
    "rocksdb.key.largest.seqno";
const std::string TablePropertiesNames::kKeySmallestSeqno =
    "rocksdb.key.smallest.seqno";
const std::string TablePropertiesNames::kDataBlockRestartInterval =
    "rocksdb.data.block.restart.interval";
const std::string TablePropertiesNames::kIndexBlockRestartInterval =
    "rocksdb.index.block.restart.interval";
const std::string TablePropertiesNames::kSeparateKeyValueInDataBlock =
    "rocksdb.separate.key.value.in.data.block";
const std::string TablePropertiesNames::kUncompressedDataSize =
    "rocksdb.uncompressed.data.size";
const std::string TablePropertiesNames::kLsmInfoAtCreation =
    "rocksdb.lsm.info.at.creation";

static std::unordered_map<std::string, OptionTypeInfo>
    table_properties_type_info = {
        {"orig_file_number",
         {offsetof(struct TableProperties, orig_file_number),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"data_size",
         {offsetof(struct TableProperties, data_size), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"uncompressed_data_size",
         {offsetof(struct TableProperties, uncompressed_data_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"index_size",
         {offsetof(struct TableProperties, index_size), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"index_partitions",
         {offsetof(struct TableProperties, index_partitions),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"top_level_index_size",
         {offsetof(struct TableProperties, top_level_index_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"index_key_is_user_key",
         {offsetof(struct TableProperties, index_key_is_user_key),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"index_value_is_delta_encoded",
         {offsetof(struct TableProperties, index_value_is_delta_encoded),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"udi_is_primary_index",
         {offsetof(struct TableProperties, udi_is_primary_index),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"standard_index_is_stub",
         {offsetof(struct TableProperties, standard_index_is_stub),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"filter_size",
         {offsetof(struct TableProperties, filter_size), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"raw_key_size",
         {offsetof(struct TableProperties, raw_key_size), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"raw_value_size",
         {offsetof(struct TableProperties, raw_value_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_data_blocks",
         {offsetof(struct TableProperties, num_data_blocks),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_data_blocks_compression_rejected",
         {offsetof(struct TableProperties,
                   num_data_blocks_compression_rejected),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_data_blocks_compression_bypassed",
         {offsetof(struct TableProperties,
                   num_data_blocks_compression_bypassed),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_uniform_blocks",
         {offsetof(struct TableProperties, num_uniform_blocks),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_entries",
         {offsetof(struct TableProperties, num_entries), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"num_filter_entries",
         {offsetof(struct TableProperties, num_filter_entries),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_deletions",
         {offsetof(struct TableProperties, num_deletions), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"num_merge_operands",
         {offsetof(struct TableProperties, num_merge_operands),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_range_deletions",
         {offsetof(struct TableProperties, num_range_deletions),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"format_version",
         {offsetof(struct TableProperties, format_version),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"fixed_key_len",
         {offsetof(struct TableProperties, fixed_key_len), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"column_family_id",
         {offsetof(struct TableProperties, column_family_id),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        // Key retains the old field name for serialization compatibility
        {"creation_time",
         {offsetof(struct TableProperties, oldest_ancestor_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"oldest_key_time",
         {offsetof(struct TableProperties, oldest_key_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"newest_key_time",
         {offsetof(struct TableProperties, newest_key_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_creation_time",
         {offsetof(struct TableProperties, file_creation_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"slow_compression_estimated_data_size",
         {offsetof(struct TableProperties,
                   slow_compression_estimated_data_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"fast_compression_estimated_data_size",
         {offsetof(struct TableProperties,
                   fast_compression_estimated_data_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"tail_start_offset",
         {offsetof(struct TableProperties, tail_start_offset),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"user_defined_timestamps_persisted",
         {offsetof(struct TableProperties, user_defined_timestamps_persisted),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"key_largest_seqno",
         {offsetof(struct TableProperties, key_largest_seqno),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"key_smallest_seqno",
         {offsetof(struct TableProperties, key_smallest_seqno),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"data_block_restart_interval",
         {offsetof(struct TableProperties, data_block_restart_interval),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"index_block_restart_interval",
         {offsetof(struct TableProperties, index_block_restart_interval),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"separate_key_value_in_data_block",
         {offsetof(struct TableProperties, separate_key_value_in_data_block),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"lsm_info_at_creation",
         {offsetof(struct TableProperties, lsm_info_at_creation),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"db_id",
         {offsetof(struct TableProperties, db_id), OptionType::kEncodedString}},
        {"db_session_id",
         {offsetof(struct TableProperties, db_session_id),
          OptionType::kEncodedString}},
        {"db_host_id",
         {offsetof(struct TableProperties, db_host_id),
          OptionType::kEncodedString}},
        {"column_family_name",
         {offsetof(struct TableProperties, column_family_name),
          OptionType::kEncodedString}},
        {"filter_policy_name",
         {offsetof(struct TableProperties, filter_policy_name),
          OptionType::kEncodedString}},
        {"comparator_name",
         {offsetof(struct TableProperties, comparator_name),
          OptionType::kEncodedString}},
        {"merge_operator_name",
         {offsetof(struct TableProperties, merge_operator_name),
          OptionType::kEncodedString}},
        {"prefix_extractor_name",
         {offsetof(struct TableProperties, prefix_extractor_name),
          OptionType::kEncodedString}},
        {"property_collectors_names",
         {offsetof(struct TableProperties, property_collectors_names),
          OptionType::kEncodedString}},
        {"compression_name",
         {offsetof(struct TableProperties, compression_name),
          OptionType::kEncodedString}},
        {"compression_options",
         {offsetof(struct TableProperties, compression_options),
          OptionType::kEncodedString}},
        {"seqno_to_time_mapping",
         {offsetof(struct TableProperties, seqno_to_time_mapping),
          OptionType::kEncodedString}},
        {"user_collected_properties",
         OptionTypeInfo::StringMap(
             offsetof(struct TableProperties, user_collected_properties),
             OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
        {"readable_properties",
         OptionTypeInfo::StringMap(
             offsetof(struct TableProperties, readable_properties),
             OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
};

Status TableProperties::Serialize(const ConfigOptions& opts,
                                  std::string* output) const {
  return OptionTypeInfo::SerializeType(opts, table_properties_type_info, this,
                                       output);
}
Status TableProperties::Parse(const ConfigOptions& opts,
                              const std::string& serialized,
                              TableProperties* table_properties) {
  return OptionTypeInfo::ParseType(opts, serialized, table_properties_type_info,
                                   table_properties);
}
bool TableProperties::AreEqual(const ConfigOptions& opts,
                               const TableProperties* other_table_properties,
                               std::string* mismatch) const {
  return OptionTypeInfo::TypesAreEqual(opts, table_properties_type_info, this,
                                       other_table_properties, mismatch);
}

#ifndef NDEBUG
// WARNING: TEST_SetRandomTableProperties assumes the following layout of
// TableProperties
//
// struct TableProperties {
//    int64_t orig_file_number = 0;
//    ...
//    ... int64_t properties only
//    ...
//    std::string db_id;
//    ...
//    ... std::string properties only
//    ...
//    std::string compression_options;
//    UserCollectedProperties user_collected_properties;
//    ...
//    ... Other extra properties: non-int64_t/non-std::string properties only
//    ...
// }
void TEST_SetRandomTableProperties(TableProperties* props) {
  Random* r = Random::GetTLSInstance();
  uint64_t* pu = &props->orig_file_number;
  assert(static_cast<void*>(pu) == static_cast<void*>(props));
  std::string* ps = &props->db_id;
  const uint64_t* const pu_end = reinterpret_cast<const uint64_t*>(ps);
  // Use the last string property's address instead of
  // the first extra property (e.g `user_collected_properties`)'s address
  // in the for-loop to avoid advancing pointer to pointing to
  // potential non-zero padding bytes between these two addresses due to
  // user_collected_properties's alignment requirement
  const std::string* const ps_end_inclusive = &props->compression_options;

  for (; pu < pu_end; ++pu) {
    *pu = r->Next64();
  }
  assert(static_cast<void*>(pu) == static_cast<void*>(ps));
  for (; ps <= ps_end_inclusive; ++ps) {
    *ps = r->RandomBinaryString(13);
  }
}
#endif

std::string ParseCompressionNameForDisplay(
    const std::string& compression_name) {
  // The single-argument overload intentionally consults globally registered
  // CompressionManagers, keyed by the encoded compatibility name, so custom
  // managers can contribute display names without an explicit manager handle.
  return ParseCompressionNameForDisplay(compression_name, nullptr);
}

std::string ParseCompressionNameForDisplay(
    const std::string& compression_name,
    std::shared_ptr<CompressionManager> compression_manager) {
  // Empty = no compression
  if (compression_name.empty()) {
    return "NoCompression";
  }

  // Check for format_version 7 format (contains ';')
  size_t first_semicolon = compression_name.find(';');
  if (first_semicolon == std::string::npos) {
    // Old format - return as-is
    return compression_name;
  }

  // New format: "<compatibility_name>;<hex_codes>;"
  size_t second_semicolon = compression_name.find(';', first_semicolon + 1);
  if (second_semicolon == std::string::npos) {
    // Malformed - missing second field
    return "Unknown";
  }

  Slice compatibility_name(compression_name.data(), first_semicolon);
  auto mgr_to_use = ResolveCompressionManagerForDisplay(compatibility_name,
                                                        compression_manager);

  // Extract hex codes
  std::string hex_codes = compression_name.substr(
      first_semicolon + 1, second_semicolon - first_semicolon - 1);

  // Validate hex string length (must be even)
  if (hex_codes.size() % 2 != 0) {
    return "Unknown";
  }

  // Parse each 2-char hex code to CompressionType.
  // Note: This intentionally mirrors GetDecompressor()'s decoding shape but
  // differs in error semantics. GetDecompressor() treats kNoCompression
  // (0x00) and values >= kDisableCompressionOption (0xFF) as corruption. For
  // display purposes, we silently filter these out and return "NoCompression"
  // if no valid types remain.
  std::vector<std::string> types;
  for (size_t i = 0; i < hex_codes.size(); i += 2) {
    const char* ptr = hex_codes.data() + i;
    uint64_t val = 0;
    if (!ParseBaseChars<16>(&ptr, 2, &val)) {
      return "Unknown";
    }
    auto ct = static_cast<CompressionType>(val);
    if (ct != kNoCompression && ct != kDisableCompressionOption) {
      types.push_back(CompressionTypeDisplayName(ct, mgr_to_use));
    }
  }

  if (types.empty()) {
    return "NoCompression";
  } else if (types.size() == 1) {
    return types[0];
  } else {
    // Multiple types - join with commas
    std::string result = types[0];
    for (size_t i = 1; i < types.size(); ++i) {
      result += "," + types[i];
    }
    return result;
  }
}

}  // namespace ROCKSDB_NAMESPACE
