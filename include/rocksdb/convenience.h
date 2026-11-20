// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/compression_type.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {
class Env;
class Logger;
class ObjectRegistry;

struct ColumnFamilyOptions;
struct DBOptions;
struct Options;

// ConfigOptions containing the parameters/controls for
// comparing objects and converting to/from strings.
// These settings control how the methods
// treat errors (e.g. ignore_unknown_objects), the format
// of the serialization (e.g. delimiter), and how to compare
// options (sanity_level).
// NOTE: members of this struct make it potentially problematic for
// static storage duration ("static initialization order fiasco")
struct ConfigOptions {
  // Constructs a new ConfigOptions with a new object registry.
  // This method should only be used when a DBOptions is not available,
  // else registry settings may be lost
  ConfigOptions();

  // Constructs a new ConfigOptions using the settings from
  // the input DBOptions.  Currently constructs a new object registry.
  explicit ConfigOptions(const DBOptions&);

  // This enum defines the RocksDB options sanity level.
  enum SanityLevel : unsigned char {
    kSanityLevelNone = 0x01,  // Performs no sanity check at all.
    // Performs minimum check to ensure the RocksDB instance can be
    // opened without corrupting / mis-interpreting the data.
    kSanityLevelLooselyCompatible = 0x02,
    // Perform exact match sanity check.
    kSanityLevelExactMatch = 0xFF,
  };

  enum Depth {
    kDepthDefault,  // Traverse nested options that are not flagged as "shallow"
    kDepthShallow,  // Do not traverse into any nested options
    kDepthDetailed,  // Traverse nested options, overriding the options shallow
                     // setting
  };

  // When true, any unused options will be ignored and OK will be returned.
  // For options files that appear to be from the current version or earlier,
  // unknown options are considered corruption regardless of this setting.
  bool ignore_unknown_options = false;

  // When true, any unsupported options will be ignored and OK will be returned
  bool ignore_unsupported_options = true;

  // If the strings are escaped (old-style?)
  bool input_strings_escaped = true;

  // Whether or not to invoke PrepareOptions after configure is called.
  bool invoke_prepare_options = true;

  // Options can be marked as Mutable (OptionTypeInfo::IsMutable()) or not.
  // When "mutable_options_only=false", all options are evaluated.
  // When "mutable_options_only="true", any option not marked as Mutable is
  // either ignored (in the case of string/equals methods) or results in an
  // error (in the case of Configure).
  bool mutable_options_only = false;

  // The separator between options when converting to a string
  std::string delimiter = ";";

  // Controls how to traverse options during print/match stages
  Depth depth = Depth::kDepthDefault;

  // Controls how options are serialized
  // Controls how pedantic the comparison must be for equivalency
  SanityLevel sanity_level = SanityLevel::kSanityLevelExactMatch;
  // `file_readahead_size` is used for readahead for the option file.
  size_t file_readahead_size = 512 * 1024;

  // The environment to use for this option
  Env* env = Env::Default();

  // The object registry to use for this options
  std::shared_ptr<ObjectRegistry> registry;

  bool IsShallow() const { return depth == Depth::kDepthShallow; }
  bool IsDetailed() const { return depth == Depth::kDepthDetailed; }

  bool IsCheckDisabled() const {
    return sanity_level == SanityLevel::kSanityLevelNone;
  }

  bool IsCheckEnabled(SanityLevel level) const {
    return (level > SanityLevel::kSanityLevelNone && level <= sanity_level);
  }
};

// The following set of functions provide a way to construct RocksDB Options
// from a string or a string-to-string map.  Here is the general rule of
// setting option values from strings by type.  Some RocksDB types are also
// supported in these APIs.  Please refer to the comment of the function itself
// to find more information about how to config those RocksDB types.
//
// * Strings:
//   Strings will be used as values directly without any truncating or
//   trimming.
//
// * Booleans:
//   - "true" or "1" => true
//   - "false" or "0" => false.
//   [Example]:
//   - {"optimize_filters_for_hits", "1"} in GetColumnFamilyOptionsFromMap, or
//   - "optimize_filters_for_hits=true" in GetColumnFamilyOptionsFromString.
//
// * Integers:
//   Integers are converted directly from string, in addition to the following
//   units that we support:
//   - 'k' or 'K' => 2^10
//   - 'm' or 'M' => 2^20
//   - 'g' or 'G' => 2^30
//   - 't' or 'T' => 2^40  // only for unsigned int with sufficient bits.
//   [Example]:
//   - {"arena_block_size", "19G"} in GetColumnFamilyOptionsFromMap, or
//   - "arena_block_size=19G" in GetColumnFamilyOptionsFromString.
//
// * Doubles / Floating Points:
//   Doubles / Floating Points are converted directly from string.  Note that
//   currently we do not support units.
//   [Example]:
//   - {"memtable_prefix_bloom_size_ratio", "0.1"} in
//   GetColumnFamilyOptionsFromMap, or
//   - "memtable_prefix_bloom_size_ratio=0.1" in
//   GetColumnFamilyOptionsFromString.
// * Array / Vectors:
//   An array is specified by a list of values, where ':' is used as
//   the delimiter to separate each value.
//   [Example]:
//   - {"compression_per_level", "kNoCompression:kSnappyCompression"}
//     in GetColumnFamilyOptionsFromMap, or
//   - "compression_per_level=kNoCompression:kSnappyCompression" in
//     GetColumnFamilyOptionsFromMapString
// * Enums:
//   The valid values of each enum are identical to the names of its constants.
//   [Example]:
//   - CompressionType: valid values are "kNoCompression",
//     "kSnappyCompression", "kZlibCompression", "kBZip2Compression", ...
//   - CompactionStyle: valid values are "kCompactionStyleLevel",
//     "kCompactionStyleUniversal", "kCompactionStyleFIFO", and
//     "kCompactionStyleNone".
//

// Take a ConfigOptions `config_options` and a ColumnFamilyOptions
// "base_options" as the default option in addition to a map "opts_map" of
// option name to option value to construct the new ColumnFamilyOptions
// "new_options".
//
// Below are the instructions of how to config some non-primitive-typed
// options in ColumnFamilyOptions:
//
// * table_factory:
//   table_factory can be configured using our custom nested-option syntax.
//
//   {option_a=value_a; option_b=value_b; option_c=value_c; ... }
//
//   A nested option is enclosed by two curly braces, within which there are
//   multiple option assignments.  Each assignment is of the form
//   "variable_name=value;".
//
//   Currently we support the following types of TableFactory:
//   - BlockBasedTableFactory:
//     Use name "block_based_table_factory" to initialize table_factory with
//     BlockBasedTableFactory.  Its BlockBasedTableFactoryOptions can be
//     configured using the nested-option syntax.
//     [Example]:
//     * {"block_based_table_factory", "{block_cache=1M;block_size=4k;}"}
//       is equivalent to assigning table_factory with a BlockBasedTableFactory
//       that has 1M LRU block-cache with block size equals to 4k:
//         ColumnFamilyOptions cf_opt;
//         BlockBasedTableOptions blk_opt;
//         blk_opt.block_cache = NewLRUCache(1 * 1024 * 1024);
//         blk_opt.block_size = 4 * 1024;
//         cf_opt.table_factory.reset(NewBlockBasedTableFactory(blk_opt));
//   - PlainTableFactory:
//     Use name "plain_table_factory" to initialize table_factory with
//     PlainTableFactory.  Its PlainTableFactoryOptions can be configured using
//     the nested-option syntax.
//     [Example]:
//     * {"plain_table_factory", "{user_key_len=66;bloom_bits_per_key=20;}"}
//
// * memtable_factory:
//   Use "memtable" to config memtable_factory.  Here are the supported
//   memtable factories:
//   - SkipList:
//     Pass "skip_list:<lookahead>" to config memtable to use SkipList,
//     or simply "skip_list" to use the default SkipList.
//     [Example]:
//     * {"memtable", "skip_list:5"} is equivalent to setting
//       memtable to SkipListFactory(5).
//   - PrefixHash:
//     Pass "prefix_hash:<hash_bucket_count>" to config memtable
//     to use PrefixHash, or simply "prefix_hash" to use the default
//     PrefixHash.
//     [Example]:
//     * {"memtable", "prefix_hash:1000"} is equivalent to setting
//       memtable to NewHashSkipListRepFactory(hash_bucket_count).
//   - HashLinkedList:
//     Pass "hash_linkedlist:<hash_bucket_count>" to config memtable
//     to use HashLinkedList, or simply "hash_linkedlist" to use the default
//     HashLinkedList.
//     [Example]:
//     * {"memtable", "hash_linkedlist:1000"} is equivalent to
//       setting memtable to NewHashLinkListRepFactory(1000).
//   - VectorRepFactory:
//     Pass "vector:<count>" to config memtable to use VectorRepFactory,
//     or simply "vector" to use the default Vector memtable.
//     [Example]:
//     * {"memtable", "vector:1024"} is equivalent to setting memtable
//       to VectorRepFactory(1024).
//
//  * compression_opts:
//    Use "compression_opts" to config compression_opts.  The value format
//    is of the form "<window_bits>:<level>:<strategy>:<max_dict_bytes>".
//    [Example]:
//    * {"compression_opts", "4:5:6:7"} is equivalent to setting:
//        ColumnFamilyOptions cf_opt;
//        cf_opt.compression_opts.window_bits = 4;
//        cf_opt.compression_opts.level = 5;
//        cf_opt.compression_opts.strategy = 6;
//        cf_opt.compression_opts.max_dict_bytes = 7;
//
// @param config_options controls how the map is processed.
// @param base_options the default options of the output "new_options".
// @param opts_map an option name to value map for specifying how "new_options"
//     should be set.
// @param new_options the resulting options based on "base_options" with the
//     change specified in "opts_map".
// @param input_strings_escaped when set to true, each escaped characters
//     prefixed by '\' in the values of the opts_map will be further converted
//     back to the raw string before assigning to the associated options.
// @param ignore_unknown_options when set to true, unknown options are ignored
//     instead of resulting in an unknown-option error.
// @return Status::OK() on success.  Otherwise, a non-ok status indicating
//     error will be returned, and "new_options" will be set to "base_options".
// @return Status::NotFound means the one (or more) of the option name in
//     the opts_map is not valid for this option
// @return Status::NotSupported means we do not know how to parse one of the
//     value for this option
// @return Status::InvalidArgument means the one of the option values is not
//     valid for this option.
Status GetColumnFamilyOptionsFromMap(
    const ConfigOptions& config_options,
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options);

// Take a ConfigOptions `config_options` and a DBOptions "base_options" as the
// default option in addition to a map "opts_map" of option name to option value
// to construct the new DBOptions "new_options".
//
// Below are the instructions of how to config some non-primitive-typed
// options in DBOptions:
//
// * rate_limiter_bytes_per_sec:
//   RateLimiter can be configured directly by specifying its bytes_per_sec.
//   [Example]:
//   - Passing {"rate_limiter_bytes_per_sec", "1024"} is equivalent to
//     passing NewGenericRateLimiter(1024) to rate_limiter_bytes_per_sec.
//
// @param config_options controls how the map is processed.
// @param base_options the default options of the output "new_options".
// @param opts_map an option name to value map for specifying how "new_options"
//     should be set.
// @param new_options the resulting options based on "base_options" with the
//     change specified in "opts_map".
// @param input_strings_escaped when set to true, each escaped characters
//     prefixed by '\' in the values of the opts_map will be further converted
//     back to the raw string before assigning to the associated options.
// @param ignore_unknown_options when set to true, unknown options are ignored
//     instead of resulting in an unknown-option error.
// @return Status::OK() on success.  Otherwise, a non-ok status indicating
//     error will be returned, and "new_options" will be set to "base_options".
// @return Status::NotFound means the one (or more) of the option name in
//     the opts_map is not valid for this option
// @return Status::NotSupported means we do not know how to parse one of the
//     value for this option
// @return Status::InvalidArgument means the one of the option values is not
//     valid for this option.
Status GetDBOptionsFromMap(
    const ConfigOptions& cfg_options, const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    DBOptions* new_options);

// Take a ConfigOptions `config_options` and a Options "base_options" as the
// default option in addition to a map "opts_map" of option name to option value
// to construct the new Options "new_options".
//
// Options are a superset of DBOptions and ColumnFamilyOptions.
Status GetOptionsFromMap(
    const ConfigOptions& config_options, const Options& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    Options* new_options);

// Take a ConfigOptions `config_options` and a BlockBasedTableOptions
// "table_options" as the default option in addition to a map "opts_map" of
// option name to option value to construct the new BlockBasedTableOptions
// "new_table_options".
//
// Below are the instructions of how to config some non-primitive-typed
// options in BlockBasedTableOptions:
//
// * filter_policy:
//   We currently only support the following FilterPolicy in the convenience
//   functions:
//   - BloomFilter: use "bloomfilter:[bits_per_key]:[use_block_based_builder]"
//     to specify BloomFilter.  The above string is equivalent to calling
//     NewBloomFilterPolicy(bits_per_key, use_block_based_builder).
//     [Example]:
//     - Pass {"filter_policy", "bloomfilter:4:true"} in
//       GetBlockBasedTableOptionsFromMap to use a BloomFilter with 4-bits
//       per key and use_block_based_builder enabled.
//
// * block_cache / block_cache_compressed:
//   We currently only support LRU cache in the GetOptions API.  The LRU
//   cache can be set by directly specifying its size.
//   [Example]:
//   - Passing {"block_cache", "1M"} in GetBlockBasedTableOptionsFromMap is
//     equivalent to setting block_cache using NewLRUCache(1024 * 1024).
//
// @param config_options controls how the map is processed.
// @param table_options the default options of the output "new_table_options".
// @param opts_map an option name to value map for specifying how
//     "new_table_options" should be set.
// @param new_table_options the resulting options based on "table_options"
//     with the change specified in "opts_map".
// @param input_strings_escaped when set to true, each escaped characters
//     prefixed by '\' in the values of the opts_map will be further converted
//     back to the raw string before assigning to the associated options.
// @param ignore_unknown_options when set to true, unknown options are ignored
//     instead of resulting in an unknown-option error.
// @return Status::OK() on success.  Otherwise, a non-ok status indicating
//     error will be returned, and "new_table_options" will be set to
//     "table_options".
Status GetBlockBasedTableOptionsFromMap(
    const ConfigOptions& config_options,
    const BlockBasedTableOptions& table_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    BlockBasedTableOptions* new_table_options);

// Take a ConfigOptions `config_options` and a default PlainTableOptions
// "table_options" as the default option in addition to a map "opts_map" of
// option name to option value to construct the new PlainTableOptions
// "new_table_options".
//
// @param config_options controls how the map is processed.
// @param table_options the default options of the output "new_table_options".
// @param opts_map an option name to value map for specifying how
//     "new_table_options" should be set.
// @param new_table_options the resulting options based on "table_options"
//     with the change specified in "opts_map".
// @param input_strings_escaped when set to true, each escaped characters
//     prefixed by '\' in the values of the opts_map will be further converted
//     back to the raw string before assigning to the associated options.
// @param ignore_unknown_options when set to true, unknown options are ignored
//     instead of resulting in an unknown-option error.
// @return Status::OK() on success.  Otherwise, a non-ok status indicating
//     error will be returned, and "new_table_options" will be set to
//     "table_options".
Status GetPlainTableOptionsFromMap(
    const ConfigOptions& config_options, const PlainTableOptions& table_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    PlainTableOptions* new_table_options);

// Take a ConfigOptions `config_options`, a string representation of option
// names and values, apply them into the base_options, and return the new
// options as a result. The string has the following format:
//   "write_buffer_size=1024;max_write_buffer_number=2"
// Nested options config is also possible. For example, you can define
// BlockBasedTableOptions as part of the string for block-based table factory:
//   "write_buffer_size=1024;block_based_table_factory={block_size=4k};"
//   "max_write_buffer_num=2"
//
Status GetColumnFamilyOptionsFromString(const ConfigOptions& config_options,
                                        const ColumnFamilyOptions& base_options,
                                        const std::string& opts_str,
                                        ColumnFamilyOptions* new_options);

Status GetDBOptionsFromString(const ConfigOptions& config_options,
                              const DBOptions& base_options,
                              const std::string& opts_str,
                              DBOptions* new_options);

Status GetStringFromDBOptions(const ConfigOptions& config_options,
                              const DBOptions& db_options,
                              std::string* opts_str);

Status GetStringFromDBOptions(std::string* opts_str,
                              const DBOptions& db_options,
                              const std::string& delimiter = ";  ");

Status GetStringFromColumnFamilyOptions(const ConfigOptions& config_options,
                                        const ColumnFamilyOptions& cf_options,
                                        std::string* opts_str);
Status GetStringFromColumnFamilyOptions(std::string* opts_str,
                                        const ColumnFamilyOptions& cf_options,
                                        const std::string& delimiter = ";  ");
Status GetStringFromCompressionType(std::string* compression_str,
                                    CompressionType compression_type);

const std::vector<CompressionType>& GetSupportedCompressions();

Status GetBlockBasedTableOptionsFromString(
    const ConfigOptions& config_options,
    const BlockBasedTableOptions& table_options, const std::string& opts_str,
    BlockBasedTableOptions* new_table_options);

Status GetPlainTableOptionsFromString(const ConfigOptions& config_options,
                                      const PlainTableOptions& table_options,
                                      const std::string& opts_str,
                                      PlainTableOptions* new_table_options);

Status GetMemTableRepFactoryFromString(
    const std::string& opts_str,
    std::unique_ptr<MemTableRepFactory>* new_mem_factory);

Status GetOptionsFromString(const Options& base_options,
                            const std::string& opts_str, Options* new_options);
Status GetOptionsFromString(const ConfigOptions& config_options,
                            const Options& base_options,
                            const std::string& opts_str, Options* new_options);

// StringToMap parses a serialized options string into a map. Each
// resulting map value is in a self-contained form (it can be embedded
// directly in a `key=value;` context -- e.g. SetOptions -- without further
// escaping). Specifically: nested braced values from the input are
// preserved with their outer braces. Permissive: accepts both braced and
// unbraced forms; values not requiring braces are returned as-is.
// Example:
//   "filter_policy={id=ribbonfilter:10;bloom_before_level=-1};block_size=4096"
// produces:
//   {filter_policy -> "{id=ribbonfilter:10;bloom_before_level=-1}",
//    block_size    -> "4096"}
Status StringToMap(const std::string& opts_str,
                   std::unordered_map<std::string, std::string>* opts_map);

// MapToString is the inverse of StringToMap: a naive `key=value;` join.
// Each map value must already be in self-contained form (as returned by
// StringToMap) -- i.e. simple text or a single balanced `{...}` block.
// Values from StringToMap satisfy this property, so the round-trip
//
//   StringToMap(MapToString(StringToMap(s))) == StringToMap(s)
//
// holds. Callers building a map by hand are responsible for ensuring
// values are self-contained; raw values containing `;` or starting with
// `{` without matching `}` won't round-trip.
Status MapToString(const std::unordered_map<std::string, std::string>& opts_map,
                   std::string* opts_str);

// Request stopping background work, if wait is true wait until it's done
void CancelAllBackgroundWork(DB* db, bool wait = false);

// Delete files which are entirely in the given range
// Could leave some keys in the range which are in files which are not
// entirely in the range. Also leaves L0 files regardless of whether they're
// in the range.
// Snapshots before the delete might not see the data in the given range.
Status DeleteFilesInRange(DB* db, ColumnFamilyHandle* column_family,
                          const Slice* begin, const Slice* end,
                          bool include_end = true);

// Delete files in multiple ranges at once
// Delete files in a lot of ranges one at a time can be slow, use this API for
// better performance in that case.
Status DeleteFilesInRanges(DB* db, ColumnFamilyHandle* column_family,
                           const RangeOpt* ranges, size_t n,
                           bool include_end = true);

// DEPRECATED
struct RangePtr {
  // In case of user_defined timestamp, if enabled, `start` and `limit` should
  // point to key without timestamp part.
  const Slice* start;
  const Slice* limit;

  RangePtr() : start(nullptr), limit(nullptr) {}
  RangePtr(const Slice* s, const Slice* l) : start(s), limit(l) {}
};

// DEPRECATED
Status DeleteFilesInRanges(DB* db, ColumnFamilyHandle* column_family,
                           const RangePtr* ranges, size_t n,
                           bool include_end = true);

// Verify the checksum of file
Status VerifySstFileChecksum(const Options& options,
                             const EnvOptions& env_options,
                             const std::string& file_path);

// Verify the checksum of file
Status VerifySstFileChecksum(const Options& options,
                             const EnvOptions& env_options,
                             const ReadOptions& _read_options,
                             const std::string& file_path,
                             const SequenceNumber& largest_seqno = 0);

}  // namespace ROCKSDB_NAMESPACE
