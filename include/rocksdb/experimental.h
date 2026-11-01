// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>
#include <variant>

#include "rocksdb/data_structure.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace experimental {

// Supported only for Leveled compaction
Status SuggestCompactRange(DB* db, ColumnFamilyHandle* column_family,
                           const Slice* begin, const Slice* end);
Status SuggestCompactRange(DB* db, const Slice* begin, const Slice* end);

// DEPRECATED: this API may be removed in a future release.
// This operation can be done through CompactRange() by setting
// CompactRangeOptions::bottommost_level_compaction set to
// BottommostLevelCompaction::kSkip and setting target level.
//
// Move all L0 files to target_level skipping compaction.
// This operation succeeds only if the files in L0 have disjoint ranges; this
// is guaranteed to happen, for instance, if keys are inserted in sorted
// order. Furthermore, all levels between 1 and target_level must be empty.
// If any of the above condition is violated, InvalidArgument will be
// returned.
Status PromoteL0(DB* db, ColumnFamilyHandle* column_family,
                 int target_level = 1);

// This function is intended to be called with the DB closed and does not write
// to the DB path. It will usually work on an open DB but may fail spuriously
// in that case, and results may be out of date on return.
Status GetFileChecksumsFromCurrentManifest(FileSystem* fs,
                                           const std::string& dbname,
                                           FileChecksumList* checksum_list);

struct UpdateManifestForFilesStateOptions {
  // When true, read current file temperatures from FileSystem and update in
  // DB manifest when a temperature other than Unknown is reported and
  // inconsistent with manifest.
  bool update_temperatures = true;

  // TODO: new_checksums: to update files to latest file checksum algorithm
};

// Utility for updating manifest of DB directory (not open) for current state
// of files on filesystem. See UpdateManifestForFilesStateOptions.
//
// To minimize interference with ongoing DB operations, only the following
// guarantee is provided, assuming no IO error encountered:
// * Only files live in DB at start AND end of call to
// UpdateManifestForFilesState() are guaranteed to be updated (as needed) in
// manifest.
//   * For example, new files after start of call to
//   UpdateManifestForFilesState() might not be updated, but that is not
//   typically required to achieve goal of manifest consistency/completeness
//   (because current DB configuration would ensure new files get the desired
//   consistent metadata).
Status UpdateManifestForFilesState(
    const DBOptions& db_opts, const std::string& db_name,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    const UpdateManifestForFilesStateOptions& opts = {});

// ****************************************************************************
// EXPERIMENTAL new filtering features
// ****************************************************************************

// KeySegmentsExtractor - A class for splitting a key into meaningful pieces, or
// "segments" for filtering purposes. We say the first key segment has segment
// ordinal 0, the second has segment ordinal 1, etc. To simplify satisfying some
// filtering requirements, the segments must encompass a complete key prefix (or
// the whole key). There cannot be gaps between segments (though segments are
// allowed to be essentially unused), and segments cannot overlap.
//
// Keys can also be put in "categories" to simplify some configuration and
// handling. A "legal" key or bound is one that does not return an error (as a
// special, unused category) from the extractor. It is also allowed for all
// keys in a category to return an empty sequence of segments.
//
// To eliminate a confusing distinction between a segment that is empty vs.
// "not present" for a particular key, each key is logically associated with
// an infinite sequence of segments, including some infinite tail of 0-length
// segments. In practice, we only represent a finite sequence that (at least)
// covers the non-trivial segments.
//
// Once in production, the behavior associated with a particular GetId()
// cannot change. Introduce a new GetId() when introducing new behaviors.
// See also SstQueryFilterConfigsManager below.
//
// This feature hasn't yet been validated with user timestamp.
//
// = A SIMPLIFIED MODEL =
// Let us start with the easiest set of contraints to satisfy with a key
// segments extractor that generally allows for correct point and range
// filtering, and add complexity from there. Here we first assume
// * The column family is using the byte-wise comparator, or reverse byte-wise
// * A single category is assigned to all keys (by the extractor)
// * Using simplified criteria for legal segment extraction, the "segment
//   maximal prefix property"
//
// SEGMENT MAXIMAL PREFIX PROPERTY: The segment that a byte is assigned to can
// only depend on the bytes that come before it, not on the byte itself nor
// anything later including the full length of the key or bound.
//
// Equivalently, two keys or bounds must agree on the segment assignment of
// position i if the two keys share a common byte-wise prefix up to at least
// position i - 1 (and i is within bounds of both keys).
//
// This specifically excludes "all or nothing" segments where it is only
// included if it reaches a particular width or delimiter. A segment resembling
// the FixedPrefixTransform would be illegal (without other assumptions); it
// must be like CappedPrefixTransform.
//
// This basically matches the notion of parsing prefix codes (see
// https://en.wikipedia.org/wiki/Prefix_code) except we have to include any
// partial segment (code word) at the end whenever an extension to that key
// might produce a full segment. An example would be parsing UTF-8 into
// segments corresponding to encoded code points, where any incomplete code
// at the end must be part of a trailing segment. Note a three-way
// correspondence between
// (a) byte-wise ordering of encoded code points, e.g.
//     { D0 98 E2 82 AC }
//     { E2 82 AC D0 98 }
// (b) lexicographic-then-byte-wise ordering of segments that are each an
//     encoded code point, e.g.
//     {{ D0 98 } { E2 82 AC }}
//     {{ E2 82 AC } { D0 98 }}
// and (c) lexicographic ordering of the decoded code points, e.g.
//     { U+0418 U+20AC }
//     { U+20AC U+0418 }
// The correspondence between (a) and (b) is a result of the segment maximal
// prefix property and is critical for correct application of filters to
// range queries. The correspondence with (c) is a handy attribute of UTF-8
// (with no over-long encodings) and might be useful to the application.
//
// Example types of key segments that can be freely mixed in any order:
// * Capped number of bytes or codewords. The number cap for the segment
// could be the same for all keys or encoded earlier in the key.
// * Up to *and including* a delimiter byte or codeword.
// * Any/all remaining bytes to the end of the key, though this implies all
// subsequent segments will be empty.
// As part of the segment maximal prefix property, if the segments do not
// extend to the end of the key, that must be implied by the bytes that are
// in segments, NOT because the potential contents of a segment were considered
// incomplete.
//
// For example, keys might consist of
// * Segment 0: Any sequence of bytes up to and including the first ':'
// character, or the whole key if no ':' is present.
// * Segment 1: The next four bytes, or less if we reach end of key.
// * Segment 2: An unsigned byte indicating the number of additional bytes in
// the segment, and then that many bytes (or less up to the end of the key).
// * Segment 3: Any/all remaining bytes in the key
//
// For an example of what can go wrong, consider using '4' as a delimiter
// but not including it with the segment leading up to it. Suppose we have
// these keys and corresponding first segments:
// "123456" -> "123" (in file 1)
// "124536" -> "12"  (in file 2)
// "125436" -> "125" (in file 1)
// Notice how byte-wise comparator ordering of the segments does not follow
// the ordering of the keys. This means we cannot safely use a filter with
// a range of segment values for filtering key range queries. For example,
// we might get a range query for ["123", "125Z") and miss that key "124536"
// in file 2 is in range because its first segment "12" is out of the range
// of the first segments on the bounds, "123" and "125". We cannot even safely
// use this for prefix-like range querying with a Bloom filter on the segments.
// For a query ["12", "124Z"), segment "12" would likely not match the Bloom
// filter in file 1 and miss "123456".
//
// CATEGORIES: The KeySegmentsExtractor is allowed to place keys in categories
// so that different parts of the key space can use different filtering
// strategies. The following property is generally recommended for safe filter
// applicability
// * CATEGORY CONTIGUOUSNESS PROPERTY: each category is contiguous in
//   comparator order. In other words, any key between two keys of category c
//   must also be in category c.
// An alternative to categories when distinct kinds of keys are interspersed
// is to leave some segments empty when they do not apply to that key.
// Filters are generally set up to handle an empty segment specially so that
// it doesn't interfere with tracking accurate ranges on non-empty occurrences
// of the segment.
//
// = BEYOND THE SIMPLIFIED MODEL =
//
// DETAILED GENERAL REQUIREMENTS (incl OTHER COMPARATORS): The exact
// requirements on a key segments extractor depend on whether and how we use
// filters to answer queries that they cannot answer directly. To understand
// this, we describe
// (A) the types of filters in terms of data they represent and can directly
// answer queries about,
// (B) the types of read queries that we want to use filters for, and
// (C) the assumptions that need to be satisfied to connect those two.
//
// TYPES OF FILTERS: Although not exhaustive, here are some useful categories
// of filter data:
// * Equivalence class filtering - Represents or over-approximates a set of
// equivalence classes on keys. The size of the representation is roughly
// proportional to the number of equivalence classes added. Bloom and ribbon
// filters are examples.
// * Order-based filtering - Represents one or more subranges of a key space or
// key segment space. A filter query only requires application of the CF
// comparator. The size of the representation is roughly proportional to the
// number of subranges and to the key or segment size. For example, we call a
// simple filter representing a minimum and a maximum value for a segment a
// min-max filter.
//
// TYPES OF READ QUERIES and their DIRECT FILTERS:
// * Point query - Whether there {definitely isn't, might be} an entry for a
// particular key in an SST file (or partition, etc.).
// The DIRECT FILTER for a point query is an equivalence class filter on the
// whole key.
// * Range query - Whether there {definitely isn't, might be} any entries
// within a lower and upper key bound, in an SST file (or partition, etc.).
//    NOTE: For this discussion, we ignore the detail of inclusive vs.
//    exclusive bounds by assuming a generalized notion of "bound" (vs. key)
//    that conveniently represents spaces between keys. For details, see
//    https://github.com/meta/rocksdb/pull/11434
// The DIRECT FILTER for a range query is an order-based filter on the whole
// key (non-empty intersection of bounds/keys). Simple minimum and maximum
// keys for each SST file are automatically provided by metadata and used in
// the read path for filtering (as well as binary search indexing).
//    PARTITIONING NOTE: SST metadata partitions do not have recorded minimum
//    and maximum keys, so require some special handling for range query
//    filtering. See https://github.com/meta/rocksdb/pull/12872 etc.
// * Where clauses - Additional constraints that can be put on range queries.
// Specifically, a where clause is a tuple <i,j,c,b1,b2> representing that the
// concatenated sequence of segments from i to j (inclusive) compares between
// b1 and b2 according to comparator c.
//    EXAMPLE: To represent that segment of ordinal i is equal to s, that would
//    be <i,i,bytewise_comparator,before(s),after(s)>.
//    NOTE: To represent something like segment has a particular prefix, you
//    would need to split the key into more segments appropriately. There is
//    little loss of generality because we can combine adjacent segments for
//    specifying where clauses and implementing filters.
// The DIRECT FILTER for a where clause is an order-based filter on the same
// sequence of segments and comparator (non-empty intersection of bounds/keys),
// or in the special case of an equality clause (see example), an equivalence
// class filter on the sequence of segments.
//
// GENERALIZING FILTERS (INDIRECT):
// * Point queries can utilize essentially any kind of filter by extracting
// applicable segments of the query key (if not using whole key) and querying
// the corresponding equivalence class or trivial range.
//    NOTE: There is NO requirement e.g. that the comparator used by the filter
//    match the CF key comparator or similar. The extractor simply needs to be
//    a pure function that does not return "out of bounds" segments.
//    FOR EXAMPLE, a min-max filter on the 4th segment of keys can also be
//    used for filtering point queries (Get/MultiGet) and could be as
//    effective and much more space efficient than a Bloom filter, depending
//    on the workload.
//
// Beyond point queries, we generally expect the key comparator to be a
// lexicographic / big endian ordering at a high level (or the reverse of that
// ordering), while each segment can use an arbitrary comparator.
//    FOR EXAMPLE, with a custom key comparator and segments extractor,
//    segment 0 could be a 4-byte unsigned little-endian integer,
//    segment 1 could be an 8-byte signed big-endian integer. This framework
//    requires segment 0 to come before segment 1 in the key and to take
//    precedence in key ordering (i.e. segment 1 order is only consulted when
//    keys are equal in segment 0).
//
// * Equivalence class filters can apply to range queries under conditions
// resembling legacy prefix filtering (prefix_extractor). An equivalence class
// filter on segments i through j and category set s is applicable to a range
// query from lb to ub if
//   * All segments through j extracted from lb and ub are equal.
//     NOTE: being in the same filtering equivalence class is insufficient, as
//     that could be unrelated inputs with a hash collision. Here we are
//     omitting details that would formally accommodate comparators in which
//     different bytes can be considered equal.
//   * The categories of lb and ub are in the category set s.
//   * COMMON SEGMENT PREFIX PROPERTY (for all x, y, z; params j, s): if
//     * Keys x and z have equal segments up through ordinal j, and
//     * Keys x and z are in categories in category set s, and
//     * Key y is ordered x < y < z according to the CF comparator,
//   then both
//     * Key y has equal segments up through ordinal j (compared to x and z)
//     * Key y is in a category in category set s
//  (This is implied by the SEGMENT MAXIMAL PREFIX PROPERTY in the simplified
//  model.)
//
// * Order-based filters on segments (rather than whole key) can apply to range
// queries (with "whole key" bounds). Specifically, an order-based filter on
// segments i through j and category set s is applicable to a range query from
// lb to ub if
//   * All segments through i-1 extracted from lb and ub are equal
//   * The categories of lb and ub are in the category set s.
//   * SEGMENT ORDERING PROPERTY for ordinal i through j, segments
//   comparator c, category set s, for all x, y, and z: if
//     * Keys x and z have equal segments up through ordinal i-1, and
//     * Keys x and z are in categories in category set s, and
//     * Key y is ordered x < y < z according to the CF comparator,
// then both
//     * The common segment prefix property is satisfied through ordinal i-1
//     and with category set s
//     * x_i..j <= y_i..j <= z_i..j according to segment comparator c, where
//     x_i..j is the concatenation of segments i through j of key x (etc.).
//     (This is implied by the SEGMENT MAXIMAL PREFIX PROPERTY in the simplified
//     model.)
//
// INTERESTING EXAMPLES:
// Consider a segment encoding called BadVarInt1 in which a byte with
// highest-order bit 1 means "start a new segment". Also consider BadVarInt0
// which starts a new segment on highest-order bit 0.
//
// Configuration: bytewise comp, BadVarInt1 format for segments 0-3 with
// segment 3 also continuing to the end of the key
// x = 0x 20 21|82 23|||
// y = 0x 20 21|82 23 24|85||
// z = 0x 20 21|82 23|84 25||
//
// For i=j=1, this set of keys violate the common segment prefix property and
// segment ordering property, so can lead to incorrect equivalence class
// filtering or order-based filtering.
//
// Suppose we modify the configuration so that "short" keys (empty in segment
// 2) are placed in an unfiltered category. In that case, x above doesn't meet
// the precondition for being limited by segment properties. Consider these
// keys instead:
// x = 0x 20 21|82 23 24|85||
// y = 0x 20 21|82 23 24|85 26|87|
// z = 0x 20 21|82 23 24|85|86|
// m = 0x 20 21|82 23 25|85|86|
// n = 0x 20 21|82 23|84 25||
//
// Although segment 1 values might be out of order with key order,
// re-categorizing the short keys has allowed satisfying the common segment
// prefix property with j=1 (and with j=0), so we can use equivalence class
// filters on segment 1, or 0, or 0 to 1. However, violation of the segment
// ordering property on i=j=1 (see z, m, n) means we can't use order-based.
//
// p = 0x 20 21|82 23|84 25 26||
// q = 0x 20 21|82 23|84 25|86|
//
// But keys can still be short from segment 2 to 3, and thus we are violating
// the common segment prefix property for segment 2 (see n, p, q).
//
// Configuration: bytewise comp, BadVarInt0 format for segments 0-3 with
// segment 3 also continuing to the end of the key. No short key category.
// x = 0x 80 81|22 83|||
// y = 0x 80 81|22 83|24 85||
// z = 0x 80 81|22 83 84|25||
// m = 0x 80 82|22 83|||
// n = 0x 80 83|22 84|24 85||
//
// Even though this violates the segment maximal prefix property of the
// simplified model, the common segment prefix property and segment ordering
// property are satisfied for the various segment ordinals. In broader terms,
// the usual rule of the delimiter going with the segment before it can be
// violated if every byte value below some threshold starts a segment. (This
// has not been formally verified and is not recommended.)
//
// Suppose that we are paranoid, however, and decide to place short keys
// (empty in segment 2) into an unfiltered category. This is potentially a
// dangerous decision because loss of continuity at least affects the
// ability to filter on segment 0 (common segment prefix property violated
// with i=j=0; see z, m, n; m not in category set). Thus, excluding short keys
// with categories is not a recommended solution either.
class KeySegmentsExtractor {
 public:
  // The extractor assigns keys to categories so that it is easier to
  // combine distinct (though disjoint) key representations within a single
  // column family while applying different or overlapping filtering
  // configurations to the categories.
  // To enable fast set representation, the user is allowed up to 64
  // categories for assigning to keys with the extractor. The user will
  // likely cast to their own enum type or scalars.
  enum KeyCategory : uint_fast8_t {
    kDefaultCategory = 0,
    kMinCategory = kDefaultCategory,
    // ... (user categories)
    // Can be used for keys ordered before typical keys. Not necessarily an
    // error.
    kReservedLowCategory = 62,
    // Can be used for keys ordered after typical keys. Not necessarily an
    // error.
    kReservedHighCategory = 63,
    kMaxUsableCategory = kReservedHighCategory,

    // Signals to the caller that an unexpected input or condition has
    // been reached and filter construction should be aborted.
    kErrorCategoryFilterScope = UINT8_MAX - 2,
    kMinErrorCategory = kErrorCategoryFilterScope,
    // Signals to the caller that an unexpected input or condition has
    // been reached and SST construction (and compaction or flush)
    // should be aborted.
    kErrorCategoryFileScope = UINT8_MAX - 1,
    // Signals to the caller that an unexpected input or condition has
    // been reached and the DB should be considered to have reached an
    // invalid state, at least in memory.
    kErrorCategoryDbScope = UINT8_MAX,
  };
  using KeyCategorySet = SmallEnumSet<KeyCategory, kMaxUsableCategory>;

  // The extractor can process three kinds of key-like inputs
  enum KeyKind {
    // User key, not including user timestamp
    kFullUserKey,
    // An iterator lower bound (inclusive). This should generally be handled
    // the same as a full user key but the distinction might be useful for
    // diagnostics or assertions.
    kInclusiveLowerBound,
    // An iterator upper bound (exclusive). Upper bounds are frequently
    // constructed by incrementing the last byte of a key prefix, and this can
    // affect what should be considered as a segment delimiter.
    kExclusiveUpperBound,
  };

  // The extractor result
  struct Result {
    // Positions in the key (or bound) that represent boundaries
    // between segments, or the exclusive end of each segment. For example, if
    // the key is "abc|123|xyz" then following the guidance of including
    // delimiters with the preceding segment, segment_ends would be {4, 8, 11},
    // representing segments "abc|" "123|" and "xyz". Empty segments are
    // naturally represented with repeated values, as in {4, 8, 8} for
    // "abc|123|", though {4, 8} would be logically equivalent because an
    // infinite sequence of 0-length segments is assumed after what is
    // explicitly represented here. However, segments might not reach the end
    // the key (no automatic last segment to the end of the key) and that is
    // OK for the WEAK ordering property.
    //
    // The first segment automatically starts at key position 0. The only way
    // to put gaps between segments of interest is to assign those gaps to
    // numbered segments, which can be left unused.
    std::vector<uint32_t> segment_ends;

    // A category to assign to the key or bound. This default may be kept,
    // such as to put all keys into a single category.
    // IMPORTANT CURRENT LIMITATION from above: each category must be
    // contiguous in key comparator order, so any key between two keys in
    // category c must also be in category c. (Typically the category will be
    // determined by segment 0 in some way, often the first byte.) The enum
    // scalar values do not need to be related to key order.
    KeyCategory category = kDefaultCategory;

    void Reset() {
      segment_ends.clear();
      category = kDefaultCategory;
    }
  };

  virtual ~KeySegmentsExtractor() = default;

  // A class name for this extractor. See also expectations in GetId().
  virtual const char* Name() const = 0;

  // An identifying string that is permanently associated with the behavior
  // of this extractor. If a behavior change is made or set in the constructor,
  // the id must change to avoid incorrect filtering behavior on DBs using a
  // previous version of the extractor.
  virtual std::string GetId() const {
    // The default implementation assumes no configuration variance in the
    // constructor and just returns the class name.
    return Name();
  }

  // Populates the extraction result and returns OK. Error can be signaled
  // with `kError` pseudo-categories. This function is expected to generate
  // non-error results (though possibly empty) on all keys or bounds expected
  // to be encountered by the DB. RocksDB will always call the function with
  // a (pointer to a) default-initialized result object.
  virtual void Extract(const Slice& key_or_bound, KeyKind kind,
                       Result* result) const = 0;
};

// Constructs a KeySegmentsExtractor for fixed-width key segments that safely
// handles short keys by truncating segments at the end of the input key.
// See comments on KeySegmentsExtractor for why this is much safer for
// filtering than "all or nothing" fixed-size segments. This is essentially
// a generalization of (New)CappedPrefixTransform.
std::shared_ptr<const KeySegmentsExtractor>
MakeSharedCappedKeySegmentsExtractor(const std::vector<size_t>& byte_widths);

// Alternatives for filtering inputs

// An individual key segment.
struct SelectKeySegment {
  // Segments are numbered starting from 0.
  explicit SelectKeySegment(uint16_t _segment_index)
      : segment_index(_segment_index) {}
  uint16_t segment_index;
};

// A range of key segments concatenated together. No concatenation operations
// are needed, however, because with no gaps between segments, a range of
// segments is a simple substring of the key.
struct SelectKeySegmentRange {
  // Segments are numbered starting from 0. Range is inclusive.
  explicit SelectKeySegmentRange(uint8_t _from_segment_index,
                                 uint8_t _to_segment_index)
      : from_segment_index(_from_segment_index),
        to_segment_index(_to_segment_index) {}
  // Inclusive
  uint8_t from_segment_index;
  // Inclusive
  uint8_t to_segment_index;
};

// User key without timestamp
struct SelectWholeKey {};

// TODO: The remaining Select* are not yet supported
// As generated by prefix_extractor
struct SelectLegacyKeyPrefix {};

struct SelectUserTimestamp {};

struct SelectColumnName {};

// NOTE: more variants might be added in the future.
// NOTE2: filtering on values is not supported because it could easily break
// overwrite semantics. (Filter out SST with newer, non-matching value but
// see obsolete value that does match.)
using FilterInput =
    std::variant<SelectWholeKey, SelectKeySegment, SelectKeySegmentRange,
                 SelectLegacyKeyPrefix, SelectUserTimestamp, SelectColumnName>;

// Base class for individual filtering schemes in terms of chosen
// FilterInputs, but not tied to a particular KeySegmentsExtractor.
//
// Not user extensible, name sometimes shortened to SQFC
class SstQueryFilterConfig {
 public:
  virtual ~SstQueryFilterConfig() = default;
};

// A filtering scheme that stores minimum and maximum values (according
// to bytewise ordering) of the specified filter input. Because the
// empty string is often a special case, the filter excludes that from the
// min/max computation and keeps a separate boolean for whether empty is
// present.
//
// The filter is also limited to the specified categories, ignoring entries
// outside the given set of categories. If not All, ranges can only be
// filtered if upper and lower bounds are in the same category (and that
// category is in the set relevant to the filter).
std::shared_ptr<SstQueryFilterConfig> MakeSharedBytewiseMinMaxSQFC(
    FilterInput select, KeySegmentsExtractor::KeyCategorySet categories =
                            KeySegmentsExtractor::KeyCategorySet::All());

std::shared_ptr<SstQueryFilterConfig> MakeSharedReverseBytewiseMinMaxSQFC(
    FilterInput select, KeySegmentsExtractor::KeyCategorySet categories =
                            KeySegmentsExtractor::KeyCategorySet::All());

// TODO: more kinds of filters, eventually including Bloom/ribbon filters
// and replacing the old filter configuration APIs

// Represents a complete strategy for representing filters in SST files
// and applying them to optimize range and point queries by excluding
// irrelevant SST files (as best we can). This is a set of filtering
// schemes and a KeySegmentsExtractor. For performance, a single extractor
// should be implemented to meet all the filtering needs of any given
// column family. KeySegmentsExtractor and FilterInput should be flexible
// enough that there is no loss of generality, e.g. with leaving segments
// blank and using segment ranges.
struct SstQueryFilterConfigs {
  std::vector<std::shared_ptr<SstQueryFilterConfig>> filters;
  std::shared_ptr<const KeySegmentsExtractor> extractor;

  // Whether this object represent an empty set of configs because no
  // applicable configurations were found. (This case is represented by
  // an internal singleton instance.)
  bool IsEmptyNotFound() const;
};

// SstQueryFilterConfigsManager provides facilities for safe and effective
// filtering version management, with simple dynamic upgrade/downgrade
// support. It is designed to encourage a development pattern that
// minimizes the risk of filter and extractor versioning bugs.
//
// SstQueryFilterConfigsManager is essentially an immutable mapping
// from {config_name_string, version_number} -> SstQueryFilterConfigs
// for some contiguous range of version numbers. It is also a starting
// point for specifying which configuration should be used, with awareness
// of other configurations that might already be persisted in SST files
// or switched to dynamically.
//
// Background: a single codebase might have many use cases and for
// each use case, a sequence of past, current, and future filtering
// configurations. It is common for future configurations to be
// implemented before automatically deployed in order to ensure that
// a DB can be effectively opened and operated on by a recent older code
// version. And it is common to maintain a reasonable history of past
// configurations to ensure smooth upgrade path and proper handling of
// older SST files that haven't been compacted recently.
//
// It would be possible to make SstQueryFilterConfigs dynamically
// configurable through strings, but that would encourage deployment
// of ad-hoc, under-tested configurations.
//
// Solution: the {config_name_string, version_number} -> SstQueryFilterConfigs
// mapping in SstQueryFilterConfigsManager formalizes the history (past and
// future) of approved/tested configurations for a given use case. Filter
// configurations are kept paired with extractors that they make sense with.
//
// The version numbers are "global" to the SstQueryFilterConfigsManager so
// that it is simple to determine whether a particular code version supports
// a particular filtering version, regardless of which use case. Numbering
// always starts with 1, as 0 is reserved for selecting a "no filters"
// configuration
//
// Consider an example initialized with this Data:
//
// SstQueryFilterConfigsManager::Data data = {
//      {1, {{"foo", foo_configs}}},
//      {2, {{"bar", bar_configs},
//           {"baz", baz_configs}}},
//      {3, {{"bar", bar_configs_v2}}},
//   };
//
// For example, MakeSharedFactory(..., "baz", 1) will use a default empty
// config, while both MakeSharedFactory(..., "baz", 2) and
// MakeSharedFactory(..., "baz", 3) select `baz_configs`. Selecting version
// >= 4 is rejected because those configurations are not known to this
// version of the code.
//
// For correct operation, existing versions should be treated as immutable
// (once committed to where they could enter production). For example, an
// update to "baz" should be done in a new version (4), not by amending to
// version 3. Note also (from before) that the behavior of named extractors
// must not change, so changes to key segment extraction should introduce
// new named extractors while keeping the old in the older configs.
//
// It is possible to eventually remove the oldest versions, as long as
// * You won't be rolling back to that version.
// * You don't have any SST files using an extractor that is only available
// in that version (and prior).
// * For each use case and version you might roll back to, you aren't removing
// the configuration in effect for that version. In our example above, we
// cannot simply remove version 1 because that would change the configuration
// of "foo" at version 2. Moving {"foo", foo_configs} to version 2 would be
// an acceptable work-around for retiring version 1.
// * There are no gaps in the version numbers specified. Even if you completely
// retire a use case and want to remove relevant code, you still need to keep
// an explicit mapping, even if it's empty, as in `{3, {}}` if retiring "bar".
//
// Internally, the SstQueryFilterConfigsManager greatly simplifies lifetime
// management for relevant objects such as KeySegmentExtractors, and provides
// a lighter weight (and less troublesome) mechanism for relevant named object
// look-up vs. ObjectRegistry. If following the guidelines above, any extractor
// referenced in a read SST file should also be referenced by the
// SstQueryFilterConfigsManager.
//
// Not user extensible
class SstQueryFilterConfigsManager
    : public std::enable_shared_from_this<SstQueryFilterConfigsManager> {
 public:
  using FilteringVersion = uint32_t;
  using NamedConfigs = std::pair<std::string, SstQueryFilterConfigs>;
  using Data =
      std::vector<std::pair<FilteringVersion, std::vector<NamedConfigs>>>;

  static Status MakeShared(const Data& data,
                           std::shared_ptr<SstQueryFilterConfigsManager>* out);

  virtual ~SstQueryFilterConfigsManager() = default;

  // EXPERIMENTAL/TEMPORARY: hook into table properties for persisting
  // filters and table_filter for applying to range queries.

  class Factory : public TablePropertiesCollectorFactory {
   public:
    // Modify the target filtering version for new filters. Returns
    // non-OK if the version is not supported. Thread-safe.
    virtual Status SetFilteringVersion(FilteringVersion ver) = 0;
    virtual FilteringVersion GetFilteringVersion() const = 0;

    // The configs_name used to create this Factory. Immutable.
    virtual const std::string& GetConfigsName() const = 0;

    // The relevant configs from the SstQueryFilterConfigsManager for
    // the ConfigsName and FilteringVersion.
    virtual const SstQueryFilterConfigs& GetConfigs() const = 0;

    // The buffers pointed to by the Slices must live as long as any read
    // operations using this table filter function.
    // Can read and process any filters created under this
    // SstQueryFilterConfigsManager but is most efficient when using the
    // same KeySegmentExtractor as this Factory's configs.
    // (That performance optimization is the only reason this function is here
    // rather than in SstQueryFilterConfigsManager.)
    virtual std::function<bool(const TableProperties&)>
    GetTableFilterForRangeQuery(Slice lower_bound_incl,
                                Slice upper_bound_excl) const = 0;
  };

  // Returns OK and creates a Factory as long as `ver` is in the
  // supported range or 0 (always empty/not found). If the particular
  // config_name is not found under that version, then
  // factory->GetConfigs().IsEmptyNotFound() will be true. Such a factory can
  // read filters but will not write any filters.
  virtual Status MakeSharedFactory(const std::string& configs_name,
                                   FilteringVersion ver,
                                   std::shared_ptr<Factory>* out) const = 0;
};

}  // namespace experimental
}  // namespace ROCKSDB_NAMESPACE
