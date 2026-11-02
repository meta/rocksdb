//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <forward_list>
#include <functional>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class BlobFetcher;
class BlobIndex;
class FilePrefetchBuffer;
class PinnableSlice;
class PrefetchBufferCollection;
class Slice;

// Wide-column serialization/deserialization primitives.
//
// Version 1 Layout:
// The two main parts of the layout are 1) a sorted index containing the column
// names and column value sizes and 2) the column values themselves. Keeping the
// index and the values separate will enable selectively reading column values
// down the line. Note that currently the index has to be fully parsed in order
// to find out the offset of each column value.
//
// Legend: cn = column name, cv = column value, cns = column name size, cvs =
// column value size.
//
//      +----------+--------------+----------+-------+----------+---...
//      | version  | # of columns |  cns 1   | cn 1  |  cvs 1   |
//      +----------+--------------+------------------+--------- +---...
//      | varint32 |   varint32   | varint32 | bytes | varint32 |
//      +----------+--------------+----------+-------+----------+---...
//
//      ... continued ...
//
//          ...---+----------+-------+----------+-------+---...---+-------+
//                |  cns N   | cn N  |  cvs N   | cv 1  |         | cv N  |
//          ...---+----------+-------+----------+-------+---...---+-------+
//                | varint32 | bytes | varint32 | bytes |         | bytes |
//          ...---+----------+-------+----------+-------+---...---+-------+
//
// Version 2 Layout (with blob index support):
// Groups all metadata upfront before variable-length data. This enables
// efficient access patterns: index-based value access skips name data
// entirely, default column access is O(1), and type checks are O(1).
//
// Legend: cn = column name, cv = column value, cns = column name size,
//         cvs = column value size, ct = column type.
//
// Section 1: HEADER (2 varints)
//   +----------+--------------+
//   | version  | # of columns |
//   | varint32 |   varint32   |
//   +----------+--------------+
//
// Section 2: SKIP INFO (3 varints)
//   +-------------------+---------------------+------------------+
//   | name_sizes_bytes  | value_sizes_bytes   | names_bytes      |
//   | varint32          | varint32            | varint32         |
//   +-------------------+---------------------+------------------+
//   name_sizes_bytes  = byte size of NAME SIZES section (section 4)
//   value_sizes_bytes = byte size of VALUE SIZES section (section 5)
//   names_bytes       = byte size of NAMES section (section 6)
//
//   Placed immediately after the header so that header + skip info form
//   a contiguous varint sequence (5 varints), enabling future SIMD-based
//   varint decoding.
//
// Section 3: COLUMN TYPES (N bytes, fixed-size)
//   +------+------+---...---+--------+
//   | ct_0 | ct_1 |         | ct_N-1 |
//   | byte | byte |         |  byte  |
//   +------+------+---...---+--------+
//   ct values are ValueType entries from db/dbformat.h, e.g.:
//     kTypeValue (0x01) = inline value
//     kTypeBlobIndex (0x11) = blob index reference
//   Future per-column types (kTypeMerge, kTypeDeletion, etc.) can be added
//   without changing the byte layout, though a reader predating a given type
//   rejects it as Corruption (see IsValidColumnValueType).
//
// Section 4: NAME SIZES (N varints)
//   +----------+----------+---...---+------------+
//   | cns_0    | cns_1    |         | cns_{N-1}  |
//   | varint32 | varint32 |         | varint32   |
//   +----------+----------+---...---+------------+
//
// Section 5: VALUE SIZES (N varints)
//   +----------+----------+---...---+------------+
//   | cvs_0    | cvs_1    |         | cvs_{N-1}  |
//   | varint32 | varint32 |         | varint32   |
//   +----------+----------+---...---+------------+
//
// Section 6: COLUMN NAMES (concatenated, sorted)
//   +------+------+---...---+--------+
//   | cn_0 | cn_1 |         | cn_N-1 |
//   | bytes| bytes|         | bytes  |
//   +------+------+---...---+--------+
//
// Section 7: COLUMN VALUES (concatenated)
//   +------+------+---...---+--------+
//   | cv_0 | cv_1 |         | cv_N-1 |
//   | bytes| bytes|         | bytes  |
//   +------+------+---...---+--------+
//
// When ct = kTypeBlobIndex, the cv contains a serialized BlobIndex.

class WideColumnSerialization {
 public:
  // Version constants for wide column serialization format.
  // - kVersion1: Original format with inline column values only.
  // - kVersion2: Extended format that supports blob index references in
  //              columns. Used when large column values are stored in blob
  //              files.
  static constexpr uint32_t kVersion1 = 1;
  static constexpr uint32_t kVersion2 = 2;

  // Serialize columns using version 1 format (no blob support)
  static Status Serialize(const WideColumns& columns, std::string& output);

  // Returns the exact number of bytes Serialize() would produce for `columns`
  // (the version 1 serialized size), computed without allocating.
  static size_t SerializedSizeV1(const WideColumns& columns);

  // Serialize columns with some columns replaced by blob indices (version 2)
  // columns: vector of (column_name, column_value) pairs
  // blob_columns: vector of (column_index, blob_index) pairs indicating which
  //               columns should be stored as blob references
  static Status SerializeV2(
      const std::vector<std::pair<std::string, std::string>>& columns,
      const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
      std::string& output);

  // Overload that takes Slice-based WideColumns directly, avoiding the
  // need to copy column names and values into string pairs.
  static Status SerializeV2(
      const WideColumns& columns,
      const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
      std::string& output);

  // Deserialize a wide-column entity (version 1, or version 2 with or without
  // blob references). `input` must be exactly one serialized entity; trailing
  // bytes are reported as Corruption (the deserializers are not prefix-parsers
  // -- an entity value is always the whole stored value).
  //
  // columns: receives the full sorted column set. For blob-backed columns,
  // value() is initially the serialized BlobIndex bytes from the entity.
  //
  // blob_columns: if non-null, receives (column_index, blob_index) pairs
  // identifying which entries in `columns` are blob references, and their
  // decoded BlobIndex data. If null, the caller is promising the entity is
  // fully resolved (no blob references); encountering one returns Corruption
  // (as it could theoretically arise from data corruption).
  // When non-null, this agrees with HasBlobColumns() about whether blob column
  // references are present.
  static Status Deserialize(
      const Slice& input, WideColumns& columns,
      std::vector<std::pair<size_t, BlobIndex>>* blob_columns);

  // Convenience wrapper of Deserialize() for callers that own a fully resolved
  // entity with no blob references (returns Corruption if one is present).
  static Status DeserializeSimple(const Slice& input, WideColumns& columns) {
    return Deserialize(input, columns, nullptr /* blob_columns */);
  }

  // Check if the serialized entity has any blob column references.
  // Sets *has_blob_columns to true if version >= 2 and at least one column
  // has blob type; false otherwise.
  // For valid input, this must agree with Deserialize() about whether blob
  // column references are present.
  // Returns Status::Corruption on decode errors.
  static Status HasBlobColumns(const Slice& input, bool& has_blob_columns);

  // Fully validates one serialized entity and calls the callback once for each
  // blob column's BlobIndex. The implementation avoids materializing column
  // names and inline values, but validates their framing, ordering, section
  // sizes, and exact input consumption before invoking any callback.
  // Returns OK with no callbacks for a valid entity without blob columns.
  static Status ForEachBlobIndex(
      const Slice& input,
      const std::function<Status(const BlobIndex&)>& callback);

  // Extracts the default column (empty name) from a serialized entity, without
  // fetching any blob. Uses an O(1) fast path (no full deserialization).
  //   is_blob_reference == false: `value` is the inline default column value.
  //   is_blob_reference == true : `value` is the raw serialized BlobIndex of
  //                               the default column; resolve it with
  //                               ResolveDefaultColumnBlobReference().
  // Either way `value` is a zero-copy Slice into `input`. If there is no
  // default column, `value` is empty and is_blob_reference is false. Returns
  // Corruption on decode errors (never NotSupported).
  static Status GetValueOfDefaultColumn(const Slice& input, Slice& value,
                                        bool& is_blob_reference);

  // Resolves all blob column references in a V2 wide-column entity,
  // fetches the blob values, and re-serializes as a V1 entity (all inline).
  // Handles inlined blobs (IsInlined) defensively.
  //
  // Used by the read path (GetContext, DBIter) when a V2 entity with blob
  // column references needs to be converted to V1 format for consumption by
  // APIs that only support V1 (e.g., TimedFullMerge,
  // PinnableWideColumns::SetWideColumnValue).
  //
  // Sets *resolved to false and leaves resolved_entity unchanged when
  // no blob columns are present.
  //
  // Optional parameters:
  //   prefetch_buffers - for prefetch optimization (nullptr = no prefetch)
  //   total_bytes_read - accumulates bytes read from blob files (nullptr =
  //   skip) num_blobs_resolved - count of blob columns resolved (nullptr =
  //   skip)
  static Status ResolveEntityBlobColumns(
      const Slice& entity_value, const Slice& user_key,
      const BlobFetcher* blob_fetcher,
      PrefetchBufferCollection* prefetch_buffers, std::string& resolved_entity,
      bool& resolved, uint64_t* total_bytes_read, uint64_t* num_blobs_resolved);

  // Multi-buffer counterpart to ResolveEntityBlobColumns that avoids
  // re-serialization. Whereas ResolveEntityBlobColumns produces a single
  // re-serialized V1 entity string, this deserializes the V2 entity and, for
  // each blob column, fetches the blob value into a freshly appended node of
  // `extra_buffers` (whose address is stable). On success, `resolved_columns`
  // holds the full column set: inline columns keep zero-copy Slices into
  // `entity_value`, and blob columns get zero-copy Slices into the
  // corresponding `extra_buffers` node. The caller must keep both
  // `entity_value` and `extra_buffers` alive while using `resolved_columns`
  // (e.g. via PinnableWideColumnsHelper::ResolveColumns, which splices
  // `extra_buffers` in alongside the original entity buffer).
  //
  // Sets `resolved` to false and leaves the outputs empty when no blob columns
  // are present. A non-inlined blob column with a null blob_fetcher (nothing to
  // read the blob with) is reported as Corruption because this could
  // hypothetically be caused by data corruption (more likely a programming
  // mistake); inlined blob columns need no fetcher.
  static Status ResolveEntityBlobColumnsMultiBuffer(
      const Slice& entity_value, const Slice& user_key,
      const BlobFetcher* blob_fetcher,
      PrefetchBufferCollection* prefetch_buffers, WideColumns& resolved_columns,
      std::forward_list<PinnableSlice>& extra_buffers, bool& resolved,
      uint64_t* total_bytes_read, uint64_t* num_blobs_resolved);

  // Resolves the default column's blob reference -- the raw serialized
  // BlobIndex returned by GetValueOfDefaultColumn() when is_blob_reference is
  // true -- into its value. An inlined BlobIndex needs no fetch. A non-inlined
  // reference with a null blob_fetcher returns Corruption: a blob-backed value
  // with no way to read the blob (a reader without blob support, e.g.
  // SstFileReader, or corrupt/legacy data). This is the single place that
  // condition is handled for default-column reads.
  static Status ResolveDefaultColumnBlobReference(
      const Slice& blob_index, const Slice& user_key,
      const BlobFetcher* blob_fetcher, PinnableSlice& value);

  // Resolves V2 entity blob columns if present, returning an effective
  // Slice for use with TimedFullMerge. If the entity has no blob columns
  // (V1 or V2 without blobs), effective_entity is set to entity_value.
  // Otherwise, blob columns are resolved and effective_entity points to
  // the re-serialized V1 data in resolved_entity.
  // The caller must keep resolved_entity alive while using effective_entity.
  static Status ResolveEntityForMerge(
      const Slice& entity_value, const Slice& user_key,
      const BlobFetcher* blob_fetcher,
      PrefetchBufferCollection* prefetch_buffers, std::string& resolved_entity,
      Slice& effective_entity);

 private:
  friend class WideColumnSerializationTest;
  // Get the serialization version from the input.
  // Sets *version to the version number.
  // Returns Status::Corruption on decode errors.
  static Status GetVersion(const Slice& input, uint32_t& version);

  // Returns InvalidArgument with the given message if size exceeds uint32_t.
  static Status ValidateWideColumnLimit(size_t size, const char* msg) {
    if (size > static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
      return Status::InvalidArgument(msg);
    }
    return Status::OK();
  }

  // Returns Corruption if prev_name >= name (columns must be strictly ordered).
  static Status ValidateColumnOrder(const Slice& prev_name, const Slice& name) {
    if (prev_name.compare(name) >= 0) {
      return Status::Corruption("Wide columns out of order");
    }
    return Status::OK();
  }

  // Shared implementation for both SerializeV2 overloads.
  // get_name(i): returns Slice for column i's name
  // get_value(i): returns Slice for column i's inline value
  template <typename GetName, typename GetValue>
  static Status SerializeV2Impl(
      size_t num_columns,
      const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
      std::string& output, GetName get_name, GetValue get_value);

  // Validates num_columns limit and builds a per-column lookup map from
  // blob_columns. Returns InvalidArgument on validation failure.
  static Status BuildBlobIndexMap(
      size_t num_columns,
      const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
      std::vector<const BlobIndex*>& blob_index_map);

  // Parses V1 layout (interleaved name/value_size pairs followed by values)
  // into columns. Used by Deserialize() for version 1 entities.
  static Status DeserializeV1(Slice& input, uint32_t num_columns,
                              std::vector<WideColumn>& columns);

  // Parses V2 layout sections 2-7 (skip info through values) into columns and
  // column types. Used by Deserialize() for version 2 entities.
  static Status DeserializeV2Impl(Slice& input, uint32_t num_columns,
                                  std::vector<WideColumn>& columns,
                                  std::vector<ValueType>& column_types);

  // Returns true if t is a supported per-column ValueType. Currently only
  // kTypeValue (inline) and kTypeBlobIndex are supported. Notably,
  // kTypeWideColumnEntity is rejected to prevent recursive nesting.
  static bool IsValidColumnValueType(ValueType t) {
    return t == kTypeValue || t == kTypeBlobIndex;
  }

  // Returns true if any of the first num_columns type bytes equals
  // kTypeBlobIndex. Typical entities have <10 columns, so a linear
  // scan is sufficient; SIMD could be considered if column counts grow.
  static bool ContainsBlobType(const char* type_bytes, uint32_t num_columns);
};

}  // namespace ROCKSDB_NAMESPACE
