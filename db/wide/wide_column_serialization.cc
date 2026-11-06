//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include <cassert>
#include <cstring>

#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_index.h"
#include "db/blob/prefetch_buffer_collection.h"
#include "db/wide/wide_columns_helper.h"
#include "rocksdb/slice.h"
#include "util/autovector.h"
#include "util/cast_util.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

Status WideColumnSerialization::BuildBlobIndexMap(
    size_t num_columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    std::vector<const BlobIndex*>& blob_index_map) {
  if (Status s = ValidateWideColumnLimit(num_columns, "Too many wide columns");
      !s.ok()) {
    return s;
  }

  blob_index_map.assign(num_columns, nullptr);
  for (const auto& blob_col : blob_columns) {
    if (blob_col.first >= blob_index_map.size()) {
      return Status::InvalidArgument("Blob column index out of range");
    }
    blob_index_map[blob_col.first] = &blob_col.second;
  }

  return Status::OK();
}

bool WideColumnSerialization::ContainsBlobType(const char* type_bytes,
                                               uint32_t num_columns) {
  for (uint32_t i = 0; i < num_columns; ++i) {
    if (static_cast<uint8_t>(type_bytes[i]) == kTypeBlobIndex) {
      return true;
    }
  }
  return false;
}

Status WideColumnSerialization::Serialize(const WideColumns& columns,
                                          std::string& output) {
  const size_t num_columns = columns.size();

  if (Status sv = ValidateWideColumnLimit(num_columns, "Too many wide columns");
      !sv.ok()) {
    return sv;
  }

  PutVarint32(&output, kVersion1);

  PutVarint32(&output, static_cast<uint32_t>(num_columns));

  const Slice* prev_name = nullptr;

  for (size_t i = 0; i < columns.size(); ++i) {
    const WideColumn& column = columns[i];

    const Slice& name = column.name();
    if (Status s_name =
            ValidateWideColumnLimit(name.size(), "Wide column name too long");
        !s_name.ok()) {
      return s_name;
    }

    if (prev_name) {
      if (Status so = ValidateColumnOrder(*prev_name, name); !so.ok()) {
        return so;
      }
    }

    const Slice& value = column.value();
    if (Status s_val =
            ValidateWideColumnLimit(value.size(), "Wide column value too long");
        !s_val.ok()) {
      return s_val;
    }

    PutLengthPrefixedSlice(&output, name);
    PutVarint32(&output, static_cast<uint32_t>(value.size()));

    prev_name = &name;
  }

  for (const auto& column : columns) {
    const Slice& value = column.value();

    output.append(value.data(), value.size());
  }

  return Status::OK();
}

size_t WideColumnSerialization::SerializedSizeV1(const WideColumns& columns) {
  size_t size = VarintLength(kVersion1) +
                VarintLength(static_cast<uint32_t>(columns.size()));

  for (const auto& column : columns) {
    const size_t name_size = column.name().size();
    const size_t value_size = column.value().size();
    size += VarintLength(name_size) + name_size + VarintLength(value_size) +
            value_size;
  }

  return size;
}

template <typename GetName, typename GetValue>
Status WideColumnSerialization::SerializeV2Impl(
    size_t num_columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    std::string& output, GetName get_name, GetValue get_value) {
  std::vector<const BlobIndex*> blob_index_map;
  if (Status s = BuildBlobIndexMap(num_columns, blob_columns, blob_index_map);
      !s.ok()) {
    return s;
  }
  assert(blob_index_map.size() == num_columns);

  // First pass: validate column ordering, compute sizes, serialize blob
  // indices, and build column types.
  // Only allocate serialized blob index strings for actual blob columns
  // (typically few) rather than all num_columns.
  std::vector<std::string> serialized_blob_indices(blob_columns.size());
  autovector<uint32_t, 16> name_sizes;
  autovector<uint32_t, 16> value_sizes;
  name_sizes.resize(num_columns);
  value_sizes.resize(num_columns);
  std::string column_types;
  column_types.reserve(num_columns);

  Slice prev_name_storage;
  bool has_prev = false;
  uint64_t name_sizes_bytes = 0;
  uint64_t names_bytes = 0;
  uint64_t total_value_sizes_bytes = 0;
  uint64_t total_values_bytes = 0;
  size_t blob_ser_idx = 0;

  for (size_t i = 0; i < num_columns; ++i) {
    const Slice name = get_name(i);
    const Slice value = get_value(i);

    if (Status sn =
            ValidateWideColumnLimit(name.size(), "Wide column name too long");
        !sn.ok()) {
      return sn;
    }

    if (has_prev) {
      if (Status so = ValidateColumnOrder(prev_name_storage, name); !so.ok()) {
        return so;
      }
    }

    name_sizes[i] = static_cast<uint32_t>(name.size());
    name_sizes_bytes += VarintLength(name_sizes[i]);
    names_bytes += name_sizes[i];

    if (blob_index_map[i] != nullptr) {
      const BlobIndex* blob_idx = blob_index_map[i];
      blob_idx->EncodeTo(&serialized_blob_indices[blob_ser_idx]);
      value_sizes[i] =
          static_cast<uint32_t>(serialized_blob_indices[blob_ser_idx].size());
      column_types.push_back(static_cast<char>(kTypeBlobIndex));
      ++blob_ser_idx;
    } else {
      if (Status svl = ValidateWideColumnLimit(value.size(),
                                               "Wide column value too long");
          !svl.ok()) {
        return svl;
      }
      value_sizes[i] = static_cast<uint32_t>(value.size());
      column_types.push_back(static_cast<char>(kTypeValue));
    }

    total_value_sizes_bytes += VarintLength(value_sizes[i]);
    total_values_bytes += value_sizes[i];

    prev_name_storage = name;
    has_prev = true;
  }

  const uint64_t kMaxWideColumnComponent =
      static_cast<uint64_t>(std::numeric_limits<uint32_t>::max());
  if (name_sizes_bytes > kMaxWideColumnComponent ||
      total_value_sizes_bytes > kMaxWideColumnComponent) {
    return Status::InvalidArgument("Wide column metadata too large");
  }
  if (names_bytes > kMaxWideColumnComponent) {
    return Status::InvalidArgument("Wide column names too large");
  }
  if (total_values_bytes > kMaxWideColumnComponent) {
    return Status::InvalidArgument("Wide column values too large");
  }

  const auto name_sizes_bytes32 = static_cast<uint32_t>(name_sizes_bytes);
  const auto names_bytes32 = static_cast<uint32_t>(names_bytes);
  const auto total_value_sizes_bytes32 =
      static_cast<uint32_t>(total_value_sizes_bytes);
  const auto total_values_bytes32 = static_cast<uint32_t>(total_values_bytes);

  // Second pass: write all V2 sections to output.
  // Pre-allocate output string.
  const size_t total_size = VarintLength(kVersion2) +
                            VarintLength(static_cast<uint32_t>(num_columns)) +
                            num_columns +  // column types
                            VarintLength(name_sizes_bytes32) +
                            VarintLength(total_value_sizes_bytes32) +
                            VarintLength(names_bytes32) + name_sizes_bytes32 +
                            total_value_sizes_bytes32 + names_bytes32 +
                            total_values_bytes32;

  const size_t base_offset = output.size();
  output.reserve(base_offset + total_size);

  // Sections 1-3: header, skip info, column types
  PutVarint32(&output, kVersion2);
  PutVarint32(&output, static_cast<uint32_t>(num_columns));
  PutVarint32(&output, name_sizes_bytes32);
  PutVarint32(&output, total_value_sizes_bytes32);
  PutVarint32(&output, names_bytes32);
  output.append(column_types);

  // Sections 4-7: resize to final size, then write all 4 sections in a
  // single loop using independent pointers. Each section's start offset is
  // known from the sizes computed in the first pass.
  if (num_columns == 0) {
    return Status::OK();
  }

  const size_t sec4_offset = output.size();
  output.resize(base_offset + total_size);

  char* s4 = &output[sec4_offset];            // section 4: name sizes
  char* s5 = s4 + name_sizes_bytes32;         // section 5: value sizes
  char* s6 = s5 + total_value_sizes_bytes32;  // section 6: names
  char* s7 = s6 + names_bytes32;              // section 7: values

  size_t blob_write_idx = 0;
  for (size_t i = 0; i < num_columns; ++i) {
    s4 = EncodeVarint32(s4, name_sizes[i]);
    s5 = EncodeVarint32(s5, value_sizes[i]);

    memcpy(s6, get_name(i).data(), name_sizes[i]);
    s6 += name_sizes[i];

    if (blob_index_map[i] != nullptr) {
      memcpy(s7, serialized_blob_indices[blob_write_idx].data(),
             value_sizes[i]);
      ++blob_write_idx;
    } else {
      memcpy(s7, get_value(i).data(), value_sizes[i]);
    }
    s7 += value_sizes[i];
  }

  return Status::OK();
}

Status WideColumnSerialization::SerializeV2(
    const std::vector<std::pair<std::string, std::string>>& columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    std::string& output) {
  return SerializeV2Impl(
      columns.size(), blob_columns, output,
      [&](size_t i) { return Slice(columns[i].first); },
      [&](size_t i) { return Slice(columns[i].second); });
}

Status WideColumnSerialization::SerializeV2(
    const WideColumns& columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
    std::string& output) {
  return SerializeV2Impl(
      columns.size(), blob_columns, output,
      [&](size_t i) { return columns[i].name(); },
      [&](size_t i) { return columns[i].value(); });
}

Status WideColumnSerialization::DeserializeV1(
    Slice& input, uint32_t num_columns, std::vector<WideColumn>& columns) {
  columns.reserve(num_columns);

  autovector<uint32_t, 16> column_value_sizes;
  column_value_sizes.reserve(num_columns);

  for (uint32_t i = 0; i < num_columns; ++i) {
    Slice name;
    if (!GetLengthPrefixedSlice(&input, &name)) {
      return Status::Corruption("Error decoding wide column name");
    }

    if (!columns.empty()) {
      if (Status so = ValidateColumnOrder(columns.back().name(), name);
          !so.ok()) {
        return so;
      }
    }

    columns.emplace_back(name, Slice());

    uint32_t value_size = 0;
    if (!GetVarint32(&input, &value_size)) {
      return Status::Corruption("Error decoding wide column value size");
    }

    column_value_sizes.emplace_back(value_size);
  }

  const Slice data(input);
  size_t pos = 0;

  for (uint32_t i = 0; i < num_columns; ++i) {
    const uint32_t value_size = column_value_sizes[i];

    if (pos + value_size > data.size()) {
      return Status::Corruption("Error decoding wide column value payload");
    }

    columns[i].value() = Slice(data.data() + pos, value_size);

    pos += value_size;
  }

  // Reject any trailing bytes after the last value: a serialized entity must be
  // exactly the whole input (V2 already enforces this via its section-size
  // check; this keeps V1 consistent).
  if (pos != data.size()) {
    return Status::Corruption(
        "Unexpected trailing data after wide column entity");
  }

  return Status::OK();
}

Status WideColumnSerialization::DeserializeV2Impl(
    Slice& input, uint32_t num_columns, std::vector<WideColumn>& columns,
    std::vector<ValueType>& column_types) {
  // Section 2: SKIP INFO (3 varints)
  uint32_t name_sizes_bytes = 0;
  uint32_t value_sizes_bytes = 0;
  uint32_t names_bytes = 0;
  if (!GetVarint32(&input, &name_sizes_bytes)) {
    return Status::Corruption("Error decoding wide column name sizes bytes");
  }
  if (!GetVarint32(&input, &value_sizes_bytes)) {
    return Status::Corruption("Error decoding wide column value sizes bytes");
  }
  if (!GetVarint32(&input, &names_bytes)) {
    return Status::Corruption("Error decoding wide column names bytes");
  }

  // Section 3: COLUMN TYPES (N bytes, each is a ValueType)
  if (input.size() < num_columns) {
    return Status::Corruption("Error decoding wide column types");
  }
  column_types.resize(num_columns);
  for (uint32_t i = 0; i < num_columns; ++i) {
    column_types[i] = static_cast<ValueType>(input[i]);
    if (!IsValidColumnValueType(column_types[i])) {
      return Status::Corruption("Unsupported wide column ValueType");
    }
  }
  input.remove_prefix(num_columns);

  // Validate that sections 4-6 fit in the remaining input. The three declared
  // section sizes are independent varint32s, so sum them in 64 bits: a uint32_t
  // sum wraps and lets a declared metadata size far larger than the input pass
  // this check, after which the section pointers below land out of bounds.
  const uint64_t metadata_size = lossless_cast<uint64_t>(name_sizes_bytes) +
                                 value_sizes_bytes + names_bytes;
  if (input.size() < metadata_size) {
    return Status::Corruption("Error decoding wide column sections");
  }

  // Set up 4 pointers into sections 4-7 for single-loop parsing.
  // Skip info gives us exact boundaries for each section.
  const char* s4 = input.data();  // section 4: name sizes
  const char* s4_limit = s4 + name_sizes_bytes;
  const char* s5 = s4_limit;  // section 5: value sizes
  const char* s5_limit = s5 + value_sizes_bytes;
  const char* s6 = s5_limit;          // section 6: names
  const char* s7 = s6 + names_bytes;  // section 7: values
  const char* input_end = input.data() + input.size();

  columns.reserve(num_columns);
  size_t name_pos = 0;
  size_t value_pos = 0;

  for (uint32_t i = 0; i < num_columns; ++i) {
    // Decode name size from section 4
    uint32_t ns = 0;
    const char* s4_next = GetVarint32Ptr(s4, s4_limit, &ns);
    if (s4_next == nullptr) {
      return Status::Corruption("Error decoding wide column name size");
    }
    s4 = s4_next;

    // Decode value size from section 5
    uint32_t vs = 0;
    const char* s5_next = GetVarint32Ptr(s5, s5_limit, &vs);
    if (s5_next == nullptr) {
      return Status::Corruption("Error decoding wide column value size");
    }
    s5 = s5_next;

    // Read name from section 6
    if (name_pos + ns > names_bytes) {
      return Status::Corruption("Error decoding wide column name");
    }
    Slice name(s6 + name_pos, ns);

    if (!columns.empty()) {
      if (Status so = ValidateColumnOrder(columns.back().name(), name);
          !so.ok()) {
        return so;
      }
    }

    // Read value from section 7
    if (s7 + value_pos + vs > input_end) {
      return Status::Corruption("Error decoding wide column value payload");
    }

    columns.emplace_back(name, Slice(s7 + value_pos, vs));
    name_pos += ns;
    value_pos += vs;
  }

  // Validate that the consumed bytes match the declared section sizes. Because
  // a V2 entity's values section is defined to extend to the end of `input`,
  // this also rejects any trailing bytes after the entity.
  const size_t total_values_bytes = static_cast<size_t>(input_end - s7);
  if (s4 != s4_limit || s5 != s5_limit || name_pos != names_bytes ||
      value_pos != total_values_bytes) {
    return Status::Corruption("Wide column section size mismatch");
  }

  return Status::OK();
}

Status WideColumnSerialization::Deserialize(
    const Slice& entity, WideColumns& columns,
    std::vector<std::pair<size_t, BlobIndex>>* blob_columns) {
  assert(columns.empty());
  assert(!blob_columns || blob_columns->empty());

  Slice input = entity;
  uint32_t version = 0;
  if (!GetVarint32(&input, &version)) {
    return Status::Corruption("Error decoding wide column version");
  }

  // A version newer than kVersion2 is reported as Corruption, consistent with
  // how the rest of the codebase classifies an unrecognized serialized
  // version/type (BlobIndex, blob log header, table footer).
  if (version > kVersion2) {
    return Status::Corruption("Corrupt or unsupported wide column version");
  }

  uint32_t num_columns = 0;
  if (!GetVarint32(&input, &num_columns)) {
    return Status::Corruption("Error decoding number of wide columns");
  }

  // A zero-column entity is still dispatched to the version-specific parser
  // below (rather than returning OK here) so that trailing bytes after an empty
  // entity are rejected as Corruption too -- both parsers handle num_columns==0
  // and validate that the input is exactly one serialized entity.
  if (version < kVersion2) {
    return DeserializeV1(input, num_columns, columns);
  }

  // V2 layout: parse columns and extract any blob column info.
  std::vector<ValueType> column_types;
  if (Status s = DeserializeV2Impl(input, num_columns, columns, column_types);
      !s.ok()) {
    return s;
  }
  assert(column_types.size() == num_columns);
  assert(columns.size() == num_columns);

  for (uint32_t i = 0; i < num_columns; ++i) {
    if (column_types[i] != kTypeBlobIndex) {
      continue;
    }

    if (!blob_columns) {
      // The caller promised a fully resolved entity; a blob reference here is
      // unexpected and may indicate corruption (or a logic error)
      return Status::Corruption(
          "Wide column blob reference in a context where it is not expected.");
    }

    BlobIndex blob_idx;
    Slice blob_slice = columns[i].value();
    if (blob_slice.empty()) {
      return Status::Corruption("Empty blob index in wide column");
    }
    if (Status bs = blob_idx.DecodeFrom(blob_slice); !bs.ok()) {
      return Status::Corruption("Error decoding blob index in wide column");
    }
    blob_columns->emplace_back(i, blob_idx);
  }

  return Status::OK();
}

Status WideColumnSerialization::HasBlobColumns(const Slice& input,
                                               bool& has_blob_columns) {
  has_blob_columns = false;

  Slice input_ref = input;

  uint32_t version = 0;
  if (!GetVarint32(&input_ref, &version)) {
    return Status::Corruption("Error decoding wide column version");
  }

  // Version 1 never has blob columns
  if (version < kVersion2) {
    return Status::OK();
  }
  if (version > kVersion2) {
    return Status::Corruption("Corrupt or unsupported wide column version");
  }

  uint32_t num_columns = 0;
  if (!GetVarint32(&input_ref, &num_columns)) {
    return Status::Corruption("Error decoding number of wide columns");
  }

  if (!num_columns) {
    return Status::OK();
  }

  // V2: Skip over SKIP INFO (3 varints) to reach COLUMN TYPES section.
  uint32_t unused_name_sizes_bytes = 0;
  uint32_t unused_value_sizes_bytes = 0;
  uint32_t unused_names_bytes = 0;
  if (!GetVarint32(&input_ref, &unused_name_sizes_bytes) ||
      !GetVarint32(&input_ref, &unused_value_sizes_bytes) ||
      !GetVarint32(&input_ref, &unused_names_bytes)) {
    return Status::Corruption("Error decoding wide column skip info");
  }
  if (input_ref.size() < num_columns) {
    return Status::Corruption("Error decoding wide column types");
  }
  for (uint32_t i = 0; i < num_columns; ++i) {
    if (!IsValidColumnValueType(static_cast<ValueType>(input_ref[i]))) {
      return Status::Corruption("Unsupported wide column ValueType");
    }
  }
  has_blob_columns = ContainsBlobType(input_ref.data(), num_columns);

  return Status::OK();
}

Status WideColumnSerialization::ForEachBlobFileNumber(
    const Slice& input,
    const std::function<Status(const BlobIndex&)>& callback) {
  Slice input_ref = input;

  uint32_t version = 0;
  if (!GetVarint32(&input_ref, &version)) {
    return Status::Corruption("Error decoding wide column version");
  }

  if (version < kVersion2) {
    return Status::OK();
  }
  if (version > kVersion2) {
    return Status::Corruption("Corrupt or unsupported wide column version");
  }

  uint32_t num_columns = 0;
  if (!GetVarint32(&input_ref, &num_columns)) {
    return Status::Corruption("Error decoding number of wide columns");
  }

  if (!num_columns) {
    return Status::OK();
  }

  // Read SKIP INFO
  uint32_t name_sizes_bytes = 0;
  uint32_t value_sizes_bytes = 0;
  uint32_t names_bytes = 0;
  if (!GetVarint32(&input_ref, &name_sizes_bytes) ||
      !GetVarint32(&input_ref, &value_sizes_bytes) ||
      !GetVarint32(&input_ref, &names_bytes)) {
    return Status::Corruption("Error decoding wide column skip info");
  }

  // Read COLUMN TYPES
  if (input_ref.size() < num_columns) {
    return Status::Corruption("Error decoding wide column types");
  }

  // Validate every column type, and note whether any blob columns are present.
  // An unrecognized/unsupported type byte is Corruption (consistent with
  // Deserialize() and HasBlobColumns()) rather than silently treated as a
  // non-blob column.
  bool has_any_blob = false;
  for (uint32_t i = 0; i < num_columns; ++i) {
    const auto type = lossless_cast<ValueType>(input_ref[i]);
    if (!IsValidColumnValueType(type)) {
      return Status::Corruption("Unsupported wide column ValueType");
    }
    if (type == kTypeBlobIndex) {
      has_any_blob = true;
    }
  }

  if (!has_any_blob) {
    return Status::OK();
  }

  // Need to skip to value sizes to find blob column value offsets,
  // then skip to the values section to decode blob indices.
  const char* type_bytes = input_ref.data();
  input_ref.remove_prefix(num_columns);

  // Skip NAME SIZES section
  if (input_ref.size() < name_sizes_bytes) {
    return Status::Corruption("Error decoding wide column name sizes");
  }
  input_ref.remove_prefix(name_sizes_bytes);

  // Read VALUE SIZES section to find blob column value offsets
  if (input_ref.size() < value_sizes_bytes) {
    return Status::Corruption("Error decoding wide column value sizes");
  }
  const char* vs_ptr = input_ref.data();
  const char* vs_limit = vs_ptr + value_sizes_bytes;
  input_ref.remove_prefix(value_sizes_bytes);

  // Skip NAMES section
  if (input_ref.size() < names_bytes) {
    return Status::Corruption("Error decoding wide column names");
  }
  input_ref.remove_prefix(names_bytes);

  // Now input_ref points to VALUES section. Walk through value sizes
  // and decode only blob column values.
  size_t value_pos = 0;
  for (uint32_t i = 0; i < num_columns; ++i) {
    uint32_t vs = 0;
    const char* vs_next = GetVarint32Ptr(vs_ptr, vs_limit, &vs);
    if (vs_next == nullptr) {
      return Status::Corruption("Error decoding wide column value size");
    }
    vs_ptr = vs_next;

    if (static_cast<uint8_t>(type_bytes[i]) == kTypeBlobIndex) {
      if (value_pos + vs > input_ref.size()) {
        return Status::Corruption("Error decoding wide column blob index");
      }
      Slice blob_slice(input_ref.data() + value_pos, vs);
      if (blob_slice.empty()) {
        return Status::Corruption("Empty blob index in wide column");
      }
      BlobIndex blob_idx;
      Status s = blob_idx.DecodeFrom(blob_slice);
      if (!s.ok()) {
        return Status::Corruption("Error decoding blob index in wide column");
      }
      Status cb_s = callback(blob_idx);
      if (!cb_s.ok()) {
        return cb_s;
      }
    }
    value_pos += vs;
  }

  return Status::OK();
}

Status WideColumnSerialization::GetVersion(const Slice& input,
                                           uint32_t& version) {
  Slice input_ref = input;

  version = 0;
  if (!GetVarint32(&input_ref, &version)) {
    return Status::Corruption("Error decoding wide column version");
  }

  return Status::OK();
}

Status WideColumnSerialization::GetValueOfDefaultColumn(
    const Slice& input, Slice& value, bool& is_blob_reference) {
  is_blob_reference = false;

  Slice input_ref = input;

  uint32_t version = 0;
  if (!GetVarint32(&input_ref, &version)) {
    return Status::Corruption("Error decoding wide column version");
  }

  // See Deserialize(): a too-new version is Corruption, not NotSupported.
  if (version > kVersion2) {
    return Status::Corruption("Corrupt or unsupported wide column version");
  }

  uint32_t num_columns = 0;
  if (!GetVarint32(&input_ref, &num_columns)) {
    return Status::Corruption("Error decoding number of wide columns");
  }

  if (!num_columns) {
    value.clear();
    return Status::OK();
  }

  if (version >= kVersion2) {
    // V2 fast path: use skip info to jump directly to values without
    // scanning through variable-length sections.

    // Read SKIP INFO (3 varints, immediately after header)
    uint32_t name_sizes_bytes = 0;
    uint32_t value_sizes_bytes = 0;
    uint32_t names_bytes = 0;
    if (!GetVarint32(&input_ref, &name_sizes_bytes)) {
      return Status::Corruption("Error decoding wide column name sizes bytes");
    }
    if (!GetVarint32(&input_ref, &value_sizes_bytes)) {
      return Status::Corruption("Error decoding wide column value sizes bytes");
    }
    if (!GetVarint32(&input_ref, &names_bytes)) {
      return Status::Corruption("Error decoding wide column names bytes");
    }

    // Read COLUMN TYPES (N bytes). We only need column 0's type here (it is the
    // default column's type only if column 0 is actually the default column,
    // checked via its name size below), but validate it is a recognized type so
    // a corrupt/unsupported type byte is reported as Corruption rather than
    // silently treated as an inline value.
    if (input_ref.size() < num_columns) {
      return Status::Corruption("Error decoding wide column types");
    }
    const auto column0_type = lossless_cast<ValueType>(input_ref[0]);
    if (!IsValidColumnValueType(column0_type)) {
      return Status::Corruption("Unsupported wide column ValueType");
    }
    const bool column0_is_blob = column0_type == kTypeBlobIndex;
    input_ref.remove_prefix(num_columns);

    // Peek first name size from NAME SIZES section
    if (input_ref.size() < name_sizes_bytes) {
      return Status::Corruption("Error decoding wide column name sizes");
    }
    Slice name_sizes_section(input_ref.data(), name_sizes_bytes);
    uint32_t first_name_size = 0;
    if (!GetVarint32(&name_sizes_section, &first_name_size)) {
      return Status::Corruption("Error decoding wide column name size");
    }
    input_ref.remove_prefix(name_sizes_bytes);

    // Peek first value size from VALUE SIZES section
    if (input_ref.size() < value_sizes_bytes) {
      return Status::Corruption("Error decoding wide column value sizes");
    }
    Slice value_sizes_section(input_ref.data(), value_sizes_bytes);
    uint32_t first_value_size = 0;
    if (!GetVarint32(&value_sizes_section, &first_value_size)) {
      return Status::Corruption("Error decoding wide column value size");
    }
    // Skip entire VALUE SIZES section using value_sizes_bytes
    input_ref.remove_prefix(value_sizes_bytes);

    // Check if the first column is the default column (empty name)
    if (first_name_size != 0) {
      value.clear();
      return Status::OK();
    }

    // Skip NAMES section
    if (input_ref.size() < names_bytes) {
      return Status::Corruption("Error decoding wide column names");
    }
    input_ref.remove_prefix(names_bytes);

    // Read the first value from VALUES section. For a blob-referenced default
    // column these are the raw serialized BlobIndex bytes.
    if (input_ref.size() < first_value_size) {
      return Status::Corruption("Error decoding wide column value payload");
    }
    value = Slice(input_ref.data(), first_value_size);
    is_blob_reference = column0_is_blob;
    return Status::OK();
  }

  // V1 fallback: full deserialization. V1 has no blob references.
  WideColumns columns;

  if (Status s = DeserializeSimple(input, columns); !s.ok()) {
    return s;
  }

  if (!WideColumnsHelper::HasDefaultColumn(columns)) {
    value.clear();
    return Status::OK();
  }

  value = WideColumnsHelper::GetDefaultColumn(columns);

  return Status::OK();
}

Status WideColumnSerialization::ResolveEntityBlobColumns(
    const Slice& entity_value, const Slice& user_key,
    const BlobFetcher* blob_fetcher, PrefetchBufferCollection* prefetch_buffers,
    std::string& resolved_entity, bool& resolved, uint64_t* total_bytes_read,
    uint64_t* num_blobs_resolved) {
  // Resolve into zero-copy columns backed by fetched blob buffers, then
  // serialize that result as a V1 (all-inline) entity.
  WideColumns resolved_columns;
  std::forward_list<PinnableSlice> extra_buffers;
  Status s = ResolveEntityBlobColumnsMultiBuffer(
      entity_value, user_key, blob_fetcher, prefetch_buffers, resolved_columns,
      extra_buffers, resolved, total_bytes_read, num_blobs_resolved);
  if (s.ok() && resolved) {
    s = Serialize(resolved_columns, resolved_entity);
  }
  return s;
}

Status WideColumnSerialization::ResolveEntityBlobColumnsMultiBuffer(
    const Slice& entity_value, const Slice& user_key,
    const BlobFetcher* blob_fetcher, PrefetchBufferCollection* prefetch_buffers,
    WideColumns& resolved_columns,
    std::forward_list<PinnableSlice>& extra_buffers, bool& resolved,
    uint64_t* total_bytes_read, uint64_t* num_blobs_resolved) {
  resolved = false;

  std::vector<WideColumn> columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;

  if (Status s = Deserialize(entity_value, columns, &blob_columns); !s.ok()) {
    return s;
  }

  if (blob_columns.empty()) {
    return Status::OK();
  }

  resolved = true;

  for (const auto& blob_col : blob_columns) {
    const size_t column_idx = blob_col.first;
    const BlobIndex& blob_idx = blob_col.second;

    if (blob_idx.IsInlined()) {
      // The inlined value bytes live in `entity_value`; point the column value
      // at them directly (zero copy). The caller keeps `entity_value` alive.
      columns[column_idx].value() = blob_idx.value();
      continue;
    }

    if (blob_fetcher == nullptr) {
      // A blob-backed entity reached a read context with no blob fetcher (a
      // reader without blob support, e.g. SstFileReader, or corrupt/legacy
      // data). This is the single place entity blob resolution reports that
      // condition; callers no longer pre-check the fetcher.
      return Status::Corruption(
          "Cannot resolve blob columns in entity without a blob fetcher");
    }

    FilePrefetchBuffer* prefetch_buffer =
        prefetch_buffers ? prefetch_buffers->GetOrCreatePrefetchBuffer(
                               blob_idx.file_number())
                         : nullptr;

    uint64_t bytes_read = 0;

    // Fetch the blob value into a fresh, address-stable backing node.
    extra_buffers.emplace_front();
    PinnableSlice& blob_value = extra_buffers.front();
    const Status fetch_s = blob_fetcher->FetchBlob(
        user_key, blob_idx, prefetch_buffer, &blob_value, &bytes_read);
    if (!fetch_s.ok()) {
      return fetch_s;
    }

    // Zero-copy Slice into the node we just added.
    columns[column_idx].value() = blob_value;

    if (total_bytes_read) {
      *total_bytes_read += bytes_read;
    }
  }

  if (num_blobs_resolved) {
    *num_blobs_resolved += blob_columns.size();
  }

  resolved_columns = std::move(columns);

  return Status::OK();
}

Status WideColumnSerialization::ResolveDefaultColumnBlobReference(
    const Slice& blob_index, const Slice& user_key,
    const BlobFetcher* blob_fetcher, PinnableSlice& value) {
  if (blob_index.empty()) {
    return Status::Corruption("Empty blob index in wide column default value");
  }

  BlobIndex blob_idx;
  if (Status s = blob_idx.DecodeFrom(blob_index); !s.ok()) {
    return Status::Corruption(
        "Error decoding blob index in wide column default value");
  }

  if (blob_idx.IsInlined()) {
    value.PinSelf(blob_idx.value());
    return Status::OK();
  }

  if (blob_fetcher == nullptr) {
    // A blob-backed default column reached a read context with no blob fetcher
    // (a reader without blob support, e.g. SstFileReader, or corrupt/legacy
    // data). This is the single place default-column blob resolution reports
    // that condition; callers no longer pre-check the fetcher.
    return Status::Corruption(
        "Cannot resolve blob-backed default column without a blob fetcher");
  }

  return blob_fetcher->FetchBlob(user_key, blob_idx,
                                 nullptr /* prefetch_buffer */, &value,
                                 nullptr /* bytes_read */);
}

Status WideColumnSerialization::ResolveEntityForMerge(
    const Slice& entity_value, const Slice& user_key,
    const BlobFetcher* blob_fetcher, PrefetchBufferCollection* prefetch_buffers,
    std::string& resolved_entity, Slice& effective_entity) {
  bool has_blob_columns = false;
  Status status = HasBlobColumns(entity_value, has_blob_columns);
  if (status.ok()) {
    if (!has_blob_columns) {
      effective_entity = entity_value;
    } else {
      // A null blob_fetcher with blob columns present is reported as Corruption
      // inside ResolveEntityBlobColumns -> ResolveEntityBlobColumnsMultiBuffer.
      bool resolved = false;
      status = ResolveEntityBlobColumns(
          entity_value, user_key, blob_fetcher, prefetch_buffers,
          resolved_entity, resolved, nullptr /* total_bytes_read */,
          nullptr /* num_blobs_resolved */);
      if (status.ok()) {
        effective_entity = resolved ? Slice(resolved_entity) : entity_value;
      }
    }
  }
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
