// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/compression_type.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {

inline constexpr char kBlobRelocationDirectoryPropertyName[] =
    "rocksdb.blob.relocation-directory";

// A relocation file starts with this engine-owned discriminator. Its blocks
// and footer use normal block-based-table offsets after the prefix.
inline constexpr char kBlobRelocationFilePrefix[] = "RDBRLC01";
inline constexpr size_t kBlobRelocationFilePrefixSize =
    sizeof(kBlobRelocationFilePrefix) - 1;

struct BlobRelocationOrigin {
  uint64_t origin_file_number = 0;
  uint64_t first_original_offset = 0;
  uint64_t last_original_offset = 0;
  uint64_t entry_count = 0;
  uint64_t record_bytes = 0;
  uint32_t entries_digest = 0;
  // Generation components of every RouteSourceID this origin partition can
  // serve. The origin component is `origin_file_number` above.
  std::vector<uint64_t> served_generation_file_numbers;
};

class BlobRelocationDirectory {
 public:
  static constexpr uint32_t kFormatVersion = 2;
  static constexpr uint32_t kOldestReadableFormatVersion = 1;
  static constexpr uint64_t kMaxOrigins = 1ULL << 20;
  static constexpr uint64_t kMaxRouteSources = 1ULL << 20;

  void EncodeTo(std::string* output) const;
  static Status DecodeFrom(const Slice& input,
                           BlobRelocationDirectory* directory);

  // Rejects aggregates that cannot fit in the physical relocation file. The
  // prefix and table metadata consume additional bytes, so record bytes must
  // fit in the remaining capacity even before those structures are counted.
  Status ValidateForFileSize(uint64_t file_size) const;

  const BlobRelocationOrigin* Find(uint64_t origin_file_number) const;

  const std::vector<BlobRelocationOrigin>& origins() const { return origins_; }
  uint64_t total_entries() const { return total_entries_; }
  uint64_t total_record_bytes() const { return total_record_bytes_; }
  uint32_t directory_digest() const { return directory_digest_; }
  uint32_t format_version() const { return format_version_; }
  bool HasRepairMetadata() const;

 private:
  friend class BlobRelocationDirectoryBuilder;

  std::vector<BlobRelocationOrigin> origins_;
  uint64_t total_entries_ = 0;
  uint64_t total_record_bytes_ = 0;
  uint32_t directory_digest_ = 0;
  uint32_t format_version_ = kFormatVersion;
};

// Accumulates the directory while records are appended in relocation-key
// order. It does not retain one object per value.
class BlobRelocationDirectoryBuilder {
 public:
  Status AddServedRouteSource(uint64_t origin_file_number,
                              uint64_t generation_file_number);
  Status Add(uint64_t origin_file_number, uint64_t original_value_offset,
             uint64_t relocated_value_offset, uint64_t stored_value_size,
             CompressionType compression);
  Status Finish(BlobRelocationDirectory* directory);

 private:
  Status FinishCurrentOrigin();

  BlobRelocationDirectory directory_;
  BlobRelocationOrigin current_;
  bool has_current_ = false;
  bool finished_ = false;
  std::vector<std::pair<uint64_t, uint64_t>> served_route_sources_;
};

// Fixed-width big-endian components preserve numeric pair ordering under the
// bytewise comparator used by relocation files.
std::string EncodeBlobRelocationKey(uint64_t origin_file_number,
                                    uint64_t original_value_offset);
Status DecodeBlobRelocationKey(const Slice& key, uint64_t* origin_file_number,
                               uint64_t* original_value_offset);

// Keeps the source block sizing/cache while removing user-key-specific table
// features that do not apply to the relocation key space.
BlockBasedTableOptions MakeBlobRelocationTableOptions(
    const BlockBasedTableOptions& source);

}  // namespace ROCKSDB_NAMESPACE
