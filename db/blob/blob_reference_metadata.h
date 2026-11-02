// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "cache/cache_reservation_manager.h"
#include "db/blob/blob_constants.h"
#include "db/blob/blob_index.h"
#include "rocksdb/status.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

struct BlockBasedTableOptions;

// One concurrent, weakly registered domain per cache and metadata role. Both
// collectors and reopened summaries use owner handles; absolute reservations
// must not be used with this domain. Disabled charging returns nullptr.
std::shared_ptr<CacheReservationManager> GetBlobReferenceReservationManager(
    const BlockBasedTableOptions& options);

extern const char kBlobReferenceDescriptorPropertyName[];
extern const char kBlobReferenceIndexBlockName[];
extern const char kBlobReferenceDirectoryBlockName[];
extern const char kBlobReferenceClassificationPropertyName[];

// A tiny checksummed property for SSTs without an exact reference block.
// frontier == 0 means legacy-only; otherwise it conservatively pins feature
// identity roots at and above frontier. Coarse SSTs cannot name routes.
void EncodeBlobReferenceClassification(uint64_t frontier, std::string* output);
Status DecodeBlobReferenceClassification(const Slice& input,
                                         uint64_t* frontier);

inline constexpr uint64_t kMaxBlobReferencesPerSst = 16ULL * 1024 * 1024;

enum class BlobReferenceEncodingClass : uint8_t {
  kLegacyDirect = 0,
  kFeatureIdentity = 1,
  kRelocated = 2,
};

struct BlobReference {
  BlobReferenceEncodingClass encoding_class =
      BlobReferenceEncodingClass::kLegacyDirect;
  uint64_t generation_file_number = 0;
  uint64_t origin_file_number = 0;
  uint64_t original_value_offset = 0;
  uint64_t stored_value_size = 0;
  // Physical bytes occupied by this record in its direct generation. Normal
  // blob files include the record header and user key; relocation files
  // include the embedded-blob trailer. GC uses this exact denominator instead
  // of deriving it from the logical value size.
  uint64_t record_bytes = 0;
  CompressionType compression = kNoCompression;
  uint32_t stable_checksum_version = 0;
  uint32_t stable_checksum = 0;
  uint32_t stored_checksum = 0;

  static Status FromBlobIndex(const BlobIndex& blob_index,
                              BlobReference* reference, bool* included);

  bool HasStableIdentity() const {
    return encoding_class != BlobReferenceEncodingClass::kLegacyDirect;
  }
};

// Bounds eager decoded-reference vectors. Persisted blocks can contain more
// entries, but callers that need a materialized vector must fail with Status
// instead of expanding hostile metadata into unbounded process memory.
inline constexpr size_t kMaxDecodedBlobReferenceBytes = 128 * 1024 * 1024;

struct BlobReferenceBlockIndexEntry {
  BlockHandle handle;
  uint64_t first_reference = 0;
  uint64_t reference_count = 0;
  uint32_t payload_checksum = 0;
  // In-memory only. The persisted index inherits its version from the block
  // header and the enclosing descriptor.
  uint32_t format_version = 2;
};

struct BlobReferenceGroup {
  BlobReferenceEncodingClass encoding_class =
      BlobReferenceEncodingClass::kLegacyDirect;
  uint64_t generation_file_number = 0;
  uint64_t origin_file_number = 0;
  uint64_t first_reference = 0;
  uint64_t reference_count = 0;
  uint64_t stored_value_bytes = 0;
  uint64_t record_bytes = 0;
  uint64_t first_block = 0;
  uint64_t block_count = 0;
  uint32_t digest = 0;
};

// A block-bounded cursor over one immutable SST's exact references. Its table
// reader must outlive the cursor. Repair merges these cursors by stable offset
// and never builds per-SST vectors or a database-wide per-reference map.
class BlobReferenceIterator {
 public:
  virtual ~BlobReferenceIterator() = default;
  virtual Status SeekSource(BlobReferenceEncodingClass encoding_class,
                            uint64_t generation, uint64_t origin) = 0;
  virtual bool Valid() const = 0;
  virtual const BlobReference& reference() const = 0;
  virtual Status Next() = 0;
};

struct BlobReferenceMetadataDescriptor {
  static constexpr uint32_t kFormatVersion = 4;
  static constexpr uint32_t kOldestReadableFormatVersion = 1;
  static constexpr uint32_t kComplete = 1U;
  static constexpr size_t kLegacyEncodedSize = 88;
  static constexpr size_t kEncodedSize = 120;

  uint32_t flags = kComplete;
  BlockHandle index_handle;
  BlockHandle directory_handle;
  uint64_t reference_count = 0;
  uint64_t group_count = 0;
  uint64_t stored_value_bytes = 0;
  uint32_t data_digest = 0;
  uint32_t index_digest = 0;
  uint32_t directory_digest = 0;
  uint32_t column_family_id = std::numeric_limits<uint32_t>::max();
  uint32_t repair_metadata_format_version = 0;
  uint64_t route_snapshot_fingerprint_high = 0;
  uint64_t route_snapshot_fingerprint_low = 0;
  // The original/build file number stored in TableProperties, not necessarily
  // the current physical file name. CompactionService may rename an imported
  // SST while preserving its immutable contents and original file number.
  uint64_t sst_file_number = 0;
  // In-memory only. EncodeTo() always writes kFormatVersion, while DecodeFrom()
  // retains an older readable version so the block decoders can preserve its
  // semantics. Version 1 lacks exact physical record-byte accounting and is
  // therefore census-readable but ineligible for standalone relocation GC.
  uint32_t format_version = kFormatVersion;

  bool IsComplete() const { return (flags & kComplete) != 0; }
  bool IsCompleteEmpty() const {
    return IsComplete() && reference_count == 0 && group_count == 0 &&
           stored_value_bytes == 0;
  }
  bool HasExactRecordBytes() const { return format_version >= 2; }
  bool HasRepairMetadata() const {
    return format_version >= 3 &&
           repair_metadata_format_version == kBlobRepairMetadataFormatVersion &&
           column_family_id != std::numeric_limits<uint32_t>::max() &&
           sst_file_number != 0;
  }

  void EncodeTo(std::string* output) const;
  static Status DecodeFrom(const Slice& input,
                           BlobReferenceMetadataDescriptor* descriptor);
};

// Validated immutable group metadata shared by the finished builder, its SST
// metadata, and the asynchronous census. Exact per-record references stay on
// disk. The reservation follows this one shared object, not each owner.
struct BlobReferenceSstSummary {
  uint64_t file_number = 0;
  BlobReferenceMetadataDescriptor descriptor;
  std::shared_ptr<CacheReservationManager> reservation_manager;
  std::unique_ptr<CacheReservationManager::CacheReservationHandle>
      reservation_handle;
  // Destroy buffers before releasing their charge.
  std::vector<BlobReferenceGroup> groups;

  size_t MemoryUsage() const {
    return sizeof(*this) + groups.capacity() * sizeof(BlobReferenceGroup);
  }
};

class BlobReferenceMetadataCollector {
 public:
  using DataBlockWriter =
      std::function<Status(const Slice&, BlockHandle* block_handle)>;

  explicit BlobReferenceMetadataCollector(
      std::shared_ptr<CacheReservationManager> cache_reservation_manager);
  BlobReferenceMetadataCollector(
      std::shared_ptr<CacheReservationManager> cache_reservation_manager,
      uint32_t column_family_id, uint64_t sst_file_number,
      uint64_t route_snapshot_fingerprint_high,
      uint64_t route_snapshot_fingerprint_low);
  ~BlobReferenceMetadataCollector();
  BlobReferenceMetadataCollector(const BlobReferenceMetadataCollector&) =
      delete;
  BlobReferenceMetadataCollector& operator=(
      const BlobReferenceMetadataCollector&) = delete;
  BlobReferenceMetadataCollector(BlobReferenceMetadataCollector&&) = delete;
  BlobReferenceMetadataCollector& operator=(BlobReferenceMetadataCollector&&) =
      delete;

  // `user_key_size` is the size stored in a normal blob-log record. It is
  // ignored for references whose generation is already a relocation file.
  Status Add(const BlobIndex& blob_index, uint64_t user_key_size);
  Status Add(const BlobIndex& blob_index);

  // Sorts and validates the collected references, writes bounded data blocks
  // through `write_data_block`, and produces the index and group-directory
  // block contents. May be called exactly once.
  Status Finish(const DataBlockWriter& write_data_block);

  // Releases all collection and encoded-block buffers after the caller has
  // persisted the Finish() outputs, or when collection is abandoned. The
  // descriptor remains available and the collector accepts no later Add().
  Status ReleaseWorkingMemory();

  // Transfers the validated group buffer and its existing cache reservation
  // after Finish(). Releases all other collector buffers without a second
  // group copy. The returned handle is transient, never encoded in MANIFEST.
  Status TakeSstSummary(
      std::shared_ptr<const BlobReferenceSstSummary>* summary);

  const Status& status() const { return status_; }
  const std::string& index_block() const { return index_block_; }
  const std::string& directory_block() const { return directory_block_; }
  const BlobReferenceMetadataDescriptor& descriptor() const {
    return descriptor_;
  }
  BlobReferenceMetadataDescriptor* mutable_descriptor() { return &descriptor_; }

  // A conservative O(1) bound used by output-file size planning before the
  // collector is sorted. Charge record encoding per reference, but framing
  // per distinct (class, generation, origin) group, not per value. The small
  // group set is charged to the same building-buffer reservation.
  uint64_t EstimatedEncodedSize() const;
  uint64_t reference_count() const { return references_.size(); }

  size_t TEST_ReservedMemory() const { return ReservedMemory(); }

 private:
  struct EstimatedGroup {
    BlobReferenceEncodingClass encoding_class;
    uint64_t generation;
    uint64_t origin;

    bool operator==(const EstimatedGroup& rhs) const {
      return encoding_class == rhs.encoding_class &&
             generation == rhs.generation && origin == rhs.origin;
    }
  };
  struct EstimatedGroupHash {
    size_t operator()(const EstimatedGroup& group) const {
      size_t hash = std::hash<uint64_t>{}(group.generation);
      hash ^= std::hash<uint64_t>{}(group.origin) + 0x9e3779b9U + (hash << 6) +
              (hash >> 2);
      return hash ^ static_cast<size_t>(group.encoding_class);
    }
  };
  using EstimatedGroups =
      std::unordered_set<EstimatedGroup, EstimatedGroupHash>;
  Status AddEstimatedGroup(const BlobReference& reference);

  template <typename Container>
  Status ReserveCapacity(Container* container, size_t target_capacity) {
    if (target_capacity <= container->capacity()) {
      return Status::OK();
    }
    const size_t current_memory = ReservedMemory();
    if (target_capacity >
        (std::numeric_limits<size_t>::max() - current_memory) /
            sizeof(typename Container::value_type)) {
      return Status::MemoryLimit("Blob reference metadata capacity overflow");
    }
    // The old allocation remains alive while reserve() allocates the new one.
    const Status status = ResizeReservation(
        current_memory +
        target_capacity * sizeof(typename Container::value_type));
    if (!status.ok()) {
      return status;
    }
    container->reserve(target_capacity);
    return UpdateReservation();
  }

  Status UpdateReservation();
  Status ResizeReservation(size_t memory_used);
  size_t ReservedMemory() const;
  Status SortValidateAndDeduplicate();
  Status BuildAndWriteDataBlocks(const DataBlockWriter& write_data_block);
  Status EncodeIndexAndDirectory();

  std::shared_ptr<CacheReservationManager> cache_reservation_manager_;
  std::unique_ptr<CacheReservationManager::CacheReservationHandle>
      reservation_handle_;
  std::vector<BlobReference> references_;
  EstimatedGroups estimated_groups_;
  std::vector<BlobReferenceBlockIndexEntry> block_index_;
  std::vector<BlobReferenceGroup> groups_;
  std::string scratch_block_;
  std::string index_block_;
  std::string directory_block_;
  BlobReferenceMetadataDescriptor descriptor_;
  Status status_;
  bool finished_ = false;
};

Status DecodeBlobReferenceIndex(
    const Slice& input, const BlobReferenceMetadataDescriptor& descriptor,
    std::vector<BlobReferenceBlockIndexEntry>* block_index);

Status DecodeBlobReferenceDirectory(
    const Slice& input, const BlobReferenceMetadataDescriptor& descriptor,
    uint64_t data_block_count, std::vector<BlobReferenceGroup>* groups);

Status DecodeBlobReferenceDataBlock(
    const Slice& input, const BlobReferenceBlockIndexEntry& index_entry,
    std::vector<BlobReference>* references);

// Decodes directly into caller-reserved capacity and never allocates an
// additional per-block vector. On failure, restores the original output size.
Status AppendBlobReferenceDataBlock(
    const Slice& input, const BlobReferenceBlockIndexEntry& index_entry,
    std::vector<BlobReference>* references);

}  // namespace ROCKSDB_NAMESPACE
