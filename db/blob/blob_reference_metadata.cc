// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_reference_metadata.h"

#include <algorithm>
#include <limits>
#include <map>
#include <mutex>
#include <tuple>
#include <utility>

#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_log_format.h"
#include "db/version_edit.h"
#include "rocksdb/table.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

std::shared_ptr<CacheReservationManager> GetBlobReferenceReservationManager(
    const BlockBasedTableOptions& options) {
  const CacheEntryRoleOptions::Decision charged =
      options.cache_usage_options.options_overrides
          .at(CacheEntryRole::kBlobReferenceMetadataBuildingBuffer)
          .charged;
  if (options.block_cache == nullptr ||
      (charged != CacheEntryRoleOptions::Decision::kEnabled &&
       charged != CacheEntryRoleOptions::Decision::kFallback)) {
    return nullptr;
  }
  // DB/cache owners may outlive other statics during process teardown.
  static auto* mutex = new std::mutex;
  static auto* domains =
      new std::map<Cache*, std::weak_ptr<CacheReservationManager>>;
  std::lock_guard<std::mutex> lock(*mutex);
  const std::map<Cache*, std::weak_ptr<CacheReservationManager>>::iterator
      found = domains->find(options.block_cache.get());
  if (found != domains->end()) {
    std::shared_ptr<CacheReservationManager> domain = found->second.lock();
    if (domain != nullptr) {
      return domain;
    }
  }
  // Remove dead cache domains only when creating one, not on the common hit.
  for (std::map<Cache*, std::weak_ptr<CacheReservationManager>>::iterator it =
           domains->begin();
       it != domains->end();) {
    if (it->second.expired()) {
      it = domains->erase(it);
    } else {
      ++it;
    }
  }
  std::shared_ptr<CacheReservationManager> domain =
      std::make_shared<ConcurrentCacheReservationManager>(
          std::make_shared<CacheReservationManagerImpl<
              CacheEntryRole::kBlobReferenceMetadataBuildingBuffer>>(
              options.block_cache));
  domains->emplace(options.block_cache.get(), domain);
  return domain;
}

const char kBlobReferenceDescriptorPropertyName[] =
    "rocksdb.blob.reference-descriptor";
const char kBlobReferenceIndexBlockName[] = "rocksdb.blob.reference-index";
const char kBlobReferenceDirectoryBlockName[] =
    "rocksdb.blob.reference-directory";
const char kBlobReferenceClassificationPropertyName[] =
    "rocksdb.blob.reference-classification";

void EncodeBlobReferenceClassification(uint64_t frontier, std::string* output) {
  output->clear();
  PutFixed32(output, 1);  // Format version.
  PutFixed64(output, frontier);
  PutFixed32(output, crc32c::Value(output->data(), output->size()));
}

Status DecodeBlobReferenceClassification(const Slice& input,
                                         uint64_t* frontier) {
  if (frontier == nullptr) {
    return Status::InvalidArgument("Null coarse blob frontier output");
  }
  if (input.size() != 16 || DecodeFixed32(input.data()) != 1 ||
      crc32c::Value(input.data(), 12) != DecodeFixed32(input.data() + 12)) {
    return Status::Corruption("Invalid blob reference classification");
  }
  *frontier = DecodeFixed64(input.data() + 4);
  if (*frontier > kFileNumberMask) {
    return Status::Corruption("Invalid coarse blob identity frontier");
  }
  return Status::OK();
}

namespace {

constexpr uint32_t kDescriptorMagic = 0x42475244U;  // BGRD
constexpr uint32_t kDataBlockMagic = 0x42475242U;   // BGRB
constexpr uint32_t kIndexBlockMagic = 0x42475249U;  // BGRI
constexpr uint32_t kDirectoryMagic = 0x42475247U;   // BGRG
constexpr uint32_t kMetadataFormatVersion =
    BlobReferenceMetadataDescriptor::kFormatVersion;
constexpr size_t kDataBlockHeaderSize = 16;
constexpr size_t kTargetDataBlockSize = 64 * 1024;
constexpr size_t kMaxDataBlockSize = 256 * 1024;
constexpr size_t kMaxTopLevelMetadataBlockSize = 64 * 1024 * 1024;
constexpr uint64_t kMaxReferenceCount = kMaxBlobReferencesPerSst;
// These persisted count limits must not depend on C++ ABI layout. Keep the
// corresponding decoded vectors separately bounded below.
constexpr uint64_t kMaxGroupCount = 1ULL << 20;
constexpr uint64_t kMaxDataBlockCount = 1ULL << 20;
constexpr size_t kMaxDecodedIndexBytes = 128 * 1024 * 1024;
constexpr size_t kMaxDecodedDirectoryBytes = 128 * 1024 * 1024;
static_assert(sizeof(BlobReferenceBlockIndexEntry) <= 128);
static_assert(sizeof(BlobReferenceGroup) <= 128);

bool CanAllocateDecodedVector(uint64_t count, size_t element_size,
                              size_t byte_limit) {
  return element_size != 0 && count <= std::numeric_limits<size_t>::max() &&
         count <= byte_limit / element_size;
}

bool IsValidCompression(CompressionType compression) {
  return compression <= kLastBuiltinCompression ||
         (compression >= kFirstCustomCompression &&
          compression <= kLastCustomCompression);
}

bool IsValidEncodingClass(BlobReferenceEncodingClass encoding_class) {
  return encoding_class >= BlobReferenceEncodingClass::kLegacyDirect &&
         encoding_class <= BlobReferenceEncodingClass::kRelocated;
}

bool IsValidGroupIdentity(BlobReferenceEncodingClass encoding_class,
                          uint64_t generation_file_number,
                          uint64_t origin_file_number) {
  if (!IsValidEncodingClass(encoding_class) || generation_file_number == 0 ||
      origin_file_number == 0) {
    return false;
  }
  const bool same_file = generation_file_number == origin_file_number;
  return encoding_class == BlobReferenceEncodingClass::kRelocated ? !same_file
                                                                  : same_file;
}

bool GetCanonicalVarint32(Slice* input, uint32_t* value) {
  if (input == nullptr || value == nullptr) {
    return false;
  }
  const size_t before = input->size();
  return GetVarint32(input, value) &&
         before - input->size() == VarintLength(*value);
}

bool GetCanonicalVarint64(Slice* input, uint64_t* value) {
  if (input == nullptr || value == nullptr) {
    return false;
  }
  const size_t before = input->size();
  return GetVarint64(input, value) &&
         before - input->size() == VarintLength(*value);
}

bool AddWithoutOverflow(uint64_t lhs, uint64_t rhs, uint64_t* sum) {
  if (sum == nullptr || rhs > std::numeric_limits<uint64_t>::max() - lhs) {
    return false;
  }
  *sum = lhs + rhs;
  return true;
}

auto StableSortKey(const BlobReference& reference) {
  return std::tie(reference.origin_file_number, reference.original_value_offset,
                  reference.encoding_class, reference.generation_file_number,
                  reference.stored_value_size, reference.record_bytes,
                  reference.compression, reference.stable_checksum_version,
                  reference.stable_checksum, reference.stored_checksum);
}

auto CanonicalSortKey(const BlobReference& reference) {
  return std::tie(reference.encoding_class, reference.generation_file_number,
                  reference.origin_file_number, reference.original_value_offset,
                  reference.stored_value_size, reference.record_bytes,
                  reference.compression, reference.stable_checksum_version,
                  reference.stable_checksum, reference.stored_checksum);
}

bool SameReference(const BlobReference& lhs, const BlobReference& rhs) {
  return CanonicalSortKey(lhs) == CanonicalSortKey(rhs);
}

bool SameGroup(const BlobReference& lhs, const BlobReference& rhs) {
  return std::tie(lhs.encoding_class, lhs.generation_file_number,
                  lhs.origin_file_number) ==
         std::tie(rhs.encoding_class, rhs.generation_file_number,
                  rhs.origin_file_number);
}

bool SameStableIdentity(const BlobReference& lhs, const BlobReference& rhs) {
  return lhs.HasStableIdentity() && rhs.HasStableIdentity() &&
         lhs.origin_file_number == rhs.origin_file_number &&
         lhs.original_value_offset == rhs.original_value_offset;
}

bool ConflictingImmutableFields(const BlobReference& lhs,
                                const BlobReference& rhs) {
  return lhs.stored_value_size != rhs.stored_value_size ||
         lhs.record_bytes != rhs.record_bytes ||
         lhs.compression != rhs.compression ||
         lhs.stable_checksum_version != rhs.stable_checksum_version ||
         lhs.stable_checksum != rhs.stable_checksum ||
         lhs.stored_checksum != rhs.stored_checksum;
}

void EncodeReferenceFields(const BlobReference& reference,
                           std::string* output) {
  assert(output != nullptr);
  PutVarint64(output, reference.stored_value_size);
  PutVarint64(output, reference.record_bytes);
  output->push_back(static_cast<char>(reference.compression));
  PutVarint32(output, reference.stable_checksum_version);
  if (reference.stable_checksum_version != 0) {
    PutFixed32(output, reference.stable_checksum);
  }
  if (reference.stable_checksum_version == BlobIndex::kStableChecksumVersion2 &&
      reference.compression != kNoCompression) {
    PutFixed32(output, reference.stored_checksum);
  }
}

Status ValidateReference(const BlobReference& reference,
                         bool require_exact_record_bytes) {
  if (!IsValidEncodingClass(reference.encoding_class)) {
    return Status::Corruption("Invalid blob reference encoding class");
  }
  if (reference.generation_file_number == 0 ||
      reference.origin_file_number == 0) {
    return Status::Corruption("Invalid blob reference file number");
  }
  if (!IsValidCompression(reference.compression)) {
    return Status::Corruption("Invalid blob reference compression");
  }
  if (require_exact_record_bytes &&
      reference.record_bytes < reference.stored_value_size) {
    return Status::Corruption("Invalid blob reference record size");
  }
  if (!require_exact_record_bytes && reference.record_bytes != 0) {
    return Status::Corruption("Unexpected legacy blob reference record size");
  }
  if (reference.encoding_class == BlobReferenceEncodingClass::kLegacyDirect) {
    if (reference.generation_file_number != reference.origin_file_number ||
        reference.stable_checksum_version != 0 ||
        reference.stable_checksum != 0 || reference.stored_checksum != 0) {
      return Status::Corruption("Invalid legacy blob reference metadata");
    }
  } else {
    if (!BlobIndex::IsSupportedStableChecksumVersion(
            reference.stable_checksum_version)) {
      return Status::Corruption("Unsupported stable checksum version");
    }
    if ((reference.stable_checksum_version ==
             BlobIndex::kStableChecksumVersion1 &&
         reference.stored_checksum != 0) ||
        (reference.stable_checksum_version ==
             BlobIndex::kStableChecksumVersion2 &&
         reference.compression == kNoCompression &&
         reference.stored_checksum !=
             BlobIndex::ExtendChecksumWithCompression(reference.stable_checksum,
                                                      kNoCompression))) {
      return Status::Corruption("Noncanonical stored blob checksum");
    }
    const bool is_identity =
        reference.generation_file_number == reference.origin_file_number;
    if (is_identity != (reference.encoding_class ==
                        BlobReferenceEncodingClass::kFeatureIdentity)) {
      return Status::Corruption("Blob reference encoding class mismatch");
    }
    if (require_exact_record_bytes &&
        reference.encoding_class == BlobReferenceEncodingClass::kRelocated &&
        (reference.stored_value_size > std::numeric_limits<uint64_t>::max() -
                                           kSimpleGen2BlobTrailerSize ||
         reference.record_bytes !=
             reference.stored_value_size + kSimpleGen2BlobTrailerSize)) {
      return Status::Corruption("Invalid relocated blob record size");
    }
  }
  return Status::OK();
}

void SetFixed32(std::string* output, size_t offset, uint32_t value) {
  assert(output != nullptr);
  assert(offset + sizeof(value) <= output->size());
  EncodeFixed32(output->data() + offset, value);
}

Status DecodeCommonBlockHeader(Slice* input, uint32_t expected_magic,
                               uint32_t expected_version,
                               uint32_t* body_checksum) {
  if (input == nullptr || body_checksum == nullptr) {
    return Status::InvalidArgument("Null blob reference metadata output");
  }
  uint32_t magic = 0;
  uint32_t version = 0;
  if (!GetFixed32(input, &magic) || !GetFixed32(input, &version) ||
      !GetFixed32(input, body_checksum)) {
    return Status::Corruption("Truncated blob reference metadata header");
  }
  if (magic != expected_magic) {
    return Status::Corruption("Blob reference metadata magic mismatch");
  }
  if (version != expected_version ||
      version < BlobReferenceMetadataDescriptor::kOldestReadableFormatVersion ||
      version > BlobReferenceMetadataDescriptor::kFormatVersion) {
    return Status::Corruption("Unsupported blob reference metadata version");
  }
  if (crc32c::Value(input->data(), input->size()) != *body_checksum) {
    return Status::Corruption("Blob reference metadata checksum mismatch");
  }
  return Status::OK();
}

}  // namespace

Status BlobReference::FromBlobIndex(const BlobIndex& blob_index,
                                    BlobReference* reference, bool* included) {
  if (reference == nullptr || included == nullptr) {
    return Status::InvalidArgument("Null blob reference output");
  }
  *included = false;
  Status status = blob_index.Validate();
  if (!status.ok()) {
    return status;
  }
  if (blob_index.IsInlined() || blob_index.IsSameFile()) {
    return Status::OK();
  }
  if (blob_index.file_number() == 0) {
    return Status::Corruption("Invalid external blob file number");
  }

  BlobReference result;
  if (blob_index.IsIndirect()) {
    result.generation_file_number = blob_index.generation_file_number();
    result.origin_file_number = blob_index.origin_file_number();
    result.original_value_offset = blob_index.original_value_offset();
    result.stable_checksum_version = blob_index.stable_checksum_version();
    result.stable_checksum = blob_index.stable_checksum();
    result.stored_checksum = blob_index.stored_checksum();
    result.encoding_class =
        result.generation_file_number == result.origin_file_number
            ? BlobReferenceEncodingClass::kFeatureIdentity
            : BlobReferenceEncodingClass::kRelocated;
  } else {
    result.encoding_class = BlobReferenceEncodingClass::kLegacyDirect;
    result.generation_file_number = blob_index.file_number();
    result.origin_file_number = blob_index.file_number();
    result.original_value_offset = blob_index.offset();
  }
  result.stored_value_size = blob_index.size();
  result.compression = blob_index.compression();
  // FromBlobIndex() validates the self-contained BlobIndex fields before the
  // collector adds the normal blob-log key/header contribution. Relocation
  // entries already have a key-independent physical record size.
  if (result.encoding_class == BlobReferenceEncodingClass::kRelocated) {
    if (result.stored_value_size >
        std::numeric_limits<uint64_t>::max() - kSimpleGen2BlobTrailerSize) {
      return Status::Corruption("Relocated blob record size overflow");
    }
    result.record_bytes = result.stored_value_size + kSimpleGen2BlobTrailerSize;
  } else {
    result.record_bytes = result.stored_value_size;
  }

  status = ValidateReference(result, /*require_exact_record_bytes=*/true);
  if (!status.ok()) {
    return status;
  }
  *reference = result;
  *included = true;
  return Status::OK();
}

void BlobReferenceMetadataDescriptor::EncodeTo(std::string* output) const {
  assert(output != nullptr);
  output->clear();
  output->reserve(kEncodedSize);
  PutFixed32(output, kDescriptorMagic);
  PutFixed32(output, kFormatVersion);
  PutFixed32(output, flags);
  PutFixed32(output, 0);
  PutFixed64(output, index_handle.offset());
  PutFixed64(output, index_handle.size());
  PutFixed64(output, directory_handle.offset());
  PutFixed64(output, directory_handle.size());
  PutFixed64(output, reference_count);
  PutFixed64(output, group_count);
  PutFixed64(output, stored_value_bytes);
  PutFixed32(output, data_digest);
  PutFixed32(output, index_digest);
  PutFixed32(output, directory_digest);
  PutFixed32(output, column_family_id);
  PutFixed32(output, repair_metadata_format_version);
  PutFixed64(output, route_snapshot_fingerprint_high);
  PutFixed64(output, route_snapshot_fingerprint_low);
  PutFixed64(output, sst_file_number);
  PutFixed32(output, 0);
  assert(output->size() == kEncodedSize);
  SetFixed32(output, kEncodedSize - sizeof(uint32_t),
             crc32c::Value(output->data(), kEncodedSize - sizeof(uint32_t)));
}

Status BlobReferenceMetadataDescriptor::DecodeFrom(
    const Slice& input, BlobReferenceMetadataDescriptor* descriptor) {
  if (descriptor == nullptr) {
    return Status::InvalidArgument("Null blob reference descriptor output");
  }
  if (input.size() != kLegacyEncodedSize && input.size() != kEncodedSize) {
    return Status::Corruption("Invalid blob reference descriptor size");
  }
  Slice cursor = input;
  uint32_t magic = 0;
  uint32_t version = 0;
  uint32_t reserved = 0;
  uint32_t descriptor_checksum = 0;
  BlobReferenceMetadataDescriptor decoded;
  uint64_t index_offset = 0;
  uint64_t index_size = 0;
  uint64_t directory_offset = 0;
  uint64_t directory_size = 0;
  if (!GetFixed32(&cursor, &magic) || !GetFixed32(&cursor, &version) ||
      !GetFixed32(&cursor, &decoded.flags) || !GetFixed32(&cursor, &reserved) ||
      reserved != 0 || !GetFixed64(&cursor, &index_offset) ||
      !GetFixed64(&cursor, &index_size) ||
      !GetFixed64(&cursor, &directory_offset) ||
      !GetFixed64(&cursor, &directory_size) ||
      !GetFixed64(&cursor, &decoded.reference_count) ||
      !GetFixed64(&cursor, &decoded.group_count) ||
      !GetFixed64(&cursor, &decoded.stored_value_bytes) ||
      !GetFixed32(&cursor, &decoded.data_digest) ||
      !GetFixed32(&cursor, &decoded.index_digest) ||
      !GetFixed32(&cursor, &decoded.directory_digest)) {
    return Status::Corruption("Malformed blob reference descriptor");
  }
  if (version >= 3) {
    if (input.size() != kEncodedSize ||
        !GetFixed32(&cursor, &decoded.column_family_id) ||
        !GetFixed32(&cursor, &decoded.repair_metadata_format_version) ||
        !GetFixed64(&cursor, &decoded.route_snapshot_fingerprint_high) ||
        !GetFixed64(&cursor, &decoded.route_snapshot_fingerprint_low) ||
        !GetFixed64(&cursor, &decoded.sst_file_number)) {
      return Status::Corruption("Malformed blob repair descriptor");
    }
  } else if (input.size() != kLegacyEncodedSize) {
    return Status::Corruption("Invalid legacy blob reference descriptor size");
  }
  if (!GetFixed32(&cursor, &descriptor_checksum) || !cursor.empty()) {
    return Status::Corruption("Malformed blob reference descriptor checksum");
  }
  if (crc32c::Value(input.data(), input.size() - sizeof(uint32_t)) !=
      descriptor_checksum) {
    return Status::Corruption("Blob reference descriptor checksum mismatch");
  }
  if (magic != kDescriptorMagic || version < kOldestReadableFormatVersion ||
      version > kFormatVersion) {
    return Status::Corruption("Unsupported blob reference descriptor");
  }
  if (decoded.flags != kComplete) {
    return Status::Corruption("Incomplete blob reference descriptor");
  }
  if (decoded.reference_count > kMaxReferenceCount ||
      decoded.group_count > kMaxGroupCount ||
      decoded.group_count > decoded.reference_count) {
    return Status::Corruption("Blob reference descriptor exceeds limits");
  }
  decoded.index_handle = BlockHandle(index_offset, index_size);
  decoded.directory_handle = BlockHandle(directory_offset, directory_size);
  decoded.format_version = version;
  if (version >= 3 && !decoded.HasRepairMetadata()) {
    return Status::Corruption("Incomplete blob repair descriptor");
  }
  uint64_t index_end = 0;
  uint64_t directory_end = 0;
  if (decoded.index_handle.IsNull() || decoded.directory_handle.IsNull() ||
      index_size == 0 || directory_size == 0 ||
      index_size > kMaxTopLevelMetadataBlockSize ||
      directory_size > kMaxTopLevelMetadataBlockSize ||
      !AddWithoutOverflow(index_offset, index_size, &index_end) ||
      !AddWithoutOverflow(directory_offset, directory_size, &directory_end) ||
      index_end >= directory_offset) {
    return Status::Corruption("Invalid blob reference metadata handle");
  }
  if (decoded.reference_count == 0 &&
      (decoded.group_count != 0 || decoded.stored_value_bytes != 0)) {
    return Status::Corruption("Invalid complete-empty blob descriptor");
  }
  *descriptor = decoded;
  return Status::OK();
}

BlobReferenceMetadataCollector::BlobReferenceMetadataCollector(
    std::shared_ptr<CacheReservationManager> cache_reservation_manager)
    : BlobReferenceMetadataCollector(std::move(cache_reservation_manager),
                                     /*column_family_id=*/0,
                                     /*sst_file_number=*/1,
                                     /*route_fingerprint_high=*/0,
                                     /*route_fingerprint_low=*/0) {}

BlobReferenceMetadataCollector::BlobReferenceMetadataCollector(
    std::shared_ptr<CacheReservationManager> cache_reservation_manager,
    uint32_t column_family_id, uint64_t sst_file_number,
    uint64_t route_snapshot_fingerprint_high,
    uint64_t route_snapshot_fingerprint_low)
    : cache_reservation_manager_(std::move(cache_reservation_manager)) {
  descriptor_.column_family_id = column_family_id;
  descriptor_.repair_metadata_format_version = kBlobRepairMetadataFormatVersion;
  descriptor_.route_snapshot_fingerprint_high = route_snapshot_fingerprint_high;
  descriptor_.route_snapshot_fingerprint_low = route_snapshot_fingerprint_low;
  descriptor_.sst_file_number = sst_file_number;
}

BlobReferenceMetadataCollector::~BlobReferenceMetadataCollector() {
  // Abandon() can destroy a collector before Add/Finish ever checks status.
  status_.PermitUncheckedError();
  ReleaseWorkingMemory().PermitUncheckedError();
}

uint64_t BlobReferenceMetadataCollector::EstimatedEncodedSize() const {
  constexpr uint64_t kFixedOverhead = 4096;
  // Three uint64 varints, compression, checksum version, and two CRCs fit
  // in 44 bytes. The extra byte amortizes additional 64-KiB fragments. Each
  // distinct group separately pays for its first data-block header, trailer,
  // index entry and directory entry. Duplicates are conservatively counted.
  constexpr uint64_t kWorstCaseBytesPerReference = 45;
  constexpr uint64_t kWorstCaseBytesPerGroup = 211;
  if (references_.size() >
      (std::numeric_limits<uint64_t>::max() - kFixedOverhead) /
          kWorstCaseBytesPerReference) {
    return std::numeric_limits<uint64_t>::max();
  }
  const uint64_t reference_bytes =
      kFixedOverhead + references_.size() * kWorstCaseBytesPerReference;
  if (estimated_groups_.size() >
      (std::numeric_limits<uint64_t>::max() - reference_bytes) /
          kWorstCaseBytesPerGroup) {
    return std::numeric_limits<uint64_t>::max();
  }
  return reference_bytes + estimated_groups_.size() * kWorstCaseBytesPerGroup;
}

Status BlobReferenceMetadataCollector::AddEstimatedGroup(
    const BlobReference& reference) {
  const EstimatedGroup group{reference.encoding_class,
                             reference.generation_file_number,
                             reference.origin_file_number};
  if (estimated_groups_.find(group) != estimated_groups_.end()) {
    return Status::OK();
  }
  constexpr size_t kNodeBytes = sizeof(EstimatedGroup) + 4 * sizeof(void*);
  const size_t group_count = estimated_groups_.size() + 1;
  const bool grow_buckets = group_count > estimated_groups_.bucket_count();
  const size_t target = grow_buckets ? group_count * 2 : 0;
  const size_t bucket_bytes = target * 4 * sizeof(void*);
  const size_t memory = ReservedMemory();
  // Collection already bounds group_count by the per-SST reference limit.
  // Cover a new node and simultaneous old/new bucket arrays before allocation.
  if (bucket_bytes > std::numeric_limits<size_t>::max() - kNodeBytes ||
      bucket_bytes + kNodeBytes > std::numeric_limits<size_t>::max() - memory) {
    return Status::MemoryLimit("Blob reference group estimate size overflow");
  }
  const Status status = ResizeReservation(memory + kNodeBytes + bucket_bytes);
  if (!status.ok()) {
    return status;
  }
  if (grow_buckets) {
    estimated_groups_.reserve(target);
  }
  estimated_groups_.insert(group);
  return UpdateReservation();
}

Status BlobReferenceMetadataCollector::Add(const BlobIndex& blob_index) {
  return Add(blob_index, /*user_key_size=*/0);
}

Status BlobReferenceMetadataCollector::Add(const BlobIndex& blob_index,
                                           uint64_t user_key_size) {
  if (!status_.ok()) {
    return status_;
  }
  if (finished_) {
    status_ = Status::InvalidArgument(
        "Cannot add to finished blob reference metadata collector");
    return status_;
  }

  BlobReference reference;
  bool included = false;
  status_ = BlobReference::FromBlobIndex(blob_index, &reference, &included);
  if (!status_.ok() || !included) {
    return status_;
  }
  if (reference.encoding_class == BlobReferenceEncodingClass::kRelocated) {
    if (reference.stored_value_size >
        std::numeric_limits<uint64_t>::max() - kSimpleGen2BlobTrailerSize) {
      status_ = Status::Corruption("Relocated blob record size overflow");
      return status_;
    }
    reference.record_bytes =
        reference.stored_value_size + kSimpleGen2BlobTrailerSize;
  } else {
    const uint64_t adjustment =
        BlobLogRecord::CalculateAdjustmentForRecordHeader(user_key_size);
    if (reference.stored_value_size >
        std::numeric_limits<uint64_t>::max() - adjustment) {
      status_ = Status::Corruption("Blob log record size overflow");
      return status_;
    }
    reference.record_bytes = reference.stored_value_size + adjustment;
  }
  if (references_.size() >= kMaxReferenceCount) {
    status_ = Status::MemoryLimit("Too many blob references in one SST");
    return status_;
  }
  if (references_.size() == references_.capacity()) {
    const size_t target_capacity =
        references_.capacity() == 0 ? 1 : references_.capacity() * 2;
    status_ = ReserveCapacity(&references_, target_capacity);
    if (!status_.ok()) {
      return status_;
    }
  }
  status_ = AddEstimatedGroup(reference);
  if (!status_.ok()) {
    return status_;
  }
  references_.push_back(reference);
  return status_;
}

Status BlobReferenceMetadataCollector::Finish(
    const DataBlockWriter& write_data_block) {
  if (!status_.ok()) {
    return status_;
  }
  if (finished_) {
    status_ = Status::InvalidArgument(
        "Blob reference metadata collector already finished");
    return status_;
  }
  finished_ = true;
  if (!write_data_block) {
    status_ = Status::InvalidArgument("Missing blob reference block writer");
    return status_;
  }

  status_ = SortValidateAndDeduplicate();
  if (status_.ok()) {
    status_ = BuildAndWriteDataBlocks(write_data_block);
  }
  if (status_.ok()) {
    status_ = EncodeIndexAndDirectory();
  }
  if (status_.ok()) {
    status_ = UpdateReservation();
  }
  return status_;
}

Status BlobReferenceMetadataCollector::ReleaseWorkingMemory() {
  finished_ = true;
  std::vector<BlobReference>().swap(references_);
  EstimatedGroups().swap(estimated_groups_);
  std::vector<BlobReferenceBlockIndexEntry>().swap(block_index_);
  std::vector<BlobReferenceGroup>().swap(groups_);
  std::string().swap(scratch_block_);
  std::string().swap(index_block_);
  std::string().swap(directory_block_);
  if (cache_reservation_manager_ == nullptr) {
    return Status::OK();
  }
  reservation_handle_.reset();
  return Status::OK();
}

Status BlobReferenceMetadataCollector::TakeSstSummary(
    std::shared_ptr<const BlobReferenceSstSummary>* summary) {
  if (summary == nullptr || !finished_ || !status_.ok()) {
    return Status::InvalidArgument("Blob reference summary is not finished");
  }
  std::shared_ptr<BlobReferenceSstSummary> result =
      std::make_shared<BlobReferenceSstSummary>();
  result->file_number = descriptor_.sst_file_number;
  result->descriptor = descriptor_;
  result->groups.swap(groups_);
  result->reservation_manager = std::move(cache_reservation_manager_);
  result->reservation_handle = std::move(reservation_handle_);
  // The old reservation still covers every buffer while they are released.
  // ReleaseWorkingMemory() no longer owns that reservation and cannot clear it.
  Status status = ReleaseWorkingMemory();
  if (status.ok() && result->reservation_manager != nullptr) {
    status = result->reservation_handle != nullptr
                 ? result->reservation_handle->Resize(result->MemoryUsage())
                 : result->reservation_manager->MakeCacheReservation(
                       result->MemoryUsage(), &result->reservation_handle);
  }
  if (!status.ok()) {
    return status;
  }
  *summary = std::move(result);
  return Status::OK();
}

size_t BlobReferenceMetadataCollector::ReservedMemory() const {
  return references_.capacity() * sizeof(BlobReference) +
         estimated_groups_.size() *
             (sizeof(EstimatedGroup) + 4 * sizeof(void*)) +
         (estimated_groups_.bucket_count() > 1
              ? estimated_groups_.bucket_count() * sizeof(void*)
              : 0) +
         block_index_.capacity() * sizeof(BlobReferenceBlockIndexEntry) +
         groups_.capacity() * sizeof(BlobReferenceGroup) +
         scratch_block_.capacity() + index_block_.capacity() +
         directory_block_.capacity();
}

Status BlobReferenceMetadataCollector::UpdateReservation() {
  return ResizeReservation(ReservedMemory());
}

Status BlobReferenceMetadataCollector::ResizeReservation(size_t memory_used) {
  if (cache_reservation_manager_ == nullptr) {
    return Status::OK();
  }
  return reservation_handle_ != nullptr
             ? reservation_handle_->Resize(memory_used)
             : cache_reservation_manager_->MakeCacheReservation(
                   memory_used, &reservation_handle_);
}

Status BlobReferenceMetadataCollector::SortValidateAndDeduplicate() {
  std::sort(references_.begin(), references_.end(),
            [](const BlobReference& lhs, const BlobReference& rhs) {
              return StableSortKey(lhs) < StableSortKey(rhs);
            });

  for (size_t i = 0; i < references_.size(); ++i) {
    Status status =
        ValidateReference(references_[i], /*require_exact_record_bytes=*/true);
    if (!status.ok()) {
      return status;
    }
    if (i > 0 && SameStableIdentity(references_[i - 1], references_[i]) &&
        ConflictingImmutableFields(references_[i - 1], references_[i])) {
      return Status::Corruption(
          "Conflicting immutable fields for one stable blob identity");
    }
    if (i > 0 && !references_[i].HasStableIdentity() &&
        !references_[i - 1].HasStableIdentity() &&
        references_[i - 1].origin_file_number ==
            references_[i].origin_file_number &&
        references_[i - 1].original_value_offset ==
            references_[i].original_value_offset &&
        ConflictingImmutableFields(references_[i - 1], references_[i])) {
      return Status::Corruption(
          "Conflicting metadata for one legacy blob reference");
    }
  }

  references_.erase(
      std::unique(references_.begin(), references_.end(), SameReference),
      references_.end());
  std::sort(references_.begin(), references_.end(),
            [](const BlobReference& lhs, const BlobReference& rhs) {
              return CanonicalSortKey(lhs) < CanonicalSortKey(rhs);
            });
  return Status::OK();
}

Status BlobReferenceMetadataCollector::BuildAndWriteDataBlocks(
    const DataBlockWriter& write_data_block) {
  descriptor_.reference_count = references_.size();
  descriptor_.stored_value_bytes = 0;
  descriptor_.data_digest = 0;

  size_t group_begin = 0;
  while (group_begin < references_.size()) {
    size_t group_end = group_begin + 1;
    while (group_end < references_.size() &&
           SameGroup(references_[group_begin], references_[group_end])) {
      ++group_end;
    }
    const bool has_feature_route = references_[group_begin].encoding_class !=
                                   BlobReferenceEncodingClass::kLegacyDirect;
    if (has_feature_route && groups_.size() >= kMaxGroupCount) {
      return Status::MemoryLimit("Too many blob reference groups in one SST");
    }

    BlobReferenceGroup group;
    group.encoding_class = references_[group_begin].encoding_class;
    group.generation_file_number =
        references_[group_begin].generation_file_number;
    group.origin_file_number = references_[group_begin].origin_file_number;
    group.first_reference = group_begin;
    group.reference_count = group_end - group_begin;
    group.first_block = block_index_.size();

    size_t fragment_begin = group_begin;
    while (fragment_begin < group_end) {
      scratch_block_.clear();
      Status reservation_status =
          ReserveCapacity(&scratch_block_, kTargetDataBlockSize);
      if (!reservation_status.ok()) {
        return reservation_status;
      }
      scratch_block_.resize(kDataBlockHeaderSize);
      scratch_block_.push_back(static_cast<char>(group.encoding_class));
      PutVarint64(&scratch_block_, group.generation_file_number);
      PutVarint64(&scratch_block_, group.origin_file_number);
      PutVarint64(&scratch_block_,
                  references_[fragment_begin].original_value_offset);

      size_t fragment_end = fragment_begin;
      uint64_t previous_offset = 0;
      while (fragment_end < group_end) {
        const BlobReference& reference = references_[fragment_end];
        std::string encoded_entry;
        if (fragment_end != fragment_begin) {
          assert(reference.original_value_offset > previous_offset);
          PutVarint64(&encoded_entry,
                      reference.original_value_offset - previous_offset);
        }
        EncodeReferenceFields(reference, &encoded_entry);
        if (fragment_end > fragment_begin &&
            scratch_block_.size() + encoded_entry.size() >
                kTargetDataBlockSize) {
          break;
        }
        scratch_block_.append(encoded_entry);
        previous_offset = reference.original_value_offset;
        if (!AddWithoutOverflow(group.stored_value_bytes,
                                reference.stored_value_size,
                                &group.stored_value_bytes) ||
            !AddWithoutOverflow(descriptor_.stored_value_bytes,
                                reference.stored_value_size,
                                &descriptor_.stored_value_bytes)) {
          return Status::Corruption("Blob reference byte total overflow");
        }
        if (!AddWithoutOverflow(group.record_bytes, reference.record_bytes,
                                &group.record_bytes)) {
          return Status::Corruption("Blob reference record byte overflow");
        }
        ++fragment_end;
      }

      const uint64_t fragment_count = fragment_end - fragment_begin;
      assert(fragment_count > 0);
      SetFixed32(&scratch_block_, 0, kDataBlockMagic);
      SetFixed32(&scratch_block_, 4, kMetadataFormatVersion);
      if (fragment_count > std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption("Blob reference fragment too large");
      }
      SetFixed32(&scratch_block_, 8, static_cast<uint32_t>(fragment_count));
      const uint32_t body_checksum =
          crc32c::Value(scratch_block_.data() + kDataBlockHeaderSize,
                        scratch_block_.size() - kDataBlockHeaderSize);
      SetFixed32(&scratch_block_, 12, body_checksum);

      if (block_index_.size() >= kMaxDataBlockCount) {
        return Status::MemoryLimit(
            "Too many blob reference data blocks in one SST");
      }

      BlockHandle handle;
      Status status = write_data_block(scratch_block_, &handle);
      if (!status.ok()) {
        return status;
      }
      if (handle.IsNull()) {
        return Status::Corruption(
            "Blob reference block writer returned null handle");
      }

      BlobReferenceBlockIndexEntry index_entry;
      index_entry.handle = handle;
      index_entry.first_reference = fragment_begin;
      index_entry.reference_count = fragment_count;
      index_entry.payload_checksum =
          crc32c::Value(scratch_block_.data(), scratch_block_.size());
      if (block_index_.size() == block_index_.capacity()) {
        const size_t target_capacity =
            block_index_.capacity() == 0 ? 1 : block_index_.capacity() * 2;
        reservation_status = ReserveCapacity(&block_index_, target_capacity);
        if (!reservation_status.ok()) {
          return reservation_status;
        }
      }
      block_index_.push_back(index_entry);
      descriptor_.data_digest =
          crc32c::Extend(descriptor_.data_digest, scratch_block_.data(),
                         scratch_block_.size());
      group.digest = crc32c::Extend(group.digest, scratch_block_.data(),
                                    scratch_block_.size());
      fragment_begin = fragment_end;
    }

    group.block_count = block_index_.size() - group.first_block;
    if (has_feature_route) {
      if (groups_.size() == groups_.capacity()) {
        const size_t target_capacity =
            groups_.capacity() == 0 ? 1 : groups_.capacity() * 2;
        Status reservation_status = ReserveCapacity(&groups_, target_capacity);
        if (!reservation_status.ok()) {
          return reservation_status;
        }
      }
      groups_.push_back(group);
    }
    group_begin = group_end;
  }

  descriptor_.group_count = groups_.size();
  return Status::OK();
}

Status BlobReferenceMetadataCollector::EncodeIndexAndDirectory() {
  uint64_t index_block_size = 12;
  const auto add_index_size = [&](uint64_t bytes) {
    return AddWithoutOverflow(index_block_size, bytes, &index_block_size) &&
           index_block_size <= kMaxTopLevelMetadataBlockSize;
  };
  if (!add_index_size(VarintLength(block_index_.size())) ||
      !add_index_size(VarintLength(descriptor_.reference_count)) ||
      !add_index_size(VarintLength(descriptor_.stored_value_bytes)) ||
      !add_index_size(sizeof(uint32_t))) {
    return Status::MemoryLimit("Blob reference index exceeds format limit");
  }
  for (const auto& entry : block_index_) {
    if (!add_index_size(VarintLength(entry.handle.offset())) ||
        !add_index_size(VarintLength(entry.handle.size())) ||
        !add_index_size(VarintLength(entry.first_reference)) ||
        !add_index_size(VarintLength(entry.reference_count)) ||
        !add_index_size(sizeof(entry.payload_checksum))) {
      return Status::MemoryLimit("Blob reference index exceeds format limit");
    }
  }

  index_block_.clear();
  Status reservation_status =
      ReserveCapacity(&index_block_, static_cast<size_t>(index_block_size));
  if (!reservation_status.ok()) {
    return reservation_status;
  }
  PutFixed32(&index_block_, kIndexBlockMagic);
  PutFixed32(&index_block_, kMetadataFormatVersion);
  PutFixed32(&index_block_, 0);
  PutVarint64(&index_block_, block_index_.size());
  PutVarint64(&index_block_, descriptor_.reference_count);
  PutVarint64(&index_block_, descriptor_.stored_value_bytes);
  PutFixed32(&index_block_, descriptor_.data_digest);
  for (const auto& entry : block_index_) {
    PutVarint64(&index_block_, entry.handle.offset());
    PutVarint64(&index_block_, entry.handle.size());
    PutVarint64(&index_block_, entry.first_reference);
    PutVarint64(&index_block_, entry.reference_count);
    PutFixed32(&index_block_, entry.payload_checksum);
  }
  assert(index_block_.size() == index_block_size);
  SetFixed32(&index_block_, 8,
             crc32c::Value(index_block_.data() + 12, index_block_.size() - 12));
  descriptor_.index_digest =
      crc32c::Value(index_block_.data(), index_block_.size());

  uint64_t directory_block_size = 12;
  const auto add_directory_size = [&](uint64_t bytes) {
    return AddWithoutOverflow(directory_block_size, bytes,
                              &directory_block_size) &&
           directory_block_size <= kMaxTopLevelMetadataBlockSize;
  };
  if (!add_directory_size(VarintLength(groups_.size()))) {
    return Status::MemoryLimit(
        "Blob reference group directory exceeds format limit");
  }
  for (const auto& group : groups_) {
    if (!add_directory_size(1) ||
        !add_directory_size(VarintLength(group.generation_file_number)) ||
        !add_directory_size(VarintLength(group.origin_file_number)) ||
        !add_directory_size(VarintLength(group.first_reference)) ||
        !add_directory_size(VarintLength(group.reference_count)) ||
        !add_directory_size(VarintLength(group.stored_value_bytes)) ||
        !add_directory_size(VarintLength(group.record_bytes)) ||
        !add_directory_size(VarintLength(group.first_block)) ||
        !add_directory_size(VarintLength(group.block_count)) ||
        !add_directory_size(sizeof(group.digest))) {
      return Status::MemoryLimit(
          "Blob reference group directory exceeds format limit");
    }
  }

  directory_block_.clear();
  reservation_status = ReserveCapacity(
      &directory_block_, static_cast<size_t>(directory_block_size));
  if (!reservation_status.ok()) {
    return reservation_status;
  }
  PutFixed32(&directory_block_, kDirectoryMagic);
  PutFixed32(&directory_block_, kMetadataFormatVersion);
  PutFixed32(&directory_block_, 0);
  PutVarint64(&directory_block_, groups_.size());
  for (const auto& group : groups_) {
    directory_block_.push_back(static_cast<char>(group.encoding_class));
    PutVarint64(&directory_block_, group.generation_file_number);
    PutVarint64(&directory_block_, group.origin_file_number);
    PutVarint64(&directory_block_, group.first_reference);
    PutVarint64(&directory_block_, group.reference_count);
    PutVarint64(&directory_block_, group.stored_value_bytes);
    PutVarint64(&directory_block_, group.record_bytes);
    PutVarint64(&directory_block_, group.first_block);
    PutVarint64(&directory_block_, group.block_count);
    PutFixed32(&directory_block_, group.digest);
  }
  assert(directory_block_.size() == directory_block_size);
  SetFixed32(&directory_block_, 8,
             crc32c::Value(directory_block_.data() + 12,
                           directory_block_.size() - 12));
  descriptor_.directory_digest =
      crc32c::Value(directory_block_.data(), directory_block_.size());
  return Status::OK();
}

Status DecodeBlobReferenceIndex(
    const Slice& input, const BlobReferenceMetadataDescriptor& descriptor,
    std::vector<BlobReferenceBlockIndexEntry>* block_index) {
  if (block_index == nullptr) {
    return Status::InvalidArgument("Null blob reference index output");
  }
  if (input.size() != descriptor.index_handle.size() ||
      input.size() > kMaxTopLevelMetadataBlockSize ||
      crc32c::Value(input.data(), input.size()) != descriptor.index_digest) {
    return Status::Corruption("Blob reference index digest mismatch");
  }
  Slice cursor = input;
  uint32_t body_checksum = 0;
  Status status = DecodeCommonBlockHeader(
      &cursor, kIndexBlockMagic, descriptor.format_version, &body_checksum);
  if (!status.ok()) {
    return status;
  }
  uint64_t block_count = 0;
  uint64_t reference_count = 0;
  uint64_t stored_value_bytes = 0;
  uint32_t data_digest = 0;
  if (!GetCanonicalVarint64(&cursor, &block_count) ||
      !GetCanonicalVarint64(&cursor, &reference_count) ||
      !GetCanonicalVarint64(&cursor, &stored_value_bytes) ||
      !GetFixed32(&cursor, &data_digest) || block_count > kMaxDataBlockCount ||
      reference_count != descriptor.reference_count ||
      stored_value_bytes != descriptor.stored_value_bytes ||
      data_digest != descriptor.data_digest) {
    return Status::Corruption("Invalid blob reference index header");
  }
  constexpr size_t kMinEncodedIndexEntrySize = 8;
  if (block_count > reference_count ||
      (reference_count == 0) != (block_count == 0) ||
      block_count > cursor.size() / kMinEncodedIndexEntrySize ||
      !CanAllocateDecodedVector(block_count,
                                sizeof(BlobReferenceBlockIndexEntry),
                                kMaxDecodedIndexBytes)) {
    return Status::Corruption("Impossible blob reference block count");
  }

  std::vector<BlobReferenceBlockIndexEntry> decoded;
  decoded.reserve(static_cast<size_t>(block_count));
  uint64_t expected_first_reference = 0;
  uint64_t previous_block_end = 0;
  for (uint64_t i = 0; i < block_count; ++i) {
    uint64_t offset = 0;
    uint64_t size = 0;
    BlobReferenceBlockIndexEntry entry;
    if (!GetCanonicalVarint64(&cursor, &offset) ||
        !GetCanonicalVarint64(&cursor, &size) ||
        !GetCanonicalVarint64(&cursor, &entry.first_reference) ||
        !GetCanonicalVarint64(&cursor, &entry.reference_count) ||
        !GetFixed32(&cursor, &entry.payload_checksum) || size == 0 ||
        size > kMaxDataBlockSize || size < kDataBlockHeaderSize ||
        entry.reference_count == 0 ||
        entry.reference_count > (size - kDataBlockHeaderSize + 1) / 4 ||
        offset >= descriptor.index_handle.offset() ||
        size > descriptor.index_handle.offset() - offset ||
        entry.first_reference != expected_first_reference ||
        (i > 0 && offset <= previous_block_end)) {
      return Status::Corruption("Invalid blob reference block index entry");
    }
    if (!AddWithoutOverflow(entry.first_reference, entry.reference_count,
                            &expected_first_reference) ||
        !AddWithoutOverflow(offset, size, &previous_block_end)) {
      return Status::Corruption("Blob reference index overflow");
    }
    entry.handle = BlockHandle(offset, size);
    entry.format_version = descriptor.format_version;
    decoded.push_back(entry);
  }
  if (!cursor.empty() || expected_first_reference != reference_count ||
      (reference_count == 0 && block_count != 0) ||
      (reference_count != 0 && block_count == 0)) {
    return Status::Corruption("Blob reference index count mismatch");
  }
  *block_index = std::move(decoded);
  return Status::OK();
}

Status DecodeBlobReferenceDirectory(
    const Slice& input, const BlobReferenceMetadataDescriptor& descriptor,
    uint64_t data_block_count, std::vector<BlobReferenceGroup>* groups) {
  if (groups == nullptr) {
    return Status::InvalidArgument("Null blob reference directory output");
  }
  if (input.size() != descriptor.directory_handle.size() ||
      input.size() > kMaxTopLevelMetadataBlockSize ||
      crc32c::Value(input.data(), input.size()) !=
          descriptor.directory_digest) {
    return Status::Corruption("Blob reference directory digest mismatch");
  }
  Slice cursor = input;
  uint32_t body_checksum = 0;
  Status status = DecodeCommonBlockHeader(
      &cursor, kDirectoryMagic, descriptor.format_version, &body_checksum);
  if (!status.ok()) {
    return status;
  }
  uint64_t group_count = 0;
  if (!GetCanonicalVarint64(&cursor, &group_count) ||
      group_count != descriptor.group_count || group_count > kMaxGroupCount) {
    return Status::Corruption("Invalid blob reference group count");
  }
  constexpr size_t kMinEncodedGroupSize = 12;
  if (group_count > descriptor.reference_count ||
      group_count > cursor.size() / kMinEncodedGroupSize ||
      !CanAllocateDecodedVector(group_count, sizeof(BlobReferenceGroup),
                                kMaxDecodedDirectoryBytes)) {
    return Status::Corruption("Impossible blob reference group count");
  }

  std::vector<BlobReferenceGroup> decoded;
  decoded.reserve(static_cast<size_t>(group_count));
  uint64_t expected_first_reference = 0;
  uint64_t expected_first_block = 0;
  uint64_t total_stored_bytes = 0;
  for (uint64_t i = 0; i < group_count; ++i) {
    if (cursor.empty()) {
      return Status::Corruption("Truncated blob reference group");
    }
    BlobReferenceGroup group;
    group.encoding_class = static_cast<BlobReferenceEncodingClass>(
        static_cast<unsigned char>(cursor[0]));
    cursor.remove_prefix(1);
    if (!IsValidEncodingClass(group.encoding_class) ||
        group.encoding_class == BlobReferenceEncodingClass::kLegacyDirect ||
        !GetCanonicalVarint64(&cursor, &group.generation_file_number) ||
        !GetCanonicalVarint64(&cursor, &group.origin_file_number) ||
        !GetCanonicalVarint64(&cursor, &group.first_reference) ||
        !GetCanonicalVarint64(&cursor, &group.reference_count) ||
        !GetCanonicalVarint64(&cursor, &group.stored_value_bytes) ||
        (descriptor.HasExactRecordBytes() &&
         !GetCanonicalVarint64(&cursor, &group.record_bytes)) ||
        !GetCanonicalVarint64(&cursor, &group.first_block) ||
        !GetCanonicalVarint64(&cursor, &group.block_count) ||
        !GetFixed32(&cursor, &group.digest) ||
        !IsValidGroupIdentity(group.encoding_class,
                              group.generation_file_number,
                              group.origin_file_number) ||
        group.reference_count == 0 ||
        (descriptor.HasExactRecordBytes() && group.record_bytes == 0) ||
        group.block_count == 0 ||
        group.first_reference >= descriptor.reference_count ||
        group.first_block >= data_block_count) {
      return Status::Corruption("Invalid blob reference group");
    }
    if (i == 0) {
      expected_first_reference = group.first_reference;
      expected_first_block = group.first_block;
    }
    if (group.first_reference != expected_first_reference ||
        group.first_block != expected_first_block) {
      return Status::Corruption("Non-contiguous blob reference groups");
    }
    if (!decoded.empty() &&
        std::tie(decoded.back().encoding_class,
                 decoded.back().generation_file_number,
                 decoded.back().origin_file_number) >=
            std::tie(group.encoding_class, group.generation_file_number,
                     group.origin_file_number)) {
      return Status::Corruption("Blob reference groups are not sorted");
    }
    if (!AddWithoutOverflow(expected_first_reference, group.reference_count,
                            &expected_first_reference) ||
        !AddWithoutOverflow(expected_first_block, group.block_count,
                            &expected_first_block) ||
        !AddWithoutOverflow(total_stored_bytes, group.stored_value_bytes,
                            &total_stored_bytes)) {
      return Status::Corruption("Blob reference group total overflow");
    }
    decoded.push_back(group);
  }
  if (!cursor.empty() ||
      (group_count != 0 &&
       (expected_first_reference != descriptor.reference_count ||
        expected_first_block != data_block_count ||
        total_stored_bytes > descriptor.stored_value_bytes)) ||
      (descriptor.reference_count == 0 && group_count != 0)) {
    return Status::Corruption("Blob reference directory totals mismatch");
  }
  *groups = std::move(decoded);
  return Status::OK();
}

namespace {

template <typename AppendReference>
Status DecodeBlobReferenceDataBlockImpl(
    const Slice& input, const BlobReferenceBlockIndexEntry& index_entry,
    AppendReference append_reference) {
  if (input.size() != index_entry.handle.size() ||
      input.size() > kMaxDataBlockSize ||
      crc32c::Value(input.data(), input.size()) !=
          index_entry.payload_checksum) {
    return Status::Corruption("Blob reference data digest mismatch");
  }
  Slice cursor = input;
  uint32_t magic = 0;
  uint32_t version = 0;
  uint32_t reference_count = 0;
  uint32_t body_checksum = 0;
  if (!GetFixed32(&cursor, &magic) || !GetFixed32(&cursor, &version) ||
      !GetFixed32(&cursor, &reference_count) ||
      !GetFixed32(&cursor, &body_checksum) || magic != kDataBlockMagic ||
      version != index_entry.format_version ||
      version < BlobReferenceMetadataDescriptor::kOldestReadableFormatVersion ||
      version > BlobReferenceMetadataDescriptor::kFormatVersion ||
      reference_count != index_entry.reference_count ||
      crc32c::Value(cursor.data(), cursor.size()) != body_checksum ||
      cursor.empty()) {
    return Status::Corruption("Invalid blob reference data header");
  }

  BlobReference first;
  first.encoding_class = static_cast<BlobReferenceEncodingClass>(
      static_cast<unsigned char>(cursor[0]));
  cursor.remove_prefix(1);
  if (!IsValidEncodingClass(first.encoding_class) ||
      !GetCanonicalVarint64(&cursor, &first.generation_file_number) ||
      !GetCanonicalVarint64(&cursor, &first.origin_file_number) ||
      !GetCanonicalVarint64(&cursor, &first.original_value_offset)) {
    return Status::Corruption("Invalid blob reference data group");
  }
  // The first legacy entry requires at least size/compression/checksum-version
  // bytes; every later entry additionally requires a non-zero offset delta.
  // Reject impossible counts before reserving attacker-controlled capacity.
  if (reference_count == 0 || reference_count > (cursor.size() + 1) / 4) {
    return Status::Corruption("Impossible blob reference data count");
  }

  uint64_t previous_offset = first.original_value_offset;
  for (uint32_t i = 0; i < reference_count; ++i) {
    BlobReference reference = first;
    if (i > 0) {
      uint64_t delta = 0;
      if (!GetCanonicalVarint64(&cursor, &delta) || delta == 0 ||
          !AddWithoutOverflow(previous_offset, delta,
                              &reference.original_value_offset)) {
        return Status::Corruption("Invalid blob reference offset delta");
      }
    }
    if (!GetCanonicalVarint64(&cursor, &reference.stored_value_size) ||
        (version >= 2 &&
         !GetCanonicalVarint64(&cursor, &reference.record_bytes)) ||
        cursor.empty()) {
      return Status::Corruption("Invalid blob reference value size");
    }
    reference.compression =
        static_cast<CompressionType>(static_cast<unsigned char>(cursor[0]));
    cursor.remove_prefix(1);
    if (!GetCanonicalVarint32(&cursor, &reference.stable_checksum_version)) {
      return Status::Corruption("Invalid blob reference checksum version");
    }
    if (reference.stable_checksum_version != 0 &&
        !GetFixed32(&cursor, &reference.stable_checksum)) {
      return Status::Corruption("Invalid blob reference checksum");
    }
    if (reference.stable_checksum_version ==
        BlobIndex::kStableChecksumVersion2) {
      if (version < 4) {
        return Status::Corruption("Dual checksum in old blob reference format");
      }
      if (reference.compression == kNoCompression) {
        reference.stored_checksum = BlobIndex::ExtendChecksumWithCompression(
            reference.stable_checksum, reference.compression);
      } else if (!GetFixed32(&cursor, &reference.stored_checksum)) {
        return Status::Corruption("Invalid blob reference stored checksum");
      }
    }
    Status status = ValidateReference(
        reference, /*require_exact_record_bytes=*/version >= 2);
    if (!status.ok()) {
      return status;
    }
    status = append_reference(reference);
    if (!status.ok()) {
      return status;
    }
    previous_offset = reference.original_value_offset;
  }
  if (!cursor.empty()) {
    return Status::Corruption("Trailing bytes in blob reference data block");
  }
  return Status::OK();
}

}  // namespace

Status DecodeBlobReferenceDataBlock(
    const Slice& input, const BlobReferenceBlockIndexEntry& index_entry,
    std::vector<BlobReference>* references) {
  if (references == nullptr) {
    return Status::InvalidArgument("Null blob reference data output");
  }
  std::vector<BlobReference> decoded;
  Status status = DecodeBlobReferenceDataBlockImpl(
      input, index_entry, [&](const BlobReference& reference) {
        decoded.push_back(reference);
        return Status::OK();
      });
  if (!status.ok()) {
    return status;
  }
  references->insert(references->end(), decoded.begin(), decoded.end());
  return Status::OK();
}

Status AppendBlobReferenceDataBlock(
    const Slice& input, const BlobReferenceBlockIndexEntry& index_entry,
    std::vector<BlobReference>* references) {
  if (references == nullptr) {
    return Status::InvalidArgument("Null blob reference data output");
  }
  if (index_entry.reference_count >
      references->capacity() - references->size()) {
    return Status::MemoryLimit(
        "Blob reference output capacity is not preallocated");
  }
  std::vector<BlobReference>& output = *references;
  const size_t output_begin = output.size();
  Status status = DecodeBlobReferenceDataBlockImpl(
      input, index_entry, [&](const BlobReference& reference) {
        output.push_back(reference);
        return Status::OK();
      });
  if (!status.ok()) {
    output.resize(output_begin);
  }
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
