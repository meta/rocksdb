// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_reference_metadata.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <numeric>
#include <tuple>
#include <utility>

#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_log_format.h"
#include "db/version_edit.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_reader.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

const char kBlobReferenceDescriptorPropertyName[] =
    "rocksdb.blob.reference-descriptor";
const char kBlobReferenceIndexBlockName[] = "rocksdb.blob.reference-index";
const char kBlobReferenceClassificationPropertyName[] =
    "rocksdb.blob.reference-classification";

void EncodeBlobReferenceClassification(uint64_t frontier, std::string* output) {
  output->clear();
  PutFixed32(output, 1);  // Format version 1.
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
    return Status::Corruption("Invalid coarse blob indirect-origin frontier");
  }
  return Status::OK();
}

namespace {

constexpr uint32_t kDescriptorMagic = 0x42475244U;  // BGRD
// Largest encoded (uncompressed) size of a data block that the writer makes.
// See "Compression and block size" at the top of the header for why 16 KB.
constexpr size_t kTargetDataBlockSize = 16 * 1024;
// Largest data block size that the decoders accept. The index decoder checks
// each block's size on disk (before decompression) and each group's run offset
// against this limit, and the run decoder checks the block contents after
// decompression (BlobReferenceGroupDecoder::DecodeRunHeader()). The writer
// keeps every block at or below kTargetDataBlockSize (16 KB), so 64 KB is a
// wide safety margin (estimate, not measured). The static_assert below checks
// that the writer's block buffer (the target size plus one run header and one
// entry) also fits.
constexpr size_t kMaxDataBlockSize = 64 * 1024;
// Largest reference index block that the writer makes and the decoders accept
// (64 MB). It bounds the memory that one index read uses. The count limits
// below do not keep the block under 64 MB. With kMaxGroupCount (2^20) groups,
// the group table alone can take about 71 MB (kMaxIndexEntrySize, 71 bytes
// per group). So EncodeIndex() checks the worst-case size against this limit
// and fails with MemoryLimit. 64 MB is an estimate of "far more than one SST
// needs", not measured.
constexpr size_t kMaxTopLevelMetadataBlockSize = 64 * 1024 * 1024;
// Count limits that both the writer and the index decoder check. They are
// part of the format, so they are fixed numbers and do not depend on C++ type
// sizes. 1M groups, or 1M data blocks of up to 16 KB each (16 GB of encoded
// entries), is far more than one SST needs (estimate, not measured).
constexpr uint64_t kMaxGroupCount = 1ULL << 20;
constexpr uint64_t kMaxDataBlockCount = 1ULL << 20;
// With these limits, the decoded block and group vectors stay under
// kMaxDecodedBlobReferenceBytes, so the index decoder needs no byte check at
// run time.
static_assert(kMaxDataBlockCount * sizeof(BlockHandle) <=
              kMaxDecodedBlobReferenceBytes);
static_assert(kMaxGroupCount * sizeof(BlobReferenceIndexEntry) <=
              kMaxDecodedBlobReferenceBytes);
// Encoded size bounds used for block fill and size planning.
constexpr size_t kMaxRunHeaderSize = 2 * kMaxVarint64Length + 1;
constexpr size_t kMaxEntrySize = 3 * kMaxVarint64Length + 1 + 8;
constexpr size_t kMaxIndexEntrySize = 1 + 7 * kMaxVarint64Length;
constexpr size_t kMaxBlockTableEntrySize = 2 * kMaxVarint64Length;
// Smallest encoded sizes, which bound a decoded count by the input size
// before any reserve(): a block size takes one varint byte; an index entry
// takes the class byte and at least six one-byte varints; a reference entry
// takes at least the size varint and either a key size varint or a CRC.
constexpr size_t kMinBlockTableEntrySize = 1;
constexpr size_t kMinIndexEntrySize = 7;
constexpr size_t kMinEntrySize = 2;
static_assert(kTargetDataBlockSize + kMaxRunHeaderSize + kMaxEntrySize <=
              kMaxDataBlockSize);
// The underlying type is unsigned and the first class is 0, so only the upper
// bound is checked. Update it when a class is added.
bool IsValidEncodingClass(BlobReferenceEncodingClass encoding_class) {
  return encoding_class <= BlobReferenceEncodingClass::kIndirectFlattened;
}

bool AddWithoutOverflow(uint64_t lhs, uint64_t rhs, uint64_t* sum) {
  if (sum == nullptr || rhs > std::numeric_limits<uint64_t>::max() - lhs) {
    return false;
  }
  *sum = lhs + rhs;
  return true;
}

// A flattened reference names a record in a relocation file, whose size is
// the stored size plus kSimpleGen2BlobTrailerSize (see "record bytes" in Terms
// at the top of blob_reference_metadata.h). Unflattened and kDirect references
// name a record in a normal blob file and store its user key size in each
// entry.
bool StoresUserKeySize(BlobReferenceEncodingClass encoding_class) {
  return encoding_class != BlobReferenceEncodingClass::kIndirectFlattened;
}

// Computes the offset that a gap of 0 gives (the gap is defined at the top of
// blob_reference_metadata.h). The gap of an entry is its offset minus this
// base:
//   - Unflattened and kDirect: previous offset + previous stored size +
//     BlobLogRecord::kHeaderSize (32 bytes) + this entry's user key size. The
//     caller passes the last two as `record_overhead`.
//   - Flattened: the offset is still the original offset in the origin file,
//     and the entry stores no user key size. So the base is previous offset +
//     previous stored size, and the caller passes 0.
// The readers (DecodeRunEntriesAt()) and the unsorted fallback
// (SortAndEncodeGroup()) call this function. AppendSorted() computes the same
// base from the group's last offset and stored size.
bool GapBase(const BlobReference& previous, uint64_t record_overhead,
             uint64_t* base) {
  return AddWithoutOverflow(previous.original_value_offset,
                            previous.stored_value_size, base) &&
         AddWithoutOverflow(*base, record_overhead, base);
}

// The user key size of a reference whose class stores it. Fails when the
// record bytes do not hold a record header.
bool UserKeySize(const BlobReference& reference, uint64_t* key_size) {
  if (reference.record_bytes - reference.stored_value_size <
      BlobLogRecord::kHeaderSize) {
    return false;
  }
  *key_size = reference.record_bytes - reference.stored_value_size -
              BlobLogRecord::kHeaderSize;
  return true;
}

// The per-record overhead of a group's records besides their stored bytes,
// apart from user keys: a record header in a normal blob file, or a trailer
// in a relocation file.
bool GroupRecordOverhead(BlobReferenceEncodingClass encoding_class,
                         uint64_t reference_count, uint64_t* overhead) {
  const uint64_t per_record = StoresUserKeySize(encoding_class)
                                  ? BlobLogRecord::kHeaderSize
                                  : kSimpleGen2BlobTrailerSize;
  if (reference_count > std::numeric_limits<uint64_t>::max() / per_record) {
    return false;
  }
  *overhead = reference_count * per_record;
  return true;
}

Status ValidateReference(const BlobReference& reference) {
  if (!IsValidEncodingClass(reference.encoding_class)) {
    return Status::Corruption("Invalid blob reference encoding class");
  }
  if (reference.generation_file_number == 0 ||
      reference.origin_file_number == 0) {
    return Status::Corruption("Invalid blob reference file number");
  }
  if (!BlobIndex::IsValidCompression(reference.compression)) {
    return Status::Corruption("Invalid blob reference compression");
  }
  // For unflattened and kDirect, this checks only record_bytes >= stored size.
  if (reference.record_bytes < reference.stored_value_size) {
    return Status::Corruption("Invalid blob reference record size");
  }
  if (reference.encoding_class == BlobReferenceEncodingClass::kDirect) {
    if (reference.generation_file_number != reference.origin_file_number ||
        reference.stable_checksum != 0 || reference.stored_checksum != 0) {
      return Status::Corruption("Invalid kDirect blob reference metadata");
    }
  } else {
    if (reference.compression == kNoCompression &&
        reference.stored_checksum !=
            BlobIndex::ExtendChecksumWithCompression(reference.stable_checksum,
                                                     kNoCompression)) {
      return Status::Corruption("Noncanonical stored blob checksum");
    }
    const bool is_unflattened =
        reference.generation_file_number == reference.origin_file_number;
    if (is_unflattened != (reference.encoding_class ==
                           BlobReferenceEncodingClass::kIndirectUnflattened)) {
      return Status::Corruption("Blob reference encoding class mismatch");
    }
    // Same rule as kIndirectFlattenedBlob in BlobIndex::DecodeFrom().
    if (!is_unflattened &&
        reference.generation_file_number < reference.origin_file_number) {
      return Status::Corruption("Flattened blob generation is below origin");
    }
    if (reference.encoding_class ==
            BlobReferenceEncodingClass::kIndirectFlattened &&
        (reference.stored_value_size > std::numeric_limits<uint64_t>::max() -
                                           kSimpleGen2BlobTrailerSize ||
         reference.record_bytes !=
             reference.stored_value_size + kSimpleGen2BlobTrailerSize)) {
      return Status::Corruption("Invalid flattened blob record size");
    }
  }
  return Status::OK();
}

void SetFixed32(std::string* output, size_t offset, uint32_t value) {
  assert(output != nullptr);
  assert(offset + sizeof(value) <= output->size());
  EncodeFixed32(output->data() + offset, value);
}

// The group key is not written: readers reach a run through its index group.
void EncodeRunHeader(const BlobReferenceRunHeader& header,
                     std::string* output) {
  PutVarint64(output, header.reference_count);
  PutVarint64(output, header.first_offset);
  output->push_back(static_cast<char>(header.default_compression));
}

// Decodes the run header at `*offset` of an uncompressed data block and moves
// `*offset` past it. The header holds no group key; the run belongs to
// `group`, the index group the reader reached it through.
Status DecodeRunHeaderAt(const Slice& block, size_t* offset,
                         const BlobReferenceGroup& group,
                         BlobReferenceRunHeader* header) {
  if (*offset >= block.size()) {
    return Status::Corruption("Blob reference run offset outside the block");
  }
  Slice cursor(block.data() + *offset, block.size() - *offset);
  BlobReferenceRunHeader decoded;
  decoded.encoding_class = group.encoding_class;
  decoded.generation_file_number = group.generation_file_number;
  decoded.origin_file_number = group.origin_file_number;
  if (!GetCanonicalVarint64(&cursor, &decoded.reference_count) ||
      !GetCanonicalVarint64(&cursor, &decoded.first_offset) || cursor.empty() ||
      decoded.reference_count == 0) {
    return Status::Corruption("Invalid blob reference run header");
  }
  decoded.default_compression =
      static_cast<CompressionType>(static_cast<unsigned char>(cursor[0]));
  cursor.remove_prefix(1);
  if (!BlobIndex::IsValidCompression(decoded.default_compression)) {
    return Status::Corruption("Invalid blob reference run compression");
  }
  *offset = block.size() - cursor.size();
  *header = decoded;
  return Status::OK();
}

// Decodes one entry without its gap from the front of `cursor` into
// `reference`: stored size, compression, record bytes and checksums. The
// caller must set the class before the call, because the class decides which
// fields the entry has. The caller sets the generation, origin and offset.
// Sets `*record_overhead` to the record bytes besides the stored value.
Status DecodeEntryBody(Slice* cursor, CompressionType default_compression,
                       BlobReference* reference, uint64_t* record_overhead) {
  uint64_t size_and_flag = 0;
  if (!GetCanonicalVarint64(cursor, &size_and_flag)) {
    return Status::Corruption("Invalid blob reference entry");
  }
  reference->stored_value_size = size_and_flag >> 1;
  reference->compression = default_compression;
  if ((size_and_flag & 1) != 0) {
    if (cursor->empty()) {
      return Status::Corruption("Invalid blob reference entry");
    }
    reference->compression =
        static_cast<CompressionType>(static_cast<unsigned char>((*cursor)[0]));
    cursor->remove_prefix(1);
    if (reference->compression == default_compression) {
      return Status::Corruption("Noncanonical blob reference compression");
    }
  }
  // Record overhead: a record header and the user key in a normal blob
  // file, or a trailer in a relocation file.
  *record_overhead = kSimpleGen2BlobTrailerSize;
  if (StoresUserKeySize(reference->encoding_class)) {
    uint64_t key_size = 0;
    if (!GetCanonicalVarint64(cursor, &key_size) ||
        !AddWithoutOverflow(BlobLogRecord::kHeaderSize, key_size,
                            record_overhead)) {
      return Status::Corruption("Invalid blob reference entry");
    }
  }
  if (!AddWithoutOverflow(reference->stored_value_size, *record_overhead,
                          &reference->record_bytes)) {
    return Status::Corruption("Blob reference record size overflow");
  }
  if (reference->HasStableIdentity()) {
    if (!GetFixed32(cursor, &reference->stable_checksum)) {
      return Status::Corruption("Invalid blob reference checksum");
    }
    if (reference->compression == kNoCompression) {
      reference->stored_checksum = BlobIndex::ExtendChecksumWithCompression(
          reference->stable_checksum, kNoCompression);
    } else if (!GetFixed32(cursor, &reference->stored_checksum)) {
      return Status::Corruption("Invalid blob reference stored checksum");
    }
  }
  return Status::OK();
}

// Decodes the entries of the run whose header was just decoded, calls
// `visit` for each reference in offset order, and moves `*offset` past the
// run. Checks each entry and the class rules. Offsets strictly increase and
// records do not overlap; with `previous_run_last` (the last reference of the
// group's previous run), this also holds for the run's first reference.
Status DecodeRunEntriesAt(
    const Slice& block, size_t* offset, const BlobReferenceRunHeader& header,
    const BlobReference* previous_run_last,
    const std::function<Status(const BlobReference&)>& visit) {
  assert(*offset <= block.size());
  Slice cursor(block.data() + *offset, block.size() - *offset);
  BlobReference previous;
  if (previous_run_last != nullptr) {
    previous = *previous_run_last;
  }
  for (uint64_t i = 0; i < header.reference_count; ++i) {
    BlobReference reference;
    reference.encoding_class = header.encoding_class;
    reference.generation_file_number = header.generation_file_number;
    reference.origin_file_number = header.origin_file_number;
    uint64_t gap = 0;
    if (i > 0 && !GetCanonicalVarint64(&cursor, &gap)) {
      return Status::Corruption("Invalid blob reference entry");
    }
    uint64_t record_overhead = 0;
    Status status = DecodeEntryBody(&cursor, header.default_compression,
                                    &reference, &record_overhead);
    if (!status.ok()) {
      return status;
    }
    if (i == 0) {
      reference.original_value_offset = header.first_offset;
    }
    if (i > 0 || previous_run_last != nullptr) {
      uint64_t base = 0;
      if (!GapBase(
              previous,
              StoresUserKeySize(header.encoding_class) ? record_overhead : 0,
              &base) ||
          (i > 0 &&
           !AddWithoutOverflow(base, gap, &reference.original_value_offset))) {
        return Status::Corruption("Blob reference offset overflow");
      }
      // A run's first offset is stored whole, so check it against the end of
      // the previous run's last record.
      if (reference.original_value_offset < base ||
          reference.original_value_offset <= previous.original_value_offset) {
        return Status::Corruption("Blob reference offsets are not ordered");
      }
    }
    status = ValidateReference(reference);
    if (status.ok()) {
      status = visit(reference);
    }
    if (!status.ok()) {
      return status;
    }
    previous = reference;
  }
  *offset = block.size() - cursor.size();
  return Status::OK();
}

// The body of BlobReference::FromBlobIndex(). For a kNoCompression indirect
// index, the stored CRC is not in the encoding: BlobIndex::stored_checksum()
// derives it from the logical CRC. With `derive_uncompressed_stored_checksum`
// false, the result's stored_checksum is 0 for such an index instead. The
// collector passes false: it does not encode that CRC (a reader derives it
// again, see DecodeEntryBody()), and it converts every reference of every
// output SST, so it skips the derivation.
Status ConvertBlobIndex(const BlobIndex& blob_index,
                        bool derive_uncompressed_stored_checksum,
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
    return Status::Corruption("Invalid blob file number in blob reference");
  }

  BlobReference result;
  if (blob_index.IsIndirect()) {
    result.generation_file_number = blob_index.generation_file_number();
    result.origin_file_number = blob_index.origin_file_number();
    result.original_value_offset = blob_index.original_value_offset();
    result.stable_checksum = blob_index.stable_checksum();
    result.stored_checksum = derive_uncompressed_stored_checksum ||
                                     blob_index.compression() != kNoCompression
                                 ? blob_index.stored_checksum()
                                 : 0;
    result.encoding_class =
        result.generation_file_number == result.origin_file_number
            ? BlobReferenceEncodingClass::kIndirectUnflattened
            : BlobReferenceEncodingClass::kIndirectFlattened;
  } else {
    result.encoding_class = BlobReferenceEncodingClass::kDirect;
    result.generation_file_number = blob_index.file_number();
    result.origin_file_number = blob_index.file_number();
    result.original_value_offset = blob_index.offset();
  }
  result.stored_value_size = blob_index.size();
  result.compression = blob_index.compression();
  // For unflattened and kDirect, record_bytes is only the stored size here:
  // the collector adds the record header and user key later. A flattened
  // reference's record bytes need no key.
  if (result.encoding_class == BlobReferenceEncodingClass::kIndirectFlattened) {
    if (result.stored_value_size >
        std::numeric_limits<uint64_t>::max() - kSimpleGen2BlobTrailerSize) {
      return Status::Corruption("Flattened blob record size overflow");
    }
    result.record_bytes = result.stored_value_size + kSimpleGen2BlobTrailerSize;
  } else {
    result.record_bytes = result.stored_value_size;
  }
  // Validate() and the construction above make every reference valid; the
  // collector does not check it again. Without the derived stored CRC, the
  // reference is valid except for that one field.
  assert(!derive_uncompressed_stored_checksum ||
         ValidateReference(result).ok());
  *reference = result;
  *included = true;
  return Status::OK();
}

}  // namespace

Status BlobReference::FromBlobIndex(const BlobIndex& blob_index,
                                    BlobReference* reference, bool* included) {
  return ConvertBlobIndex(blob_index,
                          /*derive_uncompressed_stored_checksum=*/true,
                          reference, included);
}

void BlobReferenceMetadataDescriptor::EncodeTo(std::string* output) const {
  assert(output != nullptr);
  output->clear();
  output->reserve(kEncodedSize);
  PutFixed32(output, kDescriptorMagic);
  PutFixed32(output, kFormatVersion);
  PutFixed64(output, index_handle.offset());
  PutFixed64(output, index_handle.size());
  PutFixed64(output, reference_count);
  PutFixed64(output, stored_value_bytes);
  PutFixed32(output, column_family_id);
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
  if (input.size() != kEncodedSize) {
    return Status::Corruption("Invalid blob reference descriptor size");
  }
  Slice cursor = input;
  uint32_t magic = 0;
  uint32_t version = 0;
  uint32_t descriptor_checksum = 0;
  BlobReferenceMetadataDescriptor decoded;
  uint64_t index_offset = 0;
  uint64_t index_size = 0;
  if (!GetFixed32(&cursor, &magic) || !GetFixed32(&cursor, &version) ||
      !GetFixed64(&cursor, &index_offset) ||
      !GetFixed64(&cursor, &index_size) ||
      !GetFixed64(&cursor, &decoded.reference_count) ||
      !GetFixed64(&cursor, &decoded.stored_value_bytes) ||
      !GetFixed32(&cursor, &decoded.column_family_id) ||
      !GetFixed64(&cursor, &decoded.route_snapshot_fingerprint_high) ||
      !GetFixed64(&cursor, &decoded.route_snapshot_fingerprint_low) ||
      !GetFixed64(&cursor, &decoded.sst_file_number) ||
      !GetFixed32(&cursor, &descriptor_checksum) || !cursor.empty()) {
    return Status::Corruption("Malformed blob reference descriptor");
  }
  if (crc32c::Value(input.data(), input.size() - sizeof(uint32_t)) !=
      descriptor_checksum) {
    return Status::Corruption("Blob reference descriptor checksum mismatch");
  }
  if (magic != kDescriptorMagic || version != kFormatVersion) {
    return Status::Corruption("Unsupported blob reference descriptor");
  }
  if (!decoded.HasSstIdentity()) {
    return Status::Corruption("Incomplete blob reference descriptor");
  }
  decoded.index_handle = BlockHandle(index_offset, index_size);
  uint64_t index_end = 0;
  if (decoded.index_handle.IsNull() || index_size == 0 ||
      index_size > kMaxTopLevelMetadataBlockSize ||
      !AddWithoutOverflow(index_offset, index_size, &index_end)) {
    return Status::Corruption("Invalid blob reference metadata handle");
  }
  if (decoded.reference_count == 0 && decoded.stored_value_bytes != 0) {
    return Status::Corruption("Invalid empty blob reference descriptor");
  }
  *descriptor = decoded;
  return Status::OK();
}

bool BlobReferenceGroupKey::IsValid() const {
  if (!IsValidEncodingClass(encoding_class) || generation_file_number == 0 ||
      origin_file_number == 0) {
    return false;
  }
  return encoding_class == BlobReferenceEncodingClass::kIndirectFlattened
             ? generation_file_number > origin_file_number
             : generation_file_number == origin_file_number;
}

BlobReferenceMetadataCollector::BlobReferenceMetadataCollector(
    std::shared_ptr<CacheReservationManager> cache_reservation_manager,
    const BlobReferenceSstIdentity& identity)
    : cache_reservation_manager_(std::move(cache_reservation_manager)) {
  descriptor_.column_family_id = identity.column_family_id;
  descriptor_.route_snapshot_fingerprint_high =
      identity.route_snapshot_fingerprint_high;
  descriptor_.route_snapshot_fingerprint_low =
      identity.route_snapshot_fingerprint_low;
  descriptor_.sst_file_number = identity.sst_file_number;
  // The decoder rejects a descriptor without the SST identity, so the
  // collector refuses to build one.
  if (!descriptor_.HasSstIdentity()) {
    status_ = Status::InvalidArgument(
        "Blob reference metadata collector needs a known column family and a "
        "nonzero SST file number");
  }
}

BlobReferenceMetadataCollector::~BlobReferenceMetadataCollector() {
  // The table builder's Abandon() can destroy the collector before any Add()
  // or Finish() call reads status_.
  status_.PermitUncheckedError();
}

uint64_t BlobReferenceMetadataCollector::EstimatedEncodedSize() const {
  constexpr uint64_t kFixedOverhead = 4096;
  const uint64_t groups = group_records_.size();
  // chunk_bytes_ holds every entry with its gap (a chunk that continues a
  // chain starts with its gap) and bounds the unsorted entries. Every group
  // adds a run header and an index entry.
  const uint64_t run_bytes = chunk_bytes_ + groups * kMaxRunHeaderSize;
  // Blocks hold whole chunks, so a block may end up to one chunk short of
  // the target, and every block may start with a continuation run header.
  const uint64_t blocks = run_bytes / (kTargetDataBlockSize - kChunkSize) + 1;
  return kFixedOverhead + run_bytes + groups * kMaxIndexEntrySize +
         blocks * (kMaxRunHeaderSize + BlockBasedTable::kBlockTrailerSize +
                   kMaxBlockTableEntrySize);
}

void BlobReferenceMetadataCollector::ReleaseBuffers() {
  std::vector<std::unique_ptr<char[]>>().swap(slabs_);
  slab_used_ = kSlabSize;
  std::vector<Chunk>().swap(chunks_);
  std::vector<GroupRecord>().swap(group_records_);
  decltype(group_map_)().swap(group_map_);
  last_group_ = kNoChunk;
  reference_count_ = 0;
  chunk_bytes_ = 0;
  std::vector<BlockHandle>().swap(index_.blocks);
  std::vector<BlobReferenceIndexEntry>().swap(index_.groups);
  std::vector<BlobReferenceGroup>().swap(groups_);
  std::string().swap(index_block_);
  block_buffer_bytes_ = 0;
}

Status BlobReferenceMetadataCollector::Add(const BlobIndex& blob_index,
                                           uint64_t user_key_size) {
  if (!status_.ok()) {
    return status_;
  }

  BlobReference reference;
  bool included = false;
  status_ = ConvertBlobIndex(blob_index,
                             /*derive_uncompressed_stored_checksum=*/false,
                             &reference, &included);
  if (!status_.ok() || !included) {
    return status_;
  }
  // ConvertBlobIndex() already set the record bytes of a flattened reference.
  if (StoresUserKeySize(reference.encoding_class)) {
    const uint64_t adjustment =
        BlobLogRecord::CalculateAdjustmentForRecordHeader(user_key_size);
    if (reference.stored_value_size >
        std::numeric_limits<uint64_t>::max() - adjustment) {
      status_ = Status::Corruption("Blob log record size overflow");
      return status_;
    }
    reference.record_bytes = reference.stored_value_size + adjustment;
  }
  uint32_t group = kNoChunk;
  status_ = FindOrAddGroup(reference, &group);
  if (!status_.ok()) {
    return status_;
  }
  GroupRecord& record = group_records_[group];
  if (record.group.reference_count != 0 &&
      reference.original_value_offset == record.last_offset) {
    status_ = Status::Corruption("Repeated blob reference in one SST");
    return status_;
  }
  if (record.unsorted ||
      (record.group.reference_count != 0 &&
       reference.original_value_offset < record.last_offset)) {
    // Out of order: encoded once, with its absolute offset, until Finish()
    // sorts the group.
    status_ = AppendUnsorted(group, reference);
    if (!status_.ok()) {
      return status_;
    }
    record.unsorted = true;
  } else {
    status_ = AppendSorted(group, reference);
    if (!status_.ok()) {
      return status_;
    }
  }
  BlobReferenceGroup& totals = record.group;
  ++totals.reference_count;
  if (!AddWithoutOverflow(totals.stored_value_bytes,
                          reference.stored_value_size,
                          &totals.stored_value_bytes) ||
      !AddWithoutOverflow(totals.record_bytes, reference.record_bytes,
                          &totals.record_bytes)) {
    status_ = Status::Corruption("Blob reference byte total overflow");
    return status_;
  }
  ++reference_count_;
  return status_;
}

Status BlobReferenceMetadataCollector::FindOrAddGroup(
    const BlobReference& reference, uint32_t* group) {
  // Fast path: most references belong to the same group as the previous one.
  if (last_group_ != kNoChunk) {
    const BlobReferenceGroup& last = group_records_[last_group_].group;
    if (last.group_key() == reference.group_key()) {
      *group = last_group_;
      return Status::OK();
    }
  }
  const BlobReferenceGroupKey key = reference.group_key();
  const auto found = group_map_.find(key);
  if (found != group_map_.end()) {
    *group = found->second;
    last_group_ = *group;
    return Status::OK();
  }
  if (group_records_.size() >= kMaxGroupCount) {
    return Status::MemoryLimit("Too many blob reference groups in one SST");
  }
  Status status = GrowIfFull(&group_records_, /*initial_capacity=*/16);
  if (!status.ok()) {
    return status;
  }
  // Charge one map node. If this insert may rehash, also charge a new bucket
  // array of 4 * (bucket count + 16) pointers. ReservedMemory() already counts
  // the current bucket array. A rehash allocates a new array of about twice
  // the buckets while the old array is still alive, so the extra memory is
  // about 2 times the current bucket count. Charging 4 times is a safe upper
  // bound (an estimate, not measured). The +16 covers the first insert into an
  // empty map.
  const bool may_rehash =
      group_map_.size() + 1 >
      group_map_.bucket_count() * group_map_.max_load_factor();
  status = ResizeReservation(
      ReservedMemory() + kGroupMapNodeBytes +
      (may_rehash ? 4 * (group_map_.bucket_count() + 16) * sizeof(void*) : 0));
  if (!status.ok()) {
    return status;
  }
  GroupRecord record;
  record.group.encoding_class = reference.encoding_class;
  record.group.generation_file_number = reference.generation_file_number;
  record.group.origin_file_number = reference.origin_file_number;
  record.default_compression = reference.compression;
  group_records_.push_back(std::move(record));
  indirect_group_count_ += reference.HasStableIdentity();
  *group = static_cast<uint32_t>(group_records_.size() - 1);
  group_map_.emplace(key, *group);
  last_group_ = *group;
  return UpdateReservation();
}

Status BlobReferenceMetadataCollector::NewChunk(uint32_t* chunk) {
  if (chunks_.size() >= kNoChunk - 1) {
    return Status::MemoryLimit("Too many blob reference chunks in one SST");
  }
  Status status = GrowIfFull(&chunks_, /*initial_capacity=*/128);
  if (!status.ok()) {
    return status;
  }
  if (slab_used_ + kChunkSize > kSlabSize) {
    status = GrowIfFull(&slabs_, /*initial_capacity=*/4);
    if (!status.ok()) {
      return status;
    }
    // Charge the slab before it is allocated.
    status = ResizeReservation(ReservedMemory() + kSlabSize);
    if (!status.ok()) {
      return status;
    }
    slabs_.emplace_back(new char[kSlabSize]);
    slab_used_ = 0;
  }
  slab_used_ += kChunkSize;
  chunks_.emplace_back();
  *chunk = static_cast<uint32_t>(chunks_.size() - 1);
  // Chunk i is chunk i % kChunksPerSlab of slab i / kChunksPerSlab.
  assert(ChunkData(*chunk) == slabs_.back().get() + slab_used_ - kChunkSize);
  return Status::OK();
}

Status BlobReferenceMetadataCollector::EncodeEntryBody(
    const BlobReference& reference, CompressionType default_compression,
    char* body, size_t* body_size, uint64_t* key_size) {
  *key_size = 0;
  if (StoresUserKeySize(reference.encoding_class) &&
      !UserKeySize(reference, key_size)) {
    return Status::Corruption("Blob reference record has no record header");
  }
  if (reference.stored_value_size > std::numeric_limits<uint64_t>::max() >> 1) {
    return Status::Corruption("Blob reference size overflow");
  }
  char* end = body;
  const bool differs = reference.compression != default_compression;
  end = EncodeVarint64(end,
                       (reference.stored_value_size << 1) | (differs ? 1 : 0));
  if (differs) {
    *end++ = static_cast<char>(reference.compression);
  }
  if (StoresUserKeySize(reference.encoding_class)) {
    end = EncodeVarint64(end, *key_size);
  }
  if (reference.HasStableIdentity()) {
    EncodeFixed32(end, reference.stable_checksum);
    end += sizeof(uint32_t);
    if (reference.compression != kNoCompression) {
      EncodeFixed32(end, reference.stored_checksum);
      end += sizeof(uint32_t);
    }
  }
  *body_size = static_cast<size_t>(end - body);
  return Status::OK();
}

Status BlobReferenceMetadataCollector::AppendUnsorted(
    uint32_t group, const BlobReference& reference) {
  // The absolute offset, then the entry without its gap.
  char entry[kMaxEntrySize];
  char* const body = EncodeVarint64(entry, reference.original_value_offset);
  size_t body_size = 0;
  uint64_t key_size = 0;
  Status status =
      EncodeEntryBody(reference, group_records_[group].default_compression,
                      body, &body_size, &key_size);
  if (!status.ok()) {
    return status;
  }
  const size_t entry_size = static_cast<size_t>(body - entry) + body_size;
  GroupRecord& record = group_records_[group];
  if (record.unsorted_tail_chunk == kNoChunk ||
      chunks_[record.unsorted_tail_chunk].used + entry_size > kChunkSize) {
    uint32_t chunk = kNoChunk;
    status = NewChunk(&chunk);
    if (!status.ok()) {
      return status;
    }
    if (record.unsorted_tail_chunk != kNoChunk) {
      chunks_[record.unsorted_tail_chunk].next = chunk;
    } else {
      record.unsorted_first_chunk = chunk;
    }
    record.unsorted_tail_chunk = chunk;
  }
  Chunk& tail = chunks_[record.unsorted_tail_chunk];
  memcpy(ChunkData(record.unsorted_tail_chunk) + tail.used, entry, entry_size);
  tail.used = static_cast<uint16_t>(tail.used + entry_size);
  ++tail.count;
  // The final entry has a gap no longer than this absolute offset, so these
  // bytes bound its size for EstimatedEncodedSize().
  chunk_bytes_ += entry_size;
  return Status::OK();
}

Status BlobReferenceMetadataCollector::AppendSorted(
    uint32_t group, const BlobReference& reference) {
  GroupRecord& record = group_records_[group];
  // Entry without its gap, in the on-disk encoding.
  char body[kMaxEntrySize];
  size_t body_size = 0;
  uint64_t key_size = 0;
  Status body_status = EncodeEntryBody(reference, record.default_compression,
                                       body, &body_size, &key_size);
  if (!body_status.ok()) {
    return body_status;
  }

  char gap_buffer[kMaxVarint64Length];
  size_t gap_size = 0;
  if (record.tail_chunk != kNoChunk) {
    // This computes the same base as GapBase(), from the group's last offset
    // and stored size.
    uint64_t base = 0;
    if (!AddWithoutOverflow(record.last_offset, record.last_stored_size,
                            &base) ||
        (StoresUserKeySize(reference.encoding_class) &&
         (!AddWithoutOverflow(base, BlobLogRecord::kHeaderSize, &base) ||
          !AddWithoutOverflow(base, key_size, &base)))) {
      return Status::Corruption("Blob reference offset overflow");
    }
    if (reference.original_value_offset < base) {
      return Status::Corruption("Overlapping blob references in one SST");
    }
    gap_size = static_cast<size_t>(
        EncodeVarint64(gap_buffer, reference.original_value_offset - base) -
        gap_buffer);
  }

  // The gap and the entry, in this order, either continue the tail chunk or
  // start a new one. A new chunk that is not the group's first also starts
  // with the gap (its join gap). A group's first chunk has no gap.
  if (record.tail_chunk == kNoChunk ||
      chunks_[record.tail_chunk].used + gap_size + body_size > kChunkSize) {
    uint32_t chunk = kNoChunk;
    Status status = NewChunk(&chunk);
    if (!status.ok()) {
      return status;
    }
    if (record.tail_chunk != kNoChunk) {
      chunks_[record.tail_chunk].next = chunk;
    } else {
      record.first_chunk = chunk;
    }
    record.tail_chunk = chunk;
    chunks_[chunk].first_offset = reference.original_value_offset;
  }
  Chunk& tail = chunks_[record.tail_chunk];
  char* const data = ChunkData(record.tail_chunk) + tail.used;
  memcpy(data, gap_buffer, gap_size);
  memcpy(data + gap_size, body, body_size);
  tail.used = static_cast<uint16_t>(tail.used + gap_size + body_size);
  ++tail.count;
  chunk_bytes_ += gap_size + body_size;
  record.last_offset = reference.original_value_offset;
  record.last_stored_size = reference.stored_value_size;
  return Status::OK();
}

Status BlobReferenceMetadataCollector::SortUnsortedGroup(uint32_t group) {
  TEST_SYNC_POINT("BlobReferenceMetadataCollector::SortUnsortedGroup");
  const size_t count =
      static_cast<size_t>(group_records_[group].group.reference_count);
  // Temporary memory: one sort key per reference of this one group. It stays
  // in ReservedMemory() until the vector is freed, so every charge update
  // while the group is encoded again keeps it.
  sort_scratch_bytes_ = count * sizeof(SortKey);
  Status status = UpdateReservation();
  if (status.ok()) {
    std::vector<SortKey> keys;
    keys.reserve(count);
    status = SortAndEncodeGroup(group, &keys);
    TEST_SYNC_POINT(
        "BlobReferenceMetadataCollector::SortUnsortedGroup:Encoded");
  }
  sort_scratch_bytes_ = 0;
  const Status release_status = UpdateReservation();
  if (!status.ok()) {
    // The sort error is the one to report.
    release_status.PermitUncheckedError();
    return status;
  }
  return release_status;
}

Status BlobReferenceMetadataCollector::SortAndEncodeGroup(
    uint32_t group, std::vector<SortKey>* keys) {
  const GroupRecord& record = group_records_[group];
  BlobReference reference;
  reference.encoding_class = record.group.encoding_class;
  reference.generation_file_number = record.group.generation_file_number;
  reference.origin_file_number = record.group.origin_file_number;
  const bool stores_key_size = StoresUserKeySize(reference.encoding_class);
  // Sorted chain: gap-encoded entries; each chunk stores its first offset,
  // and every chunk but the first starts with its join gap.
  for (uint32_t chunk = record.first_chunk; chunk != kNoChunk;
       chunk = chunks_[chunk].next) {
    const Chunk& header = chunks_[chunk];
    Slice cursor(ChunkData(chunk), header.used);
    uint64_t join_gap = 0;
    if (chunk != record.first_chunk &&
        !GetCanonicalVarint64(&cursor, &join_gap)) {
      return Status::Corruption("Invalid blob reference chunk join");
    }
    for (uint32_t i = 0; i < header.count; ++i) {
      uint64_t gap = 0;
      if (i > 0 && !GetCanonicalVarint64(&cursor, &gap)) {
        return Status::Corruption("Invalid blob reference entry");
      }
      const uint32_t position =
          header.used - static_cast<uint32_t>(cursor.size());
      const BlobReference previous = reference;
      uint64_t record_overhead = 0;
      Status status = DecodeEntryBody(&cursor, record.default_compression,
                                      &reference, &record_overhead);
      if (!status.ok()) {
        return status;
      }
      if (i == 0) {
        reference.original_value_offset = header.first_offset;
      } else {
        uint64_t base = 0;
        if (!GapBase(previous, stores_key_size ? record_overhead : 0, &base) ||
            !AddWithoutOverflow(base, gap, &reference.original_value_offset)) {
          return Status::Corruption("Blob reference offset overflow");
        }
      }
      keys->push_back({reference.original_value_offset, chunk, position});
    }
    if (!cursor.empty()) {
      return Status::Corruption("Blob reference chunk has trailing bytes");
    }
  }
  // Unsorted chain: each entry starts with its absolute offset. The entry
  // body is decoded here only to check it and to move past it.
  for (uint32_t chunk = record.unsorted_first_chunk; chunk != kNoChunk;
       chunk = chunks_[chunk].next) {
    const Chunk& header = chunks_[chunk];
    Slice cursor(ChunkData(chunk), header.used);
    for (uint32_t i = 0; i < header.count; ++i) {
      uint64_t offset = 0;
      if (!GetCanonicalVarint64(&cursor, &offset)) {
        return Status::Corruption("Invalid blob reference entry");
      }
      const uint32_t position =
          header.used - static_cast<uint32_t>(cursor.size());
      uint64_t record_overhead = 0;
      Status status = DecodeEntryBody(&cursor, record.default_compression,
                                      &reference, &record_overhead);
      if (!status.ok()) {
        return status;
      }
      keys->push_back({offset, chunk, position});
    }
    if (!cursor.empty()) {
      return Status::Corruption("Blob reference chunk has trailing bytes");
    }
  }
  if (keys->size() != record.group.reference_count) {
    return Status::Corruption("Blob reference group count mismatch");
  }
  std::sort(keys->begin(), keys->end(),
            [](const SortKey& lhs, const SortKey& rhs) {
              return lhs.offset < rhs.offset;
            });
  GroupRecord& reset = group_records_[group];
  reset.unsorted = false;
  reset.first_chunk = kNoChunk;
  reset.tail_chunk = kNoChunk;
  reset.unsorted_first_chunk = kNoChunk;
  reset.unsorted_tail_chunk = kNoChunk;
  // The group keeps its default compression, so no entry grows past the size
  // estimate. The old chunks stay allocated but unused until the collector
  // frees its buffers.
  for (size_t i = 0; i < keys->size(); ++i) {
    const SortKey& key = (*keys)[i];
    if (i > 0 && key.offset == (*keys)[i - 1].offset) {
      return Status::Corruption("Repeated blob reference in one SST");
    }
    // Chunk data stays in place while AppendSorted() adds chunk headers.
    Slice cursor(ChunkData(key.chunk) + key.position,
                 chunks_[key.chunk].used - key.position);
    uint64_t record_overhead = 0;
    Status status =
        DecodeEntryBody(&cursor, group_records_[group].default_compression,
                        &reference, &record_overhead);
    if (status.ok()) {
      reference.original_value_offset = key.offset;
      // Checks overlaps.
      status = AppendSorted(group, reference);
    }
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status BlobReferenceMetadataCollector::Finish(
    const BlockWriter& write_block, BlobReferenceMetadataDescriptor* descriptor,
    std::shared_ptr<const BlobReferenceSstSummary>* summary) {
  const Status status = FinishOnce(write_block, descriptor, summary);
  if (status_.ok()) {
    // Sticky: a later Add() or Finish() fails instead of writing again.
    status_ = Status::InvalidArgument(
        "Blob reference metadata collector already finished");
  }
  return status;
}

Status BlobReferenceMetadataCollector::FinishOnce(
    const BlockWriter& write_block, BlobReferenceMetadataDescriptor* descriptor,
    std::shared_ptr<const BlobReferenceSstSummary>* summary) {
  if (!status_.ok()) {
    return status_;
  }
  if (!write_block || descriptor == nullptr) {
    status_ = Status::InvalidArgument("Missing blob reference metadata output");
    return status_;
  }
  status_ = BuildAndWriteDataBlocks(write_block);
  if (status_.ok()) {
    status_ = EncodeIndex();
  }
  if (status_.ok()) {
    status_ = UpdateReservation();
  }
  if (status_.ok()) {
    status_ = write_block(index_block_, /*is_index_block=*/true,
                          &descriptor_.index_handle);
  }
  if (status_.ok() && descriptor_.index_handle.IsNull()) {
    status_ =
        Status::Corruption("Blob reference block writer returned null handle");
  }
  if (!status_.ok()) {
    return status_;
  }
  *descriptor = descriptor_;
  if (summary == nullptr) {
    return Status::OK();
  }
  std::shared_ptr<BlobReferenceSstSummary> result =
      std::make_shared<BlobReferenceSstSummary>();
  result->file_number = descriptor_.sst_file_number;
  result->descriptor = descriptor_;
  result->groups.swap(groups_);
  result->reservation_manager = std::move(cache_reservation_manager_);
  result->reservation_handle = std::move(reservation_handle_);
  // The moved reservation still covers every buffer while they are freed.
  ReleaseBuffers();
  if (result->reservation_manager != nullptr) {
    status_ = CountUnchargedCharge(
        result->reservation_handle != nullptr
            ? result->reservation_handle->Resize(result->MemoryUsage())
            : result->reservation_manager->MakeCacheReservation(
                  result->MemoryUsage(), &result->reservation_handle));
  }
  if (status_.ok()) {
    *summary = std::move(result);
  }
  return status_;
}

size_t BlobReferenceMetadataCollector::ReservedMemory() const {
  return slabs_.size() * kSlabSize +
         slabs_.capacity() * sizeof(std::unique_ptr<char[]>) +
         chunks_.capacity() * sizeof(Chunk) +
         group_records_.capacity() * sizeof(GroupRecord) + sort_scratch_bytes_ +
         group_map_.size() * kGroupMapNodeBytes +
         group_map_.bucket_count() * sizeof(void*) +
         index_.blocks.capacity() * sizeof(BlockHandle) +
         index_.groups.capacity() * sizeof(BlobReferenceIndexEntry) +
         groups_.capacity() * sizeof(BlobReferenceGroup) +
         index_block_.capacity() + block_buffer_bytes_;
}

Status BlobReferenceMetadataCollector::UpdateReservation() {
  return ResizeReservation(ReservedMemory());
}

Status AllowUnchargedWhenCacheIsFull(Status status) {
  if (status.IsMemoryLimit()) {
    return Status::OK();
  }
  return status;
}

Status BlobReferenceMetadataCollector::CountUnchargedCharge(Status status) {
  if (status.IsMemoryLimit()) {
    ++uncharged_charge_failures_;
    TEST_SYNC_POINT("BlobReferenceMetadataCollector::CountUnchargedCharge");
  }
  return AllowUnchargedWhenCacheIsFull(std::move(status));
}

Status BlobReferenceMetadataCollector::ResizeReservation(size_t memory_used) {
  if (cache_reservation_manager_ == nullptr) {
    return Status::OK();
  }
  return CountUnchargedCharge(
      reservation_handle_ != nullptr
          ? reservation_handle_->Resize(memory_used)
          : cache_reservation_manager_->MakeCacheReservation(
                memory_used, &reservation_handle_));
}

Status BlobReferenceMetadataCollector::BuildAndWriteDataBlocks(
    const BlockWriter& write_block) {
  descriptor_.reference_count = reference_count_;
  descriptor_.stored_value_bytes = 0;
  if (reference_count_ == 0) {
    return Status::OK();
  }
  Status status;
  for (uint32_t group = 0; group < group_records_.size(); ++group) {
    if (group_records_[group].unsorted) {
      status = SortUnsortedGroup(group);
      if (!status.ok()) {
        return status;
      }
    }
  }
  // The block buffer (at most one block) and the group order. Charge them
  // before they grow.
  constexpr size_t kBufferBytes =
      kTargetDataBlockSize + kMaxRunHeaderSize + kMaxEntrySize;
  block_buffer_bytes_ = kBufferBytes + group_records_.size() * sizeof(uint32_t);
  status = UpdateReservation();
  if (!status.ok()) {
    return status;
  }
  // Only the group records are sorted, not the references.
  std::vector<uint32_t> order(group_records_.size());
  std::iota(order.begin(), order.end(), uint32_t{0});
  std::sort(order.begin(), order.end(), [this](uint32_t lhs, uint32_t rhs) {
    return group_records_[lhs].group.group_key() <
           group_records_[rhs].group.group_key();
  });
  status = ReserveCapacity(&index_.groups, order.size());
  if (status.ok()) {
    // The summary keeps the indirect groups.
    status = ReserveCapacity(&groups_, indirect_group_count_);
  }
  if (!status.ok()) {
    return status;
  }

  std::string block;
  block.reserve(kBufferBytes);
  const auto flush_block = [&]() -> Status {
    if (index_.blocks.size() >= kMaxDataBlockCount) {
      return Status::MemoryLimit(
          "Too many blob reference data blocks in one SST");
    }
    Status grow_status = GrowIfFull(&index_.blocks, /*initial_capacity=*/1);
    if (!grow_status.ok()) {
      return grow_status;
    }
    BlockHandle handle;
    Status write_status = write_block(block, /*is_index_block=*/false, &handle);
    if (!write_status.ok()) {
      return write_status;
    }
    if (handle.IsNull()) {
      return Status::Corruption(
          "Blob reference block writer returned null handle");
    }
    index_.blocks.push_back(handle);
    block.clear();
    return Status::OK();
  };

  for (uint32_t group : order) {
    const GroupRecord& record = group_records_[group];
    assert(record.first_chunk != kNoChunk);
    // Bytes of a chunk's join gap; a group's first chunk has none.
    const auto gap_bytes = [&](uint32_t chunk, size_t* size) -> Status {
      *size = 0;
      if (chunk == record.first_chunk) {
        return Status::OK();
      }
      Slice cursor(ChunkData(chunk), chunks_[chunk].used);
      uint64_t gap = 0;
      if (!GetCanonicalVarint64(&cursor, &gap)) {
        return Status::Corruption("Invalid blob reference chunk join");
      }
      *size = chunks_[chunk].used - cursor.size();
      return Status::OK();
    };
    if (!block.empty() &&
        block.size() + kMaxRunHeaderSize + chunks_[record.first_chunk].used >
            kTargetDataBlockSize) {
      status = flush_block();
      if (!status.ok()) {
        return status;
      }
    }
    BlobReferenceIndexEntry index_entry;
    index_entry.group = record.group;
    index_entry.start_block = static_cast<uint32_t>(index_.blocks.size());
    index_entry.run_offset = static_cast<uint32_t>(block.size());
    index_.groups.push_back(index_entry);
    if (record.group.encoding_class != BlobReferenceEncodingClass::kDirect) {
      groups_.push_back(record.group);
    }
    if (!AddWithoutOverflow(descriptor_.stored_value_bytes,
                            record.group.stored_value_bytes,
                            &descriptor_.stored_value_bytes)) {
      return Status::Corruption("Blob reference byte total overflow");
    }
    // Each run starts at a chunk, without its join gap, and takes the following
    // chunks whole (join gap and entries) while they fit the block. The run
    // header comes first and holds the count, so the loop counts the run's
    // chunks before it copies them.
    uint32_t start = record.first_chunk;
    while (start != kNoChunk) {
      size_t start_gap = 0;
      status = gap_bytes(start, &start_gap);
      if (!status.ok()) {
        return status;
      }
      size_t run_size = chunks_[start].used - start_gap;
      uint64_t count = chunks_[start].count;
      uint32_t end = chunks_[start].next;
      while (end != kNoChunk &&
             block.size() + kMaxRunHeaderSize + run_size + chunks_[end].used <=
                 kTargetDataBlockSize) {
        run_size += chunks_[end].used;
        count += chunks_[end].count;
        end = chunks_[end].next;
      }
      BlobReferenceRunHeader run;
      run.reference_count = count;
      run.first_offset = chunks_[start].first_offset;
      run.default_compression = record.default_compression;
      EncodeRunHeader(run, &block);
      block.append(ChunkData(start) + start_gap,
                   chunks_[start].used - start_gap);
      for (uint32_t chunk = chunks_[start].next; chunk != end;
           chunk = chunks_[chunk].next) {
        block.append(ChunkData(chunk), chunks_[chunk].used);
      }
      if (end != kNoChunk) {
        // The group continues in the next block with a new run.
        status = flush_block();
        if (!status.ok()) {
          return status;
        }
      }
      start = end;
    }
  }
  status = flush_block();
  block_buffer_bytes_ = 0;
  return status;
}

Status BlobReferenceMetadataCollector::EncodeIndex() {
  const uint64_t max_size = 2 * kMaxVarint64Length +
                            index_.blocks.size() * kMaxBlockTableEntrySize +
                            index_.groups.size() * kMaxIndexEntrySize;
  if (max_size > kMaxTopLevelMetadataBlockSize) {
    return Status::MemoryLimit("Blob reference index exceeds format limit");
  }
  index_block_.clear();
  Status status = ReserveCapacity(&index_block_, max_size);
  if (!status.ok()) {
    return status;
  }
  // Data blocks are written back to back, so the block table stores only
  // the first offset and each block's size.
  PutVarint64(&index_block_, index_.blocks.size());
  if (!index_.blocks.empty()) {
    PutVarint64(&index_block_, index_.blocks.front().offset());
  }
  for (size_t i = 0; i < index_.blocks.size(); ++i) {
    const BlockHandle& handle = index_.blocks[i];
    if (i > 0 && handle.offset() != index_.blocks[i - 1].offset() +
                                        index_.blocks[i - 1].size() +
                                        BlockBasedTable::kBlockTrailerSize) {
      return Status::Corruption(
          "Blob reference data blocks are not back to back");
    }
    PutVarint64(&index_block_, handle.size());
  }
  PutVarint64(&index_block_, index_.groups.size());
  const BlobReferenceIndexEntry* previous = nullptr;
  for (const BlobReferenceIndexEntry& index_entry : index_.groups) {
    const BlobReferenceGroup& group = index_entry.group;
    const bool same_class =
        previous != nullptr &&
        previous->group.encoding_class == group.encoding_class;
    index_block_.push_back(static_cast<char>(group.encoding_class));
    PutVarint64(&index_block_,
                group.generation_file_number -
                    (same_class ? previous->group.generation_file_number : 0));
    if (group.encoding_class ==
        BlobReferenceEncodingClass::kIndirectFlattened) {
      const bool same_generation =
          same_class && previous->group.generation_file_number ==
                            group.generation_file_number;
      PutVarint64(
          &index_block_,
          group.origin_file_number -
              (same_generation ? previous->group.origin_file_number : 0));
    }
    PutVarint64(&index_block_,
                index_entry.start_block -
                    (previous != nullptr ? previous->start_block : 0));
    PutVarint64(&index_block_, index_entry.run_offset);
    PutVarint64(&index_block_, group.reference_count);
    PutVarint64(&index_block_, group.stored_value_bytes);
    if (StoresUserKeySize(group.encoding_class)) {
      // Every entry has a record header, so this does not underflow.
      uint64_t overhead = 0;
      if (!GroupRecordOverhead(group.encoding_class, group.reference_count,
                               &overhead) ||
          group.record_bytes - group.stored_value_bytes < overhead) {
        return Status::Corruption("Invalid blob reference group record bytes");
      }
      PutVarint64(&index_block_,
                  group.record_bytes - group.stored_value_bytes - overhead);
    }
    previous = &index_entry;
  }
  return Status::OK();
}

Status DecodeBlobReferenceIndex(
    const Slice& input, const BlobReferenceMetadataDescriptor& descriptor,
    BlobReferenceIndex* index) {
  if (index == nullptr) {
    return Status::InvalidArgument("Null blob reference index output");
  }
  if (input.size() != descriptor.index_handle.size() ||
      input.size() > kMaxTopLevelMetadataBlockSize) {
    return Status::Corruption("Blob reference index size mismatch");
  }
  Slice cursor = input;
  BlobReferenceIndex decoded;
  uint64_t block_count = 0;
  if (!GetCanonicalVarint64(&cursor, &block_count) ||
      block_count > kMaxDataBlockCount ||
      block_count > cursor.size() / kMinBlockTableEntrySize) {
    return Status::Corruption("Invalid blob reference block count");
  }
  // The data blocks are back to back and the index block follows the last
  // one, so each offset is rebuilt from the first offset and the sizes.
  decoded.blocks.reserve(static_cast<size_t>(block_count));
  uint64_t next_offset = 0;
  if (block_count > 0 && !GetCanonicalVarint64(&cursor, &next_offset)) {
    return Status::Corruption("Invalid blob reference block offset");
  }
  for (uint64_t i = 0; i < block_count; ++i) {
    uint64_t size = 0;
    uint64_t end = 0;
    if (!GetCanonicalVarint64(&cursor, &size) || size == 0 ||
        size > kMaxDataBlockSize ||
        !AddWithoutOverflow(next_offset,
                            size + BlockBasedTable::kBlockTrailerSize, &end) ||
        end > descriptor.index_handle.offset()) {
      return Status::Corruption("Invalid blob reference block handle");
    }
    decoded.blocks.emplace_back(next_offset, size);
    next_offset = end;
  }
  if (block_count > 0 && next_offset != descriptor.index_handle.offset()) {
    return Status::Corruption(
        "Blob reference index does not follow the last data block");
  }
  // There is no fixed limit on the reference count. As a sanity check, the
  // count must fit in the data blocks: an entry takes at least kMinEntrySize
  // (2) bytes, so a block of kMaxDataBlockSize (64 KB) bytes holds at most
  // 32K entries. The writer keeps every block at or below
  // kTargetDataBlockSize (16 KB) uncompressed bytes, so a valid SST always
  // passes. The size check above is on the size on disk, so for a compressed
  // block this is a loose bound, not an exact one.
  if (descriptor.reference_count >
      block_count * (kMaxDataBlockSize / kMinEntrySize)) {
    return Status::Corruption(
        "Blob reference count does not fit its data blocks");
  }

  uint64_t group_count = 0;
  if (!GetCanonicalVarint64(&cursor, &group_count) ||
      group_count > kMaxGroupCount ||
      group_count > cursor.size() / kMinIndexEntrySize ||
      (group_count == 0) != (block_count == 0) ||
      (group_count == 0) != (descriptor.reference_count == 0)) {
    return Status::Corruption("Invalid blob reference group count");
  }
  decoded.groups.reserve(static_cast<size_t>(group_count));
  uint64_t reference_count = 0;
  uint64_t stored_value_bytes = 0;
  for (uint64_t i = 0; i < group_count; ++i) {
    const BlobReferenceIndexEntry* previous =
        decoded.groups.empty() ? nullptr : &decoded.groups.back();
    BlobReferenceIndexEntry entry;
    BlobReferenceGroup& group = entry.group;
    if (cursor.empty()) {
      return Status::Corruption("Truncated blob reference index");
    }
    group.encoding_class = static_cast<BlobReferenceEncodingClass>(
        static_cast<unsigned char>(cursor[0]));
    cursor.remove_prefix(1);
    if (!IsValidEncodingClass(group.encoding_class)) {
      return Status::Corruption("Invalid blob reference encoding class");
    }
    const bool same_class =
        previous != nullptr &&
        previous->group.encoding_class == group.encoding_class;
    uint64_t value = 0;
    if (!GetCanonicalVarint64(&cursor, &value) ||
        !AddWithoutOverflow(
            same_class ? previous->group.generation_file_number : 0, value,
            &group.generation_file_number)) {
      return Status::Corruption("Invalid blob reference group generation");
    }
    group.origin_file_number = group.generation_file_number;
    if (group.encoding_class ==
        BlobReferenceEncodingClass::kIndirectFlattened) {
      const bool same_generation =
          same_class && previous->group.generation_file_number ==
                            group.generation_file_number;
      if (!GetCanonicalVarint64(&cursor, &value) ||
          !AddWithoutOverflow(
              same_generation ? previous->group.origin_file_number : 0, value,
              &group.origin_file_number)) {
        return Status::Corruption("Invalid blob reference group origin");
      }
    }
    if (!group.group_key().IsValid()) {
      return Status::Corruption("Invalid blob reference group");
    }
    if (previous != nullptr &&
        !(previous->group.group_key() < group.group_key())) {
      return Status::Corruption("Blob reference groups are not ordered");
    }
    uint64_t start_delta = 0;
    uint64_t run_offset = 0;
    uint64_t start_block = 0;
    uint64_t user_key_bytes = 0;
    uint64_t overhead = 0;
    if (!GetCanonicalVarint64(&cursor, &start_delta) ||
        !GetCanonicalVarint64(&cursor, &run_offset) ||
        !GetCanonicalVarint64(&cursor, &group.reference_count) ||
        !GetCanonicalVarint64(&cursor, &group.stored_value_bytes) ||
        (StoresUserKeySize(group.encoding_class) &&
         !GetCanonicalVarint64(&cursor, &user_key_bytes)) ||
        group.reference_count == 0 ||
        !GroupRecordOverhead(group.encoding_class, group.reference_count,
                             &overhead) ||
        !AddWithoutOverflow(group.stored_value_bytes, overhead,
                            &group.record_bytes) ||
        !AddWithoutOverflow(group.record_bytes, user_key_bytes,
                            &group.record_bytes) ||
        !AddWithoutOverflow(reference_count, group.reference_count,
                            &reference_count) ||
        !AddWithoutOverflow(stored_value_bytes, group.stored_value_bytes,
                            &stored_value_bytes)) {
      return Status::Corruption("Invalid blob reference group totals");
    }
    if (!AddWithoutOverflow(previous != nullptr ? previous->start_block : 0,
                            start_delta, &start_block) ||
        start_block >= block_count || run_offset >= kMaxDataBlockSize ||
        (previous == nullptr && (start_block != 0 || run_offset != 0)) ||
        (previous != nullptr && start_delta == 0 &&
         run_offset <= previous->run_offset)) {
      return Status::Corruption("Invalid blob reference group position");
    }
    entry.start_block = static_cast<uint32_t>(start_block);
    entry.run_offset = static_cast<uint32_t>(run_offset);
    decoded.groups.push_back(entry);
  }
  if (!cursor.empty()) {
    return Status::Corruption("Trailing bytes in blob reference index");
  }
  if (reference_count != descriptor.reference_count ||
      stored_value_bytes != descriptor.stored_value_bytes) {
    return Status::Corruption("Blob reference index does not match totals");
  }
  *index = std::move(decoded);
  return Status::OK();
}

Status CopyIndirectBlobReferenceGroups(
    const BlobReferenceIndex& index,
    const std::function<Status(size_t buffer_bytes)>& before_allocation,
    std::vector<BlobReferenceGroup>* groups) {
  assert(groups != nullptr);
  size_t count = 0;
  for (const BlobReferenceIndexEntry& entry : index.groups) {
    count += entry.group.encoding_class != BlobReferenceEncodingClass::kDirect;
  }
  if (before_allocation) {
    Status status = before_allocation(count * sizeof(BlobReferenceGroup));
    if (!status.ok()) {
      return status;
    }
  }
  std::vector<BlobReferenceGroup> result;
  result.reserve(count);
  for (const BlobReferenceIndexEntry& entry : index.groups) {
    if (entry.group.encoding_class != BlobReferenceEncodingClass::kDirect) {
      result.push_back(entry.group);
    }
  }
  *groups = std::move(result);
  return Status::OK();
}

BlobReferenceGroupDecoder::BlobReferenceGroupDecoder(
    const BlobReferenceIndex& index, size_t group)
    : index_(index),
      group_(group),
      block_(index.groups[group].start_block),
      offset_(index.groups[group].run_offset),
      remaining_(index.groups[group].group.reference_count) {
  assert(group < index.groups.size());
}

Status BlobReferenceGroupDecoder::DecodeRunHeader(const Slice& contents,
                                                  uint64_t* reference_count) {
  assert(!done());
  // The index decoder checks each block's size on disk. A compressed block
  // is smaller on disk than its contents, so the contents are checked here,
  // after decompression. The count bounds assume this limit for every block.
  if (contents.size() > kMaxDataBlockSize) {
    return Status::Corruption(
        "Blob reference data block is larger than 64 KB after decompression");
  }
  const BlobReferenceGroup& group = index_.groups[group_].group;
  contents_ = contents;
  Status status = DecodeRunHeaderAt(contents_, &offset_, group, &header_);
  if (status.ok() && header_.reference_count > remaining_) {
    status = Status::Corruption("Blob reference run count exceeds group");
  }
  if (status.ok()) {
    *reference_count = header_.reference_count;
  }
  return status;
}

Status BlobReferenceGroupDecoder::DecodeRunEntries(
    const std::function<Status(const BlobReference&)>& visit) {
  Status status = DecodeRunEntriesAt(
      contents_, &offset_, header_, has_previous_ ? &previous_ : nullptr,
      [&](const BlobReference& reference) {
        if (!AddWithoutOverflow(stored_value_bytes_,
                                reference.stored_value_size,
                                &stored_value_bytes_) ||
            !AddWithoutOverflow(record_bytes_, reference.record_bytes,
                                &record_bytes_)) {
          return Status::Corruption("Blob reference byte total overflow");
        }
        previous_ = reference;
        has_previous_ = true;
        return visit(reference);
      });
  if (!status.ok()) {
    return status;
  }
  remaining_ -= header_.reference_count;
  const bool at_block_end = offset_ == contents_.size();
  if (remaining_ != 0) {
    // The group continues at the start of the next block.
    if (!at_block_end || block_ + 1 >= index_.blocks.size()) {
      return Status::Corruption("Blob reference group is cut short");
    }
    ++block_;
    offset_ = 0;
    return Status::OK();
  }
  const BlobReferenceGroup& group = index_.groups[group_].group;
  if (stored_value_bytes_ != group.stored_value_bytes ||
      record_bytes_ != group.record_bytes) {
    return Status::Corruption("Blob reference group totals mismatch");
  }
  // The last run must end where the next group starts, or at the end of its
  // block (the last group: of the last block).
  bool fits = false;
  if (group_ + 1 < index_.groups.size()) {
    const BlobReferenceIndexEntry& next = index_.groups[group_ + 1];
    fits = (next.start_block == block_ && next.run_offset == offset_) ||
           (at_block_end && next.start_block == block_ + 1 &&
            next.run_offset == 0);
  } else {
    fits = at_block_end && block_ + 1 == index_.blocks.size();
  }
  if (!fits) {
    return Status::Corruption("Blob reference run does not end at next run");
  }
  return Status::OK();
}

Status BlobReferenceGroupDecoder::DecodeRun(
    const Slice& contents,
    const std::function<Status(const BlobReference&)>& visit) {
  uint64_t reference_count = 0;
  Status status = DecodeRunHeader(contents, &reference_count);
  return status.ok() ? DecodeRunEntries(visit) : status;
}

namespace {

// Decodes group `group` with blocks from `read_block`. `*loaded_block` is the
// block already in `*contents`, so a block shared by two groups is read once.
Status DecodeGroup(const BlobReferenceIndex& index, size_t group,
                   const BlobReferenceBlockReader& read_block,
                   size_t* loaded_block, Slice* contents,
                   const std::function<Status(const BlobReference&)>& visit) {
  BlobReferenceGroupDecoder decoder(index, group);
  while (!decoder.done()) {
    if (*loaded_block != decoder.next_block()) {
      Status status = read_block(decoder.next_block(), contents);
      if (!status.ok()) {
        return status;
      }
      *loaded_block = decoder.next_block();
    }
    Status status = decoder.DecodeRun(*contents, visit);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

}  // namespace

Status ForEachBlobReferenceInGroup(
    const BlobReferenceIndex& index, size_t group,
    const BlobReferenceBlockReader& read_block,
    const std::function<Status(const BlobReference&)>& visit) {
  if (group >= index.groups.size()) {
    return Status::InvalidArgument("Blob reference group out of range");
  }
  size_t loaded_block = std::numeric_limits<size_t>::max();
  Slice contents;
  return DecodeGroup(index, group, read_block, &loaded_block, &contents, visit);
}

Status ForEachBlobReference(
    const BlobReferenceIndex& index, const BlobReferenceBlockReader& read_block,
    const std::function<Status(const BlobReference&)>& visit) {
  // Groups are visited in index order. Each group's runs end where the next
  // group starts, so the walk covers every block from start to end; each
  // block is read once.
  size_t loaded_block = std::numeric_limits<size_t>::max();
  Slice contents;
  for (size_t group = 0; group < index.groups.size(); ++group) {
    Status status =
        DecodeGroup(index, group, read_block, &loaded_block, &contents, visit);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
