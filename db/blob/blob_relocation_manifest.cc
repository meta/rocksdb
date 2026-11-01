// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_relocation_manifest.h"

#include <algorithm>
#include <limits>
#include <set>
#include <sstream>
#include <utility>

#include "util/coding.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

size_t BlobRelocationState::IncrementalRetentionBytes(
    const BlobRelocationState& base,
    const std::vector<BlobRelocationRouteSource>& changed_routes,
    const std::vector<uint64_t>& changed_files) const {
  size_t bytes = sizeof(*this);
  const auto add = [&bytes](size_t count, size_t item_bytes) {
    if (item_bytes == 0) {
      return;
    }
    if (count > (std::numeric_limits<size_t>::max() - bytes) / item_bytes) {
      bytes = std::numeric_limits<size_t>::max();
    } else {
      bytes += count * item_bytes;
    }
  };
  add(1, files_.RetainedShardBytes(base.files_));
  add(1, routes_.RetainedShardBytes(base.routes_));
  add(1, targets_.RetainedShardBytes(base.targets_));
  std::set<uint64_t> targets;
  for (const BlobRelocationRouteSource& source : changed_routes) {
    const BlobRelocationRoute* const old_route = base.FindRoute(source);
    const BlobRelocationRoute* const new_route = FindRoute(source);
    if (old_route != nullptr) {
      targets.insert(old_route->GetPhysicalFileNumber());
    }
    if (new_route != nullptr) {
      targets.insert(new_route->GetPhysicalFileNumber());
    }
  }
  for (uint64_t file_number : targets) {
    const std::shared_ptr<TargetSummary>* const old_target =
        base.targets_.Find(file_number);
    const std::shared_ptr<TargetSummary>* const new_target =
        targets_.Find(file_number);
    if (old_target != nullptr && new_target != nullptr &&
        *old_target == *new_target) {
      continue;
    }
    const std::array<const TargetSummary*, 2> summaries{
        old_target != nullptr ? old_target->get() : nullptr,
        new_target != nullptr ? new_target->get() : nullptr};
    for (const TargetSummary* summary : summaries) {
      if (summary == nullptr) {
        continue;
      }
      add(1, sizeof(TargetSummary) + 4 * sizeof(void*));
      add(summary->sources.capacity(), sizeof(BlobRelocationRouteSource));
      add(summary->origin_counts.size(),
          sizeof(std::map<uint64_t, size_t>::value_type) + 4 * sizeof(void*));
    }
  }
  for (uint64_t file_number : changed_files) {
    const std::shared_ptr<BlobRelocationFileMetaData> old_file =
        base.FindFile(file_number);
    const std::shared_ptr<BlobRelocationFileMetaData> new_file =
        FindFile(file_number);
    if (old_file == new_file) {
      continue;
    }
    const std::array<const BlobRelocationFileMetaData*, 2> files{
        old_file.get(), new_file.get()};
    for (const BlobRelocationFileMetaData* file : files) {
      if (file == nullptr) {
        continue;
      }
      add(1, sizeof(BlobRelocationFileMetaData) + 8 * sizeof(void*));
      add(file->GetChecksumMethod().capacity(), 1);
      add(file->GetChecksumValue().capacity(), 1);
      add(file->GetDirectory().origins().capacity(),
          sizeof(BlobRelocationOrigin));
      // A one-byte varint can decode to an eight-byte generation, and vectors
      // can retain geometrically grown capacity. This covers served-generation
      // buffers plus the shared encoded directory without decoding/scanning it.
      add(file->GetAddition().GetEncodedDirectory().capacity(), 17);
    }
  }
  return bytes;
}

std::shared_ptr<BlobRelocationFileMetaData> BlobRelocationState::FindFile(
    uint64_t file_number) const {
  const std::shared_ptr<BlobRelocationFileMetaData>* file =
      files_.Find(file_number);
  return file != nullptr ? *file : nullptr;
}

void BlobRelocationState::SetFile(
    std::shared_ptr<BlobRelocationFileMetaData> file) {
  assert(file != nullptr);
  const uint64_t file_number = file->GetFileNumber();
  const std::shared_ptr<BlobRelocationFileMetaData> previous =
      FindFile(file_number);
  if (previous != nullptr && previous->IsMarkedMissing()) {
    --missing_file_count_;
  }
  if (file->IsMarkedMissing()) {
    ++missing_file_count_;
  }
  if (previous != nullptr && !aggregate_overflow_) {
    physical_file_bytes_ -= previous->GetFileSize();
    record_bytes_ -= previous->GetDirectory().total_record_bytes();
  }
  if (file->GetFileSize() >
          std::numeric_limits<uint64_t>::max() - physical_file_bytes_ ||
      file->GetDirectory().total_record_bytes() >
          std::numeric_limits<uint64_t>::max() - record_bytes_) {
    aggregate_overflow_ = true;
  } else if (!aggregate_overflow_) {
    physical_file_bytes_ += file->GetFileSize();
    record_bytes_ += file->GetDirectory().total_record_bytes();
  }
  files_.Set(file_number, std::move(file));
  validated_.store(false, std::memory_order_relaxed);
}

void BlobRelocationState::DeleteFile(uint64_t file_number) {
  const std::shared_ptr<BlobRelocationFileMetaData> file =
      FindFile(file_number);
  if (file != nullptr && file->IsMarkedMissing()) {
    --missing_file_count_;
  }
  if (file != nullptr && !aggregate_overflow_) {
    physical_file_bytes_ -= file->GetFileSize();
    record_bytes_ -= file->GetDirectory().total_record_bytes();
  }
  files_.Erase(file_number);
  validated_.store(false, std::memory_order_relaxed);
}

std::shared_ptr<BlobRelocationState::TargetSummary>
BlobRelocationState::MutableTarget(uint64_t file_number) {
  // Clone the target shard BEFORE checking the summary's sharing count: that
  // clone itself can introduce another owner of the summary.
  std::shared_ptr<TargetSummary>* target = targets_.FindMutable(file_number);
  if (target == nullptr) {
    targets_.Set(file_number, std::make_shared<TargetSummary>());
    target = targets_.FindMutable(file_number);
  } else if (target->use_count() != 1) {
    *target = std::make_shared<TargetSummary>(**target);
  }
  return *target;
}

void BlobRelocationState::RemoveTargetSource(const BlobRelocationRoute& route) {
  const std::shared_ptr<TargetSummary> target =
      MutableTarget(route.GetPhysicalFileNumber());
  std::vector<BlobRelocationRouteSource>::iterator source = std::lower_bound(
      target->sources.begin(), target->sources.end(), route.GetSource());
  assert(source != target->sources.end() && *source == route.GetSource());
  target->sources.erase(source);
  std::map<uint64_t, size_t>::iterator count =
      target->origin_counts.find(route.GetOriginFileNumber());
  assert(count != target->origin_counts.end() && count->second != 0);
  if (--count->second == 0) {
    target->origin_counts.erase(count);
  }
  if (target->sources.empty()) {
    targets_.Erase(route.GetPhysicalFileNumber());
  }
}

void BlobRelocationState::SetRoute(BlobRelocationRoute route) {
  const BlobRelocationRouteSource source = route.GetSource();
  const BlobRelocationRoute* previous = routes_.Find(source);
  if (previous != nullptr) {
    if (*previous == route) {
      return;
    }
    RemoveTargetSource(*previous);
  }
  const std::shared_ptr<TargetSummary> target =
      MutableTarget(route.GetPhysicalFileNumber());
  target->sources.insert(
      std::lower_bound(target->sources.begin(), target->sources.end(), source),
      source);
  ++target->origin_counts[route.GetOriginFileNumber()];
  routes_.Set(source, std::move(route));
  validated_.store(false, std::memory_order_relaxed);
}

void BlobRelocationState::DeleteRoute(const BlobRelocationRouteSource& source) {
  const BlobRelocationRoute* previous = routes_.Find(source);
  if (previous != nullptr) {
    RemoveTargetSource(*previous);
    routes_.Erase(source);
    validated_.store(false, std::memory_order_relaxed);
  }
}

const std::vector<BlobRelocationRouteSource>*
BlobRelocationState::SourcesForTarget(uint64_t physical_file_number) const {
  const std::shared_ptr<TargetSummary>* target =
      targets_.Find(physical_file_number);
  return target != nullptr ? &(*target)->sources : nullptr;
}

size_t BlobRelocationState::TargetOriginCount(
    uint64_t physical_file_number, uint64_t origin_file_number) const {
  const std::shared_ptr<TargetSummary>* target =
      targets_.Find(physical_file_number);
  if (target == nullptr) {
    return 0;
  }
  const std::map<uint64_t, size_t>::const_iterator count =
      (*target)->origin_counts.find(origin_file_number);
  return count != (*target)->origin_counts.end() ? count->second : 0;
}

void BlobRelocationState::ComputeFingerprint(uint32_t format_version,
                                             uint64_t* high, uint64_t* low) {
  // A canonical digest of fixed shards is independent of replay batching and
  // process identity. Only changed shards are re-encoded; an empty shard has
  // the same zero digest whether never created or emptied by deletion.
  std::string encoded;
  PutFixed32(&encoded, 2U /* private CompactionService fingerprint format */);
  PutFixed32(&encoded, format_version);
  PutFixed64(&encoded, files_.values().size());
  PutFixed64(&encoded, routes_.values().size());
  for (size_t shard = 0; shard < FileIndex::kShardCount; ++shard) {
    if (files_.dirty(shard)) {
      std::string shard_bytes;
      const std::shared_ptr<FileIndex::Map>& files = files_.shard(shard);
      if (files != nullptr) {
        for (const FileIndex::Map::value_type& entry : *files) {
          const std::shared_ptr<BlobRelocationFileMetaData>& file =
              entry.second;
          PutFixed64(&shard_bytes, file->GetFileNumber());
          PutFixed32(&shard_bytes, file->GetColumnFamilyId());
          PutFixed64(&shard_bytes, file->GetFileSize());
          PutLengthPrefixedSlice(&shard_bytes, file->GetChecksumMethod());
          PutLengthPrefixedSlice(&shard_bytes, file->GetChecksumValue());
          PutFixed32(&shard_bytes, file->GetDirectoryDigest());
          PutFixed32(&shard_bytes, file->IsMarkedMissing() ? 1U : 0U);
        }
      }
      uint64_t shard_high = 0;
      uint64_t shard_low = 0;
      if (!shard_bytes.empty()) {
        Hash2x64(shard_bytes.data(), shard_bytes.size(), &shard_high,
                 &shard_low);
      }
      files_.SetFingerprint(shard, shard_high, shard_low);
    }
    PutFixed64(&encoded, files_.fingerprint_high(shard));
    PutFixed64(&encoded, files_.fingerprint_low(shard));
    if (routes_.dirty(shard)) {
      std::string shard_bytes;
      const std::shared_ptr<RouteIndex::Map>& routes = routes_.shard(shard);
      if (routes != nullptr) {
        for (const RouteIndex::Map::value_type& entry : *routes) {
          const BlobRelocationRoute& route = entry.second;
          PutFixed64(&shard_bytes, route.GetGenerationFileNumber());
          PutFixed64(&shard_bytes, route.GetOriginFileNumber());
          PutFixed64(&shard_bytes, route.GetPhysicalFileNumber());
        }
      }
      uint64_t shard_high = 0;
      uint64_t shard_low = 0;
      if (!shard_bytes.empty()) {
        Hash2x64(shard_bytes.data(), shard_bytes.size(), &shard_high,
                 &shard_low);
      }
      routes_.SetFingerprint(shard, shard_high, shard_low);
    }
    PutFixed64(&encoded, routes_.fingerprint_high(shard));
    PutFixed64(&encoded, routes_.fingerprint_low(shard));
  }
  Hash2x64(encoded.data(), encoded.size(), high, low);
}

BlobRelocationFileAddition::BlobRelocationFileAddition(
    uint64_t file_number, uint32_t column_family_id, uint64_t file_size,
    std::string checksum_method, std::string checksum_value,
    std::string encoded_directory, uint64_t tail_size)
    : file_number_(file_number),
      column_family_id_(column_family_id),
      file_size_(file_size),
      tail_size_(tail_size),
      checksum_method_(std::move(checksum_method)),
      checksum_value_(std::move(checksum_value)),
      encoded_directory_(
          std::make_shared<const std::string>(std::move(encoded_directory))) {
  Status status = CacheDirectoryValidation();
  status.PermitUncheckedError();
}

const std::string& BlobRelocationFileAddition::GetEncodedDirectory() const {
  static const std::string empty;
  return encoded_directory_ != nullptr ? *encoded_directory_ : empty;
}

Status BlobRelocationFileAddition::ValidateScalars() const {
  if (format_version_ != kFormatVersion) {
    return Status::NotSupported("Unknown blob relocation manifest format");
  }
  if (file_number_ == kInvalidBlobFileNumber) {
    return Status::Corruption("Invalid blob relocation file number");
  }
  if (file_size_ <= kBlobRelocationFilePrefixSize) {
    return Status::Corruption("Invalid blob relocation file size");
  }
  // Normal opens must have the exact builder-produced metadata range. Zero is
  // reserved for offline discovery, which has no durable addition yet.
  if (tail_size_ == 0 || tail_size_ > file_size_) {
    return Status::Corruption("Invalid blob relocation metadata tail size");
  }
  if (checksum_method_.empty() != checksum_value_.empty()) {
    return Status::Corruption("Incomplete blob relocation file checksum");
  }
  if (GetEncodedDirectory().empty()) {
    return Status::Corruption("Missing blob relocation directory");
  }
  return Status::OK();
}

Status BlobRelocationFileAddition::DecodeAndValidateDirectory(
    std::shared_ptr<const BlobRelocationDirectory>* directory) const {
  if (directory == nullptr) {
    return Status::InvalidArgument("Null blob relocation directory output");
  }
  BlobRelocationDirectory decoded;
  Status status =
      BlobRelocationDirectory::DecodeFrom(GetEncodedDirectory(), &decoded);
  if (!status.ok()) {
    return status;
  }
  status = decoded.ValidateForFileSize(file_size_);
  if (!status.ok()) {
    return status;
  }
  if (decoded.origins().empty() || decoded.total_entries() == 0) {
    return Status::Corruption("Empty blob relocation directory");
  }
  *directory =
      std::make_shared<const BlobRelocationDirectory>(std::move(decoded));
  return Status::OK();
}

Status BlobRelocationFileAddition::CacheDirectoryValidation() {
  assert(!directory_validation_attempted_);
  directory_validation_attempted_ = true;
  directory_validation_status_ = ValidateScalars();
  if (directory_validation_status_.ok()) {
    directory_validation_status_ =
        DecodeAndValidateDirectory(&decoded_directory_);
  }
  return directory_validation_status_;
}

Status BlobRelocationFileAddition::GetValidatedDirectory(
    std::shared_ptr<const BlobRelocationDirectory>* directory) const {
  if (directory == nullptr) {
    return Status::InvalidArgument("Null blob relocation directory output");
  }
  if (directory_validation_attempted_) {
    if (!directory_validation_status_.ok()) {
      return directory_validation_status_;
    }
    assert(decoded_directory_ != nullptr);
    *directory = decoded_directory_;
    return Status::OK();
  }
  Status status = ValidateScalars();
  return status.ok() ? DecodeAndValidateDirectory(directory) : status;
}

Status BlobRelocationFileAddition::Validate(
    BlobRelocationDirectory* directory) const {
  std::shared_ptr<const BlobRelocationDirectory> decoded;
  Status status = GetValidatedDirectory(&decoded);
  if (!status.ok()) {
    return status;
  }
  if (directory != nullptr) {
    *directory = *decoded;
  }
  return Status::OK();
}

void BlobRelocationFileAddition::EncodeTo(std::string* output) const {
  PutVarint32(output, format_version_);
  PutVarint64(output, file_number_);
  PutVarint32(output, column_family_id_);
  PutVarint64(output, file_size_);
  PutLengthPrefixedSlice(output, checksum_method_);
  PutLengthPrefixedSlice(output, checksum_value_);
  PutLengthPrefixedSlice(output, GetEncodedDirectory());
  PutVarint64(output, tail_size_);
}

Status BlobRelocationFileAddition::DecodeFrom(Slice* input) {
  if (input == nullptr) {
    return Status::InvalidArgument("Null blob relocation addition input");
  }

  Slice checksum_method;
  Slice checksum_value;
  Slice encoded_directory;
  if (!GetVarint32(input, &format_version_)) {
    return Status::Corruption("Missing blob relocation addition format");
  }
  if (format_version_ != kFormatVersion) {
    return Status::NotSupported("Unknown blob relocation manifest format");
  }
  if (!GetVarint64(input, &file_number_) ||
      !GetVarint32(input, &column_family_id_) ||
      !GetVarint64(input, &file_size_) ||
      !GetLengthPrefixedSlice(input, &checksum_method) ||
      !GetLengthPrefixedSlice(input, &checksum_value) ||
      !GetLengthPrefixedSlice(input, &encoded_directory) ||
      !GetVarint64(input, &tail_size_) || !input->empty()) {
    return Status::Corruption("Malformed blob relocation file addition");
  }

  checksum_method_.assign(checksum_method.data(), checksum_method.size());
  checksum_value_.assign(checksum_value.data(), checksum_value.size());
  encoded_directory_ = std::make_shared<const std::string>(
      encoded_directory.data(), encoded_directory.size());
  decoded_directory_.reset();
  directory_validation_status_ = Status::OK();
  directory_validation_attempted_ = false;
  return CacheDirectoryValidation();
}

std::string BlobRelocationFileAddition::DebugString() const {
  std::ostringstream stream;
  stream << "file=" << file_number_ << " cf=" << column_family_id_
         << " size=" << file_size_ << " tail_size=" << tail_size_;
  std::shared_ptr<const BlobRelocationDirectory> directory;
  if (GetValidatedDirectory(&directory).ok()) {
    stream << " origins=" << directory->origins().size()
           << " entries=" << directory->total_entries()
           << " directory_digest=" << directory->directory_digest();
  } else {
    stream << " invalid";
  }
  return stream.str();
}

bool operator==(const BlobRelocationFileAddition& lhs,
                const BlobRelocationFileAddition& rhs) {
  return lhs.GetFileNumber() == rhs.GetFileNumber() &&
         lhs.GetColumnFamilyId() == rhs.GetColumnFamilyId() &&
         lhs.GetFileSize() == rhs.GetFileSize() &&
         lhs.GetTailSize() == rhs.GetTailSize() &&
         lhs.GetChecksumMethod() == rhs.GetChecksumMethod() &&
         lhs.GetChecksumValue() == rhs.GetChecksumValue() &&
         lhs.GetEncodedDirectory() == rhs.GetEncodedDirectory();
}

bool operator!=(const BlobRelocationFileAddition& lhs,
                const BlobRelocationFileAddition& rhs) {
  return !(lhs == rhs);
}

Status BlobRelocationRoute::Validate() const {
  if (format_version_ != kBlobIndirectionManifestFormatVersion) {
    return Status::NotSupported("Unknown blob relocation route format");
  }
  if (source_.generation_file_number == kInvalidBlobFileNumber ||
      source_.origin_file_number == kInvalidBlobFileNumber ||
      physical_file_number_ == kInvalidBlobFileNumber) {
    return Status::Corruption("Invalid blob relocation route file number");
  }
  if (source_.generation_file_number == physical_file_number_) {
    return Status::Corruption("Blob relocation route targets its generation");
  }
  return Status::OK();
}

void BlobRelocationRoute::EncodeTo(std::string* output) const {
  PutVarint32(output, format_version_);
  PutVarint64(output, source_.generation_file_number);
  PutVarint64(output, source_.origin_file_number);
  PutVarint64(output, physical_file_number_);
}

Status BlobRelocationRoute::DecodeFrom(Slice* input) {
  if (input == nullptr) {
    return Status::InvalidArgument("Null blob relocation route input");
  }
  if (!GetVarint32(input, &format_version_) ||
      !GetVarint64(input, &source_.generation_file_number) ||
      !GetVarint64(input, &source_.origin_file_number) ||
      !GetVarint64(input, &physical_file_number_) || !input->empty()) {
    return Status::Corruption("Malformed blob relocation route");
  }
  return Validate();
}

std::string BlobRelocationRoute::DebugString() const {
  std::ostringstream stream;
  stream << "generation=" << source_.generation_file_number
         << " origin=" << source_.origin_file_number
         << " physical=" << physical_file_number_;
  return stream.str();
}

bool operator==(const BlobRelocationRoute& lhs,
                const BlobRelocationRoute& rhs) {
  return lhs.GetSource() == rhs.GetSource() &&
         lhs.GetPhysicalFileNumber() == rhs.GetPhysicalFileNumber();
}

bool operator!=(const BlobRelocationRoute& lhs,
                const BlobRelocationRoute& rhs) {
  return !(lhs == rhs);
}

Status BlobFileRetirement::Validate() const {
  if (format_version_ != kBlobIndirectionManifestFormatVersion) {
    return Status::NotSupported("Unknown blob file retirement format");
  }
  if (source_file_number_ == kInvalidBlobFileNumber) {
    return Status::Corruption("Invalid retired blob file number");
  }
  if ((live_blob_count_ == 0) != (live_source_record_bytes_ == 0) ||
      (live_blob_count_ == 0) != (relocated_record_bytes_ == 0)) {
    return Status::Corruption("Inconsistent retired blob live aggregates");
  }
  return Status::OK();
}

void BlobFileRetirement::EncodeTo(std::string* output) const {
  PutVarint32(output, format_version_);
  PutVarint64(output, source_file_number_);
  PutVarint64(output, live_blob_count_);
  PutVarint64(output, live_source_record_bytes_);
  PutVarint64(output, relocated_record_bytes_);
}

Status BlobFileRetirement::DecodeFrom(Slice* input) {
  if (input == nullptr) {
    return Status::InvalidArgument("Null blob file retirement input");
  }
  if (!GetVarint32(input, &format_version_) ||
      !GetVarint64(input, &source_file_number_) ||
      !GetVarint64(input, &live_blob_count_) ||
      !GetVarint64(input, &live_source_record_bytes_) ||
      !GetVarint64(input, &relocated_record_bytes_) || !input->empty()) {
    return Status::Corruption("Malformed blob file retirement");
  }
  return Validate();
}

std::string BlobFileRetirement::DebugString() const {
  std::ostringstream stream;
  stream << "source=" << source_file_number_
         << " live_blobs=" << live_blob_count_
         << " live_source_record_bytes=" << live_source_record_bytes_
         << " relocated_record_bytes=" << relocated_record_bytes_;
  return stream.str();
}

bool operator==(const BlobFileRetirement& lhs, const BlobFileRetirement& rhs) {
  return lhs.GetSourceFileNumber() == rhs.GetSourceFileNumber() &&
         lhs.GetLiveBlobCount() == rhs.GetLiveBlobCount() &&
         lhs.GetLiveSourceRecordBytes() == rhs.GetLiveSourceRecordBytes() &&
         lhs.GetRelocatedRecordBytes() == rhs.GetRelocatedRecordBytes();
}

bool operator!=(const BlobFileRetirement& lhs, const BlobFileRetirement& rhs) {
  return !(lhs == rhs);
}

}  // namespace ROCKSDB_NAMESPACE
