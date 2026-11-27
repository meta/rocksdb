// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_relocation_manifest.h"

#include <algorithm>
#include <limits>
#include <map>
#include <new>
#include <sstream>
#include <utility>

#ifdef __SSE2__
#include <emmintrin.h>
#endif  // __SSE2__

#include "cache/cache_reservation_manager.h"
#include "db/blob/blob_file_meta.h"
#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_read_request.h"
#include "port/lang.h"
#include "util/coding.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

// BlobFileTable

namespace {

// Tests set this to check the plain scan on hosts with SSE2.
std::atomic<bool> blob_file_table_plain_scan{false};

uint64_t BlobFileTableHash(uint64_t key) {
  // fmix64 of MurmurHash3: consecutive file numbers spread over the shards.
  key ^= key >> 33;
  key *= 0xff51afd7ed558ccdULL;
  key ^= key >> 33;
  key *= 0xc4ceb9fe1a85ec53ULL;
  key ^= key >> 33;
  return key;
}

// Scans return the 1-based index of `key`, or 0. Keys are unique and the
// padding is 0, which is never a key, so the scans may read the padding.
template <typename Key>
uint32_t PlainScan(const Key* keys, uint32_t padded, Key key) {
  uint32_t index = 0;
  for (uint32_t i = 0; i < padded; ++i) {
    index += keys[i] == key ? i + 1 : 0;
  }
  return index;
}

#ifdef __SSE2__
uint32_t Sse2Scan(const uint32_t* keys, uint32_t padded, uint32_t key) {
  const __m128i needle = _mm_set1_epi32(static_cast<int>(key));
  for (uint32_t i = 0; i < padded; i += 4) {
    const __m128i equal = _mm_cmpeq_epi32(
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(keys + i)), needle);
    const int mask = _mm_movemask_ps(_mm_castsi128_ps(equal));
    if (mask != 0) {
      return i + static_cast<uint32_t>(__builtin_ctz(mask)) + 1;
    }
  }
  return 0;
}

uint32_t Sse2Scan(const uint64_t* keys, uint32_t padded, uint64_t key) {
  const __m128i needle = _mm_set1_epi64x(static_cast<long long>(key));
  for (uint32_t i = 0; i < padded; i += 2) {
    const __m128i equal = _mm_cmpeq_epi32(
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(keys + i)), needle);
    // Both 32-bit halves must match: AND with the halves swapped.
    const __m128i both =
        _mm_and_si128(equal, _mm_shuffle_epi32(equal, _MM_SHUFFLE(2, 3, 0, 1)));
    const int mask = _mm_movemask_pd(_mm_castsi128_pd(both));
    if (mask != 0) {
      return i + static_cast<uint32_t>(__builtin_ctz(mask)) + 1;
    }
  }
  return 0;
}
#endif  // __SSE2__

template <typename Key>
uint32_t ScanKeys(const Key* keys, uint32_t padded, Key key) {
#ifdef __SSE2__
  if (!blob_file_table_plain_scan.load(std::memory_order_relaxed)) {
    return Sse2Scan(keys, padded, key);
  }
#endif  // __SSE2__
  return PlainScan(keys, padded, key);
}

}  // namespace

void BlobFileTable::TEST_UsePlainScan(bool plain) {
  blob_file_table_plain_scan.store(plain, std::memory_order_relaxed);
}

BlobFileTable::BlobFileTable(const BlobFileTable& other)
    : shards_(other.shards_), size_(other.size_), charge_(other.charge_) {
  for (Shard* shard : shards_) {
    if (shard != nullptr) {
      Ref(shard);
    }
  }
  Charge(TopBytes(), 0);
}

BlobFileTable& BlobFileTable::operator=(const BlobFileTable& other) {
  if (this != &other) {
    BlobFileTable copy(other);
    std::swap(shards_, copy.shards_);
    std::swap(size_, copy.size_);
    std::swap(charge_, copy.charge_);
  }
  return *this;
}

BlobFileTable::~BlobFileTable() {
  size_t freed = TopBytes();
  for (Shard* shard : shards_) {
    if (shard != nullptr) {
      freed += Unref(shard);
    }
  }
  Charge(0, freed);
}

void BlobFileTable::SetCacheReservationManager(
    std::shared_ptr<CacheReservationManager> manager) {
  // A table with shards may share them with tables of another manager.
  if (shards_.empty()) {
    charge_ = std::move(manager);
  }
}

void BlobFileTable::Charge(size_t added, size_t freed) const {
  if (charge_ == nullptr || added == freed) {
    return;
  }
  // Best effort: a failed reservation never fails an edit. The manager still
  // tracks the usage when the cache refuses the reservation.
  Status s = added > freed
                 ? charge_->UpdateCacheReservation(added - freed,
                                                   /*increase=*/true)
                 : charge_->UpdateCacheReservation(freed - added,
                                                   /*increase=*/false);
  s.PermitUncheckedError();
}

void BlobFileTable::Ref(Shard* shard) {
  shard->refs.fetch_add(1, std::memory_order_relaxed);
}

size_t BlobFileTable::Unref(Shard* shard) {
  if (shard->refs.fetch_sub(1, std::memory_order_acq_rel) != 1) {
    return 0;
  }
  const size_t bytes = shard->Bytes();
  shard->~Shard();
  ::operator delete(shard);
  return bytes;
}

BlobFileTable::Shard* BlobFileTable::MakeShard(const std::vector<Entry>& run) {
  assert(!run.empty());
  const uint32_t count = static_cast<uint32_t>(run.size());
  const uint32_t padded = (count + 3) & ~uint32_t{3};
  const bool wide = std::any_of(run.begin(), run.end(), [](const Entry& entry) {
    return (entry.key >> 32) != 0;
  });
  const size_t bytes = sizeof(Shard) + size_t{padded} * (wide ? 8 : 4) +
                       size_t{count} * (sizeof(uint64_t) + sizeof(uint32_t));
  Shard* shard = static_cast<Shard*>(::operator new(bytes));
  new (shard) Shard();
  shard->refs.store(1, std::memory_order_relaxed);
  shard->count = count;
  shard->padded = padded;
  shard->wide = wide ? 1 : 0;
  for (uint32_t i = 0; i < padded; ++i) {
    const uint64_t key = i < count ? run[i].key : 0;
    assert(i >= count || key != kInvalidBlobFileNumber);
    if (wide) {
      shard->keys64()[i] = key;
    } else {
      shard->keys32()[i] = static_cast<uint32_t>(key);
    }
  }
  for (uint32_t i = 0; i < count; ++i) {
    assert(run[i].value != 0);
    shard->values()[i] = run[i].value;
    shard->counts()[i] = run[i].count;
  }
  return shard;
}

uint64_t BlobFileTable::Find(uint64_t file_number, uint32_t* count) const {
  if (count != nullptr) {
    *count = 0;
  }
  if (shards_.empty() || file_number == kInvalidBlobFileNumber) {
    return 0;
  }
  const Shard* shard =
      shards_[BlobFileTableHash(file_number) & (shards_.size() - 1)];
  if (shard == nullptr) {
    return 0;
  }
  uint32_t index = 0;
  if (shard->wide) {
    index = ScanKeys(shard->keys64(), shard->padded, file_number);
  } else if ((file_number >> 32) == 0) {
    index = ScanKeys(shard->keys32(), shard->padded,
                     static_cast<uint32_t>(file_number));
  }
  if (index == 0) {
    return 0;
  }
  if (count != nullptr) {
    *count = shard->counts()[index - 1];
  }
  return shard->values()[index - 1];
}

void BlobFileTable::Apply(const std::vector<Op>& ops) {
  if (ops.empty()) {
    return;
  }
  size_t added = 0;
  size_t freed = 0;
  if (shards_.empty()) {
    shards_.assign(1, nullptr);
    added += TopBytes();
  }
  const size_t mask = shards_.size() - 1;
  // Ops grouped by shard, in their original order within a shard.
  std::map<size_t, std::vector<const Op*>> ops_by_shard;
  for (const Op& op : ops) {
    assert(op.file_number != kInvalidBlobFileNumber);
    ops_by_shard[BlobFileTableHash(op.file_number) & mask].push_back(&op);
  }
  for (const auto& shard_ops : ops_by_shard) {
    Shard*& slot = shards_[shard_ops.first];
    std::map<uint64_t, std::pair<uint64_t, uint32_t>> entries;
    if (slot != nullptr) {
      for (uint32_t i = 0; i < slot->count; ++i) {
        entries.emplace(slot->Key(i),
                        std::make_pair(slot->values()[i], slot->counts()[i]));
      }
      size_ -= slot->count;
      // Other tables may share the old shard; this table drops it.
      freed += Unref(slot);
      slot = nullptr;
    }
    for (const Op* op : shard_ops.second) {
      if (op->value == 0) {
        entries.erase(op->file_number);
      } else {
        entries[op->file_number] = std::make_pair(op->value, op->count);
      }
    }
    if (!entries.empty()) {
      std::vector<Entry> run;
      run.reserve(entries.size());
      for (const auto& entry : entries) {
        run.push_back({entry.first, entry.second.first, entry.second.second});
      }
      slot = MakeShard(run);
      added += slot->Bytes();
      size_ += run.size();
    }
  }
  const size_t shard_count = shards_.size();
  if (size_ > 2 * kTargetEntriesPerShard * shard_count ||
      (shard_count > 1 && size_ * 4 < kTargetEntriesPerShard * shard_count)) {
    size_t new_count = 1;
    while (new_count * kTargetEntriesPerShard < size_) {
      new_count <<= 1;
    }
    Rebuild(new_count, &added, &freed);
  }
  Charge(added, freed);
}

void BlobFileTable::Rebuild(size_t shard_count, size_t* added, size_t* freed) {
  assert(shard_count > 0 && (shard_count & (shard_count - 1)) == 0);
  std::vector<std::vector<Entry>> runs(shard_count);
  ForEach([&](uint64_t file_number, uint64_t value, uint32_t count) {
    runs[BlobFileTableHash(file_number) & (shard_count - 1)].push_back(
        {file_number, value, count});
  });
  std::vector<Shard*> shards(shard_count, nullptr);
  for (size_t i = 0; i < shard_count; ++i) {
    if (!runs[i].empty()) {
      std::sort(
          runs[i].begin(), runs[i].end(),
          [](const Entry& lhs, const Entry& rhs) { return lhs.key < rhs.key; });
      shards[i] = MakeShard(runs[i]);
      *added += shards[i]->Bytes();
    }
  }
  *freed += TopBytes();
  std::swap(shards_, shards);
  *added += TopBytes();
  for (Shard* shard : shards) {
    if (shard != nullptr) {
      *freed += Unref(shard);
    }
  }
}

size_t BlobFileTable::ApproximateMemoryUsage() const {
  size_t bytes = TopBytes();
  for (const Shard* shard : shards_) {
    if (shard != nullptr) {
      bytes += shard->Bytes();
    }
  }
  return bytes;
}

namespace {

// Each relocation file and each route has its own 128-bit digest: Hash2x64 of
// an encoding that starts with a type tag (1 = file, 2 = route).
// BlobRelocationState adds the digest of an item when the item is set and
// subtracts it when the item is replaced or deleted. Sums do not depend on
// order, so the state fingerprint does not depend on the order of edits.
// AddDigest() adds the digest of `encoded` when `add` is true, else subtracts
// it.
void AddDigest(const std::string& encoded, bool add, uint64_t* high,
               uint64_t* low) {
  uint64_t digest_high = 0;
  uint64_t digest_low = 0;
  Hash2x64(encoded.data(), encoded.size(), &digest_high, &digest_low);
  if (add) {
    *high += digest_high;
    *low += digest_low;
  } else {
    *high -= digest_high;
    *low -= digest_low;
  }
}

std::string EncodeFileForDigest(const BlobRelocationFileMetaData& file) {
  std::string encoded;
  PutFixed32(&encoded, 1U /* relocation file */);
  PutFixed64(&encoded, file.GetFileNumber());
  PutFixed64(&encoded, file.GetFileSize());
  PutLengthPrefixedSlice(&encoded, file.GetChecksumMethod());
  PutLengthPrefixedSlice(&encoded, file.GetChecksumValue());
  PutFixed32(&encoded, file.GetDirectoryDigest());
  PutFixed32(&encoded, file.IsMarkedLost() ? 1U : 0U);
  return encoded;
}

std::string EncodeRouteForDigest(uint64_t source, uint64_t target) {
  std::string encoded;
  PutFixed32(&encoded, 2U /* route */);
  PutFixed64(&encoded, source);
  PutFixed64(&encoded, target);
  return encoded;
}

}  // namespace

// BlobRelocationState

const BlobRelocationState::Files& BlobRelocationState::files() const {
  STATIC_AVOID_DESTRUCTION(Files, empty);
  return files_ != nullptr ? *files_ : empty;
}

BlobRelocationState::Files& BlobRelocationState::MutableFiles() {
  if (files_ == nullptr) {
    files_ = std::make_shared<const Files>();
  }
  if (files_.use_count() != 1) {
    files_ = std::make_shared<const Files>(*files_);
  }
  return const_cast<Files&>(*files_);
}

uint64_t BlobRelocationState::Lookup(uint64_t file_number,
                                     uint64_t* count) const {
  const auto it = pending_.find(file_number);
  if (it == pending_.end()) {
    uint32_t table_count = 0;
    const uint64_t value = table_.Find(file_number, &table_count);
    if (count != nullptr) {
      *count = table_count;
    }
    return value;
  }
  if (count != nullptr) {
    *count = it->second.second;
  }
  return it->second.first;
}

void BlobRelocationState::Stage(uint64_t file_number, uint64_t value,
                                uint64_t count) {
  pending_[file_number] = std::make_pair(value, count);
  validated_.store(false, std::memory_order_relaxed);
}

void BlobRelocationState::Seal() {
  if (pending_.empty()) {
    return;
  }
  std::vector<BlobFileTable::Op> ops;
  ops.reserve(pending_.size());
  for (const auto& entry : pending_) {
    uint32_t count = 0;
    const uint64_t value = table_.Find(entry.first, &count);
    // VersionBuilder rejects more than 2^32 - 1 routes to one target.
    assert(entry.second.second <= std::numeric_limits<uint32_t>::max());
    const uint32_t final_count = static_cast<uint32_t>(entry.second.second);
    if (entry.second.first != value || final_count != count) {
      ops.push_back({entry.first, entry.second.first, final_count});
    }
  }
  pending_.clear();
  table_.Apply(ops);
}

size_t BlobRelocationState::IncrementalRetentionBytes(
    const BlobRelocationState& base,
    const std::vector<uint64_t>& changed_files) const {
  // A census change log entry that holds this state is charged the result.
  //
  // Counted:
  // - The BlobRelocationState object itself (sizeof(*this)).
  // - The files vectors of both states, when this state has its own copy.
  // - The metadata of each changed relocation file, old and new. No other
  //   reservation charges this metadata, and an earlier change log entry can
  //   still hold the old one. Each object counts as its size plus 8 pointers
  //   (an estimate of the allocation overhead) plus its checksum strings.
  //
  // Not counted: the file table. Its shards and top arrays are charged by
  // ownership to the file metadata reservation (see
  // BlobFileTable::SetCacheReservationManager()). Each shard is charged once
  // and released when its last holder frees it, so the table stays charged
  // while any entry or Version holds it. Counting it per entry would charge the
  // table copy of every flush again.
  size_t bytes = sizeof(*this);
  const auto add = [&bytes](size_t count, size_t item_bytes) {
    if (item_bytes != 0 &&
        count > (std::numeric_limits<size_t>::max() - bytes) / item_bytes) {
      bytes = std::numeric_limits<size_t>::max();
    } else {
      bytes += count * item_bytes;
    }
  };
  if (files_ != base.files_) {
    add(files().capacity() + base.files().capacity(),
        sizeof(std::shared_ptr<BlobRelocationFileMetaData>));
  }
  for (uint64_t file_number : changed_files) {
    const BlobRelocationFileMetaData* const old_file =
        base.PeekFile(file_number);
    const BlobRelocationFileMetaData* const new_file = PeekFile(file_number);
    if (old_file == new_file) {
      continue;
    }
    for (const BlobRelocationFileMetaData* file : {old_file, new_file}) {
      if (file != nullptr) {
        add(1, sizeof(BlobRelocationFileMetaData) + 8 * sizeof(void*));
        add(file->GetChecksumMethod().capacity(), 1);
        add(file->GetChecksumValue().capacity(), 1);
      }
    }
  }
  return bytes;
}

std::shared_ptr<BlobRelocationFileMetaData> BlobRelocationState::FindFile(
    uint64_t file_number) const {
  const BlobRelocationFileMetaData* file = PeekFile(file_number);
  if (file == nullptr) {
    return nullptr;
  }
  const Files& all = files();
  const auto it = std::lower_bound(
      all.begin(), all.end(), file_number,
      [](const std::shared_ptr<BlobRelocationFileMetaData>& value,
         uint64_t number) { return value->GetFileNumber() < number; });
  assert(it != all.end() && it->get() == file);
  return *it;
}

const BlobRelocationFileMetaData* BlobRelocationState::PeekFile(
    uint64_t file_number) const {
  const uint64_t value = Lookup(file_number);
  return BlobFileTable::IsRelocationFile(value)
             ? BlobFileTable::AsRelocationFile(value)
             : nullptr;
}

uint64_t BlobRelocationState::FindRouteTarget(uint64_t old_file_number) const {
  const uint64_t value = Lookup(old_file_number);
  return BlobFileTable::IsRoute(value) ? BlobFileTable::RouteTarget(value)
                                       : kInvalidBlobFileNumber;
}

size_t BlobRelocationState::RouteCount(uint64_t file_number) const {
  uint64_t count = 0;
  const uint64_t value = Lookup(file_number, &count);
  return BlobFileTable::IsRelocationFile(value) ? count : 0;
}

void BlobRelocationState::SetFile(
    std::shared_ptr<BlobRelocationFileMetaData> file) {
  assert(file != nullptr);
  const uint64_t file_number = file->GetFileNumber();
  const BlobRelocationFileMetaData* previous = PeekFile(file_number);
  if (previous != nullptr) {
    AddDigest(EncodeFileForDigest(*previous), /*add=*/false, &digest_high_,
              &digest_low_);
    if (previous->IsMarkedLost()) {
      --lost_file_count_;
    }
  }
  AddDigest(EncodeFileForDigest(*file), /*add=*/true, &digest_high_,
            &digest_low_);
  if (file->IsMarkedLost()) {
    ++lost_file_count_;
  }
  // A lost file is not on disk for readers of this state, so the byte totals
  // leave it out (see PhysicalFileBytes()).
  if (previous != nullptr && !previous->IsMarkedLost() &&
      !aggregate_overflow_) {
    physical_file_bytes_ -= previous->GetFileSize();
    record_bytes_ -= previous->GetTotalRecordBytes();
  }
  if (!file->IsMarkedLost()) {
    if (file->GetFileSize() >
            std::numeric_limits<uint64_t>::max() - physical_file_bytes_ ||
        file->GetTotalRecordBytes() >
            std::numeric_limits<uint64_t>::max() - record_bytes_) {
      aggregate_overflow_ = true;
    } else if (!aggregate_overflow_) {
      physical_file_bytes_ += file->GetFileSize();
      record_bytes_ += file->GetTotalRecordBytes();
    }
  }
  // A file set again keeps the routes to it.
  uint64_t route_count = 0;
  if (previous != nullptr) {
    Lookup(file_number, &route_count);
  }
  Stage(file_number,
        BlobFileTable::RelocationFileValue(file.get(), file->IsMarkedLost()),
        route_count);
  Files& files = MutableFiles();
  const auto it = std::lower_bound(
      files.begin(), files.end(), file_number,
      [](const std::shared_ptr<BlobRelocationFileMetaData>& value,
         uint64_t number) { return value->GetFileNumber() < number; });
  if (it != files.end() && (*it)->GetFileNumber() == file_number) {
    *it = std::move(file);
  } else {
    files.insert(it, std::move(file));
  }
}

void BlobRelocationState::DeleteFile(uint64_t file_number) {
  const BlobRelocationFileMetaData* file = PeekFile(file_number);
  if (file == nullptr) {
    return;
  }
  AddDigest(EncodeFileForDigest(*file), /*add=*/false, &digest_high_,
            &digest_low_);
  if (file->IsMarkedLost()) {
    --lost_file_count_;
  } else if (!aggregate_overflow_) {
    physical_file_bytes_ -= file->GetFileSize();
    record_bytes_ -= file->GetTotalRecordBytes();
  }
  Stage(file_number, 0, /*count=*/0);
  Files& files = MutableFiles();
  const auto it = std::lower_bound(
      files.begin(), files.end(), file_number,
      [](const std::shared_ptr<BlobRelocationFileMetaData>& value,
         uint64_t number) { return value->GetFileNumber() < number; });
  assert(it != files.end() && (*it)->GetFileNumber() == file_number);
  files.erase(it);
}

void BlobRelocationState::AdjustRouteCount(uint64_t target, int64_t delta) {
  uint64_t count = 0;
  const uint64_t value = Lookup(target, &count);
  if (!BlobFileTable::IsRelocationFile(value)) {
    // The target is not a live relocation file. Either the route names a
    // missing target (validation reports it), or the target was deleted earlier
    // in the same build.
    return;
  }
  assert(delta >= 0 || count >= static_cast<uint64_t>(-delta));
  Stage(target, value,
        static_cast<uint64_t>(static_cast<int64_t>(count) + delta));
}

void BlobRelocationState::SetRoute(const BlobRelocationRoute& route) {
  const uint64_t source = route.GetOldFileNumber();
  const uint64_t target = route.GetTargetFileNumber();
  const uint64_t previous = FindRouteTarget(source);
  if (previous == target) {
    return;
  }
  if (previous != kInvalidBlobFileNumber) {
    AdjustRouteCount(previous, -1);
    AddDigest(EncodeRouteForDigest(source, previous), /*add=*/false,
              &digest_high_, &digest_low_);
  } else {
    ++route_count_;
  }
  AddDigest(EncodeRouteForDigest(source, target), /*add=*/true, &digest_high_,
            &digest_low_);
  Stage(source, BlobFileTable::RouteValue(target), /*count=*/0);
  AdjustRouteCount(target, 1);
}

void BlobRelocationState::DeleteRoute(uint64_t old_file_number) {
  const uint64_t target = FindRouteTarget(old_file_number);
  if (target == kInvalidBlobFileNumber) {
    return;
  }
  AddDigest(EncodeRouteForDigest(old_file_number, target), /*add=*/false,
            &digest_high_, &digest_low_);
  Stage(old_file_number, 0, /*count=*/0);
  --route_count_;
  AdjustRouteCount(target, -1);
}

void BlobRelocationState::SetBlobFile(const BlobFileMetaData* file) {
  assert(file != nullptr);
  Stage(file->GetBlobFileNumber(), BlobFileTable::BlobFileValue(file),
        /*count=*/0);
}

void BlobRelocationState::DeleteBlobFile(uint64_t file_number) {
  if (BlobFileTable::IsBlobFile(Lookup(file_number))) {
    Stage(file_number, 0, /*count=*/0);
  }
}

void BlobRelocationState::ComputeFingerprint(uint32_t format_version,
                                             uint64_t* high,
                                             uint64_t* low) const {
  std::string encoded;
  PutFixed32(&encoded, 3U /* fingerprint encoding version */);
  PutFixed32(&encoded, format_version);
  PutFixed64(&encoded, files().size());
  PutFixed64(&encoded, route_count_);
  PutFixed64(&encoded, digest_high_);
  PutFixed64(&encoded, digest_low_);
  Hash2x64(encoded.data(), encoded.size(), high, low);
}

// BlobRelocationFileAddition

BlobRelocationFileAddition::BlobRelocationFileAddition(
    uint64_t file_number, uint64_t file_size, std::string checksum_method,
    std::string checksum_value, const Slice& encoded_directory,
    uint64_t tail_size)
    : file_number_(file_number),
      file_size_(file_size),
      tail_size_(tail_size),
      checksum_method_(std::move(checksum_method)),
      checksum_value_(std::move(checksum_value)) {
  BlobRelocationDirectory directory;
  directory_status_ =
      BlobRelocationDirectory::DecodeFrom(encoded_directory, &directory);
  if (directory_status_.ok()) {
    directory_status_ = directory.ValidateForFileSize(file_size_);
  }
  if (directory_status_.ok()) {
    total_entries_ = directory.total_entries();
    total_record_bytes_ = directory.total_record_bytes();
    directory_digest_ = directory.directory_digest();
  }
}

BlobRelocationFileAddition::BlobRelocationFileAddition(
    uint64_t file_number, uint64_t file_size, std::string checksum_method,
    std::string checksum_value, uint64_t total_entries,
    uint64_t total_record_bytes, uint32_t directory_digest, uint64_t tail_size)
    : file_number_(file_number),
      file_size_(file_size),
      tail_size_(tail_size),
      checksum_method_(std::move(checksum_method)),
      checksum_value_(std::move(checksum_value)),
      total_entries_(total_entries),
      total_record_bytes_(total_record_bytes),
      directory_digest_(directory_digest) {}

Status BlobRelocationFileAddition::Validate() const {
  if (!directory_status_.ok()) {
    return directory_status_;
  }
  if (file_number_ == kInvalidBlobFileNumber ||
      file_number_ > kMaxBlobRelocationFileNumber) {
    return Status::Corruption("Invalid blob relocation file number");
  }
  if (file_size_ <= kBlobRelocationFilePrefixSize) {
    return Status::Corruption("Invalid blob relocation file size");
  }
  // An open reads the table metadata range that the builder recorded, so the
  // tail size must be nonzero and fit in the file.
  if (tail_size_ == 0 || tail_size_ > file_size_) {
    return Status::Corruption("Invalid blob relocation metadata tail size");
  }
  if (checksum_method_.empty() != checksum_value_.empty()) {
    return Status::Corruption("Incomplete blob relocation file checksum");
  }
  // Every record holds at least its trailer (kSimpleGen2BlobTrailerSize), and
  // all records fit in the file after the prefix.
  if (total_entries_ == 0 ||
      total_entries_ > total_record_bytes_ / kSimpleGen2BlobTrailerSize ||
      total_record_bytes_ > file_size_ - kBlobRelocationFilePrefixSize) {
    return Status::Corruption("Invalid blob relocation file totals");
  }
  return Status::OK();
}

void BlobRelocationFileAddition::EncodeTo(std::string* output) const {
  PutVarint64(output, file_number_);
  PutVarint64(output, file_size_);
  PutLengthPrefixedSlice(output, checksum_method_);
  PutLengthPrefixedSlice(output, checksum_value_);
  PutVarint64(output, total_entries_);
  PutVarint64(output, total_record_bytes_);
  PutFixed32(output, directory_digest_);
  PutVarint64(output, tail_size_);
}

Status BlobRelocationFileAddition::DecodeFrom(Slice* input) {
  if (input == nullptr) {
    return Status::InvalidArgument("Null blob relocation addition input");
  }

  Slice checksum_method;
  Slice checksum_value;
  if (!GetVarint64(input, &file_number_) || !GetVarint64(input, &file_size_) ||
      !GetLengthPrefixedSlice(input, &checksum_method) ||
      !GetLengthPrefixedSlice(input, &checksum_value) ||
      !GetVarint64(input, &total_entries_) ||
      !GetVarint64(input, &total_record_bytes_) ||
      !GetFixed32(input, &directory_digest_) ||
      !GetVarint64(input, &tail_size_) || !input->empty()) {
    return Status::Corruption("Malformed blob relocation file addition");
  }

  checksum_method_.assign(checksum_method.data(), checksum_method.size());
  checksum_value_.assign(checksum_value.data(), checksum_value.size());
  directory_status_ = Status::OK();
  return Validate();
}

std::string BlobRelocationFileAddition::DebugString() const {
  std::ostringstream stream;
  stream << "file=" << file_number_ << " size=" << file_size_
         << " tail_size=" << tail_size_ << " entries=" << total_entries_
         << " record_bytes=" << total_record_bytes_
         << " directory_digest=" << directory_digest_;
  return stream.str();
}

BlobPhysicalFile BlobRelocationFileAddition::ToPhysicalFile() const {
  return BlobPhysicalFile{
      BlobFileOpenInfo{file_number_, checksum_value_, checksum_method_},
      file_size_, /*is_relocation=*/true, tail_size_, directory_digest_};
}

bool operator==(const BlobRelocationFileAddition& lhs,
                const BlobRelocationFileAddition& rhs) {
  return lhs.GetFileNumber() == rhs.GetFileNumber() &&
         lhs.GetFileSize() == rhs.GetFileSize() &&
         lhs.GetTailSize() == rhs.GetTailSize() &&
         lhs.GetChecksumMethod() == rhs.GetChecksumMethod() &&
         lhs.GetChecksumValue() == rhs.GetChecksumValue() &&
         lhs.GetTotalEntries() == rhs.GetTotalEntries() &&
         lhs.GetTotalRecordBytes() == rhs.GetTotalRecordBytes() &&
         lhs.GetDirectoryDigest() == rhs.GetDirectoryDigest();
}

bool operator!=(const BlobRelocationFileAddition& lhs,
                const BlobRelocationFileAddition& rhs) {
  return !(lhs == rhs);
}

// Routes

Status BlobRelocationRoute::Validate() const {
  if (old_file_number_ == kInvalidBlobFileNumber ||
      target_file_number_ == kInvalidBlobFileNumber ||
      old_file_number_ > kMaxBlobRelocationFileNumber ||
      target_file_number_ > kMaxBlobRelocationFileNumber) {
    return Status::Corruption("Invalid blob relocation route file number");
  }
  if (old_file_number_ == target_file_number_) {
    return Status::Corruption("Blob relocation route targets its own file");
  }
  return Status::OK();
}

void BlobRelocationRoute::EncodeTo(std::string* output) const {
  PutVarint64(output, old_file_number_);
  PutVarint64(output, target_file_number_);
}

Status BlobRelocationRoute::DecodeFrom(Slice* input) {
  if (input == nullptr) {
    return Status::InvalidArgument("Null blob relocation route input");
  }
  if (!GetVarint64(input, &old_file_number_) ||
      !GetVarint64(input, &target_file_number_) || !input->empty()) {
    return Status::Corruption("Malformed blob relocation route");
  }
  return Validate();
}

void BlobRelocationRouteDeletion::EncodeTo(std::string* output) const {
  PutVarint64(output, old_file_number_);
}

Status BlobRelocationRouteDeletion::DecodeFrom(Slice* input) {
  if (input == nullptr) {
    return Status::InvalidArgument("Null blob relocation route deletion input");
  }
  if (!GetVarint64(input, &old_file_number_) || !input->empty() ||
      old_file_number_ == kInvalidBlobFileNumber) {
    return Status::Corruption("Malformed blob relocation route deletion");
  }
  return Status::OK();
}

std::string BlobRelocationRoute::DebugString() const {
  std::ostringstream stream;
  stream << "old=" << old_file_number_ << " target=" << target_file_number_;
  return stream.str();
}

bool operator==(const BlobRelocationRoute& lhs,
                const BlobRelocationRoute& rhs) {
  return lhs.GetOldFileNumber() == rhs.GetOldFileNumber() &&
         lhs.GetTargetFileNumber() == rhs.GetTargetFileNumber();
}

bool operator!=(const BlobRelocationRoute& lhs,
                const BlobRelocationRoute& rhs) {
  return !(lhs == rhs);
}

}  // namespace ROCKSDB_NAMESPACE
