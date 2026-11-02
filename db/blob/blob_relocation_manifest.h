// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "db/blob/blob_constants.h"
#include "db/blob/blob_relocation_file.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

inline constexpr uint32_t kBlobIndirectionManifestFormatVersion = 1;

// Durable immutable metadata for one physical relocation file. The complete
// encoded directory is persisted so VersionBuilder can validate route origin
// membership without opening the file or trusting redundant summaries.
class BlobRelocationFileAddition {
 public:
  static constexpr uint32_t kFormatVersion = 2;
  BlobRelocationFileAddition() = default;
  BlobRelocationFileAddition(const BlobRelocationFileAddition&) = default;
  BlobRelocationFileAddition& operator=(const BlobRelocationFileAddition&) =
      default;
  BlobRelocationFileAddition(BlobRelocationFileAddition&&) noexcept = default;
  BlobRelocationFileAddition& operator=(BlobRelocationFileAddition&&) noexcept =
      default;
  ~BlobRelocationFileAddition() {
    // Validation is cached so repeated VersionBuilder checks cannot re-decode
    // a large malformed directory while holding the DB mutex. Some metadata
    // copies are destroyed without consulting the cache.
    directory_validation_status_.PermitUncheckedError();
  }

  BlobRelocationFileAddition(uint64_t file_number, uint32_t column_family_id,
                             uint64_t file_size, std::string checksum_method,
                             std::string checksum_value,
                             std::string encoded_directory, uint64_t tail_size);

  uint64_t GetFileNumber() const { return file_number_; }
  uint32_t GetColumnFamilyId() const { return column_family_id_; }
  uint64_t GetFileSize() const { return file_size_; }
  uint64_t GetTailSize() const { return tail_size_; }
  const std::string& GetChecksumMethod() const { return checksum_method_; }
  const std::string& GetChecksumValue() const { return checksum_value_; }
  const std::string& GetEncodedDirectory() const;

  Status Validate() const { return Validate(nullptr); }
  Status Validate(BlobRelocationDirectory* directory) const;
  Status GetValidatedDirectory(
      std::shared_ptr<const BlobRelocationDirectory>* directory) const;
  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

  std::string DebugString() const;

 private:
  Status ValidateScalars() const;
  Status CacheDirectoryValidation();
  Status DecodeAndValidateDirectory(
      std::shared_ptr<const BlobRelocationDirectory>* directory) const;

  uint32_t format_version_ = kFormatVersion;
  uint64_t file_number_ = kInvalidBlobFileNumber;
  uint32_t column_family_id_ = 0;
  uint64_t file_size_ = 0;
  uint64_t tail_size_ = 0;
  std::string checksum_method_;
  std::string checksum_value_;
  std::shared_ptr<const std::string> encoded_directory_;
  std::shared_ptr<const BlobRelocationDirectory> decoded_directory_;
  Status directory_validation_status_;
  bool directory_validation_attempted_ = false;
};

bool operator==(const BlobRelocationFileAddition& lhs,
                const BlobRelocationFileAddition& rhs);
bool operator!=(const BlobRelocationFileAddition& lhs,
                const BlobRelocationFileAddition& rhs);

// Shared immutable Version-owned state for one relocation file. Destruction
// of the last shared instance is the physical-obsoletion ownership boundary.
class BlobRelocationFileMetaData {
 public:
  template <typename Deleter>
  static std::shared_ptr<BlobRelocationFileMetaData> Create(
      BlobRelocationFileAddition addition,
      std::shared_ptr<const BlobRelocationDirectory> directory,
      Deleter deleter) {
    return std::shared_ptr<BlobRelocationFileMetaData>(
        new BlobRelocationFileMetaData(std::move(addition),
                                       std::move(directory)),
        std::move(deleter));
  }

  static std::shared_ptr<BlobRelocationFileMetaData> Create(
      BlobRelocationFileAddition addition,
      std::shared_ptr<const BlobRelocationDirectory> directory) {
    return std::shared_ptr<BlobRelocationFileMetaData>(
        new BlobRelocationFileMetaData(std::move(addition),
                                       std::move(directory)));
  }

  template <typename Deleter>
  static std::shared_ptr<BlobRelocationFileMetaData> Create(
      BlobRelocationFileAddition addition, BlobRelocationDirectory directory,
      Deleter deleter) {
    return Create(
        std::move(addition),
        std::make_shared<const BlobRelocationDirectory>(std::move(directory)),
        std::move(deleter));
  }

  static std::shared_ptr<BlobRelocationFileMetaData> Create(
      BlobRelocationFileAddition addition, BlobRelocationDirectory directory) {
    return Create(
        std::move(addition),
        std::make_shared<const BlobRelocationDirectory>(std::move(directory)));
  }

  BlobRelocationFileMetaData(const BlobRelocationFileMetaData&) = delete;
  BlobRelocationFileMetaData& operator=(const BlobRelocationFileMetaData&) =
      delete;
  BlobRelocationFileMetaData(BlobRelocationFileMetaData&&) = delete;
  BlobRelocationFileMetaData& operator=(BlobRelocationFileMetaData&&) = delete;
  ~BlobRelocationFileMetaData() = default;

  uint64_t GetFileNumber() const { return addition_.GetFileNumber(); }
  uint32_t GetColumnFamilyId() const { return addition_.GetColumnFamilyId(); }
  uint64_t GetFileSize() const { return addition_.GetFileSize(); }
  uint64_t GetTailSize() const { return addition_.GetTailSize(); }
  const std::string& GetChecksumMethod() const {
    return addition_.GetChecksumMethod();
  }
  const std::string& GetChecksumValue() const {
    return addition_.GetChecksumValue();
  }
  uint32_t GetDirectoryDigest() const { return directory_->directory_digest(); }
  const BlobRelocationDirectory& GetDirectory() const { return *directory_; }
  const std::shared_ptr<const BlobRelocationDirectory>& GetDirectoryShared()
      const {
    return directory_;
  }
  const BlobRelocationFileAddition& GetAddition() const { return addition_; }

  // A loss marker belongs to an immutable Version, not the physical file.
  // Keep the original ownership token so old Versions remain readable and
  // physical obsoletion still has exactly one owner. A later reappearing copy
  // must not resurrect values in a Version that has accepted the loss.
  static std::shared_ptr<BlobRelocationFileMetaData> CreateMarkedMissing(
      std::shared_ptr<BlobRelocationFileMetaData> backing) {
    std::shared_ptr<BlobRelocationFileMetaData> marked =
        Create(backing->GetAddition(), backing->GetDirectoryShared());
    marked->missing_ = true;
    marked->backing_metadata_ = std::move(backing);
    return marked;
  }
  bool IsMarkedMissing() const { return missing_; }

 private:
  BlobRelocationFileMetaData(
      BlobRelocationFileAddition addition,
      std::shared_ptr<const BlobRelocationDirectory> directory)
      : addition_(std::move(addition)), directory_(std::move(directory)) {}

  BlobRelocationFileAddition addition_;
  std::shared_ptr<const BlobRelocationDirectory> directory_;
  std::shared_ptr<BlobRelocationFileMetaData> backing_metadata_;
  bool missing_ = false;
};

struct BlobRelocationRouteSource {
  uint64_t generation_file_number = kInvalidBlobFileNumber;
  uint64_t origin_file_number = kInvalidBlobFileNumber;

  friend bool operator==(const BlobRelocationRouteSource& lhs,
                         const BlobRelocationRouteSource& rhs) {
    return std::tie(lhs.generation_file_number, lhs.origin_file_number) ==
           std::tie(rhs.generation_file_number, rhs.origin_file_number);
  }

  friend bool operator!=(const BlobRelocationRouteSource& lhs,
                         const BlobRelocationRouteSource& rhs) {
    return !(lhs == rhs);
  }

  friend bool operator<(const BlobRelocationRouteSource& lhs,
                        const BlobRelocationRouteSource& rhs) {
    return std::tie(lhs.generation_file_number, lhs.origin_file_number) <
           std::tie(rhs.generation_file_number, rhs.origin_file_number);
  }
};

class BlobRelocationRoute {
 public:
  BlobRelocationRoute() = default;
  BlobRelocationRoute(uint64_t generation_file_number,
                      uint64_t origin_file_number,
                      uint64_t physical_file_number)
      : source_{generation_file_number, origin_file_number},
        physical_file_number_(physical_file_number) {}

  const BlobRelocationRouteSource& GetSource() const { return source_; }
  uint64_t GetGenerationFileNumber() const {
    return source_.generation_file_number;
  }
  uint64_t GetOriginFileNumber() const { return source_.origin_file_number; }
  uint64_t GetPhysicalFileNumber() const { return physical_file_number_; }

  Status Validate() const;
  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);
  std::string DebugString() const;

 private:
  uint32_t format_version_ = kBlobIndirectionManifestFormatVersion;
  BlobRelocationRouteSource source_;
  uint64_t physical_file_number_ = kInvalidBlobFileNumber;
};

// A fixed-width copy-on-write index for immutable Version metadata. A Version
// with no relocation changes shares the complete state. An unlocked builder
// copies only the shards it changes, once per shard, rather than all live
// routes. Values are visited in deterministic shard/key order, NOT global key
// order; point lookup must use Find(), not binary search over this view.
template <typename Key, typename Value>
class BlobRelocationStateIndex {
 public:
  static constexpr size_t kShardCount = 64;
  using Map = std::map<Key, Value>;

  class Values {
   public:
    class const_iterator {
     public:
      using iterator_category = std::forward_iterator_tag;
      using value_type = Value;
      using difference_type = std::ptrdiff_t;
      using pointer = const Value*;
      using reference = const Value&;

      const_iterator() = default;

      reference operator*() const { return iterator_->second; }
      pointer operator->() const { return &iterator_->second; }
      const_iterator& operator++() {
        ++iterator_;
        SkipEmptyShards();
        return *this;
      }
      const_iterator operator++(int) {
        const_iterator previous = *this;
        ++*this;
        return previous;
      }
      friend bool operator==(const const_iterator& lhs,
                             const const_iterator& rhs) {
        return lhs.owner_ == rhs.owner_ && lhs.shard_ == rhs.shard_ &&
               (lhs.shard_ == kShardCount || lhs.iterator_ == rhs.iterator_);
      }
      friend bool operator!=(const const_iterator& lhs,
                             const const_iterator& rhs) {
        return !(lhs == rhs);
      }

     private:
      friend class Values;
      const_iterator(const BlobRelocationStateIndex* owner, size_t shard)
          : owner_(owner), shard_(shard) {
        if (shard_ != kShardCount && owner_->shards_[shard_] != nullptr) {
          iterator_ = owner_->shards_[shard_]->begin();
        }
        SkipEmptyShards();
      }
      void SkipEmptyShards() {
        while (shard_ < kShardCount) {
          const std::shared_ptr<Map>& map = owner_->shards_[shard_];
          if (map != nullptr && iterator_ != map->end()) {
            return;
          }
          ++shard_;
          if (shard_ < kShardCount && owner_->shards_[shard_] != nullptr) {
            iterator_ = owner_->shards_[shard_]->begin();
          }
        }
      }
      const BlobRelocationStateIndex* owner_ = nullptr;
      size_t shard_ = kShardCount;
      typename Map::const_iterator iterator_;
    };

    explicit Values(const BlobRelocationStateIndex* owner) : owner_(owner) {}
    const_iterator begin() const { return const_iterator(owner_, 0); }
    const_iterator end() const { return const_iterator(owner_, kShardCount); }
    size_t size() const { return owner_->size_; }
    bool empty() const { return size() == 0; }

   private:
    const BlobRelocationStateIndex* owner_;
  };

  static size_t Shard(uint64_t key) { return key % kShardCount; }
  static size_t Shard(const BlobRelocationRouteSource& key) {
    return (key.generation_file_number ^ (key.origin_file_number >> 6)) %
           kShardCount;
  }
  const Value* Find(const Key& key) const {
    const std::shared_ptr<Map>& map = shards_[Shard(key)];
    if (map == nullptr) {
      return nullptr;
    }
    typename Map::const_iterator iterator = map->find(key);
    return iterator == map->end() ? nullptr : &iterator->second;
  }
  Value* FindMutable(const Key& key) {
    if (Find(key) == nullptr) {
      return nullptr;
    }
    Map& map = MutableShard(Shard(key));
    return &map.find(key)->second;
  }
  void Set(const Key& key, Value value) {
    Map& map = MutableShard(Shard(key));
    if (map.insert_or_assign(key, std::move(value)).second) {
      ++size_;
    }
  }
  void Erase(const Key& key) {
    if (Find(key) != nullptr) {
      size_ -= MutableShard(Shard(key)).erase(key);
    }
  }
  Values values() const { return Values(this); }
  const std::shared_ptr<Map>& shard(size_t index) const {
    return shards_[index];
  }
  bool dirty(size_t index) const { return dirty_[index]; }
  void SetFingerprint(size_t index, uint64_t high, uint64_t low) {
    fingerprint_high_[index] = high;
    fingerprint_low_[index] = low;
    dirty_[index] = false;
  }
  uint64_t fingerprint_high(size_t index) const {
    return fingerprint_high_[index];
  }
  uint64_t fingerprint_low(size_t index) const {
    return fingerprint_low_[index];
  }
  // Conservative incremental retention bound. Charge both newly cloned and
  // retired shards: an outstanding earlier log entry can still pin the latter
  // after the latest Version has stopped sharing it. Unchanged shards cost 0.
  size_t RetainedShardBytes(const BlobRelocationStateIndex& base) const {
    constexpr size_t kNodeBytes =
        sizeof(typename Map::value_type) + 4 * sizeof(void*);
    constexpr size_t kMapBytes = sizeof(Map) + 4 * sizeof(void*);
    size_t bytes = 0;
    for (size_t index = 0; index < kShardCount; ++index) {
      if (shards_[index] == base.shards_[index]) {
        continue;
      }
      const std::array<const Map*, 2> maps{shards_[index].get(),
                                           base.shards_[index].get()};
      for (const Map* map : maps) {
        if (map == nullptr) {
          continue;
        }
        if (kMapBytes > std::numeric_limits<size_t>::max() - bytes) {
          return std::numeric_limits<size_t>::max();
        }
        bytes += kMapBytes;
        if (map->size() >
            (std::numeric_limits<size_t>::max() - bytes) / kNodeBytes) {
          return std::numeric_limits<size_t>::max();
        }
        bytes += map->size() * kNodeBytes;
      }
    }
    return bytes;
  }

 private:
  Map& MutableShard(size_t index) {
    std::shared_ptr<Map>& map = shards_[index];
    if (map == nullptr) {
      map = std::make_shared<Map>();
    } else if (map.use_count() != 1) {
      map = std::make_shared<Map>(*map);
    }
    dirty_[index] = true;
    return *map;
  }
  std::array<std::shared_ptr<Map>, kShardCount> shards_;
  std::array<bool, kShardCount> dirty_{};
  std::array<uint64_t, kShardCount> fingerprint_high_{};
  std::array<uint64_t, kShardCount> fingerprint_low_{};
  size_t size_ = 0;
};

// Only LIVE files/routes are durable. Deletion history belongs to the bounded
// process-local change log and active publication reservations, not snapshots.
// An unpublished builder exclusively owns the mutable state; published
// Versions expose it only through const handles and read-only views.
class BlobRelocationState {
 public:
  using FileIndex =
      BlobRelocationStateIndex<uint64_t,
                               std::shared_ptr<BlobRelocationFileMetaData>>;
  using RouteIndex =
      BlobRelocationStateIndex<BlobRelocationRouteSource, BlobRelocationRoute>;
  BlobRelocationState() = default;
  BlobRelocationState(const BlobRelocationState& base)
      : files_(base.files_),
        routes_(base.routes_),
        targets_(base.targets_),
        physical_file_bytes_(base.physical_file_bytes_),
        record_bytes_(base.record_bytes_),
        missing_file_count_(base.missing_file_count_),
        aggregate_overflow_(base.aggregate_overflow_),
        validated_(base.validated()) {}

  FileIndex::Values files() const { return files_.values(); }
  RouteIndex::Values routes() const { return routes_.values(); }
  uint64_t PhysicalFileBytes() const { return physical_file_bytes_; }
  uint64_t RecordBytes() const { return record_bytes_; }
  bool HasMissingFiles() const { return missing_file_count_ != 0; }
  bool AggregateOverflow() const { return aggregate_overflow_; }
  std::shared_ptr<BlobRelocationFileMetaData> FindFile(
      uint64_t file_number) const;
  const BlobRelocationRoute* FindRoute(
      const BlobRelocationRouteSource& source) const {
    return routes_.Find(source);
  }
  void SetFile(std::shared_ptr<BlobRelocationFileMetaData> file);
  void DeleteFile(uint64_t file_number);
  void SetRoute(BlobRelocationRoute route);
  void DeleteRoute(const BlobRelocationRouteSource& source);
  const std::vector<BlobRelocationRouteSource>* SourcesForTarget(
      uint64_t physical_file_number) const;
  size_t TargetOriginCount(uint64_t physical_file_number,
                           uint64_t origin_file_number) const;
  void ComputeFingerprint(uint32_t format_version, uint64_t* high,
                          uint64_t* low);
  size_t IncrementalRetentionBytes(
      const BlobRelocationState& base,
      const std::vector<BlobRelocationRouteSource>& changed_routes,
      const std::vector<uint64_t>& changed_files) const;
  bool validated() const { return validated_.load(std::memory_order_relaxed); }
  void MarkValidated() const {
    validated_.store(true, std::memory_order_relaxed);
  }

 private:
  struct TargetSummary {
    std::vector<BlobRelocationRouteSource> sources;
    std::map<uint64_t, size_t> origin_counts;
  };
  using TargetIndex =
      BlobRelocationStateIndex<uint64_t, std::shared_ptr<TargetSummary>>;
  std::shared_ptr<TargetSummary> MutableTarget(uint64_t file_number);
  void RemoveTargetSource(const BlobRelocationRoute& route);

  FileIndex files_;
  RouteIndex routes_;
  TargetIndex targets_;
  uint64_t physical_file_bytes_ = 0;
  uint64_t record_bytes_ = 0;
  size_t missing_file_count_ = 0;
  bool aggregate_overflow_ = false;
  mutable std::atomic<bool> validated_{false};
};

bool operator==(const BlobRelocationRoute& lhs, const BlobRelocationRoute& rhs);
bool operator!=(const BlobRelocationRoute& lhs, const BlobRelocationRoute& rhs);

// An exact, atomic transition that retires one normal identity-root blob file
// after standalone GC. Unlike BlobFileGarbage, the live aggregates are
// absolute and replay-idempotent. A nonempty retirement must be accompanied by
// an identity route whose target directory has the same aggregates; an empty
// retirement authorizes dropping the source without a route.
class BlobFileRetirement {
 public:
  BlobFileRetirement() = default;
  BlobFileRetirement(uint64_t source_file_number, uint64_t live_blob_count,
                     uint64_t live_source_record_bytes,
                     uint64_t relocated_record_bytes)
      : source_file_number_(source_file_number),
        live_blob_count_(live_blob_count),
        live_source_record_bytes_(live_source_record_bytes),
        relocated_record_bytes_(relocated_record_bytes) {}

  uint64_t GetSourceFileNumber() const { return source_file_number_; }
  uint64_t GetLiveBlobCount() const { return live_blob_count_; }
  uint64_t GetLiveSourceRecordBytes() const {
    return live_source_record_bytes_;
  }
  uint64_t GetRelocatedRecordBytes() const { return relocated_record_bytes_; }

  Status Validate() const;
  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);
  std::string DebugString() const;

 private:
  uint32_t format_version_ = kBlobIndirectionManifestFormatVersion;
  uint64_t source_file_number_ = kInvalidBlobFileNumber;
  uint64_t live_blob_count_ = 0;
  uint64_t live_source_record_bytes_ = 0;
  uint64_t relocated_record_bytes_ = 0;
};

bool operator==(const BlobFileRetirement& lhs, const BlobFileRetirement& rhs);
bool operator!=(const BlobFileRetirement& lhs, const BlobFileRetirement& rhs);

}  // namespace ROCKSDB_NAMESPACE
