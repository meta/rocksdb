//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_partition_manager.h"

#include <array>
#include <atomic>
#include <cinttypes>
#include <memory>
#include <utility>

#include "cache/cache_key.h"
#include "cache/typed_cache.h"
#include "db/blob/blob_contents.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_completion_callback.h"
#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_writer.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "rocksdb/file_system.h"
#include "util/aligned_buffer.h"
#include "util/compression.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class RoundRobinBlobFilePartitionStrategy : public BlobFilePartitionStrategy {
 public:
  using BlobFilePartitionStrategy::SelectPartition;

  const char* Name() const override {
    return "RoundRobinBlobFilePartitionStrategy";
  }

  uint32_t SelectPartition(uint32_t num_partitions,
                           uint32_t /*column_family_id*/, const Slice& /*key*/,
                           const Slice& /*value*/) override {
    assert(num_partitions > 0);
    return static_cast<uint32_t>(
        next_partition_.fetch_add(1, std::memory_order_relaxed) %
        static_cast<uint64_t>(num_partitions));
  }

 private:
  std::atomic<uint64_t> next_partition_{0};
};

struct DirectWriteCompressionState {
  CompressionOptions compression_opts;
  // `working_area` must be released before its owning compressor.
  std::unique_ptr<Compressor> compressor;
  Compressor::ManagedWorkingArea working_area;
};

DirectWriteCompressionState& GetDirectWriteCompressionState(
    CompressionType compression, const CompressionOptions& compression_opts) {
  assert(compression <= kLastBuiltinCompression);

  static thread_local std::array<DirectWriteCompressionState,
                                 static_cast<size_t>(kLastBuiltinCompression) +
                                     1>
      compression_states;

  auto& compression_state =
      compression_states[static_cast<size_t>(compression)];
  if (compression != kNoCompression &&
      (compression_state.compressor == nullptr ||
       compression_state.compression_opts != compression_opts)) {
    // BDW compression settings are mutable, so rebuild the per-thread cached
    // compressor when the latest published opts for this type change.
    compression_state.working_area = Compressor::ManagedWorkingArea{};
    compression_state.compressor.reset();
    compression_state.compression_opts = compression_opts;
    compression_state.compressor =
        GetBuiltinV2CompressionManager()->GetCompressor(compression_opts,
                                                        compression);
    if (compression_state.compressor != nullptr) {
      compression_state.working_area =
          compression_state.compressor->ObtainWorkingArea();
    }
  }

  return compression_state;
}

}  // namespace

BlobFilePartitionManager::BlobFilePartitionManager(
    uint32_t num_partitions,
    std::shared_ptr<BlobFilePartitionStrategy> strategy,
    FileNumberAllocator file_number_allocator, FileSystem* fs,
    SystemClock* clock, Statistics* statistics, const FileOptions& file_options,
    std::string db_path, std::string column_family_name,
    uint64_t blob_file_size, bool use_fsync,
    uint64_t blob_writer_max_buffer_size, BlobFileCache* blob_file_cache,
    BlobFileCompletionCallback* blob_callback,
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    FileChecksumGenFactory* file_checksum_gen_factory,
    const FileTypeSet& checksum_handoff_file_types,
    const std::shared_ptr<IOTracer>& io_tracer, std::string db_id,
    std::string db_session_id, Logger* info_log)
    : num_partitions_(num_partitions == 0 ? 1 : num_partitions),
      strategy_(strategy != nullptr
                    ? std::move(strategy)
                    : std::make_shared<RoundRobinBlobFilePartitionStrategy>()),
      file_number_allocator_(std::move(file_number_allocator)),
      fs_(fs),
      clock_(clock),
      statistics_(statistics),
      file_options_(file_options),
      db_path_(std::move(db_path)),
      column_family_name_(std::move(column_family_name)),
      blob_file_size_(blob_file_size),
      use_fsync_(use_fsync),
      blob_writer_max_buffer_size_(blob_writer_max_buffer_size),
      blob_file_cache_(blob_file_cache),
      blob_callback_(blob_callback),
      listeners_(listeners),
      file_checksum_gen_factory_(file_checksum_gen_factory),
      checksum_handoff_file_types_(checksum_handoff_file_types),
      io_tracer_(io_tracer),
      db_id_(std::move(db_id)),
      db_session_id_(std::move(db_session_id)),
      info_log_(info_log) {
  partitions_.reserve(num_partitions_);
  for (uint32_t i = 0; i < num_partitions_; ++i) {
    partitions_.emplace_back(new Partition());
  }
}

BlobFilePartitionManager::~BlobFilePartitionManager() {
  if (blob_file_cache_ != nullptr) {
    std::vector<uint64_t> tracked_file_numbers;
    {
      ReadLock lock(&file_partition_mutex_);
      tracked_file_numbers.reserve(file_to_partition_.size());
      for (const auto& entry : file_to_partition_) {
        tracked_file_numbers.push_back(entry.first);
      }
    }

    for (uint64_t file_number : tracked_file_numbers) {
      // Dropping the CF or closing the DB can destroy the manager while active
      // direct-write readers are still cached. Evict them before the CF-owned
      // blob cache goes away so Close() does not retain obsolete footer-less
      // readers for files that are no longer live.
      blob_file_cache_->Evict(file_number);
    }
  }
}

void BlobFilePartitionManager::ResetPartitionState(Partition* partition) {
  partition->writer.reset();
  partition->file_number = 0;
  partition->file_size = 0;
  partition->blob_count = 0;
  partition->total_blob_bytes = 0;
  partition->garbage_blob_count = 0;
  partition->garbage_blob_bytes = 0;
  partition->column_family_id = 0;
  partition->compression = kNoCompression;
  partition->sync_required = false;
}

void BlobFilePartitionManager::AddFilePartitionMapping(uint64_t file_number,
                                                       uint32_t partition_idx) {
  WriteLock lock(&file_partition_mutex_);
  file_to_partition_[file_number] = partition_idx;
}

void BlobFilePartitionManager::RemoveFilePartitionMapping(
    uint64_t file_number) {
  WriteLock lock(&file_partition_mutex_);
  file_to_partition_.erase(file_number);
}

Status BlobFilePartitionManager::OpenNewBlobFile(Partition* partition,
                                                 uint32_t column_family_id,
                                                 CompressionType compression,
                                                 uint32_t partition_idx) {
  assert(partition != nullptr);
  assert(!partition->writer);

  const uint64_t blob_file_number = file_number_allocator_();
  const std::string blob_file_path = BlobFileName(db_path_, blob_file_number);

  AddFilePartitionMapping(blob_file_number, partition_idx);

  if (blob_callback_ != nullptr) {
    blob_callback_->OnBlobFileCreationStarted(
        blob_file_path, column_family_name_,
        /*job_id=*/0, BlobFileCreationReason::kFlush);
  }

  std::unique_ptr<FSWritableFile> file;
  FileOptions writer_file_options = file_options_;
  writer_file_options.open_contract = FileOpenContract::kNoReopenForWrite;
  const uint64_t max_buffer_size =
      blob_writer_max_buffer_size_.load(std::memory_order_relaxed);
  if (max_buffer_size > 0) {
    writer_file_options.writable_file_max_buffer_size = max_buffer_size;
  }
  Status s = NewWritableFile(fs_, blob_file_path, &file, writer_file_options);
  if (!s.ok()) {
    RemoveFilePartitionMapping(blob_file_number);
    return s;
  }

  const bool perform_data_verification =
      checksum_handoff_file_types_.Contains(FileType::kBlobFile);
  auto file_writer = std::make_unique<WritableFileWriter>(
      std::move(file), blob_file_path, writer_file_options, clock_, io_tracer_,
      statistics_, Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS, listeners_,
      file_checksum_gen_factory_, perform_data_verification);

  // This only drains WritableFileWriter's buffered bytes so readers can see
  // each appended record promptly. Durability still comes from SyncAllOpenFiles
  // or AppendFooter(), both of which call Sync().
  constexpr bool kDoFlushEachRecord = true;
  auto blob_log_writer = std::make_unique<BlobLogWriter>(
      std::move(file_writer), clock_, statistics_, blob_file_number, use_fsync_,
      kDoFlushEachRecord);

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range{};
  BlobLogHeader header(column_family_id, compression, has_ttl,
                       expiration_range);
  s = blob_log_writer->WriteHeader(WriteOptions(), header);
  if (!s.ok()) {
    RemoveFilePartitionMapping(blob_file_number);
    return s;
  }

  partition->writer = std::move(blob_log_writer);
  partition->file_number = blob_file_number;
  partition->file_size = BlobLogHeader::kSize;
  partition->blob_count = 0;
  partition->total_blob_bytes = 0;
  partition->garbage_blob_count = 0;
  partition->garbage_blob_bytes = 0;
  partition->column_family_id = column_family_id;
  partition->compression = compression;
  partition->sync_required = false;
  return Status::OK();
}

Status BlobFilePartitionManager::SealActiveBlobFile(
    const WriteOptions& write_options, Partition* partition,
    SealedFile* sealed_file) {
  assert(partition != nullptr);
  assert(partition->writer);
  assert(sealed_file != nullptr);

  const uint64_t file_number = partition->file_number;
  Status s =
      FinalizeBlobFile(write_options, partition->writer.get(), file_number,
                       partition->blob_count, partition->total_blob_bytes,
                       &sealed_file->addition);
  if (!s.ok()) {
    RemoveFilePartitionMapping(file_number);
    ResetPartitionState(partition);
    return s;
  }

  sealed_file->garbage_blob_count = partition->garbage_blob_count;
  sealed_file->garbage_blob_bytes = partition->garbage_blob_bytes;
  ResetPartitionState(partition);
  return Status::OK();
}

Status BlobFilePartitionManager::SealDeferredFile(
    const WriteOptions& write_options, DeferredFile* deferred,
    SealedFile* sealed_file) {
  assert(deferred != nullptr);
  assert(deferred->writer);
  assert(sealed_file != nullptr);

  Status s = FinalizeBlobFile(
      write_options, deferred->writer.get(), deferred->file_number,
      deferred->blob_count, deferred->total_blob_bytes, &sealed_file->addition);
  if (!s.ok()) {
    RemoveFilePartitionMapping(deferred->file_number);
    return s;
  }

  sealed_file->garbage_blob_count = deferred->garbage_blob_count;
  sealed_file->garbage_blob_bytes = deferred->garbage_blob_bytes;
  deferred->writer.reset();
  return Status::OK();
}

Status BlobFilePartitionManager::FinalizeBlobFile(
    const WriteOptions& write_options, BlobLogWriter* writer,
    uint64_t file_number, uint64_t blob_count, uint64_t total_blob_bytes,
    BlobFileAddition* addition) {
  assert(writer != nullptr);
  assert(addition != nullptr);

  BlobLogFooter footer;
  footer.blob_count = blob_count;

  std::string checksum_method;
  std::string checksum_value;
  Status s = writer->AppendFooter(write_options, footer, &checksum_method,
                                  &checksum_value);
  if (!s.ok()) {
    return s;
  }

  if (blob_callback_ != nullptr) {
    const std::string blob_file_path = BlobFileName(db_path_, file_number);
    s = blob_callback_->OnBlobFileCompleted(
        blob_file_path, column_family_name_, /*job_id=*/0, file_number,
        BlobFileCreationReason::kFlush, s, checksum_value, checksum_method,
        blob_count, total_blob_bytes, /*set_bg_error_on_space_limit=*/true);
    if (!s.ok()) {
      return s;
    }
  }

  *addition =
      BlobFileAddition(file_number, blob_count, total_blob_bytes,
                       std::move(checksum_method), std::move(checksum_value));
  if (blob_file_cache_ != nullptr) {
    const BlobFileOpenInfo blob_file{file_number, addition->GetChecksumValue(),
                                     addition->GetChecksumMethod()};
    blob_file_cache_->RegisterBlobFileChecksum(blob_file);
  }
  return Status::OK();
}

void BlobFilePartitionManager::AddSealedFileGarbage(
    const SealedFile& sealed_file, std::vector<BlobFileGarbage>* garbages) {
  assert(garbages != nullptr);

  if (sealed_file.garbage_blob_count == 0) {
    assert(sealed_file.garbage_blob_bytes == 0);
    return;
  }

  garbages->emplace_back(sealed_file.addition.GetBlobFileNumber(),
                         sealed_file.garbage_blob_count,
                         sealed_file.garbage_blob_bytes);
}

bool BlobFilePartitionManager::MarkPartitionGarbage(Partition* partition,
                                                    uint64_t file_number,
                                                    uint64_t blob_count,
                                                    uint64_t blob_bytes) {
  assert(partition != nullptr);

  if (!partition->writer || partition->file_number != file_number) {
    return false;
  }

  partition->garbage_blob_count += blob_count;
  partition->garbage_blob_bytes += blob_bytes;
  assert(partition->garbage_blob_count <= partition->blob_count);
  assert(partition->garbage_blob_bytes <= partition->total_blob_bytes);
  return true;
}

bool BlobFilePartitionManager::MarkDeferredFileGarbage(DeferredFile* deferred,
                                                       uint64_t file_number,
                                                       uint64_t blob_count,
                                                       uint64_t blob_bytes) {
  assert(deferred != nullptr);

  if (deferred->file_number != file_number) {
    return false;
  }

  deferred->garbage_blob_count += blob_count;
  deferred->garbage_blob_bytes += blob_bytes;
  assert(deferred->garbage_blob_count <= deferred->blob_count);
  assert(deferred->garbage_blob_bytes <= deferred->total_blob_bytes);
  return true;
}

bool BlobFilePartitionManager::MarkSealedFileGarbage(
    std::vector<SealedFile>* sealed_files, uint64_t file_number,
    uint64_t blob_count, uint64_t blob_bytes) {
  assert(sealed_files != nullptr);

  for (auto& sealed_file : *sealed_files) {
    if (sealed_file.addition.GetBlobFileNumber() != file_number) {
      continue;
    }

    sealed_file.garbage_blob_count += blob_count;
    sealed_file.garbage_blob_bytes += blob_bytes;
    assert(sealed_file.garbage_blob_count <=
           sealed_file.addition.GetTotalBlobCount());
    assert(sealed_file.garbage_blob_bytes <=
           sealed_file.addition.GetTotalBlobBytes());
    return true;
  }

  return false;
}

Status BlobFilePartitionManager::MaybePrepopulateBlobCache(
    const BlobDirectWriteSettings& settings, const Slice& original_value,
    uint64_t blob_file_number, uint64_t blob_offset) {
  if (settings.blob_cache == nullptr ||
      settings.prepopulate_blob_cache != PrepopulateBlobCache::kFlushOnly) {
    return Status::OK();
  }

  FullTypedCacheInterface<BlobContents, BlobContentsCreator> blob_cache{
      settings.blob_cache};
  const OffsetableCacheKey base_cache_key(db_id_, db_session_id_,
                                          blob_file_number);
  const CacheKey cache_key = base_cache_key.WithOffset(blob_offset);
  return blob_cache.InsertSaved(cache_key.AsSlice(), original_value, nullptr,
                                Cache::Priority::BOTTOM,
                                CacheTier::kVolatileTier);
}

uint32_t BlobFilePartitionManager::SelectWideColumnPartition(
    uint32_t column_family_id, const Slice& key,
    const WideColumns& columns) const {
  return strategy_->SelectPartition(num_partitions_, column_family_id, key,
                                    columns) %
         num_partitions_;
}

Status BlobFilePartitionManager::WriteBlob(
    const WriteOptions& write_options, uint32_t column_family_id,
    CompressionType compression, const Slice& key, const Slice& value,
    uint64_t* blob_file_number, uint64_t* blob_offset, uint64_t* blob_size,
    const BlobDirectWriteSettings& settings, const uint32_t* partition_idx) {
  assert(blob_file_number != nullptr);
  assert(blob_offset != nullptr);
  assert(blob_size != nullptr);

  // Do compression before taking the partition mutex so large-value CPU work
  // does not serialize writers. A concurrent file rollover can still cause
  // this compressed buffer to be discarded below, which is acceptable on this
  // non-hot failure/configuration-change path.
  GrowableBuffer compressed_value;
  Slice write_value = value;
  if (compression != kNoCompression) {
    auto& compression_state =
        GetDirectWriteCompressionState(compression, settings.compression_opts);
    if (compression_state.compressor == nullptr) {
      return Status::NotSupported(
          "Blob direct write compression type not supported");
    }
    Status s = LegacyForceBuiltinCompression(*compression_state.compressor,
                                             &compression_state.working_area,
                                             value, &compressed_value);
    if (!s.ok()) {
      return s;
    }
    write_value = Slice(compressed_value);
  }

  // Partition selection is based on the logical write inputs. In particular,
  // strategies that inspect value contents or size see the original
  // uncompressed value rather than `write_value`. The modulo here is
  // intentional so custom strategies can return arbitrary hashed or sentinel
  // values without violating the partition bounds.
  const uint32_t selected_partition_idx =
      partition_idx != nullptr
          ? (*partition_idx % num_partitions_)
          : (strategy_->SelectPartition(num_partitions_, column_family_id, key,
                                        value) %
             num_partitions_);

  {
    MutexLock lock(&mutex_);
    Partition* partition = partitions_[selected_partition_idx].get();

    auto seal_current_file = [&]() -> Status {
      if (!partition->writer) {
        return Status::OK();
      }
      SealedFile sealed_file;
      Status s = SealActiveBlobFile(write_options, partition, &sealed_file);
      if (s.ok()) {
        current_generation_sealed_files_.push_back(std::move(sealed_file));
      }
      return s;
    };

    const uint64_t record_size =
        BlobLogRecord::kHeaderSize + key.size() + write_value.size();
    const uint64_t future_file_size =
        partition->file_size + record_size + BlobLogFooter::kSize;

    if (partition->writer &&
        (partition->column_family_id != column_family_id ||
         partition->compression != compression ||
         (partition->blob_count > 0 && future_file_size > blob_file_size_))) {
      Status s = seal_current_file();
      if (!s.ok()) {
        return s;
      }
    }

    if (!partition->writer) {
      Status s = OpenNewBlobFile(partition, column_family_id, compression,
                                 selected_partition_idx);
      if (!s.ok()) {
        return s;
      }
    }

    uint64_t key_offset = 0;
    Status s = partition->writer->AddRecord(write_options, key, write_value,
                                            &key_offset, blob_offset);
    if (!s.ok()) {
      return s;
    }

    partition->sync_required = true;
    partition->blob_count += 1;
    partition->total_blob_bytes += record_size;
    partition->file_size = BlobLogHeader::kSize + partition->total_blob_bytes;

    *blob_file_number = partition->file_number;
    *blob_size = write_value.size();
  }

  Status prepopulate_s = MaybePrepopulateBlobCache(
      settings, value, *blob_file_number, *blob_offset);
  if (!prepopulate_s.ok() && info_log_ != nullptr) {
    ROCKS_LOG_WARN(info_log_,
                   "Failed to pre-populate direct-write blob cache entry: %s",
                   prepopulate_s.ToString().c_str());
  }

  return Status::OK();
}

void BlobFilePartitionManager::RotateCurrentGeneration(uint64_t memtable_id) {
  MutexLock lock(&mutex_);

  GenerationBatch batch;
  batch.memtable_id = memtable_id;
  batch.sealed_files = std::move(current_generation_sealed_files_);
  current_generation_sealed_files_.clear();

  for (auto& partition : partitions_) {
    if (!partition->writer) {
      continue;
    }

    DeferredFile deferred;
    deferred.writer = std::move(partition->writer);
    deferred.file_number = partition->file_number;
    deferred.blob_count = partition->blob_count;
    deferred.total_blob_bytes = partition->total_blob_bytes;
    deferred.garbage_blob_count = partition->garbage_blob_count;
    deferred.garbage_blob_bytes = partition->garbage_blob_bytes;
    ResetPartitionState(partition.get());
    batch.deferred_files.emplace_back(std::move(deferred));
  }

  pending_generations_.emplace_back(std::move(batch));
}

BlobFilePartitionManager::GenerationBatch*
BlobFilePartitionManager::FindPendingGeneration(uint64_t memtable_id) {
  for (auto& batch : pending_generations_) {
    if (batch.memtable_id == memtable_id) {
      return &batch;
    }
  }
  return nullptr;
}

Status BlobFilePartitionManager::PrepareFlushAdditions(
    const WriteOptions& write_options,
    const std::vector<uint64_t>& memtable_ids,
    std::vector<BlobFileAddition>* additions,
    std::vector<BlobFileGarbage>* garbages,
    std::vector<std::vector<uint64_t>>* generation_blob_file_numbers) {
  assert(additions != nullptr);
  assert(garbages != nullptr);
  additions->clear();
  garbages->clear();
  if (generation_blob_file_numbers != nullptr) {
    generation_blob_file_numbers->clear();
  }

  MutexLock lock(&mutex_);
  for (const uint64_t memtable_id : memtable_ids) {
    GenerationBatch* const batch_ptr = FindPendingGeneration(memtable_id);
    if (batch_ptr == nullptr) {
      return Status::Corruption(
          "Missing blob direct write generation metadata for flush");
    }
    GenerationBatch& batch = *batch_ptr;
    while (!batch.deferred_files.empty()) {
      DeferredFile deferred = std::move(batch.deferred_files.front());
      batch.deferred_files.pop_front();

      SealedFile sealed_file;
      Status s = SealDeferredFile(write_options, &deferred, &sealed_file);
      if (!s.ok()) {
        return s;
      }
      // Keep each successfully sealed file attached to the generation
      // immediately. If a later seal or the flush job itself fails, retry must
      // reuse these exact on-disk files instead of finalizing replacements.
      batch.sealed_files.push_back(std::move(sealed_file));
    }

    std::vector<uint64_t>* generation_file_numbers = nullptr;
    if (generation_blob_file_numbers != nullptr) {
      generation_blob_file_numbers->emplace_back();
      generation_file_numbers = &generation_blob_file_numbers->back();
      generation_file_numbers->reserve(batch.sealed_files.size());
    }

    for (const auto& sealed_file : batch.sealed_files) {
      additions->push_back(sealed_file.addition);
      AddSealedFileGarbage(sealed_file, garbages);
      if (generation_file_numbers != nullptr) {
        generation_file_numbers->push_back(
            sealed_file.addition.GetBlobFileNumber());
      }
    }
  }

  return Status::OK();
}

Status BlobFilePartitionManager::MarkBlobWriteAsGarbage(uint64_t file_number,
                                                        uint64_t blob_count,
                                                        uint64_t blob_bytes) {
  if (blob_count == 0) {
    assert(blob_bytes == 0);
    return Status::OK();
  }

  MutexLock lock(&mutex_);
  for (auto& partition : partitions_) {
    if (MarkPartitionGarbage(partition.get(), file_number, blob_count,
                             blob_bytes)) {
      return Status::OK();
    }
  }

  if (MarkSealedFileGarbage(&current_generation_sealed_files_, file_number,
                            blob_count, blob_bytes)) {
    return Status::OK();
  }

  for (auto& batch : pending_generations_) {
    for (auto& deferred : batch.deferred_files) {
      if (MarkDeferredFileGarbage(&deferred, file_number, blob_count,
                                  blob_bytes)) {
        return Status::OK();
      }
    }
    if (MarkSealedFileGarbage(&batch.sealed_files, file_number, blob_count,
                              blob_bytes)) {
      return Status::OK();
    }
  }

  const std::string message =
      "Could not match failed blob direct-write rollback for file #" +
      std::to_string(file_number);
  if (info_log_ != nullptr) {
    ROCKS_LOG_ERROR(info_log_, "%s", message.c_str());
  }
  return Status::Corruption(message);
}

void BlobFilePartitionManager::CommitFlushedGenerations(
    uint64_t earliest_unflushed_memtable_id) {
  std::vector<uint64_t> sealed_file_numbers;
  {
    MutexLock lock(&mutex_);
    while (!pending_generations_.empty() &&
           pending_generations_.front().memtable_id <
               earliest_unflushed_memtable_id) {
      for (const auto& sealed_file :
           pending_generations_.front().sealed_files) {
        sealed_file_numbers.push_back(sealed_file.addition.GetBlobFileNumber());
      }
      pending_generations_.pop_front();
    }
  }
  RemoveFilePartitionMappings(sealed_file_numbers);
}

Status BlobFilePartitionManager::SyncAllOpenFiles(
    const WriteOptions& write_options) {
  MutexLock lock(&mutex_);
  for (const auto& partition : partitions_) {
    if (!partition->writer || !partition->sync_required) {
      continue;
    }
    Status s = partition->writer->Sync(write_options);
    if (!s.ok()) {
      return s;
    }
    partition->sync_required = false;
  }
  return Status::OK();
}

void BlobFilePartitionManager::GetActiveBlobFileNumbers(
    UnorderedSet<uint64_t>* file_numbers) const {
  assert(file_numbers != nullptr);
  ReadLock lock(&file_partition_mutex_);
  for (const auto& entry : file_to_partition_) {
    file_numbers->insert(entry.first);
  }
}

void BlobFilePartitionManager::GetProtectedBlobFileNumbers(
    UnorderedSet<uint64_t>* file_numbers) const {
  assert(file_numbers != nullptr);
  ReadLock lock(&file_partition_mutex_);
  for (const auto& entry : protected_blob_file_refs_) {
    file_numbers->insert(entry.first);
  }
}

bool BlobFilePartitionManager::IsTrackedBlobFileNumber(
    uint64_t file_number) const {
  ReadLock lock(&file_partition_mutex_);
  return file_to_partition_.find(file_number) != file_to_partition_.end() ||
         protected_blob_file_refs_.find(file_number) !=
             protected_blob_file_refs_.end();
}

void BlobFilePartitionManager::ProtectSealedBlobFileNumbers(
    const std::vector<uint64_t>& file_numbers) {
  if (file_numbers.empty()) {
    return;
  }

  WriteLock lock(&file_partition_mutex_);
  for (uint64_t file_number : file_numbers) {
    ++protected_blob_file_refs_[file_number];
  }
}

void BlobFilePartitionManager::UnprotectSealedBlobFileNumbers(
    const std::vector<uint64_t>& file_numbers) {
  if (file_numbers.empty()) {
    return;
  }

  WriteLock lock(&file_partition_mutex_);
  for (uint64_t file_number : file_numbers) {
    auto it = protected_blob_file_refs_.find(file_number);
    if (it == protected_blob_file_refs_.end()) {
      if (info_log_ != nullptr) {
        ROCKS_LOG_ERROR(info_log_,
                        "Memtable blob protection underflow for file #%" PRIu64,
                        file_number);
      }
      continue;
    }

    assert(it->second > 0);
    --it->second;
    if (it->second == 0) {
      protected_blob_file_refs_.erase(it);
      if (blob_file_cache_ != nullptr) {
        // Once the last memtable-backed reference goes away, any cached reader
        // for this file is either obsolete or can be reopened through the
        // manifest-visible path. Evict it here so delayed protection does not
        // leave an obsolete blob reader behind until DB close.
        blob_file_cache_->Evict(file_number);
        if (file_to_partition_.find(file_number) == file_to_partition_.end()) {
          blob_file_cache_->UnregisterBlobFileChecksum(file_number);
        }
      }
    }
  }
}

void BlobFilePartitionManager::RemoveFilePartitionMappings(
    const std::vector<uint64_t>& file_numbers) {
  if (file_numbers.empty()) {
    return;
  }

  WriteLock lock(&file_partition_mutex_);
  for (uint64_t file_number : file_numbers) {
    file_to_partition_.erase(file_number);
    if (blob_file_cache_ != nullptr) {
      // In-flight direct-write reads may have cached a footer-less reader for
      // this file before it was sealed. Drop it now so future manifest-visible
      // reads reopen against the finalized on-disk size and footer state.
      blob_file_cache_->Evict(file_number);
      if (protected_blob_file_refs_.find(file_number) ==
          protected_blob_file_refs_.end()) {
        blob_file_cache_->UnregisterBlobFileChecksum(file_number);
      }
    }
  }
}

Status BlobFilePartitionManager::ResolveBlobDirectWriteIndex(
    const ReadOptions& read_options, const Slice& user_key,
    const BlobIndex& blob_idx, const Version* version,
    BlobFileCache* blob_file_cache, FilePrefetchBuffer* prefetch_buffer,
    PinnableSlice* blob_value, uint64_t* bytes_read) {
  assert(blob_value != nullptr);

  if (version != nullptr) {
    // Only fall back when the blob file is still owned exclusively by the
    // write path and therefore absent from Version metadata. Once Version
    // knows the file number, every result from Version::GetBlob(), including
    // corruption, must propagate directly.
    if (blob_idx.HasTTL() || blob_idx.IsInlined() ||
        version->storage_info()->GetBlobFileMetaData(blob_idx.file_number()) !=
            nullptr) {
      return version->GetBlob(read_options, user_key, blob_idx, prefetch_buffer,
                              blob_value, bytes_read);
    }
  }

  if (blob_file_cache == nullptr) {
    return version != nullptr ? Status::Corruption("Invalid blob file number")
                              : Status::NotFound();
  }

  if (read_options.read_tier == kBlockCacheTier) {
    // The direct-write fallback below may need to open the blob file reader,
    // which `kBlockCacheTier` forbids. Keep the normal Version-backed path
    // above eligible for cache-only hits.
    return Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
  }

  Status s;
  const BlobFileOpenInfo blob_file{blob_idx.file_number(), Slice(), Slice()};
  CacheHandleGuard<BlobFileReader> reader;
  s = blob_file_cache->GetBlobFileReader(read_options, blob_file, &reader,
                                         /*allow_footer_skip_retry=*/true);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<BlobContents> blob_contents;
  s = reader.GetValue()->GetBlob(read_options, user_key, blob_idx.offset(),
                                 blob_idx.size(), blob_idx.compression(),
                                 prefetch_buffer, nullptr, &blob_contents,
                                 bytes_read);
  if (s.ok()) {
    blob_value->PinSelf(blob_contents->data());
    return s;
  }

  if (!s.IsCorruption()) {
    return s;
  }

  reader.Reset();
  blob_file_cache->Evict(blob_idx.file_number());

  std::unique_ptr<BlobFileReader> fresh_reader;
  s = blob_file_cache->OpenBlobFileReaderUncached(
      read_options, blob_file, &fresh_reader,
      /*allow_footer_skip_retry=*/true);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<BlobContents> fresh_contents;
  s = fresh_reader->GetBlob(read_options, user_key, blob_idx.offset(),
                            blob_idx.size(), blob_idx.compression(),
                            prefetch_buffer, nullptr, &fresh_contents,
                            bytes_read);
  if (s.ok()) {
    blob_value->PinSelf(fresh_contents->data());
    CacheHandleGuard<BlobFileReader> ignored;
    blob_file_cache->RefreshBlobFileReader(blob_file, &fresh_reader, &ignored)
        .PermitUncheckedError();
  }

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
