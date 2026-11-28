//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_garbage.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_write_batch_transformer.h"
#include "port/port.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/hash_containers.h"
namespace ROCKSDB_NAMESPACE {

class BlobFileCache;
class BlobFileCompletionCallback;
class BlobIndex;
class BlobLogWriter;
class FilePrefetchBuffer;
class IOTracer;
class Logger;
class PinnableSlice;
class Version;
class WritableFileWriter;
struct FileOptions;
struct ReadOptions;

// Manages per-partition blob files for write-path blob direct write.
//
// The v1 design keeps each memtable switch as one FIFO generation batch.
// Direct-write files for that memtable are sealed and registered when the
// matching flush commits. The current implementation still serializes blob
// appends through one manager mutex; follow-up PRs can add independent
// per-partition locks to allow concurrent blob writes without changing the
// generation/flush contract. Follow-up PRs can also broaden compatibility as
// the feature matures.
//
// Durability: each blob append is handed to the OS at once (flush-on-write),
// but it is not synced. The WAL record of a blob direct write holds only a
// BlobIndex, so a WAL sync must not make that record durable before its blob
// bytes. Before every WAL sync that can make such a record durable,
// DBImpl::SyncBlobDirectWriteFiles() calls SyncAllOpenFiles() on every manager,
// which syncs the files with unsynced records and the directory entries of new
// files. Each WAL sync calls it after it fixes the list of WALs to sync and
// before the WAL fsync, so every WAL record appended before the WAL sync
// started has durable blob bytes. A sync write leads its write group, appends
// the group's blob values, syncs the blob files, and only then appends the
// group's WAL record and syncs the WAL. SyncWAL() and FlushWAL(true) do not
// keep writers out: the sync does not cover the blob value of a record appended
// during it (see DBImpl::SyncBlobDirectWriteFiles()). The closed-WAL sync
// before a flush syncs only closed WALs, which get no new records. Apart from
// SyncBlobDirectWriteFiles(), a blob direct write file is synced only when it
// is sealed. If a blob file fails to sync or seal, no later memtable flush can
// succeed, so the flush stops the DB (see StopDBAfterBlobDirectWriteError() in
// db_impl_compaction_flush.cc).
//
// Known limitation: a sync holds the manager mutex during its file and
// directory fsyncs, and a memtable switch takes the same mutex while it holds
// the DB mutex. Every WAL sync syncs the managers of all column families that
// use blob direct write, not only the one being written or flushed. For
// example, a flush of a column family without blob direct write runs the
// closed-WAL sync, which syncs every such manager. A memtable switch of a
// blob direct write column family can wait for those fsyncs while it holds
// the DB mutex, and with it every other thread that needs the DB mutex. Blob
// appends of other writers also wait for the manager mutex during those
// fsyncs.
class BlobFilePartitionManager {
 public:
  using FileNumberAllocator = std::function<uint64_t()>;

  // Creates the per-column-family manager for write-path blob files.
  BlobFilePartitionManager(
      uint32_t num_partitions,
      std::shared_ptr<BlobFilePartitionStrategy> strategy,
      FileNumberAllocator file_number_allocator, FileSystem* fs,
      SystemClock* clock, Statistics* statistics,
      const FileOptions& file_options, std::string blob_dir,
      std::string column_family_name, uint64_t blob_file_size, bool use_fsync,
      uint64_t blob_writer_max_buffer_size, BlobFileCache* blob_file_cache,
      BlobFileCompletionCallback* blob_callback,
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      FileChecksumGenFactory* file_checksum_gen_factory,
      const FileTypeSet& checksum_handoff_file_types,
      const std::shared_ptr<IOTracer>& io_tracer, std::string db_id,
      std::string db_session_id, Logger* info_log);

  // Evicts cached readers for manager-owned files during shutdown.
  ~BlobFilePartitionManager();

  // Appends one blob record to a partition file and returns the resulting
  // BlobIndex location metadata.
  Status WriteBlob(const WriteOptions& write_options, uint32_t column_family_id,
                   CompressionType compression, const Slice& key,
                   const Slice& value, uint64_t* blob_file_number,
                   uint64_t* blob_offset, uint64_t* blob_size,
                   const BlobDirectWriteSettings& settings,
                   const uint32_t* partition_idx = nullptr);

  // Selects the partition to use for all blob-backed columns of one PutEntity
  // operation. The return value is already normalized to [0, num_partitions_).
  uint32_t SelectWideColumnPartition(uint32_t column_family_id,
                                     const Slice& key,
                                     const WideColumns& columns) const;

  // Move the current active partition files into the generation of the
  // memtable `memtable_id`, which just became immutable. Called from
  // SwitchMemtable() while DB mutex is held. No I/O is performed here.
  void RotateCurrentGeneration(uint64_t memtable_id);

  // Seal the queued generations of the memtables `memtable_ids` and return all
  // blob additions and initial-garbage updates that must be registered with
  // the flush of those memtables. Concurrent flushes pick different
  // memtables, so generations are matched by memtable ID, not by queue
  // position. Generations stay queued until CommitFlushedGenerations() removes
  // them. If `generation_blob_file_numbers` is non-null, it receives the
  // sealed blob file numbers for each memtable, in `memtable_ids` order.
  Status PrepareFlushAdditions(const WriteOptions& write_options,
                               const std::vector<uint64_t>& memtable_ids,
                               std::vector<BlobFileAddition>* additions,
                               std::vector<BlobFileGarbage>* garbages,
                               std::vector<std::vector<uint64_t>>*
                                   generation_blob_file_numbers = nullptr);

  // Remove the generations of the memtables older than
  // `earliest_unflushed_memtable_id`, whose flush results are installed, and
  // stop tracking their sealed files as manager-owned. A flush that finishes
  // before an older one is installed with it, or rolled back if it fails, so
  // the flush that prepared a generation cannot remove it on its own.
  void CommitFlushedGenerations(uint64_t earliest_unflushed_memtable_id);

  // In v1 flush-on-write mode each AddRecord flushes to the OS immediately, so
  // there is nothing extra to do for the non-sync write path.
  Status FlushAllOpenFiles(const WriteOptions& /*write_options*/) {
    return Status::OK();
  }

  // DBImpl calls this before a WAL sync. It syncs the active and deferred
  // blob files that have unsynced records, and the directory entries of new
  // blob files. "Open" here means not sealed yet; a successful seal already
  // synced the file. A failed seal can leave the BlobLogWriter object after
  // its underlying writable file was closed (see the sync rules below). How a
  // file moves:
  // - The active file of a partition receives appends for the current
  //   memtable.
  // - At rollover (the next record would make the active file larger than
  //   blob_file_size), the manager seals the file and keeps it with the
  //   current memtable (current_generation_sealed_files_).
  //   If that seal fails, the file keeps its error (seal_status) and can keep
  //   its writer object (current_generation_failed_files_).
  // - At a memtable switch, these files and every active partition file move
  //   to a pending generation (pending_generations_). The sealed files go to
  //   its sealed_files as SealedFile entries. The failed files and the active
  //   files go to its deferred_files as DeferredFile entries.
  // - The matching flush seals the remaining files (it writes and syncs the
  //   footer and releases the writer) and registers all sealed files in the
  //   MANIFEST when it commits. If a file already has a seal error, the flush
  //   returns that error.
  // Sync rules: the sync_required flag of a file tracks unsynced appends.
  // This function syncs the active and deferred files that need it. If a
  // failed file still has a writer and needs a sync, it returns the seal
  // error, so the WAL sync fails. Creating a file marks the blob directory
  // for a sync, and this function fsyncs the directory last, if marked.
  // See "Durability" in the class comment.
  Status SyncAllOpenFiles(const WriteOptions& write_options);

  // Returns blob files still owned by the manager and therefore not yet safe
  // to consider obsolete.
  void GetActiveBlobFileNumbers(UnorderedSet<uint64_t>* file_numbers) const;

  // Returns sealed blob files that are still reachable through live memtables
  // or old SuperVersions and therefore must not be purged yet.
  void GetProtectedBlobFileNumbers(UnorderedSet<uint64_t>* file_numbers) const;

  // Returns true when the blob file is still owned by the write path or
  // protected by a live memtable / old SuperVersion.
  bool IsTrackedBlobFileNumber(uint64_t file_number) const;

  void SetBlobWriterMaxBufferSize(uint64_t blob_writer_max_buffer_size) {
    blob_writer_max_buffer_size_.store(blob_writer_max_buffer_size,
                                       std::memory_order_relaxed);
  }

  // Increments / decrements memtable-held protection on sealed blob files.
  void ProtectSealedBlobFileNumbers(const std::vector<uint64_t>& file_numbers);
  void UnprotectSealedBlobFileNumbers(
      const std::vector<uint64_t>& file_numbers);

  // Stops protecting file numbers that are now registered in MANIFEST.
  void RemoveFilePartitionMappings(const std::vector<uint64_t>& file_numbers);

  // Marks blob records from a failed transformed write as initial garbage for
  // the target blob file while keeping the physical bytes in place.
  // Returns Corruption if the manager can no longer match the blob file.
  Status MarkBlobWriteAsGarbage(uint64_t file_number, uint64_t blob_count,
                                uint64_t blob_bytes);

  // Resolves a direct-write BlobIndex by consulting manifest-visible state
  // first, then falling back to direct blob-file reads only when the target
  // blob file is still write-path-owned and therefore not yet tracked by
  // Version. Existing manifest-visible read results, including I/O failures,
  // are returned directly rather than masked by fallback logic.
  static Status ResolveBlobDirectWriteIndex(
      const ReadOptions& read_options, const Slice& user_key,
      const BlobIndex& blob_idx, const Version* version,
      BlobFileCache* blob_file_cache, FilePrefetchBuffer* prefetch_buffer,
      PinnableSlice* blob_value, uint64_t* bytes_read);

 private:
  struct Partition {
    // Active writer for this partition, or null when the partition is idle.
    std::unique_ptr<BlobLogWriter> writer;
    // Metadata tracked while the active file remains open.
    uint64_t file_number = 0;
    uint64_t file_size = 0;
    uint64_t blob_count = 0;
    uint64_t total_blob_bytes = 0;
    // Failed transformed writes already appended to this physical file.
    uint64_t garbage_blob_count = 0;
    uint64_t garbage_blob_bytes = 0;
    uint32_t column_family_id = 0;
    CompressionType compression = kNoCompression;
    bool sync_required = false;
  };

  struct DeferredFile {
    DeferredFile() = default;
    DeferredFile(DeferredFile&&) = default;
    DeferredFile& operator=(DeferredFile&&) = default;
    ~DeferredFile();

    // Blob writer moved out of an active partition at memtable rotation.
    std::unique_ptr<BlobLogWriter> writer;
    // Metadata preserved until flush preparation seals the file.
    uint64_t file_number = 0;
    uint64_t blob_count = 0;
    uint64_t total_blob_bytes = 0;
    // Failed transformed writes already appended before the file is sealed.
    uint64_t garbage_blob_count = 0;
    uint64_t garbage_blob_bytes = 0;
    // Set once sealing fails. The writer cannot be finalized again, and
    // memtables still reference the file, so every flush of its generation
    // fails with this status.
    Status seal_status;
    // The file has appended records that were not synced yet.
    bool sync_required = false;
  };

  struct SealedFile {
    // Blob-file metadata ready to be added to MANIFEST.
    BlobFileAddition addition;
    // Initial unreachable bytes that should be registered as garbage
    // alongside the file addition.
    uint64_t garbage_blob_count = 0;
    uint64_t garbage_blob_bytes = 0;
  };

  struct GenerationBatch {
    // The memtable that became immutable when this generation was rotated.
    uint64_t memtable_id = 0;
    // Files that were still open when the memtable became immutable.
    std::deque<DeferredFile> deferred_files;
    // Files already sealed for this memtable generation.
    std::vector<SealedFile> sealed_files;
  };

  // Clears all active-file bookkeeping from a partition after sealing.
  void ResetPartitionState(Partition* partition);
  // Returns the queued generation of `memtable_id`, or null.
  GenerationBatch* FindPendingGeneration(uint64_t memtable_id);
  // Moves a partition's open file out of the partition and resets it.
  DeferredFile DetachPartitionFile(Partition* partition);
  // Marks a blob file as manager-owned until it is committed or abandoned.
  void AddFilePartitionMapping(uint64_t file_number, uint32_t partition_idx);
  // Stops tracking a blob file after open failure.
  void RemoveFilePartitionMapping(uint64_t file_number);

  // Opens a new active blob file for one partition.
  Status OpenNewBlobFile(Partition* partition, uint32_t column_family_id,
                         CompressionType compression, uint32_t partition_idx);

  // Fsyncs blob_dir_ so that the directory entries of new blob files survive a
  // power loss. SyncAllOpenFiles() calls it when dir_sync_required_ is set.
  Status SyncBlobDirectory(const WriteOptions& write_options);

  // Finalizes one active partition file and returns the MANIFEST addition.
  // Sets `*space_limit_reached` if the SstFileManager's max allowed space is
  // reached; the seal still succeeds.
  Status SealActiveBlobFile(const WriteOptions& write_options,
                            Partition* partition, SealedFile* sealed_file,
                            bool* space_limit_reached);
  // Finalizes one deferred file that was carried over to flush preparation.
  Status SealDeferredFile(const WriteOptions& write_options,
                          DeferredFile* deferred, SealedFile* sealed_file);
  // Appends the footer, reports file completion, and builds the resulting
  // BlobFileAddition for one sealed blob file.
  Status FinalizeBlobFile(const WriteOptions& write_options,
                          BlobLogWriter* writer, uint64_t file_number,
                          uint64_t blob_count, uint64_t total_blob_bytes,
                          BlobFileAddition* addition,
                          bool* space_limit_reached);
  // Appends one initial-garbage record to the flush output if any failed
  // transformed writes targeted the sealed blob file.
  static void AddSealedFileGarbage(const SealedFile& sealed_file,
                                   std::vector<BlobFileGarbage>* garbages);
  // Returns true if the active open file matches `file_number` and consumes
  // the failed-write garbage accounting.
  static bool MarkPartitionGarbage(Partition* partition, uint64_t file_number,
                                   uint64_t blob_count, uint64_t blob_bytes);
  // Returns true if the deferred flush-preparation file matches `file_number`
  // and consumes the failed-write garbage accounting.
  static bool MarkDeferredFileGarbage(DeferredFile* deferred,
                                      uint64_t file_number, uint64_t blob_count,
                                      uint64_t blob_bytes);
  // Returns true if one already-sealed file matches `file_number` and
  // consumes the failed-write garbage accounting.
  static bool MarkSealedFileGarbage(std::vector<SealedFile>* sealed_files,
                                    uint64_t file_number, uint64_t blob_count,
                                    uint64_t blob_bytes);
  // Seeds the blob cache with the original uncompressed value when configured.
  Status MaybePrepopulateBlobCache(const BlobDirectWriteSettings& settings,
                                   const Slice& original_value,
                                   uint64_t blob_file_number,
                                   uint64_t blob_offset);

  // Configured partition fanout and write-selection strategy.
  const uint32_t num_partitions_;
  std::shared_ptr<BlobFilePartitionStrategy> strategy_;
  // Shared services needed to create, track, and cache blob files.
  FileNumberAllocator file_number_allocator_;
  FileSystem* fs_;
  SystemClock* clock_;
  Statistics* statistics_;
  FileOptions file_options_;
  // The directory of this column family's blob files: its first cf_paths
  // entry, which is not always the DB directory.
  std::string blob_dir_;
  std::string column_family_name_;
  // File creation policy and shared blob-file infrastructure.
  uint64_t blob_file_size_;
  bool use_fsync_;
  std::atomic<uint64_t> blob_writer_max_buffer_size_;
  BlobFileCache* blob_file_cache_;
  BlobFileCompletionCallback* blob_callback_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  FileChecksumGenFactory* file_checksum_gen_factory_;
  FileTypeSet checksum_handoff_file_types_;
  std::shared_ptr<IOTracer> io_tracer_;
  // DB identity used when constructing cache keys.
  std::string db_id_;
  std::string db_session_id_;
  Logger* info_log_;

  // One active writer slot per configured partition.
  std::vector<std::unique_ptr<Partition>> partitions_;

  // Protects partition state and generation queues.
  mutable port::Mutex mutex_;
  // Files already sealed while the current mutable memtable was active.
  std::vector<SealedFile> current_generation_sealed_files_;
  // Files whose seal failed while the current mutable memtable was active.
  std::vector<DeferredFile> current_generation_failed_files_;
  // FIFO immutable memtable generations waiting to be flushed.
  std::deque<GenerationBatch> pending_generations_;
  // After a power loss, a new blob file can be missing, even if its data was
  // synced, unless its directory entry was fsynced. Set by OpenNewBlobFile().
  // Cleared after SyncBlobDirectory() succeeds.
  bool dir_sync_required_ = false;

  // Tracks blob files still owned by active or deferred write-path state until
  // MANIFEST commit publishes them.
  std::unordered_map<uint64_t, uint32_t> file_to_partition_;
  // Sealed direct-write files that remain reachable from live memtables, such
  // as old SuperVersions serving lazy iterator reads after flush commit.
  std::unordered_map<uint64_t, uint32_t> protected_blob_file_refs_;
  // Protects file_to_partition_ and protected_blob_file_refs_.
  mutable port::RWMutex file_partition_mutex_;
};

}  // namespace ROCKSDB_NAMESPACE
