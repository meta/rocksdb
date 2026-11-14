# Debates: blob_db Documentation

## Debate: linked_ssts semantics

- **CC position**: Did not flag the doc's description as incorrect. Implicitly accepted "The linked_ssts set tracks which SST files contain BlobIndex references pointing to this blob file." CC also praised the force GC description as correct.
- **Codex position**: Flagged as [WRONG]. RocksDB does not track all SSTs that reference a blob file. Each SST only stores one `oldest_blob_file_number`, and `VersionBuilder` links the SST to that single blob file. The inverse mapping is used by forced GC only. It is not a complete per-blob-file reference set.
- **Code evidence**: In `db/version_builder.cc:992`: `const uint64_t blob_file_number = f->oldest_blob_file_number;` followed by `mutable_meta->LinkSst(file_number)`. Only the oldest blob file number from each SST is used for linking. An SST may contain BlobIndex references to many blob files, but it is only linked to the one with the smallest file number. In `db/version_edit.cc:79`: `if (oldest_blob_file_number == kInvalidBlobFileNumber || oldest_blob_file_number > blob_index.file_number()) { oldest_blob_file_number = blob_index.file_number(); }` confirms only the minimum is tracked.
- **Resolution**: Codex is correct. The doc's claim is misleading. `linked_ssts` is the inverse mapping of `oldest_blob_file_number`, not a complete reference set.
- **Risk level**: high — maintainers relying on `linked_ssts` as a full reference graph would make incorrect assumptions about deletion safety.

## Debate: Crash recovery failure modes

- **CC position**: Listed as a "Completeness Gap" but accepted the existing doc text: "If the BlobFileAddition was committed but the footer is missing, BlobFileReader::Create() will fail the size check (file size < header + footer = 62 bytes for an incomplete file) and return Status::Corruption."
- **Codex position**: Flagged as [WRONG]. The initial size check only rejects files smaller than 62 bytes. Larger truncated or malformed files continue to footer decode and can fail on magic-number validation or CRC validation.
- **Code evidence**: In `db/blob/blob_file_reader.cc:115`: `if (*file_size < BlobLogHeader::kSize + BlobLogFooter::kSize) { return Status::Corruption("Malformed blob file"); }` — this only catches files under 62 bytes. For files >= 62 bytes with a corrupt footer, `ReadFooter()` at line 199 reads the last 32 bytes and calls `BlobLogFooter::DecodeFrom()` which checks magic number and CRC. In `db/blob/blob_log_format.cc`, `DecodeFrom` validates magic number and CRC. A file that is, say, 100 bytes (> 62) but has random data in the footer region will fail at footer decode, not the size check.
- **Resolution**: Codex is correct. The doc was too specific about the failure mode. Incomplete files can fail the size check, footer magic number validation, or footer CRC validation, depending on how much data reached disk.
- **Risk level**: medium — developers debugging crash recovery need to understand all possible failure modes.

## Debate: GetBlobStats() return fields

- **CC position**: Did not flag the doc's claim that GetBlobStats() returns `total_file_count`.
- **Codex position**: Flagged as [WRONG]. `GetBlobStats()` returns only `total_file_size`, `total_garbage_size`, and `space_amp`. File count comes from `GetBlobFiles().size()`.
- **Code evidence**: In `db/version_set.h:446-450`: `struct BlobStats { uint64_t total_file_size = 0; uint64_t total_garbage_size = 0; double space_amp = 0.0; };` — no `total_file_count` field. In `db/internal_stats.cc:894`: `oss << "Number of blob files: " << vstorage->GetBlobFiles().size()` — file count is obtained separately.
- **Resolution**: Codex is correct. The doc incorrectly lists `total_file_count` as a field of `GetBlobStats()`.
- **Risk level**: low — the file count is still available, just from a different API.

## Debate: OnBlobFileCreated success semantics

- **CC position**: Did not flag the doc's claim that `OnBlobFileCreated` is called "when a blob file has been successfully written."
- **Codex position**: Flagged as [WRONG]. `OnBlobFileCreated` is called for both success and failure. Also, job-info payloads use `BlobFileAdditionInfo`/`BlobFileGarbageInfo`, not `BlobFileInfo`.
- **Code evidence**: In `include/rocksdb/listener.h:881-882`: "It will be called whether the file is successfully created or not. User can check info.status to see if it succeeded or not." In `include/rocksdb/listener.h:380,487,491`: `FlushJobInfo` and `CompactionJobInfo` use `std::vector<BlobFileAdditionInfo>` and `std::vector<BlobFileGarbageInfo>`, not `BlobFileInfo`.
- **Resolution**: Codex is correct on both points.
- **Risk level**: medium — applications relying on `OnBlobFileCreated` to mean "success" would miss failure notifications.

## Debate: Dynamic reconfiguration scope

- **CC position**: Did not flag the doc's claim that `enable_blob_files` and `min_blob_size` changes "affect new writes only."
- **Codex position**: Flagged as [MISLEADING]. These settings are consulted during compaction output too. Old inline values can be extracted during later compactions, and existing blob references can be inlined back during GC.
- **Code evidence**: In `db/compaction/compaction_iterator.cc`, `ExtractLargeValueIfNeededImpl()` checks the current `min_blob_size` and `enable_blob_files` for every key-value pair emitted during compaction output, regardless of whether the value was originally inline or a blob reference. `GarbageCollectBlobIfNeeded()` similarly uses current settings when deciding whether to re-extract or inline a relocated blob.
- **Resolution**: Codex is correct. The settings affect all future flush/compaction outputs, not just application writes. This includes retroactive extraction of inline values and inlining of existing blob references.
- **Risk level**: medium — operators toggling these settings need to understand compaction-time effects.

## Debate: Statistics coverage (active vs. legacy)

- **CC position**: Did not flag the statistics tables as including inactive/legacy tickers.
- **Codex position**: Flagged as [WRONG]. Many operation-count/latency tickers (BLOB_DB_NUM_GET, BLOB_DB_GET_MICROS, BLOB_DB_GC_NUM_FILES, etc.) are only used by the legacy stacked BlobDB in `utilities/blob_db/`, not the integrated BlobDB in `db/blob/`.
- **Code evidence**: The integrated BlobDB code (`db/blob/blob_source.cc`, `db/blob/blob_file_reader.cc`, `db/blob/blob_log_writer.cc`, `db/blob/blob_file_builder.cc`, `db/compaction/compaction_job.cc`) only records: cache tickers (BLOB_DB_CACHE_*), I/O tickers (BLOB_DB_BLOB_FILE_BYTES_WRITTEN/READ, BLOB_DB_BLOB_FILE_SYNCED), compression histograms (BLOB_DB_COMPRESSION_MICROS, BLOB_DB_DECOMPRESSION_MICROS), file I/O histograms (BLOB_DB_BLOB_FILE_WRITE_MICROS, BLOB_DB_BLOB_FILE_READ_MICROS, BLOB_DB_BLOB_FILE_SYNC_MICROS), and GC relocated tickers (BLOB_DB_GC_NUM_KEYS_RELOCATED, BLOB_DB_GC_BYTES_RELOCATED). The operation-count tickers (BLOB_DB_NUM_PUT, BLOB_DB_NUM_GET, etc.), operation-latency histograms (BLOB_DB_GET_MICROS, BLOB_DB_WRITE_MICROS, etc.), key/value size histograms (BLOB_DB_KEY_SIZE, BLOB_DB_VALUE_SIZE), and most GC tickers (BLOB_DB_GC_NUM_FILES, BLOB_DB_GC_NUM_NEW_FILES, BLOB_DB_GC_FAILURES) are only recorded in `utilities/blob_db/`.
- **Resolution**: Codex is correct. The doc should clearly separate actively recorded integrated BlobDB metrics from legacy-only enums.
- **Risk level**: high — users monitoring the inactive tickers would see zero values and think BlobDB is not working.
