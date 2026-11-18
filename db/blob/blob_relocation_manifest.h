// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

// Blob relocation: MANIFEST records and Version state
// ===================================================
//
// Background
// ----------
// Indirect blob GC removes garbage from blob files without rewriting any SST.
// It copies the live values of whole blob files into a relocation file. The
// SSTs keep their old references, so every reader must still find each value,
// also after a crash and reopen. Files on disk cannot answer this: a relocation
// file on disk may be half written or not installed yet. So the MANIFEST
// records where values live, and each Version owns that state, as it owns SST
// and blob files.
// This file holds:
//   - the MANIFEST record codecs for relocation files and routes. A route sends
//     reads of a file that GC removed to the relocation file that now holds its
//     values (see Terms). VersionEdit encodes these records with the codecs in
//     this file, and VersionBuilder checks and applies them.
//   - the per-Version state built from these records (BlobFileTable and
//     BlobRelocationState). Version::GetBlob reads through this state.
// The code that creates relocation files and routes is not in this file.
//
// Terms
// -----
// Origin, original offset, stable identity, generation number, live file, old
// file and route are defined in db/blob/blob_index.h. This file adds:
// - relocation file: a block-based table that holds copied values. A reader
//   finds a value in it by its stable identity; see blob_relocation_file.h.
// - target: the live relocation file that a route points to. It holds all live
//   values of the old file.
// - physical file: a file that exists on disk under its file number: an SST, a
//   blob file or a relocation file. A generation number is not always a
//   physical file: after GC removes a blob file, its number can stay only as
//   the key of a route.
// - indirect origin: a normal blob file written with enable_blob_indirection on
//   (BlobFileAddition::IsIndirectOrigin()). Only such a file can be the origin
//   of a kIndirectBlob reference.
// - format fence: a required MANIFEST record that says a column family or the
//   whole DB may hold data that only blob indirection writes (feature data,
//   defined below). A binary that does not know the record fails to open the
//   DB. The per-CF format fence marks one column family, and the DB-global
//   format fence marks the whole DB (see "MANIFEST records" below).
// - feature data: persisted state that only blob indirection writes: a format
//   fence, a relocation file, a route, a blob file retirement or accepted-loss
//   record, an indirect-origin blob file, or an SST with exact reference
//   metadata (db/blob/blob_reference_metadata.h).
// - fenced column family: a column family whose per-CF format fence is set.
//   Only a fenced column family has a file table (BlobFileTable).
// - legacy column family: a column family without the per-CF format fence. It
//   has no file table and no relocation state.
// - activation: the first edit in the DB that sets a per-CF format fence. From
//   then on the VersionSet keeps the DB-global format fence.
//
// Lifecycle of one value
// ----------------------
// This example follows one value from its write until its route is deleted.
// It uses key "k", blob file 12, relocation files 20 and 30, and offset 4096.
// All of these are example values.
//
// Base RocksDB
// 1. Write and flush. Put("k", v) goes to the memtable. With
//    enable_blob_files, a flush writes the SST, and a value of at least
//    min_blob_size goes to a blob file instead, here file 12 at offset 4096.
//    The SST keeps only a small BlobIndex that points to it. Later
//    compactions copy the BlobIndex, not the value.
// 2. Garbage. When "k" is overwritten or deleted, a compaction drops its old
//    BlobIndex, and the value in file 12 becomes garbage. File 12 cannot be
//    deleted while it holds other live values. Compaction-time blob GC (base
//    RocksDB, enable_blob_garbage_collection) can move a value only when a
//    compaction rewrites the SST entry that points to it, because a kBlob
//    index holds the file number and offset. So file 12 goes away only after
//    compactions have rewritten every SST entry that points to it.
//
// Blob indirection enables moving values in blob files without rewriting SSTs
// 3. With blob indirection, the SST stores a kIndirectBlob index (see
//    db/blob/blob_index.h) with origin = 12 and original offset = 4096. Its
//    generation number is 12: for kIndirectBlob it equals the origin and is not
//    stored. Origin and original offset never change, so they name the value
//    for its whole life (its stable identity). The generation number tells a
//    reader where to start looking.
// 4. Indirect blob GC. A background job picks input files that hold a lot of
//    garbage. An input is a blob file written with enable_blob_indirection on
//    (an indirect origin), or a relocation file from an earlier GC round.
//    Here the only input is blob file 12. The job copies only the live values
//    of its inputs into a new relocation file 20, then installs the result
//    with one VersionEdit:
//      - adds relocation file 20;
//      - adds a route 12 -> 20, so that references that still say
//        generation number 12 find their value in file 20;
//      - re-points every route whose target is an input to the new relocation
//        file. No route targets file 12 here, so nothing is re-pointed (step
//        4b shows a round that re-points a route);
//      - removes the input files from the Version: a kBlobFileRetirement
//        record for a blob file (here file 12), a kBlobRelocationFileDeletion
//        record for a relocation file. The files are deleted from disk later,
//        as other obsolete files are. The routes stay; step 7 removes them.
//    No SST changes. The SSTs still say generation number 12.
// 4b. A later GC round. Relocation file 20 is now an input, and its live
//    values move to relocation file 30. The edit adds file 30, changes route
//    12 -> 20 to 12 -> 30 (so a read still follows at most one route), adds
//    route 20 -> 30 for references that a compaction already flattened to 20
//    (step 6), and deletes file 20. If an input has no live values left, no
//    reference can need it, so the edit deletes the routes into the input
//    instead of re-pointing them.
// 5. Read. A read of key "k" decodes its BlobIndex (generation number 12,
//    origin 12, original offset 4096). The blob cache key is the stable
//    identity (origin 12, original offset 4096), not the file the value is in.
//    A whole-value read that misses fills the cache under that key (when a blob
//    cache is set and ReadOptions::fill_cache is true), so a cached value still
//    hits after indirect blob GC moves it, and a hit needs no file lookup
//    (BlobSource::GetIndirectBlobFromCache()). On a cache miss, the Version
//    looks up generation number 12 in its file table (BlobFileTable,
//    Version::ResolveBlobPhysicalFile()). One entry per number says what the
//    number is now:
//      - a live blob file: the value has not moved; read it there;
//      - a live relocation file: a flattened reference (step 6); read it
//        there;
//      - a route: here 12 -> 20, so look up 20, which must be a live
//        relocation file. A route target is never itself a route key, so a
//        read follows at most one route (one hop).
//    Anything else is Corruption: no entry, a route whose target is not a
//    live relocation file, or a live blob file that is not the origin of
//    the reference or is not an indirect origin. How the value is found
//    inside a relocation file is in "Reads" in
//    db/blob/blob_relocation_file.h.
// 6. Flatten. A later compaction may rewrite the generation number of a
//    reference to the route target (generation number = 20, a flattened
//    reference). Origin and original offset do not change. Such a reference
//    finds file 20 in the file table directly. Flatten is a side effect of
//    compaction: no job rewrites an SST only to flatten it.
// 7. Route deletion. When no SST uses old file number 12 any more, a later
//    run of the census job (the background step that updates the blob
//    reference counts after each install) writes a route deletion record,
//    which removes its route (12 -> 20, or 12 -> 30 after step 4b). It is not
//    a separate job (see DBImpl::RetireBlobRelocationRoutes()).
//
// MANIFEST records (codecs in this file and in VersionEdit)
// ---------------------------------------------------------
// All tags are required tags (below kTagSafeIgnoreMask), so a binary that does
// not know them fails to open the DB instead of misreading it.
// - Per-CF format fence (kBlobIndirectionFormatVersion): marks a column family
//   that may hold feature data. Every edit that adds an SST built with
//   enable_blob_indirection on sets it; the first such edit is the activation
//   (see Terms). It is sticky: it stays after the option is turned off. The
//   relocation records below need it.
// - DB-global fence (kDbBlobIndirectionFormatVersion): marks the DB, not a
//   column family. Each MANIFEST snapshot writes it first when the DB's format
//   is active.
// - Relocation file addition and deletion: a Version owns the live relocation
//   files of its column family. The addition record keeps only numbers
//   (BlobRelocationFileAddition); the directory stays in the file. File numbers
//   are never reused.
// - Route and route deletion: one route per old file. The old file is not a
//   live file, the target is a live relocation file of the same column family,
//   and no route points to a deleted file. So a target is never itself a route
//   key: no chains, one hop. A route deletion record removes the route of an
//   old file that no SST uses any more, so the Version does not keep unused
//   routes forever.
// - Blob file retirement (kBlobFileRetirement): removes a live indirect-origin
//   blob file that GC has collected. A file with live values also gets its
//   route in the same edit.
// - Accepted loss (kBlobPhysicalFileLoss): records that the bytes of a live
//   relocation file are gone. It never names a normal or indirect-origin blob
//   file; a missing one uses the base dropped-file record (kDroppedBlobFile).
//   VersionBuilder keeps the file in the Version and marks it lost, and reads
//   treat its values as deleted. An open with best_efforts_blob_db_repair
//   marks a missing or corrupt relocation file lost; tests can also create a
//   loss. WriteCurrentStateToManifest() writes the record again for each file
//   that is marked lost. RepairDB writes it again only for a lost file that a
//   kept SST still references. See "Lost relocation files" below.
// - The DB-global fence stays set even after the last fenced column family is
//   dropped.
//
// File protection
// ---------------
// New file numbers come from one DB-wide counter, so two column families do
// not get the same file number. Nothing checks this across column families.
// These checks keep a wrong MANIFEST record from deleting or misreading a file:
// - Per column family: VersionBuilder applies relocation, route, retirement and
//   loss records to the state of the edit's own column family. It rejects a
//   relocation file whose number is already an SST, a blob file or a different
//   relocation file there (an identical repeated record, as at replay, is
//   accepted); a new SST whose number is a relocation file there; a new blob
//   file whose number is a relocation file or a route there; a route whose
//   target is not a live relocation file there; a relocation file deletion that
//   names no live relocation file there; and a loss record that names no live
//   relocation file there.
// - Purge: the obsolete file purge is DB-wide. A .blob file stays while any
//   column family's Version lists it as a blob file or a relocation file
//   (Version::RemoveLiveFiles() for a normal purge, Version::AddLiveFiles()
//   for a full scan).
// - Read: when a reader opens a file, it checks the column family ID in the
//   blob file header, or in the table properties of a relocation file, and
//   returns Corruption on a mismatch. Each read in a relocation file also
//   checks the value's key (origin, original offset), so a wrong route cannot
//   return another value.
//
// Lost relocation files
// ---------------------
// A loss record (kBlobPhysicalFileLoss, above) marks a relocation file lost
// when its bytes are gone (missing or corrupt). This is the life of a lost
// file:
// 1. Mark. VersionBuilder marks the file lost (see "Accepted loss" above),
//    and the open writes the loss record into its new MANIFEST.
// 2. Read. A reference whose generation number, or the target of its route,
//    is a lost file reads as a same-sequence point deletion
//    (VersionStorageInfo::IsBlobReferenceLost()). A wide-column entity whose
//    columns are all lost blob columns reads as a deletion. Any other entity
//    loses only its lost blob columns (Version::IsBlobValueLost()).
// 3. Repair. A compaction whose input Version has a lost file turns such
//    references into deletions (Compaction::RepairsLostBlobReferences()).
//    Its output has exact reference metadata, also at L0
//    (Compaction::CollectExactBlobReferences()). The reason: a coarse SST pins
//    every generation number at or above its frontier (see "Coarse SST" in
//    db/blob/blob_reference_metadata.h). A lost relocation file stays pinned
//    while an old number routed to it is at or above that frontier, so a coarse
//    output could keep the lost file pinned, and the lost file would not be
//    removed.
// 4. Remove. The census (db/blob/blob_reference_census.h) tracks which SSTs
//    use each blob file. When it finds no SST that uses the lost file,
//    indirect blob GC deletes the file without reading it. The loss record
//    goes away with the file record.
//
// Safety: an open with best_efforts_blob_db_repair fails instead of falling
// back to an older point in the MANIFEST, or to an older MANIFEST file, when
// that would drop files whose values are still on disk in a relocation file
// (VersionEditHandlerPointInTime::CheckRepairFallbackAgainstGc() and
// VersionSet::TryRecover()).
//
// Why a relocation file is marked lost and not dropped: the base dropped-file
// record (kDroppedBlobFile) removes a file from the Version, and the base
// dropped list keeps the file only while some SST has an
// oldest_blob_file_number at or below it. Flattened and routed references do
// not set oldest_blob_file_number, so the dropped list would forget a
// relocation file while SSTs still use it. Also, a route target must be a
// relocation file in the Version. So a lost relocation file stays in the
// Version until indirect blob GC removes it.

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

// The format version that both MANIFEST format fences store: the per-CF fence
// (kBlobIndirectionFormatVersion) and the DB-global fence
// (kDbBlobIndirectionFormatVersion). A fence is not set when its value is 0, so
// VersionEdit decoding returns Corruption for a fence record that holds 0. It
// returns NotSupported for any other value that is not this one, so a binary
// does not open a DB that uses a format it does not know.
inline constexpr uint32_t kBlobIndirectionManifestFormatVersion = 2;

class BlobFileMetaData;

// The largest relocation file number and route file number. It equals
// kFileNumberMask (2^62 - 1), the limit of all table and blob file numbers,
// because relocation files take their numbers from the same counter; a
// static_assert in version_edit.cc checks this. BlobFileTable (below) also
// relies on this limit: it stores a route as target << 1 | 1, so bit 0 marks a
// route and the other 63 bits hold the target. For example, a route to file 20
// is stored as 41, and 41 >> 1 gives 20 back. Any number up to 2^62 - 1 fits,
// with one bit to spare. See "Tagged value" in the BlobFileTable comment for
// the full encoding.
constexpr uint64_t kMaxBlobRelocationFileNumber = (uint64_t{1} << 62) - 1;
class CacheReservationManager;

// The MANIFEST record that adds one relocation file to a column family. It
// keeps only numbers: file number, file size, checksum, tail size, total
// entries, total record bytes and the directory digest. The directory itself
// (see blob_relocation_file.h) stays in the file; a reader checks the file's
// directory against the digest kept here. The tail size is the byte size of the
// table metadata at the end of the file (index, properties, metaindex and
// footer); an open uses it to read that range. The record has no version or
// column family field: the format fence gives the version, and the VersionEdit
// names the column family.
struct BlobPhysicalFile;

class BlobRelocationFileAddition {
 public:
  BlobRelocationFileAddition() = default;
  BlobRelocationFileAddition(const BlobRelocationFileAddition&) = default;
  BlobRelocationFileAddition& operator=(const BlobRelocationFileAddition&) =
      default;
  BlobRelocationFileAddition(BlobRelocationFileAddition&&) noexcept = default;
  BlobRelocationFileAddition& operator=(BlobRelocationFileAddition&&) noexcept =
      default;
  ~BlobRelocationFileAddition() { directory_status_.PermitUncheckedError(); }

  // From a finished file: takes the totals and digest from its encoded
  // directory. A directory that does not decode makes Validate() fail.
  BlobRelocationFileAddition(uint64_t file_number, uint64_t file_size,
                             std::string checksum_method,
                             std::string checksum_value,
                             const Slice& encoded_directory,
                             uint64_t tail_size);
  // From stored numbers.
  BlobRelocationFileAddition(uint64_t file_number, uint64_t file_size,
                             std::string checksum_method,
                             std::string checksum_value, uint64_t total_entries,
                             uint64_t total_record_bytes,
                             uint32_t directory_digest, uint64_t tail_size);

  uint64_t GetFileNumber() const { return file_number_; }
  uint64_t GetFileSize() const { return file_size_; }
  uint64_t GetTailSize() const { return tail_size_; }
  const std::string& GetChecksumMethod() const { return checksum_method_; }
  const std::string& GetChecksumValue() const { return checksum_value_; }
  uint64_t GetTotalEntries() const { return total_entries_; }
  uint64_t GetTotalRecordBytes() const { return total_record_bytes_; }
  uint32_t GetDirectoryDigest() const { return directory_digest_; }

  // The physical file that a reader opens for this relocation file. The
  // result points to this addition's checksum strings, so this addition must
  // outlive it.
  BlobPhysicalFile ToPhysicalFile() const;

  Status Validate() const;
  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);
  std::string DebugString() const;

 private:
  uint64_t file_number_ = kInvalidBlobFileNumber;
  uint64_t file_size_ = 0;
  uint64_t tail_size_ = 0;
  std::string checksum_method_;
  std::string checksum_value_;
  uint64_t total_entries_ = 0;
  uint64_t total_record_bytes_ = 0;
  uint32_t directory_digest_ = 0;
  // Set when the directory given to the constructor did not decode.
  Status directory_status_;
};

bool operator==(const BlobRelocationFileAddition& lhs,
                const BlobRelocationFileAddition& rhs);
bool operator!=(const BlobRelocationFileAddition& lhs,
                const BlobRelocationFileAddition& rhs);

// The in-memory metadata of one live relocation file. Versions share it through
// shared_ptr and never change it. When the last holder drops it, the deleter
// given to Create() runs: VersionBuilder's deleter marks the file obsolete, so
// the file can then be deleted from disk.
class BlobRelocationFileMetaData {
 public:
  template <typename Deleter>
  static std::shared_ptr<BlobRelocationFileMetaData> Create(
      BlobRelocationFileAddition addition, Deleter deleter) {
    return std::shared_ptr<BlobRelocationFileMetaData>(
        new BlobRelocationFileMetaData(std::move(addition)),
        std::move(deleter));
  }

  static std::shared_ptr<BlobRelocationFileMetaData> Create(
      BlobRelocationFileAddition addition) {
    return std::shared_ptr<BlobRelocationFileMetaData>(
        new BlobRelocationFileMetaData(std::move(addition)));
  }

  BlobRelocationFileMetaData(const BlobRelocationFileMetaData&) = delete;
  BlobRelocationFileMetaData& operator=(const BlobRelocationFileMetaData&) =
      delete;
  BlobRelocationFileMetaData(BlobRelocationFileMetaData&&) = delete;
  BlobRelocationFileMetaData& operator=(BlobRelocationFileMetaData&&) = delete;
  ~BlobRelocationFileMetaData() = default;

  uint64_t GetFileNumber() const { return addition_.GetFileNumber(); }
  uint64_t GetFileSize() const { return addition_.GetFileSize(); }
  uint64_t GetTailSize() const { return addition_.GetTailSize(); }
  const std::string& GetChecksumMethod() const {
    return addition_.GetChecksumMethod();
  }
  const std::string& GetChecksumValue() const {
    return addition_.GetChecksumValue();
  }
  uint64_t GetTotalEntries() const { return addition_.GetTotalEntries(); }
  uint64_t GetTotalRecordBytes() const {
    return addition_.GetTotalRecordBytes();
  }
  uint32_t GetDirectoryDigest() const { return addition_.GetDirectoryDigest(); }
  const BlobRelocationFileAddition& GetAddition() const { return addition_; }

  // An accepted loss is recorded per Version, not on the physical file: older
  // Versions without the loss can still read the file. This returns a copy of
  // `backing` that is marked lost. The copy keeps `backing` alive, so the file
  // still has exactly one owner object, whose release makes the file obsolete.
  // A Version with the loss never reads the file, even if a copy of the file
  // appears on disk again later.
  static std::shared_ptr<BlobRelocationFileMetaData> CreateMarkedLost(
      std::shared_ptr<BlobRelocationFileMetaData> backing) {
    std::shared_ptr<BlobRelocationFileMetaData> marked =
        Create(backing->GetAddition());
    marked->lost_ = true;
    marked->backing_metadata_ = std::move(backing);
    return marked;
  }
  bool IsMarkedLost() const { return lost_; }

 private:
  explicit BlobRelocationFileMetaData(BlobRelocationFileAddition addition)
      : addition_(std::move(addition)) {}

  BlobRelocationFileAddition addition_;
  std::shared_ptr<BlobRelocationFileMetaData> backing_metadata_;
  bool lost_ = false;
};

// A route: sends reads of an old file number (a reference's generation number)
// to the relocation file that holds its live values now. The MANIFEST stores
// it, and the Version owns it. Validate() checks only the record itself;
// VersionBuilder checks it against the Version (see the top of this file).
class BlobRelocationRoute {
 public:
  BlobRelocationRoute() = default;
  BlobRelocationRoute(uint64_t old_file_number, uint64_t target_file_number)
      : old_file_number_(old_file_number),
        target_file_number_(target_file_number) {}

  // The old file number.
  uint64_t GetOldFileNumber() const { return old_file_number_; }
  // The target: a live relocation file.
  uint64_t GetTargetFileNumber() const { return target_file_number_; }

  Status Validate() const;
  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);
  std::string DebugString() const;

 private:
  uint64_t old_file_number_ = kInvalidBlobFileNumber;
  uint64_t target_file_number_ = kInvalidBlobFileNumber;
};

// The MANIFEST record that deletes a route. It holds only the old file number.
class BlobRelocationRouteDeletion {
 public:
  BlobRelocationRouteDeletion() = default;
  explicit BlobRelocationRouteDeletion(uint64_t old_file_number)
      : old_file_number_(old_file_number) {}

  uint64_t GetOldFileNumber() const { return old_file_number_; }

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

 private:
  uint64_t old_file_number_ = kInvalidBlobFileNumber;
};

// The BlobFileTable of one Version of a column family that has used blob
// indirection: its format fence is set (the fence stays after
// enable_blob_indirection is turned off, because feature data may remain). The
// read path and VersionBuilder use it to look up a file number. It maps a file
// number to one of:
//   - a live blob file,
//   - a live relocation file, or
//   - a route (old file -> target).
// The three key sets never overlap. File number 0 (kInvalidBlobFileNumber) is
// never a key. A read of a reference looks up its generation number once, plus
// once more for the target when it finds a route.
//
// Shards: the table is an array of shards. The shard of a key is fmix64(key) &
// (shard count - 1). The shard count is a power of two, chosen so that a shard
// holds about kTargetEntriesPerShard (32) entries. A shard with 32 keys of 4
// bytes has 128 bytes of keys (about two cache lines). A lookup scans these
// keys. The SSE2 scan stops at the first match; the plain scan reads all keys.
// The value 32 is an estimate, not a measured best. Apply() picks a new shard
// count when the average shard holds more than 2 * kTargetEntriesPerShard (64)
// entries, or fewer than kTargetEntriesPerShard / 4 (8) entries while there is
// more than one shard. The new count is the smallest power of two that gives at
// most 32 entries per shard on average. The gap between 8 and 64 keeps a table
// from resizing again after a few edits.
//
// Shard layout (one allocation per shard):
//
//   +--------+-------------------+-----------------+-----------------+
//   | Shard  | keys              | values          | route counts    |
//   | header | (padded count)    | (count)         | (count)         |
//   +--------+-------------------+-----------------+-----------------+
//
//   - keys: sorted file numbers. They are 32-bit when every key of the shard
//     fits in 32 bits, else 64-bit. They are padded with 0 to a multiple of 4,
//     so the SSE2 scan can always load whole 16-byte groups; 0 never matches a
//     key.
//   - values: one 64-bit tagged value per key (see below).
//   - route counts: one 32-bit count per key: the number of routes to this
//     relocation file, 0 for other entries. VersionBuilder uses it to reject
//     the deletion of a file that a route still targets, without a scan of all
//     routes.
//
// Tagged value: metadata pointers are 8-byte aligned, so the low 3 bits are
// free for tags.
//   - route: target << 1 | 1 (bit 0 set). File numbers are at most
//     kMaxBlobRelocationFileNumber (2^62 - 1), so the target always fits.
//   - live blob file: the BlobFileMetaData pointer, low 3 bits 0.
//   - live relocation file: the BlobRelocationFileMetaData pointer, with bit 1
//     (value 2) set. Bit 2 (value 4) marks a relocation file that a loss
//     record (kBlobPhysicalFileLoss) names.
//
// Ownership: the table does not own the metadata. The Version's blob file and
// relocation file vectors own it, and they always hold the same files as the
// table.
//
// Copy-on-write: a table is immutable once its Version is published, so
// Versions share memory at two levels.
//   1. A new Version whose edits change no table entry shares the whole
//      BlobRelocationState of its base Version (one shared_ptr copy; see
//      VersionStorageInfo::MutableBlobRelocationState()). For example, a
//      compaction that writes no blob file and drops no blob reference. A
//      dropped reference adds garbage to a blob file, which gives the file
//      new BlobFileMetaData and so changes its table entry.
//   2. Otherwise the new Version copies the BlobRelocationState. The copy of
//      the table copies only the top array of shard pointers and adds one
//      reference to each shard. Apply() then builds a new shard for each shard
//      that the edit touches. The new table drops its reference to the old
//      shard, but other tables keep using it. A shard is freed when its last
//      table drops it.
//
// Example: a flush adds blob file 205 to Version 100, which makes Version 101.
// The table has 4 shards, and file 205 falls in shard 2. All numbers are
// example values.
//
//   Version 100 table             Version 101 table
//   top array                     top array (a copy)
//   +-----+                       +-----+
//   | [0] |-----> shard A <-------| [0] |
//   | [1] |-----> shard B <-------| [1] |
//   | [2] |-----> shard C         | [2] |-----> shard C2 (C plus file 205)
//   | [3] |-----> shard D <-------| [3] |
//   +-----+                       +-----+
//
//   References: A, B and D have 2 each. C and C2 have 1 each. When Version 100
//   is freed, A, B and D go down to 1, and C is freed.
//
// Memory per shard (from the struct sizes; allocator overhead is not
// counted):
//   - Shard header: 16 bytes (refs, count, padded, wide; four 32-bit fields).
//   - Each entry: a 4-byte key (8 bytes when a key of the shard does not fit
//     in 32 bits), an 8-byte tagged value and a 4-byte route count, so 16
//     bytes. The keys are padded to a multiple of 4.
//   - A full shard of 32 entries: 16 + 32 * 16 = 528 bytes.
//   - The top array: 8 bytes per shard.
//
// Memory example. The numbers are estimates computed from the sizes above,
// not measurements. A column family has 10,000 entries (live blob files,
// relocation files and routes together), in a table built in one step (for
// example, the table of the first fenced Version):
//   - The shard count is 512, the smallest power of two that gives at most 32
//     entries per shard on average. A shard holds about 20 entries and takes
//     about 330 bytes.
//   - A table that grows edit by edit is rebuilt only when the average shard
//     holds more than 64 entries, so at 10,000 entries it can have 256 shards
//     of about 39 entries (about 640 bytes each). Its total size is about the
//     same.
//   - One full table takes about 175 KB: about 171 KB of shards and a 4 KB
//     top array.
//   - A new Version of case 1 above adds no table memory.
//   - A new Version of case 2 adds the 4 KB top array plus about 330 bytes for
//     each touched shard. A flush that adds one blob file touches one shard
//     (about 4.3 KB). An indirect blob GC edit that adds relocation file 20
//     and turns blob file 12 into route 12 -> 20 touches at most 2 shards
//     (about 4.7 KB). It also copies the relocation file vector (16 bytes per
//     relocation file).
//   - Ten live Versions that each differ from the previous one by such an
//     edit take about 175 KB + 10 * 4.7 KB = 222 KB. Ten full copies would
//     take about 1.75 MB.
//
// Worst case: when Apply() picks a new shard count, or one edit touches every
// shard, the new Version builds all shards, as a full copy would. A resize
// happens only when the average shard holds more than 64 or fewer than 8
// entries, so it is rare (see "Shards" above).
class BlobFileTable {
 public:
  static constexpr size_t kTargetEntriesPerShard = 32;

  // One change for Apply(): set the entry of `file_number` to the tagged
  // `value` and the route count `count`. A `value` of 0 erases the entry.
  struct Op {
    uint64_t file_number = kInvalidBlobFileNumber;
    uint64_t value = 0;
    uint32_t count = 0;
  };

  static uint64_t BlobFileValue(const BlobFileMetaData* file) {
    const uint64_t value = reinterpret_cast<uint64_t>(file);
    assert((value & 7) == 0);
    return value;
  }
  static uint64_t RelocationFileValue(const BlobRelocationFileMetaData* file,
                                      bool lost) {
    const uint64_t value = reinterpret_cast<uint64_t>(file);
    assert((value & 7) == 0);
    return value | 2 | (lost ? 4 : 0);
  }
  static uint64_t RouteValue(uint64_t target) { return target << 1 | 1; }
  static bool IsRoute(uint64_t value) { return (value & 1) != 0; }
  static uint64_t RouteTarget(uint64_t value) { return value >> 1; }
  static bool IsRelocationFile(uint64_t value) {
    return value != 0 && !IsRoute(value) && (value & 2) != 0;
  }
  static bool IsBlobFile(uint64_t value) {
    return value != 0 && !IsRoute(value) && (value & 2) == 0;
  }
  static bool IsLost(uint64_t value) {
    return IsRelocationFile(value) && (value & 4) != 0;
  }
  static const BlobFileMetaData* AsBlobFile(uint64_t value) {
    assert(IsBlobFile(value));
    assert((value & 7) == 0);
    return reinterpret_cast<const BlobFileMetaData*>(value & ~uint64_t{7});
  }
  static const BlobRelocationFileMetaData* AsRelocationFile(uint64_t value) {
    assert(IsRelocationFile(value));
    return reinterpret_cast<const BlobRelocationFileMetaData*>(value &
                                                               ~uint64_t{7});
  }

  BlobFileTable() = default;
  BlobFileTable(const BlobFileTable& other);
  BlobFileTable& operator=(const BlobFileTable& other);
  ~BlobFileTable();

  // The tagged value of `file_number`, or 0. `count`, when not null, gets the
  // entry's route count (0 when there is no entry).
  uint64_t Find(uint64_t file_number, uint32_t* count = nullptr) const;

  // Applies `ops` in order. It builds new copies of the touched shards, or of
  // all shards when the shard count changes. Other copies of the table keep the
  // old shards.
  void Apply(const std::vector<Op>& ops);
  size_t size() const { return size_; }
  // Bytes of the top array and the shards, shared ones included.
  size_t ApproximateMemoryUsage() const;

  // Charges the table memory to `manager`, the cache reservation for file
  // metadata: the top array of every table copy, and each shard once, when it
  // is built. The charge is best effort: a failed charge never fails an edit.
  // The call has an effect only on a table without shards, because a table with
  // shards may share them with tables of another manager. Copies of the table
  // share the manager.
  void SetCacheReservationManager(
      std::shared_ptr<CacheReservationManager> manager);
  // Visits every (file number, value, count), in no particular order.
  template <typename Fn>
  void ForEach(Fn&& fn) const {
    for (const Shard* shard : shards_) {
      if (shard != nullptr) {
        for (uint32_t i = 0; i < shard->count; ++i) {
          fn(shard->Key(i), shard->values()[i], shard->counts()[i]);
        }
      }
    }
  }

  // Tests choose the key scan; production uses SSE2 when it is available.
  static void TEST_UsePlainScan(bool plain);

 private:
  struct Shard;
  struct Entry {
    uint64_t key;
    uint64_t value;
    uint32_t count;
  };
  static Shard* MakeShard(const std::vector<Entry>& run);
  static void Ref(Shard* shard);

  // Returns the bytes freed: the shard's size if this was the last reference.
  static size_t Unref(Shard* shard);
  void Rebuild(size_t shard_count, size_t* added, size_t* freed);
  void Charge(size_t added, size_t freed) const;
  size_t TopBytes() const { return shards_.capacity() * sizeof(Shard*); }

  std::vector<Shard*> shards_;
  size_t size_ = 0;
  std::shared_ptr<CacheReservationManager> charge_;

  struct Shard {
    std::atomic<uint32_t> refs;
    uint32_t count;
    uint32_t padded;
    uint32_t wide;
    const uint32_t* keys32() const {
      return reinterpret_cast<const uint32_t*>(this + 1);
    }
    uint32_t* keys32() { return reinterpret_cast<uint32_t*>(this + 1); }
    const uint64_t* keys64() const {
      return reinterpret_cast<const uint64_t*>(this + 1);
    }
    uint64_t* keys64() { return reinterpret_cast<uint64_t*>(this + 1); }
    size_t KeyBytes() const { return size_t{padded} * (wide ? 8 : 4); }
    const uint64_t* values() const {
      return reinterpret_cast<const uint64_t*>(
          reinterpret_cast<const char*>(this + 1) + KeyBytes());
    }
    uint64_t* values() {
      return reinterpret_cast<uint64_t*>(reinterpret_cast<char*>(this + 1) +
                                         KeyBytes());
    }
    const uint32_t* counts() const {
      return reinterpret_cast<const uint32_t*>(values() + count);
    }
    uint32_t* counts() { return reinterpret_cast<uint32_t*>(values() + count); }
    uint64_t Key(uint32_t i) const { return wide ? keys64()[i] : keys32()[i]; }
    size_t Bytes() const {
      return sizeof(Shard) + KeyBytes() +
             size_t{count} * (sizeof(uint64_t) + sizeof(uint32_t));
    }
  };
};

// The relocation state of one Version of a fenced column family: its live
// relocation files, its routes and the file table (BlobFileTable), which also
// lists the live blob files. The read path and VersionBuilder use the table for
// lookups. A legacy column family has no relocation state (VersionStorageInfo
// holds a null pointer). Routes live only in the table. Relocation files are
// also kept in a vector sorted by file number, for iteration. Deleted files and
// routes are not kept (no deletion history).
//
// Ownership: while a VersionBuilder builds a new Version, it owns the state
// alone and changes it. A published Version exposes the state only through
// const handles.
class BlobRelocationState {
 public:
  using Files = std::vector<std::shared_ptr<BlobRelocationFileMetaData>>;

  BlobRelocationState() = default;
  BlobRelocationState(const BlobRelocationState& base)
      : files_(base.files_),
        table_(base.table_),
        route_count_(base.route_count_),
        physical_file_bytes_(base.physical_file_bytes_),
        record_bytes_(base.record_bytes_),
        lost_file_count_(base.lost_file_count_),
        digest_high_(base.digest_high_),
        digest_low_(base.digest_low_),
        aggregate_overflow_(base.aggregate_overflow_),
        validated_(base.validated()) {
    assert(base.pending_.empty());
  }

  // Sorted by file number.
  const Files& files() const;
  const BlobFileTable& table() const {
    assert(pending_.empty());
    return table_;
  }
  size_t route_count() const { return route_count_; }
  // Visits every route, in no particular order.
  template <typename Fn>
  void ForEachRoute(Fn&& fn) const {
    if (route_count_ == 0) {
      return;
    }
    table().ForEach([&](uint64_t file_number, uint64_t value, uint32_t) {
      if (BlobFileTable::IsRoute(value)) {
        fn(BlobRelocationRoute(file_number, BlobFileTable::RouteTarget(value)));
      }
    });
  }

  // The total file size and the total record bytes of the live relocation
  // files. SetFile() and DeleteFile() keep them up to date.
  uint64_t PhysicalFileBytes() const { return physical_file_bytes_; }
  uint64_t RecordBytes() const { return record_bytes_; }
  // True once one of the two totals above would overflow. The totals then stop
  // changing, and Version validation fails with Corruption.
  bool AggregateOverflow() const { return aggregate_overflow_; }
  // True when at least one relocation file is marked lost (an accepted loss).
  bool HasLostFiles() const { return lost_file_count_ != 0; }
  size_t LostFileCount() const { return lost_file_count_; }
  std::shared_ptr<BlobRelocationFileMetaData> FindFile(
      uint64_t file_number) const;
  // Like FindFile(), without copying the shared pointer (read path).
  const BlobRelocationFileMetaData* PeekFile(uint64_t file_number) const;

  // The target of the route of `old_file_number`, or kInvalidBlobFileNumber.
  uint64_t FindRouteTarget(uint64_t old_file_number) const;

  // The number of routes to relocation file `file_number`.
  size_t RouteCount(uint64_t file_number) const;

  // Mutators of a state that is not published yet. They buffer the table
  // changes in `pending_`. Seal() applies the buffer to the table; it must run
  // before table() is read or the state is published.
  void SetFile(std::shared_ptr<BlobRelocationFileMetaData> file);
  void DeleteFile(uint64_t file_number);
  void SetRoute(const BlobRelocationRoute& route);
  void DeleteRoute(uint64_t old_file_number);

  // Live blob files of a fenced column family, for lookups. The Version's blob
  // file vector owns them.
  void SetBlobFile(const BlobFileMetaData* file);
  void DeleteBlobFile(uint64_t file_number);
  void Seal();

  // Drops the relocation file metadata of a state that no Version uses any
  // more. The metadata deleters use the column family, so the caller runs this
  // under the DB mutex. The rest (the file table) can then be freed without the
  // mutex. Only destruction may follow.
  void ReleaseFiles() { files_.reset(); }
  void SetCacheReservationManager(
      std::shared_ptr<CacheReservationManager> manager) {
    assert(pending_.empty());
    table_.SetCacheReservationManager(std::move(manager));
  }
  bool sealed() const { return pending_.empty(); }
  // Computes a 128-bit fingerprint of the relocation files and routes in O(1).
  // SetFile(), DeleteFile(), SetRoute() and DeleteRoute() keep two running
  // digest sums up to date, so the result does not depend on the order of the
  // changes. The fingerprint also covers `format_version` and the file and
  // route counts.
  void ComputeFingerprint(uint32_t format_version, uint64_t* high,
                          uint64_t* low) const;
  // Returns an estimate, meant as an upper bound, of the memory that this state
  // keeps alive beyond `base` and that no other reservation charges.
  // `changed_files` lists the relocation files that can differ between the two
  // states. The file table is not counted (the reason is in
  // IncrementalRetentionBytes() in blob_relocation_manifest.cc).
  size_t IncrementalRetentionBytes(
      const BlobRelocationState& base,
      const std::vector<uint64_t>& changed_files) const;

  // True when this state was fully checked, or when VersionBuilder checked
  // every change on top of a checked base
  // (VersionStorageInfo::MarkBlobRelocationChangesValidated()). Release builds
  // then skip the full scan in
  // VersionStorageInfo::ValidateBlobRelocationState(). A copy keeps the flag,
  // and any staged change (Stage()) clears it. It is atomic because many
  // threads read a published Version.
  bool validated() const { return validated_.load(std::memory_order_relaxed); }
  void MarkValidated() const {
    validated_.store(true, std::memory_order_relaxed);
  }

 private:
  Files& MutableFiles();

  // The tagged value of `file_number` and its route count, pending edits
  // included.
  uint64_t Lookup(uint64_t file_number, uint64_t* count = nullptr) const;
  void Stage(uint64_t file_number, uint64_t value, uint64_t count);
  void AdjustRouteCount(uint64_t target, int64_t delta);

  std::shared_ptr<const Files> files_;
  BlobFileTable table_;
  // Table changes not applied yet (see Seal()): file number -> (new tagged
  // value, route count). A value of 0 erases the entry. The count is 64-bit
  // here, while the table keeps 32 bits: a build applies route changes one by
  // one, so a target's count can for a moment be larger than its final count.
  std::map<uint64_t, std::pair<uint64_t, uint64_t>> pending_;
  size_t route_count_ = 0;
  uint64_t physical_file_bytes_ = 0;
  uint64_t record_bytes_ = 0;
  size_t lost_file_count_ = 0;
  // Sums of the digests of every relocation file and route.
  uint64_t digest_high_ = 0;
  uint64_t digest_low_ = 0;
  bool aggregate_overflow_ = false;
  mutable std::atomic<bool> validated_{false};
};

bool operator==(const BlobRelocationRoute& lhs, const BlobRelocationRoute& rhs);
bool operator!=(const BlobRelocationRoute& lhs, const BlobRelocationRoute& rhs);

}  // namespace ROCKSDB_NAMESPACE
