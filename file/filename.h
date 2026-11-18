//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// File names used by DB code

#pragma once
#include <stdint.h>

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/wal_iterator.h"

namespace ROCKSDB_NAMESPACE {

class Env;
struct ColumnFamilyDescriptor;
class Directory;
class SystemClock;
class WritableFileWriter;

#ifdef OS_WIN
constexpr char kFilePathSeparator = '\\';
#else
constexpr char kFilePathSeparator = '/';
#endif

// Return the name of the log file with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
std::string LogFileName(const std::string& dbname, uint64_t number);

std::string LogFileName(uint64_t number);

std::string BlobFileName(uint64_t number);

std::string BlobFileName(const std::string& bdirname, uint64_t number);

std::string BlobFileName(const std::string& dbname, const std::string& blob_dir,
                         uint64_t number);

std::string ArchivalDirectory(const std::string& dbname);

//  Return the name of the archived log file with the specified number
//  in the db named by "dbname". The result will be prefixed with "dbname".
std::string ArchivedLogFileName(const std::string& dbname, uint64_t num);

std::string MakeTableFileName(const std::string& name, uint64_t number);

std::string MakeTableFileName(uint64_t number);

// Return the name of sstable with LevelDB suffix
// created from RocksDB sstable suffixed name
std::string Rocks2LevelTableFileName(const std::string& fullname);

// the reverse function of MakeTableFileName
// TODO(yhchiang): could merge this function with ParseFileName()
uint64_t TableFileNameToNumber(const std::string& name);

// Return the name of the sstable with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
std::string TableFileName(const std::vector<DbPath>& db_paths, uint64_t number,
                          uint32_t path_id);

// Sufficient buffer size for FormatFileNumber.
const size_t kFormatFileNumberBufSize = 38;

void FormatFileNumber(uint64_t number, uint32_t path_id, char* out_buf,
                      size_t out_buf_size);

// Return the name of the descriptor file for the db named by
// "dbname" and the specified incarnation number.  The result will be
// prefixed with "dbname".
std::string DescriptorFileName(const std::string& dbname, uint64_t number);

std::string DescriptorFileName(uint64_t number);

extern const std::string kCurrentFileName;  // = "CURRENT"

// Return the name of the current file.  This file contains the name
// of the current manifest file.  The result will be prefixed with
// "dbname".
std::string CurrentFileName(const std::string& dbname);

// Path of the repair catalog (REPAIR-IN-PROGRESS) that feature RepairDB
// writes (see RepairCatalogRecordType). An open reads and resolves the catalog
// before it looks for CURRENT or a MANIFEST (see CheckRepairCatalogForOpen()).
// So while the catalog exists, an unfinished repair does not look like a new
// DB, and best-efforts recovery does not pick a MANIFEST that the repair has
// not published. RepairDB deletes the catalog when it finishes. If a catalog is
// left behind, a later RepairDB run or a read-write open resolves and deletes
// it. DestroyDB() also deletes it. Read-only and secondary opens only check it.
std::string RepairInProgressFileName(const std::string& dbname);
// True for a file name RepairDB uses while it runs (its staged MANIFEST and
// reference scratch files): REPAIR-<number>-...dbtmp. Such a file is not
// needed once no repair catalog exists.
bool IsRepairStagingFileName(const std::string& file_name);

// The repair catalog (REPAIR-IN-PROGRESS) is a private log that feature
// RepairDB writes, so that a crash or a retry can tell which files belong to
// the attempt. Each record is one checksummed log record with an ordinal. The
// ordinals are 1, 2, 3 and so on, with no gap. Record types:
// - kHeader: the owner session ID, and the original CURRENT content or the
//   fact that CURRENT was absent.
// - kInput: an original file. kIntent: an output file the attempt may create.
// - kSeal: the final size and CRC32C checksum of an output file.
// - kArchive: an input file that the attempt moves to the lost/ directory.
// - kCoverage: the WAL number below which every WAL is converted, and the
//   last sequence so far (see the table at RepairCatalogRecord).
// - kCandidate: the sealed new MANIFEST that CURRENT will name.
// - kPolicy: per CF, its name, comparator, timestamp size and original
//   floor.
// - kSyncMarker: written before each catalog sync (see below).
enum class RepairCatalogRecordType : uint32_t {
  kHeader,
  kInput,
  kIntent,
  kSeal,
  kArchive,
  kCoverage,
  kCandidate,
  kPolicy,
  // Written just before each catalog sync: every record before it is
  // durable. Damage after the last marker is a torn unsynced tail.
  kSyncMarker
};

// One catalog record. All record types share these fields, and each type uses
// them as the table below shows. A field that a type does not list is 0 or
// empty.
//
//   type         field          meaning
//   -----------  -------------  ----------------------------------------------
//   kHeader      path           content of the original CURRENT, or empty
//                identity       session ID of the repair attempt (the owner)
//                number         next file number when the attempt started
//                column_family  kOriginalCurrentAbsent (1) if the original
//                               CURRENT was absent, else 0
//                source_wal     kHeaderTwoWriteQueues (bit 0),
//                               kHeaderUnorderedWrite (bit 1),
//                               kHeaderCreateUnknownCfs (bit 2)
//   kInput       path           the original file
//                number         file number parsed from the file name
//                size           file size when repair recorded the input
//                source_wal     the WAL number, for a WAL input only
//                identity       kOriginalManifest for the MANIFEST that the
//                               original CURRENT names (checksum is set), or
//                               kOriginalManifestAbsent if that MANIFEST is
//                               missing
//   kIntent      path           an output that the attempt may create
//                number         file number of the output
//                column_family  CF ID of an SST or blob file, else 0
//                source_wal     the WAL that an SST is converted from, else 0
//   kSeal        (intent fields) plus:
//                size           final size of the output
//                checksum       CRC32C checksum of the output
//                identity       kUnusedOutput when the new MANIFEST does not
//                               use the output (the seal is written again)
//   kArchive     path           an input to move to the lost/ directory
//                identity       session ID of the attempt
//                size           size of the input
//                checksum       CRC32C checksum of the input
//   kCoverage    number         certified floor: every WAL below it is
//                               converted
//                size           last sequence so far: the original
//                               MANIFEST's last sequence, raised by each
//                               converted WAL
//   kCandidate   (seal fields of the new MANIFEST) plus:
//                source_wal     the certified floor
//   kPolicy      column_family  CF ID
//                path           CF name
//                identity       comparator name
//                number         timestamp size of the comparator
//                source_wal     the original floor (see "WAL coverage" in
//                               db/repair.cc)
//   kSyncMarker  (no fields)
struct RepairCatalogRecord {
  // Values of `identity` that have a fixed meaning (see the table above).
  static constexpr char kOriginalManifest[] = "original-manifest";
  static constexpr char kOriginalManifestAbsent[] = "original-manifest-absent";
  static constexpr char kUnusedOutput[] = "unused";
  // Value of `column_family` in a kHeader record when the original CURRENT was
  // absent. It is 0 when CURRENT was present.
  static constexpr uint32_t kOriginalCurrentAbsent = 1;
  // Bits of `source_wal` in a kHeader record. They record the options that the
  // repair attempt ran with.
  static constexpr uint64_t kHeaderTwoWriteQueues = uint64_t{1} << 0;
  static constexpr uint64_t kHeaderUnorderedWrite = uint64_t{1} << 1;
  static constexpr uint64_t kHeaderCreateUnknownCfs = uint64_t{1} << 2;

  RepairCatalogRecordType type = RepairCatalogRecordType::kHeader;
  uint64_t ordinal = 0;
  std::string path;
  std::string identity;
  uint64_t number = 0;
  uint64_t size = 0;
  uint32_t checksum = 0;
  uint32_t column_family = 0;
  uint64_t source_wal = 0;
  void EncodeTo(std::string* output) const;
};

// The records of one catalog after ReadRepairCatalog(). Inputs, outputs (the
// latest intent or seal of each output) and archives are keyed by path, and
// policies by CF ID.
struct RepairCatalog {
  RepairCatalogRecord header;
  std::map<std::string, RepairCatalogRecord> inputs;
  std::map<std::string, RepairCatalogRecord> outputs;
  std::map<std::string, RepairCatalogRecord> archives;
  std::map<uint32_t, RepairCatalogRecord> policies;
  RepairCatalogRecord candidate;
  uint64_t coverage_floor = 0;
  uint64_t coverage_max_sequence = 0;
  uint64_t last_ordinal = 0;
};

// Reads REPAIR-IN-PROGRESS into `catalog`. An absent file is OK, and
// `*recognized` is false. `*recognized` is true when the first 128 bytes of the
// file contain the catalog magic prefix ("rocksdb.blob-gc.repair."). Then any
// error is about a real catalog, and callers must not ignore it. An existing
// file without the magic prefix returns Corruption with `*recognized` false.
// An I/O error before the magic check is returned as is, with `*recognized`
// false.
Status ReadRepairCatalog(Env* env, const std::string& dbname,
                         RepairCatalog* catalog, bool* recognized);

// Sets `*checksum` to the CRC32C of the first `prefix_size` bytes of `path`.
// A seal stores the size it saw, so a later check reads only those bytes.
Status GetRepairFileChecksum(Env* env, const std::string& path,
                             uint64_t prefix_size, uint32_t* checksum);
// Decides whether the repair attempt in `catalog` committed (CURRENT names
// its candidate MANIFEST) and finishes or rolls back its files. If CURRENT
// names neither the original nor the candidate MANIFEST, it returns
// Corruption and keeps both.
// - RepairDB retry (`repair_retry`, under the DB LOCK): rolls back an
//   uncommitted attempt, or finishes the cleanup of a committed one.
// - Read-write open: finishes the cleanup of a committed attempt before it
//   allocates file numbers, replays or purges files. An uncommitted attempt
//   fails the open with "retry RepairDB".
// - Read-only and secondary opens (`read_only`): check the seals of a
//   committed attempt and never delete or move files. An uncommitted attempt
//   also fails them.
// `seals_verified` is true only for the RepairDB call that sealed every
// output and archive itself, under the same DB LOCK. It skips the seal check
// of the kept outputs, the candidate and the archives. It still checks the
// seal of an unused output before it deletes it, and it reads the candidate
// MANIFEST to check the log floors and the last sequence.
Status ResolveRepairCatalog(Env* env, const std::string& dbname,
                            const RepairCatalog& catalog, bool read_only,
                            bool repair_retry, bool* committed,
                            bool seals_verified);
// Reads and resolves the repair catalog for an open. An absent catalog is OK.
// A recognized catalog is resolved now: if it is damaged, not committed or
// ambiguous, the open fails with that error. A file that cannot be opened,
// whose first bytes cannot be read, or that is not a recognized catalog is not
// an error by itself: it is returned in *unresolved_catalog. The caller fails
// the open with it only if the replayed MANIFEST shows blob indirection state,
// or, when there is nothing to replay, if HasBlobIndirectionFileEvidence()
// finds any.
Status CheckRepairCatalogForOpen(Env* env, const std::string& dbname,
                                 bool read_only, Status* unresolved_catalog);

// Persisted blob indirection evidence. Configuration is never evidence. Open
// and RepairDB use the same rules.
class VersionEdit;
struct FileMetaData;
bool IsBlobIndirectionManifestEvidence(const VersionEdit& edit);
// `prefix` is the first BlobLogHeader::kSize bytes of a blob file.
bool IsBlobIndirectionBlobFileEvidence(const Slice& prefix);
// `meta` holds the decoded descriptor and classification table properties.
bool IsBlobIndirectionTableEvidence(const FileMetaData& meta);

// Scans every MANIFEST in `dbname` and the metadata of every SST and blob file
// in the DB and CF paths for blob indirection evidence. Used only when there
// is no MANIFEST to replay. Unreadable files are skipped.
bool HasBlobIndirectionFileEvidence(
    Env* env, const std::string& dbname, const std::vector<DbPath>& db_paths,
    const std::vector<ColumnFamilyDescriptor>& cfs);

// Return the name of the lock file for the db named by
// "dbname".  The result will be prefixed with "dbname".
std::string LockFileName(const std::string& dbname);

// Return the name of a temporary file owned by the db named "dbname".
// The result will be prefixed with "dbname".
std::string TempFileName(const std::string& dbname, uint64_t number);

// A helper structure for prefix of info log names.
struct InfoLogPrefix {
  char buf[260];
  Slice prefix;
  // Prefix with DB absolute path encoded
  explicit InfoLogPrefix(bool has_log_dir, const std::string& db_absolute_path);
  // Default Prefix
  explicit InfoLogPrefix();
};

// Return the name of the info log file for "dbname".
std::string InfoLogFileName(const std::string& dbname,
                            const std::string& db_path = "",
                            const std::string& log_dir = "");

// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string& dbname, uint64_t ts,
                               const std::string& db_path = "",
                               const std::string& log_dir = "");

extern const std::string kOptionsFileNamePrefix;  // = "OPTIONS-"
extern const std::string
    kCompactionProgressFileNamePrefix;         // =
                                               // "COMPACTION_PROGRESS-"
extern const std::string kTempFileNameSuffix;  // = "dbtmp"

// Return a options file name given the "dbname" and file number.
// Format:  OPTIONS-[number].dbtmp
std::string OptionsFileName(const std::string& dbname, uint64_t file_num);
std::string OptionsFileName(uint64_t file_num);

// Return a temp options file name given the "dbname" and file number.
// Format:  OPTIONS-[number]
std::string TempOptionsFileName(const std::string& dbname, uint64_t file_num);

// Return a compaction progress file name given the timestamp.
// Format:  COMPACTION_PROGRESS-[timestamp]
std::string CompactionProgressFileName(const std::string& dbname,
                                       uint64_t timestamp);

// Return a temp compaction progress file name given the timestamp.
// Format:  COMPACTION_PROGRESS-[timestamp].dbtmp
std::string TempCompactionProgressFileName(const std::string& dbname,
                                           uint64_t timestamp);

// Return the DB session temporary directory.
std::string SessionTmpDir(const std::string& dbname);

// Return the path for the client-selected staging directory name. When the
// session temporary directory is not used, an already DB-rooted path is
// returned unchanged for compatibility with legacy clients.
std::string RemoteCompactionJobDir(const std::string& dbname,
                                   bool use_output_root_dir,
                                   const std::string& job_dir_name);

// Return the name to use for a metadatabase. The result will be prefixed with
// "dbname".
std::string MetaDatabaseName(const std::string& dbname, uint64_t number);

// Return the name of the Identity file which stores a unique number for the db
// that will get regenerated if the db loses all its data and is recreated fresh
// either from a backup-image or empty
std::string IdentityFileName(const std::string& dbname);

// If filename is a rocksdb file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.  If the
// filename was successfully parsed, returns true.  Else return false.
// info_log_name_prefix is the path of info logs.
bool ParseFileName(const std::string& filename, uint64_t* number,
                   const Slice& info_log_name_prefix, FileType* type,
                   WalFileType* log_type = nullptr);
// Same as previous function, but skip info log files.
bool ParseFileName(const std::string& filename, uint64_t* number,
                   FileType* type, WalFileType* log_type = nullptr);

// Make the CURRENT file point to the descriptor file with the
// specified number. On its success and when dir_contains_current_file is not
// nullptr, the function will fsync the directory containing the CURRENT file
// when
IOStatus SetCurrentFile(const WriteOptions& write_options, FileSystem* fs,
                        const std::string& dbname, uint64_t descriptor_number,
                        Temperature temp,
                        FSDirectory* dir_contains_current_file);

// Make the IDENTITY file for the db
Status SetIdentityFile(const WriteOptions& write_options, Env* env,
                       const std::string& dbname, Temperature temp,
                       const std::string& db_id = {});

// Sync manifest file `file`.
IOStatus SyncManifest(const ImmutableDBOptions* db_options,
                      const WriteOptions& write_options,
                      WritableFileWriter* file);

// Return list of file names of info logs in `file_names`.
// The list only contains file name. The parent directory name is stored
// in `parent_dir`.
// `db_log_dir` should be the one as in options.db_log_dir
Status GetInfoLogFiles(const std::shared_ptr<FileSystem>& fs,
                       const std::string& db_log_dir, const std::string& dbname,
                       std::string* parent_dir,
                       std::vector<std::string>* file_names);

std::string NormalizePath(const std::string& path);
}  // namespace ROCKSDB_NAMESPACE
