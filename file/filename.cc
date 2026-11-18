//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "file/filename.h"

#include <cctype>
#include <cinttypes>
#include <cstdio>
#include <limits>
#include <set>
#include <vector>

#include "db/blob/blob_log_format.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/blob/blob_relocation_file.h"
#include "db/builder.h"
#include "db/log_reader.h"
#include "db/version_edit.h"
#include "file/file_util.h"
#include "file/random_access_file_reader.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/file_system.h"
#include "table/meta_blocks.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

const std::string kCurrentFileName = "CURRENT";
const std::string kOptionsFileNamePrefix = "OPTIONS-";
const std::string kCompactionProgressFileNamePrefix = "COMPACTION_PROGRESS-";
const std::string kTempFileNameSuffix = "dbtmp";
static constexpr char kRepairInProgressFileName[] = "REPAIR-IN-PROGRESS";

std::string RepairInProgressFileName(const std::string& dbname) {
  return dbname + "/" + kRepairInProgressFileName;
}

bool IsRepairStagingFileName(const std::string& file_name) {
  static constexpr char kPrefix[] = "REPAIR-";
  static constexpr size_t kPrefixSize = sizeof(kPrefix) - 1;
  const Slice name(file_name);
  return name.starts_with(kPrefix) && name.size() > kPrefixSize &&
         isdigit(static_cast<unsigned char>(name[kPrefixSize])) &&
         name.ends_with("." + kTempFileNameSuffix);
}

namespace {
constexpr char kRepairCatalogMagic[] = "rocksdb.blob-gc.repair.v1";
class RepairCatalogReporter : public log::Reader::Reporter {
 public:
  // Some read paths return before they look at `status`. The destructor marks
  // it as checked, as ASSERT_STATUS_CHECKED builds require.
  ~RepairCatalogReporter() override { status.PermitUncheckedError(); }
  Status status;
  void Corruption(size_t, const Status& s, uint64_t) override {
    if (status.ok()) {
      status = s;
    }
  }
};
// Reads catalog records with the log format's framing and checksums, but
// without the log reader's unbounded record buffer and WAL decompression. One
// record is at most one log block (32 KB), but it can be split over the end of
// one block and the start of the next.
class RepairCatalogReader : public log::Reader {
 public:
  RepairCatalogReader(std::unique_ptr<SequentialFileReader> file,
                      RepairCatalogReporter* reporter)
      : log::Reader(nullptr, std::move(file), reporter, true, 0) {}

  Status ReadCatalogRecord(Slice* record, std::string* scratch, bool* found) {
    *found = false;
    scratch->clear();
    bool fragmented = false;
    for (;;) {
      Slice fragment;
      size_t dropped = 0;
      const uint8_t type = ReadPhysicalRecord(&fragment, &dropped, nullptr);
      if (type == kEof ||
          ((type == kBadHeader || type == kBadRecordLen) && IsEOF())) {
        // An incomplete last record is a torn write at the end of the file.
        // It allowed no action, so ignore it.
        return Status::OK();
      }
      if (type == log::kFullType && !fragmented) {
        *record = fragment;
        *found = true;
        return Status::OK();
      }
      if ((type == log::kFirstType && !fragmented) ||
          ((type == log::kMiddleType || type == log::kLastType) &&
           fragmented)) {
        if (fragment.size() > log::kBlockSize - scratch->size()) {
          return Status::Corruption(
              "Repair catalog logical record exceeds bound");
        }
        scratch->append(fragment.data(), fragment.size());
        fragmented = true;
        if (type == log::kLastType) {
          *record = *scratch;
          *found = true;
          return Status::OK();
        }
        continue;
      }
      return Status::Corruption("Invalid repair catalog physical record");
    }
  }
};
Status SyncRepairDirectory(Env* env, const std::string& path) {
  const size_t slash = path.find_last_of("/\\");
  std::unique_ptr<FSDirectory> directory;
  Status status = env->GetFileSystem()->NewDirectory(
      slash == std::string::npos ? "." : path.substr(0, slash), IOOptions(),
      &directory, nullptr);
  return status.ok() ? directory->FsyncWithDirOptions(IOOptions(), nullptr,
                                                      DirFsyncOptions(path))
                     : status;
}
Status VerifyRepairSeal(Env* env, const RepairCatalogRecord& record,
                        bool allow_prefix) {
  uint64_t size = 0;
  Status status = env->GetFileSize(record.path, &size);
  if (!status.ok()) {
    return status;
  }
  if (size < record.size || (!allow_prefix && size != record.size)) {
    return Status::Corruption("Repair output identity changed", record.path);
  }
  uint32_t checksum = 0;
  status = GetRepairFileChecksum(env, record.path, record.size, &checksum);
  return !status.ok() ? status
         : checksum == record.checksum
             ? Status::OK()
             : Status::Corruption("Repair output seal mismatch", record.path);
}
}  // namespace

void RepairCatalogRecord::EncodeTo(std::string* output) const {
  output->clear();
  PutLengthPrefixedSlice(output, kRepairCatalogMagic);
  PutVarint64(output, ordinal);
  PutVarint32(output, static_cast<uint32_t>(type));
  PutLengthPrefixedSlice(output, path);
  PutLengthPrefixedSlice(output, identity);
  PutVarint64(output, number);
  PutVarint64(output, size);
  PutFixed32(output, checksum);
  PutVarint32(output, column_family);
  PutVarint64(output, source_wal);
}

Status GetRepairFileChecksum(Env* env, const std::string& path,
                             uint64_t prefix_size, uint32_t* checksum) {
  std::unique_ptr<SequentialFileReader> file;
  Status status = SequentialFileReader::Create(
      env->GetFileSystem(), path, FileOptions(), &file, nullptr, nullptr);
  if (!status.ok()) {
    return status;
  }
  *checksum = 0;
  char scratch[log::kBlockSize];
  while (prefix_size != 0) {
    Slice bytes;
    status = file->Read(
        static_cast<size_t>(std::min<uint64_t>(prefix_size, sizeof(scratch))),
        &bytes, scratch, Env::IO_TOTAL);
    if (!status.ok()) {
      return status;
    }
    if (bytes.empty()) {
      return Status::Corruption("Truncated repair output", path);
    }
    *checksum = crc32c::Extend(*checksum, bytes.data(), bytes.size());
    prefix_size -= bytes.size();
  }
  return Status::OK();
}

namespace {
// True if `record` is a well-formed catalog record of type kSyncMarker.
bool IsRepairSyncMarkerRecord(Slice record) {
  Slice magic;
  uint64_t ordinal = 0;
  uint32_t type = 0;
  return GetLengthPrefixedSlice(&record, &magic) &&
         magic == kRepairCatalogMagic && GetVarint64(&record, &ordinal) &&
         GetVarint32(&record, &type) &&
         type == static_cast<uint32_t>(RepairCatalogRecordType::kSyncMarker);
}

// Reads at most `record_limit` records (all records on the first pass). A
// marker is written just before its sync, so the last marker in the file may
// itself be unsynced, but a marker followed by a later marker was synced (the
// writer only goes on after the sync returns). So on the first pass, physical
// damage followed by two or more valid markers is Corruption: synced records
// are damaged. Otherwise it is a torn unsynced tail, and the catalog is read
// again up to the last marker before the damage. Records after that marker
// allowed no action, so dropping them is safe. A bad checksum makes the log
// reader drop the rest of its 32 KB block, so markers in that block are not
// seen: damage to synced records in the last block also reads as a torn tail.
Status ReadRepairCatalogRecords(Env* env, const std::string& dbname,
                                RepairCatalog* catalog, bool* recognized,
                                uint64_t record_limit) {
  *recognized = false;
  const std::string path = RepairInProgressFileName(dbname);
  // Most DBs have no catalog, so check first and open the file only when it
  // exists, as DB::Open() does for CURRENT. Any error other than NotFound is
  // returned: it does not mean that there is no catalog.
  Status status =
      env->GetFileSystem()->FileExists(path, IOOptions(), /*dbg=*/nullptr);
  if (status.IsNotFound() || status.IsPathNotFound()) {
    return Status::OK();
  }
  if (!status.ok()) {
    return status;
  }
  std::unique_ptr<SequentialFileReader> file;
  status = SequentialFileReader::Create(env->GetFileSystem(), path,
                                        FileOptions(), &file, nullptr, nullptr);
  if (status.IsNotFound() || status.IsPathNotFound()) {
    // The file was removed after the check above.
    return Status::OK();
  }
  if (!status.ok()) {
    return status;
  }
  // A file with this name is recognized only if its first 128 bytes contain the
  // catalog magic prefix ("rocksdb.blob-gc.repair."). For an unrecognized file,
  // *recognized stays false and the read returns Corruption. The caller treats
  // this as an error only when it finds other persisted blob indirection
  // evidence.
  char prefix_buffer[128];
  Slice prefix;
  status =
      file->Read(sizeof(prefix_buffer), &prefix, prefix_buffer, Env::IO_TOTAL);
  if (!status.ok()) {
    return status;
  }
  *recognized =
      prefix.ToString().find("rocksdb.blob-gc.repair.") != std::string::npos;
  status = SequentialFileReader::Create(env->GetFileSystem(), path,
                                        FileOptions(), &file, nullptr, nullptr);
  if (!status.ok()) {
    return status;
  }
  if (!*recognized) {
    // Not a catalog (see the comment above the prefix read).
    return Status::Corruption("Unrecognized repair catalog", path);
  }
  RepairCatalogReporter reporter;
  RepairCatalogReader reader(std::move(file), &reporter);
  std::string scratch;
  Slice record;
  uint64_t records = 0;
  uint64_t marker_records = 0;
  const bool first_pass = record_limit == std::numeric_limits<uint64_t>::max();
  for (;;) {
    if (records == record_limit) {
      return Status::OK();
    }
    bool found = false;
    status = reader.ReadCatalogRecord(&record, &scratch, &found);
    if (!status.ok()) {
      if (!first_pass || marker_records == 0) {
        return status;
      }
      // Count the valid sync markers after the damage.
      int later_markers = 0;
      for (;;) {
        bool later_found = false;
        Status later =
            reader.ReadCatalogRecord(&record, &scratch, &later_found);
        if (later.ok() && !later_found) {
          break;
        }
        if (later.ok() && IsRepairSyncMarkerRecord(record) &&
            ++later_markers == 2) {
          return status;
        }
      }
      if (reader.hasReadError()) {
        // An I/O error is not a torn tail.
        return status;
      }
      reporter.status.PermitUncheckedError();
      TEST_SYNC_POINT("ReadRepairCatalog:TornUnsyncedTail");
      *catalog = RepairCatalog();
      return ReadRepairCatalogRecords(env, dbname, catalog, recognized,
                                      marker_records);
    }
    if (!found) {
      break;
    }
    Slice magic, pathname, identity;
    RepairCatalogRecord decoded;
    uint32_t type;
    if (!GetLengthPrefixedSlice(&record, &magic) ||
        magic != kRepairCatalogMagic) {
      return *recognized ? Status::Corruption("Unknown repair catalog record")
                         : Status::OK();
    }
    *recognized = true;
    if (record.size() > log::kBlockSize ||
        !GetVarint64(&record, &decoded.ordinal) ||
        !GetVarint32(&record, &type) ||
        type > static_cast<uint32_t>(RepairCatalogRecordType::kSyncMarker) ||
        !GetLengthPrefixedSlice(&record, &pathname) ||
        !GetLengthPrefixedSlice(&record, &identity) ||
        !GetVarint64(&record, &decoded.number) ||
        !GetVarint64(&record, &decoded.size) ||
        !GetFixed32(&record, &decoded.checksum) ||
        !GetVarint32(&record, &decoded.column_family) ||
        !GetVarint64(&record, &decoded.source_wal) || !record.empty() ||
        decoded.ordinal != catalog->last_ordinal + 1 ||
        (catalog->candidate.number != 0 &&
         type != static_cast<uint32_t>(RepairCatalogRecordType::kSyncMarker))) {
      return Status::Corruption("Malformed repair catalog record");
    }
    decoded.type = static_cast<RepairCatalogRecordType>(type);
    decoded.path = pathname.ToString();
    decoded.identity = identity.ToString();
    catalog->last_ordinal = decoded.ordinal;
    switch (decoded.type) {
      case RepairCatalogRecordType::kHeader:
        if (decoded.ordinal != 1 || decoded.identity.empty() ||
            decoded.number == 0 || decoded.column_family > 1 ||
            decoded.source_wal > 7 ||
            (decoded.column_family == 1 && !decoded.path.empty())) {
          return Status::Corruption("Invalid repair catalog authority");
        }
        catalog->header = decoded;
        break;
      case RepairCatalogRecordType::kInput:
        if (!catalog->inputs.emplace(decoded.path, decoded).second) {
          return Status::Corruption("Duplicate repair catalog input");
        }
        break;
      case RepairCatalogRecordType::kIntent:
        if (catalog->inputs.count(decoded.path) != 0 || decoded.number == 0 ||
            !catalog->outputs.emplace(decoded.path, decoded).second) {
          return Status::Corruption("Conflicting repair allocation intent");
        }
        break;
      case RepairCatalogRecordType::kSeal: {
        const std::map<std::string, RepairCatalogRecord>::iterator intent =
            catalog->outputs.find(decoded.path);
        if (intent == catalog->outputs.end() ||
            (intent->second.type != RepairCatalogRecordType::kIntent &&
             !(intent->second.type == RepairCatalogRecordType::kSeal &&
               decoded.identity == RepairCatalogRecord::kUnusedOutput &&
               decoded.size == intent->second.size &&
               decoded.checksum == intent->second.checksum)) ||
            intent->second.number != decoded.number ||
            intent->second.column_family != decoded.column_family ||
            intent->second.source_wal != decoded.source_wal) {
          return Status::Corruption("Repair seal has no allocation intent");
        }
        intent->second = decoded;
        break;
      }
      case RepairCatalogRecordType::kArchive:
        if (decoded.identity != catalog->header.identity ||
            catalog->inputs.count(decoded.path) == 0 ||
            !catalog->archives.emplace(decoded.path, decoded).second) {
          return Status::Corruption("Repair archive has no original input");
        }
        break;
      case RepairCatalogRecordType::kCoverage:
        if (decoded.number < catalog->coverage_floor ||
            decoded.size < catalog->coverage_max_sequence) {
          return Status::Corruption("Repair coverage moved backwards");
        }
        catalog->coverage_floor = decoded.number;
        catalog->coverage_max_sequence = decoded.size;
        break;
      case RepairCatalogRecordType::kCandidate:
        if (decoded.path != DescriptorFileName(dbname, decoded.number) ||
            catalog->outputs.count(decoded.path) == 0 ||
            catalog->outputs.at(decoded.path).type !=
                RepairCatalogRecordType::kSeal ||
            catalog->outputs.at(decoded.path).identity ==
                RepairCatalogRecord::kUnusedOutput ||
            catalog->outputs.at(decoded.path).number != decoded.number ||
            catalog->outputs.at(decoded.path).size != decoded.size ||
            catalog->outputs.at(decoded.path).checksum != decoded.checksum ||
            decoded.source_wal != catalog->coverage_floor) {
          return Status::Corruption("Invalid sealed repair candidate");
        }
        catalog->candidate = decoded;
        break;
      case RepairCatalogRecordType::kPolicy:
        if (decoded.path.empty() || decoded.identity.empty() ||
            !catalog->policies.emplace(decoded.column_family, decoded).second) {
          return Status::Corruption("Invalid repair column-family policy");
        }
        break;
      case RepairCatalogRecordType::kSyncMarker:
        marker_records = records + 1;
        break;
    }
    ++records;
  }
  if (!*recognized) {
    reporter.status.PermitUncheckedError();
    return Status::OK();
  }
  if (catalog->header.identity.empty() || catalog->last_ordinal == 0) {
    return Status::Corruption(
        "Repair catalog has no validated authority header");
  }
  if (reader.hasReadError()) {
    return reporter.status.ok() ? Status::IOError("Repair catalog read failed")
                                : reporter.status;
  }
  // A torn tail cannot own a new output or seal. A validated earlier intent
  // still owns its unfinished file; rollback does not rely on a torn seal.
  if (!reporter.status.ok() && !reader.IsEOF()) {
    return reporter.status;
  }
  return Status::OK();
}

}  // namespace

Status ReadRepairCatalog(Env* env, const std::string& dbname,
                         RepairCatalog* catalog, bool* recognized) {
  return ReadRepairCatalogRecords(env, dbname, catalog, recognized,
                                  std::numeric_limits<uint64_t>::max());
}

Status ResolveRepairCatalog(Env* env, const std::string& dbname,
                            const RepairCatalog& catalog, bool read_only,
                            bool repair_retry, bool* committed,
                            bool seals_verified) {
  // Steps:
  // 1. Decide what CURRENT names: the original MANIFEST (the attempt did not
  //    commit; this includes an absent CURRENT when the header says
  //    kOriginalCurrentAbsent) or the candidate MANIFEST (it committed). If
  //    neither, return Corruption.
  // 2. Original MANIFEST: fail unless the caller is a RepairDB retry that is
  //    not read-only. Check that the original MANIFEST did not change.
  // 3. Candidate MANIFEST: check its seal, its log floors, its last sequence,
  //    and the seals of the outputs that it uses (identity is not
  //    kUnusedOutput). A read-only caller stops here.
  // 4. Delete every output that the committed MANIFEST does not use. For an
  //    attempt that did not commit, this is every output (the rollback).
  // 5. Committed only: move the archived inputs to the lost/ directory.
  // 6. Delete the catalog and sync the directory.
  const auto verify_seal = [&](const RepairCatalogRecord& record,
                               bool allow_prefix) {
    return seals_verified ? Status::OK()
                          : VerifyRepairSeal(env, record, allow_prefix);
  };
  *committed = false;
  std::string current;
  Status status = ReadFileToString(env, CurrentFileName(dbname), &current);
  const bool absent = status.IsNotFound() || status.IsPathNotFound();
  if (!status.ok() && !absent) {
    return status;
  }
  // In the kHeader record, `column_family` is kOriginalCurrentAbsent (1) if
  // the original CURRENT was absent, else 0, and `path` holds the content of
  // the original CURRENT (see the table at RepairCatalogRecord).
  const bool original = absent ? catalog.header.column_family ==
                                     RepairCatalogRecord::kOriginalCurrentAbsent
                               : catalog.header.column_family == 0 &&
                                     current == catalog.header.path;
  const bool candidate =
      status.ok() && catalog.candidate.number != 0 &&
      current ==
          DescriptorFileName("", catalog.candidate.number).substr(1) + "\n";
  if (!original && !candidate) {
    return Status::Corruption(
        "Repair authority is ambiguous; preserve both images");
  }
  if (original && (!repair_retry || read_only)) {
    return Status::Corruption("Uncommitted feature repair; retry RepairDB");
  }
  if (original) {
    for (const auto& input : catalog.inputs) {
      if (input.second.identity == RepairCatalogRecord::kOriginalManifest) {
        status = VerifyRepairSeal(env, input.second, false);
        if (!status.ok()) {
          return status;
        }
      } else if (input.second.identity ==
                 RepairCatalogRecord::kOriginalManifestAbsent) {
        status = env->FileExists(input.first);
        if (!status.IsNotFound() && !status.IsPathNotFound()) {
          return status.ok()
                     ? Status::Corruption("Original repair authority changed")
                     : status;
        }
      }
    }
  }
  if (candidate) {
    status = verify_seal(catalog.candidate, true);
    if (!status.ok()) {
      return status;
    }
    // Each CF that has a kPolicy record, including an empty one, must have a
    // log number of at least max(certified floor, original floor) in the
    // candidate MANIFEST, and the candidate's last sequence must be at least
    // the kCoverage sequence (see "WAL coverage" in db/repair.cc). Repair
    // seals the candidate MANIFEST before CURRENT changes.
    std::unique_ptr<SequentialFileReader> file;
    status = SequentialFileReader::Create(env->GetFileSystem(),
                                          catalog.candidate.path, FileOptions(),
                                          &file, nullptr, nullptr);
    if (!status.ok()) {
      return status;
    }
    RepairCatalogReporter reporter;
    log::Reader reader(nullptr, std::move(file), &reporter, true, 0);
    Slice record;
    std::string scratch;
    std::map<uint32_t, uint64_t> floors;
    uint64_t sequence = 0;
    while (reader.ReadRecord(&record, &scratch)) {
      VersionEdit edit;
      status = edit.DecodeFrom(record);
      if (!status.ok()) {
        return status;
      }
      if (edit.HasLogNumber()) {
        floors[edit.GetColumnFamily()] = edit.GetLogNumber();
      }
      if (edit.HasLastSequence()) {
        sequence = std::max(sequence, edit.GetLastSequence());
      }
    }
    if (!reporter.status.ok()) {
      return reporter.status;
    }
    if (reader.hasReadError() || sequence < catalog.coverage_max_sequence) {
      return Status::Corruption("Incomplete repair WAL-prefix certificate");
    }
    for (const auto& policy : catalog.policies) {
      const std::map<uint32_t, uint64_t>::const_iterator floor =
          floors.find(policy.first);
      // In a kPolicy record, `source_wal` holds the original floor of the CF.
      if (floor == floors.end() ||
          floor->second <
              std::max(catalog.coverage_floor, policy.second.source_wal)) {
        return Status::Corruption("Repair candidate omitted a CF replay floor");
      }
    }
    for (const auto& output : catalog.outputs) {
      if (output.second.type == RepairCatalogRecordType::kSeal &&
          output.second.identity != RepairCatalogRecord::kUnusedOutput) {
        status = verify_seal(output.second, false);
        if (!status.ok()) {
          return status;
        }
      }
    }
    *committed = true;
    if (read_only) {
      return Status::OK();
    }
  }
  for (const auto& output : catalog.outputs) {
    if (candidate &&
        ((output.second.type == RepairCatalogRecordType::kSeal &&
          output.second.identity != RepairCatalogRecord::kUnusedOutput) ||
         output.first == catalog.candidate.path)) {
      continue;
    }
    status = env->FileExists(output.first);
    if (status.IsNotFound() || status.IsPathNotFound()) {
      // A prior cleanup may have unlinked the file but failed its directory
      // sync. Finish that durability obligation before removing the catalog.
      status = SyncRepairDirectory(env, output.first);
      if (!status.ok()) {
        return status;
      }
      continue;
    }
    if (!status.ok()) {
      return status;
    }
    if (output.second.type == RepairCatalogRecordType::kSeal) {
      status = VerifyRepairSeal(env, output.second, false);
      if (!status.ok()) {
        return status;
      }
    }
    status = env->DeleteFile(output.first);
    if (!status.ok()) {
      return status;
    }
    status = SyncRepairDirectory(env, output.first);
    if (!status.ok()) {
      return status;
    }
  }
  if (candidate) {
    for (const auto& archive : catalog.archives) {
      const size_t slash = archive.first.find_last_of("/\\");
      const std::string dir =
          slash == std::string::npos ? "." : archive.first.substr(0, slash);
      const std::string lost = dir + "/lost";
      const std::string destination =
          lost + "/" + archive.second.identity + "-" +
          archive.first.substr(slash == std::string::npos ? 0 : slash + 1);
      status = env->FileExists(archive.first);
      if (status.IsNotFound() || status.IsPathNotFound()) {
        RepairCatalogRecord moved = archive.second;
        moved.path = destination;
        status = verify_seal(moved, false);
        if (!status.ok()) {
          return status;
        }
        status = SyncRepairDirectory(env, destination);
        if (status.ok()) {
          status = SyncRepairDirectory(env, archive.first);
        }
        if (!status.ok()) {
          return status;
        }
        continue;
      }
      if (!status.ok()) {
        return status;
      }
      status = verify_seal(archive.second, false);
      if (!status.ok()) {
        return status;
      }
      status = env->CreateDirIfMissing(lost);
      if (!status.ok()) {
        return status;
      }
      status = env->FileExists(destination);
      if (!status.IsNotFound() && !status.IsPathNotFound()) {
        return status.ok()
                   ? Status::Corruption("Repair archive ownership conflict",
                                        destination)
                   : status;
      }
      status = env->RenameFile(archive.first, destination);
      if (!status.ok()) {
        return status;
      }
      status = SyncRepairDirectory(env, destination);
      if (status.ok()) {
        status = SyncRepairDirectory(env, archive.first);
      }
      if (!status.ok()) {
        return status;
      }
    }
  }
  status = env->DeleteFile(RepairInProgressFileName(dbname));
  return status.ok()
             ? SyncRepairDirectory(env, RepairInProgressFileName(dbname))
             : status;
}

bool IsBlobIndirectionManifestEvidence(const VersionEdit& edit) {
  return edit.HasBlobIndirectionFormatVersion() ||
         edit.HasDbBlobIndirectionFormatVersion() ||
         !edit.GetBlobRelocationFileAdditions().empty() ||
         !edit.GetBlobPhysicalFileLosses().empty();
}

bool IsBlobIndirectionBlobFileEvidence(const Slice& prefix) {
  if (prefix.starts_with(
          Slice(kBlobRelocationFilePrefix, kBlobRelocationFilePrefixSize))) {
    return true;
  }
  BlobLogHeader header;
  // is_indirect_origin is allowed only in a version 2 header.
  return header.DecodeFrom(prefix).ok() && header.is_indirect_origin;
}

bool IsBlobIndirectionTableEvidence(const FileMetaData& meta) {
  // A decoded classification property, even a legacy one, is persisted
  // feature evidence.
  return !meta.blob_reference_descriptor.empty() ||
         meta.oldest_coarse_indirect_origin_file_number !=
             kInvalidBlobFileNumber ||
         meta.blob_reference_legacy_metadata;
}

bool HasBlobIndirectionFileEvidence(
    Env* env, const std::string& dbname, const std::vector<DbPath>& db_paths,
    const std::vector<ColumnFamilyDescriptor>& cfs) {
  TEST_SYNC_POINT("HasBlobIndirectionFileEvidence:Scan");
  std::vector<std::string> files;
  if (env->GetChildren(dbname, &files).ok()) {
    for (const std::string& file_name : files) {
      uint64_t number = 0;
      FileType type;
      if (!ParseFileName(file_name, &number, &type) ||
          type != kDescriptorFile) {
        continue;
      }
      std::unique_ptr<SequentialFileReader> file;
      Status opened = SequentialFileReader::Create(
          env->GetFileSystem(), dbname + "/" + file_name, FileOptions(), &file,
          nullptr, nullptr);
      if (!opened.ok()) {
        continue;
      }
      RepairCatalogReporter reporter;
      log::Reader reader(nullptr, std::move(file), &reporter, true, 0);
      Slice bytes;
      std::string scratch;
      while (reader.ReadRecord(&bytes, &scratch)) {
        VersionEdit edit;
        // Positive evidence suffices even if other MANIFEST records were
        // damaged.
        if (edit.DecodeFrom(bytes).ok() &&
            IsBlobIndirectionManifestEvidence(edit)) {
          reporter.status.PermitUncheckedError();
          return true;
        }
      }
      reporter.status.PermitUncheckedError();
    }
  }
  // Probe only metadata, with no shared block/persistent-cache admission.
  std::set<std::string> paths{dbname};
  for (const DbPath& path : db_paths) {
    paths.insert(path.path);
  }
  for (const ColumnFamilyDescriptor& cf : cfs) {
    for (const DbPath& path : cf.options.cf_paths) {
      paths.insert(path.path);
    }
  }
  DBOptions db_options;
  db_options.env = env;
  const ColumnFamilyOptions default_cf =
      cfs.empty() ? ColumnFamilyOptions() : cfs.front().options;
  const ImmutableOptions ioptions(Options(db_options, default_cf));
  // A probe has no MANIFEST checksum context for the files it opens.
  FileOptions probe_options;
  probe_options.file_checksum_func_name = kNoFileChecksumFuncName;
  for (const std::string& directory : paths) {
    std::vector<std::string> names;
    if (!env->GetChildren(directory, &names).ok()) {
      continue;
    }
    for (const std::string& name : names) {
      uint64_t number = 0;
      FileType type;
      if (!ParseFileName(name, &number, &type) ||
          (type != kTableFile && type != kBlobFile)) {
        continue;
      }
      const std::string path = directory + "/" + name;
      std::unique_ptr<RandomAccessFileReader> file;
      Status opened = RandomAccessFileReader::Create(
          env->GetFileSystem(), path, probe_options, &file, nullptr);
      if (!opened.ok()) {
        continue;
      }
      if (type == kBlobFile) {
        char scratch[BlobLogHeader::kSize];
        Slice bytes;
        opened = file->Read(0, sizeof(scratch), IOOptions(), &bytes, scratch,
                            nullptr);
        if (opened.ok() && IsBlobIndirectionBlobFileEvidence(bytes)) {
          return true;
        }
        continue;
      }
      uint64_t size = 0;
      if (!env->GetFileSize(path, &size).ok()) {
        continue;
      }
      std::unique_ptr<TableProperties> properties;
      ReadOptions read_options;
      read_options.fill_cache = false;
      if (!ReadTableProperties(file.get(), size, 0, ioptions, read_options,
                               &properties)
               .ok()) {
        continue;
      }
      FileMetaData meta;
      ExtractBlobReferenceDescriptorFromTableProperties(*properties, &meta);
      // A descriptor alone is evidence even if the classification is bad.
      if (!ExtractBlobReferenceClassificationFromTableProperties(*properties,
                                                                 &meta)
               .ok()) {
        meta.oldest_coarse_indirect_origin_file_number = kInvalidBlobFileNumber;
        meta.blob_reference_legacy_metadata = false;
      }
      if (IsBlobIndirectionTableEvidence(meta)) {
        return true;
      }
    }
  }
  return false;
}

Status CheckRepairCatalogForOpen(Env* env, const std::string& dbname,
                                 bool read_only, Status* unresolved_catalog) {
  assert(unresolved_catalog != nullptr);
  *unresolved_catalog = Status::OK();
  RepairCatalog catalog;
  bool recognized = false;
  Status status = ReadRepairCatalog(env, dbname, &catalog, &recognized);
  if (!status.ok() && !recognized) {
    // A failed probe or an unrelated file of this name is not activation
    // proof for a legacy database. The caller decides from the replayed
    // MANIFEST, before any open step writes or deletes a file.
    *unresolved_catalog = status;
    return Status::OK();
  }
  if (!status.ok() || !recognized) {
    return status;
  }
  bool committed = false;
  return ResolveRepairCatalog(env, dbname, catalog, read_only,
                              /*repair_retry=*/false, &committed,
                              /*seals_verified=*/false);
}

static const std::string kRocksDbTFileExt = "sst";
static const std::string kLevelDbTFileExt = "ldb";
static const std::string kRocksDBBlobFileExt = "blob";
static const std::string kArchivalDirName = "archive";

// Given a path, flatten the path name by replacing all chars not in
// {[0-9,a-z,A-Z,-,_,.]} with _. And append '_LOG\0' at the end.
// Return the number of chars stored in dest not including the trailing '\0'.
static size_t GetInfoLogPrefix(const std::string& path, char* dest, int len) {
  const char suffix[] = "_LOG";

  size_t write_idx = 0;
  size_t i = 0;
  size_t src_len = path.size();

  while (i < src_len && write_idx < len - sizeof(suffix)) {
    if ((path[i] >= 'a' && path[i] <= 'z') ||
        (path[i] >= '0' && path[i] <= '9') ||
        (path[i] >= 'A' && path[i] <= 'Z') || path[i] == '-' ||
        path[i] == '.' || path[i] == '_') {
      dest[write_idx++] = path[i];
    } else {
      if (i > 0) {
        dest[write_idx++] = '_';
      }
    }
    i++;
  }
  assert(sizeof(suffix) <= len - write_idx);
  // "\0" is automatically added by snprintf
  snprintf(dest + write_idx, len - write_idx, suffix);
  write_idx += sizeof(suffix) - 1;
  return write_idx;
}

static std::string MakeFileName(uint64_t number, const char* suffix) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%06llu.%s",
           static_cast<unsigned long long>(number), suffix);
  return buf;
}

static std::string MakeFileName(const std::string& name, uint64_t number,
                                const char* suffix) {
  return name + "/" + MakeFileName(number, suffix);
}

std::string LogFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name, number, "log");
}

std::string LogFileName(uint64_t number) {
  assert(number > 0);
  return MakeFileName(number, "log");
}

std::string BlobFileName(uint64_t number) {
  assert(number > 0);
  return MakeFileName(number, kRocksDBBlobFileExt.c_str());
}

std::string BlobFileName(const std::string& blobdirname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(blobdirname, number, kRocksDBBlobFileExt.c_str());
}

std::string BlobFileName(const std::string& dbname, const std::string& blob_dir,
                         uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname + "/" + blob_dir, number,
                      kRocksDBBlobFileExt.c_str());
}

std::string ArchivalDirectory(const std::string& dir) {
  return dir + "/" + kArchivalDirName;
}
std::string ArchivedLogFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name + "/" + kArchivalDirName, number, "log");
}

std::string MakeTableFileName(const std::string& path, uint64_t number) {
  return MakeFileName(path, number, kRocksDbTFileExt.c_str());
}

std::string MakeTableFileName(uint64_t number) {
  return MakeFileName(number, kRocksDbTFileExt.c_str());
}

std::string Rocks2LevelTableFileName(const std::string& fullname) {
  assert(fullname.size() > kRocksDbTFileExt.size() + 1);
  if (fullname.size() <= kRocksDbTFileExt.size() + 1) {
    return "";
  }
  return fullname.substr(0, fullname.size() - kRocksDbTFileExt.size()) +
         kLevelDbTFileExt;
}

uint64_t TableFileNameToNumber(const std::string& name) {
  uint64_t number = 0;
  uint64_t base = 1;
  int pos = static_cast<int>(name.find_last_of('.'));
  while (--pos >= 0 && name[pos] >= '0' && name[pos] <= '9') {
    number += (name[pos] - '0') * base;
    base *= 10;
  }
  return number;
}

std::string TableFileName(const std::vector<DbPath>& db_paths, uint64_t number,
                          uint32_t path_id) {
  assert(number > 0);
  std::string path;
  if (path_id >= db_paths.size()) {
    path = db_paths.back().path;
  } else {
    path = db_paths[path_id].path;
  }
  return MakeTableFileName(path, number);
}

void FormatFileNumber(uint64_t number, uint32_t path_id, char* out_buf,
                      size_t out_buf_size) {
  if (path_id == 0) {
    snprintf(out_buf, out_buf_size, "%" PRIu64, number);
  } else {
    snprintf(out_buf, out_buf_size,
             "%" PRIu64
             "(path "
             "%" PRIu32 ")",
             number, path_id);
  }
}

std::string DescriptorFileName(uint64_t number) {
  assert(number > 0);
  char buf[100];
  snprintf(buf, sizeof(buf), "MANIFEST-%06llu",
           static_cast<unsigned long long>(number));
  return buf;
}

std::string DescriptorFileName(const std::string& dbname, uint64_t number) {
  return dbname + "/" + DescriptorFileName(number);
}

std::string CurrentFileName(const std::string& dbname) {
  return dbname + "/" + kCurrentFileName;
}

std::string LockFileName(const std::string& dbname) { return dbname + "/LOCK"; }

std::string TempFileName(const std::string& dbname, uint64_t number) {
  return MakeFileName(dbname, number, kTempFileNameSuffix.c_str());
}

InfoLogPrefix::InfoLogPrefix(bool has_log_dir,
                             const std::string& db_absolute_path) {
  if (!has_log_dir) {
    const char kInfoLogPrefix[] = "LOG";
    // "\0" is automatically added to the end
    snprintf(buf, sizeof(buf), kInfoLogPrefix);
    prefix = Slice(buf, sizeof(kInfoLogPrefix) - 1);
  } else {
    size_t len =
        GetInfoLogPrefix(NormalizePath(db_absolute_path), buf, sizeof(buf));
    prefix = Slice(buf, len);
  }
}

std::string InfoLogFileName(const std::string& dbname,
                            const std::string& db_path,
                            const std::string& log_dir) {
  if (log_dir.empty()) {
    return dbname + "/LOG";
  }

  InfoLogPrefix info_log_prefix(true, db_path);
  return log_dir + "/" + info_log_prefix.buf;
}

// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string& dbname, uint64_t ts,
                               const std::string& db_path,
                               const std::string& log_dir) {
  char buf[50];
  snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(ts));

  if (log_dir.empty()) {
    return dbname + "/LOG.old." + buf;
  }

  InfoLogPrefix info_log_prefix(true, db_path);
  return log_dir + "/" + info_log_prefix.buf + ".old." + buf;
}

std::string OptionsFileName(uint64_t file_num) {
  char buffer[256];
  snprintf(buffer, sizeof(buffer), "%s%06" PRIu64,
           kOptionsFileNamePrefix.c_str(), file_num);
  return buffer;
}
std::string OptionsFileName(const std::string& dbname, uint64_t file_num) {
  return dbname + "/" + OptionsFileName(file_num);
}

std::string TempOptionsFileName(const std::string& dbname, uint64_t file_num) {
  char buffer[256];
  snprintf(buffer, sizeof(buffer), "%s%06" PRIu64 ".%s",
           kOptionsFileNamePrefix.c_str(), file_num,
           kTempFileNameSuffix.c_str());
  return dbname + "/" + buffer;
}

std::string CompactionProgressFileName(const std::string& dbname,
                                       uint64_t timestamp) {
  char buffer[256];
  snprintf(buffer, sizeof(buffer), "%s%llu",
           kCompactionProgressFileNamePrefix.c_str(),
           static_cast<unsigned long long>(timestamp));
  return dbname + "/" + buffer;
}

std::string TempCompactionProgressFileName(const std::string& dbname,
                                           uint64_t timestamp) {
  char buffer[256];
  snprintf(buffer, sizeof(buffer), "%s%llu.%s",
           kCompactionProgressFileNamePrefix.c_str(),
           static_cast<unsigned long long>(timestamp),
           kTempFileNameSuffix.c_str());
  return dbname + "/" + buffer;
}

std::string SessionTmpDir(const std::string& dbname) {
  std::string result = dbname;
  result.append("/").append(kSessionTmpDirName);
  return result;
}

std::string RemoteCompactionJobDir(const std::string& dbname,
                                   bool use_output_root_dir,
                                   const std::string& job_dir_name) {
  if (!use_output_root_dir) {
    std::string db_prefix = dbname;
    if (db_prefix.empty() || db_prefix.back() != '/') {
      db_prefix.append("/");
    }
    if (job_dir_name.compare(0, db_prefix.size(), db_prefix) == 0) {
      return job_dir_name;
    }
    return db_prefix.append(job_dir_name);
  }
  std::string result = SessionTmpDir(dbname);
  result.append("/").append(job_dir_name);
  return result;
}

std::string MetaDatabaseName(const std::string& dbname, uint64_t number) {
  char buf[100];
  snprintf(buf, sizeof(buf), "/METADB-%llu",
           static_cast<unsigned long long>(number));
  return dbname + buf;
}

std::string IdentityFileName(const std::string& dbname) {
  return dbname + "/IDENTITY";
}

// Owned filenames have the form:
//    dbname/IDENTITY
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/<info_log_name_prefix>
//    dbname/<info_log_name_prefix>.old.[0-9]+
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|blob)
//    dbname/METADB-[0-9]+
//    dbname/OPTIONS-[0-9]+
//    dbname/OPTIONS-[0-9]+.dbtmp
//    dbname/COMPACTION_PROGRESS-[timestamp]
//    dbname/COMPACTION_PROGRESS-[timestamp].dbtmp
//    Disregards / at the beginning
//
// The DB directory may also contain the session temporary directory (see
// SessionTmpDir) and dbname/archive/.
// ParseFileName deliberately rejects directories so the obsolete-file scan
// does not treat them as deletion candidates.
bool ParseFileName(const std::string& fname, uint64_t* number, FileType* type,
                   WalFileType* log_type) {
  return ParseFileName(fname, number, "", type, log_type);
}

bool ParseFileName(const std::string& fname, uint64_t* number,
                   const Slice& info_log_name_prefix, FileType* type,
                   WalFileType* log_type) {
  Slice rest(fname);
  if (fname.length() > 1 && fname[0] == '/') {
    rest.remove_prefix(1);
  }
  if (rest == "IDENTITY") {
    *number = 0;
    *type = kIdentityFile;
  } else if (rest == "CURRENT") {
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {
    *number = 0;
    *type = kDBLockFile;
  } else if (info_log_name_prefix.size() > 0 &&
             rest.starts_with(info_log_name_prefix)) {
    rest.remove_prefix(info_log_name_prefix.size());
    if (rest == "" || rest == ".old") {
      *number = 0;
      *type = kInfoLogFile;
    } else if (rest.starts_with(".old.")) {
      uint64_t ts_suffix;
      // sizeof also counts the trailing '\0'.
      rest.remove_prefix(sizeof(".old.") - 1);
      if (!ConsumeDecimalNumber(&rest, &ts_suffix)) {
        return false;
      }
      *number = ts_suffix;
      *type = kInfoLogFile;
    }
  } else if (rest.starts_with("MANIFEST-")) {
    rest.remove_prefix(strlen("MANIFEST-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else if (rest.starts_with("METADB-")) {
    rest.remove_prefix(strlen("METADB-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kMetaDatabase;
    *number = num;
  } else if (rest.starts_with(kOptionsFileNamePrefix)) {
    uint64_t ts_suffix;
    bool is_temp_file = false;
    rest.remove_prefix(kOptionsFileNamePrefix.size());
    const std::string kTempFileNameSuffixWithDot =
        std::string(".") + kTempFileNameSuffix;
    if (rest.ends_with(kTempFileNameSuffixWithDot)) {
      rest.remove_suffix(kTempFileNameSuffixWithDot.size());
      is_temp_file = true;
    }
    if (!ConsumeDecimalNumber(&rest, &ts_suffix)) {
      return false;
    }
    *number = ts_suffix;
    *type = is_temp_file ? kTempFile : kOptionsFile;
  } else if (rest.starts_with(kCompactionProgressFileNamePrefix)) {
    uint64_t timestamp;
    bool is_temp_file = false;
    rest.remove_prefix(kCompactionProgressFileNamePrefix.size());
    const std::string kTempFileNameSuffixWithDot =
        std::string(".") + kTempFileNameSuffix;
    if (rest.ends_with(kTempFileNameSuffixWithDot)) {
      rest.remove_suffix(kTempFileNameSuffixWithDot.size());
      is_temp_file = true;
    }
    if (!ConsumeDecimalNumber(&rest, &timestamp)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *number = timestamp;
    *type = is_temp_file ? kTempFile : kCompactionProgressFile;
  } else {
    // Avoid strtoull() to keep filename format independent of the
    // current locale
    bool archive_dir_found = false;
    if (rest.starts_with(kArchivalDirName)) {
      if (rest.size() <= kArchivalDirName.size()) {
        return false;
      }
      rest.remove_prefix(kArchivalDirName.size() +
                         1);  // Add 1 to remove / also
      if (log_type) {
        *log_type = kArchivedLogFile;
      }
      archive_dir_found = true;
    }
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (rest.size() <= 1 || rest[0] != '.') {
      return false;
    }
    rest.remove_prefix(1);

    Slice suffix = rest;
    if (suffix == Slice("log")) {
      *type = kWalFile;
      if (log_type && !archive_dir_found) {
        *log_type = kAliveLogFile;
      }
    } else if (archive_dir_found) {
      return false;  // Archive dir can contain only log files
    } else if (suffix == Slice(kRocksDbTFileExt) ||
               suffix == Slice(kLevelDbTFileExt)) {
      *type = kTableFile;
    } else if (suffix == Slice(kRocksDBBlobFileExt)) {
      *type = kBlobFile;
    } else if (suffix == Slice(kTempFileNameSuffix)) {
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}

IOStatus SetCurrentFile(const WriteOptions& write_options, FileSystem* fs,
                        const std::string& dbname, uint64_t descriptor_number,
                        Temperature temp,
                        FSDirectory* dir_contains_current_file) {
  // Remove leading "dbname/" and add newline to manifest file name
  std::string manifest = DescriptorFileName(dbname, descriptor_number);
  Slice contents = manifest;
  assert(contents.starts_with(dbname + "/"));
  contents.remove_prefix(dbname.size() + 1);
  std::string tmp = TempFileName(dbname, descriptor_number);
  IOOptions opts;
  IOStatus s = PrepareIOFromWriteOptions(write_options, opts);
  opts.type = IOType::kMetadata;
  FileOptions file_opts;
  file_opts.temperature = temp;
  file_opts.io_options.type = IOType::kMetadata;
  if (s.ok()) {
    s = WriteStringToFile(fs, contents.ToString() + "\n", tmp, true, opts,
                          file_opts);
  }
  TEST_SYNC_POINT_CALLBACK("SetCurrentFile:BeforeRename", &s);
  if (s.ok()) {
    TEST_KILL_RANDOM_WITH_WEIGHT("SetCurrentFile:0", REDUCE_ODDS2);
    s = fs->RenameFile(tmp, CurrentFileName(dbname), opts, nullptr);
    TEST_KILL_RANDOM_WITH_WEIGHT("SetCurrentFile:1", REDUCE_ODDS2);
    TEST_SYNC_POINT_CALLBACK("SetCurrentFile:AfterRename", &s);
  }
  if (s.ok()) {
    if (dir_contains_current_file != nullptr) {
      s = dir_contains_current_file->FsyncWithDirOptions(
          opts, nullptr, DirFsyncOptions(CurrentFileName(dbname)));
    }
  } else {
    fs->DeleteFile(tmp, opts, nullptr)
        .PermitUncheckedError();  // NOTE: PermitUncheckedError is acceptable
                                  // here as we are already handling an error
                                  // case, and this is just a best-attempt
                                  // effort at some cleanup
  }
  return s;
}

Status SetIdentityFile(const WriteOptions& write_options, Env* env,
                       const std::string& dbname, Temperature temp,
                       const std::string& db_id) {
  std::string id;
  if (db_id.empty()) {
    id = env->GenerateUniqueId();
  } else {
    id = db_id;
  }
  assert(!id.empty());
  // Reserve the filename dbname/000000.dbtmp for the temporary identity file
  std::string tmp = TempFileName(dbname, 0);
  std::string identify_file_name = IdentityFileName(dbname);
  Status s;
  IOOptions opts;
  s = PrepareIOFromWriteOptions(write_options, opts);
  opts.type = IOType::kMetadata;
  FileOptions file_opts;
  file_opts.temperature = temp;
  file_opts.io_options.type = IOType::kMetadata;
  if (s.ok()) {
    s = WriteStringToFile(env->GetFileSystem().get(), id, tmp,
                          /*should_sync=*/true, opts, file_opts);
  }
  if (s.ok()) {
    // Env::RenameFile() takes no IOOptions, so this rename is not tagged with
    // IOType::kMetadata. It stays on Env so that Env overrides still apply.
    s = env->RenameFile(tmp, identify_file_name);
  }
  std::unique_ptr<FSDirectory> dir_obj;
  if (s.ok()) {
    s = env->GetFileSystem()->NewDirectory(dbname, opts, &dir_obj, nullptr);
  }
  if (s.ok()) {
    s = dir_obj->FsyncWithDirOptions(opts, nullptr,
                                     DirFsyncOptions(identify_file_name));
  }

  // The default Close() could return "NotSupported" and we bypass it
  // if it is not impelmented. Detailed explanations can be found in
  // db/db_impl/db_impl.h
  if (s.ok()) {
    Status temp_s = dir_obj->Close(opts, nullptr);
    if (!temp_s.ok()) {
      if (temp_s.IsNotSupported()) {
        temp_s.PermitUncheckedError();
      } else {
        s = temp_s;
      }
    }
  }
  if (!s.ok()) {
    env->DeleteFile(tmp).PermitUncheckedError();
  }
  return s;
}

IOStatus SyncManifest(const ImmutableDBOptions* db_options,
                      const WriteOptions& write_options,
                      WritableFileWriter* file) {
  TEST_KILL_RANDOM_WITH_WEIGHT("SyncManifest:0", REDUCE_ODDS2);
  StopWatch sw(db_options->clock, db_options->stats, MANIFEST_FILE_SYNC_MICROS);
  IOOptions io_options;
  IOStatus s = PrepareIOFromWriteOptions(write_options, io_options);
  if (!s.ok()) {
    return s;
  }
  return db_options->use_fsync ? file->Fsync(io_options)
                               : file->Sync(io_options);
}

Status GetInfoLogFiles(const std::shared_ptr<FileSystem>& fs,
                       const std::string& db_log_dir, const std::string& dbname,
                       std::string* parent_dir,
                       std::vector<std::string>* info_log_list) {
  assert(parent_dir != nullptr);
  assert(info_log_list != nullptr);
  uint64_t number = 0;
  FileType type = kWalFile;

  if (!db_log_dir.empty()) {
    *parent_dir = db_log_dir;
  } else {
    *parent_dir = dbname;
  }

  InfoLogPrefix info_log_prefix(!db_log_dir.empty(), dbname);

  std::vector<std::string> file_names;
  Status s = fs->GetChildren(*parent_dir, IOOptions(), &file_names, nullptr);

  if (!s.ok()) {
    return s;
  }

  for (auto& f : file_names) {
    if (ParseFileName(f, &number, info_log_prefix.prefix, &type) &&
        (type == kInfoLogFile)) {
      info_log_list->push_back(f);
    }
  }
  return Status::OK();
}

std::string NormalizePath(const std::string& path) {
  std::string dst;

  if (path.length() > 2 && path[0] == kFilePathSeparator &&
      path[1] == kFilePathSeparator) {  // Handle UNC names
    dst.append(2, kFilePathSeparator);
  }

  for (auto c : path) {
    if (!dst.empty() && (c == kFilePathSeparator || c == '/') &&
        (dst.back() == kFilePathSeparator || dst.back() == '/')) {
      continue;
    }
    dst.push_back(c);
  }
  return dst;
}

}  // namespace ROCKSDB_NAMESPACE
