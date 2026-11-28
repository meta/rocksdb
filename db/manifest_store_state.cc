//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/manifest_store_state.h"

#include <algorithm>
#include <utility>

#include "db/db_impl/db_impl.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/manifest_ops.h"
#include "db/version_edit.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "options/db_options.h"
#include "rocksdb/experimental.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/defer.h"

namespace ROCKSDB_NAMESPACE {

ManifestCommitIdGenerator::ManifestCommitIdGenerator(std::string db_session_id)
    : db_session_id_(std::move(db_session_id)) {}

std::string ManifestCommitIdGenerator::Next() {
  return db_session_id_ + "-" +
         std::to_string(counter_.fetch_add(1, std::memory_order_relaxed));
}

namespace {

// The version of the stub payload. A reader returns NotSupported for any
// other version, for example the version of a newer binary.
constexpr uint32_t kManifestStubPayloadVersion = 1;
// The version of the copy marker payload.
constexpr uint32_t kManifestCopyMarkerPayloadVersion = 1;

// A FSSequentialFile that reads a string.
class StringSequentialFile : public FSSequentialFile {
 public:
  explicit StringSequentialFile(std::string data) : data_(std::move(data)) {}

  IOStatus Read(size_t n, const IOOptions& /*options*/, Slice* result,
                char* scratch, IODebugContext* /*dbg*/) override {
    const size_t len = std::min(n, data_.size() - pos_);
    memcpy(scratch, data_.data() + pos_, len);
    pos_ += len;
    *result = Slice(scratch, len);
    if (len < n) {
      FreeData();
    }
    return IOStatus::OK();
  }

  IOStatus Skip(uint64_t n) override {
    // The step is at most the size of data_, so it fits in size_t. A
    // lossless_cast does not compile where size_t is 32 bits.
    pos_ += static_cast<size_t>(
        std::min<uint64_t>(n, lossless_cast<uint64_t>(data_.size() - pos_)));
    return IOStatus::OK();
  }

 private:
  // Frees the bytes when a read reaches the end. The readers read the bytes
  // only once, and a secondary or follower keeps its reader until the next
  // replay. A later read returns no bytes, as at the end of a file that did
  // not grow.
  void FreeData() {
    if (freed_) {
      return;
    }
    freed_ = true;
    std::string().swap(data_);
    pos_ = 0;
    [[maybe_unused]] size_t capacity = data_.capacity();
    TEST_SYNC_POINT_CALLBACK("StringSequentialFile::FreeData", &capacity);
  }

  std::string data_;
  size_t pos_ = 0;
  bool freed_ = false;
};

// A FSWritableFile that appends to a string.
class StringWritableFile : public FSWritableFile {
 public:
  explicit StringWritableFile(std::string* data) : data_(data) {}

  using FSWritableFile::Append;
  IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
    data_->append(data.data(), data.size());
    return IOStatus::OK();
  }
  IOStatus Close(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }
  IOStatus Flush(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }
  IOStatus Sync(const IOOptions& /*options*/,
                IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }
  uint64_t GetFileSize(const IOOptions& /*options*/,
                       IODebugContext* /*dbg*/) override {
    return data_->size();
  }

 private:
  std::string* const data_;
};

// Collects the rows of a scan.
class CollectRowsVisitor : public ManifestRowVisitor {
 public:
  Status OnDbRow(const ManifestDbRowResult& db_row) override {
    db_row_ = db_row;
    return Status::OK();
  }
  Status OnRow(const ManifestRow& row) override {
    rows_.push_back(row);
    return Status::OK();
  }

  ManifestDbRowResult db_row_;
  std::vector<ManifestRow> rows_;
};

// Returns a plain IOError with the message `msg`. It has no retryable flag,
// no scope and no data-loss flag, so the error handler makes it a fatal error.
IOStatus PlainIOError(const std::string& msg) {
  return IOStatus::IOError("Store mode: " + msg);
}

}  // namespace

void EncodeManifestStub(const ManifestStub& stub, std::string* record) {
  std::string payload;
  PutVarint32(&payload, kManifestStubPayloadVersion);
  PutVarint32(&payload, lossless_cast<uint32_t>(stub.state));
  PutLengthPrefixedSlice(&payload, stub.db_id);
  if (stub.state == ManifestStubState::kEntering) {
    PutVarint64(&payload, stub.from_manifest_number);
    PutVarint64(&payload, stub.from_manifest_size);
    PutVarint64(&payload, stub.done_stub_number);
  }
  PutVarint32(record, kManifestStoreStub);
  PutLengthPrefixedSlice(record, payload);
}

Status DecodeManifestStub(const Slice& record, bool* is_stub,
                          ManifestStub* stub) {
  Slice input = record;
  uint32_t tag = 0;
  *is_stub = false;
  if (!GetVarint32(&input, &tag) || tag != kManifestStoreStub) {
    return Status::OK();
  }
  *is_stub = true;
  Slice payload;
  uint32_t version = 0;
  uint32_t state = 0;
  Slice db_id;
  if (!GetLengthPrefixedSlice(&input, &payload) || !input.empty() ||
      !GetVarint32(&payload, &version)) {
    return Status::Corruption("Bad stub MANIFEST record");
  }
  if (version != kManifestStubPayloadVersion) {
    return Status::NotSupported("Unknown stub MANIFEST version " +
                                std::to_string(version));
  }
  if (!GetVarint32(&payload, &state) ||
      !GetLengthPrefixedSlice(&payload, &db_id)) {
    return Status::Corruption("Bad stub MANIFEST record");
  }
  if (state > lossless_cast<uint32_t>(ManifestStubState::kEntering)) {
    return Status::NotSupported("Unknown stub MANIFEST state " +
                                std::to_string(state));
  }
  // The check above keeps the value in the range of the enum, so the
  // narrowing cast is safe.
  stub->state = static_cast<ManifestStubState>(state);
  stub->db_id = db_id.ToString();
  if (stub->state == ManifestStubState::kEntering &&
      (!GetVarint64(&payload, &stub->from_manifest_number) ||
       !GetVarint64(&payload, &stub->from_manifest_size) ||
       !GetVarint64(&payload, &stub->done_stub_number))) {
    return Status::Corruption("Bad stub MANIFEST record");
  }
  if (!payload.empty()) {
    return Status::Corruption("Bad stub MANIFEST record");
  }
  return Status::OK();
}

Status ReadCurrentManifestInfo(FileSystem* fs, const std::string& dbname,
                               CurrentManifestInfo* info) {
  *info = CurrentManifestInfo();
  std::string path;
  Status s = GetCurrentManifestPath(dbname, fs, /*is_retry=*/false, &path,
                                    &info->number);
  if (s.IsNotFound() || s.IsPathNotFound()) {
    return Status::OK();
  }
  if (!s.ok()) {
    return s;
  }
  info->exists = true;
  return ReadManifestStub(fs, path, &info->is_stub, &info->stub);
}

Status ReadManifestStub(FileSystem* fs, const std::string& path, bool* is_stub,
                        ManifestStub* stub) {
  *is_stub = false;
  ManifestReporter reporter;
  std::unique_ptr<log::Reader> reader;
  Status s = NewManifestReader(fs, path, &reporter, &reader);
  if (!s.ok()) {
    return s;
  }
  // The reporter is called only from ReadRecord(), so setting status_ptr after
  // NewManifestReader() is safe.
  Status read_status;
  reporter.status_ptr = &read_status;
  Slice record;
  std::string scratch;
  if (!reader->ReadRecord(&record, &scratch)) {
    // An empty MANIFEST, or one whose first record was cut short, is not a
    // stub. A damaged first record returns the Corruption that the reader
    // reported.
    return read_status;
  }
  // Only the first record decides whether the file is a stub.
  Status decode_s = DecodeManifestStub(record, is_stub, stub);
  if (*is_stub && decode_s.ok() && !read_status.ok()) {
    // A stub file holds only this record, so damage that the reader reported
    // is damage of the stub.
    return read_status;
  }
  // Damage that the reader skipped before the first record of a normal
  // MANIFEST is left to its recovery, which reads the file again and reports
  // it.
  read_status.PermitUncheckedError();
  return decode_s;
}

bool ShouldSearchForSurvivingManifestStub(const Status& current_s,
                                          const CurrentManifestInfo& current) {
  if (current.is_stub) {
    return false;
  }
  if (current_s.ok()) {
    return !current.exists;
  }
  return current_s.IsNotFound() || current_s.IsPathNotFound() ||
         current_s.IsCorruption();
}

Status FindSurvivingManifestStub(FileSystem* fs, const std::string& dbname,
                                 const std::vector<std::string>& filenames,
                                 bool* found) {
  *found = false;
  for (const std::string& name : filenames) {
    uint64_t number = 0;
    FileType type;
    if (!ParseFileName(name, &number, &type) || type != kDescriptorFile) {
      continue;
    }
    bool is_stub = false;
    ManifestStub stub;
    Status s = ReadManifestStub(fs, DescriptorFileName(dbname, number),
                                &is_stub, &stub);
    if (is_stub) {
      s.PermitUncheckedError();
      *found = true;
      return Status::OK();
    }
    if (!s.ok() && !s.IsNotFound() && !s.IsPathNotFound() &&
        !s.IsCorruption()) {
      return s;
    }
  }
  return Status::OK();
}

Status CheckNoManifestStub(FileSystem* fs, const std::string& dbname,
                           const std::vector<std::string>& filenames,
                           const std::string& what,
                           bool* current_names_manifest_file) {
  *current_names_manifest_file = false;
  const std::string not_supported =
      what + " does not support a DB in store mode";
  CurrentManifestInfo current;
  Status s = ReadCurrentManifestInfo(fs, dbname, &current);
  // A stub that this binary cannot decode (s not OK) is still a stub.
  if (current.is_stub) {
    s.PermitUncheckedError();
    return Status::NotSupported(not_supported);
  }
  // A valid CURRENT that names a normal MANIFEST file returns OK here, and an
  // error that leaves the authority unknown is returned.
  if (!ShouldSearchForSurvivingManifestStub(s, current)) {
    *current_names_manifest_file = s.ok();
    return s;
  }
  s.PermitUncheckedError();
  bool found = false;
  s = FindSurvivingManifestStub(fs, dbname, filenames, &found);
  if (!s.ok()) {
    return s;
  }
  if (found) {
    return Status::NotSupported(
        not_supported +
        ": CURRENT is missing or damaged, and a MANIFEST stub "
        "is in the folder");
  }
  return Status::OK();
}

namespace {

// Writes `records` to a new MANIFEST file `number`, syncs it and points
// CURRENT to it. For a stub and for the MANIFEST file of a rollback.
IOStatus WriteManifestRecords(const ImmutableDBOptions& db_options,
                              const std::string& dbname, uint64_t number,
                              const std::vector<std::string>& records,
                              const WriteOptions& write_options,
                              FSDirectory* dir_contains_current_file) {
  FileSystem* fs = db_options.fs.get();
  const std::string fname = DescriptorFileName(dbname, number);
  FileOptions file_opts;
  file_opts.temperature = db_options.metadata_write_temperature;
  file_opts.io_options.type = IOType::kManifest;
  std::unique_ptr<FSWritableFile> file;
  IOStatus s = NewWritableFile(fs, fname, &file, file_opts);
  if (s.ok()) {
    log::Writer writer(
        std::make_unique<WritableFileWriter>(std::move(file), fname, file_opts),
        /*log_number=*/0, /*recycle_log_files=*/false);
    for (size_t i = 0; s.ok() && i < records.size(); ++i) {
      s = writer.AddRecord(write_options, records[i]);
    }
    if (s.ok()) {
      s = SyncManifest(&db_options, write_options, writer.file());
    }
    if (s.ok()) {
      s = writer.Close(write_options);
    }
  }
  if (s.ok()) {
    s = SetCurrentFile(write_options, fs, dbname, number,
                       db_options.metadata_write_temperature,
                       dir_contains_current_file);
  }
  return s;
}

}  // namespace

IOStatus WriteManifestStub(const ImmutableDBOptions& db_options,
                           const std::string& dbname, uint64_t number,
                           const ManifestStub& stub,
                           const WriteOptions& write_options,
                           FSDirectory* dir_contains_current_file) {
  std::string record;
  EncodeManifestStub(stub, &record);
  return WriteManifestRecords(db_options, dbname, number, {record},
                              write_options, dir_contains_current_file);
}

void EncodeManifestCopyMarker(bool sync_marked, std::string* record) {
  std::string payload;
  PutVarint32(&payload, kManifestCopyMarkerPayloadVersion);
  payload.push_back(sync_marked ? 1 : 0);
  PutVarint32(record, kManifestCopyMarker);
  PutLengthPrefixedSlice(record, payload);
}

Status DecodeManifestCopyMarker(const Slice& record, bool* is_marker,
                                bool* sync_marked) {
  Slice input = record;
  uint32_t tag = 0;
  *is_marker = false;
  if (!GetVarint32(&input, &tag) || tag != kManifestCopyMarker) {
    return Status::OK();
  }
  *is_marker = true;
  Slice payload;
  uint32_t version = 0;
  if (!GetLengthPrefixedSlice(&input, &payload) || !input.empty() ||
      !GetVarint32(&payload, &version)) {
    return Status::Corruption("Bad MANIFEST copy marker");
  }
  if (version != kManifestCopyMarkerPayloadVersion) {
    return Status::NotSupported("Unknown MANIFEST copy marker version " +
                                std::to_string(version));
  }
  if (payload.size() != 1 || lossless_cast<uint8_t>(payload[0]) > 1) {
    return Status::Corruption("Bad MANIFEST copy marker");
  }
  *sync_marked = payload[0] == 1;
  return Status::OK();
}

IOStatus WriteManifestCopyPointer(FileSystem* fs, const std::string& dbname,
                                  uint64_t number,
                                  const WriteOptions& write_options,
                                  FSDirectory* dir, Logger* info_log,
                                  bool* rename_attempted) {
  *rename_attempted = false;
  const std::string copy_name = ManifestCopyFileName(dbname, number);
  const std::string contents = copy_name.substr(dbname.size() + 1) + "\n";
  const std::string tmp = ManifestCopyPointerTempFileName(dbname);
  IOOptions opts;
  IOStatus s = PrepareIOFromWriteOptions(write_options, opts);
  opts.type = IOType::kMetadata;
  FileOptions file_opts;
  file_opts.io_options.type = IOType::kMetadata;
  if (s.ok()) {
    s = WriteStringToFile(fs, contents, tmp, /*should_sync=*/true, opts,
                          file_opts);
  }
  TEST_SYNC_POINT("WriteManifestCopyPointer:BeforeRename");
  if (s.ok()) {
    // A rename that returns an error can still have happened (for example a
    // timeout on a remote file system), so the caller must treat the pointer
    // as possibly naming the new copy from here on.
    *rename_attempted = true;
    s = fs->RenameFile(tmp, ManifestCopyPointerFileName(dbname), opts, nullptr);
  }
  if (!s.ok()) {
    // The write error stays the result. A temporary file that is already gone
    // (after a rename that happened) is not a cleanup error.
    IOStatus delete_s = fs->DeleteFile(tmp, opts, nullptr);
    if (!delete_s.ok() && !delete_s.IsNotFound() &&
        !delete_s.IsPathNotFound()) {
      ROCKS_LOG_WARN(info_log,
                     "Store mode: cannot delete the temporary copy pointer "
                     "file %s: %s",
                     tmp.c_str(), delete_s.ToString().c_str());
    }
  }
  if (s.ok() && dir != nullptr) {
    s = dir->FsyncWithDirOptions(
        opts, nullptr, DirFsyncOptions(ManifestCopyPointerFileName(dbname)));
  }
  return s;
}

Status ReadManifestCopyPointer(FileSystem* fs, const std::string& dbname,
                               uint64_t* number) {
  *number = 0;
  IOOptions opts;
  opts.type = IOType::kMetadata;
  std::string contents;
  IOStatus s = ReadFileToString(fs, ManifestCopyPointerFileName(dbname), opts,
                                &contents);
  if (s.IsNotFound() || s.IsPathNotFound()) {
    return Status::OK();
  }
  if (!s.ok()) {
    return s;
  }
  bool is_pointer = false;
  if (contents.empty() || contents.back() != '\n' ||
      !ParseManifestCopyFileName(contents.substr(0, contents.size() - 1),
                                 number, &is_pointer) ||
      is_pointer) {
    *number = 0;
    return Status::Corruption("Bad MANIFEST copy pointer file");
  }
  return Status::OK();
}

IOStatus DeleteManifestCopies(FileSystem* fs, const std::string& dbname,
                              uint64_t keep_number, bool delete_pointer) {
  IOOptions opts;
  opts.type = IOType::kManifest;
  std::vector<std::string> children;
  IOStatus s = fs->GetChildren(dbname, opts, &children, nullptr);
  if (!s.ok()) {
    return s;
  }
  // The pointer goes first, so that it never names a deleted copy.
  if (delete_pointer) {
    for (const std::string& name : {ManifestCopyPointerFileName(dbname),
                                    ManifestCopyPointerTempFileName(dbname)}) {
      s = fs->DeleteFile(name, opts, nullptr);
      if (s.IsNotFound() || s.IsPathNotFound()) {
        s = IOStatus::OK();
      }
      if (!s.ok()) {
        return s;
      }
    }
  }
  for (const std::string& name : children) {
    uint64_t number = 0;
    bool is_pointer = false;
    if (!ParseManifestCopyFileName(name, &number, &is_pointer) || is_pointer ||
        number == keep_number) {
      continue;
    }
    IOStatus del = fs->DeleteFile(dbname + "/" + name, opts, nullptr);
    if (!del.ok() && !del.IsNotFound() && s.ok()) {
      s = del;
    }
  }
  return s;
}

Status ScanManifestStore(ManifestStore* store, bool consistent,
                         ManifestStoreScan* scan) {
  CollectRowsVisitor visitor;
  ManifestScanOptions opts;
  opts.consistent = consistent;
  IOStatus io_s = store->Scan(opts, &visitor);
  if (!io_s.ok()) {
    return io_s;
  }
  scan->db_row = std::move(visitor.db_row_);
  return PublicRowsToManifestRows(scan->db_row.db_row, visitor.rows_,
                                  &scan->rows, &scan->extra_fields);
}

Status LargestFileNumber(FileSystem* fs, const std::string& dbname,
                         uint64_t* largest) {
  std::vector<std::string> children;
  IOOptions io_opts;
  io_opts.type = IOType::kManifest;
  Status s = fs->GetChildren(dbname, io_opts, &children, nullptr);
  *largest = 0;
  for (const std::string& name : children) {
    uint64_t number = 0;
    FileType type;
    bool is_pointer = false;
    if (ParseFileName(name, &number, &type) ||
        (ParseManifestCopyFileName(name, &number, &is_pointer) &&
         !is_pointer)) {
      *largest = std::max(*largest, number);
    }
  }
  return s;
}

Status ReadDbIdFromIdentityFile(FileSystem* fs, const std::string& dbname,
                                std::string* db_id) {
  db_id->clear();
  Status s = ReadFileToString(fs, IdentityFileName(dbname), IOOptions(), db_id);
  if (s.IsNotFound() || s.IsPathNotFound()) {
    db_id->clear();
    return Status::OK();
  }
  if (!s.ok()) {
    return s;
  }
  while (!db_id->empty() && db_id->back() == '\n') {
    db_id->pop_back();
  }
  return Status::OK();
}

void DeleteStubFile(FileSystem* fs, const std::string& dbname, uint64_t number,
                    Logger* info_log) {
  IOOptions io_opts;
  io_opts.type = IOType::kManifest;
  const std::string stub_name = DescriptorFileName(dbname, number);
  IOStatus delete_s = fs->DeleteFile(stub_name, io_opts, nullptr);
  if (!delete_s.ok()) {
    ROCKS_LOG_WARN(info_log, "Store mode: cannot delete the stub %s: %s",
                   stub_name.c_str(), delete_s.ToString().c_str());
  }
}

Status PointCurrentBackFromEnteringStub(const WriteOptions& write_options,
                                        FileSystem* fs,
                                        const std::string& dbname,
                                        const CurrentManifestInfo& current,
                                        Temperature temperature,
                                        FSDirectory* dir, Logger* info_log) {
  IOOptions io_opts;
  io_opts.type = IOType::kManifest;
  uint64_t size = 0;
  Status s = fs->GetFileSize(
      DescriptorFileName(dbname, current.stub.from_manifest_number), io_opts,
      &size, nullptr);
  if (!s.ok() || size != current.stub.from_manifest_size) {
    return Status::Corruption(
        "Store mode: the MANIFEST file of an unfinished enter is missing or "
        "has another size");
  }
  s = SetCurrentFile(write_options, fs, dbname,
                     current.stub.from_manifest_number, temperature, dir);
  if (!s.ok()) {
    return s;
  }
  DeleteStubFile(fs, dbname, current.number, info_log);
  return Status::OK();
}

Status CheckScannedStoreRows(const ManifestStoreScan& scan,
                             const std::string& stub_db_id) {
  switch (scan.extra_fields.authority) {
    case ManifestAuthority::kStore:
      break;
    case ManifestAuthority::kFile:
      return Status::InvalidArgument(
          "Store mode: the DB row says that the MANIFEST file is the "
          "authority (the DB was rolled back to file mode)");
    case ManifestAuthority::kShadow:
      return Status::NotSupported(
          "Store mode: the DB row says that the DB is in shadow mode");
  }
  if (scan.rows.db.db_id != stub_db_id) {
    return Status::Corruption(
        "Store mode: the db id of the DB row (" + scan.rows.db.db_id +
        ") differs from the stub MANIFEST (" + stub_db_id + ")");
  }
  return Status::OK();
}

Status RecordsToManifestBytes(const std::vector<std::string>& records,
                              std::string* bytes) {
  bytes->clear();
  log::Writer writer(std::make_unique<WritableFileWriter>(
                         std::make_unique<StringWritableFile>(bytes),
                         "store rows", FileOptions()),
                     /*log_number=*/0, /*recycle_log_files=*/false);
  const WriteOptions write_options;
  for (const std::string& record : records) {
    IOStatus s = writer.AddRecord(write_options, record);
    if (!s.ok()) {
      return s;
    }
  }
  return writer.Close(write_options);
}

std::unique_ptr<FSSequentialFile> NewStringSequentialFile(std::string data) {
  return std::make_unique<StringSequentialFile>(std::move(data));
}

Status CheckManifestDbRowOwner(const ManifestRow& db_row,
                               const std::string& db_id, ManifestRows* rows,
                               ManifestDbRowExtraFields* extra) {
  ManifestRows decoded;
  ManifestDbRowExtraFields decoded_extra;
  Status s = PublicRowsToManifestRows(db_row, {}, &decoded, &decoded_extra);
  if (s.ok() && !db_id.empty() && !decoded.db.db_id.empty() &&
      decoded.db.db_id != db_id) {
    s = Status::InvalidArgument(
        "Store mode: the store holds the rows of another DB (db id " +
        decoded.db.db_id + "), not of this DB (db id " + db_id + ")");
  }
  if (rows != nullptr) {
    *rows = std::move(decoded);
  }
  if (extra != nullptr) {
    *extra = decoded_extra;
  }
  return s;
}

Status CheckOwnerAndClaim(ManifestStore* store,
                          const ManifestStoreOpenOptions& opts,
                          const std::string& expected_db_id,
                          ManifestDbRowResult* claimed,
                          ManifestDbRowExtraFields* extra) {
  ManifestStoreOpenOptions claim_opts = opts;
  for (int attempt = 1;; ++attempt) {
    IOStatus io_s = store->ReadDbRow(claimed);
    if (io_s.IsNotFound()) {
      return Status::NotFound("Store mode: the store has no DB row");
    }
    if (!io_s.ok()) {
      return Status::IOError("Store mode: cannot read the DB row: " +
                             io_s.ToString());
    }
    Status s = CheckManifestDbRowOwner(claimed->db_row, expected_db_id);
    if (!s.ok()) {
      return s;
    }
    claim_opts.expected_commit_sequence = claimed->state.commit_sequence;
    claim_opts.expected_last_commit_id = claimed->state.last_commit_id;
    TEST_SYNC_POINT("CheckOwnerAndClaim:BeforeClaim");
    io_s = store->Open(claim_opts, claimed);
    if (io_s.IsBusy()) {
      // The DB row changed after the read, and the claim changed nothing.
      if (attempt == kMaxOwnerClaimAttempts) {
        return Status::TryAgain(
            "Store mode: the DB row changed during each claim; open again");
      }
      continue;
    }
    if (io_s.IsNotFound()) {
      return Status::NotFound("Store mode: the store has no DB row");
    }
    if (io_s.IsIOFenced()) {
      return IOStatus::IOFenced("Store mode: a newer instance claimed the DB");
    }
    if (!io_s.ok()) {
      return Status::IOError("Store mode: cannot claim the DB: " +
                             io_s.ToString());
    }
    // The claimed row is the row that the check read, so the owner check
    // passes again. This call is needed for the extra fields.
    return CheckManifestDbRowOwner(claimed->db_row, expected_db_id,
                                   /*rows=*/nullptr, extra);
  }
}

ManifestStoreState::ManifestStoreState(std::shared_ptr<ManifestStore> store,
                                       const std::string& db_session_id,
                                       uint32_t feature_version)
    : store_(std::move(store)),
      commit_ids_(db_session_id),
      feature_version_(feature_version) {}

void ManifestStoreState::Init(const ManifestStoreScan& scan) {
  applied_commit_sequence_.store(scan.db_row.state.commit_sequence,
                                 std::memory_order_relaxed);
  extra_fields_ = scan.extra_fields;
  batch_base_rows_ = ManifestRows();
  batch_base_rows_.db = scan.rows.db;
  for (const auto& [cf_id, cf_rows] : scan.rows.cfs) {
    batch_base_rows_.cfs[cf_id].cf = cf_rows.cf;
    if (!cf_rows.cf.dropped) {
      continue;
    }
    std::vector<uint64_t> files;
    std::vector<uint64_t> blob_files;
    for (const auto& [number, file] : cf_rows.files) {
      files.push_back(number);
    }
    for (const auto& [number, blob_file] : cf_rows.blob_files) {
      blob_files.push_back(number);
    }
    QueueDroppedCfRows(cf_id, files, blob_files);
  }
}

void ManifestStoreState::QueueDroppedCfRows(
    uint32_t cf_id, const std::vector<uint64_t>& files,
    const std::vector<uint64_t>& blob_files) {
  for (uint64_t number : files) {
    dropped_cf_rows_.push_back({ManifestRowType::kFile, cf_id, number});
  }
  for (uint64_t number : blob_files) {
    dropped_cf_rows_.push_back({ManifestRowType::kBlobFile, cf_id, number});
  }
  dropped_cf_rows_.push_back({ManifestRowType::kColumnFamily, cf_id, 0});
}

IOStatus ManifestStoreState::Stop(const IOStatus& error) {
  if (stop_error_.ok()) {
    stop_error_ = error;
    ROCKS_LOG_ERROR(retry_policy_.info_log,
                    "Store mode: the DB is stopped; later MANIFEST writes "
                    "fail until the DB is opened again: %s",
                    stop_error_.ToString().c_str());
  }
  return stop_error_;
}

void ManifestStoreState::OnApplied(const ManifestRowChanges& changes,
                                   size_t cleanup_deletes) {
  applied_commit_sequence_.fetch_add(1, std::memory_order_relaxed);
  rewrite_.clear();
  batch_base_rows_.db = changes.db;
  for (const auto& [cf_id, cf] : changes.cf_upserts) {
    batch_base_rows_.cfs[cf_id].cf = cf;
  }
  for (size_t i = 0; i < cleanup_deletes; ++i) {
    const ManifestRowKey& key = dropped_cf_rows_.front();
    if (key.type == ManifestRowType::kColumnFamily) {
      batch_base_rows_.cfs.erase(key.cf_id);
    }
    dropped_cf_rows_.pop_front();
  }
}

void ManifestStoreState::PrepareRewrite() {
  for (const auto& [cf_id, cf_rows] : rewrite_) {
    batch_base_rows_.cfs[cf_id].cf = cf_rows.cf;
  }
}

void ManifestStoreState::MergeRewrite(ManifestRowChanges* changes) const {
  for (const auto& [cf_id, cf_rows] : rewrite_) {
    // BuildManifestRowChanges() writes the column family row only when the
    // batch changes it.
    changes->cf_upserts.emplace(cf_id, cf_rows.cf);
    for (const auto& [number, file] : cf_rows.files) {
      if (changes->file_deletes.count({cf_id, number}) == 0) {
        // emplace() keeps the row of the batch, which is newer.
        changes->file_upserts.emplace(std::make_pair(cf_id, number), file);
      }
    }
    for (const auto& [number, blob_file] : cf_rows.blob_files) {
      if (changes->blob_file_deletes.count({cf_id, number}) == 0) {
        changes->blob_file_upserts.emplace(std::make_pair(cf_id, number),
                                           blob_file);
      }
    }
  }
}

IOStatus ManifestStoreState::CommitBatch(const ManifestRowChanges& changes,
                                         uint64_t in_flight_file_number) {
  const RetryPolicy& policy = retry_policy_;
  if (!stop_error_.ok()) {
    return stop_error_;
  }
  ManifestDbRowExtraFields extra = extra_fields_;
  extra.in_flight_file_number =
      std::min(in_flight_file_number, changes.db.next_file_number);
  if (extra.copy_mode == ManifestCopyMode::kSync) {
    // Sync copy retention: the sync copy gets this batch only after the
    // commit, so until then it still names the files whose rows this commit
    // deletes. Keep them at or above the in-flight file number. The next
    // commit, sent after the copy holds this batch, publishes its own number
    // and so releases them.
    for (const auto& key : changes.file_deletes) {
      if (changes.file_upserts.count(key) == 0) {
        extra.in_flight_file_number =
            std::min(extra.in_flight_file_number, key.second);
      }
    }
    for (const auto& key : changes.blob_file_deletes) {
      if (changes.blob_file_upserts.count(key) == 0) {
        extra.in_flight_file_number =
            std::min(extra.in_flight_file_number, key.second);
      }
    }
  }
  ManifestCommit commit;
  Status s = ManifestRowChangesToCommit(changes, extra, feature_version_,
                                        applied_commit_sequence_,
                                        commit_ids_.Next(), &commit);
  if (!s.ok()) {
    return Stop(PlainIOError("cannot build a commit: " + s.ToString()));
  }
  // Rows of dropped column families to delete with this commit.
  size_t cleanup_limit = kDroppedCfRowsPerCommit;
  TEST_SYNC_POINT_CALLBACK("ManifestStoreState::CommitBatch:CleanupLimit",
                           &cleanup_limit);
  size_t cleanup_deletes = 0;
  while (cleanup_deletes < dropped_cf_rows_.size() &&
         cleanup_deletes < cleanup_limit) {
    const ManifestRowKey& key = dropped_cf_rows_[cleanup_deletes];
    // A batch never changes a row of a dropped column family:
    // ProcessManifestWrites() skips the edits of dropped column families,
    // and column family ids are never reused.
    assert(
        std::none_of(commit.upserts.begin(), commit.upserts.end(),
                     [&](const ManifestRow& row) { return row.key == key; }));
    assert(std::find(commit.deletes.begin(), commit.deletes.end(), key) ==
           commit.deletes.end());
    commit.deletes.push_back(key);
    ++cleanup_deletes;
  }

  IOStatus io_s = store_->Commit(commit);
  if (io_s.IsNotSupported() && cleanup_deletes > 0) {
    // The store rejected the commit for good, so nothing of it applied. The
    // deletes of dropped column family rows are optional and can make the
    // commit too large for the store, so send the batch once more without
    // them. They stay queued for a later batch. This happens only after a
    // definite first-send rejection: after an unknown outcome the first
    // request can still apply, so a different request is never sent then.
    ROCKS_LOG_WARN(
        policy.info_log,
        "Store mode: the store rejected commit %s with %" ROCKSDB_PRIszt
        " deletes of dropped column family rows; sending it again "
        "without them (store: %s)",
        commit.commit_id.c_str(), cleanup_deletes, io_s.ToString().c_str());
    commit.deletes.resize(commit.deletes.size() - cleanup_deletes);
    cleanup_deletes = 0;
    commit.commit_id = commit_ids_.Next();
    io_s = store_->Commit(commit);
  }
  if (io_s.ok()) {
    OnApplied(changes, cleanup_deletes);
    return IOStatus::OK();
  }
  // The status text of the last failed store call goes into the message of
  // the error that RocksDB returns. Only the text: the code and the flags
  // come from RocksDB.
  std::string store_text = io_s.ToString();
  auto fenced = [&]() {
    return Stop(IOStatus::IOFenced(
        "Store mode: a newer instance claimed the DB (store: " + store_text +
        ")"));
  };
  auto stop = [&](const std::string& msg) {
    return Stop(PlainIOError(msg + " (store: " + store_text + ")"));
  };
  if (io_s.IsIOFenced()) {
    return fenced();
  }
  if (io_s.IsTryAgain()) {
    // A retryable IOError, not TryAgain: the error handler starts auto-resume
    // for it, and DBImpl::ResumeImpl() then commits an empty batch only after
    // an IOError.
    IOStatus retry = PlainIOError(
        "the store did not apply a commit (store: " + store_text + ")");
    retry.SetRetryable(true);
    return retry;
  }
  if (io_s.IsNotSupported()) {
    // The store rejects this commit for good and did not apply it, for
    // example because the commit is too large. A resend would get the same
    // result, so the batch fails at once with a non-retryable IOError. A
    // caller that sets it as the background error (for example a flush or a
    // compaction) gets a fatal error (see the result table at
    // ManifestStoreState::CommitBatch() in db/manifest_store_state.h).
    // The store state does not stop (no Stop()), because the stored commit
    // sequence did not change.
    return PlainIOError(
        "the store rejected a commit for good (store: " + store_text + ")");
  }
  // Busy from a send means that another commit changed the commit sequence.
  // RocksDB still reads the DB row. When the row shows this commit, it
  // applied. Otherwise the DB stops, and a row with the expected commit
  // sequence means that the store does not follow the rules of
  // ManifestStore::Commit().
  bool last_send_busy = io_s.IsBusy();

  // The outcome is unknown (or Busy). Learn it from the DB row.
  //
  // Each loop iteration reads the DB row. A DB row that still holds the
  // expected commit sequence shows only that the commit had not applied when
  // RocksDB read the row; an earlier send can still be in progress in the
  // store and apply later. RocksDB then resends the same commit, if the
  // previous send returned a status other than Busy and another retry is
  // allowed. The expected commit sequence lets at most one of the sends apply.
  // A retry is a resend, or a ReadDbRow() that repeats a failed one. RocksDB
  // makes at most RetryPolicy::max_retries retries, and stops the DB when it
  // needs one more. After a failed ReadDbRow() or a failed resend it waits
  // RetryPolicy::retry_interval_us. A resend that gets Busy has no wait: Busy
  // means that the commit sequence changed, so the next ReadDbRow() ends the
  // loop.
  int retries = 0;
  auto retry_limit_reached = [&]() { return ++retries > policy.max_retries; };
  while (true) {
    ManifestDbRowResult db_row;
    TEST_SYNC_POINT("ManifestStoreState::CommitBatch:BeforeReadDbRow");
    IOStatus read_s = store_->ReadDbRow(&db_row);
    bool wait = false;
    if (read_s.ok()) {
      if (db_row.state.commit_sequence == applied_commit_sequence_ + 1 &&
          db_row.state.last_commit_id == commit.commit_id) {
        OnApplied(changes, cleanup_deletes);
        return IOStatus::OK();
      }
      if (db_row.state.commit_sequence != applied_commit_sequence_) {
        return stop(
            "the DB row holds another commit, so the outcome of a commit is "
            "unknown");
      }
      if (last_send_busy) {
        return stop(
            "the store returned Busy, but the DB row holds the expected commit "
            "sequence");
      }
      if (retry_limit_reached()) {
        return stop("the outcome of a commit is unknown");
      }
      ROCKS_LOG_WARN(policy.info_log,
                     "Store mode: the outcome of commit %s is unknown and the "
                     "DB row shows that it had not applied yet, so it is sent "
                     "again (store: %s)",
                     commit.commit_id.c_str(), store_text.c_str());
      TEST_SYNC_POINT("ManifestStoreState::CommitBatch:BeforeResend");
      IOStatus resend_s = store_->Commit(commit);
      if (resend_s.ok()) {
        OnApplied(changes, cleanup_deletes);
        return IOStatus::OK();
      }
      store_text = resend_s.ToString();
      if (resend_s.IsIOFenced()) {
        return fenced();
      }
      if (resend_s.IsNotSupported()) {
        // The resend never applies, but the first send can still apply, so
        // the outcome stays unknown and the DB stops.
        return stop(
            "the store rejected a resent commit for good, and the outcome of "
            "the first send is unknown");
      }
      last_send_busy = resend_s.IsBusy();
      wait = !last_send_busy;
    } else {
      store_text = read_s.ToString();
      if (read_s.IsIOFenced()) {
        return fenced();
      }
      if (read_s.IsNotFound()) {
        return stop("the store has no DB row");
      }
      if (retry_limit_reached()) {
        return stop("the outcome of a commit is unknown");
      }
      wait = true;
    }
    if (policy.opening_or_closing && policy.opening_or_closing()) {
      return stop(
          "the DB is opening or closing and the outcome of a commit is "
          "unknown");
    }
    if (wait && policy.clock != nullptr) {
      // Sleep in steps of at most kMaxSleepStepUs (100 ms), so that a close
      // does not wait for the whole interval. The value 100 ms is a choice,
      // not measured: it keeps the extra delay of a close short, and wakes up
      // only 10 times a second. A step fits in the int of
      // SleepForMicroseconds().
      constexpr uint64_t kMaxSleepStepUs = 100000;
      uint64_t remaining = policy.retry_interval_us;
      while (remaining > 0 &&
             !(policy.opening_or_closing && policy.opening_or_closing())) {
        const uint64_t step = std::min(remaining, kMaxSleepStepUs);
        policy.clock->SleepForMicroseconds(static_cast<int>(step));
        remaining -= step;
      }
    }
  }
}

namespace {

// Reads the records of the sync-marked copy that the pointer file names. It
// keeps every complete record outside an atomic group and every complete
// atomic group. A crash can leave two kinds of torn tail, and both are dropped
// without an error:
// - a last record that was cut short (the log reader stops before it), and
// - an atomic group that is still open at the end of the file.
// A damaged record (a bad checksum, or a record that cannot be decoded), or an
// atomic group with a wrong number of records or with a normal record inside
// it, returns Corruption. `next_file_number` gets the largest next file number
// of the kept records.
Status ReadSyncCopyRecords(FileSystem* fs, const std::string& dbname,
                           std::vector<std::string>* records,
                           uint64_t* next_file_number) {
  *next_file_number = 0;
  uint64_t number = 0;
  Status s = ReadManifestCopyPointer(fs, dbname, &number);
  if (!s.ok()) {
    return s;
  }
  if (number == 0) {
    return Status::InvalidArgument(
        "Store mode: no MANIFEST file copy to roll back from without the "
        "store");
  }
  const std::string path = ManifestCopyFileName(dbname, number);
  // A torn write at the end (a record cut short by a crash) is not reported
  // as corruption; the reader stops before it. Any reported corruption is
  // damage, and the reader drops the rest of its 32 KB block, which can hold
  // later commits. So the copy cannot be used then.
  ManifestReporter reporter;
  std::unique_ptr<log::Reader> reader;
  s = NewManifestReader(fs, path, &reporter, &reader);
  if (!s.ok()) {
    return s;
  }
  // The reporter is called only from ReadRecord(), so setting status_ptr after
  // NewManifestReader() is safe.
  Status read_status;
  reporter.status_ptr = &read_status;
  Slice record;
  std::string scratch;
  bool first = true;
  bool bad_record = false;
  // The records of the atomic group that is open, and the size of that group,
  // which is the remaining count of its first record plus 1. The group checks
  // are the ones of AtomicGroupReadBuffer::AddEdit(), which the file-mode
  // replay of the new MANIFEST file runs.
  std::vector<std::string> group;
  size_t group_size = 0;
  uint64_t group_next_file_number = 0;
  while (reader->ReadRecord(&record, &scratch)) {
    if (!read_status.ok() || bad_record) {
      return Status::Corruption(
          "Store mode: the MANIFEST file copy has a damaged record before its "
          "end");
    }
    if (first) {
      bool is_marker = false;
      bool sync_marked = false;
      s = DecodeManifestCopyMarker(record, &is_marker, &sync_marked);
      if (!s.ok()) {
        return s;
      }
      if (!is_marker || !sync_marked) {
        // A copy of background mode can miss many commits.
        return Status::InvalidArgument(
            "Store mode: the MANIFEST file copy was not written in sync mode, "
            "so it may miss commits");
      }
      first = false;
      continue;
    }
    VersionEdit edit;
    if (!edit.DecodeFrom(record).ok()) {
      bad_record = true;
      continue;
    }
    const uint64_t edit_next_file_number =
        edit.HasNextFile() ? edit.GetNextFile() : 0;
    if (!edit.IsInAtomicGroup()) {
      if (!group.empty()) {
        return Status::Corruption(
            "Store mode: the MANIFEST file copy has a record inside an atomic "
            "group");
      }
      records->push_back(record.ToString());
      *next_file_number = std::max(*next_file_number, edit_next_file_number);
      continue;
    }
    if (group.empty()) {
      group_size = size_t{edit.GetRemainingEntries()} + 1;
    }
    group.push_back(record.ToString());
    // It also fails when a new group starts while a group is open, unless the
    // remaining counts happen to agree (AtomicGroupReadBuffer has the same
    // limit).
    if (group.size() + edit.GetRemainingEntries() != group_size) {
      return Status::Corruption(
          "Store mode: an atomic group in the MANIFEST file copy has a wrong "
          "number of records");
    }
    group_next_file_number =
        std::max(group_next_file_number, edit_next_file_number);
    if (group.size() == group_size) {
      records->insert(records->end(), group.begin(), group.end());
      group.clear();
      *next_file_number = std::max(*next_file_number, group_next_file_number);
      group_next_file_number = 0;
    }
  }
  // A group that is still open here is dropped. Only a torn tail leaves it,
  // because a failed copy append in sync mode stops the DB, and the next
  // DB::Open() writes a new copy.
  // The last record can also be damaged in a way that the checksum does not
  // show (it does not decode).
  if (!read_status.ok() || bad_record) {
    return Status::Corruption(
        "Store mode: the MANIFEST file copy has a damaged record");
  }
  if (first) {
    return Status::Corruption("Store mode: the MANIFEST file copy is empty");
  }
  return Status::OK();
}

// Sets the authority of the DB row to kFile with a commit, which ends store
// mode in the store. `db_row` is the DB row after the claim.
// `feature_version` is DBOptions::manifest_row_feature_version. The commit
// encodes the DB row with the feature version that the user chose, so that
// every binary that can open the DB can read the row.
Status CommitFileAuthority(ManifestStore* store, const std::string& commit_id,
                           const ManifestDbRowResult& db_row,
                           uint32_t feature_version) {
  ManifestRows rows;
  ManifestDbRowExtraFields extra;
  Status s = PublicRowsToManifestRows(db_row.db_row, {}, &rows, &extra);
  if (!s.ok()) {
    return s;
  }
  if (extra.authority == ManifestAuthority::kFile) {
    return Status::OK();
  }
  extra.authority = ManifestAuthority::kFile;
  std::vector<ManifestRow> public_rows;
  s = ManifestRowsToPublicRows(rows, extra, feature_version, &public_rows);
  if (!s.ok()) {
    return s;
  }
  ManifestCommit commit;
  commit.expected_commit_sequence = db_row.state.commit_sequence;
  commit.commit_id = commit_id;
  // ManifestRowsToPublicRows() puts the DB row first.
  commit.db_row = public_rows.front();
  IOStatus io_s = store->Commit(commit);
  if (!io_s.ok()) {
    return Status::IOError(
        "Store mode: the MANIFEST file holds the metadata now, but setting "
        "the DB row to file mode failed; call RollBackManifestStore() again: " +
        io_s.ToString());
  }
  return Status::OK();
}

// The steps of RollBackManifestStore() after it took the folder lock.
Status RollBackManifestStoreLocked(const DBOptions& db_options,
                                   const std::string& dbname) {
  FileSystem* fs = db_options.env->GetFileSystem().get();
  CurrentManifestInfo current;
  Status s = ReadCurrentManifestInfo(fs, dbname, &current);
  if (!s.ok()) {
    return s;
  }
  if (!current.exists) {
    return Status::InvalidArgument(CurrentFileName(dbname), "does not exist");
  }
  ManifestStore* store = db_options.manifest_store.get();
  ManifestCommitIdGenerator commit_ids(
      DBImpl::GenerateDbSessionId(db_options.env));
  ManifestDbRowResult claimed;
  if (store != nullptr) {
    // The db id of this DB comes from the stub, or for a MANIFEST file from
    // its records, else from the IDENTITY file. Without a db id, the owner
    // of the DB row cannot be checked.
    std::string expected_db_id;
    if (current.is_stub) {
      expected_db_id = current.stub.db_id;
    } else {
      s = experimental::GetDbIdentityFromManifest(
          fs, DescriptorFileName(dbname, current.number), &expected_db_id);
      if (s.IsNotFound()) {
        // The MANIFEST has no db id. A missing IDENTITY file is not an error
        // here; any other read error is returned.
        s = ReadDbIdFromIdentityFile(fs, dbname, &expected_db_id);
      }
      if (!s.ok()) {
        return s;
      }
      if (expected_db_id.empty()) {
        return Status::InvalidArgument(
            "Store mode: neither the MANIFEST file nor the IDENTITY file has "
            "a db id, so the DB row of the store cannot be checked");
      }
    }
    // The claim fences the old primary in the store.
    ManifestStoreOpenOptions opts;
    opts.commit_id = commit_ids.Next();
    s = CheckOwnerAndClaim(store, opts, expected_db_id, &claimed);
    if (s.IsNotFound()) {
      return Status::InvalidArgument("Store mode: the store has no DB row");
    }
    if (!s.ok()) {
      return s;
    }
  }
  if (!current.is_stub) {
    // The MANIFEST file holds the metadata, for example after a rollback
    // without the store. Only the DB row and the copies are left to update;
    // the MANIFEST file is never replaced, so writes since then stay.
    if (store == nullptr) {
      return Status::OK();
    }
    s = CommitFileAuthority(store, commit_ids.Next(), claimed,
                            db_options.manifest_row_feature_version);
    if (s.ok()) {
      s = DeleteManifestCopies(fs, dbname, /*keep_number=*/0,
                               /*delete_pointer=*/true);
    }
    return s;
  }
  if (current.stub.state == ManifestStubState::kCreating) {
    return Status::InvalidArgument(
        "Store mode: creating the DB is not finished; open the DB read-write "
        "to finish it");
  }
  if (current.stub.state == ManifestStubState::kEntering) {
    if (store == nullptr) {
      return Status::InvalidArgument(
          "Store mode: rolling back an unfinished enter needs the store");
    }
    ManifestRows rows;
    ManifestDbRowExtraFields extra;
    s = PublicRowsToManifestRows(claimed.db_row, {}, &rows, &extra);
    if (!s.ok()) {
      return s;
    }
    if (extra.authority == ManifestAuthority::kStore) {
      return Status::InvalidArgument(
          "Store mode: open the DB read-write to finish entering store mode, "
          "then roll back");
    }
    if (extra.authority != ManifestAuthority::kFile) {
      return Status::Corruption(
          "Store mode: an \"entering\" stub MANIFEST, but the DB row has "
          "another authority");
    }
    // The switch did not happen: the MANIFEST file still holds the metadata.
    std::unique_ptr<FSDirectory> dir;
    s = fs->NewDirectory(dbname, IOOptions(), &dir, nullptr);
    if (s.ok()) {
      s = PointCurrentBackFromEnteringStub(
          WriteOptions(), fs, dbname, current,
          db_options.metadata_write_temperature, dir.get(),
          db_options.info_log.get());
    }
    if (s.ok()) {
      s = DeleteManifestCopies(fs, dbname, /*keep_number=*/0,
                               /*delete_pointer=*/true);
    }
    return s;
  }

  // A done stub: write a new MANIFEST file with the full state. With the
  // store, the rows are exact. Without it, only a sync-marked copy is safe.
  std::vector<std::string> records;
  uint64_t next_file_number = 0;
  if (store != nullptr) {
    ManifestStoreScan scan;
    s = ScanManifestStore(store, /*consistent=*/false, &scan);
    if (s.ok() && scan.extra_fields.authority != ManifestAuthority::kStore) {
      s = Status::Corruption(
          "Store mode: a stub MANIFEST, but the DB row is not in store mode");
    }
    if (s.ok()) {
      s = ManifestRowsToRecords(scan.rows, &records);
      next_file_number = scan.rows.db.next_file_number;
    }
  } else {
    s = ReadSyncCopyRecords(fs, dbname, &records, &next_file_number);
  }
  uint64_t largest = 0;
  if (s.ok()) {
    s = LargestFileNumber(fs, dbname, &largest);
  }
  if (!s.ok()) {
    return s;
  }
  // The new MANIFEST number and the next file number are above every file
  // in the folder and at or above the next file number of the store (or,
  // without the store, of the copy), so no file number is used twice.
  const uint64_t manifest_number = std::max(largest + 1, next_file_number);
  VersionEdit next_file;
  next_file.SetNextFile(manifest_number + 1);
  records.emplace_back();
  if (!next_file.EncodeTo(&records.back())) {
    return Status::Corruption("Unable to encode a VersionEdit");
  }
  const ImmutableDBOptions immutable_options(db_options);
  std::unique_ptr<FSDirectory> dir;
  s = fs->NewDirectory(dbname, IOOptions(), &dir, nullptr);
  if (s.ok()) {
    s = WriteManifestRecords(immutable_options, dbname, manifest_number,
                             records, WriteOptions(), dir.get());
  }
  if (s.ok() && store != nullptr) {
    s = CommitFileAuthority(store, commit_ids.Next(), claimed,
                            db_options.manifest_row_feature_version);
    if (s.ok()) {
      // The new MANIFEST file holds the metadata now.
      DeleteStubFile(fs, dbname, current.number, db_options.info_log.get());
      s = DeleteManifestCopies(fs, dbname, /*keep_number=*/0,
                               /*delete_pointer=*/true);
    }
  }
  return s;
}

}  // namespace

Status RollBackManifestStore(const DBOptions& db_options,
                             const std::string& dbname) {
  Env* env = db_options.env;
  FileLock* lock = nullptr;
  Status s = env->LockFile(LockFileName(dbname), &lock);
  if (!s.ok()) {
    return s;
  }
  s = RollBackManifestStoreLocked(db_options, dbname);
  // A failed unlock is the result only when the rollback itself worked, so
  // that it does not hide the first error.
  Status unlock_s = env->UnlockFile(lock);
  if (!unlock_s.ok()) {
    if (s.ok()) {
      s = unlock_s;
    } else {
      ROCKS_LOG_WARN(db_options.info_log.get(),
                     "Store mode: cannot unlock %s after a failed rollback: %s",
                     LockFileName(dbname).c_str(), unlock_s.ToString().c_str());
    }
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
