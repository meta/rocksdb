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
#include <vector>

#include "file/file_util.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "test_util/sync_point.h"
#include "util/stop_watch.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

const std::string kCurrentFileName = "CURRENT";
const std::string kOptionsFileNamePrefix = "OPTIONS-";
const std::string kCompactionProgressFileNamePrefix = "COMPACTION_PROGRESS-";
const std::string kTempFileNameSuffix = "dbtmp";

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
  FileOptions file_opts;
  file_opts.temperature = temp;
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
  FileOptions file_opts;
  file_opts.temperature = temp;
  if (s.ok()) {
    s = WriteStringToFile(env->GetFileSystem().get(), id, tmp,
                          /*should_sync=*/true, opts, file_opts);
  }
  if (s.ok()) {
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
  // if it is not implemented. Detailed explanations can be found in
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
