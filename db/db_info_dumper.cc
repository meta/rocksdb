//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_info_dumper.h"

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <string>
#include <vector>

#include "file/filename.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

void DumpDBFileSummary(const ImmutableDBOptions& options,
                       const std::string& dbname,
                       const std::string& session_id) {
  if (options.info_log == nullptr) {
    return;
  }

  auto* env = options.env;
  uint64_t number = 0;
  FileType type = kInfoLogFile;

  std::vector<std::string> files;
  uint64_t file_num = 0;
  uint64_t file_size;
  std::string file_info;
  std::string wal_info;

  Header(options.info_log, "DB SUMMARY\n");
  {
    std::string hostname;
    if (env->GetHostNameString(&hostname).ok()) {
      Header(options.info_log, "Host name (Env):  %s\n", hostname.c_str());
    }
  }
  Header(options.info_log, "DB Session ID:  %s\n", session_id.c_str());

  Status s;
  // Get files in dbname dir
  s = env->GetChildren(dbname, &files);
  if (!s.ok()) {
    Error(options.info_log, "Error when reading %s dir %s\n", dbname.c_str(),
          s.ToString().c_str());
  }
  std::sort(files.begin(), files.end());
  for (const std::string& file : files) {
    if (!ParseFileName(file, &number, &type)) {
      continue;
    }
    switch (type) {
      case kCurrentFile:
        Header(options.info_log, "CURRENT file:  %s\n", file.c_str());
        break;
      case kIdentityFile:
        Header(options.info_log, "IDENTITY file:  %s\n", file.c_str());
        break;
      case kDescriptorFile:
        s = env->GetFileSize(dbname + "/" + file, &file_size);
        if (s.ok()) {
          Header(options.info_log,
                 "MANIFEST file:  %s size: %" PRIu64 " Bytes\n", file.c_str(),
                 file_size);
        } else {
          Error(options.info_log,
                "Error when reading MANIFEST file: %s/%s %s\n", dbname.c_str(),
                file.c_str(), s.ToString().c_str());
        }
        break;
      case kWalFile:
        s = env->GetFileSize(dbname + "/" + file, &file_size);
        if (s.ok()) {
          wal_info.append(file)
              .append(" size: ")
              .append(std::to_string(file_size))
              .append(" ; ");
        } else {
          Error(options.info_log, "Error when reading LOG file: %s/%s %s\n",
                dbname.c_str(), file.c_str(), s.ToString().c_str());
        }
        break;
      case kTableFile:
        if (++file_num < 10) {
          file_info.append(file).append(" ");
        }
        break;
      case kDBLockFile:
      case kTempFile:
      case kInfoLogFile:
      case kMetaDatabase:
      case kOptionsFile:
      case kBlobFile:
      case kCompactionProgressFile:
      default:
        break;
    }
  }

  // Get sst files in db_path dir
  for (auto& db_path : options.db_paths) {
    if (dbname.compare(db_path.path) != 0) {
      s = env->GetChildren(db_path.path, &files);
      if (s.IsNotFound() || s.IsPathNotFound()) {
        Header(options.info_log,
               "Directory from db_paths/cf_paths does not yet exist: %s\n",
               db_path.path.c_str());
        continue;
      } else if (!s.ok()) {
        Error(options.info_log, "Error when reading %s dir %s\n",
              db_path.path.c_str(), s.ToString().c_str());
        continue;
      }
      std::sort(files.begin(), files.end());
      for (const std::string& file : files) {
        if (ParseFileName(file, &number, &type)) {
          if (type == kTableFile && ++file_num < 10) {
            file_info.append(file).append(" ");
          }
        }
      }
    }
    Header(options.info_log,
           "SST files in %s dir, Total Num: %" PRIu64 ", files: %s\n",
           db_path.path.c_str(), file_num, file_info.c_str());
    file_num = 0;
    file_info.clear();
  }

  // Get wal file in wal_dir
  const auto& wal_dir = options.GetWalDir(dbname);
  bool log_wal_info = true;
  if (!options.IsWalDirSameAsDBPath(dbname)) {
    s = env->GetChildren(wal_dir, &files);
    if (s.IsNotFound() || s.IsPathNotFound()) {
      Header(options.info_log,
             "Write Ahead Log directory does not yet exist: %s\n",
             wal_dir.c_str());
      log_wal_info = false;
    } else if (!s.ok()) {
      Error(options.info_log, "Error when reading wal dir %s: %s\n",
            wal_dir.c_str(), s.ToString().c_str());
      log_wal_info = false;
    }
    wal_info.clear();
    for (const std::string& file : files) {
      if (ParseFileName(file, &number, &type)) {
        if (type == kWalFile) {
          s = env->GetFileSize(wal_dir + "/" + file, &file_size);
          if (s.ok()) {
            wal_info.append(file)
                .append(" size: ")
                .append(std::to_string(file_size))
                .append(" ; ");
          } else {
            Error(options.info_log, "Error when reading LOG file %s/%s %s\n",
                  wal_dir.c_str(), file.c_str(), s.ToString().c_str());
          }
        }
      }
    }
  }
  if (log_wal_info) {
    Header(options.info_log, "Write Ahead Log file in %s: %s\n",
           wal_dir.c_str(), wal_info.c_str());
  }
}
}  // namespace ROCKSDB_NAMESPACE
