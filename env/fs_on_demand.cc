// Copyright (c) 2024-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "env/fs_on_demand.h"

#include <algorithm>
#include <set>

#include "file/filename.h"
#include "port/port.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
// Check if the input path is under orig (typically the local directory), and if
// so, change it to the equivalent path under replace (typically the remote
// directory). For example, if orig is "/data/follower", replace is
// "/data/leader", and the given path is "/data/follower/000010.sst", on return
// the path would be changed to
// "/data/leader/000010.sst".
// Return value is true if the path was modified, false otherwise
bool OnDemandFileSystem::CheckPathAndAdjust(const std::string& orig,
                                            const std::string& replace,
                                            std::string& path) {
  size_t pos = path.find(orig);
  if (pos > 0) {
    return false;
  }
  path.replace(pos, orig.length(), replace);
  return true;
}

bool OnDemandFileSystem::LookupFileType(const std::string& name,
                                        FileType* type) {
  std::size_t found = name.find_last_of('/');
  std::string file_name = name.substr(found);
  uint64_t number = 0;
  return ParseFileName(file_name, &number, type);
}

// RocksDB opens non-SST files for reading in sequential file mode. This
// includes CURRENT, OPTIONS, MANIFEST etc. For these files, we open them
// in place in the source directory. For files that are appendable or
// can be renamed, which is MANIFEST and CURRENT files, we wrap the
// underlying FSSequentialFile with another class that checks when EOF
// has been reached and re-opens the file to see the latest data. On some
// distributed file systems, this is necessary.
IOStatus OnDemandFileSystem::NewSequentialFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  FileType type;
  static std::unordered_set<FileType> valid_types(
      {kWalFile, kDescriptorFile, kCurrentFile, kIdentityFile, kOptionsFile});
  if (!LookupFileType(fname, &type) ||
      (valid_types.find(type) == valid_types.end())) {
    return IOStatus::NotSupported();
  }

  IOStatus s;
  std::string rname = fname;
  if (CheckPathAndAdjust(local_path_, remote_path_, rname)) {
    // First clear any local directory cache as it may be out of date
    target()->DiscardCacheForDirectory(rname);

    std::unique_ptr<FSSequentialFile> inner_file;
    s = target()->NewSequentialFile(rname, file_opts, &inner_file, dbg);
    if (s.ok() && type == kDescriptorFile) {
      result->reset(new OnDemandSequentialFile(std::move(inner_file), this,
                                               file_opts, rname));
    } else {
      *result = std::move(inner_file);
    }
  } else {
    s = target()->NewSequentialFile(fname, file_opts, result, dbg);
  }
  return s;
}

// This is only supported for SST files. If the file is present locally,
// i.e in the destination dir, we just open it and return. If its in the
// remote, i.e source dir, we link it locally and open the link.
// TODO: Add support for blob files belonging to the new BlobDB
IOStatus OnDemandFileSystem::NewRandomAccessFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  FileType type;
  if (!LookupFileType(fname, &type) || type != kTableFile) {
    return IOStatus::NotSupported();
  }

  IOStatus s = target()->FileExists(fname, file_opts.io_options, nullptr);
  if (s.IsNotFound() || s.IsPathNotFound()) {
    std::string rname = fname;
    if (CheckPathAndAdjust(local_path_, remote_path_, rname)) {
      // First clear any local directory cache as it may be out of date
      target()->DiscardCacheForDirectory(rname);

      s = target()->LinkFile(rname, fname, IOOptions(), nullptr);
      if (!s.ok()) {
        return s;
      }
    }
  }

  return s.ok() ? target()->NewRandomAccessFile(fname, file_opts, result, dbg)
                : s;
}

// We don't expect to create any writable file other than info LOG files.
IOStatus OnDemandFileSystem::NewWritableFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  FileType type;
  if (!LookupFileType(fname, &type) || type != kInfoLogFile) {
    return IOStatus::NotSupported();
  }

  std::string rname = fname;
  if (CheckPathAndAdjust(local_path_, remote_path_, rname)) {
    // First clear any local directory cache as it may be out of date
    target()->DiscardCacheForDirectory(rname);

    IOStatus s = target()->FileExists(rname, file_opts.io_options, dbg);
    if (s.ok()) {
      return IOStatus::InvalidArgument(
          "Writing to a file present in the remote directory not supoprted");
    }
  }

  return target()->NewWritableFile(fname, file_opts, result, dbg);
}

// Currently not supported, as there's no need for RocksDB to create a
// directory object for a DB in read-only mode.
IOStatus OnDemandFileSystem::NewDirectory(
    const std::string& /*name*/, const IOOptions& /*io_opts*/,
    std::unique_ptr<FSDirectory>* /*result*/, IODebugContext* /*dbg*/) {
  return IOStatus::NotSupported();
}

// Check if the given file exists, either locally or remote. If the file is an
// SST file, then link it locally. We assume if the file existence is being
// checked, its for verification purposes, for example while replaying the
// MANIFEST. The file will be opened for reading  some time in the future.
IOStatus OnDemandFileSystem::FileExists(const std::string& fname,
                                        const IOOptions& options,
                                        IODebugContext* dbg) {
  IOStatus s = target()->FileExists(fname, options, dbg);
  if (!s.IsNotFound() && !s.IsPathNotFound()) {
    return s;
  }

  std::string rname = fname;
  if (CheckPathAndAdjust(local_path_, remote_path_, rname)) {
    // First clear any local directory cache as it may be out of date
    target()->DiscardCacheForDirectory(rname);

    FileType type;
    if (LookupFileType(fname, &type) && type == kTableFile) {
      s = target()->LinkFile(rname, fname, options, dbg);
    } else {
      s = target()->FileExists(rname, options, dbg);
    }
  }
  return s;
}

// Doa listing of both the local and remote directories and merge the two.
IOStatus OnDemandFileSystem::GetChildren(const std::string& dir,
                                         const IOOptions& options,
                                         std::vector<std::string>* result,
                                         IODebugContext* dbg) {
  std::string rdir = dir;
  IOStatus s = target()->GetChildren(dir, options, result, dbg);
  if (!s.ok() || !CheckPathAndAdjust(local_path_, remote_path_, rdir)) {
    return s;
  }

  std::vector<std::string> rchildren;
  // First clear any local directory cache as it may be out of date
  target()->DiscardCacheForDirectory(rdir);
  s = target()->GetChildren(rdir, options, &rchildren, dbg);
  if (s.ok()) {
    std::for_each(rchildren.begin(), rchildren.end(), [&](std::string& name) {
      // Adjust name
      (void)CheckPathAndAdjust(remote_path_, local_path_, name);
    });
    std::sort(result->begin(), result->end());
    std::sort(rchildren.begin(), rchildren.end());

    std::vector<std::string> output;
    output.reserve(result->size() + rchildren.size());
    std::set_union(result->begin(), result->end(), rchildren.begin(),
                   rchildren.end(), std::back_inserter(output));
    *result = std::move(output);
  }
  return s;
}

// Doa listing of both the local and remote directories and merge the two.
IOStatus OnDemandFileSystem::GetChildrenFileAttributes(
    const std::string& dir, const IOOptions& options,
    std::vector<FileAttributes>* result, IODebugContext* dbg) {
  std::string rdir = dir;
  IOStatus s = target()->GetChildrenFileAttributes(dir, options, result, dbg);
  if (!s.ok() || !CheckPathAndAdjust(local_path_, remote_path_, rdir)) {
    return s;
  }

  std::vector<FileAttributes> rchildren;
  // First clear any local directory cache as it may be out of date
  target()->DiscardCacheForDirectory(rdir);
  s = target()->GetChildrenFileAttributes(rdir, options, &rchildren, dbg);
  if (s.ok()) {
    struct FileAttributeSorter {
      bool operator()(const FileAttributes& lhs, const FileAttributes& rhs) {
        return lhs.name < rhs.name;
      }
    } file_attr_sorter;

    std::for_each(
        rchildren.begin(), rchildren.end(), [&](FileAttributes& file) {
          // Adjust name
          (void)CheckPathAndAdjust(remote_path_, local_path_, file.name);
        });
    std::sort(result->begin(), result->end(), file_attr_sorter);
    std::sort(rchildren.begin(), rchildren.end(), file_attr_sorter);

    std::vector<FileAttributes> output;
    output.reserve(result->size() + rchildren.size());
    std::set_union(rchildren.begin(), rchildren.end(), result->begin(),
                   result->end(), std::back_inserter(output), file_attr_sorter);
    *result = std::move(output);
  }
  return s;
}

IOStatus OnDemandFileSystem::GetFileSize(const std::string& fname,
                                         const IOOptions& options,
                                         uint64_t* file_size,
                                         IODebugContext* dbg) {
  uint64_t local_size = 0;
  IOStatus s = target()->GetFileSize(fname, options, &local_size, dbg);
  if (!s.ok() && !s.IsNotFound() && !s.IsPathNotFound()) {
    return s;
  }

  if (s.IsNotFound() || s.IsPathNotFound()) {
    std::string rname = fname;
    if (CheckPathAndAdjust(local_path_, remote_path_, rname)) {
      // First clear any local directory cache as it may be out of date
      target()->DiscardCacheForDirectory(rname);

      FileType type;
      if (LookupFileType(fname, &type) && type == kTableFile) {
        s = target()->LinkFile(rname, fname, options, dbg);
        if (s.ok()) {
          s = target()->GetFileSize(fname, options, &local_size, dbg);
        }
      } else {
        s = target()->GetFileSize(rname, options, &local_size, dbg);
      }
    }
  }
  *file_size = local_size;
  return s;
}

// An implementation of Read that tracks whether we've reached EOF. If so,
// re-open the file to try to read past the previous EOF offset. After
// re-opening, positing it back to the last read offset.
IOStatus OnDemandSequentialFile::Read(size_t n, const IOOptions& options,
                                      Slice* result, char* scratch,
                                      IODebugContext* dbg) {
  IOStatus s;
  if (eof_) {
    // Reopen the file. With some distributed file systems, this is required
    // in order to get the new size
    file_.reset();
    s = fs_->NewSequentialFile(path_, file_opts_, &file_, dbg);
    if (!s.ok()) {
      return IOStatus::IOError("While opening file after relinking, got error ",
                               s.ToString());
    }
    s = file_->Skip(offset_);
    if (!s.ok()) {
      return IOStatus::IOError(
          "While seeking to offset" + std::to_string(offset_) + "got error",
          s.ToString());
    }
    eof_ = false;
  }

  s = file_->Read(n, options, result, scratch, dbg);
  if (s.ok()) {
    offset_ += result->size();
    if (result->size() < n) {
      // We reached EOF. Mark it so we know to relink next time
      eof_ = true;
    }
  }
  return s;
}

IOStatus OnDemandSequentialFile::Skip(uint64_t n) {
  IOStatus s = file_->Skip(n);
  if (s.ok()) {
    offset_ += n;
  }
  return s;
}

bool OnDemandSequentialFile::use_direct_io() const {
  return file_->use_direct_io();
}

size_t OnDemandSequentialFile::GetRequiredBufferAlignment() const {
  return file_->GetRequiredBufferAlignment();
}

Temperature OnDemandSequentialFile::GetTemperature() const {
  return file_->GetTemperature();
}

std::shared_ptr<FileSystem> NewOnDemandFileSystem(
    const std::shared_ptr<FileSystem>& fs, const std::string& src_path,
    const std::string& dest_path) {
  return std::make_shared<OnDemandFileSystem>(fs, src_path, dest_path);
}
}  // namespace ROCKSDB_NAMESPACE
