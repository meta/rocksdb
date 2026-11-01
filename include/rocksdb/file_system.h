// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// A FileSystem is an interface used by the rocksdb implementation to access
// storage functionality like the filesystem etc.  Callers
// may wish to provide a custom FileSystem object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All FileSystem implementations are safe for concurrent access from
// multiple threads without any external synchronization.
//
// WARNING: Since this is a new interface, it is expected that there will be
// some changes as storage systems are ported over.

#pragma once

#include <stdint.h>

#include <any>
#include <chrono>
#include <cstdarg>
#include <functional>
#include <limits>
#include <memory>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/customizable.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/thread_status.h"

namespace folly {
class IOExecutor;
}

namespace ROCKSDB_NAMESPACE {

class FileLock;
class FSDirectory;
class FSRandomAccessFile;
class FSRandomRWFile;
class FSSequentialFile;
class FSWritableFile;
class Logger;
class Slice;
struct ImmutableDBOptions;
struct MutableDBOptions;
class RateLimiter;
struct ConfigOptions;

using AccessPattern = RandomAccessFile::AccessPattern;
using FileAttributes = Env::FileAttributes;

// DEPRECATED
// Priority of an IO request. This is a hint and does not guarantee any
// particular QoS.
// IO_LOW - Typically background reads/writes such as compaction/flush
// IO_HIGH - Typically user reads/synchronous WAL writes
enum class IOPriority : uint8_t {
  kIOLow,
  kIOHigh,
  kIOTotal,
};

// Type of the data begin read/written. It can be passed down as a flag
// for the FileSystem implementation to optionally handle different types in
// different ways
enum class IOType : uint8_t {
  kData,
  kFilter,
  kIndex,
  kMetadata,
  kWAL,
  kManifest,
  kLog,
  kUnknown,
  kInvalid,
};

// enum representing various operations supported by underlying FileSystem.
// These need to be set in SupportedOps API for RocksDB to use them.
enum FSSupportedOps {
  kAsyncIO,   // Supports async reads
  kFSBuffer,  // Supports handing off the file system allocated read buffer
              // to the caller of Read/MultiRead
  kVerifyAndReconstructRead,  // Supports a higher level of data integrity. See
                              // the verify_and_reconstruct_read flag in
                              // IOOptions.
  kFSPrefetch,                // Supports prefetch operations
};

// Per-request options that can be passed down to the FileSystem
// implementation. These are hints and are not necessarily guaranteed to be
// honored. More hints can be added here in the future to indicate things like
// storage media (HDD/SSD) to be used, replication level etc.
struct IOOptions {
  // Timeout for the operation in microseconds
  std::chrono::microseconds timeout;

  // DEPRECATED
  // Priority - high or low
  IOPriority prio;

  // Priority used to charge rate limiter configured in file system level (if
  // any)
  // Limitation: right now RocksDB internal does not consider this
  // rate_limiter_priority
  Env::IOPriority rate_limiter_priority;

  // Type of data being read/written
  IOType type;

  // EXPERIMENTAL
  // An option map that's opaque to RocksDB. It can be used to implement a
  // custom contract between a FileSystem user and the provider. This is only
  // useful in cases where a RocksDB user directly uses the FileSystem or file
  // object for their own purposes, and wants to pass extra options to APIs
  // such as NewRandomAccessFile and NewWritableFile. Prefer typed fields on
  // IOOptions/FileOptions when a standardized semantic exists.
  std::unordered_map<std::string, std::string> property_bag;

  // Force directory fsync, some file systems like btrfs may skip directory
  // fsync, set this to force the fsync
  bool force_dir_fsync;

  // Can be used by underlying file systems to skip recursing through sub
  // directories and list only files in GetChildren API.
  bool do_not_recurse;

  // Setting this flag indicates a corruption was detected by a previous read,
  // so the caller wants to re-read the data with much stronger data integrity
  // checking and correction, i.e requests the file system to reconstruct the
  // data from redundant copies and verify checksums, if available, in order
  // to have a better chance of success. It is expected that this will have a
  // much higher overhead than a normal read.
  // This is a hint. At a minimum, the file system should implement this flag in
  // FSRandomAccessFile::Read and FSSequentialFile::Read
  // NOTE: The file system must support kVerifyAndReconstructRead in
  // FSSupportedOps, otherwise this feature will not be used.
  bool verify_and_reconstruct_read;

  // EXPERIMENTAL
  Env::IOActivity io_activity = Env::IOActivity::kUnknown;

  IOOptions() : IOOptions(false) {}

  explicit IOOptions(bool force_dir_fsync_)
      : timeout(std::chrono::microseconds::zero()),
        prio(IOPriority::kIOLow),
        rate_limiter_priority(Env::IO_TOTAL),
        type(IOType::kUnknown),
        force_dir_fsync(force_dir_fsync_),
        do_not_recurse(false),
        verify_and_reconstruct_read(false) {}
};

struct DirFsyncOptions {
  enum FsyncReason : uint8_t {
    kNewFileSynced,
    kFileRenamed,
    kDirRenamed,
    kFileDeleted,
    kDefault,
  } reason;

  std::string renamed_new_name;  // for kFileRenamed
  // add other options for other FsyncReason

  DirFsyncOptions();

  explicit DirFsyncOptions(std::string file_renamed_new_name);

  explicit DirFsyncOptions(FsyncReason fsync_reason);
};

// File-scope contracts that describe how RocksDB will open/create and access a
// file. A FileSystem that detects a contract violation must return a non-OK
// status or preserve its normal valid semantics; it must not expose undefined
// or corrupt data.
enum class FileOpenContract : uint8_t {
  kDefault = 0,
  // RocksDB will not call ReopenWritableFile for this file to overwrite,
  // append, or truncate data. SyncFile is a separate durability API.
  kNoReopenForWrite = 1 << 0,
  // RocksDB will not open readers while this file is open for write.
  kNoReadersWhileOpenForWrite = 1 << 1,
};

constexpr FileOpenContract operator|(FileOpenContract lhs,
                                     FileOpenContract rhs) {
  return static_cast<FileOpenContract>(static_cast<uint8_t>(lhs) |
                                       static_cast<uint8_t>(rhs));
}

inline FileOpenContract& operator|=(FileOpenContract& lhs,
                                    FileOpenContract rhs) {
  lhs = lhs | rhs;
  return lhs;
}

constexpr bool HasFileOpenContract(FileOpenContract contracts,
                                   FileOpenContract contract) {
  return (static_cast<uint8_t>(contracts) & static_cast<uint8_t>(contract)) ==
         static_cast<uint8_t>(contract);
}

struct FileOptions : EnvOptions {
  // Embedded IOOptions to control the parameters for any IOs that need
  // to be issued for the file open/creation
  IOOptions io_options;

  // EXPERIMENTAL
  // The feature is in development and is subject to change.
  // When creating a new file, set the temperature of the file so that
  // underlying file systems can put it with appropriate storage media and/or
  // coding.
  Temperature temperature = Temperature::kUnknown;

  // File-open contract. kDefault uses the provider's standard semantics.
  FileOpenContract open_contract = FileOpenContract::kDefault;

  // The checksum type that is used to calculate the checksum value for
  // handoff during file writes.
  ChecksumType handoff_checksum_type;

  // Expose write lifetime hint on the FileOptions level to provide more
  // flexibility in setting the hint in downstream, custom implementations
  // that might be able to process the hint only at the time of the actual
  // FSWritableFile object creation.
  Env::WriteLifeTimeHint write_hint = Env::WLTH_NOT_SET;

  // File checksum of the file being opened. Empty string if no checksum is
  // available.
  std::string file_checksum;

  // Name of the checksum function used to compute file_checksum. Set to
  // kUnknownFileChecksumFuncName when file was created without a checksum
  // factory. Set to kNoFileChecksumFuncName when no checksum metadata is
  // available.
  // Production FileSystems will accept empty values for both
  // file_checksum and file_checksum_func_name, but internally within RocksDB
  // that is forbidden for checking/auditing purposes.
  std::string file_checksum_func_name;

  // EXPERIMENTAL
  // This is used to pass file metadata that can be used by the file system
  // to accelerate file opening. The content is opaque to RocksDB and is
  // left to the file system to interpret. This is especially useful in the
  // case of remote file systems to avoid expensive RPCs to retrieve the
  // metadata.
  std::string* file_metadata = nullptr;

  FileOptions() : EnvOptions(), handoff_checksum_type(ChecksumType::kCRC32c) {}

  FileOptions(const DBOptions& opts)
      : EnvOptions(opts),
        temperature(opts.metadata_write_temperature),
        handoff_checksum_type(ChecksumType::kCRC32c) {}

  FileOptions(const EnvOptions& opts)
      : EnvOptions(opts), handoff_checksum_type(ChecksumType::kCRC32c) {}

  FileOptions(const FileOptions& opts)
      : EnvOptions(opts),
        io_options(opts.io_options),
        temperature(opts.temperature),
        open_contract(opts.open_contract),
        handoff_checksum_type(opts.handoff_checksum_type),
        write_hint(opts.write_hint),
        file_checksum(opts.file_checksum),
        file_checksum_func_name(opts.file_checksum_func_name),
        file_metadata(opts.file_metadata) {}

  FileOptions& operator=(const FileOptions&) = default;
  FileOptions(FileOptions&&) = default;
  FileOptions& operator=(FileOptions&&) = default;
  ~FileOptions() = default;
};

// A structure to pass back some debugging information from the FileSystem
// implementation to RocksDB in case of an IO error
// TODO(virajthakur): Update all calls to FS APIs for writes to pass in
// IODebugContext
struct IODebugContext {
  // file_path to be filled in by RocksDB in case of an error
  std::string file_path;

  // A map of counter names to values - set by the FileSystem implementation
  std::map<std::string, uint64_t> counters;

  // To be set by the FileSystem implementation
  std::string msg;

  // To be set by the application, to allow tracing logs/metrics from user ->
  // RocksDB -> FS.
  const std::string* request_id = nullptr;

  // In order to log required information in IO tracing for different
  // operations, Each bit in trace_data stores which corresponding info from
  // IODebugContext will be added in the trace. Foreg, if trace_data = 1, it
  // means bit at position 0 is set so TraceData::kRequestID (request_id) will
  // be logged in the trace record.
  //
  enum TraceData : char {
    // The value of each enum represents the bitwise position for
    // that information in trace_data which will be used by IOTracer for
    // tracing. Make sure to add them sequentially.
    kRequestID = 0,
  };
  uint64_t trace_data = 0;

  // Arbitrary structure containing cost information about the IO request
  std::any cost_info;

  // FileSystem implementations can use this mutex to synchronize concurrent
  // reads/writes as needed (e.g. to update the counters or cost_info field)
  std::shared_mutex mutex;

  IODebugContext() {}

  // Copy constructor
  IODebugContext(const IODebugContext& other)
      : file_path(other.file_path),
        counters(other.counters),
        msg(other.msg),
        trace_data(other.trace_data),
        cost_info(other.cost_info),
        _request_id(other.request_id ? *other.request_id : "") {
    request_id = other.request_id ? &_request_id : nullptr;
  }

  // Copy assignment operator
  IODebugContext& operator=(const IODebugContext& other) {
    if (this != &other) {
      file_path = other.file_path;
      counters = other.counters;
      msg = other.msg;
      trace_data = other.trace_data;
      cost_info = other.cost_info;
      _request_id = other.request_id ? *other.request_id : "";
      request_id = other.request_id ? &_request_id : nullptr;
    }
    return *this;
  }

  void AddCounter(std::string& name, uint64_t value) {
    counters.emplace(name, value);
  }

  // Called by underlying file system to set request_id and log request_id in
  // IOTracing.
  void SetRequestId(const std::string* updated_request_id) {
    request_id = updated_request_id;
    trace_data |= (1 << TraceData::kRequestID);
  }

  std::string ToString() {
    std::ostringstream ss;
    ss << file_path << ", ";
    for (auto counter : counters) {
      ss << counter.first << " = " << counter.second << ",";
    }
    ss << msg;
    return ss.str();
  }

 private:
  // Private member that allows for safe copying of IODebugContext without any
  // memory ownership issues. After copying, request_id can point directly to
  // this field.
  std::string _request_id;
};

// A function pointer type for custom destruction of void pointer passed to
// ReadAsync API. RocksDB/caller is responsible for deleting the void pointer
// allocated by FS in ReadAsync API.
using IOHandleDeleter = std::function<void(void*)>;

// The FileSystem, FSSequentialFile, FSRandomAccessFile, FSWritableFile,
// FSRandomRWFileclass, and FSDIrectory classes define the interface between
// RocksDB and storage systems, such as Posix filesystems,
// remote filesystems etc.
// The interface allows for fine grained control of individual IO operations,
// such as setting a timeout, prioritization, hints on data placement,
// different handling based on type of IO etc.
// This is accomplished by passing an instance of IOOptions to every
// API call that can potentially perform IO. Additionally, each such API is
// passed a pointer to a IODebugContext structure that can be used by the
// storage system to include troubleshooting information. The return values
// of the APIs is of type IOStatus, which can indicate an error code/sub-code,
// as well as metadata about the error such as its scope and whether its
// retryable.
// NewCompositeEnv can be used to create an Env with a custom FileSystem for
// DBOptions::env.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class FileSystem : public Customizable {
 public:
  FileSystem();

  // No copying allowed
  FileSystem(const FileSystem&) = delete;
  FileSystem(FileSystem&&) = delete;

  ~FileSystem() override;

  static const char* Type() { return "FileSystem"; }
  static const char* kDefaultName() { return "DefaultFileSystem"; }

  // Loads the FileSystem specified by the input value into the result
  // @see Customizable for a more detailed description of the parameters and
  // return codes
  // @param config_options Controls how the FileSystem is loaded
  // @param value The name and optional properties describing the file system
  //      to load.
  // @param result On success, returns the loaded FileSystem
  // @return OK if the FileSystem was successfully loaded.
  // @return not-OK if the load failed.
  static Status CreateFromString(const ConfigOptions& options,
                                 const std::string& value,
                                 std::shared_ptr<FileSystem>* result);

  // Return a default FileSystem suitable for the current operating
  // system.
  static std::shared_ptr<FileSystem> Default();

  // Handles the event when a new DB or a new ColumnFamily starts using the
  // specified data paths.
  //
  // The data paths might be shared by different DBs or ColumnFamilies,
  // so RegisterDbPaths might be called with the same data paths.
  // For example, when CreateColumnFamily is called multiple times with the same
  // data path, RegisterDbPaths will also be called with the same data path.
  //
  // If the return status is ok, then the paths must be correspondingly
  // called in UnregisterDbPaths;
  // otherwise this method should have no side effect, and UnregisterDbPaths
  // do not need to be called for the paths.
  //
  // Different implementations may take different actions.
  // By default, it's a no-op and returns Status::OK.
  virtual Status RegisterDbPaths(const std::vector<std::string>& /*paths*/) {
    return Status::OK();
  }
  // Handles the event a DB or a ColumnFamily stops using the specified data
  // paths.
  //
  // It should be called corresponding to each successful RegisterDbPaths.
  //
  // Different implementations may take different actions.
  // By default, it's a no-op and returns Status::OK.
  virtual Status UnregisterDbPaths(const std::vector<std::string>& /*paths*/) {
    return Status::OK();
  }

  // EXPERIMENTAL
  //
  // Used in the RocksDB coroutine query interface. Coroutine-based read
  // requests will run on an EventBase from this executor. Returns nullptr when
  // coroutine read execution is not supported. When called from one of its
  // EventBases, the executor's getEventBase() must return that EventBase.
  virtual folly::IOExecutor* GetReadExecutor() { return nullptr; }

  // Increase the maximum number of threads used by the FileSystem-owned read
  // IO executor to at least the requested number. The executor is not reduced
  // when its current maximum is larger. The number must be positive.
  virtual void SetReadIOExecutorThreads(int /*number*/) {}

  // Create a brand new sequentially-readable file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure stores nullptr in *result and returns non-OK.  If the file does
  // not exist, returns a non-OK status.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual IOStatus NewSequentialFile(const std::string& fname,
                                     const FileOptions& file_opts,
                                     std::unique_ptr<FSSequentialFile>* result,
                                     IODebugContext* dbg) = 0;

  // Create a brand new random access read-only file with the
  // specified name.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.  If the file does not exist, returns a non-OK
  // status.
  //
  // The returned file may be concurrently accessed by multiple threads.
  virtual IOStatus NewRandomAccessFile(
      const std::string& fname, const FileOptions& file_opts,
      std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) = 0;
  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual IOStatus NewWritableFile(const std::string& fname,
                                   const FileOptions& file_opts,
                                   std::unique_ptr<FSWritableFile>* result,
                                   IODebugContext* dbg) = 0;

  // Create an object that writes to a file with the specified name.
  // `FSWritableFile::Append()`s will append after any existing content.  If the
  // file does not already exist, creates it.
  //
  // On success, stores a pointer to the file in *result and returns OK.  On
  // failure stores nullptr in *result and returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual IOStatus ReopenWritableFile(
      const std::string& /*fname*/, const FileOptions& /*options*/,
      std::unique_ptr<FSWritableFile>* /*result*/, IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("ReopenWritableFile");
  }

  // Reuse an existing file by renaming it and opening it as writable.
  virtual IOStatus ReuseWritableFile(const std::string& fname,
                                     const std::string& old_fname,
                                     const FileOptions& file_opts,
                                     std::unique_ptr<FSWritableFile>* result,
                                     IODebugContext* dbg);

  // Open `fname` for random read and write, if file doesn't exist the file
  // will not be created.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual IOStatus NewRandomRWFile(const std::string& /*fname*/,
                                   const FileOptions& /*options*/,
                                   std::unique_ptr<FSRandomRWFile>* /*result*/,
                                   IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported(
        "RandomRWFile is not implemented in this FileSystem");
  }

  // Opens `fname` as a memory-mapped file for read and write (in-place updates
  // only, i.e., no appends). On success, stores a raw buffer covering the whole
  // file in `*result`. The file must exist prior to this call.
  virtual IOStatus NewMemoryMappedFileBuffer(
      const std::string& /*fname*/,
      std::unique_ptr<MemoryMappedFileBuffer>* /*result*/) {
    return IOStatus::NotSupported(
        "MemoryMappedFileBuffer is not implemented in this FileSystem");
  }

  // Create an object that represents a directory. Will fail if directory
  // doesn't exist. If the directory exists, it will open the directory
  // and create a new Directory object.
  //
  // On success, stores a pointer to the new Directory in
  // *result and returns OK. On failure stores nullptr in *result and
  // returns non-OK.
  virtual IOStatus NewDirectory(const std::string& name,
                                const IOOptions& io_opts,
                                std::unique_ptr<FSDirectory>* result,
                                IODebugContext* dbg) = 0;

  // Returns OK if the named file exists.
  //         NotFound if the named file does not exist,
  //                  the calling process does not have permission to determine
  //                  whether this file exists, or if the path is invalid.
  //         IOError if an IO Error was encountered
  virtual IOStatus FileExists(const std::string& fname,
                              const IOOptions& options,
                              IODebugContext* dbg) = 0;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  // Returns OK if "dir" exists and "*result" contains its children.
  //         NotFound if "dir" does not exist, the calling process does not have
  //                  permission to access "dir", or if "dir" is invalid.
  //         IOError if an IO Error was encountered
  virtual IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                               std::vector<std::string>* result,
                               IODebugContext* dbg) = 0;

  // Store in *result the attributes of the children of the specified directory.
  // In case the implementation lists the directory prior to iterating the files
  // and files are concurrently deleted, the deleted files will be omitted from
  // result.
  // The name attributes are relative to "dir".
  // Original contents of *results are dropped.
  // Returns OK if "dir" exists and "*result" contains its children.
  //         NotFound if "dir" does not exist, the calling process does not have
  //                  permission to access "dir", or if "dir" is invalid.
  //         IOError if an IO Error was encountered
  virtual IOStatus GetChildrenFileAttributes(
      const std::string& dir, const IOOptions& options,
      std::vector<FileAttributes>* result, IODebugContext* dbg) {
    assert(result != nullptr);
    std::vector<std::string> child_fnames;
    IOStatus s = GetChildren(dir, options, &child_fnames, dbg);
    if (!s.ok()) {
      return s;
    }
    result->resize(child_fnames.size());
    size_t result_size = 0;
    for (size_t i = 0; i < child_fnames.size(); ++i) {
      const std::string path = dir + "/" + child_fnames[i];
      if (!(s = GetFileSize(path, options, &(*result)[result_size].size_bytes,
                            dbg))
               .ok()) {
        if (FileExists(path, options, dbg).IsNotFound()) {
          // The file may have been deleted since we listed the directory
          continue;
        }
        return s;
      }
      (*result)[result_size].name = std::move(child_fnames[i]);
      result_size++;
    }
    result->resize(result_size);
    return IOStatus::OK();
  }

// This seems to clash with a macro on Windows, so #undef it here
#ifdef DeleteFile  // ODR-SAFE
#undef DeleteFile
#endif
  // Delete the named file.
  virtual IOStatus DeleteFile(const std::string& fname,
                              const IOOptions& options,
                              IODebugContext* dbg) = 0;

  // Truncate the named file to the specified size.
  virtual IOStatus Truncate(const std::string& /*fname*/, size_t /*size*/,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported(
        "Truncate is not supported for this FileSystem");
  }

  // Create the specified directory. Returns error if directory exists.
  virtual IOStatus CreateDir(const std::string& dirname,
                             const IOOptions& options, IODebugContext* dbg) = 0;

  // Creates directory if missing. Return Ok if it exists, or successful in
  // Creating.
  virtual IOStatus CreateDirIfMissing(const std::string& dirname,
                                      const IOOptions& options,
                                      IODebugContext* dbg) = 0;

  // Delete the specified directory.
  virtual IOStatus DeleteDir(const std::string& dirname,
                             const IOOptions& options, IODebugContext* dbg) = 0;

  // Store the size of fname in *file_size.
  virtual IOStatus GetFileSize(const std::string& fname,
                               const IOOptions& options, uint64_t* file_size,
                               IODebugContext* dbg) = 0;

  // Store the last modification time of fname in *file_mtime.
  virtual IOStatus GetFileModificationTime(const std::string& fname,
                                           const IOOptions& options,
                                           uint64_t* file_mtime,
                                           IODebugContext* dbg) = 0;
  // Rename file src to target.
  virtual IOStatus RenameFile(const std::string& src, const std::string& target,
                              const IOOptions& options,
                              IODebugContext* dbg) = 0;

  // Hard Link file src to target.
  virtual IOStatus LinkFile(const std::string& /*src*/,
                            const std::string& /*target*/,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported(
        "LinkFile is not supported for this FileSystem");
  }

  // Syncs file data and, when requested, file metadata for `fname`.
  //
  // The default implementation reopens the file as writable, calls Sync() or
  // Fsync(), then closes it. Filesystems that already make file contents
  // durable on file flush/close can override this as a no-op to avoid the
  // reopen overhead.
  //
  // RocksDB code that needs to sync a named file should use this API instead of
  // hand-rolling ReopenWritableFile()+Sync()/Fsync(). This lets filesystems
  // reject ReopenWritableFile() for post-close data writes while still
  // providing a cleaner path-level sync implementation.
  virtual IOStatus SyncFile(const std::string& fname,
                            const FileOptions& file_opts,
                            const IOOptions& io_opts, bool use_fsync,
                            IODebugContext* dbg);

  virtual IOStatus NumFileLinks(const std::string& /*fname*/,
                                const IOOptions& /*options*/,
                                uint64_t* /*count*/, IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported(
        "Getting number of file links is not supported for this FileSystem");
  }

  virtual IOStatus AreFilesSame(const std::string& /*first*/,
                                const std::string& /*second*/,
                                const IOOptions& /*options*/, bool* /*res*/,
                                IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported(
        "AreFilesSame is not supported for this FileSystem");
  }

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  virtual IOStatus LockFile(const std::string& fname, const IOOptions& options,
                            FileLock** lock, IODebugContext* dbg) = 0;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  virtual IOStatus UnlockFile(FileLock* lock, const IOOptions& options,
                              IODebugContext* dbg) = 0;

  // *path is set to a temporary directory that can be used for testing. It may
  // or many not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  virtual IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                                    IODebugContext* dbg) = 0;

  // Create and returns a default logger (an instance of EnvLogger) for storing
  // informational messages. Derived classes can override to provide custom
  // logger.
  virtual IOStatus NewLogger(const std::string& fname, const IOOptions& io_opts,
                             std::shared_ptr<Logger>* result,
                             IODebugContext* dbg);

  // Get full directory name for this db.
  virtual IOStatus GetAbsolutePath(const std::string& db_path,
                                   const IOOptions& options,
                                   std::string* output_path,
                                   IODebugContext* dbg) = 0;

  // Sanitize the FileOptions. Typically called by a FileOptions/EnvOptions
  // copy constructor
  virtual void SanitizeFileOptions(FileOptions* /*opts*/) const {}

  // OptimizeForLogRead will create a new FileOptions object that is a copy of
  // the FileOptions in the parameters, but is optimized for reading log files.
  virtual FileOptions OptimizeForLogRead(const FileOptions& file_options) const;

  // OptimizeForManifestRead will create a new FileOptions object that is a copy
  // of the FileOptions in the parameters, but is optimized for reading manifest
  // files.
  virtual FileOptions OptimizeForManifestRead(
      const FileOptions& file_options) const;

  // OptimizeForLogWrite will create a new FileOptions object that is a copy of
  // the FileOptions in the parameters, but is optimized for writing log files.
  // Default implementation returns the copy of the same object.
  virtual FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                          const DBOptions& db_options) const;

  // OptimizeForManifestWrite will create a new FileOptions object that is a
  // copy of the FileOptions in the parameters, but is optimized for writing
  // manifest files. Default implementation returns the copy of the same
  // object.
  virtual FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const;

  // OptimizeForCompactionTableWrite will create a new FileOptions object that
  // is a copy of the FileOptions in the parameters, but is optimized for
  // writing table files.
  virtual FileOptions OptimizeForCompactionTableWrite(
      const FileOptions& file_options,
      const ImmutableDBOptions& immutable_ops) const;

  // OptimizeForCompactionTableRead will create a new FileOptions object that
  // is a copy of the FileOptions in the parameters, but is optimized for
  // reading table files.
  virtual FileOptions OptimizeForCompactionTableRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const;

  // OptimizeForBlobFileRead will create a new FileOptions object that
  // is a copy of the FileOptions in the parameters, but is optimized for
  // reading blob files.
  virtual FileOptions OptimizeForBlobFileRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const;

// This seems to clash with a macro on Windows, so #undef it here
#ifdef GetFreeSpace  // ODR-SAFE
#undef GetFreeSpace
#endif

  // Get the amount of free disk space
  virtual IOStatus GetFreeSpace(const std::string& /*path*/,
                                const IOOptions& /*options*/,
                                uint64_t* /*diskfree*/,
                                IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("GetFreeSpace");
  }

  virtual IOStatus IsDirectory(const std::string& /*path*/,
                               const IOOptions& options, bool* is_dir,
                               IODebugContext* /*dgb*/) = 0;

  // Poll for completion of read IO requests. The Poll() method should call the
  // callback functions to indicate completion of read requests.
  // Underlying FS is required to support Poll API. Poll implementation should
  // ensure that the callback gets called at IO completion, and return only
  // after the callback has been called.
  // If Poll returns partial results for any reads, its caller reponsibility to
  // call Read or ReadAsync in order to get the remaining bytes.
  virtual IOStatus Poll(std::vector<void*>& /*io_handles*/,
                        size_t /*min_completions*/) {
    return IOStatus::OK();
  }

  // Abort the read IO requests submitted asynchronously. Underlying FS is
  // required to support AbortIO API. AbortIO implementation should ensure that
  // the all the read requests related to io_handles should be aborted and
  // it should call the callback for these io_handles.
  virtual IOStatus AbortIO(std::vector<void*>& /*io_handles*/) {
    return IOStatus::OK();
  }

  // EXPERIMENTAL
  // Discard any directory metadata cached in memory for the specified
  // directory and its descendants. Useful for distributed file systems
  // where the local cache may be out of sync with the actual directory state.
  //
  // The implementation is not required to be thread safe. Its the caller's
  // responsibility to ensure that no directory operations happen
  // concurrently.
  virtual void DiscardCacheForDirectory(const std::string& /*path*/) {}

  // Indicates to upper layers which FileSystem operations mentioned in
  // FSSupportedOps are supported by underlying FileSystem. Each bit in
  // supported_ops argument represent corresponding FSSupportedOps operation.
  // Foreg:
  //  If async_io is supported by the underlying FileSystem, then supported_ops
  //  will have corresponding bit (i.e FSSupportedOps::kAsyncIO) set to 1.
  //
  // By default, async_io and prefetch operation are set and FS should override
  // this API and set all the operations they support provided in FSSupportedOps
  // (including async_io and prefetch).
  virtual void SupportedOps(int64_t& supported_ops) {
    supported_ops = 0;
    supported_ops |= (1 << FSSupportedOps::kAsyncIO);
    supported_ops |= (1 << FSSupportedOps::kFSPrefetch);
  }

  // Returns false if this FileSystem never allocates space beyond a file's
  // logical size (for example, via FSWritableFile::Allocate() or
  // PrepareWrite()). RocksDB may then skip work whose only purpose is to
  // release preallocated space, such as reopening and truncating the last
  // WAL file during DB open. This should reflect the capability of the
  // FileSystem rather than any per-file options or build configuration,
  // since files may have been created by a prior process with different
  // options or a different build. Defaults to true.
  virtual bool IsPreallocationSupported() { return true; }

  // If you're adding methods here, remember to add them to EnvWrapper too.

 private:
  void operator=(const FileSystem&);
  void operator=(FileSystem&&) = delete;
};

// A file abstraction for reading sequentially through a file
class FSSequentialFile {
 public:
  FSSequentialFile() {}

  virtual ~FSSequentialFile() = default;

  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // After call, result->size() < n only if end of file has been
  // reached (or non-OK status). Read might fail if called again after
  // first result->size() < n.
  //
  // REQUIRES: External synchronization
  virtual IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                        char* scratch, IODebugContext* dbg) = 0;

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  virtual IOStatus Skip(uint64_t n) = 0;

  // Indicates the upper layers if the current SequentialFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  virtual IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) {
    return IOStatus::NotSupported("InvalidateCache not supported.");
  }

  // Positioned Read for direct I/O
  // If Direct I/O enabled, offset, n, and scratch should be properly aligned
  virtual IOStatus PositionedRead(uint64_t /*offset*/, size_t /*n*/,
                                  const IOOptions& /*options*/,
                                  Slice* /*result*/, char* /*scratch*/,
                                  IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("PositionedRead");
  }

  // EXPERIMENTAL
  // When available, returns the actual temperature for the file. This is
  // useful in case some outside process moves a file from one tier to another,
  // though the temperature is generally expected not to change while a file is
  // open.
  virtual Temperature GetTemperature() const { return Temperature::kUnknown; }

  // If you're adding methods here, remember to add them to
  // SequentialFileWrapper too.
};

using FSAllocationPtr = std::unique_ptr<void, std::function<void(void*)>>;

// A read IO request structure for use in MultiRead and asynchronous Read APIs.
struct FSReadRequest {
  // Input parameter that represents the file offset in bytes.
  uint64_t offset;

  // Input parameter that represents the length to read in bytes. `result` only
  // returns fewer bytes if end of file is hit (or `status` is not OK).
  size_t len;

  // A buffer that MultiRead() can optionally place data in. It can
  // ignore this and allocate its own buffer.
  // The lifecycle of scratch will be until IO is completed.
  //
  // In case of asynchronous reads, its an output parameter and it will be
  // maintained until callback has been called. Scratch is allocated by RocksDB
  // and will be passed to underlying FileSystem.
  char* scratch;

  // Output parameter set by MultiRead() to point to the start of the data
  // buffer.
  //
  // When FSReadRequest::scratch is provided, this should point to
  // FSReadRequest::scratch. When FSSupportedOps::kFSBuffer is enabled and
  // FSReadRequest::scratch is nullptr, this points to the start of the
  // data buffer allocated by the FileSystem.
  //
  // WARNING: Even with the FSSupportedOps::kFSBuffer optimization, you must
  // still use result.data() to get the start of the actual data that was read.
  // Do NOT treat FSReadRequest::fs_scratch as a char* to the start of a valid
  // data buffer.
  Slice result;

  // Output parameter set by underlying FileSystem that represents status of
  // read request.
  IOStatus status;

  // fs_scratch is a unique pointer to an arbitrary object allocated by the
  // underlying FileSystem.
  //
  // Instead of having the FileSystem spend CPU cycles copying data into the
  // FSReadRequest::scratch buffer provided by RocksDB, RocksDB can directly use
  // a buffer allocated by the FileSystem.
  //
  // This optimization is enabled for MultiReads (sync and async) with non
  // direct io, when these conditions hold:
  // 1. The FileSystem has overriden the SupportedOps() API and set
  // FSSupportedOps::kFSBuffer.
  // 2. FSReadRequest::scratch is set to nullptr.
  //
  // RocksDB will:
  // 1. Reuse the buffer allocated by the FileSystem.
  // 2. Take ownership of the object managed by fs_scratch.
  // 3. Handle invoking the custom deleter function from the FSAllocationPtr.
  //
  // WARNING: Do NOT assume that fs_scratch points to the start of the actual
  // char* data returned by the read. As the type signature suggests, fs_scratch
  // is a pointer to any arbitrary data type. Use result.data() to get a valid
  // start to the real data. See https://github.com/meta/rocksdb/pull/13189
  // for more context.
  FSAllocationPtr fs_scratch;
};

// A file abstraction for randomly reading the contents of a file.
class FSRandomAccessFile {
 public:
  FSRandomAccessFile() {}

  virtual ~FSRandomAccessFile() = default;

  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
  // to the data that was read (including if fewer than "n" bytes were
  // successfully read).  May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used.  If an error was encountered, returns a non-OK
  // status.
  //
  // After call, result->size() < n only if end of file has been
  // reached (or non-OK status). Read might fail if called again after
  // first result->size() < n.
  //
  // Safe for concurrent use by multiple threads.
  // If Direct I/O enabled, offset, n, and scratch should be aligned properly.
  virtual IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                        Slice* result, char* scratch,
                        IODebugContext* dbg) const = 0;

  // Readahead the file starting from offset by n bytes for caching.
  // If it's not implemented (default: `NotSupported`), RocksDB will create
  // internal prefetch buffer to improve read performance.
  virtual IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("Prefetch");
  }

  // Read a bunch of blocks as described by reqs. The blocks can
  // optionally be read in parallel. This is a synchronous call, i.e it
  // should return after all reads have completed. The reads will be
  // non-overlapping but can be in any order. If the function return Status
  // is not ok, status of individual requests will be ignored and return
  // status will be assumed for all read requests. The function return status
  // is only meant for errors that occur before processing individual read
  // requests.
  virtual IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                             const IOOptions& options, IODebugContext* dbg) {
    assert(reqs != nullptr);
    for (size_t i = 0; i < num_reqs; ++i) {
      FSReadRequest& req = reqs[i];
      req.status =
          Read(req.offset, req.len, options, &req.result, req.scratch, dbg);
    }
    return IOStatus::OK();
  }

  // Tries to get an unique ID for this file that will be the same each time
  // the file is opened (and will stay the same while the file is open).
  // Furthermore, it tries to make this ID at most "max_size" bytes. If such an
  // ID can be created this function returns the length of the ID and places it
  // in "id"; otherwise, this function returns 0, in which case "id"
  // may not have been modified.
  //
  // This function guarantees, for IDs from a given environment, two unique ids
  // cannot be made equal to each other by adding arbitrary bytes to one of
  // them. That is, no unique ID is the prefix of another.
  //
  // This function guarantees that the returned ID will not be interpretable as
  // a single varint.
  //
  // Note: these IDs are only valid for the duration of the process.
  virtual size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
    return 0;  // Default implementation to prevent issues with backwards
               // compatibility.
  }

  enum AccessPattern { kNormal, kRandom, kSequential, kWillNeed, kWontNeed };

  virtual void Hint(AccessPattern /*pattern*/) {}

  // Indicates the upper layers if the current RandomAccessFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  virtual IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) {
    return IOStatus::NotSupported("InvalidateCache not supported.");
  }

  // This API reads the requested data in FSReadRequest asynchronously. This is
  // a asynchronous call, i.e it should return after submitting the request.
  //
  // When the read request is completed, callback function specified in cb
  // should be called with arguments cb_arg and the result populated in
  // FSReadRequest with result and status fileds updated by FileSystem.
  // cb_arg should be used by the callback to track the original request
  // submitted.
  //
  // This API should also populate io_handle which should be used by
  // underlying FileSystem to store the context in order to distinguish the read
  // requests at their side and provide the custom deletion function in del_fn.
  // RocksDB guarantees that the del_fn for io_handle will be called after
  // receiving the callback. Furthermore, RocksDB guarantees that if it calls
  // the Poll API for this io_handle, del_fn will be called after the Poll
  // returns. RocksDB is responsible for managing the lifetime of io_handle.
  //
  // req contains the request offset and size passed as input parameter of read
  // request and result and status fields are output parameter set by underlying
  // FileSystem. The data should always be read into scratch field.
  //
  // How to enable:
  // In order to enable ReadAsync, FS needs to override SupportedOps() API and
  // set FSSupportedOps::kAsyncIO in SupportedOps() as:
  //  {
  //    supported_ops |= (1 << FSSupportedOps::kAsyncIO);
  //  }
  //
  // Note: If FS supports ReadAsync API, it should also override Poll and
  // AbortIO API.
  //
  // Default implementation is to read the data synchronously.
  virtual IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                             std::function<void(FSReadRequest&, void*)> cb,
                             void* cb_arg, void** /*io_handle*/,
                             IOHandleDeleter* /*del_fn*/, IODebugContext* dbg) {
    req.status =
        Read(req.offset, req.len, opts, &(req.result), req.scratch, dbg);
    cb(req, cb_arg);
    return IOStatus::OK();
  }

  // EXPERIMENTAL
  // Submit a read for RocksDB coroutine queries. The implementation owns the
  // completion executor and must update req.status and invoke cb(req) exactly
  // once.
  //
  // Returns true when the asynchronous path is used and false when the request
  // is completed using the synchronous Read fallback.
  //
  // All arguments and any memory they reference remain valid until cb is
  // invoked.
  //
  // Default implementation reads the data synchronously and invokes the
  // callback before returning. It uses MultiRead() when scratch is nullptr so
  // implementations supporting kFSBuffer can provide an allocated buffer.
  virtual bool SubmitReadAsync(FSReadRequest& req, const IOOptions& opts,
                               std::function<void(FSReadRequest&)> cb,
                               IODebugContext* dbg) {
    if (req.scratch == nullptr) {
      IOStatus status = MultiRead(&req, /*num_reqs=*/1, opts, dbg);
      if (!status.ok()) {
        req.status = std::move(status);
      }
    } else {
      req.status =
          Read(req.offset, req.len, opts, &(req.result), req.scratch, dbg);
    }
    cb(req);
    return false;
  }

  // EXPERIMENTAL
  // When available, returns the actual temperature for the file. This is
  // useful in case some outside process moves a file from one tier to another,
  // though the temperature is generally expected not to change while a file is
  // open.
  virtual Temperature GetTemperature() const { return Temperature::kUnknown; }

  // Get the file size on an open-for-reading file without re-seeking the file's
  // path in the filesystem. The default implementation returns "not supported"
  // so that user implementations of FSRandomAccessFile do not need to
  // immediately implement this function.
  virtual IOStatus GetFileSize(uint64_t* /*result*/) {
    return IOStatus::NotSupported("GetFileSize Not Supported");
  }

  // EXPERIMENTAL
  // Returns metadata for the file that can be passed back later to the file
  // system when reopening this file. This is optional. The implementation
  // can return NotSupported. The metadata, if returned, is not mandatory
  // for the file system to use when reopening. It can be ignored and the
  // only downside is slower file open time.
  // The returned metadata must not exceed kMaxFileOpenMetadataSize bytes.
  // Larger metadata will be silently discarded by RocksDB.
  static constexpr size_t kMaxFileOpenMetadataSize = 8 * 1024;  // 8KB
  virtual IOStatus GetFileOpenMetadata(std::string* /*metadata*/) {
    return IOStatus::NotSupported("GetFileOpenMetadata not supported");
  }

  // If you're adding methods here, remember to add them to
  // RandomAccessFileWrapper too.
};

// A data structure brings the data verification information, which is
// used together with data being written to a file.
struct DataVerificationInfo {
  // checksum of the data being written.
  Slice checksum;
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class FSWritableFile {
 public:
  FSWritableFile()
      : last_preallocated_block_(0),
        preallocation_block_size_(0),
        io_priority_(Env::IO_TOTAL),
        write_hint_(Env::WLTH_NOT_SET),
        strict_bytes_per_sync_(false) {}

  explicit FSWritableFile(const FileOptions& options)
      : last_preallocated_block_(0),
        preallocation_block_size_(0),
        io_priority_(Env::IO_TOTAL),
        write_hint_(Env::WLTH_NOT_SET),
        strict_bytes_per_sync_(options.strict_bytes_per_sync) {}

  // For cases when Close() hasn't been called, many derived classes of
  // FSWritableFile will need to call Close() non-virtually in their destructor,
  // and ignore the result, to ensure resources are released.
  virtual ~FSWritableFile() {}

  // Append data to the end of the file
  // Note: A WritableFile object must support either Append or
  // PositionedAppend, so the users cannot mix the two.
  virtual IOStatus Append(const Slice& data, const IOOptions& options,
                          IODebugContext* dbg) = 0;

  // Append data with verification information.
  // Note that this API change is experimental and it might be changed in
  // the future. Currently, RocksDB only generates crc32c based checksum for
  // the file writes when the checksum handoff option is set.
  // Expected behavior: if the handoff_checksum_type in FileOptions (currently,
  // ChecksumType::kCRC32C is set as default) is not supported by this
  // FSWritableFile, the information in DataVerificationInfo can be ignored
  // (i.e. does not perform checksum verification).
  virtual IOStatus Append(const Slice& data, const IOOptions& options,
                          const DataVerificationInfo& /* verification_info */,
                          IODebugContext* dbg) {
    return Append(data, options, dbg);
  }

  // PositionedAppend data to the specified offset. The new EOF after append
  // must be larger than the previous EOF. This is to be used when writes are
  // not backed by OS buffers and hence has to always start from the start of
  // the sector. The implementation thus needs to also rewrite the last
  // partial sector.
  // Note: PositionAppend does not guarantee moving the file offset after the
  // write. A WritableFile object must support either Append or
  // PositionedAppend, so the users cannot mix the two.
  //
  // PositionedAppend() can only happen on the page/sector boundaries. For that
  // reason, if the last write was an incomplete sector we still need to rewind
  // back to the nearest sector/page and rewrite the portion of it with whatever
  // we need to add. We need to keep where we stop writing.
  //
  // PositionedAppend() can only write whole sectors. For that reason we have to
  // pad with zeros for the last write and trim the file when closing according
  // to the position we keep in the previous step.
  //
  // PositionedAppend() requires aligned buffer to be passed in. The alignment
  // required is queried via GetRequiredBufferAlignment()
  virtual IOStatus PositionedAppend(const Slice& /* data */,
                                    uint64_t /* offset */,
                                    const IOOptions& /*options*/,
                                    IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("PositionedAppend");
  }

  // PositionedAppend data with verification information.
  // Note that this API change is experimental and it might be changed in
  // the future. Currently, RocksDB only generates crc32c based checksum for
  // the file writes when the checksum handoff option is set.
  // Expected behavior: if the handoff_checksum_type in FileOptions (currently,
  // ChecksumType::kCRC32C is set as default) is not supported by this
  // FSWritableFile, the information in DataVerificationInfo can be ignored
  // (i.e. does not perform checksum verification).
  virtual IOStatus PositionedAppend(
      const Slice& /* data */, uint64_t /* offset */,
      const IOOptions& /*options*/,
      const DataVerificationInfo& /* verification_info */,
      IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("PositionedAppend");
  }

  // Truncate is necessary to trim the file to the correct size
  // before closing. It is not always possible to keep track of the file
  // size due to whole pages writes. If called with other writes to follow,
  // the behavior is file system specific. Posix will reseek to the new EOF.
  // Other file systems may behave differently. Its the caller's
  // responsibility to check the file system contract.
  virtual IOStatus Truncate(uint64_t /*size*/, const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::OK();
  }

  // The caller should call Close() before destroying the FSWritableFile to
  // surface any errors associated with finishing writes to the file.
  // The file is considered closed regardless of return status.
  // (However, implementations must also clean up properly in the destructor
  // even if Close() is not called.)
  virtual IOStatus Close(const IOOptions& /*options*/,
                         IODebugContext* /*dbg*/) = 0;

  // Flush any internally buffered data to the underlying storage, so that
  // the data is no longer dependent on this process's memory. After this
  // call, the data should survive a process crash but is not necessarily
  // persisted to stable storage. Use Sync() for that guarantee.
  // All flushed data must be readable through file access APIs (e.g.
  // FSRandomAccessFile), though path-level metadata queries such as
  // FileSystem::GetFileSize() might lag on some implementations.
  // Not thread-safe; see IsSyncThreadSafe().
  virtual IOStatus Flush(const IOOptions& options, IODebugContext* dbg) = 0;

  // Persist data to stable storage. After this call, the data should
  // survive power failures. Does not necessarily persist file metadata
  // (e.g. file size); see Fsync().
  // Sync() implies Flush(): implementations must ensure all internally
  // buffered data is also flushed.
  // Not safe to call concurrently with Append() or Flush() unless
  // IsSyncThreadSafe() returns true.
  virtual IOStatus Sync(const IOOptions& options, IODebugContext* dbg) = 0;

  /*
   * Persist data and metadata to stable storage.
   * By default, sync only data.
   * Override this method for environments where we need to sync
   * metadata as well.
   */
  virtual IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) {
    return Sync(options, dbg);
  }

  // true if Sync() and Fsync() are safe to call concurrently with Append()
  // and Flush().
  virtual bool IsSyncThreadSafe() const { return false; }

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  virtual void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
    write_hint_ = hint;
  }

  /*
   * If rate limiting is enabled, change the file-granularity priority used in
   * rate-limiting writes.
   *
   * In the presence of finer-granularity priority such as
   * `WriteOptions::rate_limiter_priority`, this file-granularity priority may
   * be overridden by a non-Env::IO_TOTAL finer-granularity priority and used as
   * a fallback for Env::IO_TOTAL finer-granularity priority.
   *
   * If rate limiting is not enabled, this call has no effect.
   */
  virtual void SetIOPriority(Env::IOPriority pri) { io_priority_ = pri; }

  virtual Env::IOPriority GetIOPriority() { return io_priority_; }

  virtual Env::WriteLifeTimeHint GetWriteLifeTimeHint() { return write_hint_; }
  /*
   * Get the size of valid data in the file.
   */
  virtual uint64_t GetFileSize(const IOOptions& /*options*/,
                               IODebugContext* /*dbg*/) = 0;

  /*
   * Get and set the default pre-allocation block size for writes to
   * this file.  If non-zero, then Allocate will be used to extend the
   * underlying storage of a file (generally via fallocate) if the Env
   * instance supports it.
   */
  virtual void SetPreallocationBlockSize(size_t size) {
    preallocation_block_size_ = size;
  }

  virtual void GetPreallocationStatus(size_t* block_size,
                                      size_t* last_allocated_block) {
    *last_allocated_block = last_preallocated_block_;
    *block_size = preallocation_block_size_;
  }

  // For documentation, refer to RandomAccessFile::GetUniqueId()
  virtual size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
    return 0;  // Default implementation to prevent issues with backwards
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  // This call has no effect on dirty pages in the cache.
  virtual IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) {
    return IOStatus::NotSupported("InvalidateCache not supported.");
  }

  // Sync a file range with disk.
  // offset is the starting byte of the file range to be synchronized.
  // nbytes specifies the length of the range to be synchronized.
  // This asks the OS to initiate flushing the cached data to disk,
  // without waiting for completion.
  // Default implementation does nothing.
  virtual IOStatus RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/,
                             const IOOptions& options, IODebugContext* dbg) {
    if (strict_bytes_per_sync_) {
      return Sync(options, dbg);
    }
    return IOStatus::OK();
  }

  // PrepareWrite performs any necessary preparation for a write
  // before the write actually occurs.  This allows for pre-allocation
  // of space on devices where it can result in less file
  // fragmentation and/or less waste from over-zealous filesystem
  // pre-allocation.
  virtual void PrepareWrite(size_t offset, size_t len, const IOOptions& options,
                            IODebugContext* dbg) {
    if (preallocation_block_size_ == 0) {
      return;
    }
    // If this write would cross one or more preallocation blocks,
    // determine what the last preallocation block necessary to
    // cover this write would be and Allocate to that point.
    const auto block_size = preallocation_block_size_;
    size_t new_last_preallocated_block =
        (offset + len + block_size - 1) / block_size;
    if (new_last_preallocated_block > last_preallocated_block_) {
      size_t num_spanned_blocks =
          new_last_preallocated_block - last_preallocated_block_;
      Allocate(block_size * last_preallocated_block_,
               block_size * num_spanned_blocks, options, dbg)
          .PermitUncheckedError();
      last_preallocated_block_ = new_last_preallocated_block;
    }
  }

  // Pre-allocates space for a file.
  virtual IOStatus Allocate(uint64_t /*offset*/, uint64_t /*len*/,
                            const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
    return IOStatus::OK();
  }

  // If you're adding methods here, remember to add them to
  // WritableFileWrapper too.

 protected:
  size_t preallocation_block_size() { return preallocation_block_size_; }

 private:
  size_t last_preallocated_block_;
  size_t preallocation_block_size_;
  // No copying allowed
  FSWritableFile(const FSWritableFile&);
  void operator=(const FSWritableFile&);
  FSWritableFile(FSWritableFile&&) = delete;
  void operator=(FSWritableFile&&) = delete;

 protected:
  Env::IOPriority io_priority_;
  Env::WriteLifeTimeHint write_hint_;
  const bool strict_bytes_per_sync_;
};

// A file abstraction for random reading and writing.
class FSRandomRWFile {
 public:
  FSRandomRWFile() {}

  // For cases when Close() hasn't been called, many derived classes of
  // FSRandomRWFile will need to call Close() non-virtually in their destructor,
  // and ignore the result, to ensure resources are released.
  virtual ~FSRandomRWFile() = default;

  // Indicates if the class makes use of direct I/O
  // If false you must pass aligned buffer to Write()
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  // Write bytes in `data` at  offset `offset`, Returns Status::OK() on success.
  // Pass aligned buffer when use_direct_io() returns true.
  virtual IOStatus Write(uint64_t offset, const Slice& data,
                         const IOOptions& options, IODebugContext* dbg) = 0;

  // Read up to `n` bytes starting from offset `offset` and store them in
  // result, provided `scratch` size should be at least `n`.
  //
  // After call, result->size() < n only if end of file has been
  // reached (or non-OK status). Read might fail if called again after
  // first result->size() < n.
  //
  // Returns Status::OK() on success.
  virtual IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                        Slice* result, char* scratch,
                        IODebugContext* dbg) const = 0;

  virtual IOStatus Flush(const IOOptions& options, IODebugContext* dbg) = 0;

  virtual IOStatus Sync(const IOOptions& options, IODebugContext* dbg) = 0;

  virtual IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) {
    return Sync(options, dbg);
  }

  // The caller should call Close() before destroying the FSRandomRWFile to
  // surface any errors associated with finishing writes to the file.
  // The file is considered closed regardless of return status.
  // (However, implementations must also clean up properly in the destructor
  // even if Close() is not called.)
  virtual IOStatus Close(const IOOptions& options, IODebugContext* dbg) = 0;

  // EXPERIMENTAL
  // When available, returns the actual temperature for the file. This is
  // useful in case some outside process moves a file from one tier to another,
  // though the temperature is generally expected not to change while a file is
  // open.
  virtual Temperature GetTemperature() const { return Temperature::kUnknown; }

  // If you're adding methods here, remember to add them to
  // RandomRWFileWrapper too.

  // No copying allowed
  FSRandomRWFile(const RandomRWFile&) = delete;
  FSRandomRWFile& operator=(const RandomRWFile&) = delete;
};

// MemoryMappedFileBuffer object represents a memory-mapped file's raw buffer.
// Subclasses should release the mapping upon destruction.
class FSMemoryMappedFileBuffer {
 public:
  FSMemoryMappedFileBuffer(void* _base, size_t _length)
      : base_(_base), length_(_length) {}

  virtual ~FSMemoryMappedFileBuffer() = 0;

  // We do not want to unmap this twice. We can make this class
  // movable if desired, however, since
  FSMemoryMappedFileBuffer(const FSMemoryMappedFileBuffer&) = delete;
  FSMemoryMappedFileBuffer& operator=(const FSMemoryMappedFileBuffer&) = delete;
  FSMemoryMappedFileBuffer(FSMemoryMappedFileBuffer&&) = delete;
  FSMemoryMappedFileBuffer& operator=(FSMemoryMappedFileBuffer&&) = delete;

  void* GetBase() const { return base_; }
  size_t GetLen() const { return length_; }

 protected:
  void* base_;
  const size_t length_;
};

// Directory object represents collection of files and implements
// filesystem operations that can be executed on directories.
class FSDirectory {
 public:
  // For cases when Close() hasn't been called, many derived classes of
  // FSDirectory will need to call Close() non-virtually in their destructor,
  // and ignore the result, to ensure resources are released.
  virtual ~FSDirectory() = default;
  // Fsync directory. Can be called concurrently from multiple threads.
  virtual IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) = 0;

  // FsyncWithDirOptions after renaming a file. Depends on the filesystem, it
  // may fsync directory or just the renaming file (e.g. btrfs). By default, it
  // just calls directory fsync.
  virtual IOStatus FsyncWithDirOptions(
      const IOOptions& options, IODebugContext* dbg,
      const DirFsyncOptions& /*dir_fsync_options*/) {
    return Fsync(options, dbg);
  }

  // Calling Close() before destroying a FSDirectory is recommended to surface
  // any errors associated with finishing writes (in case of future features).
  // The directory is considered closed regardless of return status.
  virtual IOStatus Close(const IOOptions& /*options*/,
                         IODebugContext* /*dbg*/) {
    return IOStatus::NotSupported("Close");
  }

  virtual size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
    return 0;
  }

  // If you're adding methods here, remember to add them to
  // DirectoryWrapper too.
};

// Below are helpers for wrapping most of the classes in this file.
// They forward all calls to another instance of the class.
// Useful when wrapping the default implementations.
// Typical usage is to inherit your wrapper from *Wrapper, e.g.:
//
// class MySequentialFileWrapper : public
// ROCKSDB_NAMESPACE::FSSequentialFileWrapper {
//  public:
//   MySequentialFileWrapper(ROCKSDB_NAMESPACE::FSSequentialFile* target):
//     ROCKSDB_NAMESPACE::FSSequentialFileWrapper(target) {}
//   Status Read(size_t n, FileSystem::IOOptions& options, Slice* result,
//               char* scratch, FileSystem::IODebugContext* dbg) override {
//     cout << "Doing a read of size " << n << "!" << endl;
//     return ROCKSDB_NAMESPACE::FSSequentialFileWrapper::Read(n, options,
//     result,
//                                                 scratch, dbg);
//   }
//   // All other methods are forwarded to target_ automatically.
// };
//
// This is often more convenient than inheriting the class directly because
// (a) Don't have to override and forward all methods - the Wrapper will
//     forward everything you're not explicitly overriding.
// (b) Don't need to update the wrapper when more methods are added to the
//     rocksdb class. Unless you actually want to override the behavior.
//     (And unless rocksdb people forgot to update the *Wrapper class.)

// An implementation of Env that forwards all calls to another Env.
// May be useful to clients who wish to override just part of the
// functionality of another Env.
class FileSystemWrapper : public FileSystem {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit FileSystemWrapper(const std::shared_ptr<FileSystem>& t);
  ~FileSystemWrapper() override = default;

  // Return the target to which this Env forwards all calls
  FileSystem* target() const { return target_.get(); }

  folly::IOExecutor* GetReadExecutor() override {
    return target_->GetReadExecutor();
  }

  void SetReadIOExecutorThreads(int num) override {
    target_->SetReadIOExecutorThreads(num);
  }

  // The following text is boilerplate that forwards all methods to target()
  IOStatus NewSequentialFile(const std::string& f, const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* dbg) override {
    return target_->NewSequentialFile(f, file_opts, r, dbg);
  }
  IOStatus NewRandomAccessFile(const std::string& f,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* r,
                               IODebugContext* dbg) override {
    return target_->NewRandomAccessFile(f, file_opts, r, dbg);
  }
  IOStatus NewWritableFile(const std::string& f, const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* dbg) override {
    return target_->NewWritableFile(f, file_opts, r, dbg);
  }
  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override {
    return target_->ReopenWritableFile(fname, file_opts, result, dbg);
  }
  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSWritableFile>* r,
                             IODebugContext* dbg) override {
    return target_->ReuseWritableFile(fname, old_fname, file_opts, r, dbg);
  }
  IOStatus NewRandomRWFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override {
    return target_->NewRandomRWFile(fname, file_opts, result, dbg);
  }
  IOStatus NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    return target_->NewMemoryMappedFileBuffer(fname, result);
  }
  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    return target_->NewDirectory(name, io_opts, result, dbg);
  }
  IOStatus FileExists(const std::string& f, const IOOptions& io_opts,
                      IODebugContext* dbg) override {
    return target_->FileExists(f, io_opts, dbg);
  }
  IOStatus GetChildren(const std::string& dir, const IOOptions& io_opts,
                       std::vector<std::string>* r,
                       IODebugContext* dbg) override {
    return target_->GetChildren(dir, io_opts, r, dbg);
  }
  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& options,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* dbg) override {
    return target_->GetChildrenFileAttributes(dir, options, result, dbg);
  }
  IOStatus DeleteFile(const std::string& f, const IOOptions& options,
                      IODebugContext* dbg) override {
    return target_->DeleteFile(f, options, dbg);
  }
  IOStatus Truncate(const std::string& fname, size_t size,
                    const IOOptions& options, IODebugContext* dbg) override {
    return target_->Truncate(fname, size, options, dbg);
  }
  IOStatus CreateDir(const std::string& d, const IOOptions& options,
                     IODebugContext* dbg) override {
    return target_->CreateDir(d, options, dbg);
  }
  IOStatus CreateDirIfMissing(const std::string& d, const IOOptions& options,
                              IODebugContext* dbg) override {
    return target_->CreateDirIfMissing(d, options, dbg);
  }
  IOStatus DeleteDir(const std::string& d, const IOOptions& options,
                     IODebugContext* dbg) override {
    return target_->DeleteDir(d, options, dbg);
  }
  IOStatus GetFileSize(const std::string& f, const IOOptions& options,
                       uint64_t* s, IODebugContext* dbg) override {
    return target_->GetFileSize(f, options, s, dbg);
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override {
    return target_->GetFileModificationTime(fname, options, file_mtime, dbg);
  }

  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override {
    return target_->GetAbsolutePath(db_path, options, output_path, dbg);
  }

  IOStatus RenameFile(const std::string& s, const std::string& t,
                      const IOOptions& options, IODebugContext* dbg) override {
    return target_->RenameFile(s, t, options, dbg);
  }

  IOStatus LinkFile(const std::string& s, const std::string& t,
                    const IOOptions& options, IODebugContext* dbg) override {
    return target_->LinkFile(s, t, options, dbg);
  }

  IOStatus SyncFile(const std::string& fname, const FileOptions& file_opts,
                    const IOOptions& io_opts, bool use_fsync,
                    IODebugContext* dbg) override {
    return target_->SyncFile(fname, file_opts, io_opts, use_fsync, dbg);
  }

  IOStatus NumFileLinks(const std::string& fname, const IOOptions& options,
                        uint64_t* count, IODebugContext* dbg) override {
    return target_->NumFileLinks(fname, options, count, dbg);
  }

  IOStatus AreFilesSame(const std::string& first, const std::string& second,
                        const IOOptions& options, bool* res,
                        IODebugContext* dbg) override {
    return target_->AreFilesSame(first, second, options, res, dbg);
  }

  IOStatus LockFile(const std::string& f, const IOOptions& options,
                    FileLock** l, IODebugContext* dbg) override {
    return target_->LockFile(f, options, l, dbg);
  }

  IOStatus UnlockFile(FileLock* l, const IOOptions& options,
                      IODebugContext* dbg) override {
    return target_->UnlockFile(l, options, dbg);
  }

  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override {
    return target_->GetTestDirectory(options, path, dbg);
  }
  IOStatus NewLogger(const std::string& fname, const IOOptions& options,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override {
    return target_->NewLogger(fname, options, result, dbg);
  }

  void SanitizeFileOptions(FileOptions* opts) const override {
    target_->SanitizeFileOptions(opts);
  }

  FileOptions OptimizeForLogRead(
      const FileOptions& file_options) const override {
    return target_->OptimizeForLogRead(file_options);
  }
  FileOptions OptimizeForManifestRead(
      const FileOptions& file_options) const override {
    return target_->OptimizeForManifestRead(file_options);
  }
  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                  const DBOptions& db_options) const override {
    return target_->OptimizeForLogWrite(file_options, db_options);
  }
  FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const override {
    return target_->OptimizeForManifestWrite(file_options);
  }
  FileOptions OptimizeForCompactionTableWrite(
      const FileOptions& file_options,
      const ImmutableDBOptions& immutable_ops) const override {
    return target_->OptimizeForCompactionTableWrite(file_options,
                                                    immutable_ops);
  }
  FileOptions OptimizeForCompactionTableRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const override {
    return target_->OptimizeForCompactionTableRead(file_options, db_options);
  }
  FileOptions OptimizeForBlobFileRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const override {
    return target_->OptimizeForBlobFileRead(file_options, db_options);
  }
  IOStatus GetFreeSpace(const std::string& path, const IOOptions& options,
                        uint64_t* diskfree, IODebugContext* dbg) override {
    return target_->GetFreeSpace(path, options, diskfree, dbg);
  }
  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dbg) override {
    return target_->IsDirectory(path, options, is_dir, dbg);
  }

  const Customizable* Inner() const override { return target_.get(); }
  Status PrepareOptions(const ConfigOptions& options) override;
  std::string SerializeOptions(const ConfigOptions& config_options,
                               const std::string& header) const override;

  IOStatus Poll(std::vector<void*>& io_handles,
                size_t min_completions) override {
    return target_->Poll(io_handles, min_completions);
  }

  IOStatus AbortIO(std::vector<void*>& io_handles) override {
    return target_->AbortIO(io_handles);
  }

  void DiscardCacheForDirectory(const std::string& path) override {
    target_->DiscardCacheForDirectory(path);
  }

  void SupportedOps(int64_t& supported_ops) override {
    return target_->SupportedOps(supported_ops);
  }

  bool IsPreallocationSupported() override {
    return target_->IsPreallocationSupported();
  }

 protected:
  std::shared_ptr<FileSystem> target_;
};

class FSSequentialFileWrapper : public FSSequentialFile {
 public:
  // Creates a FileWrapper around the input File object and without
  // taking ownership of the object
  explicit FSSequentialFileWrapper(FSSequentialFile* t) : target_(t) {}

  FSSequentialFile* target() const { return target_; }

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override {
    return target_->Read(n, options, result, scratch, dbg);
  }
  IOStatus Skip(uint64_t n) override { return target_->Skip(n); }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override {
    return target_->PositionedRead(offset, n, options, result, scratch, dbg);
  }
  Temperature GetTemperature() const override {
    return target_->GetTemperature();
  }

 private:
  FSSequentialFile* target_;
};

class FSSequentialFileOwnerWrapper : public FSSequentialFileWrapper {
 public:
  // Creates a FileWrapper around the input File object and takes
  // ownership of the object
  explicit FSSequentialFileOwnerWrapper(std::unique_ptr<FSSequentialFile>&& t)
      : FSSequentialFileWrapper(t.get()), guard_(std::move(t)) {}

 private:
  std::unique_ptr<FSSequentialFile> guard_;
};

class FSRandomAccessFileWrapper : public FSRandomAccessFile {
 public:
  // Creates a FileWrapper around the input File object and without
  // taking ownership of the object
  explicit FSRandomAccessFileWrapper(FSRandomAccessFile* t) : target_(t) {}

  FSRandomAccessFile* target() const { return target_; }

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    return target_->Read(offset, n, options, result, scratch, dbg);
  }
  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override {
    return target_->MultiRead(reqs, num_reqs, options, dbg);
  }
  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& options,
                    IODebugContext* dbg) override {
    return target_->Prefetch(offset, n, options, dbg);
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }
  void Hint(AccessPattern pattern) override { target_->Hint(pattern); }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }
  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override {
    return target()->ReadAsync(req, opts, cb, cb_arg, io_handle, del_fn, dbg);
  }
  bool SubmitReadAsync(FSReadRequest& req, const IOOptions& opts,
                       std::function<void(FSReadRequest&)> cb,
                       IODebugContext* dbg) override {
    return target()->SubmitReadAsync(req, opts, std::move(cb), dbg);
  }
  Temperature GetTemperature() const override {
    return target_->GetTemperature();
  }

  IOStatus GetFileSize(uint64_t* result) override {
    return target_->GetFileSize(result);
  }
  IOStatus GetFileOpenMetadata(std::string* metadata) override {
    return target_->GetFileOpenMetadata(metadata);
  }

 private:
  std::unique_ptr<FSRandomAccessFile> guard_;
  FSRandomAccessFile* target_;
};

class FSRandomAccessFileOwnerWrapper : public FSRandomAccessFileWrapper {
 public:
  // Creates a FileWrapper around the input File object and takes
  // ownership of the object
  explicit FSRandomAccessFileOwnerWrapper(
      std::unique_ptr<FSRandomAccessFile>&& t)
      : FSRandomAccessFileWrapper(t.get()), guard_(std::move(t)) {}

 private:
  std::unique_ptr<FSRandomAccessFile> guard_;
};

class FSWritableFileWrapper : public FSWritableFile {
 public:
  // Creates a FileWrapper around the input File object and without
  // taking ownership of the object
  explicit FSWritableFileWrapper(FSWritableFile* t) : target_(t) {}

  FSWritableFile* target() const { return target_; }

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override {
    return target_->Append(data, options, dbg);
  }
  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& verification_info,
                  IODebugContext* dbg) override {
    return target_->Append(data, options, verification_info, dbg);
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override {
    return target_->PositionedAppend(data, offset, options, dbg);
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& verification_info,
                            IODebugContext* dbg) override {
    return target_->PositionedAppend(data, offset, options, verification_info,
                                     dbg);
  }
  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override {
    return target_->Truncate(size, options, dbg);
  }
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Close(options, dbg);
  }
  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Flush(options, dbg);
  }
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Sync(options, dbg);
  }
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Fsync(options, dbg);
  }
  bool IsSyncThreadSafe() const override { return target_->IsSyncThreadSafe(); }

  bool use_direct_io() const override { return target_->use_direct_io(); }

  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
    target_->SetWriteLifeTimeHint(hint);
  }

  Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return target_->GetWriteLifeTimeHint();
  }

  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override {
    return target_->GetFileSize(options, dbg);
  }

  void SetPreallocationBlockSize(size_t size) override {
    target_->SetPreallocationBlockSize(size);
  }

  void GetPreallocationStatus(size_t* block_size,
                              size_t* last_allocated_block) override {
    target_->GetPreallocationStatus(block_size, last_allocated_block);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& options,
                     IODebugContext* dbg) override {
    return target_->RangeSync(offset, nbytes, options, dbg);
  }

  void PrepareWrite(size_t offset, size_t len, const IOOptions& options,
                    IODebugContext* dbg) override {
    target_->PrepareWrite(offset, len, options, dbg);
  }

  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& options,
                    IODebugContext* dbg) override {
    return target_->Allocate(offset, len, options, dbg);
  }

 private:
  FSWritableFile* target_;
};

class FSWritableFileOwnerWrapper : public FSWritableFileWrapper {
 public:
  // Creates a FileWrapper around the input File object and takes
  // ownership of the object
  explicit FSWritableFileOwnerWrapper(std::unique_ptr<FSWritableFile>&& t)
      : FSWritableFileWrapper(t.get()), guard_(std::move(t)) {}

 private:
  std::unique_ptr<FSWritableFile> guard_;
};

class FSRandomRWFileWrapper : public FSRandomRWFile {
 public:
  // Creates a FileWrapper around the input File object and without
  // taking ownership of the object
  explicit FSRandomRWFileWrapper(FSRandomRWFile* t) : target_(t) {}

  FSRandomRWFile* target() const { return target_; }

  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& options,
                 IODebugContext* dbg) override {
    return target_->Write(offset, data, options, dbg);
  }
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    return target_->Read(offset, n, options, result, scratch, dbg);
  }
  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Flush(options, dbg);
  }
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Sync(options, dbg);
  }
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Fsync(options, dbg);
  }
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Close(options, dbg);
  }
  Temperature GetTemperature() const override {
    return target_->GetTemperature();
  }

 private:
  FSRandomRWFile* target_;
};

class FSRandomRWFileOwnerWrapper : public FSRandomRWFileWrapper {
 public:
  // Creates a FileWrapper around the input File object and takes
  // ownership of the object
  explicit FSRandomRWFileOwnerWrapper(std::unique_ptr<FSRandomRWFile>&& t)
      : FSRandomRWFileWrapper(t.get()), guard_(std::move(t)) {}

 private:
  std::unique_ptr<FSRandomRWFile> guard_;
};

class FSDirectoryWrapper : public FSDirectory {
 public:
  // Creates a FileWrapper around the input File object and takes
  // ownership of the object
  explicit FSDirectoryWrapper(std::unique_ptr<FSDirectory>&& t)
      : guard_(std::move(t)) {
    target_ = guard_.get();
  }

  // Creates a FileWrapper around the input File object and without
  // taking ownership of the object
  explicit FSDirectoryWrapper(FSDirectory* t) : target_(t) {}

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Fsync(options, dbg);
  }

  IOStatus FsyncWithDirOptions(
      const IOOptions& options, IODebugContext* dbg,
      const DirFsyncOptions& dir_fsync_options) override {
    return target_->FsyncWithDirOptions(options, dbg, dir_fsync_options);
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    return target_->Close(options, dbg);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

 private:
  std::unique_ptr<FSDirectory> guard_;
  FSDirectory* target_;
};

// A utility routine: write "data" to the named file.
IOStatus WriteStringToFile(FileSystem* fs, const Slice& data,
                           const std::string& fname, bool should_sync = false,
                           const IOOptions& io_options = IOOptions(),
                           const FileOptions& file_options = FileOptions());

// A utility routine: read contents of named file into *data
IOStatus ReadFileToString(FileSystem* fs, const std::string& fname,
                          std::string* data);

// A utility routine: read contents of named file into *data
IOStatus ReadFileToString(FileSystem* fs, const std::string& fname,
                          const IOOptions& opts, std::string* data);

}  // namespace ROCKSDB_NAMESPACE
