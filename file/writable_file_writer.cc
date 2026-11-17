//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/writable_file_writer.h"

#include <algorithm>
#include <cstdio>
#include <mutex>
#include <string>

#include "db/version_edit.h"
#include "file/file_util.h"
#include "monitoring/histogram.h"
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "rocksdb/io_status.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/crc32c.h"
#include "util/random.h"
#include "util/rate_limiter_impl.h"

namespace ROCKSDB_NAMESPACE {
inline Histograms GetFileWriteHistograms(Histograms file_writer_hist,
                                         Env::IOActivity io_activity) {
  if (file_writer_hist == Histograms::SST_WRITE_MICROS ||
      file_writer_hist == Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS) {
    switch (io_activity) {
      case Env::IOActivity::kFlush:
        return Histograms::FILE_WRITE_FLUSH_MICROS;
      case Env::IOActivity::kCompaction:
        return Histograms::FILE_WRITE_COMPACTION_MICROS;
      case Env::IOActivity::kDBOpen:
        return Histograms::FILE_WRITE_DB_OPEN_MICROS;
      default:
        break;
    }
  }
  return Histograms::HISTOGRAM_ENUM_MAX;
}

constexpr char kDestinationBufferChecksumMismatch[] =
    "Checksum handoff detected data corruption after copying data into "
    "WritableFileWriter destination buffer";
constexpr char kLogicalAppendChecksumMismatch[] =
    "Checksum handoff detected logical append checksum mismatch after copying "
    "caller data into WritableFileWriter buffers";

static IOStatus ChecksumMismatchStatus(size_t size,
                                       Crc32cChecksum expected_checksum,
                                       uint32_t actual_crc32c,
                                       const std::string& file_name,
                                       const char* mismatch_message) {
  char expected_checksum_buf[11];
  char actual_crc32c_buf[11];
  snprintf(expected_checksum_buf, sizeof(expected_checksum_buf), "0x%08x",
           static_cast<unsigned int>(expected_checksum.value));
  snprintf(actual_crc32c_buf, sizeof(actual_crc32c_buf), "0x%08x",
           static_cast<unsigned int>(actual_crc32c));
  std::string message = mismatch_message;
  message.append(": file=")
      .append(file_name)
      .append(" size=")
      .append(std::to_string(size))
      .append(" expected_crc32c=")
      .append(expected_checksum_buf)
      .append(" actual_crc32c=")
      .append(actual_crc32c_buf);
  return IOStatus::Corruption(message);
}

static IOStatus ValidateDataChecksum(const Slice& data, Crc32cChecksum checksum,
                                     const std::string& file_name,
                                     const char* mismatch_message) {
  const uint32_t actual_crc32c = crc32c::Value(data.data(), data.size());
  if (actual_crc32c == checksum.value) {
    return IOStatus::OK();
  }
  return ChecksumMismatchStatus(data.size(), checksum, actual_crc32c, file_name,
                                mismatch_message);
}

IOStatus WritableFileWriter::Create(const std::shared_ptr<FileSystem>& fs,
                                    const std::string& fname,
                                    const FileOptions& file_opts,
                                    std::unique_ptr<WritableFileWriter>* writer,
                                    IODebugContext* dbg) {
  if (file_opts.use_direct_writes &&
      0 == file_opts.writable_file_max_buffer_size) {
    return IOStatus::InvalidArgument(
        "Direct write requires writable_file_max_buffer_size > 0");
  }
  std::unique_ptr<FSWritableFile> file;
  IOStatus io_s = fs->NewWritableFile(fname, file_opts, &file, dbg);
  if (io_s.ok()) {
    writer->reset(new WritableFileWriter(std::move(file), fname, file_opts));
  }
  return io_s;
}

IOStatus WritableFileWriter::Append(const Slice& data, const IOOptions& opts,
                                    IODebugContext* /*dbg*/) {
  return AppendSlice(opts, data, /*crc32c_checksum=*/nullptr);
}

IOStatus WritableFileWriter::Append(const Slice& data, const IOOptions& opts,
                                    Crc32cChecksum crc32c_checksum) {
  return AppendSlice(opts, data, &crc32c_checksum);
}

IOStatus WritableFileWriter::AppendSlice(
    const IOOptions& opts, const Slice& data_slice,
    const Crc32cChecksum* crc32c_checksum) {
  if (seen_error()) {
    return GetWriterHasPreviousErrorStatus();
  }

  StopWatch sw(clock_, stats_, hist_type_,
               GetFileWriteHistograms(hist_type_, opts.io_activity));

  const IOOptions io_options = FinalizeIOOptions(opts);
  const char* src = data_slice.data();
  size_t left = data_slice.size();
  IOStatus s;
  pending_sync_ = true;

  TEST_KILL_RANDOM_WITH_WEIGHT("WritableFileWriter::Append:0", REDUCE_ODDS2);

  UpdateFileChecksum(data_slice);

  {
    IOSTATS_TIMER_GUARD(prepare_write_nanos);
    TEST_SYNC_POINT("WritableFileWriter::Append:BeforePrepareWrite");
    writable_file_->PrepareWrite(static_cast<size_t>(GetFileSize()), left,
                                 io_options, nullptr);
  }

  // See whether we need to enlarge the buffer to avoid the flush
  if (buf_.Capacity() - buf_.CurrentSize() < left) {
    for (size_t cap = buf_.Capacity();
         cap < max_buffer_size_;  // There is still room to increase
         cap *= 2) {
      // See whether the next available size is large enough.
      // Buffer will never be increased to more than max_buffer_size_.
      size_t desired_capacity = std::min(cap * 2, max_buffer_size_);
      if (desired_capacity - buf_.CurrentSize() >= left ||
          (use_direct_io() && desired_capacity == max_buffer_size_)) {
        buf_.AllocateNewBuffer(desired_capacity, true);
        break;
      }
    }
  }

  // Flush only when buffered I/O
  if (!use_direct_io() && (buf_.Capacity() - buf_.CurrentSize()) < left) {
    if (buf_.CurrentSize() > 0) {
      if (!buffered_data_with_checksum_) {
        // If we're not calculating checksum of buffered data, fill the
        // buffer before flushing so that the writes are aligned. This will
        // benefit file system performance.
        size_t appended = buf_.Append(src, left);
        left -= appended;
        src += appended;
      }
      s = Flush(io_options);
      if (!s.ok()) {
        set_seen_error(s);
        return s;
      }
    }
    assert(buf_.CurrentSize() == 0);
  }

  if (perform_data_verification_ && buffered_data_with_checksum_ &&
      crc32c_checksum != nullptr) {
    if (use_direct_io() || (buf_.Capacity() - buf_.CurrentSize()) >= left) {
      if ((buf_.Capacity() - buf_.CurrentSize()) >= left) {
        size_t copied_offset = buf_.CurrentSize();
        size_t appended = buf_.Append(src, left);
        if (appended != left) {
          s = IOStatus::Corruption("Write buffer append failure");
        }
        if (s.ok()) {
          Slice copied_data(buf_.BufferStart() + copied_offset, appended);
          s = ValidateCopiedDataChecksum(copied_data, *crc32c_checksum);
        }
        if (s.ok()) {
          buffered_data_crc32c_checksum_ = crc32c::Crc32cCombine(
              buffered_data_crc32c_checksum_, crc32c_checksum->value, appended);
        } else {
          buf_.Size(0);
          buffered_data_crc32c_checksum_ = 0;
        }
      } else {
        uint32_t logical_append_crc32c = 0;
        while (s.ok() && left > 0) {
          if (buf_.CurrentSize() == buf_.Capacity()) {
            s = Flush(io_options);
            continue;
          }
          const size_t copied_offset = buf_.CurrentSize();
          const size_t append_size =
              std::min(left, buf_.Capacity() - buf_.CurrentSize());
          size_t appended = buf_.Append(src, append_size);
          if (appended != append_size) {
            s = IOStatus::Corruption("Write buffer append failure");
            break;
          }
          Slice copied_data(buf_.BufferStart() + copied_offset, appended);
          const uint32_t appended_crc32c =
              crc32c::Value(copied_data.data(), copied_data.size());
          buffered_data_crc32c_checksum_ = crc32c::Crc32cCombine(
              buffered_data_crc32c_checksum_, appended_crc32c, appended);
          logical_append_crc32c = crc32c::Crc32cCombine(
              logical_append_crc32c, appended_crc32c, appended);
          left -= appended;
          src += appended;

          if (left > 0) {
            s = Flush(io_options);
            if (!s.ok()) {
              break;
            }
          }
        }
        if (s.ok() && logical_append_crc32c != crc32c_checksum->value) {
          s = ChecksumMismatchStatus(data_slice.size(), *crc32c_checksum,
                                     logical_append_crc32c, file_name_,
                                     kLogicalAppendChecksumMismatch);
          buf_.Size(0);
          buffered_data_crc32c_checksum_ = 0;
        }
      }
    } else {
      assert(buf_.CurrentSize() == 0);
      buffered_data_crc32c_checksum_ = crc32c_checksum->value;
      s = WriteBufferedWithChecksum(io_options, src, left);
    }
  } else {
    // In this case, either we do not need to do the data verification or
    // caller does not provide the checksum of the data (crc32c_checksum = 0).
    //
    // We never write directly to disk with direct I/O on.
    // or we simply use it for its original purpose to accumulate many small
    // chunks
    if (use_direct_io() || (buf_.Capacity() >= left)) {
      while (left > 0) {
        size_t appended = buf_.Append(src, left);
        if (perform_data_verification_ && buffered_data_with_checksum_) {
          buffered_data_crc32c_checksum_ =
              crc32c::Extend(buffered_data_crc32c_checksum_, src, appended);
        }
        left -= appended;
        src += appended;

        if (left > 0) {
          s = Flush(io_options);
          if (!s.ok()) {
            break;
          }
        }
      }
    } else {
      // Writing directly to file bypassing the buffer
      assert(buf_.CurrentSize() == 0);
      if (perform_data_verification_ && buffered_data_with_checksum_) {
        buffered_data_crc32c_checksum_ = crc32c::Value(src, left);
        s = WriteBufferedWithChecksum(io_options, src, left);
      } else {
        s = WriteBuffered(io_options, src, left);
      }
    }
  }

  TEST_KILL_RANDOM("WritableFileWriter::Append:1");
  if (s.ok()) {
    uint64_t cur_size = filesize_.load(std::memory_order_acquire);
    filesize_.store(cur_size + data_slice.size(), std::memory_order_release);
  } else {
    set_seen_error(s);
  }
  return s;
}

IOStatus WritableFileWriter::Pad(const IOOptions& opts, const size_t pad_bytes,
                                 const size_t max_pad_size) {
  (void)max_pad_size;
  if (seen_error()) {
    return GetWriterHasPreviousErrorStatus();
  }
  const IOOptions io_options = FinalizeIOOptions(opts);
  assert(pad_bytes < max_pad_size);
  size_t left = pad_bytes;
  size_t cap = buf_.Capacity() - buf_.CurrentSize();

  // Assume pad_bytes is small compared to buf_ capacity. So we always
  // use buf_ rather than write directly to file in certain cases like
  // Append() does.
  while (left) {
    size_t append_bytes = std::min(cap, left);
    buf_.PadWith(append_bytes, 0);
    left -= append_bytes;

    Slice data(buf_.BufferStart() + buf_.CurrentSize() - append_bytes,
               append_bytes);
    UpdateFileChecksum(data);
    if (perform_data_verification_) {
      buffered_data_crc32c_checksum_ = crc32c::Extend(
          buffered_data_crc32c_checksum_,
          buf_.BufferStart() + buf_.CurrentSize() - append_bytes, append_bytes);
    }

    if (left > 0) {
      IOStatus s = Flush(io_options);
      if (!s.ok()) {
        set_seen_error(s);
        return s;
      }
    }
    cap = buf_.Capacity() - buf_.CurrentSize();
  }
  pending_sync_ = true;
  uint64_t cur_size = filesize_.load(std::memory_order_acquire);
  filesize_.store(cur_size + pad_bytes, std::memory_order_release);

  return IOStatus::OK();
}

IOStatus WritableFileWriter::Close(const IOOptions& opts,
                                   IODebugContext* /*dbg*/) {
  IOOptions io_options = FinalizeIOOptions(opts);
  if (seen_error()) {
    IOStatus interim;
    if (writable_file_.get() != nullptr) {
      interim = writable_file_->Close(io_options, nullptr);
      writable_file_.reset();
    }
    if (interim.ok()) {
      return IOStatus::IOError(
          "File is closed but data not flushed as writer has previous error.");
    } else {
      return interim;
    }
  }

  // Do not quit immediately on failure the file MUST be closed

  // Possible to close it twice now as we MUST close
  // in __dtor, simply flushing is not enough
  // Windows when pre-allocating does not fill with zeros
  // also with unbuffered access we also set the end of data.
  if (writable_file_.get() == nullptr) {
    return IOStatus::OK();
  }

  IOStatus s;
  s = Flush(io_options);  // flush cache to OS

  IOStatus interim;
  // In direct I/O mode we write whole pages so
  // we need to let the file know where data ends.
  if (use_direct_io()) {
    {
      FileOperationInfo::StartTimePoint start_ts;
      if (ShouldNotifyListeners()) {
        start_ts = FileOperationInfo::StartNow();
      }
      uint64_t filesz = filesize_.load(std::memory_order_acquire);
      interim = writable_file_->Truncate(filesz, io_options, nullptr);
      if (ShouldNotifyListeners()) {
        auto finish_ts = FileOperationInfo::FinishNow();
        NotifyOnFileTruncateFinish(start_ts, finish_ts, s);
        if (!interim.ok()) {
          NotifyOnIOError(interim, FileOperationType::kTruncate, file_name(),
                          filesz);
        }
      }
    }
    if (interim.ok()) {
      {
        FileOperationInfo::StartTimePoint start_ts;
        if (ShouldNotifyListeners()) {
          start_ts = FileOperationInfo::StartNow();
        }
        interim = writable_file_->Fsync(io_options, nullptr);
        if (ShouldNotifyListeners()) {
          auto finish_ts = FileOperationInfo::FinishNow();
          NotifyOnFileSyncFinish(start_ts, finish_ts, s,
                                 FileOperationType::kFsync);
          if (!interim.ok()) {
            NotifyOnIOError(interim, FileOperationType::kFsync, file_name());
          }
        }
      }
    }
    if (!interim.ok() && s.ok()) {
      s = interim;
    }
  }

  TEST_KILL_RANDOM("WritableFileWriter::Close:0");
  {
    FileOperationInfo::StartTimePoint start_ts;
    if (ShouldNotifyListeners()) {
      start_ts = FileOperationInfo::StartNow();
    }
    interim = writable_file_->Close(io_options, nullptr);
    if (ShouldNotifyListeners()) {
      auto finish_ts = FileOperationInfo::FinishNow();
      NotifyOnFileCloseFinish(start_ts, finish_ts, s);
      if (!interim.ok()) {
        NotifyOnIOError(interim, FileOperationType::kClose, file_name());
      }
    }
  }
  if (!interim.ok() && s.ok()) {
    s = interim;
  }

  writable_file_.reset();
  TEST_KILL_RANDOM("WritableFileWriter::Close:1");

  if (s.ok()) {
    if (checksum_generator_ != nullptr && !checksum_finalized_) {
      checksum_generator_->Finalize();
      checksum_finalized_ = true;
    }
  } else {
    set_seen_error(s);
  }

  return s;
}

// write out the cached data to the OS cache or storage if direct I/O
// enabled
IOStatus WritableFileWriter::Flush(const IOOptions& opts,
                                   IODebugContext* /*dbg*/) {
  if (seen_error()) {
    return GetWriterHasPreviousErrorStatus();
  }

  const IOOptions io_options = FinalizeIOOptions(opts);

  IOStatus s;
  TEST_KILL_RANDOM_WITH_WEIGHT("WritableFileWriter::Flush:0", REDUCE_ODDS2);

  if (buf_.CurrentSize() > 0) {
    if (use_direct_io()) {
      if (pending_sync_) {
        if (perform_data_verification_ && buffered_data_with_checksum_) {
          s = WriteDirectWithChecksum(io_options);
        } else {
          s = WriteDirect(io_options);
        }
      }
    } else {
      if (perform_data_verification_ && buffered_data_with_checksum_) {
        s = WriteBufferedWithChecksum(io_options, buf_.BufferStart(),
                                      buf_.CurrentSize());
      } else {
        s = WriteBuffered(io_options, buf_.BufferStart(), buf_.CurrentSize());
      }
    }
    if (!s.ok()) {
      set_seen_error(s);
      return s;
    }
  }

  {
    FileOperationInfo::StartTimePoint start_ts;
    if (ShouldNotifyListeners()) {
      start_ts = FileOperationInfo::StartNow();
    }
    s = writable_file_->Flush(io_options, nullptr);
    if (ShouldNotifyListeners()) {
      auto finish_ts = std::chrono::steady_clock::now();
      NotifyOnFileFlushFinish(start_ts, finish_ts, s);
      if (!s.ok()) {
        NotifyOnIOError(s, FileOperationType::kFlush, file_name());
      }
    }
  }

  if (!s.ok()) {
    set_seen_error(s);
    return s;
  }

  // sync OS cache to disk for every bytes_per_sync_
  // TODO: give log file and sst file different options (log
  // files could be potentially cached in OS for their whole
  // life time, thus we might not want to flush at all).

  // We try to avoid sync to the last 1MB of data. For two reasons:
  // (1) avoid rewrite the same page that is modified later.
  // (2) for older version of OS, write can block while writing out
  //     the page.
  // Xfs does neighbor page flushing outside of the specified ranges. We
  // need to make sure sync range is far from the write offset.
  if (!use_direct_io() && bytes_per_sync_) {
    const uint64_t kBytesNotSyncRange =
        1024 * 1024;                                // recent 1MB is not synced.
    const uint64_t kBytesAlignWhenSync = 4 * 1024;  // Align 4KB.
    uint64_t cur_size = filesize_.load(std::memory_order_acquire);
    if (cur_size > kBytesNotSyncRange) {
      uint64_t offset_sync_to = cur_size - kBytesNotSyncRange;
      offset_sync_to -= offset_sync_to % kBytesAlignWhenSync;
      assert(offset_sync_to >= last_sync_size_);
      if (offset_sync_to > 0 &&
          offset_sync_to - last_sync_size_ >= bytes_per_sync_) {
        s = RangeSync(last_sync_size_, offset_sync_to - last_sync_size_,
                      io_options);
        if (!s.ok()) {
          set_seen_error(s);
        }
        last_sync_size_ = offset_sync_to;
      }
    }
  }

  return s;
}

std::string WritableFileWriter::GetFileChecksum() {
  if (checksum_generator_ != nullptr) {
    assert(checksum_finalized_);
    return checksum_generator_->GetChecksum();
  } else {
    return kUnknownFileChecksum;
  }
}

const char* WritableFileWriter::GetFileChecksumFuncName() const {
  if (checksum_generator_ != nullptr) {
    return checksum_generator_->Name();
  } else {
    return kUnknownFileChecksumFuncName;
  }
}

IOStatus WritableFileWriter::PrepareIOOptions(const WriteOptions& wo,
                                              IOOptions& opts) {
  return PrepareIOFromWriteOptions(wo, opts);
}

IOStatus WritableFileWriter::Sync(const IOOptions& opts,
                                  IODebugContext* /*dbg*/) {
  return SyncWithFlush(opts, /*use_fsync=*/false);
}

IOStatus WritableFileWriter::Fsync(const IOOptions& opts,
                                   IODebugContext* /*dbg*/) {
  return SyncWithFlush(opts, /*use_fsync=*/true);
}

IOStatus WritableFileWriter::SyncWithFlush(const IOOptions& opts,
                                           bool use_fsync) {
  return Sync(opts, use_fsync, nullptr);
}

IOStatus WritableFileWriter::Sync(const IOOptions& opts, bool use_fsync,
                                  bool* previously_seen_error) {
  if (seen_error()) {
    if (previously_seen_error != nullptr) {
      *previously_seen_error = true;
    }
    return GetWriterHasPreviousErrorStatus();
  }
  if (previously_seen_error != nullptr) {
    *previously_seen_error = false;
  }

  IOOptions io_options = FinalizeIOOptions(opts);
  IOStatus s = Flush(io_options);
  if (!s.ok()) {
    set_seen_error(s);
    return s;
  }
  TEST_KILL_RANDOM("WritableFileWriter::Sync:0");
  if (!use_direct_io() && pending_sync_) {
    s = SyncInternal(io_options, use_fsync);
    if (!s.ok()) {
      set_seen_error(s);
      return s;
    }
  }
  TEST_KILL_RANDOM("WritableFileWriter::Sync:1");
  pending_sync_ = false;
  return IOStatus::OK();
}

IOStatus WritableFileWriter::SyncWithoutFlush(const IOOptions& opts,
                                              bool use_fsync) {
  return SyncWithoutFlush(opts, use_fsync, nullptr);
}

IOStatus WritableFileWriter::SyncWithoutFlush(const IOOptions& opts,
                                              bool use_fsync,
                                              bool* previously_seen_error) {
  if (seen_error()) {
    if (previously_seen_error != nullptr) {
      *previously_seen_error = true;
    }
    return GetWriterHasPreviousErrorStatus();
  }
  if (previously_seen_error != nullptr) {
    *previously_seen_error = false;
  }
  IOOptions io_options = FinalizeIOOptions(opts);
  if (!writable_file_->IsSyncThreadSafe()) {
    return IOStatus::NotSupported(
        "Can't WritableFileWriter::SyncWithoutFlush() because "
        "WritableFile::IsSyncThreadSafe() is false");
  }
  TEST_SYNC_POINT("WritableFileWriter::SyncWithoutFlush:1");
  IOStatus s = SyncInternal(io_options, use_fsync);
  TEST_SYNC_POINT("WritableFileWriter::SyncWithoutFlush:2");
  if (!s.ok()) {
    set_seen_error(s);
  }
  return s;
}

IOStatus WritableFileWriter::SyncInternal(const IOOptions& opts,
                                          bool use_fsync) {
  // Caller is supposed to check seen_error_
  IOStatus s;
  IOSTATS_TIMER_GUARD(fsync_nanos);
  TEST_SYNC_POINT("WritableFileWriter::SyncInternal:0");
  auto prev_perf_level = GetPerfLevel();

  IOSTATS_CPU_TIMER_GUARD(cpu_write_nanos, clock_);

  FileOperationInfo::StartTimePoint start_ts;
  if (ShouldNotifyListeners()) {
    start_ts = FileOperationInfo::StartNow();
  }

  if (use_fsync) {
    s = writable_file_->Fsync(opts, nullptr);
  } else {
    s = writable_file_->Sync(opts, nullptr);
  }
  if (ShouldNotifyListeners()) {
    auto finish_ts = std::chrono::steady_clock::now();
    NotifyOnFileSyncFinish(
        start_ts, finish_ts, s,
        use_fsync ? FileOperationType::kFsync : FileOperationType::kSync);
    if (!s.ok()) {
      NotifyOnIOError(
          s, (use_fsync ? FileOperationType::kFsync : FileOperationType::kSync),
          file_name());
    }
  }
  SetPerfLevel(prev_perf_level);

  // The caller will be responsible to call set_seen_error(s) if s is not OK.
  return s;
}

IOStatus WritableFileWriter::RangeSync(uint64_t offset, uint64_t nbytes,
                                       const IOOptions& opts,
                                       IODebugContext* /*dbg*/) {
  if (seen_error()) {
    return GetWriterHasPreviousErrorStatus();
  }

  IOSTATS_TIMER_GUARD(range_sync_nanos);
  TEST_SYNC_POINT("WritableFileWriter::RangeSync:0");
  const IOOptions io_options = FinalizeIOOptions(opts);
  FileOperationInfo::StartTimePoint start_ts;
  if (ShouldNotifyListeners()) {
    start_ts = FileOperationInfo::StartNow();
  }
  IOStatus s = writable_file_->RangeSync(offset, nbytes, io_options, nullptr);
  if (!s.ok()) {
    set_seen_error(s);
  }
  if (ShouldNotifyListeners()) {
    auto finish_ts = std::chrono::steady_clock::now();
    NotifyOnFileRangeSyncFinish(offset, nbytes, start_ts, finish_ts, s);
    if (!s.ok()) {
      NotifyOnIOError(s, FileOperationType::kRangeSync, file_name(), nbytes,
                      offset);
    }
  }
  return s;
}

// This method writes to disk the specified data and makes use of the rate
// limiter if available
IOStatus WritableFileWriter::WriteBuffered(const IOOptions& opts,
                                           const char* data, size_t size) {
  if (seen_error()) {
    return GetWriterHasPreviousErrorStatus();
  }

  IOStatus s;
  assert(!use_direct_io());
  const char* src = data;
  size_t left = size;
  DataVerificationInfo v_info;
  char checksum_buf[sizeof(uint32_t)];
  Env::IOPriority rate_limiter_priority_used = opts.rate_limiter_priority;

  while (left > 0) {
    size_t allowed = left;
    if (rate_limiter_ != nullptr &&
        rate_limiter_priority_used != Env::IO_TOTAL) {
      allowed = rate_limiter_->RequestToken(left, 0 /* alignment */,
                                            rate_limiter_priority_used, stats_,
                                            RateLimiter::OpType::kWrite);
    }

    {
      IOSTATS_TIMER_GUARD(write_nanos);
      TEST_SYNC_POINT("WritableFileWriter::Flush:BeforeAppend");

      FileOperationInfo::StartTimePoint start_ts;
      uint64_t old_size = writable_file_->GetFileSize(opts, nullptr);
      if (ShouldNotifyListeners()) {
        start_ts = FileOperationInfo::StartNow();
        old_size = next_write_offset_;
      }
      {
        auto prev_perf_level = GetPerfLevel();

        IOSTATS_CPU_TIMER_GUARD(cpu_write_nanos, clock_);
        if (perform_data_verification_) {
          Crc32cHandoffChecksumCalculation(src, allowed, checksum_buf);
          v_info.checksum = Slice(checksum_buf, sizeof(uint32_t));
          s = writable_file_->Append(Slice(src, allowed), opts, v_info,
                                     nullptr);
        } else {
          s = writable_file_->Append(Slice(src, allowed), opts, nullptr);
        }
        if (!s.ok()) {
          // If writable_file_->Append() failed, then the data may or may not
          // exist in the underlying memory buffer, OS page cache, remote file
          // system's buffer, etc. If WritableFileWriter keeps the data in
          // buf_, then a future Close() or write retry may send the data to
          // the underlying file again. If the data does exist in the
          // underlying buffer and gets written to the file eventually despite
          // returning error, the file may end up with two duplicate pieces of
          // data. Therefore, clear the buf_ at the WritableFileWriter layer
          // and let caller determine error handling.
          buf_.Size(0);
          buffered_data_crc32c_checksum_ = 0;
        }
        SetPerfLevel(prev_perf_level);
      }
      if (ShouldNotifyListeners()) {
        auto finish_ts = std::chrono::steady_clock::now();
        NotifyOnFileWriteFinish(old_size, allowed, start_ts, finish_ts, s);
        if (!s.ok()) {
          NotifyOnIOError(s, FileOperationType::kAppend, file_name(), allowed,
                          old_size);
        }
      }
      if (!s.ok()) {
        set_seen_error(s);
        return s;
      }
    }

    IOSTATS_ADD(bytes_written, allowed);
    TEST_KILL_RANDOM("WritableFileWriter::WriteBuffered:0");

    left -= allowed;
    src += allowed;
    uint64_t cur_size = flushed_size_.load(std::memory_order_acquire);
    flushed_size_.store(cur_size + allowed, std::memory_order_release);
  }
  buf_.Size(0);
  buffered_data_crc32c_checksum_ = 0;
  if (!s.ok()) {
    set_seen_error(s);
  }
  return s;
}

IOStatus WritableFileWriter::WriteBufferedWithChecksum(const IOOptions& opts,
                                                       const char* data,
                                                       size_t size) {
  if (seen_error()) {
    return GetWriterHasPreviousErrorStatus();
  }

  IOStatus s;
  assert(!use_direct_io());
  assert(perform_data_verification_ && buffered_data_with_checksum_);
  const char* src = data;
  size_t left = size;
  DataVerificationInfo v_info;
  char checksum_buf[sizeof(uint32_t)];
  Env::IOPriority rate_limiter_priority_used = opts.rate_limiter_priority;
  // Check how much is allowed. Here, we loop until the rate limiter allows to
  // write the entire buffer.
  // TODO: need to be improved since it sort of defeats the purpose of the rate
  // limiter
  size_t data_size = left;
  if (rate_limiter_ != nullptr && rate_limiter_priority_used != Env::IO_TOTAL) {
    while (data_size > 0) {
      size_t tmp_size;
      tmp_size =
          rate_limiter_->RequestToken(data_size, 0, rate_limiter_priority_used,
                                      stats_, RateLimiter::OpType::kWrite);
      data_size -= tmp_size;
    }
  }

  {
    IOSTATS_TIMER_GUARD(write_nanos);
    TEST_SYNC_POINT("WritableFileWriter::Flush:BeforeAppend");

    FileOperationInfo::StartTimePoint start_ts;
    uint64_t old_size = writable_file_->GetFileSize(opts, nullptr);
    if (ShouldNotifyListeners()) {
      start_ts = FileOperationInfo::StartNow();
      old_size = next_write_offset_;
    }
    {
      auto prev_perf_level = GetPerfLevel();

      IOSTATS_CPU_TIMER_GUARD(cpu_write_nanos, clock_);

      EncodeFixed32(checksum_buf, buffered_data_crc32c_checksum_);
      v_info.checksum = Slice(checksum_buf, sizeof(uint32_t));
      s = writable_file_->Append(Slice(src, left), opts, v_info, nullptr);
      SetPerfLevel(prev_perf_level);
    }
    if (ShouldNotifyListeners()) {
      auto finish_ts = std::chrono::steady_clock::now();
      NotifyOnFileWriteFinish(old_size, left, start_ts, finish_ts, s);
      if (!s.ok()) {
        NotifyOnIOError(s, FileOperationType::kAppend, file_name(), left,
                        old_size);
      }
    }
    if (!s.ok()) {
      // If writable_file_->Append() failed, then the data may or may not
      // exist in the underlying memory buffer, OS page cache, remote file
      // system's buffer, etc. If WritableFileWriter keeps the data in
      // buf_, then a future Close() or write retry may send the data to
      // the underlying file again. If the data does exist in the
      // underlying buffer and gets written to the file eventually despite
      // returning error, the file may end up with two duplicate pieces of
      // data. Therefore, clear the buf_ at the WritableFileWriter layer
      // and let caller determine error handling.
      buf_.Size(0);
      buffered_data_crc32c_checksum_ = 0;
      set_seen_error(s);
      return s;
    }
  }

  IOSTATS_ADD(bytes_written, left);
  TEST_KILL_RANDOM("WritableFileWriter::WriteBuffered:0");

  // Buffer write is successful, reset the buffer current size to 0 and reset
  // the corresponding checksum value
  buf_.Size(0);
  buffered_data_crc32c_checksum_ = 0;
  uint64_t cur_size = flushed_size_.load(std::memory_order_acquire);
  flushed_size_.store(cur_size + left, std::memory_order_release);
  if (!s.ok()) {
    set_seen_error(s);
  }
  return s;
}

void WritableFileWriter::UpdateFileChecksum(const Slice& data) {
  if (checksum_generator_ != nullptr) {
    checksum_generator_->Update(data.data(), data.size());
  }
}

// Currently, crc32c checksum is used to calculate the checksum value of the
// content in the input buffer for handoff. In the future, the checksum might be
// calculated from the existing crc32c checksums of the in WAl and Manifest
// records, or even SST file blocks.
// TODO: effectively use the existing checksum of the data being writing to
// generate the crc32c checksum instead of a raw calculation.
void WritableFileWriter::Crc32cHandoffChecksumCalculation(const char* data,
                                                          size_t size,
                                                          char* buf) {
  uint32_t v_crc32c = crc32c::Extend(0, data, size);
  EncodeFixed32(buf, v_crc32c);
}

IOStatus WritableFileWriter::ValidateCopiedDataChecksum(
    const Slice& copied_data, Crc32cChecksum checksum) {
  return ValidateDataChecksum(copied_data, checksum, file_name_,
                              kDestinationBufferChecksumMismatch);
}

// This flushes the accumulated data in the buffer. We pad data with zeros if
// necessary to the whole page.
// However, during automatic flushes padding would not be necessary.
// We always use RateLimiter if available. We move (Refit) any buffer bytes
// that are left over the
// whole number of pages to be written again on the next flush because we can
// only write on aligned
// offsets.
IOStatus WritableFileWriter::WriteDirect(const IOOptions& opts) {
  if (seen_error()) {
    assert(false);

    return IOStatus::IOError("Writer has previous error.");
  }

  assert(use_direct_io());
  IOStatus s;
  const size_t alignment = buf_.Alignment();
  assert((next_write_offset_ % alignment) == 0);

  // Calculate whole page final file advance if all writes succeed
  const size_t file_advance =
      TruncateToPageBoundary(alignment, buf_.CurrentSize());

  // Calculate the leftover tail, we write it here padded with zeros BUT we
  // will write it again in the future either on Close() OR when the current
  // whole page fills out.
  const size_t leftover_tail = buf_.CurrentSize() - file_advance;

  // Round up and pad
  buf_.PadToAlignmentWith(0);

  const char* src = buf_.BufferStart();
  uint64_t write_offset = next_write_offset_;
  size_t left = buf_.CurrentSize();
  DataVerificationInfo v_info;
  char checksum_buf[sizeof(uint32_t)];
  Env::IOPriority rate_limiter_priority_used = opts.rate_limiter_priority;

  while (left > 0) {
    // Check how much is allowed
    size_t size = left;
    if (rate_limiter_ != nullptr &&
        rate_limiter_priority_used != Env::IO_TOTAL) {
      size = rate_limiter_->RequestToken(left, buf_.Alignment(),
                                         rate_limiter_priority_used, stats_,
                                         RateLimiter::OpType::kWrite);
    }

    {
      IOSTATS_TIMER_GUARD(write_nanos);
      TEST_SYNC_POINT("WritableFileWriter::Flush:BeforeAppend");
      FileOperationInfo::StartTimePoint start_ts;
      if (ShouldNotifyListeners()) {
        start_ts = FileOperationInfo::StartNow();
      }
      // direct writes must be positional
      if (perform_data_verification_) {
        Crc32cHandoffChecksumCalculation(src, size, checksum_buf);
        v_info.checksum = Slice(checksum_buf, sizeof(uint32_t));
        s = writable_file_->PositionedAppend(Slice(src, size), write_offset,
                                             opts, v_info, nullptr);
      } else {
        s = writable_file_->PositionedAppend(Slice(src, size), write_offset,
                                             opts, nullptr);
      }

      if (ShouldNotifyListeners()) {
        auto finish_ts = std::chrono::steady_clock::now();
        NotifyOnFileWriteFinish(write_offset, size, start_ts, finish_ts, s);
        if (!s.ok()) {
          NotifyOnIOError(s, FileOperationType::kPositionedAppend, file_name(),
                          size, write_offset);
        }
      }
      if (!s.ok()) {
        buf_.Size(file_advance + leftover_tail);
        set_seen_error(s);
        return s;
      }
    }

    IOSTATS_ADD(bytes_written, size);
    left -= size;
    src += size;
    write_offset += size;
    uint64_t cur_size = flushed_size_.load(std::memory_order_acquire);
    flushed_size_.store(cur_size + size, std::memory_order_release);
    assert((next_write_offset_ % alignment) == 0);
  }

  if (s.ok()) {
    // Move the tail to the beginning of the buffer
    // This never happens during normal Append but rather during
    // explicit call to Flush()/Sync() or Close()
    buf_.RefitTail(file_advance, leftover_tail);
    // This is where we start writing next time which may or not be
    // the actual file size on disk. They match if the buffer size
    // is a multiple of whole pages otherwise filesize_ is leftover_tail
    // behind
    next_write_offset_ += file_advance;
  } else {
    set_seen_error(s);
  }
  return s;
}

IOStatus WritableFileWriter::WriteDirectWithChecksum(const IOOptions& opts) {
  if (seen_error()) {
    return GetWriterHasPreviousErrorStatus();
  }

  assert(use_direct_io());
  assert(perform_data_verification_ && buffered_data_with_checksum_);
  IOStatus s;
  const size_t alignment = buf_.Alignment();
  assert((next_write_offset_ % alignment) == 0);

  // Calculate whole page final file advance if all writes succeed
  const size_t file_advance =
      TruncateToPageBoundary(alignment, buf_.CurrentSize());

  // Calculate the leftover tail, we write it here padded with zeros BUT we
  // will write it again in the future either on Close() OR when the current
  // whole page fills out.
  const size_t leftover_tail = buf_.CurrentSize() - file_advance;

  // Round up, pad, and combine the checksum.
  size_t last_cur_size = buf_.CurrentSize();
  buf_.PadToAlignmentWith(0);
  size_t padded_size = buf_.CurrentSize() - last_cur_size;
  const char* padded_start = buf_.BufferStart() + last_cur_size;
  uint32_t padded_checksum = crc32c::Value(padded_start, padded_size);
  buffered_data_crc32c_checksum_ = crc32c::Crc32cCombine(
      buffered_data_crc32c_checksum_, padded_checksum, padded_size);

  const char* src = buf_.BufferStart();
  uint64_t write_offset = next_write_offset_;
  size_t left = buf_.CurrentSize();
  DataVerificationInfo v_info;
  char checksum_buf[sizeof(uint32_t)];

  Env::IOPriority rate_limiter_priority_used = opts.rate_limiter_priority;
  // Check how much is allowed. Here, we loop until the rate limiter allows to
  // write the entire buffer.
  // TODO: need to be improved since it sort of defeats the purpose of the rate
  // limiter
  size_t data_size = left;
  if (rate_limiter_ != nullptr && rate_limiter_priority_used != Env::IO_TOTAL) {
    while (data_size > 0) {
      size_t size;
      size = rate_limiter_->RequestToken(data_size, buf_.Alignment(),
                                         rate_limiter_priority_used, stats_,
                                         RateLimiter::OpType::kWrite);
      data_size -= size;
    }
  }

  {
    IOSTATS_TIMER_GUARD(write_nanos);
    TEST_SYNC_POINT("WritableFileWriter::Flush:BeforeAppend");
    FileOperationInfo::StartTimePoint start_ts;
    if (ShouldNotifyListeners()) {
      start_ts = FileOperationInfo::StartNow();
    }
    // direct writes must be positional
    EncodeFixed32(checksum_buf, buffered_data_crc32c_checksum_);
    v_info.checksum = Slice(checksum_buf, sizeof(uint32_t));
    s = writable_file_->PositionedAppend(Slice(src, left), write_offset, opts,
                                         v_info, nullptr);

    if (ShouldNotifyListeners()) {
      auto finish_ts = std::chrono::steady_clock::now();
      NotifyOnFileWriteFinish(write_offset, left, start_ts, finish_ts, s);
      if (!s.ok()) {
        NotifyOnIOError(s, FileOperationType::kPositionedAppend, file_name(),
                        left, write_offset);
      }
    }
    if (!s.ok()) {
      // In this case, we do not change buffered_data_crc32c_checksum_ because
      // it still aligns with the data in the buffer.
      buf_.Size(file_advance + leftover_tail);
      buffered_data_crc32c_checksum_ =
          crc32c::Value(buf_.BufferStart(), buf_.CurrentSize());
      set_seen_error(s);
      return s;
    }
  }

  IOSTATS_ADD(bytes_written, left);
  assert((next_write_offset_ % alignment) == 0);
  uint64_t cur_size = flushed_size_.load(std::memory_order_acquire);
  flushed_size_.store(cur_size + left, std::memory_order_release);

  if (s.ok()) {
    // Move the tail to the beginning of the buffer
    // This never happens during normal Append but rather during
    // explicit call to Flush()/Sync() or Close(). Also the buffer checksum will
    // recalculated accordingly.
    buf_.RefitTail(file_advance, leftover_tail);
    // Adjust the checksum value to align with the data in the buffer
    buffered_data_crc32c_checksum_ =
        crc32c::Value(buf_.BufferStart(), buf_.CurrentSize());
    // This is where we start writing next time which may or not be
    // the actual file size on disk. They match if the buffer size
    // is a multiple of whole pages otherwise filesize_ is leftover_tail
    // behind
    next_write_offset_ += file_advance;
  } else {
    set_seen_error(s);
  }
  return s;
}
Env::IOPriority WritableFileWriter::DecideRateLimiterPriority(
    Env::IOPriority writable_file_io_priority,
    Env::IOPriority op_rate_limiter_priority) {
  if (writable_file_io_priority == Env::IO_TOTAL &&
      op_rate_limiter_priority == Env::IO_TOTAL) {
    return Env::IO_TOTAL;
  } else if (writable_file_io_priority == Env::IO_TOTAL) {
    return op_rate_limiter_priority;
  } else if (op_rate_limiter_priority == Env::IO_TOTAL) {
    return writable_file_io_priority;
  } else {
    return op_rate_limiter_priority;
  }
}

IOOptions WritableFileWriter::FinalizeIOOptions(const IOOptions& opts) const {
  Env::IOPriority op_rate_limiter_priority = opts.rate_limiter_priority;
  IOOptions io_options(opts);
  if (writable_file_.get() != nullptr) {
    io_options.rate_limiter_priority =
        WritableFileWriter::DecideRateLimiterPriority(
            writable_file_->GetIOPriority(), op_rate_limiter_priority);
  }
  if (io_options.type == IOType::kUnknown) {
    io_options.type = io_type_;
  }
  return io_options;
}
}  // namespace ROCKSDB_NAMESPACE
