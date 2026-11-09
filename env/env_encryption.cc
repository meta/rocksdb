//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/env_encryption.h"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <iostream>
#include <memory>

#include "env/composite_env_wrapper.h"
#include "env/env_encryption_ctr.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/convenience.h"
#include "rocksdb/io_status.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/options_type.h"
#include "util/aligned_buffer.h"
#include "util/coding.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
std::shared_ptr<EncryptionProvider> EncryptionProvider::NewCTRProvider(
    const std::shared_ptr<BlockCipher>& cipher) {
  return std::make_shared<CTREncryptionProvider>(cipher);
}

IOStatus EncryptedSequentialFile::Read(size_t n, const IOOptions& options,
                                       Slice* result, char* scratch,
                                       IODebugContext* dbg) {
  assert(scratch);
  IOStatus io_s = file_->Read(n, options, result, scratch, dbg);
  if (!io_s.ok()) {
    return io_s;
  }
  {
    PERF_TIMER_GUARD(decrypt_data_nanos);
    io_s = status_to_io_status(
        stream_->Decrypt(offset_, (char*)result->data(), result->size()));
  }
  if (io_s.ok()) {
    offset_ += result->size();  // We've already read data from disk, so update
                                // offset_ even if decryption fails.
  }
  return io_s;
}

IOStatus EncryptedSequentialFile::Skip(uint64_t n) {
  auto status = file_->Skip(n);
  if (!status.ok()) {
    return status;
  }
  offset_ += n;
  return status;
}

bool EncryptedSequentialFile::use_direct_io() const {
  return file_->use_direct_io();
}

size_t EncryptedSequentialFile::GetRequiredBufferAlignment() const {
  return file_->GetRequiredBufferAlignment();
}

IOStatus EncryptedSequentialFile::InvalidateCache(size_t offset,
                                                  size_t length) {
  return file_->InvalidateCache(offset + prefixLength_, length);
}

IOStatus EncryptedSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                                 const IOOptions& options,
                                                 Slice* result, char* scratch,
                                                 IODebugContext* dbg) {
  assert(scratch);
  offset += prefixLength_;  // Skip prefix
  auto io_s = file_->PositionedRead(offset, n, options, result, scratch, dbg);
  if (!io_s.ok()) {
    return io_s;
  }
  offset_ = offset + result->size();
  {
    PERF_TIMER_GUARD(decrypt_data_nanos);
    io_s = status_to_io_status(
        stream_->Decrypt(offset, (char*)result->data(), result->size()));
  }
  return io_s;
}

IOStatus EncryptedRandomAccessFile::Read(uint64_t offset, size_t n,
                                         const IOOptions& options,
                                         Slice* result, char* scratch,
                                         IODebugContext* dbg) const {
  assert(scratch);
  offset += prefixLength_;
  auto io_s = file_->Read(offset, n, options, result, scratch, dbg);
  if (!io_s.ok()) {
    return io_s;
  }
  {
    PERF_TIMER_GUARD(decrypt_data_nanos);
    io_s = status_to_io_status(
        stream_->Decrypt(offset, (char*)result->data(), result->size()));
  }
  return io_s;
}

IOStatus EncryptedRandomAccessFile::Prefetch(uint64_t offset, size_t n,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  return file_->Prefetch(offset + prefixLength_, n, options, dbg);
}

size_t EncryptedRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return file_->GetUniqueId(id, max_size);
}

void EncryptedRandomAccessFile::Hint(AccessPattern pattern) {
  file_->Hint(pattern);
}

bool EncryptedRandomAccessFile::use_direct_io() const {
  return file_->use_direct_io();
}

size_t EncryptedRandomAccessFile::GetRequiredBufferAlignment() const {
  return file_->GetRequiredBufferAlignment();
}

IOStatus EncryptedRandomAccessFile::InvalidateCache(size_t offset,
                                                    size_t length) {
  return file_->InvalidateCache(offset + prefixLength_, length);
}

IOStatus EncryptedWritableFile::Append(const Slice& data,
                                       const IOOptions& options,
                                       IODebugContext* dbg) {
  AlignedBuffer buf;
  Slice dataToAppend(data);
  if (data.size() > 0) {
    auto offset = file_->GetFileSize(options, dbg);  // size including prefix
    // Encrypt in cloned buffer
    buf.Alignment(GetRequiredBufferAlignment());
    buf.AllocateNewBuffer(data.size());
    // TODO (sagar0): Modify AlignedBuffer.Append to allow doing a memmove
    // so that the next two lines can be replaced with buf.Append().
    memmove(buf.BufferStart(), data.data(), data.size());
    buf.Size(data.size());
    IOStatus io_s;
    {
      PERF_TIMER_GUARD(encrypt_data_nanos);
      io_s = status_to_io_status(
          stream_->Encrypt(offset, buf.BufferStart(), buf.CurrentSize()));
    }
    if (!io_s.ok()) {
      return io_s;
    }
    dataToAppend = Slice(buf.BufferStart(), buf.CurrentSize());
  }
  return file_->Append(dataToAppend, options, dbg);
}

IOStatus EncryptedWritableFile::PositionedAppend(const Slice& data,
                                                 uint64_t offset,
                                                 const IOOptions& options,
                                                 IODebugContext* dbg) {
  AlignedBuffer buf;
  Slice dataToAppend(data);
  offset += prefixLength_;
  if (data.size() > 0) {
    // Encrypt in cloned buffer
    buf.Alignment(GetRequiredBufferAlignment());
    buf.AllocateNewBuffer(data.size());
    memmove(buf.BufferStart(), data.data(), data.size());
    buf.Size(data.size());
    IOStatus io_s;
    {
      PERF_TIMER_GUARD(encrypt_data_nanos);
      io_s = status_to_io_status(
          stream_->Encrypt(offset, buf.BufferStart(), buf.CurrentSize()));
    }
    if (!io_s.ok()) {
      return io_s;
    }
    dataToAppend = Slice(buf.BufferStart(), buf.CurrentSize());
  }
  return file_->PositionedAppend(dataToAppend, offset, options, dbg);
}

bool EncryptedWritableFile::use_direct_io() const {
  return file_->use_direct_io();
}

bool EncryptedWritableFile::IsSyncThreadSafe() const {
  return file_->IsSyncThreadSafe();
}

size_t EncryptedWritableFile::GetRequiredBufferAlignment() const {
  return file_->GetRequiredBufferAlignment();
}

uint64_t EncryptedWritableFile::GetFileSize(const IOOptions& options,
                                            IODebugContext* dbg) {
  return file_->GetFileSize(options, dbg) - prefixLength_;
}

IOStatus EncryptedWritableFile::Truncate(uint64_t size,
                                         const IOOptions& options,
                                         IODebugContext* dbg) {
  return file_->Truncate(size + prefixLength_, options, dbg);
}

IOStatus EncryptedWritableFile::InvalidateCache(size_t offset, size_t length) {
  return file_->InvalidateCache(offset + prefixLength_, length);
}

IOStatus EncryptedWritableFile::RangeSync(uint64_t offset, uint64_t nbytes,
                                          const IOOptions& options,
                                          IODebugContext* dbg) {
  return file_->RangeSync(offset + prefixLength_, nbytes, options, dbg);
}

void EncryptedWritableFile::PrepareWrite(size_t offset, size_t len,
                                         const IOOptions& options,
                                         IODebugContext* dbg) {
  file_->PrepareWrite(offset + prefixLength_, len, options, dbg);
}

void EncryptedWritableFile::SetPreallocationBlockSize(size_t size) {
  // the size here doesn't need to include prefixLength_, as it's a
  // configuration will be use for `PrepareWrite()`.
  file_->SetPreallocationBlockSize(size);
}

void EncryptedWritableFile::GetPreallocationStatus(
    size_t* block_size, size_t* last_allocated_block) {
  file_->GetPreallocationStatus(block_size, last_allocated_block);
}

IOStatus EncryptedWritableFile::Allocate(uint64_t offset, uint64_t len,
                                         const IOOptions& options,
                                         IODebugContext* dbg) {
  return file_->Allocate(offset + prefixLength_, len, options, dbg);
}

IOStatus EncryptedWritableFile::Flush(const IOOptions& options,
                                      IODebugContext* dbg) {
  return file_->Flush(options, dbg);
}

IOStatus EncryptedWritableFile::Sync(const IOOptions& options,
                                     IODebugContext* dbg) {
  return file_->Sync(options, dbg);
}

IOStatus EncryptedWritableFile::Close(const IOOptions& options,
                                      IODebugContext* dbg) {
  return file_->Close(options, dbg);
}

bool EncryptedRandomRWFile::use_direct_io() const {
  return file_->use_direct_io();
}

size_t EncryptedRandomRWFile::GetRequiredBufferAlignment() const {
  return file_->GetRequiredBufferAlignment();
}

IOStatus EncryptedRandomRWFile::Write(uint64_t offset, const Slice& data,
                                      const IOOptions& options,
                                      IODebugContext* dbg) {
  AlignedBuffer buf;
  Slice dataToWrite(data);
  offset += prefixLength_;
  if (data.size() > 0) {
    // Encrypt in cloned buffer
    buf.Alignment(GetRequiredBufferAlignment());
    buf.AllocateNewBuffer(data.size());
    memmove(buf.BufferStart(), data.data(), data.size());
    buf.Size(data.size());
    IOStatus io_s;
    {
      PERF_TIMER_GUARD(encrypt_data_nanos);
      io_s = status_to_io_status(
          stream_->Encrypt(offset, buf.BufferStart(), buf.CurrentSize()));
    }
    if (!io_s.ok()) {
      return io_s;
    }
    dataToWrite = Slice(buf.BufferStart(), buf.CurrentSize());
  }
  return file_->Write(offset, dataToWrite, options, dbg);
}

IOStatus EncryptedRandomRWFile::Read(uint64_t offset, size_t n,
                                     const IOOptions& options, Slice* result,
                                     char* scratch, IODebugContext* dbg) const {
  assert(scratch);
  offset += prefixLength_;
  auto status = file_->Read(offset, n, options, result, scratch, dbg);
  if (!status.ok()) {
    return status;
  }
  {
    PERF_TIMER_GUARD(decrypt_data_nanos);
    status = status_to_io_status(
        stream_->Decrypt(offset, (char*)result->data(), result->size()));
  }
  return status;
}

IOStatus EncryptedRandomRWFile::Flush(const IOOptions& options,
                                      IODebugContext* dbg) {
  return file_->Flush(options, dbg);
}

IOStatus EncryptedRandomRWFile::Sync(const IOOptions& options,
                                     IODebugContext* dbg) {
  return file_->Sync(options, dbg);
}

IOStatus EncryptedRandomRWFile::Fsync(const IOOptions& options,
                                      IODebugContext* dbg) {
  return file_->Fsync(options, dbg);
}

IOStatus EncryptedRandomRWFile::Close(const IOOptions& options,
                                      IODebugContext* dbg) {
  return file_->Close(options, dbg);
}

namespace {
static std::unordered_map<std::string, OptionTypeInfo> encrypted_fs_type_info =
    {
        {"provider",
         OptionTypeInfo::AsCustomSharedPtr<EncryptionProvider>(
             0 /* No offset, whole struct*/, OptionVerificationType::kByName,
             OptionTypeFlags::kNone)},
};
// EncryptedFileSystemImpl implements an FileSystemWrapper that adds encryption
// to files stored on disk.
class EncryptedFileSystemImpl : public EncryptedFileSystem {
 public:
  const char* Name() const override {
    return EncryptedFileSystem::kClassName();
  }
  // Returns the raw encryption provider that should be used to write the input
  // encrypted file.  If there is no such provider, NotFound is returned.
  IOStatus GetWritableProvider(const std::string& /*fname*/,
                               EncryptionProvider** result) {
    if (provider_) {
      *result = provider_.get();
      return IOStatus::OK();
    } else {
      *result = nullptr;
      return IOStatus::NotFound("No WriteProvider specified");
    }
  }

  // Returns the raw encryption provider that should be used to read the input
  // encrypted file.  If there is no such provider, NotFound is returned.
  IOStatus GetReadableProvider(const std::string& /*fname*/,
                               EncryptionProvider** result) {
    if (provider_) {
      *result = provider_.get();
      return IOStatus::OK();
    } else {
      *result = nullptr;
      return IOStatus::NotFound("No Provider specified");
    }
  }

  // Creates a CipherStream for the underlying file/name using the options
  // If a writable provider is found and encryption is enabled, uses
  // this provider to create a cipher stream.
  // @param fname         Name of the writable file
  // @param underlying    The underlying "raw" file
  // @param options       Options for creating the file/cipher
  // @param prefix_length Returns the length of the encryption prefix used for
  // this file
  // @param stream        Returns the cipher stream to use for this file if it
  // should be encrypted
  // @return OK on success, non-OK on failure.
  template <class TypeFile>
  IOStatus CreateWritableCipherStream(
      const std::string& fname, const std::unique_ptr<TypeFile>& underlying,
      const FileOptions& options, size_t* prefix_length,
      std::unique_ptr<BlockAccessCipherStream>* stream, IODebugContext* dbg) {
    EncryptionProvider* provider = nullptr;
    *prefix_length = 0;
    IOStatus status = GetWritableProvider(fname, &provider);
    if (!status.ok()) {
      return status;
    } else if (provider != nullptr) {
      // Initialize & write prefix (if needed)
      AlignedBuffer buffer;
      Slice prefix;
      *prefix_length = provider->GetPrefixLength();
      if (*prefix_length > 0) {
        // Initialize prefix
        buffer.Alignment(underlying->GetRequiredBufferAlignment());
        buffer.AllocateNewBuffer(*prefix_length);
        status = status_to_io_status(provider->CreateNewPrefix(
            fname, buffer.BufferStart(), *prefix_length));
        if (status.ok()) {
          buffer.Size(*prefix_length);
          prefix = Slice(buffer.BufferStart(), buffer.CurrentSize());
          // Write prefix
          status = underlying->Append(prefix, options.io_options, dbg);
        }
        if (!status.ok()) {
          return status;
        }
      }
      // Create cipher stream
      status = status_to_io_status(
          provider->CreateCipherStream(fname, options, prefix, stream));
    }
    return status;
  }

  template <class TypeFile>
  IOStatus CreateWritableEncryptedFile(const std::string& fname,
                                       std::unique_ptr<TypeFile>& underlying,
                                       const FileOptions& options,
                                       std::unique_ptr<TypeFile>* result,
                                       IODebugContext* dbg) {
    // Create cipher stream
    std::unique_ptr<BlockAccessCipherStream> stream;
    size_t prefix_length;
    IOStatus status = CreateWritableCipherStream(fname, underlying, options,
                                                 &prefix_length, &stream, dbg);
    if (status.ok()) {
      if (stream) {
        result->reset(new EncryptedWritableFile(
            std::move(underlying), std::move(stream), prefix_length));
      } else {
        result->reset(underlying.release());
      }
    }
    return status;
  }

  // Creates a CipherStream for the underlying file/name using the options
  // If a writable provider is found and encryption is enabled, uses
  // this provider to create a cipher stream.
  // @param fname         Name of the writable file
  // @param underlying    The underlying "raw" file
  // @param options       Options for creating the file/cipher
  // @param prefix_length Returns the length of the encryption prefix used for
  // this file
  // @param stream        Returns the cipher stream to use for this file if it
  // should be encrypted
  // @return OK on success, non-OK on failure.
  template <class TypeFile>
  IOStatus CreateRandomWriteCipherStream(
      const std::string& fname, const std::unique_ptr<TypeFile>& underlying,
      const FileOptions& options, size_t* prefix_length,
      std::unique_ptr<BlockAccessCipherStream>* stream, IODebugContext* dbg) {
    EncryptionProvider* provider = nullptr;
    *prefix_length = 0;
    IOStatus io_s = GetWritableProvider(fname, &provider);
    if (!io_s.ok()) {
      return io_s;
    } else if (provider != nullptr) {
      // Initialize & write prefix (if needed)
      AlignedBuffer buffer;
      Slice prefix;
      *prefix_length = provider->GetPrefixLength();
      if (*prefix_length > 0) {
        // Initialize prefix
        buffer.Alignment(underlying->GetRequiredBufferAlignment());
        buffer.AllocateNewBuffer(*prefix_length);
        io_s = status_to_io_status(provider->CreateNewPrefix(
            fname, buffer.BufferStart(), *prefix_length));
        if (io_s.ok()) {
          buffer.Size(*prefix_length);
          prefix = Slice(buffer.BufferStart(), buffer.CurrentSize());
          // Write prefix
          io_s = underlying->Write(0, prefix, options.io_options, dbg);
        }
        if (!io_s.ok()) {
          return io_s;
        }
      }
      // Create cipher stream
      io_s = status_to_io_status(
          provider->CreateCipherStream(fname, options, prefix, stream));
    }
    return io_s;
  }

  // Creates a CipherStream for the underlying file/name using the options
  // If a readable provider is found and the file is encrypted, uses
  // this provider to create a cipher stream.
  // @param fname         Name of the writable file
  // @param underlying    The underlying "raw" file
  // @param options       Options for creating the file/cipher
  // @param prefix_length Returns the length of the encryption prefix used for
  // this file
  // @param stream        Returns the cipher stream to use for this file if it
  // is encrypted
  // @return OK on success, non-OK on failure.
  template <class TypeFile>
  IOStatus CreateSequentialCipherStream(
      const std::string& fname, const std::unique_ptr<TypeFile>& underlying,
      const FileOptions& options, size_t* prefix_length,
      std::unique_ptr<BlockAccessCipherStream>* stream, IODebugContext* dbg) {
    // Read prefix (if needed)
    AlignedBuffer buffer;
    Slice prefix;
    *prefix_length = provider_->GetPrefixLength();
    if (*prefix_length > 0) {
      // Read prefix
      buffer.Alignment(underlying->GetRequiredBufferAlignment());
      buffer.AllocateNewBuffer(*prefix_length);
      IOStatus status = underlying->Read(*prefix_length, options.io_options,
                                         &prefix, buffer.BufferStart(), dbg);
      if (!status.ok()) {
        return status;
      }
      buffer.Size(*prefix_length);
    }
    return status_to_io_status(
        provider_->CreateCipherStream(fname, options, prefix, stream));
  }

  // Creates a CipherStream for the underlying file/name using the options
  // If a readable provider is found and the file is encrypted, uses
  // this provider to create a cipher stream.
  // @param fname         Name of the writable file
  // @param underlying    The underlying "raw" file
  // @param options       Options for creating the file/cipher
  // @param prefix_length Returns the length of the encryption prefix used for
  // this file
  // @param stream        Returns the cipher stream to use for this file if it
  // is encrypted
  // @return OK on success, non-OK on failure.
  template <class TypeFile>
  IOStatus CreateRandomReadCipherStream(
      const std::string& fname, const std::unique_ptr<TypeFile>& underlying,
      const FileOptions& options, size_t* prefix_length,
      std::unique_ptr<BlockAccessCipherStream>* stream, IODebugContext* dbg) {
    // Read prefix (if needed)
    AlignedBuffer buffer;
    Slice prefix;
    *prefix_length = provider_->GetPrefixLength();
    if (*prefix_length > 0) {
      // Read prefix
      buffer.Alignment(underlying->GetRequiredBufferAlignment());
      buffer.AllocateNewBuffer(*prefix_length);
      IOStatus status = underlying->Read(0, *prefix_length, options.io_options,
                                         &prefix, buffer.BufferStart(), dbg);
      if (!status.ok()) {
        return status;
      }
      buffer.Size(*prefix_length);
    }
    return status_to_io_status(
        provider_->CreateCipherStream(fname, options, prefix, stream));
  }

 public:
  EncryptedFileSystemImpl(const std::shared_ptr<FileSystem>& base,
                          const std::shared_ptr<EncryptionProvider>& provider)
      : EncryptedFileSystem(base) {
    provider_ = provider;
    RegisterOptions("EncryptionProvider", &provider_, &encrypted_fs_type_info);
  }

  Status AddCipher(const std::string& descriptor, const char* cipher,
                   size_t len, bool for_write) override {
    return provider_->AddCipher(descriptor, cipher, len, for_write);
  }

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override {
    result->reset();
    if (options.use_mmap_reads) {
      return IOStatus::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<FSSequentialFile> underlying;
    auto status =
        FileSystemWrapper::NewSequentialFile(fname, options, &underlying, dbg);
    if (!status.ok()) {
      return status;
    }
    uint64_t file_size;
    status = FileSystemWrapper::GetFileSize(fname, options.io_options,
                                            &file_size, dbg);
    if (!status.ok()) {
      return status;
    }
    if (!file_size) {
      *result = std::move(underlying);
      return status;
    }
    // Create cipher stream
    std::unique_ptr<BlockAccessCipherStream> stream;
    size_t prefix_length;
    status = CreateSequentialCipherStream(fname, underlying, options,
                                          &prefix_length, &stream, dbg);
    if (status.ok()) {
      result->reset(new EncryptedSequentialFile(
          std::move(underlying), std::move(stream), prefix_length));
    }
    return status;
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    result->reset();
    if (options.use_mmap_reads) {
      return IOStatus::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<FSRandomAccessFile> underlying;
    auto status = FileSystemWrapper::NewRandomAccessFile(fname, options,
                                                         &underlying, dbg);
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<BlockAccessCipherStream> stream;
    size_t prefix_length;
    status = CreateRandomReadCipherStream(fname, underlying, options,
                                          &prefix_length, &stream, dbg);
    if (status.ok()) {
      if (stream) {
        result->reset(new EncryptedRandomAccessFile(
            std::move(underlying), std::move(stream), prefix_length));
      } else {
        result->reset(underlying.release());
      }
    }
    return status;
  }

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    result->reset();
    if (options.use_mmap_writes) {
      return IOStatus::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<FSWritableFile> underlying;
    IOStatus status =
        FileSystemWrapper::NewWritableFile(fname, options, &underlying, dbg);
    if (!status.ok()) {
      return status;
    }
    return CreateWritableEncryptedFile(fname, underlying, options, result, dbg);
  }

  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override {
    result->reset();
    if (options.use_mmap_reads || options.use_mmap_writes) {
      return IOStatus::InvalidArgument();
    }

    size_t prefix_length = 0;
    std::unique_ptr<BlockAccessCipherStream> stream;

    // Open file using underlying Env implementation
    std::unique_ptr<FSWritableFile> underlying;
    auto status =
        FileSystemWrapper::ReopenWritableFile(fname, options, &underlying, dbg);
    if (!status.ok()) {
      return status;
    }

    if (underlying->GetFileSize(options.io_options, dbg) != 0) {
      // read the cipher stream from file for non-empty file
      std::unique_ptr<FSRandomAccessFile> underlying_file_reader;
      status = FileSystemWrapper::NewRandomAccessFile(
          fname, options, &underlying_file_reader, dbg);
      if (!status.ok()) {
        return status;
      }

      status = CreateRandomReadCipherStream(
          fname, underlying_file_reader, options, &prefix_length, &stream, dbg);

      if (!status.ok()) {
        return status;
      }
    } else {
      // create cipher stream for new or empty file
      status = CreateWritableCipherStream(fname, underlying, options,
                                          &prefix_length, &stream, dbg);
      if (!status.ok()) {
        return status;
      }
    }

    if (stream) {
      result->reset(new EncryptedWritableFile(
          std::move(underlying), std::move(stream), prefix_length));
    } else {
      result->reset(underlying.release());
    }
    return status;
  }

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override {
    result->reset();
    if (options.use_mmap_writes) {
      return IOStatus::InvalidArgument();
    }
    // Open file using underlying Env implementation
    std::unique_ptr<FSWritableFile> underlying;
    auto status = FileSystemWrapper::ReuseWritableFile(
        fname, old_fname, options, &underlying, dbg);
    if (!status.ok()) {
      return status;
    }
    return CreateWritableEncryptedFile(fname, underlying, options, result, dbg);
  }

  IOStatus NewRandomRWFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override {
    result->reset();
    if (options.use_mmap_reads || options.use_mmap_writes) {
      return IOStatus::InvalidArgument();
    }
    // Check file exists
    bool isNewFile = !FileExists(fname, options.io_options, dbg).ok();

    // Open file using underlying Env implementation
    std::unique_ptr<FSRandomRWFile> underlying;
    auto status =
        FileSystemWrapper::NewRandomRWFile(fname, options, &underlying, dbg);
    if (!status.ok()) {
      return status;
    }
    // Create cipher stream
    std::unique_ptr<BlockAccessCipherStream> stream;
    size_t prefix_length = 0;
    if (!isNewFile) {
      // File already exists, read prefix
      status = CreateRandomReadCipherStream(fname, underlying, options,
                                            &prefix_length, &stream, dbg);
    } else {
      status = CreateRandomWriteCipherStream(fname, underlying, options,
                                             &prefix_length, &stream, dbg);
    }
    if (status.ok()) {
      if (stream) {
        result->reset(new EncryptedRandomRWFile(
            std::move(underlying), std::move(stream), prefix_length));
      } else {
        result->reset(underlying.release());
      }
    }
    return status;
  }

  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& options,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* dbg) override {
    auto status =
        FileSystemWrapper::GetChildrenFileAttributes(dir, options, result, dbg);
    if (!status.ok()) {
      return status;
    }
    for (auto it = std::begin(*result); it != std::end(*result); ++it) {
      // assert(it->size_bytes >= prefixLength);
      //  breaks env_basic_test when called on directory containing
      //  directories
      // which makes subtraction of prefixLength worrisome since
      // FileAttributes does not identify directories
      EncryptionProvider* provider;
      status = GetReadableProvider(it->name, &provider);
      if (!status.ok()) {
        return status;
      } else if (provider != nullptr) {
        it->size_bytes -= provider->GetPrefixLength();
      }
    }
    return IOStatus::OK();
  }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override {
    auto status =
        FileSystemWrapper::GetFileSize(fname, options, file_size, dbg);
    if (!status.ok() || !(*file_size)) {
      return status;
    }
    EncryptionProvider* provider;
    status = GetReadableProvider(fname, &provider);
    if (provider != nullptr && status.ok()) {
      size_t prefixLength = provider->GetPrefixLength();
      assert(*file_size >= prefixLength);
      *file_size -= prefixLength;
    }
    return status;
  }

  IOStatus SyncFile(const std::string& fname, const FileOptions& file_opts,
                    const IOOptions& io_opts, bool use_fsync,
                    IODebugContext* dbg) override {
    // SyncFile does not read or write file contents, so it can delegate
    // directly to the underlying filesystem without constructing an encrypted
    // writable wrapper.
    return FileSystemWrapper::SyncFile(fname, file_opts, io_opts, use_fsync,
                                       dbg);
  }

 private:
  std::shared_ptr<EncryptionProvider> provider_;
};
}  // namespace

Status NewEncryptedFileSystemImpl(
    const std::shared_ptr<FileSystem>& base,
    const std::shared_ptr<EncryptionProvider>& provider,
    std::unique_ptr<FileSystem>* result) {
  result->reset(new EncryptedFileSystemImpl(base, provider));
  return Status::OK();
}

std::shared_ptr<FileSystem> NewEncryptedFS(
    const std::shared_ptr<FileSystem>& base,
    const std::shared_ptr<EncryptionProvider>& provider) {
  std::unique_ptr<FileSystem> efs;
  Status s = NewEncryptedFileSystemImpl(base, provider, &efs);
  if (s.ok()) {
    s = efs->PrepareOptions(ConfigOptions());
  }
  if (s.ok()) {
    std::shared_ptr<FileSystem> result(efs.release());
    return result;
  } else {
    return nullptr;
  }
}

Env* NewEncryptedEnv(Env* base_env,
                     const std::shared_ptr<EncryptionProvider>& provider) {
  return new CompositeEnvWrapper(
      base_env, NewEncryptedFS(base_env->GetFileSystem(), provider));
}

Status BlockAccessCipherStream::Encrypt(uint64_t fileOffset, char* data,
                                        size_t dataSize) {
  // Calculate block index
  auto blockSize = BlockSize();
  uint64_t blockIndex = fileOffset / blockSize;
  size_t blockOffset = fileOffset % blockSize;
  std::unique_ptr<char[]> blockBuffer;

  std::string scratch;
  AllocateScratch(scratch);

  // Encrypt individual blocks.
  while (true) {
    char* block = data;
    size_t n = std::min(dataSize, blockSize - blockOffset);
    if (n != blockSize) {
      // We're not encrypting a full block.
      // Copy data to blockBuffer
      if (!blockBuffer) {
        // Allocate buffer
        blockBuffer = std::unique_ptr<char[]>(new char[blockSize]);
      }
      block = blockBuffer.get();
      // Copy plain data to block buffer
      memmove(block + blockOffset, data, n);
    }
    auto status = EncryptBlock(blockIndex, block, (char*)scratch.data());
    if (!status.ok()) {
      return status;
    }
    if (block != data) {
      // Copy encrypted data back to `data`.
      memmove(data, block + blockOffset, n);
    }
    dataSize -= n;
    if (dataSize == 0) {
      return Status::OK();
    }
    data += n;
    blockOffset = 0;
    blockIndex++;
  }
}

Status BlockAccessCipherStream::Decrypt(uint64_t fileOffset, char* data,
                                        size_t dataSize) {
  // Calculate block index
  auto blockSize = BlockSize();
  uint64_t blockIndex = fileOffset / blockSize;
  size_t blockOffset = fileOffset % blockSize;
  std::unique_ptr<char[]> blockBuffer;

  std::string scratch;
  AllocateScratch(scratch);

  // Decrypt individual blocks.
  while (true) {
    char* block = data;
    size_t n = std::min(dataSize, blockSize - blockOffset);
    if (n != blockSize) {
      // We're not decrypting a full block.
      // Copy data to blockBuffer
      if (!blockBuffer) {
        // Allocate buffer
        blockBuffer = std::unique_ptr<char[]>(new char[blockSize]);
      }
      block = blockBuffer.get();
      // Copy encrypted data to block buffer
      memmove(block + blockOffset, data, n);
    }
    auto status = DecryptBlock(blockIndex, block, (char*)scratch.data());
    if (!status.ok()) {
      return status;
    }
    if (block != data) {
      // Copy decrypted data back to `data`.
      memmove(data, block + blockOffset, n);
    }

    // Simply decrementing dataSize by n could cause it to underflow,
    // which will very likely make it read over the original bounds later
    assert(dataSize >= n);
    if (dataSize < n) {
      return Status::Corruption("Cannot decrypt data at given offset");
    }

    dataSize -= n;
    if (dataSize == 0) {
      return Status::OK();
    }
    data += n;
    blockOffset = 0;
    blockIndex++;
  }
}

namespace {
static std::unordered_map<std::string, OptionTypeInfo>
    rot13_block_cipher_type_info = {
        {"block_size",
         {0 /* No offset, whole struct*/, OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
};

// Implements a BlockCipher using ROT13.
//
// Note: This is a sample implementation of BlockCipher,
// it is NOT considered safe and should NOT be used in production.
class ROT13BlockCipher : public BlockCipher {
 private:
  size_t blockSize_;

 public:
  explicit ROT13BlockCipher(size_t blockSize) : blockSize_(blockSize) {
    RegisterOptions("ROT13BlockCipherOptions", &blockSize_,
                    &rot13_block_cipher_type_info);
  }

  static const char* kClassName() { return "ROT13"; }
  const char* Name() const override { return kClassName(); }

  size_t BlockSize() override { return blockSize_; }
  Status Encrypt(char* data) override {
    for (size_t i = 0; i < blockSize_; ++i) {
      data[i] += 13;
    }
    return Status::OK();
  }
  Status Decrypt(char* data) override { return Encrypt(data); }
};

static const std::unordered_map<std::string, OptionTypeInfo>
    ctr_encryption_provider_type_info = {
        {"cipher",
         OptionTypeInfo::AsCustomSharedPtr<BlockCipher>(
             0 /* No offset, whole struct*/, OptionVerificationType::kByName,
             OptionTypeFlags::kNone)},
};
}  // anonymous namespace

void CTRCipherStream::AllocateScratch(std::string& scratch) {
  auto blockSize = cipher_->BlockSize();
  scratch.reserve(blockSize);
}

Status CTRCipherStream::EncryptBlock(uint64_t blockIndex, char* data,
                                     char* scratch) {
  // Create nonce + counter
  auto blockSize = cipher_->BlockSize();
  memmove(scratch, iv_.data(), blockSize);
  EncodeFixed64(scratch, blockIndex + initialCounter_);

  // Encrypt nonce + counter
  auto status = cipher_->Encrypt(scratch);
  if (!status.ok()) {
    return status;
  }

  // XOR data with ciphertext.
  for (size_t i = 0; i < blockSize; i++) {
    data[i] = data[i] ^ scratch[i];
  }
  return Status::OK();
}

Status CTRCipherStream::DecryptBlock(uint64_t blockIndex, char* data,
                                     char* scratch) {
  // For CTR decryption & encryption are the same
  return EncryptBlock(blockIndex, data, scratch);
}

CTREncryptionProvider::CTREncryptionProvider(
    const std::shared_ptr<BlockCipher>& c)
    : cipher_(c) {
  RegisterOptions("Cipher", &cipher_, &ctr_encryption_provider_type_info);
}

bool CTREncryptionProvider::IsInstanceOf(const std::string& name) const {
  // Special case for test purposes.
  if (name == "1://test" && cipher_ != nullptr) {
    return cipher_->IsInstanceOf(ROT13BlockCipher::kClassName());
  } else {
    return EncryptionProvider::IsInstanceOf(name);
  }
}

size_t CTREncryptionProvider::GetPrefixLength() const {
  return defaultPrefixLength;
}

Status CTREncryptionProvider::AddCipher(const std::string& /*descriptor*/,
                                        const char* cipher, size_t len,
                                        bool /*for_write*/) {
  if (cipher_) {
    return Status::NotSupported("Cannot add keys to CTREncryptionProvider");
  } else if (strcmp(ROT13BlockCipher::kClassName(), cipher) == 0) {
    cipher_ = std::make_shared<ROT13BlockCipher>(len);
    return Status::OK();
  } else {
    return BlockCipher::CreateFromString(ConfigOptions(), std::string(cipher),
                                         &cipher_);
  }
}

// decodeCTRParameters decodes the initial counter & IV from the given
// (plain text) prefix.
static void decodeCTRParameters(const char* prefix, size_t blockSize,
                                uint64_t& initialCounter, Slice& iv) {
  // First block contains 64-bit initial counter
  initialCounter = DecodeFixed64(prefix);
  // Second block contains IV
  iv = Slice(prefix + blockSize, blockSize);
}

Status CTREncryptionProvider::CreateNewPrefix(const std::string& /*fname*/,
                                              char* prefix,
                                              size_t prefixLength) const {
  if (!cipher_) {
    return Status::InvalidArgument("Encryption Cipher is missing");
  }
  // Create & seed rnd.
  Random rnd((uint32_t)SystemClock::Default()->NowMicros());
  // Fill entire prefix block with random values.
  for (size_t i = 0; i < prefixLength; i++) {
    prefix[i] = rnd.Uniform(256) & 0xFF;
  }
  // Take random data to extract initial counter & IV
  auto blockSize = cipher_->BlockSize();
  uint64_t initialCounter;
  Slice prefixIV;
  decodeCTRParameters(prefix, blockSize, initialCounter, prefixIV);

  // Now populate the rest of the prefix, starting from the third block.
  PopulateSecretPrefixPart(prefix + (2 * blockSize),
                           prefixLength - (2 * blockSize), blockSize);

  // Encrypt the prefix, starting from block 2 (leave block 0, 1 with initial
  // counter & IV unencrypted)
  CTRCipherStream cipherStream(cipher_, prefixIV.data(), initialCounter);
  Status status;
  {
    PERF_TIMER_GUARD(encrypt_data_nanos);
    status = cipherStream.Encrypt(0, prefix + (2 * blockSize),
                                  prefixLength - (2 * blockSize));
  }

  return status;
}

// PopulateSecretPrefixPart initializes the data into a new prefix block
// in plain text.
// Returns the amount of space (starting from the start of the prefix)
// that has been initialized.
size_t CTREncryptionProvider::PopulateSecretPrefixPart(
    char* /*prefix*/, size_t /*prefixLength*/, size_t /*blockSize*/) const {
  // Nothing to do here, put in custom data in override when needed.
  return 0;
}

Status CTREncryptionProvider::CreateCipherStream(
    const std::string& fname, const EnvOptions& options, Slice& prefix,
    std::unique_ptr<BlockAccessCipherStream>* result) {
  if (!cipher_) {
    return Status::InvalidArgument("Encryption Cipher is missing");
  }
  // Read plain text part of prefix.
  auto blockSize = cipher_->BlockSize();
  uint64_t initialCounter;
  Slice iv;
  decodeCTRParameters(prefix.data(), blockSize, initialCounter, iv);

  // If the prefix is smaller than twice the block size, we would below read a
  // very large chunk of the file (and very likely read over the bounds)
  assert(prefix.size() >= 2 * blockSize);
  if (prefix.size() < 2 * blockSize) {
    return Status::Corruption("Unable to read from file " + fname +
                              ": read attempt would read beyond file bounds");
  }

  // Decrypt the encrypted part of the prefix, starting from block 2 (block 0, 1
  // with initial counter & IV are unencrypted)
  CTRCipherStream cipherStream(cipher_, iv.data(), initialCounter);
  Status status;
  {
    PERF_TIMER_GUARD(decrypt_data_nanos);
    status = cipherStream.Decrypt(0, (char*)prefix.data() + (2 * blockSize),
                                  prefix.size() - (2 * blockSize));
  }
  if (!status.ok()) {
    return status;
  }

  // Create cipher stream
  return CreateCipherStreamFromPrefix(fname, options, initialCounter, iv,
                                      prefix, result);
}

// CreateCipherStreamFromPrefix creates a block access cipher stream for a file
// given name and options. The given prefix is already decrypted.
Status CTREncryptionProvider::CreateCipherStreamFromPrefix(
    const std::string& /*fname*/, const EnvOptions& /*options*/,
    uint64_t initialCounter, const Slice& iv, const Slice& /*prefix*/,
    std::unique_ptr<BlockAccessCipherStream>* result) {
  (*result) = std::unique_ptr<BlockAccessCipherStream>(
      new CTRCipherStream(cipher_, iv.data(), initialCounter));
  return Status::OK();
}

namespace {
static void RegisterEncryptionBuiltins() {
  static std::once_flag once;
  std::call_once(once, [&]() {
    auto lib = ObjectRegistry::Default()->AddLibrary("encryption");
    // Match "CTR" or "CTR://test"
    lib->AddFactory<EncryptionProvider>(
        ObjectLibrary::PatternEntry(CTREncryptionProvider::kClassName(), true)
            .AddSuffix("://test"),
        [](const std::string& uri, std::unique_ptr<EncryptionProvider>* guard,
           std::string* /*errmsg*/) {
          if (EndsWith(uri, "://test")) {
            std::shared_ptr<BlockCipher> cipher =
                std::make_shared<ROT13BlockCipher>(32);
            guard->reset(new CTREncryptionProvider(cipher));
          } else {
            guard->reset(new CTREncryptionProvider());
          }
          return guard->get();
        });

    lib->AddFactory<EncryptionProvider>(
        "1://test", [](const std::string& /*uri*/,
                       std::unique_ptr<EncryptionProvider>* guard,
                       std::string* /*errmsg*/) {
          std::shared_ptr<BlockCipher> cipher =
              std::make_shared<ROT13BlockCipher>(32);
          guard->reset(new CTREncryptionProvider(cipher));
          return guard->get();
        });

    // Match "ROT13" or "ROT13:[0-9]+"
    lib->AddFactory<BlockCipher>(
        ObjectLibrary::PatternEntry(ROT13BlockCipher::kClassName(), true)
            .AddNumber(":"),
        [](const std::string& uri, std::unique_ptr<BlockCipher>* guard,
           std::string* /* errmsg */) {
          size_t colon = uri.find(':');
          if (colon != std::string::npos) {
            size_t block_size = ParseSizeT(uri.substr(colon + 1));
            guard->reset(new ROT13BlockCipher(block_size));
          } else {
            guard->reset(new ROT13BlockCipher(32));
          }

          return guard->get();
        });
  });
}
}  // namespace

Status BlockCipher::CreateFromString(const ConfigOptions& config_options,
                                     const std::string& value,
                                     std::shared_ptr<BlockCipher>* result) {
  RegisterEncryptionBuiltins();
  return LoadSharedObject<BlockCipher>(config_options, value, result);
}

Status EncryptionProvider::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<EncryptionProvider>* result) {
  RegisterEncryptionBuiltins();
  return LoadSharedObject<EncryptionProvider>(config_options, value, result);
}

}  // namespace ROCKSDB_NAMESPACE
