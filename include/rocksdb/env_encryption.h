//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "rocksdb/customizable.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class EncryptionProvider;

struct ConfigOptions;

// Returns an Env that encrypts data when stored on disk and decrypts data when
// read from disk.
Env* NewEncryptedEnv(Env* base_env,
                     const std::shared_ptr<EncryptionProvider>& provider);
std::shared_ptr<FileSystem> NewEncryptedFS(
    const std::shared_ptr<FileSystem>& base_fs,
    const std::shared_ptr<EncryptionProvider>& provider);

// BlockAccessCipherStream is the base class for any cipher stream that
// supports random access at block level (without requiring data from other
// blocks). E.g. CTR (Counter operation mode) supports this requirement.
class BlockAccessCipherStream {
 public:
  virtual ~BlockAccessCipherStream() = default;

  // BlockSize returns the size of each block supported by this cipher stream.
  virtual size_t BlockSize() = 0;

  // Encrypt one or more (partial) blocks of data at the file offset.
  // Length of data is given in dataSize.
  virtual Status Encrypt(uint64_t fileOffset, char* data, size_t dataSize);

  // Decrypt one or more (partial) blocks of data at the file offset.
  // Length of data is given in dataSize.
  virtual Status Decrypt(uint64_t fileOffset, char* data, size_t dataSize);

 protected:
  // Allocate scratch space which is passed to EncryptBlock/DecryptBlock.
  virtual void AllocateScratch(std::string&) = 0;

  // Encrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual Status EncryptBlock(uint64_t blockIndex, char* data,
                              char* scratch) = 0;

  // Decrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual Status DecryptBlock(uint64_t blockIndex, char* data,
                              char* scratch) = 0;
};

// BlockCipher
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class BlockCipher : public Customizable {
 public:
  ~BlockCipher() override = default;

  // Creates a new BlockCipher from the input config_options and value
  // The value describes the type of provider (and potentially optional
  // configuration parameters) used to create this provider.
  // For example, if the value is "ROT13", a ROT13BlockCipher is created.
  //
  // @param config_options  Options to control how this cipher is created
  //                        and initialized.
  // @param value  The value might be:
  //   - ROT13         Create a ROT13 Cipher
  //   - ROT13:nn      Create a ROT13 Cipher with block size of nn
  // @param result The new cipher object
  // @return OK if the cipher was successfully created
  // @return NotFound if an invalid name was specified in the value
  // @return InvalidArgument if either the options were not valid
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value,
                                 std::shared_ptr<BlockCipher>* result);

  static const char* Type() { return "BlockCipher"; }
  // Short-cut method to create a ROT13 BlockCipher.
  // This cipher is only suitable for test purposes and should not be used in
  // production!!!
  static std::shared_ptr<BlockCipher> NewROT13Cipher(size_t block_size);

  // BlockSize returns the size of each block supported by this cipher stream.
  virtual size_t BlockSize() = 0;

  // Encrypt a block of data.
  // Length of data is equal to BlockSize().
  virtual Status Encrypt(char* data) = 0;

  // Decrypt a block of data.
  // Length of data is equal to BlockSize().
  virtual Status Decrypt(char* data) = 0;
};

// The encryption provider is used to create a cipher stream for a specific
// file. The returned cipher stream will be used for actual
// encryption/decryption actions.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class EncryptionProvider : public Customizable {
 public:
  ~EncryptionProvider() override = default;

  // Creates a new EncryptionProvider from the input config_options and value.
  // The value describes the type of provider (and potentially optional
  // configuration parameters) used to create this provider.
  // For example, if the value is "CTR", a CTREncryptionProvider will be
  // created. If the value is end with "://test" (e.g CTR://test"), the
  // provider will be initialized in "TEST" mode prior to being returned.
  //
  // @param config_options  Options to control how this provider is created
  //                        and initialized.
  // @param value  The value might be:
  //   - CTR         Create a CTR provider
  //   - CTR://test Create a CTR provider and initialize it for tests.
  // @param result The new provider object
  // @return OK if the provider was successfully created
  // @return NotFound if an invalid name was specified in the value
  // @return InvalidArgument if either the options were not valid
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value,
                                 std::shared_ptr<EncryptionProvider>* result);

  static const char* Type() { return "EncryptionProvider"; }

  // Short-cut method to create a CTR-provider
  static std::shared_ptr<EncryptionProvider> NewCTRProvider(
      const std::shared_ptr<BlockCipher>& cipher);

  // GetPrefixLength returns the length of the prefix that is added to every
  // file and used for storing encryption options. For optimal performance, the
  // prefix length should be a multiple of the page size.
  virtual size_t GetPrefixLength() const = 0;

  // CreateNewPrefix initialized an allocated block of prefix memory
  // for a new file.
  virtual Status CreateNewPrefix(const std::string& fname, char* prefix,
                                 size_t prefixLength) const = 0;

  // Method to add a new cipher key for use by the EncryptionProvider.
  // @param descriptor   Descriptor for this key
  // @param cipher       The cryptographic key to use
  // @param len          The length of the cipher key
  // @param for_write If true, this cipher should be used for writing files.
  //                  If false, this cipher should only be used for reading
  //                  files
  // @return OK if the cipher was successfully added to the provider, non-OK
  // otherwise
  virtual Status AddCipher(const std::string& descriptor, const char* cipher,
                           size_t len, bool for_write) = 0;

  // CreateCipherStream creates a block access cipher stream for a file given
  // name and options.
  virtual Status CreateCipherStream(
      const std::string& fname, const EnvOptions& options, Slice& prefix,
      std::unique_ptr<BlockAccessCipherStream>* result) = 0;

  // Returns a string representing an encryption marker prefix for this
  // provider. If a marker is provided, this marker can be used to tell whether
  // a file is encrypted by this provider.  The marker will also be part of any
  // encryption prefix for this provider.
  virtual std::string GetMarker() const { return ""; }
};

class EncryptedSequentialFile : public FSSequentialFile {
 protected:
  std::unique_ptr<FSSequentialFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  uint64_t offset_;
  const size_t prefixLength_;

 public:
  // Default ctor. Given underlying sequential file is supposed to be at
  // offset == prefixLength.
  EncryptedSequentialFile(std::unique_ptr<FSSequentialFile>&& f,
                          std::unique_ptr<BlockAccessCipherStream>&& s,
                          size_t prefixLength)
      : file_(std::move(f)),
        stream_(std::move(s)),
        offset_(prefixLength),
        prefixLength_(prefixLength) {}

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;

  IOStatus Skip(uint64_t n) override;

  bool use_direct_io() const override;

  size_t GetRequiredBufferAlignment() const override;

  IOStatus InvalidateCache(size_t offset, size_t length) override;

  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;
};

class EncryptedRandomAccessFile : public FSRandomAccessFile {
 protected:
  std::unique_ptr<FSRandomAccessFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  size_t prefixLength_;

 public:
  EncryptedRandomAccessFile(std::unique_ptr<FSRandomAccessFile>&& f,
                            std::unique_ptr<BlockAccessCipherStream>&& s,
                            size_t prefixLength)
      : file_(std::move(f)),
        stream_(std::move(s)),
        prefixLength_(prefixLength) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& options,
                    IODebugContext* dbg) override;

  size_t GetUniqueId(char* id, size_t max_size) const override;

  void Hint(AccessPattern pattern) override;

  bool use_direct_io() const override;

  size_t GetRequiredBufferAlignment() const override;

  IOStatus InvalidateCache(size_t offset, size_t length) override;

  // Intentionally leave GetFileSize not overridden here, so that it inherits
  // the default implementation from its parent class, which is Not Supported.
  //
  // As GetFileSize API is not required to be implemented yet, we use encrypted
  // file system in unit test to validate the rest of the system could continue
  // working with the Not Supported behavior.
  //
  // IOStatus GetFileSize(uint64_t* /*result*/) override;
};

class EncryptedWritableFile : public FSWritableFile {
 protected:
  std::unique_ptr<FSWritableFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  size_t prefixLength_;

 public:
  // Default ctor. Prefix is assumed to be written already.
  EncryptedWritableFile(std::unique_ptr<FSWritableFile>&& f,
                        std::unique_ptr<BlockAccessCipherStream>&& s,
                        size_t prefixLength)
      : file_(std::move(f)),
        stream_(std::move(s)),
        prefixLength_(prefixLength) {}

  using FSWritableFile::Append;
  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override;

  using FSWritableFile::PositionedAppend;
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override;

  bool IsSyncThreadSafe() const override;

  bool use_direct_io() const override;

  size_t GetRequiredBufferAlignment() const override;

  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override;

  IOStatus InvalidateCache(size_t offset, size_t length) override;

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& options,
                     IODebugContext* dbg) override;

  void PrepareWrite(size_t offset, size_t len, const IOOptions& options,
                    IODebugContext* dbg) override;

  void SetPreallocationBlockSize(size_t size) override;

  void GetPreallocationStatus(size_t* block_size,
                              size_t* last_allocated_block) override;

  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& options,
                    IODebugContext* dbg) override;

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;
};

class EncryptedRandomRWFile : public FSRandomRWFile {
 protected:
  std::unique_ptr<FSRandomRWFile> file_;
  std::unique_ptr<BlockAccessCipherStream> stream_;
  size_t prefixLength_;

 public:
  EncryptedRandomRWFile(std::unique_ptr<FSRandomRWFile>&& f,
                        std::unique_ptr<BlockAccessCipherStream>&& s,
                        size_t prefixLength)
      : file_(std::move(f)),
        stream_(std::move(s)),
        prefixLength_(prefixLength) {}

  bool use_direct_io() const override;

  size_t GetRequiredBufferAlignment() const override;

  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& options,
                 IODebugContext* dbg) override;

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;
};

class EncryptedFileSystem : public FileSystemWrapper {
 public:
  explicit EncryptedFileSystem(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}
  // Method to add a new cipher key for use by the EncryptionProvider.
  // @param description  Descriptor for this key.
  // @param cipher       The cryptographic key to use
  // @param len          The length of the cipher key
  // @param for_write If true, this cipher should be used for writing files.
  //                  If false, this cipher should only be used for reading
  //                  files
  // @return OK if the cipher was successfully added to the provider, non-OK
  // otherwise
  virtual Status AddCipher(const std::string& descriptor, const char* cipher,
                           size_t len, bool for_write) = 0;
  static const char* kClassName() { return "EncryptedFileSystem"; }
  bool IsInstanceOf(const std::string& name) const override {
    if (name == kClassName()) {
      return true;
    } else {
      return FileSystemWrapper::IsInstanceOf(name);
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
