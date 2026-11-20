//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/env_encryption.h"

namespace ROCKSDB_NAMESPACE {
// CTRCipherStream implements BlockAccessCipherStream using an
// Counter operations mode.
// See https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation
//
// Note: This is a possible implementation of BlockAccessCipherStream,
// it is considered suitable for use.
class CTRCipherStream final : public BlockAccessCipherStream {
 private:
  std::shared_ptr<BlockCipher> cipher_;
  std::string iv_;
  uint64_t initialCounter_;

 public:
  CTRCipherStream(const std::shared_ptr<BlockCipher>& c, const char* iv,
                  uint64_t initialCounter)
      : cipher_(c), iv_(iv, c->BlockSize()), initialCounter_(initialCounter) {}
  ~CTRCipherStream() override = default;

  size_t BlockSize() override { return cipher_->BlockSize(); }

 protected:
  void AllocateScratch(std::string&) override;

  Status EncryptBlock(uint64_t blockIndex, char* data, char* scratch) override;

  Status DecryptBlock(uint64_t blockIndex, char* data, char* scratch) override;
};

// This encryption provider uses a CTR cipher stream, with a given block cipher
// and IV.
//
// Note: This is a possible implementation of EncryptionProvider,
// it is considered suitable for use, provided a safe BlockCipher is used.
class CTREncryptionProvider : public EncryptionProvider {
 private:
  std::shared_ptr<BlockCipher> cipher_;

 protected:
  // For optimal performance when using direct IO, the prefix length should be a
  // multiple of the page size. This size is to ensure the first real data byte
  // is placed at largest known alignment point for direct io.
  const static size_t defaultPrefixLength = 4096;

 public:
  explicit CTREncryptionProvider(
      const std::shared_ptr<BlockCipher>& c = nullptr);
  ~CTREncryptionProvider() override = default;

  static const char* kClassName() { return "CTR"; }
  const char* Name() const override { return kClassName(); }
  bool IsInstanceOf(const std::string& name) const override;
  size_t GetPrefixLength() const override;
  Status CreateNewPrefix(const std::string& fname, char* prefix,
                         size_t prefixLength) const override;
  Status CreateCipherStream(
      const std::string& fname, const EnvOptions& options, Slice& prefix,
      std::unique_ptr<BlockAccessCipherStream>* result) override;

  Status AddCipher(const std::string& descriptor, const char* /*cipher*/,
                   size_t /*len*/, bool /*for_write*/) override;

 protected:
  // PopulateSecretPrefixPart initializes the data into a new prefix block
  // that will be encrypted. This function will store the data in plain text.
  // It will be encrypted later (before written to disk).
  // Returns the amount of space (starting from the start of the prefix)
  // that has been initialized.
  virtual size_t PopulateSecretPrefixPart(char* prefix, size_t prefixLength,
                                          size_t blockSize) const;

  // CreateCipherStreamFromPrefix creates a block access cipher stream for a
  // file given
  // given name and options. The given prefix is already decrypted.
  virtual Status CreateCipherStreamFromPrefix(
      const std::string& fname, const EnvOptions& options,
      uint64_t initialCounter, const Slice& iv, const Slice& prefix,
      std::unique_ptr<BlockAccessCipherStream>* result);
};

Status NewEncryptedFileSystemImpl(
    const std::shared_ptr<FileSystem>& base_fs,
    const std::shared_ptr<EncryptionProvider>& provider,
    std::unique_ptr<FileSystem>* fs);

}  // namespace ROCKSDB_NAMESPACE
