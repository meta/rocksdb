//  Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file contains classes containing fields to protect individual entries.
// The classes are named "ProtectionInfo<suffix>", where <suffix> indicates the
// combination of fields that are covered. Each field has a single letter
// abbreviation as follows.
//
// K = key
// V = value
// O = optype aka value type
// S = seqno
// C = CF ID
//
// Then, for example, a class that protects an entry consisting of key, value,
// optype, and CF ID (i.e., a `WriteBatch` entry) would be named
// `ProtectionInfoKVOC`.
//
// The `ProtectionInfo.*` classes are templated on the integer type used to hold
// the XOR of hashes for each field. Only unsigned integer types are supported,
// and the maximum supported integer width is 64 bits. When the integer type is
// narrower than the hash values, we lop off the most significant bits to make
// them fit.
//
// The `ProtectionInfo.*` classes are all intended to be non-persistent. We do
// not currently make the byte order consistent for integer fields before
// hashing them, so the resulting values are endianness-dependent.

#pragma once

#include <type_traits>

#include "db/dbformat.h"
#include "rocksdb/types.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

template <typename T>
class ProtectionInfo;
template <typename T>
class ProtectionInfoKVO;
template <typename T>
class ProtectionInfoKVOC;
template <typename T>
class ProtectionInfoKVOS;
template <typename T>
class ProtectionInfoKV;

// Aliases for 64-bit protection infos.
using ProtectionInfo64 = ProtectionInfo<uint64_t>;
using ProtectionInfoKVO64 = ProtectionInfoKVO<uint64_t>;
using ProtectionInfoKVOC64 = ProtectionInfoKVOC<uint64_t>;
using ProtectionInfoKVOS64 = ProtectionInfoKVOS<uint64_t>;

template <typename T>
class ProtectionInfo {
 public:
  ProtectionInfo() = default;

  Status GetStatus() const;
  ProtectionInfoKVO<T> ProtectKVO(const Slice& key, const Slice& value,
                                  ValueType op_type) const;
  ProtectionInfoKVO<T> ProtectKVO(const SliceParts& key,
                                  const SliceParts& value,
                                  ValueType op_type) const;
  ProtectionInfoKV<T> ProtectKV(const Slice& key, const Slice& value) const;

 private:
  friend class ProtectionInfoKVO<T>;
  friend class ProtectionInfoKVOS<T>;
  friend class ProtectionInfoKVOC<T>;
  friend class ProtectionInfoKV<T>;

  // Each field is hashed with an independent value so we can catch fields being
  // swapped. Per the `NPHash64()` docs, using consecutive seeds is a pitfall,
  // and we should instead vary our seeds by a large odd number. This value by
  // which we increment (0xD28AAD72F49BD50B) was taken from
  // `head -c8 /dev/urandom | hexdump`, run repeatedly until it yielded an odd
  // number. The values are computed manually since the Windows C++ compiler
  // complains about the overflow when adding constants.
  static const uint64_t kSeedK = 0;
  static const uint64_t kSeedV = 0xD28AAD72F49BD50B;
  static const uint64_t kSeedO = 0xA5155AE5E937AA16;
  static const uint64_t kSeedS = 0x77A00858DDD37F21;
  static const uint64_t kSeedC = 0x4A2AB5CBD26F542C;

  explicit ProtectionInfo(T val) : val_(val) {
    static_assert(sizeof(ProtectionInfo<T>) == sizeof(T), "");
  }

  T GetVal() const { return val_; }
  void SetVal(T val) { val_ = val; }

  void Encode(uint8_t len, char* dst) const {
    assert(sizeof(val_) >= len);
    switch (len) {
      case 1:
        dst[0] = static_cast<uint8_t>(val_);
        break;
      case 2:
        EncodeFixed16(dst, static_cast<uint16_t>(val_));
        break;
      case 4:
        EncodeFixed32(dst, static_cast<uint32_t>(val_));
        break;
      case 8:
        EncodeFixed64(dst, static_cast<uint64_t>(val_));
        break;
      default:
        assert(false);
    }
  }

  bool Verify(uint8_t len, const char* checksum_ptr) const {
    assert(sizeof(val_) >= len);
    switch (len) {
      case 1:
        return static_cast<uint8_t>(checksum_ptr[0]) ==
               static_cast<uint8_t>(val_);
      case 2:
        return DecodeFixed16(checksum_ptr) == static_cast<uint16_t>(val_);
      case 4:
        return DecodeFixed32(checksum_ptr) == static_cast<uint32_t>(val_);
      case 8:
        return DecodeFixed64(checksum_ptr) == static_cast<uint64_t>(val_);
      default:
        assert(false);
        return false;
    }
  }

  T val_ = 0;
};

template <typename T>
class ProtectionInfoKVO {
 public:
  ProtectionInfoKVO() = default;

  ProtectionInfo<T> StripKVO(const Slice& key, const Slice& value,
                             ValueType op_type) const;
  ProtectionInfo<T> StripKVO(const SliceParts& key, const SliceParts& value,
                             ValueType op_type) const;

  ProtectionInfoKVOC<T> ProtectC(ColumnFamilyId column_family_id) const;
  ProtectionInfoKVOS<T> ProtectS(SequenceNumber sequence_number) const;

  void UpdateK(const Slice& old_key, const Slice& new_key);
  void UpdateK(const SliceParts& old_key, const SliceParts& new_key);
  void UpdateV(const Slice& old_value, const Slice& new_value);
  void UpdateV(const SliceParts& old_value, const SliceParts& new_value);
  void UpdateO(ValueType old_op_type, ValueType new_op_type);

  // Encode this protection info into `len` bytes and stores them in `dst`.
  void Encode(uint8_t len, char* dst) const { info_.Encode(len, dst); }
  // Verify this protection info against the protection info encoded by Encode()
  // at the first `len` bytes of `checksum_ptr`.
  // Returns true iff the verification is successful.
  bool Verify(uint8_t len, const char* checksum_ptr) const {
    return info_.Verify(len, checksum_ptr);
  }

 private:
  friend class ProtectionInfo<T>;
  friend class ProtectionInfoKVOS<T>;
  friend class ProtectionInfoKVOC<T>;

  explicit ProtectionInfoKVO(T val) : info_(val) {
    static_assert(sizeof(ProtectionInfoKVO<T>) == sizeof(T), "");
  }

  T GetVal() const { return info_.GetVal(); }
  void SetVal(T val) { info_.SetVal(val); }

  ProtectionInfo<T> info_;
};

template <typename T>
class ProtectionInfoKVOC {
 public:
  ProtectionInfoKVOC() = default;

  ProtectionInfoKVO<T> StripC(ColumnFamilyId column_family_id) const;

  void UpdateK(const Slice& old_key, const Slice& new_key) {
    kvo_.UpdateK(old_key, new_key);
  }
  void UpdateK(const SliceParts& old_key, const SliceParts& new_key) {
    kvo_.UpdateK(old_key, new_key);
  }
  void UpdateV(const Slice& old_value, const Slice& new_value) {
    kvo_.UpdateV(old_value, new_value);
  }
  void UpdateV(const SliceParts& old_value, const SliceParts& new_value) {
    kvo_.UpdateV(old_value, new_value);
  }
  void UpdateO(ValueType old_op_type, ValueType new_op_type) {
    kvo_.UpdateO(old_op_type, new_op_type);
  }
  void UpdateC(ColumnFamilyId old_column_family_id,
               ColumnFamilyId new_column_family_id);

  void Encode(uint8_t len, char* dst) const { kvo_.Encode(len, dst); }
  bool Verify(uint8_t len, const char* checksum_ptr) const {
    return kvo_.Verify(len, checksum_ptr);
  }

 private:
  friend class ProtectionInfoKVO<T>;

  explicit ProtectionInfoKVOC(T val) : kvo_(val) {
    static_assert(sizeof(ProtectionInfoKVOC<T>) == sizeof(T), "");
  }

  T GetVal() const { return kvo_.GetVal(); }
  void SetVal(T val) { kvo_.SetVal(val); }

  ProtectionInfoKVO<T> kvo_;
};

template <typename T>
class ProtectionInfoKVOS {
 public:
  ProtectionInfoKVOS() = default;

  ProtectionInfoKVO<T> StripS(SequenceNumber sequence_number) const;

  void UpdateK(const Slice& old_key, const Slice& new_key) {
    kvo_.UpdateK(old_key, new_key);
  }
  void UpdateK(const SliceParts& old_key, const SliceParts& new_key) {
    kvo_.UpdateK(old_key, new_key);
  }
  void UpdateV(const Slice& old_value, const Slice& new_value) {
    kvo_.UpdateV(old_value, new_value);
  }
  void UpdateV(const SliceParts& old_value, const SliceParts& new_value) {
    kvo_.UpdateV(old_value, new_value);
  }
  void UpdateO(ValueType old_op_type, ValueType new_op_type) {
    kvo_.UpdateO(old_op_type, new_op_type);
  }
  void UpdateS(SequenceNumber old_sequence_number,
               SequenceNumber new_sequence_number);

  void Encode(uint8_t len, char* dst) const { kvo_.Encode(len, dst); }
  bool Verify(uint8_t len, const char* checksum_ptr) const {
    return kvo_.Verify(len, checksum_ptr);
  }

 private:
  friend class ProtectionInfoKVO<T>;

  explicit ProtectionInfoKVOS(T val) : kvo_(val) {
    static_assert(sizeof(ProtectionInfoKVOS<T>) == sizeof(T), "");
  }

  T GetVal() const { return kvo_.GetVal(); }
  void SetVal(T val) { kvo_.SetVal(val); }

  ProtectionInfoKVO<T> kvo_;
};

template <typename T>
class ProtectionInfoKV {
 public:
  ProtectionInfoKV() = default;

  void Encode(uint8_t len, char* dst) const { info_.Encode(len, dst); }
  bool Verify(uint8_t len, const char* checksum_ptr) const {
    return info_.Verify(len, checksum_ptr);
  }

 private:
  friend class ProtectionInfo<T>;

  explicit ProtectionInfoKV(T val) : info_(val) {
    static_assert(sizeof(ProtectionInfoKV<T>) == sizeof(T));
  }

  ProtectionInfo<T> info_;
};

template <typename T>
Status ProtectionInfo<T>::GetStatus() const {
  if (val_ != 0) {
    return Status::Corruption("ProtectionInfo mismatch");
  }
  return Status::OK();
}

template <typename T>
ProtectionInfoKVO<T> ProtectionInfo<T>::ProtectKVO(const Slice& key,
                                                   const Slice& value,
                                                   ValueType op_type) const {
  T val = GetVal();
  val = val ^ static_cast<T>(GetSliceNPHash64(key, ProtectionInfo<T>::kSeedK));
  val =
      val ^ static_cast<T>(GetSliceNPHash64(value, ProtectionInfo<T>::kSeedV));
  val = val ^
        static_cast<T>(NPHash64(reinterpret_cast<char*>(&op_type),
                                sizeof(op_type), ProtectionInfo<T>::kSeedO));
  return ProtectionInfoKVO<T>(val);
}

template <typename T>
ProtectionInfoKVO<T> ProtectionInfo<T>::ProtectKVO(const SliceParts& key,
                                                   const SliceParts& value,
                                                   ValueType op_type) const {
  T val = GetVal();
  val = val ^
        static_cast<T>(GetSlicePartsNPHash64(key, ProtectionInfo<T>::kSeedK));
  val = val ^
        static_cast<T>(GetSlicePartsNPHash64(value, ProtectionInfo<T>::kSeedV));
  val = val ^
        static_cast<T>(NPHash64(reinterpret_cast<char*>(&op_type),
                                sizeof(op_type), ProtectionInfo<T>::kSeedO));
  return ProtectionInfoKVO<T>(val);
}

template <typename T>
ProtectionInfoKV<T> ProtectionInfo<T>::ProtectKV(const Slice& key,
                                                 const Slice& value) const {
  T val = GetVal();
  val = val ^ static_cast<T>(GetSliceNPHash64(key, ProtectionInfo<T>::kSeedK));
  val =
      val ^ static_cast<T>(GetSliceNPHash64(value, ProtectionInfo<T>::kSeedV));
  return ProtectionInfoKV<T>(val);
}

template <typename T>
void ProtectionInfoKVO<T>::UpdateK(const Slice& old_key, const Slice& new_key) {
  T val = GetVal();
  val = val ^
        static_cast<T>(GetSliceNPHash64(old_key, ProtectionInfo<T>::kSeedK));
  val = val ^
        static_cast<T>(GetSliceNPHash64(new_key, ProtectionInfo<T>::kSeedK));
  SetVal(val);
}

template <typename T>
void ProtectionInfoKVO<T>::UpdateK(const SliceParts& old_key,
                                   const SliceParts& new_key) {
  T val = GetVal();
  val = val ^ static_cast<T>(
                  GetSlicePartsNPHash64(old_key, ProtectionInfo<T>::kSeedK));
  val = val ^ static_cast<T>(
                  GetSlicePartsNPHash64(new_key, ProtectionInfo<T>::kSeedK));
  SetVal(val);
}

template <typename T>
void ProtectionInfoKVO<T>::UpdateV(const Slice& old_value,
                                   const Slice& new_value) {
  T val = GetVal();
  val = val ^
        static_cast<T>(GetSliceNPHash64(old_value, ProtectionInfo<T>::kSeedV));
  val = val ^
        static_cast<T>(GetSliceNPHash64(new_value, ProtectionInfo<T>::kSeedV));
  SetVal(val);
}

template <typename T>
void ProtectionInfoKVO<T>::UpdateV(const SliceParts& old_value,
                                   const SliceParts& new_value) {
  T val = GetVal();
  val = val ^ static_cast<T>(
                  GetSlicePartsNPHash64(old_value, ProtectionInfo<T>::kSeedV));
  val = val ^ static_cast<T>(
                  GetSlicePartsNPHash64(new_value, ProtectionInfo<T>::kSeedV));
  SetVal(val);
}

template <typename T>
void ProtectionInfoKVO<T>::UpdateO(ValueType old_op_type,
                                   ValueType new_op_type) {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(reinterpret_cast<char*>(&old_op_type),
                                      sizeof(old_op_type),
                                      ProtectionInfo<T>::kSeedO));
  val = val ^ static_cast<T>(NPHash64(reinterpret_cast<char*>(&new_op_type),
                                      sizeof(new_op_type),
                                      ProtectionInfo<T>::kSeedO));
  SetVal(val);
}

template <typename T>
ProtectionInfo<T> ProtectionInfoKVO<T>::StripKVO(const Slice& key,
                                                 const Slice& value,
                                                 ValueType op_type) const {
  T val = GetVal();
  val = val ^ static_cast<T>(GetSliceNPHash64(key, ProtectionInfo<T>::kSeedK));
  val =
      val ^ static_cast<T>(GetSliceNPHash64(value, ProtectionInfo<T>::kSeedV));
  val = val ^
        static_cast<T>(NPHash64(reinterpret_cast<char*>(&op_type),
                                sizeof(op_type), ProtectionInfo<T>::kSeedO));
  return ProtectionInfo<T>(val);
}

template <typename T>
ProtectionInfo<T> ProtectionInfoKVO<T>::StripKVO(const SliceParts& key,
                                                 const SliceParts& value,
                                                 ValueType op_type) const {
  T val = GetVal();
  val = val ^
        static_cast<T>(GetSlicePartsNPHash64(key, ProtectionInfo<T>::kSeedK));
  val = val ^
        static_cast<T>(GetSlicePartsNPHash64(value, ProtectionInfo<T>::kSeedV));
  val = val ^
        static_cast<T>(NPHash64(reinterpret_cast<char*>(&op_type),
                                sizeof(op_type), ProtectionInfo<T>::kSeedO));
  return ProtectionInfo<T>(val);
}

template <typename T>
ProtectionInfoKVOC<T> ProtectionInfoKVO<T>::ProtectC(
    ColumnFamilyId column_family_id) const {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(
                  reinterpret_cast<char*>(&column_family_id),
                  sizeof(column_family_id), ProtectionInfo<T>::kSeedC));
  return ProtectionInfoKVOC<T>(val);
}

template <typename T>
ProtectionInfoKVO<T> ProtectionInfoKVOC<T>::StripC(
    ColumnFamilyId column_family_id) const {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(
                  reinterpret_cast<char*>(&column_family_id),
                  sizeof(column_family_id), ProtectionInfo<T>::kSeedC));
  return ProtectionInfoKVO<T>(val);
}

template <typename T>
void ProtectionInfoKVOC<T>::UpdateC(ColumnFamilyId old_column_family_id,
                                    ColumnFamilyId new_column_family_id) {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(
                  reinterpret_cast<char*>(&old_column_family_id),
                  sizeof(old_column_family_id), ProtectionInfo<T>::kSeedC));
  val = val ^ static_cast<T>(NPHash64(
                  reinterpret_cast<char*>(&new_column_family_id),
                  sizeof(new_column_family_id), ProtectionInfo<T>::kSeedC));
  SetVal(val);
}

template <typename T>
ProtectionInfoKVOS<T> ProtectionInfoKVO<T>::ProtectS(
    SequenceNumber sequence_number) const {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(reinterpret_cast<char*>(&sequence_number),
                                      sizeof(sequence_number),
                                      ProtectionInfo<T>::kSeedS));
  return ProtectionInfoKVOS<T>(val);
}

template <typename T>
ProtectionInfoKVO<T> ProtectionInfoKVOS<T>::StripS(
    SequenceNumber sequence_number) const {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(reinterpret_cast<char*>(&sequence_number),
                                      sizeof(sequence_number),
                                      ProtectionInfo<T>::kSeedS));
  return ProtectionInfoKVO<T>(val);
}

template <typename T>
void ProtectionInfoKVOS<T>::UpdateS(SequenceNumber old_sequence_number,
                                    SequenceNumber new_sequence_number) {
  T val = GetVal();
  val = val ^ static_cast<T>(NPHash64(
                  reinterpret_cast<char*>(&old_sequence_number),
                  sizeof(old_sequence_number), ProtectionInfo<T>::kSeedS));
  val = val ^ static_cast<T>(NPHash64(
                  reinterpret_cast<char*>(&new_sequence_number),
                  sizeof(new_sequence_number), ProtectionInfo<T>::kSeedS));
  SetVal(val);
}
}  // namespace ROCKSDB_NAMESPACE
