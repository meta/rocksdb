//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdint>

#include "table/unique_id_impl.h"
#include "util/coding_lean.h"
#include "util/hash.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

std::string EncodeSessionId(uint64_t upper, uint64_t lower) {
  std::string db_session_id(20U, '\0');
  char* buf = db_session_id.data();
  // Preserving `lower` is slightly tricky. 36^12 is slightly more than
  // 62 bits, so we use 12 chars plus the bottom two bits of one more.
  // (A tiny fraction of 20 digit strings go unused.)
  uint64_t a = (upper << 2) | (lower >> 62);
  uint64_t b = lower & (UINT64_MAX >> 2);
  PutBaseChars<36>(&buf, 8, a, /*uppercase*/ true);
  PutBaseChars<36>(&buf, 12, b, /*uppercase*/ true);
  assert(buf == &db_session_id.back() + 1);
  return db_session_id;
}

Status DecodeSessionId(const std::string& db_session_id, uint64_t* upper,
                       uint64_t* lower) {
  const size_t len = db_session_id.size();
  if (len == 0) {
    return Status::NotSupported("Missing db_session_id");
  }
  // Anything from 13 to 24 chars is reasonable. We don't have to limit to
  // exactly 20.
  if (len < 13) {
    return Status::NotSupported("Too short db_session_id");
  }
  if (len > 24) {
    return Status::NotSupported("Too long db_session_id");
  }
  uint64_t a = 0;
  uint64_t b = 0;
  const char* buf = &db_session_id.front();
  bool success = ParseBaseChars<36>(&buf, len - 12U, &a);
  if (!success) {
    return Status::NotSupported("Bad digit in db_session_id");
  }
  success = ParseBaseChars<36>(&buf, 12U, &b);
  if (!success) {
    return Status::NotSupported("Bad digit in db_session_id");
  }
  assert(buf == &db_session_id.back() + 1);
  *upper = a >> 2;
  *lower = (b & (UINT64_MAX >> 2)) | (a << 62);
  return Status::OK();
}

Status GetSstInternalUniqueId(const std::string& db_id,
                              const std::string& db_session_id,
                              uint64_t file_number, UniqueIdPtr out,
                              bool force) {
  if (!force) {
    if (db_id.empty()) {
      return Status::NotSupported("Missing db_id");
    }
    if (file_number == 0) {
      return Status::NotSupported("Missing or bad file number");
    }
    if (db_session_id.empty()) {
      return Status::NotSupported("Missing db_session_id");
    }
  }
  uint64_t session_upper = 0;  // Assignment to appease clang-analyze
  uint64_t session_lower = 0;  // Assignment to appease clang-analyze
  {
    Status s = DecodeSessionId(db_session_id, &session_upper, &session_lower);
    if (!s.ok()) {
      if (!force) {
        return s;
      } else {
        // A reasonable fallback in case malformed
        Hash2x64(db_session_id.data(), db_session_id.size(), &session_upper,
                 &session_lower);
        if (session_lower == 0) {
          session_lower = session_upper | 1;
        }
      }
    }
  }

  // Exactly preserve session lower to ensure that session ids generated
  // during the same process lifetime are guaranteed unique.
  // DBImpl also guarantees (in recent versions) that this is not zero,
  // so that we can guarantee unique ID is never all zeros. (Can't assert
  // that here because of testing and old versions.)
  // We put this first in anticipation of matching a small-ish set of cache
  // key prefixes to cover entries relevant to any DB.
  out.ptr[0] = session_lower;

  // Hash the session upper (~39 bits entropy) and DB id (120+ bits entropy)
  // for very high global uniqueness entropy.
  // (It is possible that many DBs descended from one common DB id are copied
  // around and proliferate, in which case session id is critical, but it is
  // more common for different DBs to have different DB ids.)
  uint64_t db_a;
  uint64_t db_b;
  Hash2x64(db_id.data(), db_id.size(), session_upper, &db_a, &db_b);

  // Xor in file number for guaranteed uniqueness by file number for a given
  // session and DB id. (Xor slightly better than + here. See
  // https://github.com/pdillinger/unique_id )
  out.ptr[1] = db_a ^ file_number;

  // Extra (optional) global uniqueness
  if (out.extended) {
    out.ptr[2] = db_b;
  }

  return Status::OK();
}

namespace {
// For InternalUniqueIdToExternal / ExternalUniqueIdToInternal we want all
// zeros in first 128 bits to map to itself, so that excluding zero in
// internal IDs (session_lower != 0 above) does the same for external IDs.
// These values are meaningless except for making that work.
constexpr uint64_t kHiOffsetForZero = 17391078804906429400U;
constexpr uint64_t kLoOffsetForZero = 6417269962128484497U;
}  // namespace

void InternalUniqueIdToExternal(UniqueIdPtr in_out) {
  uint64_t hi;
  uint64_t lo;
  BijectiveHash2x64(in_out.ptr[1] + kHiOffsetForZero,
                    in_out.ptr[0] + kLoOffsetForZero, &hi, &lo);
  in_out.ptr[0] = lo;
  in_out.ptr[1] = hi;
  if (in_out.extended) {
    in_out.ptr[2] += lo + hi;
  }
}

void ExternalUniqueIdToInternal(UniqueIdPtr in_out) {
  uint64_t lo = in_out.ptr[0];
  uint64_t hi = in_out.ptr[1];
  if (in_out.extended) {
    in_out.ptr[2] -= lo + hi;
  }
  BijectiveUnhash2x64(hi, lo, &hi, &lo);
  in_out.ptr[0] = lo - kLoOffsetForZero;
  in_out.ptr[1] = hi - kHiOffsetForZero;
}

std::string EncodeUniqueIdBytes(UniqueIdPtr in) {
  std::string ret(in.extended ? 24U : 16U, '\0');
  EncodeFixed64(ret.data(), in.ptr[0]);
  EncodeFixed64(&ret[8], in.ptr[1]);
  if (in.extended) {
    EncodeFixed64(&ret[16], in.ptr[2]);
  }
  return ret;
}

Status DecodeUniqueIdBytes(const std::string& unique_id, UniqueIdPtr out) {
  if (unique_id.size() != (out.extended ? 24 : 16)) {
    return Status::NotSupported("Not a valid unique_id");
  }
  const char* buf = &unique_id.front();
  out.ptr[0] = DecodeFixed64(&buf[0]);
  out.ptr[1] = DecodeFixed64(&buf[8]);
  if (out.extended) {
    out.ptr[2] = DecodeFixed64(&buf[16]);
  }
  return Status::OK();
}

template <typename ID>
Status GetUniqueIdFromTablePropertiesHelper(const TableProperties& props,
                                            std::string* out_id) {
  ID tmp{};
  Status s = GetSstInternalUniqueId(props.db_id, props.db_session_id,
                                    props.orig_file_number, &tmp);
  if (s.ok()) {
    InternalUniqueIdToExternal(&tmp);
    *out_id = EncodeUniqueIdBytes(&tmp);
  } else {
    out_id->clear();
  }
  return s;
}

Status GetExtendedUniqueIdFromTableProperties(const TableProperties& props,
                                              std::string* out_id) {
  return GetUniqueIdFromTablePropertiesHelper<UniqueId64x3>(props, out_id);
}

Status GetUniqueIdFromTableProperties(const TableProperties& props,
                                      std::string* out_id) {
  return GetUniqueIdFromTablePropertiesHelper<UniqueId64x2>(props, out_id);
}

std::string UniqueIdToHumanString(const std::string& id) {
  std::string hex = Slice(id).ToString(/*hex*/ true);
  std::string result;
  result.reserve(hex.size() + hex.size() / 16);
  for (size_t i = 0; i < hex.size(); i++) {
    if (i > 0 && i % 16 == 0) {
      result.push_back('-');
    }
    result.push_back(hex[i]);
  }
  return result;
}

std::string InternalUniqueIdToHumanString(UniqueIdPtr in) {
  std::string str = "{";
  str += std::to_string(in.ptr[0]);
  str += ",";
  str += std::to_string(in.ptr[1]);
  if (in.extended) {
    str += ",";
    str += std::to_string(in.ptr[2]);
  }
  str += "}";
  return str;
}

}  // namespace ROCKSDB_NAMESPACE
