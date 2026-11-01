//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_addition.h"

#include <ostream>
#include <sstream>

#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// Tags for custom fields. Note that these get persisted in the manifest,
// so existing tags should not be modified.
enum BlobFileAddition::CustomFieldTags : uint32_t {
  kEndMarker,

  // Add forward compatible fields here
  kIdentityRoot,

  /////////////////////////////////////////////////////////////////////

  kForwardIncompatibleMask = 1 << 6,

  // Add forward incompatible fields here
};

void BlobFileAddition::EncodeTo(std::string* output) const {
  PutVarint64(output, blob_file_number_);
  PutVarint64(output, total_blob_count_);
  PutVarint64(output, total_blob_bytes_);
  PutLengthPrefixedSlice(output, checksum_method_);
  PutLengthPrefixedSlice(output, checksum_value_);

  // Encode any custom fields here. The format to use is a Varint32 tag (see
  // CustomFieldTags above) followed by a length prefixed slice. Unknown custom
  // fields will be ignored during decoding unless they're in the forward
  // incompatible range.

  if (is_identity_root_) {
    PutVarint32(output, kIdentityRoot);
    std::string encoded;
    PutVarint32(&encoded, 1);
    PutLengthPrefixedSlice(output, encoded);
  }

  TEST_SYNC_POINT_CALLBACK("BlobFileAddition::EncodeTo::CustomFields", output);

  PutVarint32(output, kEndMarker);
}

Status BlobFileAddition::DecodeFrom(Slice* input) {
  constexpr char class_name[] = "BlobFileAddition";

  if (!GetVarint64(input, &blob_file_number_)) {
    return Status::Corruption(class_name, "Error decoding blob file number");
  }

  if (!GetVarint64(input, &total_blob_count_)) {
    return Status::Corruption(class_name, "Error decoding total blob count");
  }

  if (!GetVarint64(input, &total_blob_bytes_)) {
    return Status::Corruption(class_name, "Error decoding total blob bytes");
  }

  Slice checksum_method;
  if (!GetLengthPrefixedSlice(input, &checksum_method)) {
    return Status::Corruption(class_name, "Error decoding checksum method");
  }
  checksum_method_ = checksum_method.ToString();

  Slice checksum_value;
  if (!GetLengthPrefixedSlice(input, &checksum_value)) {
    return Status::Corruption(class_name, "Error decoding checksum value");
  }
  checksum_value_ = checksum_value.ToString();
  is_identity_root_ = false;

  bool has_identity_root = false;
  while (true) {
    uint32_t custom_field_tag = 0;
    if (!GetVarint32(input, &custom_field_tag)) {
      return Status::Corruption(class_name, "Error decoding custom field tag");
    }

    if (custom_field_tag == kEndMarker) {
      break;
    }

    if (custom_field_tag & kForwardIncompatibleMask) {
      return Status::Corruption(
          class_name, "Forward incompatible custom field encountered");
    }

    Slice custom_field_value;
    if (!GetLengthPrefixedSlice(input, &custom_field_value)) {
      return Status::Corruption(class_name,
                                "Error decoding custom field value");
    }
    if (custom_field_tag == kIdentityRoot) {
      uint32_t value = 0;
      if (has_identity_root || !GetVarint32(&custom_field_value, &value) ||
          !custom_field_value.empty() || value != 1) {
        return Status::Corruption(class_name,
                                  "Error decoding identity-root field");
      }
      has_identity_root = true;
      is_identity_root_ = true;
    }
  }

  return Status::OK();
}

std::string BlobFileAddition::DebugString() const {
  std::ostringstream oss;

  oss << *this;

  return oss.str();
}

std::string BlobFileAddition::DebugJSON() const {
  JSONWriter jw;

  jw << *this;

  jw.EndObject();

  return jw.Get();
}

bool operator==(const BlobFileAddition& lhs, const BlobFileAddition& rhs) {
  return lhs.GetBlobFileNumber() == rhs.GetBlobFileNumber() &&
         lhs.GetTotalBlobCount() == rhs.GetTotalBlobCount() &&
         lhs.GetTotalBlobBytes() == rhs.GetTotalBlobBytes() &&
         lhs.GetChecksumMethod() == rhs.GetChecksumMethod() &&
         lhs.GetChecksumValue() == rhs.GetChecksumValue() &&
         lhs.IsIdentityRoot() == rhs.IsIdentityRoot();
}

bool operator!=(const BlobFileAddition& lhs, const BlobFileAddition& rhs) {
  return !(lhs == rhs);
}

std::ostream& operator<<(std::ostream& os,
                         const BlobFileAddition& blob_file_addition) {
  os << "blob_file_number: " << blob_file_addition.GetBlobFileNumber()
     << " total_blob_count: " << blob_file_addition.GetTotalBlobCount()
     << " total_blob_bytes: " << blob_file_addition.GetTotalBlobBytes()
     << " checksum_method: " << blob_file_addition.GetChecksumMethod()
     << " checksum_value: "
     << Slice(blob_file_addition.GetChecksumValue()).ToString(/* hex */ true)
     << " identity_root: " << blob_file_addition.IsIdentityRoot();

  return os;
}

JSONWriter& operator<<(JSONWriter& jw,
                       const BlobFileAddition& blob_file_addition) {
  jw << "BlobFileNumber" << blob_file_addition.GetBlobFileNumber()
     << "TotalBlobCount" << blob_file_addition.GetTotalBlobCount()
     << "TotalBlobBytes" << blob_file_addition.GetTotalBlobBytes()
     << "ChecksumMethod" << blob_file_addition.GetChecksumMethod()
     << "ChecksumValue"
     << Slice(blob_file_addition.GetChecksumValue()).ToString(/* hex */ true)
     << "IdentityRoot" << blob_file_addition.IsIdentityRoot();

  return jw;
}

}  // namespace ROCKSDB_NAMESPACE
