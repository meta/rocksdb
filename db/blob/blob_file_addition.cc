//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_addition.h"

#include <ostream>
#include <sstream>

#include "db/blog/blog_format.h"
#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

namespace {

std::string UnsupportedBlobFileSchemaVersionMessage(uint8_t schema_version) {
  return "Unsupported blob file schema version " +
         std::to_string(schema_version) +
         " in BlobFileAddition format descriptor; supported non-legacy blog "
         "schema versions are 1.." +
         std::to_string(kBlogCurrentSchemaVersion) +
         " and legacy blob files omit the format descriptor";
}

Status DecodePhysicalBlobFileSize(Slice* input,
                                  uint64_t* physical_blob_file_size) {
  assert(input != nullptr);
  assert(physical_blob_file_size != nullptr);

  if (*physical_blob_file_size != 0) {
    return Status::Corruption("BlobFileAddition",
                              "Duplicate physical blob file size field");
  }

  if (!GetVarint64(input, physical_blob_file_size) || !input->empty()) {
    return Status::Corruption("BlobFileAddition",
                              "Malformed physical blob file size field");
  }

  return Status::OK();
}

Status DecodeFormatDescriptor(Slice* input, uint8_t* schema_version,
                              std::string* file_identity) {
  assert(input != nullptr);
  assert(schema_version != nullptr);
  assert(file_identity != nullptr);

  if (*schema_version != kLegacyBlobFileSchemaVersion ||
      !file_identity->empty()) {
    return Status::Corruption("BlobFileAddition",
                              "Duplicate blob file format descriptor field");
  }

  if (input->empty()) {
    return Status::Corruption("BlobFileAddition",
                              "Missing blob file schema version");
  }

  *schema_version = lossless_cast<uint8_t>((*input)[0]);
  input->remove_prefix(1);

  if (*schema_version == kLegacyBlobFileSchemaVersion) {
    return Status::Corruption(
        "BlobFileAddition",
        "Blob file format descriptor cannot use legacy schema version");
  }
  if (*schema_version > kBlogCurrentSchemaVersion) {
    return Status::Corruption(
        "BlobFileAddition",
        UnsupportedBlobFileSchemaVersionMessage(*schema_version));
  }
  if (input->size() != kBlobFileIdentitySize) {
    return Status::Corruption("BlobFileAddition",
                              "Malformed blob file identity");
  }

  *file_identity = input->ToString();
  input->remove_prefix(input->size());
  return Status::OK();
}

Status ValidateDecodedBlobFileAddition(
    const BlobFileAddition& blob_file_addition) {
  if (blob_file_addition.GetPhysicalBlobFileSize() != 0 &&
      blob_file_addition.GetPhysicalBlobFileSize() <
          blob_file_addition.GetTotalBlobBytes()) {
    return Status::Corruption("BlobFileAddition",
                              "Physical blob file size is smaller than total "
                              "blob bytes");
  }
  if (blob_file_addition.GetSchemaVersion() == kLegacyBlobFileSchemaVersion) {
    if (!blob_file_addition.GetFileIdentity().empty()) {
      return Status::Corruption("BlobFileAddition",
                                "Legacy blob file cannot carry file identity");
    }
    return Status::OK();
  }
  if (blob_file_addition.GetFileIdentity().size() != kBlobFileIdentitySize) {
    return Status::Corruption("BlobFileAddition",
                              "Malformed blob file identity length");
  }

  return Status::OK();
}

}  // namespace

// Tags for custom fields. Note that these get persisted in the manifest,
// so existing tags should not be modified.
enum BlobFileAddition::CustomFieldTags : uint32_t {
  kEndMarker,

  // Add forward compatible fields here
  kPhysicalBlobFileSize,

  /////////////////////////////////////////////////////////////////////

  kForwardIncompatibleMask = 1 << 6,

  // Add forward incompatible fields here
  kFormatDescriptor = 1 << 6,
};

void BlobFileAddition::EncodeTo(std::string* output) const {
  PutVarint64(output, blob_file_number_);
  PutVarint64(output, total_blob_count_);
  PutVarint64(output, total_blob_bytes_);
  PutLengthPrefixedSlice(output, checksum_method_);
  PutLengthPrefixedSlice(output, checksum_value_);

  if (physical_blob_file_size_ != 0) {
    PutVarint32(output, kPhysicalBlobFileSize);
    std::string encoded_physical_blob_file_size;
    PutVarint64(&encoded_physical_blob_file_size, physical_blob_file_size_);
    PutLengthPrefixedSlice(output, encoded_physical_blob_file_size);
  }

  if (schema_version_ != kLegacyBlobFileSchemaVersion) {
    PutVarint32(output, kFormatDescriptor);
    std::string encoded_format_descriptor;
    encoded_format_descriptor.push_back(lossless_cast<char>(schema_version_));
    encoded_format_descriptor.append(file_identity_);
    PutLengthPrefixedSlice(output, encoded_format_descriptor);
  }

  // Encode any custom fields here. The format to use is a Varint32 tag (see
  // CustomFieldTags above) followed by a length prefixed slice. Unknown custom
  // fields will be ignored during decoding unless they're in the forward
  // incompatible range.

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

  while (true) {
    uint32_t custom_field_tag = 0;
    if (!GetVarint32(input, &custom_field_tag)) {
      return Status::Corruption(class_name, "Error decoding custom field tag");
    }

    if (custom_field_tag == kEndMarker) {
      break;
    }

    Slice custom_field_value;
    if (!GetLengthPrefixedSlice(input, &custom_field_value)) {
      return Status::Corruption(class_name,
                                "Error decoding custom field value");
    }

    if (custom_field_tag == kPhysicalBlobFileSize) {
      Status s = DecodePhysicalBlobFileSize(&custom_field_value,
                                            &physical_blob_file_size_);
      if (!s.ok()) {
        return s;
      }
      continue;
    }

    if (custom_field_tag == kFormatDescriptor) {
      Status s = DecodeFormatDescriptor(&custom_field_value, &schema_version_,
                                        &file_identity_);
      if (!s.ok()) {
        return s;
      }
      continue;
    }

    if (custom_field_tag & kForwardIncompatibleMask) {
      return Status::Corruption(
          class_name, "Forward incompatible custom field encountered");
    }
  }

  return ValidateDecodedBlobFileAddition(*this);
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
         lhs.GetPhysicalBlobFileSize() == rhs.GetPhysicalBlobFileSize() &&
         lhs.GetSchemaVersion() == rhs.GetSchemaVersion() &&
         lhs.GetFileIdentity() == rhs.GetFileIdentity();
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
     << Slice(blob_file_addition.GetChecksumValue()).ToString(/* hex */ true);

  if (blob_file_addition.GetPhysicalBlobFileSize() != 0) {
    os << " physical_blob_file_size: "
       << blob_file_addition.GetPhysicalBlobFileSize();
  }
  if (blob_file_addition.GetSchemaVersion() != kLegacyBlobFileSchemaVersion) {
    os << " schema_version: " << uint32_t{blob_file_addition.GetSchemaVersion()}
       << " file_identity: "
       << Slice(blob_file_addition.GetFileIdentity()).ToString(/* hex */ true);
  }

  return os;
}

JSONWriter& operator<<(JSONWriter& jw,
                       const BlobFileAddition& blob_file_addition) {
  jw << "BlobFileNumber" << blob_file_addition.GetBlobFileNumber()
     << "TotalBlobCount" << blob_file_addition.GetTotalBlobCount()
     << "TotalBlobBytes" << blob_file_addition.GetTotalBlobBytes()
     << "ChecksumMethod" << blob_file_addition.GetChecksumMethod()
     << "ChecksumValue"
     << Slice(blob_file_addition.GetChecksumValue()).ToString(/* hex */ true);
  if (blob_file_addition.GetPhysicalBlobFileSize() != 0) {
    jw << "PhysicalBlobFileSize"
       << blob_file_addition.GetPhysicalBlobFileSize();
  }
  if (blob_file_addition.GetSchemaVersion() != kLegacyBlobFileSchemaVersion) {
    jw << "SchemaVersion" << uint32_t{blob_file_addition.GetSchemaVersion()}
       << "FileIdentity"
       << Slice(blob_file_addition.GetFileIdentity()).ToString(/* hex */ true);
  }

  return jw;
}

}  // namespace ROCKSDB_NAMESPACE
