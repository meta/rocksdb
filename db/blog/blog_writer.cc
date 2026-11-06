//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_writer.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <limits>

#include "options/options_helper.h"
#include "table/format.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/fastrange.h"
#include "util/prefix_varint.h"
#include "util/string_util.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {
namespace {

TEST_BlogNoncanonicalConfig g_blog_noncanonical_config;

IOStatus ValidatePropertySectionSize(const BlogPropertyMap& properties,
                                     const char* section_name) {
  uint64_t property_section_size = kBlogBlockTrailerSize;
  for (const auto& [name, value] : properties) {
    const uint64_t name_size = name.size();
    const uint64_t value_size = value.size();
    const uint64_t encoded_property_size =
        PrefixVarint64Length(name_size) + name_size +
        PrefixVarint64Length(value_size) + value_size;
    if (encoded_property_size >
        kBlogMaxPropertySectionSize - property_section_size) {
      return IOStatus::InvalidArgument(
          std::string(section_name) +
          ": property section exceeds maximum size " +
          std::to_string(kBlogMaxPropertySectionSize));
    }
    property_section_size += encoded_property_size;
  }
  return IOStatus::OK();
}

enum BlogDecisionTag : uint64_t {
  kBlogDecisionForceFull = 1,
  kBlogDecisionOverlongVarint = 2,
  kBlogDecisionUnspecifiedSize = 3,
  kBlogDecisionShuffleHeaderRequiredProps = 4,
  kBlogDecisionShuffleHeaderIgnorableProps = 5,
  kBlogDecisionSplitIgnorableProps = 6,
  kBlogDecisionInjectAuxRecord = 7,
  kBlogDecisionAuxPayloadSize = 8,
  kBlogDecisionAuxPayloadByte = 9,
  kBlogDecisionRecordPad512 = 10,
  kBlogDecisionRecordPad4K = 11,
  kBlogDecisionPreferPadFF = 12,
  kBlogDecisionHeaderPad512 = 13,
  kBlogDecisionHeaderPad4K = 14,
  kBlogDecisionHeaderPreferPadFF = 15,
  kBlogDecisionShuffleFooterProps = 16,
  kBlogDecisionShuffleDynamicIgnorableProps = 17,
};

struct BlogDecisionInput {
  uint64_t record_ordinal;
  uint64_t tag;
  uint64_t extra;
  char escape_sequence[kBlogEscapeSequenceSize];
};

uint64_t HashBlogDecision(const TEST_BlogNoncanonicalConfig& config,
                          const BlogFileHeader& header, uint64_t record_ordinal,
                          uint64_t tag, uint64_t extra = 0) {
  BlogDecisionInput input{
      record_ordinal,
      tag,
      extra,
      {},
  };
  memcpy(input.escape_sequence, header.escape_sequence,
         kBlogEscapeSequenceSize);
  return XXH3_64bits_withSeed(&input, sizeof(input), config.seed);
}

bool ShouldApplyDecision(const TEST_BlogNoncanonicalConfig& config,
                         const BlogFileHeader& header, uint64_t record_ordinal,
                         uint64_t tag, uint32_t one_in, uint64_t extra = 0) {
  if (!config.enabled || one_in == 0) {
    return false;
  }
  return FastRange64(
             HashBlogDecision(config, header, record_ordinal, tag, extra),
             uint64_t{one_in}) == 0;
}

uint32_t PickDecision(const TEST_BlogNoncanonicalConfig& config,
                      const BlogFileHeader& header, uint64_t record_ordinal,
                      uint64_t tag, uint32_t upper_bound, uint64_t extra = 0) {
  if (upper_bound == 0) {
    return 0;
  }
  return static_cast<uint32_t>(
      FastRange64(HashBlogDecision(config, header, record_ordinal, tag, extra),
                  uint64_t{upper_bound}));
}

char* EncodePrefixVarint64WithMinBytes(char* dst, uint64_t value,
                                       uint32_t min_bytes) {
  assert(min_bytes >= 1);
  assert(min_bytes < kMaxPrefixVarint64Length);
  switch (min_bytes) {
    case 1:
      return EncodePrefixVarint64<1>(dst, value);
    case 2:
      return EncodePrefixVarint64<2>(dst, value);
    case 3:
      return EncodePrefixVarint64<3>(dst, value);
    case 4:
      return EncodePrefixVarint64<4>(dst, value);
    case 5:
      return EncodePrefixVarint64<5>(dst, value);
    case 6:
      return EncodePrefixVarint64<6>(dst, value);
    case 7:
      return EncodePrefixVarint64<7>(dst, value);
    case 8:
      return EncodePrefixVarint64<8>(dst, value);
  }
  assert(false);
  return dst;
}

uint32_t ChoosePaddingAlignment(const TEST_BlogNoncanonicalConfig& config,
                                const BlogFileHeader& header,
                                uint64_t record_ordinal, uint64_t tag_512,
                                uint64_t tag_4k) {
  if (ShouldApplyDecision(config, header, record_ordinal, tag_4k,
                          config.extra_padding_4k_one_in)) {
    return kBlogPageFriendlyAlignment;
  }
  if (ShouldApplyDecision(config, header, record_ordinal, tag_512,
                          config.extra_padding_512b_one_in)) {
    return kBlogFlushFriendlyAlignment;
  }
  return kBlogMinRecordAlignment;
}

bool PreferPadWith0xFF(const TEST_BlogNoncanonicalConfig& config,
                       const BlogFileHeader& header, uint64_t record_ordinal,
                       uint64_t tag) {
  return ShouldApplyDecision(config, header, record_ordinal, tag,
                             config.prefer_padding_with_0xff_one_in);
}

void MaybeShuffleProperties(BlogPropertyMap* props,
                            const TEST_BlogNoncanonicalConfig& config,
                            const BlogFileHeader& header,
                            uint64_t record_ordinal, uint64_t tag,
                            uint32_t one_in) {
  if (props == nullptr || props->size() < 2 ||
      !ShouldApplyDecision(config, header, record_ordinal, tag, one_in)) {
    return;
  }

  assert(props->size() <= std::numeric_limits<uint32_t>::max());
  const uint32_t shift = PickDecision(config, header, record_ordinal, tag + 1,
                                      static_cast<uint32_t>(props->size()));
  std::rotate(props->begin(), props->begin() + shift, props->end());
  if (props->size() > 2 &&
      ShouldApplyDecision(config, header, record_ordinal, tag + 2, 2)) {
    std::reverse(props->begin(), props->end());
  }
}

}  // namespace

void TEST_SetBlogNoncanonicalConfig(const TEST_BlogNoncanonicalConfig& config) {
  g_blog_noncanonical_config = config;
}

const TEST_BlogNoncanonicalConfig& TEST_GetBlogNoncanonicalConfig() {
  return g_blog_noncanonical_config;
}

TEST_BlogNoncanonicalConfigScope::TEST_BlogNoncanonicalConfigScope(
    const TEST_BlogNoncanonicalConfig& config)
    : prev_(TEST_GetBlogNoncanonicalConfig()) {
  TEST_SetBlogNoncanonicalConfig(config);
}

TEST_BlogNoncanonicalConfigScope::~TEST_BlogNoncanonicalConfigScope() {
  TEST_SetBlogNoncanonicalConfig(prev_);
}

BlogFileWriter::BlogFileWriter(std::unique_ptr<WritableFileWriter>&& dest,
                               const BlogFileHeader& header, bool manual_flush)
    : dest_(std::move(dest)), header_(header), manual_flush_(manual_flush) {}

BlogFileWriter::~BlogFileWriter() = default;

IOStatus BlogFileWriter::WriteHeader(const WriteOptions& wo) {
  assert(!header_written_);
  if (!IsSupportedChecksumType(header_.checksum_type)) {
    return IOStatus::InvalidArgument(
        "Blog header: unsupported checksum type " +
        std::to_string(lossless_cast<int>(header_.checksum_type)));
  }

  IOStatus size_status =
      ValidatePropertySectionSize(header_.required_properties, "Blog header");
  if (!size_status.ok()) {
    return size_status;
  }

  const TEST_BlogNoncanonicalConfig& config = TEST_GetBlogNoncanonicalConfig();

  // Copies: test shuffling below must not mutate the writer's header.
  BlogPropertyMap required_props = header_.required_properties;
  BlogPropertyMap ignorable_props = header_.ignorable_properties;
  MaybeShuffleProperties(&required_props, config, header_,
                         /*record_ordinal=*/0,
                         kBlogDecisionShuffleHeaderRequiredProps,
                         config.shuffle_properties_one_in);
  MaybeShuffleProperties(&ignorable_props, config, header_,
                         /*record_ordinal=*/0,
                         kBlogDecisionShuffleHeaderIgnorableProps,
                         config.shuffle_properties_one_in);

  BlogFileHeader header_copy = header_;
  header_copy.required_properties = std::move(required_props);
  // EncodeTo ignores the ignorable map, but don't carry body-record content
  // in a struct about to be encoded as a header.
  header_copy.ignorable_properties.clear();

  std::string buf;
  header_copy.EncodeTo(&buf);
  BlogPadding header_pad(
      lossless_cast<uint8_t>(buf.back()), buf.size(),
      ChoosePaddingAlignment(config, header_, /*record_ordinal=*/0,
                             kBlogDecisionHeaderPad512,
                             kBlogDecisionHeaderPad4K),
      PreferPadWith0xFF(config, header_, /*record_ordinal=*/0,
                        kBlogDecisionHeaderPreferPadFF));
  header_pad.AppendTo(&buf);

  IOStatus s = EmitBytes(wo, buf);
  if (s.ok()) {
    header_written_ = true;
  }

  if (s.ok() && !ignorable_props.empty()) {
    const bool split_ignorable =
        ignorable_props.size() > 1 &&
        ShouldApplyDecision(config, header_, /*record_ordinal=*/0,
                            kBlogDecisionSplitIgnorableProps,
                            config.split_ignorable_properties_one_in);
    if (split_ignorable) {
      for (const auto& prop : ignorable_props) {
        std::string payload;
        EncodeBlogProperties(BlogPropertyMap{prop}, &payload);
        s = AddRecordInternal(wo, kBlogIgnorablePropertiesRecord, payload,
                              kNoCompression, nullptr, /*force_full=*/true,
                              /*allow_unspecified_size=*/false,
                              /*record_size=*/nullptr);
        if (!s.ok()) {
          break;
        }
      }
    } else {
      std::string payload;
      EncodeBlogProperties(ignorable_props, &payload);
      s = AddRecordInternal(wo, kBlogIgnorablePropertiesRecord, payload,
                            kNoCompression, nullptr, /*force_full=*/true,
                            /*allow_unspecified_size=*/false,
                            /*record_size=*/nullptr);
    }
  }

  if (s.ok()) {
    s = MaybeFlush(wo);
  }
  return s;
}

IOStatus BlogFileWriter::AddBlobRecord(const WriteOptions& wo,
                                       const Slice& payload,
                                       CompressionType comp_type,
                                       uint64_t* blob_offset,
                                       uint64_t uncompressed_size) {
  // Reset first so a rejected call reports no sizes rather than stale ones.
  last_blob_record_size_ = 0;
  last_auxiliary_record_size_ = 0;

  if (payload.empty()) {
    return IOStatus::InvalidArgument(
        "Blog blob record payload must not be empty");
  }

  uint64_t blob_record_size = 0;
  IOStatus s =
      AddRecordInternal(wo, kBlogBlobRecord, payload, comp_type, blob_offset,
                        /*force_full=*/false,
                        /*allow_unspecified_size=*/true, &blob_record_size);
  if (s.ok()) {
    last_blob_record_size_ = blob_record_size;
    blob_stats_.count++;
    blob_stats_.payload_bytes += payload.size();
    blob_stats_.overhead_bytes += blob_record_size - payload.size();
    if (comp_type != kNoCompression) {
      blob_stats_.compressed_bytes += payload.size();
      blob_stats_.uncompressed_bytes += uncompressed_size;
    }

    s = MaybeEmitAuxiliaryRecord(wo);
  }

  if (s.ok()) {
    s = MaybeFlush(wo);
  }
  return s;
}

IOStatus BlogFileWriter::AddWriteBatchRecord(const WriteOptions& wo,
                                             const Slice& wb_data,
                                             CompressionType comp_type) {
  IOStatus s =
      AddRecordInternal(wo, kBlogWriteBatchRecord, wb_data, comp_type, nullptr,
                        /*force_full=*/false,
                        /*allow_unspecified_size=*/true,
                        /*record_size=*/nullptr);
  if (s.ok()) {
    s = MaybeFlush(wo);
  }
  return s;
}

IOStatus BlogFileWriter::AddIgnorablePropertiesRecord(
    const WriteOptions& wo, const BlogPropertyMap& props) {
  assert(header_written_);
  assert(std::all_of(props.begin(), props.end(),
                     [](const BlogPropertyMap::value_type& prop) {
                       return IsBlogIgnorableProperty(prop.first);
                     }));

  const TEST_BlogNoncanonicalConfig& config = TEST_GetBlogNoncanonicalConfig();
  BlogPropertyMap ignorable_props = props;
  MaybeShuffleProperties(&ignorable_props, config, header_, record_ordinal_,
                         kBlogDecisionShuffleDynamicIgnorableProps,
                         config.shuffle_properties_one_in);

  IOStatus s;
  if (ignorable_props.size() > 1 && ShouldSplitIgnorableProperties()) {
    for (const auto& prop : ignorable_props) {
      std::string payload;
      EncodeBlogProperties(BlogPropertyMap{prop}, &payload);
      s = AddRecordInternal(wo, kBlogIgnorablePropertiesRecord, payload,
                            kNoCompression, nullptr, /*force_full=*/true,
                            /*allow_unspecified_size=*/false,
                            /*record_size=*/nullptr);
      if (!s.ok()) {
        break;
      }
    }
  } else {
    std::string payload;
    EncodeBlogProperties(ignorable_props, &payload);
    s = AddRecordInternal(wo, kBlogIgnorablePropertiesRecord, payload,
                          kNoCompression, nullptr, /*force_full=*/true,
                          /*allow_unspecified_size=*/false,
                          /*record_size=*/nullptr);
  }

  if (s.ok()) {
    s = MaybeFlush(wo);
  }
  return s;
}

IOStatus BlogFileWriter::AddPreambleStartRecord(const WriteOptions& wo) {
  IOStatus s = AddRecordInternal(wo, kBlogPreambleStartRecord, Slice(),
                                 kNoCompression, nullptr,
                                 /*force_full=*/false,
                                 /*allow_unspecified_size=*/false,
                                 /*record_size=*/nullptr);
  if (s.ok()) {
    s = MaybeFlush(wo);
  }
  return s;
}

IOStatus BlogFileWriter::WriteFooter(
    const WriteOptions& wo, const BlogFileFooterProperties& properties) {
  assert(header_written_);
  assert(!sealed_);
  assert(std::none_of(
      properties.properties.begin(), properties.properties.end(),
      [](const BlogPropertyMap::value_type& prop) {
        const std::string& name = prop.first;
        return name == "blobCount" || name == "blobPayloadBytes" ||
               name == "blobCompressedBytes" ||
               name == "blobUncompressedBytes" || name == "blobOverheadBytes" ||
               name == "compressionTypes";
      }));

  BlogFileFooterProperties footer_properties = properties;
  if (blob_stats_.count > 0) {
    footer_properties.SetBlobCount(blob_stats_.count);
    footer_properties.SetBlobPayloadBytes(blob_stats_.payload_bytes);
    if (blob_stats_.compressed_bytes > 0) {
      footer_properties.SetBlobCompressedBytes(blob_stats_.compressed_bytes);
      footer_properties.SetBlobUncompressedBytes(
          blob_stats_.uncompressed_bytes);
    }
    footer_properties.SetBlobOverheadBytes(blob_stats_.overhead_bytes);
  }
  if (!compression_type_set_.empty()) {
    footer_properties.SetCompressionTypes(compression_type_set_);
  }

  IOStatus size_status = ValidatePropertySectionSize(
      footer_properties.properties, "Blog footer properties");
  if (!size_status.ok()) {
    return size_status;
  }

  const TEST_BlogNoncanonicalConfig& config = TEST_GetBlogNoncanonicalConfig();
  MaybeShuffleProperties(&footer_properties.properties, config, header_,
                         record_ordinal_, kBlogDecisionShuffleFooterProps,
                         config.shuffle_properties_one_in);

  std::string properties_payload;
  footer_properties.EncodeTo(&properties_payload);
  sealed_ = true;
  const uint64_t properties_offset = offset_;
  IOStatus s = AddRecordInternal(
      wo, kBlogFooterPropertiesRecord, properties_payload, kNoCompression,
      nullptr, /*force_full=*/true, /*allow_unspecified_size=*/false,
      /*record_size=*/nullptr);
  if (!s.ok()) {
    return s;
  }

  assert(offset_ > properties_offset);
  const uint64_t relative_bytes = offset_ - properties_offset;
  assert(relative_bytes % kBlogMinRecordAlignment == 0);
  const uint64_t relative_offset_4b = relative_bytes / kBlogMinRecordAlignment;
  assert(relative_offset_4b <= UINT32_MAX);

  BlogFileFooterLocator locator;
  locator.entries.push_back(
      {kBlogFooterPropertiesRecord, static_cast<uint32_t>(relative_offset_4b)});
  std::string locator_payload;
  locator.EncodeTo(&locator_payload);
  s = AddRecordInternal(wo, kBlogFooterLocatorRecord, locator_payload,
                        kNoCompression, nullptr, /*force_full=*/true,
                        /*allow_unspecified_size=*/false,
                        /*record_size=*/nullptr);
  if (s.ok()) {
    s = MaybeFlush(wo);
  }
  return s;
}

IOStatus BlogFileWriter::WriteBuffer(const WriteOptions& wo) {
  IOOptions opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(wo, opts);
  if (s.ok()) {
    s = dest_->Flush(opts);
  }
  return s;
}

IOStatus BlogFileWriter::Sync(const WriteOptions& wo, bool use_fsync) {
  IOOptions opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(wo, opts);
  if (s.ok()) {
    s = use_fsync ? dest_->Fsync(opts) : dest_->Sync(opts);
  }
  return s;
}

IOStatus BlogFileWriter::Close(const WriteOptions& wo) {
  IOOptions opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(wo, opts);
  if (s.ok()) {
    s = dest_->Close(opts);
  }
  return s;
}

uint64_t BlogFileWriter::EstimateNextBlobWritePhysicalGrowth(
    const Slice& payload, CompressionType comp_type) const {
  assert(header_written_);

  const RecordPlan blob_plan =
      PlanRecord(kBlogBlobRecord, payload, /*force_full=*/false,
                 /*allow_unspecified_size=*/true, record_ordinal_);
  uint64_t growth =
      EstimateRecordSize(payload, comp_type, kBlogBlobRecord, blob_plan,
                         offset_, /*skip_padding=*/false);

  const uint64_t aux_record_ordinal = record_ordinal_ + 1;
  if (ShouldEmitAuxiliaryRecord(aux_record_ordinal)) {
    const std::string aux_payload = BuildAuxiliaryPayload(aux_record_ordinal);
    const RecordPlan aux_plan =
        PlanRecord(kBlogWriteBatchRecord, aux_payload, /*force_full=*/false,
                   /*allow_unspecified_size=*/true, aux_record_ordinal);
    growth +=
        EstimateRecordSize(aux_payload, kNoCompression, kBlogWriteBatchRecord,
                           aux_plan, offset_ + growth, /*skip_padding=*/false);
  }

  return growth;
}

BlogFileWriter::RecordPlan BlogFileWriter::PlanRecord(
    BlogRecordType type, const Slice& payload, bool force_full,
    bool allow_unspecified_size, uint64_t record_ordinal) const {
  RecordPlan plan;

  const TEST_BlogNoncanonicalConfig& config = TEST_GetBlogNoncanonicalConfig();
  plan.padding_alignment = ChoosePaddingAlignment(
      config, header_, record_ordinal, kBlogDecisionRecordPad512,
      kBlogDecisionRecordPad4K);
  plan.prefer_padding_with_0xff = PreferPadWith0xFF(
      config, header_, record_ordinal, kBlogDecisionPreferPadFF);

  if (payload.empty()) {
    return plan;
  }

  const uint32_t natural_varint_len = PrefixVarint64Length(payload.size());
  plan.use_compact = !force_full &&
                     natural_varint_len <= kBlogCompactVarintMaxBytes &&
                     type == header_.compact_record_type;

  if (plan.use_compact &&
      ShouldApplyDecision(config, header_, record_ordinal,
                          kBlogDecisionForceFull,
                          config.force_full_record_one_in)) {
    plan.use_compact = false;
  }

  if (plan.use_compact) {
    return plan;
  }

  plan.full_varint_min_bytes =
      std::max(uint32_t{kBlogCompactVarintMaxBytes} + 1, natural_varint_len);

  const bool body_record = type == kBlogBlobRecord ||
                           type == kBlogWriteBatchRecord ||
                           type == kBlogManifestRecord;
  if (allow_unspecified_size && body_record &&
      ShouldApplyDecision(config, header_, record_ordinal,
                          kBlogDecisionUnspecifiedSize,
                          config.unspecified_size_record_one_in)) {
    plan.use_unspecified_size = true;
    plan.padding_alignment = kBlogMinRecordAlignment;
    return plan;
  }

  if (plan.full_varint_min_bytes < kMaxPrefixVarint64Length - 1 &&
      ShouldApplyDecision(config, header_, record_ordinal,
                          kBlogDecisionOverlongVarint,
                          config.overlong_full_varint_one_in)) {
    const uint32_t extra_bytes =
        uint32_t{kMaxPrefixVarint64Length - 1} - plan.full_varint_min_bytes;
    plan.full_varint_min_bytes +=
        1 + PickDecision(config, header_, record_ordinal,
                         kBlogDecisionOverlongVarint, extra_bytes,
                         /*extra=*/1);
  }

  return plan;
}

uint64_t BlogFileWriter::EstimateRecordSize(
    const Slice& payload, CompressionType comp_type, BlogRecordType type,
    const RecordPlan& plan, uint64_t record_offset, bool skip_padding) const {
  if (payload.empty()) {
    std::string checksum_input;
    PutPrefixVarint64(&checksum_input, 0);
    checksum_input.push_back(lossless_cast<char>(type));
    const uint32_t checksum =
        ComputeBuiltinChecksum(header_.checksum_type, checksum_input.data(),
                               checksum_input.size()) +
        ChecksumModifierForContext(header_.incarnation_id(), record_offset);
    uint64_t record_size = kBlogEscapeSequenceSize + checksum_input.size() + 4;
    if (!skip_padding) {
      record_size += ComputeBlogPaddingLength(
          static_cast<uint8_t>(checksum >> 24), record_offset + record_size,
          plan.padding_alignment);
    }
    return record_size;
  }

  uint64_t prefix_size = kBlogEscapeSequenceSize;
  if (plan.use_compact) {
    prefix_size += PrefixVarint64Length(payload.size());
  } else if (plan.use_unspecified_size) {
    prefix_size += kMaxPrefixVarint64Length + 6;
  } else {
    prefix_size += std::max(plan.full_varint_min_bytes,
                            PrefixVarint64Length(payload.size())) +
                   6;
  }

  const uint64_t payload_file_offset = record_offset + prefix_size;
  const uint64_t trailer_offset = payload_file_offset + payload.size();
  const uint32_t checksum =
      ComputeBlogRecordChecksum(header_.checksum_type, payload.data(),
                                payload.size(), lossless_cast<char>(comp_type),
                                header_.incarnation_id(), payload_file_offset);

  uint64_t record_size = prefix_size + payload.size() + kBlogBlockTrailerSize;
  if (!skip_padding) {
    record_size += ComputeBlogPaddingLength(
        static_cast<uint8_t>(checksum >> 24),
        trailer_offset + kBlogBlockTrailerSize, plan.padding_alignment);
  }
  return record_size;
}

bool BlogFileWriter::IsBlobRoleFile() const {
  return header_.compact_record_type == kBlogBlobRecord ||
         header_.GetProperty(kBlogPropRole) == "blob";
}

bool BlogFileWriter::ShouldEmitAuxiliaryRecord(uint64_t record_ordinal) const {
  const TEST_BlogNoncanonicalConfig& config = TEST_GetBlogNoncanonicalConfig();
  return IsBlobRoleFile() && config.max_auxiliary_payload_bytes > 0 &&
         ShouldApplyDecision(config, header_, record_ordinal,
                             kBlogDecisionInjectAuxRecord,
                             config.inject_auxiliary_record_one_in);
}

bool BlogFileWriter::ShouldSplitIgnorableProperties() const {
  return ShouldApplyDecision(
      TEST_GetBlogNoncanonicalConfig(), header_, record_ordinal_,
      kBlogDecisionSplitIgnorableProps,
      TEST_GetBlogNoncanonicalConfig().split_ignorable_properties_one_in);
}

std::string BlogFileWriter::BuildAuxiliaryPayload(
    uint64_t record_ordinal) const {
  const TEST_BlogNoncanonicalConfig& config = TEST_GetBlogNoncanonicalConfig();
  assert(config.max_auxiliary_payload_bytes > 0);

  const uint32_t payload_size =
      1 + PickDecision(config, header_, record_ordinal,
                       kBlogDecisionAuxPayloadSize,
                       config.max_auxiliary_payload_bytes);
  std::string payload(payload_size, 'a');
  for (uint32_t i = 0; i < payload_size; ++i) {
    const uint32_t next = PickDecision(config, header_, record_ordinal,
                                       kBlogDecisionAuxPayloadByte, 26, i);
    payload[i] = static_cast<char>('a' + next);
  }
  return payload;
}

IOStatus BlogFileWriter::AddRecordInternal(
    const WriteOptions& wo, BlogRecordType type, const Slice& payload,
    CompressionType comp_type, uint64_t* payload_offset, bool force_full,
    bool allow_unspecified_size, uint64_t* record_size) {
  assert(header_written_);
  assert(IsKnownBlogRecordType(type));
  assert(sealed_ == IsBlogFooterRecordType(type));
  assert(IsValidBlogCompressionType(lossless_cast<uint8_t>(comp_type)));
  assert(!payload.empty() || comp_type == kNoCompression);
  assert(comp_type == kNoCompression ||
         header_.HasProperty(kBlogPropCompressionCompatibilityName));
  assert(comp_type != kStreamingCompressionSentinel ||
         (type == kBlogWriteBatchRecord &&
          header_.HasProperty(kBlogPropWriteBatchStreamingCompressionType)));
  assert(!allow_unspecified_size || type == kBlogBlobRecord ||
         type == kBlogWriteBatchRecord || type == kBlogManifestRecord);
  assert(!IsBlogFooterRecordType(type) || force_full);
  assert(type != kBlogPreambleStartRecord || payload.empty());
  assert(type != kBlogFooterLocatorRecord || !payload.empty());
  assert(payload.size() < kBlogUnspecifiedSize);

  const uint64_t offset_before = offset_;
  const RecordPlan plan = PlanRecord(type, payload, force_full,
                                     allow_unspecified_size, record_ordinal_);
  IOStatus s =
      EmitRecordWithPlan(wo, type, payload, comp_type, plan, payload_offset);
  if (s.ok()) {
    ++record_ordinal_;
    if (comp_type != kNoCompression &&
        comp_type != kStreamingCompressionSentinel) {
      compression_type_set_.Add(comp_type);
    }
    if (record_size != nullptr) {
      *record_size = offset_ - offset_before;
    }
  }
  return s;
}

IOStatus BlogFileWriter::EmitRecordWithPlan(const WriteOptions& wo,
                                            BlogRecordType type,
                                            const Slice& payload,
                                            CompressionType comp_type,
                                            const RecordPlan& plan,
                                            uint64_t* payload_offset) {
  if (payload.empty()) {
    return EmitTrivialRecord(wo, type, plan);
  }
  if (plan.use_compact) {
    return EmitCompactRecord(wo, payload, comp_type, plan, payload_offset);
  }
  return EmitFullRecord(wo, type, payload, comp_type, plan, payload_offset);
}

IOStatus BlogFileWriter::EmitTrivialRecord(const WriteOptions& wo,
                                           BlogRecordType type,
                                           const RecordPlan& plan) {
  std::string buf;
  buf.append(header_.escape_sequence, kBlogEscapeSequenceSize);
  PutPrefixVarint64(&buf, 0);
  buf.push_back(lossless_cast<char>(type));

  const char* cksum_data = buf.data() + kBlogEscapeSequenceSize;
  const size_t cksum_data_size = buf.size() - kBlogEscapeSequenceSize;
  const uint32_t checksum =
      ComputeBuiltinChecksum(header_.checksum_type, cksum_data,
                             cksum_data_size) +
      ChecksumModifierForContext(header_.incarnation_id(), offset_);
  PutFixed32(&buf, checksum);

  IOStatus s = EmitBytes(wo, buf);
  if (!s.ok()) {
    return s;
  }

  const BlogPadding pad(static_cast<uint8_t>(checksum >> 24), offset_,
                        plan.padding_alignment, plan.prefer_padding_with_0xff);
  return EmitPadding(wo, pad);
}

IOStatus BlogFileWriter::EmitCompactRecord(const WriteOptions& wo,
                                           const Slice& payload,
                                           CompressionType comp_type,
                                           const RecordPlan& plan,
                                           uint64_t* payload_offset) {
  assert(!payload.empty());

  std::string prefix;
  prefix.append(header_.escape_sequence, kBlogEscapeSequenceSize);
  PutPrefixVarint64(&prefix, payload.size());

  IOStatus s = EmitBytes(wo, prefix);
  if (!s.ok()) {
    return s;
  }

  return EmitPayloadTrailerPadding(wo, payload, comp_type, plan, payload_offset,
                                   /*skip_padding=*/false);
}

IOStatus BlogFileWriter::EmitFullRecord(const WriteOptions& wo,
                                        BlogRecordType type,
                                        const Slice& payload,
                                        CompressionType comp_type,
                                        const RecordPlan& plan,
                                        uint64_t* payload_offset) {
  assert(!payload.empty());

  std::string prefix;
  prefix.append(header_.escape_sequence, kBlogEscapeSequenceSize);

  const size_t varint_start = prefix.size();
  if (plan.use_unspecified_size) {
    PutPrefixVarint64(&prefix, kBlogUnspecifiedSize);
  } else if (plan.full_varint_min_bytes == kMaxPrefixVarint64Length) {
    PutPrefixVarint64(&prefix, payload.size());
  } else {
    char buf[kMaxPrefixVarint64Length];
    char* end = EncodePrefixVarint64WithMinBytes(buf, payload.size(),
                                                 plan.full_varint_min_bytes);
    prefix.append(buf, static_cast<size_t>(end - buf));
  }

  prefix.push_back(lossless_cast<char>(type));
  prefix.push_back(lossless_cast<char>(comp_type));

  const char* prefix_data = prefix.data() + varint_start;
  const size_t prefix_data_size = prefix.size() - varint_start;
  const uint32_t prefix_checksum =
      ComputeBuiltinChecksum(header_.checksum_type, prefix_data,
                             prefix_data_size) +
      ChecksumModifierForContext(header_.incarnation_id(), offset_);
  PutFixed32(&prefix, prefix_checksum);

  IOStatus s = EmitBytes(wo, prefix);
  if (!s.ok()) {
    return s;
  }

  return EmitPayloadTrailerPadding(wo, payload, comp_type, plan, payload_offset,
                                   type == kBlogFooterLocatorRecord);
}

IOStatus BlogFileWriter::EmitPayloadTrailerPadding(
    const WriteOptions& wo, const Slice& payload, CompressionType comp_type,
    const RecordPlan& plan, uint64_t* payload_offset, bool skip_padding) {
  if (payload_offset != nullptr) {
    *payload_offset = offset_;
  }

  IOStatus s = EmitBytes(wo, payload);
  if (!s.ok()) {
    return s;
  }

  char trailer[kBlogBlockTrailerSize];
  trailer[0] = lossless_cast<char>(comp_type);
  const uint32_t checksum = ComputeBlogRecordChecksum(
      header_.checksum_type, payload.data(), payload.size(), trailer[0],
      header_.incarnation_id(), offset_ - payload.size());
  EncodeFixed32(trailer + 1, checksum);
  s = EmitBytes(wo, trailer, kBlogBlockTrailerSize);
  if (!s.ok()) {
    return s;
  }

  if (skip_padding) {
    return IOStatus::OK();
  }

  const BlogPadding pad(
      lossless_cast<uint8_t>(trailer[kBlogBlockTrailerSize - 1]), offset_,
      plan.padding_alignment, plan.prefer_padding_with_0xff);
  return EmitPadding(wo, pad);
}

IOStatus BlogFileWriter::EmitPadding(const WriteOptions& wo,
                                     const BlogPadding& pad) {
  if (pad.count == 0) {
    return IOStatus::OK();
  }

  std::array<char, 256> scratch;
  scratch.fill(pad.byte);

  uint32_t remaining = pad.count;
  while (remaining > 0) {
    const size_t to_write = std::min<size_t>(remaining, scratch.size());
    IOStatus s = EmitBytes(wo, scratch.data(), to_write);
    if (!s.ok()) {
      return s;
    }
    remaining -= static_cast<uint32_t>(to_write);
  }
  return IOStatus::OK();
}

IOStatus BlogFileWriter::MaybeFlush(const WriteOptions& wo) {
  if (manual_flush_) {
    return IOStatus::OK();
  }

  IOOptions opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(wo, opts);
  if (s.ok()) {
    s = dest_->Flush(opts);
  }
  return s;
}

IOStatus BlogFileWriter::MaybeEmitAuxiliaryRecord(const WriteOptions& wo) {
  if (!ShouldEmitAuxiliaryRecord(record_ordinal_)) {
    return IOStatus::OK();
  }

  const std::string payload = BuildAuxiliaryPayload(record_ordinal_);
  return AddRecordInternal(wo, kBlogWriteBatchRecord, payload, kNoCompression,
                           nullptr, /*force_full=*/false,
                           /*allow_unspecified_size=*/true,
                           &last_auxiliary_record_size_);
}

IOStatus BlogFileWriter::EmitBytes(const WriteOptions& wo, const Slice& data) {
  return EmitBytes(wo, data.data(), data.size());
}

IOStatus BlogFileWriter::EmitBytes(const WriteOptions& wo, const char* data,
                                   size_t len) {
  IOOptions opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(wo, opts);
  if (!s.ok()) {
    return s;
  }

  s = dest_->Append(Slice(data, len), opts);
  if (s.ok()) {
    offset_ += len;
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
