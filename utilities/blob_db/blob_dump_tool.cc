//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/blob_db/blob_dump_tool.h"

#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "db/blog/blog_reader.h"
#include "file/random_access_file_reader.h"
#include "file/readahead_raf.h"
#include "file/sequence_file_reader.h"
#include "port/port.h"
#include "rocksdb/advanced_compression.h"
#include "rocksdb/convenience.h"
#include "rocksdb/file_system.h"
#include "table/format.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/prefix_varint.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE::blob_db {

namespace {

constexpr size_t kBlobDumpReadaheadSize = 2 * 1024 * 1024;

struct BlogDumpSummary {
  uint64_t blob_records = 0;
  uint64_t auxiliary_body_records = 0;
  uint64_t blob_bytes = 0;
  uint64_t uncompressed_blob_bytes = 0;
  uint64_t raw_blob_bytes = 0;
  bool saw_compressed_blob = false;
};

const char* GetBlogRecordTypeName(BlogRecordType type) {
  switch (type) {
    case kBlogIgnorablePropertiesRecord:
      return "IgnorableProperties";
    case kBlogBlobRecord:
      return "Blob";
    case kBlogPreambleStartRecord:
      return "PreambleStart";
    case kBlogWriteBatchRecord:
      return "WriteBatch";
    case kBlogManifestRecord:
      return "Manifest";
    case kBlogFooterIndexRecord:
      return "FooterIndex";
    case kBlogFooterPropertiesRecord:
      return "FooterProperties";
    case kBlogFooterFileChecksumInfo:
      return "FooterFileChecksumInfo";
    case kBlogFooterLocatorRecord:
      return "FooterLocator";
  }
  return nullptr;
}

std::string DescribeBlogRecordType(BlogRecordType type) {
  char buf[16];
  snprintf(buf, sizeof(buf), "0x%02X", uint32_t{lossless_cast<uint8_t>(type)});
  const char* name = GetBlogRecordTypeName(type);
  if (name == nullptr) {
    return std::string(buf);
  }
  return std::string(name) + " (" + buf + ")";
}

std::string DescribeCompressionType(CompressionType type) {
  std::string compression_name;
  if (GetStringFromCompressionType(&compression_name, type).ok()) {
    return compression_name;
  }
  return "Unknown (" + std::to_string(lossless_cast<uint32_t>(type)) + ")";
}

std::string DescribeChecksumType(ChecksumType type) {
  switch (type) {
    case kNoChecksum:
      return "kNoChecksum";
    case kCRC32c:
      return "kCRC32c";
    case kxxHash:
      return "kxxHash";
    case kxxHash64:
      return "kxxHash64";
    case kXXH3:
      return "kXXH3";
  }
  return "Unknown (" + std::to_string(lossless_cast<uint32_t>(type)) + ")";
}

bool IsPrintableAscii(Slice value) {
  for (size_t i = 0; i < value.size(); ++i) {
    const unsigned char uc = lossless_cast<unsigned char>(value[i]);
    if (uc < 0x20 || uc > 0x7E) {
      return false;
    }
  }
  return true;
}

bool TryDecodeBlogUint64Property(Slice value, uint64_t* decoded_value) {
  return GetPrefixVarint64(&value, decoded_value) && value.empty();
}

bool GetBlogUint64Property(const BlogPropertyMap& props, const char* name,
                           uint64_t* decoded_value) {
  for (const auto& [prop_name, prop_value] : props) {
    if (prop_name != name) {
      continue;
    }
    return TryDecodeBlogUint64Property(prop_value, decoded_value);
  }
  return false;
}

std::string DescribeCompressionTypesProperty(Slice value) {
  std::string result = "[";
  for (size_t i = 0; i < value.size(); ++i) {
    if (i > 0) {
      result += ", ";
    }
    result += DescribeCompressionType(
        lossless_cast<CompressionType>(lossless_cast<uint8_t>(value[i])));
  }
  result += "]";
  return result;
}

std::string FormatBlogPropertyValue(const std::string& name,
                                    const std::string& value) {
  if (name == "compressionTypes") {
    return DescribeCompressionTypesProperty(Slice(value));
  }

  if (IsPrintableAscii(value)) {
    return value;
  }

  uint64_t decoded_value = 0;
  if (TryDecodeBlogUint64Property(Slice(value), &decoded_value)) {
    return std::to_string(decoded_value);
  }

  return "0x" + Slice(value).ToString(true /* hex */);
}

void PrintBlogProperties(const char* label, const BlogPropertyMap& props) {
  if (props.empty()) {
    return;
  }
  fprintf(stdout, "  %s:\n", label);
  for (const auto& [name, value] : props) {
    fprintf(stdout, "    %s : %s\n", name.c_str(),
            FormatBlogPropertyValue(name, value).c_str());
  }
}

bool IsBlogBlobFile(const BlogFileHeader& header) {
  const std::string role = header.GetProperty(kBlogPropRole);
  return header.compact_record_type == kBlogBlobRecord &&
         (role.empty() || role == "blob");
}

bool IsAuxiliaryBodyRecord(BlogRecordType type) {
  const uint8_t raw_type = lossless_cast<uint8_t>(type);
  return raw_type >= 0x10 && raw_type <= 0x7F && type != kBlogBlobRecord;
}

Status ResolveBlogDumpDecompressor(
    const BlogFileHeader& header, std::shared_ptr<Decompressor>* decompressor) {
  std::shared_ptr<CompressionManager> compression_manager;
  const std::string compatibility_name =
      header.GetProperty(kBlogPropCompressionCompatibilityName);

  if (compatibility_name.empty()) {
    compression_manager = GetBuiltinV2CompressionManager();
  } else {
    Status s = ResolveCompressionManagerByCompatibilityName(
        Slice(compatibility_name), nullptr, &compression_manager);
    if (!s.ok()) {
      return s;
    }
    if (compression_manager == nullptr) {
      return Status::Corruption(
          "Blog blob file: no compatible compression manager for \"" +
          compatibility_name + "\"");
    }
  }

  *decompressor = compression_manager->GetDecompressor();
  if (*decompressor == nullptr) {
    return Status::NotSupported(
        "Blog blob file: unable to construct decompressor");
  }

  return Status::OK();
}

Status DecompressBlogBlobForDump(
    const Slice& payload, CompressionType record_compression_type,
    const std::shared_ptr<Decompressor>& decompressor,
    const Status& decompressor_status, std::string* uncompressed_payload) {
  if (record_compression_type == kNoCompression) {
    uncompressed_payload->assign(payload.data(), payload.size());
    return Status::OK();
  }

  if (record_compression_type == kStreamingCompressionSentinel) {
    return Status::NotSupported(
        "Blog blob dump does not support streaming-compressed blob records");
  }

  if (decompressor == nullptr) {
    if (!decompressor_status.ok()) {
      return decompressor_status;
    }
    return Status::NotSupported(
        "Blog blob dump does not have a compatible decompressor");
  }

  Decompressor::Args args;
  args.compression_type = record_compression_type;
  args.compressed_data = payload;
  Status s = decompressor->ExtractUncompressedSize(args);
  if (!s.ok()) {
    return s;
  }

  uncompressed_payload->resize(args.uncompressed_size);
  return decompressor->DecompressBlock(args, uncompressed_payload->data());
}

}  // namespace

BlobDumpTool::BlobDumpTool()
    : reader_(nullptr), buffer_(nullptr), buffer_size_(0) {}

Status BlobDumpTool::Run(const std::string& filename, DisplayType show_key,
                         DisplayType show_blob,
                         DisplayType show_uncompressed_blob,
                         bool show_summary) {
  Status s;
  const auto fs = FileSystem::Default();
  IOOptions io_opts;
  s = fs->FileExists(filename, io_opts, nullptr);
  if (!s.ok()) {
    return s;
  }
  uint64_t file_size = 0;
  s = fs->GetFileSize(filename, io_opts, &file_size, nullptr);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<FSRandomAccessFile> file;
  s = fs->NewRandomAccessFile(filename, FileOptions(), &file, nullptr);
  if (!s.ok()) {
    return s;
  }
  file = NewReadaheadRandomAccessFile(std::move(file), kBlobDumpReadaheadSize);
  if (file_size == 0) {
    return Status::Corruption("File is empty.");
  }
  reader_.reset(new RandomAccessFileReader(std::move(file), filename));

  Slice magic_slice;
  s = Read(
      0,
      lossless_cast<size_t>(std::min(file_size, uint64_t{kBlogFileMagicSize})),
      &magic_slice);
  if (!s.ok()) {
    return s;
  }
  if (BlogFileHeader::IsBlogFormat(magic_slice.data(), magic_slice.size())) {
    return DumpBlogBlobFile(filename, show_key, show_blob,
                            show_uncompressed_blob, show_summary);
  }

  uint64_t offset = 0;
  uint64_t footer_offset = 0;
  CompressionType compression = kNoCompression;
  s = DumpBlobLogHeader(&offset, &compression);
  if (!s.ok()) {
    return s;
  }
  s = DumpBlobLogFooter(file_size, &footer_offset);
  if (!s.ok()) {
    return s;
  }
  uint64_t total_records = 0;
  uint64_t total_key_size = 0;
  uint64_t total_blob_size = 0;
  uint64_t total_uncompressed_blob_size = 0;
  if (show_key != DisplayType::kNone || show_summary) {
    while (offset < footer_offset) {
      s = DumpRecord(show_key, show_blob, show_uncompressed_blob, show_summary,
                     compression, &offset, &total_records, &total_key_size,
                     &total_blob_size, &total_uncompressed_blob_size);
      if (!s.ok()) {
        break;
      }
    }
  }
  if (show_summary) {
    fprintf(stdout, "Summary:\n");
    fprintf(stdout, "  total records: %" PRIu64 "\n", total_records);
    fprintf(stdout, "  total key size: %" PRIu64 "\n", total_key_size);
    fprintf(stdout, "  total blob size: %" PRIu64 "\n", total_blob_size);
    if (compression != kNoCompression) {
      fprintf(stdout, "  total raw blob size: %" PRIu64 "\n",
              total_uncompressed_blob_size);
    }
  }
  return s;
}

Status BlobDumpTool::Read(uint64_t offset, size_t size, Slice* result) {
  if (buffer_size_ < size) {
    if (buffer_size_ == 0) {
      buffer_size_ = 4096;
    }
    while (buffer_size_ < size) {
      buffer_size_ *= 2;
    }
    buffer_.reset(new char[buffer_size_]);
  }
  Status s =
      reader_->Read(offset, size, IOOptions(), result, buffer_.get(), nullptr);
  if (!s.ok()) {
    return s;
  }
  if (result->size() != size) {
    return Status::Corruption("Reach the end of the file unexpectedly.");
  }
  return s;
}

Status BlobDumpTool::DumpBlogBlobFile(const std::string& filename,
                                      DisplayType show_key,
                                      DisplayType show_blob,
                                      DisplayType show_uncompressed_blob,
                                      bool show_summary) {
  const auto fs = FileSystem::Default();
  std::unique_ptr<FSSequentialFile> file;
  Status s = fs->NewSequentialFile(filename, FileOptions(), &file, nullptr);
  if (!s.ok()) {
    return s;
  }

  auto seq_reader = std::make_unique<SequentialFileReader>(
      std::move(file), filename, kBlobDumpReadaheadSize);
  BlogFileReader blog_reader(std::move(seq_reader), nullptr,
                             /*verify_checksums=*/true);

  BlogFileHeader header;
  s = blog_reader.ReadHeader(&header);
  if (!s.ok()) {
    return s;
  }
  if (!IsBlogBlobFile(header)) {
    return Status::NotSupported(
        "Blog file is not a blob-role file (compact_record_type=" +
        DescribeBlogRecordType(header.compact_record_type) + ", role=\"" +
        header.GetProperty(kBlogPropRole) + "\")");
  }

  std::shared_ptr<Decompressor> decompressor;
  Status decompressor_status =
      ResolveBlogDumpDecompressor(header, &decompressor);
  const bool has_decompressor = decompressor_status.ok();

  fprintf(stdout, "Blog blob file header:\n");
  fprintf(stdout, "  Schema Version     : %" PRIu32 "\n",
          uint32_t{header.schema_version});
  fprintf(stdout, "  Checksum Type      : %s\n",
          DescribeChecksumType(header.checksum_type).c_str());
  fprintf(stdout, "  Compact Record Type: %s\n",
          DescribeBlogRecordType(header.compact_record_type).c_str());
  fprintf(stdout, "  Escape Sequence    : %s\n",
          Slice(header.escape_sequence, kBlogEscapeSequenceSize)
              .ToString(true /* hex */)
              .c_str());
  if (show_key != DisplayType::kNone) {
    fprintf(stdout,
            "  Note               : Keys are not stored in blog-format blob "
            "files\n");
  }
  PrintBlogProperties("Properties", header.properties);

  const bool should_print_records =
      show_key != DisplayType::kNone || show_blob != DisplayType::kNone ||
      show_uncompressed_blob != DisplayType::kNone;

  BlogDumpSummary summary;
  BlogFileFooterProperties footer_properties;
  BlogFileFooterLocator footer_locator;
  bool has_footer_properties = false;
  bool has_footer_locator = false;
  bool has_footer_raw_blob_bytes = false;
  uint64_t footer_raw_blob_bytes = 0;

  while (true) {
    BlogRecordType record_type = kBlogBlobRecord;
    Slice payload;
    std::string scratch;
    uint64_t record_offset = 0;
    CompressionType record_compression_type = kNoCompression;

    s = blog_reader.ReadRecord(&record_type, &payload, &scratch, &record_offset,
                               &record_compression_type);
    if (s.IsNotFound()) {
      break;
    }
    if (!s.ok()) {
      return s;
    }

    if (record_type == kBlogBlobRecord) {
      std::string uncompressed_payload;
      const bool need_uncompressed_payload =
          show_uncompressed_blob != DisplayType::kNone ||
          (show_summary && record_compression_type != kNoCompression &&
           !has_footer_raw_blob_bytes);

      if (need_uncompressed_payload) {
        Status ds = DecompressBlogBlobForDump(payload, record_compression_type,
                                              decompressor, decompressor_status,
                                              &uncompressed_payload);
        if (!ds.ok()) {
          if (show_uncompressed_blob != DisplayType::kNone) {
            return Status::CopyAppendMessage(
                ds, "; ",
                "while dumping blog blob record at offset " +
                    std::to_string(record_offset));
          }
          ds.PermitUncheckedError();
        }
      }

      if (should_print_records) {
        fprintf(stdout,
                "Read blob record with offset 0x%" PRIx64 " (%" PRIu64 "):\n",
                record_offset, record_offset);
        fprintf(stdout, "  payload size: %" PRIu64 "\n",
                lossless_cast<uint64_t>(payload.size()));
        fprintf(stdout, "  compression : %s\n",
                DescribeCompressionType(record_compression_type).c_str());
        if (show_blob != DisplayType::kNone) {
          fprintf(stdout, "  blob        : ");
          DumpSlice(payload, show_blob);
        }
        if (show_uncompressed_blob != DisplayType::kNone) {
          fprintf(stdout, "  raw blob    : ");
          DumpSlice(Slice(uncompressed_payload), show_uncompressed_blob);
        }
      }

      ++summary.blob_records;
      summary.blob_bytes += lossless_cast<uint64_t>(payload.size());
      if (record_compression_type == kNoCompression) {
        summary.uncompressed_blob_bytes +=
            lossless_cast<uint64_t>(payload.size());
      } else {
        summary.saw_compressed_blob = true;
        if (!uncompressed_payload.empty()) {
          summary.raw_blob_bytes +=
              lossless_cast<uint64_t>(uncompressed_payload.size());
        }
      }
      continue;
    }

    if (record_type == kBlogFooterPropertiesRecord) {
      BlogFileFooterProperties parsed_footer_properties;
      s = parsed_footer_properties.DecodeFrom(payload);
      if (!s.ok()) {
        return s;
      }
      footer_properties = std::move(parsed_footer_properties);
      has_footer_properties = true;
      has_footer_raw_blob_bytes = GetBlogUint64Property(
          footer_properties.properties, "blobUncompressedBytes",
          &footer_raw_blob_bytes);
      continue;
    }

    if (record_type == kBlogFooterLocatorRecord) {
      BlogFileFooterLocator parsed_footer_locator;
      s = parsed_footer_locator.DecodeFrom(payload);
      if (!s.ok()) {
        return s;
      }
      footer_locator = std::move(parsed_footer_locator);
      has_footer_locator = true;
      continue;
    }

    if (IsAuxiliaryBodyRecord(record_type)) {
      ++summary.auxiliary_body_records;
      if (should_print_records) {
        fprintf(stdout,
                "Read auxiliary body record with offset 0x%" PRIx64 " (%" PRIu64
                "):\n",
                record_offset, record_offset);
        fprintf(stdout, "  type        : %s\n",
                DescribeBlogRecordType(record_type).c_str());
        fprintf(stdout, "  payload size: %" PRIu64 "\n",
                lossless_cast<uint64_t>(payload.size()));
      }
    }
  }

  if (!has_footer_properties && !has_footer_locator) {
    fprintf(stdout, "No blog blob footer.\n");
  } else {
    fprintf(stdout, "Blog blob file footer:\n");
    if (has_footer_properties) {
      PrintBlogProperties("Properties", footer_properties.properties);
    }
    if (has_footer_locator) {
      fprintf(stdout, "  Footer locator entries: %" PRIu64 "\n",
              lossless_cast<uint64_t>(footer_locator.entries.size()));
      for (const auto& entry : footer_locator.entries) {
        fprintf(stdout, "    %s : relative offset %" PRIu32 " * 4 bytes\n",
                DescribeBlogRecordType(entry.record_type).c_str(),
                entry.relative_offset_4B);
      }
    }
  }

  if (show_summary) {
    fprintf(stdout, "Summary:\n");
    fprintf(stdout, "  total blob records: %" PRIu64 "\n",
            summary.blob_records);
    fprintf(stdout, "  total blob size: %" PRIu64 "\n", summary.blob_bytes);
    if (summary.saw_compressed_blob) {
      if (has_footer_raw_blob_bytes) {
        fprintf(stdout, "  total raw blob size: %" PRIu64 "\n",
                footer_raw_blob_bytes + summary.uncompressed_blob_bytes);
      } else if (summary.raw_blob_bytes != 0 ||
                 summary.uncompressed_blob_bytes != 0) {
        fprintf(stdout, "  total raw blob size: %" PRIu64 "\n",
                summary.raw_blob_bytes + summary.uncompressed_blob_bytes);
      } else if (!has_decompressor) {
        fprintf(stdout, "  total raw blob size: unavailable (%s)\n",
                decompressor_status.ToString().c_str());
      }
    }
    fprintf(stdout, "  total auxiliary body records: %" PRIu64 "\n",
            summary.auxiliary_body_records);
  }

  return Status::OK();
}

Status BlobDumpTool::DumpBlobLogHeader(uint64_t* offset,
                                       CompressionType* compression) {
  Slice slice;
  Status s = Read(0, BlobLogHeader::kSize, &slice);
  if (!s.ok()) {
    return s;
  }
  BlobLogHeader header;
  s = header.DecodeFrom(slice);
  if (!s.ok()) {
    return s;
  }
  fprintf(stdout, "Blob log header:\n");
  fprintf(stdout, "  Version          : %" PRIu32 "\n", header.version);
  fprintf(stdout, "  Column Family ID : %" PRIu32 "\n",
          header.column_family_id);
  std::string compression_str;
  if (!GetStringFromCompressionType(&compression_str, header.compression)
           .ok()) {
    compression_str = "Unrecongnized compression type (" +
                      std::to_string((int)header.compression) + ")";
  }
  fprintf(stdout, "  Compression      : %s\n", compression_str.c_str());
  fprintf(stdout, "  Expiration range : %s\n",
          GetString(header.expiration_range).c_str());
  *offset = BlobLogHeader::kSize;
  *compression = header.compression;
  return s;
}

Status BlobDumpTool::DumpBlobLogFooter(uint64_t file_size,
                                       uint64_t* footer_offset) {
  auto no_footer = [&]() {
    *footer_offset = file_size;
    fprintf(stdout, "No blob log footer.\n");
    return Status::OK();
  };
  if (file_size < BlobLogHeader::kSize + BlobLogFooter::kSize) {
    return no_footer();
  }
  Slice slice;
  *footer_offset = file_size - BlobLogFooter::kSize;
  Status s = Read(*footer_offset, BlobLogFooter::kSize, &slice);
  if (!s.ok()) {
    return s;
  }
  BlobLogFooter footer;
  s = footer.DecodeFrom(slice);
  if (!s.ok()) {
    return no_footer();
  }
  fprintf(stdout, "Blob log footer:\n");
  fprintf(stdout, "  Blob count       : %" PRIu64 "\n", footer.blob_count);
  fprintf(stdout, "  Expiration Range : %s\n",
          GetString(footer.expiration_range).c_str());
  return s;
}

Status BlobDumpTool::DumpRecord(DisplayType show_key, DisplayType show_blob,
                                DisplayType show_uncompressed_blob,
                                bool /*show_summary*/,
                                CompressionType compression, uint64_t* offset,
                                uint64_t* total_records,
                                uint64_t* total_key_size,
                                uint64_t* total_blob_size,
                                uint64_t* total_uncompressed_blob_size) {
  if (show_key != DisplayType::kNone) {
    fprintf(stdout, "Read record with offset 0x%" PRIx64 " (%" PRIu64 "):\n",
            *offset, *offset);
  }
  Slice slice;
  Status s = Read(*offset, BlobLogRecord::kHeaderSize, &slice);
  if (!s.ok()) {
    return s;
  }
  BlobLogRecord record;
  s = record.DecodeHeaderFrom(slice);
  if (!s.ok()) {
    return s;
  }
  uint64_t key_size = record.key_size;
  uint64_t value_size = record.value_size;
  if (show_key != DisplayType::kNone) {
    fprintf(stdout, "  key size   : %" PRIu64 "\n", key_size);
    fprintf(stdout, "  value size : %" PRIu64 "\n", value_size);
    fprintf(stdout, "  expiration : %" PRIu64 "\n", record.expiration);
  }
  *offset += BlobLogRecord::kHeaderSize;
  s = Read(*offset, static_cast<size_t>(key_size + value_size), &slice);
  if (!s.ok()) {
    return s;
  }
  // Decompress value
  std::string uncompressed_value;
  if (compression == kNoCompression) {
    uncompressed_value.assign(slice.data() + static_cast<size_t>(key_size),
                              static_cast<size_t>(value_size));
  } else if (show_uncompressed_blob != DisplayType::kNone) {
    auto decompressor =
        GetBuiltinV2CompressionManager()->GetDecompressorOptimizeFor(
            compression);
    if (!decompressor) {
      return Status::NotSupported("Unsupported compression type");
    }
    Slice compressed_data(slice.data() + key_size,
                          static_cast<size_t>(value_size));
    Decompressor::Args args;
    args.compression_type = compression;
    args.compressed_data = compressed_data;
    s = decompressor->ExtractUncompressedSize(args);
    if (!s.ok()) {
      return s;
    }
    uncompressed_value.resize(args.uncompressed_size);
    s = decompressor->DecompressBlock(args, uncompressed_value.data());
    if (!s.ok()) {
      return s;
    }
  }
  if (show_key != DisplayType::kNone) {
    fprintf(stdout, "  key        : ");
    DumpSlice(Slice(slice.data(), static_cast<size_t>(key_size)), show_key);
    if (show_blob != DisplayType::kNone) {
      fprintf(stdout, "  blob       : ");
      DumpSlice(Slice(slice.data() + static_cast<size_t>(key_size),
                      static_cast<size_t>(value_size)),
                show_blob);
    }
    if (show_uncompressed_blob != DisplayType::kNone) {
      fprintf(stdout, "  raw blob   : ");
      DumpSlice(Slice(uncompressed_value), show_uncompressed_blob);
    }
  }
  *offset += key_size + value_size;
  *total_records += 1;
  *total_key_size += key_size;
  *total_blob_size += value_size;
  *total_uncompressed_blob_size +=
      lossless_cast<uint64_t>(uncompressed_value.size());
  return s;
}

void BlobDumpTool::DumpSlice(const Slice s, DisplayType type) {
  if (type == DisplayType::kRaw) {
    fprintf(stdout, "%s\n", s.ToString().c_str());
  } else if (type == DisplayType::kHex) {
    fprintf(stdout, "%s\n", s.ToString(true /*hex*/).c_str());
  } else if (type == DisplayType::kDetail) {
    char buf[100];
    for (size_t i = 0; i < s.size(); i += 16) {
      memset(buf, 0, sizeof(buf));
      for (size_t j = 0; j < 16 && i + j < s.size(); j++) {
        unsigned char c = s[i + j];
        snprintf(buf + j * 3 + 15, 2, "%x", c >> 4);
        snprintf(buf + j * 3 + 16, 2, "%x", c & 0xf);
        snprintf(buf + j + 65, 2, "%c", (0x20 <= c && c <= 0x7e) ? c : '.');
      }
      for (size_t p = 0; p + 1 < sizeof(buf); p++) {
        if (buf[p] == 0) {
          buf[p] = ' ';
        }
      }
      fprintf(stdout, "%s\n", i == 0 ? buf + 15 : buf);
    }
  }
}

template <class T>
std::string BlobDumpTool::GetString(std::pair<T, T> p) {
  if (p.first == 0 && p.second == 0) {
    return "nil";
  }
  return "(" + std::to_string(p.first) + ", " + std::to_string(p.second) + ")";
}

}  // namespace ROCKSDB_NAMESPACE::blob_db
