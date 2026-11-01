//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_fetcher.h"

#include "db/table_properties_collector.h"
#include "file/file_util.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/perf_level.h"
#include "table/block_based/binary_search_index_reader.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_cache.h"
#include "table/format.h"
#include "test_util/testharness.h"
#include "util/defer.h"
#include "utilities/memory_allocators.h"

namespace ROCKSDB_NAMESPACE {
namespace {
struct MemcpyStats {
  int num_stack_buf_memcpy;
  int num_heap_buf_memcpy;
  int num_compressed_buf_memcpy;
};

struct BufAllocationStats {
  int num_heap_buf_allocations;
  int num_compressed_buf_allocations;
};

struct TestStats {
  MemcpyStats memcpy_stats;
  BufAllocationStats buf_allocation_stats;
};

class BlockFetcherTest : public testing::Test {
 public:
  enum class Mode {
    kBufferedRead = 0,
    kBufferedMmap,
    kDirectRead,
    kNumModes,
  };
  // use NumModes as array size to avoid "size of array '...' has non-integral
  // type" errors.
  const static int NumModes = static_cast<int>(Mode::kNumModes);

 protected:
  void SetUp() override {
    SetupSyncPointsToMockDirectIO();
    test_dir_ = test::PerThreadDBPath("block_fetcher_test");
    env_ = Env::Default();
    fs_ = FileSystem::Default();
    ASSERT_OK(fs_->CreateDir(test_dir_, IOOptions(), nullptr));
  }

  void TearDown() override { EXPECT_OK(DestroyDir(env_, test_dir_)); }

  void AssertSameBlock(const std::string& block1, const std::string& block2) {
    ASSERT_EQ(block1, block2);
  }

  // Creates a table with kv pairs (i, i) where i ranges from 0 to 9, inclusive.
  void CreateTable(const std::string& table_name,
                   const CompressionType& compression_type) {
    std::unique_ptr<WritableFileWriter> writer;
    NewFileWriter(table_name, &writer);

    // Create table builder.
    ImmutableOptions ioptions(options_);
    InternalKeyComparator comparator(options_.comparator);
    ColumnFamilyOptions cf_options(options_);
    MutableCFOptions moptions(cf_options);
    InternalTblPropCollFactories factories;
    const ReadOptions read_options;
    const WriteOptions write_options;
    std::unique_ptr<TableBuilder> table_builder(table_factory_.NewTableBuilder(
        TableBuilderOptions(ioptions, moptions, read_options, write_options,
                            comparator, &factories, compression_type,
                            CompressionOptions(), 0 /* column_family_id */,
                            kDefaultColumnFamilyName, -1 /* level */,
                            kUnknownNewestKeyTime),
        writer.get()));

    // Build table.
    for (int i = 0; i < 9; i++) {
      std::string key = ToInternalKey(std::to_string(i));
      // Append "00000000" to string value to enhance compression ratio
      std::string value = "00000000" + std::to_string(i);
      table_builder->Add(key, value);
    }
    ASSERT_OK(table_builder->Finish());
  }

  void FetchIndexBlock(const std::string& table_name,
                       CountedMemoryAllocator* heap_buf_allocator,
                       CountedMemoryAllocator* compressed_buf_allocator,
                       MemcpyStats* memcpy_stats, BlockContents* index_block,
                       std::string* result,
                       BlockType block_type = BlockType::kIndex) {
    FileOptions fopt(options_);
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(table_name, fopt, &file);

    BlockHandle index_handle;
    FindIndexBlockHandle(file.get(), &index_handle);

    CompressionType compression_type;
    FetchBlock(file.get(), index_handle, block_type, false /* compressed */,
               false /* do_uncompress */, heap_buf_allocator,
               compressed_buf_allocator, index_block, memcpy_stats,
               &compression_type);
    ASSERT_EQ(compression_type, CompressionType::kNoCompression);
    result->assign(index_block->data.ToString());
  }

  void FindIndexBlockHandle(RandomAccessFileReader* file,
                            BlockHandle* index_handle) {
    Footer footer;
    uint64_t file_size = 0;
    ReadFooter(file, &footer, &file_size);

    // Index handle comes from metaindex for format_version >= 6
    ASSERT_TRUE(footer.index_handle().IsNull());

    ASSERT_OK(FindMetaBlockInFile(file, file_size, kBlockBasedTableMagicNumber,
                                  ImmutableOptions(options_), {},
                                  kIndexBlockName, index_handle));
  }

  // Fetches the first data block in both direct IO and non-direct IO mode.
  //
  // compressed: whether the data blocks are compressed;
  // do_uncompress: whether the data blocks should be uncompressed on fetching.
  // compression_type: the expected compression type.
  //
  // Expects:
  // Block contents are the same.
  // Bufferr allocation and memory copy statistics are expected.
  void TestFetchDataBlock(
      const std::string& table_name_prefix, bool compressed, bool do_uncompress,
      std::array<TestStats, NumModes> expected_stats_by_mode) {
    for (CompressionType compression_type : GetSupportedCompressions()) {
      bool do_compress = compression_type != kNoCompression;
      if (compressed != do_compress) {
        continue;
      }
      std::string compression_type_str =
          CompressionTypeToString(compression_type);

      std::string table_name = table_name_prefix + compression_type_str;
      CreateTable(table_name, compression_type);

      CompressionType expected_compression_type_after_fetch =
          (compressed && !do_uncompress) ? compression_type : kNoCompression;

      BlockContents blocks[NumModes];
      std::string block_datas[NumModes];
      MemcpyStats memcpy_stats[NumModes];
      CountedMemoryAllocator heap_buf_allocators[NumModes];
      CountedMemoryAllocator compressed_buf_allocators[NumModes];
      for (int i = 0; i < NumModes; ++i) {
        SetMode(static_cast<Mode>(i));
        FetchFirstDataBlock(table_name, compressed, do_uncompress,
                            expected_compression_type_after_fetch,
                            &heap_buf_allocators[i],
                            &compressed_buf_allocators[i], &blocks[i],
                            &block_datas[i], &memcpy_stats[i]);
      }

      for (int i = 0; i < NumModes - 1; ++i) {
        AssertSameBlock(block_datas[i], block_datas[i + 1]);
      }

      // Check memcpy and buffer allocation statistics.
      for (int i = 0; i < NumModes; ++i) {
        const TestStats& expected_stats = expected_stats_by_mode[i];

        ASSERT_EQ(memcpy_stats[i].num_stack_buf_memcpy,
                  expected_stats.memcpy_stats.num_stack_buf_memcpy);
        ASSERT_EQ(memcpy_stats[i].num_heap_buf_memcpy,
                  expected_stats.memcpy_stats.num_heap_buf_memcpy);
        ASSERT_EQ(memcpy_stats[i].num_compressed_buf_memcpy,
                  expected_stats.memcpy_stats.num_compressed_buf_memcpy);

        if (kXpressCompression == compression_type) {
          // XPRESS allocates memory internally, thus does not support for
          // custom allocator verification
          continue;
        } else {
          ASSERT_EQ(
              heap_buf_allocators[i].GetNumAllocations(),
              expected_stats.buf_allocation_stats.num_heap_buf_allocations);
          ASSERT_EQ(compressed_buf_allocators[i].GetNumAllocations(),
                    expected_stats.buf_allocation_stats
                        .num_compressed_buf_allocations);

          // The allocated buffers are not deallocated until
          // the block content is deleted.
          ASSERT_EQ(heap_buf_allocators[i].GetNumDeallocations(), 0);
          ASSERT_EQ(compressed_buf_allocators[i].GetNumDeallocations(), 0);
          blocks[i].allocation.reset();
          ASSERT_EQ(
              heap_buf_allocators[i].GetNumDeallocations(),
              expected_stats.buf_allocation_stats.num_heap_buf_allocations);
          ASSERT_EQ(compressed_buf_allocators[i].GetNumDeallocations(),
                    expected_stats.buf_allocation_stats
                        .num_compressed_buf_allocations);
        }
      }
    }
  }

  void SetMode(Mode mode) {
    switch (mode) {
      case Mode::kBufferedRead:
        options_.use_direct_reads = false;
        options_.allow_mmap_reads = false;
        break;
      case Mode::kBufferedMmap:
        options_.use_direct_reads = false;
        options_.allow_mmap_reads = true;
        break;
      case Mode::kDirectRead:
        options_.use_direct_reads = true;
        options_.allow_mmap_reads = false;
        break;
      case Mode::kNumModes:
        assert(false);
    }
  }

  void VerifyUserDefinedIndexReadCountsAsIndexBlock(
      const std::string& table_name) {
    CountedMemoryAllocator allocator;
    MemcpyStats memcpy_stats{};
    BlockContents index_block;
    SetMode(Mode::kBufferedRead);

    FileOptions fopt(options_);
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(table_name, fopt, &file);
    BlockHandle index_handle;
    FindIndexBlockHandle(file.get(), &index_handle);

    SetPerfLevel(PerfLevel::kEnableCount);
    Defer reset_perf_level([] { SetPerfLevel(PerfLevel::kDisable); });
    get_perf_context()->Reset();

    CompressionType compression_type;
    FetchBlock(file.get(), index_handle, BlockType::kUserDefinedIndex,
               false /* compressed */, false /* do_uncompress */, &allocator,
               &allocator, &index_block, &memcpy_stats, &compression_type);
    ASSERT_EQ(compression_type, CompressionType::kNoCompression);

    ASSERT_EQ(get_perf_context()->block_read_count, 1);
    ASSERT_EQ(get_perf_context()->index_block_read_count, 1);
    ASSERT_EQ(get_perf_context()->filter_block_read_count, 0);
    ASSERT_EQ(get_perf_context()->compression_dict_block_read_count, 0);
    ASSERT_GT(get_perf_context()->index_block_read_byte, 0);
    ASSERT_EQ(get_perf_context()->block_read_byte,
              get_perf_context()->index_block_read_byte);
    ASSERT_EQ(get_perf_context()->metadata_block_read_byte, 0);
  }

 private:
  std::string test_dir_;
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  BlockBasedTableFactory table_factory_;
  Options options_;

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }

  void NewFileWriter(const std::string& filename,
                     std::unique_ptr<WritableFileWriter>* writer) {
    std::string path = Path(filename);
    FileOptions file_options;
    ASSERT_OK(WritableFileWriter::Create(env_->GetFileSystem(), path,
                                         file_options, writer, nullptr));
  }

  void NewFileReader(const std::string& filename, const FileOptions& opt,
                     std::unique_ptr<RandomAccessFileReader>* reader) {
    std::string path = Path(filename);
    std::unique_ptr<FSRandomAccessFile> f;
    ASSERT_OK(fs_->NewRandomAccessFile(path, opt, &f, nullptr));
    reader->reset(new RandomAccessFileReader(std::move(f), path,
                                             env_->GetSystemClock().get()));
  }

  void NewTableReader(const ImmutableOptions& ioptions,
                      const FileOptions& foptions,
                      const InternalKeyComparator& comparator,
                      const std::string& table_name,
                      std::unique_ptr<BlockBasedTable>* table) {
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(table_name, foptions, &file);

    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(Path(table_name), &file_size));

    std::unique_ptr<TableReader> table_reader;
    ReadOptions ro;
    const auto* table_options =
        table_factory_.GetOptions<BlockBasedTableOptions>();
    ASSERT_NE(table_options, nullptr);
    ASSERT_OK(BlockBasedTable::Open(ro, ioptions, EnvOptions(), *table_options,
                                    comparator, std::move(file), file_size,
                                    0 /* block_protection_bytes_per_key */,
                                    &table_reader, 0 /* tail_size */));

    table->reset(static_cast<BlockBasedTable*>(table_reader.release()));
  }

  std::string ToInternalKey(const std::string& key) {
    InternalKey internal_key(key, 0, ValueType::kTypeValue);
    return internal_key.Encode().ToString();
  }

  void ReadFooter(RandomAccessFileReader* file, Footer* footer,
                  uint64_t* file_size_out = nullptr) {
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file->file_name(), &file_size));
    IOOptions opts;
    ASSERT_OK(ReadFooterFromFile(opts, file, *fs_,
                                 nullptr /* prefetch_buffer */, file_size,
                                 footer, kBlockBasedTableMagicNumber));
    if (file_size_out) {
      *file_size_out = file_size;
    }
  }

  // NOTE: compression_type returns the compression type of the fetched block
  // contents, so if the block is fetched and uncompressed, then it's
  // kNoCompression.
  void FetchBlock(RandomAccessFileReader* file, const BlockHandle& block,
                  BlockType block_type, bool compressed, bool do_uncompress,
                  MemoryAllocator* heap_buf_allocator,
                  MemoryAllocator* compressed_buf_allocator,
                  BlockContents* contents, MemcpyStats* stats,
                  CompressionType* compression_type) {
    ImmutableOptions ioptions(options_);
    ReadOptions roptions;
    PersistentCacheOptions persistent_cache_options;
    Footer footer;
    ReadFooter(file, &footer);
    auto mgr = GetBuiltinV2CompressionManager();
    std::unique_ptr<BlockFetcher> fetcher(new BlockFetcher(
        file, nullptr /* prefetch_buffer */, footer, roptions, block, contents,
        ioptions, do_uncompress, compressed, block_type,
        mgr->GetDecompressor().get(), persistent_cache_options,
        heap_buf_allocator, compressed_buf_allocator));

    ASSERT_OK(fetcher->ReadBlockContents());

    stats->num_stack_buf_memcpy = fetcher->TEST_GetNumStackBufMemcpy();
    stats->num_heap_buf_memcpy = fetcher->TEST_GetNumHeapBufMemcpy();
    stats->num_compressed_buf_memcpy =
        fetcher->TEST_GetNumCompressedBufMemcpy();

    if (do_uncompress) {
      *compression_type = kNoCompression;
    } else {
      *compression_type = fetcher->compression_type();
    }
  }

  // NOTE: expected_compression_type is the expected compression
  // type of the fetched block content, if the block is uncompressed,
  // then the expected compression type is kNoCompression.
  void FetchFirstDataBlock(const std::string& table_name, bool compressed,
                           bool do_uncompress,
                           CompressionType expected_compression_type,
                           MemoryAllocator* heap_buf_allocator,
                           MemoryAllocator* compressed_buf_allocator,
                           BlockContents* block, std::string* result,
                           MemcpyStats* memcpy_stats) {
    ImmutableOptions ioptions(options_);
    InternalKeyComparator comparator(options_.comparator);
    FileOptions foptions(options_);

    // Get block handle for the first data block.
    std::unique_ptr<BlockBasedTable> table;
    NewTableReader(ioptions, foptions, comparator, table_name, &table);

    std::unique_ptr<BlockBasedTable::IndexReader> index_reader;
    ReadOptions ro;
    ASSERT_OK(BinarySearchIndexReader::Create(
        table.get(), ro, nullptr /* prefetch_buffer */, false /* use_cache */,
        false /* prefetch */, false /* pin */, nullptr /* lookup_context */,
        &index_reader));

    std::unique_ptr<InternalIteratorBase<IndexValue>> iter(
        index_reader->NewIterator(
            ReadOptions(), false /* disable_prefix_seek */, nullptr /* iter */,
            nullptr /* get_context */, nullptr /* lookup_context */));
    ASSERT_OK(iter->status());
    iter->SeekToFirst();
    BlockHandle first_block_handle = iter->value().handle;

    // Fetch first data block.
    std::unique_ptr<RandomAccessFileReader> file;
    NewFileReader(table_name, foptions, &file);
    CompressionType compression_type;
    FetchBlock(file.get(), first_block_handle, BlockType::kData, compressed,
               do_uncompress, heap_buf_allocator, compressed_buf_allocator,
               block, memcpy_stats, &compression_type);
    ASSERT_EQ(compression_type, expected_compression_type);
    result->assign(block->data.ToString());
  }
};

// Skip the following tests in lite mode since direct I/O is unsupported.

// Fetch index block under both direct IO and non-direct IO.
// Expects:
// the index block contents are the same for both read modes.
TEST_F(BlockFetcherTest, FetchIndexBlock) {
  for (CompressionType compression : GetSupportedCompressions()) {
    std::string table_name =
        "FetchIndexBlock" + CompressionTypeToString(compression);
    CreateTable(table_name, compression);

    CountedMemoryAllocator allocator;
    MemcpyStats memcpy_stats;
    BlockContents indexes[NumModes];
    std::string index_datas[NumModes];
    for (int i = 0; i < NumModes; ++i) {
      SetMode(static_cast<Mode>(i));
      FetchIndexBlock(table_name, &allocator, &allocator, &memcpy_stats,
                      &indexes[i], &index_datas[i]);
    }
    for (int i = 0; i < NumModes - 1; ++i) {
      AssertSameBlock(index_datas[i], index_datas[i + 1]);
    }
  }
}

TEST_F(BlockFetcherTest, UserDefinedIndexUsesIndexCacheHelpers) {
  ASSERT_NE(nullptr, GetCacheItemHelper(BlockType::kUserDefinedIndex,
                                        CacheTier::kVolatileTier));
  ASSERT_NE(nullptr, GetCacheItemHelper(BlockType::kUserDefinedIndex,
                                        CacheTier::kNonVolatileBlockTier));
}

TEST_F(BlockFetcherTest, UserDefinedIndexReadCountsAsIndexBlock) {
  std::string table_name = "UserDefinedIndexReadCountsAsIndexBlock";
  CreateTable(table_name, kNoCompression);

  VerifyUserDefinedIndexReadCountsAsIndexBlock(table_name);
}

// Data blocks are not compressed,
// fetch data block under direct IO, mmap IO,and non-direct IO.
// Expects:
// 1. in non-direct IO mode, allocate a heap buffer and memcpy the block
//    into the buffer;
// 2. in direct IO mode, allocate a heap buffer and memcpy from the
//    direct IO buffer to the heap buffer.
TEST_F(BlockFetcherTest, FetchUncompressedDataBlock) {
  TestStats expected_non_mmap_stats = {
      {
          0 /* num_stack_buf_memcpy */,
          1 /* num_heap_buf_memcpy */,
          0 /* num_compressed_buf_memcpy */,
      },
      {
          1 /* num_heap_buf_allocations */,
          0 /* num_compressed_buf_allocations */,
      }};
  TestStats expected_mmap_stats = {{
                                       0 /* num_stack_buf_memcpy */,
                                       0 /* num_heap_buf_memcpy */,
                                       0 /* num_compressed_buf_memcpy */,
                                   },
                                   {
                                       0 /* num_heap_buf_allocations */,
                                       0 /* num_compressed_buf_allocations */,
                                   }};
  std::array<TestStats, NumModes> expected_stats_by_mode{{
      expected_non_mmap_stats /* kBufferedRead */,
      expected_mmap_stats /* kBufferedMmap */,
      expected_non_mmap_stats /* kDirectRead */,
  }};
  TestFetchDataBlock("FetchUncompressedDataBlock", false, false,
                     expected_stats_by_mode);
}

// Data blocks are compressed,
// fetch data block under both direct IO and non-direct IO,
// but do not uncompress.
// Expects:
// 1. in non-direct IO mode, allocate a compressed buffer and memcpy the block
//    into the buffer;
// 2. in direct IO mode, allocate a compressed buffer and memcpy from the
//    direct IO buffer to the compressed buffer.
TEST_F(BlockFetcherTest, FetchCompressedDataBlock) {
  TestStats expected_non_mmap_stats = {
      {
          0 /* num_stack_buf_memcpy */,
          0 /* num_heap_buf_memcpy */,
          1 /* num_compressed_buf_memcpy */,
      },
      {
          0 /* num_heap_buf_allocations */,
          1 /* num_compressed_buf_allocations */,
      }};
  TestStats expected_mmap_stats = {{
                                       0 /* num_stack_buf_memcpy */,
                                       0 /* num_heap_buf_memcpy */,
                                       0 /* num_compressed_buf_memcpy */,
                                   },
                                   {
                                       0 /* num_heap_buf_allocations */,
                                       0 /* num_compressed_buf_allocations */,
                                   }};
  std::array<TestStats, NumModes> expected_stats_by_mode{{
      expected_non_mmap_stats /* kBufferedRead */,
      expected_mmap_stats /* kBufferedMmap */,
      expected_non_mmap_stats /* kDirectRead */,
  }};
  TestFetchDataBlock("FetchCompressedDataBlock", true, false,
                     expected_stats_by_mode);
}

// Data blocks are compressed,
// fetch and uncompress data block under both direct IO and non-direct IO.
// Expects:
// 1. in non-direct IO mode, since the block is small, so it's first memcpyed
//    to the stack buffer, then a heap buffer is allocated and the block is
//    uncompressed into the heap.
// 2. in direct IO mode mode, allocate a heap buffer, then directly uncompress
//    and memcpy from the direct IO buffer to the heap buffer.
TEST_F(BlockFetcherTest, FetchAndUncompressCompressedDataBlock) {
  TestStats expected_buffered_read_stats = {
      {
          1 /* num_stack_buf_memcpy */,
          1 /* num_heap_buf_memcpy */,
          0 /* num_compressed_buf_memcpy */,
      },
      {
          1 /* num_heap_buf_allocations */,
          0 /* num_compressed_buf_allocations */,
      }};
  TestStats expected_mmap_stats = {{
                                       0 /* num_stack_buf_memcpy */,
                                       1 /* num_heap_buf_memcpy */,
                                       0 /* num_compressed_buf_memcpy */,
                                   },
                                   {
                                       1 /* num_heap_buf_allocations */,
                                       0 /* num_compressed_buf_allocations */,
                                   }};
  TestStats expected_direct_read_stats = {
      {
          0 /* num_stack_buf_memcpy */,
          1 /* num_heap_buf_memcpy */,
          0 /* num_compressed_buf_memcpy */,
      },
      {
          1 /* num_heap_buf_allocations */,
          0 /* num_compressed_buf_allocations */,
      }};
  std::array<TestStats, NumModes> expected_stats_by_mode{{
      expected_buffered_read_stats,
      expected_mmap_stats,
      expected_direct_read_stats,
  }};
  TestFetchDataBlock("FetchAndUncompressCompressedDataBlock", true, true,
                     expected_stats_by_mode);
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
