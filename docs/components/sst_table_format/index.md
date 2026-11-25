# RocksDB SST Table Format

## Overview

RocksDB stores persistent data in Sorted String Table (SST) files. The default block-based table format organizes data into fixed-size blocks with prefix-compressed keys, supports bloom/ribbon filters for fast negative lookups, and provides multiple index formats for efficient seeking. Two alternative formats -- PlainTable (mmap-optimized) and CuckooTable (hash-based point lookups) -- serve specialized use cases.

**Key source files:** `table/format.h`, `table/format.cc`, `include/rocksdb/table.h`, `table/block_based/block_based_table_builder.cc`, `table/block_based/block_based_table_reader.cc`, `table/block_based/block_builder.cc`, `table/block_based/block.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Overview | [01_overview.md](01_overview.md) | Table types (block-based, plain, cuckoo), format versions 2-7, checksum types, context-aware checksums, and key configuration options. |
| 2. Block-Based File Layout | [02_block_based_format.md](02_block_based_format.md) | SST file structure, block types, BlockHandle/IndexValue encoding, footer layout, metaindex block, block trailer, and table open workflow. |
| 3. Data Block Structure | [03_data_blocks.md](03_data_blocks.md) | Entry format with prefix compression, restart points, packed footer, separated KV storage, uniform key detection, and BlockBuilder/Block reader. |
| 4. Block Reading and Caching | [04_block_fetcher.md](04_block_fetcher.md) | BlockFetcher workflow, buffer management, checksum verification, decompression, block retrieval pipeline, MultiGet batching, and prefetch buffer. |
| 5. Table Building Pipeline | [05_table_builder.md](05_table_builder.md) | BlockBasedTableBuilder states, Add/EmitBlock/Finish workflows, compression verification, parallel compression integration, and tail size estimation. |
| 6. Index Block Formats | [06_index_formats.md](06_index_formats.md) | Binary search, hash search, partitioned (two-level), and binary-search-with-first-key index types; key shortening, sequence number stripping, and index reader architecture. |
| 7. Filter Blocks | [07_filter_blocks.md](07_filter_blocks.md) | Bloom and Ribbon filter policies, FilterBitsBuilder/Reader, full filter block builder and reader, whole-key and prefix filtering. |
| 8. Partitioned Index and Filter | [08_partitioned_index_filter.md](08_partitioned_index_filter.md) | Two-level index/filter architecture, partition sizing, build and read workflows, CacheDependencies prefetching, and decoupled vs coupled partitioning. |
| 9. Data Block Hash Index | [09_data_block_hash_index.md](09_data_block_hash_index.md) | In-block hash index for O(1) point lookups, hash table format with 1-byte buckets, collision handling, and integration with DataBlockIter::SeekForGet(). |
| 10. PlainTable Format | [10_plain_table.md](10_plain_table.md) | Mmap-optimized format with sequential key-value records, hash-based prefix index, total-order mode, and limitations (no compression, no checksums). |
| 11. CuckooTable Format | [11_cuckoo_table.md](11_cuckoo_table.md) | Cuckoo hashing for O(1) point lookups, cache-friendly block design, BFS collision resolution, and fixed key/value length requirement. |
| 12. Table Properties | [12_table_properties.md](12_table_properties.md) | Built-in and user-collected properties, TablePropertiesCollector API, property serialization, aggregation, and internal collectors. |
| 13. SstFileWriter | [13_sst_file_writer.md](13_sst_file_writer.md) | External SST file creation for bulk loading via IngestExternalFile(), key ordering requirements, global sequence number assignment, and page cache invalidation. |

## Key Characteristics

- **Block-based format is default**: Data organized into ~4KB blocks with prefix-compressed keys and 5-byte trailers (compression type + checksum)
- **Seven format versions**: Version 2 (minimum) through 7 (latest, default), each adding encoding optimizations or integrity features
- **Multiple checksum algorithms**: CRC32c, xxHash, xxHash64, XXH3 (default); context-aware checksums in format_version >= 6 detect misplaced blocks
- **Four index types**: Binary search (default), hash search, partitioned two-level, and binary search with first key for deferred data block reads
- **Bloom and Ribbon filters**: Probabilistic structures for fast negative lookups; partitioned filters for large SST files; Ribbon saves ~30% space vs Bloom
- **Data block hash index**: Optional 1-byte-per-bucket hash index appended to data blocks for O(1) point lookups (~10% throughput improvement)
- **Separated KV storage**: Optional layout storing keys and values in separate block sections, reducing per-entry overhead
- **Parallel compression**: Multi-threaded block compression during flush/compaction via ring buffer architecture
- **Three table formats**: Block-based (general purpose), PlainTable (mmap/memory), CuckooTable (hash-based point lookups)

## Key Invariants

- Checksum is computed over block payload + compression type byte, verified before decompression (not after)
- For format_version >= 6, block checksums include a context modifier derived from a per-file random base_context_checksum and the block offset
- Keys must be added in sorted order within each SST file; the table builder enforces this
- The footer is always the last 53 bytes of the file, with the magic number at the very end
- Index separator keys are >= last key in one data block and < first key in the next data block
