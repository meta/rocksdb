# User Defined Index (UDI)

## Overview

User Defined Index (UDI) is an experimental framework that allows users to plug custom index implementations into RocksDB's block-based table format. UDI indexes are built alongside the internal index during SST file construction and stored as separate meta blocks. At read time, users choose per-query whether to use the UDI or the internal index for forward iteration and point lookups. The internal index is always built and maintained for correctness and backward compatibility.

**Key source files:** `include/rocksdb/user_defined_index.h`, `table/block_based/user_defined_index_wrapper.h`, `table/block_based/block_based_table_builder.cc`, `table/block_based/block_based_table_reader.cc`, `utilities/trie_index/trie_index_factory.h`, `utilities/trie_index/louds_trie.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Public API and Interfaces | [01_public_api.md](01_public_api.md) | `UserDefinedIndexBuilder`, `UserDefinedIndexIterator`, `UserDefinedIndexReader`, and `UserDefinedIndexFactory` interfaces, value type mapping, and separator contract. |
| 2. Integration with Block-Based Table | [02_table_integration.md](02_table_integration.md) | Wrapper classes, write path flow (OnKeyAdded/AddIndexEntry/Finish), read path flow (meta block loading, reader wrapping, iterator routing), and error handling. |
| 3. Configuration and Options | [03_configuration.md](03_configuration.md) | `BlockBasedTableOptions::user_defined_index_factory`, `fail_if_no_udi_on_open`, `ReadOptions::table_index_factory`, monitoring statistics, and migration strategies. |
| 4. Sequence Number Handling | [04_sequence_numbers.md](04_sequence_numbers.md) | Same-user-key block boundaries, `IndexEntryContext`, `SeekContext`, the all-or-nothing seqno encoding strategy, and overflow block runs. |
| 5. Trie Index Implementation | [05_trie_index.md](05_trie_index.md) | `TrieIndexFactory`, LOUDS-encoded Fast Succinct Trie, hybrid dense/sparse encoding, path compression, and serialization format. |
| 6. Trie Seek and Iteration | [06_trie_seek_iteration.md](06_trie_seek_iteration.md) | `LoudsTrieIterator` seek and next algorithms, dense/sparse traversal, prefix key handling, overflow run iteration, and bounds checking. |
| 7. Compaction and Lifecycle | [07_compaction_lifecycle.md](07_compaction_lifecycle.md) | Index rebuilding during compaction, configuration evolution, adding/removing/changing UDI types, and interaction with flush. |
| 8. Limitations and Performance | [08_limitations_performance.md](08_limitations_performance.md) | Parallel compression incompatibility, forward-only iteration, monolithic block, memory overhead, space efficiency tradeoffs, and benchmarking guidance. |
| 9. Stress Test and db_bench | [09_stress_test_db_bench.md](09_stress_test_db_bench.md) | `--use_trie_index` flag in db_bench and db_stress, stress test integration, and validation coverage. |

## Key Characteristics

- **Supplemental index:** UDI complements the internal index; both are always built for backward compatibility
- **Custom encoding:** Implementations control their own serialization format (tries, hash tables, compressed bitmaps)
- **Read-time selection:** Per-query choice via `ReadOptions::table_index_factory`
- **Forward-only:** SeekToFirst, Seek, Next, Get supported; reverse iteration returns `NotSupported`
- **Meta block storage:** Stored as `"rocksdb.user_defined_index.<factory_name>"` with `BlockType::kUserDefinedIndex`
- **Comparator-aware:** Framework passes comparator via `UserDefinedIndexOption`; trie implementation requires `BytewiseComparator`
- **Parallel compression incompatible:** UDI disables parallel compression (wrapper lacks `PrepareIndexEntry`/`FinishIndexEntry` split)
- **Monolithic block:** UDI block is a single meta block (no partitioning), though it works atop partitioned internal indexes

## Key Invariants

- Keys passed to `UserDefinedIndexBuilder` are user keys (internal key trailers stripped by wrapper)
- Separator returned by `AddIndexEntry` must satisfy: `last_key_in_current_block <= separator < first_key_in_next_block`
- Memory backing `Finish()` output must remain valid until builder destruction
- UDI blocks receive standard SST block checksums; implementations may add internal checksums
