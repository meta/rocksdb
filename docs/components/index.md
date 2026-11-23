# RocksDB Components

This directory collects component-specific design notes and implementation
walkthroughs.

## Sections

| Section | Path | Summary |
|---------|------|---------|
| Read Flow | [read_flow/index.md](read_flow/index.md) | Point lookups, MultiGet, iterators, cache integration, range deletions, and read-side tuning. |
| Write Flow | [write_flow/index.md](write_flow/index.md) | Write APIs, write thread, WAL, memtable insertion, sequence numbers, write modes, crash recovery, and performance. |
| Trie MemTableRep | [trie_memtable/index.md](trie_memtable/index.md) | Adaptive radix-trie memtable: key encoding, node layout and fan-out, lazy leaves, key ownership, and the single-writer lock-free concurrency model. |
| IndexFactory / UDI | [index_factory/index.md](index_factory/index.md) | Pluggable custom SST index SPI: index modes, per-read selection, on-disk markers, cache accounting, parallel builds, and migration. |
| Stress Test | [stress_test/index.md](stress_test/index.md) | Stress-test-specific design notes, invariants, and debugging references. |
