# Configuration and Options

**Files:** `include/rocksdb/table.h`, `include/rocksdb/options.h`, `include/rocksdb/statistics.h`

## Table Options

### user_defined_index_factory

`BlockBasedTableOptions::user_defined_index_factory` (see `include/rocksdb/table.h`)

Type: `std::shared_ptr<UserDefinedIndexFactory>`, default `nullptr`.

When set, RocksDB builds a UDI alongside the internal index during SST file construction. The UDI is stored as a meta block in the SST file.

Note: This option also controls UDI loading at table open time. If absent during `BlockBasedTable::Open()`, no UDI reader is created, and `ReadOptions::table_index_factory` is effectively ignored.

### fail_if_no_udi_on_open

`BlockBasedTableOptions::fail_if_no_udi_on_open` (see `include/rocksdb/table.h`)

Type: `bool`, default `false`.

Controls error handling when a UDI meta block is missing from an SST file being opened:

| Value | Behavior |
|-------|----------|
| `false` | Log a warning and continue using internal index (allows reading old SST files) |
| `true` | Return `Status::Corruption` and fail table open (enforces UDI presence) |

Note: A UDI block handle with `size() == 0` is treated as "no UDI" and skipped regardless of this setting.

## Read Options

### table_index_factory

`ReadOptions::table_index_factory` (see `include/rocksdb/options.h`)

Type: `const UserDefinedIndexFactory*`, default `nullptr`.

When set, RocksDB uses the UDI (if present in the SST file) instead of the internal index for iteration. The factory `Name()` must match the UDI block name in the SST file; otherwise an error iterator is returned.

Important: This option alone is not sufficient. The SST file must have been opened with a matching `BlockBasedTableOptions::user_defined_index_factory` so the `UserDefinedIndexReaderWrapper` exists. If the open-time factory is absent, there is no UDI reader wrapper, and this option is ignored.

Supported operations: SeekToFirst, Seek, Next, Get, MultiGet. Reverse operations (SeekToLast, SeekForPrev, Prev) return `Status::NotSupported`.

## Monitoring

### Statistics

| Ticker | Description |
|--------|-------------|
| `SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT` | Incremented when a UDI meta block is not found during SST table open |

See `include/rocksdb/statistics.h`.

## Configuration Example

```cpp
#include "utilities/trie_index/trie_index_factory.h"

// Build-time: configure UDI factory in table options
auto trie_factory = std::make_shared<trie_index::TrieIndexFactory>();
BlockBasedTableOptions table_options;
table_options.user_defined_index_factory = trie_factory;

Options db_options;
db_options.table_factory.reset(NewBlockBasedTableFactory(table_options));

DB* db;
DB::Open(db_options, "/tmp/testdb", &db);

// Read-time: select UDI per query
ReadOptions ro;
ro.table_index_factory = trie_factory.get();
auto iter = db->NewIterator(ro);
iter->SeekToFirst();
while (iter->Valid()) {
  // Iteration navigates data blocks via the trie index
  iter->Next();
}
delete iter;
delete db;
```

## Migration Strategies

### Adding UDI to an existing database

1. Set `user_defined_index_factory` in table options
2. Leave `fail_if_no_udi_on_open = false` (default)
3. New flushes and compactions build UDI; old SST files without UDI are read using the internal index
4. After full compaction, all SST files have UDI; optionally set `fail_if_no_udi_on_open = true`

### Removing UDI

1. Remove `user_defined_index_factory` from table options
2. Remove `table_index_factory` from read options
3. New flushes and compactions omit UDI; old SST files with UDI blocks are read normally (UDI block is ignored)

### Changing UDI type

Old SST files have UDI blocks with the old factory name. The new factory will not find its expected block name.

Recommended approach:

1. Set the new `user_defined_index_factory` with `fail_if_no_udi_on_open = false`
2. New SST files get the new UDI type; old files fall back to internal index
3. After full compaction, all files have the new type
4. Optionally set `fail_if_no_udi_on_open = true`
