# IndexFactory / User-Defined Index (UDI)

**EXPERIMENTAL.** The public SPI, the options, and the on-disk marker described
here may change without a compatibility guarantee.

`IndexFactory` is the pluggable index SPI for block-based tables. It lets an
application supply its own index structure for SST files alongside, or instead
of, the built-in index selected by `BlockBasedTableOptions::index_type`.

| Area | Path |
|------|------|
| Public SPI | [`include/rocksdb/index_factory.h`](../../../include/rocksdb/index_factory.h) |
| Backward-compat shim | [`include/rocksdb/user_defined_index.h`](../../../include/rocksdb/user_defined_index.h) |
| Mode / read selection options | [`include/rocksdb/table.h`](../../../include/rocksdb/table.h), [`include/rocksdb/options.h`](../../../include/rocksdb/options.h) |
| Built-in index adapter | `table/block_based/builtin_index_factory.{h,cc}` |
| Reader wrapper | `table/block_based/user_defined_index_wrapper.h` |
| Reference implementation | `utilities/trie_index/` (trie index, registered as `trie_index`) |

## SPI shape

Three cooperating interfaces, mirroring `FilterPolicy`'s factory/builder/reader
split:

- `IndexFactory` -- a `Customizable`; creates builders and readers. Register an
  implementation in an `ObjectLibrary` to make it reachable by name from an
  OPTIONS file.
- `IndexFactoryBuilder` -- receives block boundaries during SST construction
  (`OnKeyAdded`, `AddIndexEntry`), then serializes itself in `Finish()`.
- `IndexFactoryReader` -- parses the serialized block and vends
  `IndexFactoryIterator`s that map a seek target to a data-block handle.

The legacy `UserDefinedIndex*` names remain as aliases, and the legacy
`NewBuilder()` / `NewReader(Slice&)` overloads still compile.

## Modes

`BlockBasedTableOptions::index_mode` is the single control over which index
tiers an SST is written with and which one reads use by default.

| Mode | Standard index written | Custom index written | Default read target | Missing/unusable custom block |
|------|------------------------|----------------------|---------------------|-------------------------------|
| `kStandardOnly` | full | no (factory ignored entirely) | standard | n/a |
| `kStandardDefault` | full | yes | standard | warn, fall back to standard |
| `kStandardRequired` | full | yes | standard | hard error at open |
| `kCustomDefault` | full | yes | custom | hard error at open |
| `kCustomOnly` | stub only | yes | custom | hard error at open |

`ReadOptions::read_index` overrides the target for a single read:

- `kDefault` -- follow the effective mode. In primary/custom-only mode, the
  table factory takes precedence over the legacy
  `ReadOptions::table_index_factory` pointer. In secondary mode that pointer
  selects the custom reader and must match the table factory's name.
- `kBuiltin` -- force the standard index. Fails loudly on a `kCustomOnly` file,
  which has no real standard index to read.
- `kPreferCustom` -- use the custom index when the SST has one, else the
  standard index. Best-effort by design so a migration keeps serving reads.
  If a legacy factory pointer is also supplied, its name must match.
  `kBuiltin` ignores that pointer.

The deprecated `fail_if_no_udi_on_open` and `use_udi_as_primary_index` bools map
onto `kStandardRequired` and `kCustomDefault`. An explicitly set `index_mode`
always wins over them, on every configuration path -- whole-map
`ConfigureOptions()`, single-key `ConfigureOption()` / `SetOptions()`, and the C
API -- so setting `index_mode=kStandardOnly` is a reliable rollback switch even
when a stale bool is left behind.

Old OPTIONS files serialized `kStandardDefault` even when legacy flags selected
another mode. Unmarked input containing that default and legacy flags retains
the old interpretation. New OPTIONS files serialize `index_mode_explicit=true`
so even an explicit default overrides stale flags. Include this marker in a
mixed map or string when the default is intended to be authoritative;
single-key `ConfigureOption()` remains an explicit selection. New files also
serialize equivalent legacy booleans for `kStandardDefault`, `kStandardRequired`,
and `kCustomDefault`, so older binaries preserve routing when loading newer
OPTIONS files with `ignore_unknown_options=true`. The other modes have no
legacy equivalent; custom-only files remain protected by the footer feature bit.

Legacy aliases retain their boolean state across configuration calls. Clearing
an alias recomputes the mode from the remaining enabled aliases unless a mode
was selected explicitly. Explicit modes clear ignored legacy inputs so a copied
options struct preserves the selected behavior when used to construct a new
factory. Legacy-derived options retain `index_mode=kStandardDefault`; use
`GetEffectiveIndexMode()` to inspect the resolved policy. Copying those options
and clearing a legacy bool also clears its policy. For a C++
`BlockBasedTableOptions` struct, the default
`kStandardDefault` value cannot distinguish an explicit choice from an unset
field; configure `index_mode` on the factory to make that choice explicit.

## On-disk representation

- The serialized custom index is a meta block keyed
  `kIndexFactoryMetaPrefix + factory->Name()`, i.e.
  `"rocksdb.user_defined_index.<name>"`. This string is pinned by a test and
  must not change.
- A `kCustomOnly` file sets the footer feature bit
  `kFooterFeatureRequireUserDefinedIndex` and the `standard_index_is_stub` table
  property. The footer bit lives in bytes that every `format_version >= 6`
  reader already required to be zero, so a binary that predates this feature
  rejects such a file with `NotSupported` instead of misreading the stub as an
  empty table. `kCustomOnly` therefore requires `format_version >= 6`, enforced
  in the table builder, in `ValidateOptions()`, and in the footer writer.
- A zero-size custom index block means "no custom index for this SST" and is
  tolerated in every mode except on a stub file. Builders returning OK with
  empty output still produce this representation when a full standard index is
  written. `kCustomOnly` rejects empty output because it has no standard
  fallback.

## Open-time behavior

`BlockBasedTable::Open` fails closed before reading anything when the
configuration cannot serve the file:

- `kCustomDefault` / `kCustomOnly` with no factory -> `InvalidArgument`.
- A stub file opened with no factory, or with `index_mode=kStandardOnly` ->
  `InvalidArgument`.

After that, a custom block that is present but unusable is treated like a
missing one: in `kStandardDefault` the reader logs an ERROR, drops the block,
and serves the file from its standard index. Every other mode propagates the
error. Only `Corruption` / `NotSupported` / `InvalidArgument` qualify for the
fallback; an `IOError` or a block-cache limit is transient or environmental and
propagates so the normal table-cache retry can re-attempt the open rather than
baking a degraded reader into a long-lived `Rep`.

## Factory limitations

Custom factories cannot be combined with user-defined timestamps unless
`kStandardOnly` ignores the factory.

The trie index requires the bytewise comparator. `DB::Open` rejects any other
comparator when the trie factory is configured.

### Trie index formats

`TrieIndexFactory` takes a `format_version` that selects the layout of the
indexes it writes (`user_defined_index_factory={id=trie_index;format_version=2}`
in an options string). Readers handle both formats, whichever one the factory
writes, so a DB may hold SSTs of both.

- **Format 1** (default) is a LOUDS-encoded succinct trie. It stores data-block
  offsets and sizes as 32-bit integers: a block with either field above
  `UINT32_MAX` makes construction return `NotSupported`, including in
  `kStandardDefault`, where trie construction is optional. This can fail
  flushes, compactions, or `SstFileWriter::Finish()`. Total SST size above
  4 GiB alone is not the rejection condition. File-size targets do not split L0
  outputs or `SstFileWriter` outputs; bound those outputs, use format 2, or use
  `kStandardOnly`. Format 1 is host-byte-order and is supported only on
  little-endian platforms.
- **Format 2** is a path-compressed trie over front-coded buckets of
  separators, with block handles in frame-of-reference columns (layout in
  `utilities/trie_index/trie_v2.h`). It is smaller and faster to search, has no
  4 GiB limit, and is portable across byte orders. Released RocksDB versions
  without format 2 fail to open an SST with a format 2 index whenever the trie
  factory is configured, in every index mode. Before downgrading, switch back
  to format 1 and compact.

## Cache and memory accounting

- Custom index blocks use `BlockType::kUserDefinedIndex` and are accounted as
  index blocks everywhere: block-cache hit/miss/insert tickers
  (`BLOCK_CACHE_INDEX_*`), `PerfContext::index_block_read_{byte,count}`, cache
  priority, and `prepopulate_block_cache` warm-up.
- The raw block is charged exactly once. When the table reader owns it
  (`cache_index_and_filter_blocks=false`, the default),
  `BlockBasedTable::ApproximateMemoryUsage()` adds it. When it lives in the
  block cache, the cache charges it and the table reader does not.
  `IndexFactoryReader::ApproximateMemoryUsage()` reports only the reader's own
  allocations, excluding the serialized block. A reader whose estimate includes
  the block can override `MemoryUsageIncludesIndexBlock()` to return true; the
  wrapper then subtracts the block size from that estimate. The trie readers
  report just their auxiliary lookup tables.

## Parallel compression

`CompressionOptions::parallel_threads > 1` works with a custom index. Each
builder votes via `SupportsParallelAddEntry()`; if any builder declines, or
claims support but cannot hand back a prepared entry, the SST falls back to
single-threaded compression with a `ROCKS_LOG_WARN` naming the builder.

Builders that opt in get a two-phase protocol: `PrepareAddEntry()` runs on the
emit thread and only touches its own prepared-entry slot plus the builder's size
estimate; `FinishAddEntry()` runs on the background writer thread, one block at
a time in emit order, and owns the builder's buffered state. The table builder's
ring-buffer CAS provides the ordering and the happens-before edge, and
`Finish()` runs only after every worker thread has been joined.

## Migration and rollback

Recommended path: `kStandardDefault` -> `kStandardRequired` -> `kCustomDefault`
-> `kCustomOnly`, compacting between steps so existing SSTs pick up the new
layout.

Rolling back from `kCustomDefault` or below needs no rewrite: those files
always carry a full standard index, so lowering `index_mode` sends reads back to
it without waiting for compaction. A table reader fixes its routing when it
opens the SST, so a `SetOptions()` change reaches only SSTs opened afterwards.
Reopen the DB to reroute every file.

`kCustomOnly` is effectively a one-way door. Those SSTs have no usable standard
index, so before dropping the factory you must move to `kCustomDefault` and
compact every file, with the factory still attached.

### Gotchas

- **Tooling.** `sst_dump` and `ldb` construct their table reader from options
  that carry no `user_defined_index_factory`, so a `kCustomOnly` file cannot be
  opened by them at all -- `raw`, `scan`, `check`, and `--show_properties` all
  fail identically. `kCustomDefault` files are unaffected because their standard
  index is fully populated.
- **OPTIONS round-trip.** `user_defined_index_factory` is registered as a
  `Customizable` shared pointer, so `LoadLatestOptions()` and a deep
  `TableFactory::GetOptionString()` / `ConfigureFromString()` pair do restore it
  by name, provided the implementation is in the object registry. A *shallow*
  column-family options round-trip
  (`GetStringFromColumnFamilyOptions()` with default `ConfigOptions`) does not
  serialize nested table-factory sub-options at all, so anything rebuilding
  options that way gets a default `BlockBasedTableFactory` with neither
  `index_mode` nor the factory. db_stress's backup/restore path is one such
  caller, which is why the crash test disables `backup_one_in` in the custom
  modes.
- **Checksum verification on `kCustomOnly`.** `VerifyChecksum()` walks data
  blocks through the effective index, which on a stub file is always the custom
  one. Completeness of the sweep therefore depends on the custom index
  enumerating every data-block handle. This is inherent to a mode with no
  standard index.
- **No Java API.** `index_mode`, `read_index`, and
  `user_defined_index_factory` are not exposed through the JNI bindings.

### Rejected combinations

`ValidateOptions()` (and, for callers such as `SstFileWriter` that never reach
it, the table builder) rejects:

- a custom factory together with user-defined timestamps;
- `kCustomDefault` / `kCustomOnly` with `kTwoLevelIndexSearch` or
  `partition_filters`;
- `kCustomOnly` with `format_version < 6`;
- `kCustomDefault` / `kCustomOnly` with no factory.

`TrieIndexFactory::ValidateOptions()` also rejects, at `DB::Open`:

- a comparator other than the bytewise comparator;
- a `format_version` other than 1 or 2;
- format 1 on a big-endian platform.

## Observability

- `SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT` counts SSTs that could not use their
  custom index, whether the reader fell back or the open failed. It is expected
  to be noisy while a factory is being rolled out; the ERROR log lines
  distinguish the cases.
- `PerfContext::index_block_read_byte` / `index_block_read_count` and the
  `BLOCK_CACHE_INDEX_*` tickers include custom index blocks.
- `TableProperties::standard_index_is_stub` and `udi_is_primary_index` are
  informational markers for diagnostics and `sst_dump --show_properties`.

## Testing

| Suite | Coverage |
|-------|----------|
| `table_test` (`UserDefinedIndexTest`, `UserDefinedIndexStressTest`) | mode routing, fail-closed open matrix, on-disk format pinning, memory accounting |
| `builtin_index_factory_test` | the built-in index wrapped behind the SPI |
| `trie_index_test`, `trie_index_db_test` | the reference trie implementation, all index modes, serial vs parallel byte-identity |
| `block_fetcher_test` | custom index blocks counted as index blocks |
| `db_stress` / `db_crashtest.py` | `--use_trie_index`, `--index_mode`, `--read_index`, `--trie_index_format_version` randomized per run |
| `db_bench` | `--use_trie_index`, `--index_mode`, `--read_index`, `--trie_index_format_version` |
