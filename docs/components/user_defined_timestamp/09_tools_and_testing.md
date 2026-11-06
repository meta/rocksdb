# Tools and Testing

**Files:** `tools/db_bench_tool.cc`, `tools/db_crashtest.py`, `tools/ldb_cmd.cc`, `db_stress_tool/db_stress_common.h`, `db_stress_tool/db_stress_gflags.cc`, `db_stress_tool/db_stress_test_base.cc`, `include/rocksdb/sst_file_writer.h`, `table/sst_file_writer.cc`

## db_bench

`db_bench` supports UDT via the `--user_timestamp_size` flag (see `tools/db_bench_tool.cc`).

| Flag | Description |
|------|-------------|
| `--user_timestamp_size=N` | Number of bytes in a user-defined timestamp (only 0 and 8 are supported; 8 uses uint64_t) |

When `user_timestamp_size` is non-zero, `db_bench` uses a monotonically increasing uint64_t timestamp allocator (`TimestampEmulator`) that assigns a new timestamp to each operation. The comparator is automatically set to `BytewiseComparatorWithU64Ts()`.

Read operations can use either the "latest" timestamp or a random past timestamp, controlled by the `--read_with_latest_user_timestamp` flag. Note that when `read_with_latest_user_timestamp=true`, `TimestampEmulator::GetTimestampForRead()` calls `Allocate()`, which advances the emulated timestamp counter rather than pinning a stable value.

## Stress Test

The crash/stress test supports UDT via:

| Flag | Description |
|------|-------------|
| `--user_timestamp_size=N` | Timestamp size (0 = disabled, 8 = uint64_t) |
| `--persist_user_defined_timestamps` | Whether to persist timestamps in SST files |

The stress test validates UDT correctness under concurrent reads, writes, compactions, flushes, and recovery. However, many feature combinations are disabled when UDT is enabled:

| Disabled with UDT | Reason |
|-------------------|--------|
| Transactions | Not compatible with db_stress UDT paths |
| External file ingestion | Validation limitations |
| `use_multiscan=1` | Forced to 0 by crash test |

When `persist_user_defined_timestamps=false` (memtable-only UDT), additional features are disabled:

| Disabled with memtable-only UDT | Reason |
|--------------------------------|--------|
| Blob files | Compatibility restriction |
| Atomic flush | Option validation rejects this combination |
| Concurrent memtable write | Option validation rejects this combination |
| MultiGet / MultiGetEntity | Not exercised in memtable-only mode |
| Iterator-heavy verification | Verification limitations |

## SstFileWriter

`SstFileWriter` (see `include/rocksdb/sst_file_writer.h`) supports writing SST files with timestamps:

| Method | Description |
|--------|-------------|
| `Put(user_key, timestamp, value)` | Add a Put entry with timestamp |
| `Delete(user_key, timestamp)` | Add a Delete entry with timestamp |
| `DeleteRange(begin_key, end_key, timestamp)` | Add a range deletion with timestamp |

When `persist_user_defined_timestamps=false`, only the minimum timestamp is accepted by `Put` and `Delete`, and the timestamp will not be persisted in the SST file.

The non-timestamp overloads (`Put(key, value)`, `Delete(key)`, `DeleteRange(begin, end)`, `Merge(key, value)`) require that the comparator is **not** timestamp-aware.

## External File Ingestion

For column families with UDT enabled, external SST file ingestion (via `DB::IngestExternalFile()`) has the following limitations:

1. The ingested file's user key range (without timestamp) must not overlap with the database's existing key range
2. When ingesting multiple files, their key ranges must not overlap with each other
3. Ingestion behind mode (`ingest_behind=true`) is not supported
4. When an ingested file contains both point data and range deletions for the same key, the point data overrides the range deletion regardless of which has the higher timestamp

## Test Files

Key test files for UDT functionality:

| File | Coverage |
|------|----------|
| `db/db_with_timestamp_basic_test.cc` | Basic Put/Get/Delete/Iterator with timestamps |
| `db/db_with_timestamp_compaction_test.cc` | Compaction with timestamp GC |
| `db/db_readonly_with_timestamp_test.cc` | Read-only mode with timestamps |
| `util/udt_util_test.cc` | UDT utility functions and recovery handler |
| `utilities/transactions/write_committed_transaction_ts_test.cc` | WriteCommitted transactions with timestamps |
| `table/sst_file_reader_test.cc` | SST file reading with timestamps |
