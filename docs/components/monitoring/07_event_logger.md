# Event Logger

**Files:** `logging/event_logger.h`, `logging/event_logger.cc`

## Overview

`EventLogger` logs important database events as structured JSON to the LOG file. Events include flush, compaction, table file creation, and recovery. The output is designed for machine parsing and post-mortem analysis.

## Log Format

Each event is written as a single log line with the prefix `EVENT_LOG_v1` followed by a JSON object:

```
2015/01/15-14:13:25.788019 1105ef000 EVENT_LOG_v1 {"time_micros": 1421360005788015, "event": "table_file_creation", "file_number": 12, "file_size": 1909699}
```

Every JSON object includes a `time_micros` field (Unix microsecond timestamp) and an `event` field identifying the event type.

## Event Types

**Flush events:**
- `flush_started` -- memtable flush begins. Includes `num_memtables`, `total_num_input_entries`, `num_deletes`, `total_data_size`, `memory_usage`, `flush_reason`.
- `flush_finished` -- flush completes. Includes `output_compression`, `lsm_state` (array of file counts per level), `immutable_memtables`.

**Compaction events:**
- `compaction_started` -- compaction begins. Includes `cf_name`, `compaction_reason`, input file lists per level, `score`, `input_data_size`, `oldest_snapshot_seqno`.
- `compaction_finished` -- compaction completes. Includes `compaction_time_micros`, `compaction_time_cpu_micros`, `output_level`, `num_output_files`, `total_output_size`, `num_input_records`, `num_output_records`, `num_subcompactions`, `output_compression`, `lsm_state`.

**Table file events:**
- `table_file_creation` -- SST file created. Includes `file_number`, `file_size`.
- `table_file_deletion` -- SST file deleted.

**Recovery events:**
- `recovery_started` -- WAL replay begins. Includes `wal_files`.
- `recovery_finished` -- recovery completes successfully. Includes `status`.
- `recovery_failed` -- recovery fails. Includes error status.

## JSONWriter

`JSONWriter` (see `logging/event_logger.h`) provides a streaming JSON builder with key-value pairs, arrays, and nested objects. It enforces a key-value alternation protocol via internal state (`kExpectKey`, `kExpectValue`, `kInArray`).

`EventLoggerStream` wraps `JSONWriter` and automatically:
1. Lazily creates the writer on first use
2. Adds the `time_micros` field
3. Flushes the JSON string to the `Logger` on destruction

## Writing to EventLogger

EventLogger writes can go directly to the `Logger` or to a `LogBuffer` for deferred output:
- `EventLogger::Log()` -- writes immediately to the LOG file
- `EventLogger::LogToBuffer(log_buffer)` -- buffers the entry for later flushing (used in critical sections where I/O should be deferred)

## Parsing Events

Extract and parse events from the LOG file:

```bash
grep 'EVENT_LOG_v1' LOG | sed 's/.*EVENT_LOG_v1 //'
```

Each extracted line is a valid JSON object that can be processed with standard JSON tools.

**Note:** Background errors are not surfaced through EventLogger. They are exposed via `EventListener::OnBackgroundError()` callbacks (see `include/rocksdb/listener.h`).

**Note:** EventLogger logs at `INFO_LEVEL`. If `DBOptions::info_log_level` is set to `WARN_LEVEL` or higher, EventLogger events will be filtered out.
