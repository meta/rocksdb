# File I/O Events

**Files:** include/rocksdb/listener.h, file/random_access_file_reader.h, file/random_access_file_reader.cc, file/writable_file_writer.cc, file/sequence_file_reader.cc, db/version_set.cc

## Opt-In Gate: ShouldBeNotifiedOnFileIO

File I/O event callbacks are **disabled by default**. To receive them, override ShouldBeNotifiedOnFileIO() to return true. The default implementation returns false.

The gate is checked in the file reader/writer classes. When constructing RandomAccessFileReader, WritableFileWriter, and SequentialFileReader, a filtered list of listeners that returned true from ShouldBeNotifiedOnFileIO() is stored. Only these listeners receive I/O callbacks.

The check via ShouldNotifyListeners() (see RandomAccessFileReader in file/random_access_file_reader.h) is a simple !listeners_.empty() check on the pre-filtered list, making the hot-path cost near zero when no listeners opt in.

Exception: OnIOError dispatched from VersionSet::Close() for MANIFEST verification failures bypasses this gate entirely. See the MANIFEST Verification section below.

## Available I/O Callbacks

| Callback | Triggered By |
|----------|-------------|
| OnFileReadFinish | Completion of a read operation (random or sequential) |
| OnFileWriteFinish | Completion of a write (append or positioned append) |
| OnFileFlushFinish | Completion of a file-level flush (not memtable flush) |
| OnFileSyncFinish | Completion of a sync or fsync |
| OnFileRangeSyncFinish | Completion of a range sync |
| OnFileTruncateFinish | Completion of a file truncate |
| OnFileCloseFinish | Completion of a file close |

## FileOperationInfo Struct

All I/O callbacks receive a FileOperationInfo struct (see include/rocksdb/listener.h) with:

| Field | Description |
|-------|-------------|
| type | FileOperationType enum (kRead, kWrite, kSync, kClose, etc.) |
| path | File path |
| temperature | File temperature hint (best-effort, not guaranteed) |
| offset | Byte offset of the operation (set for reads, writes, and range syncs) |
| length | Number of bytes (set for reads, writes, and range syncs) |
| duration | Wall-clock duration as std::chrono::nanoseconds |
| start_ts | System clock timestamp at operation start |
| status | Operation result status |

Warning: The offset and length fields are not initialized by the constructor and have indeterminate values for flush, sync, fsync, truncate, and close callbacks. Listeners must not read these fields from those callback types.

Duration is measured using std::chrono::steady_clock for monotonic timing, while start_ts uses std::chrono::system_clock for wall-clock correlation.

## Dispatch Flow by File Wrapper

### RandomAccessFileReader

In RandomAccessFileReader::Read():

Step 1: Record start timestamp via FileOperationInfo::StartNow()
Step 2: Perform the actual filesystem read
Step 3: Record finish timestamp via FileOperationInfo::FinishNow()
Step 4: If ShouldNotifyListeners(), call NotifyOnFileReadFinish() with offset, size, timestamps, and status
Step 5: If the read failed, additionally call NotifyOnIOError() with the IOStatus

RandomAccessFileReader dispatches both OnFileReadFinish and OnIOError. MultiRead batches may trigger multiple OnFileReadFinish callbacks per call.

### SequentialFileReader

SequentialFileReader only dispatches OnFileReadFinish. It does NOT call OnIOError on read failures. The error status is available through the FileOperationInfo::status field in the OnFileReadFinish callback.

### WritableFileWriter

WritableFileWriter dispatches both finish callbacks and OnIOError for write, flush, sync, range sync, truncate, and close operations.

For the Close() path specifically: truncate, fsync, and close finish callbacks are notified with the pre-existing aggregate status, while the underlying operation's failure (if any) is reported via OnIOError with the specific failing IOStatus. Do not rely solely on the finish callback status from Close() to detect individual operation failures.

## OnIOError

Called whenever an I/O operation fails. For wrapper-generated callbacks (RandomAccessFileReader, WritableFileWriter), this requires ShouldBeNotifiedOnFileIO() to return true.

The IOErrorInfo struct (see include/rocksdb/listener.h) provides:

| Field | Description |
|-------|-------------|
| io_status | The IOStatus with error details |
| operation | Which file operation failed (FileOperationType) |
| file_path | Path to the file |
| length | Number of bytes attempted |
| offset | Byte offset of the operation |

OnIOError is called in addition to the corresponding finish callback (e.g., OnFileReadFinish), not instead of it.

## MANIFEST Verification OnIOError

VersionSet::Close() performs MANIFEST file verification (size check and optional content validation controlled by verify_manifest_content_on_close). If verification fails, OnIOError is dispatched with FileOperationType::kVerify.

This dispatch path differs from the file-wrapper callbacks in two ways:
- It iterates directly over the listeners vector without checking ShouldBeNotifiedOnFileIO()
- No corresponding finish callback is emitted; only OnIOError fires

Applications that gate their OnIOError implementation on ShouldBeNotifiedOnFileIO() returning true may still receive these MANIFEST verification errors.

## Performance Considerations

File I/O callbacks are called on the thread performing I/O, which may be a hot read or write path. Implementations should:

- Keep callback execution time minimal
- Avoid blocking operations
- Use lock-free data structures (atomics, thread-local storage) for statistics collection
- Be aware that a single read may trigger multiple callbacks (e.g., multi-read batches in RandomAccessFileReader::MultiRead())
