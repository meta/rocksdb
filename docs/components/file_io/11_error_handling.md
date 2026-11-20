# Error Handling

**Files:** `include/rocksdb/io_status.h`, `file/writable_file_writer.h`, `file/random_access_file_reader.h`, `file/sequence_file_reader.h`

## IOStatus

`IOStatus` (see `include/rocksdb/io_status.h`) extends `Status` with I/O-specific error information. It adds three attributes beyond the standard error code/subcode:

### Error Attributes

| Attribute | Type | Purpose |
|-----------|------|---------|
| retryable_ | bool | Whether the operation can be retried (e.g., temporary network failure) |
| data_loss_ | bool | Whether unrecoverable data loss occurred |
| scope_ | IOErrorScope | Scope of the error (filesystem, file, or range) |

Note: These attributes are defined in the base Status class (include/rocksdb/status.h), not in IOStatus directly. IOStatus inherits them and provides setter/accessor methods (SetRetryable, GetRetryable, SetDataLoss, GetDataLoss, SetScope, GetScope).

### Error Scope

| Scope | Meaning | Example |
|-------|---------|---------|
| `kIOErrorScopeFileSystem` | Affects entire filesystem | Disk failure, mount point unavailable |
| `kIOErrorScopeFile` | Affects a single file | Corrupted file, permission denied on one file |
| `kIOErrorScopeRange` | Affects a byte range | Single corrupted block |

The scope helps RocksDB determine the blast radius of an error and whether the database can continue operating.

### Common Error Types

| Factory Method | Meaning |
|----------------|---------|
| `IOStatus::IOError()` | Generic I/O error |
| `IOStatus::NoSpace()` | Out of disk space (subcode `kNoSpace`) |
| `IOStatus::PathNotFound()` | File or directory not found (subcode `kPathNotFound`) |
| `IOStatus::IOFenced()` | I/O fenced for distributed systems (subcode `kIOFenced`) |
| `IOStatus::Corruption()` | Data corruption detected |
| `IOStatus::TimedOut()` | Operation timed out |

## Error Handling in Writers

WritableFileWriter uses a sticky error model:

Step 1: First I/O error sets seen_error_ to true via set_seen_error().
Step 2: Subsequent Append, Flush, and Sync operations check seen_error_ first and return an error immediately if set.
Step 3: Close() is an exception: even when seen_error_ is true, it still calls the underlying FSWritableFile::Close() and resets the file handle before returning an error. If the underlying close succeeds, Close() returns an IOError explaining that the file was closed after a prior write error; if the close itself fails, it returns that close error instead.
Step 4: For relaxed-consistency use cases, reset_seen_error() clears the flag to allow continued operation.

In debug builds, `seen_injected_error_` separately tracks errors injected by `FaultInjectionTestFS` for testing purposes.

## Error Handling in Readers

`RandomAccessFileReader` and `SequentialFileReader` use a stateless error model:
- Errors are returned immediately to the caller
- No internal error state is maintained
- Each read operation is independent
- Callers are responsible for handling errors appropriately

## IOStatus Checking

`IOStatus` inherits from `Status`, which supports `ROCKSDB_ASSERT_STATUS_CHECKED` mode. In this mode, failing to check an `IOStatus` before it goes out of scope triggers an assertion. Use `.PermitUncheckedError()` to explicitly document intentionally ignored errors.

## Error Recovery Patterns

### Retryable Errors

When `IOStatus::GetRetryable()` returns true, the operation can potentially succeed on retry. Common retryable scenarios include temporary network failures in distributed file systems and transient resource exhaustion.

### Verify and Reconstruct

When a read returns corrupted data, RocksDB can re-read with `IOOptions::verify_and_reconstruct_read = true`, requesting the file system to reconstruct data from redundant copies. This requires `FileSystem` support for `kVerifyAndReconstructRead`.

### IO Fencing

`IOStatus::IOFenced()` is used in distributed systems where a node's access to storage is revoked (fenced). The database transitions to read-only mode and requires manual intervention.
