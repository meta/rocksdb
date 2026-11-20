# Other Events

**Files:** include/rocksdb/listener.h, db/db_impl/db_impl.cc, db/db_impl/db_impl_write.cc

## OnMemTableSealed

Called before a memtable is made immutable (sealed). This is tied to DBImpl::SwitchMemtable(), which is called from multiple paths:

- Write buffer full: The active memtable reached write_buffer_size
- WriteBufferManager: Global WriteBufferManager triggered flush
- WAL size limit: Total WAL size exceeded max_total_wal_size
- Manual flush: User called DB::Flush()
- WBWI ingestion: Transaction commit as memtable
- Recovery and error-recovery paths

The MemTableInfo struct provides:

| Field | Description |
|-------|-------------|
| cf_name | Column family name |
| first_seqno | Sequence number of the first entry inserted |
| earliest_seqno | Guaranteed lower bound on sequence numbers in this memtable |
| num_entries | Total number of entries |
| num_deletes | Total number of deletes |
| newest_udt | Newest user-defined timestamp (only populated when persist_user_defined_timestamps is false AND the column family's comparator has a non-zero timestamp size) |

Note: earliest_seqno is guaranteed to be less than or equal to the sequence number of any key that could be in this memtable. Any write with a larger or equal sequence number will be in this memtable or a later one.

## OnExternalFileIngested

Called after an external file is ingested via DB::IngestExternalFile(). This callback runs on the same thread as IngestExternalFile() -- if the callback blocks, IngestExternalFile() will block.

Dispatched from DBImpl::IngestExternalFiles() via NotifyOnExternalFileIngested(). The callback fires once per column family in the ingestion request, not once per individual file. For the common single-CF IngestExternalFile() call, this means one callback per API call.

The ExternalFileIngestionInfo struct provides:

| Field | Description |
|-------|-------------|
| cf_name | Column family name |
| external_file_path | Original path of the file outside the DB |
| internal_file_path | Path of the file inside the DB after ingestion |
| global_seqno | The global sequence number assigned to keys in this file |
| table_properties | Properties of the ingested table |

## OnColumnFamilyHandleDeletionStarted

Called before a column family handle is deleted. The handle parameter is a pointer to the ColumnFamilyHandle being deleted; it becomes a dangling pointer after the deletion completes, so it must not be stored or used after the callback returns.

This callback enables cleanup of per-column-family resources maintained outside RocksDB. It does not check the shutting_down_ flag.
