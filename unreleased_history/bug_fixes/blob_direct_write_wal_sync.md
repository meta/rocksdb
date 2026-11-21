Fixed blob direct write values that could be lost after a power loss although their WAL record had been synced by `SyncWAL()`, `FlushWAL(true)`, a sync write, or the WAL sync before a flush.
