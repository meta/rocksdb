Fixed a native memory leak in the Java API where `RocksDB.close()` did not release the default `ReadOptions`, leaking one native object per opened DB instance.
