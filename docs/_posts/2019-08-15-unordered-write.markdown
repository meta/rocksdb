---
title: Higher write throughput with `unordered_write` feature
layout: post
author: maysamyabandeh
category: blog
---

Since RocksDB 6.3, The `unordered_write=`true option together with WritePrepared transactions offers 34-42% higher write throughput compared to vanilla RocksDB. If the application can handle more relaxed ordering guarantees, the gain in throughput would increase to 63-131%.

### Background

Currently RocksDB API delivers the following powerful guarantees:
- Atomic reads: Either all of a write batch is visible to reads or none of it.
- Read-your-own writes: When a write thread returns to the user, a subsequent read by the same thread will be able to see its own writes.
- Immutable Snapshots: The reads visible to the snapshot are immutable in the sense that it will not be affected by any in-flight or future writes.

### `unordered_write`

The `unordered_write` feature, when turned on, relaxes the default guarantees of RocksDB. While it still gives read-your-own-write property, neither atomic reads nor the immutable snapshot properties are provided any longer. However, RocksDB users could still get read-your-own-write and immutable snapshots when using this feature in conjunction with TransactionDB configured with WritePrepared transactions and `two_write_queues`. You can read [here](https://github.com/meta/rocksdb/wiki/unordered_write) to learn about the design of `unordered_write` and [here](https://github.com/meta/rocksdb/wiki/WritePrepared-Transactions) to learn more about WritePrepared transactions.

### How to use it?

To get the same guarantees as vanilla RocksdB:

    DBOptions db_options;
    db_options.unordered_write = true;
    db_options.two_write_queues = true;
    DB* db;
    {
      TransactionDBOptions txn_db_options;
      txn_db_options.write_policy = TxnDBWritePolicy::WRITE_PREPARED;
      txn_db_options.skip_concurrency_control = true;
      TransactionDB* txn_db;
      TransactionDB::Open(options, txn_db_options, kDBPath, &txn_db);
      db = txn_db;
    }
    db->Write(...);

To get relaxed guarantees:

    DBOptions db_options;
    db_options.unordered_write = true;
    DB* db;
    DB::Open(db_options, kDBPath, &db);
    db->Write(...);

# Benchmarks

    TEST_TMPDIR=/dev/shm/ ~/db_bench --benchmarks=fillrandom --threads=32 --num=10000000 -max_write_buffer_number=16 --max_background_jobs=64 --batch_size=8 --writes=3000000 -level0_file_num_compaction_trigger=99999 --level0_slowdown_writes_trigger=99999 --level0_stop_writes_trigger=99999 -enable_pipelined_write=false -disable_auto_compactions --transaction_db=true --unordered_write=1 --disable_wal=0

Throughput with `unordered_write`=true and using WritePrepared transaction:
- WAL: +42%
- No-WAL: +34%
Throughput with `unordered_write`=true
- WAL: +63%
- NoWAL: +131%
