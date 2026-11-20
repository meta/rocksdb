# Tailing Iterator

**Files:** db/forward_iterator.h, db/forward_iterator.cc, include/rocksdb/options.h

## Overview

Tailing iterators are a special iterator type that can see data written after the iterator was created. Unlike normal iterators, they do not take a snapshot at creation time, allowing them to iterate over newly inserted records.

Tailing iterators are enabled by setting ReadOptions::tailing = true (see ReadOptions in include/rocksdb/options.h).

## ForwardIterator

When tailing = true, the iterator tree uses ForwardIterator (see db/forward_iterator.h) instead of MergingIterator as the internal iterator under DBIter.

ForwardIterator is optimized for sequential forward reads:
- It maintains separate mutable and immutable internal iterators
- The mutable iterator reads from the current memtable only
- The immutable iterator reads from SST files and immutable memtables
- Both use kMaxSequenceNumber, effectively disabling sequence number filtering at this level (filtering is done by DBIter above)

## Forward-Only Restriction

Tailing iterators only support forward operations:

| Operation | Supported |
|-----------|-----------|
| Seek() | Yes |
| SeekToFirst() | Yes |
| Next() | Yes |
| SeekForPrev() | No (Status::NotSupported) |
| SeekToLast() | No (Status::NotSupported) |
| Prev() | No (Status::NotSupported) |

## State Tracking

ForwardIterator tracks database state changes (memtable flushes, compactions) and invalidates its internal iterators when the state changes. It maintains an interval (prev_key, current_key] covered by the immutable iterator, allowing it to skip re-seeking the immutable iterator when Seek(target) falls within this interval.

## Data Visibility

Tailing iterators have weaker visibility guarantees than normal iterators:

- New data written after iterator creation may or may not be visible
- Seek() or SeekToFirst() can be thought of as creating an implicit snapshot -- data written after the seek may not be seen until the next seek
- There is no guaranteed point-in-time consistency

## DeleteRange Incompatibility

Tailing iterators are not compatible with range tombstones (DeleteRange()). If a range tombstone is encountered in the memtable or L0 files at creation time, or in SST files during iteration, the iterator returns Status::NotSupported.

This limitation exists because the ForwardIterator does not integrate range tombstone processing the way MergingIterator does.
