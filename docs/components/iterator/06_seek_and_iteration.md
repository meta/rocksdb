# Seek and Iteration Semantics

**Files:** db/db_iter.cc, db/db_iter.h, include/rocksdb/iterator_base.h, include/rocksdb/iterator.h

## Core Operations

| Operation | Behavior |
|-----------|----------|
| Seek(target) | Position at the first user key >= target |
| SeekForPrev(target) | Position at the last user key <= target |
| SeekToFirst() | Position at the smallest user key (or lower bound if set) |
| SeekToLast() | Position at the largest user key (or before upper bound if set) |
| Next() | Advance to the next user key in forward order |
| Prev() | Move to the previous user key in reverse order |

Note: target does not include the user-defined timestamp even when the timestamp feature is enabled.

## Valid/Status Contract

The Valid() and status() methods follow a strict contract:

- If Valid() == true, then status().ok() is guaranteed
- If Valid() == false, the caller must check status():
  - status().ok(): End of data reached normally
  - status() is non-OK: An error occurred (corruption, I/O failure, etc.)
- Seek() and SeekForPrev() clear any previous error status
- Next() and Prev() require Valid() == true and status().ok() as preconditions. Calling them on an invalid iterator or with non-OK status is undefined behavior (debug-mode assertion failure).

## Seek Flow in DBIter

When DBIter::Seek(target) is called:

Step 1: Construct the internal seek key by appending the snapshot sequence number and kValueTypeForSeek to the target user key (via SetSavedKeyToSeekTarget())

Step 2: Call iter_.Seek() on the internal MergingIterator

Step 3: Set direction to kForward

Step 4: Call FindNextUserEntry() to find the first visible user key at or after the target

Step 5: If prefix_same_as_start is set, record the prefix of the seek key for future Next() calls to check against

## SeekForPrev Flow in DBIter

DBIter::SeekForPrev(target):

Step 1: Construct the internal seek key with sequence number 0 and kValueTypeForSeekForPrev (via SetSavedKeyToSeekForPrevTarget()). Using seq=0 ensures the seek target is the smallest possible internal key for the target user key. If the target is at or above iterate_upper_bound, the key is clamped to (iterate_upper_bound, kMaxSequenceNumber, kValueTypeForSeekForPrev) to position just before the upper bound.

Step 2: Call iter_.SeekForPrev() on the internal iterator

Step 3: Set direction to kReverse

Step 4: Call PrevInternal() to find the most recent visible value for the user key at or before the target

## Direction Switching

Switching between forward and reverse iteration requires repositioning the internal iterator:

**Forward to Reverse** (ReverseToBackward()): If current_entry_is_merged_ is true (meaning the internal iterator may have advanced past the current key's entries), seek the internal iterator back to saved_key_. Otherwise, the internal iterator is already at the correct position.

**Reverse to Forward** (ReverseToForward()): Seek the internal iterator to saved_key_ with kMaxSequenceNumber to position it at the first internal entry for the current user key, then skip forward past any entries with user key less than saved_key_.

Direction switching involves at least one internal Seek() call, making mixed forward/reverse iteration more expensive than pure forward or reverse scans.

## Skip Optimization (Reseek)

When FindNextUserEntryInternal() encounters many internal entries for the same user key (e.g., many overwritten versions), it falls back to a seek-based skip after exceeding max_sequential_skip_in_iterations entries (see MutableCFOptions in include/rocksdb/advanced_options.h):

- If skipping the current saved key: Seek to (saved_key_, sequence=0, type=kTypeDeletion) to jump past all versions
- If skipping entries with sequence numbers above the snapshot: Seek to (saved_key_, snapshot_sequence, type=kValueTypeForSeek) to jump to the first visible version

This reseek is performed at most once per user key to avoid infinite loops (tracked by reseek_done). The statistic NUMBER_OF_RESEEKS_IN_ITERATION counts how often this optimization triggers.

## SeekToFirst and SeekToLast

SeekToFirst() delegates to Seek(*iterate_lower_bound_) if a lower bound is set. Otherwise, it calls iter_.SeekToFirst() directly. When prefix seek mode is active (not total order), max_skip_ is set to uint64_t::max to disable the reseek optimization, since the iterator may need to scan all entries in the prefix.

SeekToLast() delegates to SeekForPrev(*iterate_upper_bound_) if an upper bound is set, returning the greatest key strictly less than iterate_upper_bound. Otherwise, it calls iter_.SeekToLast() directly and then calls PrevInternal() to find the last visible user key.

## Thread Yield Check

Long-running iteration loops (e.g., scanning through many tombstones) periodically check ROCKSDB_THREAD_YIELD_CHECK_ABORT(). If an abort is requested, the iterator sets status_ = Status::Aborted("Query abort.") and returns invalid. This prevents single iterator operations from monopolizing CPU indefinitely.
