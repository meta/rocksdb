# DBIter - User-Facing Iterator

**Files:** db/db_iter.h, db/db_iter.cc, db/arena_wrapped_db_iter.h, db/arena_wrapped_db_iter.cc

## Role

DBIter is the core user-facing iterator. It wraps an InternalIterator (typically a MergingIterator) and translates the stream of internal keys (user_key + sequence_number + type) into deduplicated user keys with proper snapshot isolation. It handles:

- Filtering entries by sequence number (snapshot visibility)
- Collapsing multiple versions of the same user key into the most recent visible version
- Processing deletions (kTypeDeletion, kTypeSingleDeletion, kTypeDeletionWithTimestamp)
- Resolving merge operations (kTypeMerge) by accumulating operands and applying the merge operator
- Fetching blob values (kTypeBlobIndex) from blob files
- Deserializing wide-column entities (kTypeWideColumnEntity)
- Handling user-defined timestamps (iter_start_ts / timestamp range filtering)

## Direction State Machine

DBIter maintains a Direction enum (kForward or kReverse) that tracks the current iteration direction. The internal iterator positioning conventions differ by direction:

**Forward direction (kForward):**
- If current_entry_is_merged_ == false: the internal iterator is positioned exactly at the entry that yields the current key() and value()
- If current_entry_is_merged_ == true: the internal iterator is positioned immediately after the last merge operand that contributed to the current value

**Reverse direction (kReverse):**
- The internal iterator is positioned just before all entries whose user key equals this->key()

When the direction changes (e.g., calling Prev() after Next()), DBIter must reposition the internal iterator. This is handled by ReverseToForward() and ReverseToBackward(), both of which may issue a Seek() on the internal iterator.

## Forward Iteration: FindNextUserEntry

The core forward iteration logic lives in FindNextUserEntryInternal(). After Next() or Seek(), this method scans forward through internal keys to find the next visible user key.

Step 1: Parse the current internal key

Step 2: Check upper bound -- if the user key is at or past iterate_upper_bound, stop

Step 3: Check prefix constraint -- if prefix_same_as_start is set and the prefix differs, stop

Step 4: Check visibility -- the entry must have a sequence number <= the snapshot sequence (via IsVisible())

Step 5: Process by type:
- kTypeValue / kTypeValuePreferredSeqno: Set value directly, return valid
- kTypeBlobIndex: Retrieve blob value from blob file (or defer if allow_unprepared_value)
- kTypeWideColumnEntity: Deserialize wide columns
- kTypeMerge: Enter MergeValuesNewToOld() to collect and resolve merge operands
- kTypeDeletion / kTypeSingleDeletion: Skip this user key and continue scanning

Step 6: If too many entries for the same key have been skipped (exceeds max_sequential_skip_in_iterations, see MutableCFOptions), perform a reseek directly to the target sequence number instead of scanning linearly

## Reverse Iteration: PrevInternal and FindValueForCurrentKey

Reverse iteration is more complex because internal keys are sorted newest-first within a user key, but Prev() needs to find the value of the previous user key, which requires scanning from oldest to newest.

PrevInternal() flow:

Step 1: Read the user key at the current internal iterator position (via saved_key_)

Step 2: Check lower bound -- if below iterate_lower_bound, invalidate

Step 3: Call FindValueForCurrentKey() -- scan backward (older entries) through all versions of this user key, accumulating merge operands and finding the latest visible value

Step 4: Call FindUserKeyBeforeSavedKey() -- position the internal iterator before saved_key_ for the next Prev() call

FindValueForCurrentKey() iterates backward through entries with the same user key. It tracks the most recent visible value type and, for merge operations, accumulates operands from oldest to newest (using PushOperandBack()). If too many entries are skipped, it falls back to FindValueForCurrentKeyUsingSeek(), which issues a forward Seek() to the user key and resolves the value using the forward path.

Important: Reverse iteration requires that values be pinned (IsValuePinned() returns true), because FindValueForCurrentKey() saves value slices while scanning past them. Before calling FindValueForCurrentKey(), DBIter enables temporary pinning via TempPinData(), which activates the PinnedIteratorsManager. Standard block-based and memtable iterators support pinning through this mechanism. The NotSupported error only occurs with custom InternalIterator implementations that cannot pin values even with a PinnedIteratorsManager active.

## Merge Resolution

When a kTypeMerge entry is the most recent visible entry for a user key, DBIter must collect all merge operands and apply the merge operator.

**Forward merge** (MergeValuesNewToOld()):
- Starts from the newest merge operand, scans forward to find older operands or a base value
- If a kTypeValue or kTypeBlobIndex base is found, calls MergeWithPlainBaseValue() / MergeWithBlobBaseValue()
- If no base value is found (all merges), calls MergeWithNoBaseValue()

**Reverse merge** (in FindValueForCurrentKey()):
- Scans backward, collecting operands via PushOperandBack() (oldest first)
- After finding the last merge operand or a base value, applies the merge

## Blob Value Handling

When the value type is kTypeBlobIndex, the actual value is stored in a blob file. DBIter uses an embedded BlobReader to retrieve it. If allow_unprepared_value is set, the blob index is saved in lazy_blob_index_ and the actual blob read is deferred until PrepareValue() is called.

## Local Statistics

DBIter batches statistics updates (next/prev counts, bytes read, skip counts) in a LocalStatistics struct to avoid contention on global atomic counters during iteration. These are flushed to the global Statistics object when the iterator is destroyed.

## Memtable Flush Trigger

DBIter can trigger memtable flushes based on scan patterns. If an iterator scans many invisible entries (entries filtered out by snapshot visibility, including tombstones and hidden puts) in the active memtable during forward iteration, it may mark the memtable for flush via active_mem_->MarkForFlush(). This is controlled by two options in MutableCFOptions (see include/rocksdb/advanced_options.h):

- memtable_op_scan_flush_trigger: Per-operation threshold for invisible entries scanned. Must be non-zero for the feature to be active.
- memtable_avg_op_scan_flush_trigger: Windowed average threshold across operations within a Seek-to-Seek window. Only takes effect when memtable_op_scan_flush_trigger is also set.

Note: This feature is currently disabled for tailing iterators.
