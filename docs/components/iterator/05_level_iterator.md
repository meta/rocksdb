# LevelIterator

**Files:** db/version_set.cc, db/version_set.h

## Role

LevelIterator is an internal class defined in db/version_set.cc that iterates across all SST files within a single sorted level (L1 and below). It lazily opens BlockBasedTableIterators as the scan crosses file boundaries, avoiding the cost of opening all files in a level upfront.

## Design

Since files in L1+ are non-overlapping and sorted by key range, LevelIterator can treat the level as a single sorted sequence. It maintains:

- A LevelFilesBrief (sorted file list) plus FindFile() binary search to determine which file contains a given key
- The currently open BlockBasedTableIterator for the active file
- A range tombstone iterator for the active file (updated when switching files)

## File Switching Flow

When iteration moves past the last key in the current file during sequential forward scanning:

Step 1: The is_next_read_sequential_ flag is set by NextAndGetResult()

Step 2: Close the current data iterator

Step 3: Advance to the next file in the level's sorted file list

Step 4: Open a new BlockBasedTableIterator for the next file (heap-allocated via TableCache, not arena-backed)

Step 5: If is_next_read_sequential_ is set and ReadOptions::adaptive_readahead is true, the readahead state is transferred via UpdateReadaheadState() so the readahead window is not reset

This readahead state transfer is important for maintaining scan performance across file boundaries during sequential forward reads. The transfer does not occur during seeks or backward iteration.

## Range Tombstone Sentinel Keys

LevelIterator uses "sentinel keys" at file boundaries to prevent MergingIterator from moving past a file before its range tombstones have been fully processed. When IsDeleteRangeSentinelKey() returns true, MergingIterator knows this is a boundary marker and not a real data key.

The sentinel keys ensure that range tombstones from one file are properly applied to point keys in overlapping ranges from other levels before LevelIterator switches to the next file.

## Lazy File Opening

Files are opened on demand: when Seek() identifies the target file via the file index, or when forward/backward iteration reaches a file boundary. The TableCache is used to open files, which may serve the table reader from cache if the file was previously opened.

## Range Tombstone Iterator Management

Each file in the level may have its own set of range tombstones. When LevelIterator switches files, it updates the range tombstone iterator pointer that MergingIterator holds. This is managed via the tombstone_iter_ptr mechanism in MergeIteratorBuilder::AddPointAndTombstoneIterator():

- MergingIterator stores a pointer-to-pointer for each level's range tombstone iterator
- When LevelIterator opens a new file, it updates the pointed-to iterator with the new file's range tombstones
- MergingIterator sees the updated range tombstones on its next positioning operation
