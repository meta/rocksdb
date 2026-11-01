With `ReadOptions::pin_data`, iterators over read-only DBs opened with `max_open_files = -1` and `allow_mmap_reads = true` no longer accumulate memory as they move across SST files.
