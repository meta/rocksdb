Fixed `CompactFiles()` trivial moves retaining `SstFileManager` space reservations, which could incorrectly reject subsequent compactions as `Compaction too large`.
