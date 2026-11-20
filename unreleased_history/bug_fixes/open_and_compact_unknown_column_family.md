Fixed a crash in `DB::OpenAndCompact()` when the compaction input names a column family that the OPTIONS file of the input does not list. It now returns `Status::InvalidArgument`.
