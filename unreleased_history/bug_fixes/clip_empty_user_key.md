Fixed `DB::ClipColumnFamily()` leaving an empty-string key outside the kept range when it is the only key in an L0 SST file.
