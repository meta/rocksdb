Batched `MultiGet()` now allocates per-key lookup state once for a batch of 17 to 32 keys that reaches SST files, instead of growing it incrementally.
