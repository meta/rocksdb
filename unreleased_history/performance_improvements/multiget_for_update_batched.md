`Transaction::MultiGetForUpdate()` now reads its locked keys with one batched `MultiGet` lookup per column family instead of one `Get()` per key.
