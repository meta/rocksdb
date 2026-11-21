In the C API, `rocksdb_slicetransform_create()` accepts a NULL `in_domain`, which puts every key in the transform's domain without a callback per key.
