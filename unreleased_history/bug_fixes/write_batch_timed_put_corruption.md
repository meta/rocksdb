Fixed `WriteBatch` decoding of a `TimedPut` record whose value is shorter than its write time trailer, e.g. in a corrupted WAL, to return `Status::Corruption` instead of reading out of bounds.
