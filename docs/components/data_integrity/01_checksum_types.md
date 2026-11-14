# Checksum Types and Algorithms

**Files:** `include/rocksdb/table.h`, `util/crc32c.h`, `util/crc32c.cc`, `util/xxhash.cc`, `util/hash.h`

## ChecksumType Enum

The `ChecksumType` enum in `include/rocksdb/table.h` defines the available checksum algorithms for SST block verification:

| Value | Name | Availability | Performance |
|-------|------|--------------|-------------|
| 0x0 | kNoChecksum | Always | N/A (not recommended) |
| 0x1 | kCRC32c | Always | ~20 GB/s with SSE4.2 |
| 0x2 | kxxHash | Always | Good |
| 0x3 | kxxHash64 | Always | Better (64-bit truncated to 32) |
| 0x4 | kXXH3 | Since RocksDB 6.27 | ~60 GB/s with AVX2 (default) |

All checksums use 32 bits of checking power (1 in 4 billion chance of missing random corruption). The default is `kXXH3`, which provides the best performance on modern hardware.

**Configuration:** Set `checksum` in `BlockBasedTableOptions` (see `include/rocksdb/table.h`).

## CRC32c Implementation

The CRC32c implementation in `util/crc32c.cc` selects the fastest available hardware path at startup:

| Platform | Instruction | Notes |
|----------|-------------|-------|
| x86 with SSE4.2 | `_mm_crc32_u64` / `_mm_crc32_u8` | Most common fast path |
| ARM64 | CRC instructions via intrinsics | ARM hardware CRC |
| PowerPC | Crypto extensions | PPC-specific |
| Fallback | Slice-by-8 software algorithm | Universal |

The function `IsFastCrc32Supported()` in `util/crc32c.h` returns a string describing the detected hardware support.

## CRC32c Masking

CRC32c values stored on disk are masked using `crc32c::Mask()` (see `util/crc32c.h`):

The masking formula rotates right by 15 bits and adds a constant (`kMaskDelta = 0xa282ead8`). This prevents problems with embedded CRCs: computing a CRC over data that itself contains a CRC could produce degenerate results. `crc32c::Unmask()` reverses the transformation.

Masking is applied to CRC32c values in both WAL records and SST block checksums. `ComputeBuiltinChecksum()` calls `crc32c::Mask()` internally when the checksum type is `kCRC32c`. Non-CRC checksums (xxHash, XXH3) are stored unmasked.

## CRC32c Combine

`crc32c::Crc32cCombine()` in `util/crc32c.h` takes two unmasked CRC values and a length, producing the CRC of the concatenation. This runs in O(log(length)) time and is used in the WAL writer to combine the precomputed CRC of the record type byte with the payload CRC without re-scanning the type byte.

## xxHash / XXH3

The xxHash family (`util/xxhash.cc`) comes from the upstream xxHash library. XXH3 leverages SIMD instructions (AVX2, AVX512, NEON) for peak throughput, achieving roughly 3x the performance of hardware-accelerated CRC32c on modern x86 CPUs.

Unlike CRC32c, xxHash values are not masked before storage. The algorithm's design provides sufficient collision resistance without the masking step.

## Checksum Computation Functions

`ComputeBuiltinChecksum()` and `ComputeBuiltinChecksumWithLastByte()` in `table/format.h` dispatch to the appropriate algorithm based on the `ChecksumType` parameter. The "with last byte" variant is used for SST block checksums where the compression type byte is logically appended to the block data but stored separately in the trailer.

## Algorithm Selection Guide

| Use Case | Recommended | Rationale |
|----------|-------------|-----------|
| Default for new databases | kXXH3 | Fastest, default since 6.27 |
| Compatibility with older RocksDB | kCRC32c | Universally supported |
| WAL records | CRC32c (hardcoded) | Fast hardware path; not configurable |
| Handoff checksums | CRC32c (hardcoded) | Storage layer compatibility |
| Full-file checksums | CRC32c (builtin factory) | Custom factories can use other algorithms |

Note: The checksum type stored in an SST footer is immutable. RocksDB can read files with any supported checksum type regardless of the current configuration, so changing the checksum type only affects newly written files.

## Platform Performance Notes

XXH3 performance varies significantly across architectures. On x86 with AVX2, XXH3 achieves roughly 60 GB/s. On ARM (e.g., Graviton3), XXH3 can be 2.4x to 5x slower than on Intel due to differences in SIMD instruction support and compiler optimization. CRC32c with hardware acceleration is more consistent across architectures (both x86 SSE4.2 and ARM CRC instructions provide good performance).
