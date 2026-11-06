#ifndef ROCKSDB_UTIL_CRC32C_ASM_H
#define ROCKSDB_UTIL_CRC32C_ASM_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// SVE2-accelerated CRC32C (Castagnoli). Takes/returns the uninverted CRC.
uint32_t crc32_iscsi_sve2(const char *BUF, uint64_t LEN, uint32_t wCRC);

// ARM64 hardware CRC32C fallback. Takes/returns the uninverted CRC.
uint32_t crc32_iscsi_crc_ext(const char *BUF, uint64_t LEN, uint32_t wCRC);

#ifdef __cplusplus
}
#endif

#endif  // ROCKSDB_UTIL_CRC32C_ASM_H
