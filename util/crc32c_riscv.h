//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef UTIL_CRC32C_RISCV_H
#define UTIL_CRC32C_RISCV_H

#include <cstddef>
#include <cstdint>

// RISC-V Zbc/Zvbc/Zbb extension detection
// HAVE_RISCV_ZBC is defined by Makefile/CMake when -march=rv64gc_zbc is supported
// HAVE_RISCV_ZVBC is defined when the compiler supports the _zvbc extension (vector CLMUL)
// HAVE_RISCV_ZBB is defined when the compiler supports the _zbb extension (basic bit-manipulation)
#if defined(__riscv) && defined(HAVE_RISCV_ZBC)

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Runtime Zbc extension detection
 */
bool crc32c_riscv_zbc_runtime_check(void);

#if defined(HAVE_RISCV_ZVBC)
/**
 * Runtime Zvbc availability check (vector carry-less multiply).
 */
bool crc32c_riscv_zvbc_available(void);
#endif

#if defined(HAVE_RISCV_ZBB)
/**
 * Runtime Zbb availability check (basic bit-manipulation, e.g. rev8). Required
 * together with Zvbc for the vector CRC path.
 */
bool crc32c_riscv_zbb_available(void);
#endif

/**
 * RISC-V CRC32C computation. Takes the Zvbc vector path when both Zvbc and Zbb
 * are available at runtime; otherwise uses the Zbc scalar path.
 */
uint32_t crc32c_riscv(uint32_t crc, unsigned char const* data, size_t len);

#ifdef __cplusplus
}
#endif

#endif  // __riscv && HAVE_RISCV_ZBC

#endif  // UTIL_CRC32C_RISCV_H
