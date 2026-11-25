//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef UTIL_CRC32C_RISCV_H
#define UTIL_CRC32C_RISCV_H

#include <cstddef>
#include <cstdint>

// RISC-V Zbc extension detection
// HAVE_RISCV_ZBC is defined by Makefile/CMake when -march=rv64gc_zbc is supported
#if defined(__riscv) && defined(HAVE_RISCV_ZBC)

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Runtime Zbc extension detection
 */
bool crc32c_riscv_zbc_runtime_check(void);

/**
 * RISC-V optimized CRC32C computation using Zbc CLMUL instructions
 */
uint32_t crc32c_riscv(uint32_t crc, unsigned char const* data, size_t len);

#ifdef __cplusplus
}
#endif

#endif  // __riscv && HAVE_RISCV_ZBC

#endif  // UTIL_CRC32C_RISCV_H
