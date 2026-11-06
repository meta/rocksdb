//  Copyright (c) 2024-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef UTIL_CRC32C_RISCV_H
#define UTIL_CRC32C_RISCV_H

#include <cinttypes>
#include <cstddef>

#if (defined(__riscv) && defined(__riscv_zbc)) || defined(CRC32C_RISCV_SIM)

uint32_t crc32c_riscv(uint32_t crc, unsigned char const* data, size_t len);
uint32_t crc32c_runtime_check(void);

#endif  // RISC-V Zbc or CRC32C_RISCV_SIM

#endif  // UTIL_CRC32C_RISCV_H
