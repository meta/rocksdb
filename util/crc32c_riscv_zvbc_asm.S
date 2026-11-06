//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// RISC-V CRC32C assembly entry using the Zvbc (vector CLMUL) extension.
//
// This file is the vector-accelerated counterpart of crc32c_riscv_asm.S. It
// implements crc32c_riscv_zvbc(), a self-contained entry point that reorders
// the calling-convention arguments, inverts the CRC value (xori crc, -1) on
// entry and exit, and delegates the actual folding computation to
// crc32c_riscv_zvbc_internal(), which is expanded from the crc_refl_func
// macro in crc32c_riscv_zvbc_macros.h.
//
// Function: crc32c_riscv_zvbc(buffer, len, init_crc)
//   - buffer:   Pointer to input data (a0 -> a1 after arg shuffle)
//   - len:      Length of data in bytes (a1 -> a2 after arg shuffle)
//   - init_crc: Initial CRC value (a2 -> a0 after arg shuffle)
//   - Returns:  Computed CRC32C value in a0
//
// Requires: RISC-V Zvbc (vector CLMUL) + Zbc (scalar CLMUL) + Zbb (rev8),
//           enabled via .option arch in crc32c_riscv_zvbc_macros.h.
#if defined(HAVE_RISCV_ZBC) && defined(HAVE_RISCV_ZVBC) && defined(HAVE_RISCV_ZBB)
#include "crc32c_riscv_zvbc_constants.h"
#include "crc32c_riscv_zvbc_macros.h"

crc_refl_func crc32c_riscv_zvbc_internal

    .text
    .align 3
    .global crc32c_riscv_zvbc
    .type   crc32c_riscv_zvbc, %function
crc32c_riscv_zvbc:
    mv a7, a2
    mv a2, a1
    mv a1, a0
    mv a0, a7
    xori a0, a0, -1
    addi sp, sp, -8
    sd ra, (sp)
1:
    auipc ra, %pcrel_hi(crc32c_riscv_zvbc_internal)
    jalr  ra, %pcrel_lo(1b)(ra)
    ld ra, (sp)
    addi sp, sp, 8
    xori a0, a0, -1
    ret
#endif
