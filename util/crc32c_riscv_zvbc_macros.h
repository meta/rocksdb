//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// RISC-V CRC32C function template for the Zvbc (vector CLMUL) extension.
//
// This header provides the crc_refl_func macro, the function template expanded
// by crc32c_riscv_zvbc_asm.S. It is the vector counterpart of the macros in
// crc32_refl_riscv_clmul.h. A single crc_refl_func expansion produces a full
// CRC32C routine with two paths:
//   - Small buffer (< 128 bytes): byte-wise table walk over
//     crc32_table_iscsi_refl.
//   - Large buffer: vectorized CLMUL folding (128 bytes/iter) followed by a
//     128->64 fold and a Barrett reduction to the final 32-bit CRC.
//
// The expansion expects seed (a0), buf (a1) and len (a2) on entry and returns
// the result in seed (a0), inverting the CRC value at entry and exit.
//
// Requires: RISC-V Zvbc + Zbc + Zbb (enabled via .option arch below).
// This file is included by crc32c_riscv_zvbc_asm.S.

#include "crc32c_riscv_zvbc_fold.h"

.macro crc_refl_func name
    .option arch, +v, +zvbc, +zbc, +zbb
    .text
    .align  3
    .type \name, @function
    .global \name
\name:
    xori seed, seed, -1
    slli seed, seed, 0x20
    srli seed, seed, 0x20
    li a3, 0
    li t1, 128
    bgeu len, t1, .crc_clmul_pre

.crc_tab_pre:
    beq len, zero, .done
    la crc_tab_addr, .lanchor_crc_tab
    add buf_end, buf, len

    .align 3
.loop_crc_tab:
    lbu t1, 0(buf)
    addi buf, buf, 1
    xor t1, seed, t1
    zext.b a7, t1
    slli a7, a7, 0x2
    add a7, a7, crc_tab_addr
    lw seed, 0(a7)
    srliw t1, t1, 0x8
    xor seed, seed, t1
    bne buf, buf_end, .loop_crc_tab

.done:
    xori seed, seed, -1
    sext.w seed, seed
    ret

    .align 2
.crc_clmul_pre:
    vsetivli zero, 2, e64, m1, ta, ma
    crc_refl_load_first_block
    crc_load_p4
    la t0, .refl_sld_mask
    vle64.v v0, (t0)
    beq buf, buf_end, .clmul_loop_end

    crc_refl_loop

.clmul_loop_end:
    refl_fold_1024b_to_128b

    // 128-bit -> 64-bit folding
    li t2, const_low
    li t3, const_high
    li t6, 0xffffffff
    clmul t4, t0, t3
    clmulh t3, t0, t3
    xor t1, t1, t4
    slli t4, t1, 0x20
    srli t4, t4, 0x20
    srli t1, t1, 0x20
    clmul t0, t4, t2
    slli t3, t3, 0x20
    xor t3, t3, t1
    xor t3, t3, t0

    // Barrett reduction
    and t4, t3, t6
    li t2, const_quo
    li t1, const_poly
    clmul t4, t4, t2
    and t4, t4, t6
    clmul t4, t4, t1
    xor t4, t3, t4
    srli seed, t4, 0x20

    j .crc_tab_pre
    .size   \name, .-\name
.section    .rodata,"a",@progbits
.refl_sld_mask:
    .quad   1, 0
.endm
