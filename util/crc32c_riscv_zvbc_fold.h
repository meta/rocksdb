//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// RISC-V CRC32C folding core for the Zvbc (vector CLMUL) extension.
//
// This header is the vector counterpart of crc_fold_riscv_clmul.h. It drives
// both the scalar Zbc CLMUL units and the Zvbc vector CLMUL units in parallel
// to fold 128 bytes per iteration, and provides reflected ("refl") and normal
// ("norm") variants of each stage.
//
// Register allocation and macros provided:
//   - crc_refl_load_first_block / crc_norm_load_first_block:
//       load the first 128 bytes into scalar (s0-s7) and vector (v1-v4) state.
//   - crc_load_p4: load the folding constants and the loop end pointer.
//   - crc_refl_loop / crc_norm_loop: the 128-byte-per-iter main fold loop.
//   - refl_fold_1024b_to_128b / norm_fold_1024b_to_128b:
//       reduce the 8x128-bit accumulator down to 128 bits.
//
// This file is included by crc32c_riscv_zvbc_macros.h.

// parameters
#define seed          a0
#define buf           a1
#define len           a2
#define buf_end       a6
#define crc_tab_addr  a5
#define vec_shuffle   v23

.macro crc_refl_load_first_block
    addi sp, sp, -128
    sd ra, 120(sp)
    sd gp, 112(sp)
    sd tp, 104(sp)
    sd s0, 96(sp)
    sd s1, 88(sp)
    sd s2, 80(sp)
    sd s3, 72(sp)
    sd s4, 64(sp)
    sd s5, 56(sp)
    sd s6, 48(sp)
    sd s7, 40(sp)
    sd s8, 32(sp)
    sd s9, 24(sp)
    sd s10, 16(sp)
    sd s11, 8(sp)

    addi t0, buf, 64
    addi t1, buf, 80
    addi t2, buf, 96
    addi t3, buf, 112

    ld s0, 0(buf)
    ld s1, 8(buf)
    ld s2, 16(buf)
    ld s3, 24(buf)
    ld s4, 32(buf)
    ld s5, 40(buf)
    ld s6, 48(buf)
    ld s7, 56(buf)
    vle64.v v1, (t0)
    vle64.v v2, (t1)
    vle64.v v3, (t2)
    vle64.v v4, (t3)

    addi buf, buf, 128
    andi a3, len, ~127
    addi t0, a3, -128
    sub len, len, a3

    xor s0, s0, seed
.endm

.macro crc_norm_load_first_block
    addi sp, sp, -128
    sd ra, 120(sp)
    sd gp, 112(sp)
    sd tp, 104(sp)
    sd s0, 96(sp)
    sd s1, 88(sp)
    sd s2, 80(sp)
    sd s3, 72(sp)
    sd s4, 64(sp)
    sd s5, 56(sp)
    sd s6, 48(sp)
    sd s7, 40(sp)
    sd s8, 32(sp)
    sd s9, 24(sp)
    sd s10, 16(sp)
    sd s11, 8(sp)

    addi t0, buf, 64
    addi t1, buf, 80
    addi t2, buf, 96
    addi t3, buf, 112
    ld s0, 0(buf)
    ld s1, 8(buf)
    ld s2, 16(buf)
    ld s3, 24(buf)
    ld s4, 32(buf)
    ld s5, 40(buf)
    ld s6, 48(buf)
    ld s7, 56(buf)
    vle64.v v8, (t0)
    vle64.v v9, (t1)
    vle64.v v10, (t2)
    vle64.v v11, (t3)
    rev8 s0, s0
    rev8 s1, s1
    rev8 s2, s2
    rev8 s3, s3
    rev8 s4, s4
    rev8 s5, s5
    rev8 s6, s6
    rev8 s7, s7
    vsetivli zero, 16, e8, m1, ta, ma
    vle8.v vec_shuffle, 0(t4)
    vrgather.vv v1, v8, vec_shuffle
    vrgather.vv v2, v9, vec_shuffle
    vrgather.vv v3, v10, vec_shuffle
    vrgather.vv v4, v11, vec_shuffle
    vsetivli zero, 2, e64, m1, ta, ma

    addi buf, buf, 128
    andi a3, len, ~127
    addi t0, a3, -128
    sub len, len, a3

    xor s0, s0, seed
.endm

.macro crc_load_p4
    add buf_end, buf, t0
    la t4, .crc_loop_const
    ld a3, 0(t4)
    ld a4, 8(t4)
    vle64.v v5, 0(t4)
.endm

.macro crc_refl_loop
    .align 3
.combine_8x16_refl_loop:
    // Utilizing both scalar and vector units to process 128 bytes of data
    // Use ra, gp, tp as temporaries to cover register shortage in loop
    ld a5, 0(buf)
    clmulh t0, a3, s0
    clmulh t2, a3, s2
    ld ra, 8(buf)
    clmulh t4, a3, s4
    clmulh t6, a3, s6
    ld gp, 16(buf)
    clmul t1, a4, s1
    clmul t3, a4, s3
    ld tp, 24(buf)
    clmul t5, a4, s5
    clmul a7, a4, s7
    ld s8, 32(buf)
    clmul s0, a3, s0
    clmul s2, a3, s2
    ld s9, 40(buf)
    clmul s4, a3, s4
    clmul s6, a3, s6
    ld s10, 48(buf)
    clmulh s1, a4, s1
    clmulh s3, a4, s3
    ld s11, 56(buf)
    clmulh s5, a4, s5
    clmulh s7, a4, s7

    addi buf, buf, 64
    vle64.v v10, (buf)
    addi buf, buf, 16
    vclmul.vv v6, v1, v5
    vclmulh.vv v1, v1, v5
    vle64.v v11, (buf)
    addi buf, buf, 16
    vclmul.vv v7, v2, v5
    vclmulh.vv v2, v2, v5
    vle64.v v12, (buf)
    addi buf, buf, 16
    vclmul.vv v8, v3, v5
    vclmulh.vv v3, v3, v5
    vle64.v v13, (buf)
    addi buf, buf, 16
    vclmul.vv v9, v4, v5
    vclmulh.vv v4, v4, v5
    vmv.v.v v14, v6
    vmv.v.v v15, v7
    vmv.v.v v16, v8
    vmv.v.v v17, v9
    vslideup.vi v6, v1, 1
    vslideup.vi v7, v2, 1
    vslideup.vi v8, v3, 1
    vslideup.vi v9, v4, 1
    vsetivli zero, 2, e64, m1, ta, mu
    vslidedown.vi v1, v14, 1, v0.t
    vslidedown.vi v2, v15, 1, v0.t
    vslidedown.vi v3, v16, 1, v0.t
    vslidedown.vi v4, v17, 1, v0.t
    vsetivli zero, 2, e64, m1, ta, ma

    xor t1, t1, s0
    xor t3, t3, s2
    xor t5, t5, s4
    xor a7, a7, s6
    xor t0, t0, s1
    xor t2, t2, s3
    xor t4, t4, s5
    xor t6, t6, s7
    xor s0, t1, a5
    xor s2, t3, gp
    xor s4, t5, s8
    xor s6, a7, s10
    xor s1, t0, ra
    xor s3, t2, tp
    xor s5, t4, s9
    xor s7, t6, s11

    vxor.vv v1, v1, v6
    vxor.vv v2, v2, v7
    vxor.vv v3, v3, v8
    vxor.vv v4, v4, v9
    vxor.vv v1, v1, v10
    vxor.vv v2, v2, v11
    vxor.vv v3, v3, v12
    vxor.vv v4, v4, v13

    bne buf, buf_end, .combine_8x16_refl_loop
.endm

.macro crc_norm_loop
    .align 3
.combine_8x16_norm_loop:
    // Utilizing both scalar and vector units to process 128 bytes of data
    // Use ra, gp, tp as temporaries to cover register shortage in loop
    ld a5, 0(buf)
    clmul t0, a3, s0
    clmul t2, a3, s2
    ld ra, 8(buf)
    clmul t4, a3, s4
    clmul t6, a3, s6
    ld gp, 16(buf)
    clmulh t1, a4, s1
    clmulh t3, a4, s3
    ld tp, 24(buf)
    clmulh t5, a4, s5
    clmulh a7, a4, s7
    ld s8, 32(buf)
    clmulh s0, a3, s0
    clmulh s2, a3, s2
    ld s9, 40(buf)
    clmulh s4, a3, s4
    clmulh s6, a3, s6
    ld s10, 48(buf)
    clmul s1, a4, s1
    clmul s3, a4, s3
    ld s11, 56(buf)
    clmul s5, a4, s5
    clmul s7, a4, s7

    addi buf, buf, 64
    vle64.v v10, (buf)
    addi buf, buf, 16
    vclmul.vv v6, v1, v22
    vclmulh.vv v1, v1, v22
    vle64.v v11, (buf)
    addi buf, buf, 16
    vclmul.vv v7, v2, v22
    vclmulh.vv v2, v2, v22
    vle64.v v12, (buf)
    addi buf, buf, 16
    vclmul.vv v8, v3, v22
    vclmulh.vv v3, v3, v22
    vle64.v v13, (buf)
    addi buf, buf, 16
    vclmul.vv v9, v4, v22
    vclmulh.vv v4, v4, v22
    vmv.v.v v14, v6
    vmv.v.v v15, v7
    vmv.v.v v16, v8
    vmv.v.v v17, v9
    vslideup.vi v6, v1, 1
    vslideup.vi v7, v2, 1
    vslideup.vi v8, v3, 1
    vslideup.vi v9, v4, 1
    vsetivli zero, 2, e64, m1, ta, mu
    vslidedown.vi v1, v14, 1, v0.t
    vslidedown.vi v2, v15, 1, v0.t
    vslidedown.vi v3, v16, 1, v0.t
    vslidedown.vi v4, v17, 1, v0.t
    vsetivli zero, 2, e64, m1, ta, ma

    rev8 a5, a5
    rev8 gp, gp
    rev8 s8, s8
    rev8 s10, s10
    rev8 ra, ra
    rev8 tp, tp
    rev8 s9, s9
    rev8 s11, s11
    vsetivli zero, 16, e8, m1, ta, ma
    vrgather.vv v18, v10, vec_shuffle
    vrgather.vv v19, v11, vec_shuffle
    vrgather.vv v20, v12, vec_shuffle
    vrgather.vv v21, v13, vec_shuffle
    vsetivli zero, 2, e64, m1, ta, ma

    xor t1, t1, s0
    xor t3, t3, s2
    xor t5, t5, s4
    xor a7, a7, s6
    xor t0, t0, s1
    xor t2, t2, s3
    xor t4, t4, s5
    xor t6, t6, s7
    xor s0, t1, a5
    xor s2, t3, gp
    xor s4, t5, s8
    xor s6, a7, s10
    xor s1, t0, ra
    xor s3, t2, tp
    xor s5, t4, s9
    xor s7, t6, s11

    vxor.vv v1, v1, v6
    vxor.vv v2, v2, v7
    vxor.vv v3, v3, v8
    vxor.vv v4, v4, v9
    vxor.vv v1, v1, v18
    vxor.vv v2, v2, v19
    vxor.vv v3, v3, v20
    vxor.vv v4, v4, v21

    bne buf, buf_end, .combine_8x16_norm_loop
.endm

.macro refl_fold_1024b_to_128b
    mv t6, sp
    la t5, .crc_loop_const
    ld a3, 16(t5)
    ld a4, 24(t5)

    addi sp, sp, -16
    vse64.v v4, 0(sp)
    addi sp, sp, -16
    vse64.v v3, 0(sp)
    addi sp, sp, -16
    vse64.v v2, 0(sp)
    addi sp, sp, -16
    vse64.v v1, 0(sp)

    clmul t0, a3, s0
    clmulh t1, a3, s0
    clmul t2, a4, s1
    clmulh t3, a4, s1
    xor s0, s2, t0
    xor s0, s0, t2
    xor s1, s3, t1
    xor s1, s1, t3
    clmul t0, a3, s0
    clmulh t1, a3, s0
    clmul t2, a4, s1
    clmulh t3, a4, s1
    xor s0, s4, t0
    xor s0, s0, t2
    xor s1, s5, t1
    xor s1, s1, t3
    clmul t4, a3, s0
    clmulh t5, a3, s0
    clmul t2, a4, s1
    clmulh t3, a4, s1
    xor s0, s6, t4
    xor t0, s0, t2
    xor s1, s7, t5
    xor t1, s1, t3

    .align 3
.16b_fold:
    clmul t4, a3, t0
    clmulh t5, a3, t0
    clmul t2, a4, t1
    clmulh t3, a4, t1
    ld s0, 0(sp)
    ld s1, 8(sp)
    addi sp, sp, 16
    xor t0, s0, t4
    xor t0, t0, t2
    xor t1, s1, t5
    xor t1, t1, t3
    bne sp, t6, .16b_fold

    ld ra, 120(sp)
    ld gp, 112(sp)
    ld tp, 104(sp)
    ld s0, 96(sp)
    ld s1, 88(sp)
    ld s2, 80(sp)
    ld s3, 72(sp)
    ld s4, 64(sp)
    ld s5, 56(sp)
    ld s6, 48(sp)
    ld s7, 40(sp)
    ld s8, 32(sp)
    ld s9, 24(sp)
    ld s10, 16(sp)
    ld s11, 8(sp)
    addi sp, sp, 128
.endm

.macro norm_fold_1024b_to_128b
    mv t6, sp
    la t5, .crc_loop_const
    ld a3, 16(t5)
    ld a4, 24(t5)

    addi sp, sp, -16
    vse64.v v4, 0(sp)
    addi sp, sp, -16
    vse64.v v3, 0(sp)
    addi sp, sp, -16
    vse64.v v2, 0(sp)
    addi sp, sp, -16
    vse64.v v1, 0(sp)

    clmulh t0, a3, s0
    clmul t1, a3, s0
    clmulh t2, a4, s1
    clmul t3, a4, s1
    xor s0, s2, t0
    xor s0, s0, t2
    xor s1, s3, t1
    xor s1, s1, t3
    clmulh t0, a3, s0
    clmul t1, a3, s0
    clmulh t2, a4, s1
    clmul t3, a4, s1
    xor s0, s4, t0
    xor s0, s0, t2
    xor s1, s5, t1
    xor s1, s1, t3
    clmulh t4, a3, s0
    clmul t5, a3, s0
    clmulh t2, a4, s1
    clmul t3, a4, s1
    xor s0, s6, t4
    xor t0, s0, t2
    xor s1, s7, t5
    xor t1, s1, t3

    .align 3
.16b_fold:
    clmulh t4, a3, t0
    clmul t5, a3, t0
    clmulh t2, a4, t1
    clmul t3, a4, t1
    ld s0, 8(sp)
    ld s1, 0(sp)
    xor t0, s0, t4
    xor t0, t0, t2
    xor t1, s1, t5
    xor t1, t1, t3
    addi sp, sp, 16
    bne sp, t6, .16b_fold

    ld ra, 120(sp)
    ld gp, 112(sp)
    ld tp, 104(sp)
    ld s0, 96(sp)
    ld s1, 88(sp)
    ld s2, 80(sp)
    ld s3, 72(sp)
    ld s4, 64(sp)
    ld s5, 56(sp)
    ld s6, 48(sp)
    ld s7, 40(sp)
    ld s8, 32(sp)
    ld s9, 24(sp)
    ld s10, 16(sp)
    ld s11, 8(sp)
    addi sp, sp, 128
.endm
