//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// RISC-V CRC32 folding implementation using Zbc CLMUL instructions
//
// This header provides the crc_fold_loop macro for efficient CRC computation
// using the carry-less multiply (CLMUL) instructions from the RISC-V Zbc
// extension. The algorithm uses a folding approach similar to Intel's CRC32
// implementation with PCLMULQDQ.
//
// Algorithm overview:
//   1. Load 4x128-bit chunks from input buffer
//   2. Fold-by-4: Multiply each chunk by folding constants (k1, k2)
//   3. Reduce 4 chunks to 1 via fold-by-1 (using k3, k4 constants)
//   4. Final reduction to CRC bit width (32 or 64 bits)
// This file is included by crc32c_riscv_asm.S

#define SEED a0
#define BUF  a1
#define LEN  a2
#define POLY a3
#define MU   a4
#define K1   t5
#define K2   t6
#define K3   t5
#define K4   t6
#define K5   t5
#define K6   t6

#define X3HIGH t0
#define HIGH   t0
#define X3LOW  t1
#define LOW    t1

#define X2HIGH t2
#define X2LOW  a5
#define X1HIGH a6
#define X1LOW  a7
#define X0HIGH t3
#define X0LOW  t4

#define BUF3HIGH s4
#define BUF3LOW  s5
#define BUF2HIGH s6
#define BUF2LOW  s7
#define BUF1HIGH s8
#define BUF1LOW  s9
#define BUF0HIGH s10
#define BUF0LOW  s11

#define X3K1LOW  ra
#define X3K2HIGH gp
#define X2K1LOW  tp
#define X2K2HIGH s0
#define X1K1LOW  s1
#define X1K2HIGH a0
#define X0K1LOW  s2
#define X0K2HIGH s3

/* repeated fold-by-four followed by fold-by-one */
/* takes parameter \bits, bit length of polynomial (32 or 64) */
/* \endianswap is a boolean parameter, controlling whether an endiannes swap is
 * needed (true for norm crc on little-endian cpu, false for refl crc) */
/* expects SEED (a0), BUF (a1) and LEN (a2) to hold those values */
/* expects BUF is doubleword-aligned */
/* returns 128-bit result in HIGH:LOW (t0:t1) */
/* returns updated buffer ptr & length in BUF and LEN */
/* trashes all caller-saved registers except POLY and MU (a3/a4) */
.macro crc_fold_loop bits:req endianswap:req reflected:req

	/* for a reflected crc, clmulh gets low word and vice-versa */
.macro clmul_low rd:req, rs1:req, rs2:req
.if !\reflected
	clmul \rd, \rs1, \rs2
.else
	clmulh \rd, \rs1, \rs2
.endif
.endm
.macro clmul_high rd:req, rs1:req, rs2:req
.if !\reflected
	clmulh \rd, \rs1, \rs2
.else
	clmul \rd, \rs1, \rs2
.endif
.endm

	/* does enough buffer exist for a 4-fold? */
	li t0, 128
	bltu LEN, t0, .fold_1

	/* push callee-saved registers to stack */
	addi sp, sp, -136
	sd a3, 128(sp)
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

	/* load initial 4 128-bit chunks */
	ld X3HIGH, 0(BUF)
	ld X3LOW, 8(BUF)
	ld X2HIGH, 16(BUF)
	ld X2LOW, 24(BUF)
	ld X1HIGH, 32(BUF)
	ld X1LOW, 40(BUF)
	ld X0HIGH, 48(BUF)
	ld X0LOW, 56(BUF)

	addi BUF, BUF, 64
	addi LEN, LEN, -64

	/* endianness swap */
.if \endianswap
	rev8 X3HIGH, X3HIGH
	rev8 X3LOW, X3LOW
	rev8 X2HIGH, X2HIGH
	rev8 X2LOW, X2LOW
	rev8 X1HIGH, X1HIGH
	rev8 X1LOW, X1LOW
	rev8 X0HIGH, X0HIGH
	rev8 X0LOW, X0LOW
.endif

	/* xor in seed */
.if (\bits != 64) && \endianswap
	slli SEED, SEED, 64 - \bits
.endif
	xor X3HIGH, X3HIGH, SEED

	/* load constants */
	ld K1, .k1
	ld K2, .k2

	/* calculate how far we'll fold til and load LEN with the amount left */
	srli a3, LEN, 6
	slli a3, a3, 6
	add a3, BUF, a3
	and LEN, LEN, 0x3f

.align 3
.fold_4_loop:
	/* carryless multiply each high doubleword by k1, get 128-bit result */
	/* interleve fetching next 4 128-bit chunks */
	clmul_low X3K1LOW, K1, X3HIGH
	ld BUF3HIGH, 0(BUF)
	clmul_low X2K1LOW, K1, X2HIGH
	ld BUF3LOW, 8(BUF)
	clmul_low X1K1LOW, K1, X1HIGH
	ld BUF2HIGH, 16(BUF)
	clmul_low X0K1LOW, K1, X0HIGH
	ld BUF2LOW, 24(BUF)
	clmul_high X3HIGH, K1, X3HIGH
	ld BUF1HIGH, 32(BUF)
	clmul_high X2HIGH, K1, X2HIGH
	ld BUF1LOW, 40(BUF)
	clmul_high X1HIGH, K1, X1HIGH
	ld BUF0HIGH, 48(BUF)
	clmul_high X0HIGH, K1, X0HIGH
	ld BUF0LOW, 56(BUF)

	addi BUF, BUF, 64

	/* endianness swap */
.if \endianswap
	rev8 BUF3HIGH, BUF3HIGH
	rev8 BUF3LOW, BUF3LOW
	rev8 BUF2HIGH, BUF2HIGH
	rev8 BUF2LOW, BUF2LOW
	rev8 BUF1HIGH, BUF1HIGH
	rev8 BUF1LOW, BUF1LOW
	rev8 BUF0HIGH, BUF0HIGH
	rev8 BUF0LOW, BUF0LOW
.endif

	/* carryless multiply each low doubleword by k2 */
	clmul_high X3K2HIGH, K2, X3LOW
	clmul_high X2K2HIGH, K2, X2LOW
	clmul_high X1K2HIGH, K2, X1LOW
	clmul_high X0K2HIGH, K2, X0LOW
	clmul_low X3LOW, K2, X3LOW
	clmul_low X2LOW, K2, X2LOW
	clmul_low X1LOW, K2, X1LOW
	clmul_low X0LOW, K2, X0LOW

	/* xor results together */
	xor BUF3LOW, BUF3LOW, X3K1LOW
	xor BUF2LOW, BUF2LOW, X2K1LOW
	xor BUF1LOW, BUF1LOW, X1K1LOW
	xor BUF0LOW, BUF0LOW, X0K1LOW
	xor X3HIGH, BUF3HIGH, X3HIGH
	xor X2HIGH, BUF2HIGH, X2HIGH
	xor X1HIGH, BUF1HIGH, X1HIGH
	xor X0HIGH, BUF0HIGH, X0HIGH
	xor X3LOW, X3LOW, BUF3LOW
	xor X2LOW, X2LOW, BUF2LOW
	xor X1LOW, X1LOW, BUF1LOW
	xor X0LOW, X0LOW, BUF0LOW
	xor X3HIGH, X3K2HIGH, X3HIGH
	xor X2HIGH, X2K2HIGH, X2HIGH
	xor X1HIGH, X1K2HIGH, X1HIGH
	xor X0HIGH, X0K2HIGH, X0HIGH

	bne BUF, a3, .fold_4_loop

	/* we've four folded as much as we can, fold-by-one values in regs */
	/* load fold-by-one constants */
	ld K3, .k3
	ld K4, .k4

	clmul_high s0, K3, X3HIGH
	clmul_low s1, K3, X3HIGH
	clmul_high s2, K4, X3LOW
	clmul_low s3, K4, X3LOW
	xor HIGH, X2HIGH, s0
	xor HIGH, HIGH, s2
	xor LOW, X2LOW, s1
	xor LOW, LOW, s3

	clmul_high s0, K3, HIGH
	clmul_low s1, K3, HIGH
	clmul_high s2, K4, LOW
	clmul_low s3, K4, LOW
	xor HIGH, X1HIGH, s0
	xor HIGH, HIGH, s2
	xor LOW, X1LOW, s1
	xor LOW, LOW, s3

	clmul_high s0, K3, HIGH
	clmul_low s1, K3, HIGH
	clmul_high s2, K4, LOW
	clmul_low s3, K4, LOW
	xor HIGH, X0HIGH, s0
	xor HIGH, HIGH, s2
	xor LOW, X0LOW, s1
	xor LOW, LOW, s3

	/* pop register values saved on stack */
	ld a3, 128(sp)
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
	addi sp, sp, 136

	/* load fold loop constant, check if any more 1-folding to do */
	li t4, 16
	bgeu LEN, t4, .fold_1_loop
	/* else jump straight to end */
	j .fold_1_cleanup

.fold_1:
	li t4, 16 /* kept throughout loop */
	/* handle case where not enough buffer to do any fold */
	/* .fold_1_done must be defined by the crc32/64 fold reduction macro */
	bltu LEN, t4, .fold_1_done

	/* load in initial values and xor with seed */
	ld HIGH, 0(BUF)
.if \endianswap
	rev8 HIGH, HIGH
.endif

.if (\bits != 64) && \endianswap
	slli SEED, SEED, 64 - \bits
.endif
	xor HIGH, HIGH, SEED

	ld LOW, 8(BUF)
.if \endianswap
	rev8 LOW, LOW
.endif

	addi LEN, LEN, -16
	addi BUF, BUF, 16

	bltu a2, t4, .fold_1_cleanup

	/* precomputed constants */
	ld K3, .k3
	ld K4, .k4
.fold_1_loop:
	/* multiply high and low by constants to get two 128-bit result */
	clmul_high t2, K3, HIGH
	clmul_low t3, K3, HIGH
	clmul_high a5, K4, LOW
	clmul_low a6, K4, LOW

	/* load next 128-bits of buffer */
	ld HIGH, 0(BUF)
	ld LOW, 8(BUF)
.if \endianswap
	rev8 HIGH, HIGH
	rev8 LOW, LOW
.endif

	addi LEN, LEN, -16
	addi BUF, BUF, 16

	/* fold in values with xor */
	xor HIGH, HIGH, t2
	xor HIGH, HIGH, a5
	xor LOW, LOW, t3
	xor LOW, LOW, a6

	bgeu LEN, t4, .fold_1_loop

.fold_1_cleanup:
.endm