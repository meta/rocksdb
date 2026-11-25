//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// RISC-V CRC32C reflected polynomial reduction macros
//
// This header provides macros for the final reduction and byte-level processing
// of CRC32C using the reflected polynomial (iSCSI CRC32C: 0x82F63B78).
// It complements crc_fold_riscv_clmul.h which handles the main folding loop.
//
// Macros provided:
//   - crc32_refl_fold_reduction: Final 128-bit to 32-bit reduction after folding
//   - barrett_reduce: Barrett reduction algorithm for arbitrary bit widths
//   - crc32_refl_align: Align buffer to 64-bit boundary
//   - crc32_refl_excess: Process remaining bytes (< 16 bytes) after folding
//
// This file is included by crc32c_riscv_asm.S

#include "crc_fold_riscv_clmul.h"

/* folding reflected final reduction */
/* expects 128-bit value in HIGH:LOW (t0:t1), puts return value in SEED (a0) */
/* trashes t2, t3, a5, a6 and t5, t6 */
.macro crc32_refl_fold_reduction
	/* load precalculated constants */
	ld K4, .k4
	ld K5, .k5

	/* fold remaining 128 bits into 96 */
	clmul t3, K4, t0
	xor t1, t3, t1
	clmulh t0, K4, t0

	/* high = (low >> 32) | (high << 32) */
	slli t0, t0, 32
	srli t3, t1, 32
	or t0, t0, t3

	/* fold last 96 bits into 64 */
	slli t1, t1, 32
	srli t1, t1, 32
	clmul t1, K5, t1
	xor t1, t1, t0

	/* barrett's reduce 64 bits */
	clmul t0, MU, t1
	slli t0, t0, 32
	srli t0, t0, 32
	clmul t0, POLY, t0
	xor t0, t1, t0
	srli SEED, t0, 32

.fold_1_done:
.endm

/* barrett's reduction on a \bits bit-length value, returning result in seed */
/* bits must be 64, 32, 16 or 8 */
/* value and seed must be zero-extended */
.macro barrett_reduce seed:req, value:req, bits:req
	/* combine value with seed */
	xor t0, \seed, \value
.if (\bits < 64)
	slli t0, t0, (64 - \bits)
.endif

	/* multiply by mu, which is 2^96 divided by our polynomial */
	clmul t0, t0, MU

.if (\bits == 16) || (\bits == 8)
	clmulh t0, t0, POLY
	/* subtract from original for smaller sizes */
	srli t1, \seed, \bits
	xor \seed, t0, t1
.else
	clmulh \seed, t0, POLY
.endif

.endm

/* align buffer to 64-bits updating seed */
/* expects SEED (a0), BUF (a1), LEN (a2), MU (a3), POLY (a4) to hold values */
/* expects crc32_refl_excess to be called later */
/* trashes t0 and t1 */
.macro crc32_refl_align
	/* is buffer already aligned to 128-bits? */
	andi t0, BUF, 0b111
	beqz t0, .align_done

.align_8:
	/* is enough buffer left? */
	li t0, 1
	bltu LEN, t0, .excess_done

	/* is buffer misaligned by one byte? */
	andi t0, BUF, 0b001
	beqz t0, .align_16

	/* perform barrett's reduction on one byte */
	lbu t1, (BUF)
	barrett_reduce SEED, t1, 8
	addi LEN, LEN, -1
	addi BUF, BUF, 1

.align_16:
	li t0, 2
	bltu LEN, t0, .excess_8

	andi t0, BUF, 0b010
	beqz t0, .align_32

	lhu t1, (BUF)
	barrett_reduce SEED, t1, 16
	addi LEN, LEN, -2
	addi BUF, BUF, 2

.align_32:
	li t0, 4
	bltu LEN, t0, .excess_16

	andi t0, BUF, 0b100
	beqz t0, .align_done

	lwu t1, (BUF)
	barrett_reduce SEED, t1, 32
	addi LEN, LEN, -4
	addi BUF, BUF, 4

.align_done:
.endm

/* barrett's reduce excess buffer left following fold */
/* expects SEED (a0), BUF (a1), LEN (a2), MU (a3), POLY (a4) to hold values */
/* expects less than 127 bits to be left in doubleword-aligned buffer */
/* trashes t0, t1 and t3 */
.macro crc32_refl_excess
	/* do we have any excess left? */
	beqz LEN, .excess_done

	/* barret's reduce the remaining excess */
	/* at most there is 127 bytes left */
.excess_64:
	andi t0, LEN, 0b1000
	beqz t0, .excess_32
	ld t1, (BUF)
	barrett_reduce SEED, t1, 64
	addi BUF, BUF, 8

.excess_32:
	andi t0, LEN, 0b0100
	beqz t0, .excess_16
	lwu t1, (BUF)
	barrett_reduce SEED, t1, 32
	addi BUF, BUF, 4

.excess_16:
	andi t0, LEN, 0b0010
	beqz t0, .excess_8
	lhu t1, (BUF)
	barrett_reduce SEED, t1, 16
	addi BUF, BUF, 2

.excess_8:
	andi t0, LEN, 0b0001
	beqz t0, .excess_done
	lbu t1, (BUF)
	barrett_reduce SEED, t1, 8

.excess_done:
.endm