//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// RISC-V CRC32C assembly implementation using Zbc CLMUL extension
//
// This file implements the core CRC32C computation function using the RISC-V
// Zbc extension's carry-less multiply (CLMUL) instructions. The algorithm is
// based on the Intel CRC32 implementation with PCLMULQDQ, adapted for RISC-V.
//
// Function: crc32c_riscv_zbc(buffer, len, init_crc)
//   - buffer:   Pointer to input data (a0 -> a1 after arg shuffle)
//   - len:      Length of data in bytes (a1 -> a2 after arg shuffle)
//   - init_crc: Initial CRC value (a2 -> a0 after arg shuffle)
//   - Returns:  Computed CRC32C value in a0
//
// Algorithm:
//   1. Align buffer to 64-bit boundary (crc32_refl_align)
//   2. Fold 128-bit chunks using CLMUL (crc_fold_loop from crc_fold_riscv_clmul.h)
//   3. Reduce 128-bit result to 32-bit CRC (crc32_refl_fold_reduction)
//   4. Process remaining bytes (crc32_refl_excess)
//
// Requires: RISC-V Zbc extension (enabled via .option arch, +zbc)

#include "crc32_refl_riscv_clmul.h"

/* uint32_t crc32c_riscv_zbc(uint8_t *buffer, int len, uint32_t init_crc) */
.text
.option arch, +zbc
.align 1
.global crc32c_riscv_zbc
.type crc32c_riscv_zbc, %function
crc32c_riscv_zbc:
	/* switch around arguments to match common crc functions */
	mv t0, a2
	mv a2, a1
	mv a1, a0
	mv a0, t0

	/* load precomputed constants */
	ld POLY, .poly_refl
	ld MU, .mu

	/* zero-extend seed */
	slli SEED, SEED, 32
	srli SEED, SEED, 32

	/* align buffer to 128-bits, then fold */
	crc32_refl_align

	crc_fold_loop 32 0 1
	crc32_refl_fold_reduction

	/* handle any remaining excess */
	crc32_refl_excess

	/* sign-extend result */
	sext.w SEED, SEED
	ret

/* precomputed constants */
.poly_refl:
	.dword 0x0000000105ec76f1
.mu:
	.dword 0x4869ec38dea713f1
.k1:
	.dword 0x00000000740eef02
.k2:
	.dword 0x000000009e4addf8
.k3:
	.dword 0x00000000f20c0dfe
.k4:
	.dword 0x000000014cd00bd6
.k5:
	.dword 0x00000000dd45aab8

