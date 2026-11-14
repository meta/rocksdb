//  Copyright (c) 2018, Arm Limited and affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/crc32c_arm64.h"

#if defined(HAVE_ARM64_CRC)

#if defined(__linux__)
#include <asm/hwcap.h>
#endif
#if defined(ROCKSDB_AUXV_GETAUXVAL_PRESENT) || \
    defined(ROCKSDB_AUXV_ELF_AUX_INFO_PRESENT)
#include <sys/auxv.h>
#ifndef HWCAP_CRC32
#define HWCAP_CRC32 (1 << 7)
#endif
#ifndef HWCAP_PMULL
#define HWCAP_PMULL (1 << 4)
#endif
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#elif defined(__OpenBSD__)
// clang-format off
#include <machine/armreg.h>
#include <machine/cpu.h>
#include <sys/types.h>
#include <sys/sysctl.h>
// clang-format on
#endif

#ifdef HAVE_ARM64_CRYPTO
/* unfolding to compute 8 * 3 = 24 bytes parallelly */
#define CRC32C24BYTES(ITR)                                    \
  crc1 = crc32c_u64(crc1, *(buf64 + BLK_LENGTH + (ITR)));     \
  crc2 = crc32c_u64(crc2, *(buf64 + BLK_LENGTH * 2 + (ITR))); \
  crc0 = crc32c_u64(crc0, *(buf64 + (ITR)));

/* unfolding to compute 24 * 7 = 168 bytes parallelly */
#define CRC32C7X24BYTES(ITR)     \
  do {                           \
    CRC32C24BYTES((ITR) * 7 + 0) \
    CRC32C24BYTES((ITR) * 7 + 1) \
    CRC32C24BYTES((ITR) * 7 + 2) \
    CRC32C24BYTES((ITR) * 7 + 3) \
    CRC32C24BYTES((ITR) * 7 + 4) \
    CRC32C24BYTES((ITR) * 7 + 5) \
    CRC32C24BYTES((ITR) * 7 + 6) \
  } while (0)
#endif

extern bool pmull_runtime_flag;

uint32_t crc32c_runtime_check(void) {
#if defined(ROCKSDB_AUXV_GETAUXVAL_PRESENT) || \
    defined(ROCKSDB_AUXV_ELF_AUX_INFO_PRESENT)
  uint64_t auxv = 0;
#if defined(ROCKSDB_AUXV_GETAUXVAL_PRESENT)
  auxv = getauxval(AT_HWCAP);
#elif defined(ROCKSDB_AUXV_ELF_AUX_INFO_PRESENT)
  elf_aux_info(AT_HWCAP, &auxv, sizeof(auxv));
#endif
  return (auxv & HWCAP_CRC32) != 0;
#elif defined(__APPLE__)
  int r;
  size_t l = sizeof(r);
  if (sysctlbyname("hw.optional.armv8_crc32", &r, &l, NULL, 0) == -1) return 0;
  return r == 1;
#elif defined(__OpenBSD__)
  int r = 0;
  const int isar0_mib[] = {CTL_MACHDEP, CPU_ID_AA64ISAR0};
  uint64_t isar0;
  size_t len = sizeof(isar0);

  if (sysctl(isar0_mib, 2, &isar0, &len, NULL, 0) != -1) {
    if (ID_AA64ISAR0_CRC32(isar0) >= ID_AA64ISAR0_CRC32_BASE) r = 1;
  }
  return r;
#else
  return 0;
#endif
}

bool crc32c_pmull_runtime_check(void) {
#if defined(ROCKSDB_AUXV_GETAUXVAL_PRESENT) || \
    defined(ROCKSDB_AUXV_ELF_AUX_INFO_PRESENT)
  uint64_t auxv = 0;
#if defined(ROCKSDB_AUXV_GETAUXVAL_PRESENT)
  auxv = getauxval(AT_HWCAP);
#elif defined(ROCKSDB_AUXV_ELF_AUX_INFO_PRESENT)
  elf_aux_info(AT_HWCAP, &auxv, sizeof(auxv));
#endif
  return (auxv & HWCAP_PMULL) != 0;
#elif defined(__APPLE__)
  return true;
#elif defined(__OpenBSD__)
  bool r = false;
  const int isar0_mib[] = {CTL_MACHDEP, CPU_ID_AA64ISAR0};
  uint64_t isar0;
  size_t len = sizeof(isar0);

  if (sysctl(isar0_mib, 2, &isar0, &len, NULL, 0) != -1) {
    if (ID_AA64ISAR0_AES(isar0) >= ID_AA64ISAR0_AES_PMULL) r = true;
  }
  return r;
#else
  return false;
#endif
}

#ifdef ROCKSDB_UBSAN_RUN
#if defined(__clang__)
__attribute__((__no_sanitize__("alignment")))
#elif defined(__GNUC__)
__attribute__((__no_sanitize_undefined__))
#endif
#endif
uint32_t crc32c_arm64(uint32_t crc, unsigned char const* data, size_t len) {
  const uint8_t* buf8;
  const uint64_t* buf64 = (uint64_t*)data;
  int length = (int)len;
  crc ^= 0xffffffff;

  /*
   * Pmull runtime check here.
   * Raspberry Pi supports crc32 but doesn't support pmull.
   * Skip Crc32c Parallel computation if no crypto extension available.
   */
  if (pmull_runtime_flag) {
/* Macro (HAVE_ARM64_CRYPTO) is used for compiling check  */
#ifdef HAVE_ARM64_CRYPTO
/* Crc32c Parallel computation
 *   Algorithm comes from Intel whitepaper:
 *   crc-iscsi-polynomial-crc32-instruction-paper
 *
 * Input data is divided into three equal-sized blocks
 *   Three parallel blocks (crc0, crc1, crc2) for 1024 Bytes
 *   One Block: 42(BLK_LENGTH) * 8(step length: crc32c_u64) bytes
 */
#define BLK_LENGTH 42
    while (length >= 1024) {
      uint64_t t0, t1;
      uint32_t crc0 = 0, crc1 = 0, crc2 = 0;

      /* Parallel Param:
       *   k0 = CRC32(x ^ (42 * 8 * 8 * 2 - 1));
       *   k1 = CRC32(x ^ (42 * 8 * 8 - 1));
       */
      uint32_t k0 = 0xe417f38a, k1 = 0x8f158014;

      /* Prefetch data for following block to avoid cache miss */
      PREF1KL1((uint8_t*)buf64, 1024);

      /* First 8 byte for better pipelining */
      crc0 = crc32c_u64(crc, *buf64++);

      /* 3 blocks crc32c parallel computation
       * Macro unfolding to compute parallelly
       * 168 * 6 = 1008 (bytes)
       */
      CRC32C7X24BYTES(0);
      CRC32C7X24BYTES(1);
      CRC32C7X24BYTES(2);
      CRC32C7X24BYTES(3);
      CRC32C7X24BYTES(4);
      CRC32C7X24BYTES(5);
      buf64 += (BLK_LENGTH * 3);

      /* Last 8 bytes */
      crc = crc32c_u64(crc2, *buf64++);

      t0 = (uint64_t)vmull_p64(crc0, k0);
      t1 = (uint64_t)vmull_p64(crc1, k1);

      /* Merge (crc0, crc1, crc2) -> crc */
      crc1 = crc32c_u64(0, t1);
      crc ^= crc1;
      crc0 = crc32c_u64(0, t0);
      crc ^= crc0;

      length -= 1024;
    }

    if (length == 0) return crc ^ (0xffffffffU);
#endif
  }  // if Pmull runtime check here

  buf8 = (const uint8_t*)buf64;
  while (length >= 8) {
    crc = crc32c_u64(crc, *(const uint64_t*)buf8);
    buf8 += 8;
    length -= 8;
  }

  /* The following is more efficient than the straight loop */
  if (length >= 4) {
    crc = crc32c_u32(crc, *(const uint32_t*)buf8);
    buf8 += 4;
    length -= 4;
  }

  if (length >= 2) {
    crc = crc32c_u16(crc, *(const uint16_t*)buf8);
    buf8 += 2;
    length -= 2;
  }

  if (length >= 1) crc = crc32c_u8(crc, *buf8);

  crc ^= 0xffffffff;
  return crc;
}

#endif
