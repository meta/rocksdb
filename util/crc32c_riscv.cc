//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  This file provides runtime Zbc extension detection and CRC32C computation
//  using the RISC-V Zbc (carry-less multiply) extension. The detection logic
//  supports multiple methods with fallback:
//
//  Detection methods (in order of preference):
//    1. riscv_hwprobe syscall (Linux 6.4+) - official kernel API
//    2. /proc/cpuinfo parsing - fallback for older kernels
//
//  Public API:
//    - crc32c_riscv_zbc_runtime_check(): Runtime Zbc availability check
//    - crc32c_riscv(): CRC32C computation using Zbc CLMUL instructions
//
//  The actual CRC computation is implemented in crc32c_riscv_asm.S using
//  the folding algorithm with CLMUL instructions from the Zbc extension.

#include "util/crc32c_riscv.h"

#ifdef HAVE_RISCV_ZBC

#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <linux/types.h>

//riscv_hwprobe definitions for compatibility with older kernels
#ifndef __NR_riscv_hwprobe
#if defined(__riscv) && __riscv_xlen == 64
  #define __NR_riscv_hwprobe 258
#endif
#endif

// hwprobe key definitions for extension detection
#ifndef RISCV_HWPROBE_KEY_IMA_EXT_0
#define RISCV_HWPROBE_KEY_IMA_EXT_0      4
#define RISCV_HWPROBE_EXT_ZBC            (1 << 7)
#endif

// Minimum kernel version for hwprobe support (Linux 6.4)
#define REQ_KERNEL_MAJOR 6
#define REQ_KERNEL_MINOR 4

// riscv_hwprobe structure definition
struct riscv_hwprobe {
    __s64 key;
    __u64 value;
};

// Cached result for Zbc availability (-1 = not yet checked, 0 = no, 1 = yes)
static int zbc_available_cached = -1;

/**
 * Parse kernel version from uname
 */
static int parse_kernel_version(int* major, int* minor, int* patch) {
    struct utsname uts;
    if (uname(&uts) != 0) {
        return -1;
    }

    *patch = 0;
    if (sscanf(uts.release, "%d.%d.%d", major, minor, patch) >= 2) {
        return 0;
    }

    return -1;
}

/**
 * Check if kernel version supports hwprobe (Linux 6.4+)
 */
static int kernel_supports_hwprobe(void) {
    int major, minor, patch;
    if (parse_kernel_version(&major, &minor, &patch) != 0) {
        return 0;
    }

    return (major > REQ_KERNEL_MAJOR) ||
           (major == REQ_KERNEL_MAJOR && minor >= REQ_KERNEL_MINOR);
}


/**
 * Detect ZBC extension using riscv_hwprobe syscall
 */
static int detect_with_hwprobe(int* has_zbc) {
#ifdef __NR_riscv_hwprobe
    if (!kernel_supports_hwprobe()) {
        return -1;
    }

    struct riscv_hwprobe probe;
    probe.key = RISCV_HWPROBE_KEY_IMA_EXT_0;
    probe.value = 0;

    long ret = syscall(__NR_riscv_hwprobe, &probe, 1, 0, NULL, 0);
    if (ret != 0) {
        return -1;
    }

    *has_zbc = (probe.value & RISCV_HWPROBE_EXT_ZBC) ? 1 : 0;
    return 0;
#else
    return -1;
#endif
}

/**
 * Check for ZBC extension via /proc/cpuinfo
 */
static int check_riscv_zbc_cpuinfo(void) {
    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (!cpuinfo) {
        return 0;
    }

    char line[512];
    int found = 0;

    while (fgets(line, sizeof(line), cpuinfo)) {
        if (strncmp(line, "isa", 3) == 0) {
            if (strstr(line, "_zbc") != NULL) {
                found = 1;
                break;
            }
        }
    }

    fclose(cpuinfo);
    return found;
}

/**
 * Main detection function with caching and multiple fallback methods
 */
static int detect_zbc_extension(int* has_zbc) {
    if (has_zbc == NULL) {
        return -1;
    }

    // Return cached result if available
    if (zbc_available_cached != -1) {
        *has_zbc = zbc_available_cached;
        return 0;
    }

    // Initialize to not found
    *has_zbc = 0;

    if (detect_with_hwprobe(has_zbc) == 0) {
        zbc_available_cached = *has_zbc;
        return 0;
    }


    *has_zbc = check_riscv_zbc_cpuinfo();
    zbc_available_cached = *has_zbc;
    return 0;
}


// Assembly function declaration - matches signature in crc32c_riscv_asm.S
extern "C" uint32_t crc32c_riscv_zbc(const unsigned char *buffer, int len, uint32_t init_crc);

extern "C" {

/**
 * Runtime Zbc extension detection
 */
bool crc32c_riscv_zbc_runtime_check(void) {
    int has_zbc = 0;
    return (detect_zbc_extension(&has_zbc) == 0 && has_zbc);
}

/**
 * RISC-V CRC32C computation using Zbc CLMUL instructions
 */
#ifdef ROCKSDB_UBSAN_RUN
#if defined(__clang__)
__attribute__((__no_sanitize__("alignment")))
#elif defined(__GNUC__)
__attribute__((__no_sanitize_undefined__))
#endif
#endif

uint32_t crc32c_riscv(uint32_t crc, const unsigned char *data, size_t len) {
    uint32_t c = crc ^ 0xffffffff;
    const size_t kMaxChunk = 1024 * 1024 * 1024;
    while (len > 0) {
        int chunk_len = (len > kMaxChunk) ? (int)kMaxChunk : (int)len;

        // Call the underlying RISC-V assembly implementation
        c = crc32c_riscv_zbc(data, chunk_len, c);

        data += chunk_len;
        len -= chunk_len;
    }
    return c ^ 0xffffffff;
}

}
#endif  // HAVE_RISCV_ZBC
