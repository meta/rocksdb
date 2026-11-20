//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  This file provides runtime crypto-extension detection and CRC32C
//  computation for RISC-V. It probes three extensions independently at runtime
//  (cached, with fallback):
//    - Zbc:  scalar carry-less multiply - the base CRC path.
//    - Zvbc: vector carry-less multiply - the accelerated path (VLEN=128 only).
//    - Zbb:  basic bit-manipulation (rev8) - required by the Zvbc path.
//  The Zvbc path is used only when Zvbc+Zbb are available and VLEN is 128 bits
//  (the Zvbc asm is VLEN=128-tuned); otherwise the Zbc scalar path is used.
//  VLEN is read at runtime via the vlenb CSR.
//
//  Detection methods (in order of preference):
//    1. riscv_hwprobe syscall (Linux 6.4+) - official kernel API
//    2. /proc/cpuinfo parsing - fallback for older kernels
//
//  Public API:
//    - crc32c_riscv_zbc_runtime_check(): Runtime Zbc availability check
//    - crc32c_riscv_zvbc_available():    Runtime Zvbc availability check
//    - crc32c_riscv_zbb_available():     Runtime Zbb availability check
//    - crc32c_riscv(): CRC32C computation (Zvbc path when Zvbc+Zbb are
//                      available, otherwise the Zbc scalar path)
//
//  The actual CRC computation is implemented in crc32c_riscv_asm.S (Zbc) and
//  crc32c_riscv_zvbc_asm.S (Zvbc) using the folding algorithm with CLMUL
//  instructions.

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
#define RISCV_HWPROBE_EXT_ZVBC           (1 << 18)
#define RISCV_HWPROBE_EXT_ZBB            (1 << 5)
#endif

// Minimum kernel version for hwprobe support (Linux 6.4)
#define REQ_KERNEL_MAJOR 6
#define REQ_KERNEL_MINOR 4

// riscv_hwprobe structure definition
struct riscv_hwprobe {
    __s64 key;
    __u64 value;
};

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
 * Detect crypto extensions using riscv_hwprobe syscall
 */
static int detect_with_hwprobe(int* has_zbc, int* has_zvbc, int* has_zbb) {
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
    *has_zvbc = (probe.value & RISCV_HWPROBE_EXT_ZVBC) ? 1 : 0;
    *has_zbb = (probe.value & RISCV_HWPROBE_EXT_ZBB) ? 1 : 0;
    return 0;
#else
    return -1;
#endif
}

/**
 * Check for a RISC-V extension via /proc/cpuinfo
 */
static int check_riscv_extension(const char* extension) {
    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (!cpuinfo) {
        return 0;
    }

    char line[512];
    int found = 0;

    while (fgets(line, sizeof(line), cpuinfo)) {
        if (strncmp(line, "isa", 3) == 0) {
            if (strstr(line, extension) != NULL) {
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
static int detect_crypto_extensions(int* has_zbc, int* has_zvbc, int* has_zbb) {
    if (has_zbc == NULL || has_zvbc == NULL || has_zbb == NULL) {
        return -1;
    }

    static int cached_zbc = -1;
    static int cached_zvbc = -1;
    static int cached_zbb = -1;

    // Return cached results if available
    if (cached_zbc != -1 && cached_zvbc != -1 && cached_zbb != -1) {
        *has_zbc = cached_zbc;
        *has_zvbc = cached_zvbc;
        *has_zbb = cached_zbb;
        return 0;
    }

    // Initialize to not found
    *has_zbc = 0;
    *has_zvbc = 0;
    *has_zbb = 0;

    if (detect_with_hwprobe(has_zbc, has_zvbc, has_zbb) < 0) {
        *has_zbc = check_riscv_extension("zbc");
        *has_zvbc = check_riscv_extension("zvbc");
        *has_zbb = check_riscv_extension("zbb");
    }

    cached_zbc = *has_zbc;
    cached_zvbc = *has_zvbc;
    cached_zbb = *has_zbb;
    return 0;
}

#if defined(HAVE_RISCV_ZVBC)
// Read VLEN (vector register length) in bits via the vlenb CSR. Must only be
// called when the V extension is enabled (i.e., when Zvbc is available),
// otherwise it would SIGILL.
static unsigned read_vlen_bits(void) {
    unsigned long vlenb;
    __asm__ volatile("csrr %0, vlenb" : "=r"(vlenb));
    return (unsigned)(vlenb * 8);
}
#endif  // HAVE_RISCV_ZVBC

// Assembly function declarations - match signatures in the .S files
extern "C" uint32_t crc32c_riscv_zbc(const unsigned char *buffer, int len,
                                      uint32_t init_crc);

#if defined(HAVE_RISCV_ZVBC)
// crc32c_riscv_zvbc handles CRC inversion internally.
extern "C" uint32_t crc32c_riscv_zvbc(const uint8_t *buffer, size_t len,
                                      uint32_t init_crc);
#endif

// Cached detection results
static bool s_has_zbc_cached = false;
static bool s_has_zvbc_cached = false;
static bool s_has_zbb_cached = false;
static unsigned s_vlen_bits = 0;  // VLEN in bits; meaningful only when zvbc is available
static bool s_detection_done = false;

/**
 * Perform detection once and cache results
 */
static void detect_once(void) {
    if (s_detection_done) {
        return;
    }
    int has_zbc = 0;
    int has_zvbc = 0;
    int has_zbb = 0;
    if (detect_crypto_extensions(&has_zbc, &has_zvbc, &has_zbb) == 0) {
        s_has_zbc_cached = (has_zbc != 0);
        s_has_zvbc_cached = (has_zvbc != 0);
        s_has_zbb_cached = (has_zbb != 0);
#if defined(HAVE_RISCV_ZVBC)
        // Zvbc implies V is enabled, so reading vlenb here is safe.
        if (s_has_zvbc_cached) {
            s_vlen_bits = read_vlen_bits();
        }
#endif
    }
    s_detection_done = true;
}

extern "C" {

/**
 * Runtime Zbc availability check.
 */
bool crc32c_riscv_zbc_runtime_check(void) {
    detect_once();
    return s_has_zbc_cached;
}

#if defined(HAVE_RISCV_ZVBC)
/**
 * Runtime Zvbc availability check (vector carry-less multiply).
 */
bool crc32c_riscv_zvbc_available(void) {
    detect_once();
    return s_has_zvbc_cached;
}
#endif

#if defined(HAVE_RISCV_ZBB)
/**
 * Runtime Zbb availability check (basic bit-manipulation, e.g. rev8). Required
 * together with Zvbc for the vector CRC path.
 */
bool crc32c_riscv_zbb_available(void) {
    detect_once();
    return s_has_zbb_cached;
}
#endif

/**
 * RISC-V CRC32C computation. Takes the Zvbc vector path (128 bytes/iter) when
 * both Zvbc and Zbb are available at runtime; otherwise falls back to the Zbc
 * scalar path (64 bytes/iter).
 */
#ifdef ROCKSDB_UBSAN_RUN
#if defined(__clang__)
__attribute__((__no_sanitize__("alignment")))
#elif defined(__GNUC__)
__attribute__((__no_sanitize_undefined__))
#endif
#endif

uint32_t crc32c_riscv(uint32_t crc, const unsigned char *data, size_t len) {
    const size_t kMaxChunk = 1024 * 1024 * 1024;
    detect_once();

    // Convert masked CRC to pure CRC for the assembly routines, then back.
    uint32_t c = crc ^ 0xffffffff;
#if defined(HAVE_RISCV_ZVBC)
    if (s_has_zvbc_cached && s_has_zbb_cached && s_vlen_bits == 128) {
        // Zvbc vectorized path: 128 bytes per loop iteration.
        while (len > 0) {
            size_t chunk_len = (len > kMaxChunk) ? kMaxChunk : len;
            c = crc32c_riscv_zvbc(data, chunk_len, c);
            data += chunk_len;
            len -= chunk_len;
        }
        return c ^ 0xffffffff;
    } else
#endif
    {
        // Zbc scalar path: 64 bytes per loop iteration.
        while (len > 0) {
            int chunk_len = (len > kMaxChunk) ? (int)kMaxChunk : (int)len;
            c = crc32c_riscv_zbc(data, chunk_len, c);
            data += chunk_len;
            len -= chunk_len;
        }
        return c ^ 0xffffffff;
    }
}

}  // extern "C"
#endif  // HAVE_RISCV_ZBC
