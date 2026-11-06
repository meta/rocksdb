#ifndef __AARCH64_LABEL_H__
#define __AARCH64_LABEL_H__

#ifdef __USER_LABEL_PREFIX__
#define CONCAT1(a, b) CONCAT2(a, b)
#define CONCAT2(a, b) a ## b
#define cdecl(x) CONCAT1 (__USER_LABEL_PREFIX__, x)
#else
#define cdecl(x) x
#endif

#ifdef __APPLE__
#define ASM_DEF_RODATA .section	__TEXT,__const
#else
#define ASM_DEF_RODATA .section .rodata
#endif

#endif
