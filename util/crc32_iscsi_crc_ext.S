	.text
	.align		6
	.arch		armv8-a+crc


#include "crc32_aarch64_common.h"
	BUF		.req	x0
	LEN		.req	x1
	wCRC		.req	w2
	data0		.req	x4
	data1		.req	x5
	data2		.req	x6
	data3		.req	x7
	wdata		.req	w3
.macro	crc32_u64	dst,src,data
	crc32cx		\dst,\src,\data
.endm
.macro	crc32_u32	dst,src,data
	crc32cw		\dst,\src,\data
.endm
.macro	crc32_u16	dst,src,data
	crc32ch		\dst,\src,\data
.endm
.macro	crc32_u8	dst,src,data
	crc32cb		\dst,\src,\data
.endm

	/**
	 * uint32_t crc32_iscsi_crc_ext(const unsigned char *BUF,
	 *                  uint64_t LEN,uint32_t wCRC);
	 */
	.global	cdecl(crc32_iscsi_crc_ext)
#ifndef __APPLE__
	.type	crc32_iscsi_crc_ext, %function
#endif
cdecl(crc32_iscsi_crc_ext):
	crc32_hw_common	crc32c
#ifndef __APPLE__
	.size	crc32_iscsi_crc_ext, .-crc32_iscsi_crc_ext
#endif
