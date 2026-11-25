#include "aarch64_label.h"

.macro	crc32_hw_common		poly_type

.ifc	\poly_type,crc32
	mvn		wCRC,wCRC
.endif
	cbz		LEN, .zero_length_ret
	tbz		BUF, 0, .align_short
	ldrb		wdata,[BUF],1
	sub		LEN,LEN,1
	crc32_u8	wCRC,wCRC,wdata
.align_short:
	tst		BUF,2
	ccmp		LEN,1,0,ne
	bhi		.align_short_2
	tst		BUF,4
	ccmp		LEN,3,0,ne
	bhi		.align_word

.align_finish:

	cmp		LEN, 63
	bls		.loop_16B
.loop_64B:
	ldp		data0, data1, [BUF],#16
	prfm		pldl2keep,[BUF,2048]
	sub		LEN,LEN,#64
	ldp		data2, data3, [BUF],#16
	prfm		pldl1keep,[BUF,256]
	cmp		LEN,#64
	crc32_u64	wCRC, wCRC, data0
	crc32_u64	wCRC, wCRC, data1
	ldp		data0, data1, [BUF],#16
	crc32_u64	wCRC, wCRC, data2
	crc32_u64	wCRC, wCRC, data3
	ldp		data2, data3, [BUF],#16
	crc32_u64	wCRC, wCRC, data0
	crc32_u64	wCRC, wCRC, data1
	crc32_u64	wCRC, wCRC, data2
	crc32_u64	wCRC, wCRC, data3
	bge		.loop_64B

.loop_16B:
	cmp		LEN, 15
	bls		.less_16B
	ldp		data0, data1, [BUF],#16
	sub		LEN,LEN,#16
	cmp		LEN,15
	crc32_u64	wCRC, wCRC, data0
	crc32_u64	wCRC, wCRC, data1
	bls		.less_16B
	ldp		data0, data1, [BUF],#16
	sub		LEN,LEN,#16
	cmp		LEN,15
	crc32_u64	wCRC, wCRC, data0
	crc32_u64	wCRC, wCRC, data1
	bls		.less_16B
	ldp		data0, data1, [BUF],#16
	sub		LEN,LEN,#16   //MUST less than 16B
	crc32_u64	wCRC, wCRC, data0
	crc32_u64	wCRC, wCRC, data1
.less_16B:
	cmp		LEN, 7
	bls		.less_8B
	ldr		data0, [BUF], 8
	sub		LEN, LEN, #8
	crc32_u64	wCRC, wCRC, data0
.less_8B:
	cmp		LEN, 3
	bls		.less_4B
	ldr		wdata, [BUF], 4
	sub		LEN, LEN, #4
	crc32_u32	wCRC, wCRC, wdata
.less_4B:
	cmp		LEN, 1
	bls		.less_2B
	ldrh		wdata, [BUF], 2
	sub		LEN, LEN, #2
	crc32_u16	wCRC, wCRC, wdata
.less_2B:
	cbz		LEN, .zero_length_ret
	ldrb		wdata, [BUF]
	crc32_u8	wCRC, wCRC, wdata
.zero_length_ret:
.ifc	\poly_type,crc32
	mvn		w0, wCRC
.else
	mov		w0, wCRC
.endif
	ret
.align_short_2:
	ldrh		wdata, [BUF], 2
	sub		LEN, LEN, 2
	tst		BUF, 4
	crc32_u16	wCRC, wCRC, wdata
	ccmp		LEN, 3, 0, ne
	bls		.align_finish
.align_word:
	ldr		wdata, [BUF], 4
	sub		LEN, LEN, #4
	crc32_u32	wCRC, wCRC, wdata
	b .align_finish
.endm

.macro	crc32_3crc_fold poly_type
.ifc	\poly_type,crc32
	mvn		wCRC,wCRC
.endif
	cbz		LEN, .zero_length_ret
	tbz		BUF, 0, .align_short
	ldrb		wdata,[BUF],1
	sub		LEN,LEN,1
	crc32_u8	wCRC,wCRC,wdata
.align_short:
	tst		BUF,2
	ccmp		LEN,1,0,ne
	bhi		.align_short_2
	tst		BUF,4
	ccmp		LEN,3,0,ne
	bhi		.align_word

.align_finish:
	cmp	LEN,1023
	adr	const_adr, .Lconstants
	bls	1f
	ldp	dconst0,dconst1,[const_adr]
2:
	ldr		crc0_data0,[ptr_crc0],8
	prfm		pldl2keep,[ptr_crc0,3*1024-8]
	mov		crc1,0
	mov		crc2,0
	add		ptr_crc1,ptr_crc0,336
	add		ptr_crc2,ptr_crc0,336*2
	crc32_u64	crc0,crc0,crc0_data0
	.set		offset,0
	.set		ptr_offset,8
	.rept		5
	ldp		crc0_data0,crc0_data1,[ptr_crc0],16
	ldp		crc1_data0,crc1_data1,[ptr_crc1],16
	.set		offset,offset+64
	.set		ptr_offset,ptr_offset+16
	prfm		pldl2keep,[ptr_crc0,3*1024-ptr_offset+offset]
	crc32_u64	crc0,crc0,crc0_data0
	crc32_u64	crc0,crc0,crc0_data1
	ldp		crc2_data0,crc2_data1,[ptr_crc2],16
	crc32_u64	crc1,crc1,crc1_data0
	crc32_u64	crc1,crc1,crc1_data1
	crc32_u64	crc2,crc2,crc2_data0
	crc32_u64	crc2,crc2,crc2_data1
	.endr
	.set		l1_offset,0
	.rept		10
	ldp		crc0_data0,crc0_data1,[ptr_crc0],16
	ldp		crc1_data0,crc1_data1,[ptr_crc1],16
	.set		offset,offset+64
	.set		ptr_offset,ptr_offset+16
	prfm		pldl2keep,[ptr_crc0,3*1024-ptr_offset+offset]
	prfm		pldl1keep,[ptr_crc0,2*1024-ptr_offset+l1_offset]
	.set		l1_offset,l1_offset+64
	crc32_u64	crc0,crc0,crc0_data0
	crc32_u64	crc0,crc0,crc0_data1
	ldp		crc2_data0,crc2_data1,[ptr_crc2],16
	crc32_u64	crc1,crc1,crc1_data0
	crc32_u64	crc1,crc1,crc1_data1
	crc32_u64	crc2,crc2,crc2_data0
	crc32_u64	crc2,crc2,crc2_data1
	.endr

	.rept		6
	ldp		crc0_data0,crc0_data1,[ptr_crc0],16
	ldp		crc1_data0,crc1_data1,[ptr_crc1],16
	.set		ptr_offset,ptr_offset+16
	prfm		pldl1keep,[ptr_crc0,2*1024-ptr_offset+l1_offset]
	.set		l1_offset,l1_offset+64
	crc32_u64	crc0,crc0,crc0_data0
	crc32_u64	crc0,crc0,crc0_data1
	ldp		crc2_data0,crc2_data1,[ptr_crc2],16
	crc32_u64	crc1,crc1,crc1_data0
	crc32_u64	crc1,crc1,crc1_data1
	crc32_u64	crc2,crc2,crc2_data0
	crc32_u64	crc2,crc2,crc2_data1
	.endr
	ldr		crc2_data0,[ptr_crc2]
	fmov		dtmp0,xcrc0
	fmov		dtmp1,xcrc1
	crc32_u64	crc2,crc2,crc2_data0
	add		ptr_crc0,ptr_crc0,1024-(336+8)
	pmull		vtmp0.1q,vtmp0.1d,vconst0.1d
	sub		LEN,LEN,1024
	pmull		vtmp1.1q,vtmp1.1d,vconst1.1d
	cmp		LEN,1024
	fmov		xcrc0,dtmp0
	fmov		xcrc1,dtmp1
	crc32_u64	crc0,wzr,xcrc0
	crc32_u64	crc1,wzr,xcrc1

	eor		crc0,crc0,crc2
	eor		crc0,crc0,crc1

	bhs	2b
1:
	cmp		LEN, 63
	bls		.loop_16B
.loop_64B:
	ldp		data0, data1, [BUF],#16
	sub		LEN,LEN,#64
	ldp		data2, data3, [BUF],#16
	cmp		LEN,#64
	crc32_u64	wCRC, wCRC, data0
	crc32_u64	wCRC, wCRC, data1
	ldp		data0, data1, [BUF],#16
	crc32_u64	wCRC, wCRC, data2
	crc32_u64	wCRC, wCRC, data3
	ldp		data2, data3, [BUF],#16
	crc32_u64	wCRC, wCRC, data0
	crc32_u64	wCRC, wCRC, data1
	crc32_u64	wCRC, wCRC, data2
	crc32_u64	wCRC, wCRC, data3
	bge		.loop_64B

.loop_16B:
	cmp		LEN, 15
	bls		.less_16B
	ldp		data0, data1, [BUF],#16
	sub		LEN,LEN,#16
	cmp		LEN,15
	crc32_u64	wCRC, wCRC, data0
	crc32_u64	wCRC, wCRC, data1
	bls		.less_16B
	ldp		data0, data1, [BUF],#16
	sub		LEN,LEN,#16
	cmp		LEN,15
	crc32_u64	wCRC, wCRC, data0
	crc32_u64	wCRC, wCRC, data1
	bls		.less_16B
	ldp		data0, data1, [BUF],#16
	sub		LEN,LEN,#16   //MUST less than 16B
	crc32_u64	wCRC, wCRC, data0
	crc32_u64	wCRC, wCRC, data1
.less_16B:
	cmp		LEN, 7
	bls		.less_8B
	ldr		data0, [BUF], 8
	sub		LEN, LEN, #8
	crc32_u64	wCRC, wCRC, data0
.less_8B:
	cmp		LEN, 3
	bls		.less_4B
	ldr		wdata, [BUF], 4
	sub		LEN, LEN, #4
	crc32_u32	wCRC, wCRC, wdata
.less_4B:
	cmp		LEN, 1
	bls		.less_2B
	ldrh		wdata, [BUF], 2
	sub		LEN, LEN, #2
	crc32_u16	wCRC, wCRC, wdata
.less_2B:
	cbz		LEN, .zero_length_ret
	ldrb		wdata, [BUF]
	crc32_u8	wCRC, wCRC, wdata
.zero_length_ret:
.ifc	\poly_type,crc32
	mvn		w0, wCRC
.else
	mov		w0, wCRC
.endif
	ret
.align_short_2:
	ldrh		wdata, [BUF], 2
	sub		LEN, LEN, 2
	tst		BUF, 4
	crc32_u16	wCRC, wCRC, wdata
	ccmp		LEN, 3, 0, ne
	bls		.align_finish
.align_word:
	ldr		wdata, [BUF], 4
	sub		LEN, LEN, #4
	crc32_u32	wCRC, wCRC, wdata
	b .align_finish
.Lconstants:
.ifc	\poly_type,crc32
	.quad		0xb486819b
	.quad		0x76278617
.else
	.quad		0xe417f38a
	.quad		0x8f158014
.endif

.endm
