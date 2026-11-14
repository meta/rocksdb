	.arch armv8.5-a+crypto+crc+sve2-aes
	.file	"crc32_iscsi_sve2.c"
	.text
	.align	2
	.p2align 4,,11
	.global	crc32_iscsi_sve2
	.type	crc32_iscsi_sve2, %function
crc32_iscsi_sve2:
.LFB4364:
	.cfi_startproc
	cntb	x11, all, mul #8
	cmp	w1, w11
	blt	.L9
	cntb	x10
	cntb	x9, all, mul #2
	mov	x4, x0
	uxtw	x2, w2
	sxtw	x10, w10
	sxtw	x9, w9
	cntb	x8, all, mul #3
	cntb	x7, all, mul #4
	add	x13, x4, x9
	mov	z0.d, x2
	sxtw	x8, w8
	add	x2, x4, x10
	sxtw	x7, w7
	cntb	x6, all, mul #5
	cntb	x5, all, mul #6
	ptrue	p0.b, all
	sub	sp, sp, #48
	.cfi_def_cfa_offset 48
	sxtw	x6, w6
	sxtw	x5, w5
	cntb	x0, all, mul #7
	ld1d	z2.d, p0/z, [x2]
	ld1d	z4.d, p0/z, [x13]
	add	x2, x4, x8
	add	x13, x4, x7
	mov	z1.b, #0
	add	x12, sp, 31
	sxtw	x0, w0
	sxtw	x3, w11
	ptrue	p1.b, vl1
	ld1d	z3.d, p0/z, [x2]
	splice	z0.d, p1, z0.d, z1.d
	add	x2, x4, x6
	ld1d	z19.d, p0/z, [x13]
	decb	x1, all, mul #8
	add	x13, x4, x5
	ld1d	z1.d, p0/z, [x4]
	ld1d	z18.d, p0/z, [x2]
	movprfx	z1.d, p0/z, z1.d
	eor	z1.d, p0/m, z1.d, z0.d
	add	x2, x4, x0
	ld1d	z17.d, p0/z, [x13]
	and	x12, x12, -32
	adrp	x13, .LANCHOR0
	add	x4, x4, x3
	add	x13, x13, :lo12:.LANCHOR0
	ld1d	z7.d, p0/z, [x2]
	ld1d	z0.d, p0/z, [x13]
	cmp	w1, w11
	blt	.L3
	add	x10, x4, x10
	add	x9, x4, x9
	add	x8, x4, x8
	add	x7, x4, x7
	add	x6, x4, x6
	add	x2, x4, x5
	add	x0, x4, x0
	.p2align 3,,7
.L4:
	ld1d	z5.d, p0/z, [x4]
	prfm    PLDL1STRM, [x4, 768]
	prfm    PLDL2STRM, [x4, 2048]
	prfm    PLDL2STRM, [x4, 3072]
	prfm    PLDL3STRM, [x4, 4096]
	pmullt	z16.q, z1.d, z0.d
	pmullb	z1.q, z1.d, z0.d
	eor3	z1.d, z1.d, z16.d, z5.d
	ld1d	z5.d, p0/z, [x10]
	prfm    PLDL1STRM, [x10, 768]
	prfm    PLDL2STRM, [x10, 2048]
	prfm    PLDL2STRM, [x10, 3072]
	prfm    PLDL3STRM, [x10, 4096]
	pmullt	z6.q, z2.d, z0.d
	pmullb	z2.q, z2.d, z0.d
	eor3	z2.d, z2.d, z6.d, z5.d
	ld1d	z5.d, p0/z, [x9]
	prfm    PLDL1STRM, [x9, 768]
	prfm    PLDL2STRM, [x9, 2048]
	prfm    PLDL2STRM, [x9, 3072]
	prfm    PLDL3STRM, [x9, 4096]
	pmullt	z16.q, z4.d, z0.d
	pmullb	z4.q, z4.d, z0.d
	eor3	z4.d, z4.d, z16.d, z5.d
	ld1d	z5.d, p0/z, [x8]
	prfm    PLDL1STRM, [x8, 768]
	prfm    PLDL2STRM, [x8, 2048]
	prfm    PLDL2STRM, [x8, 3072]
	prfm    PLDL3STRM, [x8, 4096]
	pmullt	z6.q, z3.d, z0.d
	pmullb	z3.q, z3.d, z0.d
	eor3	z3.d, z3.d, z6.d, z5.d
	ld1d	z5.d, p0/z, [x7]
	prfm    PLDL1STRM, [x7, 768]
	prfm    PLDL2STRM, [x7, 2048]
	prfm    PLDL2STRM, [x7, 3072]
	prfm    PLDL3STRM, [x7, 4096]
	pmullt	z16.q, z19.d, z0.d
	pmullb	z19.q, z19.d, z0.d
	eor3	z19.d, z19.d, z16.d, z5.d
	ld1d	z5.d, p0/z, [x6]
	prfm    PLDL1STRM, [x6, 768]
	prfm    PLDL2STRM, [x6, 2048]
	prfm    PLDL2STRM, [x6, 3072]
	prfm    PLDL3STRM, [x6, 4096]
	pmullt	z6.q, z18.d, z0.d
	decb	x1, all, mul #8
	pmullb	z18.q, z18.d, z0.d
	eor3	z18.d, z18.d, z6.d, z5.d
	ld1d	z5.d, p0/z, [x2]
	prfm    PLDL1STRM, [x2, 768]
	prfm    PLDL2STRM, [x2, 2048]
	prfm    PLDL2STRM, [x2, 3072]
	prfm    PLDL3STRM, [x2, 4096]
	pmullt	z16.q, z17.d, z0.d
	pmullt	z6.q, z7.d, z0.d
	pmullb	z17.q, z17.d, z0.d
	pmullb	z7.q, z7.d, z0.d
	eor3	z17.d, z17.d, z16.d, z5.d
	add	x4, x4, x3
	ld1d	z5.d, p0/z, [x0]
	prfm    PLDL1STRM, [x0, 768]
	prfm    PLDL2STRM, [x0, 2048]
	prfm    PLDL2STRM, [x0, 3072]
	prfm    PLDL3STRM, [x0, 4096]
	add	x10, x10, x3
	add	x9, x9, x3
	add	x8, x8, x3
	add	x7, x7, x3
	add	x6, x6, x3
	add	x2, x2, x3
	add	x0, x0, x3
	eor3	z7.d, z7.d, z6.d, z5.d
	cmp	w1, w11
	bge	.L4
.L3:
	ptrue	p0.b, all
	add	x0, x13, 64
	add	x2, x13, 32
	ld1d	z5.d, p0/z, [x2]
	pmullt	z6.q, z1.d, z5.d
	pmullt	z16.q, z4.d, z5.d
	pmullb	z0.q, z1.d, z5.d
	pmullb	z4.q, z4.d, z5.d
	ld1d	z1.d, p0/z, [x0]
	eor3	z4.d, z4.d, z16.d, z17.d
	eor3	z0.d, z0.d, z6.d, z19.d
	add	x13, x13, 96
	mov	x0, 3582
	ld1d	z6.d, p0/z, [x13]
	pmullt	z19.q, z0.d, z1.d
	movk	x0, 0xf20c, lsl 16
	pmullb	z0.q, z0.d, z1.d
	eor3	z0.d, z0.d, z19.d, z4.d
	pmullt	z4.q, z0.d, z6.d
	pmullb	z0.q, z0.d, z6.d
	pmullt	z6.q, z2.d, z5.d
	pmullb	z2.q, z2.d, z5.d
	eor3	z2.d, z2.d, z6.d, z18.d
	fmov	d6, x0
	adrp	x0, .LC0
	pmullt	z16.q, z3.d, z5.d
	pmullb	z3.q, z3.d, z5.d
	pmullt	z5.q, z2.d, z1.d
	eor3	z3.d, z3.d, z16.d, z7.d
	pmullb	z2.q, z2.d, z1.d
	eor3	z2.d, z2.d, z5.d, z3.d
	eor3	z0.d, z0.d, z4.d, z2.d
	ldr	q16, [x0, #:lo12:.LC0]
	st1d	z0.d, p0, [x12]
	mov	x0, 32039
	ldp	q1, q0, [x12]
	movk	x0, 0x493c, lsl 16
	fmov	d3, x0
	mov	x0, 43704
	movk	x0, 0xdd45, lsl 16
	fmov	d4, x0
	fmov	d17, d1
	fmov	d7, xzr
	pmull2	v16.1q, v16.2d, v1.2d
	mov	x0, 5105
	movi	v1.4s, 0
	movk	x0, 0xdea7, lsl 16
	fmov	d2, x0
	mov	x0, 30449
	pmull	v6.1q, v6.1d, v17.1d
	movk	x0, 0x5ec, lsl 16
	eor	v0.16b, v0.16b, v16.16b
	movk	x0, 0x1, lsl 32
	fmov	d5, x0
	eor	v0.16b, v0.16b, v6.16b
	pmull	v6.1q, v0.1d, v3.1d
	ins	v1.d[0], v0.d[1]
	eor	v1.16b, v1.16b, v6.16b
	fmov	s3, s1
	ext	v1.16b, v1.16b, v1.16b, #4
	pmull	v4.1q, v4.1d, v3.1d
	ins	v0.d[0], v4.d[0]
	ins	v0.d[1], v7.d[0]
	eor	v0.16b, v0.16b, v1.16b
	fmov	s1, s0
	pmull	v2.1q, v2.1d, v1.1d
	fmov	s2, s2
	pmull	v2.1q, v5.1d, v2.1d
	eor	v0.16b, v0.16b, v2.16b
	umov	w0, v0.s[1]
	cmp	w1, 0
	bgt	.L11
	add	sp, sp, 48
	.cfi_remember_state
	.cfi_def_cfa_offset 0
	ret
	.p2align 2,,3
.L11:
	.cfi_restore_state
	mov	w2, w0
	mov	x0, x4
	add	sp, sp, 48
	.cfi_def_cfa_offset 0
.L9:
	b	crc32_iscsi_crc_ext
	.cfi_endproc
.LFE4364:
	.size	crc32_iscsi_sve2, .-crc32_iscsi_sve2
	.section	.rodata.cst16,"aM",@progbits,16
	.align	4
.LC0:
	.xword	4060876286
	.xword	1228700967
	.section	.rodata
	.align	4
	.set	.LANCHOR0,. + 0
	.type	p4.3, %object
	.size	p4.3, 32
p4.3:
	.word	-592348508
	.word	0
	.word	-1176491130
	.word	0
	.word	-592348508
	.word	0
	.word	-1176491130
	.word	0
	.type	p4.2, %object
	.size	p4.2, 32
p4.2:
	.word	1771228834
	.word	0
	.word	221995154
	.word	0
	.word	1771228834
	.word	0
	.word	221995154
	.word	0
	.type	p3.1, %object
	.size	p3.1, 32
p3.1:
	.word	1947135746
	.word	0
	.word	-1639260680
	.word	0
	.word	1947135746
	.word	0
	.word	-1639260680
	.word	0
	.type	p2.0, %object
	.size	p2.0, 32
p2.0:
	.word	1034342603
	.word	0
	.word	-1169177970
	.word	0
	.word	1034342603
	.word	0
	.word	-1169177970
	.word	0
	.ident	"GCC: (GNU) 11.3.0"
	.section	.note.GNU-stack,"",@progbits
