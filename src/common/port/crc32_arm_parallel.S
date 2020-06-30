
//input:x0:data_buffer , x1:the len of data , x2:the value of crc32,
//output:x2:the value of crc32, x3: table coverage stat buffer address(only for debug)

//table coverage stat buffer: 7*32B
//addr x3     ~ x3+31   :  space for mul_table_72
//addr x3+32  ~ x3+63   :  space for mul_table_152
//addr x3+64  ~ x3+95   :  space for mul_table_312
//addr x3+96  ~ x3+127  :  space for mul_table_632
//addr x3+128  ~ x3+159 :  space for mul_table_1272
//addr x3+160  ~ x3+191 :  space for mul_table_2712
//addr x3+192  ~ x3+223 :  space for mul_table_5432

//debug enable
.set DEBUG,0


.global crc32_arm
.text

.if DEBUG
.macro tbl_cvrg_stat base_addr, offset, index, tmp_reg_A, tmp_reg_B, tmp_reg_B_W, tmp_reg_C, tmp_reg_D
	STP		\tmp_reg_A, \tmp_reg_B, [sp, #-0x10]!
	STP		\tmp_reg_C, \tmp_reg_D, [sp, #-0x10]!
	LSR \tmp_reg_A, \index, #3
	AND \tmp_reg_A, \tmp_reg_A, 0x1F
	ADD \tmp_reg_A, \tmp_reg_A, \offset
	ADD \tmp_reg_A, \tmp_reg_A, \base_addr
	LDRB \tmp_reg_B_W, [\tmp_reg_A]
	AND \tmp_reg_C, \index, #0x7
	LDR	\tmp_reg_D, =0x1
	LSL \tmp_reg_D, \tmp_reg_D, \tmp_reg_C
	ORR \tmp_reg_B, \tmp_reg_B, \tmp_reg_D
	STRB \tmp_reg_B_W, [\tmp_reg_A]
	LDP 	\tmp_reg_C, \tmp_reg_D, [sp], #0x10
	LDP		\tmp_reg_A, \tmp_reg_B, [sp], #0x10
.endm
.endif





crc32_arm:

// PUSH the register to STACK
    STP		X4, X5, [sp, #-0x10]!
	STP		X6, X7, [sp, #-0x10]!
	STP		X8, X9, [sp, #-0x10]!
	STP		X10, X11, [sp, #-0x10]!
	STP		X12, X13, [sp, #-0x10]!
	STP		X14, X15, [sp, #-0x10]!
	STP		X16, X17, [sp, #-0x10]!
	STP		X18, X19, [sp, #-0x10]!
	STP		X20, X21, [sp, #-0x10]!
	STP		X22, X23, [sp, #-0x10]!
	STP		X24, X25, [sp, #-0x10]!
	STP		X26, X27, [sp, #-0x10]!
	STP		X28, X29, [sp, #-0x10]!
	.if DEBUG
		.print "Compile Debug version!"
    .endif
	LDR		x16,=0x1FE0
    LDR		X22, =0xff 	
	CMP     X1, x16
	B.GE    crcB3_8160
    CMP     X1, #1920
	B.GE    crcB3_1920
	CMP     X1, #960
	B.GE    crcB3_960
	CMP     X1, #480
	B.GE    crcB3_480
	CMP     X1, #240
	B.GE    crcB3_240
	CMP     X1, #8
	B.GE    bit8
	B       less_than_8

//	PRFM PLDL1STRM,[X0 ,#0x1FE0]
//	PRFM PLDL1STRM,[X18,#0x1FE0]
//	PRFM PLDL1STRM,[X19,#0x1FE0]
crcB3_8160:		
//	LDR		X5,=mul_table_72  //get the table of base address	
	LDR		X23,=mul_table_5432
	LDR		X29,=mul_table_2712
	LDR		X6, =2720		 //(2720=8160/3 use tabel 5432,2712)
//	LDR		X7, =0x1800
//	LDR		X8, =0x1400   //for 2720
	EOR		X11, X11, X11	// crc1, clear 0
	EOR		X12, X12, X12	// crc2, clear 0
	EOR     X20, X20, X20   //counter init 0
	ADD		X18, X0, X6		// get the memory base of address block1(block0~block2)
	ADD  	X19, X6, X18	// get the momory base of address block2
 //calculate crc0(block0)/crc1(block1)/crc2(block2)  	
crcB3_8160_segment_loops:		
	LDP     X13, X9,[X0], #0x10
	LDP     X25, X10,[X18], #0x10
	LDP     X26, X27,[X19], #0x10
//	PRFM	PLDL1STRM,[X0 ,#0x70]
//	PRFM	PLDL1STRM,[X18,#0x70]
//	PRFM	PLDL1STRM,[X19,#0x70]
	CRC32CX	W2, W2, X13    //get crc0(W2)
	CRC32CX	W11, W11, X25  //get crc1(W11)
	CRC32CX	W12, W12, X26  //get crc2(W12)
	
	CRC32CX	W2, W2, X9    //get crc0(W2)
	CRC32CX	W11, W11, X10  //get crc1(W11)
	CRC32CX	W12, W12, X27  //get crc2(W12)
	
	
	ADD		X20, X20, #0x1 //x20 the count of fold3
	CMP		X20, #169 
	B.LT	crcB3_8160_segment_loops

	LDR     X13, [X0], #0x8
	LDR     X25, [X18], #0x8
	LDR     X26, [X19], #0x8
	CRC32CX	W2, W2, X13    //get crc0(W2)
	CRC32CX	W11, W11, X25  //get crc1(W11)
	CRC32CX	W12, W12, X26  //get crc2(W12)

	LDR     X13, [X0], #0x8
	LDR     X25, [X18], #0x8
	CRC32CX	W2, W2, X13
	CRC32CX	W11, W11, X25
	
	//W2:crc0 W11 crc1, W12 left one 8byte not compute
	
	//merge use table
	AND		W17, W22, W2     //crc0[7:0]
	AND		W28, W22, W11
//	ADD		X23, X5, X7
//	ADD		X29, X5, X8
	
	LDR		W15, [X23, X17, LSL #2]	//get crc(crc0_b0) from table//table is dd,x17need*2
	LDR		W14, [X29, X28, LSL #2]	
	.if DEBUG
		tbl_cvrg_stat X3, #192, X17, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #160, X28, X8, X9, W9, X10, X11
	.endif
	
	AND		W21, W22, W2, LSR #8	//crc0[15:8]
	AND		W24, W22, W2, LSR #16	// crc0[23:16]
	AND		W4, W22, W2, LSR #24	//crc0[31:24]
	
	LDR		W21, [X23, X21, LSL #2]	//(crc0_b1)
	LDR		W24, [X23, X24, LSL #2]	//(crc0_b2)
	LDR		W4, [X23, X4, LSL #2]	//(crc0_b3)
	.if DEBUG
		tbl_cvrg_stat X3, #192, X21, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #192, X24, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #192, X4,  X8, X9, W9, X10, X11
	.endif


	EOR		X15, X15, X21, LSL #8   //crc0_b0+2^8(crc0_b1)		
	EOR		X15, X15, X24, LSL #16  //crc0_b0+2^8(crc0_b1)+2^16(crc0_b2)		
	EOR		X15, X15, X4,  LSL #24  //crc0_b0+2^8(crc0_b1)+2^16(crc0_b2)+2^24(crc0_b3)	
						
	EOR		X15, X15, X14
	
	AND		W21, W22, W11, LSR #8
	AND		W24, W22, W11, LSR #16	
	AND		W4, W22, W11, LSR #24
	
	LDR		W21, [X29, X21, LSL #2]	
    LDR		W24, [X29, X24, LSL #2]
	LDR		W4, [X29, X4, LSL #2]
	.if DEBUG
		tbl_cvrg_stat X3, #160, X21, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #160, X24, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #160, X4,  X8, X9, W9, X10, X11
	.endif

	EOR		X15, X15, X21, LSL #8		
	EOR		X15, X15, X24, LSL #16			
	EOR		X15, X15, X4, LSL #24

//get final CRC32		
	 LDR        X26, [X19], #0x8  //last64bit
	 EOR		X15, X15, X26  //X15 = merge CRC+ last64bit
     CRC32CX	W12, W12, X15  //get final crc
     MOV		W2, W12
	
	 SUB	 X1, X1, x16	
	 MOV     X0, X19
	 CMP     X1, x16
	 B.GE    crcB3_8160
     CMP     X1, #1920
	 B.GE    crcB3_1920
	 CMP     X1, #960
	 B.GE    crcB3_960
	 CMP     X1, #480
	 B.GE    crcB3_480
	 CMP     X1, #240
	 B.GE    crcB3_240
	 B       bit8	
	 
	PRFM PLDL1STRM,[X0 ,#0x780]
	PRFM PLDL1STRM,[X18,#0x780]
	PRFM PLDL1STRM,[X19,#0x780]	
crcB3_1920:		
//	LDR		X5,=mul_table_72  //get the table of base address	
	LDR		X23,=mul_table_1272
	LDR		X29,=mul_table_632
	LDR		X6, =640		 //(640=1920/3 use tabel 1280,640)
//	LDR		X7, =0x1000
//	LDR		X8, =0x0c00   //for 1920
	EOR		X11, X11, X11	// crc1, clear 0
	EOR		X12, X12, X12	// crc2, clear 0
	EOR     X20, X20, X20   //counter init 0
	ADD		X18, X0, X6		// get the memory base of address block1(block0~block2)
	ADD  	X19, X6, X18	// get the momory base of address block2
 //calculate crc0(block0)/crc1(block1)/crc2(block2)  	
crcB3_1920_segment_loops_prfm:		
	LDP     X13, X9,[X0], #0x10
	LDP     X25, X10,[X18], #0x10
	LDP     X26, X27,[X19], #0x10
	ADD		X20, X20, #0x1 //x20 the count of fold3
	PRFM PLDL1STRM,[X0 ,#0x70]
	PRFM PLDL1STRM,[X18,#0x70]
	PRFM PLDL1STRM,[X19,#0x70]
	CRC32CX	W2, W2, X13    //get crc0(W2)
	CRC32CX	W11, W11, X25  //get crc1(W11)
	CRC32CX	W12, W12, X26  //get crc2(W12)
	CMP		X20, #33 
	CRC32CX	W2, W2, X9    //get crc0(W2)
	CRC32CX	W11, W11, X10  //get crc1(W11)
	CRC32CX	W12, W12, X27  //get crc2(W12)
	B.LT	crcB3_1920_segment_loops_prfm

crcB3_1920_segment_loops:		
	LDP     X13, X9,[X0], #0x10
	LDP     X25, X10,[X18], #0x10
	LDP     X26, X27,[X19], #0x10
	ADD		X20, X20, #0x1 //x20 the count of fold3
	CRC32CX	W2, W2, X13    //get crc0(W2)
	CRC32CX	W11, W11, X25  //get crc1(W11)
	CRC32CX	W12, W12, X26  //get crc2(W12)
	CMP		X20, #39
	CRC32CX	W2, W2, X9    //get crc0(W2)
	CRC32CX	W11, W11, X10  //get crc1(W11)
	CRC32CX	W12, W12, X27  //get crc2(W12)
	B.LT	crcB3_1920_segment_loops

	LDR     X13, [X0], #0x8
	LDR     X25, [X18], #0x8
	LDR     X26, [X19], #0x8
	CRC32CX	W2, W2, X13    //get crc0(W2)
	CRC32CX	W11, W11, X25  //get crc1(W11)
	CRC32CX	W12, W12, X26  //get crc2(W12)
	
	
	LDR     X13, [X0], #0x8
	LDR     X25, [X18], #0x8
	CRC32CX	W2, W2, X13
	CRC32CX	W11, W11, X25
	
	//W2:crc0 W11 crc1, W12 left one 8byte not compute
	
	//merge use table
	AND		W17, W22, W2     //crc0[7:0]
	AND		W28, W22, W11
//	ADD		X23, X5, X7
//	ADD		X29, X5, X8
	
	LDR		W15, [X23, X17, LSL #2]	//get crc(crc0_b0) from table//table is dd,x17need*2
	LDR		W14, [X29, X28, LSL #2]	
	.if DEBUG
		tbl_cvrg_stat X3, #128, X17, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #96,  X28, X8, X9, W9, X10, X11
	.endif
	
	AND		W21, W22, W2, LSR #8	//crc0[15:8]
	AND		W24, W22, W2, LSR #16	// crc0[23:16]
	AND		W4, W22, W2, LSR #24	//crc0[31:24]
	
	LDR		W21, [X23, X21, LSL #2]	//(crc0_b1)
	LDR		W24, [X23, X24, LSL #2]	//(crc0_b2)
	LDR		W4, [X23, X4, LSL #2]	//(crc0_b3)
	.if DEBUG
		tbl_cvrg_stat X3, #128, X21, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #128, X24, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #128, X4,  X8, X9, W9, X10, X11
	.endif
	
	EOR		X15, X15, X21, LSL #8   //crc0_b0+2^8(crc0_b1)		
	EOR		X15, X15, X24, LSL #16  //crc0_b0+2^8(crc0_b1)+2^16(crc0_b2)		
	EOR		X15, X15, X4,  LSL #24  //crc0_b0+2^8(crc0_b1)+2^16(crc0_b2)+2^24(crc0_b3)	
						
	EOR		X15, X15, X14
	
	AND		W21, W22, W11, LSR #8
	AND		W24, W22, W11, LSR #16	
	AND		W4, W22, W11, LSR #24
	
	LDR		W21, [X29, X21, LSL #2]	
    LDR		W24, [X29, X24, LSL #2]
	LDR		W4, [X29, X4, LSL #2]
	.if DEBUG
		tbl_cvrg_stat X3, #96, X21, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #96, X24, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #96, X4,  X8, X9, W9, X10, X11
	.endif

	EOR		X15, X15, X21, LSL #8		
	EOR		X15, X15, X24, LSL #16			
	EOR		X15, X15, X4, LSL #24

//get final CRC32		
	 LDR        X26, [X19], #0x8  //last64bit
	 EOR		X15, X15, X26  //X15 = merge CRC+ last64bit
     CRC32CX	W12, W12, X15  //get final crc
     MOV		W2, W12
	
	 SUB	 X1, X1, #1920	
	 MOV     X0, X19
     CMP     X1, #1920
	 B.GE    crcB3_1920
	 CMP     X1, #960
	 B.GE    crcB3_960
	 CMP     X1, #480
	 B.GE    crcB3_480
	 CMP     X1, #240
	 B.GE    crcB3_240
	 B       bit8
	

crcB3_960:	
	LDR		X23,=mul_table_632
	LDR		X29,=mul_table_312
    LDR		X6, =320	// 320*3 =  960 (Tables  640, 320)
//	LDR		X7, =0x0c00
//	LDR		X8, =0x0800

    EOR		X11, X11, X11	// crc1, clear 0
	EOR		X12, X12, X12	// crc2, clear 0
	EOR     X20, X20, X20   //counter init 0
	ADD		X18, X0, X6		// get the memory base of address block1(block0~block2)
	ADD  	X19, X6, X18	// get the momory base of address block2
 //calculate crc0(block0)/crc1(block1)/crc2(block2)  	
crcB3_960_segment_loops:		
	LDR     X13, [X0], #0x8
	LDR     X25, [X18], #0x8
	LDR     X26, [X19], #0x8
	CRC32CX	W2, W2, X13    //get crc0(W2)
	CRC32CX	W11, W11, X25  //get crc1(W11)
	CRC32CX	W12, W12, X26  //get crc2(W12)
	
	ADD		X20, X20, #0x1 //x20 the count of fold3
	CMP		X20, #39
	B.LT	crcB3_960_segment_loops
	
	LDR     X13, [X0], #0x8
	LDR     X25, [X18], #0x8
	CRC32CX	W2, W2, X13
	CRC32CX	W11, W11, X25
	
	//merge use table
	AND		W17, W22, W2     //crc0[7:0]
	AND		W28, W22, W11
//	ADD		X23, X5, X7		//mul_table_1272
//	ADD		X29, X5, X8		//mul_table_632
	
	LDR		W15, [X23, X17, LSL #2]	//get crc(crc0_b0) from table//table is dd,x17need*4
	LDR		W14, [X29, X28, LSL #2]	
	.if DEBUG
		tbl_cvrg_stat X3, #96, X17, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #64, X28, X8, X9, W9, X10, X11
	.endif
	
	AND		W21, W22, W2, LSR #8	//crc0[15:8]
	AND		W24, W22, W2, LSR #16	// crc0[23:16]
	AND		W4, W22, W2, LSR #24	//crc0[31:24]
	
	LDR		W21, [X23, X21, LSL #2]	//(crc0_b1)
	LDR		W24, [X23, X24, LSL #2]	//(crc0_b2)
	LDR		W4, [X23, X4, LSL #2]	//(crc0_b3)
	.if DEBUG
		tbl_cvrg_stat X3, #96, X21, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #96, X24, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #96, X4,  X8, X9, W9, X10, X11
	.endif
	//w4,w24,w21,w15: mul-table value
	EOR		X15, X15, X21, LSL #8   //crc0_b0+2^8(crc0_b1)		
	EOR		X15, X15, X24, LSL #16  //crc0_b0+2^8(crc0_b1)+2^16(crc0_b2)		
	EOR		X15, X15, X4,  LSL #24  //crc0_b0+2^8(crc0_b1)+2^16(crc0_b2)+2^24(crc0_b3)	
						
	EOR		X15, X15, X14
	
	AND		W21, W22, W11, LSR #8
	AND		W24, W22, W11, LSR #16	
	AND		W4, W22, W11, LSR #24
	
	LDR		W21, [X29, X21, LSL #2]	
    LDR		W24, [X29, X24, LSL #2]
	LDR		W4, [X29, X4, LSL #2]
	.if DEBUG
		tbl_cvrg_stat X3, #64, X21, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #64, X24, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #64, X4,  X8, X9, W9, X10, X11
	.endif
	
	EOR		X15, X15, X21, LSL #8		
	EOR		X15, X15, X24, LSL #16			
	EOR		X15, X15, X4, LSL #24
	//get final CRC32	
	 LDR        X26, [X19], #0x8  //last64bit
	 EOR		X15, X15, X26  //X15 = merge CRC+ last64bit
     CRC32CX	W12, W12, X15  //get final crc
     MOV		W2, W12
	
	 SUB     X1, X1, #960
	 MOV     X0, X19
	 CMP     X1, #960
	 B.GE    crcB3_960
	 CMP     X1, #480
	 B.GE    crcB3_480
	 CMP     X1, #240
	 B.GE    crcB3_240
	 B       bit8
	        

crcB3_480:	
	LDR		X23,=mul_table_312
	LDR		X29,=mul_table_152
    LDR		X6, =160		// 80*3 =  480 (Tables  160,  80)
//	LDR		X7, =0x0800
//	LDR		X8, =0x0400

    EOR		X11, X11, X11	// crc1, clear 0
	EOR		X12, X12, X12	// crc2, clear 0
	EOR     X20, X20, X20   //counter init 0
	ADD		X18, X0, X6		// get the memory base of address block1(block0~block2)
	ADD  	X19, X6, X18	// get the momory base of address block2	 

crcB3_480_segment_loops:		
	LDR     X13, [X0], #0x8
	LDR     X25, [X18], #0x8
	LDR     X26, [X19], #0x8
	CRC32CX	W2,  W2, X13    //get crc0(W2)
	CRC32CX	W11, W11, X25  //get crc1(W11)
	CRC32CX	W12, W12, X26  //get crc2(W12)
	
	ADD		X20, X20, #0x1 //x20 the count of fold3
	CMP		X20, #19
	B.LT	crcB3_480_segment_loops
	
	LDR     X13, [X0], #0x8
	LDR     X25, [X18], #0x8
	CRC32CX	W2, W2, X13
	CRC32CX	W11, W11, X25
	
	//merge use table
	AND		W17, W22, W2     //crc0[7:0]
	AND		W28, W22, W11
//	ADD		X23, X5, X7
//	ADD		X29, X5, X8
	
	LDR		W15, [X23, X17, LSL #2]	//get crc(crc0_b0) from table//table is dd,x17need*2
	LDR		W14, [X29, X28, LSL #2]	
	.if DEBUG
		tbl_cvrg_stat X3, #64, X17, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #32, X28, X8, X9, W9, X10, X11
	.endif
	
	AND		W21, W22, W2, LSR #8	//crc0[15:8]
	AND		W24, W22, W2, LSR #16	// crc0[23:16]
	AND		W4, W22, W2, LSR #24	//crc0[31:24]
	
	LDR		W21, [X23, X21, LSL #2]	//(crc0_b1)
	LDR		W24, [X23, X24, LSL #2]	//(crc0_b2)
	LDR		W4, [X23, X4, LSL #2]	//(crc0_b3)
	.if DEBUG
		tbl_cvrg_stat X3, #64, X21, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #64, X24, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #64, X4,  X8, X9, W9, X10, X11
	.endif
	
	EOR		X15, X15, X21, LSL #8   //crc0_b0+2^8(crc0_b1)		
	EOR		X15, X15, X24, LSL #16  //crc0_b0+2^8(crc0_b1)+2^16(crc0_b2)		
	EOR		X15, X15, X4,  LSL #24  //crc0_b0+2^8(crc0_b1)+2^16(crc0_b2)+2^24(crc0_b3)	
						
	EOR		X15, X15, X14
	
	AND		W21, W22, W11, LSR #8
	AND		W24, W22, W11, LSR #16	
	AND		W4, W22, W11, LSR #24
	
	LDR		W21, [X29, X21, LSL #2]	
    LDR		W24, [X29, X24, LSL #2]
	LDR		W4, [X29, X4, LSL #2]
	.if DEBUG
		tbl_cvrg_stat X3, #32, X21, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #32, X24, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #32, X4,  X8, X9, W9, X10, X11
	.endif
	
	EOR		X15, X15, X21, LSL #8		
	EOR		X15, X15, X24, LSL #16			
	EOR		X15, X15, X4, LSL #24
	//get final CRC32	
	
	 LDR        X26, [X19], #0x8  //last64bit
	 EOR		X15, X15, X26  //X15 = merge CRC+ last64bit
     CRC32CX	W12, W12, X15  //get final crc
     MOV		W2, W12
	 
	 SUB     X1, X1, #480
	 MOV     X0, X19
	 CMP     X1, #480
	 B.GE    crcB3_480
	 CMP     X1, #240
	 B.GE    crcB3_240
	 B       bit8
	 
	 
crcB3_240:	
	LDR		X23,=mul_table_152
	LDR		X29,=mul_table_72
    LDR		X6, =80		// 80*3 =  240 (Tables  160,  80)
//	LDR		X7, =0x0400
//	LDR		X8, =0x0000

    EOR		X11, X11, X11	// crc1, clear 0
	EOR		X12, X12, X12	// crc2, clear 0
	EOR     X20, X20, X20   //counter init 0
	ADD		X18, X0, X6		// get the memory base of address block1(block0~block2)
	ADD  	X19, X6, X18	// get the momory base of address block2	 

crcB3_240_segment_loops:		
	LDR     X13, [X0], #0x8
	LDR     X25, [X18], #0x8
	LDR     X26, [X19], #0x8
	CRC32CX	W2,  W2, X13    //get crc0(W2)
	CRC32CX	W11, W11, X25  //get crc1(W11)
	CRC32CX	W12, W12, X26  //get crc2(W12)
	
	ADD		X20, X20, #0x1 //x20 the count of fold3
	CMP		X20, #9
	B.LT	crcB3_240_segment_loops
	
	LDR     X13, [X0], #0x8
	LDR     X25, [X18], #0x8
	CRC32CX	W2, W2, X13
	CRC32CX	W11, W11, X25
	
	//merge use table
	AND		W17, W22, W2     //crc0[7:0]
	AND		W28, W22, W11
//	ADD		X23, X5, X7
//	ADD		X29, X5, X8
	
	LDR		W15, [X23, X17, LSL #2]	//get crc(crc0_b0) from table//table is dd,x17need*2
	LDR		W14, [X29, X28, LSL #2]	
	.if DEBUG
		tbl_cvrg_stat X3, #32, X17, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #0, X28, X8, X9, W9, X10, X11
	.endif
	
	AND		W21, W22, W2, LSR #8	//crc0[15:8]
	AND		W24, W22, W2, LSR #16	// crc0[23:16]
	AND		W4, W22, W2, LSR #24	//crc0[31:24]
	
	LDR		W21, [X23, X21, LSL #2]	//(crc0_b1)
	LDR		W24, [X23, X24, LSL #2]	//(crc0_b2)
	LDR		W4, [X23, X4, LSL #2]	//(crc0_b3)
	.if DEBUG
		tbl_cvrg_stat X3, #32, X21, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #32, X24, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #32, X4,  X8, X9, W9, X10, X11
	.endif
	
	EOR		X15, X15, X21, LSL #8   //crc0_b0+2^8(crc0_b1)		
	EOR		X15, X15, X24, LSL #16  //crc0_b0+2^8(crc0_b1)+2^16(crc0_b2)		
	EOR		X15, X15, X4,  LSL #24  //crc0_b0+2^8(crc0_b1)+2^16(crc0_b2)+2^24(crc0_b3)	
						
	EOR		X15, X15, X14
	
	AND		W21, W22, W11, LSR #8
	AND		W24, W22, W11, LSR #16	
	AND		W4, W22, W11, LSR #24
	
	LDR		W21, [X29, X21, LSL #2]	
    LDR		W24, [X29, X24, LSL #2]
	LDR		W4, [X29, X4, LSL #2]
	.if DEBUG
		tbl_cvrg_stat X3, #0, X21, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #0, X24, X8, X9, W9, X10, X11
		tbl_cvrg_stat X3, #0, X4,  X8, X9, W9, X10, X11
	.endif
	
	EOR		X15, X15, X21, LSL #8		
	EOR		X15, X15, X24, LSL #16			
	EOR		X15, X15, X4, LSL #24

	//get final CRC32	
	
	 LDR        X26, [X19], #0x8  //last64bit
	 EOR		X15, X15, X26  //X15 = merge CRC+ last64bit
     CRC32CX	W12, W12, X15  //get final crc
     MOV		W2, W12
	
	 SUB     X1, X1, #240
	 MOV     X0, X19
	 CMP     X1, #240
	 B.GE    crcB3_240
	 B       bit8
	 
	 	 

//LESS THAN 240-bytes at this point
bit8:
	MOV		X7, #0x1
	AND		X6, X7, X1, LSR #0x7  
	CMP		X6, #0
	B.EQ	bit7
   .set i, 0
   .rept 8
	LDP      X25, X26, [X0], #0x10
	CRC32CX	 W2, W2, X25
	CRC32CX	 W2, W2, X26
   .endr
	AND		X6, X1, #0x7f
	CMP		X6, #0
	B.EQ	do_return
	
bit7:
	AND		X6, X7, X1, LSR #0x6
	CMP		X6, #0
	B.EQ	bit6
    .set i, 0
    .rept 4
	LDP      X25, X26, [X0], #0x10
	CRC32CX	 W2, W2, X25
	CRC32CX	 W2, W2, X26
   .endr
	AND		X6, X1, #0x3f
	CMP		X6, #0
	B.EQ	do_return
	
bit6:
	AND		X6, X7, X1, LSR #0x5
	CMP		X6, #0
	B.EQ	bit5
  .set i, 0
  .rept 2
	LDP      X25, X26, [X0], #0x10
	CRC32CX	 W2, W2, X25
	CRC32CX	 W2, W2, X26
  .endr
	AND		X6, X1, #0x1f
	CMP		X6, #0
	B.EQ	do_return
	
bit5:
	AND		X6, X7, X1, LSR #0x4
	CMP		X6, #0
	B.EQ	bit4
	LDP      X25, X26, [X0], #0x10
	CRC32CX	 W2, W2, X25
	CRC32CX	 W2, W2, X26
	AND		X6, X1, #0xf
	CMP		X6, #0
	B.EQ	do_return
	
bit4:
	AND		X6, X7, X1, LSR #0x3
	CMP		X6, #0
	B.EQ	bit3
	LDR      X13, [X0], #0x8	
	CRC32CX	 W2, W2, X13
	AND		X6, X1, #0x7
	CMP		X6, #0
	B.EQ	do_return

bit3:
	AND		X6, X7, X1, LSR #0x2
	CMP		X6, #0
	B.EQ	bit2
	LDR      W13, [X0], #0x4	
	CRC32CW	 W2, W2, W13
	AND		X6, X1, #0x3
	CMP		X6, #0
	B.EQ	do_return
	
bit2:
	AND		X6, X7, X1, LSR #0x1
	CMP		X6, #0
	B.EQ	bit1	
	LDRH      W13, [X0], #0x2	
	CRC32CH	 W2, W2, W13
	AND		X6, X1, #0x1
	CMP		X6, #0
	B.EQ	do_return

bit1:
	AND		X6, X7, X1, LSR #0x0
	CMP		X6, #0
	B.EQ	do_return
	LDRB      W13, [X0], #0x1		
	CRC32CB	 W2, W2, W13
	B		 do_return
	
less_than_8:
	CMP		X1, #4
	B.LT	less_than_4
	LDR      W13, [X0], #0x4	
	CRC32CW	 W2, W2, W13
	SUB		X1, X1, #4
	
less_than_4:
	CMP		X1, #2
	B.LT	less_than_2
	LDRH      W13, [X0], #0x2	
	CRC32CH	 W2, W2, W13
	SUB		X1, X1, #2

less_than_2:
	CMP		X1, #1
	B.LT	do_return
	LDRB     W13, [X0], #0x1	
	CRC32CB	 W2, W2, W13
	B		 do_return
			
do_return:
	// pop data back
	MOV		W0, W2
	
	
	LDP		X28, X29, [sp], #0x10
	LDP		X26, X27, [sp], #0x10
	LDP		X24, X25, [sp], #0x10  
	LDP		X22, X23, [sp], #0x10
	LDP		X20, X21, [sp], #0x10
	LDP		X18, X19, [sp], #0x10
	LDP		X16, X17, [sp], #0x10
	LDP		X14, X15, [sp], #0x10
	LDP		X12, X13, [sp], #0x10
	LDP		X10, X11, [sp], #0x10
	LDP		X8, X9, [sp], #0x10
	LDP		X6, X7, [sp], #0x10
	LDP		X4, X5, [sp], #0x10
	
	RET
	
.section .data
.balign   8
mul_table_72:
.word 0x00000000,0x39d3b296,0x73a7652c,0x4a74d7ba
.word 0xe74eca58,0xde9d78ce,0x94e9af74,0xad3a1de2
.word 0xcb71e241,0xf2a250d7,0xb8d6876d,0x810535fb
.word 0x2c3f2819,0x15ec9a8f,0x5f984d35,0x664bffa3
.word 0x930fb273,0xaadc00e5,0xe0a8d75f,0xd97b65c9
.word 0x7441782b,0x4d92cabd,0x07e61d07,0x3e35af91
.word 0x587e5032,0x61ade2a4,0x2bd9351e,0x120a8788
.word 0xbf309a6a,0x86e328fc,0xcc97ff46,0xf5444dd0
.word 0x23f31217,0x1a20a081,0x5054773b,0x6987c5ad
.word 0xc4bdd84f,0xfd6e6ad9,0xb71abd63,0x8ec90ff5
.word 0xe882f056,0xd15142c0,0x9b25957a,0xa2f627ec
.word 0x0fcc3a0e,0x361f8898,0x7c6b5f22,0x45b8edb4
.word 0xb0fca064,0x892f12f2,0xc35bc548,0xfa8877de
.word 0x57b26a3c,0x6e61d8aa,0x24150f10,0x1dc6bd86
.word 0x7b8d4225,0x425ef0b3,0x082a2709,0x31f9959f
.word 0x9cc3887d,0xa5103aeb,0xef64ed51,0xd6b75fc7
.word 0x47e6242e,0x7e3596b8,0x34414102,0x0d92f394
.word 0xa0a8ee76,0x997b5ce0,0xd30f8b5a,0xeadc39cc
.word 0x8c97c66f,0xb54474f9,0xff30a343,0xc6e311d5
.word 0x6bd90c37,0x520abea1,0x187e691b,0x21addb8d
.word 0xd4e9965d,0xed3a24cb,0xa74ef371,0x9e9d41e7
.word 0x33a75c05,0x0a74ee93,0x40003929,0x79d38bbf
.word 0x1f98741c,0x264bc68a,0x6c3f1130,0x55eca3a6
.word 0xf8d6be44,0xc1050cd2,0x8b71db68,0xb2a269fe
.word 0x64153639,0x5dc684af,0x17b25315,0x2e61e183
.word 0x835bfc61,0xba884ef7,0xf0fc994d,0xc92f2bdb
.word 0xaf64d478,0x96b766ee,0xdcc3b154,0xe51003c2
.word 0x482a1e20,0x71f9acb6,0x3b8d7b0c,0x025ec99a
.word 0xf71a844a,0xcec936dc,0x84bde166,0xbd6e53f0
.word 0x10544e12,0x2987fc84,0x63f32b3e,0x5a2099a8
.word 0x3c6b660b,0x05b8d49d,0x4fcc0327,0x761fb1b1
.word 0xdb25ac53,0xe2f61ec5,0xa882c97f,0x91517be9
.word 0x8fcc485c,0xb61ffaca,0xfc6b2d70,0xc5b89fe6
.word 0x68828204,0x51513092,0x1b25e728,0x22f655be
.word 0x44bdaa1d,0x7d6e188b,0x371acf31,0x0ec97da7
.word 0xa3f36045,0x9a20d2d3,0xd0540569,0xe987b7ff
.word 0x1cc3fa2f,0x251048b9,0x6f649f03,0x56b72d95
.word 0xfb8d3077,0xc25e82e1,0x882a555b,0xb1f9e7cd
.word 0xd7b2186e,0xee61aaf8,0xa4157d42,0x9dc6cfd4
.word 0x30fcd236,0x092f60a0,0x435bb71a,0x7a88058c
.word 0xac3f5a4b,0x95ece8dd,0xdf983f67,0xe64b8df1
.word 0x4b719013,0x72a22285,0x38d6f53f,0x010547a9
.word 0x674eb80a,0x5e9d0a9c,0x14e9dd26,0x2d3a6fb0
.word 0x80007252,0xb9d3c0c4,0xf3a7177e,0xca74a5e8
.word 0x3f30e838,0x06e35aae,0x4c978d14,0x75443f82
.word 0xd87e2260,0xe1ad90f6,0xabd9474c,0x920af5da
.word 0xf4410a79,0xcd92b8ef,0x87e66f55,0xbe35ddc3
.word 0x130fc021,0x2adc72b7,0x60a8a50d,0x597b179b
.word 0xc82a6c72,0xf1f9dee4,0xbb8d095e,0x825ebbc8
.word 0x2f64a62a,0x16b714bc,0x5cc3c306,0x65107190
.word 0x035b8e33,0x3a883ca5,0x70fceb1f,0x492f5989
.word 0xe415446b,0xddc6f6fd,0x97b22147,0xae6193d1
.word 0x5b25de01,0x62f66c97,0x2882bb2d,0x115109bb
.word 0xbc6b1459,0x85b8a6cf,0xcfcc7175,0xf61fc3e3
.word 0x90543c40,0xa9878ed6,0xe3f3596c,0xda20ebfa
.word 0x771af618,0x4ec9448e,0x04bd9334,0x3d6e21a2
.word 0xebd97e65,0xd20accf3,0x987e1b49,0xa1ada9df
.word 0x0c97b43d,0x354406ab,0x7f30d111,0x46e36387
.word 0x20a89c24,0x197b2eb2,0x530ff908,0x6adc4b9e
.word 0xc7e6567c,0xfe35e4ea,0xb4413350,0x8d9281c6
.word 0x78d6cc16,0x41057e80,0x0b71a93a,0x32a21bac
.word 0x9f98064e,0xa64bb4d8,0xec3f6362,0xd5ecd1f4
.word 0xb3a72e57,0x8a749cc1,0xc0004b7b,0xf9d3f9ed
.word 0x54e9e40f,0x6d3a5699,0x274e8123,0x1e9d33b5 

mul_table_152:
.word 0x00000000,0x878a92a7,0x0af953bf,0x8d73c118
.word 0x15f2a77e,0x927835d9,0x1f0bf4c1,0x98816666
.word 0x2be54efc,0xac6fdc5b,0x211c1d43,0xa6968fe4
.word 0x3e17e982,0xb99d7b25,0x34eeba3d,0xb364289a
.word 0x57ca9df8,0xd0400f5f,0x5d33ce47,0xdab95ce0
.word 0x42383a86,0xc5b2a821,0x48c16939,0xcf4bfb9e
.word 0x7c2fd304,0xfba541a3,0x76d680bb,0xf15c121c
.word 0x69dd747a,0xee57e6dd,0x632427c5,0xe4aeb562
.word 0xaf953bf0,0x281fa957,0xa56c684f,0x22e6fae8
.word 0xba679c8e,0x3ded0e29,0xb09ecf31,0x37145d96
.word 0x8470750c,0x03fae7ab,0x8e8926b3,0x0903b414
.word 0x9182d272,0x160840d5,0x9b7b81cd,0x1cf1136a
.word 0xf85fa608,0x7fd534af,0xf2a6f5b7,0x752c6710
.word 0xedad0176,0x6a2793d1,0xe75452c9,0x60dec06e
.word 0xd3bae8f4,0x54307a53,0xd943bb4b,0x5ec929ec
.word 0xc6484f8a,0x41c2dd2d,0xccb11c35,0x4b3b8e92
.word 0x5ac60111,0xdd4c93b6,0x503f52ae,0xd7b5c009
.word 0x4f34a66f,0xc8be34c8,0x45cdf5d0,0xc2476777
.word 0x71234fed,0xf6a9dd4a,0x7bda1c52,0xfc508ef5
.word 0x64d1e893,0xe35b7a34,0x6e28bb2c,0xe9a2298b
.word 0x0d0c9ce9,0x8a860e4e,0x07f5cf56,0x807f5df1
.word 0x18fe3b97,0x9f74a930,0x12076828,0x958dfa8f
.word 0x26e9d215,0xa16340b2,0x2c1081aa,0xab9a130d
.word 0x331b756b,0xb491e7cc,0x39e226d4,0xbe68b473
.word 0xf5533ae1,0x72d9a846,0xffaa695e,0x7820fbf9
.word 0xe0a19d9f,0x672b0f38,0xea58ce20,0x6dd25c87
.word 0xdeb6741d,0x593ce6ba,0xd44f27a2,0x53c5b505
.word 0xcb44d363,0x4cce41c4,0xc1bd80dc,0x4637127b
.word 0xa299a719,0x251335be,0xa860f4a6,0x2fea6601
.word 0xb76b0067,0x30e192c0,0xbd9253d8,0x3a18c17f
.word 0x897ce9e5,0x0ef67b42,0x8385ba5a,0x040f28fd
.word 0x9c8e4e9b,0x1b04dc3c,0x96771d24,0x11fd8f83
.word 0xb58c0222,0x32069085,0xbf75519d,0x38ffc33a
.word 0xa07ea55c,0x27f437fb,0xaa87f6e3,0x2d0d6444
.word 0x9e694cde,0x19e3de79,0x94901f61,0x131a8dc6
.word 0x8b9beba0,0x0c117907,0x8162b81f,0x06e82ab8
.word 0xe2469fda,0x65cc0d7d,0xe8bfcc65,0x6f355ec2
.word 0xf7b438a4,0x703eaa03,0xfd4d6b1b,0x7ac7f9bc
.word 0xc9a3d126,0x4e294381,0xc35a8299,0x44d0103e
.word 0xdc517658,0x5bdbe4ff,0xd6a825e7,0x5122b740
.word 0x1a1939d2,0x9d93ab75,0x10e06a6d,0x976af8ca
.word 0x0feb9eac,0x88610c0b,0x0512cd13,0x82985fb4
.word 0x31fc772e,0xb676e589,0x3b052491,0xbc8fb636
.word 0x240ed050,0xa38442f7,0x2ef783ef,0xa97d1148
.word 0x4dd3a42a,0xca59368d,0x472af795,0xc0a06532
.word 0x58210354,0xdfab91f3,0x52d850eb,0xd552c24c
.word 0x6636ead6,0xe1bc7871,0x6ccfb969,0xeb452bce
.word 0x73c44da8,0xf44edf0f,0x793d1e17,0xfeb78cb0
.word 0xef4a0333,0x68c09194,0xe5b3508c,0x6239c22b
.word 0xfab8a44d,0x7d3236ea,0xf041f7f2,0x77cb6555
.word 0xc4af4dcf,0x4325df68,0xce561e70,0x49dc8cd7
.word 0xd15deab1,0x56d77816,0xdba4b90e,0x5c2e2ba9
.word 0xb8809ecb,0x3f0a0c6c,0xb279cd74,0x35f35fd3
.word 0xad7239b5,0x2af8ab12,0xa78b6a0a,0x2001f8ad
.word 0x9365d037,0x14ef4290,0x999c8388,0x1e16112f
.word 0x86977749,0x011de5ee,0x8c6e24f6,0x0be4b651
.word 0x40df38c3,0xc755aa64,0x4a266b7c,0xcdacf9db
.word 0x552d9fbd,0xd2a70d1a,0x5fd4cc02,0xd85e5ea5
.word 0x6b3a763f,0xecb0e498,0x61c32580,0xe649b727
.word 0x7ec8d141,0xf94243e6,0x743182fe,0xf3bb1059
.word 0x1715a53b,0x909f379c,0x1decf684,0x9a666423
.word 0x02e70245,0x856d90e2,0x081e51fa,0x8f94c35d
.word 0x3cf0ebc7,0xbb7a7960,0x3609b878,0xb1832adf
.word 0x29024cb9,0xae88de1e,0x23fb1f06,0xa4718da1

mul_table_312:
.word 0x00000000,0xbac2fd7b,0x70698c07,0xcaab717c
.word 0xe0d3180e,0x5a11e575,0x90ba9409,0x2a786972
.word 0xc44a46ed,0x7e88bb96,0xb423caea,0x0ee13791
.word 0x24995ee3,0x9e5ba398,0x54f0d2e4,0xee322f9f
.word 0x8d78fb2b,0x37ba0650,0xfd11772c,0x47d38a57
.word 0x6dabe325,0xd7691e5e,0x1dc26f22,0xa7009259
.word 0x4932bdc6,0xf3f040bd,0x395b31c1,0x8399ccba
.word 0xa9e1a5c8,0x132358b3,0xd98829cf,0x634ad4b4
.word 0x1f1d80a7,0xa5df7ddc,0x6f740ca0,0xd5b6f1db
.word 0xffce98a9,0x450c65d2,0x8fa714ae,0x3565e9d5
.word 0xdb57c64a,0x61953b31,0xab3e4a4d,0x11fcb736
.word 0x3b84de44,0x8146233f,0x4bed5243,0xf12faf38
.word 0x92657b8c,0x28a786f7,0xe20cf78b,0x58ce0af0
.word 0x72b66382,0xc8749ef9,0x02dfef85,0xb81d12fe
.word 0x562f3d61,0xecedc01a,0x2646b166,0x9c844c1d
.word 0xb6fc256f,0x0c3ed814,0xc695a968,0x7c575413
.word 0x3e3b014e,0x84f9fc35,0x4e528d49,0xf4907032
.word 0xdee81940,0x642ae43b,0xae819547,0x1443683c
.word 0xfa7147a3,0x40b3bad8,0x8a18cba4,0x30da36df
.word 0x1aa25fad,0xa060a2d6,0x6acbd3aa,0xd0092ed1
.word 0xb343fa65,0x0981071e,0xc32a7662,0x79e88b19
.word 0x5390e26b,0xe9521f10,0x23f96e6c,0x993b9317
.word 0x7709bc88,0xcdcb41f3,0x0760308f,0xbda2cdf4
.word 0x97daa486,0x2d1859fd,0xe7b32881,0x5d71d5fa
.word 0x212681e9,0x9be47c92,0x514f0dee,0xeb8df095
.word 0xc1f599e7,0x7b37649c,0xb19c15e0,0x0b5ee89b
.word 0xe56cc704,0x5fae3a7f,0x95054b03,0x2fc7b678
.word 0x05bfdf0a,0xbf7d2271,0x75d6530d,0xcf14ae76
.word 0xac5e7ac2,0x169c87b9,0xdc37f6c5,0x66f50bbe
.word 0x4c8d62cc,0xf64f9fb7,0x3ce4eecb,0x862613b0
.word 0x68143c2f,0xd2d6c154,0x187db028,0xa2bf4d53
.word 0x88c72421,0x3205d95a,0xf8aea826,0x426c555d
.word 0x7c76029c,0xc6b4ffe7,0x0c1f8e9b,0xb6dd73e0
.word 0x9ca51a92,0x2667e7e9,0xeccc9695,0x560e6bee
.word 0xb83c4471,0x02feb90a,0xc855c876,0x7297350d
.word 0x58ef5c7f,0xe22da104,0x2886d078,0x92442d03
.word 0xf10ef9b7,0x4bcc04cc,0x816775b0,0x3ba588cb
.word 0x11dde1b9,0xab1f1cc2,0x61b46dbe,0xdb7690c5
.word 0x3544bf5a,0x8f864221,0x452d335d,0xffefce26
.word 0xd597a754,0x6f555a2f,0xa5fe2b53,0x1f3cd628
.word 0x636b823b,0xd9a97f40,0x13020e3c,0xa9c0f347
.word 0x83b89a35,0x397a674e,0xf3d11632,0x4913eb49
.word 0xa721c4d6,0x1de339ad,0xd74848d1,0x6d8ab5aa
.word 0x47f2dcd8,0xfd3021a3,0x379b50df,0x8d59ada4
.word 0xee137910,0x54d1846b,0x9e7af517,0x24b8086c
.word 0x0ec0611e,0xb4029c65,0x7ea9ed19,0xc46b1062
.word 0x2a593ffd,0x909bc286,0x5a30b3fa,0xe0f24e81
.word 0xca8a27f3,0x7048da88,0xbae3abf4,0x0021568f
.word 0x424d03d2,0xf88ffea9,0x32248fd5,0x88e672ae
.word 0xa29e1bdc,0x185ce6a7,0xd2f797db,0x68356aa0
.word 0x8607453f,0x3cc5b844,0xf66ec938,0x4cac3443
.word 0x66d45d31,0xdc16a04a,0x16bdd136,0xac7f2c4d
.word 0xcf35f8f9,0x75f70582,0xbf5c74fe,0x059e8985
.word 0x2fe6e0f7,0x95241d8c,0x5f8f6cf0,0xe54d918b
.word 0x0b7fbe14,0xb1bd436f,0x7b163213,0xc1d4cf68
.word 0xebaca61a,0x516e5b61,0x9bc52a1d,0x2107d766
.word 0x5d508375,0xe7927e0e,0x2d390f72,0x97fbf209
.word 0xbd839b7b,0x07416600,0xcdea177c,0x7728ea07
.word 0x991ac598,0x23d838e3,0xe973499f,0x53b1b4e4
.word 0x79c9dd96,0xc30b20ed,0x09a05191,0xb362acea
.word 0xd028785e,0x6aea8525,0xa041f459,0x1a830922
.word 0x30fb6050,0x8a399d2b,0x4092ec57,0xfa50112c
.word 0x14623eb3,0xaea0c3c8,0x640bb2b4,0xdec94fcf
.word 0xf4b126bd,0x4e73dbc6,0x84d8aaba,0x3e1a57c1

mul_table_632:
.word 0x00000000,0x6b749fb2,0xd6e93f64,0xbd9da0d6
.word 0xa83e0839,0xc34a978b,0x7ed7375d,0x15a3a8ef
.word 0x55906683,0x3ee4f931,0x837959e7,0xe80dc655
.word 0xfdae6eba,0x96daf108,0x2b4751de,0x4033ce6c
.word 0xab20cd06,0xc05452b4,0x7dc9f262,0x16bd6dd0
.word 0x031ec53f,0x686a5a8d,0xd5f7fa5b,0xbe8365e9
.word 0xfeb0ab85,0x95c43437,0x285994e1,0x432d0b53
.word 0x568ea3bc,0x3dfa3c0e,0x80679cd8,0xeb13036a
.word 0x53adecfd,0x38d9734f,0x8544d399,0xee304c2b
.word 0xfb93e4c4,0x90e77b76,0x2d7adba0,0x460e4412
.word 0x063d8a7e,0x6d4915cc,0xd0d4b51a,0xbba02aa8
.word 0xae038247,0xc5771df5,0x78eabd23,0x139e2291
.word 0xf88d21fb,0x93f9be49,0x2e641e9f,0x4510812d
.word 0x50b329c2,0x3bc7b670,0x865a16a6,0xed2e8914
.word 0xad1d4778,0xc669d8ca,0x7bf4781c,0x1080e7ae
.word 0x05234f41,0x6e57d0f3,0xd3ca7025,0xb8beef97
.word 0xa75bd9fa,0xcc2f4648,0x71b2e69e,0x1ac6792c
.word 0x0f65d1c3,0x64114e71,0xd98ceea7,0xb2f87115
.word 0xf2cbbf79,0x99bf20cb,0x2422801d,0x4f561faf
.word 0x5af5b740,0x318128f2,0x8c1c8824,0xe7681796
.word 0x0c7b14fc,0x670f8b4e,0xda922b98,0xb1e6b42a
.word 0xa4451cc5,0xcf318377,0x72ac23a1,0x19d8bc13
.word 0x59eb727f,0x329fedcd,0x8f024d1b,0xe476d2a9
.word 0xf1d57a46,0x9aa1e5f4,0x273c4522,0x4c48da90
.word 0xf4f63507,0x9f82aab5,0x221f0a63,0x496b95d1
.word 0x5cc83d3e,0x37bca28c,0x8a21025a,0xe1559de8
.word 0xa1665384,0xca12cc36,0x778f6ce0,0x1cfbf352
.word 0x09585bbd,0x622cc40f,0xdfb164d9,0xb4c5fb6b
.word 0x5fd6f801,0x34a267b3,0x893fc765,0xe24b58d7
.word 0xf7e8f038,0x9c9c6f8a,0x2101cf5c,0x4a7550ee
.word 0x0a469e82,0x61320130,0xdcafa1e6,0xb7db3e54
.word 0xa27896bb,0xc90c0909,0x7491a9df,0x1fe5366d
.word 0x4b5bc505,0x202f5ab7,0x9db2fa61,0xf6c665d3
.word 0xe365cd3c,0x8811528e,0x358cf258,0x5ef86dea
.word 0x1ecba386,0x75bf3c34,0xc8229ce2,0xa3560350
.word 0xb6f5abbf,0xdd81340d,0x601c94db,0x0b680b69
.word 0xe07b0803,0x8b0f97b1,0x36923767,0x5de6a8d5
.word 0x4845003a,0x23319f88,0x9eac3f5e,0xf5d8a0ec
.word 0xb5eb6e80,0xde9ff132,0x630251e4,0x0876ce56
.word 0x1dd566b9,0x76a1f90b,0xcb3c59dd,0xa048c66f
.word 0x18f629f8,0x7382b64a,0xce1f169c,0xa56b892e
.word 0xb0c821c1,0xdbbcbe73,0x66211ea5,0x0d558117
.word 0x4d664f7b,0x2612d0c9,0x9b8f701f,0xf0fbefad
.word 0xe5584742,0x8e2cd8f0,0x33b17826,0x58c5e794
.word 0xb3d6e4fe,0xd8a27b4c,0x653fdb9a,0x0e4b4428
.word 0x1be8ecc7,0x709c7375,0xcd01d3a3,0xa6754c11
.word 0xe646827d,0x8d321dcf,0x30afbd19,0x5bdb22ab
.word 0x4e788a44,0x250c15f6,0x9891b520,0xf3e52a92
.word 0xec001cff,0x8774834d,0x3ae9239b,0x519dbc29
.word 0x443e14c6,0x2f4a8b74,0x92d72ba2,0xf9a3b410
.word 0xb9907a7c,0xd2e4e5ce,0x6f794518,0x040ddaaa
.word 0x11ae7245,0x7adaedf7,0xc7474d21,0xac33d293
.word 0x4720d1f9,0x2c544e4b,0x91c9ee9d,0xfabd712f
.word 0xef1ed9c0,0x846a4672,0x39f7e6a4,0x52837916
.word 0x12b0b77a,0x79c428c8,0xc459881e,0xaf2d17ac
.word 0xba8ebf43,0xd1fa20f1,0x6c678027,0x07131f95
.word 0xbfadf002,0xd4d96fb0,0x6944cf66,0x023050d4
.word 0x1793f83b,0x7ce76789,0xc17ac75f,0xaa0e58ed
.word 0xea3d9681,0x81490933,0x3cd4a9e5,0x57a03657
.word 0x42039eb8,0x2977010a,0x94eaa1dc,0xff9e3e6e
.word 0x148d3d04,0x7ff9a2b6,0xc2640260,0xa9109dd2
.word 0xbcb3353d,0xd7c7aa8f,0x6a5a0a59,0x012e95eb
.word 0x411d5b87,0x2a69c435,0x97f464e3,0xfc80fb51
.word 0xe92353be,0x8257cc0c,0x3fca6cda,0x54bef368

mul_table_1272:
.word 0x00000000,0xdd66cbbb,0xbf21e187,0x62472a3c
.word 0x7bafb5ff,0xa6c97e44,0xc48e5478,0x19e89fc3
.word 0xf75f6bfe,0x2a39a045,0x487e8a79,0x951841c2
.word 0x8cf0de01,0x519615ba,0x33d13f86,0xeeb7f43d
.word 0xeb52a10d,0x36346ab6,0x5473408a,0x89158b31
.word 0x90fd14f2,0x4d9bdf49,0x2fdcf575,0xf2ba3ece
.word 0x1c0dcaf3,0xc16b0148,0xa32c2b74,0x7e4ae0cf
.word 0x67a27f0c,0xbac4b4b7,0xd8839e8b,0x05e55530
.word 0xd34934eb,0x0e2fff50,0x6c68d56c,0xb10e1ed7
.word 0xa8e68114,0x75804aaf,0x17c76093,0xcaa1ab28
.word 0x24165f15,0xf97094ae,0x9b37be92,0x46517529
.word 0x5fb9eaea,0x82df2151,0xe0980b6d,0x3dfec0d6
.word 0x381b95e6,0xe57d5e5d,0x873a7461,0x5a5cbfda
.word 0x43b42019,0x9ed2eba2,0xfc95c19e,0x21f30a25
.word 0xcf44fe18,0x122235a3,0x70651f9f,0xad03d424
.word 0xb4eb4be7,0x698d805c,0x0bcaaa60,0xd6ac61db
.word 0xa37e1f27,0x7e18d49c,0x1c5ffea0,0xc139351b
.word 0xd8d1aad8,0x05b76163,0x67f04b5f,0xba9680e4
.word 0x542174d9,0x8947bf62,0xeb00955e,0x36665ee5
.word 0x2f8ec126,0xf2e80a9d,0x90af20a1,0x4dc9eb1a
.word 0x482cbe2a,0x954a7591,0xf70d5fad,0x2a6b9416
.word 0x33830bd5,0xeee5c06e,0x8ca2ea52,0x51c421e9
.word 0xbf73d5d4,0x62151e6f,0x00523453,0xdd34ffe8
.word 0xc4dc602b,0x19baab90,0x7bfd81ac,0xa69b4a17
.word 0x70372bcc,0xad51e077,0xcf16ca4b,0x127001f0
.word 0x0b989e33,0xd6fe5588,0xb4b97fb4,0x69dfb40f
.word 0x87684032,0x5a0e8b89,0x3849a1b5,0xe52f6a0e
.word 0xfcc7f5cd,0x21a13e76,0x43e6144a,0x9e80dff1
.word 0x9b658ac1,0x4603417a,0x24446b46,0xf922a0fd
.word 0xe0ca3f3e,0x3dacf485,0x5febdeb9,0x828d1502
.word 0x6c3ae13f,0xb15c2a84,0xd31b00b8,0x0e7dcb03
.word 0x179554c0,0xcaf39f7b,0xa8b4b547,0x75d27efc
.word 0x431048bf,0x9e768304,0xfc31a938,0x21576283
.word 0x38bffd40,0xe5d936fb,0x879e1cc7,0x5af8d77c
.word 0xb44f2341,0x6929e8fa,0x0b6ec2c6,0xd608097d
.word 0xcfe096be,0x12865d05,0x70c17739,0xada7bc82
.word 0xa842e9b2,0x75242209,0x17630835,0xca05c38e
.word 0xd3ed5c4d,0x0e8b97f6,0x6cccbdca,0xb1aa7671
.word 0x5f1d824c,0x827b49f7,0xe03c63cb,0x3d5aa870
.word 0x24b237b3,0xf9d4fc08,0x9b93d634,0x46f51d8f
.word 0x90597c54,0x4d3fb7ef,0x2f789dd3,0xf21e5668
.word 0xebf6c9ab,0x36900210,0x54d7282c,0x89b1e397
.word 0x670617aa,0xba60dc11,0xd827f62d,0x05413d96
.word 0x1ca9a255,0xc1cf69ee,0xa38843d2,0x7eee8869
.word 0x7b0bdd59,0xa66d16e2,0xc42a3cde,0x194cf765
.word 0x00a468a6,0xddc2a31d,0xbf858921,0x62e3429a
.word 0x8c54b6a7,0x51327d1c,0x33755720,0xee139c9b
.word 0xf7fb0358,0x2a9dc8e3,0x48dae2df,0x95bc2964
.word 0xe06e5798,0x3d089c23,0x5f4fb61f,0x82297da4
.word 0x9bc1e267,0x46a729dc,0x24e003e0,0xf986c85b
.word 0x17313c66,0xca57f7dd,0xa810dde1,0x7576165a
.word 0x6c9e8999,0xb1f84222,0xd3bf681e,0x0ed9a3a5
.word 0x0b3cf695,0xd65a3d2e,0xb41d1712,0x697bdca9
.word 0x7093436a,0xadf588d1,0xcfb2a2ed,0x12d46956
.word 0xfc639d6b,0x210556d0,0x43427cec,0x9e24b757
.word 0x87cc2894,0x5aaae32f,0x38edc913,0xe58b02a8
.word 0x33276373,0xee41a8c8,0x8c0682f4,0x5160494f
.word 0x4888d68c,0x95ee1d37,0xf7a9370b,0x2acffcb0
.word 0xc478088d,0x191ec336,0x7b59e90a,0xa63f22b1
.word 0xbfd7bd72,0x62b176c9,0x00f65cf5,0xdd90974e
.word 0xd875c27e,0x051309c5,0x675423f9,0xba32e842
.word 0xa3da7781,0x7ebcbc3a,0x1cfb9606,0xc19d5dbd
.word 0x2f2aa980,0xf24c623b,0x900b4807,0x4d6d83bc
.word 0x54851c7f,0x89e3d7c4,0xeba4fdf8,0x36c23643

mul_table_2712:
.word 0x00000000,0x5aa1f3cf,0xb543e79e,0xefe21451
.word 0x6f6bb9cd,0x35ca4a02,0xda285e53,0x8089ad9c
.word 0xded7739a,0x84768055,0x6b949404,0x313567cb
.word 0xb1bcca57,0xeb1d3998,0x04ff2dc9,0x5e5ede06
.word 0xb84291c5,0xe2e3620a,0x0d01765b,0x57a08594
.word 0xd7292808,0x8d88dbc7,0x626acf96,0x38cb3c59
.word 0x6695e25f,0x3c341190,0xd3d605c1,0x8977f60e
.word 0x09fe5b92,0x535fa85d,0xbcbdbc0c,0xe61c4fc3
.word 0x7569557b,0x2fc8a6b4,0xc02ab2e5,0x9a8b412a
.word 0x1a02ecb6,0x40a31f79,0xaf410b28,0xf5e0f8e7
.word 0xabbe26e1,0xf11fd52e,0x1efdc17f,0x445c32b0
.word 0xc4d59f2c,0x9e746ce3,0x719678b2,0x2b378b7d
.word 0xcd2bc4be,0x978a3771,0x78682320,0x22c9d0ef
.word 0xa2407d73,0xf8e18ebc,0x17039aed,0x4da26922
.word 0x13fcb724,0x495d44eb,0xa6bf50ba,0xfc1ea375
.word 0x7c970ee9,0x2636fd26,0xc9d4e977,0x93751ab8
.word 0xead2aaf6,0xb0735939,0x5f914d68,0x0530bea7
.word 0x85b9133b,0xdf18e0f4,0x30faf4a5,0x6a5b076a
.word 0x3405d96c,0x6ea42aa3,0x81463ef2,0xdbe7cd3d
.word 0x5b6e60a1,0x01cf936e,0xee2d873f,0xb48c74f0
.word 0x52903b33,0x0831c8fc,0xe7d3dcad,0xbd722f62
.word 0x3dfb82fe,0x675a7131,0x88b86560,0xd21996af
.word 0x8c4748a9,0xd6e6bb66,0x3904af37,0x63a55cf8
.word 0xe32cf164,0xb98d02ab,0x566f16fa,0x0ccee535
.word 0x9fbbff8d,0xc51a0c42,0x2af81813,0x7059ebdc
.word 0xf0d04640,0xaa71b58f,0x4593a1de,0x1f325211
.word 0x416c8c17,0x1bcd7fd8,0xf42f6b89,0xae8e9846
.word 0x2e0735da,0x74a6c615,0x9b44d244,0xc1e5218b
.word 0x27f96e48,0x7d589d87,0x92ba89d6,0xc81b7a19
.word 0x4892d785,0x1233244a,0xfdd1301b,0xa770c3d4
.word 0xf92e1dd2,0xa38fee1d,0x4c6dfa4c,0x16cc0983
.word 0x9645a41f,0xcce457d0,0x23064381,0x79a7b04e
.word 0xd049231d,0x8ae8d0d2,0x650ac483,0x3fab374c
.word 0xbf229ad0,0xe583691f,0x0a617d4e,0x50c08e81
.word 0x0e9e5087,0x543fa348,0xbbddb719,0xe17c44d6
.word 0x61f5e94a,0x3b541a85,0xd4b60ed4,0x8e17fd1b
.word 0x680bb2d8,0x32aa4117,0xdd485546,0x87e9a689
.word 0x07600b15,0x5dc1f8da,0xb223ec8b,0xe8821f44
.word 0xb6dcc142,0xec7d328d,0x039f26dc,0x593ed513
.word 0xd9b7788f,0x83168b40,0x6cf49f11,0x36556cde
.word 0xa5207666,0xff8185a9,0x106391f8,0x4ac26237
.word 0xca4bcfab,0x90ea3c64,0x7f082835,0x25a9dbfa
.word 0x7bf705fc,0x2156f633,0xceb4e262,0x941511ad
.word 0x149cbc31,0x4e3d4ffe,0xa1df5baf,0xfb7ea860
.word 0x1d62e7a3,0x47c3146c,0xa821003d,0xf280f3f2
.word 0x72095e6e,0x28a8ada1,0xc74ab9f0,0x9deb4a3f
.word 0xc3b59439,0x991467f6,0x76f673a7,0x2c578068
.word 0xacde2df4,0xf67fde3b,0x199dca6a,0x433c39a5
.word 0x3a9b89eb,0x603a7a24,0x8fd86e75,0xd5799dba
.word 0x55f03026,0x0f51c3e9,0xe0b3d7b8,0xba122477
.word 0xe44cfa71,0xbeed09be,0x510f1def,0x0baeee20
.word 0x8b2743bc,0xd186b073,0x3e64a422,0x64c557ed
.word 0x82d9182e,0xd878ebe1,0x379affb0,0x6d3b0c7f
.word 0xedb2a1e3,0xb713522c,0x58f1467d,0x0250b5b2
.word 0x5c0e6bb4,0x06af987b,0xe94d8c2a,0xb3ec7fe5
.word 0x3365d279,0x69c421b6,0x862635e7,0xdc87c628
.word 0x4ff2dc90,0x15532f5f,0xfab13b0e,0xa010c8c1
.word 0x2099655d,0x7a389692,0x95da82c3,0xcf7b710c
.word 0x9125af0a,0xcb845cc5,0x24664894,0x7ec7bb5b
.word 0xfe4e16c7,0xa4efe508,0x4b0df159,0x11ac0296
.word 0xf7b04d55,0xad11be9a,0x42f3aacb,0x18525904
.word 0x98dbf498,0xc27a0757,0x2d981306,0x7739e0c9
.word 0x29673ecf,0x73c6cd00,0x9c24d951,0xc6852a9e
.word 0x460c8702,0x1cad74cd,0xf34f609c,0xa9ee9353

mul_table_5432:
.word 0x00000000,0xfa0f2bd0,0xf1f22151,0x0bfd0a81
.word 0xe6083453,0x1c071f83,0x17fa1502,0xedf53ed2
.word 0xc9fc1e57,0x33f33587,0x380e3f06,0xc20114d6
.word 0x2ff42a04,0xd5fb01d4,0xde060b55,0x24092085
.word 0x96144a5f,0x6c1b618f,0x67e66b0e,0x9de940de
.word 0x701c7e0c,0x8a1355dc,0x81ee5f5d,0x7be1748d
.word 0x5fe85408,0xa5e77fd8,0xae1a7559,0x54155e89
.word 0xb9e0605b,0x43ef4b8b,0x4812410a,0xb21d6ada
.word 0x29c4e24f,0xd3cbc99f,0xd836c31e,0x2239e8ce
.word 0xcfccd61c,0x35c3fdcc,0x3e3ef74d,0xc431dc9d
.word 0xe038fc18,0x1a37d7c8,0x11cadd49,0xebc5f699
.word 0x0630c84b,0xfc3fe39b,0xf7c2e91a,0x0dcdc2ca
.word 0xbfd0a810,0x45df83c0,0x4e228941,0xb42da291
.word 0x59d89c43,0xa3d7b793,0xa82abd12,0x522596c2
.word 0x762cb647,0x8c239d97,0x87de9716,0x7dd1bcc6
.word 0x90248214,0x6a2ba9c4,0x61d6a345,0x9bd98895
.word 0x5389c49e,0xa986ef4e,0xa27be5cf,0x5874ce1f
.word 0xb581f0cd,0x4f8edb1d,0x4473d19c,0xbe7cfa4c
.word 0x9a75dac9,0x607af119,0x6b87fb98,0x9188d048
.word 0x7c7dee9a,0x8672c54a,0x8d8fcfcb,0x7780e41b
.word 0xc59d8ec1,0x3f92a511,0x346faf90,0xce608440
.word 0x2395ba92,0xd99a9142,0xd2679bc3,0x2868b013
.word 0x0c619096,0xf66ebb46,0xfd93b1c7,0x079c9a17
.word 0xea69a4c5,0x10668f15,0x1b9b8594,0xe194ae44
.word 0x7a4d26d1,0x80420d01,0x8bbf0780,0x71b02c50
.word 0x9c451282,0x664a3952,0x6db733d3,0x97b81803
.word 0xb3b13886,0x49be1356,0x424319d7,0xb84c3207
.word 0x55b90cd5,0xafb62705,0xa44b2d84,0x5e440654
.word 0xec596c8e,0x1656475e,0x1dab4ddf,0xe7a4660f
.word 0x0a5158dd,0xf05e730d,0xfba3798c,0x01ac525c
.word 0x25a572d9,0xdfaa5909,0xd4575388,0x2e587858
.word 0xc3ad468a,0x39a26d5a,0x325f67db,0xc8504c0b
.word 0xa713893c,0x5d1ca2ec,0x56e1a86d,0xacee83bd
.word 0x411bbd6f,0xbb1496bf,0xb0e99c3e,0x4ae6b7ee
.word 0x6eef976b,0x94e0bcbb,0x9f1db63a,0x65129dea
.word 0x88e7a338,0x72e888e8,0x79158269,0x831aa9b9
.word 0x3107c363,0xcb08e8b3,0xc0f5e232,0x3afac9e2
.word 0xd70ff730,0x2d00dce0,0x26fdd661,0xdcf2fdb1
.word 0xf8fbdd34,0x02f4f6e4,0x0909fc65,0xf306d7b5
.word 0x1ef3e967,0xe4fcc2b7,0xef01c836,0x150ee3e6
.word 0x8ed76b73,0x74d840a3,0x7f254a22,0x852a61f2
.word 0x68df5f20,0x92d074f0,0x992d7e71,0x632255a1
.word 0x472b7524,0xbd245ef4,0xb6d95475,0x4cd67fa5
.word 0xa1234177,0x5b2c6aa7,0x50d16026,0xaade4bf6
.word 0x18c3212c,0xe2cc0afc,0xe931007d,0x133e2bad
.word 0xfecb157f,0x04c43eaf,0x0f39342e,0xf5361ffe
.word 0xd13f3f7b,0x2b3014ab,0x20cd1e2a,0xdac235fa
.word 0x37370b28,0xcd3820f8,0xc6c52a79,0x3cca01a9
.word 0xf49a4da2,0x0e956672,0x05686cf3,0xff674723
.word 0x129279f1,0xe89d5221,0xe36058a0,0x196f7370
.word 0x3d6653f5,0xc7697825,0xcc9472a4,0x369b5974
.word 0xdb6e67a6,0x21614c76,0x2a9c46f7,0xd0936d27
.word 0x628e07fd,0x98812c2d,0x937c26ac,0x69730d7c
.word 0x848633ae,0x7e89187e,0x757412ff,0x8f7b392f
.word 0xab7219aa,0x517d327a,0x5a8038fb,0xa08f132b
.word 0x4d7a2df9,0xb7750629,0xbc880ca8,0x46872778
.word 0xdd5eafed,0x2751843d,0x2cac8ebc,0xd6a3a56c
.word 0x3b569bbe,0xc159b06e,0xcaa4baef,0x30ab913f
.word 0x14a2b1ba,0xeead9a6a,0xe55090eb,0x1f5fbb3b
.word 0xf2aa85e9,0x08a5ae39,0x0358a4b8,0xf9578f68
.word 0x4b4ae5b2,0xb145ce62,0xbab8c4e3,0x40b7ef33
.word 0xad42d1e1,0x574dfa31,0x5cb0f0b0,0xa6bfdb60
.word 0x82b6fbe5,0x78b9d035,0x7344dab4,0x894bf164
.word 0x64becfb6,0x9eb1e466,0x954ceee7,0x6f43c537
