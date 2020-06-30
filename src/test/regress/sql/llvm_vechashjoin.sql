/*
 * This file is used to test the function of hashjoin with LLVM Optimization
 */
----
--- Create Table and Insert Data
----
drop schema if exists llvm_hashjoin_engine cascade;
create schema llvm_hashjoin_engine;
set current_schema = llvm_hashjoin_engine;
set enable_nestloop=off;
set enable_mergejoin=off;
set codegen_cost_threshold=0;

CREATE TABLE llvm_hashjoin_engine.LLVM_HASHJOIN_TABLE_01(
    col_int1    int,
    col_int2    int,
    col_bint    bigint,
    col_char1   char(1),
    col_char2 	char(10),
    col_bchar1	bpchar(7),
    col_bchar2	bpchar(12),
    col_vchar1	varchar(18)
)with(orientation=column)
partition by range (col_int1)
(
    partition joinp1 values less than (100),
    partition joinp2 values less than (200),
    partition joinp3 values less than (500),
    partition joinp4 values less than (1000)
);

copy llvm_hashjoin_table_01 from stdin;
1	1	125	A	TIAN	GAUSS	EULER	BEAUTIFUL
1	2	256	B	DI	euler	whoisthis	BEAUTIFUL
2	25	698	C	JING	soft	software	Thisisgood
5	69	5874	D	likey	central	software	beautiful
9	852	471	B	tian	require	giveyou	seeyou
58	96	58	C	jing	data	people	people
5	12	68	E	lab	data	people	priority
12	2	45	F	department	dep	test	loft
1	658	47	a	just	gsql	give	vovitoofar
-45	78	584	h	opt	ment	mouth	partition
45	69	58	l	unit	join	join	partition
4	76	69	H	unit	warm	cold	apple
478	26	698	P	test	tie	tired	kop
895	69	741	Y	tweet	die	day	thoord
185	9857	7	D	nice	enter	enter	pastby
6	78	69	L	nice	nake	snake	nike
0	-90	-8	k	jump	fast	run	old
248	96	-2	F	ki	opt	play	play
25	65	-87	N	just	fast	mouth	phone
89	-12	8976	K	per	perfect	pacific	pear
658	\N	87	I	just	kj	lop	over
83	90	-1	\N	nice	flow	\N	wood
42	0	\N	d	\N	nhj	dea	\N
47	5	8	N	tea	sweet	sweet	nike
4	8	0	L	water	paper	nine	enter
-3	4	528	m	wa	J	BN	balance
77	5	6	L	achole	paper	Cv	coache
31	41	58	L	pi	pea	food	vegetable
345	87	985	L	pi	hook	health	healthbody
145	85	6	H	no	yes	food	phone
15	63	85	H	no	yes	good	justforfunok
-45	65	87	C	unit	join	mouth	partition
\.

CREATE TABLE llvm_hashjoin_engine.LLVM_HASHJOIN_TABLE_02(
    col_int1	int,
    col_int2	int,
    col_bint	bigint,
    col_char1	char(3),
    col_char2	char(10),
    col_bchar1	bpchar(7),
    col_bchar2	bpchar(12),
    col_vchar	varchar(18)
)with(orientation=column)
partition by range (col_int1)
(
    partition joinp1 values less than (100),
    partition joinp2 values less than (200),
    partition joinp3 values less than (500),
    partition joinp4 values less than (1000)
);

copy llvm_hashjoin_table_02 from stdin;
1	1	125	A	tian	lotion	that	nourishes
2	25	698	BCH	smooth	rough	three	beautiful
9	852	471	B	tian	require	giveyou	seeyou
147	589	8	D	jing	retui	iuh	iuh
248	96	-2	F	ki	opt	play	play
-45	65	87	C	unit	join	mouth	partition
15	48	96	BHG	achole	smoke	door	soft
23	-12	98	LHM	nice	alg	algorithm	vegetable
47	89	-2	K	skin	finger	instantly	home
-3	47	584	NHG	unit	pear	effect	effective
77	69	0	H	chair	desk	desktop	desktopnice
6	2	6	L	no	yes	good	justforfun
14	234	-78	F	ih	kij	past	apple
187	47	-2	U	ope	oper	operator	operator
248	687	7	LHG	light	light	heavy	heavy
957	2	32	F	hook	gsql	snake	tiger
478	69	8	KJH	unit	uou	uug	uugh
89	658	36	hij	lp	wang	kijh	sale
8	47	85	KI	P	mask	password	pastby
76	-2	78	E	tian	gsql	dep	enter
37	0	58	PIG	coat	nice	snale	nale
658	58	74	PIG	JK	incre	than	jon
31	96	58	m	jump	space	blank	white
8	0	6	h	\N	ki	\N	a
74	415	6874	\N	de	hbg	opy	\N
\.

analyze llvm_hashjoin_table_01;
analyze llvm_hashjoin_table_02;

----
--- test1 : test hashjoin with partition table
----
--- inner join
explain (verbose on, costs off) select A.col_int2, B.col_int2 from llvm_hashjoin_table_01 A inner join llvm_hashjoin_table_02 B on A.col_int1 = B.col_int1;
select A.col_int2, B.col_int2 from llvm_hashjoin_table_01 A inner join llvm_hashjoin_table_02 B on A.col_int1 = B.col_int1 order by 1, 2;

select A.col_int2, A.col_bint, B.col_int2, B.col_bint from llvm_hashjoin_table_01 A inner join llvm_hashjoin_table_02 B on A.col_int2 = B.col_int2 and A.col_bint = B.col_bint order by 1, 2, 3, 4;
select A.col_char1, A.col_char2, A.col_bchar1 from llvm_hashjoin_table_01 A inner join llvm_hashjoin_table_02 B on A.col_char1 = B.col_char1 order by 1, 2, 3;
select A.col_char2, B.col_bchar2 from llvm_hashjoin_table_01 A inner join llvm_hashjoin_table_02 B on A.col_char2 = B.col_char2 and A.col_bchar1 = B.col_bchar1 order by 1, 2;
select A.col_bchar1, B.col_bchar1, A.col_bint from llvm_hashjoin_table_01 A inner join llvm_hashjoin_table_02 B on A.col_bint = B.col_bint and A.col_bchar1 = B.col_bchar1 order by 1, 2, 3;

--- left/right join
select A.col_char1, B.col_int2 from llvm_hashjoin_table_01 A left join llvm_hashjoin_table_02 B on A.col_char1 = B.col_char1 and A.col_int2 = B.col_int2 order by 1, 2;
select A.col_char2, B.col_char2 from llvm_hashjoin_table_01 A right join llvm_hashjoin_table_02 B on A.col_char2 = B.col_char2 and A.col_int1 < 55 order by 1, 2;
select A.col_bchar2, B.col_bchar2 from llvm_hashjoin_table_01 A right join llvm_hashjoin_table_02 B on A.col_bchar2 = B.col_bchar2 where B.col_bchar2 is NULL order by 1, 2;

--- complicate hash keys 
select A.col_bint, A.col_bchar1, B.col_bint, B.col_bchar1 from llvm_hashjoin_table_01 A join llvm_hashjoin_table_02 B on substr(A.col_bchar1, 1, 1) = substr(B.col_bchar1, 1, 1) and A.col_bint = B.col_bint order by 1, 2, 3, 4;

----
--- test2 : text analysis_options with llvm dfx options
----
set analysis_options="on(LLVM_COMPILE, HASH_CONFLICT)";
select A.col_char1, B.col_int2 from llvm_hashjoin_table_01 A left join llvm_hashjoin_table_02 B on A.col_char1 = B.col_char1 and A.col_bint = B.col_bint order by 1, 2;
reset analysis_options;

----
---  clean table and resource
----
drop schema llvm_hashjoin_engine cascade;
