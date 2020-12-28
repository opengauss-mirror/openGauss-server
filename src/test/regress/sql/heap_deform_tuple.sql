create table heap_deform_minimal_tuple_normal_view_column_t(id1 int,id2 int);
create or replace view heap_deform_minimal_tuple_normal_view_column_v as select * from heap_deform_minimal_tuple_normal_view_column_t;
create temp table heap_deform_minimal_tuple_comment_view_column_t(id1 int,id2 int);
create or replace temp view heap_deform_minimal_tuple_comment_view_column_v as select * from heap_deform_minimal_tuple_comment_view_column_t;
comment on column heap_deform_minimal_tuple_normal_view_column_t.id1 is 'this is normal table';
comment on column heap_deform_minimal_tuple_normal_view_column_v.id1 is 'this is normal view';
comment on column heap_deform_minimal_tuple_comment_view_column_t.id1 is 'this is temp table';
comment on column heap_deform_minimal_tuple_comment_view_column_v.id1 is 'this is temp view';
\d+ heap_deform_minimal_tuple_normal_view_column_t
\d+ heap_deform_minimal_tuple_normal_view_column_v
drop view heap_deform_minimal_tuple_normal_view_column_v cascade;
drop table heap_deform_minimal_tuple_normal_view_column_t cascade;
drop view heap_deform_minimal_tuple_comment_view_column_v cascade;
drop table heap_deform_minimal_tuple_comment_view_column_t cascade;

CREATE TABLE heap_deform_minimal_tuple_s (rf_a SERIAL PRIMARY KEY,
	b INT);

CREATE TABLE heap_deform_minimal_tuple_1 (a SERIAL PRIMARY KEY,
	b INT,
	c TEXT,
	d TEXT
	);

CREATE INDEX heap_deform_minimal_tuple_b ON heap_deform_minimal_tuple_1 (b);
CREATE INDEX heap_deform_minimal_tuple_c ON heap_deform_minimal_tuple_1 (c);
CREATE INDEX heap_deform_minimal_tuple_c_b ON heap_deform_minimal_tuple_1 (c,b);
CREATE INDEX heap_deform_minimal_tuple_b_c ON heap_deform_minimal_tuple_1 (b,c);

INSERT INTO heap_deform_minimal_tuple_s (b) VALUES (0);
INSERT INTO heap_deform_minimal_tuple_s (b) SELECT b FROM heap_deform_minimal_tuple_s;
INSERT INTO heap_deform_minimal_tuple_s (b) SELECT b FROM heap_deform_minimal_tuple_s;
INSERT INTO heap_deform_minimal_tuple_s (b) SELECT b FROM heap_deform_minimal_tuple_s;
INSERT INTO heap_deform_minimal_tuple_s (b) SELECT b FROM heap_deform_minimal_tuple_s;
INSERT INTO heap_deform_minimal_tuple_s (b) SELECT b FROM heap_deform_minimal_tuple_s;
drop table heap_deform_minimal_tuple_s cascade;
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (11, 'once');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (10, 'diez');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (31, 'treinta y uno');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (22, 'veintidos');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (3, 'tres');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (20, 'veinte');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (23, 'veintitres');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (21, 'veintiuno');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (4, 'cuatro');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (14, 'catorce');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (2, 'dos');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (18, 'dieciocho');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (27, 'veintisiete');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (25, 'veinticinco');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (13, 'trece');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (28, 'veintiocho');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (32, 'treinta y dos');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (5, 'cinco');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (29, 'veintinueve');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (1, 'uno');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (24, 'veinticuatro');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (30, 'treinta');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (12, 'doce');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (17, 'diecisiete');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (9, 'nueve');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (19, 'diecinueve');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (26, 'veintiseis');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (15, 'quince');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (7, 'siete');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (16, 'dieciseis');
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (8, 'ocho');
-- This entry is needed to test that TOASTED values are copied correctly.
INSERT INTO heap_deform_minimal_tuple_1 (b, c, d) VALUES (6, 'seis', repeat('xyzzy', 100000));

CLUSTER heap_deform_minimal_tuple_c ON heap_deform_minimal_tuple_1;
INSERT INTO heap_deform_minimal_tuple_1 (b, c) VALUES (1111, 'this should fail');
ALTER TABLE heap_deform_minimal_tuple_1 CLUSTER ON heap_deform_minimal_tuple_b_c;

-- Try turning off all clustering
ALTER TABLE heap_deform_minimal_tuple_1 SET WITHOUT CLUSTER;
drop table heap_deform_minimal_tuple_1 cascade;




























create schema heap_deform_minimal_tuple_2;
set current_schema = heap_deform_minimal_tuple_2;
set time zone 'PRC';
set codegen_cost_threshold=0;
CREATE TABLE heap_deform_minimal_tuple_2.LLVM_VECEXPR_TABLE_01(
    col_int	int,
    col_bigint	bigint,
    col_float	float4,
    col_float8	float8,
    col_char	char(10),
    col_bpchar	bpchar,
    col_varchar	varchar,
    col_text1	text,
    col_text2   text,
    col_num1	numeric(10,2),
    col_num2	numeric,
    col_date	date,
    col_time    timestamp
)with(orientation=column)

partition by range (col_int)
(
    partition llvm_vecexpr_table_01_01 values less than (0),
    partition llvm_vecexpr_table_01_02 values less than (100),
    partition llvm_vecexpr_table_01_03 values less than (500),
    partition llvm_vecexpr_table_01_04 values less than (maxvalue)
);

COPY LLVM_VECEXPR_TABLE_01(col_int, col_bigint, col_float, col_float8, col_char, col_bpchar, col_varchar, col_text1, col_text2, col_num1, col_num2, col_date, col_time) FROM stdin;
1	256	3.1	3.25	beijing	AaaA	newcode	myword	myword1	3.25	3.6547	\N	2017-09-09 19:45:37
0	26	3.0	10.25	beijing	AaaA	newcode	myword	myword2	-3.2	-0.6547	\N	2017-09-09 19:45:37
3	12400	2.6	3.64755	hebei	BaaB	knife	sea	car	1.62	3.64	2017-10-09 19:45:37	2017-10-09 21:45:37
5	25685	1.0	25	anhui	CccC	computer	game	game2	7	3.65	2012-11-02 00:00:00	2018-04-09 19:45:37
-16	1345971420	3.2	2.15	hubei	AaaA	phone	pen	computer	-4.24	-6.36	2012-11-04 00:00:00	2012-11-02 00:03:10
-10	1345971420	3.2	2.15	hubei	AaaA	phone	pen	computer	4.24	0.00	2012-11-04 00:00:00	2012-11-02 00:03:10
64	-2566	1.25	2.7	jilin	DddD	girl	flower	window	65	-69.36	2012-11-03 00:00:00	2011-12-09 19:45:37
64	0	1.25	2.7	jilin	DddD	boy	flower	window	65	69.36	2012-11-03 00:00:00	2011-12-09 19:45:37
\N	256	3.1	4.25	anhui	BbbB	knife	phone	light	78.12	2.35684156	2017-10-09 19:45:37	1984-2-6 01:00:30
81	\N	4.8	3.65	luxi	EeeE	girl	sea	crow	145	56	2018-01-09 19:45:37	2018-01-09 19:45:37
8	\N	5.8	30.65	luxing	EffE	girls	sea	crown	\N	506	\N	\N
25	0	\N	3.12	lufei	EeeE	call	you	them	7.12	6.36848	2018-05-09 19:45:37	2018-05-09 19:45:37
36	5879	10.15	\N	lefei	GggG	say	you	them	2.5	-2.5648	2015-02-26 02:15:01	1984-2-6 02:15:01
36	59	10.15	\N	lefei	GggG	call	you	them	2.5	\N	2015-02-26 02:15:01	1984-2-6 02:15:01
0	0	10.15	\N	hefei	GggG	call	your	them	-2.5	2.5648	\N	1984-2-6 02:15:01
27	256	4.25	63.27	\N	FffF	code	throw	away	2.1	25.65	2018-03-09 19:45:37	\N
9	-128	-2.4	56.123	jiangsu	\N	greate	book	boy	7	-1.23	2017-12-09 19:45:37	 2012-11-02 14:20:25
1001	78956	1.25	2.568	hangzhou	CccC	\N	away	they	6.36	58.254	2017-10-09 19:45:37	1984-2-6 01:00:30
2005	12400	12.24	2.7	hangzhou	AaaA	flower	\N	car	12546	3.2546	2017-09-09 19:45:37	2012-11-02 00:03:10
8	5879	\N	1.36	luxi	DeeD	walet	wall	\N	2.58	3.54789	2000-01-01	2000-01-01 01:01:01
652	25489	8.88	1.365	hebei	god	piece	sugar	pow	\N	2.1	2012-11-02 00:00:00	2012-11-02 00:00:00
417	2	9.19	0.256	jiangxi	xizang	walet	bottle	water	11.50	-1.01256	\N	1984-2-6 01:00:30
18	65	-0.125	78.96	henan	PooP	line	black	redline	24	3.1415926	2000-01-01	\N
\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
-700	58964785	3.25	1.458	\N	qingdao	\N	2897	dog	9.36	\N	\N	2017-10-09 20:45:37
-505	1	3.24	\N	\N	BbbB	\N	myword	pen	147	875	2000-01-01 01:01:01	2000-01-01 01:01:01
\.


CREATE TABLE heap_deform_minimal_tuple_2.LLVM_VECEXPR_TABLE_02(
    col_bool	bool,
    col_sint	int2,
    col_int	int,
    col_bigint	bigint,
    col_char	char(10),
    col_bpchar	bpchar,
    col_varchar	varchar,
    col_text	text,
    col_date	date,
    col_time    timestamp
)with(orientation=column);
create index llvm_index_01 on llvm_vecexpr_table_02(col_int);
create index llvm_index_02 on llvm_vecexpr_table_02(col_char);
create index llvm_index_03 on llvm_vecexpr_table_02(col_varchar);
create index llvm_index_04 on llvm_vecexpr_table_02(col_text);
create index llvm_index_05 on llvm_vecexpr_table_02(col_date);

COPY LLVM_VECEXPR_TABLE_02(col_bool, col_sint, col_int, col_bigint, col_char, col_bpchar, col_varchar, col_text, col_date, col_time) FROM stdin;
f	1	0	256	11	111	1111	123456	2000-01-01 01:01:01	2000-01-01 01:01:01
1	1	1	0	101	11	11011	3456	\N	2000-01-01 01:01:01
0	2	2	128	24	75698	56789	12345	2017-09-09 19:45:37	\N
1	3	30	2899	11111	1111	12345	123456	2015-02-26	2012-12-02 02:15:01
0	4	417	0	245	111	1111	123456	2018-05-09 19:45:37	1984-2-6 01:00:30
f	5	\N	365487	111	1111	12345	123456	\N	1984-2-6 01:00:30
0	6	0	6987	11	111	24568	123456	\N	2018-03-07 19:45:37
t	7	18	1348971452	24	2563	2222	56789	2000-01-01	2000-01-01 01:01:01
0	8	\N	258	\N	1258	25879	25689	2014-05-12	2004-2-6 07:30:30
1	\N	569	254879963	11	\N	547	36589	2016-01-20	2012-11-02 00:00:00
\N	8	4	\N	11	111	\N	56897	2013-05-08	2012-11-02 00:03:10
\N	8	\N	\N	11	111	\N	56897	2013-05-08	2012-11-02 00:03:10
1	\N	56	58964	25	365487	5879	\N	2018-03-07	1999-2-6 01:00:30
t	\N	694	2	364	56897	\N	\N	2018-11-05	2011-2-6 01:00:30
f	-1	-30	-3658	5879	11	25879	\N	2018-03-07	\N
1	-2	-15	-24	3698	58967	698745	5879	2012-11-02	2012-11-02 00:00:00
\N	-3	2147483645	258	3698	36587	125478	111	2015-02-2	2000-01-01 01:01:01
0	12	-48	-9223372036854775802	258	36987	12587	2547	2014-03-12	2012-11-02 01:00:00
1	-3	-2	9223372036854775801	3689	256987	36547	14587	2016-01-20	2012-11-02 07:00:00
\N	-6	-2147483640	-1587452	1112	1115	12548	36589	\N	1999-2-6 01:00:30
t	-6	\N	-1587452	1112	1115	12548	36589	2014-03-12	\N
\.

analyze llvm_vecexpr_table_01;
analyze llvm_vecexpr_table_02;



select A.col_int, A.col_bigint, A.col_num1, a.col_float8, A.col_num1, a.col_date, 
        (A.col_num1 - A.col_int)/A.col_float8 <= A.col_bigint
        and ( substr(A.col_date, 1, 4) in (select substr(B.col_date, 1, 4) 
                                                from llvm_vecexpr_table_02 as B 
                                                ))
    from llvm_vecexpr_table_01 as A 
    order by 1, 2, 3, 4, 5, 6, 7;

drop schema heap_deform_minimal_tuple_2 cascade;
reset search_path;


create type type_array as (
id int,
name varchar(50),
score decimal(5,2),
create_time timestamp
);

create table heap_deform_minimal_tuple_3(a serial, b type_array[])
partition by range (a)
(partition p1 values less than(100),partition p2 values less than(maxvalue));

create table heap_deform_minimal_tuple_4(a serial, b type_array[]);

insert into heap_deform_minimal_tuple_3(b) values('{}');
insert into heap_deform_minimal_tuple_3(b) values(array[cast((1,'test',12,'2018-01-01') as type_array),cast((2,'test2',212,'2018-02-01') as type_array)]);
analyze heap_deform_minimal_tuple_3;

insert into heap_deform_minimal_tuple_4(b) values('');
insert into heap_deform_minimal_tuple_4(b) values(array[cast((1,'test',12,'2018-01-01') as type_array),cast((2,'test2',212,'2018-02-01') as type_array)]);
select * from heap_deform_minimal_tuple_4 where b>array[cast((0,'test',12,'') as type_array),cast((1,'test2',212,'') as type_array)]
order by 1,2;
update heap_deform_minimal_tuple_3 set b=array[cast((1,'test',12,'2018-01-01') as type_array),cast((2,'test2',212,'2018-02-01') as type_array)] where b='{}';

create index i_array on heap_deform_minimal_tuple_3(b) local;
select * from heap_deform_minimal_tuple_3 where b>array[cast((0,'test',12,'') as type_array),cast((1,'test2',212,'') as type_array)]
order by 1,2;

alter type type_array add attribute attr bool;
SELECT b, LISTAGG(a, ',') WITHIN GROUP(ORDER BY b DESC)
FROM heap_deform_minimal_tuple_3 group by 1;

drop type type_array cascade;
drop table heap_deform_minimal_tuple_3 cascade;
drop table  heap_deform_minimal_tuple_4 cascade;












CREATE TEMP TABLE y (
	col1 text,
	col2 text
);

INSERT INTO y VALUES ('Jackson, Sam', E'\\h');
INSERT INTO y VALUES ('It is "perfect".',E'\t');
INSERT INTO y VALUES ('', NULL);

COPY y TO stdout WITH CSV;
COPY y TO stdout WITH CSV QUOTE '''' DELIMITER '|';
COPY y TO stdout WITH CSV FORCE QUOTE col2 ESCAPE E'\\' ENCODING 'sql_ascii';
COPY y TO stdout WITH CSV FORCE QUOTE *;

-- Repeat above tests with new 9.0 option syntax

COPY y TO stdout (FORMAT CSV);
COPY y TO stdout (FORMAT CSV, QUOTE '''', DELIMITER '|');
COPY y TO stdout (FORMAT CSV, FORCE_QUOTE (col2), ESCAPE E'\\');
COPY y TO stdout (FORMAT CSV, FORCE_QUOTE *);

\copy y TO stdout (FORMAT CSV)
\copy y TO stdout (FORMAT CSV, QUOTE '''', DELIMITER '|')
\copy y TO stdout (FORMAT CSV, FORCE_QUOTE (col2), ESCAPE E'\\')
\copy y TO stdout (FORMAT CSV, FORCE_QUOTE *)

CREATE TEMP TABLE testnl (a int, b text, c int);

COPY testnl FROM stdin CSV;
1,"a field with two LFs

inside",2
\.

-- test end of copy marker
CREATE TABLE testeoc (a text);

COPY testeoc FROM stdin CSV;
a\.
\.b
c\.d
"\."
\.
drop table testnl cascade;
drop table testeoc cascade;
drop table y cascade;













CREATE TYPE type_array AS (
id int,
name varchar(50),
score decimal(5,2),
create_time timestamp
);
CREATE TABLE heap_deform_minimal_tuple_5(a serial, b type_array[])
PARTITION BY RANGE (a)
(PARTITION p1 VALUES LESS THAN(100),PARTITION p2 VALUES LESS THAN(maxvalue));

INSERT INTO heap_deform_minimal_tuple_5(b) VALUES('{}');
INSERT INTO heap_deform_minimal_tuple_5(b) VALUES(ARRAY[CAST((1,'test',12,'2018-01-01') AS type_array),CAST((2,'test2',212,'2018-02-01') AS type_array)]);
analyze heap_deform_minimal_tuple_5;

UPDATE heap_deform_minimal_tuple_5 SET b=ARRAY[CAST((1,'test',12,'2018-01-01') AS type_array),CAST((2,'test2',212,'2018-02-01') AS type_array)] WHERE b='{}';

CREATE INDEX i_array ON heap_deform_minimal_tuple_5(b) local;
SELECT * FROM heap_deform_minimal_tuple_5 WHERE b>ARRAY[CAST((0,'test',12,'') AS type_array),CAST((1,'test2',212,'') AS type_array)]
ORDER BY 1,2;

alter TYPE type_array add attribute attr bool;
SELECT b, LISTAGG(a, ',') WITHIN GROUP(ORDER BY b DESC)
FROM heap_deform_minimal_tuple_5 GROUP BY 1;
DROP TYPE type_array CASCADE;
DROP TABLE heap_deform_minimal_tuple_5 CASCADE;
CREATE TABLE heap_deform_minimal_tuple_6(col1 int, col2 int, col3 text);
CREATE INDEX iExecClearTuple_6 ON heap_deform_minimal_tuple_6(col1,col2);
INSERT INTO heap_deform_minimal_tuple_6 VALUES (0,0,'test_insert');
INSERT INTO heap_deform_minimal_tuple_6 VALUES (0,1,'test_insert');
INSERT INTO heap_deform_minimal_tuple_6 VALUES (1,1,'test_insert');
INSERT INTO heap_deform_minimal_tuple_6 VALUES (1,2,'test_insert');
INSERT INTO heap_deform_minimal_tuple_6 VALUES (0,0,'test_insert2');
INSERT INTO heap_deform_minimal_tuple_6 VALUES (2,2,'test_insert2');
INSERT INTO heap_deform_minimal_tuple_6 VALUES (0,0,'test_insert3');
INSERT INTO heap_deform_minimal_tuple_6 VALUES (3,3,'test_insert3');
INSERT INTO heap_deform_minimal_tuple_6(col1,col2) VALUES (1,1);
INSERT INTO heap_deform_minimal_tuple_6(col1,col2) VALUES (2,2);
INSERT INTO heap_deform_minimal_tuple_6(col1,col2) VALUES (3,3);
INSERT INTO heap_deform_minimal_tuple_6 VALUES (null,null,null);
SELECT col1,col2 FROM heap_deform_minimal_tuple_6 WHERE col1=0 AND col2=0 ORDER BY col1,col2 for UPDATE LIMIT 1;
DROP TABLE heap_deform_minimal_tuple_6 CASCADE;









CREATE TABLE heap_deform_minimal_tuple_7 (KEY int PRIMARY KEY);
INSERT INTO heap_deform_minimal_tuple_7 VALUES (10);
INSERT INTO heap_deform_minimal_tuple_7 VALUES (20);
INSERT INTO heap_deform_minimal_tuple_7 VALUES (30);
INSERT INTO heap_deform_minimal_tuple_7 VALUES (40);
INSERT INTO heap_deform_minimal_tuple_7 VALUES (50);
-- Test UPDATE WHERE the old row version is found first in the scan
UPDATE heap_deform_minimal_tuple_7 SET KEY = 100 WHERE KEY = 10;
-- Test UPDATE WHERE the new row version is found first in the scan
UPDATE heap_deform_minimal_tuple_7 SET KEY = 35 WHERE KEY = 40;
-- Test longer UPDATE chain
UPDATE heap_deform_minimal_tuple_7 SET KEY = 60 WHERE KEY = 50;
UPDATE heap_deform_minimal_tuple_7 SET KEY = 70 WHERE KEY = 60;
UPDATE heap_deform_minimal_tuple_7 SET KEY = 80 WHERE KEY = 70;
DROP TABLE heap_deform_minimal_tuple_7 CASCADE;






CREATE TABLE heap_deform_minimal_tuple_8 (col1 int PRIMARY KEY, col2 INT, col3 smallserial)  ;
PREPARE p1 AS INSERT INTO heap_deform_minimal_tuple_8 VALUES($1, $2) ON DUPLICATE KEY UPDATE col2 = $1*100;
EXECUTE p1(5, 50);
SELECT * FROM heap_deform_minimal_tuple_8 WHERE col1 = 5;
EXECUTE p1(5, 50);
SELECT * FROM heap_deform_minimal_tuple_8 WHERE col1 = 5;
DELETE heap_deform_minimal_tuple_8 WHERE col1 = 5;
DEALLOCATE p1;
DROP TABLE heap_deform_minimal_tuple_8 CASCADE;









create table tGin122 (
        name varchar(50) not null, 
        age int, 
        birth date, 
        ID varchar(50) , 
        phone varchar(15),
        carNum varchar(50),
        email varchar(50), 
        info text, 
        config varchar(50) default 'english',
        tv tsvector,
        i varchar(50)[],
        ts tsquery);
insert into tGin122 values('Linda', 20, '1996-06-01', '140110199606012076', '13454333333', '京A QL666', 'linda20@sohu.com', 'When he was busy with teaching men the art of living, Prometheus had left a bigcask in the care of Epimetheus. He had warned his brother not to open the lid. Pandora was a curious woman. She had been feeling very disappointed that her husband did not allow her to take a look at the contents of the cask. One day, when Epimetheus was out, she lifted the lid and out it came unrest and war, Plague and sickness, theft and violence, grief, sorrow, and all the other evils. The human world was hence to experience these evils. Only hope stayed within the mouth of the jar and never flew out. So men always have hope within their hearts.
偷窃天火之后，宙斯对人类的敌意与日俱增。一天，他令儿子赫菲斯托斯用泥塑一美女像，并请众神赠予她不同的礼物。世上的第一个女人是位迷人女郎，因为她从每位神灵那里得到了一样对男人有害的礼物，因此宙斯称她为潘多拉。
', 'ngram', '', '{''brother'',''与日俱增'',''赫菲斯托斯''}',NULL);
insert into tGin122 values('张三', 20,  '1996-07-01', '140110199607012076', '13514333333', '鲁K QL662', 'zhangsan@163.com', '希腊北部国王阿塔玛斯有两个孩子，法瑞克斯和赫勒。当国王离
开第一个妻子和一个名叫伊诺的坏女人结婚后，两个孩子受到后母残忍虐待，整个王国也受到毁灭性瘟疫的侵袭。伊诺在爱轻信的丈夫耳边进谗言，终于使国王相信：他的儿子法瑞克斯是这次灾害的罪魁祸首，并要将他献给宙斯以结束
瘟疫。可怜的孩子被推上了祭坛，将要被处死。正在此时，上帝派了一只浑身上下长着金色羊毛的公羊来将两个孩子驮在背上带走了。当他们飞过隔开欧洲和亚洲的海峡时，赫勒由于看到浩瀚的海洋而头晕目眩，最终掉进大海淹死了。
这片海洋古时候的名称叫赫勒之海，赫勒拉旁海峡便由此而来。金色公羊驮着法瑞克斯继续向前飞去，来到了黑海东岸的科尔契斯。在那里，法瑞克斯将公羊献给了宙斯；而将金羊毛送给了埃厄忒斯国王。国王将羊毛钉在一棵圣树上，
并派了一条不睡觉的龙负责看护。', 'ngram', '',  '{''法瑞克斯和赫勒'',''王国'',''埃厄忒斯国王''}',NULL); 
insert into tGin122 values('Sara', 20,  '1996-07-02', '140110199607022076', '13754333333', '冀A QL661', 'sara20@sohu.com', '英语语言结构重形合（hypotaxis），汉语重义合（parataxis）>，也就是说，英语的句子组织通常通过连接词（connectives）和词尾的曲折变化（inflection）来实现，汉语则较少使用连接词和受语法规则约束。英语句子通过表示各种关系如因果、条件、逻辑、预设等形合手段组织，环环相扣，>可以形成像树枝一样包孕许多修饰成分和分句的长句和复杂句，而汉语则多用短句和简单句。此外，英语注重使用各种短语作为句子的构成单位，在修饰位置上可前可后、十分灵活，常习惯于后置语序。这些差异就形成了王力先生所谓
的英语“化零为整”而汉语则“化整为零”特点。此外，英语多用被动语态，这在科技英语中尤为如此。了解英语和汉语这些造句差异，就可在英语长句和复杂句的理解和翻译中有意识地将英语句子按照汉语造句特点进行转化处理，短从句结构变单独句子或相反，后置变前置，被动变主动。以下结合本人在教学中遇到的例子，说说如何对生物类专业英语长句和复杂句翻译进行翻译处理。', 'english', '',  '{''parataxis'',''后置变前置'',''差异''}',NULL);
insert into tGin122 values('Mira', 20,  '1996-08-01', '140110199608012076', '13654333333', '津A QL660', 'mm20@sohu.com', '[解析]第一个分句宜将被动语态译为主动语态，第二个分句如将定>语分句处理为汉语前置，“利用能在培养组织中迅速降解而无需提供第二种生根培养基的IAA则是克服这个问题的一种有用方法。”则会因修饰语太长，不易理解，也不符合汉语习惯，宜作为分句处理。[翻译]根发端所需的生长素水平抑制根的伸长，而利用IAA则是克服这个问题的一种有用方法，因为IAA能在培养组织中迅速降解而无需提供第二种生根培养基。', 'english', '',  '{''汉语前置'',''分句处理'',''生长素水平''}',NULL);
insert into tGin122 values('Amy', 20,  ' 1996-09-01', '140110199609012076', '13854333333', '吉A QL663', 'amy2008@163.com', '[解析]该句的理解的关键是要抓住主句的结构“Current concern focus on ……, and on……”，同时不要将第二个“on”的搭配（intrusionon）与主句中第一个和第三个“on”的搭配（focuson）混淆。翻译时，为了避免宾语的修补词过长，可用“目前公众对转基因植物的关注集中在这两点”来用“一方面……；另一方面……”来分述，这样处理更符合汉语习惯。', 'ngram', '',  '{''intrusionon'',''13854333333'',''140110199609012076''}',NULL);
insert into tGin122 values('汪玲沁 ', 20,  ' 1996-09-01', '44088319921103106X', '13854333333', '沈YWZJW0', 'si2008@163.com', '晨的美好就如青草般芳香，如河溪般清澈，如玻璃般透明，如>甘露般香甜。[解析]该句的主句结构为“This led to a whole new field of academic research”，后面有一个现在分词结构“including the milestone paper by Paterson and co-workers in 1988”之后为“the milestone pape长定语从句。在翻译时，宜将该定语从句分译成句，但要将表示方法手段的现在分词结构“using an approach that could be applied to dissect the genetic make-up of any physiological, morphological and behavioural trat in plants and animals”前置译出，这样更符合汉语的表达习惯。', 'ngram', '',  '{''44088319921103106X'',''分词结构'',''透明''}',NULL);
create index tgin122_idx1 on tgin122 (substr(email,2,5));
create index tgin122_idx2 on tgin122 (upper(info));
set default_statistics_target=-2;
analyze tGin122 ((tv, ts));
select * from pg_ext_stats where schemaname='distribute_stat_2' and tablename='tgin122' order by attname;
alter table tGin122 delete statistics ((tv, ts));
update tGin122 set tv=to_tsvector(config::regconfig, coalesce(name,'') || ' ' || coalesce(ID,'') || ' ' || coalesce(carNum,'') || ' ' || coalesce(phone,'') || ' ' || coalesce(email,'') || ' ' || coalesce(info,''));
update tGin122 set ts=to_tsquery('ngram', coalesce(phone,'')); 
analyze tGin122 ((tv, ts));
select * from pg_ext_stats where schemaname='distribute_stat_2' and tablename='tgin122' order by attname;
alter table tGin122 delete statistics ((tv, ts));
select * from pg_ext_stats where schemaname='distribute_stat_2' and tablename='tgin122' order by attname;
alter table tGin122 add statistics ((tv, ts));
analyze tGin122;
select * from pg_ext_stats where schemaname='distribute_stat_2' and tablename='tgin122' order by attname;
select * from pg_stats where tablename='tgin122' and attname = 'tv';
select attname,avg_width,n_distinct,histogram_bounds from pg_stats where tablename='tgin122_idx1';
drop table tgin122 cascade;




create table heap_deform_minimal_tuple_10 (atcol1 serial8, atcol2 boolean,
	constraint heap_deform_minimal_tuple_chk check (atcol1 <= 3));;

insert into heap_deform_minimal_tuple_10 (atcol1, atcol2) values (default, true);
insert into heap_deform_minimal_tuple_10 (atcol1, atcol2) values (default, false);
select * from heap_deform_minimal_tuple_10 order by atcol1, atcol2;
alter table heap_deform_minimal_tuple_10 alter column atcol1 type boolean;
drop table heap_deform_minimal_tuple_10;


CREATE TYPE test_type3 AS (a int);
CREATE TABLE test_tbl3 (c) AS SELECT '(1)'::test_type3;
drop type test_type3 cascade;
drop table test_tbl3 cascade;






create table heap_deform_minimal_tuple_11
(
	 a_tinyint tinyint ,
	 a_smallint smallint not null,
	 a_numeric numeric(18,2) , 
	 a_decimal decimal null,
	 a_real real null,
	 a_double_precision double precision null,
	 a_dec   dec ,
	 a_integer   integer default 100,
	 a_char char(5) not null,
	 a_varchar varchar(15) null,
	 a_nvarchar2 nvarchar2(10) null,
	 a_text text   null,
	 a_date date default '2015-07-07',
	 a_time time without time zone,
	 a_timetz time with time zone default '2013-01-25 23:41:38.8',
	 a_smalldatetime smalldatetime,
	 a_money  money not null,
	 a_interval interval
);
insert into heap_deform_minimal_tuple_11 (a_smallint,a_char,a_text,a_money) values(generate_series(1,500),'fkdll','65sdcbas',20);
insert into heap_deform_minimal_tuple_11 (a_smallint,a_char,a_text,a_money) values(100,'fkdll','65sdcbas',generate_series(1,400));
create table heap_deform_minimal_tuple_12
(
	 a_tinyint tinyint ,
	 a_smallint smallint not null,
	 a_numeric numeric(18,2) , 
	 a_decimal decimal null,
	 a_real real null,
	 a_double_precision double precision null,
	 a_dec   dec ,
	 a_integer   integer default 100,
	 a_char char(5) not null,
	 a_varchar varchar(15) null,
	 a_nvarchar2 nvarchar2(10) null,
	 a_text text   null,
	 a_date date default '2015-07-07',
	 a_time time without time zone,
	 a_timetz time with time zone default '2013-01-25 23:41:38.8',
	 a_smalldatetime smalldatetime,
	 a_money  money not null,
	 a_interval interval,
partial cluster key(a_smallint)) with (orientation=column, compression = high)  ;
create index create_index_repl_trans_002 on heap_deform_minimal_tuple_12(a_smallint,a_date,a_integer);
insert into heap_deform_minimal_tuple_12 select * from heap_deform_minimal_tuple_11;

start transaction;
alter table  heap_deform_minimal_tuple_12 add column a_char_01 char(20) default '中国制造';
insert into  heap_deform_minimal_tuple_12 (a_smallint,a_char,a_money,a_char_01) values(generate_series(1,10),'li',21.1,'高斯部');
delete from heap_deform_minimal_tuple_12 where a_smallint>5 and a_char_01='高斯部';
rollback;


drop table heap_deform_minimal_tuple_12 cascade;
drop table heap_deform_minimal_tuple_11 cascade;
