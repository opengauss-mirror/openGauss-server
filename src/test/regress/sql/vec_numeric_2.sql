---- prepare work
DROP SCHEMA IF EXISTS vec_numeric_to_bigintger_2 CASCADE;
CREATE SCHEMA vec_numeric_to_bigintger_2;
SET current_schema = vec_numeric_to_bigintger_2;
SET enable_fast_numeric = on;

create table num_row(id int, cu int, num numeric(30,5)) ;
create table num_col(id int, cu int, num numeric(30,5)) with (orientation = column) ;

-- int32
insert into num_row values (1, 1, 0),(1, 1, 0.0),(1, 1, 100),(1, 1, 100.1),(1, 1, 10000.1111),(1, 1, 10000.11111),(1, 1, 21474.83647);

-- int64
insert into num_row values (1, 2, 21474.83648),(1, 2, 1000000000.1),(1, 2, 1000000111.1),(1, 2, 1001000011.11),(1, 2, 1000000000000.11111),(1, 2, 1111111111111.11111);

-- numeric
insert into num_row values (1, 3, 110000000000000.11111),(1, 3, 9223372013685477.5807),(1, 3, 9223372036854775808);

-- int32 and int64
insert into num_row values (1, 4, 10000.1111),(1, 4, 1000000111.1),(1, 4, 10000.11111),(1, 4, 1001000011.11),(1, 4, 100.111);

-- int64 and numeric
insert into num_row values (1, 5, 1000000000.1),(1, 5, 9223372013685477.5807),(1, 5, 1000000111.1),(1, 5, 1001000011.11),(1, 5, 1000100111.1),(1, 5, 1010000111.1);

-- int32 and numeric
insert into num_row values (1, 6, 0.0),(1, 6, 10000.1111),(1, 6, 110000000000000.11111),(1, 6, 10000.11111),(1, 6, 10100.11111),(1, 6, 10101.1111);

-- int32, int64 and numeric
insert into num_row values (1, 7, 100),(1, 7, 100.1),(1, 7, 10000.1111),(1, 7, 10000.11111),(1, 7, 10000000000000.11111),(1, 7, 0),(1, 7, 1000000000.1),(1, 7, 1000000111.1),(1, 7, 1001000011.11);

-- null
insert into num_row values (1, 8, 1000000000.1),(1, 8, 9223372013685477.5807),(1, 8, 1000000111.1),(1, 8, 21474.83648),(1, 8, null),(1, 8, 0.0),(1, 8, 100.111),(1, 8, 10000.1111);
insert into num_row values (1, 8, null),(1, 8, 10101.0101),(1, 8, 1000000111.1),(1, 8, 1001000011.11),(1, 8, 100),(1, 8, 100.1),(1, 8, 1111111111111.11111);

-- same value
insert into num_row values (1, 9, 10101.0101),(1, 9, 10101.0101),(1, 9, 10101.0101),(1, 9, 1111111111111.11111),(1, 9, 1111111111111.11111),(1, 9, 1111111111111.11111);

insert into num_row values (1, 10, 0),(1, 10, 1111111111111.11111),(1, 10, 110000000000000.11111);

-- int128
insert into num_row values (1, 11, 10000000000000000000),(1, 11, 9e20),(1, 11, 11.11e20);

-- insert column table by cu
insert into num_col select * from num_row where cu>=1 and cu <=11;


select * from num_col order by 1, 2, 3;
truncate num_col;

-- copy
-- int32
copy num_col from stdin;
1	1	0
1	1	0.0
1	1	100
1	1	100.1
1	1	10000.1111
1	1	10000.11111
1	1	21474.83647
\.

-- int64
copy num_col from stdin;
1	2	21474.83648
1	2	1000000000.1
1	2	1000000111.1
1	2	1001000011.11
1	2	1000000000000.11111
1	2	1111111111111.11111
\.

-- numeric
copy num_col from stdin;
1	3	110000000000000.11111
1	3	9223372013685477.5807
1	3	9223372036854775808
\.

-- int32 and int64
copy num_col from stdin;
1	4	10000.1111
1	4	1000000111.1
1	4	10000.11111
1	4	1001000011.11
1	4	100.111
\.

-- int64 and numeric
copy num_col from stdin;
1	5	1000000000.1
1	5	9223372013685477.5807
1	5	1000000111.1
1	5	1001000011.11
1	5	1000100111.1
1	5	1010000111.1
\.

-- int32 and numeric
copy num_col from stdin;
1	6	0.0
1	6	10000.1111
1	6	110000000000000.11111
1	6	10000.11111
1	6	10100.11111
1	6	10101.1111
\.

-- int32,int64 and numeric
copy num_col from stdin;
1	7	100
1	7	100.1
1	7	10000.1111
1	7	10000.11111
1	7	10000000000000.11111
1	7	0
1	7	1000000000.1
1	7	1000000111.1
1	7	1001000011.11
\.

-- null
copy num_col from stdin;
1	8	1000000000.1
1	8	9223372013685477.5807
1	8	1000000111.1
1	8	21474.83648
1	8	
1	8	0.0
1	8	100.111
1	8	10000.1111
1	8	
1	8	10101.0101
1	8	1000000111.1
1	8	1001000011.11
1	8	100
1	8	100.1
1	8	1111111111111.11111
\.

-- same value
copy num_col from stdin;
1	9	10101.0101
1	9	10101.0101
1	9	10101.0101
1	9	1111111111111.11111
1	9	1111111111111.11111
1	9	1111111111111.11111
\.

copy num_col from stdin;
1	10	0
1	10	1111111111111.11111
1	10	110000000000000.11111
\.

-- int128
copy num_col from stdin;
1	11	10000000000000000000
1	11	9e20
1	11	11.11e20
\.

select * from num_col;
create table num_col2(like num_col including all);
insert into num_col2 select * from num_col;
select * from num_col2 order by 1, 2, 3;

SET ENABLE_HASHAGG=FALSE;
CREATE TABLE AGG_BATCH_1_005(
C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE
, PARTIAL CLUSTER KEY(C_NUMERIC,C_CHAR_2)) WITH (ORIENTATION=COLUMN);
INSERT INTO AGG_BATCH_1_005 VALUES('A','ABCfeefgeq','1111ABCDEFGGAHWGS','a','abcdx','1111ABHTFADFADFDAFAFEFAGEAFEAFEAGEAGEAGEE_',455,100000,87,0.0001,0.00001,0.000001,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
-- TEST AVG
SELECT AVG(C_NUMERIC) FROM AGG_BATCH_1_005 GROUP BY C_CHAR_2 ORDER BY C_CHAR_2;

create table agg_batch_1 (id int, val1 numeric(20,0), val2 numeric(18, 18)) with (orientation=column) ;
insert into agg_batch_1 values (1, 888888888888888888, 0.999999999999999999);

---- bi64div64 bi128div128
select val1 / val2 from agg_batch_1;
select val1 / 0 from agg_batch_1;
select val1 / (val2 * 1.00) from agg_batch_1;
select (val2 * -1.00)/val1 from agg_batch_1;

---- bi64cmp64_smaller ---- bi64cmp64_larger
insert into agg_batch_1 values (1, -888888888888888888, -0.999999999999999999);
select id, min(case when val1 < 0 then val1 else 9999999999999999.99 end ), max(case when val1 < 0 then val1 else 9999999999999999.99 end ) from agg_batch_1 group by id;
select id, min(case when val1 < 0 then 888888888888888888 else 9999999999999999.99 end ), max(case when val1 < 0 then 888888888888888888 else 9999999999999999.99 end ) from agg_batch_1 group by id;
select id, min(case when val1 > 0 then 888888888888888888 else 9999999999999999.99 end ), max(case when val1 > 0 then 888888888888888888 else 9999999999999999.99 end ) from agg_batch_1 group by id;
select id, min(case when val1 > 0 then -888888888888888888 else 9999999999999999.99 end ), max(case when val1 > 0 then -888888888888888888 else 9999999999999999.99 end ) from agg_batch_1 group by id;

select id, min(case when val1 > 0 then val1 * 100 else val2 end), max(case when val1 > 0 then val1 * 100 else val2 end) from agg_batch_1 group by id;
select id, min(case when val1 > 0 then val1 * -100 else val2 end), max(case when val1 > 0 then val1 * -100 else val2 end) from agg_batch_1 group by id;
select id, min(case when val1 > 0 then val1  else val2 * 1.00 end), max(case when val1 > 0 then val1  else val2 * 1.00 end) from agg_batch_1 group by id;
select id, min(case when val1 > 0 then 9999999999999999999999999  else 0.88888888888888888888888 end), max(case when val1 > 0 then 9999999999999999999999999  else 0.88888888888888888888888 end) from agg_batch_1 group by id;

---- test int1_numeric_bi/int2_numeric_bi/int4_numeric_bi/int8_numeric_bi
create table agg_batch_2 (id int, val1 tinyint, val2 smallint, val3 int, val4 bigint, val5 numeric(7,2)) with (orientation = column);
insert into agg_batch_2 values (1,1,1,1,1,1),(1,2,2,2,2,2),(1,3,3,3,3,3);
select id, sum(val1 + val5), sum(val2 - val5), sum(val3 * val5), sum(val4 / val5) from agg_batch_2 group by id;

---- test numeric column partition info
create table item_less_1
(
    id                  integer               not null,
    val                 decimal(19,18)
) with (orientation=column) partition by range(val)
(
 partition p1 values less than(-5.00000),
 partition p2 values less than(-1.00000),
 partition p3 values less than(0.00000),
 partition p4 values less than(1.00000),
 partition p5 values less than(3.0000),
 partition p6 values less than(5.000000),
 partition p7 values less than(maxvalue)
);

create table item_less_2
(
    id                  integer               not null,
    val                 decimal(19,18)
) 
partition by range(val)
(
 partition p1 values less than(-5.00000),
 partition p2 values less than(-1.00000),
 partition p3 values less than(0.00000),
 partition p4 values less than(1.00000),
 partition p5 values less than(3.0000),
 partition p6 values less than(5.000000),
 partition p7 values less than(maxvalue)
); 

copy item_less_2 from stdin DELIMITER as ',' NULL as '' ;
1,-6.1
2,-7.4
3,-3.2
4,-4.7
5,-2.2
6,-1.1
7,-0.9
8,0.2
9,1.2
10,3
\.

insert into item_less_1 select * from item_less_2;

select count(*) from item_less_1 where val <= -5;
select count(*) from item_less_2 where val <= -5;

---- test hash_bi
create table test_vec_numeric_hash (id int, val1 numeric(18,5), val2 numeric(39, 5), val3 numeric(100, 50), val4 numeric) with (orientation=column);
insert into test_vec_numeric_hash values (1, 999999999999, 9999999999999999999999999999999999.99999, 9999999999999999999999999999999999.99999, 9999999999999999999999999999999999.99999);
insert into test_vec_numeric_hash values (1, 999999999999, 9999999999999999999999999999999999.99999, 9999999999999999999999999999999999.99999, 9999999999999999999999999999999999.99999);
insert into test_vec_numeric_hash values (1, 1, 1, 1, 1),(1, 1, 1, 1, 1),(1, 1, 1, 1, 1),(1, 0, 0, 0, 0);

select sum(id), val1 from test_vec_numeric_hash group by val1 order by 1,2;
select sum(id), val1 * val1  from test_vec_numeric_hash group by val1 * val1 order by 1,2;
select sum(id), val2 from test_vec_numeric_hash group by val2 order by 1,2;
select sum(id), sum(val1) from test_vec_numeric_hash group by case when id >= 0 and id <= 2 then val2 when id >= 3 and id <= 4 then 9999999999999999999999999999999999.99999 else 1 end  order by 1,2;
select sum(id), val1 * val2, val1 * val3  from test_vec_numeric_hash group by val1 * val2, val1 * val3 order by 1,2;
select count(*) from test_vec_numeric_hash as t1, test_vec_numeric_hash as t2 where t1.val2 = t2.val1;
select count(*) from test_vec_numeric_hash as t1, test_vec_numeric_hash as t2 where t1.val2 = t2.val2;
select count(*) from test_vec_numeric_hash as t1, test_vec_numeric_hash as t2 where t1.val2 = t2.val3;
select count(*) from test_vec_numeric_hash as t1, test_vec_numeric_hash as t2 where t1.val2 = t2.val4;
select count(*) from test_vec_numeric_hash t1 inner join test_vec_numeric_hash t2 on t1.val1 = t2.val4;

drop table test_vec_numeric_hash;
create table test_vec_numeric_hash (id int, num numeric(40,4)) with (orientation=column);
insert into test_vec_numeric_hash values (1, 11111111111111111111111111111111111.1), (1, 11111111111111111111111111111111111.01), (1, 11111111111111111111111111111111111.001), (1, 11111111111111111111111111111111111.0001), (1, 1), (1, 2);
select num, sum(id) from test_vec_numeric_hash group by num order by 1, 2;
select ln(num), sqrt(num), num::bigint, num::int, num::smallint, num::tinyint from test_vec_numeric_hash where num < 2 order by 1,2,3;
---- DROP SCHEMA
DROP SCHEMA vec_numeric_to_bigintger_2 CASCADE;
