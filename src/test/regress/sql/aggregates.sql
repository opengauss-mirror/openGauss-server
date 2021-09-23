--
--FOR BLACKLIST FEATURE: CREATE AGGERGATE、INHERITS is not supported.
--
--
-- AGGREGATES
--

SELECT avg(four) AS avg_1 FROM onek;

SELECT avg(a) AS avg_32 FROM aggtest WHERE a < 100;

-- In 7.1, avg(float4) is computed using float8 arithmetic.
-- Round the result to 3 digits to avoid platform-specific results.

SELECT avg(b)::numeric(10,3) AS avg_107_943 FROM aggtest;

SELECT avg(gpa) AS avg_3_4 FROM ONLY student;


SELECT sum(four) AS sum_1500 FROM onek;
SELECT sum(a) AS sum_198 FROM aggtest;
SELECT sum(b) AS avg_431_773 FROM aggtest;
SELECT sum(gpa) AS avg_6_8 FROM ONLY student;

SELECT max(four) AS max_3 FROM onek;
SELECT max(a) AS max_100 FROM aggtest;
SELECT max(aggtest.b) AS max_324_78 FROM aggtest;
SELECT max(student.gpa) AS max_3_7 FROM student;

SELECT stddev_pop(b) FROM aggtest;
SELECT stddev_samp(b) FROM aggtest;
SELECT var_pop(b) FROM aggtest;
SELECT var_samp(b) FROM aggtest;

SELECT stddev_pop(b::numeric) FROM aggtest;
SELECT stddev_samp(b::numeric) FROM aggtest;
SELECT var_pop(b::numeric) FROM aggtest;
SELECT var_samp(b::numeric) FROM aggtest;

-- population variance is defined for a single tuple, sample variance
-- is not
SELECT var_pop(1.0), var_samp(2.0);
SELECT stddev_pop(3.0::numeric), stddev_samp(4.0::numeric);

-- SQL2003 binary aggregates
SELECT regr_count(b, a) FROM aggtest;
SELECT regr_sxx(b, a) FROM aggtest;
SELECT regr_syy(b, a) FROM aggtest;
SELECT regr_sxy(b, a) FROM aggtest;
SELECT regr_avgx(b, a), regr_avgy(b, a) FROM aggtest;
SELECT regr_r2(b, a) FROM aggtest;
SELECT regr_slope(b, a), regr_intercept(b, a) FROM aggtest;
SELECT covar_pop(b, a), covar_samp(b, a) FROM aggtest;
SELECT corr(b, a) FROM aggtest;

SELECT count(four) AS cnt_1000 FROM onek;
SELECT count(DISTINCT four) AS cnt_4 FROM onek;

select ten, count(*), sum(four) from onek
group by ten order by ten;

select ten, count(four), sum(DISTINCT four) from onek
group by ten order by ten;

-- user-defined aggregates
SELECT newavg(four) AS avg_1 FROM onek;
SELECT newsum(four) AS sum_1500 FROM onek;
SELECT newcnt(four) AS cnt_1000 FROM onek;
SELECT newcnt(*) AS cnt_1000 FROM onek;
SELECT oldcnt(*) AS cnt_1000 FROM onek;
SELECT sum2(q1,q2) FROM int8_tbl;

-- check collation-sensitive matching between grouping expressions
select v||'a', case v||'a' when 'aa' then 1 else 0 end, count(*)
  from unnest(array['a','b']) u(v)
 group by v||'a' order by 1;
select v||'a', case when v||'a' = 'aa' then 1 else 0 end, count(*)
  from unnest(array['a','b']) u(v)
 group by v||'a' order by 1;
-- test for outer-level aggregates

-- this should work
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four) 
order by ten;

-- this should fail because subquery has an agg of its own in WHERE
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b
               where sum(distinct a.four + b.four) = b.four);

-- Test handling of sublinks within outer-level aggregates.
-- Per bug report from Daniel Grace.
create table tenk1_bak as select * from tenk1 order by unique1, unique2 limit 1;
select
  (select max((select i.unique2 from tenk1_bak i where i.unique1 = o.unique1)))
from tenk1_bak o;
drop table tenk1_bak;

--
-- test for bitwise integer aggregates
--

-- Enforce use of COMMIT instead of 2PC for temporary objects

-- CREATE TEMPORARY TABLE bitwise_test(
CREATE  TABLE bitwise_test(
  i2 INT2,
  i4 INT4,
  i8 INT8,
  i INTEGER,
  x INT2,
  y BIT(4)
);

-- empty case
SELECT
  BIT_AND(i2) AS "?",
  BIT_OR(i4)  AS "?"
FROM bitwise_test;

COPY bitwise_test FROM STDIN NULL 'null';
1	1	1	1	1	B0101
3	3	3	null	2	B0100
7	7	7	3	4	B1100
\.

SELECT
  BIT_AND(i2) AS "1",
  BIT_AND(i4) AS "1",
  BIT_AND(i8) AS "1",
  BIT_AND(i)  AS "?",
  BIT_AND(x)  AS "0",
  BIT_AND(y)  AS "0100",

  BIT_OR(i2)  AS "7",
  BIT_OR(i4)  AS "7",
  BIT_OR(i8)  AS "7",
  BIT_OR(i)   AS "?",
  BIT_OR(x)   AS "7",
  BIT_OR(y)   AS "1101"
FROM bitwise_test;

--
-- test boolean aggregates
--
-- first test all possible transition and final states

SELECT
  -- boolean and transitions
  -- null because strict
  booland_statefunc(NULL, NULL)  IS NULL AS "t",
  booland_statefunc(TRUE, NULL)  IS NULL AS "t",
  booland_statefunc(FALSE, NULL) IS NULL AS "t",
  booland_statefunc(NULL, TRUE)  IS NULL AS "t",
  booland_statefunc(NULL, FALSE) IS NULL AS "t",
  -- and actual computations
  booland_statefunc(TRUE, TRUE) AS "t",
  NOT booland_statefunc(TRUE, FALSE) AS "t",
  NOT booland_statefunc(FALSE, TRUE) AS "t",
  NOT booland_statefunc(FALSE, FALSE) AS "t";

SELECT
  -- boolean or transitions
  -- null because strict
  boolor_statefunc(NULL, NULL)  IS NULL AS "t",
  boolor_statefunc(TRUE, NULL)  IS NULL AS "t",
  boolor_statefunc(FALSE, NULL) IS NULL AS "t",
  boolor_statefunc(NULL, TRUE)  IS NULL AS "t",
  boolor_statefunc(NULL, FALSE) IS NULL AS "t",
  -- actual computations
  boolor_statefunc(TRUE, TRUE) AS "t",
  boolor_statefunc(TRUE, FALSE) AS "t",
  boolor_statefunc(FALSE, TRUE) AS "t",
  NOT boolor_statefunc(FALSE, FALSE) AS "t";

-- CREATE TEMPORARY TABLE bool_test(
CREATE  TABLE bool_test(
  b1 BOOL,
  b2 BOOL,
  b3 BOOL,
  b4 BOOL);

-- empty case
SELECT
  BOOL_AND(b1)   AS "n",
  BOOL_OR(b3)    AS "n"
FROM bool_test;

COPY bool_test FROM STDIN NULL 'null';
TRUE	null	FALSE	null
FALSE	TRUE	null	null
null	TRUE	FALSE	null
\.

SELECT
  BOOL_AND(b1)     AS "f",
  BOOL_AND(b2)     AS "t",
  BOOL_AND(b3)     AS "f",
  BOOL_AND(b4)     AS "n",
  BOOL_AND(NOT b2) AS "f",
  BOOL_AND(NOT b3) AS "t"
FROM bool_test;

SELECT
  EVERY(b1)     AS "f",
  EVERY(b2)     AS "t",
  EVERY(b3)     AS "f",
  EVERY(b4)     AS "n",
  EVERY(NOT b2) AS "f",
  EVERY(NOT b3) AS "t"
FROM bool_test;

SELECT
  BOOL_OR(b1)      AS "t",
  BOOL_OR(b2)      AS "t",
  BOOL_OR(b3)      AS "f",
  BOOL_OR(b4)      AS "n",
  BOOL_OR(NOT b2)  AS "f",
  BOOL_OR(NOT b3)  AS "t"
FROM bool_test;

--
-- Test cases that should be optimized into indexscans instead of
-- the generic aggregate implementation.
-- In Postgres-XC, plans printed by explain are the ones created on the
-- coordinator. Coordinator does not generate index scan plans.
--
analyze tenk1;		-- ensure we get consistent plans here

-- Basic cases
explain (costs off)
  select min(unique1) from tenk1;

select min(unique1) from tenk1;

explain (costs off)
  select max(unique1) from tenk1;

select max(unique1) from tenk1;

explain (costs off)
  select max(unique1) from tenk1 where unique1 < 42;

select max(unique1) from tenk1 where unique1 < 42;

explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42;

select max(unique1) from tenk1 where unique1 > 42;

explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42000;

select max(unique1) from tenk1 where unique1 > 42000;


-- multi-column index (uses tenk1_thous_tenthous)
explain (costs off)
  select max(tenthous) from tenk1 where thousand = 33;

select max(tenthous) from tenk1 where thousand = 33;

explain (costs off)
  select min(tenthous) from tenk1 where thousand = 33;

select min(tenthous) from tenk1 where thousand = 33;


-- check parameter propagation into an indexscan subquery
explain (costs off)
  select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
    from int4_tbl;
select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
from int4_tbl 
order by f1;

-- check some cases that were handled incorrectly in 8.3.0
explain (costs off)
  select distinct max(unique2) from tenk1;
select distinct max(unique2) from tenk1;
explain (costs off)
  select max(unique2) from tenk1 order by 1;

select max(unique2) from tenk1 order by 1;

explain (costs off)
  select max(unique2) from tenk1 order by max(unique2);

select max(unique2) from tenk1 order by max(unique2);

explain (costs off)
  select max(unique2) from tenk1 order by max(unique2)+1;

select max(unique2) from tenk1 order by max(unique2)+1;

explain (costs off)
  select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;

select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;


-- try it on an inheritance tree
create table minmaxtest(f1 int);
create table minmaxtest1() inherits (minmaxtest);
create table minmaxtest2() inherits (minmaxtest);
create table minmaxtest3() inherits (minmaxtest);
create index minmaxtesti on minmaxtest(f1);
create index minmaxtest1i on minmaxtest1(f1);
create index minmaxtest2i on minmaxtest2(f1 desc);
create index minmaxtest3i on minmaxtest3(f1) where f1 is not null;

insert into minmaxtest values(11), (12);
insert into minmaxtest1 values(13), (14);
insert into minmaxtest2 values(15), (16);
insert into minmaxtest3 values(17), (18);

explain (costs off)
  select min(f1), max(f1) from minmaxtest;
select min(f1), max(f1) from minmaxtest;

-- DISTINCT doesn't do anything useful here, but it shouldn't fail
explain (costs off)
  select distinct min(f1), max(f1) from minmaxtest;
select distinct min(f1), max(f1) from minmaxtest;

drop table minmaxtest cascade;

--
-- Test combinations of DISTINCT and/or ORDER BY
--

select array_agg(a order by b)
  from (values (1,4),(2,3),(3,1),(4,2)) v(a,b);
select array_agg(a order by a)
  from (values (1,4),(2,3),(3,1),(4,2)) v(a,b);
select array_agg(a order by a desc)
  from (values (1,4),(2,3),(3,1),(4,2)) v(a,b);
select array_agg(b order by a desc)
  from (values (1,4),(2,3),(3,1),(4,2)) v(a,b);

select array_agg(distinct a)
  from (values (1),(2),(1),(3),(null),(2)) v(a);
select array_agg(distinct a order by a)
  from (values (1),(2),(1),(3),(null),(2)) v(a);
select array_agg(distinct a order by a desc)
  from (values (1),(2),(1),(3),(null),(2)) v(a);
select array_agg(distinct a order by a desc nulls last)
  from (values (1),(2),(1),(3),(null),(2)) v(a);

-- string_agg tests
select string_agg(a,',') from (values('aaaa'),('bbbb'),('cccc')) g(a);
explain (verbose, costs off) select string_agg(a,',') from (values('aaaa'),('bbbb'),('cccc')) g(a);
select string_agg(a,',') from (values('aaaa'),(null),('bbbb'),('cccc')) g(a);
select string_agg(a,'AB') from (values(null),(null),('bbbb'),('cccc')) g(a);
select string_agg(a,',') from (values(null),(null)) g(a);

-- check some implicit casting cases, as per bug #5564
select string_agg(distinct f1, ',') from varchar_tbl;  -- ok
explain (verbose, costs off) select string_agg(distinct f1, ',') from varchar_tbl;  -- ok
select string_agg(distinct f1, ',' order by f1) from varchar_tbl;  -- ok
explain (verbose, costs off) select string_agg(distinct f1, ',' order by f1) from varchar_tbl;  -- ok
select string_agg(distinct f1::text, ',' order by f1) from varchar_tbl;  -- not ok
select string_agg(distinct f1, ',' order by f1::text) from varchar_tbl;  -- not ok
select string_agg(distinct f1::text, ',') from varchar_tbl;  -- ok
explain (verbose, costs off) select string_agg(distinct f1::text, ',') from varchar_tbl;  -- ok
select string_agg(distinct f1::text, ',' order by f1::text) from varchar_tbl;  -- ok
explain (verbose, costs off) select string_agg(distinct f1::text, ',' order by f1::text) from varchar_tbl;  -- ok

-- string_agg bytea tests
create table bytea_test_table(v bytea);

select string_agg(v, '' order by v) from bytea_test_table;

insert into bytea_test_table values(decode('ff','hex'));

select string_agg(v, '' order by v) from bytea_test_table;

insert into bytea_test_table values(decode('aa','hex'));

select string_agg(v, '' order by v) from bytea_test_table;
explain (verbose, costs off) select string_agg(v, '' order by v) from bytea_test_table;
select string_agg(v, NULL order by v) from bytea_test_table;
select string_agg(v, decode('ee', 'hex') order by v) from bytea_test_table;

drop table bytea_test_table;
DROP TABLE bitwise_test CASCADE;
DROP TABLE bool_test CASCADE;

create table string_agg_dn(cino int, addr text, post_cde text, valid_flag char, id int);
insert into string_agg_dn values(1,'haidian','0102568','1', 2);
insert into string_agg_dn values(2,'shangdi','0106568','2', 3);
insert into string_agg_dn values(3,'changping','0105888','5', 5);
insert into string_agg_dn values(4,'nanjing','0565888','6', 6);
insert into string_agg_dn values(5,'haidian','0211167','1', 8);

explain (verbose off, costs off)
select
	 cino, 
	 substr(string_agg(addr , ';') , 
	 0, 
	 length(string_agg(addr , ';') ) - 1) addr, 
	 substr(string_agg(a.post_cde , ';'), 
	 0, 
	 length(string_agg(post_cde , ';')) - 1)  post_cde 
from (select cino, 
			
			replace(addr, CHR(13) || CHR(10),'') addr, 
			post_cde 
			from 
			( 
			select cino,addr,post_cde, 
			ROW_NUMBER() OVER(PARTITION BY cino order by cino) rn 
			 from (select cino, addr, post_cde 
			   from string_agg_dn where valid_flag='1' 
			   group by cino, addr, post_cde) 
			  ) 
	 where rn < 7) a 
group by cino; 

select
	 cino, 
	 substr(string_agg(addr , ';') , 
	 0, 
	 length(string_agg(addr , ';') ) - 1) addr, 
	 substr(string_agg(a.post_cde , ';'), 
	 0, 
	 length(string_agg(post_cde , ';')) - 1)  post_cde 
from (select cino, 
			
			replace(addr, CHR(13) || CHR(10),'') addr, 
			post_cde 
			from 
			( 
			select cino,addr,post_cde, 
			ROW_NUMBER() OVER(PARTITION BY cino order by cino) rn 
			 from (select cino, addr, post_cde 
			   from string_agg_dn where valid_flag='1' 
			   group by cino, addr, post_cde) 
			  ) 
	 where rn < 7) a 
group by cino order by cino;

select string_agg(addr, ';') from string_agg_dn;
explain (verbose, costs off) select string_agg(addr, ';') from string_agg_dn;
select string_agg(addr, ';' order by cino) from string_agg_dn;
explain (verbose, costs off) select string_agg(addr, ';' order by cino) from string_agg_dn;
select string_agg(addr, ';') from string_agg_dn where valid_flag='5' group by cino;
explain (verbose, costs off) select string_agg(addr, ';') from string_agg_dn where valid_flag='5' group by cino;
select string_agg(cino, ';') from string_agg_dn where valid_flag='5' group by cino;
explain (verbose, costs off) select string_agg(cino, ';') from string_agg_dn where valid_flag='5' group by id;
select cino from string_agg_dn group by cino order by cino;
explain (verbose, costs off) select cino from string_agg_dn group by cino order by cino;
select max(id) from string_agg_dn having string_agg(cino, ';' order by cino) = '1;2;3;4;5';
explain (verbose, costs off) select max(id) from string_agg_dn having string_agg(cino, ';' order by cino) = '1;2;3;4;5';
select string_agg(addr, ';'order by addr) from string_agg_dn  group by valid_flag order by 1;
explain (verbose, costs off) select string_agg(addr, ';'order by addr) from string_agg_dn  group by valid_flag;
select string_agg(distinct addr, ';'order by addr) from string_agg_dn  group by valid_flag order by 1;
explain (verbose, costs off) select string_agg(distinct addr, ';'order by addr) from string_agg_dn  group by valid_flag;
select string_agg(t1.cino,','order by t1.cino) from string_agg_dn t1,string_agg_dn t2 where t1.id=t2.id;
explain (verbose, costs off) select string_agg(t1.cino,','order by t1.cino) from string_agg_dn t1,string_agg_dn t2 where t1.id=t2.id;
drop table string_agg_dn;
create table string_agg_dn_col(c1 int, c2 text) with (orientation = column);
insert into string_agg_dn_col values(1, 'test');
select c1, string_agg(c2,',') from string_agg_dn_col group by c1;
explain (verbose, costs off) select c1, string_agg(c2,',') from string_agg_dn_col group by c1;
drop table string_agg_dn_col;
create table string_agg_dn_dk_null(c1 int, c2 text, c3 regproc);
insert into string_agg_dn_dk_null values(1, 'test', 'sin');
select c3, string_agg(c2, ',') from string_agg_dn_dk_null group by c3;
explain (verbose, costs off) select c3, string_agg(c2, ',') from string_agg_dn_dk_null group by c3;
drop table string_agg_dn_dk_null;

-- test non-collection agg functions
create table t_collection(a1 int, b1 int, c1 int, d1 int);
insert into t_collection select generate_series(1, 100)%8, generate_series(1, 100)%7, generate_series(1, 100)%6, generate_series(1, 100)%5;
analyze t_collection;

-- normal
explain (costs off, verbose on)
select array_length(array_agg(d1), 1) from t_collection;
select array_length(array_agg(d1), 1) from t_collection;
explain (costs off, verbose on)
select array_length(array_agg(d1), 1) from t_collection group by b1 order by 1;
select array_length(array_agg(d1), 1) from t_collection group by b1 order by 1;
explain (costs off, verbose on)
select array_length(array_agg(distinct d1), 1) from t_collection;
select array_length(array_agg(distinct d1), 1) from t_collection;
explain (costs off, verbose on)
select array_length(array_agg(distinct d1), 1) from t_collection group by b1 order by 1;
select array_length(array_agg(distinct d1), 1) from t_collection group by b1 order by 1;
explain (costs off, verbose on)
select array_to_string(array_agg(d1 order by d1), ':') from t_collection;
select array_to_string(array_agg(d1 order by d1), ':') from t_collection;
explain (costs off, verbose on)
select array_to_string(array_agg(d1 order by d1), ':') from t_collection group by b1 order by 1;
select array_to_string(array_agg(d1 order by d1), ':') from t_collection group by b1 order by 1;
explain (costs off, verbose on)
select array_to_string(array_agg(distinct d1 order by d1), ':') from t_collection;
select array_to_string(array_agg(distinct d1 order by d1), ':') from t_collection;
explain (costs off, verbose on)
select array_to_string(array_agg(distinct d1 order by d1), ':') from t_collection group by b1 order by 1;
select array_to_string(array_agg(distinct d1 order by d1), ':') from t_collection group by b1 order by 1;

-- count(distinct)
explain (costs off, verbose on)
select array_length(array_agg(d1), 1), count(distinct(c1)) from t_collection;
select array_length(array_agg(d1), 1), count(distinct(c1)) from t_collection;
explain (costs off, verbose on)
select array_length(array_agg(d1), 1), count(distinct(c1)) from t_collection group by b1 order by 1;
select array_length(array_agg(d1), 1), count(distinct(c1)) from t_collection group by b1 order by 1;
explain (costs off, verbose on)
select array_length(array_agg(distinct d1), 1), count(distinct(c1)) from t_collection;
select array_length(array_agg(distinct d1), 1), count(distinct(c1)) from t_collection;
explain (costs off, verbose on)
select array_length(array_agg(distinct d1), 1), count(distinct(c1)) from t_collection group by b1 order by 1;
select array_length(array_agg(distinct d1), 1), count(distinct(c1)) from t_collection group by b1 order by 1;
explain (costs off, verbose on)
select array_to_string(array_agg(d1 order by d1), ':'), count(distinct(c1)) from t_collection;
select array_to_string(array_agg(d1 order by d1), ':'), count(distinct(c1)) from t_collection;
explain (costs off, verbose on)
select array_to_string(array_agg(d1 order by d1), ':'), count(distinct(c1)) from t_collection group by b1 order by 1;
select array_to_string(array_agg(d1 order by d1), ':'), count(distinct(c1)) from t_collection group by b1 order by 1;
explain (costs off, verbose on)
select array_to_string(array_agg(distinct d1 order by d1), ':'), count(distinct(c1)) from t_collection;
select array_to_string(array_agg(distinct d1 order by d1), ':'), count(distinct(c1)) from t_collection;
explain (costs off, verbose on)
select array_to_string(array_agg(distinct d1 order by d1), ':'), count(distinct(c1)) from t_collection group by b1 order by 1;
select array_to_string(array_agg(distinct d1 order by d1), ':'), count(distinct(c1)) from t_collection group by b1 order by 1;

-- multi non-collection agg
explain (costs off, verbose on)
select array_length(array_agg(d1), 1), array_length(array_agg(distinct c1), 1) from t_collection;
select array_length(array_agg(d1), 1), array_length(array_agg(distinct c1), 1) from t_collection;
explain (costs off, verbose on)
select array_length(array_agg(d1), 1), array_length(array_agg(distinct c1), 1) from t_collection group by b1 order by 1;
select array_length(array_agg(d1), 1), array_length(array_agg(distinct c1), 1) from t_collection group by b1 order by 1;
explain (costs off, verbose on)
select array_length(array_agg(distinct d1), 1), array_length(array_agg(distinct c1), 1) from t_collection;
select array_length(array_agg(distinct d1), 1), array_length(array_agg(distinct c1), 1) from t_collection;
explain (costs off, verbose on)
select array_length(array_agg(distinct d1), 1), array_length(array_agg(distinct c1), 1) from t_collection group by b1 order by 1;
select array_length(array_agg(distinct d1), 1), array_length(array_agg(distinct c1), 1) from t_collection group by b1 order by 1;
explain (costs off, verbose on)
select array_to_string(array_agg(d1 order by d1), ':'), array_length(array_agg(distinct c1), 1) from t_collection;
select array_to_string(array_agg(d1 order by d1), ':'), array_length(array_agg(distinct c1), 1) from t_collection;
explain (costs off, verbose on)
select array_to_string(array_agg(d1 order by d1), ':'), array_length(array_agg(distinct c1), 1) from t_collection group by b1 order by 1;
select array_to_string(array_agg(d1 order by d1), ':'), array_length(array_agg(distinct c1), 1) from t_collection group by b1 order by 1;
explain (costs off, verbose on)
select array_to_string(array_agg(distinct d1 order by d1), ':'), array_length(array_agg(distinct c1), 1) from t_collection;
select array_to_string(array_agg(distinct d1 order by d1), ':'), array_length(array_agg(distinct c1), 1) from t_collection;
explain (costs off, verbose on)
select array_to_string(array_agg(distinct d1 order by d1), ':'), array_length(array_agg(distinct c1), 1) from t_collection group by b1 order by 1;
select array_to_string(array_agg(distinct d1 order by d1), ':'), array_length(array_agg(distinct c1), 1) from t_collection group by b1 order by 1;
select array_to_string(ARRAY[NULL, NULL, NULL, NULL, NULL], ',', NULL) is null;
SELECT array_to_string(ARRAY[NULL, NULL, NULL, NULL, NULL], ',') is null;

-- grouping sets
explain (costs off, verbose on)
select array_length(array_agg(distinct d1), 1) from t_collection group by rollup(a1) order by a1;  -- can't push down
select array_length(array_agg(distinct d1), 1) from t_collection group by rollup(a1) order by a1;  -- can't push down
explain (costs off, verbose on)
select array_length(array_agg(distinct d1), 1) from t_collection group by rollup(a1), c1 order by a1, c1; -- can push down
select array_length(array_agg(distinct d1), 1) from t_collection group by rollup(a1), c1 order by a1, c1; -- can push down
explain (costs off, verbose on)
select array_agg(distinct d1 order by d1) from t_collection group by rollup(a1), c1 order by a1, c1; -- can push down
select array_agg(distinct d1 order by d1) from t_collection group by rollup(a1), c1 order by a1, c1; -- can push down
explain (costs off, verbose on)
select c1, d1, length(string_agg(distinct b1, 'x')) from t_collection group by grouping sets(c1, d1) order by c1, d1;
select c1, d1, length(string_agg(distinct b1, 'x')) from t_collection group by grouping sets(c1, d1) order by c1, d1;
explain (costs off, verbose on)
select c1, d1, length(string_agg(distinct b1, 'x')) from t_collection group by grouping sets((c1, d1)) order by c1, d1;
select c1, d1, length(string_agg(distinct b1, 'x')) from t_collection group by grouping sets((c1, d1)) order by c1, d1;
explain (costs off, verbose on)
select c1, d1, length(string_agg(distinct b1, 'x')) from t_collection group by cube(c1, d1) order by c1, d1;
select c1, d1, length(string_agg(distinct b1, 'x')) from t_collection group by cube(c1, d1) order by c1, d1;
explain (costs off, verbose on)
select c1, d1, length(string_agg(distinct b1, 'x')) from t_collection group by cube(c1), d1 order by c1, d1;
select c1, d1, length(string_agg(distinct b1, 'x')) from t_collection group by cube(c1), d1 order by c1, d1;
drop table t_collection;

explain (verbose, costs off)
select array(select sum(x+y) s
            from generate_series(1,3) y group by y order by s)
  from generate_series(1,3) x;

select array(select sum(x+y) s
            from generate_series(1,3) y group by y order by s)
  from generate_series(1,3) x;

create table t_fqs_abs (a int, b int, c int, d int);
insert into t_fqs_abs values (1,1,1,1);
insert into t_fqs_abs values (2,2,2,2);
insert into t_fqs_abs values (3,3,3,3);
explain (costs off, verbose on)
SELECT abs(a)
    , abs(a) + sum(b)
FROM t_fqs_abs
GROUP BY 1;
SELECT abs(a)
    , abs(a) + sum(b)
FROM t_fqs_abs
GROUP BY 1
order by 1,2;
drop table t_fqs_abs;

-- subplan in agg qual
create table agg_qual(a int, b varchar(10));
set enable_sort=off;
explain (costs off) select b in (select 'g' from agg_qual group by 1) from agg_qual group by 1 having b in (select 'g' from agg_qual group by 1);
explain (costs off) select 1,b in (select 'g' from agg_qual group by 1) from agg_qual group by 1, 2 having b in (select 'g' from agg_qual group by 1);
explain (costs off) select 1,b in (select 'g' from agg_qual group by 1),count(distinct(a)) from agg_qual group by 1, 2 having b in (select 'g' from agg_qual group by 1);
explain (costs off) select 1,b in (select 'g' from agg_qual group by 1),count(distinct(b)) from agg_qual group by 1, 2 having b in (select 'g' from agg_qual group by 1);
explain (costs off) select 1,a+5,count(distinct(b)) from agg_qual group by 1, 2 having max(b) in (select 'g' from agg_qual group by 1);
explain (costs off)
select 1
from (
 select b c1, case when b in (
   select 'g' from agg_qual 
   group by 1) then 'e' else a::text end c2, max(5)
 from agg_qual 
 group by 1,2) temp1
where temp1.c2=temp1.c1;
set enable_hashagg=off;
set enable_sort=on;
explain (costs off)
select 1
from (
 select 1 c1, case when b in (
   select 'g' from agg_qual 
   group by 1) then 'e' end c2, count(distinct(a)), max(5)
 from agg_qual 
 group by 1,2) temp1
where temp1.c2=temp1.c1;
explain (costs off)
select 1
from (
 select 1 c1, case when b in (
   select 'g' from agg_qual 
   group by 1) then 'e' end c2, max(5)
 from agg_qual 
 group by 1,2) temp1
where temp1.c2=temp1.c1;
explain (costs off)
select 1
from (
 select b c1, case when b in (
   select 'g' from agg_qual 
   group by 1) then 'e' end c2, max(5)
 from agg_qual 
 group by 1,2) temp1
where temp1.c2=temp1.c1;
explain (costs off)
select 1
from (
 select b c1, case when b in (
   select 'g' from agg_qual 
   group by 1) then 'e' else a::text end c2, max(5)
 from agg_qual 
 group by 1,2) temp1
where temp1.c2=temp1.c1;
drop table agg_qual;

-- test group by on primary key
CREATE TABLE t (pk int, b int, c int, d int);
insert into t select v,v%5,v%3,v%7 from generate_series(1,15) as v;
ALTER TABLE t ADD PRIMARY KEY (pk);

set enable_sort=off;

-- original
explain (verbose on, costs off)
SELECT pk, b, (c * sum(d))
FROM t
GROUP BY pk
order by 1,2;

SELECT pk, b, (c * count(d))
FROM t
GROUP BY pk
order by 1,2;

explain (verbose on, costs off)
SELECT pk, b, (c * count(distinct d))
FROM t
GROUP BY pk
order by 1,2;

SELECT pk, b, (c * count(distinct d))
FROM t
GROUP BY pk
order by 1,2;

explain (verbose on, costs off)
SELECT pk, b, (c * count(distinct d)), count(distinct c)
FROM t
GROUP BY pk
order by 1,2;

SELECT pk, b, (c * count(distinct d)), count(distinct c)
FROM t
GROUP BY pk
order by 1,2;

explain (verbose on, costs off)
SELECT t1.pk, t1.b, (t2.b * count(distinct t1.d)),
count(distinct t1.c), t1.d+sum(t2.c)+t1.c
FROM t t1 join t t2 on t1.b=t2.c
GROUP BY t1.pk, t2.b
order by 1,2,3,4,5;

SELECT t1.pk, t1.b, (t2.b * count(distinct t1.d)),
count(distinct t1.c), t1.d+sum(t2.c)+t1.c
FROM t t1 join t t2 on t1.b=t2.c
GROUP BY t1.pk, t2.b
order by 1,2,3,4,5;

drop table t;

-- test listagg (compatible with A db)
CREATE SCHEMA listagg_test;
SET current_schema = listagg_test;

CREATE TABLE emp
(
   empno INTEGER CONSTRAINT pk_emp PRIMARY KEY,
   ename VARCHAR(20),
   job CHAR(10),
   address TEXT,
   email TEXT,
   mgrno INT4,
   workhour INT2,
   hiredate DATE,
   termdate DATE,
   offtime TIMESTAMP,
   overtime TIMESTAMPTZ,
   vacationTime INTERVAL,
   salPerHour FLOAT4,
   bonus NUMERIC(8,2),
   deptno NUMERIC(2)
);

INSERT INTO emp VALUES (7369,'SMITH','CLERK','宝山区示范新村37号403室','smithWu@163.com',7902,8,to_date('17-12-1999', 'dd-mm-yyyy'),NULL, '2018/12/1','2019-2-20 pst', INTERVAL '5' DAY, 60.35, 2000.80,20);
INSERT INTO emp VALUES (7499,'ALLEN','SALESMAN','虹口区西康南路125弄34号201室','66allen_mm@qq.com',7698,5,to_date('20-2-2015', 'dd-mm-yyyy'),'2018-1-1','2013-12-24 12:30:00','2017-12-12 UTC', '4 DAY 6 HOUR', 9.89,899.00,30);
INSERT INTO emp VALUES (7521,'WARD','SALESMAN','城东区亨达花园7栋702','hello702@163.com',7698,10,to_date('22-2-2010','dd-mm-yyyy'),'2016/12/30', '2016-06-12 8:12:00','2012-7-10 pst',INTERVAL '30 DAY', 52.98, 1000.01,30);
INSERT INTO emp VALUES (7566,'JONES','MANAGER','莲花五村龙昌里34号601室','jonesishere@gmal.com',7839,3,to_date('2-4-2001','dd-mm-yyyy'),'2013-1-30','2010-10-13 24:00:00','2009-10-12 CST',NULL,200.00,999.10,20);
INSERT INTO emp VALUES (7654,'MARTIN','SALESMAN','开平路53号国棉四厂二宿舍1号楼2单元','mm213n@qq.com',7698,12,to_date('28-9-1997','dd-mm-yyyy'),NULL,'2018/9/25 23:00:00','1999-1-18 CST', '24 HOUR', 1.28,99.99,30);
INSERT INTO emp VALUES (7698,'BLAKE','MANAGER','建国门大街23号楼302室','blake-life@fox.mail',7839,1,to_date('1-5-1981','dd-mm-yyyy'),'2012-10-13','2009-4-29 05:35:00','2010-12-1 pst','1 YEAR 1 MONTH', 38.25,2399.50,30);
INSERT INTO emp VALUES (7782,'CLARK','MANAGER','花都大道100号世纪光花小区3023号','flower21@gmail.com',7839,5,to_date('9-6-1981','dd-mm-yyyy'),NULL,'1999-8-18 24:00:00','1999-5-12 pst','10 DAY',100.30,10000.01,10);
INSERT INTO emp VALUES (7788,'SCOTT','ANALYST','温泉路四海花园1号楼','bigbigbang@sina.com',7566,9,to_date('13-7-1987','dd-mm-yyyy')-85,'2000-10-1','1998-1-19 00:29:00','2000-2-29 UTC','1 WEEK 2 DAY',99.25,1001.01,20);
INSERT INTO emp VALUES (7839,'KING','PRESIDENT','温江区OneHouse高级别墅1栋','houseme123@yahoo.com',NULL,2,to_date('17-11-1981','dd-mm-yyyy'),NULL,NULL,NULL,'1 YEAR 30 DAY',19999.99,23011.88,10);
INSERT INTO emp VALUES (7844,'TURNER','SALESMAN','城北梁家巷132号','tur789@qq.com',7698,15,to_date('8-9-1981','dd-mm-yyyy'),'2011-07-15','1998-1-18 23:12:00','1999-1-16 pst','2 MONTH 10 DAY',99.12,9,30);
INSERT INTO emp VALUES (7876,'ADAMS','CLERK','如北街江心美寓小区1号','aking_clerk@sina.com',7788,8,to_date('13-7-1987', 'dd-mm-yyyy')-51,'2018-10-23','1999-1-18 23:12:00','2017-12-30 pst','36 DAY',2600.12,1100.0,20);
INSERT INTO emp VALUES (7900,'JAMES','CLERK','锦尚路MOCO公寓10楼','whoMe@gmail.com',7698,10,to_date('3-12-1981','dd-mm-yyyy'),'2006/12/2','2005-9-10 5:00:00','2004-11-8 pst','12 DAY 12 HOUR',95,1000.22,30);
INSERT INTO emp VALUES (7902,'FORD','ANALYST','方西区正街3号巷66号','analyse666@163.com',7566,8,to_date('3-12-1981','dd-mm-yyyy'),'2012-12-23','2012-05-12 23:00:00','2011-03-21 CST','10 WEEK',199.23,2002.12,20);
INSERT INTO emp VALUES (7934,'MILLER','CLERK','四方区洛阳路34号3号楼4单元402户','Miller*mail@sina.com',7782,'10',to_date('23-1-1982','dd-mm-yyyy'),'2016-12-30','2015-10-12 24:00:00','2015-09-12 pst','40 DAY',112.23,10234.21,10);

analyze emp;

-- test for different input type: varchar, char, text, int2, int4, int8,float4, float8, numeric, date, timestamp, interval, etc. 
SELECT deptno, listagg(ename, ',') WITHIN GROUP(ORDER BY ename) AS employees_order_by_ename_varchar FROM emp GROUP BY deptno ORDER BY 1;
SELECT mgrno, listagg(empno, ';') WITHIN GROUP(ORDER BY empno) AS empno_order_by_empno_group_by_mgr_integer FROM emp GROUP BY mgrno ORDER BY 1;
SELECT deptno, listagg(job, '-') WITHIN GROUP(ORDER BY job) AS job_order_by_job_char FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(address, '//') WITHIN GROUP(ORDER BY address) AS address_order_by_address_text_zh FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(email, '##') WITHIN GROUP(ORDER BY email) AS email_order_by_email_text_en FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(mgrno, ',') WITHIN GROUP(ORDER BY mgrno) AS mgrno_order_by_mgrno_int4 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(workhour, '; ') WITHIN GROUP(ORDER BY workhour) AS workhour_order_by_workhour_int2 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(hiredate, ', ') WITHIN GROUP(ORDER BY hiredate) AS hiredate_order_by_hiredate_date FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(offtime, '; ') WITHIN GROUP(ORDER BY offtime) AS offtime_order_by_offtime_timestamp FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(overtime, ', ') WITHIN GROUP(ORDER BY overtime) AS overtime_order_by_overtime_timestamptz FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(vacationTime, '; ') WITHIN GROUP(ORDER BY vacationTime ASC) AS vacationTime_order_by_vtime_ASC_interval FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(termdate-hiredate, '; ') WITHIN GROUP(ORDER BY termdate-hiredate DESC) AS onwork_order_by_time_desc_interval FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(salPerHour, ', ') WITHIN GROUP(ORDER BY salPerHour DESC) AS salPH_order_by_salPH_desc_float4 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(CAST(salPerHour*workhour AS FLOAT8), '; ') WITHIN GROUP(ORDER BY CAST(salPerHour*workhour AS FLOAT8) ASC) AS totalincome_order_by_tin_float8 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(CAST(workhour*7*30 AS INT8), '(Hours); ') WITHIN GROUP(ORDER BY CAST(workhour*7*30 AS INT8) DESC) AS hoursPerYear_order_by_hpy_DESC_int8 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(bonus, '($); ') WITHIN GROUP(ORDER BY bonus) AS bonus_order_by_bonus_numeric FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(termdate, '; ') WITHIN GROUP(ORDER BY termdate ASC NULLS FIRST) AS termdate_order_by_termdate_null_first_date FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(termdate, '; ') WITHIN GROUP(ORDER BY termdate ASC NULLS LAST) AS termdate_order_by_termdate_null_last_date FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(mgrno, ', ') WITHIN GROUP(ORDER BY mgrno NULLS FIRST) AS mgrno_order_by_mgrno_nulls_first_in4 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(mgrno, '. ') WITHIN GROUP(ORDER BY mgrno NULLS LAST) AS mgrno_order_by_mgrno_nulls_last_int4 FROM emp GROUP BY deptno ORDER BY 1;
SELECT mgrno, listagg(address, ' || ') WITHIN GROUP(ORDER BY NLSSORT(address, 'NLS_SORT=SCHINESE_PINYIN_M')) AS address_order_by_pinyin_text FROM emp GROUP BY mgrno ORDER BY 1;
SELECT deptno, listagg(address, ' || ') WITHIN GROUP(ORDER BY NLSSORT(address, 'NLS_SORT=SCHINESE_PINYIN_M')) AS address_order_by_pinyin_text FROM emp GROUP BY deptno ORDER BY 1;

-- test for plan changes, dfx
SET explain_perf_mode=pretty;
EXPLAIN verbose SELECT deptno, listagg(ename, ',') WITHIN GROUP(ORDER BY ename) AS employees_order_by_ename_varchar FROM emp GROUP BY deptno;
EXPLAIN verbose SELECT deptno, listagg(email, '##') WITHIN GROUP(ORDER BY email) AS email_order_by_email_text_en FROM emp GROUP BY deptno;
EXPLAIN verbose SELECT deptno, listagg(bonus, '($); ') WITHIN GROUP(ORDER BY bonus) AS bonus_order_by_bonus_numeric FROM emp GROUP BY deptno;

-- test for parameter without delimiter, special static delimiter expression, other order or group conditions
SELECT deptno, listagg(ename) WITHIN GROUP(ORDER BY ename) AS employees_noarg2_varchar FROM emp GROUP BY deptno ORDER BY 1;
SELECT mgrno, listagg(empno) WITHIN GROUP(ORDER BY empno) AS empno_order_by_empno_group_by_mgr_noarg2_integer FROM emp GROUP BY mgrno ORDER BY 1;
SELECT deptno, listagg(job) WITHIN GROUP(ORDER BY job) AS job_order_by_job_noargs_char FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(address) WITHIN GROUP(ORDER BY address) AS address_order_by_address_noarg2_text_zh FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(email) WITHIN GROUP(ORDER BY email) AS email_order_by_email_noarg2_text_en FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(mgrno) WITHIN GROUP(ORDER BY mgrno) AS mgrno_order_by_mgrno_noarg2_int4 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(workhour) WITHIN GROUP(ORDER BY workhour) AS workhour_order_by_workhour_noarg2_int2 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(hiredate) WITHIN GROUP(ORDER BY hiredate) AS hiredate_order_by_hiredate_noarg2_date FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(offtime) WITHIN GROUP(ORDER BY offtime) AS offtime_order_by_offtime_noarg2_timestamp FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(overtime) WITHIN GROUP(ORDER BY overtime) AS overtime_order_by_overtime_noarg2_timestamptz FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(vacationTime) WITHIN GROUP(ORDER BY vacationTime DESC) AS vacationTime_order_by_vtime_desc_noarg2_interval FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(CAST(salPerHour*workhour AS FLOAT8)) WITHIN GROUP(ORDER BY CAST(salPerHour*workhour AS FLOAT8) ASC) AS totalincome_order_by_tin_noarg2_float8 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(CAST(workhour*7*30 AS INT8)) WITHIN GROUP(ORDER BY CAST(workhour*7*30 AS INT8) DESC) AS hoursPerYear_order_by_hpy_DESC_noarg2_int8 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(bonus) WITHIN GROUP(ORDER BY bonus) AS bonus_order_by_bonus_noarg2_numeric FROM emp GROUP BY deptno ORDER BY 1;

SELECT deptno, listagg(',') WITHIN GROUP(ORDER BY ename) AS employees_varchar FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(email) WITHIN GROUP(ORDER BY email) AS email_order_by_email_text_en FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(vacationTime) WITHIN GROUP(ORDER BY vacationTime DESC) AS vacationTime_order_by_vtime_desc_interval FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(ename, '(' || CHR(deptno+55)|| ')') WITHIN GROUP(ORDER BY ename) AS employees_varchar FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(ename, ', ') WITHIN GROUP(ORDER BY NULL) AS employees_no_order_varchar FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(salPerHour, '; ') WITHIN GROUP(ORDER BY NULL) AS salPH_no_order_float4 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(ename, ',') WITHIN GROUP(ORDER BY hiredate) AS employees_varchar FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, listagg(address, '//') WITHIN GROUP(ORDER BY empno) AS address_order_by_empno_TEXT FROM emp GROUP BY deptno ORDER BY 1;
SELECT job, listagg(ename, '+') WITHIN GROUP(ORDER BY ename) AS ename_order_by_ename_group_by_job FROM emp GROUP BY job ORDER BY 1;
SELECT job, listagg(ename, '+') WITHIN GROUP(ORDER BY hiredate) AS ename_order_by_hiredate_group_by_job FROM emp GROUP BY job ORDER BY 1;
SELECT job, listagg(ename, '; ') WITHIN GROUP(ORDER BY bonus DESC) AS ename_order_by_bonus_group_by_job FROM emp GROUP BY job ORDER BY 1;
SELECT job, listagg(bonus, '($); ') WITHIN GROUP(ORDER BY bonus DESC) AS bonus_order_by_bonus_group_by_job FROM emp GROUP BY job ORDER BY 1;
SELECT mgrno, listagg(job, '; ') WITHIN GROUP(ORDER BY job) AS ename_order_by_job_group_by_mgrno FROM emp GROUP BY mgrno ORDER BY 1;

SET datestyle = 'SQL,DMY';
SELECT deptno, listagg(hiredate, ', ') WITHIN GROUP(ORDER BY hiredate) AS hiredate_dmy_order_by_hiredate_date FROM emp GROUP BY deptno ORDER BY 1;

SET datestyle = 'SQL,MDY';
SELECT deptno, listagg(hiredate, ', ') WITHIN GROUP(ORDER BY hiredate) AS hiredate_dmy_order_by_hiredate_date FROM emp GROUP BY deptno ORDER BY 1;

SET datestyle = 'Postgres,DMY';
SELECT deptno, listagg(hiredate, ', ') WITHIN GROUP(ORDER BY hiredate) AS hiredate_dmy_order_by_hiredate_date FROM emp GROUP BY deptno ORDER BY 1;

SELECT deptno, listagg(job, ',') WITHIN GROUP(ORDER BY job), listagg(ename,'; ') WITHIN GROUP(order by hiredate) AS employees FROM emp group by deptno ORDER BY 1;

-- test for abnormal cases: no WITHIN keyword, invalid parameter, other orders, etc. errors.
SELECT deptno, listagg(ename, ',') AS employees_without_within_varchar FROM emp GROUP BY deptno;
SELECT deptno, listagg() WITHIN GROUP(ORDER BY ename) AS employees_varchar FROM emp GROUP BY deptno;
SELECT deptno, listagg(ename, ',' ORDER BY ename) WITHIN GROUP(ORDER BY hiredate) AS employees_3_args_varchar FROM emp GROUP BY deptno;
SELECT deptno, listagg(DISTINCT ename, ',') WITHIN GROUP(ORDER BY ename) AS employees_distinct_varchar FROM emp GROUP BY deptno;
SELECT deptno, listagg(ename, '(' || MAX(deptno)|| ')') WITHIN GROUP(ORDER BY NULL) AS employees_varchar FROM emp GROUP BY deptno;

-- test for window function
SET datestyle = 'ISO,YMD';
SELECT deptno, email, mgrno, listagg(ename,'; ') WITHIN GROUP(ORDER BY hiredate) OVER(PARTITION BY deptno) AS employees FROM emp ORDER BY 1,2,3;
SELECT job, address, ename, listagg(bonus, '(￥); ') WITHIN GROUP(ORDER BY bonus) OVER(PARTITION BY job) AS bonus FROM emp ORDER BY 1,2,3;
SELECT mgrno, ename, job, hiredate,listagg(ename, ',') WITHIN GROUP(ORDER BY ename) OVER(PARTITION BY mgrno) AS employees_in_manager FROM emp ORDER BY 1,2,3,4;
SELECT deptno, email, mgrno, listagg(ename,'; ') WITHIN GROUP(ORDER BY ename) OVER(PARTITION BY deptno ORDER BY ename) AS employees FROM emp ORDER BY 1,2,3;
SELECT deptno, email, mgrno, listagg(ename,'; ') WITHIN GROUP(ORDER BY NULL) OVER(PARTITION BY deptno ORDER BY NULL) AS employees FROM emp ORDER BY 1,2,3;
SELECT deptno, email, mgrno, listagg(ename,'; ') WITHIN GROUP(ORDER BY ename) OVER(PARTITION BY deptno ORDER BY hiredate) AS employees FROM emp ORDER BY 1,2,3;
explain verbose SELECT deptno, email, mgrno, listagg(ename,'; ') WITHIN GROUP(ORDER BY ename) OVER(PARTITION BY deptno) AS employees FROM emp ORDER BY 1,2,3;
explain verbose SELECT job, address, ename, listagg(bonus, '(￥); ') WITHIN GROUP(ORDER BY bonus) OVER(PARTITION BY job) AS bonus FROM emp ORDER BY 1,2,3;

-- test agg on anyarray
CREATE TABLE arraggtest (a int default 10, f1 INT[], f2 TEXT[][], f3 FLOAT[]);
INSERT INTO arraggtest (f1, f2, f3) VALUES ('{1,2,3,4}','{{grey,red},{blue,blue}}','{1.6, 0.0}');
INSERT INTO arraggtest (f1, f2, f3) VALUES ('{1,2,3}','{{grey,red},{grey,blue}}','{1.6}');
SELECT max(f1), min(f1), max(f2), min(f2), max(f3), min(f3) FROM arraggtest order by 1, 2, 3, 4;
explain (verbose on, costs off)SELECT max(f1), min(f1), max(f2), min(f2), max(f3), min(f3) FROM arraggtest order by 1, 2, 3, 4;

drop TABLE arraggtest;
DROP SCHEMA listagg_test CASCADE;
