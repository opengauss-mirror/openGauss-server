/*--------------------------------------------------------------------------------------
 *
 *  Multiple nodegroup join test case
 *      t1(c1,c2) hash by (c1) on node group ngroup1 <datanode1, datanode3, datanode5, datanode7>
 *      t2(c1,c2) hash by (c1) on node group ngroup2 <datanode1, datanode3, datanode5, datanode7, datanode9>
 *      group1 is default nodegroup, it has datanode1..datanode12
*
 * Portions Copyright (c) 2016, Huawei
 *
 *
 * IDENTIFICATION
 *    src/test/regress/sql/nodegroup_basic_test.sql
 *---------------------------------------------------------------------------------------
 */
create schema nodegroup_multigroup_test;
set current_schema = nodegroup_multigroup_test;

create node group ngroup1 with (datanode1, datanode3, datanode5, datanode7);
create node group ngroup2 with (datanode2, datanode4, datanode6, datanode8, datanode10, datanode12);
create node group ngroup3 with (datanode1, datanode2, datanode3, datanode4, datanode5, datanode6);

-- insert select
create table t1ng1(c1 int, c2 int) distribute by hash(c1) to group ngroup1;
create table t1ng2(c1 int, c2 int) distribute by hash(c1) to group ngroup2;
create table t1ng3(c1 int, c2 int) distribute by hash(c1) to group ngroup3;

insert into t1ng1 select v,v from generate_series(1,30) as v;
insert into t1ng2 select * from t1ng1;
insert into t1ng3 select * from t1ng2;

set enable_nodegroup_explain = on;

-- test Hash Join
set enable_hashjoin=true;
set enable_mergejoin=false;
set enable_nestloop=false;

set expected_computing_nodegroup='group1';
-- T1's distribution key join T2's distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c1 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c1 order by 1;

-- T1's distribution key join T2's none-distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c2 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c2 order by 1;

-- T1's none-distribution key join T2's distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c1 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c1 order by 1;

-- T1's none-distribution key join T2's none-distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c2 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c2 order by 1;

set expected_computing_nodegroup='ngroup1';
-- T1's distribution key join T2's distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c1 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c1 order by 1;

-- T1's distribution key join T2's none-distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c2 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c2 order by 1;

-- T1's none-distribution key join T2's distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c1 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c1 order by 1;

-- T1's none-distribution key join T2's none-distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c2 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c2 order by 1;

-- T2's for update in multi-group
explain (costs off) select * from t1ng2 where c2 in (select c2 from t1ng1) order by 1 for update;
select * from t1ng2 where c2 in (select c2 from t1ng1) order by 1 for update;

set expected_computing_nodegroup='ngroup2';
-- T1's distribution key join T2's distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c1 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c1 order by 1;

-- T1's distribution key join T2's none-distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c2 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c2 order by 1;

-- T1's none-distribution key join T2's distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c1 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c1 order by 1;

-- T1's none-distribution key join T2's none-distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c2 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c2 order by 1;

--T1's for update in multi-group
explain (costs off) select * from t1ng1 where c2 in (select c2 from t1ng2) order by 1 for update;
select * from t1ng1 where c2 in (select c2 from t1ng2) order by 1 for update;

-- test MergeJoin
set enable_hashjoin=false;
set enable_mergejoin=true;
set enable_nestloop=false;

set expected_computing_nodegroup='group1';
-- T1's distribution key join T2's distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c1 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c1 order by 1;

-- T1's distribution key join T2's none-distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c2 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c2 order by 1;

-- T1's none-distribution key join T2's distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c1 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c1 order by 1;

-- T1's none-distribution key join T2's none-distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c2 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c2 order by 1;

set expected_computing_nodegroup='ngroup1';
-- T1's distribution key join T2's distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c1 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c1 order by 1;

-- T1's distribution key join T2's none-distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c2 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c2 order by 1;

-- T1's none-distribution key join T2's distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c1 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c1 order by 1;

-- T1's none-distribution key join T2's none-distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c2 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c2 order by 1;

set expected_computing_nodegroup='ngroup2';
-- T1's distribution key join T2's distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c1 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c1 order by 1;

-- T1's distribution key join T2's none-distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c2 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c1 = t2.c2 order by 1;

-- T1's none-distribution key join T2's distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c1 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c1 order by 1;

-- T1's none-distribution key join T2's none-distribution key
explain (costs off) select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c2 order by 1;
select * from t1ng1 as t1 join t1ng2 as t2 on t1.c2 = t2.c2 order by 1;

drop table t1ng1;
drop table t1ng2;
drop table t1ng3;
reset expected_computing_nodegroup;
drop node group ngroup1;
drop node group ngroup2;
drop node group ngroup3;


/*
 * Test BROADCAST can work with REDISTRIBUTION
 * proposed by Yonghua
 */
create node group ng1 with(datanode2, datanode3);
create node group ng2 with(datanode1, datanode4);
create node group ng3 with(datanode1, datanode2, datanode3, datanode4, datanode5, datanode6, datanode7, datanode8);

create table t1ng1(c1 int, c2 int) distribute by hash(c1) to group ng1;
create table t1ng2(c1 int, c2 int) distribute by hash(c1) to group ng2;

insert into t1ng1 select v,v%5 from generate_series(1,10) as v;
insert into t1ng2 select v,v%5 from generate_series(1,10) as v;

-- TODO, fix me as we have some issues support ANALYZE nodegorup table, so just hack statistics
update pg_class set reltuples = 500000 where relname = 't1ng2';
update pg_class set relpages = 500000 where relname = 't1ng2';

/* Verify HashJoin */
set enable_hashjoin=on;
set enable_mergejoin=off;
set enable_nestloop=off;
set expected_computing_nodegroup = 'group1';
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2 order by 1,2,3,4 limit 5;
-- also verify distribution-key
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c1 = t2.c1;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c1 order by 1,2,3,4 limit 5;

set expected_computing_nodegroup = 'ng1';
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2 order by 1,2,3,4 limit 5;
-- also verify distribution-key
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c1 = t2.c1;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c1 order by 1,2,3,4 limit 5;

set expected_computing_nodegroup = 'ng2';
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2 order by 1,2,3,4 limit 5;
-- also verify distribution-key
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c1 = t2.c1;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c1 order by 1,2,3,4 limit 5;

set expected_computing_nodegroup = 'ng3';
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2 order by 1,2,3,4 limit 5;
-- also verify distribution-key
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c1 = t2.c1;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c1 order by 1,2,3,4 limit 5;

/* Verify MergeJoin */
set enable_hashjoin=off;
set enable_mergejoin=on;
set enable_nestloop=off;
set expected_computing_nodegroup = 'group1';
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2 order by 1,2,3,4 limit 5;
-- also verify distribution-key
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c1 = t2.c1;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c1 order by 1,2,3,4 limit 5;

set expected_computing_nodegroup = 'ng1';
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2 order by 1,2,3,4 limit 5;
-- also verify distribution-key
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c1 = t2.c1;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c1 order by 1,2,3,4 limit 5;

set expected_computing_nodegroup = 'ng2';
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2 order by 1,2,3,4 limit 5;
-- also verify distribution-key
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c1 = t2.c1;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c1 order by 1,2,3,4 limit 5;

set expected_computing_nodegroup = 'ng3';
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2 order by 1,2,3,4 limit 5;
-- also verify distribution-key
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c1 = t2.c1;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c1 order by 1,2,3,4 limit 5;

/* Verify Nestloop */
set enable_hashjoin=off;
set enable_mergejoin=off;
set enable_nestloop=on;
set expected_computing_nodegroup = 'group1';
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2 order by 1,2,3,4 limit 5;
-- also verify distribution-key
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c1 = t2.c1;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c1 order by 1,2,3,4 limit 5;

set expected_computing_nodegroup = 'ng1';
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2 order by 1,2,3,4 limit 5;
-- also verify distribution-key
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c1 = t2.c1;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c1 order by 1,2,3,4 limit 5;

set expected_computing_nodegroup = 'ng2';
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2 order by 1,2,3,4 limit 5;
-- also verify distribution-key
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c1 = t2.c1;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c1 order by 1,2,3,4 limit 5;

set expected_computing_nodegroup = 'ng3';
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c2 order by 1,2,3,4 limit 5;
-- also verify distribution-key
explain (costs off) select * from t1ng1 as t1, t1ng2 as t2 where t1.c1 = t2.c1;
select * from t1ng1 as t1, t1ng2 as t2 where t1.c2 = t2.c1 order by 1,2,3,4 limit 5;
set expected_computing_nodegroup=optimal;
select c1, count(c2) from t1ng1 group by 1 order by 1,2;
drop table t1ng1;
drop table t1ng2;

/* Verify FORCE mode */
create table create_columnar_table_012 ( c_smallint smallint null,c_double_precision double precision,c_time_without_time_zone time without time zone null,c_time_with_time_zone time with time zone,c_integer integer default 23423,c_bigint bigint default 923423432,c_decimal decimal(19) default 923423423,c_real real,c_numeric numeric(18,12) null,c_varchar varchar(19),c_char char(57) null,c_timestamp_with_timezone timestamp with time zone,c_char2 char default '0',c_text text null,c_varchar2 varchar2(20),c_timestamp_without_timezone timestamp without time zone,c_date date,c_varchar22 varchar2(11621),c_numeric2 numeric null ) distribute by hash(c_date) to group ng1;
create index idx_smallint on create_columnar_table_012(c_smallint);
set enable_seqscan=off;
--AP function
explain (costs off) select c_smallint, c_integer from create_columnar_table_012 group by rollup(c_smallint,c_integer);
explain (costs off) select c_smallint, c_integer from create_columnar_table_012 group by cube(c_integer,c_smallint);
explain (costs off) select c_bigint, c_integer from create_columnar_table_012 group by GROUPING SETS(c_bigint,c_integer);
explain (costs off) select sum(c_bigint) from create_columnar_table_012 group by GROUPING SETS(());
--Group by
explain (costs off) select c_text, avg(c_integer) from create_columnar_table_012 group by c_text;
--Order by
explain (costs off) select c_bigint, c_integer from create_columnar_table_012 order by c_text;
--Window agg
explain (costs off) select c_smallint, c_integer, rank() OVER(PARTITION BY c_text ORDER BY c_date) from create_columnar_table_012;
explain (costs off) select c_smallint, c_integer, rank() OVER(ORDER BY c_date) from create_columnar_table_012;
explain (costs off) select c_smallint, c_integer, rank() OVER(PARTITION BY c_text ORDER BY c_date), row_number() OVER(PARTITION BY c_bigint ORDER BY c_text) from create_columnar_table_012;
explain (costs off) select rank() OVER(PARTITION BY c_date ORDER BY c_text) from create_columnar_table_012;
explain (costs off) select c_smallint, rank() OVER(PARTITION BY c_text ORDER BY c_date) from create_columnar_table_012 order by c_integer;
explain (costs off) select rank() OVER(PARTITION BY c_integer ORDER BY c_text) from create_columnar_table_012 group by c_text, c_integer;
--Limit/Offset
explain (costs off) select * from create_columnar_table_012 order by c_text limit 10;
explain (costs off) select * from create_columnar_table_012 order by c_text offset 10;
--Force mode
set expected_computing_nodegroup='ng2';
set enable_nodegroup_debug=on;
--AP function:y
explain (costs off) select c_smallint, c_integer from create_columnar_table_012 group by rollup(c_smallint,c_integer);
explain (costs off) select c_smallint, c_integer from create_columnar_table_012 group by cube(c_integer,c_smallint);
explain (costs off) select c_bigint, c_integer from create_columnar_table_012 group by GROUPING SETS(c_bigint,c_integer);
explain (costs off) select sum(c_bigint) from create_columnar_table_012 group by GROUPING SETS(());
--Group by:y
explain (costs off) select c_text, avg(c_integer) from create_columnar_table_012 group by c_text;
--Order by:y
explain (costs off) select c_bigint, c_integer from create_columnar_table_012 order by c_text;
--Window agg:y
explain (costs off) select c_smallint, c_integer, rank() OVER(PARTITION BY c_text ORDER BY c_date) from create_columnar_table_012;
explain (costs off) select c_smallint, c_integer, rank() OVER(ORDER BY c_date) from create_columnar_table_012;
explain (costs off) select c_smallint, c_integer, rank() OVER(PARTITION BY c_text ORDER BY c_date), row_number() OVER(PARTITION BY c_bigint ORDER BY c_text) from create_columnar_table_012;
explain (costs off) select rank() OVER(PARTITION BY c_date ORDER BY c_text) from create_columnar_table_012;
explain (costs off) select c_smallint, rank() OVER(PARTITION BY c_text ORDER BY c_date) from create_columnar_table_012 order by c_integer;
explain (costs off) select rank() OVER(PARTITION BY c_integer ORDER BY c_text) from create_columnar_table_012 group by c_text, c_integer;
--Limit/Offset:n
explain (costs off) select * from create_columnar_table_012 order by c_text limit 10;
explain (costs off) select * from create_columnar_table_012 order by c_text offset 10;
insert into create_columnar_table_012 (c_smallint, c_integer) values (1, 1), (1, 1);
explain (costs off) select rank() OVER(PARTITION BY c_integer ORDER BY c_text) from create_columnar_table_012;
select rank() OVER(PARTITION BY c_integer ORDER BY c_text) from create_columnar_table_012;
explain (costs off) select rank() OVER(ORDER BY c_text) from create_columnar_table_012;
select rank() OVER(ORDER BY c_text) from create_columnar_table_012;
explain (costs off) select rank() OVER(PARTITION BY sum(c_integer)) from create_columnar_table_012;
select rank() OVER(PARTITION BY sum(c_integer)) from create_columnar_table_012;

drop table create_columnar_table_012 cascade;

CREATE TABLE t1(a int);
INSERT INTO t1 VALUES (generate_series(1, 10));
CREATE INDEX idx ON t1(a);
SET enable_nodegroup_debug = on;
SET expected_computing_nodegroup = 'ng2';
SET enable_seqscan = off;
EXPLAIN (VERBOSE ON, COSTS OFF)SELECT * FROM t1 ORDER BY a;

/* Clean up */
reset expected_computing_nodegroup;
drop node group ng1;
drop node group ng2;
drop node group ng3;

drop schema nodegroup_multigroup_test cascade;
