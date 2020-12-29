/*--------------------------------------------------------------------------------------y
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
set query_dop=1002;
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
reset expected_computing_nodegroup;
drop node group ng1;
drop node group ng2;
drop node group ng3;

drop schema nodegroup_multigroup_test cascade;
reset query_dop;
--end
