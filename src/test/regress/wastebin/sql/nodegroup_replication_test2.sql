/*---------------------------------------------------------------------------------------
 *
 *  Nodegroup replicated table test case
 *
 * Portions Copyright (c) 2017, Huawei
 *
 *
 * IDENTIFICATION
 *    src/test/regress/sql/nodegroup_replication_test.sql
 *---------------------------------------------------------------------------------------
 */
create schema nodegroup_replication_test;
set current_schema = nodegroup_replication_test;

set enable_nodegroup_explain=true;
set expected_computing_nodegroup='group1';

create node group ng0 with (datanode1, datanode2, datanode3);
create node group ng1 with (datanode4, datanode5, datanode6);

create table t_row (c1 int, c2 int) distribute by hash(c1) to group ng1;

create table t1 (c1 int, c2 int) with (orientation = column, compression=middle) distribute by hash(c1) to group ng0;

create table t1_rep (c1 int, c2 int) with (orientation = column, compression=middle) distribute by replication to group ng0;

create table t2 (c1 int, c2 int) with (orientation = column, compression=middle) distribute by hash(c1) to group ng1;

create table t2_rep (c1 int, c2 int) with (orientation = column, compression=middle) distribute by replication to group ng1;

-- no distribute keys available
create table t2_rep_float (c1 float, c2 float) with (orientation = column, compression=middle) distribute by replication to group ng1;

insert into t_row select v,v from generate_series(1,10) as v;
insert into t1 select * from t_row;
insert into t1_rep select * from t1;
insert into t2 select * from t1;
insert into t2_rep select * from t2;
insert into t2_rep_float select * from t2;

analyze t_row;
analyze t1;
analyze t1_rep;
analyze t2;
analyze t2_rep;
analyze t2_rep_float;

set enable_mergejoin=off;
set enable_nestloop=off;
set enable_hashjoin=on;

-- replicate join replicate
set expected_computing_nodegroup = 'ng0';
explain (costs off) select * from t1_rep t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t2_rep t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

set expected_computing_nodegroup = 'ng1';
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

reset expected_computing_nodegroup;
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

-- replicate join hash
set expected_computing_nodegroup = 'ng0';
explain (costs off) select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;

reset expected_computing_nodegroup;
explain (costs off) select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;

set enable_nestloop=off;
set enable_hashjoin=off;
set enable_mergejoin=on;

-- replicate join replicate
set expected_computing_nodegroup = 'ng0';
explain (costs off) select * from t1_rep t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t2_rep t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

set expected_computing_nodegroup = 'ng1';
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

reset expected_computing_nodegroup;
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

-- replicate join hash
set expected_computing_nodegroup = 'ng0';
explain (costs off) select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;

reset expected_computing_nodegroup;
explain (costs off) select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;

set enable_hashjoin=off;
set enable_mergejoin=off;
set enable_nestloop=on;

-- replicate join replicate
set expected_computing_nodegroup = 'ng0';
explain (costs off) select * from t1_rep t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t2_rep t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

set expected_computing_nodegroup = 'ng1';
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

reset expected_computing_nodegroup;
explain (costs off) select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep_float t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1_rep t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1_rep) order by 1,2 limit 5;

-- replicate join hash
set expected_computing_nodegroup = 'ng0';
explain (costs off) select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;

reset expected_computing_nodegroup;
explain (costs off) select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t1 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1;
select * from t1_rep t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1;
select * from t2_rep t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1;
select * from t2_rep_float t1 join t2 t2 on t1.c1=t2.c1 order by 1,2,3,4 limit 5;
explain (costs off) select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;
select * from t2_rep t1 where t1.c1 in (select c2 from t1) order by 1,2 limit 5;

drop table t_row;
drop table t1;
drop table t2;
drop table t1_rep;
drop table t2_rep;
drop table t2_rep_float;
reset expected_computing_nodegroup;
drop node group ng0;
drop node group ng1;

drop schema nodegroup_replication_test cascade;
