/*---------------------------------------------------------------------------------------
 *
 * Test node group AGG
 *
 * Portions Copyright (c) 2016, Huawei
 *
 *
 * IDENTIFICATION
 *    src/test/regress/sql/nodegroup_basic_test.sql
 *---------------------------------------------------------------------------------------
 */
create schema nodegroup_agg_test;
set current_schema = nodegroup_agg_test;

set enable_nodegroup_explain=on;
set expected_computing_nodegroup='group1';
create node group ngroup1234 with (datanode1,datanode2,datanode3,datanode4);
create node group ngroup123456 with (datanode1,datanode2,datanode3,datanode4,datanode5,datanode6);
create node group ngroup5678 with (datanode5,datanode6,datanode7,datanode8);
create table t1ng1(c1 int, c2 int, c3 int, c4 int) distribute by hash(c1);
create table t1ng1234(c1 int, c2 int, c3 int, c4 int) distribute by hash(c1) to group ngroup1234;
insert into t1ng1 select v, v*2, v*3, v*4 from generate_series(1,30) as v;

-- populate base data
insert into t1ng1234 select * from t1ng1;

-- double size 3 times
insert into t1ng1234 select * from t1ng1234;
insert into t1ng1234 select * from t1ng1234;
insert into t1ng1234 select * from t1ng1234;

/* start test cases */
-- group by dist key, it can do locally, so no ReDis step to do
explain (costs off) select avg(c3), c1 from t1ng1234 group by 2;
select avg(c3), c1 from t1ng1234 group by 2 order by 2;

-- group by none dist key, it can not do locally, so add ReDis step on default_computing nodegroup to do aggregation to do
explain (costs off) select avg(c3), c2 from t1ng1234 group by 2;
select avg(c3), c2 from t1ng1234 group by 2 order by 2;

-- same with above but change default computing nodegroup to ngroup123456
set expected_computing_nodegroup='ngroup123456';
explain (costs off) select avg(c3), c2 from t1ng1234 group by 2;
select avg(c3), c2 from t1ng1234 group by 2 order by 2;

-- same with above but change default computing nodegroup to ngoup5678
set expected_computing_nodegroup='ngroup5678';
explain (costs off) select avg(c3), c2 from t1ng1234 group by 2;
select avg(c3), c2 from t1ng1234 group by 2 order by 2;

-- test join with agg
-- join in ngroup1
set expected_computing_nodegroup = 'group1';
explain (costs off) select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3;
select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3 order by 1,2;

explain (costs off) select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 left join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3;
select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 left join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3 order by 1,2;

explain (costs off) select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 right join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3;
select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 right join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3 order by 1,2;

-- join in t1ng1234's local nodegroup
set expected_computing_nodegroup = 'ngroup1234';
explain (costs off) select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3;
select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3 order by 1,2;

explain (costs off) select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 left join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3;
select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 left join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3 order by 1,2;

explain (costs off) select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 right join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3;
select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 right join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3 order by 1,2;

-- join in ngroup123456
set expected_computing_nodegroup = 'ngroup123456';
explain (costs off) select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3;
select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3 order by 1,2;

explain (costs off) select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 left join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3;
select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 left join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3 order by 1,2;

explain (costs off) select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 right join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3;
select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 right join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3 order by 1,2;

-- join in ngroup5678
set expected_computing_nodegroup = 'ngroup5678';
explain (costs off) select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3;
select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3 order by 1,2;

explain (costs off) select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 left join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3;
select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 left join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3 order by 1,2;

explain (costs off) select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 right join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3;
select avg(t1.c2), avg(t2.c2), t2.c2 from t1ng1 as t1 right join t1ng1234 as t2 on t1.c1 = t2.c1 group by 3 order by 1,2;

drop table t1ng1;
drop table t1ng1234;
reset expected_computing_nodegroup;
drop node group ngroup1234;
drop node group ngroup123456;
drop node group ngroup5678;

/* Verify AGG on desired nodegroup */
create node group ng1 with(datanode2, datanode3);
create node group ng2 with(datanode1, datanode4);
create node group ng3 with(datanode1, datanode2, datanode3, datanode4, datanode5, datanode6, datanode7, datanode8);

create table t1ng1(c1 int, c2 int, c3 int, c4 int, c5 int) distribute by hash(c1) to group ng1;

insert into t1ng1 select v,v%5,v%10,v%20,v%25 from generate_series(1,500) as v;

select count(*) from t1ng1;

set expected_computing_nodegroup = 'group1';
-- processing on group1 but t1ng1 is distributed on ng1, so redistribute any way
explain (costs off) select sum(c1), avg(c1), count(c1), c2,c3 from t1ng1 group by 4,5 order by 1,2,3;
select sum(c1), avg(c1), count(c1), c2,c3 from t1ng1 group by 4,5 order by 1,2,3 limit 5;

-- processing on group1 but t1ng1 is distributed on ng1, so redistribute any way
explain (costs off) select sum(c1), avg(c1), count(c1), c2 from t1ng1 group by 4 order by 1,2,3;
select sum(c1), avg(c1), count(c1), c2 from t1ng1 group by 4 order by 1,2,3 limit 5;

-- processing on group1 but t1ng1 is distributed on ng1, so redistribute any way
explain (costs off) select sum(c1), avg(c1), count(c1), c1 from t1ng1 group by 4 order by 1,2,3;
select sum(c1), avg(c1), count(c1), c1 from t1ng1 group by 4 order by 1,2,3 limit 5;

set expected_computing_nodegroup = 'ng1';
-- processing on ng1 and t1ng1 is distributed on ng1, but grouping key is c2,c3 so have to redistribution
explain (costs off) select sum(c1), avg(c1), count(c1), c2,c3 from t1ng1 group by 4,5 order by 1,2,3;
select sum(c1), avg(c1), count(c1), c2,c3 from t1ng1 group by 4,5 order by 1,2,3 limit 5;

-- processing on ng1 and t1ng1 is distributed on ng1, but grouping key is c2 so have to redistribution
explain (costs off) select sum(c1), avg(c1), count(c1), c2 from t1ng1 group by 4 order by 1,2,3;
select sum(c1), avg(c1), count(c1), c2 from t1ng1 group by 4 order by 1,2,3 limit 5;

-- processing on ng1 and t1ng1 is distributed on ng1, and grouping key c1 so we can avoid redistribution :-)
explain (costs off) select sum(c1), avg(c1), count(c1), c1 from t1ng1 group by 4 order by 1,2,3;
select sum(c1), avg(c1), count(c1), c1 from t1ng1 group by 4 order by 1,2,3 limit 5;

set expected_computing_nodegroup = 'ng2';
-- processing on ng2 but t1ng1 is distributed on ng1, so redistribute any way
explain (costs off) select sum(c1), avg(c1), count(c1), c2,c3 from t1ng1 group by 4,5 order by 1,2,3;
select sum(c1), avg(c1), count(c1), c2,c3 from t1ng1 group by 4,5 order by 1,2,3 limit 5;

-- processing on ng2 but t1ng1 is distributed on ng1, so redistribute any way
explain (costs off) select sum(c1), avg(c1), count(c1), c2 from t1ng1 group by 4 order by 1,2,3;
select sum(c1), avg(c1), count(c1), c2 from t1ng1 group by 4 order by 1,2,3 limit 5;

-- processing on ng2 but t1ng1 is distributed on ng1, so redistribute any way
explain (costs off) select sum(c1), avg(c1), count(c1), c1 from t1ng1 group by 4 order by 1,2,3;
select sum(c1), avg(c1), count(c1), c1 from t1ng1 group by 4 order by 1,2,3 limit 5;

set expected_computing_nodegroup = 'ng3';
-- processing on ng3 but t1ng1 is distributed on ng1, so redistribute any way
explain (costs off) select sum(c1), avg(c1), count(c1), c2,c3 from t1ng1 group by 4,5 order by 1,2,3;
select sum(c1), avg(c1), count(c1), c2,c3 from t1ng1 group by 4,5 order by 1,2,3 limit 5;

-- processing on ng3 but t1ng1 is distributed on ng1, so redistribute any way
explain (costs off) select sum(c1), avg(c1), count(c1), c2 from t1ng1 group by 4 order by 1,2,3;
select sum(c1), avg(c1), count(c1), c2 from t1ng1 group by 4 order by 1,2,3 limit 5;

-- processing on ng3 but t1ng1 is distributed on ng1, so redistribute any way
explain (costs off) select sum(c1), avg(c1), count(c1), c1 from t1ng1 group by 4 order by 1,2,3;
select sum(c1), avg(c1), count(c1), c1 from t1ng1 group by 4 order by 1,2,3 limit 5;

drop table t1ng1;
reset expected_computing_nodegroup;
drop node group ng1;
drop node group ng2;
drop node group ng3;

drop schema nodegroup_agg_test cascade;
