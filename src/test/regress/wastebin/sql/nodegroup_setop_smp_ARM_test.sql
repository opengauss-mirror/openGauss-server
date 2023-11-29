set query_dop=1002;
create schema nodegroup_setop_test;
set current_schema = nodegroup_setop_test;

set enable_nodegroup_explain=true;
set expected_computing_nodegroup='group1';

create node group ng0 with (datanode1, datanode2, datanode3);
create node group ng1 with (datanode4, datanode5, datanode6);
create node group ng2 with (datanode7, datanode8, datanode9);
create node group ng3 with (datanode10, datanode11, datanode12);
create node group ng4 with (datanode1, datanode2, datanode3, datanode4, datanode5, datanode6);
create node group ng5 with (datanode4, datanode5, datanode6, datanode7, datanode8, datanode9);
create node group ng6 with (datanode7, datanode8, datanode9, datanode10, datanode11, datanode12);

create table setop_hash_table_01( a int, b int ,c text) distribute by hash(a) to group ng0;
create table setop_hash_table_02( a int, b numeric ,c text) distribute by hash(a) to group ng1;
create table setop_hash_table_03( a int, b bigint ,c text) distribute by hash(b) to group ng2;
create table setop_hash_table_04( a smallint, b bigint ,c text) distribute by hash(a) to group ng3;
create table setop_replication_table_01( a int, b bigint ,c text) distribute by replication to group ng4;
create table setop_replication_table_02( a bigint, b int ,c text) distribute by replication to group ng5;
create table setop_replication_table_03( a smallint, b bigint ,c text) distribute by replication to group ng6;

create view setop_view_table_12 as select setop_hash_table_01.a as ta1, setop_hash_table_01.b as tb1, setop_hash_table_02.a as ta2, setop_hash_table_02.b as tb2 from setop_hash_table_01 inner join setop_hash_table_02 on (setop_hash_table_01.a = setop_hash_table_02.a);
create view setop_view_table_23 as select setop_hash_table_02.a as ta1, setop_hash_table_02.b as tb1, setop_hash_table_03.a as ta2, setop_hash_table_03.b as tb2 from setop_hash_table_02 inner join setop_hash_table_03 on (setop_hash_table_02.a = setop_hash_table_03.a);
create view setop_view_table_31 as select setop_hash_table_03.a as ta1, setop_hash_table_03.b as tb1, setop_hash_table_01.a as ta2, setop_hash_table_01.b as tb2 from setop_hash_table_03 inner join setop_hash_table_01 on (setop_hash_table_01.a = setop_hash_table_03.a);

create index index_on_hash_01 on setop_hash_table_01(a);
create index index_on_hash_02 on setop_hash_table_02(a);
create index index_on_hash_03 on setop_hash_table_03(b);
create index index_on_hash_04 on setop_hash_table_04(a);
create index index_on_replication_01 on setop_replication_table_01(a);
create index index_on_replication_02 on setop_replication_table_02(a);
create index index_on_replication_03 on setop_replication_table_03(a);

insert into setop_hash_table_01 values (generate_series(1,2), generate_series(2,3), 'setop_hash_table_01');
insert into setop_hash_table_02 values (generate_series(1,3), generate_series(2,4), 'setop_hash_table_02');
insert into setop_hash_table_03 values (generate_series(1,4), generate_series(2,5), 'setop_hash_table_03');
insert into setop_hash_table_04 values (generate_series(1,5), generate_series(2,6), 'setop_hash_table_04');
insert into setop_replication_table_01 values (generate_series(1,2), generate_series(2,3), 'setop_replication_table_01');
insert into setop_replication_table_02 values (generate_series(1,3), generate_series(2,4), 'setop_replication_table_02');
insert into setop_replication_table_03 values (generate_series(1,4), generate_series(2,5), 'setop_replication_table_03');

insert into setop_hash_table_01 values (generate_series(1,2), generate_series(2,3), 't');
insert into setop_hash_table_02 values (generate_series(1,3), generate_series(2,4), 't');
insert into setop_hash_table_03 values (generate_series(1,4), generate_series(2,5), 't');
insert into setop_hash_table_04 values (generate_series(1,5), generate_series(2,6), 't');
insert into setop_replication_table_01 values (generate_series(1,2), generate_series(2,3), 't');
insert into setop_replication_table_02 values (generate_series(1,3), generate_series(2,4), 't');
insert into setop_replication_table_03 values (generate_series(1,4), generate_series(2,5), 't');

analyze setop_hash_table_01;
analyze setop_hash_table_02;
analyze setop_hash_table_03;
analyze setop_hash_table_04;
analyze setop_replication_table_01;
analyze setop_replication_table_02;
analyze setop_replication_table_03;
analyze pg_auth_members;

--
---- UNION ALL: Append
--
-- hash + hash + same distributeKey + Append executes on all DNs
select * from setop_hash_table_01 union all select * from setop_hash_table_02 order by 1, 2, 3;
select a, b from setop_hash_table_01 union all select b, a from setop_hash_table_03 order by 1, 2;
select * from setop_hash_table_01 where a = 1 union all select * from setop_hash_table_02 where b = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 union all select b, a from setop_hash_table_03 where a = 1 order by 1, 2;

-- hash + hash + same distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 union all select * from setop_hash_table_02 where a = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 union all select b, a from setop_hash_table_03 where b = 1 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on all DNs
select * from setop_hash_table_01 union all select * from setop_hash_table_03  order by 1, 2, 3;
select a, b from setop_hash_table_01 union all select b, a from setop_hash_table_02  order by 1, 2;

-- hash + hash + different distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 union all select * from setop_hash_table_03 where b = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 union all select b, a from setop_hash_table_02 where a = 1 order by 1, 2;

-- hash + hash + type cast
select * from setop_hash_table_01 union all select * from setop_hash_table_04 order by 1, 2, 3;
select a, b from setop_hash_table_01 union all select b, a from setop_hash_table_04 order by 1, 2;

-- hash + replication  + Append executes on special DN
select * from setop_hash_table_01 union all select * from setop_replication_table_01 order by 1, 2, 3;

-- replication + replication
select * from setop_replication_table_01 union all select * from setop_replication_table_02 order by 1, 2, 3;

-- execute on cn + hash
select 1 from pg_auth_members union all select b from setop_hash_table_02 order by 1;

-- targetlist dosenot contains distributeKey
select a from setop_hash_table_01 union all select b from setop_hash_table_02 order by 1;
select b from setop_hash_table_01 union all select b from setop_hash_table_02 order by 1;

select ta1, tb1 from setop_view_table_12 union all select a, b from setop_hash_table_03 order by 1, 2;
select tb1, tb1 from setop_view_table_12 union all select a, b from setop_hash_table_03 order by 1, 2;

SELECT 1 AS one union all SELECT 1.1::float8  order by 1;

--
---- UNION ALL: Append: maybe MergeAppend
--
set enable_sort = false;

select * from setop_hash_table_01 union all select * from setop_hash_table_02 order by 1, 2, 3;
select a, b from setop_hash_table_01 union all select b, a from setop_hash_table_03 order by 1, 2;

-- hash + hash + same distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 union all select * from setop_hash_table_02 where a = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 union all select b, a from setop_hash_table_03 where b = 1 order by 1, 2;

-- hash + replication  + Append executes on special DN
select * from setop_hash_table_01 union all select * from setop_replication_table_01 order by 1, 2, 3;

-- replication + replication
select * from setop_replication_table_01 union all select * from setop_replication_table_02 order by 1, 2, 3;

-- targetlist dosenot contains distributeKey
select a from setop_hash_table_01 union all select b from setop_hash_table_02 order by 1;
select b from setop_hash_table_01 union all select b from setop_hash_table_02 order by 1;

reset enable_sort;
--
-- UNION
--

-- hash + hash + same distributeKey + Append executes on all DNs
select * from setop_hash_table_01 union select * from setop_hash_table_02 order by 1, 2, 3;
select a, b from setop_hash_table_01 union select b, a from setop_hash_table_03 order by 1, 2;
select * from setop_hash_table_01 where a = 1 union select * from setop_hash_table_02 where b = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 union select b, a from setop_hash_table_03 where a = 1 order by 1, 2;

-- hash + hash + same distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 union select * from setop_hash_table_02 where a = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 union select b, a from setop_hash_table_03 where b = 1 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on all DNs
select * from setop_hash_table_01 union select * from setop_hash_table_03 order by 1, 2, 3;
select a, b from setop_hash_table_01 union select b, a from setop_hash_table_02 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 union select * from setop_hash_table_03 where b = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 union select b, a from setop_hash_table_02 where a = 1 order by 1, 2;

-- hash + hash + type cast
select * from setop_hash_table_01 union select * from setop_hash_table_04 order by 1, 2, 3;
select a, b from setop_hash_table_01 union select b, a from setop_hash_table_04 order by 1, 2;

-- hash + replication  + Append executes on special DN
select * from setop_hash_table_01 union select * from setop_replication_table_01 order by 1, 2, 3;

-- replication + replication
select * from setop_replication_table_01 union select * from setop_replication_table_02 order by 1, 2, 3;

-- execute on cn + hash
select 1 from pg_auth_members union select b from setop_hash_table_02 order by 1;

-- targetlist dosenot contains distributeKey
select a from setop_hash_table_01 union select b from setop_hash_table_02 order by 1;
select b from setop_hash_table_01 union select b from setop_hash_table_02 order by 1;

select ta1, tb1 from setop_view_table_12 union select a, b from setop_hash_table_03 order by 1;
select tb1, tb1 from setop_view_table_12 union select a, b from setop_hash_table_03 order by 1, 2;

SELECT 1 AS one UNION SELECT 1.1::float8;

--
---- INTERSECT ALL
--

-- hash + hash + same distributeKey + Append executes on all DNs
select * from setop_hash_table_01 intersect all select * from setop_hash_table_02 order by 1, 2, 3;
select a, b from setop_hash_table_01 intersect all select b, a from setop_hash_table_03 order by 1, 2;
select * from setop_hash_table_01 where a = 1 intersect all select * from setop_hash_table_02 where b = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 intersect all select b, a from setop_hash_table_03 where a = 1 order by 1, 2;

-- hash + hash + same distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 intersect all select * from setop_hash_table_02 where a = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 intersect all select b, a from setop_hash_table_03 where b = 1 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on all DNs
select * from setop_hash_table_01 intersect all select * from setop_hash_table_03 order by 1, 2, 3;
select a, b from setop_hash_table_01 intersect all select b, a from setop_hash_table_02 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 intersect all select * from setop_hash_table_03 where b = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 intersect all select b, a from setop_hash_table_02 where a = 1 order by 1, 2;

-- hash + hash + type cast
select * from setop_hash_table_01 intersect all select * from setop_hash_table_04 order by 1, 2, 3;
select a, b from setop_hash_table_01 intersect all select b, a from setop_hash_table_04 order by 1, 2;

-- hash + replication  + Append executes on special DN
select * from setop_hash_table_01 intersect all select * from setop_replication_table_01 order by 1, 2, 3;

-- replication + replication
select * from setop_replication_table_01 intersect all select * from setop_replication_table_02 order by 1, 2, 3;

-- execute on cn + hash
select 1 from pg_auth_members intersect all select b from setop_hash_table_02 order by 1;

-- targetlist dosenot contains distributeKey
select a from setop_hash_table_01 intersect all select b from setop_hash_table_02 order by 1;
select b from setop_hash_table_01 intersect all select b from setop_hash_table_02 order by 1;

select * from setop_view_table_12 intersect all select * from setop_view_table_23 order by 1, 2, 3;

SELECT 1 AS one intersect all SELECT 1.1::float8 order by 1;

--
---- INTERSECT
--

-- hash + hash + same distributeKey + Append executes on all DNs
select * from setop_hash_table_01 intersect select * from setop_hash_table_02 order by 1, 2, 3;
select a, b from setop_hash_table_01 intersect select b, a from setop_hash_table_03 order by 1, 2;
select * from setop_hash_table_01 where a = 1 intersect select * from setop_hash_table_02 where b = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 intersect select b, a from setop_hash_table_03 where a = 1 order by 1, 2;

-- hash + hash + same distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 intersect select * from setop_hash_table_02 where a = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 intersect select b, a from setop_hash_table_03 where b = 1 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on all DNs
select * from setop_hash_table_01 intersect select * from setop_hash_table_03 order by 1, 2, 3;
select a, b from setop_hash_table_01 intersect select b, a from setop_hash_table_02 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 intersect select * from setop_hash_table_03 where b = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 intersect select b, a from setop_hash_table_02 where a = 1 order by 1, 2;

-- hash + hash + type cast
select * from setop_hash_table_01 intersect select * from setop_hash_table_04 order by 1, 2, 3;
select a, b from setop_hash_table_01 intersect select b, a from setop_hash_table_04 order by 1, 2;

-- hash + replication  + Append executes on special DN
select * from setop_hash_table_01 intersect select * from setop_replication_table_01 order by 1, 2;

-- replication + replication
select * from setop_replication_table_01 intersect select * from setop_replication_table_02 order by 1, 2;

-- execute on cn + hash
select 1 from pg_auth_members intersect select b from setop_hash_table_02 order by 1;

-- targetlist dosenot contains distributeKey
select a from setop_hash_table_01 intersect select b from setop_hash_table_02 order by 1;
select b from setop_hash_table_01 intersect select b from setop_hash_table_02 order by 1;

select * from setop_view_table_12 intersect select * from setop_view_table_23 order by 1, 2, 3;

SELECT 1 AS one intersect SELECT 1.1::float8 order by 1;

--
----EXCEPT ALL 
--

-- hash + hash + same distributeKey + Append executes on all DNs
select * from setop_hash_table_01 except all select * from setop_hash_table_02 order by 1, 2, 3;
select a, b from setop_hash_table_01 except all select b, a from setop_hash_table_03 order by 1, 2;
select * from setop_hash_table_01 where a = 1 except all select * from setop_hash_table_02 where b = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 except all select b, a from setop_hash_table_03 where a = 1 order by 1, 2;

-- hash + hash + same distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 except all select * from setop_hash_table_02 where a = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 except all select b, a from setop_hash_table_03 where b = 1 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on all DNs
select * from setop_hash_table_01 except all select * from setop_hash_table_03 order by 1, 2, 3;
select a, b from setop_hash_table_01 except all select b, a from setop_hash_table_02 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 except all select * from setop_hash_table_03 where b = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 except all select b, a from setop_hash_table_02 where a = 1 order by 1, 2;

-- hash + hash + type cast
select * from setop_hash_table_01 except all select * from setop_hash_table_04 order by 1, 2, 3;
select a, b from setop_hash_table_01 except all select b, a from setop_hash_table_04 order by 1, 2;

-- hash + replication  + Append executes on special DN
select * from setop_hash_table_01 except all select * from setop_replication_table_01 order by 1, 2, 3;

-- replication + replication
select * from setop_replication_table_01 except all select * from setop_replication_table_02 order by 1, 2, 3;

-- execute on cn + hash
select 1 from pg_auth_members except all select b from setop_hash_table_02 order by 1;

-- targetlist dosenot contains distributeKey
select a from setop_hash_table_01 except all select b from setop_hash_table_02 order by 1;
select b from setop_hash_table_01 except all select b from setop_hash_table_02 order by 1;

select * from setop_view_table_12 except all select * from setop_view_table_23 order by 1, 2, 3;

SELECT 1 AS one except all SELECT 1.1::float8 order by 1;

--
----EXCEPT 
--

-- hash + hash + same distributeKey + Append executes on all DNs
explain (verbose on, costs off) select * from setop_hash_table_01 union all select * from setop_hash_table_02;
select * from setop_hash_table_01 except select * from setop_hash_table_02 order by 1, 2, 3;
select a, b from setop_hash_table_01 except select b, a from setop_hash_table_03 order by 1, 2;
select * from setop_hash_table_01 where a = 1 except select * from setop_hash_table_02 where b = s1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 except select b, a from setop_hash_table_03 where a = 1 order by 1, 2;

-- hash + hash + same distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 except select * from setop_hash_table_02 where a = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 except select b, a from setop_hash_table_03 where b = 1 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on all DNs
select * from setop_hash_table_01 except select * from setop_hash_table_03 order by 1, 2, 3;
select a, b from setop_hash_table_01 except select b, a from setop_hash_table_02 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on special DN
select * from setop_hash_table_01 where a = 1 except select * from setop_hash_table_03 where b = 1 order by 1, 2, 3;
select a, b from setop_hash_table_01 where a = 1 except select b, a from setop_hash_table_02 where a = 1 order by 1, 2;

-- hash + hash + type cast
select * from setop_hash_table_01 except select * from setop_hash_table_04 order by 1, 2, 3;
select a, b from setop_hash_table_01 except select b, a from setop_hash_table_04 order by 1, 2;

-- hash + replication  + Append executes on special DN
select * from setop_hash_table_01 except select * from setop_replication_table_01 order by 1, 2, 3;

-- replication + replication
select * from setop_replication_table_01 except select * from setop_replication_table_02 order by 1, 2, 3;

-- execute on cn + hash
select 1 from pg_auth_members except select b from setop_hash_table_02 order by 1;

-- targetlist dosenot contains distributeKey
select a from setop_hash_table_01 except select b from setop_hash_table_02 order by 1;
select b from setop_hash_table_01 except select b from setop_hash_table_02 order by 1;

select * from setop_view_table_12 except select * from setop_view_table_23 order by 1, 2, 3;

SELECT 1 AS one except SELECT 1.1::float8 order by 1;

--setop optimizer
explain (verbose on, costs off) select 'setop_hash_table_01',b from setop_hash_table_01 union select 'setop_hash_table_03',a from setop_hash_table_03;
select 'setop_hash_table_01',b from setop_hash_table_01 union select 'setop_hash_table_03',a from setop_hash_table_03 order by 1,2;

explain (verbose on, costs off) SELECT b FROM setop_hash_table_01 INTERSECT (((SELECT a FROM setop_hash_table_02 UNION SELECT b FROM setop_hash_table_03))) ORDER BY 1;
SELECT b FROM setop_hash_table_01 INTERSECT (((SELECT a FROM setop_hash_table_02 UNION SELECT b FROM setop_hash_table_03))) ORDER BY 1;

--union all between replication and hash
explain (costs off) select * from setop_replication_table_01 except select * from setop_replication_table_02 
union all 
select setop_hash_table_01.a as ta1, setop_hash_table_01.b as tb1, setop_hash_table_01.c as tc1 from setop_hash_table_01 left join setop_hash_table_02 on (setop_hash_table_01.b = setop_hash_table_02.b);

select * from setop_replication_table_01 except select * from setop_replication_table_02 
union all 
select setop_hash_table_01.a as ta1, setop_hash_table_01.b as tb1, setop_hash_table_01.c as tc1 from setop_hash_table_01 left join setop_hash_table_02 on (setop_hash_table_01.b = setop_hash_table_02.b) 
order by 1, 2, 3;

explain (costs off) select distinct b*2/3+5 from setop_hash_table_01 union all select a from setop_hash_table_02 order by 1; 
select distinct b*2/3+5 from setop_replication_table_01 union all select a from setop_replication_table_02 order by 1; 


drop table setop_hash_table_01 cascade;
drop table setop_hash_table_02 cascade;
drop table setop_hash_table_03 cascade;
drop table setop_hash_table_04 cascade;
drop table setop_replication_table_01 cascade;
drop table setop_replication_table_02 cascade;
drop table setop_replication_table_03 cascade;

--Test union
create table test_union_1(a int, b int, c int) distribute by hash(a, b) to group ng0;
create table test_union_2(a int, b int, c int) distribute by hash(a, b) to group ng5;
explain (verbose on, costs off) select a, b from  test_union_1 union all select a, b from  test_union_2;
explain (verbose on, costs off) select a, b from  test_union_1 union all select b, c from  test_union_2;
explain (verbose on, costs off) select a, b from  test_union_1 union all select b, a from  test_union_2;
explain (verbose on, costs off) select b, a from  test_union_1 union all select b, a from  test_union_2;
explain (verbose on, costs off) select b, c from  test_union_1 union all select b, c from  test_union_2;

explain (verbose on, costs off) select a, b from  test_union_1 intersect select a, b from  test_union_2;
explain (verbose on, costs off) select a, b from  test_union_1 intersect select b, c from  test_union_2;
explain (verbose on, costs off) select a, b from  test_union_1 intersect select b, a from  test_union_2;
explain (verbose on, costs off) select b, a from  test_union_1 intersect select b, a from  test_union_2;
explain (verbose on, costs off) select b, c from  test_union_1 intersect select b, c from  test_union_2;

explain (verbose on, costs off) select a, b from  test_union_1 minus select a, b from  test_union_2;
explain (verbose on, costs off) select a, b from  test_union_1 minus select b, c from  test_union_2;
explain (verbose on, costs off) select a, b from  test_union_1 minus select b, a from  test_union_2;
explain (verbose on, costs off) select b, a from  test_union_1 minus select b, a from  test_union_2;
explain (verbose on, costs off) select b, c from  test_union_1 minus select b, c from  test_union_2;
explain (verbose on, costs off) select b, substr(c, 1, 3), c from  test_union_1 minus (select 1, t2.b::varchar(10), t1.c from (select a,b,case c when 1 then 1 else null end as c from test_union_2 where b<0) t1 right join test_union_2 t2 on t1.b=t2.c group by 1, 2, 3);

explain (verbose on, costs off) SELECT b,a,c FROM test_union_1 INTERSECT (((SELECT a,b,c FROM test_union_1 UNION ALL SELECT a,b,c FROM test_union_1)));

create table income_band
(
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer
)
distribute by replication to group ng0;
create table store
(
    s_store_sk                integer               not null,
    s_store_id                char(16)              not null,
    s_manager                 varchar(40)                   ,
	s_market_id               integer                       ,
    s_company_id              integer
)
distribute by replication to group ng1;
create table call_center
(
    cc_call_center_sk         integer               not null,
    cc_call_center_id         char(16)              not null,
    cc_city                   varchar(60)
)
distribute by replication to group ng2;
create table item
(
    i_item_sk                 integer               not null,
    i_item_id                 char(16)              not null,
    i_class_id                integer
)
distribute by hash (i_item_sk) to group ng3;
select s_market_id
          from store
         inner join item
            on i_class_id = s_company_id
         where s_manager like '%a%'
         group by 1
        union
        select 1
          from call_center
         where cc_city like '%b%'
        union all 
        select count(*) from income_band;

reset current_schema;
drop schema nodegroup_setop_test cascade;

drop node group ng0;
drop node group ng1;
drop node group ng2;
drop node group ng3;
drop node group ng4;
drop node group ng5;
drop node group ng6;
reset query_dop;
