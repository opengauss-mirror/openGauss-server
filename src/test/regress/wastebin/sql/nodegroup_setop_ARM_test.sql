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

create table sales_transaction_line
(
    SALES_TRAN_ID number(27,8) NOT NULL ,
    SALES_TRAN_LINE_NUM SMALLINT NOT NULL ,
    ITEM_ID number(18,9),
    ITEM_QTY NUMBER(5) NOT NULL ,
    UNIT_SELLING_PRICE_AMT DECIMAL(18,4) NOT NULL ,
    UNIT_COST_AMT DECIMAL(8,4) NULL ,
    TRAN_LINE_STATUS_CD CHAR(1) NULL ,
    SALES_TRAN_LINE_START_DTTM TIMESTAMP(6) NULL ,
    TRAN_LINE_SALES_TYPE_CD CHAR(2) NULL ,
    SALES_TRAN_LINE_END_DTTM TIMESTAMP(6) NULL ,
    TRAN_LINE_DATE DATE NULL ,
    TX_TIME INTEGER NOT NULL ,
    LOCATION INTEGER NULL ,
    LINE_RUN_DURATION_SECONDS  interval  ,
    LINE_RUN_DURATION_MINUTES  interval ,
    LINE_RUN_DURATION_HOURS    interval  ,
    LINE_RUN_DURATION_DAYS     interval ,
    unique(SALES_TRAN_ID,location) 
)  distribute by hash(sales_tran_id);

create table party
(
    party_id varchar(10) not null ,
    party_type_cd char(4) not null ,
    party_firstname varchar(20) null ,
    party_lastname varchar(20) null,
    party_street_address varchar(50) null,
    party_city char(30) null ,
    party_state char(2) null,
    party_zip char(5) null,
    party_info_source_type_cd char(4) null ,
    party_start_dt date null,
    party_first_purchase_dt date null,
    LOCATION_POINT VARCHAR(100) NULL ,
    ACTIVE_AREA VARCHAR(100) NULL ,
    ACTIVE_LINES VARCHAR(100) NULL ,
    KEY_LINE VARCHAR(100) NULL ,
    KEY_POINTS VARCHAR(100) NULL ,
    ALL_RELATED_GEO VARCHAR(100) NULL ,
primary key(party_id,party_type_cd)
)  distribute by hash(party_id);

create table district
(
    district_cd varchar(50) not null ,
    district_name varchar(100) null,
    region_cd varchar(50) not null ,
    district_mgr_associate_id number(19,18) null,
primary key(district_cd)
)
distribute by hash(district_cd);

create index i_district_1 on district (district_cd,district_mgr_associate_id) where district_mgr_associate_id > 2 and district_cd <= 'O';

INSERT INTO SALES_TRANSACTION_LINE VALUES ( 3, 4,  0.12, 0.30, 0.40, 0.40, 'A' , NULL                           , 'A' , NULL                           , DATE '1970-01-01', 9, NULL);
INSERT INTO SALES_TRANSACTION_LINE VALUES ( 12345, 1,  1.3, 1.0, 1.0, 1.0, NULL, TIMESTAMP '1973-01-01 00:00:00', NULL, TIMESTAMP '1973-01-01 00:00:00', NULL             , 1,  1);
INSERT INTO SALES_TRANSACTION_LINE VALUES ( 1.2345, 2,  2.33, 2.0, 2.0, 2.0, 'C' , TIMESTAMP '1976-01-01 00:00:00', 'C' , TIMESTAMP '1976-01-01 00:00:00', DATE '1976-01-01', 2,  2);
INSERT INTO SALES_TRANSACTION_LINE VALUES ( 12.345, 1,  3.33, 3.0, 3.0, 3.0, 'D' , TIMESTAMP '1979-01-01 00:00:00', 'D' , TIMESTAMP '1979-01-01 00:00:00', DATE '1979-01-01', 3,  3);
INSERT INTO SALES_TRANSACTION_LINE VALUES ( 123.45, 4,  4.98, 4.0, 4.0, 4.0, NULL, TIMESTAMP '1982-01-01 00:00:00', 'E' , TIMESTAMP '1982-01-01 00:00:00', DATE '1982-01-01', 4, 14);
INSERT INTO SALES_TRANSACTION_LINE VALUES ( 1234.5, 5,  5.01, 5.0, 5.0, 5.0, 'F' , NULL                           , NULL, NULL                           , DATE '1985-01-01', 5,  5);
INSERT INTO SALES_TRANSACTION_LINE VALUES ( -12345, 1,  5.01, 6.0, 6.0, 6.0, 'G' , TIMESTAMP '1988-01-01 00:00:00', 'G' , TIMESTAMP '1988-01-01 00:00:00', DATE '1988-01-01', 6, 16);
INSERT INTO SALES_TRANSACTION_LINE VALUES ( 1.2346, 7,  6, 7.0, 7.0, 7.0, 'H' , TIMESTAMP '1991-01-01 00:00:00', 'H' , TIMESTAMP '1991-01-01 00:00:00', DATE '1991-01-01', 7,  6);
INSERT INTO SALES_TRANSACTION_LINE VALUES ( 1.2347, 8,  6, 8.0, 8.0, 8.0, 'I' , TIMESTAMP '1994-01-01 00:00:00', 'I' , TIMESTAMP '1994-01-01 00:00:00', DATE '1994-01-01', 8,  5);
INSERT INTO SALES_TRANSACTION_LINE VALUES ( 1.2348, 9,  6, 9.0, 9.0, 9.0, 'J' , TIMESTAMP '1997-01-01 00:00:00', 'J' , TIMESTAMP '1997-01-01 00:00:00', DATE '1997-01-01', 9,  7);

INSERT INTO PARTY VALUES ('A', 'A', NULL, 'A' , NULL, 'A' , 'A' , 'A' , NULL, DATE '1970-01-01', NULL,'POINT(10 20)' ,'MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))','LINESTRING(1 1, 2 2, 3 3, 4 4)','LINESTRING(1 1, 2 2, 3 3, 4 4)','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))');
INSERT INTO PARTY VALUES ('B', 'B', 'B' , NULL, 'B' , NULL, 'B' , 'B' , 'B' , DATE '1973-01-01', DATE '1973-01-01','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))','LINESTRING(1 1, 2 2, 3 3, 4 4)','LINESTRING(1 1, 2 2, 3 3, 4 4)','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))');
INSERT INTO PARTY VALUES ('C', 'B', 'C' , 'C' , '  C ' , 'C' , NULL, 'C' , NULL, DATE '1976-01-01', DATE '1976-01-01','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))','LINESTRING(1 1, 2 2, 3 3, 4 4)','LINESTRING(1 1, 2 2, 3 3, 4 4)','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))');
INSERT INTO PARTY VALUES ('D', '  D', 'D' , NULL, 'D ' , 'D' , 'D' , '  D' , 'D' , NULL             , DATE '1979-01-01','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))','LINESTRING(1 1, 2 2, 3 3, 4 4)','LINESTRING(1 1, 2 2, 3 3, 4 4)','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))');
INSERT INTO PARTY VALUES ('E', 'E', NULL, 'E' , NULL, 'E' , ' ' , NULL, 'E' , DATE '1982-01-01', DATE '1982-01-01','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))','LINESTRING(1 1, 2 2, 3 3, 4 4)','LINESTRING(1 1, 2 2, 3 3, 4 4)','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))');
INSERT INTO PARTY VALUES ('F', 'F', 'F' , 'F' , 'F' , 'F' , NULL, 'F ' , ' ' , DATE '1985-01-01', DATE '1985-01-01','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))','LINESTRING(1 1, 2 2, 3 3, 4 4)','LINESTRING(1 1, 2 2, 3 3, 4 4)','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))');
INSERT INTO PARTY VALUES ('G', ' ', 'TTT GO TO BED' , 'G' , 'G' , 'G' , 'G' , 'F' , 'GOTO' , NULL             , NULL,'POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))','LINESTRING(1 1, 2 2, 3 3, 4 4)','LINESTRING(1 1, 2 2, 3 3, 4 4)','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))');
INSERT INTO PARTY VALUES ('H', 'H ', 'H' , 'H' , 'H' , 'TTT TO TO BED' , 'H' , 'G' , NULL, DATE '1991-01-01', DATE '1991-01-01','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))','LINESTRING(1 1, 2 2, 3 3, 4 4)','LINESTRING(1 1, 2 2, 3 3, 4 4)','POINT(10 20)','MULTIPOINT((1 1), (1 3), (6 3), (10 5), (20 1))');

INSERT INTO DISTRICT VALUES ('JAP', ' JAPAN', 'A', -9.223372036854775807);
INSERT INTO DISTRICT VALUES ('KOR', 'KOREA' , 'B', 3.2767);
INSERT INTO DISTRICT VALUES ('CHINA', 'SAINT LUCIA' , 'C', -3.2768);
INSERT INTO DISTRICT VALUES ('IND', 'CUBA' , 'G', NULL);
INSERT INTO DISTRICT VALUES ('AMER', ' ' , 'E', 2.147483647);
INSERT INTO DISTRICT VALUES ('MEX', NULL, 'F', NULL);
INSERT INTO DISTRICT VALUES ('NICAR', 'NICARAGUA' , 'F', 6.64738692398);
INSERT INTO DISTRICT VALUES ('COLOMA', 'COLOMABIA' , 'G', 7.2893908908);

analyze sales_transaction_line;
analyze party;
analyze district;

set enable_seqscan=off;
explain (costs off) SELECT 1
FROM sales_transaction_line ,
 (SELECT 1
 FROM district INNER JOIN party 
 ON (CASE WHEN party_state LIKE '_t_' THEN 'v' END) 
 NOT IN (SELECT tran_line_status_cd 
 FROM sales_transaction_line 
 WHERE TRAN_LINE_STATUS_CD IN (
 SELECT 'e' 
 FROM district 
 WHERE 1=0
 ORDER BY 1)
 ORDER BY 1)) dt
WHERE UNIT_COST_AMT NOT LIKE 'j%';

reset current_schema;
drop schema nodegroup_setop_test cascade;

drop node group ng0;
drop node group ng1;
drop node group ng2;
drop node group ng3;
drop node group ng4;
drop node group ng5;
drop node group ng6;
