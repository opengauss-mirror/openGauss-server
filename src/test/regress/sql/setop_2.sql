create schema distribute_setop_2;
set current_schema = distribute_setop_2;

create table setop_hash_table_01( a int, b int ,c text) ;
create table setop_hash_table_02( a int, b numeric ,c text) ;
create table setop_hash_table_03( a int, b bigint ,c text) ;
create table setop_hash_table_04( a smallint, b bigint ,c text) ;
create table setop_replication_table_01( a int, b bigint ,c text) ;
create table setop_replication_table_02( a bigint, b int ,c text) ;
create table setop_replication_table_03( a smallint, b bigint ,c text) ;

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

reset current_schema;
drop schema distribute_setop_2;
