create schema vector_distribute_distinct;
set current_schema = vector_distribute_distinct;

create table row_table_01 (c1 int, c2 bigint, c3 numeric) ;
insert into  row_table_01 values (11, 21, 31);
insert into  row_table_01 values (12, 22, 32);
insert into  row_table_01 values (12, NULL, 32);
insert into  row_table_01 values (13, 23, 33);
insert into  row_table_01 values (13, 23, NULL);
insert into  row_table_01 values (14, 24, 34);
insert into  row_table_01 values (NULL, 24, 34);
insert into  row_table_01 values (15, 25, 35);
insert into  row_table_01 values (16, 26, 36);
insert into  row_table_01 values (17, 27, 36);
insert into  row_table_01 values (18, 27, 36);
insert into  row_table_01 values (12, 22, 32);
insert into  row_table_01 values (17, 27, 36);
insert into  row_table_01 values (18, 27, 38);
insert into  row_table_01 values (17, 27, 37);

create table row_table_02 (c1 int, c2 bigint, c3 numeric) ;
insert into  row_table_02 values (11, 13, 31);
insert into  row_table_02 values (12, 14, 32);
insert into  row_table_02 values (12, NULL, 32);
insert into  row_table_02 values (13, 14, 33);
insert into  row_table_02 values (NULL, 14, 33);
insert into  row_table_02 values (14, 15, 34);
insert into  row_table_02 values (15, 15, 34);

analyze row_table_01;
analyze row_table_02;

create table distribute_table_01 (c1 int, c2 int, c3 int) with (orientation = column) ;
create table distribute_table_02 (c1 int, c2 int, c3 int)  with (orientation = column) ;
insert into distribute_table_01 select * from row_table_01;
insert into distribute_table_02 select * from row_table_02;

analyze distribute_table_01;
analyze distribute_table_02;

set enable_mergejoin=off; 
set enable_nestloop=off; 

--Test settings:
--1. Distinct within top query;
--2. Without Group;
--3. Without Agg;

explain (verbose, costs off) select distinct c2, c3 from distribute_table_01 order by c2, c3 asc;
explain (verbose, costs off) select distinct c1, c2, c3 from distribute_table_01 order by c1, c2, c3 asc;

-- distribute key is hashkey
select distinct c1 from distribute_table_01 order by c1;
select distinct c1, c2, c3 from distribute_table_01 order by c1, c2, c3;
-- distribute key is not hashkey
select distinct c2 from distribute_table_01 order by c2;

select distinct c2, c3 from (select distribute_table_01.c2, distribute_table_02.c3 from distribute_table_01, distribute_table_02 where distribute_table_01.c1=distribute_table_02.c1) a order by c2, c3 asc;


--Test settings:
-- Distribute with agg
select c2, count(c3) from (select distinct c1, c2, c3 from distribute_table_01 order by c1) a group by a.c2 order by a.c2, count(c3);

-- Distribute key is not hashkey with semi join
select c1, c2, c3 from distribute_table_01 where c1 in (select distinct c2 from distribute_table_02 where c2 >= 12 order by c2) order by distribute_table_01.c1, distribute_table_01.c2, distribute_table_01.c3;

-- Distinct within sub query
select c2, c3 from (select distinct c2, c3 from distribute_table_01 order by c2, c3 asc) as a order by c2, c3 asc;
select distinct c31 from (select distinct c2, count(c3) c31 from distribute_table_01 group by c2 order by c2) as a order by c31 asc;

--Test cased summarizd from user requirements
--1. With union;
--2. With or without distributed columns;
--3. Using agg or unique node;
explain (verbose, costs off) 
(select distinct c1, c2, c3 from distribute_table_01 where c1<13) 
union
(select distinct c1, c2, c3 from distribute_table_01 where c1>=13);

--distribute by a hash key with union
((select distinct c1, c2, c3 from distribute_table_01 where c1<13) 
union
(select distinct c1, c2, c3 from distribute_table_01 where c1>=13)) order by c1, c2, c3;

--distribute by not a key with union
((select distinct c2, c3 from distribute_table_01 where c2<23) 
union
(select distinct c2, c3 from distribute_table_01 where c2>=23)) order by c2, c3;

--unique is in DN,and append is on CN
((select distinct c1, c2, c3 from distribute_table_01 where c1<13 order by c1) 
union
(select distinct c1, c2, c3 from distribute_table_01 where c1>=13)) order by c1, c2, c3;


--Test cased summarizd from user requirements
--1. Distinct appears in top query and sub query simultaneously;
--2. Using agg and unique node;
select distinct a.c1, a.c2, b.cn from 
	(select c1, c2 from distribute_table_01 where c3 > 31) a,
	(select c1, count(*) cn from (select distinct c1, c2, c3 from distribute_table_01 where c3>32) group by c1 having count(*)>1) b
where a.c1=b.c1
order by cn desc, a.c1;

--Test settings:
--1. Distinct appears in top query;
--2. With Group;
select distinct c1, c2 from distribute_table_01 group by c1, c2 order by c1, c2;
select distinct c2, c3 from distribute_table_01 group by c2, c3 order by c2, c3;

reset current_schema;
drop schema vector_distribute_distinct cascade;

