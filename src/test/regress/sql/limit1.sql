create schema distribute_limit;
set current_schema = distribute_limit;
create table limit_table_01 (c1 int, c2 int, c3 int) ;
create table limit_table_02 (c1 int, c2 int, c3 int) ;

insert into limit_table_01 values (11, 21, 31);
insert into limit_table_01 values (12, 22, 32);
insert into limit_table_01 values (13, 23, 33);
insert into limit_table_01 values (14, 24, 34);
insert into limit_table_01 values (15, 25, 35);
insert into limit_table_01 values (16, 26, 36);

insert into limit_table_02 values (11, 11, 31);
insert into limit_table_02 values (12, 12, 32);
insert into limit_table_02 values (13, 13, 33);
insert into limit_table_02 values (14, 14, 34);
insert into limit_table_02 values (15, 15, 35);
insert into limit_table_02 values (16, 16, 36);

analyze limit_table_01;
analyze limit_table_02;

set enable_mergejoin=off; 
set enable_nestloop=off; 

--Test settings:
--1. Limit within top query;
--2. Locator type is hash;

explain (verbose, costs off) select c1, c2, c3 from limit_table_01 limit 2 offset 1;

select c1, c2, c3 from limit_table_01 order by c1 limit 2 offset 1;
select c1, c2, c3 from limit_table_01 order by c1 limit 2;
select c1, c2, c3 from limit_table_01 order by c1 offset 1;
select c1, c2, c3 from limit_table_01 order by c1 limit all;
select c1, c2, c3 from limit_table_01 order by c1 limit all offset 1;

--Test settings:
--1. Limit within sub query;
--2. Locator type is hash;

select a.c1, b.c2, b.c3 from (select c1 from limit_table_01 order by c1 limit 2 offset 1) a , limit_table_02 b where a.c1=b.c2 order by a.c1;

create table limit_table_03 (c1 int, c2 int, c3 int) ;

insert into limit_table_03 values (11, 21, 31);
insert into limit_table_03 values (12, 22, 32);
insert into limit_table_03 values (13, 23, 33);
insert into limit_table_03 values (14, 24, 34);
insert into limit_table_03 values (15, 25, 35);
insert into limit_table_03 values (16, 26, 36);

--Test settings:
--1. Limit within top query;
--2. Locator type is replication;

explain (verbose, costs off) select c1, c2, c3 from limit_table_03 limit 2 offset 1;
select c1, c2, c3 from limit_table_03 order by c1 limit 2 offset 1;

--Test settings:
--1. Limit within sub query;
--2. Locator type is replication;

explain (verbose, costs off) select a.c1, b.c2, b.c3 from (select c1 from limit_table_02 limit 2 offset 1) a , limit_table_01 b where a.c1=b.c2;
select a.c1, b.c2, b.c3 from (select c1 from limit_table_03 order by c1 limit 2 offset 1) a , limit_table_02 b where a.c1=b.c2 order by a.c1;
select a.c1, b.c2, b.c3 from (select c1 from limit_table_03 order by c1 limit 2) a , limit_table_02 b where a.c1=b.c2 order by a.c1;
select a.c1, b.c2, b.c3 from (select c1 from limit_table_03 order by c1 offset 1) a , limit_table_02 b where a.c1=b.c2 order by a.c1;

explain (verbose, costs off) select * from limit_table_01 where rownum <= 10;

drop table limit_table_01;
drop table limit_table_02;
drop table limit_table_03;

CREATE TABLE COMPRESS_TABLE_047_1(
        c_int_1 INTEGER,
        c_char_1 CHAR(50),
        c_int_2 INTEGER,
        c_dec_1 DECIMAL(10,4),
        c_char_2 CHAR(50),
        c_tsw_1 TIMESTAMP,
        c_text_1 text,
        c_date_1 DATE, 
        c_tsw_2 TIMESTAMP,
        c_date_2 DATE,
        c_text_2 text,
        c_nvarchar_1 NVARCHAR2(100),
        c_nvarchar_2 NVARCHAR2(100),
        c_dec_2 DECIMAL(10,4));

SELECT COUNT(*),MIN(C_INT_2),MAX(C_INT_2),AVG(C_INT_2) FROM COMPRESS_TABLE_047_1 GROUP BY C_INT_1, C_INT_2 HAVING C_INT_1>5000 AND C_INT_1<150000 ORDER BY C_INT_2 LIMIT 2 OFFSET 200;
DROP TABLE COMPRESS_TABLE_047_1;

reset current_schema;
drop schema distribute_limit cascade;
