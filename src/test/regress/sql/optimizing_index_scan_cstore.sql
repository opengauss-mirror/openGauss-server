set client_min_messages = ERROR;
set enable_opfusion = off;
set enable_indexonlyscan = on;
set enable_indexscan = on;
set enable_bitmapscan = off;
set enable_seqscan = off;

drop schema if exists indexscan_optimization cascade;
drop index if exists t1_c2 cascade;
drop index if exists t1_c3 cascade;
drop index if exists t1_c4 cascade;
drop index if exists t1_c4_nf cascade;
drop index if exists t1_c5 cascade;
drop index if exists t1_c3_c5 cascade;
drop index if exists t1_c3_c4_c5 cascade;

drop index if exists t2_c2 cascade;

drop index if exists t3_c2 cascade;
drop index if exists t3_c3 cascade;

drop table if exists t1 cascade;
drop table if exists t2 cascade;
drop table if exists t3 cascade;

set client_min_messages = NOTICE;

create schema indexscan_optimization;
set current_schema = indexscan_optimization;

create table t1(
    c1 int primary key,
    c2 int,
    c3 int,
    c4 varchar(20) collate "C",
    c5 bigint
);
create table t2(c1 bigint primary key, c2 bigint);
create table t3(c1 int, c2 numeric, c3 date);

create unique index t1_c2 on t1(c2);
create index t1_c3_c5 on t1(c3, c5);
create index t1_c4 on t1(c4);
create index t1_c4_nf on t1(c4 nulls first); -- nulls-first index
create index t1_lower_c4 on t1(lower(c4)); -- index on expression

create index t2_c2 on t1(c2);

create index t3_c2 on t1(c3);
create index t3_c3 on t1(c3);

insert into t1 values(0, 1, 1, 'abcdefg', 2);
insert into t1 values(1, 2, 1, 'abcdefg', 2);
insert into t1 values(2, 3, 2, 'hijklmn', 3);
insert into t1 values(3, 4, 2, 'ABCDEFG', 3);
insert into t1 values(4, 5, 3, 'hijklmn', 4);
insert into t1 values(-1, 6, 0, 'opqrst', 2);
insert into t1 values(2147483647, 7, 0, 'opq rst', 5);
insert into t1 values(-2147483647, 8, 0, 'opq rst', 5);
insert into t1 values(5, 9, 0, 'uvwxyz', 9223372036854775807);
insert into t1 values(6, 10, 1, 'uvw xyz', -9223372036854775807);
insert into t1 values(7, 11, 0, 'abc', 2);
insert into t1 values(8, 12, 0);
insert into t1 values(9, 13);
insert into t1 values(10, 14, NULL, 'hij', 0);
insert into t1 values(11, 15, 0, NULL, 0);
insert into t1 values(12, 16, 0, 'hij', NULL);

insert into t2 values(generate_series(0, 1000), generate_series(0, 1000));
insert into t2 values(9223372036854775807, 2001);
insert into t2 values(-9223372036854775807, 2002);

insert into t3 values(generate_series(0, 1000), generate_series(0, 1000), generate_series('2050-01-01'::date, '2055-06-24', '2 days'));
insert into t3 values(generate_series(1001, 2000), generate_series(-1000, -1), generate_series('1601-01-01'::date, '1606-06-22', '2 days'));

analyze t1;
analyze t2;
analyze t3;

--t1:c1
explain(costs off) select * from t1 order by c1;
select * from t1 order by c1;
select * from t1 order by c1 desc;
explain(costs off) select count(*) from t1;
select count(*) from t1;
explain(costs off) select max(c1) from t1;
select max(c1) from t1;
explain(costs off) select * from t1 where c1 = 2;
select * from t1 where c1 = 2;
select * from t1 where c1 = -1;
explain(costs off) select * from t1 where c1 < 5 order by c1;
select * from t1 where c1 < 5 order by c1;
explain(costs off) select * from t1 where c1 between 1 and 5 order by c1;
select * from t1 where c1 between 1 and 5 order by c1;

--t1:c2
explain(costs off) select * from t1 where c2 = 2 order by c2;
select * from t1 where c2 = 2;
explain(costs off) select * from t1 where c2 < 5 order by c2;
select * from t1 where c2 < 5 order by c2;
explain(costs off) select * from t1 where c2 between 1 and 5 order by c2;
select * from t1 where c2 between 1 and 5 order by c2;
explain(costs off) select /*+ indexonlyscan(t1)*/ c2 from t1 where c2 < 5 order by c2;
select /*+ indexonlyscan(t1)*/ c2 from t1 where c2 < 5 order by c2;

--t1:c3
explain(costs off) select * from t1 where c3 = 2 order by c1;
select * from t1 where c3 = 2 order by c1;
explain(costs off) select * from t1 where c3 = 2::bigint order by c1;
select * from t1 where c3 = 2::bigint order by c1;
explain(costs off) select * from t1 where c3 > 1 order by (c3,c1);
select * from t1 where c3 > 1 order by (c3,c1);
explain(costs off) select * from t1 where c3 > 1::bigint order by (c3,c1);
select * from t1 where c3 > 1::bigint order by (c3,c1);

--t1:c4
explain(costs off) select * from t1 where c4 = 'abcdefg' order by (c4, c1);
select * from t1 where c4 = 'abcdefg' order by (c4, c1);
explain(costs off) select * from t1 where c4 like 'abc%' order by (c4, c1);
select * from t1 where c4 like 'abc%' order by (c4, c1);
explain(costs off) select * from t1 where c4 < 'opqrst' order by (c4, c1);
select * from t1 where c4 < 'opqrst' order by (c4, c1);
explain(costs off) select c4 from t1  order by c4 nulls first limit 5;
select c4 from t1  order by c4 nulls first limit 5;
explain(costs off) select * from t1 where lower(c4) = 'abcdefg' order by (c4, c1);
select * from t1 where lower(c4) = 'abcdefg' order by (c4, c1);

--t1:c5
explain(costs off) select * from t1 where c5 = 2 order by c1;
select * from t1 where c5 = 2 order by c1;
select * from t1 where c5 = -1 order by c1;
explain(costs off) select * from t1 where c5 > 3 order by (c5, c1);
select * from t1 where c5 > 3 order by (c5, c1);
explain(costs off) select * from t1 where c5 between 1 and 4 order by (c5, c1);
select * from t1 where c5 between 1 and 4 order by (c5, c1);

--t1:c1 c2
explain(costs off) select * from t1 where c1 between -1 and 5 and c2 between 3 and 8 order by c1;
select * from t1 where c1 between -1 and 5 and c2 between 3 and 8 order by c1;

--t1:c3 c5
explain(costs off) select * from t1 where c3 > 0 and c5 < 5 order by c1;
select * from t1 where c3 > 0 and c5 < 5 order by c1;
explain(costs off) select /*+ indexonlyscan(t1)*/ c3,c5 from t1 where c3 > 0 and c5 < 5 order by (c3, c5);
select /*+ indexonlyscan(t1)*/ c3,c5 from t1 where c3 > 0 and c5 < 5 order by (c3, c5);


--t2:c1
explain(costs off) select * from t2 order by c1 limit 10;
select * from t2 order by c1 limit 10;
select * from t2 where c1 = 2;
select * from t2 where c1 = 9223372036854775807;
explain(costs off) select * from t2 where c1 < 5 order by c1;
select * from t2 where c1 < 5 order by c1;

--t2:c2
explain(costs off) select * from t2 where c2 = 1500;
select * from t2 where c2 = 1500;
explain(costs off) select * from t2 where c2 = 1500::bigint;
select * from t2 where c2 = 1500::bigint;
explain(costs off) select * from t2 where c2 < 1005 order by (c2, c1);
select * from t2 where c2 < 1005 order by (c2, c1);
explain(costs off) select * from t2 where c2 < 1005::bigint order by (c2, c1);
select * from t2 where c2 < 1005::bigint order by (c2, c1);


--t3
explain(costs off) select c1,c2 from t3 where c2 = 500;
select c1,c2 from t2 where c2 = 500;
explain(costs off) select c1,c2 from t3 where c2 between -2 and 2 order by (c2, c1);
select c1,c2 from t2 where c2 between -2 and 2 order by (c2, c1);
explain(costs off) select c1,c3 from t3 where c3 = '2050-12-25'::date;
select c1,c2 from t3 where c2 between -2 and 2 order by (c2, c1);
explain(costs off) select c1,c3 from t3 where c3 between '2050-05-01'::date and '2050-05-10' order by (c3, c1);
select c1,c3 from t3 where c3 between '2050-05-01'::date and '2050-05-10' order by (c3, c1);
select c1,c3 from t3 where c3 between '1606-06-20'::date and '2050-01-05' order by (c3, c1);

--t1 join t2
explain(costs off) select * from t1 join t2 on (t1.c1 = t2.c1) where t1.c1 between 1 and 3 order by t1.c1;
select * from t1 join t2 on (t1.c1 = t2.c1) where t1.c1 between 1 and 3 order by t1.c1;

drop index t1_c2 cascade;
drop index t1_c3_c5 cascade;
drop index t1_c4 cascade;
drop index t1_c4_nf cascade;
drop index t1_lower_c4 cascade;
drop index t2_c2 cascade;
drop index t3_c2 cascade;
drop index t3_c3 cascade;

--Test 3-column index

create  index t1_c3_c4_c5 on t1(c3,c4,c5);
explain(costs off) select * from t1 where c3 > 0 and c4 < 'hijlmn' and c5 < 5 order by c1;
select * from t1 where c3 > 0 and c4 < 'hijlmn' and c5 < 5 order by c1;
explain(costs off) select /*+ indexonlyscan(t1)*/ c3,c4,c5 from t1 where c3 > 0 and c4 < 'hijlmn' and c5 < 5 order by (c3, c4, c5);
select /*+ indexonlyscan(t1)*/ c3,c4,c5 from t1 where c3 > 0 and c4 < 'hijlmn' and c5 < 5 order by (c3, c4, c5);
drop index t1_c3_c4_c5 cascade;

--Test multiple index combination
create  index t1_c3 on t1(c3);
create  index t1_c5 on t1(c5);
explain(costs off) select * from t1 where c3 = 1 and c5 = 2;
select * from t1 where c3 = 1 and c5 = 2;
drop index t1_c5 cascade;
drop index t1_c3 cascade;

--Test bitmap index scan
set enable_bitmapscan = on;
set enable_indexscan = off;
set enable_indexonlyscan = off;
explain(costs off) select * from t1 where c1 between -1 and 3 order by c1;
select * from t1 where c1 between -1 and 3 order by c1;

-- clear
vacuum;

drop table t1 cascade;
drop table t2 cascade;
drop table t3 cascade;

reset current_schema;
drop schema indexscan_optimization cascade;

reset enable_opfusion;
reset enable_indexonlyscan;
reset enable_indexscan;
reset enable_bitmapscan;
reset enable_seqscan;
reset client_min_messages;




















