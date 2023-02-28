set client_min_messages = ERROR;
set enable_default_ustore_table = on;
set enable_opfusion = off;
set enable_indexonlyscan = on;
set enable_indexscan = on;
set enable_bitmapscan = off;
set enable_seqscan = off;

drop schema if exists indexscan_optimization cascade;
drop index if exists t_ustore_c2 cascade;
drop index if exists t_ustore_c4 cascade;
drop index if exists t_ustore_c3_c5 cascade;
drop index if exists t_ustore_c3_c4_c5 cascade;
drop table if exists t_ustore cascade;
set client_min_messages = NOTICE;

create schema indexscan_optimization;
set current_schema = indexscan_optimization;

create table t_ustore(
    c1 int primary key,
    c2 int,
    c3 int,
    c4 varchar(20) collate "C",
    c5 bigint
) with (storage_type=USTORE);

\d+ t_ustore

create index t_ustore_c2 on t_ustore(c2);
create index t_ustore_c4 on t_ustore(c4);
create index t_ustore_c3_c5 on t_ustore(c3, c5);

insert into t_ustore values(0, 1, 1, 'abcdefg', 2);
insert into t_ustore values(1, 2, 1, 'abcdefg', 2);
insert into t_ustore values(2, 3, 2, 'hijklmn', 3);
insert into t_ustore values(3, 4, 2, 'ABCDEFG', 3);
insert into t_ustore values(4, 5, 3, 'hijklmn', 4);
insert into t_ustore values(-1, 6, 0, 'opqrst', 2);
insert into t_ustore values(2147483647, 7, 0, 'opq rst', 5);
insert into t_ustore values(-2147483647, 8, 0, 'opq rst', 5);
insert into t_ustore values(5, 9, 0, 'uvwxyz', 9223372036854775807);
insert into t_ustore values(6, 10, 1, 'uvw xyz', -9223372036854775807);
insert into t_ustore values(7, 11, 0, 'abc', 2);
insert into t_ustore values(8, 12, 0);
insert into t_ustore values(9, 13);
insert into t_ustore values(10, 14, NULL, 'hij', 0);
insert into t_ustore values(11, 15, 0, NULL, 0);
insert into t_ustore values(12, 16, 0, 'hij', NULL);

select * from t_ustore order by c1;

explain(costs off) select count(*) from t_ustore;
select count(*) from t_ustore;
explain(costs off) select max(c1) from t_ustore;
select max(c1) from t_ustore;
explain(costs off) select * from t_ustore where c1 < 3 order by c1;
select * from t_ustore where c1 < 3 order by c1;

explain(costs off) select * from t_ustore where c2 = 2;
select * from t_ustore where c2 = 2;

explain(costs off) select * from t_ustore where c4 = 'abcdefg' order by (c4, c1);
select * from t_ustore where c4 = 'abcdefg' order by (c4, c1);

explain(costs off) select * from t_ustore where c3 > 0 and c5 < 5 order by c1;
select * from t_ustore where c3 > 0 and c5 < 5 order by c1;

explain(costs off) select /*+ indexonlyscan(t_ustore)*/ c3,c5 from t_ustore where c3 > 0 and c5 < 5 order by (c3, c5);
select /*+ indexonlyscan(t_ustore)*/ c3,c5 from t_ustore where c3 > 0 and c5 < 5 order by (c3, c5);

explain(costs off) select * from t_ustore where c1 between -1 and 5 and c2 between 3 and 8 order by c1;
select * from t_ustore where c1 between -1 and 5 and c2 between 3 and 8 order by c1;

drop index t_ustore_c2 cascade;
drop index t_ustore_c4 cascade;
drop index t_ustore_c3_c5 cascade;

create unique index t_ustore_c3_c4_c5 on t_ustore(c3, c4, c5);
explain(costs off) select * from t_ustore where c3 > 0 and c4 < 'hijlmn' and c5 < 5 order by c1;
select * from t_ustore where c3 > 0 and c4 < 'hijlmn' and c5 < 5 order by c1;

explain(costs off) select /*+ indexonlyscan(t_ustore)*/ c3, c4, c5 from t_ustore where c3 > 0 and c4 < 'hijklmn' and c5 < 5 order by (c3, c4, c5);
select /*+ indexonlyscan(t_ustore)*/ c3, c4, c5 from t_ustore where c3 > 0 and c4 < 'hijklmn' and c5 < 5 order by (c3, c4, c5);
drop index t_ustore_c3_c4_c5;

-- bitmap index scan
set enable_bitmapscan = on;
set enable_indexscan = off;
set enable_indexonlyscan = off;
explain(costs off) select * from t_ustore where c1 between -1 and 3 order by c1;
select * from t_ustore where c1 between -1 and 3 order by c1;

-- clear
drop table t_ustore cascade;
reset current_schema;
drop schema indexscan_optimization cascade;
reset enable_default_ustore_table;
reset enable_opfusion;
reset enable_indexonlyscan;
reset enable_indexscan;
reset enable_bitmapscan;
reset enable_seqscan;
reset client_min_messages;




















