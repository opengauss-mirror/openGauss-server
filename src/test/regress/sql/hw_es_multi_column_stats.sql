--==========================================================
--==========================================================
\set ECHO all
drop schema if exists hw_es_multi_column_stats cascade;
create schema hw_es_multi_column_stats;
set current_schema = hw_es_multi_column_stats;
set default_statistics_target=-2;

--========================================================== create table
create table t1 (a int, b int, c int, d int) distribute by hash(a);
create table t1r (a int, b int, c int, d int) distribute by replication;

--========================================================== empty table : analyze
analyze t1 ((a, c));
analyze t1 ((b, c));

analyze t1r ((a, c));
analyze t1r ((b, c));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

alter table t1 add statistics ((a, b));
alter table t1 add statistics ((b, c));
alter table t1 add statistics ((a, d));
alter table t1 add statistics ((b, d));
alter table t1 add statistics ((b, c, d));
alter table t1 add statistics ((a, b, c, d));

alter table t1r add statistics ((a, b));
alter table t1r add statistics ((b, c));
alter table t1r add statistics ((a, d));
alter table t1r add statistics ((b, d));
alter table t1r add statistics ((b, c, d));
alter table t1r add statistics ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

analyze t1 ((b, d));
analyze t1 ((c, d));

analyze t1r ((b, d));
analyze t1r ((c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

analyze t1;
analyze t1r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

alter table t1 delete statistics ((a, c));
alter table t1 delete statistics ((a, b));
alter table t1 delete statistics ((b, c));
alter table t1 delete statistics ((a, d));
alter table t1 delete statistics ((b, d));
alter table t1 delete statistics ((c, d));
alter table t1 delete statistics ((b, c, d));
alter table t1 delete statistics ((a, b, c, d));

alter table t1r delete statistics ((a, c));
alter table t1r delete statistics ((a, b));
alter table t1r delete statistics ((b, c));
alter table t1r delete statistics ((a, d));
alter table t1r delete statistics ((b, d));
alter table t1r delete statistics ((c, d));
alter table t1r delete statistics ((b, c, d));
alter table t1r delete statistics ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

--========================================================== insert normal data
insert into t1 values (1, 1, 1, 1);
insert into t1 values (2, 1, 1, 1);
insert into t1 values (3, 2, 1, 1);
insert into t1 values (4, 2, 1, 1);
insert into t1 values (5, 3, 2, 1);
insert into t1 values (6, 3, 2, 1);
insert into t1 values (7, 4, 2, 1);
insert into t1 values (8, 4, 2, 1);
insert into t1r select * from t1;

--========================================================== analyze t ((a, b))
analyze t1 ((a, b));
analyze t1 ((b, c));
analyze t1 ((a, d));
analyze t1 ((b, d));
analyze t1 ((c, d));
analyze t1 ((b, c, d));
analyze t1 ((a, b, c, d));

analyze t1r ((a, b));
analyze t1r ((b, c));
analyze t1r ((a, d));
analyze t1r ((b, d));
analyze t1r ((c, d));
analyze t1r ((b, c, d));
analyze t1r ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

alter table t1 delete statistics ((a, b));
alter table t1 delete statistics ((b, c));
alter table t1 delete statistics ((a, d));
alter table t1 delete statistics ((b, d));
alter table t1 delete statistics ((c, d));
alter table t1 delete statistics ((b, c, d));
alter table t1 delete statistics ((a, b, c, d));

alter table t1r delete statistics ((a, b));
alter table t1r delete statistics ((b, c));
alter table t1r delete statistics ((a, d));
alter table t1r delete statistics ((b, d));
alter table t1r delete statistics ((c, d));
alter table t1r delete statistics ((b, c, d));
alter table t1r delete statistics ((a, b, c, d));

--========================================================== alter table
analyze t1;
analyze t1r;

alter table t1 add statistics ((a, b));
alter table t1 add statistics ((b, c));
alter table t1 add statistics ((a, d));
alter table t1 add statistics ((b, d));
alter table t1 add statistics ((c, d));
alter table t1 add statistics ((b, c, d));

alter table t1r add statistics ((a, b));
alter table t1r add statistics ((b, c));
alter table t1r add statistics ((a, d));
alter table t1r add statistics ((b, d));
alter table t1r add statistics ((c, d));
alter table t1r add statistics ((b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

analyze t1;
analyze t1r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

alter table t1 delete statistics ((a, b));
alter table t1 delete statistics ((b, c));
alter table t1 delete statistics ((a, d));
alter table t1 delete statistics ((b, d));
alter table t1 delete statistics ((c, d));
alter table t1 delete statistics ((b, c, d));

alter table t1r delete statistics ((a, b));
alter table t1r delete statistics ((b, c));
alter table t1r delete statistics ((a, d));
alter table t1r delete statistics ((b, d));
alter table t1r delete statistics ((c, d));
alter table t1r delete statistics ((b, c, d));

--========================================================== analyze with alter table
alter table t1 add statistics ((a, b));
alter table t1 add statistics ((b, c));
alter table t1 add statistics ((a, d));
alter table t1 add statistics ((b, d));
alter table t1 add statistics ((b, c, d));

alter table t1r add statistics ((a, b));
alter table t1r add statistics ((b, c));
alter table t1r add statistics ((a, d));
alter table t1r add statistics ((b, d));
alter table t1r add statistics ((b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

analyze t1 ((b, d));
analyze t1 ((c, d));

analyze t1r ((b, d));
analyze t1r ((c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

analyze t1;
analyze t1r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

alter table t1 delete statistics ((a, b));
alter table t1 delete statistics ((b, c));
alter table t1 delete statistics ((a, d));
alter table t1 delete statistics ((b, d));
alter table t1 delete statistics ((c, d));
alter table t1 delete statistics ((b, c, d));

alter table t1r delete statistics ((a, b));
alter table t1r delete statistics ((b, c));
alter table t1r delete statistics ((a, d));
alter table t1r delete statistics ((b, d));
alter table t1r delete statistics ((c, d));
alter table t1r delete statistics ((b, c, d));

--========================================================== column orders
analyze t1 ((b, a));
analyze t1 ((c, b));
analyze t1 ((d, a));
analyze t1 ((d, b));
analyze t1 ((d, c));
analyze t1 ((c, b, d));

analyze t1r ((b, a));
analyze t1r ((c, b));
analyze t1r ((d, a));
analyze t1r ((d, b));
analyze t1r ((d, c));
analyze t1r ((c, b, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

alter table t1 delete statistics ((a, b));
alter table t1 delete statistics ((b, c));
alter table t1 delete statistics ((a, d));
alter table t1 delete statistics ((b, d));
alter table t1 delete statistics ((c, d));
alter table t1 delete statistics ((b, c, d));

alter table t1r delete statistics ((a, b));
alter table t1r delete statistics ((b, c));
alter table t1r delete statistics ((a, d));
alter table t1r delete statistics ((b, d));
alter table t1r delete statistics ((c, d));
alter table t1r delete statistics ((b, c, d));

--========================================================== system column and system table

analyze t1 ((xmax, xmin));
analyze t1r ((xmax, xmin));

alter table t1 add statistics ((xmax, xmin));
alter table t1r add statistics ((xmax, xmin));

alter table t1 delete statistics ((xmax, xmin));
alter table t1r delete statistics ((xmax, xmin));

analyze pg_class ((relname, relnamespace));
analyze pg_class (abc);
analyze pg_class ((relname, abc));

alter table pg_class add statistics ((relname, relnamespace));
alter table pg_class add statistics (abc);
alter table pg_class add statistics ((relname, abc));

alter table pg_class delete statistics ((relname, relnamespace));
alter table pg_class delete statistics (abc);
alter table pg_class delete statistics ((relname, abc));

--========================================================== syntax error
analyze t1 (());
analyze t1 ((b));
analyze t1 ((a, a));
analyze t1 ((c, c, d));
analyze t1 ((b, d, b));
analyze t1 ((a, b), (b, c));
analyze t1 (a, (b, c));
analyze t1 ((b, c), a);
analyze t1 ((c, e));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;

alter table t1 add statistics (());
alter table t1 add statistics ((b));
alter table t1 add statistics ((a, a));
alter table t1 add statistics ((c, c, d));
alter table t1 add statistics ((b, d, b));
alter table t1 add statistics ((a, b), (b, c));
alter table t1 add statistics (a, (b, c));
alter table t1 add statistics ((b, c), a);
alter table t1 add statistics ((c, e));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;

alter table t1 delete statistics (());
alter table t1 delete statistics ((b));
alter table t1 delete statistics ((a, a));
alter table t1 delete statistics ((c, c, d));
alter table t1 delete statistics ((b, d, b));
alter table t1 delete statistics ((a, b), (b, c));
alter table t1 delete statistics (a, (b, c));
alter table t1 delete statistics ((b, c), a);
alter table t1 delete statistics ((c, e));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;

--========================================================== data feature : data with NULL
create table t2 (a int, b int, c int, d int) distribute by hash(a);
create table t2r (a int, b int, c int, d int) distribute by replication;

insert into t2 values (1, 1, 1, 1);
insert into t2 values (NULL, 1, 1, 1);
insert into t2 values (3, NULL, 1, 1);
insert into t2 values (4, NULL, 1, 1);
insert into t2 values (5, 3, NULL, 1);
insert into t2 values (6, 3, NULL, 1);
insert into t2 values (7, 4, NULL, 1);
insert into t2 values (8, 4, NULL, 1);
insert into t2r select * from t2;

analyze t2 ((a, b));
analyze t2 ((b, c));
analyze t2 ((a, d));
analyze t2 ((b, d));
analyze t2 ((c, d));
analyze t2 ((b, c, d));

analyze t2r ((a, b));
analyze t2r ((b, c));
analyze t2r ((a, d));
analyze t2r ((b, d));
analyze t2r ((c, d));
analyze t2r ((b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t2' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t2r' order by attname;

drop table t2;
drop table t2r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t2' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t2r' order by attname;

--========================================================== data feature : data with string
create table t3 (a int, b varchar(255), c text, d char) distribute by hash(a);
create table t3r (a int, b varchar(255), c text, d char) distribute by replication;

insert into t3 values (1, 'b1b1', 'c1c1c1c1c1c1', 'd');
insert into t3 values (2, 'b1b1', 'c1c1c1c1c1c1', 'd');
insert into t3 values (3, 'b2b2b2', 'c1c1c1c1c1c1', 'd');
insert into t3 values (4, 'b2b2b2', 'c1c1c1c1c1c1', 'd');
insert into t3 values (5, 'b3b3b3b3b3', 'c2c2c2c2c2c2', 'd');
insert into t3 values (6, 'b3b3b3b3b3', 'c2c2c2c2c2c2', 'd');
insert into t3 values (7, 'b4b4b4b4b4b4', 'c2c2c2c2c2c2', 'd');
insert into t3 values (8, 'b4b4b4b4b4b4', 'c2c2c2c2c2c2', 'd');
insert into t3r select * from t3;

analyze t3 ((a, b));
analyze t3 ((b, c));
analyze t3 ((a, d));
analyze t3 ((b, d));
analyze t3 ((c, d));
analyze t3 ((b, c, d));

analyze t3r ((a, b));
analyze t3r ((b, c));
analyze t3r ((a, d));
analyze t3r ((b, d));
analyze t3r ((c, d));
analyze t3r ((b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t3' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t3r' order by attname;

drop table t3;
drop table t3r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t3' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t3r' order by attname;

--========================================================== table type : partition table
create table t5 (a int, b int, c int, d int) distribute by hash(a)
partition by range (b)
(
partition p1 values less than (1),
partition p2 values less than (2),
partition p3 values less than (3),
partition p4 values less than (4),
partition p5 values less than (5),
partition p6 values less than (maxvalue)
);
create table t5r (a int, b int, c int, d int) distribute by replication
partition by range (b)
(
partition p1 values less than (1),
partition p2 values less than (2),
partition p3 values less than (3),
partition p4 values less than (4),
partition p5 values less than (5),
partition p6 values less than (maxvalue)
);

insert into t5 values (1, 1, 1, 1);
insert into t5 values (2, 1, 1, 1);
insert into t5 values (3, 2, 1, 1);
insert into t5 values (4, 2, 1, 1);
insert into t5 values (5, 3, 2, 1);
insert into t5 values (6, 3, 2, 1);
insert into t5 values (7, 4, 2, 1);
insert into t5 values (8, 4, 2, 1);
insert into t5r select * from t5;

analyze t5 ((a, c));
analyze t5 ((b, c));

analyze t5r ((a, c));
analyze t5r ((b, c));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5r' order by attname;

alter table t5 add statistics ((a, b));
alter table t5 add statistics ((b, c));
alter table t5 add statistics ((a, d));
alter table t5 add statistics ((b, d));
alter table t5 add statistics ((b, c, d));
alter table t5 add statistics ((a, b, c, d));

alter table t5r add statistics ((a, b));
alter table t5r add statistics ((b, c));
alter table t5r add statistics ((a, d));
alter table t5r add statistics ((b, d));
alter table t5r add statistics ((b, c, d));
alter table t5r add statistics ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5r' order by attname;

analyze t5 ((b, d));
analyze t5 ((c, d));

analyze t5r ((b, d));
analyze t5r ((c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5r' order by attname;

analyze t5;
analyze t5r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5r' order by attname;

drop table t5;
drop table t5r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5r' order by attname;

--========================================================== table with index
create table t6 (a int, b int, c int, d int) distribute by hash(a);
create table t6r (a int, b int, c int, d int) distribute by replication;

create index i1_t6 on t6 (b);
create index i1_t6r on t6r (b);
create index i2_t6 on t6 (c, a);
create index i2_t6r on t6r (c, a);

insert into t6 values (1, 1, 1, 1);
insert into t6 values (2, 1, 1, 1);
insert into t6 values (3, 2, 1, 1);
insert into t6 values (4, 2, 1, 1);
insert into t6 values (5, 3, 2, 1);
insert into t6 values (6, 3, 2, 1);
insert into t6 values (7, 4, 2, 1);
insert into t6 values (8, 4, 2, 1);
insert into t6r select * from t6;

analyze t6 ((a, c));
analyze t6 ((b, c));

analyze t6r ((a, c));
analyze t6r ((b, c));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6r' order by attname;

alter table t6 add statistics ((a, b));
alter table t6 add statistics ((b, c));
alter table t6 add statistics ((a, d));
alter table t6 add statistics ((b, d));
alter table t6 add statistics ((b, c, d));
alter table t6 add statistics ((a, b, c, d));

alter table t6r add statistics ((a, b));
alter table t6r add statistics ((b, c));
alter table t6r add statistics ((a, d));
alter table t6r add statistics ((b, d));
alter table t6r add statistics ((b, c, d));
alter table t6r add statistics ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6r' order by attname;

analyze t6 ((b, d));
analyze t6 ((c, d));

analyze t6r ((b, d));
analyze t6r ((c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6r' order by attname;

analyze t6;
analyze t6r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6r' order by attname;

drop table t6 cascade;
drop table t6r cascade;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6r' order by attname;

--========================================================== drop column, modify column
create table t7 (a int, b int, c int, d int) distribute by hash(a);
create table t7r (a int, b int, c int, d int) distribute by replication;

insert into t7 values (1, 1, 1, 1);
insert into t7 values (2, 1, 1, 1);
insert into t7 values (3, 2, 1, 1);
insert into t7 values (4, 2, 1, 1);
insert into t7 values (5, 3, 2, 1);
insert into t7 values (6, 3, 2, 1);
insert into t7 values (7, 4, 2, 1);
insert into t7 values (8, 4, 2, 1);
insert into t7r select * from t7;

analyze t7;
analyze t7r;

analyze t7 ((a, b));
analyze t7 ((b, c));
analyze t7 ((a, d));
analyze t7 ((b, d));
analyze t7 ((c, d));
analyze t7 ((b, c, d));
analyze t7 ((a, b, c, d));

analyze t7r ((a, b));
analyze t7r ((b, c));
analyze t7r ((a, d));
analyze t7r ((b, d));
analyze t7r ((c, d));
analyze t7r ((b, c, d));
analyze t7r ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

-- drop column
alter table t7 drop column b;
alter table t7r drop column b;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

alter table t7 add column b int;
alter table t7r add column b int;

update t7 set b = 5 where a = 1;
update t7 set b = 5 where a = 2;
update t7 set b = 6 where a = 3;
update t7 set b = 6 where a = 4;
update t7 set b = 7 where a = 5;
update t7 set b = 7 where a = 6;
update t7 set b = 8 where a = 7;
update t7 set b = 8 where a = 8;

update t7r set b = 5 where a = 1;
update t7r set b = 5 where a = 2;
update t7r set b = 6 where a = 3;
update t7r set b = 6 where a = 4;
update t7r set b = 7 where a = 5;
update t7r set b = 7 where a = 6;
update t7r set b = 8 where a = 7;
update t7r set b = 8 where a = 8;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

analyze t7 ((a, b));
analyze t7 ((b, c));
analyze t7 ((a, d));
analyze t7 ((b, d));
analyze t7 ((c, d));
analyze t7 ((b, c, d));
analyze t7 ((a, b, c, d));

analyze t7r ((a, b));
analyze t7r ((b, c));
analyze t7r ((a, d));
analyze t7r ((b, d));
analyze t7r ((c, d));
analyze t7r ((b, c, d));
analyze t7r ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

-- modify type
alter table t7 modify d int2;
alter table t7r modify d text;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

analyze t7 ((a, d));
analyze t7 ((b, d));
analyze t7 ((c, d));
analyze t7 ((b, c, d));
analyze t7 ((a, b, c, d));

analyze t7r ((a, d));
analyze t7r ((b, d));
analyze t7r ((c, d));
analyze t7r ((b, c, d));
analyze t7r ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

drop table t7;
drop table t7r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

--==========================================================
--==========================================================
--==========================================================













--==========================================================
drop schema hw_es_multi_column_stats cascade;
reset default_statistics_target;

