/*
 * This file is used to test the functional dependency statistics
 */
set enable_ai_stats=0;
set multi_stats_type='MCV';
create schema functional_dependency;
set current_schema = functional_dependency;

set default_statistics_target = -100;

------------------------------
-- test the functional dependency statistics for void
------------------------------
create table t6(a int, b float, c char(10), d text, e timestamp, f boolean);
alter table t6 add statistics ((a, b, c, d));

set enable_functional_dependency = off;
show enable_functional_dependency;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;

set enable_functional_dependency = on;
show enable_functional_dependency;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;

alter table t6 delete statistics ((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;

drop table t6;

------------------------------
-- test the functional dependency statistics for data type
------------------------------

-- int, tinyint, smallint, bigint
create table t6(a int, b tinyint, c smallint, d bigint, e int, f int);
insert into t6 values (1, 1, 1, 1, 1, 1);
insert into t6 values (2, 1, 2, 0, 1, 2);
insert into t6 values (3, 0, 3, 0, 1, 2);
insert into t6 values (4, 1, 4, 0, 1, 2);
insert into t6 values (5, 0, 5, 0, 1, 3);
insert into t6 values (0, 0, 6, 1, 1, 3);
insert into t6 values (0, 0, 0, 0, 1, 3);
insert into t6 values (0, 1, 1, 1, 1, 3);
analyze t6;
alter table t6 add statistics ((a, b, c, d));
set enable_functional_dependency = off;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
set enable_functional_dependency = on;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
drop table t6;

-- float and decimal
create table t6(a float, b decimal, c real, d float, e decimal, f real);
insert into t6 values (1.2, 1.2, 1.2, 1.2, 1.2, 1.2);
insert into t6 values (2.2, 1.2, 2.2, 0, 1.2, 2.2);
insert into t6 values (3.2, 0, 3.2, 0, 1.2, 2.2);
insert into t6 values (4.2, 1.2, 4.2, 0, 1.2, 2.2);
insert into t6 values (5.2, 0, 5.2, 0, 1.2, 3.2);
insert into t6 values (0, 0, 6.2, 1.2, 1.2, 3.2);
insert into t6 values (0, 0, 0, 0, 1.2, 3.2);
insert into t6 values (0, 1.2, 1.2, 1.2, 1.2, 3.2);
analyze t6;
alter table t6 add statistics ((a, b, c, d));
set enable_functional_dependency = off;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
set enable_functional_dependency = on;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
drop table t6;

-- char, varchar, text
create table t6(a char(10), b text, c varchar(10), d char(10), e text, f varchar(10));
insert into t6 values ('aa', 'aa', 'aa', 'aa', 'aa', 'aa');
insert into t6 values ('bb', 'aa', 'bb', NULL, 'aa', 'bb');
insert into t6 values ('cc', NULL, 'cc', NULL, 'aa', 'bb');
insert into t6 values ('dd', 'aa', 'dd', NULL, 'aa', 'bb');
insert into t6 values ('ee', NULL, 'ee', NULL, 'aa', 'cc');
insert into t6 values (NULL, NULL, 'ff', 'aa', 'aa', 'cc');
insert into t6 values (NULL, NULL, NULL, NULL, 'aa', 'cc');
insert into t6 values (NULL, 'aa', 'aa', 'aa', 'aa', 'cc');
analyze t6;
alter table t6 add statistics ((a, b, c, d));
set enable_functional_dependency = off;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
set enable_functional_dependency = on;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
drop table t6;

-- timestamp and date
create table t6(a timestamp, b date, c timestamp, d date, e timestamp, f date);
insert into t6 values ('20220101', '20220101', '20220101', '20220101', '20220101', '20220101');
insert into t6 values ('20220102', '20220101', '20220102', NULL, '20220101', '20220102');
insert into t6 values ('20220103', 	NULL, '20220103', NULL, '20220101', '20220102');
insert into t6 values ('20220104', '20220101', '20220104', NULL, '20220101', '20220102');
insert into t6 values ('20220105', NULL, '20220105', NULL, '20220101', '20220103');
insert into t6 values (NULL, NULL, '20220106', '20220101', '20220101', '20220103');
insert into t6 values (NULL, NULL, NULL, NULL, '20220101', '20220103');
insert into t6 values (NULL, '20220101', '20220101', '20220101', '20220101', '20220103');
analyze t6;
alter table t6 add statistics ((a, b, c, d));
set enable_functional_dependency = off;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
set enable_functional_dependency = on;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
drop table t6;

-- boolean
create table t6(a boolean, b boolean, c boolean, d boolean, e boolean, f boolean);
insert into t6 values (true, false, true, true, true, true);
insert into t6 values (true, true, true, true, true, true);
insert into t6 values (true, true, true, false, true, true);
insert into t6 values (true, false, false, false, true, true);
insert into t6 values (true, NULL, true, NULL, true, true);
insert into t6 values (NULL, NULL, true, true, true, true);
insert into t6 values (NULL, NULL, NULL, NULL, true, true);
insert into t6 values (NULL, false, NULL, false, true, true);
analyze t6;
alter table t6 add statistics ((a, b, c, d));
set enable_functional_dependency = off;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
set enable_functional_dependency = on;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
drop table t6;

------------------------------
-- test functional dependency statistics for SELECT clauses
------------------------------
create table t6(a int, b int, c int, d int, e int, f int);
insert into t6 values (1, 1, 1, 1, 1, 1);
insert into t6 values (2, 1, 2, 0, 1, 2);
insert into t6 values (3, 0, 3, 0, 1, 2);
insert into t6 values (4, 1, 4, 0, 1, 2);
insert into t6 values (5, 0, 5, 0, 1, 3);
insert into t6 values (0, 0, 6, 1, 1, 3);
insert into t6 values (0, 0, 0, 0, 1, 3);
insert into t6 values (0, 1, 1, 1, 1, 3);
analyze t6;

set enable_functional_dependency = on;

alter table t6 add statistics ((a, b, c, d));
analyze t6((a, b, c, d));

select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;

-- test for two attributes 
-- {a, b} in functional dependency statistics
-- {f} not in functional dependency statistics
select * from t6 where a = 1 and b = 1;
select * from t6 where 1 = a and b = 1;
select * from t6 where a = 1 and 1 = b;
select * from t6 where 1 = a and 1 = b;

select * from t6 where a = 1 and f = 1;
select * from t6 where 1 = a and f = 1;
select * from t6 where a = 1 and 1 = f;
select * from t6 where 1 = a and 1 = f;

-- test for three attributes 
-- {a, b, c, d} in functional dependency statistics
-- {f} not in functional dependency statistics
select * from t6 where a = 1 and b = 1 and c = 1;
select * from t6 where a = 1 and b = 1 and c+1 = 1;
select * from t6 where a = 1 and b = 1 and c = 1+1;
select * from t6 where a = 1 and b = 1 and c in(1,2,3);
select * from t6 where a = 1 and b = 1 and c in(1,1,1);
select * from t6 where a = 1 and b = 1 and c>1;
select * from t6 where a = 1 and b = 1 and c<1;
select * from t6 where a = 1 and b = 1 and c>0 and c<2;
select * from t6 where a = 1 and b = 1 and c;
select * from t6 where a = 1 and b = 1 and not c;

select * from t6 where a = 1 and b = 1 and f = 1;
select * from t6 where a = 1 and b = 1 and f+1 = 1;
select * from t6 where a = 1 and b = 1 and f = 1+1;
select * from t6 where a = 1 and b = 1 and f in(1,2,3);
select * from t6 where a = 1 and b = 1 and f in(1,1,1);
select * from t6 where a = 1 and b = 1 and f>1;
select * from t6 where a = 1 and b = 1 and f<1;
select * from t6 where a = 1 and b = 1 and f>0 and f<2;
select * from t6 where a = 1 and b = 1 and f;
select * from t6 where a = 1 and b = 1 and not f;

select * from t6 where a = 1 and b = 1 and c = d;
select * from t6 where a = 1 and b = 1 and c+1 = d+1;
select * from t6 where a = 1 and b = 1 and c = d+1;
select * from t6 where a = 1 and b = 1 and c+1 = d;

select * from t6 where a = 1 and b = 1 and c = f;
select * from t6 where a = 1 and b = 1 and c+1 = f+1;
select * from t6 where a = 1 and b = 1 and c = f+1;
select * from t6 where a = 1 and b = 1 and c+1 = f;

select * from t6 where a = 1 and b = 1 and c = d+f;
select * from t6 where a = 1 and b = 1 and c+f = d;
select * from t6 where a = 1 and b = 1 and c+1 = d+f;
select * from t6 where a = 1 and b = 1 and c+f+1 = d;
select * from t6 where a = 1 and b = 1 and c = d+f+1;
select * from t6 where a = 1 and b = 1 and c+f = d+1;
select * from t6 where a = 1 and b = 1 and c+1 = d+f+1;
select * from t6 where a = 1 and b = 1 and c+f+1 = d+1;

-- test for four attributes 
-- {a, b, c, d} in functional dependency statistics
-- {e, f} not in functional dependency statistics
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d+1 = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1+1;
select * from t6 where a = 1 and b = 1 and c = 1 and d in(1,2,3);
select * from t6 where a = 1 and b = 1 and c = 1 and d in(1,1,1);
select * from t6 where a = 1 and b = 1 and c = 1 and d>1;
select * from t6 where a = 1 and b = 1 and c = 1 and d<1;
select * from t6 where a = 1 and b = 1 and c = 1 and d>0 and d<2;
select * from t6 where a = 1 and b = 1 and c = 1 and d;
select * from t6 where a = 1 and b = 1 and c = 1 and not d;

select * from t6 where a = 1 and b = 1 and c = 1 and f = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and f+1 = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and f = 1+1;
select * from t6 where a = 1 and b = 1 and c = 1 and f in(1,2,3);
select * from t6 where a = 1 and b = 1 and c = 1 and f in(1,1,1);
select * from t6 where a = 1 and b = 1 and c = 1 and f>1;
select * from t6 where a = 1 and b = 1 and c = 1 and f<1;
select * from t6 where a = 1 and b = 1 and c = 1 and f>0 and f<2;
select * from t6 where a = 1 and b = 1 and c = 1 and f;
select * from t6 where a = 1 and b = 1 and c = 1 and not f;

select * from t6 where a = 1 and b = 1 and c = 1 and f = d;
select * from t6 where a = 1 and b = 1 and c = 1 and f+1 = d+1;
select * from t6 where a = 1 and b = 1 and c = 1 and f = d+1;
select * from t6 where a = 1 and b = 1 and c = 1 and f+1 = d;

select * from t6 where a = 1 and b = 1 and c = 1 and f = d+e;
select * from t6 where a = 1 and b = 1 and c = 1 and f+e = d;
select * from t6 where a = 1 and b = 1 and c = 1 and f+1 = d+e;
select * from t6 where a = 1 and b = 1 and c = 1 and f+e+1 = d;
select * from t6 where a = 1 and b = 1 and c = 1 and f = d+e+1;
select * from t6 where a = 1 and b = 1 and c = 1 and f+e = d+1;
select * from t6 where a = 1 and b = 1 and c = 1 and f+1 = d+e+1;
select * from t6 where a = 1 and b = 1 and c = 1 and f+e+1 = d+1;

drop table t6;

------------------------------
-- test functional dependency statistics for SELECT clauses with hints
------------------------------
create table t6(a int, b int, c int, d int, e int, f int);
insert into t6 values (1, 1, 1, 1, 1, 1);
insert into t6 values (2, 1, 2, 0, 1, 2);
insert into t6 values (3, 0, 3, 0, 1, 2);
insert into t6 values (4, 1, 4, 0, 1, 2);
insert into t6 values (5, 0, 5, 0, 1, 3);
insert into t6 values (0, 0, 6, 1, 1, 3);
insert into t6 values (0, 0, 0, 0, 1, 3);
insert into t6 values (0, 1, 1, 1, 1, 3);
analyze t6;
alter table t6 add statistics ((a, b, c, d));

set enable_functional_dependency = off;
show enable_functional_dependency;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;

show enable_functional_dependency;
set enable_functional_dependency = on;
show enable_functional_dependency;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;

show enable_functional_dependency;
set enable_functional_dependency = on;
show enable_functional_dependency;
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;

show enable_functional_dependency;
set enable_functional_dependency = off;
show enable_functional_dependency;
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and f = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select /*+set(enable_functional_dependency ON)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and f = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select /*+set(enable_functional_dependency OFF)*/ * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;

drop table t6;

------------------------------
-- test functional dependency statistics for show PG_STATISTIC_EXT, ALTER TABLE ADD/DELETE STATISTICS, SELECT
------------------------------
create table t6(a int, b int, c int, d int, e int, f int);
insert into t6 values (1, 1, 1, 1, 1, 1);
insert into t6 values (2, 1, 2, 0, 1, 2);
insert into t6 values (3, 0, 3, 0, 1, 2);
insert into t6 values (4, 1, 4, 0, 1, 2);
insert into t6 values (5, 0, 5, 0, 1, 3);
insert into t6 values (0, 0, 6, 1, 1, 3);
insert into t6 values (0, 0, 0, 0, 1, 3);
insert into t6 values (0, 1, 1, 1, 1, 3);
analyze t6;

set enable_functional_dependency = on;

alter table t6 add statistics ((a, b, c, d, e, f));
analyze t6((a, b, c, d, e, f));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select * from t6 where a = 1 and b = 1 and c = 1;
select * from t6 where a = 1 and b = 1;
select * from t6 where a = 1;

alter table t6 add statistics ((a, b, c, d, e));
analyze t6((a, b, c, d, e));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select * from t6 where a = 1 and b = 1 and c = 1;
select * from t6 where a = 1 and b = 1;
select * from t6 where a = 1;

alter table t6 add statistics ((a, b, c, d));
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select * from t6 where a = 1 and b = 1 and c = 1;
select * from t6 where a = 1 and b = 1;
select * from t6 where a = 1;

alter table t6 add statistics ((a, b, c));
analyze t6((a, b, c));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select * from t6 where a = 1 and b = 1 and c = 1;
select * from t6 where a = 1 and b = 1;
select * from t6 where a = 1;

alter table t6 add statistics ((a, b));
analyze t6((a, b));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select * from t6 where a = 1 and b = 1 and c = 1;
select * from t6 where a = 1 and b = 1;
select * from t6 where a = 1;

alter table t6 delete statistics ((a, b, c, d, e, f));
alter table t6 delete statistics ((a, b, c, d, e));
alter table t6 delete statistics ((a, b, c, d));
alter table t6 delete statistics ((a, b, c));
alter table t6 delete statistics ((a, b));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select * from t6 where a = 1 and b = 1 and c = 1;
select * from t6 where a = 1 and b = 1;
select * from t6 where a = 1;

drop table t6;

------------------------------
-- test functional dependency statistics for row/column-based storage, low/high compression
------------------------------
create table t6(a int, b int, c int, d int, e int, f int) with (orientation = column);
insert into t6 values (1, 1, 1, 1, 1, 1);
insert into t6 values (2, 1, 2, 0, 1, 2);
insert into t6 values (3, 0, 3, 0, 1, 2);
insert into t6 values (4, 1, 4, 0, 1, 2);
insert into t6 values (5, 0, 5, 0, 1, 3);
insert into t6 values (0, 0, 6, 1, 1, 3);
insert into t6 values (0, 0, 0, 0, 1, 3);
insert into t6 values (0, 1, 1, 1, 1, 3);
analyze t6;
set enable_functional_dependency = on;
alter table t6 add statistics ((a, b, c, d));
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select * from t6 where a = 1 and b = 1 and c = 1;
select * from t6 where a = 1 and b = 1;
select * from t6 where a = 1;
drop table t6;

create table t6(a int, b int, c int, d int, e int, f int) with (orientation = column, compression = high);
insert into t6 values (1, 1, 1, 1, 1, 1);
insert into t6 values (2, 1, 2, 0, 1, 2);
insert into t6 values (3, 0, 3, 0, 1, 2);
insert into t6 values (4, 1, 4, 0, 1, 2);
insert into t6 values (5, 0, 5, 0, 1, 3);
insert into t6 values (0, 0, 6, 1, 1, 3);
insert into t6 values (0, 0, 0, 0, 1, 3);
insert into t6 values (0, 1, 1, 1, 1, 3);
analyze t6;
set enable_functional_dependency = on;
alter table t6 add statistics ((a, b, c, d));
analyze t6((a, b, c, d));
select stakey, stakind1, stanumbers1, staop1, stavalues1 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind2, stanumbers2, staop2, stavalues2 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind3, stanumbers3, staop3, stavalues3 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind4, stanumbers4, staop4, stavalues4 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select stakey, stakind5, stanumbers5, staop5, stavalues5 from pg_statistic_ext, pg_class where pg_class.relname = 't6' and pg_class.relfilenode = pg_statistic_ext.starelid;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1 and f = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1 and e = 1;
select * from t6 where a = 1 and b = 1 and c = 1 and d = 1;
select * from t6 where a = 1 and b = 1 and c = 1;
select * from t6 where a = 1 and b = 1;
select * from t6 where a = 1;
drop table t6;

reset enable_functional_dependency;
reset current_schema;
drop schema functional_dependency cascade;
