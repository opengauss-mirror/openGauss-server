/*
################################################################################
# TCASE NAME : skew_setup.py 
# COMPONENT(S)  : 倾斜优化功能测试的数据集，用于：skew hint功能测试、基于skew hint的hashagg优化、基于skew hint的join优化、outer join补空的hash agg倾斜优化
# PREREQUISITE  : 
# PLATFORM      : all
# DESCRIPTION   : skew optimize test cases data
# TAG           : hint
# TC LEVEL      : Level 1
################################################################################
*/


--I1.创建schema skew_hint
create schema skew_hint;
set current_schema = skew_hint;

--S1.建立数据源表
create table src(a int) with(autovacuum_enabled = off);
insert into src values(1);
create table t1(a int);
insert into t1 select generate_series(1,199) from src;

--S2.建立倾斜表，并插入数据
create table skew_t1(a integer, b int, c int)with(autovacuum_enabled = off) distribute by hash (c);
insert into skew_t1 select generate_series(1,5), generate_series(1,10), generate_series(1,100) from src;
insert into skew_t1 select 12, 12, generate_series(100,1000) from src;

create table skew_t2(a int, b int, c int)with(autovacuum_enabled = off) distribute by hash (c);
insert into skew_t2 select * from skew_t1;
insert into skew_t2 select 12, 12, generate_series(100,1000) from src;

create table skew_t3(a int, b int, c int)with(autovacuum_enabled = off) distribute by hash (c);
insert into skew_t3 select * from skew_t1;

create table skew_t4(a int, b int, c int, d int, e int, f int)with(autovacuum_enabled = off) distribute by hash (c);
insert into skew_t4 select generate_series(1,5), generate_series(1,10), generate_series(1,50), generate_series(1,100) ,generate_series(1,500) from src;
insert into skew_t4 select 12, 12, generate_series(100,500), 25, 25 from src;
insert into skew_t4 select null, null, generate_series(500,1000), 25, 25 from src;
insert into skew_t4 select null, null, generate_series(500,1000), 25, 25,25 from src;

create table skew_t5 (c0 int, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int) with(orientation = column, autovacuum_enabled = off);
insert into skew_t5 values(generate_series(1, 1000));
insert into skew_t5 values(generate_series(1, 2000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000));


--S3.用于类型测试的表
create table typetest
( 
   col_tinyint tinyint
  ,col_nvarchar2 nvarchar2(60)
  ,col_interval interval
  ,col_smallint smallint null
  ,col_integer  integer default 23423
  ,col_bigint   bigint default 923423432
  ,col_real   real
  ,col_numeric  numeric(18,12) null
  ,col_numeric2 numeric null
  ,col_double_precision double precision
  ,col_decimal      decimal(19) default 923423423
  ,col_char     char(57) null
  ,col_char2    char default '0'
  ,col_varchar    varchar(19)
  ,col_text text  null
  ,col_varchar2   varchar2(25)
  ,col_time_without_time_zone   time without time zone null
  ,col_time_with_time_zone      time with time zone
  ,col_timestamp_without_timezone timestamp
  ,col_timestamp_with_timezone    timestamptz
  ,col_smalldatetime  smalldatetime
  ,col_money      money
  ,col_date date
) with (orientation=column, autovacuum_enabled = off) distribute by hash(col_integer);

insert into typetest select 1, 'aaaaaaaaaaaaaaaaaa', '@ 1 minute', 2, i.a, 111111, i.a + 10.001, 561.322815379585 + i.a, 1112 + i.a * 3, 8885.169 - i.a * 0.125, 61032419811910588 + i.a, 'test_agg_'||i.a, 'F', 'vector_agg_'||i.a, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i.a, 'beijing_agg'||i.a, '08:20:12', '06:26:42+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-02-02 03:04', 56 + i.a, '2005-02-14' from t1 i;

--I3.建立个各类型表
--S1. CHAROID
create table char_t(a integer, c char)with(autovacuum_enabled = off) distribute by hash(c);
insert into char_t select 1, 'A' from t1 i;

--S2. BPCHAROID
create table bpchar_t(a integer, c char(57))with(autovacuum_enabled = off) distribute by hash(c);
insert into bpchar_t select 1, 'bpchar_'||i.a from t1 i;

--S3. VARCHAROID
create table varchar_t(a integer, c varchar(20))with(autovacuum_enabled = off) distribute by hash(c);
insert into varchar_t select 1, 'varchar_'||i.a from t1 i;

--S4. NVARCHAR2OID
create table nvarchar2_t(a integer, c nvarchar2(20)) distribute by hash(c);
insert into nvarchar2_t select 1, 'bbbbbbbbbbbbbbbbb' from t1 i;

--S5. DATEOID
create table date_t(a integer, c date)with(autovacuum_enabled = off) distribute by hash(c);
insert into date_t select 1, '2005-02-14' from t1 i;

--S6. TIMEOID
create table time_t(a integer, c time without time zone null)with(autovacuum_enabled = off) distribute by hash(c);
insert into time_t select 1, '08:20:12' from t1 i;

--S7. TIMESTAMPOID
create table timestamp_t(a integer, c timestamp)with(autovacuum_enabled = off) distribute by hash(c);
insert into timestamp_t select 1, 'Mon Feb 10 17:32:01.4 1997 PST' from t1 i;

--S8. TIMESTAMPTZOID
create table timestampz_t(a integer, c timestamptz)with(autovacuum_enabled = off) distribute by hash(c);
insert into timestampz_t select 1, '1971-03-23 11:14:05' from t1 i;

--S9. INTERVALOID
create table interval_t(a integer, c interval)with(autovacuum_enabled = off) distribute by hash(c);
insert into interval_t select 1, '@ 1 minute' from t1 i;

--S10. TIMETZOID
create table timez_t(a integer, c time with time zone)with(autovacuum_enabled = off) distribute by hash(c);
insert into timez_t select 1, '06:26:42+08' from t1 i;

--S11. SMALLDATETIMEOID
create table smalldatetime_t(a integer, c smalldatetime)with(autovacuum_enabled = off) distribute by hash(c);
insert into smalldatetime_t select 1, '1997-02-02 03:04' from t1 i;

--S12. TEXTOID
create table text_t(a integer, c text)with(autovacuum_enabled = off) distribute by hash(c);
insert into text_t select 1, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i.a from t1 i;

--S13. NULL值
create table null_t(a integer, c int null)with(autovacuum_enabled = off)distribute by hash(a);
insert into null_t select generate_series(1, 10), null from t1 i;

--I4.用于agg下层outr join补空优化
create table agg_null_skew(a int, b int) with(autovacuum_enabled = off) distribute by hash(a);
insert into agg_null_skew values(generate_series(5,10),generate_series(1,5));

--I5.创建schema hint
create schema hint;
set current_schema = hint;
--S1.建立数据源表
create table src(a int) with(autovacuum_enabled = off);
insert into src values(1);

--S2.建立倾斜表，并插入数据
create table hint_t1(a int, b int, c int) with(autovacuum_enabled = off) distribute by hash (a);
insert into hint_t1 select generate_series(1, 2000), generate_series(1, 1000), generate_series(1, 500) from src;

create table hint_t2(a int, b int, c int) with(autovacuum_enabled = off) distribute by hash (a);
insert into hint_t2 select generate_series(1, 1000), generate_series(1, 500), generate_series(1, 100) from src;

create table skew_t1(a int, b int, c int) with(autovacuum_enabled = off) distribute by hash (c);
insert into skew_t1 select generate_series(1,5), generate_series(1,10), generate_series(1,100) from src;
