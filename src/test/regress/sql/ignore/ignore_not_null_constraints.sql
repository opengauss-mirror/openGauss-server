-- test for insert/update ignore.
create database sql_ignore_not_null_test dbcompatibility 'B';
\c sql_ignore_not_null_test;
drop table if exists t_ignore;
create table t_ignore(col1 int, col2 int not null, col3 varchar not null);

-- sqlbypass
set enable_opfusion = on;
-- test for condition without ignore
insert into t_ignore values(1, 1, 'abcdef');
insert into t_ignore values(1, null, null);
insert into t_ignore values(1, null, 'abcdef');
insert into t_ignore values(1, 1, null);
insert into t_ignore values(1, null, null);
select * from t_ignore;
update t_ignore set col2 = null, col3 = null;
select * from t_ignore;
-- test for condition with ignore
insert /*+ ignore_error */ into t_ignore values(1, 1, 'abcdef');
explain(costs off) insert /*+ ignore_error */ into t_ignore values(1, null, null);
insert /*+ ignore_error */ into t_ignore values(1, null, null);
insert /*+ ignore_error */ into t_ignore values(1, null, 'abcdef');
insert /*+ ignore_error */ into t_ignore values(1, 1, null);
insert /*+ ignore_error */ into t_ignore values(1, null, null);
select * from t_ignore;
update /*+ ignore_error */ t_ignore set col2 = null, col3 = null;
select * from t_ignore;

-- insert ignore from other tables with null
drop table if exists t_from;
create table t_from (col1 int, col2 int, col3 varchar);
insert into t_from values(9,9,'row from others');
insert into t_from values(1, null, null);
insert /*+ ignore_error */ into t_ignore select * from t_from;
select * from t_ignore;

-- test for null replacement
set sql_ignore_strategy = 'overwrite_null';
explain(costs off) insert /*+ ignore_error */ into t_ignore values(1, null, null);
insert /*+ ignore_error */ into t_ignore values(1, null, null);
select * from t_ignore;
set sql_ignore_strategy = 'ignore_null';

-- no bypass
set enable_opfusion = off;
-- test for condition without ignore
insert into t_ignore values(1, 1, 'abcdef');
insert into t_ignore values(1, null, null);
insert into t_ignore values(1, null, 'abcdef');
insert into t_ignore values(1, 1, null);
insert into t_ignore values(1, null, null);
select * from t_ignore;
update t_ignore set col2 = null, col3 = null;
select * from t_ignore;
-- test for condition with ignore
insert /*+ ignore_error */ into t_ignore values(1, 1, 'abcdef');
explain(costs off) insert /*+ ignore_error */ into t_ignore values(1, null, null);
insert /*+ ignore_error */ into t_ignore values(1, null, null);
insert /*+ ignore_error */ into t_ignore values(1, null, 'abcdef');
insert /*+ ignore_error */ into t_ignore values(1, 1, null);
insert /*+ ignore_error */ into t_ignore values(1, null, null);
select * from t_ignore;
update t_ignore set col2 = null, col3 = null;
select * from t_ignore;

-- insert ignore from other tables with null
insert /*+ ignore_error */ into t_ignore select * from t_from;
select * from t_ignore;

-- test for null replacement
set sql_ignore_strategy = 'overwrite_null';
explain(costs off) insert /*+ ignore_error */ into t_ignore values(1, null, null);
insert /*+ ignore_error */ into t_ignore values(1, null, null);
select * from t_ignore;
set sql_ignore_strategy = 'ignore_null';

-- test for overwriting null into "zero" of specific type
set sql_ignore_strategy = 'overwrite_null';

-- timestamp
create table t_timestamp(c timestamp not null);
insert /*+ ignore_error */ into t_timestamp values (null);
select * from t_timestamp;
insert into t_timestamp values('2022-06-01 17:26:59.846448');
update /*+ ignore_error */ t_timestamp set c = null;
select * from t_timestamp;

-- timetz
create table t_timetz(c timetz not null);
set timezone to 'PRC';
insert /*+ ignore_error */ into t_timetz values (null);
select * from t_timetz;
insert into t_timetz values('00:00:01+08');
update /*+ ignore_error */ t_timetz set c = null;
select * from t_timetz;
reset timezone;
show timezone;

-- time
create table t_time(c time not null);
insert /*+ ignore_error */ into t_time values (null);
select * from t_time;
insert into t_time values('00:00:00');
update /*+ ignore_error */ t_time set c = null;
select * from t_time;

-- interval
create table t_interval(c interval not null);
insert /*+ ignore_error */ into t_interval values (null);
select * from t_interval;
insert into t_interval values('00:00:01');
update /*+ ignore_error */ t_interval set c = null;
select * from t_interval;

-- tinterval
create table t_tinterval(c tinterval not null);
insert /*+ ignore_error */ into t_tinterval values (null);
select * from t_tinterval;
update /*+ ignore_error */ t_tinterval set c = null;
select * from t_tinterval;

-- smalldatetime
create table t_smalldatetime(c smalldatetime not null);
insert /*+ ignore_error */ into t_smalldatetime values (null);
select * from t_smalldatetime;
insert into t_smalldatetime values('1991-01-01 08:00:00');
update /*+ ignore_error */ t_smalldatetime set c = null;
select * from t_smalldatetime;

-- date
create table t_date(c date not null);
insert /*+ ignore_error */ into t_date values (null);
select * from t_date;
insert into t_date values('1991-01-01 08:00:00');
update /*+ ignore_error */ t_date set c = null;
select * from t_date;

-- uuid
create table t_uuid(c uuid not null);
insert /*+ ignore_error */ into t_uuid values (null);
select * from t_uuid;
insert into t_uuid values('aaaaaaaa-0000-0000-0000-000000000000');
update /*+ ignore_error */ t_uuid set c = null;
select * from t_uuid;

-- name
create table t_name(c name not null);
insert /*+ ignore_error */ into t_name values (null);
select * from t_name;
insert into t_name values('abc');
update /*+ ignore_error */ t_name set c = null;
select * from t_name;

-- point
create table t_point(c point not null);
insert /*+ ignore_error */ into t_point values (null);
select * from t_point;
insert into t_point values('(1,1)');
update /*+ ignore_error */ t_point set c = null;
select * from t_point;

-- path
create table t_path(c path not null);
insert /*+ ignore_error */ into t_path values (null);
select * from t_path;
insert into t_path values('((1,1))');
update /*+ ignore_error */ t_path set c = null;
select * from t_path;

-- polygon
create table t_polygon(c polygon not null);
insert /*+ ignore_error */ into t_polygon values (null);
select * from t_polygon;
insert into t_polygon values('((1,1))');
update /*+ ignore_error */ t_polygon set c = null;
select * from t_polygon;

-- circle
create table t_circle(c circle not null);
insert /*+ ignore_error */ into t_circle values (null);
select * from t_circle;
insert into t_circle values('<(1,1),1>');
update /*+ ignore_error */ t_circle set c = null;
select * from t_circle;

-- box
create table t_box(c box not null);
insert /*+ ignore_error */ into t_box values (null);
select * from t_box;
insert into t_box values('(1,1),(2,2)');
update /*+ ignore_error */ t_box set c = null;
select * from t_box;

-- json
create table t_json(c json not null);
insert /*+ ignore_error */ into t_json values (null);
select * from t_json;
insert into t_json values('111');
update /*+ ignore_error */ t_json set c = null;
select * from t_json;
select * from t_json where c::text = 'null';

-- jsonb
create table t_jsonb(c jsonb not null);
insert /*+ ignore_error */ into t_jsonb values (null);
select * from t_jsonb;
insert into t_jsonb values('111');
update /*+ ignore_error */ t_jsonb set c = null;
select * from t_jsonb;
select * from t_jsonb where c::text = 'null';

-- bit
create table t_bit(c bit not null);
insert /*+ ignore_error */ into t_bit values (null);
select * from t_bit;
insert into t_bit values('1');
update /*+ ignore_error */ t_bit set c = null;
select * from t_bit;

-- tinyint
create table t_tinyint(c tinyint not null);
insert /*+ ignore_error */ into t_tinyint values (null);
select * from t_tinyint;
insert into t_tinyint values('10');
update /*+ ignore_error */ t_tinyint set c = null;
select * from t_tinyint;

-- smallint
create table t_smallint(c smallint not null);
insert /*+ ignore_error */ into t_smallint values (null);
select * from t_smallint;
insert into t_smallint values('123');
update /*+ ignore_error */ t_smallint set c = null;
select * from t_smallint;


-- int
create table t_int(c int not null);
insert /*+ ignore_error */ into t_int values (null);
select * from t_int;
insert into t_int values(9999);
update /*+ ignore_error */ t_int set c = null;
select * from t_int;

-- bigint
create table t_bigint(c bigint not null);
insert /*+ ignore_error */ into t_bigint values (null);
select * from t_bigint;
insert into t_bigint values(9999999999999999);
update /*+ ignore_error */ t_bigint set c = null;
select * from t_bigint;

-- float
create table t_float(c float not null);
insert /*+ ignore_error */ into t_float values (null);
select * from t_float;
insert into t_float values(123.99);
update /*+ ignore_error */ t_float set c = null;
select * from t_float;

-- float8
create table t_float8(c float8 not null);
insert /*+ ignore_error */ into t_float8 values (null);
select * from t_float8;
insert into t_float8 values(123.99);
update /*+ ignore_error */ t_float8 set c = null;
select * from t_float8;

-- numeric
create table t_numeric(c numeric not null);
insert /*+ ignore_error */ into t_numeric values (null);
select * from t_numeric;
insert into t_numeric values(123.99);
update /*+ ignore_error */ t_numeric set c = null;
select * from t_numeric;

-- serial
create table t_serial(c serial not null);
insert /*+ ignore_error */ into t_serial values (null);
select * from t_serial;
insert into t_serial values(123);
update /*+ ignore_error */ t_serial set c = null;
select * from t_serial;

-- bool
create table t_bool(c bool not null);
insert /*+ ignore_error */ into t_bool values (null);
select * from t_bool;
insert into t_bool values(true);
update /*+ ignore_error */ t_bool set c = null;
select * from t_bool;

-- char(n)
create table t_charn(c char(6) not null);
insert /*+ ignore_error */ into t_charn values (null);
select * from t_charn;
insert into t_charn values('abc');
update /*+ ignore_error */ t_charn set c = null;
select * from t_charn;

-- varchar(n)
create table t_varcharn(c varchar(6) not null);
insert /*+ ignore_error */ into t_varcharn values (null);
select * from t_varcharn;
insert into t_varcharn values('xxxxxx');
update /*+ ignore_error */ t_varcharn set c = null;
select * from t_varcharn;

-- text
create table t_text(c text not null);
insert /*+ ignore_error */ into t_text values (null);
select * from t_text;
insert into t_text values('xxxxxx');
update /*+ ignore_error */ t_text set c = null;
select * from t_text;

-- mixture
drop table if exists t_mix;
create table t_mix
(
    c1 int,
    c2 bigint     not null,
    c3 varchar(6) not null,
    c4 bool       not null
);
insert /*+ ignore_error */ into t_mix values(1, null, null, null);
insert /*+ ignore_error */ into t_mix values(2, 2, null, null);
select * from t_mix;
insert into t_mix values(2, 2, 'abced', true);
update /*+ ignore_error */ t_mix set c1 = 9, c2 = null, c3 = null, c4 = null;
select * from t_mix;
drop table if exists t_mix;

-- test for mixture of not null constraints and check constraints
create table t_mix(num int not null check(num > 5));
set sql_ignore_strategy = 'overwrite_null';
set enable_opfusion = on;
insert /*+ ignore_error */ into t_mix values(null);
select * from t_mix;
set enable_opfusion = off;
insert /*+ ignore_error */ into t_mix values(null);
select * from t_mix;
drop table if exists t_mix;

create table t_mix(num int not null check(num > -5));
set sql_ignore_strategy = 'overwrite_null';
set enable_opfusion = on;
insert /*+ ignore_error */ into t_mix values(null);
select * from t_mix;
set enable_opfusion = off;
insert /*+ ignore_error */ into t_mix values(null);
select * from t_mix;
drop table if exists t_mix;

create table t_mix(content text not null check(length(content) > 5));
set sql_ignore_strategy = 'overwrite_null';
set enable_opfusion = on;
insert /*+ ignore_error */ into t_mix values(null);
select * from t_mix;
set enable_opfusion = off;
insert /*+ ignore_error */ into t_mix values(null);
select * from t_mix;
drop table if exists t_mix;

-- test for partition table with not null constraint
-- opfusion: on
set enable_opfusion = on;
set enable_partition_opfusion = on;
drop table if exists t_not_null_key_partition;
CREATE TABLE t_not_null_key_partition
(
    num     integer NOT NULL,
    ca_city character varying(60)
) PARTITION BY RANGE (num)
(
    PARTITION P1 VALUES LESS THAN(5000),
    PARTITION P2 VALUES LESS THAN(10000),
    PARTITION P3 VALUES LESS THAN(15000),
    PARTITION P4 VALUES LESS THAN(20000),
    PARTITION P5 VALUES LESS THAN(25000),
    PARTITION P6 VALUES LESS THAN(30000),
    PARTITION P7 VALUES LESS THAN(40000)
);
insert into t_not_null_key_partition values(1, 'shenzhen');
select * from t_not_null_key_partition;
set sql_ignore_strategy = 'ignore_null';
explain(costs off) insert /*+ ignore_error */ into  t_not_null_key_partition values (null);
insert /*+ ignore_error */ into  t_not_null_key_partition values (null);
select * from t_not_null_key_partition;
update /*+ ignore_error */ t_not_null_key_partition set num = null;
select * from t_not_null_key_partition;
-- opfusion: off
set enable_opfusion = off;
set enable_partition_opfusion = off;
set sql_ignore_strategy = 'overwrite_null';
insert /*+ ignore_error */ into  t_not_null_key_partition values (null);
select * from t_not_null_key_partition;
update /*+ ignore_error */ t_not_null_key_partition set num = null;
select * from t_not_null_key_partition;

-- test for subpartition table
drop table if exists ignore_range_range;
CREATE TABLE ignore_range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  int NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
    PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201901' )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( -5 ),
    SUBPARTITION p_201901_b VALUES LESS THAN( 1 )
  ),
  PARTITION p_201902 VALUES LESS THAN( '201902' )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( -5 ),
    SUBPARTITION p_201902_b VALUES LESS THAN( 1 )
  )
);
-- opfusion: on
set enable_opfusion = on;
set enable_partition_opfusion = on;

-- sql_ignore_strategy: ignore_null
set sql_ignore_strategy = 'ignore_null';
insert /*+ ignore_error */  into ignore_range_range values('201901', null, '1', 1);
select * from ignore_range_range;
insert into ignore_range_range values('201901', -3, '1', 1);
update /*+ ignore_error */ ignore_range_range set dept_code = null where dept_code = -3;
select * from ignore_range_range;

-- sql_ignore_strategy: overwrite_null
delete from ignore_range_range;
set sql_ignore_strategy = 'overwrite_null';
insert /*+ ignore_error */  into ignore_range_range values('201901', null, '1', 1);
select * from ignore_range_range;
insert into ignore_range_range values('201901', -3, '1', 1);
update /*+ ignore_error */ ignore_range_range set dept_code = null where dept_code = -3;
select * from ignore_range_range;

-- opfusion: off
set enable_opfusion = off;
set enable_partition_opfusion = off;

-- sql_ignore_strategy: ignore_null
set sql_ignore_strategy = 'ignore_null';
delete from ignore_range_range;
insert /*+ ignore_error */  into ignore_range_range values('201901', null, '1', 1);
select * from ignore_range_range;
insert into ignore_range_range values('201901', -3, '1', 1);
update /*+ ignore_error */ ignore_range_range set dept_code = null where dept_code = -3;
select * from ignore_range_range;

-- sql_ignore_strategy: overwrite_null
set sql_ignore_strategy = 'overwrite_null';
delete from ignore_range_range;
insert /*+ ignore_error */  into ignore_range_range values('201901', null, '1', 1);
select * from ignore_range_range;
insert into ignore_range_range values('201901', -3, '1', 1);
update /*+ ignore_error */ ignore_range_range set dept_code = null where dept_code = -3;
select * from ignore_range_range;
delete from ignore_range_range;

-- test for ustore table
drop table if exists t_ignore;
create table t_ignore(num int not null) with (storage_type = ustore);

-- opfusion: on
set enable_opfusion = on;
set sql_ignore_strategy = 'ignore_null';
explain(costs off) insert /*+ ignore_error */ into t_ignore values(null);
insert /*+ ignore_error */ into t_ignore values(null);
select * from t_ignore;
insert into t_ignore values(1);
update /*+ ignore_error */ t_ignore set num = null where num = 1;
select * from t_ignore;

delete from t_ignore;
set sql_ignore_strategy = 'overwrite_null';
insert /*+ ignore_error */ into t_ignore values(null);
select * from t_ignore;
insert into t_ignore values(1);
update /*+ ignore_error */ t_ignore set num = null where num = 1;
select * from t_ignore;

-- opfusion: off
delete from t_ignore;
set enable_opfusion = off;
set sql_ignore_strategy = 'ignore_null';
explain(costs off) insert /*+ ignore_error */ into t_ignore values(null);
insert /*+ ignore_error */ into t_ignore values(null);
select * from t_ignore;
insert into t_ignore values(1);
update /*+ ignore_error */ t_ignore set num = null where num = 1;
select * from t_ignore;

delete from t_ignore;
set sql_ignore_strategy = 'overwrite_null';
insert /*+ ignore_error */ into t_ignore values(null);
select * from t_ignore;
insert into t_ignore values(1);
update /*+ ignore_error */ t_ignore set num = null where num = 1;
select * from t_ignore;

-- test for segment table
drop table if exists t_ignore;
create table t_ignore(num int not null) with (segment = on);

-- test for segment table, opfusion: on
set enable_opfusion = on;
set sql_ignore_strategy = 'ignore_null';
explain(costs off) insert /*+ ignore_error */ into t_ignore values(null);
insert /*+ ignore_error */ into t_ignore values(null);
select * from t_ignore;
insert into t_ignore values(1);
update /*+ ignore_error */ t_ignore set num = null where num = 1;
select * from t_ignore;

set sql_ignore_strategy = 'overwrite_null';
delete from t_ignore;
insert /*+ ignore_error */ into t_ignore values(null);
select * from t_ignore;
insert into t_ignore values(1);
update /*+ ignore_error */ t_ignore set num = null where num = 1;
select * from t_ignore;

-- test for segment table, opfusion: off
set enable_opfusion = off;
set sql_ignore_strategy = 'ignore_null';
delete from t_ignore;
explain(costs off) insert /*+ ignore_error */ into t_ignore values(null);
insert /*+ ignore_error */ into t_ignore values(null);
select * from t_ignore;
insert into t_ignore values(1);
update /*+ ignore_error */ t_ignore set num = null where num = 1;
select * from t_ignore;

set sql_ignore_strategy = 'overwrite_null';
delete from t_ignore;
insert /*+ ignore_error */ into t_ignore values(null);
select * from t_ignore;
insert into t_ignore values(1);
update /*+ ignore_error */ t_ignore set num = null where num = 1;
select * from t_ignore;

-- restore context
\c postgres
drop database if exists sql_ignore_not_null_test;