create database decode_compatibility dbcompatibility 'A';
\c decode_compatibility

set timezone to '-08';
set sql_beta_feature = 'a_style_coerce';

drop table if exists tb_test;
create table tb_test(
    c_bool bool,
    c_int1 int1,
    c_int2 int2,
    c_int4 int4,
    c_int8 int8,
    c_float4 float4,
    c_float8 float8,
    c_numeric numeric,
    c_money money,
    c_char char(10),
    c_bpchar bpchar,
    c_varchar2 varchar2,
    c_nvarchar2 nvarchar2,
    c_text text,
    c_blank_text text,
    c_char2number_success text,
    c_raw raw,
    c_date date,
    c_time time without time zone,
    c_timetz time with time zone,
    c_timestamp timestamp without time zone,
    c_timestamptz timestamp with time zone,
    c_smalldatetime smalldatetime,
    c_interval interval,
    c_reltime reltime,
    c_abstime abstime
);

-- =========================================================
-- test1: implicit type conversion from defresult to result1
-- =========================================================
insert into tb_test values(
    't', 1, 2, 4, 8, 4.4, 8.8, 9.999, 66, 'char', 'bpchar', 'varchar2', 'nvarchar2', 'text', '   ', '7.77', '1234',
    date '12-10-2010', '21:21:21', '21:21:21 pst', '2010-12-12', '2013-12-11 pst', '2003-04-12 04:05:06',
    interval '2' year, '30 DAYS 12:00:00', abstime 'Mon May 1 00:30:30 1995'
);

-- convert to bool
select decode(1, 1, c_bool, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bool, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_bool, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bool, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to int1
select decode(1, 1, c_int1, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int1, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_int1, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int1, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to int2
select decode(1, 1, c_int2, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int2, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_int2, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int2, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to int4
select decode(1, 1, c_int4, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int4, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_int4, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int4, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to int8
select decode(1, 1, c_int8, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_int8, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_int8, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_int8, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to float4
select decode(1, 1, c_float4, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float4, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_float4, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float4, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to float8
select decode(1, 1, c_float8, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_float8, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_float8, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_float8, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to numeric
select decode(1, 1, c_numeric, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_numeric, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_numeric, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_numeric, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to money
select decode(1, 1, c_money, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_money, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_money, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_money, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to char
select decode(1, 1, c_char, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_char, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_char, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_char, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to bpchar
select decode(1, 1, c_bpchar, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_bpchar, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_bpchar, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_bpchar, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to varchar2
select decode(1, 1, c_varchar2, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_varchar2, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_varchar2, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_varchar2, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to nvarchar2
select decode(1, 1, c_nvarchar2, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_nvarchar2, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_nvarchar2, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_nvarchar2, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to text
select decode(1, 1, c_text, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_text, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_text, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_text, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to raw
select decode(1, 1, c_raw, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_raw, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_raw, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_raw, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to date
select decode(1, 1, c_date, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_date, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_date, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_date, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to time
select decode(1, 1, c_time, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_time, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_time, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_time, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to timetz
select decode(1, 1, c_timetz, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timetz, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_timetz, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timetz, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to timestamp
select decode(1, 1, c_timestamp, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamp, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_timestamp, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamp, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to timestamptz
select decode(1, 1, c_timestamptz, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_timestamptz, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_timestamptz, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_timestamptz, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to smalldatetime
select decode(1, 1, c_smalldatetime, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_smalldatetime, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_smalldatetime, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_smalldatetime, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to interval
select decode(1, 1, c_interval, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_interval, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_interval, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_reltime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_interval, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to reltime
select decode(1, 1, c_reltime, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_reltime, c_abstime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_reltime, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_reltime, c_abstime) as result, pg_typeof(result) from tb_test;
-- convert to abstime
select decode(1, 1, c_abstime, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 1, c_abstime, c_reltime) as result, pg_typeof(result) from tb_test;

select decode(1, 2, c_abstime, c_bool) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_int1) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_int2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_int4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_int8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_float4) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_float8) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_numeric) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_money) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_char) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_bpchar) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_varchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_nvarchar2) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_blank_text) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_char2number_success) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_raw) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_date) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_time) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_timetz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_timestamp) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_timestamptz) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_smalldatetime) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_interval) as result, pg_typeof(result) from tb_test;
select decode(1, 2, c_abstime, c_reltime) as result, pg_typeof(result) from tb_test;

-- ====================================================
-- test2: implicit type conversion from expr to search1
-- ====================================================

-- number comparison
delete from tb_test;
insert into tb_test values(
    1, 1, 1, 1, 1, 1.0, 1.0, 1.0, 1, '1', '1', '1', '1', '1', '   ', '1', '1',
    date '12-10-2010', '21:21:21', '21:21:21 pst', '2010-10-12', '2010-10-12 pst', '2010-10-12',
    interval '2' year, '2 year', abstime '2010-10-12'
);

select decode(c_int1, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_bool, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_int1, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_int2, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_int4, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_int8, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_float4, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_float8, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_numeric, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_money, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_raw, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_bool, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int1, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int2, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int4, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_int8, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float4, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_float8, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_numeric, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_money, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_char, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_raw, 'Conversion successfully!', 'Conversion failed!') from tb_test;

-- datetime comparison
delete from tb_test;
insert into tb_test values(
    1, 1, 1, 1, 1, 1.0, 1.0, 1.0, 1,
    '12-10-2010', '12-10-2010', '12-10-2010', '12-10-2010', '12-10-2010', '   ', '1', '1',
    date '12-10-2010', '21:21:21', '21:21:21 pst', '2010-10-12', '2010-10-12 pst', '2010-10-12',
    interval '2' year, '2 year', abstime '2010-10-12'
);

select decode(c_bpchar, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_date, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamp, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamptz, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_smalldatetime, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_abstime, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_date, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamp, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamptz, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_smalldatetime, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_abstime, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_date, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamp, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamptz, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_smalldatetime, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_abstime, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_date, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamp, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamptz, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_smalldatetime, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_abstime, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_date, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamp, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamptz, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_smalldatetime, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_abstime, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_date, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_date, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_date, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_date, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_date, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_date, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamp, c_date, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamptz, c_date, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_smalldatetime, c_date, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_abstime, c_date, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_timestamp, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_timestamp, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_timestamp, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_timestamp, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_timestamp, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_timestamp, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_date, c_timestamp, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamptz, c_timestamp, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_smalldatetime, c_timestamp, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_abstime, c_timestamp, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_timestamptz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_timestamptz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_timestamptz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_timestamptz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_timestamptz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_timestamptz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_date, c_timestamptz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamp, c_timestamptz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_smalldatetime, c_timestamptz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_abstime, c_timestamptz, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_smalldatetime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_smalldatetime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_smalldatetime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_smalldatetime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_smalldatetime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_smalldatetime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_date, c_smalldatetime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamp, c_smalldatetime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamptz, c_smalldatetime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_abstime, c_smalldatetime, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_abstime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_abstime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_abstime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_abstime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_abstime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_abstime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_date, c_abstime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamp, c_abstime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timestamptz, c_abstime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_smalldatetime, c_abstime, 'Conversion successfully!', 'Conversion failed!') from tb_test;

-- time comparison
delete from tb_test;
insert into tb_test values(
    1, 1, 1, 1, 1, 1.0, 1.0, 1.0, 1,
    '21:21:21', '21:21:21', '21:21:21', '21:21:21', '21:21:21', '   ', '1', '1',
    date '12-10-2010', '21:21:21', '21:21:21 pst', '2010-10-12', '2010-10-12 pst', '2010-10-12',
    interval '2' year, '2 year', abstime '2010-10-12'
);

select decode(c_time, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timetz, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_time, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timetz, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_time, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timetz, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_time, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timetz, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_time, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timetz, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_time, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_time, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_time, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_time, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_time, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_time, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_timetz, c_time, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_timetz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_timetz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_timetz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_timetz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_timetz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_timetz, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_time, c_timetz, 'Conversion successfully!', 'Conversion failed!') from tb_test;

-- interval comparison
delete from tb_test;
insert into tb_test values(
    1, 1, 1, 1, 1, 1.0, 1.0, 1.0, 1,
    '2 year', '2 year', '2 year', '2 year', '2 year', '   ', '1', '1',
    date '12-10-2010', '21:21:21', '21:21:21 pst', '2010-10-12', '2010-10-12 pst', '2010-10-12',
    interval '2' year, '2 year', abstime '2010-10-12'
);

select decode(c_interval, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_reltime, c_char, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_interval, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_reltime, c_bpchar, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_interval, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_reltime, c_varchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_interval, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_reltime, c_nvarchar2, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_interval, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_reltime, c_text, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_interval, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_interval, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_interval, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_interval, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_interval, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_interval, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_reltime, c_interval, 'Conversion successfully!', 'Conversion failed!') from tb_test;

select decode(c_char, c_reltime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_bpchar, c_reltime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_varchar2, c_reltime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_nvarchar2, c_reltime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_text, c_reltime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_blank_text, c_reltime, 'Conversion successfully!', 'Conversion failed!') from tb_test;
select decode(c_interval, c_reltime, 'Conversion successfully!', 'Conversion failed!') from tb_test;

----
-- testcase - fix o compatibility of a_style_coerce
----

-- 1. return type
set sql_beta_feature = 'a_style_coerce';
select pg_typeof(decode(1, 1, 1, '1'));
select pg_typeof(decode(1, 1, '1', 1));
select pg_typeof(case 1 when 1 then 1 else '1' end);
select pg_typeof(case 1 when 1 then '1' else 1 end);

set sql_beta_feature = 'none';
select pg_typeof(decode(1, 1, 1, '1'));
select pg_typeof(decode(1, 1, '1', 1));
select pg_typeof(case 1 when 1 then 1 else '1' end);
select pg_typeof(case 1 when 1 then '1' else 1 end);

-- 2. operator match
set sql_beta_feature = 'a_style_coerce';
select decode(1, '1.0', 'same', 'different');
select decode('1.0', 1, 'same', 'different');
select decode(1, '1.0'::text, 'same', 'different');
select decode('1.0'::text, 1, 'same', 'different');
select case 1 when '1.0' then 'same' else 'different' end;
select case '1.0' when 1 then 'same' else 'different' end;

set sql_beta_feature = 'none';
select decode(1, '1.0', 'same', 'different');
select decode('1.0', 1, 'same', 'different');
select decode(1, '1.0'::text, 'same', 'different');
select decode('1.0'::text, 1, 'same', 'different');
select case 1 when '1.0' then 'same' else 'different' end;
select case '1.0' when 1 then 'same' else 'different' end;

set sql_beta_feature = 'none';
create table base_tab_000 (
col_tinyint tinyint,
col_smallint smallint,
col_int integer,
col_bigint bigint,
col_numeric numeric,
col_real real,
col_double double precision,
col_decimal decimal,
col_varchar varchar,
col_char char(30),
col_nvarchar2 nvarchar2,
col_text text,
col_timestamptz timestamp with time zone,
col_timestamp timestamp without time zone,
col_date date,
col_time time without time zone,
col_timetz time with time zone,
col_interval interval,
col_smalldatetine smalldatetime)
partition by range (col_int)
(
partition vector_base_tab_000_1 values less than (10),
partition vector_base_tab_000_2 values less than (1357),
partition vector_base_tab_000_3 values less than (2687),
partition vector_base_tab_000_4 values less than (maxvalue)
);
create table base_type_tab_000 (
col_tinyint tinyint,
col_smallint smallint,
col_int integer,
col_bigint bigint,
col_money money,
col_numeric numeric,
col_real real,
col_double double precision,
col_decimal decimal,
col_varchar varchar,
col_char char(30),
col_nvarchar2 nvarchar2,
col_text text,
col_timestamp timestamp with time zone,
col_timestamptz timestamp without time zone,
col_date date,
col_time time without time zone,
col_timetz time with time zone,
col_interval interval,
col_tinterval tinterval,
col_smalldatetine smalldatetime,
col_bytea bytea,
col_boolean boolean,
col_inet inet,
col_cidr cidr,
col_bit bit(10),
col_varbit varbit(10),
col_oid oid) ;

CREATE TABLE Customer (
c_id int ,
c_d_id int ,
c_w_id int ,
c_first varchar(16) ,
c_middle char(2) ,
c_last varchar(16) ,
c_street_1 varchar(20) ,
c_street_2 varchar(20) ,
c_city varchar(20) ,
c_state char(2) ,
c_zip char(9) ,
c_phone char(16) ,
c_since timestamp ,
c_credit char(2) ,
c_credit_lim numeric(12,2) ,
c_discount numeric(4,4) ,
c_balance numeric(12,2) ,
c_ytd_payment numeric(12,2) ,
c_payment_cnt int ,
c_delivery_cnt int ,
c_data varchar(500))

partition by range (c_id)
(
partition vector_engine_Customer_1 values less than (10),
partition vector_engine_Customer_2 values less than (77),
partition vector_engine_Customer_3 values less than (337),
partition vector_engine_Customer_4 values less than (573),
partition vector_engine_Customer_5 values less than (1357),
partition vector_engine_Customer_6 values less than (2033),
partition vector_engine_Customer_7 values less than (2087),
partition vector_engine_Customer_8 values less than (2387),
partition vector_engine_Customer_9 values less than (2687),
partition vector_engine_Customer_10 values less than (2987),
partition vector_engine_Customer_11 values less than (maxvalue)
);

select decode(bitand(a.col_int, 1), 1, 'warehouse', 'postoffice') as case1,
decode(bitand(a.col_tinyint, 2), 2, 'ground', 'air') as case2,
decode(bitand(b.col_smallint, 4), 4, 'insured', 'certified') as case3,
decode(a.col_real, b.col_real, a.col_char, 'postoffice'),
decode(b.col_nvarchar2, a.col_nvarchar2, 'ground', 'air'),
decode(a.col_double, b.col_double, 'insured', 'certified'),
decode(b.col_text, a.col_text, 'yes', a.col_varchar, 'no', 'default'),
decode(a.col_char,
b.col_nvarchar2,
't',
cast('h' as char),
false,
true),
decode(cast('h' as char), a.col_nvarchar2, 't', b.col_char, 'f', 't'),
decode(to_date('2010-8-1', 'yyyy-mm-dd'),
' 2010-08-01 00:00:00',
1,
to_date('2010-8-1', 'yyyy-mm-dd'),
2,
3),
'print1:' || decode('myvar1', '', 1, 'myvar2', 2)
from base_tab_000 a
join base_type_tab_000 b
on a.col_smallint = b.col_smallint
and exists
(select 1 from Customer where a.col_int = c_id)
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

create table t1 (id int, c_int int, c_varchar varchar(50)) with (storage_type=ustore);

create or replace procedure p1 (tnum int, tname varchar) as
x int;
begin
for i in 1..tnum loop
  execute immediate 'insert into ' || tname || ' values (' || i || ',' || i || ',' || '''abc''' ||  ')';
end loop;
end;
/

call p1(100, 't1');

select count(*) from (select sin((select max(B.id)
from t1 B where B.id > 5))+ decode((select max(B.id) from t1 B where B.id > 5), 100000, 200, 1000000, 300) + decode ((select max (B.id)
from t1 B where B.id>5), 100000, 200, 1000000, 300) X
from t1 A) order by 1;

reset sql_beta_feature;
reset timezone;

\c regression
clean connection to all force for database decode_compatibility;
drop database decode_compatibility;
