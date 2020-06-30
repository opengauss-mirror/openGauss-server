--
---- check maxvalue for range partition
--
--/* int2 */: sucess
create table test_maxvalue_datatype_int2(a int2)
partition by range(a)
(
	partition test_maxvalue_datatype_int2_p1 values less than (1),
	partition test_maxvalue_datatype_int2_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_int2') order by 1, 2, 3;
--/* int4 */: sucess
create table test_maxvalue_datatype_int4(a int)
partition by range(a)
(
	partition test_maxvalue_datatype_int4_p1 values less than (1),
	partition test_maxvalue_datatype_int4_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_int4') order by 1, 2, 3;
--/* int8 */: sucess
create table test_maxvalue_datatype_int8(a int8)
partition by range(a)
(
	partition test_maxvalue_datatype_int8_p1 values less than (1),
	partition test_maxvalue_datatype_int8_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_int8') order by 1, 2, 3;
--/* decimal */: sucess
create table test_maxvalue_datatype_decimal(a decimal)
partition by range(a)
(
	partition test_maxvalue_datatype_decimal_p1 values less than (7.1),
	partition test_maxvalue_datatype_decimal_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_decimal') order by 1, 2, 3;
--/* numeric */: sucess
create table test_maxvalue_datatype_numeric(a numeric(9,2))
partition by range(a)
(
	partition test_maxvalue_datatype_numeric_p1 values less than (7.1),
	partition test_maxvalue_datatype_numeric_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_numeric') order by 1, 2, 3;
--/* real */: sucess
create table test_maxvalue_datatype_real(a REAL)
partition by range(a)
(
	partition test_maxvalue_datatype_real_p1 values less than (7.1),
	partition test_maxvalue_datatype_real_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_real') order by 1, 2, 3;
--/* double precision */: sucess
create table test_maxvalue_datatype_doubleprecision(a double precision)
partition by range(a)
(
	partition test_maxvalue_datatype_doubleprecision_p1 values less than (7.1),
	partition test_maxvalue_datatype_doubleprecision_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_doubleprecision') order by 1, 2, 3;
--/* char */: sucess
create table test_maxvalue_datatype_char(a char)
partition by range(a)
(
	partition test_maxvalue_datatype_char_p1 values less than ('A'),
	partition test_maxvalue_datatype_char_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_char') order by 1, 2, 3;
--/* varchar(n) */: sucess
create table test_maxvalue_datatype_varcharn(a varchar(3))
partition by range(a)
(
	partition test_maxvalue_datatype_varcharn_p1 values less than ('A'),
	partition test_maxvalue_datatype_varcharn_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_varcharn') order by 1, 2, 3;
--/* varchar */: sucess
create table test_maxvalue_datatype_varchar(a varchar)
partition by range(a)
(
	partition test_maxvalue_datatype_varchar_p1 values less than ('A'),
	partition test_maxvalue_datatype_varchar_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_varchar') order by 1, 2, 3;
--/* char(n) */: sucess
create table test_maxvalue_datatype_charn(a char(3))
partition by range(a)
(
	partition test_maxvalue_datatype_charn_p1 values less than ('A'),
	partition test_maxvalue_datatype_charn_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_charn') order by 1, 2, 3;
--/* character(n) */: sucess
create table test_maxvalue_datatype_charactern(a character(3))
partition by range(a)
(
	partition test_maxvalue_datatype_charactern_p1 values less than ('A'),
	partition test_maxvalue_datatype_charactern_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_charactern') order by 1, 2, 3;
--/* text */: sucess
create table test_maxvalue_datatype_text(a text)
partition by range(a)
(
	partition test_maxvalue_datatype_text_p1 values less than ('A'),
	partition test_maxvalue_datatype_text_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_text') order by 1, 2, 3;
--/* nvarchar2 */: sucess
create table test_maxvalue_datatype_nvarchar2(a nvarchar2)
partition by range(a)
(
	partition test_maxvalue_datatype_nvarchar2_p1 values less than ('A'),
	partition test_maxvalue_datatype_nvarchar2_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_nvarchar2') order by 1, 2, 3;
--/* date */: sucess
create table test_maxvalue_datatype_date(a date)
partition by range(a)
(
	partition test_maxvalue_datatype_date_p1 values less than (to_date('2012-11-12','YYYY-MM-DD')),
	partition test_maxvalue_datatype_date_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_date') order by 1, 2, 3;
--/* timestamp */: sucess
create table test_maxvalue_datatype_timestamp(a timestamp)
partition by range(a)
(
	partition test_maxvalue_datatype_timestamp_p1 values less than (to_timestamp('2012-11-12','yyyy-mm-dd')),
	partition test_maxvalue_datatype_timestamp_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_timestamp') order by 1, 2, 3;
--/* timestamptz */: sucess
create table test_maxvalue_datatype_timestamptz(a timestamptz)
partition by range(a)
(
	partition test_maxvalue_datatype_timestamptz_p1 values less than (to_timestamp('2012-11-12','yyyy-mm-dd')),
	partition test_maxvalue_datatype_timestamptz_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_timestamptz') order by 1, 2, 3;
--/* name */: sucess
create table test_maxvalue_datatype_name(a name)
partition by range(a)
(
	partition test_maxvalue_datatype_name_p1 values less than ('CBY'),
	partition test_maxvalue_datatype_name_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_name') order by 1, 2, 3;
--/* bpchar */: sucess 
create table test_maxvalue_datatype_bpchar(a bpchar)
partition by range(a)
(
	partition test_maxvalue_datatype_bpchar_p1 values less than ('C'),
	partition test_maxvalue_datatype_bpchar_p2 values less than (maxvalue)
);
select c.relname, a.attname, t.typname from pg_class as c , pg_attribute as a , pg_type as t
where a.attrelid = c.oid and a.atttypid = t.oid and a.attnum > 0 and c.relname in('test_maxvalue_datatype_bpchar') order by 1, 2, 3;



--clean up
drop table test_maxvalue_datatype_bpchar;
drop table test_maxvalue_datatype_char;
drop table test_maxvalue_datatype_charactern;
drop table test_maxvalue_datatype_charn;
drop table test_maxvalue_datatype_doubleprecision;
drop table test_maxvalue_datatype_int4;
drop table test_maxvalue_datatype_int2;
drop table test_maxvalue_datatype_int8;
drop table test_maxvalue_datatype_name;
drop table test_maxvalue_datatype_numeric;
drop table test_maxvalue_datatype_nvarchar2;
drop table test_maxvalue_datatype_real;
drop table test_maxvalue_datatype_text;
drop table test_maxvalue_datatype_timestamp;
drop table test_maxvalue_datatype_varchar;
drop table test_maxvalue_datatype_varcharn;
drop table test_maxvalue_datatype_date;
drop table test_maxvalue_datatype_decimal;
drop table test_maxvalue_datatype_timestamptz;
