set enable_parse_fusion to on;

-- test ints
drop table if exists perf_pf_t;
create table perf_pf_t(
    c1 int,
    c2 tinyint,
    c3 smallint,
    c4 integer,
    c5 bigint,
    c6 binary_integer
);
insert into perf_pf_t values (32767, 127, 32000, 2147483647, 9223372036854775807, 100);
insert into perf_pf_t(c6, c5, c4, c3, c2, c1) values (50, 999999999, 100, 30000, 100, -200);
insert into perf_pf_t(c1) values (null);
insert into perf_pf_t(c1) values (2147483648); -- should error
insert into perf_pf_t(c2) values (300);  -- should error
select * from perf_pf_t;

-- test floats
drop table if exists perf_pf_t;
create table perf_pf_t(
    c1 numeric(5,2),
    c2 decimal(10,3),
    c3 dec,
    c4 double precision,
    c5 float8,
    c6 float
);
insert into perf_pf_t values (123.45, 99999.999, 3.14, 1.2345e100, -5.678e20, 0.5);
insert into perf_pf_t(c6, c5, c4, c3, c2, c1) values (0.99, 2.718e30, 9.876e50, 2.718, 12345.678, 99.99);
insert into perf_pf_t(c1) values (null);
insert into perf_pf_t(c1) values (100000.55);  -- should error
insert into perf_pf_t(c2) values ('invalid');  -- should error
select * from perf_pf_t;

-- test strings
drop table if exists perf_pf_t;
create table perf_pf_t(
    c1 char(30),
    c2 varchar(30),
    c3 varchar2(30),
    c4 nchar(30),
    c5 nvarchar(30),
    c6 text
);
insert into perf_pf_t values ('Hello', 'World', 'open', '测试', 'openGauss', 'verygood');
insert into perf_pf_t(c6, c5, c4, c3, c2, c1) values ('gooood', 'good', '数据', 'Example', 'Test', 'ABC');
insert into perf_pf_t(c1) values (null);
insert into perf_pf_t(c1) values ('This string is way too long for a char(30) field'); -- should error
insert into perf_pf_t(c2) values (decode('deadbeef', 'hex')); -- should error
select * from perf_pf_t;

-- test times
drop table if exists perf_pf_t;
create table perf_pf_t(
    c1 date,
    c2 time,
    c3 timestamp,
    c4 smalldatetime,
    c5 reltime,
    c6 abstime
);
insert into perf_pf_t values 
('2023-10-01', '12:34:56', '2023-10-01 12:34:56', '2023-10-01 12:35', '3 days', '2023-10-01 12:34:56');
insert into perf_pf_t(c6, c5, c4, c3, c2, c1) values 
('2024-01-01 00:00:00', '1 month', '2023-12-31 23:59', '2023-12-31 23:59:59', '23:59:59', '2023-12-31');
insert into perf_pf_t(c1) values (null);
insert into perf_pf_t(c1) values ('2023-13-01'); -- should error
insert into perf_pf_t(c2) values ('25:61:61'); -- should error
select * from perf_pf_t;

-- test intervals
drop table if exists perf_pf_t;
create table perf_pf_t(
    c1 interval year,
    c2 interval month,
    c3 interval day,
    c4 interval hour,
    c5 interval minute,
    c6 interval second
);
insert into perf_pf_t values 
('5 years', '12 months', '30 days', '23 hours', '59 minutes', '59 seconds');
insert into perf_pf_t(c6, c5, c4, c3, c2, c1) values 
('100 seconds', '60 minutes', '24 hours', '100 days', '13 months', '100 years');
insert into perf_pf_t(c1) values (null);
insert into perf_pf_t(c1) values ('1 year 13 months'); -- should error
insert into perf_pf_t(c5) values ('60 minutes'); -- should error
select * from perf_pf_t;

-- test other types
drop table if exists perf_pf_t;
create table perf_pf_t(
    c1 boolean,
    c2 money,
    c3 cidr,
    c4 inet,
    c5 macaddr,
    c6 uuid
);
insert into perf_pf_t values 
(true, 123.45, '192.168.1.0/24', '192.168.1.1', '08:00:2b:01:02:03', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11');
insert into perf_pf_t(c6, c5, c4, c3, c2, c1) values 
('550e8400-e29b-41d4-a716-446655440000', '00:1a:2b:3c:4d:5e', '10.0.0.1', '10.0.0.0/8', 987.65, false);
insert into perf_pf_t(c1) values (null);
insert into perf_pf_t(c1) values ('yes'); -- should error
insert into perf_pf_t(c3) values ('invalid_cidr'); -- should error
select * from perf_pf_t;

-- test unsatisfy with other clauses
drop table if exists perf_pf_t;
create table perf_pf_t(
    c1 int,
    c2 tinyint,
    c3 smallint
);
create unique index on perf_pf_t(c1);
insert into perf_pf_t values (12, 1, 12)
    on duplicate key update c2=values(c2), c3=values(c3);
select * from perf_pf_t;

-- test unsatisfy with complex columns
drop table if exists perf_pf_t;
drop type if exists compfoo cascade;
create type compfoo AS (f1 int, f2 text);
create table perf_pf_t(
    a int,
    b compfoo,
    c varbit(20)
);
insert into perf_pf_t(a, b) values(1,'(1,demo)');
insert into perf_pf_t(a, b) values(1,(1,'demo')); -- RowExpr, not const, original path
insert into perf_pf_t(a, b.f1, b.f2) values(1, 1, 'demo'); -- FieldStore, indirection, original path
insert into perf_pf_t(a, c) values (1, b'010101'); -- original path
select * from perf_pf_t;

-- test error
insert into perf_pf_t(a, not_exist) values(1, null);
insert into perf_pf_t(a, a) values(1, 1);
insert into perf_pf_t(a, b) values(2);
select * from perf_pf_t;

-- test error2
drop table if exists perf_pf_t;
create table perf_pf_t(
    a int,
    b int,
    c int
);
insert into perf_pf_t values(1); -- ok
insert into perf_pf_t values(1), (1, 2); -- should error
insert into perf_pf_t values(1, 2, 3, 4), (1, 2); -- should error
insert into perf_pf_t(a, b) values(1, 2, 3), (1, 2); -- should error
insert into perf_pf_t(a, b, c) values(1, 2); -- should error
insert into perf_pf_t(a, b, c) values(1, 2, 3, 4); -- should error
select * from perf_pf_t;

-- test error3
insert into pg_auth_history values(1, 2);
create materialized view perf_pf_v as select * from perf_pf_t;
insert into perf_pf_v values(1, 2, 3);
select * from perf_pf_v;
drop materialized view perf_pf_v;
create view perf_pf_vn as select * from perf_pf_t;
insert into perf_pf_vn values(1, 2, 3);
select * from perf_pf_vn;
drop view perf_pf_vn;
-- test error4
drop table if exists perf_pf_t;
create table perf_pf_t(
    a int,
    b int,
    c int
) with (orientation=column);
insert into perf_pf_t values (1, 2, 3);
insert /*+ ignore_error */ into perf_pf_t values (1, 2, 3); -- should error
select * from perf_pf_t;

-- test error4
alter table perf_pf_t set (append_mode=read_only);
drop table perf_pf_t;
alter table perf_pf_t set (append_mode=on);

-- test unmatch types
drop table if exists perf_pf_t;
create table perf_pf_t(
    c1 int,
    c2 tinyint,
    c3 smallint,
    c4 text
);
insert into perf_pf_t values('1', '2', '3', 4);
insert into perf_pf_t values (2, 258, 99999999, 'hello world');
insert /*+ ignore_error */ into perf_pf_t values (2, 258, 99999999, 'hello world');
select * from perf_pf_t;

-- test domain types
drop table if exists perf_pf_t cascade;
drop type if exists age cascade;
CREATE DOMAIN age AS INTEGER
    DEFAULT 18
    CHECK (VALUE >= 0 AND VALUE <= 120);
create table perf_pf_t(
    a age
);
insert into perf_pf_t values(18);
insert into perf_pf_t values(-18);
insert into perf_pf_t values(200);
select * from perf_pf_t;

-- test unsatisfy with hex
drop table if exists perf_pf_t;
create table perf_pf_t(
    c1 int,
    c2 text
);
insert into perf_pf_t values (1, '2'), (2, x'2a2a'); -- should ok
select * from perf_pf_t;

drop table if exists perf_pf_t cascade;
drop type if exists age cascade;
set enable_parse_fusion to off;
