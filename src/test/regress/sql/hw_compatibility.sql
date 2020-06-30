--test datatype date
create database td_db dbcompatibility 'C';
select datname, datcompatibility from pg_database where datname = 'td_db';
\c td_db
show sql_compatibility;
set datestyle = 'iso, ymd';
create table date_t1(a date);
insert into date_t1 values('19851012'), ('19861012'), ('20121213'),('20121231');
\d date_t1
select a from date_t1 order by 1;
select cast(a as char(10)) as result from date_t1 order by 1;
select cast(a as varchar(10)) as result from date_t1 order by 1;
select cast(a as text) as result from date_t1 order by 1;

create table date_t2(a int, b varchar(10), c char(10), d text);
insert into date_t2 values (1, '20121216', '20151216', '19851216');
select * from date_t2;
select a, cast(b as date), cast(c as date), cast(d as date) from date_t2;
drop table date_t1;
drop table date_t2;
reset datestyle;

--test function return empty string
select substr('123',4,1) is null;
select substr('123',4,1) = '';
select substr('123',4) is null;
select substr('123',4) = '';
select substr('123'::bytea,4,1) is null;
select substr('123'::bytea,4,1) = '';
select substr('123'::bytea,4) is null;
select substr('123'::bytea,4) = '';
select substrb('123',4,1) is null;
select substrb('123',4,1) = '';
select substrb('123',4) is null;
select substrb('123',4) = '';

-- null and whitespace convert to 0
CREATE TABLE INT_TBL(id int4, f1 int2, f2 int4, f3 int8, f4 numeric(12,4), f5 char, f6 varchar(10), f7 text);

INSERT INTO INT_TBL(id, f1, f2, f3, f4, f5, f6, f7) VALUES (0, 0, 0, 0, 0, '  ', '    ', '    ');

INSERT INTO INT_TBL(id, f1, f2, f3, f4, f5, f6, f7) VALUES (1, 0, 0, 0, 0, '', '', '');

SELECT * FROM INT_TBL order by 1;

SELECT * FROM INT_TBL WHERE f1=f5 AND f1=f6 AND f1=f7 order by 1;

SELECT * FROM INT_TBL WHERE f2=f6 AND f2=f5 AND f2=f7 order by 1;

SELECT * FROM INT_TBL WHERE f3=f6 AND f3=f5 AND f3=f7 order by 1;

SELECT * FROM INT_TBL WHERE f4=f6 AND f4=f5 AND f4=f7 order by 1;

CREATE TABLE TBL_EMPTY_STR(
A INT,
B VARCHAR,
C NVARCHAR2,
D TEXT,
E TSVECTOR,
F BPCHAR,
G RAW,
H OIDVECTOR,
I INT2VECTOR,
J BYTEA,
K CLOB,
L BLOB,
M NAME,
N "char"
);
--copy empty string
COPY TBL_EMPTY_STR FROM STDIN DELIMITER '|' NULL 'NULL';
|||||||||||||
|||||||||||||
\.

SELECT * FROM TBL_EMPTY_STR where b = '';
DROP TABLE TBL_EMPTY_STR;

-- testcase: empty string
CREATE TABLE alter_addcols_89 ( a int , b varchar(10) not null) WITH ( ORIENTATION = COLUMN);
INSERT INTO alter_addcols_89 VALUES(1, '');
ALTER TABLE alter_addcols_89 ALTER COLUMN b SET DATA TYPE varchar(5);
SELECT DISTINCT * FROM alter_addcols_89 ORDER BY a;
DROP TABLE alter_addcols_89;

--function to_date return date type
create table test_to_date as select to_date('2012-12-16 10:11:15') as col;
\d test_to_date;
drop table test_to_date;
create table test_to_date as select to_date('05 Dec 2000', 'DD Mon YYYY') as col;
\d test_to_date;
drop table test_to_date;

--function to_date/to_timestamp with multi blank space
select to_Date('2015 01   01','yyyy mm   dd');
select to_Date('2015   01 01','yyyy   mm   dd');
select to_timestamp('23  01  01','hh24  mi  ss');
select to_timestamp('20150101  232323','yyyymmdd  hh24miss');
select to_timestamp('20150101 232323','yyyymmdd          hh24miss');

create table tbl1(a int, b float8, c numeric(10, 0), d varchar(10), e char(5), f text, g date, h timestamp, i interval);
explain (verbose on, costs off) 
select coalesce(a, b), coalesce(a, c), coalesce(a, d), coalesce(c, d), coalesce(c, f) from tbl1;

explain (verbose on, costs off) select coalesce(a, c) from tbl1;
explain (verbose on, costs off) select coalesce(a, b, c, d, e, f) from tbl1;
explain (verbose on, costs off) select coalesce(a, '123') from tbl1;
explain (verbose on, costs off) select coalesce(c, '123') from tbl1;
explain (verbose on, costs off) select coalesce(d, '123') from tbl1;
explain (verbose on, costs off) select coalesce(e, '123') from tbl1;
explain (verbose on, costs off) select coalesce('abc', '123') from tbl1;

explain (verbose on, costs off) select case when 1 = 1 then a else b end from tbl1;
explain (verbose on, costs off) select case when 1 = 1 then a else c end from tbl1;
explain (verbose on, costs off) select case when 1 = 1 then a else d end from tbl1;
explain (verbose on, costs off) select case when 1 = 1 then c else f end from tbl1;
--ERROR
explain (verbose on, costs off) select coalesce(g, '123') from tbl1;
explain (verbose on, costs off) select coalesce(h, '123') from tbl1;
explain (verbose on, costs off) select coalesce(i, '123') from tbl1;
explain (verbose on, costs off) select coalesce(a, g) from tbl1;
explain (verbose on, costs off) select coalesce(a, h) from tbl1;
explain (verbose on, costs off) select coalesce(a, i) from tbl1;
explain (verbose on, costs off) select coalesce(d, g) from tbl1;
explain (verbose on, costs off) select coalesce(c, h) from tbl1;
explain (verbose on, costs off) select case when 1 = 1 then coalesce(c, d) else coalesce(a, f) end from tbl1;
drop table tbl1;

create table cast_impl_date_0011(val int ,t_tinyint tinyint, s_small SMALLINT,b_big bigint,f_float4  float4, f_float8 float8,
n_numeric numeric,c_char  char(20) ,c_bpchar bpchar(20),c_varchar varchar(10),c_nvarchar nvarchar2,t_text text);

insert into cast_impl_date_0011 values(1,2,12,89,21.2,0.12,0.22,'gauss','ap','hahh','xixi','dang');
analyze cast_impl_date_0011;
explain (verbose on, costs off)select coalesce(t_tinyint, c_char) from cast_impl_date_0011;
select coalesce(t_tinyint,c_char) from cast_impl_date_0011;

explain (verbose on, costs off)select coalesce(t_tinyint, c_varchar) from cast_impl_date_0011;
select coalesce(t_tinyint,c_varchar) from cast_impl_date_0011;

explain(verbose on, costs off)select coalesce(t_tinyint, c_nvarchar) from cast_impl_date_0011;
select coalesce(t_tinyint,c_nvarchar) from cast_impl_date_0011;

explain(verbose on, costs off) select coalesce(s_small, c_char) from cast_impl_date_0011;
select coalesce(s_small,c_char) from cast_impl_date_0011;

explain(verbose on, costs off)select coalesce(b_big, c_char) from cast_impl_date_0011;
select coalesce(b_big,c_char) from cast_impl_date_0011;

explain(verbose on, costs off)select coalesce(f_float4, c_char) from cast_impl_date_0011;
select coalesce(f_float4,c_char) from cast_impl_date_0011;

explain(verbose on, costs off) select coalesce(f_float8, c_char) from cast_impl_date_0011;
select coalesce(f_float8,c_char) from cast_impl_date_0011;

explain(verbose on, costs off)select coalesce(n_numeric, c_char) from cast_impl_date_0011;
select coalesce(n_numeric,c_char) from cast_impl_date_0011;

create table date_timestamp1(a date, b timestamp, c int);
create table date_timestamp2(a timestamp, b date, c int);
explain (verbose on, costs off)select t1.a from date_timestamp1 as t1, date_timestamp2 as t2 where t1.a = t2.a;
explain (verbose on, costs off)select t1.a from date_timestamp1 as t1, date_timestamp2 as t2 where t1.b = t2.b;
explain (verbose on, costs off)select t1.a from date_timestamp1 as t1, date_timestamp2 as t2 where t1.a = t2.a and t1.b = t2.b and t1.c = t2.c;
drop table date_timestamp1;
drop table date_timestamp2;

create database test dbcompatibility = '123';
/*SQL_COMPATIBILITY can not be seted*/
SET SQL_COMPATIBILITY = C;
\c regression

--add testcase for to_date compatibility test.
select to_Date('2016-01-01 24:00:00');
select to_Date('2016-01-01 24:00:00','yyyy-mm-dd hh24:mi:ss');
select to_Date('2016-01-01 00:60:00');
select to_Date('2016-01-01 00:60:00','yyyy-mm-dd hh24:mi:ss');
select to_Date('2016-01-01 00:00:60');
select to_Date('2016-01-01 00:00:60','yyyy-mm-dd hh24:mi:ss');
select to_Date('2016-01-01 00:59:60');
select to_Date('2016-01-01 00:59:60','yyyy-mm-dd hh24:mi:ss');
select to_Date('2016-01-01 24:00:60');
select to_Date('2016-01-01 24:00:60','yyyy-mm-dd hh24:mi:ss');
select to_Date('2016-01-01 24:60:00');
select to_Date('2016-01-01 24:60:00','yyyy-mm-dd hh24:mi:ss');
select to_Date('2016-01-01 24:59:60');
select to_Date('2016-01-01 24:59:60','yyyy-mm-dd hh24:mi:ss');

create database icbc_td_db_x template template0 encoding 'SQL_ASCII' dbcompatibility 'C';
\c icbc_td_db_x
create table a (c1 int, c2 text);
insert into a values (55, 'gauss'), (120, 'gauss'), (150, 'icbc');
select max(case when c2='gauss' then c1 end) from a;
select max((case when c2='gauss' then c1 end)::int) from a;
\c regression
drop database icbc_td_db_x;
