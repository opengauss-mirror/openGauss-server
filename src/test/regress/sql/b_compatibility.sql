-- b compatibility case
drop database if exists b_cmpt_db;
create database b_cmpt_db dbcompatibility 'b';

--------------------concat--------------------
-- concat case in a compatibility
\c regression
select concat('','A');
select concat(null,'A');
select concat_ws(',', 'A', null);
select concat_ws(',', 'A', '');
create table text1 (a char(10));
insert into text1 values (concat('A',''));
insert into text1 values (concat('A',null));
select * from text1 where a is null;
drop table text1;

-- concat case in b compatibility
\c b_cmpt_db
select concat('','A');
select concat(null,'A');
select concat_ws(',', 'A', null);
select concat_ws(',', 'A', '');
create table text1 (a char(10));
insert into text1 values (concat('A',''));
insert into text1 values (concat('A',null));
select * from text1 where a is null;
drop table text1;

-----------null is not equal to ''---------
-- null case in postgresql
\c regression
create table text2 (a char(10));
insert into text2 values('');
insert into text2 values (null);
select * from text2 where a is null;
select * from text2 where a='';
select * from text2 where a is not null;
drop table text2;

-- null case in b
\c b_cmpt_db
create table text2 (a char(10));
insert into text2 values('');
insert into text2 values (null);
select * from text2 where a is null;
select * from text2 where a='';
select * from text2 where a is not null;
drop table text2;

-- test int8 int1in int2in int4in
\c regression
select '-'::int8;
select int1in('');
select int1in('.1');
select int2in('s');
select int4in('s');

\c b_cmpt_db
select '-'::int8;
select int1in('');
select int1in('.1');
select int2in('s');
select int4in('s');
-- test substr
select substr(9, 2) + 1;
select substr(9, 2) + 1.2;
select substr(9, 2) + '1';
select substr(9, 2) + '1.2';
select substr(9, 2) + 'a';
select substr(1.2, 1, 3) + '1.2';
select 'a' + 1;
select 'a' + 1.2;
select 'a' + '1';
select 'a' + '1.2';
select 'a' + 'b';
select cast('.1' as int);
select cast('' as int);
select cast('1.1' as int);
select cast('s' as int);

--------------- limit #,#-------------------
-- limit case in postgresql
\c regression
create table test_limit(a int);
insert into test_limit values (1),(2),(3),(4),(5);
select * from test_limit order by 1 limit 2,3;
select * from test_limit order by 1 limit 2,6;
select * from test_limit order by 1 limit 6,2;
drop table test_limit;

-- limit case in b
\c b_cmpt_db
create table test_limit(a int);
insert into test_limit values (1),(2),(3),(4),(5);
select * from test_limit order by 1 limit 2,3;
select * from test_limit order by 1 limit 2,6;
select * from test_limit order by 1 limit 6,2;
drop table test_limit;

--------------timestampdiff-----------------
-- timestamp with time zone
-- timestamp1 > timestamp2
\c b_cmpt_db
select timestampdiff(year, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(quarter, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(week, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(month, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(day, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(hour, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(minute, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(second, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(microsecond, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');

-- timestamp2 > timestamp1
select timestampdiff(year, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(quarter, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(week, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(month, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(day, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(hour, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(minute, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(second, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(microsecond, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');

-- LEAP YEAR LEAP MONTH
select timestampdiff(day, '2016-01-01', '2017-01-01');
select timestampdiff(day, '2017-01-01', '2018-01-01');
select timestampdiff(day, '2016-01-01', '2016-02-01');
select timestampdiff(day, '2016-02-01', '2016-03-01');
select timestampdiff(day, '2016-03-01', '2016-04-01');
select timestampdiff(day, '2016-04-01', '2016-05-01');
select timestampdiff(day, '2016-05-01', '2016-06-01');
select timestampdiff(day, '2016-06-01', '2016-07-01');
select timestampdiff(day, '2016-07-01', '2016-08-01');
select timestampdiff(day, '2016-08-01', '2016-09-01');
select timestampdiff(day, '2016-09-01', '2016-10-01');
select timestampdiff(day, '2016-10-01', '2016-11-01');
select timestampdiff(day, '2016-11-01', '2016-12-01');
select timestampdiff(day, '2016-12-01', '2017-01-01');
select timestampdiff(day, '2000-02-01', '2000-03-01');
select timestampdiff(day, '1900-02-01', '1900-03-01');

-- timestamp without time zone
select timestampdiff(year, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(quarter, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(week, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(month, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(day, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(hour, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(minute, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(second, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(microsecond, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);

-- now()
select timestampdiff(year, '2018-01-01', now());
select timestampdiff(quarter, '2018-01-01', now());
select timestampdiff(week, '2018-01-01', now());
select timestampdiff(month, '2018-01-01', now());
select timestampdiff(day, '2018-01-01', now());
select timestampdiff(hour, '2018-01-01', now());
select timestampdiff(minute, '2018-01-01', now());
select timestampdiff(second, '2018-01-01', now());
select timestampdiff(microsecond, '2018-01-01', now());

-- current_timestamp
select timestampdiff(year,'2018-01-01', current_timestamp);

-- test error
select timestampdiff(yearss, '2018-01-01', now());
select timestampdiff(century, '2018-01-01', now());
select timestampdiff(year, '-0001-01-01', '2019-01-01');
select timestampdiff(microsecond, '0001-01-01', '293000-12-31');
select timestampdiff(microsecond, '2018-13-01', '2019-12-31');
select timestampdiff(microsecond, '2018-01-01', '2019-12-32');

-- test table ref
create table timestamp(a timestamp, b timestamp with time zone);
insert into timestamp values('2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');

select timestampdiff(year, '2018-01-01 01:01:01.000001', b) from timestamp;
select timestampdiff(quarter, '2018-01-01 01:01:01.000001', b) from timestamp;
select timestampdiff(week, '2018-01-01 01:01:01.000001', b) from timestamp;;
select timestampdiff(month, '2018-01-01 01:01:01.000001', b) from timestamp;;
select timestampdiff(day, '2018-01-01 01:01:01.000001', b) from timestamp;;
select timestampdiff(hour, '2018-01-01 01:01:01.000001', b) from timestamp;;
select timestampdiff(minute, '2018-01-01 01:01:01.000001',b) from timestamp;;
select timestampdiff(second, '2018-01-01 01:01:01.000001', b) from timestamp;
select timestampdiff(microsecond, '2018-01-01 01:01:01.000001', b) from timestamp;
drop table timestamp;

-- test char/varchar length
create table char_test(a char(10),b varchar(10));
insert into char_test values('零一二三四五六七八九','零一二三四五六七八九');
insert into char_test values('零1二3四5六7八9','零1二3四5六7八9');
insert into char_test values('零1二3四5六7八9','零1二3四5六7八90');
insert into char_test values('零1二3四5六7八90','零1二3四5六7八9');
insert into char_test values('零0','零1二3');
insert into char_test values('零0  ','零1二3');
insert into char_test values('零0','零1二3  ');
insert into char_test values('','');
insert into char_test values(null,null);
insert into char_test values('0','0');
select length(a),length(b) from char_test;
select lengthb(a),lengthb(b) from char_test;
select bit_length(a),bit_length(b) from char_test;
select 'ni啊shaeskeeee'::char(3);

create index a on char_test(a);
create index b on char_test(b);
set enable_seqscan to off;
select * from char_test where a = '零0';
select * from char_test where b = '零1二3';
drop table char_test;

-- Testing the behavior of CREATE VIEW statement permissions in B-compatibility mode
CREATE SCHEMA test_schema;
CREATE SCHEMA view_schema;
CREATE TABLE test_schema.test_table (a int);
CREATE VIEW test_schema.test_view AS SELECT ascii('a');
CREATE FUNCTION test_schema.bit2float8 (bit) RETURNS float8 AS
$$
BEGIN
    RETURN (SELECT int8($1));
END;
$$
LANGUAGE plpgsql;

CREATE USER test_c WITH PASSWORD 'openGauss@123';
CREATE USER test_d WITH PASSWORD 'openGauss@123';

GRANT USAGE ON SCHEMA test_schema TO test_d;
GRANT CREATE ON SCHEMA view_schema TO test_d;

GRANT USAGE ON SCHEMA test_schema TO test_c;
GRANT CREATE ON SCHEMA view_schema TO test_c;

REVOKE EXECUTE ON FUNCTION test_schema.bit2float8(bit) FROM public;

SET ROLE test_c PASSWORD 'openGauss@123';

CREATE DEFINER=test_d VIEW view_schema.new_view1 AS SELECT * FROM test_schema.test_table;
CREATE DEFINER=test_d VIEW view_schema.new_view2 AS SELECT * FROM test_schema.test_view;
CREATE DEFINER=test_d VIEW view_schema.new_view3 AS SELECT * FROM test_schema.bit2float8(b'1111');

RESET ROLE;

GRANT test_d TO test_c;

SET ROLE test_c PASSWORD 'openGauss@123';

CREATE DEFINER=test_d VIEW view_schema.new_view1 AS SELECT * FROM test_schema.test_table;
CREATE DEFINER=test_d VIEW view_schema.new_view2 AS SELECT * FROM test_schema.test_view;
CREATE DEFINER=test_d VIEW view_schema.new_view3 AS SELECT * FROM test_schema.bit2float8(b'1111');

SELECT * FROM view_schema.new_view1;
SELECT * FROM view_schema.new_view2;
SELECT * FROM view_schema.new_view3;

RESET ROLE;

SET ROLE test_d PASSWORD 'openGauss@123';

SELECT * FROM view_schema.new_view1;
SELECT * FROM view_schema.new_view2;
SELECT * FROM view_schema.new_view3;

RESET ROLE;

GRANT SELECT ON TABLE test_schema.test_table TO test_d;
GRANT SELECT ON TABLE test_schema.test_view TO test_d;
GRANT EXECUTE ON FUNCTION test_schema.bit2float8(bit) TO test_d;
GRANT USAGE ON SCHEMA view_schema TO test_d;

SET ROLE test_d PASSWORD 'openGauss@123';

SELECT * FROM view_schema.new_view1;
SELECT * FROM view_schema.new_view2;
SELECT * FROM view_schema.new_view3;

RESET ROLE;

SET ROLE test_c PASSWORD 'openGauss@123';

SELECT * FROM view_schema.new_view1;
SELECT * FROM view_schema.new_view2;
SELECT * FROM view_schema.new_view3;

RESET ROLE;

\c regression

-- test label:loop
--error
create or replace procedure doiterate(p1 int)
as
begin
label1: loop
p1 := p1+1;
if p1 < 10 then
iterate label1;
end if;
leave label1;
end loop label1;
raise notice 'p1:%',p1;
end;
/

--error
create or replace procedure doiterate(p1 int)
as
begin
<<label1>> loop
p1 := p1+1;
if p1 < 10 then
iterate label1;
end if;
leave label1;
end loop label1;
raise notice 'p1:%',p1;
end;
/

--error
create or replace procedure doiterate(p1 int)
as
begin
<<label1>> loop
p1 := p1+1;
if p1 < 10 then
continue label1;
end if;
leave label1;
end loop label1;
raise notice 'p1:%',p1;
end;
/

--success
create or replace procedure doiterate(p1 int)
as
begin
<<label1>> loop
p1 := p1+1;
if p1 < 10 then
continue label1;
end if;
exit label1;
end loop label1;
raise notice 'p1:%',p1;
end;
/

call doiterate(3);

drop procedure if exists doiterate;
\c b_cmpt_db
--success
create or replace procedure doiterate(p1 int)
as
begin
label1   :
loop
p1 := p1+1;
if p1 < 10 then
raise notice '123';
end if;
exit;
end loop label1;
end;
/

call doiterate(2);

--success
create or replace procedure doiterate(p1 int)
as
begin
LABEL1:
loop
p1 := p1+1;
if p1 < 10 then
raise notice '123';
end if;
exit LABEL1;
end loop LABEL1;
end;
/

call doiterate(2);

--success
create or replace procedure doiterate(p1 int)
as
begin
LABEL1:loop
p1 := p1+1;
if p1 < 10 then
raise notice '123';
end if;
exit LABEL1;
end loop LABEL1;
end;
/

call doiterate(2);

--success
create or replace procedure doiterate(p1 int)
as
begin
LAbEL1:
loop
p1 := p1+1;
if p1 < 10 then
raise notice '123';
end if;
exit LABeL1;
end loop LaBEL1;
end;
/

call doiterate(2);

--success
create or replace procedure doiterate(p1 int)
as
begin
label1: loop
p1 := p1+1;
if p1 < 10 then
iterate label1;
end if;
leave label1;
end loop label1;
raise notice 'p1:%',p1;
end;
/

call doiterate(3);

create or replace procedure doiterate(p1 int)
as
begin
loop
p1 := p1+1;
if p1 < 10 then
iterate;
end if;
leave;
end loop;
raise notice 'p1:%',p1;
end;
/

call doiterate(3);

create or replace function labeltest(n int) return int
as
p int;
i int;
begin
p :=2;
label1:  loop
if p < n then
p := p+1;
iterate label1;
end if;
leave label1;
end loop label1;
return p;
end;
/

call labeltest(5);

create or replace function labeltest(n int) return int
as
p int;
i int;
begin
p :=2;
LABEL1:  loop
if p < n then
p := p+1;
iterate LABEL1;
end if;
leave LABEL1;
end loop LABEL1;
return p;
end;
/

call labeltest(3);

create or replace function labeltest(n int) return int
as
p int;
i int;
begin
p :=2;
lAbel1:  loop
if p < n then
p := p+1;
iterate labEl1;
end if;
leave LAbel1;
end loop labEL1;
return p;
end;
/

call labeltest(7);

create or replace function labeltest(n int) return int
as
p int;
i int;
begin
p :=2;
loop
if p < n then
p := p+1;
iterate;
end if;
leave;
end loop;
return p;
end;
/

call labeltest(9);

create table tb_1092829(id int);
-- failed
create or replace procedure prc_1092829(n int)
as
BEGIN
“abel1*”:
loop
n:=n-1;
IF n<5 then
LEAVE “label1”;
END IF;
insert into tb_1092829 values(n);
END loop;
END;
/

-- failed
create or replace procedure prc_2_1092829(n int)
as
BEGIN
label#:
loop
n:=n-1;
IF n<5 then
LEAVE label#;
END IF;
insert into tb_1092829 values(n);
END loop;
END;
/

--failed
create or replace procedure prc_1092829(n int)
as
BEGIN
测试中文:
loop
n:=n-1;
IF n<5 then
LEAVE 测试中文;
END IF;
insert into tb_1092829 values(n);
END loop;
END;
/

-- failed
create or replace procedure prc_1092829(n int)
as
BEGIN
1_hello:
loop
n:=n-1;
IF n<5 then
LEAVE 1_hello;
END IF;
insert into tb_1092829 values(n);
END loop;
END;
/

-- success
create or replace procedure prc_1092829(n int)
as
BEGIN
_hello:
loop
n:=n-1;
IF n<5 then
LEAVE _hello;
END IF;
insert into tb_1092829 values(n);
END loop;
END;
/

call prc_1092829(10);
select * from tb_1092829;
drop function prc_1092829;
drop table tb_1092829;

--success
create or replace procedure proc_label(n int)
as
BEGIN
label11111111111111111111111111111111111111111111111111111111112: loop
n:=n-1;
IF n<5 then
LEAVE label11111111111111111111111111111111111111111111111111111111112;
END IF;
raise info 'number is %.',n;
END loop;
END;
/

--success
create or replace procedure proc_label1(n int)
as
BEGIN
label11111111111111111111111111111111111111111111111111111111112    : loop
n:=n-1;
IF n<5 then
LEAVE label11111111111111111111111111111111111111111111111111111111112;
END IF;
raise info 'number is %.',n;
END loop;
END;
/

call proc_label(5);
call proc_label1(5);
drop procedure if exists doiterate;
drop function if exists labeltest;
drop function if exists proc_label;
drop function if exists proc_label1;

\c regression

create or replace procedure func_zzm(num1 in int, num2 inout int, res out int)
as begin
num2 := num2 + 1;
res := num1 + 1;
end;
/

--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
func_zzm(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

--error
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
call func_zzm(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

CREATE OR REPLACE FUNCTION func1(num1 in int, num2 inout int, res out int) RETURNS record
AS $$
DECLARE
BEGIN
num2 := num2 + 1;
res := num1 + 1;
return;
END
$$
LANGUAGE plpgsql;

--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
raise notice '%,%',n1,n2;
func1(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

--error
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
raise notice '%,%',n1,n2;
call func1(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

create or replace procedure debug(num1 in int, num2 inout int, res out int)
as begin
num2 := num2 + 1;
res := num1 + 1;
end;
/
--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
debug(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/
--error
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
call debug(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/


create or replace procedure call(num1 in int, num2 inout int, res out int)
as begin
num2 := num2 + 1;
res := num1 + 1;
end;
/
--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
call(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

--error
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
call call(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/
drop procedure call;
CREATE OR REPLACE FUNCTION call() RETURNS int
AS $$
DECLARE
n int;
BEGIN
n := 1;
return 1;
END
$$
LANGUAGE plpgsql;

--success
declare
begin
call();
end;
/

--error
declare
begin
call;
end;
/

--error
declare
begin
call call();
end;
/

--error
declare
begin
call call;
end;
/

create schema test;
CREATE OR REPLACE FUNCTION test.func1() RETURNS int
AS $$
DECLARE
n int;
BEGIN
n := 1;
return 1;
END
$$
LANGUAGE plpgsql;

--success
declare
begin
test.func1();
end;
/

--error
declare
begin
call test.func1();
end;
/

create or replace procedure test.debug(num1 in int, num2 inout int, res out int)
as begin
num2 := num2 + 1;
res := num1 + 1;
end;
/
--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
test.debug(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/
--error
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
call test.debug(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

create or replace procedure test.call(num1 in int, num2 inout int, res out int)
as begin
num2 := num2 + 1;
res := num1 + 1;
end;
/
--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
test.call(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

--error
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
call test.call(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

drop procedure func_zzm;
drop function func1;
drop procedure debug;
drop procedure call;
drop schema test CASCADE;
\c b_cmpt_db

create or replace procedure func_zzm(num1 in int, num2 inout int, res out int)
as begin
num2 := num2 + 1;
res := num1 + 1;
end;
/

--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
func_zzm(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
call func_zzm(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

CREATE OR REPLACE FUNCTION func1(num1 in int, num2 inout int, res out int) RETURNS record
AS $$
DECLARE
BEGIN
num2 := num2 + 1;
res := num1 + 1;
return;
END
$$
LANGUAGE plpgsql;

--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
raise notice '%,%',n1,n2;
func1(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
raise notice '%,%',n1,n2;
call func1(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

create or replace procedure debug(num1 in int, num2 inout int, res out int)
as begin
num2 := num2 + 1;
res := num1 + 1;
end;
/
--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
debug(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/
--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
call debug(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/


create or replace procedure call(num1 in int, num2 inout int, res out int)
as begin
num2 := num2 + 1;
res := num1 + 1;
end;
/
--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
call(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
call call(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/
drop procedure call;
CREATE OR REPLACE FUNCTION call() RETURNS int
AS $$
DECLARE
n int;
BEGIN
n := 1;
return 1;
END
$$
LANGUAGE plpgsql;

--success
declare
begin
call();
end;
/

--error
declare
begin
call;
end;
/

--success
declare
begin
call call();
end;
/

--error
declare
begin
call call;
end;
/

create schema test;
CREATE OR REPLACE FUNCTION test.func1() RETURNS int
AS $$
DECLARE
n int;
BEGIN
n := 1;
return 1;
END
$$
LANGUAGE plpgsql;

--success
declare
begin
test.func1();
end;
/

--success
declare
begin
call test.func1();
end;
/

create or replace procedure test.debug(num1 in int, num2 inout int, res out int)
as begin
num2 := num2 + 1;
res := num1 + 1;
end;
/
--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
test.debug(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/
--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
call test.debug(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

create or replace procedure test.call(num1 in int, num2 inout int, res out int)
as begin
num2 := num2 + 1;
res := num1 + 1;
end;
/
--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
test.call(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

--success
declare
n1 int;
n2 int;
begin
n1 := 10;
n2 := 100;
call test.call(1,n1,n2);
raise notice '%,%',n1,n2;
end;
/

drop procedure func_zzm;
drop function func1;
drop procedure debug;
drop procedure call;
drop schema test CASCADE;

\c regression

--error
create or replace function while_test1() returns void as $$
declare _i integer = 0; _r record;
begin
  raise notice '---1---';
  while _i < 10 do
    _i := _i + 1;
    raise notice '%', _i;
  end while;

  raise notice '---2---';
  <<lbl>>
  while _i > 0 do
    _i := _i - 1;
    loop
      raise notice '%', _i;
      continue lbl when _i > 0;
      exit lbl;
    end loop;
  end while;

  raise notice '---3---';
  the_while:
  while _i < 10 do
    _i := _i + 1;
    continue the_while when _i % 2 = 0;
    raise notice '%', _i;
  end while the_while;

  the_while1:while _i < 10 do
    _i := _i + 1;
    continue the_while1 when _i % 2 = 0;
    raise notice '%', _i;
  end while the_while1;

  raise notice '---3---';
  _i := 1;
  while _i <= 3 loop
    raise notice '%', _i;
    _i := _i + 1;
    continue when _i = 3;
  end loop;
end; $$ language plpgsql;

--error
create or replace function while_test1() returns void as $$
declare _i integer = 0; _r record;
begin
  while _i < 10 do
    _i := _i + 1;
    raise notice '%', _i;
  end while the_while;
end; $$ language plpgsql;

--error
create or replace function while_test1() returns void as $$
declare _i integer = 10; _r record;
begin
  <<lbl>>
  while _i > 0 do
    _i := _i - 1;
    loop
      raise notice '%', _i;
      continue lbl when _i > 0;
      exit lbl;
    end loop;
  end while lbl;
end; $$ language plpgsql;

--error
create or replace function while_test1() returns void as $$
declare _i integer = 0; _r record;
begin
  the_while:
  while _i < 10 do
    _i := _i + 1;
    continue the_while when _i % 2 = 0;
    raise notice '%', _i;
  end while the_while;
end; $$ language plpgsql;

--success
create or replace function while_test1() returns void as $$
declare _i integer = 10; _r record;
begin
  <<lbl>>
  while _i > 0 loop
    _i := _i - 1;
    loop
      raise notice '%', _i;
      continue lbl when _i > 0;
      exit lbl;
    end loop;
  end loop lbl;
end; $$ language plpgsql;

select while_test1();
drop function while_test1;
\c b_cmpt_db


create or replace function while_test1() returns void as $$
declare _i integer = 0; _r record;
begin
  raise notice '---1---';
  while _i < 10 do
    _i := _i + 1;
    raise notice '%', _i;
  end while;

  raise notice '---2---';
  <<lbl>>
  while _i > 0 do
    _i := _i - 1;
    loop
      raise notice '%', _i;
      continue lbl when _i > 0;
      exit lbl;
    end loop;
  end while;

  raise notice '---3---';
  the_while:
  while _i < 10 do
    _i := _i + 1;
    continue the_while when _i % 2 = 0;
    raise notice '%', _i;
  end while the_while;

  the_while1:while _i < 10 do
    _i := _i + 1;
    continue the_while1 when _i % 2 = 0;
    raise notice '%', _i;
  end while the_while1;

  raise notice '---3---';
  _i := 1;
  while _i <= 3 loop
    raise notice '%', _i;
    _i := _i + 1;
    continue when _i = 3;
  end loop;
end; $$ language plpgsql;

select while_test1();

--error
create or replace function while_test1() returns void as $$
declare _i integer = 0; _r record;
begin
  while _i < 10 do
    _i := _i + 1;
    raise notice '%', _i;
  end while the_while;
end; $$ language plpgsql;

create or replace function while_test1() returns void as $$
declare _i integer = 10; _r record;
begin
  <<lbl>>
  while _i > 0 do
    _i := _i - 1;
    loop
      raise notice '%', _i;
      continue lbl when _i > 0;
      exit lbl;
    end loop;
  end while lbl;
end; $$ language plpgsql;

select while_test1();

create or replace function while_test1() returns void as $$
declare _i integer = 10; _r record;
begin
  <<lbl>>
  while _i > 0 loop
    _i := _i - 1;
    loop
      raise notice '%', _i;
      continue lbl when _i > 0;
      exit lbl;
    end loop;
  end loop lbl;
end; $$ language plpgsql;

select while_test1();

create or replace function while_test1() returns void as $$
declare _i integer = 0; _r record;
begin
  the_while:
  while _i < 10 do
    _i := _i + 1;
    continue the_while when _i % 2 = 0;
    raise notice '%', _i;
  end while the_while;
end; $$ language plpgsql;

select while_test1();

--error
create or replace function while_test1() returns void as $$
declare _i integer = 0; _r record;
begin
  the_while:
  while _i < 10 loop
    _i := _i + 1;
    continue the_while when _i % 2 = 0;
    raise notice '%', _i;
  end while the_while;
end; $$ language plpgsql;

--error
create or replace function while_test1() returns void as $$
declare _i integer = 0; _r record;
begin
  the_while:
  while _i < 10 do
    _i := _i + 1;
    continue the_while when _i % 2 = 0;
    raise notice '%', _i;
  end loop the_while;
end; $$ language plpgsql;

create or replace procedure while_test2()
as 
declare _i integer = 0;
BEGIN
the_while:
  while _i < 10 do
    _i := _i + 1;
    continue the_while when _i % 2 = 0;
    raise notice '%', _i;
  end while the_while;
end; 
/

select while_test2();

--error
create or replace procedure while_test2()
as 
declare _i integer = 0;
BEGIN
the_while:
  while _i < 10 loop
    _i := _i + 1;
    continue the_while when _i % 2 = 0;
    raise notice '%', _i;
  end while the_while;
end; 
/

--error
create or replace procedure while_test2()
as 
declare _i integer = 0;
BEGIN
  while _i < 10 do
    _i := _i + 1;
    continue the_while when _i % 2 = 0;
    raise notice '%', _i;
  end loop;
end; 
/

--error
create or replace procedure while_test2()
as 
declare _i integer = 0;
BEGIN
  while _i < 10 loop
    _i := _i + 1;
    continue the_while when _i % 2 = 0;
    raise notice '%', _i;
  end while;
end; 
/

drop function while_test1;
drop procedure while_test2;

\c regression
--error
CREATE or replace PROCEDURE dorepeat(p1 INT)
as 
declare
i int =0;
BEGIN
repeat
i = i + 1;
until i >p1 end repeat;
raise notice '%',i;
end;
/
--error
CREATE or replace PROCEDURE dorepeat(p1 INT)
as 
declare
i int =0;
BEGIN
<<label>>
repeat
i = i + 1;
until i >p1 end repeat label;
raise notice '%',i;
end;
/
--error
CREATE or replace PROCEDURE dorepeat(p1 INT)
as 
declare
i int =0;
BEGIN
label:
repeat
i = i + 1;
until i >p1 end repeat label;
raise notice '%',i;
end;
/
drop function if exists dorepeat;
\c b_cmpt_db

--success
CREATE or replace PROCEDURE dorepeat(p1 INT)
as 
declare
i int =0;
BEGIN
repeat
i = i + 1;
until i >p1 end repeat;
raise notice '%',i;
end;
/
select dorepeat(5);
--success
CREATE or replace PROCEDURE dorepeat(p1 INT)
as 
declare
i int =0;
BEGIN
<<label>>
repeat
i = i + 1;
until i >p1 end repeat label;
raise notice '%',i;
end;
/
select dorepeat(5);
--success
CREATE or replace PROCEDURE dorepeat(p1 INT)
as 
declare
i int =0;
BEGIN
label:
repeat
i = i + 1;
until i >p1 end repeat label;
raise notice '%',i;
end;
/
select dorepeat(5);
--success
CREATE or replace PROCEDURE dorepeat(p1 INT)
as
declare
i int =0;
BEGIN
label:repeat
i = i + 1;
until i >p1 end repeat label;
raise notice '%',i;
end;
/
select dorepeat(5);
--error
CREATE or replace PROCEDURE dorepeat(p1 INT)
as 
declare
i int =0;
BEGIN
repeat:
repeat
i = i + 1;
until i >p1 end repeat;
raise notice '%',i;
end;
/

drop function if exists dorepeat;

set behavior_compat_options='select_into_return_null';
create table test_table_030(id int, name text);
insert into test_table_030 values(1,'a'),(2,'b');
CREATE OR REPLACE FUNCTION select_into_null_func(canshu varchar(16))
returns int
as $$
DECLARE test_table_030a int;
begin
    SELECT ID into test_table_030a FROM test_table_030 WHERE NAME = canshu;
    return test_table_030a;
end; $$ language plpgsql;
select select_into_null_func('aaa');
select select_into_null_func('aaa') is null;

drop table test_table_030;
drop function select_into_null_func;
reset behavior_compat_options;

-- view sql security test

create database db_a1144425 dbcompatibility 'B';

\c db_a1144425;
create user use_a_1144425 identified by 'A@123456';
create user use_b_1144425 identified by 'A@123456';
create table sql_security_1144425(id int,cal int);
insert into sql_security_1144425 values(1,1);
insert into sql_security_1144425 values(2,2);
insert into sql_security_1144425 values(3,3);
--user a can create view in shcema
grant all on schema public to use_a_1144425;

-- definer 
create definer=use_a_1144425 sql security definer view v_1144425_2 as select * from sql_security_1144425;
-- invoker 
create definer=use_a_1144425 sql security invoker view v_1144425_1 as select * from sql_security_1144425;

--user a call view shoule be failed cause table operation
set role use_a_1144425 password 'A@123456';
select * from sql_security_1144425 order by 1,2;
-- call definer\invoker  view shoule be failed
select * from v_1144425_2 order by 1,2;
select * from v_1144425_1 order by 1,2;
-- root user shoule success in invoker ,failed in definer
reset role;
select * from v_1144425_2 order by 1,2;
select * from v_1144425_1 order by 1,2;
--user b call view shoule filed , no view permission both
set role use_b_1144425 password 'A@123456';
select * from v_1144425_2 order by 1,2;
select * from v_1144425_1 order by 1,2;
--give user b permission of view , no table permission both
reset role;
grant all on table v_1144425_2 to use_b_1144425;
grant all on table v_1144425_1 to use_b_1144425;
set role use_b_1144425 password 'A@123456';
select * from v_1144425_2 order by 1,2;
select * from v_1144425_1 order by 1,2;
-- give user b permission of table , invoker success 
reset role;
grant all on table sql_security_1144425 to use_b_1144425;
set role use_b_1144425 password 'A@123456';
select * from v_1144425_2 order by 1,2;
select * from v_1144425_1 order by 1,2;
-- give user a permission of table , definer  success 
reset role;
grant all on table sql_security_1144425 to use_a_1144425;
set role use_b_1144425 password 'A@123456';
select * from v_1144425_2 order by 1,2;
-- revoke user b permision of table , definer success, invoker failed
reset role;
revoke all on table sql_security_1144425 from use_b_1144425;
set role use_b_1144425 password 'A@123456';
select * from v_1144425_2 order by 1,2;
select * from v_1144425_1 order by 1,2;
reset role;
-- test auth passdown for multi view;
--clear all 
drop table sql_security_1144425 cascade;
drop user use_a_1144425 cascade;
drop user use_b_1144425 cascade;

create user use_a_1144425 identified by 'A@123456';
create user use_b_1144425 identified by 'A@123456';
create table sql_security_1144425(id int,cal int);
insert into sql_security_1144425 values(1,1);
--user a can create view in shcema
grant all on schema public to use_a_1144425;
-- definer view v_d_inner
create definer=use_a_1144425 sql security definer view v_d_inner as select * from sql_security_1144425;
-- invoker view v_i_inner
create definer=use_a_1144425 sql security invoker view v_i_inner as select * from sql_security_1144425;
-- definer view v_d_d_outer
create definer=use_a_1144425 sql security definer view v_d_d_outer as select * from v_d_inner;
-- definer view v_d_i_outer
create definer=use_a_1144425 sql security definer view v_d_i_outer as select * from v_i_inner;
-- definer view v_i_d_outer
create definer=use_a_1144425 sql security invoker view v_i_d_outer as select * from v_d_inner;
-- definer view v_i_i_outer
create definer=use_a_1144425 sql security invoker view v_i_i_outer as select * from v_i_inner;
-- root could only success in vii 
select * from v_i_i_outer;
select * from v_i_d_outer;
select * from v_d_i_outer;
select * from v_d_d_outer;
-- give user b permisson of view;
grant all on table v_i_i_outer to use_b_1144425;
grant all on table v_i_d_outer to use_b_1144425;
grant all on table v_d_i_outer to use_b_1144425;
grant all on table v_d_d_outer to use_b_1144425;
-- user b got error of table ,in vdi ,vdd. vii ,vid got view error
set role use_b_1144425 password 'A@123456';
select * from v_i_i_outer;
select * from v_i_d_outer;
select * from v_d_i_outer;
select * from v_d_d_outer;
--now give user a permission of table 
reset role;
grant all on table sql_security_1144425 to use_a_1144425;
-- root could  success in all 
select * from v_i_i_outer;
select * from v_i_d_outer;
select * from v_d_i_outer;
select * from v_d_d_outer;
-- user b only success in  vdd , vdi
set role use_b_1144425 password 'A@123456';
select * from v_i_i_outer;
select * from v_i_d_outer;
select * from v_d_i_outer;
select * from v_d_d_outer;
reset role;
-- coverage test
create view v1 as select * from sql_security_1144425;
create or replace view v1 as select * from sql_security_1144425;
create or replace definer=use_a_1144425 view v1 as select * from sql_security_1144425;
create sql security invoker view v2 as select * from sql_security_1144425;
create or replace sql security invoker view v2 as select * from sql_security_1144425;
create or replace definer=use_a_1144425 sql security invoker view v2 as select * from sql_security_1144425;
alter view v1 as select * from sql_security_1144425;
alter definer=use_a_1144425 view v1 as select * from sql_security_1144425;
alter sql security definer view v2 as select * from sql_security_1144425;
alter definer=use_a_1144425 sql security definer view v2 as select * from sql_security_1144425;
alter sql security definer view v1 as select * from sql_security_1144425;
alter definer=use_a_1144425 sql security definer view v1 as select * from sql_security_1144425;

--pg_dump test 
--see view_definer_test.source

--clear all 
drop table sql_security_1144425 cascade;
drop user use_a_1144425 cascade;
drop user use_b_1144425 cascade;

-- sql security end 


\c regression
drop database b_cmpt_db;
drop database db_a1144425;
DROP USER test_c;
DROP USER test_d;

-- view sql security bugfix
create database db_a1144877 dbcompatibility 'B';
\c db_a1144877;

create user use_a_1144877 identified by 'A@123456';
create user use_b_1144877 identified by 'A@123456';
--create
create table sql_security_1144877(id int,cal int);
insert into sql_security_1144877 values(1,1);
insert into sql_security_1144877 values(2,2);
insert into sql_security_1144877 values(3,3);

create schema s_1144877;
create table s_1144877.sql_security_1144877(id int,cal int);
insert into s_1144877.sql_security_1144877 values(2,1);
insert into s_1144877.sql_security_1144877 values(3,2);
insert into s_1144877.sql_security_1144877 values(4,3);

create or replace procedure p_1144877 as
begin
create sql security invoker view v_1144877 as select * from s_1144877.sql_security_1144877;

create sql security definer view v_1144877_1 as select * from sql_security_1144877;
end;
/

call p_1144877();
--root pass 
select * from v_1144877 order by 1,2;
select * from v_1144877_1 order by 1,2;

--a call 
grant select on v_1144877 to use_a_1144877;
grant select on v_1144877_1 to use_a_1144877;
grant all on table s_1144877.sql_security_1144877 to use_a_1144877;
set role use_a_1144877 password 'A@123456';
select * from v_1144877 order by 1,2;
select * from v_1144877_1 order by 1,2;

reset role;

drop user use_a_1144877 cascade;
drop user use_b_1144877 cascade;

\c regression
drop database db_a1144877;