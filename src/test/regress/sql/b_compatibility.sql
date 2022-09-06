-- b compatibility case
drop database if exists b;
create database b dbcompatibility 'b';

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
\c b
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
\c b
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

\c b
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
\c b
create table test_limit(a int);
insert into test_limit values (1),(2),(3),(4),(5);
select * from test_limit order by 1 limit 2,3;
select * from test_limit order by 1 limit 2,6;
select * from test_limit order by 1 limit 6,2;
drop table test_limit;

--------------timestampdiff-----------------
-- timestamp with time zone
-- timestamp1 > timestamp2
\c b
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
\c b
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
drop procedure if exists doiterate;
drop function if exists labeltest;

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
\c b

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

drop database b;
DROP USER test_c;
DROP USER test_d;
