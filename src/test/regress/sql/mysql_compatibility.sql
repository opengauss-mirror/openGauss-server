-- B db compatibility case
drop database if exists B_db;
create database B_db dbcompatibility 'B';

--------------------concat--------------------
-- concat case in A db compatibility
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

-- concat case in B db compatibility
\c B_db
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

-- null case in B db
\c B_db
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

\c B_db
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
create table test(a int);
insert into test values (1),(2),(3),(4),(5);
select * from test order by 1 limit 2,3;
select * from test order by 1 limit 2,6;
select * from test order by 1 limit 6,2;
drop table test;

-- limit case in B db
\c B_db
create table test(a int);
insert into test values (1),(2),(3),(4),(5);
select * from test order by 1 limit 2,3;
select * from test order by 1 limit 2,6;
select * from test order by 1 limit 6,2;
drop table test;

--------------timestampdiff-----------------
-- timestamp with time zone
-- timestamp1 > timestamp2
\c B_db
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

--test uservar
show enable_set_variable_b_format;
set enable_set_variable_b_format = on;
set @var_t_1 = 10;
select @var_t_1;
select @var_t_1 := @var_t_1 + 2;
select @var_t_1 = 2 = 2;
select @var_t_1;

create table test_var (col1 text, col2 text);
insert into test_var values('abc','def');
insert into test_var values('abc2','def3');
select * from test_var;
select @var_row := @var_row + 1 as row , t.* from (select @var_row := 0) r, test_var t;


-- test var_name
set @v1 := 1;
set @1a_b.2$3 := 2;
set @a_b.2$3 := 3;
set @_ab.2$3 := 4;
set @.ab_2$3 := 5;
set @$ab.2_3 := 6;
select @v1, @1a_b.2$3, @a_b.2$3, @_ab.2$3, @.ab_2$3, @$ab.2_3;

drop table if exists test1;
create table test1 (f1 int,f2 int,f3 text);
-- insertStmt
set @v2 := 'insert into test1 values(1, 2, 123)';
prepare stmt2 as @v2;
execute stmt2;
select * from test1;

-- updateStmt
set @vx := 2, @vy := 'world';
set @v3 := 'update test1 set f3 = left(@vy, (@vx) :: int)';
prepare stmt3 as @v3;
execute stmt3;
select * from test1;

-- deleteStmt
set @v4 := 'delete from test1 where f1 = 1';
prepare stmt4 as @v4;
execute stmt4;
select * from test1;

--plpgsql
create table t_plg(a int);
insert into t_plg values(1),(2),(3);
set @plg = 1;
DO $$
begin
perform @plg := @plg + 1 from t_plg;
end;
$$;

--test in where 
CREATE TABLE employee (
   id int ,
   salary int not null
);

INSERT INTO employee VALUES(1, 100);
INSERT INTO employee VALUES(2, 200);
INSERT INTO employee VALUES(3, 300);

SELECT salary, (@rowno := @rowno + 1) AS rowno FROM employee, (SELECT @rowno := 0) r;

SELECT salary, rowno
FROM (
    SELECT salary, (@rowno := @rowno + 1) AS rowno
    FROM employee, (SELECT @rowno := 0) r
) m
WHERE rowno = 2;

--test in error
SELECT salary, (@rowno := salary) AS rowno FROM employee ;

SELECT salary, (@rowno := salary + 1) AS rowno FROM employee ;

--test in order by 
set @rowno = 0;
SELECT salary, (@rowno := @rowno + 1) AS rowno FROM employee order by  @rowno;

set @rowno = 0;
SELECT salary, (@rowno := @rowno + 1) AS rowno FROM employee order by  @rowno desc;

set @rowno = 0;
SELECT salary, @rowno AS rowno FROM employee order by  (@rowno := @rowno + 1) desc;

--test in update

set @rowno = 0;
update employee set salary = 999 where (@rowno := @rowno + 1) < 2;
select @rowno;
select * from employee;

--test in delete
set @rowno = 0;
delete from employee where (@rowno := @rowno + 1) < 2;
select @rowno;
select * from employee;

--test in const
set @rowno = 0;
SELECT (@rowno := sin(1));
select @rowno;

set @rowno = 0;
SELECT (@rowno := 1+2+3+4);
select @rowno;

create table test_con (a int);
insert into test_con values(1);

set @rowno = 0;
select (@rowno := (select a+10086 from test_con limit 1));
select @rowno;

-- test insert select 
create table t1 (a int);
insert into t1 values(1),(10);
set @num := 0;
create table t2(b int);
insert into t2 select @num + 10 from t1;
select * from t2;
select @num;

--test as function parameter
set @num := 1;
select sin(@num := @num + 1) from t1;

-- procedure
set enable_set_variable_b_format = on;
set @v1 := 10, @v2 := 'abc';
drop table if exists test_pro;
create table test_pro(f1 int, f2 varchar(20));
create or replace procedure pro_insert()
as
begin
    perform @v1 := @v1 +1;
    insert into test_pro values(@v1, @v2);
end;
/
call pro_insert();
select * from test_pro;
select @v1;
set @v1 := 14, @v2 := 'xxx';
call pro_insert();
select * from test_pro;
select @v1;

set enable_set_variable_b_format = on;
set @v1 = 1;

set @v1 = 1;
create or replace function func_add_sql(num1 bigint, num2 bigint) return bigint
as
begin
    return @v1 := 2 + @v1 ;
end;
/
call func_add_sql(-2, -5);
select @v1;


set @a = 0;

create table test_set(c1 int);
insert into test_set values(1),(2),(3),(4);
select c1 from test_set where (c1 > @a := 1000) or (c1 < @a := 2 + @a);
select @a;

explain (costs off) select c1 from test_set where (c1 > @a := 1000) or (c1 < @a := 2 + @a);
--test trig
drop table if exists test_trigger_des_tbl;
drop table if exists test_trigger_src_tbl;
CREATE TABLE test_trigger_src_tbl(id1 INT, id2 INT, id3 INT);
CREATE TABLE test_trigger_des_tbl(id1 INT, id2 INT, id3 INT);
set @va = 1;

CREATE OR REPLACE FUNCTION tri_insert_func() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
    perform @va := @va + 100;
    INSERT INTO test_trigger_des_tbl VALUES(NEW.id1, NEW.id2, NEW.id3);
  RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
		   
CREATE TRIGGER insert_trigger
           BEFORE INSERT ON test_trigger_src_tbl
           FOR EACH ROW
           EXECUTE PROCEDURE tri_insert_func();
		   
INSERT INTO test_trigger_src_tbl VALUES(100);
select @va;
select * from test_trigger_src_tbl;
select * from test_trigger_des_tbl;

set enable_set_variable_b_format = 0;
select @var_t_1 := 2;



\c regression

drop database B_db;
