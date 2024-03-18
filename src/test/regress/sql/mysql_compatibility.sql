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
create table test_limit_table(a int);
insert into test_limit_table values (1),(2),(3),(4),(5);
select * from test_limit_table order by 1 limit 2,3;
select * from test_limit_table order by 1 limit 2,6;
select * from test_limit_table order by 1 limit 6,2;
drop table test_limit_table;

-- limit case in B db
\c B_db
create table test_limit_table(a int);
insert into test_limit_table values (1),(2),(3),(4),(5);
select * from test_limit_table order by 1 limit 2,3;
select * from test_limit_table order by 1 limit 2,6;
select * from test_limit_table order by 1 limit 6,2;
drop table test_limit_table;

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

-- prepare with empty var or not exist var
set @v_empty := '';
prepare stmt_empty as @v_empty;
prepare stmt_empty as @does_not_exist;

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
-- bugfix for in plpgsql
prepare stmtabv as select 123;
CREATE OR REPLACE PROCEDURE pppabc (id text) as 
begin
 set @abcttt = concat('select ' , id );
 DEALLOCATE prepare stmtabv;
 prepare stmtabv as @abcttt;
end;
/

execute stmtabv;

call pppabc(12334);

execute stmtabv;

call pppabc(123345);

execute stmtabv;

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

-- test insert right ref
drop table if exists ins_sel_t0;
CREATE TABLE ins_sel_t0 ( c3 INT , c10 INT ) ;
INSERT INTO ins_sel_t0 VALUES ( -66 , 54 ) ,
    ( EXISTS ( SELECT 76 AS c42 WHERE c3 = 12 IS NOT FALSE ) NOT IN ( 75 >= -80 ) , -99 ) ; -- should error
drop table ins_sel_t0;

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

--@var retest 
--select
select @a1:=cast(1 as int2);
select @a2:=cast(2 as int4);
select @a3:=cast(3 as int8);
select @a4:=cast(4 as number);
select @a5:=cast(5.5 as numeric);
select @a6:=cast(6.76 as number(5));
select @a7:=cast(0.54 as number(3,3));
select @a8:=cast(8.0 as number(4,1));
select @a9:=cast(9.66 as float4);
select @a10:=cast(10.33 as float8);
select @a11:=cast(11.2 as real);
select @a1,@a2,@a3,@a4,@a5,@a6,@a7,@a8,@a9,@a10,@a11;

--select
select @a1:=cast(1 as char);
select @a2:=cast(2 as varchar);
select @a3:=cast(3 as clob);
select @a4:=cast(4 as text);
select @a5:=cast(5.5 as name);
select @a6:=cast(6.76 as nchar);
select @a7:=cast(7.54 as char(4));
select @a8:=cast(8.0 as nchar(4));
select @a9:=cast(9.66 as varchar(4));
select @a10:=cast(10.33 as varchar2(4));
select @a11:=cast(11.2 as nvarchar2(4));
select @a1,@a2,@a3,@a4,@a5,@a6,@a7,@a8,@a9,@a10,@a11;

--select
select @a1:=cast('2012-12-12' as date);
select @a2:=cast('10:25:32' as time);
select @a3:=cast('2023-01-22' as timestamp);
select @a4:=cast('2003-04-12 04:05:06' as smalldatetime);
select @a5:=cast(INTERVAL '3' year as interval year);
select @a6:=cast(INTERVAL '3' DAY as interval day to second);
select @a7:=cast('90' as reltime);
select @a1,@a2,@a3,@a4,@a5,@a6,@a7;


--select
select @a1:='[1,2,3]';
select @a2:='[1,[2,4,6],3]';
select @a3:='[1,{"aa":"ss","bb":4},3]';
select @a4:='{"aa":"ss","bb":4}';
select @a5:='{"aa":"ss","bb":4,"cc":{"dd":9}}';
select @a6:='{"aa":[2,3,4],"bb":4}';
select @a1,@a2,@a3,@a4,@a5,@a6;

--外表
create table tt_1130949(a1 text PRIMARY KEY);
insert into tt_1130949 values('d'),('r'),('i'),('j');
--建表
create table tab_1130949(a1 int not null,a2 char(8) unique,a3 text primary key,a4 date default '2023-02-03',a5 varchar(16) check(a5 is not null),a6 text REFERENCES tt_1130949(a1));
--index
create index on tab_1130949(a1);
create index on tab_1130949 using btree(a2);
create index on tab_1130949 using gin(to_tsvector('ngram', a4));
--insert
insert into tab_1130949 values(1,'a','b','2012-12-14','c','d');
insert into tab_1130949 values(2,'q','w','2013-12-14','e','r');
insert into tab_1130949 values(3,'t','y','2014-12-14','u','i');
insert into tab_1130949 values(4,'f','g','2015-12-14','h','j');

--select 变量
select @b1:=a1 from tab_1130949;
select @b2:=a2 from tab_1130949;
select @b3:=a3 from tab_1130949;
select @b4:=a4 from tab_1130949;
select @b5:=a5 from tab_1130949;
select @b6:=a6 from tab_1130949;
select @b1,@b2,@b3,@b4,@b5,@b6;


drop table if exists tt_1130949 cascade;

drop table if exists tab_1130956 cascade;

--建表
create table tab_1130965(a1 int,a2 int);
--插入数据
insert into tab_1130965 values(1,1),(2,3),(3,2),(4,1);

--select
--表字段与常量
select (@bq1:=case when tab_1130965.a1<3 then tab_1130965.a1 +3 else tab_1130965.a1 end) from tab_1130965;
--表字段与表字段
select (@bq2:=case when tab_1130965.a1<tab_1130965.a2 then tab_1130965.a1 else tab_1130965.a2 end) from tab_1130965;
--表字段与变量
set @asd1:=5;
select (@bq3:=case when tab_1130965.a1< @asd1 then tab_1130965.a1 else @asd1 end) from tab_1130965;
--变量与变量
set @asd2:=3;
select (@bq4:=case when @asd1> @asd2 then @asd2 else @asd1 end);
--变量与常量
set @asd2:=2;
select (@bq5:=case when @asd1>3 then @asd2 else @asd1 end);
select @bq1,@bq2,@bq3,@bq4,@bq5;

--创建函数
create or replace function fun_1131007(b1 in int,b2 in int,b3 out int)return int
as
begin
select @bb:=b1>b2 into b3;
raise notice '%',b3;
return @bb;
end;
/

select fun_1131007(1,2);

--建表
create table tab_1131021(id int,aa char(8));
insert into tab_1131021 values(1,'name');
--select
set @a_1131021:=1;
select @a_1131021:=@a_1131021+id from tab_1131021;
select @a_1131021:=@a_1131021+aa from tab_1131021;--报错

drop table if exists tab_1131021 cascade;

--建表
create table tab_1131027(id int,aa char(8));
insert into tab_1131027 values(1,'name'),(2,'ss'),(3,'dd');

--select
select @a_1131027:=min(id) from tab_1131027;
select @a_1131027:=max(id) from tab_1131027;
select @a_1131027:=sum(id) from tab_1131027;
select @a_1131027:=avg(id) from tab_1131027;
select @a_1131027:=count(id) from tab_1131027;

drop table if exists tab_1131027 cascade;

--select
select @a_1131028:=cast('x' as char(4));
select @a_1131028:=cast('x' as varchar(4));
select @a_1131028:=cast('x' as nchar(4));
select @a_1131028:=cast('x' as varchar2(4));
select @a_1131028:=cast('x' as text);
select @a_1131028:=cast(2 as int);
select @a_1131028:=cast(2 as number);

DROP PROCEDURE IF EXISTS load_tbtest_WITH_REPLACE;
CREATE PROCEDURE load_tbtest_WITH_REPLACE(id_count IN INT) AS
BEGIN
    SET @id = 1;
    WHILE @id <= id_count LOOP
        raise info 'id is %',@id;
        IF @id % 10 = 0 THEN
            SET @lsql = '';
        END IF;
        SET @id = @id + 1;
        raise info 'id+ is %',@id;
    END LOOP;
END;
/
call load_tbtest_WITH_REPLACE(3);


--bugfix @var := expr in group by 
drop table if exists tt_bug1 ;
create  table tt_bug1(a int );
insert into tt_bug1 values(1),(11),(111);
SELECT a, @a, a as xx FROM tt_bug1 GROUP BY @a:= 999,a order by a ;
SELECT a, @a, count(a) as xx FROM tt_bug1 GROUP BY @a:= a+11 ,a order by a ;
SELECT @a, avg(a) as xx FROM tt_bug1 GROUP BY @a:= 11;


set enable_set_variable_b_format = 0;
select @var_t_1 := 2;



\c regression

drop database B_db;
