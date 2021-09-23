drop schema if exists dams_ci;
drop trigger if exists insert_trigger on test_trigger_src_tbl;
drop table if exists test_trigger_des_tbl;
drop table if exists test_trigger_src_tbl;
drop package if exists trigger_test;
drop table if exists test1;
drop table if exists dams_ci.test1;
drop table if exists dams_ci.DB_LOG;
drop table if exists au_pkg;
create table au_pkg(id int,name varchar);
create schema dams_ci;
create table test_trigger_src_tbl(id1 int, id2 int, id3 int);
create table test_trigger_des_tbl(id1 int, id2 int, id3 int);
create table test1(col1 int);
insert into test1 values(50);
create table dams_ci.test1(col1 int);
drop package if exists exp_pkg;
create or replace package exp_pkg as
  user_exp EXCEPTION;
end exp_pkg;
create or replace package body exp_pkg as
end exp_pkg;
/

create or replace function func1(param int) return number 
as
declare
a exception;
begin
  if (param = 1) then
    raise exp_pkg.user_exp;
  end if;
  raise info 'number is %', param;
  exception
    when exp_pkg.user_exp then
      raise info 'user_exp raise';
  return 0;
end;
/
call func1(1); --user_exp raise

CREATE TABLE dams_ci.DB_LOG
(ID VARCHAR(20),
PROC_NAME VARCHAR(100),
INFO VARCHAR(4000),
LOG_LEVEL VARCHAR(10),
TIME_STAMP VARCHAR(23),
ERROR_BACKTRACE VARCHAR(4000),
ERR_STACK VARCHAR(4000),
STEP_NO VARCHAR(20),
LOG_DATE VARCHAR(8)
);

CREATE OR REPLACE PACKAGE dams_ci.pack_log AS
  PROCEDURE excption_1(in_desc IN db_log.id%TYPE,
                     in_info IN db_log.info%TYPE);
END pack_log;
/
CREATE OR REPLACE PACKAGE DAMS_CI.UT_P_PCKG_DAMS_DEPT_ISSUE AUTHID CURRENT_USER
IS
PROCEDURE proc_get_appinfo2();
END UT_P_PCKG_DAMS_DEPT_ISSUE ;
/

CREATE OR REPLACE PACKAGE "dams_ci"."ctp_mx_pckg_init" AS
  type ref_cursor IS ref CURSOR;
  PROCEDURE proc_get_appinfo1(appinfo OUT ref_cursor);
END ctp_mx_pckg_init;
/

create or replace package trigger_test as
  function tri_insert_func() return trigger;
end trigger_test;
create or replace package body trigger_test as
  function tri_insert_func() return trigger as
    begin
        insert into test_trigger_des_tbl values(new.id1, new.id2, new.id3);
        return new;
    end;
end trigger_test;
/

create trigger insert_trigger
    before insert on test_trigger_src_tbl
    for each row
    execute procedure trigger_test.tri_insert_func(); --不支持触发器调用package函数

insert into test_trigger_src_tbl values(1,1,1);

create or replace package dams_ci as
    procedure proc();
end dams_ci;
/

insert into test_trigger_src_tbl values(1,1,1);

create schema dams_ci;
create or replace package dams_ci.emp_bonus13 is
var5 int:=42;
var6 int:=43;
procedure testpro1();
end emp_bonus13;
/
create or replace package body dams_ci.emp_bonus13 is
var1 int:=46;
var2 int:=47;
procedure testpro1()
is
a int:=48;
b int:=49;
begin
insert into test1 values(50);
commit;
end;
procedure testpro2()
is
a int:=48;
b int:=49;
begin
insert into test1 values(50);
commit;
end;
begin
testpro1(56);
insert into test1 values(var5);
end emp_bonus13;
/

create or replace package dams_ci.emp_bonus13 is
var5 int:=42;
var6 int:=43;
procedure testpro1();
end emp_bonus13;
/
create or replace package body dams_ci.emp_bonus13 is
var1 int:=46;
var2 int:=47;
procedure testpro1()
is
a int:=48;
b int:=49;
begin
insert into test1 values(50);
commit;
end;
begin
testpro1(56);
end emp_bonus13;
/

drop package body dams_ci.emp_bonus13;
select pkgname,pkgspecsrc,pkgbodydeclsrc from gs_package where pkgname='emp_bonus13';

create or replace package feature_cross_test as
--111
   data1 int; --全局变量
   data2 int; --全局变量
   function func3(a int --函数入参注释
)return number; --公有函数
   procedure proc3(a int /*存储过程入参注释*/);
end feature_cross_test;
/
create or replace package body feature_cross_test as
/*********************************
包体头部注释快 end
**********************************/
   function func3(a int --函数入参注释 end
   )return number is
--函数头部注释
   begin
      data1 := 1;
      data2 := 2;
      insert into t1 values(data1);
      insert into t1 values(data2);
      return 0;
   end;
   procedure proc3(a int /*存储过程入参注释 end*/) is
/***********
存储过程头部注释 end
***********/
   begin
     insert into t1 values (1000);
     commit;
     insert into t1 values (2000);
     rollback;
   end;
end feature_cross_test; --包定义结束
/

create or replace package autonomous_pkg_150_1 IS
  count int := 1;
  function autonomous_f_150_1(num1 int) return int;
end autonomous_pkg_150_1;
/

create or replace package body autonomous_pkg_150_1 as
  autonomous_1 int :=10;
  autonomous_count int :=1;
function autonomous_f_150_1(num1 int) return int
  is
  re_int int;
  begin
  re_int:=autonomous_1;
  insert into au_pkg values(count,'autonomous_f_150_1_public_count');
  insert into au_pkg values(autonomous_count,'autonomous_f_150_1_count');
  count:=count+1;
  autonomous_count:=autonomous_count+1;
  return re_int+num1;
  end;
function autonomous_f_150_1_private(pnum1 int) return int
  is
  re_int int;
  begin
  re_int:=autonomous_1;
  insert into au_pkg values(count,'autonomous_f_150_1_private_public_count');
  insert into au_pkg values(autonomous_count,'autonomous_f_150_1_private_private_count');
  count:=count+1;
  autonomous_count:=autonomous_count+1;
  return re_int+pnum1;
  end;
end autonomous_pkg_150_1;
/

begin
    perform autonomous_pkg_150_1.autonomous_f_150_2_out(3);
end;
/
drop function if exists func1;
create or replace package exp_pkg as
  user_exp EXCEPTION;
  function func1(param int) return number;
end exp_pkg;
create or replace package body exp_pkg as
  function func1(param int) return number as
  begin
    if (param = 1) then
      raise user_exp;
    end if;
    raise info 'number is %', param;
    exception
      when user_exp then
        raise info 'user_exp raise';
    return 0;
  end;
end exp_pkg;
/
call exp_pkg.func1(1);

create or replace package transaction_test as
  data1 character(20) := 'global data1';
  data2 character(20) := 'global data2';
  function func(data1 int, data2 int, data1 int) return number;
end transaction_test;
/

create or replace package transaction_test as
  data1 character(20) := 'global data1';
  data2 character(20) := 'global data2';
end transaction_test;
/

create or replace package body transaction_test as
  data1 character(20) := 'global data1';
  data2 character(20) := 'global data2';
end transaction_test;
/

drop package transaction_test;


drop package if exists exp_pkg;
drop package autonomous_pkg_150_1;
\sf feature_cross_test.func3
\sf func1
select pkgspecsrc,pkgbodydeclsrc from gs_package where pkgname='feature_cross_test';

create or replace package autonomous_pkg_150 IS
  count int:=1;
  PROCEDURE autonomous_p_150(num4 int);
end autonomous_pkg_150;
/


CREATE OR REPLACE PACKAGE BODY autonomous_pkg_150 as
  autonomous_1 int:=10;
  autonomous_count int:=1;
  PROCEDURE autonomous_p_150(num4 int)
IS
PRAGMA AUTONOMOUS_TRANSACTION;
re_int INT;
BEGIN
  re_int:=autonomous_1;
  autonomous_count:=autonomous_count+1;
  select count(*)) into re_int from test1; 
  insert into test1 values(autonomous_count);
  commit;
END;
END autonomous_pkg_150;
/

CREATE OR REPLACE PACKAGE BODY autonomous_pkg_150 as
  autonomous_1 int:=10;
  autonomous_count int:=1;
  PROCEDURE autonomous_p_150(num4 int)
IS
PRAGMA AUTONOMOUS_TRANSACTION;
re_int INT;
BEGIN
  re_int:=autonomous_1;
  autonomous_count:=autonomous_count+1;
  select count(*) into re_int from test1; 
  insert into test1 values(autonomous_count);
  commit;
END;
END autonomous_pkg_150;
/



create or replace package autonomous_pkg_150_1 IS
  count int := 1;
  function autonomous_f_150_1(num1 int) return int;
end autonomous_pkg_150_1;
/

create or replace package body autonomous_pkg_150_1 as
  autonomous_1 int :=10;
  autonomous_count int :=1;
function autonomous_f_150_1_private(pnum1 int) return int
  is
  declare PRAGMA AUTONOMOUS_TRANSACTION;
  begin
  return 1;
  end;
function autonomous_f_150_1(num1 int) return int
  is
  declare PRAGMA AUTONOMOUS_TRANSACTION;
  re_int int;
  begin
  autonomous_f_150_1_private(1);
  return 1;
  end;
end autonomous_pkg_150_1;
/

call autonomous_pkg_150_1.autonomous_f_150_1(1);


create or replace package autonomous_pkg_150_2 IS
  count int := 1;
  function autonomous_f_150_2(num1 int) return int;
end autonomous_pkg_150_2;
/


create or replace package body autonomous_pkg_150_2 as
  autonomous_1 int :=10;
  autonomous_count int :=1;
function autonomous_f_150_2(num1 int) return int
  is
  declare PRAGMA AUTONOMOUS_TRANSACTION;
  re_int int;
  begin
  return 2;
  end;
function autonomous_f_150_2_private(pnum1 int) return int
  is
  declare PRAGMA AUTONOMOUS_TRANSACTION;
  re_int int;
  begin
  autonomous_pkg_150_1.autonomous_f_150_1_private(1);
  return 2;
  end;
end autonomous_pkg_150_2;
/

call autonomous_pkg_150_2.autonomous_f_150_2_private(1);

drop table if exists au_pkg;
create table au_pkg(id int,name varchar);
create or replace package autonomous_pkg_150_1 IS
  count int := 1;
  function autonomous_f_150_1(num1 int) return int;
end autonomous_pkg_150_1;
/

create or replace package body autonomous_pkg_150_1 as
  autonomous_1 int :=10;
  autonomous_count int :=1;
function autonomous_f_150_1(num1 int) return int
  is
  declare PRAGMA AUTONOMOUS_TRANSACTION;
  re_int int;
  begin
  re_int:=autonomous_1;
  insert into au_pkg values(count,'autonomous_f_150_1_public_count');
  insert into au_pkg values(autonomous_count,'autonomous_f_150_1_count');
  count:=count+1;
  autonomous_count:=autonomous_count+1;
  return re_int+num1;
  end;
function autonomous_f_150_1_private(pnum1 int) return int
  is
  declare PRAGMA AUTONOMOUS_TRANSACTION;
  re_int int;
  begin
  re_int:=autonomous_1;
  insert into au_pkg values(count,'autonomous_f_150_1_private_public_count');
  insert into au_pkg values(autonomous_count,'autonomous_f_150_1_private_private_count');
  count:=count+1;
  autonomous_count:=autonomous_count+1;
  return re_int+pnum1;
  end;
begin
perform autonomous_f_150_1(autonomous_f_150_1_private(1));
perform autonomous_f_150_1_private((select autonomous_f_150_1(2)));
end autonomous_pkg_150_1;
/

declare
begin
perform autonomous_pkg_150_1.autonomous_f_150_1(2);
end;
/

create or replace package pack_log
is
ab varchar2(10)='asdf';
bb int = 11;
procedure p1(a varchar2(10));
procedure p2();
end pack_log;
/
 
 
create or replace package body pack_log
is
procedure p1(a varchar2(10))
is
begin
null;
end;
procedure p2()
is
begin
null;
end;
end pack_log;
/

declare
ab varchar2(10):='11';
BEGIN
pack_log.p1(pack_log.ab || '11');
insert into test1 values(pack_log.bb);
END;
/

CREATE OR REPLACE PACKAGE CTP_MX_PCKG_INIT AS

  type ref_cursor IS ref CURSOR;
  --rcuror ref_cursor;

  PROCEDURE proc_get_appinfo(appinfo OUT ref_cursor);

  PROCEDURE proc_get_servinfo(appname IN varchar2, servinfo OUT ref_cursor);
  --end proc_get_servinfo;

  PROCEDURE proc_get_monitor_switch(appname IN varchar2,
                                    switchinfo OUT ref_cursor);
  --end proc_get_monitor_switch;

  PROCEDURE proc_get_useablity_info(checkers OUT ref_cursor);
  --end proc_get_useablity_info;

  PROCEDURE proc_get_trade_define(trades OUT ref_cursor);
  --end proc_get_trade_define;

  PROCEDURE proc_get_resource_define(resources OUT ref_cursor);
  --end proc_get_resource_define;
  PROCEDURE proc_get_trade_info(tradeRef OUT ref_cursor);

  PROCEDURE proc_get_resource_info(resourceRef OUT ref_cursor);
END CTP_MX_PCKG_INIT;
/
reset session AUTHORIZATION;
create or replace package cnt as
end cnt;
/


create user user1 password 'huawei@123';
set session AUTHORIZATION user1 PASSWORD 'huawei@123';
reset session AUTHORIZATION;


create or replace package commit_rollback_test as
  procedure exec_func3(ret_num out int);
  procedure exec_func4(add_num in int);
end commit_rollback_test;
/
create or replace package body commit_rollback_test as
  procedure exec_func3(ret_num out int) as
  begin
    ret_num := 1+1;
    commit;
  end;

  procedure exec_func4(add_num in int)
  as
    sum_num int;
  begin
    sum_num := add_num + exec_func3();
    commit;
  end;
end commit_rollback_test;
/

call commit_rollback_test.exec_func4(1);

create or replace package multi_sql as
function func5() return int;
function func16() return int;
end multi_sql;
/

create or replace package body multi_sql as
function func5() return int as
begin
  return (data5);
end;

function func16() return int as
begin
  alter function func5() rename to func25;
  return 0;
end;
end multi_sql;
/

create or replace package cnt as
  c1 sys_refcursor;
end cnt;
/


call multi_sql.func16();

drop package if exists multi_sql;
drop package if exists commit_rollback_test;

drop package if exists cnt;

drop package pack_log;
drop package CTP_MX_PCKG_INIT; 

drop table if exists au_pkg;
drop package autonomous_pkg_150_2;
drop package autonomous_pkg_150_1;
drop package autonomous_pkg_150;
drop package feature_cross_test;
drop function func1;

drop package dams_ci.emp_bonus13;
drop package if exists exp_pkg;
drop trigger if exists insert_trigger on test_trigger_src_tbl;
drop table if exists dams_ci.DB_LOG;
drop table if exists test_trigger_des_tbl;
drop table if exists test_trigger_src_tbl;
drop package if exists dams_ci.pack_log;
drop package if exists dams_ci.ut_p_pckg_dams_dept_issue;
drop package if exists dams_ci.ctp_mx_pckg_init;
drop package if exists trigger_test;
drop table if exists test1;
drop table if exists dams_ci.test1;
drop package if exists trigger_test;
drop package if exists dams_ci;
drop schema if exists dams_ci cascade;

--test online help
\h CREATE PACKAGE
\h CREATE PACKAGE BODY
\h DROP PACKAGE
\h DROP PACKAGE BODY
