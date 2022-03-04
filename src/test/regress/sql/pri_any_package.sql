CREATE USER test_create_any_package_role PASSWORD 'Gauss@1234';
GRANT create any package to test_create_any_package_role;

CREATE SCHEMA pri_package_schema;
set search_path=pri_package_schema;

SET ROLE test_create_any_package_role PASSWORD 'Gauss@1234';
set search_path=pri_package_schema;
drop package if exists pri_exp_pkg;

create or replace package pri_exp_pkg as
  user_exp EXCEPTION;
end pri_exp_pkg;
/

create or replace package body pri_exp_pkg as
end pri_exp_pkg;
/

create or replace function pri_func1(param int) return number 
as
declare
a exception;
begin
  if (param = 1) then
    raise pri_exp_pkg.user_exp;
  end if;
  raise info 'number is %', param;
  exception
    when pri_exp_pkg.user_exp then
      raise info 'user_exp raise';
  return 0;
end;
/

reset role;
set search_path=pri_package_schema;

drop package if exists pkg_auth_1;
CREATE OR REPLACE package pkg_auth_1
is
a int;
END pkg_auth_1;
/
CREATE OR REPLACE package body pkg_auth_1
is
END pkg_auth_1;
/
drop package if exists pkg_auth_2;
CREATE OR REPLACE package pkg_auth_2
is
b int;
procedure a();
END pkg_auth_2;
/
CREATE OR REPLACE package body pkg_auth_2
is
procedure a 
is
begin
pkg_auth_1.a:=1;
end;
END pkg_auth_2;
/

CREATE USER test_execute_any_package_role PASSWORD 'Gauss@1234';
GRANT execute any package to test_execute_any_package_role;
SET ROLE test_execute_any_package_role PASSWORD 'Gauss@1234';
set search_path=pri_package_schema;

begin
pri_package_schema.pkg_auth_1.a:=1;
end;
/

begin
pri_package_schema.pkg_auth_2.b:=1;
end;
/


reset role;
create user user_1 password 'Gauss@1234';
create user user_2 password 'Gauss@1234';
create user user_3 password 'Gauss@1234';
create user user_any password 'Gauss@1234';

set role user_1 password 'Gauss@1234';
create or replace package user_1.pri_pkg_same_arg_1
is
a int;
end pri_pkg_same_arg_1;
/
create or replace package body user_1.pri_pkg_same_arg_1
is
end pri_pkg_same_arg_1;
/ 

set role user_2 password 'Gauss@1234';
create or replace package user_2.pri_pkg_same_arg_2
is
b int;
end pri_pkg_same_arg_2;
/
create or replace package body user_2.pri_pkg_same_arg_2
is
end pri_pkg_same_arg_2;
/

set role user_any password 'Gauss@1234';
CREATE OR REPLACE package user_any.pkg_auth_2
is
b int;
procedure a();
END pkg_auth_2;
/
CREATE OR REPLACE package body user_any.pkg_auth_2
is
procedure a 
is
begin
user_2.pri_pkg_same_arg_2.b:=1;
user_1.pri_pkg_same_arg_1.a:=2;
end;
END pkg_auth_2;
/

reset role;
GRANT create any package to user_any;
GRANT execute any package to user_any;
set  role user_any password 'Gauss@1234';
CREATE OR REPLACE package user_any.pkg_auth_2
is
b int;
procedure a();
END pkg_auth_2;
/
CREATE OR REPLACE package body user_any.pkg_auth_2
is
procedure a 
is
begin
user_2.pri_pkg_same_arg_2.b:=1;
user_1.pri_pkg_same_arg_1.a:=2;
end;
END pkg_auth_2;
/

call user_any.pkg_auth_2.a();

set role user_3 password 'Gauss@1234';
call user_any.pkg_auth_2.a();
reset role;
GRANT execute any package to user_3;
set role user_3 password 'Gauss@1234';
call user_any.pkg_auth_2.a();

reset role;
drop package pri_package_schema.pkg_auth_1;
drop package pri_package_schema.pkg_auth_2;
drop package pri_package_schema.pri_exp_pkg;
drop package user_1.pri_pkg_same_arg_1;
drop package user_2.pri_pkg_same_arg_2;
drop package user_any.pkg_auth_2;
drop schema pri_package_schema cascade;
drop user user_1,user_2,user_3,user_any,test_create_any_package_role,test_execute_any_package_role cascade;
