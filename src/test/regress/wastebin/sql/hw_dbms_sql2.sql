----===============1.嵌套==============
CREATE OR REPLACE PACKAGE package_001 IS
PROCEDURE testpro1(var3 int);
END package_001;
/
create or replace package body package_001 is
procedure testpro1(var3 int)
is
begin
commit;
end;
end package_001;
/

create or replace procedure proc_test3() as
context_id number;
query text;
define_column_ret int;
v1 varchar2;
begin
query := 'call package_001.testpro1(o_ret);';
context_id := dbe_sql.register_context();
rollback;
dbe_sql.sql_set_sql(context_id, query, 1);
rollback;
dbe_sql.sql_bind_variable(context_id, 'o_ret',1,10);
define_column_ret := dbe_sql.sql_run(context_id);
dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.sql_unregister_context(context_id);

--输出结果
RAISE INFO 'v1: %' ,v1;
end;
/
call proc_test3();
drop package package_001;
---===============1.嵌套==============
CREATE OR REPLACE PACKAGE package_001 IS
PROCEDURE testpro1(var3 int);
END package_001;
/
create or replace package body package_001 is
procedure testpro1(var3 int)
is
begin
commit;
end;
end package_001;
/

create or replace procedure proc_test3() as
context_id number;
query text;
define_column_ret int;
v1 varchar2;
proc_name varchar2;
begin
proc_name:='package_001.testpro1';
query := 'call '||proc_name||'(o_ret);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'o_ret',1,10);
define_column_ret := dbe_sql.sql_run(context_id);
dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.sql_unregister_context(context_id);

--输出结果
RAISE INFO 'v1: %' ,v1;
end;
/
call proc_test3();
drop package package_001;

---===============2.全局变量==============

CREATE OR REPLACE PACKAGE package_001 IS
a int;
b int;
PROCEDURE testpro1(var3 int);
END package_001;
/
create or replace package body package_001 is
procedure testpro1(var3 int)
is
begin
a = 10;
raise INFO 'a:%' ,a;
commit;
end;
end package_001;
/

create or replace procedure proc_test3() as
context_id number;
query text;
define_column_ret int;
v1 varchar2;
proc_name varchar2;
begin
proc_name:='package_001.testpro1';
query := 'call '||proc_name||'(o_ret);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'o_ret',1,10);

define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.sql_unregister_context(context_id);

--输出结果
RAISE INFO 'v1: %' ,v1;
end;
/
call proc_test3();
drop package package_001;
----========================3.savepoint-----------
CREATE OR REPLACE PACKAGE package_001 IS
a int;
b int;
PROCEDURE testpro1(var3 int);
END package_001;
/
create or replace package body package_001 is
procedure testpro1(var3 int)
is
begin
a=11;
savepoint s1;
a = 10;
ROLLBACK TO SAVEPOINT s1;
raise INFO 'a:%' ,a;
commit;
end;
end package_001;
/

create or replace procedure proc_test3() as
context_id number;
query text;
define_column_ret int;
v1 varchar2;
proc_name varchar2;
begin
proc_name:='package_001.testpro1';
query := 'call '||proc_name||'(o_ret);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'o_ret',1,10);

define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.sql_unregister_context(context_id);

--输出结果
RAISE INFO 'v1: %' ,v1;
end;
/
call proc_test3();
drop package package_001;
----========================3.重载-----------
CREATE OR REPLACE PACKAGE package_001 IS
a int;
b int;
PROCEDURE testpro1(var3 int);
PROCEDURE testpro1(var3 int, var4 int);
END package_001;
/
create or replace package body package_001 is
procedure testpro1(var3 int)
is
begin
a = 10;
raise INFO 'a:%' ,a;
commit;
end;
PROCEDURE testpro1(var3 int, var4 int)
is
begin
a = 11;
raise INFO 'a:%' ,a;
rollback;
end;
end package_001;
/

create or replace procedure proc_test3() as
context_id number;
query text;
define_column_ret int;
v1 varchar2;
proc_name varchar2;
begin
proc_name:='package_001.testpro1';
query := 'call '||proc_name||'(o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'o_ret',1,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',1,10);

define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
--dbe_sql.sql_unregister_context(context_id);

--输出结果
RAISE INFO 'v1: %' ,v1;
end;
/
call proc_test3();
drop package package_001;

---========================3.重载-----------
CREATE OR REPLACE PACKAGE package_001 IS
a int;
b int;
PROCEDURE testpro1(var3 int);
PROCEDURE testpro1(var3 int, var4 int);
END package_001;
/
create or replace package body package_001 is
procedure testpro1(var3 int)
is
begin
a = 10;
raise INFO 'a:%' ,a;
commit;
end;
PROCEDURE testpro1(var3 int, var4 int)
is
begin
a = 11;
raise INFO 'a:%' ,a;
rollback;
end;
end package_001;
/

create or replace procedure proc_test3() as
context_id number;
query text;
define_column_ret int;
v1 varchar2;
proc_name varchar2;
begin
proc_name:='package_001.testpro1';
query := 'call '||proc_name||'(o_ret,o_ret2);';
context_id := dbe_sql.register_context();
dbe_sql.sql_set_sql(context_id, query, 1);
dbe_sql.sql_bind_variable(context_id, 'o_ret',1,10);
dbe_sql.sql_bind_variable(context_id, 'o_ret2',1,10);

define_column_ret := dbe_sql.sql_run(context_id);

dbe_sql.get_variable_result(context_id,'o_ret',v1);
dbe_sql.sql_unregister_context(context_id);

--输出结果
RAISE INFO 'v1: %' ,v1;
end;
/
CREATE OR REPLACE PACKAGE package_002 IS
PROCEDURE testpro1(var3 int);
END package_002;
/
create or replace package body package_002 is
procedure testpro1(var3 int)
is
begin
perform proc_test3(); 
commit;
end;
end package_002;
/
CREATE OR REPLACE PACKAGE package_003 IS
PROCEDURE testpro1(var3 int);
END package_003;
/
create or replace package body package_003 is
procedure testpro1(var3 int)
is
begin
perform package_002.testpro1(1);
commit;
end;
end package_003;
/
call package_003.testpro1(1);
drop package package_001;
drop package package_002;
drop package package_003;
-----------------------cursor---------------------------
create table t1(a int);
insert into t1 values (1);
insert into t1 values (2);
create or replace procedure p2 (c4 in int,c2 out int,c3 out int,c1 out sys_refcursor) as
va t1;
i int;
begin
open c1 for select * from t1;
begin
i = 1/0;
exception 
when others then
    c3=100;
    raise info '%', 'exception1';
end;
i=2/0;
exception 
when others then
    
	c4=100;
	c2=c4+10;
    raise info '%', 'exception2';
end;
/
select * from  p2(1);
drop table t1;
drop procedure p2;
