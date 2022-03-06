-- test for autonomous transaction with out ref cursor param 

create schema pl_auto_ref;
set current_schema to pl_auto_ref;

-- 1. (a) base use, no commit
create table t1(a int,b number(3),c varchar2(20),d clob,e blob,f text);
insert into t1 values (1,100,'var1','clob1','1234abd1','text1');
insert into t1 values (2,200,'var2','clob2','1234abd2','text2');
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 1. (b) base use, fetch before return
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1;
fetch c1 into va;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 2. base use, commit
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1;
commit;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 2. (a) base use, commit, and error
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
vb int;
begin
open c1 for select * from t1;
commit;
vb := 3/0;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 2. base use, fetch before commit
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1;
fetch c1 into va;
commit;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 3. cursor not use
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
null;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 4. (a) cursor close after open, no commit
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1;
close c1;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 4. (b) cursor close after open, commit
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1;
commit;
close c1;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 5. nested call, not support now
-- (a) p1->p2->p3, p2,p3 auto
create or replace package pck1 as
procedure p1;
procedure p2 (c2 out sys_refcursor);
procedure p3 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as

procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;

procedure p2 (c2 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
p3(c2);
end;

procedure p3 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1;
commit;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- (b) p1->p2->p3, p2,auto
create or replace package pck1 as
procedure p1;
procedure p2 (c2 out sys_refcursor);
procedure p3 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as

procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;

procedure p2 (c2 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
p3(c2);
end;

procedure p3 (c1 out sys_refcursor) as
--PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1;
commit;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- (c) p1->p2->p3, p3,auto
create or replace package pck1 as
procedure p1;
procedure p2 (c2 out sys_refcursor);
procedure p3 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as

procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;

procedure p2 (c2 out sys_refcursor) as
--PRAGMA AUTONOMOUS_TRANSACTION;
begin
p3(c2);
end;

procedure p3 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1;
commit;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 6. exception情况
-- (a).1 自治事务open前异常
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
vb int;
begin
vb := 3/0;
open c1 for select * from t1;
commit;
exception when division_by_zero then
commit;
return;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- (a).2 自治事务open后异常
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
vb int;
begin
open c1 for select * from t1;
vb := 3/0;
exception when division_by_zero then
commit;
return;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- (a).3 自治事务匿名块exception
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
vb int;
begin
vb := 0;
begin
vb := 3/0;
exception when division_by_zero then
commit;
return;
end;
open c1 for select * from t1;
commit;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- (a).4 自治事务匿名块exception
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
vb int;
begin
vb := 0;
open c1 for select * from t1;
commit;
begin
vb := 3/0;
exception when division_by_zero then
commit;
return;
end;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- (a).4 自治事务匿名块exception
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
vb int;
begin
vb := 0;
open c1 for select * from t1;
begin
vb := 3/0;
exception when division_by_zero then
commit;
return;
end;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- (b).1 主事务exception
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
vb int;
begin
p2(c1);
vb := 3/0;
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
exception when division_by_zero then
close c1;
commit;
return;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1;
commit;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 7.自治事务commit,rollback,savepoint
-- (a) before open
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
insert into t1 values (1,100,'var1','clob1','1234abd1','text1');
commit;
insert into t1 values (2,200,'var2','clob2','1234abd2','text2');
savepoint s1;
rollback to s1;
open c1 for select * from t1;
fetch c1 into va;
end;
end pck1;
/
truncate table t1;
call pck1.p1();
drop package pck1;

-- (b) rollback before open
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
insert into t1 values (1,100,'var1','clob1','1234abd1','text1');
commit;
insert into t1 values (2,200,'var2','clob2','1234abd2','text2');
savepoint s1;
open c1 for select * from t1;
fetch c1 into va;
rollback to s1;
end;
end pck1;
/
truncate table t1;
call pck1.p1();
drop package pck1;

-- (c) rollback after open
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
insert into t1 values (1,100,'var1','clob1','1234abd1','text1');
commit;
insert into t1 values (2,200,'var2','clob2','1234abd2','text2');
open c1 for select * from t1;
savepoint s1;
fetch c1 into va;
rollback to s1;
end;
end pck1;
/
truncate table t1;
call pck1.p1();
drop package pck1;

--8. multi param
create table t1_test(a int, b int, c int);
create table t2_test(a int, b varchar2(10));
insert into t1_test values(1,2,3);
insert into t1_test values(4,5,6);
insert into t2_test values(1,'aaa');
insert into t2_test values(2,'bbb');
create or replace package pck1 as
procedure p1;
procedure p2 (c1 in int, c2 in int, c3 out sys_refcursor, c4 out int, c5 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 int;
c2 int;
c3 sys_refcursor;
c4 int;
c5 sys_refcursor;
v1 t1_test;
v2 t2_test;
begin
p2(c1,c2,c3,c4,c5);
raise info 'c3 rowcount: %', c3%rowcount;
fetch c3 into v1;
raise info 'c3: %', v1;
raise info 'c3: rowcount: %', c3%rowcount;
close c3;
raise info 'c5 rowcount: %', c5%rowcount;
fetch c5 into v2;
raise info 'c5: %', v2;
raise info 'c5: rowcount: %', c5%rowcount;
close c5;
end;
procedure p2 (c1 in int, c2 in int, c3 out sys_refcursor, c4 out int, c5 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1_test;
vb t2_test;
begin
c4 := 4;
open c3 for select * from t1_test;
open c5 for select * from t2_test;
fetch c3 into va;
fetch c5 into vb;
commit;
end;
end pck1;
/

call pck1.p1();
drop package pck1;
drop table t1_test;
drop table t2_test;

-- 9.自治事务存在重载的情况,私有存过带有重载的情况
drop table t1;
create table t1(a int,b number(3),c varchar2(20),d clob,e blob,f text);
insert into t1 values (1,100,'var1','clob1','1234abd1','text1');
insert into t1 values (2,200,'var2','clob2','1234abd2','text2');
-- 9.(a)公有的同名自治事务存过
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
procedure p2(c1 int,c2 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
vn int;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
loop
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
exit when c1%notfound;
end loop;
close c1;
p2(vn,c1);
loop
fetch c1 into vc;
exit when c1%notfound;
raise info 'c1 rowcount %',c1%rowcount;
end loop;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1 order by t1;
end;
procedure p2(c1 int,c2 out sys_refcursor) as
pragma autonomous_transaction;
begin
open c2 for select * from t1;
end;
end pck1;
/

call pck1.p1();
drop package pck1;
-- 9.(b)顺序调用自治事务
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
procedure p3(c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
vn int;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
loop
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
exit when c1%notfound;
end loop;
close c1;
p3(c1);
loop
fetch c1 into vc;
raise info 'p3 %',vc;
raise info 'p3 rowcount %',c1%rowcount;
exit when c1%notfound;
end loop;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1 order by t1;
end;
procedure p3(c1 out sys_refcursor) as
pragma autonomous_transaction;
c2 int;
begin
open c1 for select * from t1 order by t1 desc;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 9.(c)顺序调用自治事务以及非自治事务
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
procedure p3(c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
vn int;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
loop
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
exit when c1%notfound;
end loop;
close c1;
p3(c1);
loop
fetch c1 into vc;
raise info 'p3 %',vc;
raise info 'p3 rowcount %',c1%rowcount;
exit when c1%notfound;
end loop;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1 order by t1;
end;
procedure p3(c1 out sys_refcursor) as
c2 int;
begin
open c1 for select * from t1 order by t1 desc;
end;
end pck1;
/
call pck1.p1();
drop package pck1;

-- 9.(d) 包外存储过程
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace procedure p3(c1 out sys_refcursor)
is
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1;
raise notice 'public.c1';
end;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p3(c1);
raise info 'public.p2%',c1%rowcount;
loop
fetch c1 into vc;
exit when c1%notfound;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
end loop;
close c1;
p2(c1);
raise info 'rowcount: %', c1%rowcount;
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1 order by t1 desc;
end;
end pck1;
/

call pck1.p1();
drop package pck1;
drop procedure p3;

-- 10.自治事务增删查改
drop table t1;
create table t1(a int);
insert into t1 values (1);
insert into t1 values (2);
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
p2(c1);
raise info 'rowcount: %', c1%rowcount;
loop
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
exit when c1%notfound;
end loop;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
vb int;
begin
update t1 set a=100;
insert into t1 values(1000);
commit;
select count(*) into vb from t1;
raise info 'vb is %',vb;
open c1 for select * from t1;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 10.(b) p1里delete之后进行commit
create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t1;
begin
delete from t1;---delete数据
commit;
p2(c1);
raise info 'rowcount: %', c1%rowcount;
loop
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
exit when c1%notfound;
end loop;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
vb int;
begin
update t1 set a=10;
insert into t1 values(8);
commit;
select count(*) into vb from t1;
raise info 'vb is %',vb;
open c1 for select * from t1;
end;
end pck1;
/

call pck1.p1();
drop package pck1;

-- 11. 跨包调用
drop table t1_test;
create table t1_test(a int, b int, c int);
insert into t1_test values(1,2,3);
insert into t1_test values(4,5,6);


create or replace package pck1 as
procedure p1;
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1 as
c3 sys_refcursor;
v1 t1_test;
begin
p2(c3);
raise info 'c3 rowcount: %', c3%rowcount;
fetch c3 into v1;
raise info 'c3: %', v1;
raise info 'c3: rowcount: %', c3%rowcount;
close c3;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
if c1%isopen then
  close c1;
  raise notice 'cursor is open';
else
  open c1 for select * from t1_test;
  raise info 'cursor is close';
end if;
end;
end pck1;
/

create or replace package pck2 as
procedure p1();
procedure p2(c1 out sys_refcursor);
end pck2;
/

create or replace package body pck2 as
procedure p1 as
c1 sys_refcursor;
v1 t1_test;
begin
 pck1.p2(c1);
 loop
 fetch c1 into v1;
 exit when c1%notfound;
 raise info 'v1 is %',v1;
 raise info 'c1 rowcount is %',c1%rowcount;
 end loop;
 close c1;
 end;
 procedure p2(c1 out sys_refcursor) as
 PRAGMA AUTONOMOUS_TRANSACTION;
 c2 sys_refcursor;
 va t1_test;
 begin
    pck1.p2(c2);
	loop
	fetch c2 into va;
	exit when c2%notfound;
	raise info 'va is %',va;
	raise info 'c2 rowcount %',c2%rowcount;
	end loop;
	close c2;
 end;
end pck2;
/

call pck1.p1();
call pck2.p1();
drop package pck1;
drop package pck2;

-- 12.自治事务里调用自治事务
drop table t1_test;
create table t1_test(a int primary key,b number(3),c varchar2(20),d clob,e blob,f text) partition by range(a)(partition p1 values less than(10),partition p2 values less than(20),partition p3 values less than(maxvalue));

insert into t1_test values (1,100,'var1','clob1','1234abd1','text1');
insert into t1_test values (2,200,'var2','clob2','1234abd2','text2');
insert into t1_test values (11,100,'var1','clob1','1234abd1','text1');
insert into t1_test values (12,200,'var2','clob2','1234abd2','text2');
insert into t1_test values (21,100,'var1','clob1','1234abd1','text1');
insert into t1_test values (32,200,'var2','clob2','1234abd2','text2');

create or replace package pck1 as
procedure p1(c1 t1_test);
procedure p2 (c1 out sys_refcursor);
end pck1;
/

create or replace package body pck1 as
procedure p1(c1 t1_test) as
PRAGMA AUTONOMOUS_TRANSACTION;
c2 sys_refcursor;
begin
p2(c2);
loop
fetch c2 into c1;
exit when c2%notfound;
raise info 'c2 rowcount is %',c2%rowcount;
raise info 'c1 is %',c1;
end loop;
end;
procedure p2(c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
open c1 for select * from t1_test where a<15;
end;
end pck1;
/
call pck1.p1((1,100,'var1','clob1','1234abd1','text1'));
drop package pck1;


-- 13.(a) 主事务commit rollback
drop table t1;
create table t1(a int);
insert into t1 values(1);
insert into t1 values(2);
create or replace procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1 for select * from t1;
end;
/
CREATE OR REPLACE PROCEDURE check1(a int)  AS
DECLARE
c1 sys_refcursor;
vc t1;
BEGIN
p2(c1);
fetch c1 into vc;
while c1%found loop
raise notice 'isopen:%',c1%isopen;
raise notice 'found:%',c1%found;
raise notice 'ans;%', vc;
fetch c1 into vc;
insert into t1 values(a);
if vc.a > 1 then
commit;
else
rollback;
end if;
end loop;
close c1;
END;
/
call check1(1);
DROP procedure check1;

-- 13.(b) savepoint
truncate table t1;
CREATE OR REPLACE PROCEDURE check4(a int)  AS
DECLARE
c1 sys_refcursor;
vc t1;
BEGIN
		insert into t1 values (1);
		insert into t1 values (2);
		insert into t1 values (1);
		insert into t1 values (2);
		commit;
		p2(c1);
		fetch c1 into vc;
		savepoint sp1;
		raise notice 'found:%',c1%found;
		raise notice 'isopen:%',c1%isopen;
		raise notice 'rowcount:%',c1%rowcount;
	    while c1%found loop
			raise notice 'isopen:%',c1%isopen;
			raise notice 'found:%',c1%found;
			raise notice 'ans;%', vc;
	        fetch c1 into vc;
	        insert into t1 values(a);
	        if vc.a > 1 then
	           commit;
	           savepoint sp1;
	           raise '%',1/0;
	        else
	           rollback to sp1;
	        end if;
	    end loop;
	    close c1;
	    exception
	    	when others then 
	    		raise notice 'exception';
	    		fetch c1 into vc;
				raise notice 'isopen:%',c1%isopen;
				raise notice 'found:%',c1%found;
				raise notice 'ans;%', vc;
				close c1;

END;
/

call check4(5);

-- 13. (c) before cursor
truncate table t1;
insert into t1 values(1);
insert into t1 values(2);
CREATE OR REPLACE PROCEDURE check6(a int)  AS
DECLARE
c1 sys_refcursor;
vc t1;
BEGIN
insert into t1 values(a);
savepoint aa;
p2(c1);
fetch c1 into vc;
while c1%found loop
raise notice 'isopen:%',c1%isopen;
raise notice 'found:%',c1%found;
raise notice 'ans;%', vc;
fetch c1 into vc;
end loop;
rollback to aa;
close c1;
END;
/
call check6(3);
drop procedure check6;
drop procedure p2;

-- test call auto procedure at last
create or replace procedure p4 as
PRAGMA AUTONOMOUS_TRANSACTION;
va int;
begin
va := 1;
commit;
end;
/
create or replace procedure p3(c3 out sys_refcursor) as
begin
open c3 for select * from t1;
end;
/
create or replace procedure p2(c2 out sys_refcursor) as
begin
p3(c2);
p4();
raise info 'p2:%',c2;
end;
/
create or replace procedure p1() as
c1 sys_refcursor;
begin
p2(c1);
raise info 'p1:%',c1;
end;
/

call p1();

drop procedure p4;
drop procedure p3;
drop procedure p2;
drop procedure p1;

-- test only in param procedure
create or replace procedure out_refcursor_t2_u1_a(c1 in sys_refcursor)
        as PRAGMA AUTONOMOUS_TRANSACTION;
        begin
        open c1 for select id from count_info;
        end;
/
declare
        c1 sys_refcursor;
        v1 int;
        begin
        out_refcursor_t2_u1_a(c1);
        fetch c1 into v1;
        end;
/
drop procedure out_refcursor_t2_u1_a;

-- test deadlock caused by autonomous session
create type type001 as(c1 number(7,2),c2 varchar(30));
drop table if exists t2_test;
create table t2_test(a int,b number(3),         c varchar2(20),d clob,e blob,f text,g type001);
insert into t2_test values      (1,100,'var1','clob1','1234abd1','text1',(1.00,'aaa'));
insert into t2_test values      (2,200,'var2','clob2','1234abd2','text2',(2.00,'bbb'));

create or replace package pck1 as procedure p1; procedure p2 (c1 out sys_refcursor); end pck1;
/

create or replace package body pck1 as
procedure p1 as
c1 sys_refcursor;
vc t2_test;
begin
delete from t2_test;---delete数据
pg_sleep(30);
p2(c1);
raise info 'rowcount: %', c1%rowcount;
loop
fetch c1 into vc;
raise info '%', vc;
raise info 'rowcount: %', c1%rowcount;
exit when c1%notfound;
end loop;
close c1;
end;
procedure p2 (c1 out sys_refcursor) as
PRAGMA AUTONOMOUS_TRANSACTION;
va t2_test;
vb int;
begin
update t2_test set a=10;
insert into t2_test values(8);
commit;
select count(*) into vb from t2_test;
raise info 'vb is %',vb;
open c1 for select * from t2_test;
end;
end pck1;
/

call pck1.p1();
select * from t2_test;
drop package pck1;
drop table t2_test;
drop type type001;

-- test dynquery sql when open cursor
drop table count_info;
drop table refcursor_info;
create table count_info (id bigserial primary key,count int,info text);
create table refcursor_info (v varchar,info varchar);
insert into count_info (count,info) values (1,'a'),(2,'b'),(3,'c'),(4,'d');
create or replace package out_refcursor_029_pkg_t1 IS
      procedure out_refcursor_029_t1(cur1 out sys_refcursor);
      procedure invoke();
      end out_refcursor_029_pkg_t1;
/
create or replace package body out_refcursor_029_pkg_t1 as
        procedure out_refcursor_029_t1(cur1 out sys_refcursor)
        as PRAGMA AUTONOMOUS_TRANSACTION;
        begin
        open cur1 for 'select count,info from count_info where count<:c' using 4;
        end;
        procedure invoke() is
        declare
        c1 sys_refcursor;
        v1 int;
        v2 text;
        tmp_v1 int;
        tmp_v2 varchar;
        begin
        out_refcursor_029_t1(c1);
        if c1%ISOPEN then
        LOOP
  FETCH c1 INTO v1,v2;
  tmp_v1:=c1%ROWCOUNT;
  tmp_v2:=v1||v2||c1%FOUND;
  insert into refcursor_info values (tmp_v1,tmp_v2);
  EXIT WHEN C1%NOTFOUND;
  END LOOP;
  end if;
  tmp_v1:=c1%ROWCOUNT;
  tmp_v2:=to_char(c1%ISOPEN)||to_char(c1%FOUND);
  insert into refcursor_info values (tmp_v1,tmp_v2);
  close c1;
  tmp_v1:=c1%ROWCOUNT;
  tmp_v2:=to_char(c1%ISOPEN)||to_char(c1%FOUND)||to_char(c1%NOTFOUND);
  insert into refcursor_info values (tmp_v1,tmp_v2);
  end;
  end out_refcursor_029_pkg_t1;
/
call out_refcursor_029_pkg_t1.invoke();
select * from refcursor_info;
drop table refcursor_info;
drop table count_info;
drop package out_refcursor_029_pkg_t1;

-- test cursor assign value (should error)
drop table t1;
create table t1 (a int, b int);
create or replace procedure p1 ( out sys_refcursor)
as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
$1 := 'abc';
end;
/

declare
va sys_refcursor;
begin
p1(va);
end;
/

create or replace procedure p1 (va out sys_refcursor)
as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
va := 'abc';
end;
/

declare
va sys_refcursor;
begin
p1(va);
end;
/

drop table t1;
drop procedure p1;

-- test function with ref cursor
CREATE OR REPLACE function f1( C2 out SYS_REFCURSOR)
LANGUAGE plpgsql
AS $$
declare
PRAGMA AUTONOMOUS_TRANSACTION;
begin
 return 1;
 END;
$$;

CREATE OR REPLACE function f1( ) returns SYS_REFCURSOR
LANGUAGE plpgsql
AS $$
declare
PRAGMA AUTONOMOUS_TRANSACTION;
begin
 return 1;
 END;
$$;

CREATE OR REPLACE function f1( C2 out SYS_REFCURSOR, C1 out INT)
LANGUAGE plpgsql
AS $$
declare
PRAGMA AUTONOMOUS_TRANSACTION;
begin
 null;
 END;
$$;

CREATE OR REPLACE function f1( ) returns SYS_REFCURSOR
LANGUAGE plpgsql
AS $$
declare
begin
 return 1;
 END;
$$;

drop function f1();


-- clean
drop schema pl_auto_ref cascade;


