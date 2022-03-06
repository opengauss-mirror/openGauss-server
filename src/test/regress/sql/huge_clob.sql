-- test create type table of 
-- check compatibility --
show sql_compatibility; -- expect A --

-- create new schema --
drop schema if exists huge_clob;
create schema huge_clob;
set current_schema = huge_clob;

create table bigclobtbl031(c1 int,c2 clob,c3 clob,c4 blob,c5 date,c6 timestamp,c7 varchar2);
insert into bigclobtbl031 values(generate_series(1,5),repeat('AAAA11111aaaaaaaaaaaaaaaaa',1000000),repeat('abdededfj12345679ujik',1000000),hextoraw(repeat('12345678990abcdef',1000)),sysdate,to_timestamp('','yyyy-mm-dd hh24:mi:ss.ff6'),7000);
update bigclobtbl031 set c2=c2||c2||c2||c2||c2;
update bigclobtbl031 set c2=c2||c2||c2||c2||c2;
update bigclobtbl031 set c2=c2||c2;
update bigclobtbl031 set c3='clobclob3';

--I1.clob in
create or replace procedure pro_cb4_031(c1 clob,c2 clob)
is
v1 clob;
v2 clob;
begin
v1:=dbe_lob.substr(c1,10,1);
v2:=dbe_lob.substr(c2,10,1);
raise info 'c1 is %',v1;
raise info 'c2 is %',v2;
end;
/

create or replace procedure pro_cb4_031_1 is
v1 clob;
v2 clob;
begin
execute immediate 'select c2 from bigclobtbl031 where c1=1' into v1;
execute immediate 'select c3 from bigclobtbl031 where c1=1' into v2;
pro_cb4_031(v1,v2);
end;
/

call pro_cb4_031_1();

--I2.clob > 1G out
create or replace procedure pro_cb4_031(c1 out clob,c2 out clob)
is
v1 clob;
v2 clob;
begin
execute immediate 'select c2 from bigclobtbl031 where c1=1' into v1;
execute immediate 'select c3 from bigclobtbl031 where c1=1' into v2;
c1:=v1;
c2:=v2;
end;
/

create or replace procedure pro_cb4_031_1 is
v1 clob;
v2 clob;
v3 clob;
v4 clob;
begin
pro_cb4_031(v1,v2);
v3:=dbe_lob.substr(v1,10,1);
v4:=dbe_lob.substr(v2,10,1);
raise info 'v3 is %',v3;
raise info 'v4 is %',v4;
end;
/

call pro_cb4_031_1();

-- <1G out
create or replace procedure pro_cb4_031(c1 out clob,c2 out clob)
is
v1 clob;
v2 clob;
begin
execute immediate 'select c3 from bigclobtbl031 where c1=1' into v1;
execute immediate 'select c3 from bigclobtbl031 where c1=2' into v2;
c1:=v1;
c2:=v2;
end;
/

call pro_cb4_031_1();

--I3.clob as inout
create or replace procedure pro_cb4_031(c1 inout clob,c2 inout clob)
is
v1 clob;
v2 clob;
begin
execute immediate 'select c2 from bigclobtbl031 where c1=1' into v1;
execute immediate 'select c3 from bigclobtbl031 where c1=1' into v2;
c1:=v1;
c2:=v2;
end;
/

create or replace procedure pro_cb4_031_1 is
v1 clob;
v2 clob;
v3 clob;
v4 clob;
begin
pro_cb4_031(v1,v2);
v3:=dbe_lob.substr(v1,10,1);
v4:=dbe_lob.substr(v2,10,1);
raise info 'v3 is %',v3;
raise info 'v4 is %',v4;
end;
/

call pro_cb4_031_1();

--I4. < 1GB clob inout
create or replace procedure pro_cb4_031(c1 inout clob,c2 clob,c3 out clob)
is
v1 clob;
v2 clob;
v3 clob;
begin
execute immediate 'select c3 from bigclobtbl031 where c1=1' into v1;
execute immediate 'select c3 from bigclobtbl031 where c1=2' into v2;
execute immediate 'select c3 from bigclobtbl031 where c1=3' into v3;
c1:=v1;
c2:=v2;
c3:=v3||'clob3clob3clob3clob3';
end;
/

create or replace procedure pro_cb4_031_1 is
v1 clob;
v2 clob;
v3 clob;
v4 clob;
v5 clob;
v6 clob;
begin
pro_cb4_031(v1,v2,v3);
v4:=dbe_lob.substr(v1,10,1);
v5:=dbe_lob.substr(v2,10,1);
v6:=dbe_lob.substr(v3,10,1);
raise info 'v4 is %',v4;
raise info 'v5 is %',v5;
raise info 'v6 is %',v6;
end;
/

call pro_cb4_031_1();
--I5. table of clob
create or replace procedure pro_cb4_031 is
type ty1 is table of clob;
v1 ty1;
begin
for i in 1..10 loop
execute immediate 'select c2 from bigclobtbl031 where c1='||i into v1(i);
update bigclobtbl030 set c3=v1(i)||v1(i) where c1=i;
end loop;
end;
/

call pro_cb4_031();

-- array 
create or replace procedure pro_cb4_031 is
type ty1 is varray(10) of clob;
v1 ty1;
begin
for i in 1..10 loop
execute immediate 'select c2 from bigclobtbl031 where c1='||i into v1(i);
update bigclobtbl030 set c3=v1(i)||v1(i) where c1=i;
end loop;
end;
/

call pro_cb4_031();
select c1,c2,length(c2),c3,length(c3) from bigclobtbl031 where c1>5 and c1<10 order by 1,2,3,4,5;
update bigclobtbl031 set c3='clob3clob3';
--I6.record 
create or replace procedure pro_cb4_031 is
type ty1 is record(c1 int,c2 clob);
v1 ty1;
begin
execute immediate 'select c2 from bigclobtbl031 where c1=1' into v1.c2;
end;
/

call pro_cb4_031();

--I7 fetch 
create or replace procedure pro_cb4_037 is
v1 clob;
v2 clob;
v3 clob;
v4 int;
cursor cor1 is select c2 from bigclobtbl031 where c1=1;
begin
open cor1;
loop
fetch cor1 into v1;
fetch cor1 into v1;
fetch cor1 into v1;
fetch cor1 into v1;
fetch cor1 into v1;
exit when cor1%notfound;
end loop;
close cor1;
end;
/

call pro_cb4_037();

drop table if exists cloblongtbl;
create table cloblongtbl (a int, b clob, c clob);
insert into cloblongtbl values (generate_series(1,4),repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',5000000),repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',5000000));
update cloblongtbl set b = b||b;
update cloblongtbl set c = c||c;
update cloblongtbl set b = b||b where a = 2;
update cloblongtbl set c = c||c where a = 3;
update cloblongtbl set b = b||b where a = 4;
update cloblongtbl set c = c||c where a = 4;
select a, length(b || c) from cloblongtbl order by 1;
drop table if exists cloblongtbl;
-- clean
drop schema if exists huge_clob cascade;