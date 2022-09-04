-- test create type table of 
-- check compatibility --
show sql_compatibility; -- expect A --

-- create new schema --
drop schema if exists huge_clob;
create schema huge_clob;
set current_schema = huge_clob;

drop table if exists cloblongtbl;
create table cloblongtbl (a int, b clob, c clob);
-- insert data less than 1G
insert into cloblongtbl values (generate_series(1,4),repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',5000000),repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',5000000));
update cloblongtbl set b = b||b;
update cloblongtbl set c = c||c;
-- b > 1G && c < 1G when a = 2
update cloblongtbl set b = b||b where a = 2;
-- b < 1G && c > 1G when a = 3
update cloblongtbl set c = c||c where a = 3;
-- b > 1G && c > 1G when a = 4
update cloblongtbl set b = b||b where a = 4;
update cloblongtbl set c = c||c where a = 4;
select a, length(b || c) from cloblongtbl order by 1;

-- reset data for other test
update cloblongtbl set b = b || b where a = 1;
update cloblongtbl set b = b || b where a = 3;
update cloblongtbl set c='cloblessthan1G' where a = 1;
update cloblongtbl set c='cloblessthan1G' where a = 2;
update cloblongtbl set c='cloblessthan1G' where a = 3;
update cloblongtbl set c='cloblessthan1G' where a = 4;


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
execute immediate 'select b from cloblongtbl where a=1' into v1;
execute immediate 'select c from cloblongtbl where a=1' into v2;
pro_cb4_031(v1,v2);
end;
/

call pro_cb4_031_1();

create or replace procedure pro_cb4_005 is
v1 clob;
v2 clob;
v3 clob;
v4 integer;
begin
execute immediate 'select b from cloblongtbl where a=1' into v1;
dbe_lob.read(v1,10,2,v2);
end;
/

call pro_cb4_005();

--I2.clob > 1G out
create or replace procedure pro_cb4_031(c1 out clob,c2 out clob)
is
v1 clob;
v2 clob;
begin
execute immediate 'select b from cloblongtbl where a=1' into v1;
execute immediate 'select c from cloblongtbl where a=1' into v2;
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
execute immediate 'select c from cloblongtbl where a=1' into v1;
execute immediate 'select c from cloblongtbl where a=2' into v2;
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
execute immediate 'select b from cloblongtbl where a=1' into v1;
execute immediate 'select c from cloblongtbl where a=1' into v2;
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
execute immediate 'select c from cloblongtbl where a=1' into v1;
execute immediate 'select c from cloblongtbl where a=2' into v2;
execute immediate 'select c from cloblongtbl where a=3' into v3;
c1:=v1;
c2:=v2;
c3:=v3||'clobclobclobclob';
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
execute immediate 'select b from cloblongtbl where a='||i into v1(i);
update cloblongtbl set c=v1(i)||v1(i) where a=i;
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
execute immediate 'select b from cloblongtbl where a='||i into v1(i);
update cloblongtbl set c=v1(i)||v1(i) where a=i;
end loop;
end;
/

call pro_cb4_031();
select a,b,length(b),c,length(c) from cloblongtbl where a>5 and a<10 order by 1,2,3,4,5;
update cloblongtbl set c='cloblessthan1G';
--I6.record 
create or replace procedure pro_cb4_031 is
type ty1 is record(c1 int,c2 clob);
v1 ty1;
begin
execute immediate 'select b from cloblongtbl where a=1' into v1.c2;
end;
/

call pro_cb4_031();

--I7 fetch 
create or replace procedure pro_cb4_037 is
v1 clob;
v2 clob;
v3 clob;
v4 int;
cursor cor1 is select b from cloblongtbl where a=1;
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

create or replace procedure test_self_update is
v1 clob;
begin
execute immediate 'select b from cloblongtbl where a=1' into v1;
update cloblongtbl set b=v1 where a=1;
savepoint aaa;
update cloblongtbl set b=v1 where a=2;
rollback to aaa;
commit;
end;
/

call test_self_update();

create or replace procedure test_update_delete is
v1 clob;
begin
execute immediate 'select b from cloblongtbl where a=1' into v1;
update cloblongtbl set b=v1 where a=1;
rollback;
update cloblongtbl set b=v1 where a=2;
commit;
end;
/

call test_update_delete();

begin;
delete from cloblongtbl where a < 3;
rollback;

begin;
delete from cloblongtbl where a = 1;
delete from cloblongtbl where a = 2;
rollback;

drop table if exists clob_sel_1;
create table clob_sel_1(c1 int,c2 clob);
insert into clob_sel_1 values(1,repeat('AAAA11111aaaaaaaaaaaa',1000000));
update clob_sel_1 set c2=c2||c2||c2||c2||c2;
update clob_sel_1 set c2=c2||c2||c2||c2||c2;
update clob_sel_1 set c2=c2||c2;
update clob_sel_1 set c2=c2||c2;

create or replace procedure test_lob_read_new
as
declare
    dest_clob clob;
	src_lob clob;
	PSV_SQL varchar(100);
begin
    PSV_SQL := 'select c2 from clob_sel_1 where c1 = 1 ';
	EXECUTE IMMEDIATE PSV_SQL into dest_clob;
    dbe_lob.read(dest_clob, 1, 1, src_lob);
	DBE_OUTPUT.print_line(src_lob);
	return;
end;
/
call test_lob_read_new();

create or replace procedure test_lob_read_into
as
declare
    dest_clob clob;
	src_lob clob;
begin
    select c2 from clob_sel_1 into dest_clob where c1 = 1;
    dbe_lob.read(dest_clob, 1, 1, src_lob);
	DBE_OUTPUT.print_line(src_lob);
	return;
end;
/
call test_lob_read_into();

create or replace procedure test_lob_append_new
as
declare
    dest_clob clob;
	src_lob clob;
	PSV_SQL varchar(100);
begin
    PSV_SQL := 'select c2 from clob_sel_1 where c1 = 1 ';
	EXECUTE IMMEDIATE PSV_SQL into dest_clob;
    src_lob := dbe_lob.append(dest_clob,'no money no work');
	update clob_sel_1 set c2=src_lob;
	return;
end;
/
call test_lob_append_new();

create or replace procedure test_lob_append_into
as
declare
    dest_clob clob;
	src_lob clob;
begin
    select c2 from clob_sel_1 into dest_clob where c1 = 1;
    src_lob := dbe_lob.append(dest_clob,'runrunrun');
	update clob_sel_1 set c2=src_lob;
	return;
end;
/
call test_lob_append_into();

-- partition
create table bigclobtbl018(c1 int,c2 clob,c3 clob,c4 blob,c5 date,c6 timestamp,c7 number(7))
partition by range(c1) (partition p1 values less than(5),partition p2 values less than(10),partition p3 values less than(maxvalue));
insert into bigclobtbl018 values(generate_series(1,30),repeat('AAAA11111李白杜甫杜牧白居易唐宋八大家',1000000),repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',1000000),hextoraw(repeat('12345678990abcdef',1000)),sysdate,to_timestamp('','yyyy-mm-dd hh24:mi:ss.ff6'),7000);
update bigclobtbl018 set c2=c2||c2||c2||c2||c2 where mod(c1,3)=1;
update bigclobtbl018 set c2=c2||c2||c2||c2||c2 where mod(c1,3)=1;
update bigclobtbl018 set c3=c3||c3 where mod(c1,3)=1;
create or replace procedure pro_cb4_018 is
v1 clob;
v2 clob;
v3 clob;
begin
v2:='v2v2v2v2v2v2';
execute immediate 'select c2 from bigclobtbl018 where c1=10' into v1;
update bigclobtbl018 set c3=v1 where c1=10;
end;
/
call pro_cb4_018();
select length(c3),length(c2) from bigclobtbl018 where c1=10;
-- sub partition
create table bigclobtbl015(c1 int,c2 text,c3 clob,c4 blob,c5 date,c6 timestamp,c7 number(7), c8 clob)
partition by range(c1) subpartition by range (c2) (partition p1 values less than(5)(subpartition p1sub1 values less than('BB'),subpartition p1sub2 values less than('CC')),partition p2 values less than(10)(subpartition p2sub1 values less than('BB'),subpartition p2sub2 values less than('CC')),partition p3 values less than(maxvalue)(subpartition p3sub1 values less than('BB'),subpartition p3sub2 values less than('CC')));
 insert into bigclobtbl015 values(generate_series(1,5),'AAAA11111李白杜甫杜牧白居易唐宋八大家',repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',1000000),hextoraw(repeat('12345678990abcdef',1000)),sysdate,to_timestamp('','yyyy-mm-dd hh24:mi:ss.ff6'),7000);
update bigclobtbl015 set c3=c3||c3||c3||c3 where  c1=1;

create or replace procedure pro_cb4_019 is
v1 clob;
v2 clob;
v3 clob;
begin
v2:='v2v2v2v2v2v2';
execute immediate 'select c3 from bigclobtbl015 where c1=1' into v1;
update bigclobtbl015 set c8=v1 where c1=1;
end;
/
call pro_cb4_019();

select length(c3) from bigclobtbl015 where c1=1;

create table cloblongtbl001(c1 int,c2 number,c3 varchar2,c4 clob,c5 blob,c6 text);
alter table cloblongtbl001 alter c3 set storage external;
alter table cloblongtbl001 alter c4 set storage external;
alter table cloblongtbl001 alter c5 set storage external;
alter table cloblongtbl001 alter c6 set storage external;
insert into cloblongtbl001 values(-1,-1,repeat('abRF中国',1000),repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',1000),HEXTORAW(repeat('12345678990abcdef',1000)),repeat('text春眠不觉晓',1000));
insert into cloblongtbl001 values(1,1,repeat('abRF中国',1000),repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',10000000),HEXTORAW(repeat('12345678990abcdef',10000000)),repeat('text春眠不觉晓',1000));
update cloblongtbl001 set c4=c4||c4 where c1=1;
insert into cloblongtbl001 values(3,3,repeat('abRF中国',10000000),repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',10000000),HEXTORAW(repeat('12345678990abcdef',10000000)),repeat('text春眠不觉晓',1000));
update cloblongtbl001 set c4=c4||c4 where c1=3;
update cloblongtbl001 set c4=c4||c2 where c1=3;
update cloblongtbl001 set c4=c4||c4 where c1=3;
insert into cloblongtbl001 values(4,4,repeat('abRF中国',1000),repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',10000000),HEXTORAW(repeat('12345678990abcdef',1000)),repeat('text春眠不觉晓',1000));
update cloblongtbl001 set c4=c4||c4 where c1=4;
update cloblongtbl001 set c4=c4||c4 where c1=4;
update cloblongtbl001 set c4=c4||c4 where c1=4;
insert into cloblongtbl001 values(6,6,repeat('abRF中国',1000),repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',10000000),HEXTORAW(repeat('12345678990abcdef',1000)),repeat('text春眠不觉晓',1000));
update cloblongtbl001 set c4=c4||c4 where c1=6;
update cloblongtbl001 set c4=c4||c4 where c1=6;
update cloblongtbl001 set c4=c4||c4 where c1=6;
update cloblongtbl001 set c4=c4||c4 where c1=6;
insert into cloblongtbl001 values(7,7,'1234455app','1234455app','1234455a','567893clob');
create table cloblongtbl001_1(c1 int,c2 clob,c3 clob);
create or replace procedure pro_cb4_001 is
v1 clob;
v2 clob;
v3 clob;
v4 integer;
begin
select c4 into v1 from cloblongtbl001 where c1=1;
select c4 into v2 from cloblongtbl001 where c1=3;
raise info 'v1 is %',length(v1);
raise info 'v2 is %',length(v2);
dbe_lob.write(v2,32767,1,v1);
raise info 'v2 is %',length(v2);
insert into cloblongtbl001_1 values(1,v2,null);
end;
/
call pro_cb4_001();

create or replace procedure pro_cb4_001 is
v1 clob;
v2 clob:='bigbibbig';
v3 clob;
v4 integer;
begin
select c4 into v1 from cloblongtbl001 where c1=7;
select c4 into v2 from cloblongtbl001 where c1=3;
dbe_lob.write(v2,length(v1),1000,v1);
update cloblongtbl001_1 set c2=v2 where c1=1;
end;
/

call pro_cb4_001();

drop table if exists cloblongtbl001;
create table cloblongtbl001(c1 int,c2 number,c3 varchar2,c4 clob,c5 blob,c6 text);
insert into cloblongtbl001 values(-1,-1,repeat('abRF中国',1000),
	repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',1000),
	HEXTORAW(repeat('12345678990abcdef',1000)),repeat('text春眠不觉晓',1000));

insert into cloblongtbl001 values(1,1,repeat('abRF中国',1000),
	repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',10000000),
	HEXTORAW(repeat('12345678990abcdef',10000000)),repeat('text春眠不觉晓',1000));

update cloblongtbl001 set c4=c4||c4 where c1=1;
insert into cloblongtbl001 values(3,3,repeat('abRF中国',10000000),
	repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',10000000),
	HEXTORAW(repeat('12345678990abcdef',10000000)),repeat('text春眠不觉晓',1000));

update cloblongtbl001 set c4=c4||c4 where c1=3;
update cloblongtbl001 set c4=c4||c2 where c1=3;
update cloblongtbl001 set c4=c4||c4 where c1=3;
insert into cloblongtbl001 values(4,4,repeat('abRF中国',1000),
	repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',10000000),
	HEXTORAW(repeat('12345678990abcdef',1000)),repeat('text春眠不觉晓',1000));

update cloblongtbl001 set c4=c4||c4 where c1=4;
update cloblongtbl001 set c4=c4||c4 where c1=4;
update cloblongtbl001 set c4=c4||c4 where c1=4;
insert into cloblongtbl001 values(6,6,repeat('abRF中国',1000),
	repeat('唐李白床前明月光，疑是地上霜，举头望明月，低头思故乡',10000000),
	HEXTORAW(repeat('12345678990abcdef',1000)),repeat('text春眠不觉晓',1000));

create or replace procedure pro_cb4_001 is
v1 clob;
v2 clob;
v3 clob;
v4 integer;
begin
  select c4 into v1 from cloblongtbl001 where c1=1;
  select c4 into v2 from cloblongtbl001 where c1=3;
  raise info 'v1 is %',length(v1);
  raise info 'v2 is %',length(v2);
  dbe_lob.write(v2,32767,1,v1);
  raise info 'v2 is %',length(v2);
end;
/

call pro_cb4_001();

drop table if exists clob_sel_1;
drop table if exists cloblongtbl;
-- clean
drop schema if exists huge_clob cascade;
