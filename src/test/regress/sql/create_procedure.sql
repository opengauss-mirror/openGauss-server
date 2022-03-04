create procedure test_procedure_test(int,int)
SHIPPABLE IMMUTABLE
as
begin
	select $1 + $2;
end;
/

create or replace function test2(space boolean default true) return integer as
declare result integer;
begin
if(space is null) then
perform oracle."put$json_printer.pretty_print".test1(12) into result;
return result;
else
return oracle."put$json_printer.pretty_print".test1(15);
end if;
end;
/

drop function test2;
drop procedure test_procedure_test;


create schema "test.test.test";

CREATE OR REPLACE PROCEDURE "test.test.test".prc_add
(
    param1    IN   INTEGER,
    param2    IN OUT  INTEGER
)
AS
BEGIN
   param2:= param1 + param2;
   dbe_output.print_line('result is: '||to_char(param2));
END;
/

CREATE OR REPLACE PROCEDURE "test.test.test".prc_add2
(
    param1    IN   INTEGER,
    param2    IN   INTEGER
)
AS
BEGIN
   "test.test.test".prc_add(param1, param2);
END;
/

drop procedure "test.test.test".prc_add2;
drop procedure "test.test.test".prc_add;
drop schema "test.test.test";

set behavior_compat_options='allow_procedure_compile_check';
drop table if exists bbb;
drop table if exists aaa;
CREATE TABLE bbb(id1 INT, id2 INT, id3 INT);
CREATE TABLE aaa(id1 INT, id2 INT, id3 INT);
CREATE OR REPLACE FUNCTION tri_insert_func() RETURNS TRIGGER AS
$$
DECLARE
v int;
BEGIN
select count(1) INTO v from bbb where id1 = NEW.id1;
RAISE INFO 'v1: : %' ,v;
RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
CREATE TRIGGER insert_trigger11
BEFORE INSERT ON aaa
FOR EACH ROW
EXECUTE PROCEDURE tri_insert_func();
insert into aaa values(1,2,3);
select * from aaa;
drop TRIGGER insert_trigger11 ON aaa;
drop FUNCTION tri_insert_func;
drop table if exists bbb;
drop table if exists aaa;
create or replace function checkqweerr(a integer) returns int as $$
declare
b int;
begin
select multi_call211(a) + 1 into b;
return b;
end;
$$ language plpgsql;
call checkqweerr(1);
create or replace procedure checkipoooowdsd2() as
declare
c1 sys_refcursor;
begin
open c1 for delete from tb_test111;
end;
/
call checkipoooowdsd2();
set behavior_compat_options='';
