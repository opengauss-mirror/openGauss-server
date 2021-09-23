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
