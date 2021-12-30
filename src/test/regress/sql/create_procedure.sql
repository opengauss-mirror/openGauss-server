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
