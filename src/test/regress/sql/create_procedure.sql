create procedure test_procedure_test(int,int)
SHIPPABLE IMMUTABLE
as
begin
	select $1 + $2;
end;
/
drop procedure test_procedure_test;