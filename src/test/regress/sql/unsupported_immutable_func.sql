drop procedure procedure_name_immutable;

create or replace procedure procedure_name_immutable
IMMUTABLE
NOT SHIPPABLE NOT LEAKPROOF
STRICT EXTERNAL
SECURITY INVOKER PACKAGE
COST 480 ROWS 528
IS
begin
NULL;
rollback;
end;
/

