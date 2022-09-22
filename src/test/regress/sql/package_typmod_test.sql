create type ty1 is (id int,cat1 bit(5));
create or replace package pak1 as
var ty1;
type tp_tb1 is table of var.cat1%type;
tb1 tp_tb1;
procedure p1;
end pak1 ;
/
create or replace package body pak1 as
procedure p1 as
begin
tb1=tp_tb1(12,13,5,0);
raise info '%', tb1;
end;
end pak1;
/
call pak1.p1();