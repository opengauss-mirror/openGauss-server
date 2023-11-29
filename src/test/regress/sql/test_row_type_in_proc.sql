create table type_test(a int, b char(10));
create table test_row_type(a int, b char(10));

create or replace procedure test_row_type_in_pro()
as declare
rec type_test%rowtype;
a int;
begin
select x.* , x2.a into rec, a from test_row_type();
end;
/

create or replace procedure test_row_type_in_pro()
as declare
rec type_test%rowtype;
a int;
begin
select x.* , x2.a into rec.*, a from test_row_type();
end;
/

create or replace procedure test_row_type_in_pro()
as declare
rec type_test%rowtype;
a int;
begin
select x.* into rec.* from test_row_type();
end;
/

create or replace procedure test_row_type_in_pro()
as declare
rec type_test%rowtype;
a int;
begin
select x.* into rec from test_row_type();
end;
/
