-- test insert into table values record
-- check compatibility --
show sql_compatibility; -- expect A --

-- create new schema --
drop schema if exists plpgsql_table;
create schema plpgsql_table;
set current_schema = plpgsql_table;
set behavior_compat_options='';
create table record_cursor_tbl(result varchar2(10), mod number);
insert into record_cursor_tbl values('a',2);

create or replace procedure record_cursor_p1
as
begin
    for rec in (select a.mod || a.result, a.* from record_cursor_tbl a) loop
        insert into record_cursor_tbl values(rec);
        null;
    end loop;
end;
/
call record_cursor_p1();
drop procedure record_cursor_p1;
drop table record_cursor_tbl;
 
set behavior_compat_options='allow_procedure_compile_check';
create table plpgsql_table.insert_table(a int, b int);
create table plpgsql_table.insert_table2(a int, b int);
create type plpgsql_table.ComType as (a int, b int);

-- normal insert record type.
create or replace function testInsertRecord() RETURNS int as $$
declare
TYPE RR1 is record(a int, b int);
r RR1;
r1 ComType;
TYPE AA1 is varray(100) of RR1;
TYPE AA2 is varray(100) of ComType;
TYPE TT1 is table of RR1;
TYPE TT2 is table of ComType;
a1 AA1;
a2 AA2;
t1 TT1;
t2 TT2;
begin
r = (1,1);
r1 = (1,1);
insert into insert_table values r;
insert into insert_table values r1;
insert into insert_table values(r.a, r.b);

a1[0] = (2,2);
a1[1] = (3,3);
insert into insert_table values a1[0];
insert into insert_table values a1(1);

a2[0] = (4,4);
a2[1] = (5,5);
insert into insert_table values a2[0];
insert into insert_table values a2(1);

t1(0) = (6,6);
t1(1) = (7,7);
insert into insert_table values t1[0];
insert into insert_table values t1(1);

t2(0) = (8,8);
t2(1) = (9,9);
insert into insert_table values t2[0];
insert into insert_table values t2(1);

return 1;
end;
$$ language plpgsql;

-- insert unsupport type variable.
create or replace function testInsertRecordError1() RETURNS int as $$
declare
i int;
begin
i = 1;
insert into insert_table values i;
return 1;
end;
$$ language plpgsql;

create or replace function testInsertRecordError2() RETURNS int as $$
declare
TYPE RR1 is record(a int, b int);
r RR1;
i int;
begin
r = (1,1);
i = 1;
insert into insert_table values(1,1) r;
return 1;
end;
$$ language plpgsql;

create or replace function testInsertRecordError3() RETURNS int as $$
declare
TYPE RR1 is record(a int, b int);
r RR1;
r1 RR1;;
begin
r = (1,1);
r1 = (2,2);
insert into insert_table values r, r1;
return 1;
end;
$$ language plpgsql;

create or replace function testInsertRecordError4() RETURNS int as $$
declare
TYPE RR1 is record(a int, b int);
TYPE AA1 is varray(100) of RR1;
a1 AA1;
begin
a1[0] = (1,1);
a1[1] = (2,2);
insert into insert_table values a1;
return 1;
end;
$$ language plpgsql;

create or replace function testInsertRecordError5() RETURNS int as $$
declare
TYPE RR1 is record(a int, b int);
TYPE AA1 is table of RR1;
a1 AA1;
begin
a1[0] = (1,1);
a1[1] = (2,2);
insert into insert_table values a1;
return 1;
end;
$$ language plpgsql;


create or replace function testInsertRecordError6() RETURNS int as $$
declare
TYPE RR1 is record(a int, b int);
TYPE AA1 is table of RR1;
a1 AA1;
begin
a1[0] = (1,1);
a1[1] = (2,2);
insert into insert_table values a1[0], a1[1];
return 1;
end;
$$ language plpgsql;

select testInsertRecord();
select testInsertRecordError4();
select testInsertRecordError5();

create or replace function testForInsertRec() RETURNS int as $$
declare
begin
for rec in (select a, b from insert_table) loop
insert into insert_table2 values rec;
end loop;
return 1;
end;
$$ language plpgsql;

create or replace function testForInsertRecError1() RETURNS int as $$
declare
begin
for rec in (select a, b, 1 from insert_table) loop
insert into insert_table2 values rec;
end loop;
return 1;
end;
$$ language plpgsql;

select testForInsertRec();
select testForInsertRecError1();

select * from insert_table;
select * from insert_table2;

reset behavior_compat_options;
drop table insert_table;
drop table insert_table2;
drop type ComType;
drop function testInsertRecord;
drop function testInsertRecordError1;
drop function testInsertRecordError2;
drop function testInsertRecordError3;
drop function testInsertRecordError4;
drop function testInsertRecordError5;
drop function testInsertRecordError6;
drop function testForInsertRec;
drop function testForInsertRecError1;
drop schema if exists plpgsql_table;
