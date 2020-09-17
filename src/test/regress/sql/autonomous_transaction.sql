create table at_tb2(id int, val varchar(20));
create or replace function at_test2(i int) returns integer
LANGUAGE plpgsql
as $$
declare
pragma autonomous_transaction;
begin
START TRANSACTION;
insert into at_tb2 values(1, 'before s1');
if i > 10 then
rollback;
else
commit;
end if;
return i;
end;
$$;
select at_test2(15);
select * from at_tb2;
select at_test2(5);
select * from at_tb2;

truncate table at_tb2;
create or replace procedure at_test3(i int)
AS 
DECLARE
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  START TRANSACTION;
  insert into at_tb2 values(1, 'before s1');
  insert into at_tb2 values(2, 'after s1');
  if i > 10 then
  rollback;
  else
  commit;
  end if;
end;
/
call at_test3(6);
select * from at_tb2;

truncate table at_tb2;
create or replace procedure at_test4(i int)
AS 
DECLARE
BEGIN
  insert into at_tb2 values(3, 'klk');
  PERFORM at_test3(6);
  insert into at_tb2 values(4, 'klk');
  PERFORM at_test3(15);
end;
/
select at_test4(6);
select * from at_tb2;

truncate table at_tb2;
create or replace procedure at_test5(i int)
AS 
DECLARE
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  insert into at_tb2 values(3, 'klk');
  commit;
end;
/
select at_test5(6);
select * from at_tb2;

truncate table at_tb2;
create or replace procedure at_test6(i int)
AS 
DECLARE
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  insert into at_tb2 values(3, 'klk');
  rollback;
end;
/
select at_test6(6);
select * from at_tb2;

truncate table at_tb2;
DECLARE
begin
insert into at_tb2 values(1, 'begin');
PERFORM at_test3(6);
end;
/
select * from at_tb2;

truncate table at_tb2;
begin;
insert into at_tb2 values(1, 'begin');
select * from at_tb2;
call at_test3(6);
select * from at_tb2;
rollback;
select * from at_tb2;

create table at_test1 (a int);
create or replace procedure autonomous_test()
AS 
declare
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  START TRANSACTION;
  for i in 0..9 loop
  if i % 2 = 0 then
  execute 'insert into at_test1 values ('||i::integer||')';
  end if;
  end loop;
  commit;
end;
/

truncate table at_test1;
begin;
insert into at_test1 values(1);
select * from at_test1;
call autonomous_test();
select * from at_test1;
rollback;
select * from at_test1;


create or replace function autonomous_test2() returns integer
LANGUAGE plpgsql
as $$
declare
PRAGMA AUTONOMOUS_TRANSACTION;
begin
START TRANSACTION;
for i in 0..9 loop
  if i % 2 = 0 then
  execute 'insert into at_test1 values ('||i::integer||')';
  end if;
  end loop;
  commit;
  return 42;
  end;
$$;
truncate table at_test1;
begin;
insert into at_test1 values(20);
select * from at_test1;
select autonomous_test2();
select * from at_test1;
rollback;
select * from at_test1;

create or replace function autonomous_test3() returns text
LANGUAGE plpgsql
as $$
declare
PRAGMA AUTONOMOUS_TRANSACTION;
begin
START TRANSACTION;
for i in 0..9 loop
  if i % 2 = 0 then
  execute 'insert into at_test1 values ('||i::integer||')';
  end if;
  end loop;
  commit;
  return 'autonomous_test3 end';
  end;
$$;
truncate table at_test1;
begin;
insert into at_test1 values(30);
select * from at_test1;
select autonomous_test3();
select * from at_test1;
rollback;
select * from at_test1;

CREATE TABLE cp_test1 (a int, b text);
CREATE TABLE cp_test2 (a int, b text);
CREATE TABLE cp_test3 (a int, b text);
CREATE OR REPLACE FUNCTION autonomous_cp() RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    START TRANSACTION;
    insert into cp_test1 values(1,'a'),(2,'b');
	insert into cp_test2 values(1,'c'),(2,'d');
	with s1 as (select cp_test1.a, cp_test1.b from cp_test1 left join cp_test2 on cp_test1.a = cp_test2.a) insert into cp_test3 select * from s1;
    COMMIT;
  RETURN 42;
END;
$$;
select autonomous_cp();
select * from cp_test3;

CREATE TABLE tg_test1 (a int, b varchar(25), c timestamp, d int);
CREATE TABLE tg_test2 (a int, b varchar(25), c timestamp, d int);
CREATE OR REPLACE FUNCTION tri_insert_test2_func() RETURNS TRIGGER AS
$$
DECLARE
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
insert into tg_test2 values(new.a,new.b,new.c,new.d);
RETURN NEW;
commit;
END
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER TG_TEST2_TEMP
before insert
ON  tg_test1
FOR EACH ROW
EXECUTE PROCEDURE tri_insert_test2_func();
insert into tg_test1 values(1,'a','2020-08-13 09:00:00', 1);  
  
