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

--- prepare
drop table if exists tt_in;
drop table if exists tt_main;
drop table if exists tt01;
drop table if exists tt02;
drop table if exists tt_sess;
create table tt_in (id int,a date);
create table tt_main (id int,a date);
create table tt01(c1 int);
create table tt02(c1 int, c2 int);
create table tt_sess (id int,a date);

--- case 1
CREATE OR REPLACE PROCEDURE autonomous_tt_in_p_062(num1 int,num2 int)  AS 
 DECLARE num3 int := 4;
 PRAGMA AUTONOMOUS_TRANSACTION;
 BEGIN
 START TRANSACTION; 
 SET local TRANSACTION ISOLATION LEVEL REPEATABLE READ; 
 insert into tt_in select id,a from tt_main; 
 commit;
 END;
 /
 SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ;
start transaction ;
insert into tt_main values(35,sysdate);
select autonomous_tt_in_p_062(1,35);              
COMMIT;
 
 
--- case 2
 CREATE FUNCTION ff2(v int, v2 int) RETURNS void AS $$
    DECLARE
PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO tt02 VALUES (v, v2);
    END;
$$ LANGUAGE plpgsql;
select ff2(1,1);

create or replace function perform_select(i int) returns integer
LANGUAGE plpgsql
as $$
declare
PRAGMA AUTONOMOUS_TRANSACTION;
begin

perform *from tt01 where c1=i;
return i;
end;
$$;
select perform_select(1);

--- case 3
CREATE FUNCTION ff_into1(v int) RETURNS void AS $$
DECLARE
num int;
PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        select *from tt01 limit 1 into num;
    END;
$$ LANGUAGE plpgsql;
select ff_into1(1);

CREATE FUNCTION ff_into2(v int) RETURNS void AS $$
DECLARE
num int;
PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        execute 'select *from tt01 limit 1' into num;
END;
$$ LANGUAGE plpgsql;
select ff_into2(1);

--- case 4

CREATE OR REPLACE FUNCTION autonomous_f_test(num1 int) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
START TRANSACTION;
insert into tt_sess values(1,2,3);
commit;
RETURN 42;
END;
$$;

select autonomous_f_test(1);

CREATE OR REPLACE FUNCTION autonomous_tt_sess_f_107(num1 int,num2 int) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
num3 int := 4;
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN 
	FOR i IN 0..9 LOOP    
		START TRANSACTION;
		insert into tt_sess values (1,sysdate); 
		EXECUTE 'INSERT INTO tt_sess VALUES (' || num3+i::text || '，sysdate)';   
		IF i % 2 = 0 THEN
			COMMIT;    
		ELSE
			ROLLBACK;    
		END IF;  
	END LOOP;  
	RETURN num1+num2+num3;
END;
$$;

select autonomous_tt_sess_f_107(1,2);	



CREATE OR REPLACE FUNCTION autonomous_e_test(num1 int) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
BEGIN
START TRANSACTION;
insert into tt_sess values(1,2,3);
commit;
RETURN 42;
EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE 'exception happens';
		return -1;
END;		
END;
$$;

select autonomous_e_test(1);

CREATE FUNCTION ff_subblock() RETURNS void AS $$
DECLARE
num int;
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    BEGIN
        insert into tt01 values(1);
    END;
    BEGIN
        insert into tt01 values(2);
    END;
END;
$$ LANGUAGE plpgsql;

begin;
insert into tt01 values(3);
select ff_subblock();
rollback;

select* from tt01;

--- clean
drop table if exists tt_in;
drop table if exists tt_main;
drop table if exists tt01;
drop table if exists tt02;
drop table if exists tt_sess;



