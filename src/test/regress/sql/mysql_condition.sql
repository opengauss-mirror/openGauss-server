-- declare handler 
drop database if exists mysql_test;
drop database if exists td_test;

create database mysql_test dbcompatibility='B';
create database td_test dbcompatibility='C';

\c td_test

declare
    a int;
begin
    declare exit handler for 22012
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/

\c mysql_test
-- error_code
declare
    a int;
begin
    declare exit handler for 22012
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/

declare
    a int;
begin
    declare exit handler for 1
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/

declare
    a int;
begin
    declare exit handler for 0
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/
-- sqlstate [value] sqlstate_value
declare
    a int;
begin
    declare exit handler for sqlstate '22012'
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/

declare
    a int;
begin
    declare exit handler for sqlstate value "22012"
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/

-- condition_name
declare
    a int;
begin
    declare exit handler for DIVISION_BY_ZERO
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/
-- SQLWARNING
declare
begin
    declare exit handler for sqlwarning
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    CREATE USER pri_user_independent WITH INDEPENDENT IDENTIFIED BY "1234@abc";
end;
/

declare
begin
    declare exit handler for "sqlwarning"
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    CREATE USER pri_user_independent WITH INDEPENDENT IDENTIFIED BY "1234@abc";
end;
/

-- NOT FOUND
declare
begin
    declare exit handler for not FOUND
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    create table t_rowcompress_pglz_compresslevel(id int) with (compresstype=1,compress_level=2);
end;
/

-- sqlexception
declare
    a int;
begin
    declare exit handler for sqlexception
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/

--condition_values
declare
    a int;
begin
    declare exit handler for sqlexception, not FOUND
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/
-- declare handlers
declare
    a int;
begin
    declare exit handler for not FOUND
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    declare exit handler for sqlexception
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    create table t_rowcompress_pglz_compresslevel(id int) with (compresstype=1,compress_level=2);
    a := 1/0;
end;
/

declare
    a int;
begin
    declare exit handler for not FOUND
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    declare exit handler for sqlexception
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
    create table t_rowcompress_pglz_compresslevel(id int) with (compresstype=1,compress_level=2);
end;
/

-- use declare handler and exception when at the same time
declare
    a int;
begin
    declare exit handler for sqlexception
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
    exception when others then
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
end;
/

-- delcare continue handler for condition_value
create table declare_handler_t_continue (i INT PRIMARY KEY, j INT);
create table declare_handler_t_exit (i INT PRIMARY KEY, j INT);

CREATE OR REPLACE PROCEDURE proc_continue_sqlexception()  IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        RAISE NOTICE 'SQLEXCEPTION HANDLER: SQLSTATE = %, SQLERRM = %', SQLSTATE, SQLERRM;

    INSERT INTO declare_handler_t_continue VALUES (1, 1);
    INSERT INTO declare_handler_t_continue VALUES (2, 1);
    RAISE division_by_zero;
    INSERT INTO declare_handler_t_continue VALUES (1, 1);
    INSERT INTO declare_handler_t_continue VALUES (3, 1);
END;
/
call proc_continue_sqlexception();
SELECT * FROM declare_handler_t_continue ORDER BY i;
TRUNCATE TABLE declare_handler_t_continue;

-- declare continue handler
CREATE OR REPLACE PROCEDURE proc_continue_sqlexception()  IS
BEGIN
    DECLARE CONTINUE HANDLER FOR unique_violation
        RAISE NOTICE 'SQLEXCEPTION HANDLER: SQLSTATE = %, SQLERRM = %', SQLSTATE, SQLERRM;

    INSERT INTO declare_handler_t_continue VALUES (1, 1);
    INSERT INTO declare_handler_t_continue VALUES (2, 1);
    INSERT INTO declare_handler_t_continue VALUES (1, 1);
    INSERT INTO declare_handler_t_continue VALUES (3, 1);
END;
/
call proc_continue_sqlexception();
SELECT * FROM declare_handler_t_continue ORDER BY i;
-- declare exit handler
CREATE OR REPLACE PROCEDURE proc_ex()  IS
BEGIN
    DECLARE EXIT HANDLER FOR unique_violation
        RAISE NOTICE 'unique_violation HANDLER: SQLSTATE = %, SQLERRM = %', SQLSTATE, SQLERRM;

    INSERT INTO declare_handler_t_exit VALUES (1, 1);
    INSERT INTO declare_handler_t_exit VALUES (2, 1);
    INSERT INTO declare_handler_t_exit VALUES (1, 1); /* duplicate key */
    INSERT INTO declare_handler_t_exit VALUES (3, 1);
END;
/
call proc_ex();
SELECT * FROM declare_handler_t_exit ORDER BY i;
CREATE OR REPLACE PROCEDURE proc_null()  IS
BEGIN
    DECLARE EXIT HANDLER FOR unique_violation
        RAISE NOTICE 'unique_violation HANDLER: SQLSTATE = %, SQLERRM = %', SQLSTATE, SQLERRM;
END;
/
call proc_null();
CREATE TABLE tb1(
col1 INT PRIMARY KEY,
col2 text
);
CREATE OR REPLACE PROCEDURE proc1(IN col1 INT, IN col2 text) AS
DECLARE result VARCHAR;
declare pragma autonomous_transaction;
BEGIN
DECLARE CONTINUE HANDLER FOR 23505
begin
RAISE NOTICE 'SQLSTATE = %',SQLSTATE;
end;
if col1>10 then  
INSERT INTO tb1 VALUES(col1,'lili');
END IF;  
IF col1 <= 10 THEN
INSERT INTO tb1(col1,col2) VALUES(col1,col2);
commit;
ELSE
INSERT INTO tb1(col1,col2) VALUES(col1,col2);
rollback;
END IF;
END;
/
call proc1(1, 1);
call proc1(1, 5);
call proc1(11, 11);
call proc1(11, 5);
select * from tb1;

CREATE OR REPLACE PROCEDURE proc1(IN a text) AS
BEGIN
if a='22012' then
raise info 'zero error';
else
raise info 'emmm....';
end if;
end;
/
CREATE OR REPLACE PROCEDURE proc2(IN var1 int,var2 int) AS
begin
DECLARE CONTINUE HANDLER FOR sqlstate'22012'
begin
RAISE NOTICE 'SQLSTATE = %',SQLSTATE;
var1=0;
end;
var1= var1 / var2;
RAISE INFO 'result: %', var1;
END;
/
CREATE OR REPLACE PROCEDURE proc3(a1 int,b1 int) AS
BEGIN
DECLARE CONTINUE HANDLER FOR sqlstate'22012',sqlstate'0A000'
begin
RAISE NOTICE 'SQLSTATE = %',SQLSTATE;
perform proc1(SQLSTATE);
end;
a1=a1/b1;
IF b1 = 0 THEN
raise info 'b1 is zero';
create table tb1();
perform proc2(b1, a1);
END IF;
raise info 'END';
END;
/
CALL proc3(1,0);
CALL proc3(0,0);
create table company(name varchar(100), loc varchar(100), no integer PRIMARY KEY);
insert into company values ('macrosoft',    'usa',          001);
insert into company values ('oracle',       'usa',          002);
insert into company values ('backberry',    'canada',       003);
create or replace procedure test_cursor_handler()
as

  declare company_name    varchar(100);
  declare company_loc varchar(100);
  declare company_no  integer;
begin
  DECLARE CONTINUE HANDLER FOR unique_violation 
  begin 
    RAISE NOTICE 'SQLSTATE = %',SQLSTATE;
  end;
  declare c1_all cursor is --cursor without args 
      select name, loc, no from company order by 1, 2, 3;
  if not c1_all%isopen then
      open c1_all;
  end if;
  loop
      fetch c1_all into company_name, company_loc, company_no;
      exit when c1_all%notfound;
      insert into company values (company_name,company_loc,company_no);
      raise notice '% : % : %',company_name,company_loc,company_no;
  end loop;
  if c1_all%isopen then
      close c1_all;
  end if;
end;
/
call test_cursor_handler();
\c regression
drop database mysql_test;
drop database td_test;
