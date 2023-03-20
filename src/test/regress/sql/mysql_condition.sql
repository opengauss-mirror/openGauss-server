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
\c regression
drop database mysql_test;
drop database td_test;
