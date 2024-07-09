create schema plpgsql_pipelined_unsupported;
set search_path to plpgsql_pipelined_unsupported;

-- return no collection
CREATE OR REPLACE FUNCTION return_no_collection() returns int pipelined LANGUAGE plpgsql AS
    $BODY$
begin
    pipe row (null);
end;
$BODY$;


-- with out param
CREATE OR REPLACE FUNCTION func_with_out_param(id OUT int) returns _int4 pipelined LANGUAGE plpgsql AS
    $BODY$
begin
    pipe row (null);
end;
$BODY$;

select func_with_out_param();

-- func with out param autonomous
CREATE OR REPLACE FUNCTION func_with_out_param_autonm(id OUT int) returns _int4 pipelined LANGUAGE plpgsql AS
    $BODY$
declare
    PRAGMA AUTONOMOUS_TRANSACTION;
begin
    pipe row (null);
end;
$BODY$;
select func_with_out_param_autonm();

-- index by test
CREATE OR REPLACE PACKAGE unsupported_pkg AS
    TYPE numset_tbl IS TABLE OF number index by binary_integer;
    FUNCTION func_test(n NUMBER) RETURN numset_tbl PIPELINED;
END unsupported_pkg;
/

-- return next 
CREATE OR REPLACE FUNCTION func_test(n NUMBER) RETURN _int4 PIPELINED IS
BEGIN
    RETURN next 1;
END;
/

-- return query 
CREATE OR REPLACE FUNCTION func_test(n NUMBER) RETURN _int4 PIPELINED IS
BEGIN
    RETURN query select 1;
END;
/

-- return something
CREATE OR REPLACE FUNCTION func_test(n NUMBER) RETURN _int4 PIPELINED IS
BEGIN
    return 1;
END;
/

-- do not call return: success
CREATE OR REPLACE FUNCTION func_test(n NUMBER) RETURN _int4 PIPELINED IS
BEGIN
    /* do not return */
END;
/

--  no pipelined flag in package body
CREATE OR REPLACE PACKAGE unsupported_pkg AS
    TYPE numset_tbl IS TABLE OF NUMBER;
    FUNCTION func_test(n NUMBER) RETURN numset_tbl pipelined;
END unsupported_pkg;
/
CREATE OR REPLACE PACKAGE BODY unsupported_pkg AS
    FUNCTION func_test(n NUMBER) RETURN numset_tbl
        IS
        a numset_tbl := numset_tbl();
    BEGIN
        NULL;
    END;
END unsupported_pkg;
/


-- pipelined flag only in package body
CREATE OR REPLACE PACKAGE unsupported_pkg AS
    TYPE numset_tbl IS TABLE OF NUMBER;
    FUNCTION func_test(n NUMBER) RETURN numset_tbl;
END unsupported_pkg;
/
CREATE OR REPLACE PACKAGE BODY unsupported_pkg AS
    FUNCTION func_test(n NUMBER) RETURN numset_tbl pipelined
        IS
        a numset_tbl := numset_tbl();
    BEGIN
        NULL;
    END;
END unsupported_pkg;
/

--  setof
CREATE OR REPLACE PACKAGE unsupported_pkg IS
    TYPE numset_tbl IS TABLE OF INTEGER;
    FUNCTION func_test(n number) RETURN numset_tbl pipelined;
END unsupported_pkg;
/
CREATE OR REPLACE PACKAGE BODY unsupported_pkg AS
    FUNCTION func_test(n number) RETURN numset_tbl PIPELINED IS
    BEGIN
        FOR i IN 1..n
            LOOP
                pipe row (i);
            END LOOP;
    END;
END unsupported_pkg;
/
select *
from unsupported_pkg.func_test(generate_series(2, 4));
select unsupported_pkg.func_test(generate_series(2, 4));

-- pipelined with setof function
CREATE OR REPLACE PACKAGE unsupported_pkg IS
    FUNCTION func_test(n number) RETURN setof
    number pipelined;
END unsupported_pkg;
/

CREATE OR REPLACE FUNCTION func_test(n number) RETURN setof number pipelined IS
BEGIN
    FOR i IN 1..n
        LOOP
            pipe row (i);
        END LOOP;
END;
/

-- procedure with pipelined
CREATE OR REPLACE procedure func_test(n NUMBER) PIPELINED IS BEGIN
    NULL;
RETURN;
END;
/

-- procedure with pip row
CREATE OR REPLACE procedure func_test(n NUMBER) IS
BEGIN
    pipe row (1);
END;
/

-- multiple pipelined
CREATE OR REPLACE FUNCTION func_test(n NUMBER) RETURN numberset PIPELINED PIPELINED IS
BEGIN
    RETURN;
END;
/

-- alter function pipelined: unsupported gramer
CREATE OR REPLACE FUNCTION alter_func_pipelined(n NUMBER) RETURN int IS
BEGIN
    RETURN NULL;
END;
/
alter function alter_func_pipelined(number) pipelined;


create or replace type tb_type_0013 as table of varchar2(2000);
drop table if exists t_pipelined_0013;
create table t_pipelined_0013(c1 int);
create or replace function func_pipelined_0013(count in number)
returns tb_type_0013 pipelined language plpgsql as
$BODY$
declare
begin
for i in 1 .. count loop
insert into t_pipelined_0013 values(i);
-- pipe row( 'insert into test values( ' || i || ') success');
perform pg_sleep(1);
update t_pipelined_0013 set c1 = 10 where c1 = i;
end loop;
-- pipe row( 'All done!' );
return;
end;
$BODY$;

-- cannot perform a DML operation inside a query
select func_pipelined_0013(3);

reset search_path;
drop schema plpgsql_pipelined_unsupported cascade;
