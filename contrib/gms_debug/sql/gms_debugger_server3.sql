-- setups
create extension if not exists gms_debug;
drop schema if exists gms_debugger_test3 cascade;
create schema gms_debugger_test3;
set search_path = gms_debugger_test3;

-- test for implicit variables
CREATE OR REPLACE function test_debug3(a in integer) return integer
AS
declare
b int;
BEGIN
    CASE a
        WHEN 1 THEN
            b := 111;
        ELSE
            b := 999;
    END CASE;
    raise info 'pi_return : %',pi_return ;
    return b;
    EXCEPTION WHEN others THEN
        b := 101;
    return b;
END;
/

select * from gms_debug.initialize();

select pg_sleep(1);

-- start debug - 1st run
select * from test_debug3(1);

-- start debug - 2nd run - to be aborted
select * from test_debug3(1);

select * from gms_debug.debug_off();

drop schema gms_debugger_test3 cascade;
