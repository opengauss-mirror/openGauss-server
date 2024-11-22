-- setups
create extension if not exists gms_debug;
drop schema if exists gms_debugger_test4 cascade;
create schema gms_debugger_test4;
set search_path = gms_debugger_test4;

create table test(a int, b varchar(40), c timestamp);
insert into test values (2, 'Giving to the Needy', '2020-02-02');
insert into test values (3, 'Prayer', '2021-12-02');
insert into test values (5, 'Fasting', '2030-03-02');
insert into test values (7, 'Treasures in Heaven', '2040-04-02');

CREATE OR REPLACE FUNCTION test_debug(x int) RETURNS SETOF test AS
$BODY$
DECLARE
    sql_stmt VARCHAR2(500);
    r test%rowtype;
    rec record;
    b_tmp text;
    cnt int;
    a_tmp int;
    cur refcursor;
    n_tmp NUMERIC(24,6);
    t_tmp tsquery;
    CURSOR cur_arg(criterion INTEGER) IS
        SELECT * FROM test WHERE a < criterion;
BEGIN
    cnt := 0;
    FOR r IN SELECT * FROM test
    WHERE a > x
    LOOP
        RETURN NEXT r;
    END LOOP;

    FOR rec in SELECT * FROM test
    WHERE a < x
    LOOP
        RETURN NEXT rec;
    END LOOP;

    FORALL index_1 IN 0..1
        INSERT INTO test VALUES (index_1, 'Happy Children''s Day!', '2021-6-1');

    SELECT b FROM test where a = 7 INTO b_tmp;
    sql_stmt := 'select a from test where b = :1;';
    OPEN cur FOR sql_stmt USING b_tmp;
    IF cur%isopen then LOOP
        FETCH cur INTO a_tmp;
        EXIT WHEN cur%notfound;
        END LOOP;
    END IF;
    CLOSE cur;
    WHILE cnt < 3 LOOP
        cnt := cnt + 1;
    END LOOP;

    RAISE INFO 'cnt is %', cnt;

    RETURN;

END
$BODY$
LANGUAGE plpgsql;

-- test for step into
CREATE OR REPLACE FUNCTION test_debug4(a in integer) return integer
AS
declare
b int;
BEGIN
    CASE a
        WHEN 1 THEN
            b := 111;
            call test_debug(a);
        ELSE
            b := 999;
    END CASE;
    return b;
    raise info 'pi_return : %',pi_return ;
    EXCEPTION WHEN others THEN
        b := 101;
    return b;
END;
/

select * from gms_debug.initialize();

select pg_sleep(1);

select * from test_debug4(1);

-- test with client error in exception
select * from test_debug4(1);

select * from gms_debug.debug_off();

drop schema gms_debugger_test4 cascade;
