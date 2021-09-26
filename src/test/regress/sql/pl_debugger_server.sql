-- setups
drop schema if exists pl_debugger cascade;
create schema pl_debugger;
set search_path = pl_debugger;
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

create table show_code_table(lineno int, code text, canBreak bool);

do $$
declare
    funcoid oid;
begin
    select oid from pg_proc into funcoid where proname = 'test_debug';
    INSERT INTO show_code_table SELECT * FROM dbe_pldebugger.info_code(funcoid);
end;
$$;

select * from show_code_table;

create table debug_info(nodename text, port smallint);

create function turn_on_debugger(funcname text)
    returns int
as $$
declare
    funcoid bigint;
    cnt int;
begin
    select count(*) from pg_proc into cnt where proname = funcname;
    if cnt != 1 then
        raise exception 'There are multiple/no function with name %', funcname;
    end if;

    select oid from pg_proc into funcoid where proname = funcname;
    insert into debug_info select * from dbe_pldebugger.turn_on(funcoid);
    return 0;
end
$$ language plpgsql;

-- next without attach
select * from dbe_pldebugger.next();

-- info locals without attach
select * from dbe_pldebugger.info_locals();

-- attach fail (target not turned on)
select * from dbe_pldebugger.attach('sgnode', 1);

-- abort without attach
select * from dbe_pldebugger.abort();

-- turn off without turn on
select * from dbe_pldebugger.turn_off(1);

-- turn on dropped function
CREATE OR REPLACE FUNCTION pld_simple(a int)
 RETURNS boolean
AS $$
declare
begin
    insert into test values(a);
    return true;
end
$$ language plpgsql;

do $$
declare
    funcoid bigint;
begin
    select oid from pg_proc into funcoid where proname = 'pld_simple';
    drop function pld_simple;
    insert into debug_info select * from dbe_pldebugger.turn_on(funcoid);
end;
$$;

truncate debug_info;

-- turn on language sql function
select dbe_pldebugger.turn_on(f.oid) from pg_proc f, pg_language l where f.prolang = l.oid and l.lanname = 'sql' limit 1;
-- turn on language java function (no such thing)
-- turn on language internal function
select dbe_pldebugger.turn_on(f.oid) from pg_proc f, pg_language l where f.prolang = l.oid and l.lanname = 'internal' limit 1;
-- turn on language c function
select dbe_pldebugger.turn_on(f.oid) from pg_proc f, pg_language l where f.prolang = l.oid and l.lanname = 'c' limit 1;
-- turn on correctly
truncate debug_info;

select * from turn_on_debugger('test_debug');

select * from dbe_pldebugger.local_debug_server_info();

select * from debug_info;

-- start debug - 1st run
select * from test_debug(4);

-- start debug - 2nd run - to be aborted
select * from test_debug(4);

-- commit/rollback in procedure
create table tb1(a int);
create or replace procedure test_debug2 as
begin
    insert into tb1 values (1000);
    commit;
    insert into tb1 values (2000);
    rollback;
end;
/

truncate debug_info;

select * from turn_on_debugger('test_debug2');

select * from dbe_pldebugger.local_debug_server_info();

select * from debug_info;

select dbe_pldebugger.turn_off(oid) from pg_proc where proname = 'test_debug';

-- start debug
select * from test_debug2();


-- test for implicit variables
CREATE OR REPLACE PROCEDURE test_debug3(a in integer, b out integer)
AS
BEGIN
    CASE a
        WHEN 1 THEN
            b := 111;
        ELSE
            b := 999;
    END CASE;
    raise info 'pi_return : %',pi_return ;
    EXCEPTION WHEN others THEN
        b := 101;
END;
/

truncate debug_info;

select * from turn_on_debugger('test_debug3');

select * from dbe_pldebugger.local_debug_server_info();

select * from debug_info;

select dbe_pldebugger.turn_off(oid) from pg_proc where proname = 'test_debug2';

-- start debug
select * from test_debug3(1);



-- test for step into
CREATE OR REPLACE PROCEDURE test_debug4(a in integer, b out integer)
AS
BEGIN
    CASE a
        WHEN 1 THEN
            b := 111;
            call test_debug(a);
        ELSE
            b := 999;
    END CASE;
    raise info 'pi_return : %',pi_return ;
    EXCEPTION WHEN others THEN
        b := 101;
END;
/

truncate debug_info;

select * from turn_on_debugger('test_debug4');

select * from dbe_pldebugger.local_debug_server_info();

select * from debug_info;

select * from test_debug4(1);

-- test with breakpoint
select * from test_debug4(1);

select dbe_pldebugger.turn_off(oid) from pg_proc where proname = 'test_debug3';

select dbe_pldebugger.turn_off(oid) from pg_proc where proname = 'test_debug4';

create or replace function test_debug_recursive (ct int, pr int)
returns table (counter int, product int)
language plpgsql
as $$
begin
    return query select ct, pr;
    if ct < 5 then
        return query select * from test_debug_recursive(ct+ 1, pr * (ct+ 1));
    end if;
end $$;

truncate debug_info;

select * from turn_on_debugger('test_debug_recursive');

select * from test_debug_recursive (1, 1);

drop schema pl_debugger cascade;

