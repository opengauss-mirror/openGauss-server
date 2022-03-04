-- wait for server establishment
select pg_sleep(3);

set search_path = pl_debugger;

-- only one record is expected
select count(*) from debug_info;

-- attach debug server
select dbe_pldebugger.attach(nodename, port) from debug_info;

select * from dbe_pldebugger.info_locals();

create table tmp_holder (res text);

do $$
declare
    funcoid oid;
begin
    select oid from pg_proc into funcoid where proname = 'test_debug';
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 0); -- negative
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 5); -- headerline
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 15); -- ok
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 17); -- invalid
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 22); -- ok
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 15); -- duplicate
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 29); -- ok
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 32); -- ok
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 35); -- ok
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 39); -- ok
    insert into tmp_holder select * from dbe_pldebugger.disable_breakpoint(4); -- ok
    insert into tmp_holder select * from dbe_pldebugger.disable_breakpoint(5); -- ok
    insert into tmp_holder select * from dbe_pldebugger.enable_breakpoint( 5); -- ok
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 50); -- exceeds
    insert into tmp_holder select * from dbe_pldebugger.delete_breakpoint(1);  -- ok
    insert into tmp_holder select * from dbe_pldebugger.delete_breakpoint(0);  -- ok
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 15); -- add new bp at previously deleted one, bp id should proceed
    insert into tmp_holder select breakpointno || ':' || lineno || ':' || query || ':' || enable from dbe_pldebugger.info_breakpoints();
end;
$$;

select * from tmp_holder;

select * from dbe_pldebugger.print_var('test.c');

select * from dbe_pldebugger.print_var('<UNKNOWN>');

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select * from dbe_pldebugger.info_locals();

select * from dbe_pldebugger.print_var('cur');

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select * from dbe_pldebugger.info_locals();

select * from dbe_pldebugger.print_var('rec');

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select * from dbe_pldebugger.info_locals();

select * from dbe_pldebugger.print_var('row');

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select * from dbe_pldebugger.info_locals();

select * from dbe_pldebugger.print_var('b_tmp');

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.continue();

-- wait again
select pg_sleep(1);

select dbe_pldebugger.attach(nodename, port) from debug_info;

select * from dbe_pldebugger.abort();

-- wait for test_debug2
select pg_sleep(1);

select dbe_pldebugger.attach(nodename, port) from debug_info;

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

-- wait for test_debug3
select pg_sleep(1);

select dbe_pldebugger.attach(nodename, port) from debug_info;

select funcname, lineno, query from dbe_pldebugger.next();

select * from dbe_pldebugger.info_locals();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.continue();

select funcname, lineno, query from dbe_pldebugger.next();

-- wait for test_debug4
select pg_sleep(1);

select dbe_pldebugger.attach(nodename, port) from debug_info;

select funcname, lineno, query from dbe_pldebugger.step();

select * from dbe_pldebugger.info_locals();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.continue();

select funcname, lineno, query from dbe_pldebugger.continue();

-- test with client error in exception
select pg_sleep(1);

select dbe_pldebugger.attach(nodename, port) from debug_info;

SELECT funcname, lineno, query FROM DBE_PLDEBUGGER.step(); 

SELECT funcname, lineno, query FROM DBE_PLDEBUGGER.step(); 

SELECT funcname, lineno, query FROM DBE_PLDEBUGGER.step(); 

SELECT funcname, lineno, query FROM DBE_PLDEBUGGER.step(); 

SELECT funcname, lineno, query FROM DBE_PLDEBUGGER.step(); 

SELECT * FROM DBE_PLDEBUGGER.abort();

-- test with breakpoint

select pg_sleep(1);

select dbe_pldebugger.attach(nodename, port) from debug_info;

truncate tmp_holder;

do $$
declare
    funcoid oid;
    funcoid2 oid;
begin
    select oid from pg_proc into funcoid where proname = 'test_debug';
    select oid from pg_proc into funcoid2 where proname = 'test_debug4';
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 31);
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 44);
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid2, 12);
    insert into tmp_holder select breakpointno || ':' || lineno || ':' || query from dbe_pldebugger.info_breakpoints();
end;
$$;

select * from tmp_holder;

select funcname, lineno, query from dbe_pldebugger.continue();

select * from dbe_pldebugger.info_locals();

select funcname, lineno, query from dbe_pldebugger.continue();

select frameno, funcname, lineno, query from dbe_pldebugger.backtrace();

select * from dbe_pldebugger.info_locals();

select funcname, lineno, query from dbe_pldebugger.continue();

select frameno, funcname, lineno, query from dbe_pldebugger.backtrace();

select * from dbe_pldebugger.info_locals();

select funcname, lineno, query from dbe_pldebugger.continue();

select funcname, lineno, query from dbe_pldebugger.continue();

-- test with finish without encountered breakpoint

select pg_sleep(1);

select dbe_pldebugger.attach(nodename, port) from debug_info;

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.finish();

select funcname, lineno, query from dbe_pldebugger.finish();

-- test with finish with encountered breakpoint and inner error

select pg_sleep(1);

select dbe_pldebugger.attach(nodename, port) from debug_info;

truncate tmp_holder;

do $$
declare
    funcoid oid;
begin
    select oid from pg_proc into funcoid where proname = 'test_debug';
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 31);
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 44);
    insert into tmp_holder select breakpointno || ':' || lineno || ':' || query from dbe_pldebugger.info_breakpoints();
end;
$$;

select funcname, lineno, query from dbe_pldebugger.finish();

select funcname, lineno, query from dbe_pldebugger.finish();

select funcname, lineno, query from dbe_pldebugger.finish();

select funcname, lineno, query from dbe_pldebugger.finish();

-- test recursive debug

select pg_sleep(1);

select dbe_pldebugger.attach(nodename, port) from debug_info;

truncate tmp_holder;

do $$
declare
    funcoid oid;
begin
    select oid from pg_proc into funcoid where proname = 'test_debug_recursive';
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 5);
    insert into tmp_holder select breakpointno || ':' || lineno || ':' || query from dbe_pldebugger.info_breakpoints();
end;
$$;

select funcname, lineno, query from dbe_pldebugger.step();

select frameno, funcname, lineno, query from dbe_pldebugger.backtrace();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.step();

select frameno, funcname, lineno, query from dbe_pldebugger.backtrace();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.step();

select frameno, funcname, lineno, query from dbe_pldebugger.backtrace();

select funcname, lineno, query from dbe_pldebugger.continue();

select frameno, funcname, lineno, query from dbe_pldebugger.backtrace();

select funcname, lineno, query from dbe_pldebugger.continue();

select frameno, funcname, lineno, query from dbe_pldebugger.backtrace();

select funcname, lineno, query from dbe_pldebugger.continue();

select frameno, funcname, lineno, query from dbe_pldebugger.backtrace();

select * from dbe_pldebugger.info_locals();

select * from dbe_pldebugger.info_locals(0);

select * from dbe_pldebugger.info_locals(1);

select * from dbe_pldebugger.info_locals(2);

select * from dbe_pldebugger.info_locals(3);

select * from dbe_pldebugger.info_locals(4); -- exceeds

select * from dbe_pldebugger.info_locals(-1); -- exceeds

select * from dbe_pldebugger.print_var('ct');

select * from dbe_pldebugger.print_var('ct', 0);

select * from dbe_pldebugger.print_var('ct', 1);

select * from dbe_pldebugger.print_var('ct', 2);

select * from dbe_pldebugger.print_var('ct', 3);

select * from dbe_pldebugger.print_var('ct', 4);

select * from dbe_pldebugger.print_var('ct', -1); -- exceeds

do $$
declare
    funcoid oid;
begin
    select oid from pg_proc into funcoid where proname = 'test_debug_recursive';
    insert into tmp_holder select * from dbe_pldebugger.delete_breakpoint(0);
    insert into tmp_holder select breakpointno || ':' || lineno || ':' || query from dbe_pldebugger.info_breakpoints();
end;
$$;

select funcname, lineno, query from dbe_pldebugger.continue();

select frameno, funcname, lineno, query from dbe_pldebugger.backtrace();

select funcname, lineno, query from dbe_pldebugger.continue();

-- test_empty
select pg_sleep(1);

select dbe_pldebugger.attach(nodename, port) from debug_info;


-- test set_var
select pg_sleep(1);

select dbe_pldebugger.attach(nodename, port) from debug_info;

select * from dbe_pldebugger.info_locals();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

-- immutable to sql injection
select * from dbe_pldebugger.set_var('x', '1; create table injection(a int)');
select * from dbe_pldebugger.set_var('x', 'create table injection(a int)');

select * from dbe_pldebugger.set_var('x', '1');

select * from dbe_pldebugger.set_var('vint', '1+1'); -- can be expresion

select * from dbe_pldebugger.set_var('vnum', '1'); -- support type cast

select * from dbe_pldebugger.set_var('vfloat', '1');

select * from dbe_pldebugger.set_var('vvarchar', 'there is no spoon'); -- fails if not quoted

select * from dbe_pldebugger.set_var('vvarchar', '$$there is no spoon$$'); -- this works

select * from dbe_pldebugger.set_var('vtext', '$$Why oh why didnt I take the blue pill?$$');

select * from dbe_pldebugger.set_var('vrow', '1'); -- can not directly assign row type

select * from dbe_pldebugger.set_var('vrow', 'select * from test order by 1 limit 1'); -- syntax error

select * from dbe_pldebugger.set_var('vrow', '$$select * from test order by 1 limit 1$$'); -- still won't work

-- row should be changed by assignment member by member
select * from dbe_pldebugger.set_var('test.a', '1');
select * from dbe_pldebugger.set_var('test.b', '$$now$$');
select * from dbe_pldebugger.set_var('test.c', '$$2021-07-31$$::timestamp + now() - now()'); -- function call is not ok
select * from dbe_pldebugger.set_var('test.c', '$$2021-07-31$$::timestamp'); -- function call is ok

select * from dbe_pldebugger.set_var('vrec', '(1,1,1)::test%rowtype'); -- not ok
select * from dbe_pldebugger.set_var('vrec', 'ROW(1,1,1)');

select * from dbe_pldebugger.set_var('vrefcursor', '1'); -- set cursor variable directly is not allowed

select * from dbe_pldebugger.set_var('vconst', '1'); -- set constant variable is not allowed

select * from dbe_pldebugger.set_var('vint', '1;SELECT OID FROM PG_PROC limit 1;'); -- embedding of another sql is not allowed

select * from dbe_pldebugger.set_var('vint', '1;S'); -- incomplete second stmt will give warning


select * from dbe_pldebugger.info_locals();

select funcname, lineno, query from dbe_pldebugger.next();

select funcname, lineno, query from dbe_pldebugger.next();

select * from dbe_pldebugger.info_locals();

select funcname, lineno, query from dbe_pldebugger.next();

select * from dbe_pldebugger.info_locals();

select funcname, lineno, query from dbe_pldebugger.next();

-- test package
select pg_sleep(1);

select dbe_pldebugger.attach(nodename, port) from debug_info;

select * from dbe_pldebugger.info_locals();

select * from dbe_pldebugger.next();

select * from dbe_pldebugger.next();

truncate tmp_holder;

do $$
declare
    funcoid oid;
begin
    select oid from pg_proc into funcoid where proname = 'pro1';
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 12);
    insert into tmp_holder select breakpointno || ':' || lineno || ':' || query from dbe_pldebugger.info_breakpoints();
end;
$$;

select funcname, lineno, query from dbe_pldebugger.continue();

SELECT * FROM DBE_PLDEBUGGER.info_locals();

SELECT * FROM DBE_PLDEBUGGER.backtrace();

SELECT * FROM DBE_PLDEBUGGER.continue();
