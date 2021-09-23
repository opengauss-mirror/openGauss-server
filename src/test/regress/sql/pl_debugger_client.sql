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
    insert into tmp_holder select * from dbe_pldebugger.add_breakpoint(funcoid, 50); -- exceeds
    insert into tmp_holder select * from dbe_pldebugger.delete_breakpoint(1);
    insert into tmp_holder select breakpointno || ':' || lineno || ':' || query from dbe_pldebugger.info_breakpoints();
end;
$$;

select * from tmp_holder;

select * from dbe_pldebugger.print_var('test.c');

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

select * from tmp_holder;

select funcname, lineno, query from dbe_pldebugger.continue();

select * from dbe_pldebugger.info_locals();

select funcname, lineno, query from dbe_pldebugger.continue();

select * from dbe_pldebugger.backtrace();

select * from dbe_pldebugger.info_locals();

select funcname, lineno, query from dbe_pldebugger.continue();

select * from dbe_pldebugger.backtrace();

select * from dbe_pldebugger.info_locals();

select funcname, lineno, query from dbe_pldebugger.continue();

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

select * from dbe_pldebugger.backtrace();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.step();

select * from dbe_pldebugger.backtrace();

select funcname, lineno, query from dbe_pldebugger.step();

select funcname, lineno, query from dbe_pldebugger.step();

select * from dbe_pldebugger.backtrace();

select funcname, lineno, query from dbe_pldebugger.continue();

select * from dbe_pldebugger.backtrace();

select funcname, lineno, query from dbe_pldebugger.continue();

select * from dbe_pldebugger.backtrace();

select funcname, lineno, query from dbe_pldebugger.continue();

select * from dbe_pldebugger.backtrace();

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

select * from dbe_pldebugger.backtrace();

select funcname, lineno, query from dbe_pldebugger.continue();