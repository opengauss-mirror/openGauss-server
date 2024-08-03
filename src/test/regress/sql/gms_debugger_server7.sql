-- setups
create extension if not exists gms_debug;
drop schema if exists gms_debugger_test7 cascade;
create schema gms_debugger_test7;
set search_path = gms_debugger_test7;

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

select * from gms_debug.initialize();

select pg_sleep(1);

select * from test_debug_recursive (1, 1);

-- test with client error in exception
select * from test_debug_recursive (1, 1);

select * from gms_debug.debug_off();

drop schema gms_debugger_test7 cascade;
