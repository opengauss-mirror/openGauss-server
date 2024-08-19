--
-- test memory usage of vacuum
--

select name, setting from pg_settings where name in ('maintenance_work_mem') order by name;

create or replace procedure insert_table(n int)
as
sql text;
begin
for i in 1..n loop
sql:='insert into vacuum_test_unused values (' || i ||')';
execute sql;
commit;
end loop;
end;
/

create table vacuum_test_unused(id integer);
create table vacuum_test(id integer, s char(100)) with (autovacuum_enabled=off);
create index vacuum_test_i1 on vacuum_test(id);

insert into vacuum_test(id, s) select generate_series(1, 5000000, 1) as id, cast(generate_series(1, 5000000, 1) as char(100)) as s;
delete from vacuum_test where id % 2 = 0;

-- raise up oldest_minxid
call insert_table(2000);

-- 250W dead tuples stored as array will use 19MB at least. It will cause index scan twice when maintenance_work_mem is 16MB.
-- Use tidstore can lower the memory usage.
\set VERBOSITY terse
vacuum verbose vacuum_test;
\set VERBOSITY default

-- cleanup
drop procedure insert_table;
drop table vacuum_test_unused;
drop table vacuum_test;