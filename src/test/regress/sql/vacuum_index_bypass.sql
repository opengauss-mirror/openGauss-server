--
-- test bypass index vacuum
--

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
insert into vacuum_test(id, s) select generate_series(1, 10000, 1) as id, cast(generate_series(1, 10000, 1) as char(100)) as s;

--
-- case 0: table without dead tuples won't bypass vacuum index.
--
-- raise up oldest_minxid
call insert_table(2000);

\set VERBOSITY terse
vacuum verbose vacuum_test;
\set VERBOSITY default

--
-- case 1: pages with dead tuples less than 2%, bypass vacuum index.
--
delete from vacuum_test where id % 2 = 0 and id <= 150;

-- raise up oldest_minxid
call insert_table(2000);

\set VERBOSITY terse
vacuum verbose vacuum_test;
\set VERBOSITY default

-- bypass index twice to see the difference.
\set VERBOSITY terse
vacuum verbose vacuum_test;
\set VERBOSITY default

--
-- case 2: pages with dead tuples more than 2%, won't trigger, bypass vacuum index.
--
truncate table vacuum_test;
insert into vacuum_test(id, s) select generate_series(1, 10000, 1) as id, cast(generate_series(1, 10000, 1) as char(100)) as s;
delete from vacuum_test where id % 2 = 0 and id <= 300;

-- raise up oldest_minxid
call insert_table(2000);

\set VERBOSITY terse
vacuum verbose vacuum_test;
\set VERBOSITY default

--
-- case 3: aggresive mode won't bypass vacuum index.
--
truncate table vacuum_test;
insert into vacuum_test(id, s) select generate_series(1, 10000, 1) as id, cast(generate_series(1, 10000, 1) as char(100)) as s;
delete from vacuum_test where id % 2 = 0 and id <= 150;

-- raise up oldest_minxid
call insert_table(2000);

\set VERBOSITY terse
vacuum freeze verbose vacuum_test;
\set VERBOSITY default

--
-- case 4: table without index won't bypass vacuum index.
--
drop index vacuum_test_i1;
truncate table vacuum_test;
insert into vacuum_test(id, s) select generate_series(1, 10000, 1) as id, cast(generate_series(1, 10000, 1) as char(100)) as s;
delete from vacuum_test where id % 2 = 0 and id <= 150;

-- raise up oldest_minxid
call insert_table(2000);

\set VERBOSITY terse
vacuum verbose vacuum_test;
\set VERBOSITY default

--
-- case 5: ustore won't bypass vacuum index.
--
drop table vacuum_test;
create table vacuum_test(id integer, s char(100)) with (autovacuum_enabled=off, storage_type = ustore);
create index vacuum_test_i1 on vacuum_test(id);
insert into vacuum_test(id, s) select generate_series(1, 10000, 1) as id, cast(generate_series(1, 10000, 1) as char(100)) as s;
delete from vacuum_test where id % 2 = 0 and id <= 150;

-- raise up oldest_minxid
call insert_table(2000);

\set VERBOSITY terse
vacuum verbose vacuum_test;
\set VERBOSITY default

-- cleanup
drop procedure insert_table;
drop table vacuum_test_unused;
drop table vacuum_test;