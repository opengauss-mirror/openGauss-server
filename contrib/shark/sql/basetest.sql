create database test_a dbcompatibility 'A';
\c test_a
create extension shark;

\c contrib_regression
create schema basetest;
set search_path = 'basetest';
select N'abc';

create table t1 (a int);
create columnstore index t1_idx1 on t1(a);

CREATE OR REPLACE FUNCTION test RETURNS INT AS
$$
declare
a int;
BEGIN
a := 2;
RETURN a;
END;
$$
LANGUAGE 'pltsql';

create procedure p1
is
begin
null;
end;
/

select l.lanname from pg_language l join pg_proc p on l.oid = p.prolang and p.proname in ('test', 'p1');

drop schema basetest cascade;