create schema deserialize_func1;
set current_schema = deserialize_func1;

create sequence test_seq;

create or replace function test_func() return int
as
begin
return 1;
end;
/

create table t1(key int, id int default nextval('test_seq'));

create table t2(key int, id int default test_func());

select * from pg_get_tabledef('deserialize_func1.t1');

select * from pg_get_tabledef('deserialize_func1.t2');

alter schema deserialize_func1 rename to deserialize_func2;

select * from pg_get_tabledef('deserialize_func2.t1');

select * from pg_get_tabledef('deserialize_func2.t2');

alter table deserialize_func2.t1 alter column id drop default;

alter table deserialize_func2.t1 alter column id set default nextval('deserialize_func2.test_seq');

select * from pg_get_tabledef('deserialize_func2.t1');

alter table deserialize_func2.t2 alter column id drop default;

alter table deserialize_func2.t2 alter column id set default deserialize_func2.test_func();

select * from pg_get_tabledef('deserialize_func2.t2');

set current_schema = deserialize_func2;

create type type1 as (a int);

create view v1 as select '(1)'::type1;

alter type type1 rename to type2;

select pg_get_viewdef('v1');

drop view v1 cascade;

create table atest12 as select x as a, 10001-x as b from generate_series(1, 10000) x;

create function func1(integer, integer) returns boolean as $$begin return $1 < $2; end $$ language plpgsql immutable;

create operator <<< (procedure=func1, leftarg=integer, rightarg=integer, restrict=scalarltsel);

create view atest12v as select * from atest12 where b <<< 5;

select pg_get_viewdef('atest12v');

drop schema if exists deserialize_func1 cascade;

drop schema if exists deserialize_func2 cascade;
