create schema create_index_if_not_exists;
set current_schema = create_index_if_not_exists;

create table test(a int);
create index test_index on test(a);
create index test_index on test(a);
create index if not exists test_index on test(a);
create index if not exists test_index1 on test(a);
create index if not exists on test(a);

drop table test;
drop schema create_index_if_not_exists;

create database gin_db;
\c gin_db

CREATE OPERATOR CLASS gin_int_ops2 DEFAULT FOR TYPE integer USING gin AS
	  OPERATOR        1   <,
	  OPERATOR        2   <=,
	  OPERATOR        3   =,
	  OPERATOR        4   >=,
	  OPERATOR        5   >,
	  FUNCTION        1   int4lt(integer, integer),
	  FUNCTION        2   int4ne(integer, integer),
	  FUNCTION        3   int4eq(integer, integer),
	  FUNCTION        4   int4ge(integer, integer),
	  FUNCTION        5   int4gt(integer, integer);

create table t2(v1 int,v2 int);
insert into t2 values(1,2);
insert into t2 values(1,2);
CREATE INDEX idx_t2_content_gin ON t2 USING GIN (v1);

\c postgres
drop database gin_db;
