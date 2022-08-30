--
-- UPDATE syntax tests
--

CREATE TABLE update_test (
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
);

INSERT INTO update_test VALUES (5, 10, 'foo');
INSERT INTO update_test(b, a) VALUES (15, 10);

SELECT * FROM update_test;

UPDATE update_test SET a = DEFAULT, b = DEFAULT;

SELECT * FROM update_test;

-- aliases for the UPDATE target table
UPDATE update_test AS t SET b = 10 WHERE t.a = 10;

SELECT * FROM update_test;

UPDATE update_test t SET b = t.b + 10 WHERE t.a = 10;

SELECT * FROM update_test;

--
-- Test VALUES in FROM
--

UPDATE update_test SET a=v.i FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

SELECT * FROM update_test;

--
-- Test multiple-set-clause syntax
--

UPDATE update_test SET (c,b,a) = ('bugle', b+11, DEFAULT) WHERE c = 'foo';
SELECT * FROM update_test;
UPDATE update_test SET (c,b) = ('car', a+b), a = a + 1 WHERE a = 10;
SELECT * FROM update_test;
-- fail, multi assignment to same column:
UPDATE update_test SET (c,b) = ('car', a+b), b = a + 1 WHERE a = 10;

-- XXX this should work, but doesn't yet:
UPDATE update_test SET (a,b) = (select a,b FROM update_test where c = 'foo')
  WHERE a = 10;

-- if an alias for the target table is specified, don't allow references
-- to the original table name
UPDATE update_test AS t SET b = update_test.b + 10 WHERE t.a = 10;

-- Make sure that we can update to a TOASTed value.
UPDATE update_test SET c = repeat('x', 10000) WHERE c = 'car';
SELECT a, b, char_length(c) FROM update_test;

DROP TABLE update_test;

-- test update, on update current_timestamp
create database mysql dbcompatibility 'B';
\c mysql
create table update_test_e(a int, b timestamp on update current_timestamp);
insert into update_test_e values(1);
insert into update_test_e values(2);
select * from update_test_e;
update update_test_e set a = 11 where a = 1;
select * from update_test_e;
update update_test_e set a=1, b = now()-10 where a=11;
select * from update_test_e;
update update_test_e set a = 1 where a = 1;
select * from update_test_e;
drop table update_test_e;

create table update_test_f(a int, b timestamp default current_timestamp on update current_timestamp);
insert into update_test_f values(1);
insert into update_test_f values(2);
select * from update_test_f;
update update_test_f set a = 11 where a = 1;
select * from update_test_f;
drop table update_test_f;

create table update_test_g(a int, b timestamp default current_timestamp on update current_timestamp, c timestamp on update current_timestamp);
insert into update_test_g values(1);
insert into update_test_g values(2);
select * from update_test_g;
update update_test_g set a = 1 where a = 1;
select * from update_test_g;
create incremental materialized view timestamp_test AS select * from update_test_g;
select * from timestamp_test;
drop materialized view timestamp_test;
drop table update_test_g;

create table update_test_h(a int, b timestamp default current_timestamp on update current_timestamp);
insert into update_test_h values(1);
select * from update_test_h;
with tab as (update update_test_h set a=10 where a=1 returning 1) select * from tab;
select * from update_test_h;
drop table update_test_h;

CREATE TABLE t2 (a int, b timestamp on update current_timestamp);
INSERT INTO t2 VALUES(1);
select * from t2;
PREPARE insert_reason(integer) AS UPDATE t2 SET a = $1;
EXECUTE insert_reason(52);
select * from t2;
drop table t2;

create table t1(a int, b timestamp on update current_timestamp);
insert into t1 values(1);

CREATE OR REPLACE FUNCTION TEST1(val integer)
RETURNS VOID AS $$
declare
val int;
begin
update t1 set a=val;
end;
$$
LANGUAGE sql;
CREATE OR REPLACE FUNCTION TEST2(val integer)
RETURNS integer AS $$
update t1 set a=val;
select 1;
$$
LANGUAGE sql;
select TEST2(11);
select * from t1;
select TEST1(2);
select * from t1;
DROP FUNCTION TEST1;
DROP FUNCTION TEST2;
drop table t1;

CREATE TABLE t1(a int, b timestamp DEFAULT current_timestamp on update current_timestamp);
CREATE TABLE t2(a int, b timestamp DEFAULT current_timestamp, c varchar, d timestamp on update current_timestamp);
CREATE TABLE t3(a int, b char, c timestamp on update current_timestamp, d text);
INSERT INTO t1 VALUES(1);
INSERT INTO t2(a,c) VALUES(1,'test');
INSERT INTO t3(a,b,d) VALUES(1,'T','asfdsaf');
SELECT * FROM t1;
SELECT * FROM t2;
SELECT * FROM t3;
CREATE OR REPLACE FUNCTION test1(src int, dest int) RETURNS void AS $$
  BEGIN
    UPDATE t1 SET a = dest WHERE a = src;
    UPDATE t2 SET a = dest WHERE a = src;
    UPDATE t3 SET a = dest WHERE a = src;
  END;
$$ LANGUAGE PLPGSQL;
SELECT test1(1,10);
SELECT * FROM t1;
SELECT * FROM t2;
SELECT * FROM t3;
DROP FUNCTION test1;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

CREATE TABLE t1(a int, b timestamp on update current_timestamp);
CREATE TABLE t2(a int, b timestamp on update current_timestamp);
INSERT INTO t1 VALUES(1);
INSERT INTO t2 VALUES(2);
SELECT * FROM t1;
SELECT * FROM t2;
with a as (update t1 set a=a+10 returning 1), b as (update t2 set a=a+10 returning 1) select * from a,b;
SELECT * FROM t1;
SELECT * FROM t2;
with a as (update t1 set a=a+10), b as (update t2 set a=a+10) select 1;
SELECT * FROM t1;
SELECT * FROM t2;
DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t4(a int, b timestamp);
\d t4
alter table t4 modify b timestamp on update current_timestamp;
\d t4
alter table t4 modify b timestamp;
\d t4
alter table t4 alter b set default now();
\d t4
alter table t4 alter b drop default;
\d t4

alter table t4 alter b set default now();
\d t4
alter table t4 modify b timestamp on update current_timestamp;
\d t4
alter table t4 alter b drop default;
\d t4
alter table t4 modify b timestamp;
\d t4

alter table t4 modify b timestamp on update current_timestamp;
\d t4
alter table t4 alter b set default now();
\d t4;
alter table t4 modify b timestamp;
\d t4
alter table t4 alter b drop default;
\d t4
alter table t4 modify b not null;
alter table t4 modify b timestamp on update current_timestamp;
\d t4
alter table t4 modify b null;
alter table t4 modify b timestamp;
\d t4

alter table t4 modify b timestamp on update current_timestamp;
\d t4
alter table t4 modify b timestamp on update localtimestamp;
\d t4

CREATE TABLE t5(id int, a timestamp default now() on update current_timestamp, b timestamp on update current_timestamp, c timestamp default now());
\d t5
create table t6 (like t5 including defaults);
\d t6
alter table t6 modify b timestamp on update localtimestamp;
\d t6
alter table t6 modify b timestamp;
\d t6

-- \! @abs_bindir@/gs_dump mysql -p @portstring@ -f @abs_bindir@/dump_type.sql -F p >/dev/null 2>&1;

-- create table test_feature(a int, b timestamp on update current_timestamp);
-- insert into test_feature values (1);
-- update test_feature set a=2 where a=1;
-- select * from test_feature;
-- \! @abs_bindir@/gsql -d mysql -p @portstring@ -c "update test_feature set a=3;" >/dev/null 2>&1;
-- select * from test_feature;

\c regression
DROP database mysql;