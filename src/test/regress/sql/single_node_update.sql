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

alter table t4 alter b set default now();
\d t4;
alter table t4 change b b1 timestamp on update current_timestamp;
\d t4
alter table t4 change b1 b2 timestamp not null default now() on update localtimestamp;
\d t4

CREATE TABLE t5(id int, a timestamp default now() on update current_timestamp, b timestamp on update current_timestamp, c timestamp default now());
\d t5
create table t6 (like t5 including defaults);
\d t6
alter table t6 modify b timestamp on update localtimestamp;
\d t6
alter table t6 modify b timestamp;
\d t6

CREATE TABLE goodscheck (
goodsid bigint,
goodscode varchar(20) DEFAULT NULL::varchar,
status integer,
isdelete integer,
introduce varchar(150) DEFAULT NULL::varchar,
createtime timestamp(0) without time zone DEFAULT NULL::timestamp without time zone,
createby varchar(20) DEFAULT NULL::varchar,
updatetime timestamp(0) without time zone DEFAULT NULL::timestamp without time zone ON UPDATE CURRENT_TIMESTAMP,
updateby varchar(20) DEFAULT NULL::varchar
);
ALTER TABLE goodscheck ADD CONSTRAINT goodscheck_pkey PRIMARY KEY (goodsid);
CREATE FUNCTION update_timestamp()
RETURNS trigger
LANGUAGE plpgsql
AUTHID DEFINER NOT FENCED NOT SHIPPABLE
AS $function$
BEGIN
NEW.updateTime = now();
RETURN NEW;
END;
$function$;
CREATE TRIGGER goodscheck_updatetime_trriger
BEFORE UPDATE OF updatetime ON goodscheck
FOR EACH ROW
EXECUTE PROCEDURE update_timestamp();
INSERT INTO goodscheck(goodsid,goodscode,status,isdelete,introduce,createtime,createby,updatetime,updateby)
VALUES (322,'1673994937684815874',3,0,'fff','2023-07-14 10:24:51',null,'2023-08-23 10:11:30','wangjun');
update goodscheck
set goodsId = 888,
status = 2,
introduce = 'test',
updateTime = current_timestamp,
updateBy = 'zljtest'
WHERE 1=1
AND goodsId=322;
drop table goodscheck;
drop function update_timestamp();

-- \! @abs_bindir@/gs_dump mysql -p @portstring@ -f @abs_bindir@/dump_type.sql -F p >/dev/null 2>&1;

-- create table test_feature(a int, b timestamp on update current_timestamp);
-- insert into test_feature values (1);
-- update test_feature set a=2 where a=1;
-- select * from test_feature;
-- \! @abs_bindir@/gsql -d mysql -p @portstring@ -c "update test_feature set a=3;" >/dev/null 2>&1;
-- select * from test_feature;

CREATE TABLE t_dmpportal_common_intent (
id bigserial NOT NULL,
intent_name character varying(120) NOT NULL,
upt_time timestamp ON UPDATE CURRENT_TIMESTAMP);
ALTER TABLE t_dmpportal_common_intent ADD CONSTRAINT pk_t_dmpportal_common_intent_1675307617_0 PRIMARY KEY USING btree (id);
insert into t_dmpportal_common_intent values(1,'1',current_timestamp), (2,'2',current_timestamp), (3,'3',current_timestamp);
select * from t_dmpportal_common_intent;
select count(upt_time) from t_dmpportal_common_intent group by upt_time order by upt_time;
set enable_opfusion to on;
explain (costs off) update t_dmpportal_common_intent set intent_name='update_2' where id=2;
update t_dmpportal_common_intent set intent_name='update_2' where id=2;
select count(upt_time) from t_dmpportal_common_intent group by upt_time order by upt_time;
select * from t_dmpportal_common_intent;
set enable_opfusion to off;
update t_dmpportal_common_intent set intent_name='update_2' where id=2;
select count(upt_time) from t_dmpportal_common_intent group by upt_time order by upt_time;
select * from t_dmpportal_common_intent;
update t_dmpportal_common_intent set intent_name='2' where id=2;
select count(upt_time) from t_dmpportal_common_intent group by upt_time order by upt_time;
select * from t_dmpportal_common_intent;


show sql_beta_feature;
show enable_partition_opfusion;
show enable_opfusion;
set sql_beta_feature = 'a_style_coerce, partition_opfusion';
set enable_partition_opfusion = on;
set enable_opfusion = on;

create table bypass_pt_update (
    a serial primary key,
    b int default 1
) partition by range(a) (
    partition p1 values less than (5),
    partition p2 values less than (maxvalue)
);

insert into bypass_pt_update(b) select generate_series(1,8);
select * from bypass_pt_update order by a;

explain (verbose on, costs off) update bypass_pt_update set b = 2 where a = 1;
update bypass_pt_update set b = 2 where a = 1;

explain (verbose on, costs off) update bypass_pt_update set a = 9 where a = 2;
update bypass_pt_update set a = 9 where a = 2;

select * from bypass_pt_update order by a;

drop table bypass_pt_update;
set sql_beta_feature='a_style_coerce';
set enable_partition_opfusion = off;
set enable_opfusion = off;

\c regression
DROP database mysql;
