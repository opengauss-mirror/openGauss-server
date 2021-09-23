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

SELECT * FROM update_test ORDER BY a, b, c;

UPDATE update_test SET a = DEFAULT, b = DEFAULT;

SELECT * FROM update_test  ORDER BY a, b, c;

-- aliases for the UPDATE target table
UPDATE update_test AS t SET b = 10 WHERE t.a = 10;

SELECT * FROM update_test  ORDER BY a, b, c;

UPDATE update_test t SET b = t.b + 10 WHERE t.a = 10;

SELECT * FROM update_test  ORDER BY a, b, c;

--
-- Test VALUES in FROM
--

UPDATE update_test SET a=v.i FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

SELECT * FROM update_test  ORDER BY a, b, c;

--
-- Test multiple-set-clause syntax
--

UPDATE update_test SET (c,b,a) = ('bugle', b+11, DEFAULT) WHERE c = 'foo';
SELECT * FROM update_test  ORDER BY a, b, c;
UPDATE update_test SET (c,b) = ('car', a+b), a = a + 1 WHERE a = 10;
SELECT * FROM update_test  ORDER BY a, b, c;
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
SELECT a, b, char_length(c) FROM update_test ORDER BY a;

DROP TABLE update_test;

--test "update tablename AS aliasname SET aliasname.colname = colvalue;"
CREATE TABLE update_test_c(
    a    INT DEFAULT 10
);
CREATE TABLE update_test_d(
    a    INT DEFAULT 10,
    b    INT
);

INSERT INTO update_test_c (a) VALUES (1);
SELECT * FROM update_test_c;
UPDATE update_test_c AS test_c SET test_c.a = 2;
SELECT * FROM update_test_c;
UPDATE update_test_c AS test_c SET test_c.a = 3 WHERE test_c.a = 2;
SELECT * FROM update_test_c;
UPDATE update_test_c test_c SET test_c.a = 4;
SELECT * FROM update_test_c;
UPDATE update_test_c AS test_c SET test_c.a = 5 WHERE test_c.a = 4;
SELECT * FROM update_test_c;
UPDATE update_test_c AS test_c SET test_a.a = 6;
SELECT * FROM update_test_c;
UPDATE update_test_c test_c SET test_a.a = 7;
SELECT * FROM update_test_c;

INSERT INTO update_test_d (a,b) VALUES (1,2);
SELECT * FROM update_test_d;
UPDATE update_test_d AS test_d SET test_d.a = 3, test_d.b = 4;
SELECT * FROM update_test_d;
UPDATE update_test_d AS test_d SET test_d.a = 5, test_d.b = 6 WHERE test_d.a = 3 AND test_d.b = 4;
SELECT * FROM update_test_d;
UPDATE update_test_d test_d SET test_d.a = 7, test_d.b = 8;
SELECT * FROM update_test_d;
UPDATE update_test_d test_d SET test_d.a = 9, test_d.b = 10  WHERE test_d.a = 7 AND test_d.b = 8;
SELECT * FROM update_test_d;
UPDATE update_test_d AS test_d SET test_d.a = 11, test_b.b = 12;
SELECT * FROM update_test_d;
UPDATE update_test_d test_d SET test_d.a = 11, test_b.b = 12;
SELECT * FROM update_test_d;

DROP TABLE update_test_c;
DROP TABLE update_test_d;
DROP TABLE update_test_d;

create table tbl_update(a1 int,a2 varchar2(100));
ALTER TABLE tbl_update ADD PRIMARY KEY(a1);
delete from tbl_update;
insert into tbl_update values(1,'a');
insert into tbl_update values(2,'b');
insert into tbl_update values(3,'c');
insert into tbl_update values(4,'d');
insert into tbl_update values(11,'aa');
select * from tbl_update order by a1;
create table sub_tab(t1 int,t2 varchar2(100));
insert into sub_tab values(11,'aa');
select * from sub_tab;
update tbl_update a set (a1,a2)=(100,'hello') from sub_tab t where t.t1=a.a1;
select * from tbl_update order by a1;
update tbl_update a1 set (a1,a2)=(101,'hello world') from sub_tab t where t.t1=a1.a1;
select * from tbl_update order by a1;
drop table tbl_update;
drop table sub_tab;

create table test_tbl_a(a int);
insert into test_tbl_a values(1);
select * from test_tbl_a;
update test_tbl_a a set a=2;
select * from test_tbl_a;
update test_tbl_a a set a=3 where a.a=2;
select * from test_tbl_a;
drop table test_tbl_a;

create table test_tbl_b(a int, b int);
insert into test_tbl_b values(1,2);
select * from test_tbl_b;
update test_tbl_b as a set (a,b)=(3,4);
update test_tbl_b set c = 100;
select * from test_tbl_b;
update test_tbl_b as a set (a,b)=(5,6) where a.a=3 and a.b=4;
select * from test_tbl_b;
update test_tbl_b as a set (a.a, a.b)=(7,8) where a.a=5 and a.b=6;
select * from test_tbl_b;
drop table test_tbl_b;

CREATE TYPE complex AS (b int,c int);
CREATE TYPE complex AS (b int,c int);
create table test_tbl_c(a complex);
ALTER TABLE test_tbl_c ADD PRIMARY KEY(a);
insert into test_tbl_c values((1,2));
select * from test_tbl_c;
update test_tbl_c col set col.a.b=(100);
select * from test_tbl_c;
drop table test_tbl_c;
drop type complex;

-- test multiple column set with GROUP BY of UPDATE
CREATE TABLE update_multiple_set_01(a INT, b INT, c INT);
CREATE TABLE update_multiple_set_02(a INT, b INT, c INT);
UPDATE update_multiple_set_02 t2 SET (b, c) = (SELECT b, c FROM update_multiple_set_01 t1 WHERE t1.a=t2.a GROUP BY 1, 2);
DROP TABLE update_multiple_set_01;
DROP TABLE update_multiple_set_02;

-- test multiple column set with GROUP BY alias of UPDATE
drop table usview08t;
drop table offers_20050701;
create table usview08t(location_id int, on_hand_unit_qty int, on_order_qty int);
create table offers_20050701(location_id int null, visits int null);
insert into usview08t values(1,3,5);
insert into offers_20050701 values(2,4);
UPDATE usview08t Table_008 SET (on_hand_unit_qty,on_order_qty) = (SELECT AVG(VISITS),154 c2 FROM offers_20050701 GROUP BY c2);
select * from usview08t;
UPDATE usview08t t2 SET (t2.on_hand_unit_qty, t2.on_order_qty) = (SELECT AVG(VISITS),154 FROM offers_20050701);
UPDATE usview08t Table_008 SET (on_hand_unit_qty,on_hand_unit_qty) = (SELECT AVG(VISITS),154 c2 FROM offers_20050701 GROUP BY c2);
drop table usview08t;
drop table offers_20050701;

--test table name reference or alias reference
create table test (b int, a int);
insert into test values(1,2);
update test set test.a=10;
update test t set t.b=20;
select * from test;
drop table test;

create table test(a int[3],b int);
insert into test values('{1,2,3}',4);
update test set test.a='{10,20,30}';
select * from test;
update test t set t.a='{11,21,31}';
select * from test;
update test set a='{12,22,32}';
select * from test;
update test set a[1,2]='{13,23}';
select * from test;
--must compatible with previous features, though not perfect
update test set test.a[1,2]='{14,24}';
select * from test;
update test t set t.a[1,2]='{15,25}';
select * from test;
drop table test;

create type newtype as(a int, b int);
create table test(a newtype,b int);
insert into test values(ROW(1,2),3);
update test set test.a=ROW(10,20);
select * from test;
update test t set t.a=ROW(11,21);
select * from test;
--Ambiguous scene
--update field a of column a rather than column a of table a
update test a set a.a=12;
--update field b of column a rather than column b of table a
update test a set a.b=22;
select * from test;
--fail
update test a set a.a=ROW(13,23);
update test a set a.c=10;
update test b set b.c=10;
--must compatible with previous features, though not perfect
update test a set a.a.a=12;
select * from test;
drop table test;
drop type newtype;

--test update in merge into
create table test_d (a int, b int);
create table test_s (a int, b int);
insert into test_d values(generate_series(6,10),1);
insert into test_s values(generate_series(1,10),2);
merge into test_d using test_s on(test_d.a=test_s.a) when matched then update set test_d.b=test_s.b;
select * from test_d order by a;
truncate table test_s;
insert into test_s values(generate_series(1,10),20);
merge into test_d d using test_s on(d.a=test_s.a) when matched then update set d.b=test_s.b;
select * from test_d order by a;
drop table test_d;
drop table test_s;

create table test_d(a int[3],b int);
create table test_s(a int[3],b int);
insert into test_d values('{1,2,3}',4);
insert into test_s values('{10,20,30}',4);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.a=test_s.a;
select * from test_d;
truncate table test_s;
insert into test_s values('{11,21,31}',4);
merge into test_d d using test_s on(d.b=test_s.b) when matched then update set d.a=test_s.a;
select * from test_d;
--must compatible with previous features, though not perfect
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.a[1,3]=test_s.a[1,3];
select * from test_d;
merge into test_d d using test_s on(d.b=test_s.b) when matched then update set d.a[1,3]=test_s.a[1,3];
select * from test_d;
drop table test_d;
drop table test_s;

create type newtype as(a int,b int);
create table test_d(a newtype, b int);
create table test_s(a newtype, b int);
insert into test_d values(ROW(1,2),3);
insert into test_s values(ROW(10,20),3);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.a=test_s.a;
select * from test_d;
truncate table test_s;
insert into test_s values(ROW(11,12),3);
merge into test_d d using test_s on(d.b=test_s.b) when matched then update set d.a=test_s.a;
select * from test_d;
truncate table test_s;
insert into test_s values(ROW(22,22),3);
merge into test_d a using test_s on(a.b=test_s.b) when matched then update set a.a=21;
merge into test_d a using test_s on(a.b=test_s.b) when matched then update set a.b=22;
select * from test_d;
--fail
merge into test_d a using test_s on(a.b=test_s.b) when matched then update set a.a=test_s.a;
--must compatible with previous features, though not perfect
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.a.a=test_s.b;
select * from test_d;
merge into test_d d using test_s on(d.b=test_s.b) when matched then update set d.a.a=test_s.b;
select * from test_d;
drop table test_s;
drop table test_d;
drop type newtype;

-- test update multiple entries for the same column with subselect
create table test (a int[2], b int);
insert into test values('{1,2}',3);
update test set (a[1],a[2])=(select 10,20);
select * from test;
drop table test;
create type nt as(a int,b int);
create table test(a nt,b nt,c int);
insert into test values(row(1,2),row(3,4),5);
update test set (a.b,b.b)=(select 20,40);
select * from test;
drop table test;
drop type nt;

-- test comment in subselect of update
create table test(a int,b int);
insert into test values(1,2);
update test set (a)=(select /*comment*/10);
select * from test;
update test set (a)=(select /*+comment*/20);
select * from test;
drop table test;

--test update multiple fields of column which using composite type at once
create type nt as(a int,b int);
create table aa (a nt, b int,c char);
explain (verbose on, costs off) insert into aa values(ROW(1,2),3,'4');
insert into aa values(ROW(1,2),3,'4');
explain (verbose on, costs off) update aa set a.a=10,a.b=20 where c='4';
update aa set a.a=10,a.b=20 where c='4';
select * from aa;
drop table aa;
drop type nt;

--test update multiple values of of an array at once
create table test (a int[2], b int,c char);
insert into test values('{1,2}',3,'4');
explain (verbose on, costs off) update test set a[1]=100,a[2]=200 where c='4';
update test set a[1]=100,a[2]=200 where c='4';
select * from test;
explain (verbose on, costs off) update test set a[1,2]='{101,201}' where c='4';
update test set a[1,2]='{101,201}' where c='4';
select * from test;
explain (verbose on, costs off) insert into test  (a[1,2],b,c) values('{113,114}',4,'5');
insert into test  (a[1,2],b,c) values('{113,114}',4,'5');
select * from test order by 3;
select a[1,2] from test where c='4';
explain (verbose on, costs off) insert into test (a[1],a[2],b,c)values(1,2,3,'6');
insert into test (a[1],a[2],b,c)values(1,2,3,'6');
select * from test order by 3;
explain (verbose on, costs off) insert into test (a[1:2],b,c)values('{1,2}',3,'7');
insert into test (a[1:2],b,c)values('{1,2}',3,'7');
select * from test order by 3;
explain (verbose on, costs off) update test set a[1:2]='{10,20}' where c='7';
update test set a[1:2]='{10,20}' where c='7';
select * from test order by 3;
drop table test;
