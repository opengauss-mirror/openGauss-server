set enable_global_stats = true;
SET ENABLE_STREAM_OPERATOR = ON;

--
-- UPDATE syntax tests
--
CREATE TABLE cstore_update_test (
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
) with (orientation = orc) tablespace hdfs_ts ;

INSERT INTO cstore_update_test VALUES (5, 10, 'foo');
INSERT INTO cstore_update_test(b, a) VALUES (15, 10);

SELECT * FROM cstore_update_test ORDER BY a, b, c;

-- test for UPDATE DEFAULT VALUE
UPDATE cstore_update_test SET b = DEFAULT;

SELECT * FROM cstore_update_test  ORDER BY a, b, c;

-- aliases for the UPDATE target table
UPDATE cstore_update_test AS t SET b = 10 WHERE t.a = 10;

SELECT * FROM cstore_update_test  ORDER BY a, b, c;

UPDATE cstore_update_test t SET b = t.b + 10 WHERE t.a = 10;

SELECT * FROM cstore_update_test  ORDER BY a, b, c;

--
-- Test multiple-set-clause syntax
--

UPDATE cstore_update_test SET (c,b) = ('bugle', b+11) WHERE c = 'foo';
SELECT * FROM cstore_update_test  ORDER BY a, b, c;
UPDATE cstore_update_test SET (c,b) = ('dd', b+11) WHERE c is null;
SELECT * FROM cstore_update_test  ORDER BY a, b, c;
UPDATE cstore_update_test SET (c,b) = ('car', a+b) WHERE a = 10;
SELECT * FROM cstore_update_test  ORDER BY a, b, c;
-- fail, multi assignment to same column:
UPDATE cstore_update_test SET (c,b) = ('car', a+b), b = a + 1 WHERE a = 10;

-- if an alias for the target table is specified, don't allow references
-- to the original table name
UPDATE cstore_update_test AS t SET b = cstore_update_test.b + 10 WHERE t.a = 10;
UPDATE cstore_update_test AS t SET b = t.b + 10 WHERE t.a = 10;
SELECT * FROM cstore_update_test ORDER BY a, b, c;

-- Make sure that we can update to a TOASTed value.
UPDATE cstore_update_test SET c = repeat('x', 10000) WHERE c = 'car';
SELECT a, b, char_length(c) FROM cstore_update_test ORDER BY a;

DROP TABLE cstore_update_test;

--test "update tablename AS aliasname SET aliasname.colname = colvalue;"
CREATE TABLE cstore_update_test_d(
    a    INT DEFAULT 10,
    b    INT
)with(orientation = orc) tablespace hdfs_ts ;

INSERT INTO cstore_update_test_d (a,b) VALUES (1,2);
SELECT * FROM cstore_update_test_d;
UPDATE cstore_update_test_d AS test_d SET test_d.b = 4;
SELECT * FROM cstore_update_test_d;
UPDATE cstore_update_test_d AS test_d SET test_d.b = 6 WHERE test_d.a = 1 AND test_d.b = 4;
SELECT * FROM cstore_update_test_d;
UPDATE cstore_update_test_d test_d SET test_d.b = 8;
SELECT * FROM cstore_update_test_d;
UPDATE cstore_update_test_d test_d SET test_d.b = 10  WHERE test_d.a = 1 AND test_d.b = 8;
SELECT * FROM cstore_update_test_d;
UPDATE cstore_update_test_d AS test_d SET test_b.b = 12;
SELECT * FROM cstore_update_test_d;
UPDATE cstore_update_test_d test_d SET test_b.b = 12;
SELECT * FROM cstore_update_test_d;

DROP TABLE cstore_update_test_d;
DROP TABLE cstore_update_test_d;
create table cstore_tbl_update(a1 int,a2 varchar2(100)) with(orientation = orc) tablespace hdfs_ts  ;
delete from cstore_tbl_update;
insert into cstore_tbl_update values(1,'a');
insert into cstore_tbl_update values(2,'b');
insert into cstore_tbl_update values(3,'c');
insert into cstore_tbl_update values(4,'d');
insert into cstore_tbl_update values(11,'aa');
select * from cstore_tbl_update ORDER BY a1,a2;
create table cstore_sub_tab(t1 int,t2 varchar2(100))with(orientation = orc) tablespace hdfs_ts  ;
insert into cstore_sub_tab values(11,'aa');
select * from cstore_sub_tab;
update cstore_tbl_update a set a2='hello' from cstore_sub_tab t where t.t1=a.a1;
select * from cstore_tbl_update ORDER BY a1,a2;
drop table cstore_tbl_update;
drop table cstore_sub_tab;

create table cstore_test_tbl_b(a int, b int) with(orientation = orc) tablespace hdfs_ts ;
insert into cstore_test_tbl_b values(1,2);
select * from cstore_test_tbl_b;
update cstore_test_tbl_b as a set b=4;
update cstore_test_tbl_b set c = 100;
select * from cstore_test_tbl_b;
update cstore_test_tbl_b as a set b=6 where a.a=1 and a.b=4;
select * from cstore_test_tbl_b;
update cstore_test_tbl_b as a set a.b=8 where a.a=1 and a.b=6;
select * from cstore_test_tbl_b;
drop table cstore_test_tbl_b;

-----
--- Non-deterministic UPDATE
-----
create table cstore_update_row_1(id int, cu int, num int);
create table cstore_update_row_2(id int, cu int, num int);

insert into cstore_update_row_1 values (1, generate_series(1, 2400), generate_series(1, 2400));
insert into cstore_update_row_2 values (1, 10, generate_series(1, 2400));

--- table
create table cstore_update_1 (id int, cu int, num int) with (orientation = orc) tablespace hdfs_ts  distribute by hash(id);
create table cstore_update_2 (id int, cu int, num int) with (orientation = orc) tablespace hdfs_ts  distribute by hash(id);

insert into cstore_update_1 select * from cstore_update_row_1;
insert into cstore_update_2 select * from cstore_update_row_2;

--- in transaction, should not report error
start transaction;
update cstore_update_1 set cu = 1;
update cstore_update_1 set cu = 2;
update cstore_update_1 set cu = 1;
select count(*) from cstore_update_1 where cu = 1;
commit;

--null batch test for LLT
insert into cstore_update_1 select * from cstore_update_1 where id=300000;
delete from cstore_update_1 where id=300000;
update cstore_update_1 set cu=1 where id=300000;

--- clean
drop table cstore_update_row_1;
drop table cstore_update_row_2;
drop table cstore_update_1;
drop table cstore_update_2;

create table test_update_column(a int, b int) with (ORIENTATION='column');
insert into test_update_column values(1,1);
update test_update_column set a = 2 where b = 1 returning *;
drop table test_update_column;
