-- 列存gin索引的创建、删除、查询、alter、reindex操作。

create schema cgin_index;
set current_schema = cgin_index;

-- Set GUC paramemter
SET ENABLE_SEQSCAN=OFF;
SET ENABLE_INDEXSCAN=OFF;
SET ENABLE_BITMAPSCAN=ON;
SET ENABLE_FAST_QUERY_SHIPPING=OFF;

--case1 普通列存表的索引创建
DROP TABLE IF EXISTS test_cgin_1;
DROP TABLE IF EXISTS test_cgin_2;
CREATE TABLE test_cgin_1 (id INT, info text) with(orientation=column);
CREATE TABLE test_cgin_2 (id INT, first_name text, last_name text) with(orientation=column);

CREATE INDEX test_cgin_1_idx ON test_cgin_1 USING GIN(to_tsvector('ngram', info));
CREATE INDEX test_cgin_1_idx2 ON test_cgin_1 USING GIN(to_tsvector('ngram', ''));
CREATE INDEX test_cgin_1_idx3 ON test_cgin_1 USING GIN(to_tsvector('', info));
CREATE INDEX test_cgin_2_first_name_idx  ON test_cgin_2 USING GIN(to_tsvector('ngram', first_name));
CREATE INDEX test_cgin_2_first_last_name_idx ON test_cgin_2 USING GIN(to_tsvector('ngram', first_name || last_name));
CREATE INDEX test_cgin_2_first_last_name_idx2 ON test_cgin_2 USING GIN(to_tsvector('ngram',first_name),to_tsvector('ngram',last_name)) with (fastupdate = on);

INSERT INTO test_cgin_1 SELECT id, md5(random()::text) FROM (SELECT * FROM generate_series(1,100) AS id) AS x;
INSERT INTO test_cgin_1 values(5, 'test');
INSERT INTO test_cgin_1 values(15, 'test');
INSERT INTO test_cgin_1 values(9, '&yue_123test');
INSERT INTO test_cgin_1 values(4, 'jdlt@est&&Gt@est');
INSERT INTO test_cgin_2 SELECT id, md5(random()::text), md5(random()::text) FROM
          (SELECT * FROM generate_series(1,100) AS id) AS x;
INSERT INTO test_cgin_2 values(1, 'test', '&67575@gauss');
INSERT INTO test_cgin_2 values(2, 'test1', 'gauss');
INSERT INTO test_cgin_2 values(3, 'test2', 'gauss2');
INSERT INTO test_cgin_2 values(4, 'test3', 'test');
INSERT INTO test_cgin_2 values(5, 'gauss_123_@test', 'test');
INSERT INTO test_cgin_2 values(6, '', '');
INSERT INTO test_cgin_2 values(6, ' ', ' ');

SELECT * FROM test_cgin_1 WHERE id <10 and to_tsvector('ngram', info) @@ to_tsquery('ngram', 'test') ORDER BY id, info;

SELECT * FROM test_cgin_2 WHERE to_tsvector('ngram', first_name) @@ to_tsquery('ngram', 'test') ORDER BY id, first_name, last_name;
SELECT * FROM test_cgin_2 WHERE to_tsvector('ngram', first_name || last_name) @@ to_tsquery('ngram', 'test') ORDER BY id, first_name, last_name;
SELECT * FROM test_cgin_2 WHERE to_tsvector('ngram', last_name || first_name) @@ to_tsquery('ngram', 'test') ORDER BY id, first_name, last_name;
SELECT * FROM test_cgin_2 WHERE to_tsvector('ngram', first_name) @@ to_tsquery('ngram', 'test') OR to_tsvector('ngram', last_name) @@ to_tsquery('ngram', 'test') ORDER BY id, first_name, last_name;
SELECT count(*) FROM test_cgin_2 WHERE to_tsvector('ngram', first_name || last_name) @@ to_tsquery('ngram', 'gauss');
SELECT count(*) FROM test_cgin_2 WHERE to_tsvector('ngram', last_name || first_name) @@ to_tsquery('ngram', 'gauss');
SELECT count(*) FROM test_cgin_2 WHERE to_tsvector('ngram', first_name ) @@ to_tsquery('ngram', 'test') and to_tsvector('ngram', last_name ) @@ to_tsquery('ngram', 'test');

--case2 列存分区表的索引创建
DROP TABLE IF EXISTS test_gin_student;
CREATE TABLE test_gin_student
(
    num int,
    data1 text,
    data2 text
)with (orientation=column,max_batchrow= 30700, compression = middle)
PARTITION BY RANGE(num)
(
    PARTITION num1 VALUES LESS THAN(10000),
    PARTITION num2 VALUES LESS THAN(20000),
    PARTITION num3 VALUES LESS THAN(30000),
	PARTITION num4 VALUES LESS THAN(maxvalue)
);

CREATE INDEX test_gin_student_index1 ON test_gin_student USING GIN(to_tsvector('ngram', data1)) LOCAL;
CREATE INDEX test_gin_student_index2 ON test_gin_student USING GIN(to_tsvector('ngram', data2)) LOCAL
(
    PARTITION data2_index_1,
    PARTITION data2_index_2 TABLESPACE pg_default,
    PARTITION data2_index_3 TABLESPACE pg_default,
	PARTITION data2_index_4
) TABLESPACE pg_default;

INSERT INTO test_gin_student SELECT id, md5(random()::text), md5(random()::text) FROM
          (SELECT * FROM generate_series(1,290) AS id) AS x;
INSERT INTO test_gin_student values(9999, 'test', '&67575@gauss');
INSERT INTO test_gin_student values(10001, 'test1', 'gauss');
INSERT INTO test_gin_student values(20003, 'test2', 'gauss2');
INSERT INTO test_gin_student values(20004, 'test3', 'test');
INSERT INTO test_gin_student values(5, 'gauss_123_@test', 'test');
SELECT * FROM test_gin_student WHERE to_tsvector('ngram', data1) @@ to_tsquery('ngram', 'test') ORDER BY num, data1, data2;
SELECT * FROM test_gin_student WHERE to_tsvector('ngram', data1) @@ to_tsquery('ngram', 'test') and to_tsvector('ngram', data2) @@ to_tsquery('ngram', 'test') and num < 10 ORDER BY num, data1, data2;
SELECT * FROM test_gin_student WHERE to_tsvector('ngram', data1) @@ to_tsquery('ngram', 'test') or to_tsvector('ngram', data2) @@ to_tsquery('ngram', 'test') and num < 10 ORDER BY num, data1, data2;
SELECT * FROM test_gin_student WHERE to_tsvector('ngram', data1) @@ to_tsquery('ngram', 'test') or (num < 10) and to_tsvector('ngram', data2) @@ to_tsquery('ngram', 'test') ORDER BY num, data1, data2;

--case3 alter index测试：预期禁掉，不支持
--rename\tablespace\set storage_parameter\reset storage_parameter\unusable\rebuild\rename partition\move partition
ALTER INDEX IF EXISTS test_cgin_1_idx RENAME TO test_cgin_2_idx;
ALTER INDEX IF EXISTS test_cgin_2_idx RENAME TO test_cgin_1_idx;

ALTER INDEX IF EXISTS test_cgin_1_idx SET (FASTUPDATE =OFF);
\d+ test_cgin_1_idx
ALTER INDEX IF EXISTS test_cgin_1_idx RESET (FASTUPDATE);
\d+ test_cgin_1_idx
ALTER INDEX IF EXISTS test_cgin_1_idx SET (FASTUPDATE =ON);
\d+ test_cgin_1_idx
ALTER INDEX IF EXISTS test_gin_student_index2 RENAME PARTITION data2_index_1 TO data2_index_11;
ALTER INDEX test_cgin_1_idx UNUSABLE;

ALTER INDEX test_cgin_1_idx REBUILD; 

--case4 drop index

DROP INDEX IF EXISTS test_cgin_1_idx;
DROP INDEX IF EXISTS test_cgin_2_idx;
DROP INDEX IF EXISTS test_cgin_2_first_name_idx;
DROP INDEX IF EXISTS test_cgin_2_first_last_name_idx;
DROP INDEX IF EXISTS test_gin_student_index1;
DROP INDEX IF EXISTS test_gin_student_index2;

DROP TABLE test_cgin_1;
DROP TABLE test_cgin_2;
DROP TABLE test_gin_student;

-- case 5 
DROP TABLE IF EXISTS test_cgin_3;
create table test_cgin_3(id int, name varchar(1000), fruit varchar(1000))  with (orientation=column,max_batchrow= 30700, compression = middle);
insert into test_cgin_3 values(1, 'gauss1', 'apple');
insert into test_cgin_3 values(2, 'gauss1', 'apple1');
insert into test_cgin_3 values(3, 'gauss1', 'apple2');
insert into test_cgin_3 values(4, 'gauss1', 'apple3');
insert into test_cgin_3 values(5, 'gauss2', 'pear');
insert into test_cgin_3 values(6, 'gauss2', 'apple');
insert into test_cgin_3 values(7, 'gauss2', 'banana');
insert into test_cgin_3 values(8, 'gauss3', 'apple');
insert into test_cgin_3 values(8, '', '');
create index test_cgin_3_idx on test_cgin_3 using gin(to_tsvector('ngram',name),to_tsvector('ngram',fruit)) with (fastupdate = on);
insert into test_cgin_3 values(8, ' ', ' ');
insert into test_cgin_3 values(8, '', '');
select * from test_cgin_3 where to_tsvector('ngram',fruit)@@to_tsquery('ngram','apple') ORDER BY id, name, fruit;
select count(*) from test_cgin_3 where to_tsvector('ngram',fruit)@@to_tsquery('ngram','apple') and to_tsvector('ngram',name)@@to_tsquery('ngram','gauss1');
select count(*) from test_cgin_3 where to_tsvector('ngram',fruit)@@to_tsquery('ngram','apple') or to_tsvector('ngram',name)@@to_tsquery('ngram','gauss1');
DROP TABLE test_cgin_3;

-- case 6
DROP TABLE IF EXISTS test_cgin_cbtree_1;
create table test_cgin_cbtree_1(id int, name varchar(1000)) with (orientation=column);
insert into test_cgin_cbtree_1 values(1, 'apple');
insert into test_cgin_cbtree_1 values(2, 'pear');
insert into test_cgin_cbtree_1 values(3, 'apple pear');
create index cbtree_idx_1 on test_cgin_cbtree_1 using btree(id);
create index cbtree_idx_2 on test_cgin_cbtree_1 using btree(name);
create index cgin_idx_1 on test_cgin_cbtree_1 using gin(to_tsvector('ngram',name)) with (fastupdate = on);
EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF)
select count(*) from test_cgin_cbtree_1 where id =1 or to_tsvector('ngram',name)@@to_tsquery('ngram','pear');
select count(*) from test_cgin_cbtree_1 where id =1 or to_tsvector('ngram',name)@@to_tsquery('ngram','pear');
EXPLAIN (NUM_NODES OFF, NODES OFF, COSTS OFF) 
select count(*) from test_cgin_cbtree_1 where id in (1,3) and name in ('apple','pear');
select count(*) from test_cgin_cbtree_1 where id in (1,3) and name in ('apple','pear');

DROP TABLE test_cgin_cbtree_1;

RESET ENABLE_SEQSCAN;
RESET ENABLE_INDEXSCAN;
RESET ENABLE_BITMAPSCAN;
RESET ENABLE_FAST_QUERY_SHIPPING;

drop schema cgin_index CASCADE;
