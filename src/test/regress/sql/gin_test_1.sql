-- gin 创建 修改 重建 删除 测试

-- Set GUC paramemter
SET ENABLE_SEQSCAN=OFF;
SET ENABLE_INDEXSCAN=OFF;
SET ENABLE_BITMAPSCAN=ON;
SET ENABLE_FAST_QUERY_SHIPPING=OFF;

-- 普通表
DROP TABLE IF EXISTS test_gin_1;
DROP TABLE IF EXISTS test_gin_2;
CREATE TABLE test_gin_1 (id INT, info INT[]);
CREATE TABLE test_gin_2 (id INT, first_name text, last_name text);

-- 创建分区表
DROP TABLE IF EXISTS test_gin_student;

CREATE TABLE test_gin_student
(
    num int,
    data1 text,
    data2 text
)
PARTITION BY RANGE(num)
(
    PARTITION num1 VALUES LESS THAN(10000),
    PARTITION num2 VALUES LESS THAN(20000),
    PARTITION num3 VALUES LESS THAN(30000)
);

-- 创建索引
CREATE INDEX  test_gin_1_idx ON test_gin_1 USING GIN(info);
CREATE INDEX  test_gin_2_first_name_idx  ON test_gin_2 USING GIN(to_tsvector('ngram', first_name));
CREATE INDEX  test_gin_2_first_last_name_idx ON test_gin_2 USING GIN(to_tsvector('ngram', first_name || last_name));

--创建分区表索引test_gin_student_index1，不指定索引分区的名字。
CREATE INDEX test_gin_student_index1 ON test_gin_student USING GIN(to_tsvector('english', data1)) LOCAL;
--创建分区索引test_gin_student_index2，并指定索引分区的名字。
CREATE INDEX test_gin_student_index2 ON test_gin_student USING GIN(to_tsvector('english', data2)) LOCAL
(
    PARTITION data2_index_1,
    PARTITION data2_index_2 TABLESPACE pg_default,
    PARTITION data2_index_3 TABLESPACE pg_default
) TABLESPACE pg_default;

-- 数据导入
INSERT INTO test_gin_1 SELECT g, ARRAY[1, g % 5, g] FROM generate_series(1, 20000) g;
INSERT INTO test_gin_2 SELECT id, md5(random()::text), md5(random()::text) FROM
          (SELECT * FROM generate_series(1,10000) AS id) AS x;
INSERT INTO test_gin_student SELECT id, md5(random()::text), md5(random()::text) FROM
          (SELECT * FROM generate_series(1,29000) AS id) AS x;

-- 查询
SELECT * FROM test_gin_1 WHERE info @> '{2}' AND info @> '{22}' ORDER BY id, info;
SELECT * FROM test_gin_1 WHERE info @> '{22}' OR info @> '{32}' ORDER BY id, info;

SELECT * FROM test_gin_2 WHERE to_tsvector('ngram', first_name) @@ to_tsquery('ngram', 'test') ORDER BY id, first_name, last_name;
SELECT * FROM test_gin_2 WHERE to_tsvector('ngram', first_name || last_name) @@ to_tsquery('ngram', 'test') ORDER BY id, first_name, last_name;

SELECT * FROM test_gin_student WHERE to_tsvector('english', data1) @@ to_tsquery('english', 'test') ORDER BY num, data1, data2;

-- 索引更新
-- 重命名索引
ALTER INDEX IF EXISTS test_gin_1_idx RENAME TO test_gin_2_idx;
ALTER INDEX IF EXISTS test_gin_2_idx RENAME TO test_gin_1_idx;

-- 重命名索引分区
ALTER INDEX IF EXISTS test_gin_student_index2 RENAME PARTITION data2_index_1 TO data2_index_11;

-- 修改索引空间
-- 修改索引分区空间

-- 设置索引storage_parameter
ALTER INDEX IF EXISTS test_gin_1_idx SET (FASTUPDATE =OFF);
\d+ test_gin_1_idx
ALTER INDEX IF EXISTS test_gin_1_idx RESET (FASTUPDATE);
\d+ test_gin_1_idx
ALTER INDEX IF EXISTS test_gin_1_idx SET (FASTUPDATE =ON);
\d+ test_gin_1_idx

-- 设置索引不可用
ALTER INDEX test_gin_1_idx UNUSABLE;

INSERT INTO test_gin_1 SELECT g, ARRAY[1, g % 5, g] FROM generate_series(1, 20000) g;

-- rebuild索引

ALTER INDEX test_gin_1_idx REBUILD; 

SELECT * FROM test_gin_1 WHERE info @> '{22}' ORDER BY id, info;

SELECT * FROM test_gin_1 WHERE info @> '{22}' AND info @> '{2}' AND info @> '{1}' ORDER BY id, info;

-- 删除索引
DROP INDEX IF EXISTS test_gin_1_idx;

--- 索引表空间设置

--- 索引表
DROP TABLE test_gin_1;
DROP TABLE test_gin_2;
DROP TABLE test_gin_student;
