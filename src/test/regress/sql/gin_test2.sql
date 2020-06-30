-- 分区表gin 创建 修改 重建 删除 测试

-- Set GUC paramemter
SET ENABLE_SEQSCAN=OFF;
SET ENABLE_INDEXSCAN=OFF;
SET ENABLE_BITMAPSCAN=ON;


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
INSERT INTO test_gin_student SELECT id, md5(random()::text), md5(random()::text) FROM
          (SELECT * FROM generate_series(1,2900) AS id) AS x;

-- 查询		  
SELECT * FROM test_gin_student WHERE to_tsvector('english', data1) @@ to_tsquery('english', 'test') ORDER BY num, data1, data2;

-- 重命名索引分区
ALTER INDEX IF EXISTS test_gin_student_index2 RENAME PARTITION data2_index_1 TO data2_index_11;

DROP TABLE test_gin_student;

RESET ENABLE_SEQSCAN;
RESET ENABLE_INDEXSCAN;
RESET ENABLE_BITMAPSCAN;


