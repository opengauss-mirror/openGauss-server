CREATE DATABASE frwt_db;
\c frwt_db

-- With Ties功能测试
-- 数据库初始化：创建测试表并插入数据
create table t(X char,Y number,Z NUMBER);
insert into t values('A',1,NULL);
insert into t values('B',2,1);
insert into t values('C',3,1);
insert into t values('D',4,2);
insert into t values('E',5,3);
insert into t values('F',6,47);

SELECT * FROM T FETCH FIRST 20 PERCENT ROW ONLY;
SELECT * FROM T FETCH NEXT 20 PERCENT ROW WITH TIES;

CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    score INT,
    group_id INT
);

-- 插入数据
INSERT INTO test_table (name, score, group_id) VALUES
('Alice', 95, 1),
('Bob', 95, 1),
('Charlie', 90, 2),
('David', 85, 2),
('Eve', 80, 3),
('Frank', 80, 3),
('Grace', 75, 4),
('Heidi', 70, 4),
('Ivan', 65, 5),
('Judy', 60, 5);

-- 测试用例 1: 基本功能测试
-- 1.1 无 WITH TIES
SELECT * FROM test_table ORDER BY score FETCH NEXT 3 ROWS ONLY;

-- 测试用例 2: ORDER BY 列的影响
-- 2.1 单列排序
SELECT * FROM test_table ORDER BY score FETCH NEXT 3 ROWS WITH TIES;

-- 2.2 多列排序
SELECT * FROM test_table ORDER BY score DESC, name FETCH NEXT 3 ROWS WITH TIES;
SELECT * FROM test_table ORDER BY score, name FETCH NEXT 3 ROWS WITH TIES;

-- 2.3 NULL 值排序
-- 添加一些包含 NULL 值的数据
INSERT INTO test_table (name, score, group_id) VALUES
('Ken', NULL, 6),
('Laura', NULL, 6);
SELECT * FROM test_table ORDER BY score DESC FETCH NEXT 3 ROWS WITH TIES;

-- 测试用例 3: 边界情况测试
-- 3.1 没有 ORDER BY
SELECT * FROM test_table FETCH NEXT 3 ROWS WITH TIES;

-- 3.2 返回 0 行
SELECT * FROM test_table ORDER BY score FETCH NEXT 0 ROWS WITH TIES;

-- 3.3 FETCH FIRST 0 ROWS WITH TIES
SELECT * FROM test_table ORDER BY score DESC FETCH FIRST 0 ROWS WITH TIES;

-- 测试用例 4: 百分比和 WITH TIES 结合
-- 4.1 百分比限制和 WITH TIES
SELECT * FROM test_table ORDER BY score FETCH NEXT 3 PERCENT ROWS WITH TIES;

-- 测试用例 5: 复杂查询
-- 5.1 联合查询中的 WITH TIES
SELECT * FROM (
    SELECT * FROM test_table WHERE group_id IN (1, 2) 
    UNION ALL 
    SELECT * FROM test_table WHERE group_id IN (3, 4)
) AS combined ORDER BY score DESC FETCH NEXT 3 ROWS WITH TIES;
-- 5.2 子查询中的 WITH TIES
-- 不支持在子查询中使用 FETCH NEXT ... ROWS WITH TIES
SELECT * FROM (
    SELECT * FROM test_table WHERE group_id IN (1, 2) ORDER BY group_id FETCH NEXT 3 ROWS WITH TIES
    UNION ALL 
    SELECT * FROM test_table WHERE group_id IN (3, 4) ORDER BY group_id FETCH NEXT 3 ROWS WITH TIES
) AS combined;
-- 5.3 WITH TIES 与 UNION ALL 结合
-- 不支持在 UNION ALL 中使用 FETCH NEXT ... ROWS WITH TIES
SELECT * FROM test_table WHERE group_id IN (1, 2) ORDER BY group_id FETCH NEXT 3 ROWS WITH TIES
UNION ALL 
SELECT * FROM test_table WHERE group_id IN (3, 4) ORDER BY group_id FETCH NEXT 3 ROWS WITH TIES;

-- 5.4 WITH TIES 与 CTE
WITH cte1 AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS row_num
    FROM test_table
    WHERE group_id IN (1, 2)
    FETCH NEXT 3 ROWS WITH TIES
),
cte2 AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS row_num
    FROM test_table
    WHERE group_id IN (3, 4)
    OFFSET 2 ROWS FETCH NEXT 3 ROWS WITH TIES
)
SELECT * FROM cte1
UNION ALL
SELECT * FROM cte2;

-- 测试用例 6: WITH TIES 与其他功能结合
-- 6.1 WITH TIES 与 OFFSET
SELECT * FROM test_table ORDER BY score DESC LIMIT 6;
SELECT * FROM test_table ORDER BY score DESC FETCH NEXT 3 ROWS WITH TIES OFFSET 2;

-- 6.2 WITH TIES 与 rownum
SELECT name, score, ROW_NUMBER() OVER(ORDER BY score DESC) AS row_num 
FROM test_table ORDER BY score DESC FETCH NEXT 3 ROWS WITH TIES;
SELECT name, score FROM test_table WHERE rownum <= 3 ORDER BY score DESC FETCH NEXT 5 ROWS WITH TIES;
SELECT name, score FROM test_table WHERE rownum <= 5 ORDER BY score DESC FETCH NEXT 3 ROWS WITH TIES;


-- 测试用例 7：物化视图
-- 7.1 创建源表并插入数据
CREATE TABLE employees (
    id INT,
    name VARCHAR2(50),
    department VARCHAR2(50),
    salary NUMBER
);

-- 插入数据
INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'HR', 5000);
INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Finance', 6000);
INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'IT', 7000);
INSERT INTO employees (id, name, department, salary) VALUES (4, 'David', 'HR', 5500);
INSERT INTO employees (id, name, department, salary) VALUES (5, 'Eve', 'Finance', 6200);
INSERT INTO employees (id, name, department, salary) VALUES (6, 'Frank', 'IT', 7100);
INSERT INTO employees (id, name, department, salary) VALUES (7, 'Eve', 'Finance', 6200);
INSERT INTO employees (id, name, department, salary) VALUES (8, 'Eve1', 'Finance', 8000);
INSERT INTO employees (id, name, department, salary) VALUES (9, 'Eve2', 'Finance', 8000);

-- 7.1 : 创建包含 ROW LIMITING CLAUSE 的增量更新物化视图
-- Unsupport Feature
CREATE INCREMENTAL MATERIALIZED VIEW emp_mv
AS
SELECT id, name, department, salary
FROM employees
ORDER BY salary DESC
FETCH FIRST 3 ROWS ONLY;

-- 7.2 : 创建包含 ROW LIMITING CLAUSE 的物化视图
CREATE MATERIALIZED VIEW emp_mv
AS
SELECT id, name, department, salary
FROM employees
ORDER BY salary DESC
OFFSET 2 ROWS
FETCH FIRST 3 ROWS ONLY;
\d+ emp_mv

-- 7.3 : 创建包含 ROW LIMITING CLAUSE 的普通视图
CREATE VIEW emp_view_only
AS
SELECT id, name, department, salary
FROM employees
ORDER BY salary DESC
OFFSET 2 ROWS
FETCH FIRST 3 ROWS ONLY;
\d+ emp_view_only

CREATE VIEW emp_view_ties
AS
SELECT id, name, department, salary
FROM employees
ORDER BY salary DESC
OFFSET 2 ROWS
FETCH FIRST 3 ROWS WITH TIES;
\d+ emp_view_ties

CREATE VIEW emp_view_percent
AS
SELECT id, name, department, salary
FROM employees
ORDER BY salary DESC
OFFSET 2 ROWS
FETCH FIRST 33.4 PERCENT ROWS ONLY;
\d+ emp_view_percent

CREATE VIEW emp_view_percent_ties
AS
SELECT id, name, department, salary
FROM employees
ORDER BY salary DESC
OFFSET 2 ROWS
FETCH FIRST 45.8 PERCENT ROWS WITH TIES;
\d+ emp_view_percent_ties

CREATE VIEW emp_view_limit
AS
SELECT id, name, department, salary
FROM employees
ORDER BY salary DESC
LIMIT 5;
\d+ emp_view_limit

SELECT COUNT(*) FROM emp_mv;
SELECT COUNT(*) FROM emp_view_only;
SELECT COUNT(*) FROM emp_view_ties;
SELECT COUNT(*) FROM emp_view_percent;
SELECT COUNT(*) FROM emp_view_percent_ties;

INSERT INTO employees (id, name, department, salary) VALUES (10, 'Eve', 'Finance', 6200);

-- 7.4 : 更新视图
REFRESH MATERIALIZED VIEW emp_mv;
SELECT COUNT(*) FROM emp_mv;
SELECT COUNT(*) FROM emp_view_only;
SELECT COUNT(*) FROM emp_view_ties;
SELECT COUNT(*) FROM emp_view_percent_ties;

-- 测试用例 8：PL/pgSQL 函数
DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN 
        SELECT COUNT(*) AS count
        FROM (
          SELECT * 
          FROM test_table 
          ORDER BY score DESC 
          FETCH FIRST 33.4 PERCENT ROWS WITH TIES
        )
    LOOP
        RAISE NOTICE 'Total: %', rec.count;
    END LOOP;
END $$;

-- 测试用例 9: SMP支持
SET query_dop=1002;
DELETE FROM test_table;
INSERT INTO test_table SELECT generate_series(1, 100000), 'Alice', RANDOM() % 100, RANDOM() % 10;
CREATE INDEX ON test_table (score);
SELECT DISTINCT score FROM (
  SELECT t1.id AS id, t1.score AS score 
  FROM test_table AS t1 
  JOIN test_table AS t2 ON t1.id = t2.id
  ORDER BY t1.score DESC
  FETCH FIRST 10 ROWS WITH TIES
) AS t;
SET query_dop=1;

DROP TABLE test_table CASCADE;

-- PERCENT 功能测试
-- 创建测试表
CREATE TABLE percent_test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    score INT,
    group_id INT
);

-- 插入测试数据
INSERT INTO percent_test_table (name, score, group_id) VALUES
('Alice', 95, 1),
('Bob', 90, 1),
('Charlie', 85, 2),
('David', 80, 2),
('Eve', 75, 3),
('Frank', 70, 3),
('Grace', 65, 4),
('Heidi', 60, 4),
('Ivan', 55, 5),
('Judy', 50, 5),
('Ken', 45, 6),
('Laura', 40, 6),
('Mike', 35, 7),
('Nancy', 30, 7),
('Oscar', 25, 8),
('Peggy', 20, 8),
('Quincy', 15, 9),
('Ruth', 10, 9);

-- 用例 1: 未排序查询使用 FETCH NEXT N PERCENT
SELECT * FROM percent_test_table FETCH NEXT 50 PERCENT ROWS ONLY;

-- 用例 2: FETCH NEXT N PERCENT 与 ORDER BY 结合
SELECT * FROM percent_test_table ORDER BY score DESC FETCH NEXT 50 PERCENT ROWS ONLY;

-- 用例 3: FETCH NEXT N PERCENT 与 OFFSET 结合
SELECT * FROM percent_test_table ORDER BY score DESC OFFSET 5 FETCH NEXT 30 PERCENT ROWS ONLY;

-- 用例 4: 使用不同百分比值的测试
-- 获取 10% 的行
SELECT * FROM percent_test_table ORDER BY score DESC FETCH NEXT 10 PERCENT ROWS ONLY;

-- 获取 75% 的行
SELECT * FROM percent_test_table ORDER BY score DESC FETCH NEXT 75 PERCENT ROWS ONLY;

-- 用例 5: 超过 100% 的百分比测试
SELECT * FROM percent_test_table ORDER BY score DESC FETCH NEXT 110 PERCENT ROWS ONLY;

-- 用例 6: FETCH NEXT N PERCENT WITH TIES
SELECT * FROM percent_test_table ORDER BY score DESC FETCH NEXT 50 PERCENT ROWS WITH TIES;

-- 用例 7: FETCH NEXT 0 PERCENT
SELECT * FROM percent_test_table ORDER BY score DESC FETCH NEXT 0 PERCENT ROWS ONLY;

-- 用例 8：FETCH NEXT N ROWS 使用浮点数
SELECT * FROM percent_test_table ORDER BY score DESC FETCH NEXT 50.5 PERCENT ROWS ONLY;
SELECT * FROM percent_test_table ORDER BY score DESC FETCH NEXT 50.5 PERCENT ROWS WITH TIES;

-- 用例 9: 空数据集测试
CREATE TABLE empty_table (
    id SERIAL PRIMARY KEY,
    value INT
);

-- 尝试从空表中获取数据
SELECT * FROM empty_table FETCH NEXT 0 PERCENT ROWS ONLY;
SELECT * FROM empty_table FETCH NEXT 50 PERCENT ROWS ONLY;
SELECT * FROM empty_table FETCH NEXT NULL PERCENT ROWS WITH TIES; 

-- 用例 10: 多列排序测试
SELECT * FROM percent_test_table ORDER BY group_id ASC, score DESC FETCH NEXT 30 PERCENT ROWS ONLY;

-- 用例 11: 含有 NULL 值的数据处理
-- 插入包含 NULL 的数据
INSERT INTO percent_test_table (name, score, group_id) VALUES
('Tom', NULL, 10),
('Jerry', NULL, 11);

-- 按 score 排序并获取 50% 的行，处理 NULL
SELECT * FROM percent_test_table ORDER BY score DESC NULLS LAST FETCH NEXT 50 PERCENT ROWS ONLY;

-- 用例 12：分区表支持
CREATE TABLE percent_partitioned_table (
    id int,
    name VARCHAR2(50),
    score NUMBER,
    group_id NUMBER
)
PARTITION BY RANGE (group_id) (
    PARTITION percent_partition_1 VALUES LESS THAN (3),
    PARTITION percent_partition_2 VALUES LESS THAN (5),
    PARTITION percent_partition_3 VALUES LESS THAN (7),
    PARTITION percent_partition_4 VALUES LESS THAN (9),
    PARTITION percent_partition_5 VALUES LESS THAN (11)
);

INSERT INTO percent_partitioned_table (name, score, group_id) VALUES
('Alice', 95, 1),
('Bob', 90, 1),
('Charlie', 85, 2),
('David', 80, 2),
('Eve', 75, 3),
('Frank', 70, 3),
('Peter', 70, 3),
('Grace', 65, 4),
('Heidi', 60, 4),
('Ivan', 55, 5),
('Judy', 50, 5),
('Ken', 45, 6),
('Laura', 40, 6),
('Mike', 35, 7),
('Nancy', 30, 7),
('Oscar', 25, 8),
('Peggy', 20, 8),
('Quincy', 15, 9),
('Ruth', 10, 9);

-- 分区表用例 1: 未排序查询使用 FETCH NEXT N PERCENT
SELECT * FROM percent_partitioned_table FETCH FIRST 50 PERCENT ROWS ONLY;

-- 分区表用例 2: FETCH NEXT N PERCENT 与 ORDER BY 结合
SELECT * FROM percent_partitioned_table ORDER BY score DESC FETCH FIRST 50 PERCENT ROWS ONLY;

-- 分区表用例 3: FETCH NEXT N PERCENT 与 OFFSET 结合
SELECT * FROM percent_partitioned_table ORDER BY score DESC OFFSET 5 ROWS FETCH NEXT 30 PERCENT ROWS ONLY;

-- 分区表用例 4: 使用不同百分比值的测试
-- 获取 10% 的行
SELECT * FROM percent_partitioned_table ORDER BY score DESC FETCH FIRST 10 PERCENT ROWS ONLY;

-- 获取 75% 的行
SELECT * FROM percent_partitioned_table ORDER BY score DESC FETCH FIRST 75 PERCENT ROWS ONLY;

-- 分区表用例 5: 超过 100% 的百分比测试
SELECT * FROM percent_partitioned_table ORDER BY score DESC FETCH FIRST 110 PERCENT ROWS ONLY;

-- 分区表用例 6: FETCH NEXT N PERCENT WITH TIES
SELECT * FROM percent_partitioned_table ORDER BY score DESC FETCH FIRST 50 PERCENT ROWS WITH TIES;

-- 分区表用例 7: FETCH NEXT 0 PERCENT
SELECT * FROM percent_partitioned_table ORDER BY score DESC FETCH FIRST 0 PERCENT ROWS ONLY;

-- 列存支持测试
CREATE TABLE customer_test2
(
  state_ID   CHAR(2),
  state_NAME VARCHAR2(40),
  num    NUMBER
)
WITH (ORIENTATION = COLUMN);

INSERT INTO customer_test2 VALUES ('CA', 'California', 1);
INSERT INTO customer_test2 VALUES ('TX', 'Texas', 2);
INSERT INTO customer_test2 VALUES ('NY', 'New York', 3);
INSERT  INTO customer_test2 VALUES ('FL', 'Florida', 3);
INSERT INTO customer_test2 VALUES ('FL', 'Florida', 4);
INSERT INTO customer_test2 VALUES ('IL', 'Illinois', 4);
INSERT INTO customer_test2 VALUES ('IL', 'Illinois', 5);

SELECT * FROM customer_test2 ORDER BY num DESC FETCH FIRST 50 PERCENT ROWS ONLY;
SELECT * FROM customer_test2 ORDER BY num DESC FETCH FIRST 50 PERCENT ROWS WITH TIES;

-- 测试用例 13: 列存表
CREATE TABLE customer_test3
(
  state_ID   CHAR(2),
  state_NAME VARCHAR2(40),
  num    NUMBER
) WITH (ORIENTATION = COLUMN);
INSERT INTO customer_test3 VALUES ('CA', 'California', 1);
INSERT INTO customer_test3 VALUES ('TX', 'Texas', 2);

-- 用例 13.1: 正常执行
SELECT * FROM customer_test3 ORDER BY num DESC FETCH FIRST 5 ROWS ONLY;
-- 用例 13.2: WITH TIES
-- NOT SUPPORTED
SELECT * FROM customer_test3 ORDER BY num DESC FETCH FIRST 50 PERCENT ROWS ONLY;
-- NOT SUPPORTED
SELECT * FROM customer_test3 ORDER BY num DESC FETCH FIRST 5 ROWS WITH TIES;
-- NOT SUPPORTED
SELECT * FROM customer_test3 ORDER BY num DESC FETCH FIRST 50 PERCENT ROWS WITH TIES;
-- NOT SUPPORTED
SELECT * FROM customer_test3 FETCH NEXT 10 ROWS WITH TIES;

-- 用例 14：存储过程
CREATE OR REPLACE PROCEDURE fetch_rows_with_ties_procedure
IS
BEGIN
    FOR rec IN 
        SELECT * FROM percent_partitioned_table ORDER BY score DESC FETCH FIRST 33.4 PERCENT ROWS WITH TIES
    LOOP
        RAISE NOTICE 'Name: %, Score: %', rec.name, rec.score;
    END LOOP;
END;
/
CALL fetch_rows_with_ties_procedure();

\c postgres
DROP DATABASE frwt_db;
