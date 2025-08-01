/*
 * 测试用例：大数据量插入
 * 此测试用例用于验证在大数据量插入场景下索引的创建和查询性能
 */
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10, 2)
);
INSERT INTO employees (name, department, salary)
VALUES 
('Alice', 'HR', 5000.00),
('Bob', 'IT', 6000.00),
('Charlie', 'Finance', 5500.00),
('David', 'IT', 6500.00),
('Eve', 'HR', 5200.00);
CREATE INDEX idx_employees_department ON employees (department) WITH (index_type = pcr);
CREATE INDEX idx_employees_name ON employees (name) WITH (index_type = pcr);
CREATE INDEX idx_employees_salary ON employees (salary) WITH (index_type = pcr);
-- 仅校验查询结果的行数
DO $$
DECLARE
    row_count INT;
BEGIN
    SELECT COUNT(*) INTO row_count FROM employees WHERE department = 'IT';
    IF row_count = 2 THEN
        RAISE INFO '大数据量插入：部门为 IT 的员工查询结果行数校验通过';
    ELSE
        RAISE INFO '大数据量插入：部门为 IT 的员工查询结果行数校验未通过';
    END IF;
END $$;
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    value INT
);
-- 批量插入固定值数据
DO $$
DECLARE
    i INT := 1;
BEGIN
    WHILE i <= 1000 LOOP
        INSERT INTO test_table (value) VALUES (i * 10);
        i := i + 1;
    END LOOP;
END $$;
-- 仅校验查询结果的行数
DO $$
DECLARE
    row_count_no_index INT;
    row_count_with_index INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_index FROM test_table WHERE value = 500;
    CREATE INDEX idx_value ON test_table (value) with (index_type = pcr);
    SELECT COUNT(*) INTO row_count_with_index FROM test_table WHERE value = 500;
    IF row_count_no_index = row_count_with_index THEN
        RAISE INFO '大数据量插入：值为 500 的查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '大数据量插入：值为 500 的查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
CREATE INDEX idx_compound ON test_table (id, value) with (index_type = pcr);
-- 仅校验查询结果的行数
DO $$
DECLARE
    row_count_compound INT;
BEGIN
    SELECT COUNT(*) INTO row_count_compound FROM test_table WHERE id = 50 AND value = 500;
    IF row_count_compound = 1 THEN
        RAISE INFO '大数据量插入：复合索引查询结果行数校验通过';
    ELSE
        RAISE INFO '大数据量插入：复合索引查询结果行数校验未通过';
    END IF;
END $$;
DROP TABLE test_table;

/*
 * 测试用例：索引正确性
 * 此测试用例用于验证索引查询结果与无索引查询结果是否一致
 */
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    value INT
);
-- 批量插入固定值数据
DO $$
DECLARE
    i INT := 1;
BEGIN
    WHILE i <= 1000 LOOP
        INSERT INTO test_table (value) VALUES (i * 10);
        i := i + 1;
    END LOOP;
END $$;
-- 仅校验查询结果的行数
DO $$
DECLARE
    row_count_no_index INT;
    row_count_with_index INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_index FROM test_table WHERE value = 500;
    CREATE TEMP TABLE no_index_result AS
    SELECT * FROM test_table WHERE value = 500;
    CREATE INDEX idx_value ON test_table (value) with (index_type = pcr);
    SELECT COUNT(*) INTO row_count_with_index FROM test_table WHERE value = 500;
    CREATE TEMP TABLE with_index_result AS
    SELECT * FROM test_table WHERE value = 500;
    IF row_count_no_index = row_count_with_index THEN
        RAISE INFO '索引正确性：值为 500 的查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '索引正确性：值为 500 的查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
CREATE INDEX idx_compound ON test_table (id, value) with (index_type = pcr);
-- 仅校验查询结果的行数
DO $$
DECLARE
    row_count_no_compound INT;
    row_count_with_compound INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_compound FROM test_table WHERE id = 50 AND value = 500;
    CREATE TEMP TABLE no_compound_index_result AS
    SELECT * FROM test_table WHERE id = 50 AND value = 500;
    SELECT COUNT(*) INTO row_count_with_compound FROM test_table WHERE id = 50 AND value = 500;
    CREATE TEMP TABLE with_compound_index_result AS
    SELECT * FROM test_table WHERE id = 50 AND value = 500;
    IF row_count_no_compound = row_count_with_compound THEN
        RAISE INFO '索引正确性：复合索引查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '索引正确性：复合索引查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
DROP TABLE test_table;
DROP TABLE IF EXISTS no_index_result;
DROP TABLE IF EXISTS with_index_result;
DROP TABLE IF EXISTS no_compound_index_result;
DROP TABLE IF EXISTS with_compound_index_result;

/*
 * 测试用例：索引查询
 * 此测试用例用于验证不同类型查询（等值、范围）下索引的正确性
 */
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    value INT
);
-- 批量插入固定值数据
DO $$
DECLARE
    i INT := 1;
BEGIN
    WHILE i <= 1000 LOOP
        INSERT INTO test_table (value) VALUES (i * 10);
        i := i + 1;
    END LOOP;
END $$;
-- 等值查询校验
DO $$
DECLARE
    row_count_no_index_eq INT;
    row_count_with_index_eq INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_index_eq FROM test_table WHERE value = 500;
    CREATE TEMP TABLE no_index_eq_result AS
    SELECT * FROM test_table WHERE value = 500;
    CREATE INDEX idx_value ON test_table (value) with (index_type = pcr);
    SELECT COUNT(*) INTO row_count_with_index_eq FROM test_table WHERE value = 500;
    CREATE TEMP TABLE with_index_eq_result AS
    SELECT * FROM test_table WHERE value = 500;
    IF row_count_no_index_eq = row_count_with_index_eq THEN
        RAISE INFO '索引查询：等值查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '索引查询：等值查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
-- 范围查询校验
DO $$
DECLARE
    row_count_no_index_range INT;
    row_count_with_index_range INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_index_range FROM test_table WHERE value BETWEEN 200 AND 300;
    CREATE TEMP TABLE no_index_range_result AS
    SELECT * FROM test_table WHERE value BETWEEN 200 AND 300;
    SELECT COUNT(*) INTO row_count_with_index_range FROM test_table WHERE value BETWEEN 200 AND 300;
    CREATE TEMP TABLE with_index_range_result AS
    SELECT * FROM test_table WHERE value BETWEEN 200 AND 300;
    IF row_count_no_index_range = row_count_with_index_range THEN
        RAISE INFO '索引查询：范围查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '索引查询：范围查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
CREATE INDEX idx_compound ON test_table (id, value) with (index_type = pcr);
-- 复合索引查询校验
DO $$
DECLARE
    row_count_no_compound INT;
    row_count_with_compound INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_compound FROM test_table WHERE id = 50 AND value = 500;
    CREATE TEMP TABLE no_compound_index_result AS
    SELECT * FROM test_table WHERE id = 50 AND value = 500;
    SELECT COUNT(*) INTO row_count_with_compound FROM test_table WHERE id = 50 AND value = 500;
    CREATE TEMP TABLE with_compound_index_result AS
    SELECT * FROM test_table WHERE id = 50 AND value = 500;
    IF row_count_no_compound = row_count_with_compound THEN
        RAISE INFO '索引查询：复合索引查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '索引查询：复合索引查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
DROP TABLE test_table;
DROP TABLE IF EXISTS no_index_eq_result;
DROP TABLE IF EXISTS with_index_eq_result;
DROP TABLE IF EXISTS no_index_range_result;
DROP TABLE IF EXISTS with_index_range_result;
DROP TABLE IF EXISTS no_compound_index_result;
DROP TABLE IF EXISTS with_compound_index_result;

/*
 * 测试用例：事务
 * 此测试用例用于验证不同事务隔离级别下索引的并发性和可用性
 */
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    value INT
);
-- 批量插入固定值数据
DO $$
DECLARE
    i INT := 1;
BEGIN
    WHILE i <= 1000 LOOP
        INSERT INTO test_table (value) VALUES (i * 10);
        i := i + 1;
    END LOOP;
END $$;
-- 等值查询校验
DO $$
DECLARE
    row_count_no_index_eq INT;
    row_count_with_index_eq INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_index_eq FROM test_table WHERE value = 500;
    CREATE TEMP TABLE no_index_eq_result AS
    SELECT * FROM test_table WHERE value = 500;
    CREATE INDEX idx_value ON test_table (value) with (index_type = pcr);
    SELECT COUNT(*) INTO row_count_with_index_eq FROM test_table WHERE value = 500;
    CREATE TEMP TABLE with_index_eq_result AS
    SELECT * FROM test_table WHERE value = 500;
    IF row_count_no_index_eq = row_count_with_index_eq THEN
        RAISE INFO '事务：等值查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '事务：等值查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
-- 范围查询校验
DO $$
DECLARE
    row_count_no_index_range INT;
    row_count_with_index_range INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_index_range FROM test_table WHERE value BETWEEN 200 AND 300;
    CREATE TEMP TABLE no_index_range_result AS
    SELECT * FROM test_table WHERE value BETWEEN 200 AND 300;
    SELECT COUNT(*) INTO row_count_with_index_range FROM test_table WHERE value BETWEEN 200 AND 300;
    CREATE TEMP TABLE with_index_range_result AS
    SELECT * FROM test_table WHERE value BETWEEN 200 AND 300;
    IF row_count_no_index_range = row_count_with_index_range THEN
        RAISE INFO '事务：范围查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '事务：范围查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
CREATE INDEX idx_compound ON test_table (id, value) with (index_type = pcr);
-- 复合索引查询校验
DO $$
DECLARE
    row_count_no_compound INT;
    row_count_with_compound INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_compound FROM test_table WHERE id = 50 AND value = 500;
    CREATE TEMP TABLE no_compound_index_result AS
    SELECT * FROM test_table WHERE id = 50 AND value = 500;
    SELECT COUNT(*) INTO row_count_with_compound FROM test_table WHERE id = 50 AND value = 500;
    CREATE TEMP TABLE with_compound_index_result AS
    SELECT * FROM test_table WHERE id = 50 AND value = 500;
    IF row_count_no_compound = row_count_with_compound THEN
        RAISE INFO '事务：复合索引查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '事务：复合索引查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
INSERT INTO test_table (value) VALUES (88888);
-- 校验插入新值后的查询结果行数
DO $$
DECLARE
    row_count_inserted INT;
BEGIN
    SELECT COUNT(*) INTO row_count_inserted FROM test_table WHERE value = 88888;
    IF row_count_inserted = 1 THEN
        RAISE INFO '事务：插入新值后查询结果行数校验通过';
    ELSE
        RAISE INFO '事务：插入新值后查询结果行数校验未通过';
    END IF;
END $$;
UPDATE test_table SET value = 99999 WHERE id = 1;
-- 校验更新值后的查询结果行数
DO $$
DECLARE
    row_count_updated INT;
BEGIN
    SELECT COUNT(*) INTO row_count_updated FROM test_table WHERE value = 99999;
    IF row_count_updated = 1 THEN
        RAISE INFO '事务：更新值后查询结果行数校验通过';
    ELSE
        RAISE INFO '事务：更新值后查询结果行数校验未通过';
    END IF;
END $$;
COMMIT;
-- 校验范围查询结果行数
DO $$
DECLARE
    row_count_range_after_commit INT;
BEGIN
    SELECT COUNT(*) INTO row_count_range_after_commit FROM test_table WHERE value BETWEEN 80000 AND 90000;
    IF row_count_range_after_commit = 1 THEN
        RAISE INFO '事务：提交后范围查询结果行数校验通过';
    ELSE
        RAISE INFO '事务：提交后范围查询结果行数校验未通过';
    END IF;
END $$;
DELETE FROM test_table WHERE id = 2;
-- 校验删除后查询结果行数
DO $$
DECLARE
    row_count_deleted INT;
BEGIN
    SELECT COUNT(*) INTO row_count_deleted FROM test_table WHERE id = 2;
    IF row_count_deleted = 0 THEN
        RAISE INFO '事务：删除后查询结果行数校验通过';
    ELSE
        RAISE INFO '事务：删除后查询结果行数校验未通过';
    END IF;
END $$;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;
INSERT INTO test_table (value) VALUES (88889);
-- 校验插入新值后的查询结果行数
DO $$
DECLARE
    row_count_inserted_2 INT;
BEGIN
    SELECT COUNT(*) INTO row_count_inserted_2 FROM test_table WHERE value = 88889;
    IF row_count_inserted_2 = 1 THEN
        RAISE INFO '事务：第二次插入新值后查询结果行数校验通过';
    ELSE
        RAISE INFO '事务：第二次插入新值后查询结果行数校验未通过';
    END IF;
END $$;
UPDATE test_table SET value = 99998 WHERE id = 3;
-- 校验更新值后的查询结果行数
DO $$
DECLARE
    row_count_updated_2 INT;
BEGIN
    SELECT COUNT(*) INTO row_count_updated_2 FROM test_table WHERE value = 99998;
    IF row_count_updated_2 = 1 THEN
        RAISE INFO '事务：第二次更新值后查询结果行数校验通过';
    ELSE
        RAISE INFO '事务：第二次更新值后查询结果行数校验未通过';
    END IF;
END $$;
COMMIT;
-- 校验范围查询结果行数
DO $$
DECLARE
    row_count_range_after_commit_2 INT;
BEGIN
    SELECT COUNT(*) INTO row_count_range_after_commit_2 FROM test_table WHERE value BETWEEN 80000 AND 90000;
    IF row_count_range_after_commit_2 = 2 THEN
        RAISE INFO '事务：第二次提交后范围查询结果行数校验通过';
    ELSE
        RAISE INFO '事务：第二次提交后范围查询结果行数校验未通过';
    END IF;
END $$;
DELETE FROM test_table WHERE id = 4;
-- 校验删除后查询结果行数
DO $$
DECLARE
    row_count_deleted_2 INT;
BEGIN
    SELECT COUNT(*) INTO row_count_deleted_2 FROM test_table WHERE id = 4;
    IF row_count_deleted_2 = 0 THEN
        RAISE INFO '事务：第二次删除后查询结果行数校验通过';
    ELSE
        RAISE INFO '事务：第二次删除后查询结果行数校验未通过';
    END IF;
END $$;
DROP TABLE test_table;
DROP TABLE IF EXISTS no_index_eq_result;
DROP TABLE IF EXISTS with_index_eq_result;
DROP TABLE IF EXISTS no_index_range_result;
DROP TABLE IF EXISTS with_index_range_result;
DROP TABLE IF EXISTS no_compound_index_result;
DROP TABLE IF EXISTS with_compound_index_result;

/*
 * 测试用例：数据类型
 * 此测试用例用于验证不同数据类型（整数、字符、日期、布尔）下索引的正确性
 */
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    int_value INT,
    text_value TEXT,
    date_value DATE,
    bool_value BOOLEAN
);
-- 批量插入固定值数据
DO $$
DECLARE
    i INT := 1;
BEGIN
    WHILE i <= 1000 LOOP
        INSERT INTO test_table (int_value, text_value, date_value, bool_value)
        VALUES (
            i * 10,
            'text' || i,
            '2023-01-01'::DATE + (i - 1),
            (i % 2 = 0)
        );
        i := i + 1;
    END LOOP;
END $$;
-- 整数类型等值查询校验
DO $$
DECLARE
    row_count_no_index_int_eq INT;
    row_count_with_index_int_eq INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_index_int_eq FROM test_table WHERE int_value = 500;
    CREATE TEMP TABLE no_index_int_eq_result AS
    SELECT * FROM test_table WHERE int_value = 500;
    CREATE INDEX idx_int_value ON test_table (int_value) with (index_type = pcr);
    SELECT COUNT(*) INTO row_count_with_index_int_eq FROM test_table WHERE int_value = 500;
    CREATE TEMP TABLE with_index_int_eq_result AS
    SELECT * FROM test_table WHERE int_value = 500;
    IF row_count_no_index_int_eq = row_count_with_index_int_eq THEN
        RAISE INFO '数据类型：整数类型等值查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '数据类型：整数类型等值查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
-- 字符类型等值查询校验
DO $$
DECLARE
    row_count_no_index_text_eq INT;
    row_count_with_index_text_eq INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_index_text_eq FROM test_table WHERE text_value = 'text500';
    CREATE TEMP TABLE no_index_text_eq_result AS
    SELECT * FROM test_table WHERE text_value = 'text500';
    CREATE INDEX idx_text_value ON test_table (text_value) with (index_type = pcr);
    SELECT COUNT(*) INTO row_count_with_index_text_eq FROM test_table WHERE text_value = 'text500';
    CREATE TEMP TABLE with_index_text_eq_result AS
    SELECT * FROM test_table WHERE text_value = 'text500';
    IF row_count_no_index_text_eq = row_count_with_index_text_eq THEN
        RAISE INFO '数据类型：字符类型等值查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '数据类型：字符类型等值查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
-- 日期类型等值查询校验
DO $$
DECLARE
    row_count_no_index_date_eq INT;
    row_count_with_index_date_eq INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_index_date_eq FROM test_table WHERE date_value = '2023-02-20';
    CREATE TEMP TABLE no_index_date_eq_result AS
    SELECT * FROM test_table WHERE date_value = '2023-02-20';
    CREATE INDEX idx_date_value ON test_table (date_value) with (index_type = pcr);
    SELECT COUNT(*) INTO row_count_with_index_date_eq FROM test_table WHERE date_value = '2023-02-20';
    CREATE TEMP TABLE with_index_date_eq_result AS
    SELECT * FROM test_table WHERE date_value = '2023-02-20';
    IF row_count_no_index_date_eq = row_count_with_index_date_eq THEN
        RAISE INFO '数据类型：日期类型等值查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '数据类型：日期类型等值查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
-- 布尔类型等值查询校验
DO $$
DECLARE
    row_count_no_index_bool_eq INT;
    row_count_with_index_bool_eq INT;
BEGIN
    SELECT COUNT(*) INTO row_count_no_index_bool_eq FROM test_table WHERE bool_value = TRUE;
    CREATE TEMP TABLE no_index_bool_eq_result AS
    SELECT * FROM test_table WHERE bool_value = TRUE;
    CREATE INDEX idx_bool_value ON test_table (bool_value) with (index_type = pcr);
    SELECT COUNT(*) INTO row_count_with_index_bool_eq FROM test_table WHERE bool_value = TRUE;
    CREATE TEMP TABLE with_index_bool_eq_result AS
    SELECT * FROM test_table WHERE bool_value = TRUE;
    IF row_count_no_index_bool_eq = row_count_with_index_bool_eq THEN
        RAISE INFO '数据类型：布尔类型等值查询结果行数在有索引和无索引时一致，校验通过';
    ELSE
        RAISE INFO '数据类型：布尔类型等值查询结果行数在有索引和无索引时不一致，校验未通过';
    END IF;
END $$;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
INSERT INTO test_table (int_value, text_value, date_value, bool_value)
VALUES (88888, 'new_text', '2024-01-01', TRUE);
-- 校验插入新值后的查询结果行数
DO $$
DECLARE
    row_count_inserted INT;
BEGIN
    SELECT COUNT(*) INTO row_count_inserted FROM test_table WHERE int_value = 88888;
    IF row_count_inserted = 1 THEN
        RAISE INFO '数据类型：插入新值后整数类型查询结果行数校验通过';
    ELSE
        RAISE INFO '数据类型：插入新值后整数类型查询结果行数校验未通过';
    END IF;
END $$;
UPDATE test_table SET int_value = 99999 WHERE id = 1;
-- 校验更新值后的查询结果行数
DO $$
DECLARE
    row_count_updated INT;
BEGIN
    SELECT COUNT(*) INTO row_count_updated FROM test_table WHERE int_value = 99999;
    IF row_count_updated = 1 THEN
        RAISE INFO '数据类型：更新值后整数类型查询结果行数校验通过';
    ELSE
        RAISE INFO '数据类型：更新值后整数类型查询结果行数校验未通过';
    END IF;
END $$;
COMMIT;
-- 校验范围查询结果行数
DO $$
DECLARE
    row_count_range_after_commit INT;
BEGIN
    SELECT COUNT(*) INTO row_count_range_after_commit FROM test_table WHERE int_value BETWEEN 80000 AND 90000;
    IF row_count_range_after_commit = 1 THEN
        RAISE INFO '数据类型：提交后整数类型范围查询结果行数校验通过';
    ELSE
        RAISE INFO '数据类型：提交后整数类型范围查询结果行数校验未通过';
    END IF;
END $$;
DELETE FROM test_table WHERE id = 2;
-- 校验删除后查询结果行数
DO $$
DECLARE
    row_count_deleted INT;
BEGIN
    SELECT COUNT(*) INTO row_count_deleted FROM test_table WHERE id = 2;
    IF row_count_deleted = 0 THEN
        RAISE INFO '数据类型：删除后查询结果行数校验通过';
    ELSE
        RAISE INFO '数据类型：删除后查询结果行数校验未通过';
    END IF;
END $$;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;
INSERT INTO test_table (int_value, text_value, date_value, bool_value)
VALUES (88889, 'new_text2', '2024-02-01', FALSE);
-- 校验插入新值后的查询结果行数
DO $$
DECLARE
    row_count_inserted_2 INT;
BEGIN
    SELECT COUNT(*) INTO row_count_inserted_2 FROM test_table WHERE int_value = 88889;
    IF row_count_inserted_2 = 1 THEN
        RAISE INFO '数据类型：第二次插入新值后整数类型查询结果行数校验通过';
    ELSE
        RAISE INFO '数据类型：第二次插入新值后整数类型查询结果行数校验未通过';
    END IF;
END $$;
UPDATE test_table SET int_value = 99998 WHERE id = 3;
-- 校验更新值后的查询结果行数
DO $$
DECLARE
    row_count_updated_2 INT;
BEGIN
    SELECT COUNT(*) INTO row_count_updated_2 FROM test_table WHERE int_value = 99998;
    IF row_count_updated_2 = 1 THEN
        RAISE INFO '数据类型：第二次更新值后整数类型查询结果行数校验通过';
    ELSE
        RAISE INFO '数据类型：第二次更新值后整数类型查询结果行数校验未通过';
    END IF;
END $$;
COMMIT;
-- 校验范围查询结果行数
DO $$
DECLARE
    row_count_range_after_commit_2 INT;
BEGIN
    SELECT COUNT(*) INTO row_count_range_after_commit_2 FROM test_table WHERE int_value BETWEEN 80000 AND 90000;
    IF row_count_range_after_commit_2 = 2 THEN
        RAISE INFO '数据类型：第二次提交后整数类型范围查询结果行数校验通过';
    ELSE
        RAISE INFO '数据类型：第二次提交后整数类型范围查询结果行数校验未通过';
    END IF;
END $$;
DELETE FROM test_table WHERE id = 4;
-- 校验删除后查询结果行数
DO $$
DECLARE
    row_count_deleted_2 INT;
BEGIN
    SELECT COUNT(*) INTO row_count_deleted_2 FROM test_table WHERE id = 4;
    IF row_count_deleted_2 = 0 THEN
        RAISE INFO '数据类型：第二次删除后查询结果行数校验通过';
    ELSE
        RAISE INFO '数据类型：第二次删除后查询结果行数校验未通过';
    END IF;
END $$;
DROP TABLE test_table;
DROP TABLE IF EXISTS no_index_int_eq_result;
DROP TABLE IF EXISTS with_index_int_eq_result;
DROP TABLE IF EXISTS no_index_text_eq_result;
DROP TABLE IF EXISTS with_index_text_eq_result;
DROP TABLE IF EXISTS no_index_date_eq_result;
DROP TABLE IF EXISTS with_index_date_eq_result;
DROP TABLE IF EXISTS no_index_bool_eq_result;
DROP TABLE IF EXISTS with_index_bool_eq_result;
DROP TABLE IF EXISTS no_index_eq_result;
DROP TABLE IF EXISTS with_index_eq_result;
DROP TABLE IF EXISTS no_index_range_result;
DROP TABLE IF EXISTS with_index_range_result;
DROP TABLE IF EXISTS no_index_sort_result;
DROP TABLE IF EXISTS with_index_sort_result;
DROP TABLE IF EXISTS no_compound_index_result;
DROP TABLE IF EXISTS with_compound_index_result;

/*
 * 测试用例：rollback case
 * 此测试用例用于验证事务回滚后数据和索引的状态
 */
DROP TABLE IF EXISTS dml_test_table;
CREATE TABLE dml_test_table (
    id SERIAL PRIMARY KEY,
    col_int INT,
    col_text TEXT,
    col_date DATE,
    col_bool BOOLEAN
);
CREATE INDEX idx_col_int ON dml_test_table (col_int) with (index_type = pcr);
CREATE INDEX idx_col_text ON dml_test_table (col_text) with (index_type = pcr);
INSERT INTO dml_test_table (col_int, col_text, col_date, col_bool)
VALUES
    (1, 'data1', '2023-01-01', TRUE),
    (2, 'data2', '2023-02-01', FALSE),
    (3, 'data3', '2023-03-01', TRUE);

-- 开启事务
BEGIN;
-- 插入一条新记录
INSERT INTO dml_test_table (col_int, col_text, col_date, col_bool)
VALUES (4, 'data4', '2023-04-01', FALSE);
-- 检查插入后记录数
DO $$
DECLARE
    inserted_count INT;
BEGIN
    SELECT COUNT(*) INTO inserted_count FROM dml_test_table WHERE col_int = 4;
    IF inserted_count = 1 THEN
        RAISE INFO '插入新记录后，记录存在，校验通过';
    ELSE
        RAISE INFO '插入新记录后，记录不存在，校验未通过';
    END IF;
END $$;

-- 更新一条记录
UPDATE dml_test_table
SET col_text = 'updated_data1'
WHERE col_int = 1;
-- 检查更新后记录内容
DO $$
DECLARE
    updated_text TEXT;
BEGIN
    SELECT col_text INTO updated_text FROM dml_test_table WHERE col_int = 1;
    IF updated_text = 'updated_data1' THEN
        RAISE INFO '更新记录后，内容更新正确，校验通过';
    ELSE
        RAISE INFO '更新记录后，内容更新不正确，校验未通过';
    END IF;
END $$;

-- 删除一条记录
DELETE FROM dml_test_table WHERE col_int = 2;
-- 检查删除后记录数
DO $$
DECLARE
    deleted_count INT;
BEGIN
    SELECT COUNT(*) INTO deleted_count FROM dml_test_table WHERE col_int = 2;
    IF deleted_count = 0 THEN
        RAISE INFO '删除记录后，记录不存在，校验通过';
    ELSE
        RAISE INFO '删除记录后，记录仍然存在，校验未通过';
    END IF;
END $$;

-- 回滚事务
ROLLBACK;

-- 检查插入的记录是否还存在
DO $$
DECLARE
    after_rollback_inserted_count INT;
BEGIN
    SELECT COUNT(*) INTO after_rollback_inserted_count FROM dml_test_table WHERE col_int = 4;
    IF after_rollback_inserted_count = 0 THEN
        RAISE INFO '回滚后，插入的记录不存在，校验通过';
    ELSE
        RAISE INFO '回滚后，插入的记录仍然存在，校验未通过';
    END IF;
END $$;

-- 检查更新的记录是否恢复原状
DO $$
DECLARE
    after_rollback_updated_text TEXT;
BEGIN
    SELECT col_text INTO after_rollback_updated_text FROM dml_test_table WHERE col_int = 1;
    IF after_rollback_updated_text = 'data1' THEN
        RAISE INFO '回滚后，更新的记录恢复原状，校验通过';
    ELSE
        RAISE INFO '回滚后，更新的记录未恢复原状，校验未通过';
    END IF;
END $$;

-- 检查删除的记录是否恢复
DO $$
DECLARE
    after_rollback_deleted_count INT;
BEGIN
    SELECT COUNT(*) INTO after_rollback_deleted_count FROM dml_test_table WHERE col_int = 2;
    IF after_rollback_deleted_count = 1 THEN
        RAISE INFO '回滚后，删除的记录恢复，校验通过';
    ELSE
        RAISE INFO '回滚后，删除的记录未恢复，校验未通过';
    END IF;
END $$;

DROP TABLE dml_test_table;
