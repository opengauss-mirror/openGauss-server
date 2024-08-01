-- create new schema
drop schema if exists test_binary;
create schema test_binary;
set search_path=test_binary;

set float_suffix_acceptance to on;
SELECT 3.14f;
SELECT 10.0d;
SELECT -2.5f;
SELECT -10.0d;
SELECT 1f;
SELECT 2d;
SELECT -3F;
SELECT -4D;
SELECT 123e3d;
SELECT 5.5df;
SELECT 3.6D;
SELECT 8.6fabc;

CREATE TABLE test_table (
    id INT,
    float_value FLOAT,
    double_value BINARY_DOUBLE
);

INSERT INTO test_table (id, float_value, double_value) VALUES (1, 3.14f, 2.7182d);
SELECT float_value * 2d, double_value + 1.5f FROM test_table;
DROP TABLE test_table;

SELECT round(3.14159f, 2);
SELECT trunc(3.14159f, 2);
SELECT sqrt(4.0f);
SELECT power(2.0f, 3.0f);
SELECT sin(0.5f);
SELECT cos(0.5f);
SELECT log(10.0f);
SELECT exp(2.0f);

CREATE FUNCTION test_function(input_val float) RETURNS float AS $$
BEGIN
  RETURN input_val * 2.0f;
END;
$$ LANGUAGE plpgsql;

SELECT test_function(10.5f);
drop function test_function;

CREATE TABLE test_trigger_src_tbl(id1 INT, id2 INT, id3 INT);
CREATE TABLE test_trigger_des_tbl(id1 INT, id2 INT, id3 INT);
CREATE OR REPLACE FUNCTION tri_insert_func() RETURNS TRIGGER AS
$$
DECLARE
    input_value FLOAT;
BEGIN
    input_value := TG_ARGV[0]::FLOAT;
    INSERT INTO test_trigger_des_tbl VALUES(NEW.id1, NEW.id2, NEW.id3, input_value);
    RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
CREATE TRIGGER insert_trigger
           BEFORE INSERT ON test_trigger_src_tbl
           FOR EACH ROW
           EXECUTE PROCEDURE tri_insert_func(2.5f);
DROP TRIGGER insert_trigger ON test_trigger_src_tbl;
CREATE TRIGGER insert_trigger
           BEFORE INSERT ON test_trigger_src_tbl
           FOR EACH ROW
           EXECUTE PROCEDURE tri_insert_func(2.5d);
DROP TRIGGER insert_trigger ON test_trigger_src_tbl;
drop function tri_insert_func;
drop table test_trigger_src_tbl;
drop table test_trigger_des_tbl;

SELECT 1.5f = 1.5; -- 返回 true
SELECT 1.5f <> 2.0; -- 返回 true
SELECT 1.5f > 1.0; -- 返回 true
SELECT 1.5f < 2.0; -- 返回 true
SELECT (1.5f > 1.0) AND (2.5f < 3.0); -- 返回 true
SELECT (1.5f > 1.0) OR (2.5f > 3.0); -- 返回 true
SELECT 1.5f + 2.5; -- 返回 4.0
SELECT 3.5f - 1.5; -- 返回 2.0
SELECT 2.0f * 3.0; -- 返回 6.0
SELECT 4.0f / 2.0; -- 返回 2.0

set float_suffix_acceptance to off;
SELECT 3.14f;
SELECT 10.0d;
SELECT -2.5f;
SELECT -10.0d;
SELECT 1f;
SELECT 2d;
SELECT -3F;
SELECT -4D;
SELECT 123e3d;
SELECT 5.5df;
SELECT 3.6D;
SELECT 8.6fabc;

INSERT INTO test_table (id, float_value, double_value) VALUES (1, 3.14f, 2.7182d);
SELECT float_value * 2d, double_value + 1.5f FROM test_table;
DROP TABLE test_table;

SELECT round(3.14159f, 2);
SELECT trunc(3.14159f, 2);
SELECT sqrt(4.0f);
SELECT power(2.0f, 3.0f);
SELECT sin(0.5f);
SELECT cos(0.5f);
SELECT log(10.0f);
SELECT exp(2.0f);

CREATE FUNCTION test_function(input_val float) RETURNS float AS $$
BEGIN
  RETURN input_val * 2.0f;
END;
$$ LANGUAGE plpgsql;

SELECT test_function(10.5f);
DROP FUNCTION test_function;

CREATE TABLE test_trigger_src_tbl(id1 INT, id2 INT, id3 INT);
CREATE TABLE test_trigger_des_tbl(id1 INT, id2 INT, id3 INT);
CREATE OR REPLACE FUNCTION tri_insert_func() RETURNS TRIGGER AS
$$
DECLARE
    input_value FLOAT;
BEGIN
    input_value := TG_ARGV[0]::FLOAT;
    INSERT INTO test_trigger_des_tbl VALUES(NEW.id1, NEW.id2, NEW.id3, input_value);
    RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
CREATE TRIGGER insert_trigger
           BEFORE INSERT ON test_trigger_src_tbl
           FOR EACH ROW
           EXECUTE PROCEDURE tri_insert_func(2.5f);
DROP TRIGGER insert_trigger ON test_trigger_src_tbl;
CREATE TRIGGER insert_trigger
           BEFORE INSERT ON test_trigger_src_tbl
           FOR EACH ROW
           EXECUTE PROCEDURE tri_insert_func(2.5d);
DROP TRIGGER insert_trigger ON test_trigger_src_tbl;
drop function tri_insert_func;
drop table test_trigger_src_tbl;
drop table test_trigger_des_tbl;

SELECT 1.5f = 1.5;
SELECT 1.5f <> 2.0;
SELECT 1.5f > 1.0;
SELECT 1.5f < 2.0;
SELECT (1.5f > 1.0) AND (2.5f < 3.0);
SELECT (1.5f > 1.0) OR (2.5f > 3.0);
SELECT 1.5f + 2.5;
SELECT 3.5f - 1.5;
SELECT 2.0f * 3.0;
SELECT 4.0f / 2.0;

set float_suffix_acceptance to on;


SELECT CONCAT('The value is ', TO_CHAR(BINARY_DOUBLE_INFINITY)) AS result;
SELECT CONCAT('The value is ', TO_CHAR(-BINARY_DOUBLE_INFINITY)) AS result;
SELECT CONCAT('The value is ', TO_CHAR(-BINARY_DOUBLE_NAN)) AS result;
set enable_binary_special_o_format to on;
SELECT CONCAT('The value is ', TO_CHAR(BINARY_DOUBLE_INFINITY)) AS result;
SELECT CONCAT('The value is ', TO_CHAR(-BINARY_DOUBLE_INFINITY)) AS result;
SELECT CONCAT('The value is ', TO_CHAR(-BINARY_DOUBLE_NAN)) AS result;

SELECT BINARY_DOUBLE_INFINITY;
SELECT BINARY_DOUBLE_NAN;

CREATE TABLE test_table (
    id INT,
    float_value binary_double
);
INSERT INTO test_table (id, float_value)
VALUES (1, BINARY_DOUBLE_NAN);
SELECT * FROM test_table WHERE float_value = BINARY_DOUBLE_NAN;

INSERT INTO test_table (id, float_value)
VALUES (2, BINARY_DOUBLE_INFINITY);

SELECT * FROM test_table WHERE float_value = BINARY_DOUBLE_INFINITY;
SELECT float_value + 1 FROM test_table WHERE id = 1;
SELECT float_value * 2 FROM test_table WHERE id = 2;
SELECT * FROM test_table WHERE float_value = BINARY_DOUBLE_NAN OR float_value = BINARY_DOUBLE_INFINITY;
DROP TABLE test_table;

SELECT ABS(binary_double_infinity);
SELECT CEIL(binary_double_infinity);
SELECT SQRT(binary_double_infinity);
SELECT FLOOR(binary_double_infinity);
SELECT SQRT(binary_double_infinity);
SELECT EXP(binary_double_infinity);
SELECT LOG(binary_double_infinity);
SELECT POWER(binary_double_infinity, 2);
SELECT SIN(binary_double_infinity);

SELECT ABS(binary_double_nan);
SELECT CEIL(binary_double_nan);
SELECT SQRT(binary_double_nan);
SELECT FLOOR(binary_double_nan);
SELECT SQRT(binary_double_nan);
SELECT EXP(binary_double_nan);
SELECT LOG(binary_double_nan);
SELECT POWER(binary_double_nan, 2);
SELECT SIN(binary_double_nan);

-- 创建自定义函数
CREATE FUNCTION test_function(value FLOAT)
  RETURNS FLOAT
  LANGUAGE plpgsql
AS $$
BEGIN
  IF value = binary_double_infinity THEN
    RETURN 1.0;
  ELSE
    RETURN 0.0;
  END IF;
END;
$$;

SELECT test_function(binary_double_nan);
DROP FUNCTION test_function;
--表达式
SELECT binary_double_infinity = binary_double_infinity; -- 返回 true
SELECT binary_double_nan = binary_double_nan; -- 返回 false
SELECT binary_double_nan = binary_double_infinity; -- 返回 false

SELECT binary_double_infinity <> 0.0; -- 返回 true
SELECT binary_double_nan <> binary_double_nan; -- 返回 true

SELECT binary_double_infinity > 0.0; -- 返回 true
SELECT binary_double_nan < binary_double_infinity; -- 返回 false

SELECT (binary_double_infinity > 0.0) AND (binary_double_nan < binary_double_infinity); -- 返回 false
SELECT (binary_double_infinity > 0.0) OR (binary_double_nan < binary_double_infinity); -- 返回 true

SELECT binary_double_infinity + 1.0; -- 返回正无穷大
SELECT binary_double_nan + 1.0; -- 返回 NaN

SELECT binary_double_infinity - binary_double_infinity; -- 返回 NaN
SELECT binary_double_nan - 1.0; -- 返回 NaN

SELECT binary_double_infinity * binary_double_infinity; -- 返回正无穷大
SELECT binary_double_nan * 1.0; -- 返回 NaN

SELECT binary_double_infinity / binary_double_infinity; -- 返回 NaN
SELECT binary_double_nan / 1.0; -- 返回 NaN

CREATE TABLE T1(binary_double_nan INT);
INSERT INTO T1 VALUES(1),(2),(3);
SELECT binary_double_nan;
SELECT binary_double_nan FROM T1;
SELECT T1.binary_double_nan FROM T1;
DROP TABLE T1;

--CHECK约束
create table t1(
    num binary_double
    CONSTRAINT check_num CHECK (num > 100)
);
insert into t1 values (binary_double_infinity);--插入成功
insert into t1 values (-binary_double_infinity);--插入失败
insert into t1 values (binary_double_nan);--插入成功
drop table t1;

--隐式转换
create table t1 (id int);
insert into t1 values(binary_double_infinity);--插入失败
insert into t1 values(-binary_double_infinity);--插入失败
insert into t1 values(binary_double_nan);--插入失败
drop table t1;

--聚集函数
create table t1(id binary_double);
insert into t1 values(99);
insert into t1 values(100);
insert into t1 values(binary_double_infinity);
insert into t1 values(binary_double_nan);
select avg(id) from t1;
select sum(id) from t1;
select max(id) from t1;
select min(id) from t1;
drop table t1;

--关键字测试
create table binary_double_infinity(col1 float4);
drop table binary_double_infinity;

create table t1(binary_double_nan float4);
insert into t1 values(3.14),(10),(15);
select binary_double_nan from t1;
select t1.binary_double_nan from t1;
drop table t1;

create table t1(binary_double_infinity float4 DEFAULT binary_double_infinity, id int);
insert into t1 (id) values(10);
insert into t1 values (100,9);

select * from t1;
select * from t1 where binary_double_infinity = 100;
select * from t1 where binary_double_infinity = binary_double_infinity;
select id from t1 group by id having binary_double_infinity > 100;

drop table t1;

select 3.14binary_double_nan;
select 3.14binary_double_infinity;

create user binary_double_infinity with password'gauss@123';
drop user binary_double_infinity;

create schema binary_double_infinity;
drop schema binary_double_infinity;

CREATE FUNCTION binary_double_infinity(a INTEGER, b INTEGER)
  RETURNS INTEGER
  LANGUAGE plpgsql
AS
$$
BEGIN
  RETURN a + b;
END;
$$;
drop function binary_double_infinity;

create type binary_double_infinity AS (
    id INT,
    name VARCHAR(50),
    age INT
);
drop type binary_double_infinity;

set disable_keyword_options = 'binary_double_nan';

select 3.14binary_double_nan;
select 3.14binary_double_infinity;

create user binary_double_nan with password'gauss@123';
drop user binary_double_nan;

create schema binary_double_nan;
drop schema binary_double_nan;

CREATE FUNCTION binary_double_nan(a INTEGER, b INTEGER)
  RETURNS INTEGER
  LANGUAGE plpgsql
AS
$$
BEGIN
  RETURN a + b;
END;
$$;
drop function binary_double_nan;

create type binary_double_nan AS (
    id INT,
    name VARCHAR(50),
    age INT
);
drop type binary_double_nan;
set disable_keyword_options = '';

set float_suffix_acceptance = on;
CREATE TABLE employees (
emp_id INT PRIMARY KEY,
emp_name VARCHAR(100) NOT NULL,
emp_salary DECIMAL(10, 2) DEFAULT 3.14f,
hire_date DATE DEFAULT CURRENT_DATE
);
INSERT INTO employees (emp_id, emp_name, hire_date) VALUES (1, 'John Doe', '2024-01-01');
SELECT * FROM employees;
DROP TABLE employees;

CREATE TABLE employees (
emp_id INT PRIMARY KEY,
emp_name VARCHAR(100) NOT NULL,
emp_salary DECIMAL(10, 2) DEFAULT 3.14d,
hire_date DATE DEFAULT CURRENT_DATE
);
INSERT INTO employees (emp_id, emp_name, hire_date) VALUES (1, 'John Doe', '2024-01-01');
SELECT * FROM employees;
DROP TABLE employees;

drop schema if exists test_binary cascade;
