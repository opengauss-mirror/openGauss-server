set d_format_behavior_compat_options = 'enable_sbr_identifier';
--表名
CREATE TABLE [my_table] (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100)
);
ALTER TABLE [my_table]
ADD user_email VARCHAR(100);
DROP TABLE [my_table];
--保留关键字
CREATE TABLE [cast] (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100)
);
ALTER TABLE [cast]
ADD user_email VARCHAR(100);
DROP TABLE [cast];
--非保留关键字
CREATE TABLE [CATALOG] (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100)
);
ALTER TABLE [CATALOG]
ADD user_email VARCHAR(100);
DROP TABLE [CATALOG];


--函数名
CREATE OR REPLACE FUNCTION [calculate_age](birthdate DATE)
RETURNS INT AS $$
BEGIN
    RETURN EXTRACT(YEAR FROM AGE(birthdate));
END;
$$ LANGUAGE plpgsql;
DROP FUNCTION [calculate_age](DATE);
--保留关键字
CREATE OR REPLACE FUNCTION [cast](birthdate DATE)
RETURNS INT AS $$
BEGIN
    RETURN EXTRACT(YEAR FROM AGE(birthdate));
END;
$$ LANGUAGE plpgsql;
DROP FUNCTION [cast](DATE);
--非保留关键字
CREATE OR REPLACE FUNCTION [CATALOG](birthdate DATE)
RETURNS INT AS $$
BEGIN
    RETURN EXTRACT(YEAR FROM AGE(birthdate));
END;
$$ LANGUAGE plpgsql;
DROP FUNCTION [CATALOG](DATE);

--列名
CREATE TABLE my_table (
    user_id INT PRIMARY KEY,
    [user_name] VARCHAR(100)
);
ALTER TABLE my_table
ADD user_email VARCHAR(100);
DROP TABLE my_table;

CREATE TABLE my_table (
    user_id INT PRIMARY KEY,
    [cast] VARCHAR(100)
);
ALTER TABLE my_table
ADD [CATALOG] VARCHAR(100);
DROP TABLE my_table;

--变量
DO $$
DECLARE
    [my_variable] INT := 10;
    [cast] TEXT := 'Hello, PostgreSQL!';
    [CATALOG] DATE := '2024-01-01';
BEGIN
    RAISE NOTICE 'Variable 1: %', [my_variable];
    RAISE NOTICE 'Variable 2: %', [cast];
    RAISE NOTICE 'Variable 3: %', [CATALOG];
END $$;

--索引
CREATE TABLE Users (
    UserID INT PRIMARY KEY,
    UserName NVARCHAR(50),
    Email NVARCHAR(100)
);

CREATE CLUSTERED INDEX [UserID]
ON Users (UserID);
CREATE CLUSTERED INDEX [cast]
ON Users (UserID);
CREATE CLUSTERED INDEX [CATALOG]
ON Users (UserID);
DROP INDEX [UserID];
DROP INDEX [cast];
DROP INDEX [CATALOG];

--视图
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(50)
);
CREATE VIEW [sales_employees] AS
SELECT employee_id, name, department
FROM employees
WHERE department = 'Sales';
CREATE VIEW [cast] AS
SELECT employee_id, name, department
FROM employees
WHERE department = 'Sales';
CREATE VIEW [CATALOG] AS
SELECT employee_id, name, department
FROM employees
WHERE department = 'Sales';

-- 存储过程
DROP TABLE IF EXISTS Employees;

CREATE TABLE IF NOT EXISTS Employees(
    EmployeeID SERIAL PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50)
);
CREATE OR REPLACE FUNCTION [GetEmployeeDetails](IN employee_id INT)
RETURNS TABLE (EmployeeID INT, FirstName VARCHAR(50), LastName VARCHAR(50)) AS $$
BEGIN
    RETURN QUERY
    SELECT EmployeeID, FirstName, LastName
    FROM Employees
    WHERE EmployeeID = employee_id;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION [GetEmployeeDetails];
-- 创建触发器函数
CREATE OR REPLACE FUNCTION [trg_AfterInsert_func]()
RETURNS TRIGGER AS $$
BEGIN
    RAISE NOTICE 'New employee added';
    RETURN NEW;  -- 返回 NEW 表示允许插入操作
END;
$$ LANGUAGE plpgsql;

-- 创建触发器
CREATE TRIGGER [trg_AfterInsert]
AFTER INSERT ON Employees
FOR EACH ROW
EXECUTE PROCEDURE [trg_AfterInsert_func]();
DROP TRIGGER IF EXISTS [trg_AfterInsert] ON Employees;
-- 类型
CREATE DOMAIN PhoneNumber AS VARCHAR(15);
DROP TYPE [PhoneNumber];
DROP TABLE Employees;

--database,schema,大小写
CREATE DATABASE [TEST_DB];
DROP DATABASE TEST_DB;
CREATE DATABASE [TEST_DB];
DROP DATABASE [test_db];
CREATE DATABASE [TEST_DB];
DROP DATABASE [TEST_DB];
CREATE schema [TEST_S];
DROP schema TEST_S;
CREATE schema [TEST_S];
DROP schema [test_s];
CREATE schema [TEST_S];
DROP schema [TEST_S];

-- 特殊符号
CREATE TABLE [!@#] (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100)
);
DROP TABLE [!@#];

CREATE TABLE [ ] (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100)
);
DROP TABLE [ ];

CREATE TABLE [[] (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100)
);
DROP TABLE [[];

CREATE TABLE [你好] (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100)
);
DROP TABLE [你好];

CREATE TABLE ['] (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100)
);
DROP TABLE ['];

CREATE TABLE ["] (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100)
);
DROP TABLE ["];

CREATE TABLE []] (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100)
);
DROP TABLE []]; --error

--原有数组相关语法不支持
SELECT '{1, 2, 3}'::int[3] AS numbers;
SELECT ARRAY[1, 2, 3] AS numbers;

CREATE TABLE test_array (
    id SERIAL PRIMARY KEY,
    numbers INT[3]
);

DO $$
DECLARE
    numbers INT[3] := ARRAY[1, 2, 3];
BEGIN
    RAISE NOTICE 'Array: %', numbers;
END $$;
