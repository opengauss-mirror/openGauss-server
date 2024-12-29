-- 删除已有的表和触发器
DROP TABLE IF EXISTS employees;
DROP FUNCTION IF EXISTS log_salary_changes;

-- 创建员工表
CREATE TABLE employees (
    emp_id SERIAL PRIMARY KEY,
    emp_name VARCHAR(100),
    salary NUMERIC(10, 2)
);

-- 创建日志表
CREATE TABLE salary_changes (
    change_id SERIAL PRIMARY KEY,
    emp_id INT,
    old_salary NUMERIC(10, 2),
    new_salary NUMERIC(10, 2),
    change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建触发器函数
CREATE OR REPLACE FUNCTION log_salary_changes() RETURNS TRIGGER AS $$
BEGIN
    IF OLD.salary IS DISTINCT FROM NEW.salary THEN
        INSERT INTO salary_changes(emp_id, old_salary, new_salary)
        VALUES (OLD.emp_id, OLD.salary, NEW.salary);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 创建触发器
CREATE TRIGGER salary_update_trigger
AFTER UPDATE ON employees
FOR EACH ROW
EXECUTE FUNCTION log_salary_changes();

-- 插入测试数据
INSERT INTO employees (emp_name, salary) VALUES ('Alice', 50000), ('Bob', 60000);

-- 更新员工薪水，触发触发器
UPDATE employees SET salary = 55000 WHERE emp_name = 'Alice';

-- 查询日志表，验证触发器功能
SELECT emp_id, old_salary, new_salary FROM salary_changes ORDER BY change_id;
