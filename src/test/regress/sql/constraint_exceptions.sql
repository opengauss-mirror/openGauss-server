-- 删除已有的表
DROP TABLE IF EXISTS products;

-- 创建产品表，设置唯一约束和检查约束
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) UNIQUE,
    price NUMERIC(10, 2) CHECK (price > 0)
);

-- 插入测试数据
INSERT INTO products (product_name, price) VALUES ('Laptop', 1000.00);

-- 尝试插入重复的产品名，期望违反唯一约束
INSERT INTO products (product_name, price) VALUES ('Laptop', 1200.00);

-- 尝试插入价格为负数的产品，期望违反检查约束
INSERT INTO products (product_name, price) VALUES ('Smartphone', -500.00);

-- 查询产品表，验证数据插入情况
SELECT product_name, price FROM products;
