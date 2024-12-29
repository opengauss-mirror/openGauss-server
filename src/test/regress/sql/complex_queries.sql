-- 删除已有的表
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

-- 创建客户表
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100)
);

-- 创建订单表
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    order_amount NUMERIC(10, 2)
);

-- 插入测试数据
INSERT INTO customers (customer_name) VALUES ('Alice'), ('Bob'), ('Charlie');
INSERT INTO orders (customer_id, order_amount) VALUES (1, 100.00), (1, 150.00), (2, 200.00);

-- 查询每个客户的订单总金额，使用子查询
SELECT customer_name, (
    SELECT SUM(order_amount)
    FROM orders
    WHERE orders.customer_id = customers.customer_id
) AS total_spent
FROM customers;

-- 查询订单金额大于100的客户信息，使用联合查询
SELECT customers.customer_name, orders.order_amount
FROM customers
JOIN orders ON customers.customer_id = orders.customer_id
WHERE orders.order_amount > 100;
