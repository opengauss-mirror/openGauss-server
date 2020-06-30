CREATE FOREIGN TABLE products (id SERIAL PRIMARY KEY, price FLOAT);

INSERT INTO products (price) VALUES (80.0);
INSERT INTO products (price) VALUES (120.0);
INSERT INTO products (price) VALUES (99.9);
INSERT INTO products (price) VALUES (101.9);

UPDATE products SET price=price*1.10
   WHERE price <= 99.9;

SELECT * FROM products ORDER BY id;

-- The next command should fail as the primary key is immutable.
UPDATE products SET id=42 where price=120.0;

SELECT * FROM products ORDER BY id;

DROP FOREIGN TABLE products;
