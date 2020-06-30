CREATE FOREIGN TABLE products (id SERIAL, price float);

INSERT INTO products (price) VALUES (3.141);
INSERT INTO products (price) VALUES (2.718);
INSERT INTO products (price) VALUES (2.236);
INSERT INTO products (price) VALUES (2.828);

CREATE VIEW products_above_average_price AS
SELECT id, price FROM products WHERE price > (SELECT AVG(price) FROM products);

SELECT * FROM products_above_average_price;

INSERT INTO products (price) VALUES (-3.412);
INSERT INTO products (price) VALUES (3.236);

SELECT * FROM products_above_average_price;

DROP VIEW products_above_average_price;
DROP FOREIGN TABLE products;
