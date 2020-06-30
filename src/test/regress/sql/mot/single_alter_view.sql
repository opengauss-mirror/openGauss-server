CREATE FOREIGN TABLE products (id SERIAL, price float);

INSERT INTO products (price) VALUES (3.141);
INSERT INTO products (price) VALUES (2.718);
INSERT INTO products (price) VALUES (2.236);
INSERT INTO products (price) VALUES (2.828);

CREATE VIEW products_under_average_price AS
SELECT id, price FROM products WHERE price > (SELECT AVG(price) FROM products);

INSERT INTO products (price) VALUES (-3.412);

SELECT * FROM products_under_average_price;

-- Things that can be done with ALTER VIEW:
-- Rename:
ALTER VIEW products_under_average_price RENAME TO products_above_average_price;
-- ALTER [COLUMN] SET/DROP DEFAULT TODO
-- OWNER TO TODO
-- SET SCHEMA TODO
-- SET (view_option_name) TODO
-- RESET (view option name) TODO

INSERT INTO products (price) VALUES (3.236);

SELECT * FROM products_above_average_price;

DROP VIEW products_above_average_price;
DROP FOREIGN TABLE products;
