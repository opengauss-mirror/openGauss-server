CREATE FOREIGN TABLE products (id serial, price float);

INSERT INTO products (price) VALUES (80.0);
INSERT INTO products (price) VALUES (120.0);
INSERT INTO products (price) VALUES (99.9);
INSERT INTO products (price) VALUES (101.9) 
    RETURNING id, price as inserted_price;

UPDATE products SET price=price*1.10
   WHERE price <= 99.9
   RETURNING id, price AS updated_price;

DELETE FROM products
   WHERE price>=110
   RETURNING id, price AS deleted_price;

DROP FOREIGN TABLE products;
