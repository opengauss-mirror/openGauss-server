\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS MyCMK CASCADE;

CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE customer (
    customer_id integer NOT NULL,
    id integer  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
    first_name character varying(45) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
    last_name character varying(45) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
    alias_name character (50) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) DEFAULT 'ali'
);
INSERT INTO customer VALUES(1,1,'Jared','Ely');
INSERT INTO customer VALUES(1,2,'Mary','Smith');
INSERT INTO customer VALUES(1,3,'Patricia','Johnson');
INSERT INTO customer VALUES(1,4,'Linda','Williams');
INSERT INTO customer VALUES(1,5,'Barbara','Jones');
INSERT INTO customer VALUES(1,6,'Elizabeth','Brown');
INSERT INTO customer VALUES(1,7,'Jennifer','Davis');
INSERT INTO customer VALUES(1,8,'Maria','Miller');
INSERT INTO customer VALUES(1,9,'Susan','Wilson');
INSERT INTO customer VALUES(1,10,'Margaret','Moore');
INSERT INTO customer VALUES(1,11,'Dorothy','Taylor');
INSERT INTO customer VALUES(1,12,'Lisa','Anderson');
INSERT INTO customer VALUES(1,13,'Nancy','Thomas');
INSERT INTO customer VALUES(1,14,'Karen','Jackson');
INSERT INTO customer VALUES(1,15,'Betty','White');
INSERT INTO customer VALUES(1,16,'Helen','Harris');
INSERT INTO customer VALUES(1,17,'Sandra','Martin');
INSERT INTO customer VALUES(1,18,'Adam','Rodriguez');
INSERT INTO customer VALUES(1,19,'Carol','Garcia');
INSERT INTO customer VALUES(1,20,'Jamie','Rice');
INSERT INTO customer VALUES(1,21,'Annette','Olson');
INSERT INTO customer VALUES(1,22,'Annie','Russell');

select * from customer where customer_id = 1 AND id = 1;
select * from customer where customer_id = 2 OR first_name = 'Jamie';
select * from customer where id = 1;
select * from customer where id <> 1;

SELECT last_name, first_name FROM customer WHERE first_name = 'Jamie';
SELECT last_name, first_name FROM customer WHERE first_name <> 'Jamie';
SELECT last_name, first_name FROM customer WHERE first_name = 'Jamie' AND last_name = 'Rice';
SELECT first_name, last_name FROM customer WHERE last_name = 'Rodriguez' OR  first_name = 'Adam';
SELECT
   first_name,
   last_name
FROM
   customer
WHERE 
   first_name IN ('Ann','Anne','Annie');
SELECT
   first_name,
   last_name
FROM
   customer
WHERE 
   first_name LIKE 'Ann%';
SELECT
   first_name,
   LENGTH(first_name) name_length
FROM
   customer
WHERE 
   first_name LIKE 'A%' AND
   LENGTH(first_name) BETWEEN 3 AND 5
ORDER BY
   name_length;
SELECT 
   first_name, 
   last_name
FROM 
   customer 
WHERE 
   first_name LIKE 'Bra%' AND 
   last_name <> 'Motley';
SELECT
   first_name,
   last_name
FROM
   customer
WHERE
   customer.first_name = 'Jamie';
SELECT * from customer where id > 1; 
SELECT * from customer where id < 1; 
SELECT * from customer where id != 1; 
DROP TABLE customer;
DROP COLUMN ENCRYPTION KEY MyCEK;
DROP CLIENT MASTER KEY MyCMK;

\! gs_ktool -d all