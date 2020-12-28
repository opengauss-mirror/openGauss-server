\! gs_ktool -d all
\! gs_ktool -g

--
-- JOIN
-- Test JOIN clauses
--
CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE COLUMN ENCRYPTION KEY MyCEK1 WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE basket_a (
    id INT PRIMARY KEY,
    fruit_a VARCHAR (100) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
    fruit VARCHAR (100) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) 
);
 
CREATE TABLE basket_b (
    id INT PRIMARY KEY,
    fruit_b VARCHAR (100) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
    fruit VARCHAR (100) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC)   
);
 
CREATE TABLE basket_c (
    id INT PRIMARY KEY,
    fruit_c VARCHAR (100) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC),
    fruit VARCHAR (100) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) 
);

CREATE TABLE basket_d (
    id INT PRIMARY KEY,
    fruit_d VARCHAR (100) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = RANDOMIZED),
    fruit VARCHAR (100) NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) 
);
INSERT INTO basket_a (id, fruit_a,fruit)
VALUES
    (1, 'Orange','Orange'),
    (2, 'Apple','Apple'),
    (3, 'Banana','Banana'),
    (4, 'Cucumber','Cucumber');
 
INSERT INTO basket_b (id, fruit_b, fruit)
VALUES
    (1, 'Orange','Orange'),
    (2, 'Apple','Apple'),
    (3, 'peach','peach'),
    (4, 'Pear','Pear');
INSERT INTO basket_c (id, fruit_c,fruit)
VALUES
    (1, 'Orange','Orange'),
    (2, 'Apple','Apple'),
    (3, 'Watermelon','Watermelon'),
    (4, 'Pear','Pear');
INSERT INTO basket_d (id, fruit_d,fruit)
VALUES
    (1, 'Orange','Orange'),
    (2, 'banana','banana'),
    (3, 'Watermelon','Watermelon'),
    (4, 'Pear','Pear');

select * from basket_a where id =1 order by id;
select * from basket_a where id <>1 order by id;
select * from basket_a where id >1 order by id;
select * from basket_a where id <4 order by id;

SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    b.id id_b,
    b.fruit_b fruit_b
FROM
    basket_a a
CROSS JOIN basket_b b
order by id_a, id_b;


SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    b.id id_b,
    b.fruit_b fruit_b
FROM
    basket_a a
INNER JOIN basket_b b ON (a.fruit_a = b.fruit_b)
order by id_a, id_b;

SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    b.id id_b,
    b.fruit_b fruit_b
FROM
    basket_a a
INNER JOIN basket_b b ON (a.fruit_a = b.fruit_b and a.fruit_a = 'Apple')
order by id_a, id_b;

SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    b.id id_b,
    b.fruit_b fruit_b
FROM
    basket_a a
INNER JOIN basket_b b on (a.id=b.id)
where a.fruit_a = b.fruit_b and a.fruit_a = 'Apple'
order by id_a, id_b;

SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    b.id id_b,
    b.fruit_b fruit_b
FROM
    basket_a a 
NATURAL JOIN basket_b b
order by id_a, id_b;

SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    b.id id_b,
    b.fruit_b fruit_b
FROM
    basket_a a (id,fruit_a)
NATURAL JOIN basket_b b(id,fruit_b)
order by id_a, id_b;

SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    b.id id_b,
    b.fruit_b fruit_b
FROM
    basket_a a
LEFT JOIN basket_b b ON a.fruit_a = b.fruit_b
order by id_a, id_b;
SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    b.id id_b,
    b.fruit_b fruit_b
FROM
    basket_a a
RIGHT JOIN basket_b b ON a.fruit_a = b.fruit_b
order by id_a,id_b;

SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    b.id id_b,
    b.fruit_b fruit_b
FROM
    basket_a a
FULL OUTER JOIN basket_b b ON a.fruit_a = b.fruit_b
order by id_a, id_b;

SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    b.id id_b,
    b.fruit_b fruit_b
FROM
    basket_a a
FULL JOIN basket_b b using (fruit)
   WHERE a.id IS NULL OR b.id IS NULL
order by id_a, id_b;

SELECT * FROM basket_a INNER JOIN basket_b USING (fruit) ORDER BY basket_a.id;


SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    c.id id_c,
    c.fruit_c fruit_c
FROM
    basket_a a
FULL JOIN basket_c c ON a.fruit_a = c.fruit_c
ORDER BY id_a, id_c;


SELECT
    a.id id_a,
    a.fruit fruit_a,
    b.id id_b,
    b.fruit fruit_b
FROM
    basket_a a
FULL JOIN basket_b b ON a.fruit_a = b.fruit_b
where a.fruit='Apple' and b.fruit = 'Apple'
ORDER BY id_a, id_b;

SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    d.id id_d,
    d.fruit_d fruit_d
FROM
    basket_a a
FULL JOIN basket_d d ON a.fruit_a = d.fruit_d
ORDER BY id_a, id_d;

SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    b.id id_b,
    b.fruit_b fruit_b
FROM
    basket_a a
LEFT OUTER JOIN basket_b b USING (fruit)
WHERE b.id IS NULL
order by id_a, id_b;

SELECT
    a.id id_a,
    a.fruit_a fruit_a,
    c.id id_c,
    c.fruit_c fruit_c
FROM
    basket_a a
LEFT JOIN basket_c c USING (fruit)
order by id_a, id_c;

CREATE TABLE join_t1 (
    id INT PRIMARY KEY,
    customer_id1 smallint NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC),
    age1 TINYINT NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC)
);

CREATE TABLE join_t2 (
    id INT PRIMARY KEY,
    customer_id2 int NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC),
    age2 TINYINT NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC)
);

CREATE TABLE join_t3 (
    id INT PRIMARY KEY,
    customer_id3 int NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = DETERMINISTIC),
    age3 TINYINT NOT NULL ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK1, ENCRYPTION_TYPE = RANDOMIZED)
);

INSERT INTO join_t1 VALUES (1, 1, 15);
INSERT INTO join_t1 VALUES (2, 1, 20);
INSERT INTO join_t2 VALUES (1, 1, 30);
INSERT INTO join_t3 VALUES (1, 1, 20);
INSERT INTO join_t1 VALUES (3, 2, 25);
INSERT INTO join_t2 VALUES (2, 2, 25);

SELECT * FROM join_t1 JOIN join_t2 ON (join_t1.customer_id1=join_t2.customer_id2) JOIN join_t3 ON (join_t2.customer_id2=join_t3.customer_id3 AND join_t1.customer_id1=join_t3.customer_id3);

SELECT * FROM join_t1 JOIN join_t2 ON (join_t1.age1=join_t2.age2);


SELECT * 
    FROM join_t1 JOIN join_t2 on (join_t1.customer_id1 <= join_t2.customer_id2)
    ORDER BY join_t1.id, join_t2.id;


SELECT * 
    FROM join_t1 JOIN join_t2 on (join_t1.age1 = join_t2.age2)
    ORDER BY join_t1.id, join_t2.id;


-- SELECT * FROM
--     (SELECT * FROM join_t1) as s1
--     INNER JOIN
--     (SELECT * FROM join_t1) s2
--     USING (customer_id)
--     ORDER BY s1.id, s2.id;


SELECT * FROM
    (SELECT id, customer_id1 as s1_customer_id FROM join_t1) as s1
    NATURAL INNER JOIN
    (SELECT id, customer_id2 as s2_customer_id FROM join_t2) as s2
    ORDER BY  s1.id, s2.id;

select * from join_t1 left join join_t2 on (join_t1.customer_id1=join_t2.customer_id2 and  join_t1.customer_id1 is not null) ORDER BY join_t1.id, join_t2.id;

select * from join_t1, join_t2, join_t3 where join_t1.customer_id1 = join_t2.customer_id2 and join_t2.age2 = 30;


select * from join_t1 t1 left join
  (join_t2 t2 join join_t3 t3 on t2.customer_id2 = t3.customer_id3)
  on t1.customer_id1 = t2.customer_id2 and t1.customer_id1 = t3.customer_id3
where t1.age1 = 15;

select * from join_t1 t1 left join
  (join_t2 t2 join join_t3 t3 on t2.customer_id2 = t3.customer_id3)
  on t1.customer_id1 = t2.customer_id2 and t1.customer_id1 = t3.customer_id3
where t2.customer_id2 = 1;

DROP TABLE join_t1;
DROP TABLE join_t2;
DROP TABLE join_t3;

DROP TABLE basket_a;
DROP TABLE basket_b;
DROP TABLE basket_c;
DROP TABLE basket_d;
DROP CLIENT MASTER KEY MyCMK CASCADE;

\! gs_ktool -d all