set enable_query_parameterization=on;

CREATE TABLE customer_t1
(
c_customer_sk integer,
c_customer_id char(5),
c_first_name char(6),
c_last_name char(8),
Amount integer
);
INSERT INTO customer_t1(c_customer_sk, c_customer_id, c_first_name,Amount) VALUES (3769, 'hello', 'Grace', 1000);
INSERT INTO customer_t1(c_customer_sk, c_customer_id, c_first_name,Amount) VALUES (3869, 'hello', 'Grace', 1000);
DELETE FROM customer_t1 WHERE c_customer_sk = 3869;
drop table customer_t1;

CREATE TABLE customer_t1
(
c_customer_sk integer,
c_customer_id char(5),
c_first_name char(6),
c_last_name char(8),
Amount integer
);
INSERT INTO customer_t1(c_customer_sk, c_customer_id, c_first_name,Amount) VALUES (3769, 'hello', 'Grace', 1000);
INSERT INTO customer_t1(c_customer_sk, c_customer_id, c_first_name,Amount) VALUES (3869, 'hello', 'Grace', 1000);
DELETE FROM customer_t1 WHERE c_customer_sk = 3869;
drop table customer_t1;

set enable_query_parameterization=off;