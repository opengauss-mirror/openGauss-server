-- create type: ok
CREATE TYPE person_type AS (age int, name text);
--  create table: fail
CREATE TABLE persons OF person_type
PARTITION BY RANGE (AGE)
(
	PARTITION persons_p1 VALUES LESS THAN (10),
	PARTITION persons_p2 VALUES LESS THAN (20),
	PARTITION persons_p3 VALUES LESS THAN (30)
);
-- create table: ok
CREATE TABLE persons_test OF person_type;
-- create table 
CREATE TABLE persons (age int, name text)
PARTITION BY RANGE (age)
(
	PARTITION persons_p1 VALUES LESS THAN (10),
	PARTITION persons_p2 VALUES LESS THAN (20),
	PARTITION persons_p3 VALUES LESS THAN (30)
);
alter table persons of person_type;
alter table persons not of;
DROP TYPE person_type CASCADE;
DROP TABLE persons;
