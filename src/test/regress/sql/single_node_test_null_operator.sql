-- A format database not support <=>
SELECT 1 <=> 1;
SELECT '' <=> NULL;
SELECT NULL <=> NULL;
SELECT (1,2) <=> (1,2);

create database test dbcompatibility = 'B';
\c test

-- expression comparison
SELECT 1 <=> 1;
SELECT 1 <=> 2;
SELECT 1<=>-1;
SELECT -1<=>-1;
SELECT 1<=>(-1);
SELECT '1' <=> '2';
SELECT 1 <=> NULL;
SELECT '' <=> NULL;
SELECT '' <=> '';
SELECT NULL <=> NULL;

-- row expression comparison
SELECT (1,2) <=> (1,2);
SELECT (1,2,3) <=> (1,2,3);
SELECT (NULL,2,3) <=> (NULL,2,3);
SELECT (NULL,2,3) <=> (1,2,3);
SELECT ('',2,3) <=> (NULL,2,3);
SELECT ('',2,3) <=> ('',2,3);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1(a int, b text, c int);
insert into t1 values(1, 'a', 10);
insert into t1 values(null, 'b', 20);
insert into t1 values(3, null, 30);
insert into t1 values(4, 'd', null);
insert into t1 values(5, '', 50);

CREATE TABLE t2(a int, b text, c int);
insert into t2 values(1, 'a', 1);
insert into t2 values(null, 'b', 2);
insert into t2 values(3, null, 3);
insert into t2 values(4, 'd', null);
insert into t2 values(5, null, 5);

SELECT * FROM t1,t2 WHERE t1.a <=> t2.a;
SELECT * FROM t1,t2 WHERE t1.b <=> t2.b;

SELECT * FROM t1,t2 WHERE (t1.a,t1.b) <=> (t2.a,t2.b);
SELECT * FROM t1,t2 WHERE (t1.a,t1.c) <=> (t2.a,t2.c);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

\c regression
DROP DATABASE IF EXISTS test;
