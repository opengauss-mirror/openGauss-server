create database test_join dbcompatibility='D';
\c test_join
create extension shark;
--update and delete in join scenes
CREATE TABLE t_t_mutil_t1 (col1 INT, col2 INT);
CREATE TABLE t_t_mutil_t2 (col1 INT, col2 INT);
CREATE TABLE t_t_mutil_t3 (col1 INT, col2 INT);
INSERT INTO t_t_mutil_t1 VALUES (1, 1), (1, 1);
INSERT INTO t_t_mutil_t2 VALUES (1, 1), (1, 2);
INSERT INTO t_t_mutil_t3 VALUES (1, 1), (1, 3);
BEGIN TRANSACTION;
DELETE t_t_mutil_t1
FROM t_t_mutil_t1 a
JOIN t_t_mutil_t2 b ON a.col2 = b.col2
RIGHT JOIN t_t_mutil_t3 c ON b.col2 = c.col2;

DELETE t_t_mutil_t2
FROM t_t_mutil_t2 b
JOIN t_t_mutil_t1 a ON a.col2 = b.col2
LEFT JOIN t_t_mutil_t3 c ON b.col2 = c.col2;

DELETE t_t_mutil_t3
FROM t_t_mutil_t3 c
JOIN t_t_mutil_t1 a ON a.col2 = c.col2
INNER JOIN t_t_mutil_t2 b ON b.col2 = c.col2;
SELECT * FROM t_t_mutil_t1;
SELECT * FROM t_t_mutil_t2;
SELECT * FROM t_t_mutil_t3;

UPDATE t_t_mutil_t1 a
RIGHT JOIN t_t_mutil_t2 b ON a.col2 = b.col2
RIGHT JOIN t_t_mutil_t3 c ON b.col2 = c.col2
SET a.col2 = 0, b.col2 = 0;

UPDATE t_t_mutil_t2 b
LEFT JOIN t_t_mutil_t1 a ON a.col2 = b.col2
LEFT JOIN t_t_mutil_t3 c ON b.col2 = c.col2
SET a.col2 = 0, b.col2 = 0;

UPDATE t_t_mutil_t3 c
INNER JOIN t_t_mutil_t1 a ON a.col2 = c.col2
INNER JOIN t_t_mutil_t2 b ON b.col2 = c.col2
SET a.col2 = 0, b.col2 = 0, c.col2 = 0;
SELECT * FROM t_t_mutil_t1;
SELECT * FROM t_t_mutil_t2;
SELECT * FROM t_t_mutil_t3;
ROLLBACK TRANSACTION;
DROP TABLE IF EXISTS t_t_mutil_t1;
DROP TABLE IF EXISTS t_t_mutil_t2;
DROP TABLE IF EXISTS t_t_mutil_t3;

\c postgres
drop database test_join;