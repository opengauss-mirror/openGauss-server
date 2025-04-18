SELECT * FROM query_parameterization_views();
set enable_query_parameterization=on;

CREATE TABLE test(c1 INT, c2 INT);
INSERT INTO test(C1, C2) VALUES(1, 1);
-- count should be 1
SELECT COUNT(*) FROM query_parameterization_views();

DROP TABLE test;
CREATE TABLE test(c1 INT, c2 INT);
-- count should be 0
SELECT COUNT(*) FROM query_parameterization_views();

INSERT INTO test(C1, C2) VALUES(2, 3);
INSERT INTO test(C1, C2) VALUES(3, 4);
-- only 1 insert record
SELECT query_type, is_bypass, param_types, param_nums, parameterized_query FROM query_parameterization_views();

set enable_query_parameterization=off;
-- count should be 1
SELECT COUNT(*) FROM query_parameterization_views();

set enable_query_parameterization=on;
UPDATE test SET C1 = 100 WHERE C1 = 3;
SET max_parameterized_query_stored=1;
-- 2 records, insert & update
SELECT query_type, is_bypass, param_types, param_nums, parameterized_query FROM query_parameterization_views();

DELETE FROM test where C1 = 3;
-- only 1 delete record
SELECT query_type, is_bypass, param_types, param_nums, parameterized_query FROM query_parameterization_views();
SET max_parameterized_query_stored=1;


ALTER TABLE test DROP COLUMN C2;
-- no record left 
SELECT query_type, is_bypass, param_types, param_nums, parameterized_query FROM query_parameterization_views();

DROP TABLE test;

CREATE TABLE test1(id int, column1 int, column2 int, column3 int, column4 int, column5 int, column6 int, column7 int, column8 int, column9 int, column10 int);
INSERT INTO test1(id, column1, column2, column3, column4, column5, column6, column7, column8, column9, column10) VALUES(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
SELECT query_type, is_bypass, param_types, param_nums, parameterized_query FROM query_parameterization_views();
DROP TABLE test1;

set enable_query_parameterization=off;