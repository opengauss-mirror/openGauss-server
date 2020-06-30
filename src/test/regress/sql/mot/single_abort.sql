CREATE FOREIGN TABLE mm_table (k integer);

BEGIN;

INSERT INTO mm_table VALUES (42);

SELECT * from mm_table WHERE k = 42;

ABORT;

SELECT * from mm_table WHERE k = 42;

DROP FOREIGN TABLE mm_table;
