CREATE FOREIGN TABLE mm_table (k integer);
BEGIN;
INSERT INTO mm_table VALUES (1), (2);
SAVEPOINT p1;
INSERT INTO mm_table VALUES (3), (4);
ROLLBACK TO SAVEPOINT p1;
COMMIT;
-- Transaction should abort due to error, so now tables are empty:
SELECT * FROM mm_table;
DROP FOREIGN TABLE mm_table;
