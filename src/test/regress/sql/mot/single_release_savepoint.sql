CREATE FOREIGN TABLE mm_table (k integer);
BEGIN;
INSERT INTO mm_table VALUES (1), (2);
SAVEPOINT p1;
INSERT INTO mm_table VALUES (3), (4);
COMMIT;

-- Transaction should abort, so now tables are empty:
SELECT * FROM mm_table;

DROP TABLE pg_table;
CREATE TABLE pg_table (i int);
INSERT INTO pg_table VALUES (generate_series(1,4));

BEGIN;
INSERT INTO mm_table VALUES (1), (2);
SAVEPOINT p1;
INSERT INTO pg_table VALUES (5);
SAVEPOINT p2;
INSERT INTO mm_table VALUES (3);
rollback to savepoint p2;
COMMIT;

SELECT * FROM mm_table order by k;
SELECT * FROM pg_table order by i;

DROP FOREIGN TABLE mm_table;
DROP TABLE pg_table;
