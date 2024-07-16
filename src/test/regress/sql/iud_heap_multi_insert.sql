DROP TABLE IF EXISTS prep202407_hmi;
DROP TABLE IF EXISTS dest202407_hmi;

CREATE TABLE prep202407_hmi (i int);
CREATE TABLE dest202407_hmi (i int);

SHOW enable_heap_multi_insert_for_insert_select;
INSERT INTO prep202407_hmi SELECT generate_series(1,10000);
INSERT INTO dest202407_hmi SELECT * FROM prep202407_hmi;
SELECT count(*) FROM dest202407_hmi;

DROP TABLE IF EXISTS prep202407_hmi;
DROP TABLE IF EXISTS dest202407_hmi;
