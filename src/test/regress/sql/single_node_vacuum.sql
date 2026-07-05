--
-- VACUUM
--

CREATE TABLE vactst (i INT);
INSERT INTO vactst VALUES (1);
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
SELECT * FROM vactst;
VACUUM FULL vactst;
UPDATE vactst SET i = i + 1;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
VACUUM (FULL) vactst;
DELETE FROM vactst;
SELECT * FROM vactst;

VACUUM (FULL, FREEZE) vactst;
VACUUM (ANALYZE, FULL) vactst;

CREATE TABLE vaccluster (i INT PRIMARY KEY);
ALTER TABLE vaccluster CLUSTER ON vaccluster_pkey;
CLUSTER vaccluster;

CREATE FUNCTION do_analyze() RETURNS VOID VOLATILE LANGUAGE SQL
	AS 'ANALYZE pg_am';
CREATE FUNCTION wrap_do_analyze(c INT) RETURNS INT IMMUTABLE LANGUAGE SQL
	AS 'SELECT $1 FROM do_analyze()';
CREATE INDEX ON vaccluster(wrap_do_analyze(i));
INSERT INTO vaccluster VALUES (1), (2);
ANALYZE vaccluster;

set xc_maintenance_mode = on;
VACUUM FULL pg_am;
VACUUM FULL pg_class;
VACUUM FULL pg_database;
set xc_maintenance_mode = off;
VACUUM FULL vaccluster;
VACUUM FULL vactst;

-- check behavior with duplicate column mentions
VACUUM ANALYZE vaccluster(i,i);
ANALYZE vaccluster(i,i);

DROP TABLE vaccluster;
DROP TABLE vactst;

CREATE TABLE vacuum_truncate_test(i INT NOT NULL, j text)
    WITH (vacuum_truncate=false,
    toast.vacuum_truncate=false,
    autovacuum_enabled=false);
SELECT reloptions FROM pg_class WHERE oid = 'vacuum_truncate_test'::regclass;
INSERT INTO vacuum_truncate_test VALUES (1, NULL), (NULL, NULL);
VACUUM (FREEZE) vacuum_truncate_test;
SELECT pg_relation_size('vacuum_truncate_test') > 0;

SELECT reloptions FROM pg_class WHERE oid =
    (SELECT reltoastrelid FROM pg_class
    WHERE oid = 'vacuum_truncate_test'::regclass);

ALTER TABLE vacuum_truncate_test RESET (vacuum_truncate);
SELECT reloptions FROM pg_class WHERE oid = 'vacuum_truncate_test'::regclass;
INSERT INTO vacuum_truncate_test VALUES (1, NULL), (NULL, NULL);
VACUUM (FREEZE) vacuum_truncate_test;
SELECT pg_relation_size('vacuum_truncate_test') = 0;

INSERT INTO vacuum_truncate_test VALUES (1, NULL), (NULL, NULL);
SET vacuum_truncate = false;
VACUUM (FREEZE) vacuum_truncate_test;
SELECT pg_relation_size('vacuum_truncate_test') > 0;
RESET vacuum_truncate;
VACUUM (FREEZE) vacuum_truncate_test;
SELECT pg_relation_size('vacuum_truncate_test') = 0;

ALTER TABLE vacuum_truncate_test SET (vacuum_truncate=true);
INSERT INTO vacuum_truncate_test VALUES (1, NULL), (NULL, NULL);
SET vacuum_truncate = false;
VACUUM (FREEZE) vacuum_truncate_test;
SELECT pg_relation_size('vacuum_truncate_test') = 0;

CREATE TABLE vacuum_truncate_not_support_type(a int) WITH (vacuum_truncate=true, orientation=column);
CREATE TABLE vacuum_truncate_not_support_type(a int) WITH (vacuum_truncate=true, storage_type=ustore);
CREATE TABLE vacuum_truncate_not_support_type(a int) WITH (vacuum_truncate=true, segment=on);

CREATE TABLE vacuum_truncate_col(a int) WITH (orientation=column);
CREATE TABLE vacuum_truncate_ustore(a int) WITH (storage_type=ustore);
CREATE TABLE vacuum_truncate_seg(a int) WITH (segment=on);
ALTER TABLE vacuum_truncate_col SET (vacuum_truncate=true);
ALTER TABLE vacuum_truncate_ustore SET (vacuum_truncate=true);
ALTER TABLE vacuum_truncate_seg SET (vacuum_truncate=true);

drop table vacuum_truncate_col;
drop table vacuum_truncate_ustore;
drop table vacuum_truncate_seg;
drop table vacuum_truncate_test;
