-- ALTER COLUMN TYPE must preserve the selected clustering index.
\pset format unaligned
SET client_min_messages = warning;
CREATE TEMP TABLE alter_type_cluster (
    c0 integer PRIMARY KEY,
    c1 varchar(32),
    c2 integer UNIQUE
);
CREATE INDEX alter_type_cluster_i0 ON alter_type_cluster(c1);
CREATE INDEX alter_type_cluster_multi ON alter_type_cluster(c1, c2);
INSERT INTO alter_type_cluster VALUES (1, 'b', 3), (2, 'a', 2);

-- Preserve the ordinary index marker when its storage is reused.
CLUSTER alter_type_cluster USING alter_type_cluster_i0;
ALTER TABLE alter_type_cluster ALTER COLUMN c1 TYPE text;
SELECT 'ordinary' AS scenario, indisclustered,
       (SELECT count(*) FROM pg_index WHERE indrelid = 'alter_type_cluster'::regclass AND indisclustered) AS marked_count
FROM pg_index WHERE indexrelid = 'alter_type_cluster_i0'::regclass;
CLUSTER alter_type_cluster;

-- Preserve a unique constraint's index marker across a table rewrite.
CLUSTER alter_type_cluster USING alter_type_cluster_c2_key;
ALTER TABLE alter_type_cluster ALTER COLUMN c2 TYPE bigint;
SELECT 'unique' AS scenario, indisclustered,
       (SELECT count(*) FROM pg_index WHERE indrelid = 'alter_type_cluster'::regclass AND indisclustered) AS marked_count
FROM pg_index WHERE indexrelid = 'alter_type_cluster_c2_key'::regclass;
CLUSTER alter_type_cluster;

-- Preserve the primary key index marker as well.
CLUSTER alter_type_cluster USING alter_type_cluster_pkey;
ALTER TABLE alter_type_cluster ALTER COLUMN c0 TYPE bigint;
SELECT 'primary' AS scenario, indisclustered,
       (SELECT count(*) FROM pg_index WHERE indrelid = 'alter_type_cluster'::regclass AND indisclustered) AS marked_count
FROM pg_index WHERE indexrelid = 'alter_type_cluster_pkey'::regclass;

-- One index depending on multiple altered columns must be restored once.
CLUSTER alter_type_cluster USING alter_type_cluster_multi;
ALTER TABLE alter_type_cluster ALTER COLUMN c1 TYPE varchar(64), ALTER COLUMN c2 TYPE numeric;
SELECT 'multiple_columns' AS scenario, indisclustered,
       (SELECT count(*) FROM pg_index WHERE indrelid = 'alter_type_cluster'::regclass AND indisclustered) AS marked_count
FROM pg_index WHERE indexrelid = 'alter_type_cluster_multi'::regclass;

-- Constraint backing indexes must also preserve the marker when storage is reused.
CREATE TEMP TABLE alter_type_cluster_reuse (
    p varchar(32) PRIMARY KEY,
    u varchar(32) UNIQUE
);
INSERT INTO alter_type_cluster_reuse VALUES ('a', 'b'), ('c', 'd');
CREATE TEMP TABLE alter_type_cluster_nodes AS
SELECT c.oid AS old_oid, c.relname, c.relfilenode AS old_node
FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
WHERE i.indrelid = 'alter_type_cluster_reuse'::regclass;
ALTER TABLE alter_type_cluster_reuse CLUSTER ON alter_type_cluster_reuse_pkey;
ALTER TABLE alter_type_cluster_reuse ALTER COLUMN p TYPE text;
SELECT 'primary_reuse' AS scenario, i.indisclustered,
       c.oid <> n.old_oid AS replaced, c.relfilenode = n.old_node AS storage_reused,
       (SELECT count(*) FROM pg_index WHERE indrelid = 'alter_type_cluster_reuse'::regclass AND indisclustered) AS marked_count
FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
JOIN alter_type_cluster_nodes n ON n.relname = c.relname
WHERE c.oid = 'alter_type_cluster_reuse_pkey'::regclass;
ALTER TABLE alter_type_cluster_reuse CLUSTER ON alter_type_cluster_reuse_u_key;
ALTER TABLE alter_type_cluster_reuse ALTER COLUMN u TYPE text;
SELECT 'unique_reuse' AS scenario, i.indisclustered,
       c.oid <> n.old_oid AS replaced, c.relfilenode = n.old_node AS storage_reused,
       (SELECT count(*) FROM pg_index WHERE indrelid = 'alter_type_cluster_reuse'::regclass AND indisclustered) AS marked_count
FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
JOIN alter_type_cluster_nodes n ON n.relname = c.relname
WHERE c.oid = 'alter_type_cluster_reuse_u_key'::regclass;
DROP TABLE alter_type_cluster_nodes, alter_type_cluster_reuse;

-- Explicitly clearing the marker must override automatic restoration.
ALTER TABLE alter_type_cluster ALTER COLUMN c1 TYPE text, SET WITHOUT CLUSTER;
SELECT 'explicit_clear' AS scenario, count(*) AS marked_count
FROM pg_index WHERE indrelid = 'alter_type_cluster'::regclass AND indisclustered;
ALTER TABLE alter_type_cluster ALTER COLUMN c1 TYPE varchar(128);
SELECT 'unclustered' AS scenario, count(*) AS marked_count
FROM pg_index WHERE indrelid = 'alter_type_cluster'::regclass AND indisclustered;

-- An explicitly selected replacement must also override restoration.
ALTER TABLE alter_type_cluster CLUSTER ON alter_type_cluster_i0;
ALTER TABLE alter_type_cluster ALTER COLUMN c1 TYPE text, CLUSTER ON alter_type_cluster_multi;
SELECT 'explicit_change' AS scenario,
       (SELECT indisclustered FROM pg_index WHERE indexrelid = 'alter_type_cluster_i0'::regclass) AS old_index,
       (SELECT indisclustered FROM pg_index WHERE indexrelid = 'alter_type_cluster_multi'::regclass) AS new_index;
ALTER TABLE alter_type_cluster ALTER COLUMN c0 TYPE numeric;
SELECT 'unaffected_index' AS scenario, indisclustered,
       (SELECT count(*) FROM pg_index WHERE indrelid = 'alter_type_cluster'::regclass AND indisclustered) AS marked_count
FROM pg_index WHERE indexrelid = 'alter_type_cluster_multi'::regclass;
SELECT c0, c1, c2 FROM alter_type_cluster ORDER BY c0;
DROP TABLE alter_type_cluster;
-- Online ALTER TYPE releases table locks between catalog passes.
CREATE TABLE alter_type_cluster_online (c integer PRIMARY KEY);
INSERT INTO alter_type_cluster_online VALUES (1), (2);
ALTER TABLE alter_type_cluster_online CLUSTER ON alter_type_cluster_online_pkey;
ALTER TABLE CONCURRENTLY alter_type_cluster_online ALTER COLUMN c TYPE bigint;
SELECT 'online_primary' AS scenario, indisclustered,
       (SELECT count(*) FROM pg_index WHERE indrelid = 'alter_type_cluster_online'::regclass AND indisclustered) AS marked_count
FROM pg_index WHERE indexrelid = 'alter_type_cluster_online_pkey'::regclass;
SELECT c FROM alter_type_cluster_online ORDER BY c;
DROP TABLE alter_type_cluster_online;
RESET client_min_messages;
\pset format aligned
