-- BM25 LOCAL partition index: build, partition-pruned scan, DML and rebuild.
SET client_min_messages = error;

DROP TABLE IF EXISTS bm25_partition_local CASCADE;

CREATE TABLE bm25_partition_local (
    id int,
    part_key int,
    content text
)
PARTITION BY RANGE (part_key)
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);

INSERT INTO bm25_partition_local VALUES
    (1, 1, 'apple apple'),
    (2, 2, 'apple banana'),
    (3, 3, 'banana'),
    (11, 101, 'banana banana'),
    (12, 102, 'apple');

CREATE INDEX bm25_partition_local_idx ON bm25_partition_local USING bm25(content) LOCAL
(
    PARTITION bm25_partition_local_p1_idx,
    PARTITION bm25_partition_local_p2_idx
);

CREATE INDEX bm25_partition_global_idx ON bm25_partition_local USING bm25(content) GLOBAL;

\pset format unaligned
\pset tuples_only on

SELECT count(*)
FROM pg_partition
WHERE parentid = 'bm25_partition_local_idx'::regclass
  AND indisusable;

SELECT pg_catalog.bm25_table_stat('^bm25_partition_local$', 'apple', 'content');

SET enable_seqscan = off;
SET enable_indexscan = on;

COPY (
    SELECT id
    FROM bm25_partition_local
    WHERE part_key < 100
    ORDER BY content <&> 'apple' DESC
    LIMIT 2
) TO STDOUT;

COPY (
    SELECT id
    FROM bm25_partition_local
    WHERE part_key >= 100 AND part_key < 200
    ORDER BY content <&> 'banana' DESC
    LIMIT 1
) TO STDOUT;

COPY (
    SELECT id
    FROM (
        SELECT id
        FROM bm25_partition_local
        ORDER BY content <&> 'apple' DESC
        LIMIT 3
    ) q
    ORDER BY id
) TO STDOUT;

ALTER TABLE bm25_partition_local ADD PARTITION p3 VALUES LESS THAN (300);
INSERT INTO bm25_partition_local VALUES (21, 201, 'grape grape');

SELECT pg_catalog.bm25_table_stat('^bm25_partition_local$', 'apple', 'content');

COPY (
    SELECT id
    FROM bm25_partition_local
    WHERE part_key >= 200 AND part_key < 300
    ORDER BY content <&> 'grape' DESC
    LIMIT 1
) TO STDOUT;

UPDATE bm25_partition_local SET content = 'apple apple apple' WHERE id = 3;
DELETE FROM bm25_partition_local WHERE id = 1;
VACUUM bm25_partition_local;

COPY (
    SELECT id
    FROM bm25_partition_local
    WHERE part_key < 100
    ORDER BY content <&> 'apple' DESC
    LIMIT 1
) TO STDOUT;

ALTER TABLE bm25_partition_local MODIFY PARTITION p1 UNUSABLE LOCAL INDEXES;
SELECT count(*)
FROM pg_partition
WHERE parentid = 'bm25_partition_local_idx'::regclass
  AND indisusable;
SELECT pg_catalog.bm25_table_stat('^bm25_partition_local$', 'apple', 'content');

ALTER TABLE bm25_partition_local MODIFY PARTITION p1 REBUILD UNUSABLE LOCAL INDEXES;
REINDEX INDEX bm25_partition_local_idx PARTITION bm25_partition_local_p1_idx;
SELECT count(*)
FROM pg_partition
WHERE parentid = 'bm25_partition_local_idx'::regclass
  AND indisusable;

REINDEX INDEX bm25_partition_local_idx;
COPY (
    SELECT id
    FROM bm25_partition_local
    WHERE part_key < 100
    ORDER BY content <&> 'apple' DESC
    LIMIT 1
) TO STDOUT;

-- Rollback must mark every touched physical index, including a partition
-- first visited after multiple inserts into another partition.
BEGIN;
INSERT INTO bm25_partition_local VALUES
    (31, 31, 'apple'),
    (32, 32, 'apple apple');
INSERT INTO bm25_partition_local VALUES (131, 131, 'banana banana banana');
ROLLBACK;

COPY (
    SELECT id
    FROM bm25_partition_local
    WHERE part_key < 100
    ORDER BY content <&> 'apple' DESC
    LIMIT 1
) TO STDOUT;

COPY (
    SELECT id
    FROM bm25_partition_local
    WHERE part_key >= 100 AND part_key < 200
    ORDER BY content <&> 'banana' DESC
    LIMIT 1
) TO STDOUT;

REINDEX INDEX bm25_partition_local_idx;
COPY (
    SELECT id
    FROM bm25_partition_local
    WHERE part_key < 100
    ORDER BY content <&> 'apple' DESC
    LIMIT 1
) TO STDOUT;

COPY (
    SELECT id
    FROM bm25_partition_local
    WHERE part_key >= 100 AND part_key < 200
    ORDER BY content <&> 'banana' DESC
    LIMIT 1
) TO STDOUT;
DROP TABLE bm25_partition_local;

-- MERGE copies heap blocks directly, so rebuild BM25 against the merged heap.
-- Keep a btree LOCAL index alongside BM25 to exercise both merge paths.
CREATE TABLE bm25_partition_merge (id int, part_key int, content text)
PARTITION BY RANGE (part_key)
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200),
    PARTITION p3 VALUES LESS THAN (300)
);
INSERT INTO bm25_partition_merge VALUES
    (1, 1, 'apple'),
    (2, 60, 'apple apple'),
    (3, 101, 'banana'),
    (4, 150, 'apple banana');
CREATE INDEX bm25_partition_merge_idx ON bm25_partition_merge USING bm25(content) LOCAL;
CREATE INDEX bm25_partition_merge_btree_idx ON bm25_partition_merge(id) LOCAL;
ALTER TABLE bm25_partition_merge SPLIT PARTITION p1 AT (50)
    INTO (PARTITION p1a, PARTITION p1b);

BEGIN;
ALTER TABLE bm25_partition_merge MERGE PARTITIONS p1a, p1b INTO PARTITION p1rollback;
SELECT string_agg(id::text, ',' ORDER BY id) FROM (
    SELECT id FROM bm25_partition_merge
    ORDER BY content <&> 'apple' DESC LIMIT 10
) q;
SELECT pg_catalog.bm25_table_stat('^bm25_partition_merge$', 'apple', 'content');
ROLLBACK;

-- Rollback restores both original partitions and their physical indexes.
SELECT string_agg(relname, ',' ORDER BY relname)
FROM pg_partition
WHERE parentid = 'bm25_partition_merge'::regclass AND parttype = 'p';
SELECT string_agg(id::text, ',' ORDER BY id) FROM (
    SELECT id FROM bm25_partition_merge WHERE part_key < 50
    ORDER BY content <&> 'apple' DESC LIMIT 10
) q;
SELECT string_agg(id::text, ',' ORDER BY id) FROM (
    SELECT id FROM bm25_partition_merge WHERE part_key >= 50 AND part_key < 100
    ORDER BY content <&> 'apple' DESC LIMIT 10
) q;
SELECT pg_catalog.bm25_table_stat('^bm25_partition_merge$', 'apple', 'content');

ALTER TABLE bm25_partition_merge MERGE PARTITIONS p1a, p1b INTO PARTITION p1merged;
SELECT string_agg(id::text, ',' ORDER BY id) FROM (
    SELECT id FROM bm25_partition_merge
    ORDER BY content <&> 'apple' DESC LIMIT 10
) q;
SELECT string_agg(id::text, ',' ORDER BY id) FROM (
    SELECT id FROM bm25_partition_merge WHERE part_key < 100
    ORDER BY content <&> 'apple' DESC LIMIT 10
) q;
SELECT string_agg(id::text, ',' ORDER BY id) FROM (
    SELECT id FROM bm25_partition_merge WHERE part_key >= 100
    ORDER BY content <&> 'apple' DESC LIMIT 10
) q;
SELECT pg_catalog.bm25_table_stat('^bm25_partition_merge$', 'apple', 'content');
SELECT pg_catalog.bm25_table_stat('^bm25_partition_merge$', 'banana', 'content');
SELECT count(*) FROM pg_partition
WHERE parentid IN ('bm25_partition_merge_idx'::regclass,
                   'bm25_partition_merge_btree_idx'::regclass) AND indisusable;
SELECT string_agg(id::text, ',' ORDER BY id) FROM bm25_partition_merge WHERE id > 0;

-- Merge a populated partition with an empty one.
ALTER TABLE bm25_partition_merge MERGE PARTITIONS p2, p3 INTO PARTITION p23;
SELECT string_agg(id::text, ',' ORDER BY id) FROM (
    SELECT id FROM bm25_partition_merge
    ORDER BY content <&> 'apple' DESC LIMIT 10
) q;
SELECT string_agg(id::text, ',' ORDER BY id) FROM (
    SELECT id FROM bm25_partition_merge WHERE part_key >= 100 AND part_key < 300
    ORDER BY content <&> 'banana' DESC LIMIT 10
) q;
SELECT pg_catalog.bm25_table_stat('^bm25_partition_merge$', 'apple', 'content');
SELECT string_agg(id::text, ',' ORDER BY id) FROM bm25_partition_merge WHERE id > 0;
DROP TABLE bm25_partition_merge;

-- Expression indexes must rebuild from the expression, not a raw attribute.
CREATE TABLE bm25_partition_merge_expr (id int, part_key int, content text)
PARTITION BY RANGE (part_key)
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
INSERT INTO bm25_partition_merge_expr VALUES
    (1, 1, 'APPLE APPLE'), (2, 101, 'APPLE BANANA');
CREATE INDEX bm25_partition_merge_expr_idx
    ON bm25_partition_merge_expr USING bm25(lower(content)) LOCAL;
ALTER TABLE bm25_partition_merge_expr MERGE PARTITIONS p1, p2 INTO PARTITION p12;
SELECT string_agg(id::text, ',' ORDER BY id) FROM (
    SELECT id FROM bm25_partition_merge_expr
    ORDER BY lower(content) <&> 'apple' DESC LIMIT 10
) q;
DROP TABLE bm25_partition_merge_expr;

-- Materialize an exhaustive TAAT score oracle before testing bounded DAAT.
-- Each partition has 128 distinct lengths; for "apple", shorter rows rank first.
SET enable_bm25_global_idf = off;
SET bm25_topk = 5;
SET bm25_k1 = 1.2;
SET bm25_b = 0.75;
SET max_score_ratio = 1.05;
CREATE TABLE bm25_partition_topk (id int, part_key int, content text)
PARTITION BY RANGE (part_key)
(
    PARTITION p1 VALUES LESS THAN (1), PARTITION p2 VALUES LESS THAN (2),
    PARTITION p3 VALUES LESS THAN (3), PARTITION p4 VALUES LESS THAN (4)
);
INSERT INTO bm25_partition_topk
SELECT p * 1000 + i, p, 'apple ' || repeat('banana ', i)
FROM generate_series(0, 3) p, generate_series(1, 128) i;
CREATE INDEX bm25_partition_topk_idx ON bm25_partition_topk USING bm25(content) LOCAL;
SET enable_bm25_taat = on;
CREATE TEMP TABLE bm25_partition_topk_oracle AS
SELECT id, content <&> 'apple' AS score FROM bm25_partition_topk
ORDER BY content <&> 'apple' DESC;
SELECT count(*) = 512 FROM bm25_partition_topk_oracle;
SET enable_bm25_taat = off;

SELECT
    (SELECT array_agg(score ORDER BY score DESC) FROM (
        SELECT content <&> 'apple' AS score FROM bm25_partition_topk
        ORDER BY content <&> 'apple' DESC LIMIT 17 OFFSET 11
    ) q) =
    (SELECT array_agg(score ORDER BY score DESC) FROM (
        SELECT score FROM bm25_partition_topk_oracle ORDER BY score DESC LIMIT 17 OFFSET 11
    ) q);

-- Reject the best 96 rows in every partition, forcing several candidate expansions.
SELECT
    (SELECT array_agg(score ORDER BY score DESC) FROM (
        SELECT content <&> 'apple' AS score FROM bm25_partition_topk WHERE id % 1000 > 96
        ORDER BY content <&> 'apple' DESC LIMIT 19 OFFSET 9
    ) q) =
    (SELECT array_agg(score ORDER BY score DESC) FROM (
        SELECT score FROM bm25_partition_topk_oracle WHERE id % 1000 > 96
        ORDER BY score DESC LIMIT 19 OFFSET 9
    ) q);

-- Equal scores across partitions need the complete secondary-key ordering.
SELECT
    (SELECT array_agg(id ORDER BY score DESC, id) FROM (
        SELECT id, score FROM (
            SELECT id, content <&> 'apple' AS score FROM bm25_partition_topk
            ORDER BY content <&> 'apple' DESC OFFSET 0
        ) indexed ORDER BY score DESC, id LIMIT 19 OFFSET 5
    ) q) =
    (SELECT array_agg(id ORDER BY score DESC, id) FROM (
        SELECT id, score FROM bm25_partition_topk_oracle ORDER BY score DESC, id LIMIT 19 OFFSET 5
    ) q);

-- Reusing one generic plan must reset the per-partition bound on every execution.
SET plan_cache_mode = force_generic_plan;
PREPARE bm25_partition_topk_limit(bigint, bigint) AS
SELECT
    (SELECT array_agg(score ORDER BY score DESC) FROM (
        SELECT content <&> 'apple' AS score FROM bm25_partition_topk
        ORDER BY content <&> 'apple' DESC LIMIT $1 OFFSET $2
    ) q) IS NOT DISTINCT FROM
    (SELECT array_agg(score ORDER BY score DESC) FROM (
        SELECT score FROM bm25_partition_topk_oracle ORDER BY score DESC LIMIT $1 OFFSET $2
    ) q);
EXECUTE bm25_partition_topk_limit(3, 0);
EXECUTE bm25_partition_topk_limit(21, 13);
EXECUTE bm25_partition_topk_limit(NULL, 0);
EXECUTE bm25_partition_topk_limit(0, 0);
EXECUTE bm25_partition_topk_limit(5000, 0);
EXECUTE bm25_partition_topk_limit(7, 5000);
EXECUTE bm25_partition_topk_limit(NULL, 11);
EXECUTE bm25_partition_topk_limit(9, NULL);
EXECUTE bm25_partition_topk_limit(3, 5);
DEALLOCATE bm25_partition_topk_limit;
RESET plan_cache_mode;

-- Changed scoring parameters and a ratio below one cannot invalidate DAAT bounds.
SET bm25_k1 = 2.7;
SET bm25_b = 0.1;
SET max_score_ratio = 0.5;
SET enable_bm25_taat = on;
CREATE TEMP TABLE bm25_partition_topk_reweighted AS
SELECT id, content <&> 'apple banana' AS score FROM bm25_partition_topk
ORDER BY content <&> 'apple banana' DESC;
SET enable_bm25_taat = off;
SELECT
    (SELECT array_agg(score ORDER BY score DESC) FROM (
        SELECT content <&> 'apple banana' AS score FROM bm25_partition_topk
        ORDER BY content <&> 'apple banana' DESC LIMIT 33 OFFSET 17
    ) q) =
    (SELECT array_agg(score ORDER BY score DESC) FROM (
        SELECT score FROM bm25_partition_topk_reweighted ORDER BY score DESC LIMIT 33 OFFSET 17
    ) q);

DROP TABLE bm25_partition_topk_reweighted;
DROP TABLE bm25_partition_topk_oracle;
DROP TABLE bm25_partition_topk;
RESET bm25_k1;
RESET bm25_b;
RESET max_score_ratio;
RESET bm25_topk;
RESET enable_bm25_taat;
RESET enable_bm25_global_idf;
