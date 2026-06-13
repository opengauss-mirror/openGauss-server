DROP TABLE IF EXISTS ai_vec_part_hnsw CASCADE;
CREATE TABLE ai_vec_part_hnsw (
    id int,
    part_key int,
    embedding vector(3)
)
PARTITION BY RANGE (part_key)
(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (20)
);

INSERT INTO ai_vec_part_hnsw VALUES
    (1, 1, '[0.11,0.12,0.13]'),
    (2, 5, '[0.21,0.22,0.23]'),
    (3, 11, '[0.31,0.32,0.33]'),
    (4, 15, '[0.41,0.42,0.43]');

CREATE INDEX ai_vec_part_hnsw_idx ON ai_vec_part_hnsw USING hnsw (embedding vector_l2_ops) LOCAL
(
    PARTITION p1_hnsw_idx,
    PARTITION p2_hnsw_idx
);

SELECT relname, indisusable
FROM pg_partition
WHERE parentid = 'ai_vec_part_hnsw_idx'::regclass
ORDER BY relname;

SELECT pg_get_indexdef('ai_vec_part_hnsw_idx'::regclass, true);

ALTER TABLE ai_vec_part_hnsw ADD PARTITION p3 VALUES LESS THAN (30);

INSERT INTO ai_vec_part_hnsw VALUES
    (5, 21, '[0.51,0.52,0.53]'),
    (6, 25, '[0.61,0.62,0.63]');

ANALYZE ai_vec_part_hnsw;
SET enable_seqscan = off;
\pset tuples_only on
EXPLAIN (COSTS OFF)
SELECT id, part_key
FROM ai_vec_part_hnsw
ORDER BY embedding <-> '[0.35,0.36,0.37]'::vector
LIMIT 2;

EXPLAIN (COSTS OFF)
SELECT id, part_key
FROM ai_vec_part_hnsw
ORDER BY embedding <-> '[0.35,0.36,0.37]'::vector
LIMIT (floor(random() * 2) + 1)::int;
\pset tuples_only off

EXPLAIN
SELECT id, part_key
FROM ai_vec_part_hnsw
WHERE part_key >= 20 AND part_key < 30
ORDER BY embedding <-> '[0.55,0.56,0.57]'::vector
LIMIT 2;

SELECT id, part_key
FROM ai_vec_part_hnsw
WHERE part_key >= 20 AND part_key < 30
ORDER BY embedding <-> '[0.55,0.56,0.57]'::vector
LIMIT 2;
RESET enable_seqscan;

SELECT relname, indisusable
FROM pg_partition
WHERE parentid = 'ai_vec_part_hnsw_idx'::regclass
ORDER BY relname;

ALTER TABLE ai_vec_part_hnsw MODIFY PARTITION p1 UNUSABLE LOCAL INDEXES;

SELECT relname, indisusable
FROM pg_partition
WHERE parentid = 'ai_vec_part_hnsw_idx'::regclass
ORDER BY relname;

ALTER TABLE ai_vec_part_hnsw MODIFY PARTITION p1 REBUILD UNUSABLE LOCAL INDEXES;
REINDEX INDEX ai_vec_part_hnsw_idx PARTITION p1_hnsw_idx;

SELECT relname, indisusable
FROM pg_partition
WHERE parentid = 'ai_vec_part_hnsw_idx'::regclass
ORDER BY relname;

DROP TABLE ai_vec_part_hnsw;

DROP TABLE IF EXISTS ai_vec_part_ivfflat CASCADE;
CREATE TABLE ai_vec_part_ivfflat (
    id int,
    part_key int,
    embedding vector(3)
)
PARTITION BY RANGE (part_key)
(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (20)
);

INSERT INTO ai_vec_part_ivfflat VALUES
    (1, 1, '[1,1,1]'),
    (2, 2, '[2,2,2]'),
    (3, 11, '[3,3,3]'),
    (4, 12, '[4,4,4]'),
    (5, 13, '[5,5,5]'),
    (6, 14, '[6,6,6]');

CREATE INDEX ai_vec_part_ivfflat_idx ON ai_vec_part_ivfflat USING ivfflat (embedding vector_l2_ops) LOCAL
(
    PARTITION p1_ivfflat_idx,
    PARTITION p2_ivfflat_idx
)
WITH (lists = 1);

SELECT relname, indisusable
FROM pg_partition
WHERE parentid = 'ai_vec_part_ivfflat_idx'::regclass
ORDER BY relname;

SELECT pg_get_indexdef('ai_vec_part_ivfflat_idx'::regclass, true);

ALTER TABLE ai_vec_part_ivfflat ADD PARTITION p3 VALUES LESS THAN (30);

INSERT INTO ai_vec_part_ivfflat VALUES
    (7, 21, '[7,7,7]'),
    (8, 22, '[8,8,8]');

ANALYZE ai_vec_part_ivfflat;
SET enable_seqscan = off;
EXPLAIN
SELECT id, part_key
FROM ai_vec_part_ivfflat
WHERE part_key >= 20 AND part_key < 30
ORDER BY embedding <-> '[7.1,7.1,7.1]'::vector
LIMIT 2;

SELECT id, part_key
FROM ai_vec_part_ivfflat
WHERE part_key >= 20 AND part_key < 30
ORDER BY embedding <-> '[7.1,7.1,7.1]'::vector
LIMIT 2;
RESET enable_seqscan;

SELECT relname, indisusable
FROM pg_partition
WHERE parentid = 'ai_vec_part_ivfflat_idx'::regclass
ORDER BY relname;

DROP TABLE ai_vec_part_ivfflat;

DROP TABLE IF EXISTS ai_vec_part_ustore CASCADE;
CREATE TABLE ai_vec_part_ustore (
    id int,
    part_key int,
    embedding vector(3)
) WITH (storage_type = ustore)
PARTITION BY RANGE (part_key)
(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (20)
);

CREATE INDEX ai_vec_part_ustore_idx ON ai_vec_part_ustore USING hnsw (embedding vector_l2_ops) LOCAL
(
    PARTITION p1_ustore_hnsw_idx,
    PARTITION p2_ustore_hnsw_idx
);

DROP TABLE ai_vec_part_ustore;
