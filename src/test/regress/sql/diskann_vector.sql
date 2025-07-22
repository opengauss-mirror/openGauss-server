create database test;
\c test

-- diskann basic test
CREATE TABLE diskann_t1
(
    id          SERIAL PRIMARY KEY,
    description TEXT,
    embedding   vector(6)
);
INSERT INTO diskann_t1 (description, embedding)
VALUES ('item 1', '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6]'),
       ('item 2', '[0.7, 0.8, 0.9, 1.0, 1.1, 1.2]'),
       ('item 3', '[1.3, 1.4, 1.5, 1.6, 1.7, 1.8]'),
       ('item 4', '[0.5, 0.4, 0.3, 0.2, 0.1, 0.0]'),
       ('item 5', '[0.9, 0.8, 0.7, 0.6, 0.5, 0.4]'),
       ('item 6', '[0.2, 0.4, 0.6, 0.8, 1.0, 1.2]'),
       ('item 7', '[0.3, 0.6, 0.9, 1.2, 1.5, 1.8]'),
       ('item 8', '[0.4, 0.3, 0.2, 0.1, 0.0, 0.1]'),
       ('item 9', '[0.6, 0.5, 0.4, 0.3, 0.2, 0.1]'),
       ('item 10', '[0.8, 0.6, 0.4, 0.2, 0.0, 0.2]');
analyze diskann_t1;
-- l2 distance
CREATE INDEX idx_vectors_l2 ON diskann_t1 USING diskann(embedding vector_l2_ops);
EXPLAIN
(COSTS OFF)
SELECT /*+ indexscan(diskann_t1 idx_vectors_l2) */ id, description
FROM diskann_t1
ORDER BY embedding <-> '[0.3, 0.4, 0.5, 0.6, 0.7, 0.8]' LIMIT 3;
-- cosine distance
CREATE INDEX idx_vectors_cosine ON diskann_t1 USING diskann(embedding vector_cosine_ops);
EXPLAIN
(COSTS OFF)
SELECT /*+ indexscan(diskann_t1 idx_vectors_cosine) */ id, description
FROM diskann_t1
ORDER BY embedding <=> '[0.3, 0.4, 0.5, 0.6, 0.7, 0.8]' LIMIT 3;
-- ip distance
CREATE INDEX idx_vectors_ip ON diskann_t1 USING diskann(embedding vector_ip_ops);
EXPLAIN
(COSTS OFF)
SELECT /*+ indexscan(diskann_t1 idx_vectors_ip) */ id, description
FROM diskann_t1
ORDER BY embedding <#> '[0.3, 0.4, 0.5, 0.6, 0.7, 0.8]' LIMIT 3;

-- diskann index size test
CREATE TABLE diskann_t2
(
    id          SERIAL PRIMARY KEY,
    description TEXT,
    embedding   vector(10)
);

-- insert 10 items
INSERT INTO diskann_t2 (description, embedding)
VALUES ('item 1', '[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]'),
       ('item 2', '[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1]'),
       ('item 3', '[0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]'),
       ('item 4', '[0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3]'),
       ('item 5', '[0.5, 0.4, 0.3, 0.2, 0.1, 0.0, 0.1, 0.2, 0.3, 0.4]'),
       ('item 6', '[0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0, 0.1, 0.2, 0.3]'),
       ('item 7', '[0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0, 0.1, 0.2]'),
       ('item 8', '[0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0, 0.1]'),
       ('item 9', '[0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0]'),
       ('item 10', '[1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]');

-- test different index size
CREATE INDEX idx_vectors_10d_16 ON diskann_t2 USING diskann(embedding) WITH (index_size = 16);
CREATE INDEX idx_vectors_10d_100 ON diskann_t2 USING diskann(embedding) WITH (index_size = 100);
CREATE INDEX idx_vectors_10d_1000 ON diskann_t2 USING diskann(embedding) WITH (index_size = 1000);
CREATE INDEX idx_vectors_10d_invalid1 ON diskann_t2 USING diskann(embedding) WITH (index_size = 15);
CREATE INDEX idx_vectors_10d_invalid2 ON diskann_t2 USING diskann(embedding) WITH (index_size = 1001);
-- search test
EXPLAIN
(COSTS OFF)
SELECT /*+ indexscan(diskann_t2 idx_vectors_10d_100) */ id, description
FROM diskann_t2
ORDER BY embedding <-> '[0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]' LIMIT 3;

\c regression
drop database test;
