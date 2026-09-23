DROP TABLE IF EXISTS vector_buffer_fastcheck;

CREATE TABLE vector_buffer_fastcheck(id int, embedding vector(32));

INSERT INTO vector_buffer_fastcheck
SELECT id, ('[' || string_agg(id::text, ',') || ']')::vector
FROM generate_series(1, 10) AS ids(id)
CROSS JOIN generate_series(1, 32) AS dims(pos)
GROUP BY id;

CREATE INDEX vector_buffer_invalid_idx
    ON vector_buffer_fastcheck USING hnsw (embedding vector_l2_ops)
    WITH (enable_vector_payload_storage = on, use_mmap = on);

CREATE INDEX vector_buffer_invalid_idx
    ON vector_buffer_fastcheck USING hnsw (embedding vector_l2_ops)
    WITH (enable_vector_payload_storage = on, enable_pq = on);

CREATE INDEX vector_buffer_invalid_idx
    ON vector_buffer_fastcheck USING hnsw (embedding vector_l2_ops)
    WITH (enable_vector_payload_storage = on, enable_rabitq = on);

CREATE INDEX vector_buffer_invalid_idx
    ON vector_buffer_fastcheck USING hnsw (embedding vector_l2_ops)
    WITH (enable_vector_payload_storage = on, enable_lsg = on);

CREATE INDEX vector_buffer_invalid_idx
    ON vector_buffer_fastcheck USING hnsw (embedding vector_l2_ops)
    WITH (enable_vector_payload_storage = on, m = 16, ef_construction = 16);

CREATE INDEX vector_buffer_hnsw_idx
    ON vector_buffer_fastcheck USING hnsw (embedding vector_l2_ops)
    WITH (enable_vector_payload_storage = on);

CREATE INDEX vector_buffer_invalid_diskann_idx
    ON vector_buffer_fastcheck USING diskann (embedding vector_l2_ops)
    WITH (enable_vector_payload_storage = on, enable_rabitq = on);

CREATE INDEX vector_buffer_ivfflat_idx
    ON vector_buffer_fastcheck USING ivfflat (embedding vector_l2_ops)
    WITH (lists = 1, enable_vector_payload_storage = on);

CREATE INDEX vector_buffer_diskann_idx
    ON vector_buffer_fastcheck USING diskann (embedding vector_l2_ops)
    WITH (index_size = 16, enable_vector_payload_storage = on);

ALTER INDEX vector_buffer_hnsw_idx
    SET (enable_vector_payload_storage = off);

SET enable_seqscan = off;
SET enable_vector_buffer_cache = off;

SELECT /*+ indexscan(vector_buffer_fastcheck vector_buffer_hnsw_idx) */ id
FROM vector_buffer_fastcheck
ORDER BY embedding <-> '[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]'
LIMIT 1;

SET enable_vector_buffer_cache = on;

SELECT /*+ indexscan(vector_buffer_fastcheck vector_buffer_hnsw_idx) */ id
FROM vector_buffer_fastcheck
ORDER BY embedding <-> '[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]'
LIMIT 1;

SELECT /*+ indexscan(vector_buffer_fastcheck vector_buffer_hnsw_idx) */ id
FROM vector_buffer_fastcheck
ORDER BY embedding <-> '[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]'
LIMIT 1;

SELECT /*+ indexscan(vector_buffer_fastcheck vector_buffer_ivfflat_idx) */ id
FROM vector_buffer_fastcheck
ORDER BY embedding <-> '[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]'
LIMIT 1;

SELECT /*+ indexscan(vector_buffer_fastcheck vector_buffer_diskann_idx) */ id
FROM vector_buffer_fastcheck
ORDER BY embedding <-> '[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]'
LIMIT 1;

SELECT capacity_bytes > 0 AND used_bytes > 0 AND entries > 0 AND active_pools >= 3 AS global_stats_ok
FROM pg_stat_vector_buffer;

SELECT lookups = hits + misses AND lookups > 0 AND hits > 0 AS hit_rate_ok
FROM pg_stat_vector_buffer_hit_rate;

SELECT count(*) >= 3 AS pool_stats_ok
FROM pg_stat_vector_buffer_pool
WHERE state = 'ACTIVE';

SELECT count(*) > 0 AS chunk_stats_ok
FROM pg_stat_vector_buffer_chunk
WHERE state IN ('ACTIVE', 'DRAINING');

SELECT count(*) >= 3 AS hash_stats_ok
FROM pg_stat_vector_buffer_hash_chain;

CREATE TABLE vector_buffer_bridgecheck AS SELECT * FROM vector_buffer_fastcheck;
CREATE INDEX vector_buffer_bridge_idx
    ON vector_buffer_bridgecheck USING diskann (embedding vector_l2_ops)
    WITH (index_size = 16, enable_vector_payload_storage = on);
INSERT INTO vector_buffer_bridgecheck VALUES (20, ('[' || repeat('20,', 31) || '20]')::vector);
BEGIN;
INSERT INTO vector_buffer_bridgecheck VALUES (30, ('[' || repeat('30,', 31) || '30]')::vector);
ROLLBACK;
UPDATE vector_buffer_bridgecheck SET embedding = ('[' || repeat('40,', 31) || '40]')::vector WHERE id = 20;
VACUUM vector_buffer_bridgecheck;
SELECT id FROM vector_buffer_bridgecheck
ORDER BY embedding <-> '[40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40,40]' LIMIT 1;
DROP TABLE vector_buffer_bridgecheck;

CREATE TABLE vector_buffer_rabitqcheck(id int, embedding vector(32));
INSERT INTO vector_buffer_rabitqcheck SELECT * FROM vector_buffer_fastcheck;
CREATE INDEX vector_buffer_rabitq_idx
    ON vector_buffer_rabitqcheck USING diskann (embedding vector_l2_ops)
    WITH (index_size = 16, enable_rabitq = on, rabitq_bits = 2, enable_vector_payload_storage = off);
SELECT id FROM vector_buffer_rabitqcheck
ORDER BY embedding <-> '[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]'
LIMIT 1;
DELETE FROM vector_buffer_rabitqcheck WHERE id = 1;
VACUUM vector_buffer_rabitqcheck;
SELECT id FROM vector_buffer_rabitqcheck
ORDER BY embedding <-> '[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]'
LIMIT 1;
REINDEX INDEX vector_buffer_rabitq_idx;
SELECT id FROM vector_buffer_rabitqcheck
ORDER BY embedding <-> '[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]'
LIMIT 1;
DROP TABLE vector_buffer_rabitqcheck;

-- A dead primary TID must not hide a live duplicate, or recycle its vector.
CREATE FUNCTION vector_buffer_test_value(i int) RETURNS vector AS $$
SELECT ('[' || repeat(i::text || ',', 31) || i::text || ']')::vector;
$$ LANGUAGE SQL IMMUTABLE;
CREATE TABLE vector_buffer_vacuumcheck AS SELECT * FROM vector_buffer_fastcheck;
CREATE INDEX vector_buffer_vacuum_idx ON vector_buffer_vacuumcheck
USING diskann (embedding vector_l2_ops) WITH (index_size = 16, enable_vector_payload_storage = on);
BEGIN;
INSERT INTO vector_buffer_vacuumcheck VALUES (20, vector_buffer_test_value(20));
ROLLBACK;
INSERT INTO vector_buffer_vacuumcheck VALUES (30, vector_buffer_test_value(20));
VACUUM vector_buffer_vacuumcheck;
SELECT id = 30 AS live_duplicate FROM vector_buffer_vacuumcheck
ORDER BY embedding <-> vector_buffer_test_value(20) LIMIT 1;
BEGIN;
INSERT INTO vector_buffer_vacuumcheck VALUES (40, vector_buffer_test_value(40));
ROLLBACK;
VACUUM vector_buffer_vacuumcheck;
INSERT INTO vector_buffer_vacuumcheck VALUES (50, vector_buffer_test_value(50));
BEGIN;
INSERT INTO vector_buffer_vacuumcheck VALUES (60, vector_buffer_test_value(60));
ROLLBACK;
VACUUM vector_buffer_vacuumcheck;
SELECT id = 50 AS reused_payload_live FROM vector_buffer_vacuumcheck
ORDER BY embedding <-> vector_buffer_test_value(50) LIMIT 1;
DROP TABLE vector_buffer_vacuumcheck;

-- A pruned directed edge can still point at an already retired graph node.
CREATE FUNCTION vector_buffer_test_directed(i int) RETURNS vector AS $$
SELECT ('[' || string_agg((sin(i * j * 79.0 + i * i * j)::real)::text, ',') || ']')::vector
FROM generate_series(1, 32) j;
$$ LANGUAGE SQL IMMUTABLE;
CREATE TABLE vector_buffer_directedcheck(id int, embedding vector(32));
INSERT INTO vector_buffer_directedcheck SELECT i, vector_buffer_test_directed(i) FROM generate_series(1, 100) i;
CREATE INDEX vector_buffer_directed_idx ON vector_buffer_directedcheck USING diskann (embedding vector_l2_ops)
WITH (index_size = 32, enable_vector_payload_storage = on);
BEGIN;
INSERT INTO vector_buffer_directedcheck SELECT i, vector_buffer_test_directed(i) FROM generate_series(101, 200) i;
ROLLBACK;
INSERT INTO vector_buffer_directedcheck SELECT i, vector_buffer_test_directed(i) FROM generate_series(201, 250) i;
VACUUM vector_buffer_directedcheck;
SELECT id = 250 AS directed_vacuum_ok FROM vector_buffer_directedcheck
ORDER BY embedding <-> vector_buffer_test_directed(250) LIMIT 1;
INSERT INTO vector_buffer_directedcheck SELECT i, vector_buffer_test_directed(i) FROM generate_series(251, 270) i;
VACUUM vector_buffer_directedcheck;
SELECT id = 270 AS directed_insert_ok FROM vector_buffer_directedcheck
ORDER BY embedding <-> vector_buffer_test_directed(270) LIMIT 1;
DROP TABLE vector_buffer_directedcheck;
DROP FUNCTION vector_buffer_test_directed(int);

-- Retire several IVF owners on the same page, then reuse their payload slots.
CREATE TABLE vector_buffer_ivfrecyclecheck AS SELECT * FROM vector_buffer_fastcheck;
CREATE INDEX vector_buffer_ivfrecycle_idx ON vector_buffer_ivfrecyclecheck USING ivfflat (embedding vector_l2_ops)
WITH (lists = 1, enable_vector_payload_storage = on);
BEGIN;
INSERT INTO vector_buffer_ivfrecyclecheck SELECT i, vector_buffer_test_value(i) FROM generate_series(11, 20) i;
ROLLBACK;
VACUUM vector_buffer_ivfrecyclecheck;
INSERT INTO vector_buffer_ivfrecyclecheck VALUES (30, vector_buffer_test_value(30));
VACUUM vector_buffer_ivfrecyclecheck;
SELECT id = 30 AS ivf_recycle_ok FROM vector_buffer_ivfrecyclecheck
ORDER BY embedding <-> vector_buffer_test_value(30) LIMIT 1;
DROP TABLE vector_buffer_ivfrecyclecheck;

-- IVFFlat payloads retain the original halfvec / bit Datum layout.
CREATE TABLE vector_buffer_halfcheck(id int, embedding halfvec(32));
INSERT INTO vector_buffer_halfcheck SELECT id, embedding::halfvec FROM vector_buffer_fastcheck;
CREATE INDEX vector_buffer_half_idx ON vector_buffer_halfcheck USING ivfflat (embedding halfvec_l2_ops)
WITH (lists = 1, enable_vector_payload_storage = on);
INSERT INTO vector_buffer_halfcheck VALUES (30, vector_buffer_test_value(30)::halfvec);
SELECT id = 30 AS halfvec_payload_ok FROM vector_buffer_halfcheck
ORDER BY embedding <-> vector_buffer_test_value(30)::halfvec LIMIT 1;
DROP TABLE vector_buffer_halfcheck;
CREATE TABLE vector_buffer_bitcheck(id int, embedding bit(32));
INSERT INTO vector_buffer_bitcheck SELECT i, i::bit(32) FROM generate_series(1, 10) i;
CREATE INDEX vector_buffer_bit_idx ON vector_buffer_bitcheck USING ivfflat (embedding bit_hamming_ops)
WITH (lists = 1, enable_vector_payload_storage = on);
INSERT INTO vector_buffer_bitcheck VALUES (30, 30::bit(32));
SELECT id = 30 AS bit_payload_ok FROM vector_buffer_bitcheck ORDER BY embedding <~> 30::bit(32) LIMIT 1;
DROP TABLE vector_buffer_bitcheck;

-- Raise an ERROR while the scanner still owns a borrowed payload, then reuse the session.
CREATE FUNCTION vector_buffer_test_distance(a vector, b vector) RETURNS float8 AS $$
BEGIN
    IF current_setting('application_name') = 'vbp_abort_test' AND a::text LIKE '[7,%' THEN
        RAISE division_by_zero;
    END IF;
    RETURN vector_l2_squared_distance(a, b);
END;
$$ LANGUAGE plpgsql;
CREATE OPERATOR CLASS vector_buffer_test_ops FOR TYPE vector USING ivfflat AS
OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
FUNCTION 1 vector_buffer_test_distance(vector, vector), FUNCTION 3 l2_distance(vector, vector);
CREATE TABLE vector_buffer_abortcheck AS SELECT * FROM vector_buffer_fastcheck;
CREATE INDEX vector_buffer_abort_idx ON vector_buffer_abortcheck USING ivfflat (embedding vector_buffer_test_ops)
WITH (lists = 1, enable_vector_payload_storage = on);
SET enable_vector_buffer_cache = off;
SET application_name = 'vbp_abort_test';
DO $$
DECLARE caught boolean := false;
BEGIN
    BEGIN
        PERFORM id FROM vector_buffer_abortcheck ORDER BY embedding <-> vector_buffer_test_value(1) LIMIT 1;
    EXCEPTION WHEN division_by_zero THEN
        caught := true;
    END;
    IF NOT caught THEN
        RAISE EXCEPTION 'borrowed payload error path was not exercised';
    END IF;
END;
$$;
RESET application_name;
SELECT id = 1 AS abort_cleanup_ok FROM vector_buffer_abortcheck
ORDER BY embedding <-> vector_buffer_test_value(1) LIMIT 1;
DROP TABLE vector_buffer_abortcheck;
DROP OPERATOR CLASS vector_buffer_test_ops USING ivfflat;
DROP FUNCTION vector_buffer_test_distance(vector, vector);
DROP FUNCTION vector_buffer_test_value(int);

SHOW vacuum_gtt_defer_check_age;
SHOW wait_dummy_time;

RESET enable_vector_buffer_cache;
RESET enable_seqscan;

DROP TABLE vector_buffer_fastcheck;
