-- P3-7 local-machine calibration: deterministic 100k rows, HNSW only.
-- The 10m tier was cancelled and DiskANN is outside the accepted scope.
\set ON_ERROR_STOP on
\timing on

SET client_min_messages = warning;
SET search_path = ogai, public;

DELETE FROM ogai.vectorize_tasks
 WHERE task_name = 'p37_hnsw_100k_task';
DROP TABLE IF EXISTS public.p37_hnsw_100k CASCADE;

\echo === P37_CAL CREATE_TABLES ===
CREATE TABLE public.p37_hnsw_100k (
    id integer PRIMARY KEY,
    tenant_id integer NOT NULL,
    title text NOT NULL,
    content text NOT NULL,
    ogai_embedding vector(8) NOT NULL
);
\echo === P37_CAL INSERT_HNSW_100K ===
INSERT INTO public.p37_hnsw_100k
SELECT i,
       i % 100,
       'document-' || i,
       CASE i % 4
         WHEN 0 THEN 'database database kernel transaction query optimizer '
         WHEN 1 THEN 'storage storage buffer checkpoint wal recovery '
         WHEN 2 THEN 'vector vector embedding similarity nearest search '
         ELSE 'memory memory cache executor worker parallel '
       END ||
       CASE WHEN i % 10000 = 0 THEN 'rare_p37_token ' ELSE '' END ||
       'record ' || i,
       ('[' ||
        round((((i % 4) * 0.23 + (i % 97)::numeric / 9700)), 6) || ',' ||
        round(((((i + 1) % 4) * 0.19 + (i % 89)::numeric / 8900)), 6) || ',' ||
        round(((((i + 2) % 4) * 0.17 + (i % 83)::numeric / 8300)), 6) || ',' ||
        round(((((i + 3) % 4) * 0.13 + (i % 79)::numeric / 7900)), 6) || ',' ||
        round((((i *  7) % 101)::numeric / 101), 6) || ',' ||
        round((((i * 11) % 103)::numeric / 103), 6) || ',' ||
        round((((i * 13) % 107)::numeric / 107), 6) || ',' ||
        round((((i * 17) % 109)::numeric / 109), 6) ||
        ']')::vector
  FROM generate_series(1, 100000) AS i;

\echo === P37_CAL BUILD_BM25_HNSW_TABLE ===
CREATE INDEX p37_hnsw_100k_bm25_idx
    ON public.p37_hnsw_100k USING bm25 (content);
\echo === P37_CAL BUILD_HNSW_100K ===
CREATE INDEX p37_hnsw_100k_ann_idx
    ON public.p37_hnsw_100k USING hnsw (ogai_embedding vector_cosine_ops)
    WITH (m = 8, ef_construction = 32);

ANALYZE public.p37_hnsw_100k;

INSERT INTO ogai.vectorize_tasks(
    task_name, type, index_type, model_key, src_schema, src_table,
    src_col, primary_key, method, dim, enable_bm25, owner_name)
VALUES (
    'p37_hnsw_100k_task', 'sync', 'hnsw', 'p37_stub', 'public',
    'p37_hnsw_100k', 'content', 'id', 'append', 8, true, CURRENT_USER);

\echo === P37_CAL SIZES ===
SELECT relname,
       pg_size_pretty(pg_relation_size(oid)) AS heap_size,
       pg_size_pretty(pg_indexes_size(oid)) AS indexes_size,
       pg_size_pretty(pg_total_relation_size(oid)) AS total_size
  FROM pg_class
 WHERE relname = 'p37_hnsw_100k'
 ORDER BY relname;

\echo === P37_CAL COUNTS ===
SELECT 'hnsw' AS kind, count(*) FROM public.p37_hnsw_100k;
