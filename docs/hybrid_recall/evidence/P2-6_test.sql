-- P2-6: initial HybridRecall cost model.
-- Small work should avoid task-stream startup; sufficiently expensive
-- branches should amortize it and select the parallel alternative.
\set ON_ERROR_STOP on

SET enable_hybrid_recall = on;
SET hybrid_recall_parallel_mode = auto;

\echo === P2-6 SMALL FIXTURE: EXPECT SERIAL ===
EXPLAIN (VERBOSE, COSTS ON)
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 10;

DROP TABLE IF EXISTS hybrid_recall_cost_docs CASCADE;
CREATE TABLE hybrid_recall_cost_docs (
    id bigint PRIMARY KEY,
    tenant_id int NOT NULL,
    content text NOT NULL,
    embedding vector(8) NOT NULL
);

INSERT INTO hybrid_recall_cost_docs
SELECT g,
       1,
       'database cost-model document ' || g::text,
       '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector
FROM generate_series(1, 20000) AS g;

CREATE INDEX hrcd_bm25_idx ON hybrid_recall_cost_docs USING bm25 (content);
CREATE INDEX hrcd_hnsw_idx ON hybrid_recall_cost_docs USING hnsw (embedding vector_cosine_ops);
ANALYZE hybrid_recall_cost_docs;

\echo === P2-6 LARGE FIXTURE: EXPECT PARALLEL ===
EXPLAIN (VERBOSE, COSTS ON)
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_cost_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 10;

DROP TABLE hybrid_recall_cost_docs CASCADE;
RESET hybrid_recall_parallel_mode;
