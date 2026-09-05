-- Native regression coverage for the HybridRecall BM25 + HNSW operator.
-- The deterministic dataset and insertion order stabilize candidate identities;
-- the golden checks plan structure and identical serial/parallel ordered output.

SET client_min_messages = error;
\pset format unaligned
CREATE EXTENSION IF NOT EXISTS ogai;

DROP FUNCTION IF EXISTS hybrid_recall_schedule_plan_properties(text);
DROP TABLE IF EXISTS hybrid_recall_schedule_docs CASCADE;
CREATE TABLE hybrid_recall_schedule_docs (
    id int PRIMARY KEY,
    tenant_id int NOT NULL,
    content text NOT NULL,
    embedding vector(8) NOT NULL
);

INSERT INTO hybrid_recall_schedule_docs
SELECT i,
       i % 4,
       (ARRAY['database','kernel','vector','memory','network'])[1 + i % 5]
         || ' '
         || repeat((ARRAY['alpha','beta','gamma','delta','epsilon','zeta','eta'])[1 + i % 7],
                   1 + i % 3)
         || ' record ' || i,
       ('['  || round(((i       % 11)::numeric / 11), 4)
         || ',' || round(((i *  2 % 13)::numeric / 13), 4)
         || ',' || round(((i *  3 % 17)::numeric / 17), 4)
         || ',' || round(((i *  5 % 19)::numeric / 19), 4)
         || ',' || round(((i *  7 % 23)::numeric / 23), 4)
         || ',' || round(((i * 11 % 29)::numeric / 29), 4)
         || ',' || round(((i * 13 % 31)::numeric / 31), 4)
         || ',' || round(((i * 17 % 37)::numeric / 37), 4)
       || ']')::vector
FROM generate_series(1, 200) AS i;

CREATE INDEX hybrid_recall_schedule_bm25_idx
    ON hybrid_recall_schedule_docs USING bm25(content);
CREATE INDEX hybrid_recall_schedule_hnsw_idx
    ON hybrid_recall_schedule_docs USING hnsw(embedding vector_cosine_ops);
ANALYZE hybrid_recall_schedule_docs;

-- Return timing-free, independently assertable properties of a plan.  The
-- variants exercise accepted and rejected V1 shapes without executing the
-- reserved marker when a shape is deliberately rejected.
CREATE OR REPLACE FUNCTION hybrid_recall_schedule_plan_properties(variant text)
RETURNS TABLE(plan_property text, observed int) AS $$
DECLARE
    plan_line text;
    query_sql text;
    extra_qual text := '';
    extra_sort text := '';
    offset_clause text := '';
    operator_nodes int := 0;
    serial_strategies int := 0;
    parallel_strategies int := 0;
    final_topk_lines int := 0;
    text_candidate_lines int := 0;
    vector_candidate_lines int := 0;
    rrf_k_lines int := 0;
    text_weight_lines int := 0;
    vector_weight_lines int := 0;
    zero_worker_lines int := 0;
    two_worker_lines int := 0;
    bm25_scans int := 0;
    ann_scans int := 0;
    hnsw_scans int := 0;
    local_gathers int := 0;
    text_operators int := 0;
    vector_operators int := 0;
    pushed_filters int := 0;
    post_filter_modes int := 0;
    ordinary_sorts int := 0;
    marker_sort_keys int := 0;
    initplans int := 0;
BEGIN
    IF variant = 'sublink' THEN
        extra_qual := ' AND EXISTS (SELECT 1)';
    ELSIF variant = 'offset' THEN
        offset_clause := ' OFFSET 1';
    ELSIF variant = 'two_sort_keys' THEN
        extra_sort := ', id';
    ELSIF variant <> 'base' THEN
        RAISE EXCEPTION 'unknown HybridRecall plan variant: %', variant;
    END IF;

    query_sql := $query$
        SELECT id
        FROM hybrid_recall_schedule_docs
        WHERE tenant_id = 1$query$ || extra_qual || $query$
        ORDER BY ogai.hybrid_rrf_marker(
            id, content, embedding, 'database',
            '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
            '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
        ) DESC$query$ || extra_sort || ' LIMIT 8' || offset_clause;

    FOR plan_line IN EXECUTE 'EXPLAIN (VERBOSE, COSTS OFF) ' || query_sql LOOP
        IF position('Hybrid Recall' IN plan_line) > 0 THEN
            operator_nodes := operator_nodes + 1;
        END IF;
        IF position('Strategy: Serial RRF' IN plan_line) > 0 THEN
            serial_strategies := serial_strategies + 1;
        END IF;
        IF position('Strategy: Parallel RRF' IN plan_line) > 0 THEN
            parallel_strategies := parallel_strategies + 1;
        END IF;
        IF position('Final Top-K: 8' IN plan_line) > 0 THEN
            final_topk_lines := final_topk_lines + 1;
        END IF;
        IF position('Text Candidates: 40' IN plan_line) > 0 THEN
            text_candidate_lines := text_candidate_lines + 1;
        END IF;
        IF position('Vector Candidates: 40' IN plan_line) > 0 THEN
            vector_candidate_lines := vector_candidate_lines + 1;
        END IF;
        IF position('RRF k: 60.000' IN plan_line) > 0 THEN
            rrf_k_lines := rrf_k_lines + 1;
        END IF;
        IF position('Text Weight: 0.500' IN plan_line) > 0 THEN
            text_weight_lines := text_weight_lines + 1;
        END IF;
        IF position('Vector Weight: 0.500' IN plan_line) > 0 THEN
            vector_weight_lines := vector_weight_lines + 1;
        END IF;
        IF position('Planned Branch Workers: 0' IN plan_line) > 0 THEN
            zero_worker_lines := zero_worker_lines + 1;
        END IF;
        IF position('Planned Branch Workers: 2' IN plan_line) > 0 THEN
            two_worker_lines := two_worker_lines + 1;
        END IF;
        IF position('hybrid_recall_schedule_bm25_idx' IN plan_line) > 0 THEN
            bm25_scans := bm25_scans + 1;
        END IF;
        IF position('Ann Index Scan' IN plan_line) > 0 THEN
            ann_scans := ann_scans + 1;
        END IF;
        IF position('hybrid_recall_schedule_hnsw_idx' IN plan_line) > 0 THEN
            hnsw_scans := hnsw_scans + 1;
        END IF;
        IF position('LOCAL GATHER' IN plan_line) > 0 THEN
            local_gathers := local_gathers + 1;
        END IF;
        IF position('Order By:' IN plan_line) > 0 AND position('<&>' IN plan_line) > 0 THEN
            text_operators := text_operators + 1;
        END IF;
        IF position('Order By:' IN plan_line) > 0 AND position('<=>' IN plan_line) > 0 THEN
            vector_operators := vector_operators + 1;
        END IF;
        IF position('Filter:' IN plan_line) > 0 AND position('tenant_id = 1' IN plan_line) > 0 THEN
            pushed_filters := pushed_filters + 1;
        END IF;
        IF position('Filter Mode: Post-Filter' IN plan_line) > 0 THEN
            post_filter_modes := post_filter_modes + 1;
        END IF;
        IF position('->  Sort' IN plan_line) > 0 THEN
            ordinary_sorts := ordinary_sorts + 1;
        END IF;
        IF position('Sort Key:' IN plan_line) > 0 AND
           position('hybrid_rrf_marker' IN plan_line) > 0 THEN
            marker_sort_keys := marker_sort_keys + 1;
        END IF;
        IF position('InitPlan ' IN plan_line) > 0 THEN
            initplans := initplans + 1;
        END IF;
    END LOOP;

    RETURN QUERY
    SELECT properties.name::text, properties.value::int
    FROM (VALUES
        ( 1, 'Hybrid Recall nodes', operator_nodes),
        ( 2, 'Serial RRF strategies', serial_strategies),
        ( 3, 'Parallel RRF strategies', parallel_strategies),
        ( 4, 'Final Top-K metadata', final_topk_lines),
        ( 5, 'Text candidate metadata', text_candidate_lines),
        ( 6, 'Vector candidate metadata', vector_candidate_lines),
        ( 7, 'RRF k metadata', rrf_k_lines),
        ( 8, 'Text weight metadata', text_weight_lines),
        ( 9, 'Vector weight metadata', vector_weight_lines),
        (10, 'Zero branch-worker metadata', zero_worker_lines),
        (11, 'Two branch-worker metadata', two_worker_lines),
        (12, 'BM25 index scans', bm25_scans),
        (13, 'ANN index scans', ann_scans),
        (14, 'HNSW index scans', hnsw_scans),
        (15, 'LOCAL GATHER streams', local_gathers),
        (16, 'BM25 order operators', text_operators),
        (17, 'Vector order operators', vector_operators),
        (18, 'Pushed tenant filters', pushed_filters),
        (19, 'ANN post-filter modes', post_filter_modes),
        (20, 'Ordinary Sort nodes', ordinary_sorts),
        (21, 'Marker Sort Keys', marker_sort_keys),
        (22, 'InitPlans', initplans)
    ) AS properties(ord, name, value)
    ORDER BY properties.ord;
END;
$$ LANGUAGE plpgsql;

-- Rollback switch: no HybridRecall or task streams are planned while disabled.
SET enable_hybrid_recall = off;
SELECT 'rollback-disabled' AS test_case, plan_property, observed
FROM hybrid_recall_schedule_plan_properties('base')
WHERE plan_property IN ('Hybrid Recall nodes', 'Parallel RRF strategies', 'LOCAL GATHER streams');

-- Explicit serial mode: full operator metadata, both branch indexes, filter
-- pushdown, and absence of task streams are all independently watched.
SET enable_hybrid_recall = on;
SET hybrid_recall_parallel_mode = off;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
       ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
       ) AS hybrid_score
FROM hybrid_recall_schedule_docs
WHERE tenant_id = 1
ORDER BY hybrid_score DESC
LIMIT 8;
SELECT 'mode-off-serial' AS test_case, plan_property, observed
FROM hybrid_recall_schedule_plan_properties('base')
WHERE plan_property NOT IN ('Two branch-worker metadata', 'Ordinary Sort nodes',
                            'Marker Sort Keys', 'InitPlans');
SELECT 'mode-off' AS result_case,
       id AS doc_id,
       ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
       ) AS hybrid_score
FROM hybrid_recall_schedule_docs
WHERE tenant_id = 1
ORDER BY hybrid_score DESC
LIMIT 8;

-- Supported forced parallel shape: both branch plans are wrapped by LOCAL
-- GATHER and retain their BM25/HNSW access paths and pushed common filter.
SET hybrid_recall_parallel_mode = force;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
       ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
       ) AS hybrid_score
FROM hybrid_recall_schedule_docs
WHERE tenant_id = 1
ORDER BY hybrid_score DESC
LIMIT 8;
SELECT 'force-supported-parallel' AS test_case, plan_property, observed
FROM hybrid_recall_schedule_plan_properties('base')
WHERE plan_property NOT IN ('Zero branch-worker metadata', 'Ordinary Sort nodes',
                            'Marker Sort Keys', 'InitPlans');
SELECT 'force' AS result_case,
       id AS doc_id,
       ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
       ) AS hybrid_score
FROM hybrid_recall_schedule_docs
WHERE tenant_id = 1
ORDER BY hybrid_score DESC
LIMIT 8;

-- Cost-based mode must be able to choose either implementation.  Extreme but
-- valid planner cost values make the choice independent of host load.
SET hybrid_recall_parallel_mode = auto;
SET cpu_operator_cost = 0.0001;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
       ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
       ) AS hybrid_score
FROM hybrid_recall_schedule_docs
WHERE tenant_id = 1
ORDER BY hybrid_score DESC
LIMIT 8;
SELECT 'auto-cost-serial' AS test_case, plan_property, observed
FROM hybrid_recall_schedule_plan_properties('base')
WHERE plan_property IN ('Hybrid Recall nodes', 'Serial RRF strategies',
                        'Parallel RRF strategies', 'Zero branch-worker metadata',
                        'Two branch-worker metadata', 'LOCAL GATHER streams',
                        'BM25 index scans', 'HNSW index scans');
SELECT 'auto-serial' AS result_case,
       id AS doc_id,
       ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
       ) AS hybrid_score
FROM hybrid_recall_schedule_docs
WHERE tenant_id = 1
ORDER BY hybrid_score DESC
LIMIT 8;

SET cpu_operator_cost = 1.0;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
       ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
       ) AS hybrid_score
FROM hybrid_recall_schedule_docs
WHERE tenant_id = 1
ORDER BY hybrid_score DESC
LIMIT 8;
SELECT 'auto-cost-parallel' AS test_case, plan_property, observed
FROM hybrid_recall_schedule_plan_properties('base')
WHERE plan_property IN ('Hybrid Recall nodes', 'Serial RRF strategies',
                        'Parallel RRF strategies', 'Zero branch-worker metadata',
                        'Two branch-worker metadata', 'LOCAL GATHER streams',
                        'BM25 index scans', 'HNSW index scans');
SELECT 'auto-parallel' AS result_case,
       id AS doc_id,
       ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
       ) AS hybrid_score
FROM hybrid_recall_schedule_docs
WHERE tenant_id = 1
ORDER BY hybrid_score DESC
LIMIT 8;

-- The four result blocks above deliberately expose ordered id + exact fused
-- score.  pg_regress therefore guards serial/parallel semantic equality as
-- well as each mode's fixed golden result.

-- Unsupported parallel boundaries.  Even in force mode these shapes must
-- stay on an ordinary Sort plan; no HybridRecall node or LOCAL GATHER may
-- leak into the plan.
RESET cpu_operator_cost;
SET hybrid_recall_parallel_mode = force;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
       ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
       ) AS hybrid_score
FROM hybrid_recall_schedule_docs
WHERE tenant_id = 1 AND EXISTS (SELECT 1)
ORDER BY hybrid_score DESC
LIMIT 8;
SELECT 'force-reject-sublink' AS test_case, plan_property, observed
FROM hybrid_recall_schedule_plan_properties('sublink')
WHERE plan_property IN ('Hybrid Recall nodes', 'Serial RRF strategies',
                        'Parallel RRF strategies', 'LOCAL GATHER streams',
                        'Ordinary Sort nodes', 'Marker Sort Keys', 'InitPlans');

EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
       ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
       ) AS hybrid_score
FROM hybrid_recall_schedule_docs
WHERE tenant_id = 1
ORDER BY hybrid_score DESC
LIMIT 8 OFFSET 1;
SELECT 'force-reject-offset' AS test_case, plan_property, observed
FROM hybrid_recall_schedule_plan_properties('offset')
WHERE plan_property IN ('Hybrid Recall nodes', 'Serial RRF strategies',
                        'Parallel RRF strategies', 'LOCAL GATHER streams',
                        'Ordinary Sort nodes', 'Marker Sort Keys', 'InitPlans');

EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
       ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
       ) AS hybrid_score
FROM hybrid_recall_schedule_docs
WHERE tenant_id = 1
ORDER BY hybrid_score DESC, id
LIMIT 8;
SELECT 'force-reject-two-sort-keys' AS test_case, plan_property, observed
FROM hybrid_recall_schedule_plan_properties('two_sort_keys')
WHERE plan_property IN ('Hybrid Recall nodes', 'Serial RRF strategies',
                        'Parallel RRF strategies', 'LOCAL GATHER streams',
                        'Ordinary Sort nodes', 'Marker Sort Keys', 'InitPlans');

DROP INDEX hybrid_recall_schedule_hnsw_idx;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
       ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":40,"vector_candidates":40,"rrf_k":60,"weights":[0.5,0.5]}'::jsonb
       ) AS hybrid_score
FROM hybrid_recall_schedule_docs
WHERE tenant_id = 1
ORDER BY hybrid_score DESC
LIMIT 8;
SELECT 'force-reject-missing-hnsw' AS test_case, plan_property, observed
FROM hybrid_recall_schedule_plan_properties('base')
WHERE plan_property IN ('Hybrid Recall nodes', 'Serial RRF strategies',
                        'Parallel RRF strategies', 'LOCAL GATHER streams',
                        'BM25 index scans', 'ANN index scans', 'HNSW index scans',
                        'Ordinary Sort nodes', 'Marker Sort Keys');

RESET hybrid_recall_parallel_mode;
RESET enable_hybrid_recall;
DROP FUNCTION hybrid_recall_schedule_plan_properties(text);
DROP TABLE hybrid_recall_schedule_docs;
