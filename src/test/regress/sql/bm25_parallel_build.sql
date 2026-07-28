-- BM25 parallel CREATE INDEX (heap parallel scan + parallel reorder workers). No ORDER BY queries (ties unstable).
-- Requires table parallel_workers > 0 and postmaster able to launch background workers.
SET client_min_messages = error;

DROP TABLE IF EXISTS bm25_parallel_build;

CREATE TABLE bm25_parallel_build (
    id int PRIMARY KEY,
    content text
) WITH (parallel_workers = 4);

CREATE INDEX bm25_empty_explicit_dict_idx ON bm25_parallel_build USING bm25(content)
WITH (dict_path = '/not/a/real/bm25_dict');

\pset tuples_only on
SELECT reloptions = ARRAY['dict_path=/not/a/real/bm25_dict'] AS explicit_dict_path_persisted
FROM pg_class
WHERE relname = 'bm25_empty_explicit_dict_idx';

DROP INDEX bm25_empty_explicit_dict_idx;

CREATE INDEX bm25_bare_default_idx ON bm25_parallel_build USING bm25(content)
WITH (dict_path = DEFAULT);

SELECT reloptions = ARRAY['dict_path=default'] AS bare_default_normalized
FROM pg_class
WHERE relname = 'bm25_bare_default_idx';

INSERT INTO bm25_parallel_build VALUES (0, 'bare_default_token');
DROP INDEX bm25_bare_default_idx;
TRUNCATE bm25_parallel_build;

INSERT INTO bm25_parallel_build(id, content)
SELECT g, ('tok' || (g % 80))::text
FROM generate_series(1, 8000) g;

CREATE INDEX bm25_parallel_build_idx ON bm25_parallel_build USING bm25(content);

SELECT EXISTS (
    SELECT 1 FROM pg_class
    WHERE relname = 'bm25_parallel_build_idx'
      AND reloptions = ARRAY['dict_path=DEFAULT']
) AS default_dict_path_persisted;
\pset tuples_only off

DROP TABLE bm25_parallel_build;
