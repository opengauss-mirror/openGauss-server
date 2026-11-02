-- BM25 parallel CREATE INDEX (heap parallel scan + parallel reorder workers). No ORDER BY queries (ties unstable).
-- Requires table parallel_workers > 0 and postmaster able to launch background workers.
SET client_min_messages = error;

DROP TABLE IF EXISTS bm25_parallel_build;

CREATE TABLE bm25_parallel_build (
    id int PRIMARY KEY,
    content text
) WITH (parallel_workers = 4);

\pset tuples_only on

-- Reject invalid custom paths even when no rows need tokenization.
CREATE INDEX bm25_relative_dict_idx ON bm25_parallel_build USING bm25(content)
WITH (dict_path = 'relative/bm25_dict');
CREATE INDEX bm25_outside_dict_idx ON bm25_parallel_build USING bm25(content)
WITH (dict_path = '/');
CREATE INDEX bm25_special_dict_idx ON bm25_parallel_build USING bm25(content)
WITH (dict_path = '/bm25;dict');
CREATE INDEX bm25_missing_dict_idx ON bm25_parallel_build USING bm25(content)
WITH (dict_path = '/not/a/real/bm25_dict');

SELECT NOT EXISTS (
    SELECT 1 FROM pg_class
    WHERE relname IN ('bm25_relative_dict_idx', 'bm25_outside_dict_idx',
                      'bm25_special_dict_idx', 'bm25_missing_dict_idx')
) AS invalid_dict_paths_rejected;

-- NULL documents also skip tokenization and must not bypass DDL validation.
INSERT INTO bm25_parallel_build VALUES (0, NULL);
CREATE INDEX bm25_null_dict_idx ON bm25_parallel_build USING bm25(content)
WITH (dict_path = 'relative/bm25_dict');
TRUNCATE bm25_parallel_build;

-- An explicitly empty path selects the default dictionary.
CREATE INDEX bm25_empty_dict_idx ON bm25_parallel_build USING bm25(content)
WITH (dict_path = '');
SELECT reloptions = ARRAY['dict_path='] AS empty_dict_path_persisted
FROM pg_class
WHERE relname = 'bm25_empty_dict_idx';
INSERT INTO bm25_parallel_build VALUES (0, 'empty_default_token');
DROP INDEX bm25_empty_dict_idx;
TRUNCATE bm25_parallel_build;

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
