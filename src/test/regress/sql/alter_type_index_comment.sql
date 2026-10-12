-- ALTER COLUMN TYPE must preserve comments on rebuilt indexes.
\pset format unaligned
\pset null '(null)'
BEGIN;
SET LOCAL client_min_messages = warning;
CREATE TEMP TABLE alter_type_index_comment (
    c0 integer PRIMARY KEY,
    c2 varchar(32) UNIQUE,
    c3 integer
);
INSERT INTO alter_type_index_comment VALUES (1, 'row', 2);
CREATE INDEX alter_type_comment_i0 ON alter_type_index_comment(c2);
CREATE INDEX alter_type_comment_rewrite ON alter_type_index_comment(c3);
CREATE INDEX alter_type_comment_no_comment ON alter_type_index_comment(c3);
COMMENT ON INDEX alter_type_comment_i0 IS 'ordinary ''index'' comment';
COMMENT ON INDEX alter_type_index_comment_pkey IS 'primary index comment';
COMMENT ON INDEX alter_type_index_comment_c2_key IS 'unique index comment';
COMMENT ON INDEX alter_type_comment_rewrite IS 'rewrite index comment';
-- varchar to text can reuse the index storage.
ALTER TABLE alter_type_index_comment ALTER COLUMN c2 TYPE text;
-- integer to bigint requires rebuilding the table and index storage.
ALTER TABLE alter_type_index_comment ALTER COLUMN c0 TYPE bigint;
ALTER TABLE alter_type_index_comment ALTER COLUMN c3 TYPE bigint;
SELECT relname AS index_name, obj_description(oid, 'pg_class') AS comment
FROM pg_class
WHERE relnamespace = pg_my_temp_schema()
  AND relname IN ('alter_type_comment_i0', 'alter_type_index_comment_pkey',
                  'alter_type_index_comment_c2_key', 'alter_type_comment_rewrite',
                  'alter_type_comment_no_comment')
ORDER BY relname;
SELECT c0, c2, c3 FROM alter_type_index_comment;
ROLLBACK;
\pset format aligned
\pset null ''
