-- PR 9695 / DTS27: lock an ordinary table while coercing its row type.
CREATE SCHEMA rowtype_lock_backport;
SET current_schema = rowtype_lock_backport;
CREATE TABLE locked_row(id integer, label varchar(20));
BEGIN;
SELECT '(7,stable)'::locked_row AS row_value;
SELECT EXISTS (
    SELECT 1 FROM pg_locks
    WHERE relation = 'rowtype_lock_backport.locked_row'::regclass
      AND mode = 'AccessShareLock' AND granted AND pid = pg_backend_pid()
) AS row_type_locked;
ROLLBACK;
-- System and standalone composite types must retain normal coercion behavior.
SELECT '42'::integer AS system_type_value;
CREATE TYPE standalone_pair AS (id integer, label text);
SELECT '(9,standalone)'::standalone_pair AS standalone_value;
DROP TYPE standalone_pair;
DROP TABLE locked_row;
RESET current_schema;
DROP SCHEMA rowtype_lock_backport;
