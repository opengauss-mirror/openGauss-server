-- Retaining an invalid view must preserve its triggers without bypassing other dependencies.
\set VERBOSITY terse
SET client_min_messages = warning;
CREATE SCHEMA drop_column_view_trigger;
SET current_schema = drop_column_view_trigger;
CREATE TABLE t0(c0 int, c1 int);
INSERT INTO t0 VALUES (1, 2);
CREATE VIEW v0 AS SELECT c0, c1 FROM t0;
CREATE TABLE trigger_log(fired boolean);
CREATE FUNCTION f0() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO trigger_log VALUES (true);
    RETURN NEW;
END;
$$;
CREATE TRIGGER tr0 INSTEAD OF UPDATE ON v0 FOR EACH ROW EXECUTE PROCEDURE f0();

-- The column is dropped, but the invalid view retains its trigger.
ALTER TABLE t0 DROP COLUMN c0;
-- The trigger still depends on its function, even while its view is invalid.
DROP FUNCTION f0();
SELECT tgname FROM pg_trigger WHERE tgrelid = 'v0'::regclass AND NOT tgisinternal;
UPDATE v0 SET c1 = 3 WHERE c0 = 1;
SELECT count(*) AS fired_count FROM trigger_log;

-- Restoring the column makes the view and the existing trigger usable again.
ALTER TABLE t0 ADD COLUMN c0 int;
UPDATE t0 SET c0 = 1;
SELECT * FROM v0;
UPDATE v0 SET c1 = 3 WHERE c0 = 1;
SELECT count(*) AS fired_count FROM trigger_log;

-- A direct function dependency still removes the trigger with CASCADE, leaving the view intact.
BEGIN;
DROP FUNCTION f0() CASCADE;
SELECT tgname FROM pg_trigger WHERE tgrelid = 'v0'::regclass AND NOT tgisinternal;
SELECT * FROM v0;
ROLLBACK;

-- An explicit trigger drop is unaffected by view retention.
DROP TRIGGER tr0 ON v0;
SELECT count(*) AS remaining_triggers FROM pg_trigger WHERE tgfoid = 'f0'::regproc;
CREATE TRIGGER tr0 INSTEAD OF UPDATE ON v0 FOR EACH ROW EXECUTE PROCEDURE f0();

-- Removing the dependent view allows an ordinary column drop.
DROP VIEW v0;
SELECT count(*) AS remaining_triggers FROM pg_trigger WHERE tgfoid = 'f0'::regproc;
ALTER TABLE t0 DROP COLUMN c0;
SELECT * FROM t0;

-- Recreate the dependency and verify CASCADE removes the view and its trigger.
ALTER TABLE t0 ADD COLUMN c0 int;
CREATE VIEW v0 AS SELECT c0, c1 FROM t0;
CREATE TRIGGER tr0 INSTEAD OF UPDATE ON v0 FOR EACH ROW EXECUTE PROCEDURE f0();
ALTER TABLE t0 DROP COLUMN c0 CASCADE;
SELECT count(*) AS remaining_views FROM pg_class
    WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'drop_column_view_trigger')
        AND relname = 'v0';
SELECT count(*) AS remaining_triggers FROM pg_trigger WHERE tgfoid = 'f0'::regproc;
SELECT * FROM t0;

RESET current_schema;
DROP SCHEMA drop_column_view_trigger CASCADE;
RESET client_min_messages;
\set VERBOSITY default
