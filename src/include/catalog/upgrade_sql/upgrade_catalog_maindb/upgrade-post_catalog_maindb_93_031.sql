DROP FUNCTION IF EXISTS pg_catalog.raise_application_error(INTEGER, TEXT, BOOL);

CREATE OR REPLACE FUNCTION pg_catalog.raise_application_error(
    IN code INTEGER,
    IN message TEXT,
    IN keep_errors BOOL DEFAULT FALSE
) RETURNS void
AS '$libdir/plpgsql', 'raise_application_error'
LANGUAGE C VOLATILE NOT FENCED;
