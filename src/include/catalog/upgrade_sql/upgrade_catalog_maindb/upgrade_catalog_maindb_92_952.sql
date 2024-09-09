CREATE OR REPLACE FUNCTION information_schema._pg_interval_type(typid oid, mod int4) RETURNS text
    LANGUAGE sql
    IMMUTABLE
    NOT FENCED
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT
  CASE WHEN $1 IN (1186) /* interval */
           THEN pg_catalog.upper(pg_catalog.substring(pg_catalog.format_type($1, $2), 'interval[()0-9]* #"%#"', '#'))
       ELSE null::text
  END$$;