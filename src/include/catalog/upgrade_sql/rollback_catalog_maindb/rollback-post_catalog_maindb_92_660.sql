DROP FUNCTION IF EXISTS pg_catalog.regexp_count(text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_count(text, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_count(text, text, int, text) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text, int, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text, int, int, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text, int, int, int, text) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.regexp_replace(text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_replace(text, text, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_replace(text, text, text, int, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_replace(text, text, text, int, int, text) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.regexp_substr(text, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_substr(text, text, int, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_substr(text, text, int, int, text) CASCADE;
