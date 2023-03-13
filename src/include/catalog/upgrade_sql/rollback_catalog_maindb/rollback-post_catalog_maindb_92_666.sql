DROP AGGREGATE IF EXISTS pg_catalog.group_concat(text, "any") CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.group_concat_transfn(internal, text, VARIADIC "any") CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.group_concat_finalfn(internal) CASCADE;
