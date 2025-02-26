DROP FUNCTION IF EXISTS pg_catalog.jsonb_insert(jsonb, text[], jsonb, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5610;
CREATE FUNCTION pg_catalog.jsonb_insert (
jsonb, text[], jsonb, boolean DEFAULT false
) RETURNS jsonb LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_insert';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_set(jsonb, text[], jsonb, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5611;
CREATE FUNCTION pg_catalog.jsonb_set(
jsonb, text[], jsonb, boolean DEFAULT false
) RETURNS jsonb LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_set';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_delete(jsonb, int) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5612;
CREATE FUNCTION pg_catalog.jsonb_delete (
jsonb, int
) RETURNS jsonb LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_delete_idx';
COMMENT ON FUNCTION PG_CATALOG.jsonb_delete(jsonb, int) is 'implementation of - operator';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_delete(jsonb, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5613;
CREATE FUNCTION pg_catalog.jsonb_delete (
jsonb, text
) RETURNS jsonb LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_delete';
COMMENT ON FUNCTION PG_CATALOG.jsonb_delete(jsonb, text) is 'implementation of - operator';

DROP FUNCTION IF EXISTS pg_catalog.jsonb_delete(jsonb, text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5614;
CREATE FUNCTION pg_catalog.jsonb_delete (
jsonb, text[]
) RETURNS jsonb LANGUAGE INTERNAL IMMUTABLE STRICT as 'jsonb_delete_array';
COMMENT ON FUNCTION PG_CATALOG.jsonb_delete(jsonb, text[]) is 'implementation of - operator';

DROP OPERATOR IF EXISTS pg_catalog.-(jsonb, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3252;
CREATE OPERATOR pg_catalog.-(LEFTARG = jsonb, RIGHTARG = text, PROCEDURE = jsonb_delete);
COMMENT ON OPERATOR pg_catalog.->(jsonb, text) IS 'delete text';
DROP OPERATOR IF EXISTS pg_catalog.-(jsonb, int) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3251;
CREATE OPERATOR pg_catalog.-(LEFTARG = jsonb, RIGHTARG = int, PROCEDURE = jsonb_delete);
COMMENT ON OPERATOR pg_catalog.-(jsonb, int) IS 'delete index';
DROP OPERATOR IF EXISTS pg_catalog.-(jsonb, text[]) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3253;
CREATE OPERATOR pg_catalog.-(LEFTARG = jsonb, RIGHTARG = text[], PROCEDURE = jsonb_delete);
COMMENT ON OPERATOR pg_catalog.-(jsonb, text[]) IS 'delete text[]';