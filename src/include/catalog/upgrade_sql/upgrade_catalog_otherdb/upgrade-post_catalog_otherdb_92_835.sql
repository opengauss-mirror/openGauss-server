DROP FUNCTION IF EXISTS pg_catalog.boolum(bool) CASCADE;
set LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 83;
CREATE FUNCTION pg_catalog.boolum(bool) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'boolum';

DROP OPERATOR IF EXISTS pg_catalog.-(none, bool) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6565;
CREATE OPERATOR pg_catalog.-(
  RIGHTARG = bool,
  PROCEDURE = boolum
);
