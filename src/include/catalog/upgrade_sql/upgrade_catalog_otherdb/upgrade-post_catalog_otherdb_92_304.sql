DROP FUNCTION IF EXISTS pg_catalog.array_remove(anyarray, anyelement) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6555;
CREATE FUNCTION pg_catalog.array_remove (
  anyarray, anyelement
) RETURNS anyarray LANGUAGE INTERNAL IMMUTABLE as 'array_remove';

DROP FUNCTION IF EXISTS pg_catalog.array_replace(anyarray, anyelement, anyelement) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6556;
CREATE FUNCTION pg_catalog.array_replace (
  anyarray, anyelement, anyelement
) RETURNS anyarray LANGUAGE INTERNAL IMMUTABLE as 'array_replace';


DROP FUNCTION IF EXISTS pg_catalog.first_transition(anyelement, anyelement) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6558;
CREATE FUNCTION pg_catalog.first_transition (
anyelement, anyelement
) RETURNS anyelement LANGUAGE INTERNAL IMMUTABLE STRICT as 'first_transition';

DROP FUNCTION IF EXISTS pg_catalog.last_transition(anyelement, anyelement) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6559;
CREATE FUNCTION pg_catalog.last_transition (
anyelement, anyelement
) RETURNS anyelement LANGUAGE INTERNAL IMMUTABLE STRICT as 'last_transition';

DROP aggregate IF EXISTS pg_catalog.first(anyelement) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6560;
create aggregate first(anyelement) (
    sfunc = first_transition,
    stype = anyelement
);

DROP aggregate IF EXISTS pg_catalog.last(anyelement) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6561;
create aggregate last(anyelement) (
    sfunc = last_transition,
    stype = anyelement
);