DROP FUNCTION IF EXISTS pg_catalog.network_larger(inet, inet) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6666;
CREATE FUNCTION pg_catalog.network_larger (
inet, inet
) RETURNS inet LANGUAGE INTERNAL STRICT as 'network_larger';

DROP FUNCTION IF EXISTS pg_catalog.network_smaller(inet, inet) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6667;
CREATE FUNCTION pg_catalog.network_smaller (
inet, inet
) RETURNS inet LANGUAGE INTERNAL STRICT as 'network_smaller';

DROP aggregate IF EXISTS pg_catalog.max(inet) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6668;
create aggregate max(inet) (
    sfunc = network_larger,
    stype = inet
);

DROP aggregate IF EXISTS pg_catalog.min(inet) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6669;
create aggregate min(inet) (
    sfunc = network_smaller,
    stype = inet
);