DO $$
DECLARE
has_hash16 int;
has_5820 int;
BEGIN
    select count(*) from pg_type where typname = 'hash16' limit 1 into has_hash16;
    select count(*) from pg_proc where oid = 5820 limit 1 into has_5820;
    if has_hash16 = 1 and has_5820 = 0 then
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5820;
        CREATE OR REPLACE FUNCTION pg_catalog.hash16_eq(hash16, hash16) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE as 'hash16_eq';
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
    end if;
END$$;

DO $$
DECLARE
has_hash16 int;
has_5819 int;
BEGIN
    select count(*) from pg_type where typname = 'hash16' limit 1 into has_hash16;
    select count(*) from pg_proc where oid = 5819 limit 1 into has_5819;
    if has_hash16 = 1 and has_5819 = 0 then
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5819;
        CREATE OR REPLACE FUNCTION pg_catalog.hash16_add(hash16, hash16) RETURNS hash16 LANGUAGE INTERNAL IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE as 'hash16_add';
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
    end if;
END$$;