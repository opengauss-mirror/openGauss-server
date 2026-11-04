DROP FUNCTION IF EXISTS pg_catalog.array_reverse(anyarray) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9974;
CREATE FUNCTION pg_catalog.array_reverse(
    "array" anyarray
) RETURNS anyarray LANGUAGE INTERNAL IMMUTABLE STRICT AS 'array_reverse';
COMMENT ON FUNCTION pg_catalog.array_reverse(anyarray) IS 'reverse array';

DROP FUNCTION IF EXISTS pg_catalog.array_sort(anyarray) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9975;
CREATE FUNCTION pg_catalog.array_sort(
    "array" anyarray
) RETURNS anyarray LANGUAGE INTERNAL IMMUTABLE STRICT AS 'array_sort' ;
COMMENT ON FUNCTION pg_catalog.array_sort(anyarray) IS 'sort array';

DROP FUNCTION IF EXISTS pg_catalog.array_sort(anyarray, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9976;
CREATE FUNCTION pg_catalog.array_sort(
    "array" anyarray, descending boolean
) RETURNS anyarray AS 'array_sort_order' LANGUAGE INTERNAL IMMUTABLE STRICT;
COMMENT ON FUNCTION pg_catalog.array_sort(anyarray, boolean) IS 'sort array';

DROP FUNCTION IF EXISTS pg_catalog.array_sort(anyarray, boolean, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9977;
CREATE FUNCTION pg_catalog.array_sort(
    "array" anyarray, descending boolean, nulls_first boolean
) RETURNS anyarray AS 'array_sort_order_nulls_first' LANGUAGE INTERNAL IMMUTABLE STRICT;
COMMENT ON FUNCTION pg_catalog.array_sort(anyarray, boolean, boolean) IS 'sort array';

