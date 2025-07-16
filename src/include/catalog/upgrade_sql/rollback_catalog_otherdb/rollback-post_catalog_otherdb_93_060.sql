DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_ops USING btree CASCADE;
DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_ops USING btree CASCADE;
DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_ops USING ubtree CASCADE;
DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_ops USING ubtree CASCADE;

DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'halfvec' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.l2_distance(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.inner_product(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.cosine_distance(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.l1_distance(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.l2_norm(halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.l2_normalize(halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_lt(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_le(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_eq(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_ne(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_ge(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_gt(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_cmp(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_l2_squared_distance(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_negative_inner_product(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec(halfvec, int4, boolean) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_to_vector(halfvec, int4, boolean) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<->(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<#>(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<=>(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<+>(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<=(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.=(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<>(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.>=(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.>(halfvec, halfvec) CASCADE;
    end if;
END;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_in(cstring, oid, int4) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.halfvec_typmod_in(_cstring) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.halfvec_recv(internal, oid, int4) CASCADE;
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'halfvec' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_out(pg_catalog.halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_send(pg_catalog.halfvec) CASCADE;
    end if;
END;
DROP TYPE IF EXISTS pg_catalog.halfvec CASCADE;
DROP TYPE IF EXISTS pg_catalog._halfvec CASCADE;