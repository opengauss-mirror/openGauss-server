DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_ops USING btree CASCADE;
DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_ops USING btree CASCADE;
DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_ops USING ubtree CASCADE;
DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_ops USING ubtree CASCADE;

DECLARE
    cnt int;
BEGIN
    select count(*) into cnt from pg_am where amname = 'ivfflat';
    if cnt = 1 then
        DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_l2_ops USING ivfflat CASCADE;
        DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_l2_ops USING ivfflat CASCADE;
        DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_ip_ops USING ivfflat CASCADE;
        DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_ip_ops USING ivfflat CASCADE;
        DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_cosine_ops USING ivfflat CASCADE;
        DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_cosine_ops USING ivfflat CASCADE;
    end if;
END;

DECLARE
    cnt int;
BEGIN
    select count(*) into cnt from pg_am where amname = 'hnsw';
    if cnt = 1 then
        DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_l2_ops USING hnsw CASCADE;
        DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_l2_ops USING hnsw CASCADE;
        DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_ip_ops USING hnsw CASCADE;
        DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_ip_ops USING hnsw CASCADE;
        DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_cosine_ops USING hnsw CASCADE;
        DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_cosine_ops USING hnsw CASCADE;
        DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_l1_ops USING hnsw CASCADE;
        DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_l1_ops USING hnsw CASCADE;
    end if;
END;

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
        DROP FUNCTION IF EXISTS pg_catalog.vector_dims(halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.binary_quantize(halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.subvector(halfvec, int4, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_add(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_sub(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_mul(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_concat(halfvec, halfvec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_accum(_float8, halfvec) CASCADE;
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
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_to_float4(halfvec, int4, boolean) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_to_halfvec(vector, int4, boolean) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.halfvec_to_vector(halfvec, int4, boolean) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<->(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<#>(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<=>(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<+>(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.+(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.-(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.*(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.||(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<=(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.=(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<>(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.>=(halfvec, halfvec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.>(halfvec, halfvec) CASCADE;
        drop aggregate if exists pg_catalog.avg(halfvec) CASCADE;
        drop aggregate if exists pg_catalog.sum(halfvec) CASCADE;
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

DROP FUNCTION IF EXISTS pg_catalog.halfvec_avg(_float8) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_to_halfvec(_int4, int4, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_to_halfvec(_float4, int4, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_to_halfvec(_float8, int4, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_to_halfvec(_numeric, int4, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnsw_halfvec_support(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflat_halfvec_support(internal, internal) CASCADE;