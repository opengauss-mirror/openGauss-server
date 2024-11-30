DROP OPERATOR FAMILY IF EXISTS vector_ops USING btree CASCADE; 
DROP OPERATOR CLASS IF EXISTS vector_ops USING btree CASCADE;
DROP OPERATOR FAMILY IF EXISTS vector_ubt_ops USING ubtree CASCADE;
DROP OPERATOR CLASS IF EXISTS vector_ubt_ops USING ubtree CASCADE;
DROP OPERATOR FAMILY IF EXISTS sparsevec_ops USING btree CASCADE; 
DROP OPERATOR CLASS IF EXISTS sparsevec_ops USING btree CASCADE; 
DROP OPERATOR FAMILY IF EXISTS sparsevec_ubt_ops USING ubtree CASCADE; 
DROP OPERATOR CLASS IF EXISTS sparsevec_ubt_ops USING ubtree CASCADE; 

DO $$
DECLARE
    cnt int;
BEGIN
    select count(*) into cnt from pg_am where amname = 'ivfflat';
    if cnt = 1 then
        DROP OPERATOR FAMILY IF EXISTS vector_l2_ops USING ivfflat CASCADE; 
        DROP OPERATOR CLASS IF EXISTS vector_l2_ops USING ivfflat CASCADE; 
        DROP OPERATOR FAMILY IF EXISTS vector_ip_ops USING ivfflat CASCADE; 
        DROP OPERATOR CLASS IF EXISTS vector_ip_ops USING ivfflat CASCADE; 
        DROP OPERATOR FAMILY IF EXISTS vector_cosine_ops USING ivfflat CASCADE; 
        DROP OPERATOR CLASS IF EXISTS vector_cosine_ops USING ivfflat CASCADE;  
        DROP OPERATOR FAMILY IF EXISTS bit_hamming_ops USING ivfflat CASCADE; 
        DROP OPERATOR CLASS IF EXISTS bit_hamming_ops USING ivfflat CASCADE; 
    end if;
END$$;

DO $$
DECLARE
    cnt int;
BEGIN
    select count(*) into cnt from pg_am where amname = 'hnsw';
    if cnt = 1 then
        DROP OPERATOR FAMILY IF EXISTS vector_l2_ops USING hnsw CASCADE;
        DROP OPERATOR CLASS IF EXISTS vector_l2_ops USING hnsw CASCADE; 
        DROP OPERATOR FAMILY IF EXISTS vector_ip_ops USING hnsw CASCADE;
        DROP OPERATOR CLASS IF EXISTS vector_ip_ops USING hnsw CASCADE;
        DROP OPERATOR FAMILY IF EXISTS vector_cosine_ops USING hnsw CASCADE;
        DROP OPERATOR CLASS IF EXISTS vector_cosine_ops USING hnsw CASCADE;
        DROP OPERATOR FAMILY IF EXISTS vector_l1_ops USING hnsw CASCADE; 
        DROP OPERATOR CLASS IF EXISTS vector_l1_ops USING hnsw CASCADE;
        DROP OPERATOR FAMILY IF EXISTS bit_hamming_ops USING hnsw CASCADE; 
        DROP OPERATOR CLASS IF EXISTS bit_hamming_ops USING hnsw CASCADE; 
        DROP OPERATOR FAMILY IF EXISTS bit_jaccard_ops USING hnsw CASCADE;
        DROP OPERATOR CLASS IF EXISTS bit_jaccard_ops USING hnsw CASCADE;
        DROP OPERATOR FAMILY IF EXISTS sparsevec_l2_ops USING hnsw CASCADE; 
        DROP OPERATOR CLASS IF EXISTS sparsevec_l2_ops USING hnsw CASCADE; 
        DROP OPERATOR FAMILY IF EXISTS sparsevec_ip_ops USING hnsw CASCADE; 
        DROP OPERATOR CLASS IF EXISTS sparsevec_ip_ops USING hnsw CASCADE; 
        DROP OPERATOR FAMILY IF EXISTS sparsevec_cosine_ops USING hnsw CASCADE; 
        DROP OPERATOR CLASS IF EXISTS sparsevec_cosine_ops USING hnsw CASCADE; 
        DROP OPERATOR FAMILY IF EXISTS sparsevec_l1_ops USING hnsw CASCADE;
        DROP OPERATOR CLASS IF EXISTS sparsevec_l1_ops USING hnsw CASCADE;
    end if;
END$$;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'sparsevec' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.l2_distance(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.inner_product(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.cosine_distance(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.l1_distance(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.l2_norm(sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.l2_normalize(sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.sparsevec_lt(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.sparsevec_le(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.sparsevec_eq(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.sparsevec_ne(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.sparsevec_ge(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.sparsevec_gt(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.sparsevec_cmp(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.sparsevec_l2_squared_distance(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.sparsevec_negative_inner_product(sparsevec, sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.sparsevec(sparsevec, int4, boolean) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.sparsevec_to_vector(sparsevec, int4, boolean) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<->(sparsevec, sparsevec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<#>(sparsevec, sparsevec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<=>(sparsevec, sparsevec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<+>(sparsevec, sparsevec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<(sparsevec, sparsevec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<=(sparsevec, sparsevec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.=(sparsevec, sparsevec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<>(sparsevec, sparsevec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.>=(sparsevec, sparsevec) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.>(sparsevec, sparsevec) CASCADE;
    end if;
END$$;

DROP FUNCTION IF EXISTS sparsevec_in(cstring, oid, int4) CASCADE;
DROP FUNCTION IF EXISTS sparsevec_typmod_in(_cstring) CASCADE;
DROP FUNCTION IF EXISTS sparsevec_recv(internal, oid, int4) CASCADE;
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'sparsevec' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS sparsevec_out(pg_catalog.sparsevec) CASCADE;
        DROP FUNCTION IF EXISTS sparsevec_send(pg_catalog.sparsevec) CASCADE;
    end if;
END$$;
DROP TYPE IF EXISTS pg_catalog.sparsevec CASCADE;
DROP TYPE IF EXISTS pg_catalog._sparsevec CASCADE;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'vector' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.inner_product(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.cosine_distance(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.l1_distance(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.l2_distance(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_dims(vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_norm(vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.l2_normalize(vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.binary_quantize(vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.subvector(vector, int4, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_add(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_sub(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_mul(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_concat(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_lt(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_le(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_eq(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_ne(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_ge(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_gt(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_cmp(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_l2_squared_distance(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_negative_inner_product(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_spherical_distance(vector, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_accum(_float8, vector) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector(vector, int4, boolean) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_to_float4(vector, int4, boolean) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.vector_to_sparsevec(vector, int4, boolean) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<->(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<#>(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<=>(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<+>(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.+(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.-(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.*(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.||(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<=(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.=(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.<>(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.>=(vector, vector) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.>(vector, vector) CASCADE;
        drop aggregate if exists pg_catalog.avg(vector) CASCADE;
        drop aggregate if exists pg_catalog.sum(vector) CASCADE;
    end if;
END$$;

DROP FUNCTION IF EXISTS vector_in(cstring, oid, int4) CASCADE;
DROP FUNCTION IF EXISTS vector_typmod_in(_cstring) CASCADE;
DROP FUNCTION IF EXISTS vector_recv(internal, oid, int4) CASCADE;
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'vector' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS vector_out(pg_catalog.vector) CASCADE;
        DROP FUNCTION IF EXISTS vector_send(pg_catalog.vector) CASCADE;
    end if;
END$$;
DROP TYPE IF EXISTS pg_catalog.vector CASCADE;
DROP TYPE IF EXISTS pg_catalog._vector CASCADE;

DROP OPERATOR IF EXISTS pg_catalog.<~>(bit, bit) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<%>(bit, bit) CASCADE;
DROP ACCESS METHOD IF EXISTS ivfflat CASCADE;
DROP ACCESS METHOD IF EXISTS hnsw CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.vector_avg(_float8) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.vector_combine(_float8, _float8) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_to_vector(_int4, int4, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_to_vector(_float4, int4, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_to_vector(_float8, int4, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_to_vector(_numeric, int4, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflatbuild(internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflatbuildempty(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflatinsert(internal, internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflatbulkdelete(internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflatvacuumcleanup(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflatcostestimate(internal, internal, internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflatoptions(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflatvalidate(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflatbeginscan(internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflatrescan(internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflatgettuple(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflatendscan(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflathandler(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ivfflat_bit_support(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswbuild(internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswbuildempty(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswinsert(internal, internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswbulkdelete(internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswvacuumcleanup(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswcostestimate(internal, internal, internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswoptions(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswvalidate(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswbeginscan(internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswrescan(internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswdelete(internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswgettuple(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswendscan(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnswhandler(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnsw_bit_support(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hnsw_sparsevec_support(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.hamming_distance(bit, bit) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.jaccard_distance(bit, bit) CASCADE;

