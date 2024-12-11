DROP TYPE IF EXISTS pg_catalog.vector CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_TYPE, 8305, 8308, b;
CREATE TYPE vector;

DROP FUNCTION IF EXISTS pg_catalog.vector_in(cstring, oid, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8423;
CREATE FUNCTION pg_catalog.vector_in(cstring, oid, int4)
RETURNS vector
AS 'vector_in'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_out(vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8424;
CREATE FUNCTION pg_catalog.vector_out(vector)
RETURNS cstring
AS 'vector_out'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_typmod_in(_cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8425;
CREATE FUNCTION pg_catalog.vector_typmod_in(_cstring)
RETURNS int4
AS 'vector_typmod_in'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_recv(internal, oid, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8426;
CREATE FUNCTION pg_catalog.vector_recv(internal, oid, int4)
RETURNS vector
AS 'vector_recv'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_send(vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8427;
CREATE FUNCTION pg_catalog.vector_send(vector)
RETURNS bytea
AS 'vector_send'
LANGUAGE INTERNAL
STABLE STRICT;

CREATE TYPE vector (
	INPUT     = vector_in,
	OUTPUT    = vector_out,
	TYPMOD_IN = vector_typmod_in,
	RECEIVE   = vector_recv,
	SEND      = vector_send,
	STORAGE   = external
);

DROP FUNCTION IF EXISTS pg_catalog.l2_distance(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8433;
CREATE FUNCTION pg_catalog.l2_distance(vector, vector)
RETURNS float8
AS 'l2_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.inner_product(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8437;
CREATE FUNCTION pg_catalog.inner_product(vector, vector)
RETURNS float8
AS 'inner_product'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.cosine_distance(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8435;
CREATE FUNCTION pg_catalog.cosine_distance(vector, vector)
RETURNS float8
AS 'cosine_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.l1_distance(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8436;
CREATE FUNCTION pg_catalog.l1_distance(vector, vector)
RETURNS float8
AS 'l1_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_dims(vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8428;
CREATE FUNCTION pg_catalog.vector_dims(vector)
RETURNS int4
AS 'vector_dims'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_norm(vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8438;
CREATE FUNCTION pg_catalog.vector_norm(vector)
RETURNS float8
AS 'vector_norm'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.l2_normalize(vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8200;
CREATE FUNCTION pg_catalog.l2_normalize(vector)
RETURNS vector
AS 'l2_normalize'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.binary_quantize(vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8201;
CREATE FUNCTION pg_catalog.binary_quantize(vector)
RETURNS varbit
AS 'binary_quantize'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.subvector(vector, int, int) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8202;
CREATE FUNCTION pg_catalog.subvector(vector, int, int)
RETURNS vector
AS 'subvector'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_add(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8439;
CREATE FUNCTION pg_catalog.vector_add(vector, vector)
RETURNS vector
AS 'vector_add'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_sub(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8440;
CREATE FUNCTION pg_catalog.vector_sub(vector, vector)
RETURNS vector
AS 'vector_sub'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_mul(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8203;
CREATE FUNCTION pg_catalog.vector_mul(vector, vector)
RETURNS vector
AS 'vector_mul'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_concat(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8204;
CREATE FUNCTION pg_catalog.vector_concat(vector, vector)
RETURNS vector
AS 'vector_concat'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_lt(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8441;
CREATE FUNCTION pg_catalog.vector_lt(vector, vector)
RETURNS bool
AS 'vector_lt'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_le(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8442;
CREATE FUNCTION pg_catalog.vector_le(vector, vector)
RETURNS bool
AS 'vector_le'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_eq(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8443;
CREATE FUNCTION pg_catalog.vector_eq(vector, vector)
RETURNS bool
AS 'vector_eq'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_ne(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8444;
CREATE FUNCTION pg_catalog.vector_ne(vector, vector)
RETURNS bool
AS 'vector_ne'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_ge(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8445;
CREATE FUNCTION pg_catalog.vector_ge(vector, vector)
RETURNS bool
AS 'vector_ge'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_gt(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8446;
CREATE FUNCTION pg_catalog.vector_gt(vector, vector)
RETURNS bool
AS 'vector_gt'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_cmp(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8450;
CREATE FUNCTION pg_catalog.vector_cmp(vector, vector)
RETURNS int4
AS 'vector_cmp'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_l2_squared_distance(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8431;
CREATE FUNCTION pg_catalog.vector_l2_squared_distance(vector, vector)
RETURNS float8
AS 'vector_l2_squared_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_negative_inner_product(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8434;
CREATE FUNCTION pg_catalog.vector_negative_inner_product(vector, vector)
RETURNS float8
AS 'vector_negative_inner_product'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_spherical_distance(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8432;
CREATE FUNCTION pg_catalog.vector_spherical_distance(vector, vector)
RETURNS float8
AS 'vector_spherical_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_accum(_float8, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8447;
CREATE FUNCTION pg_catalog.vector_accum(_float8, vector)
RETURNS _float8
AS 'vector_accum'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_avg(_float8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8449;
CREATE FUNCTION pg_catalog.vector_avg(_float8)
RETURNS vector
AS 'vector_avg'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_combine(_float8, _float8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8448;
CREATE FUNCTION pg_catalog.vector_combine(_float8, _float8)
RETURNS _float8
AS 'vector_combine'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector(vector, int4, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8214;
CREATE FUNCTION pg_catalog.vector(vector, int4, boolean)
RETURNS vector
AS 'vector'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.array_to_vector(_int4, int4, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8215;
CREATE FUNCTION pg_catalog.array_to_vector(_int4, int4, boolean)
RETURNS vector
AS 'array_to_vector'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.array_to_vector(_float4, int4, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8216;
CREATE FUNCTION pg_catalog.array_to_vector(_float4, int4, boolean)
RETURNS vector
AS 'array_to_vector'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.array_to_vector(_float8, int4, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8217;
CREATE FUNCTION pg_catalog.array_to_vector(_float8, int4, boolean)
RETURNS vector
AS 'array_to_vector'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.array_to_vector(_numeric, int4, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8218;
CREATE FUNCTION pg_catalog.array_to_vector(_numeric, int4, boolean)
RETURNS vector
AS 'array_to_vector'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_to_float4(vector, int4, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8219;
CREATE FUNCTION pg_catalog.vector_to_float4(vector, int4, boolean)
RETURNS _float4
AS 'vector_to_float4'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_to_int4(vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8212;
CREATE FUNCTION pg_catalog.vector_to_int4(vector)
RETURNS _int4
AS 'vector_to_int4'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_to_float8(vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8213;
CREATE FUNCTION pg_catalog.vector_to_float8(vector)
RETURNS _float8
AS 'vector_to_float8'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_to_numeric(vector, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8221;
CREATE FUNCTION pg_catalog.vector_to_numeric(vector, int4)
RETURNS _numeric
AS 'vector_to_numeric'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflatbuild(internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8417;
CREATE FUNCTION pg_catalog.ivfflatbuild(internal, internal, internal)
RETURNS internal
AS 'ivfflatbuild'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflatbuildempty(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8418;
CREATE FUNCTION pg_catalog.ivfflatbuildempty(internal)
RETURNS void
AS 'ivfflatbuildempty'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflatinsert(internal, internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8412;
CREATE FUNCTION pg_catalog.ivfflatinsert(internal, internal, internal, internal, internal, internal)
RETURNS boolean
AS 'ivfflatinsert'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflatbulkdelete(internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8419;
CREATE FUNCTION pg_catalog.ivfflatbulkdelete(internal, internal, internal, internal)
RETURNS internal
AS 'ivfflatbulkdelete'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflatvacuumcleanup(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8420;
CREATE FUNCTION pg_catalog.ivfflatvacuumcleanup(internal, internal)
RETURNS internal
AS 'ivfflatvacuumcleanup'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflatcostestimate(internal, internal, internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8421;
CREATE FUNCTION pg_catalog.ivfflatcostestimate(internal, internal, internal, internal, internal, internal, internal)
RETURNS void
AS 'ivfflatcostestimate'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflatoptions(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8422;
CREATE FUNCTION pg_catalog.ivfflatoptions(internal, internal)
RETURNS internal
AS 'ivfflatoptions'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflatvalidate(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8205;
CREATE FUNCTION pg_catalog.ivfflatvalidate(internal)
RETURNS boolean
AS 'ivfflatvalidate'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflatbeginscan(internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8413;
CREATE FUNCTION pg_catalog.ivfflatbeginscan(internal, internal, internal)
RETURNS internal
AS 'ivfflatbeginscan'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflatrescan(internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8415;
CREATE FUNCTION pg_catalog.ivfflatrescan(internal, internal, internal, internal, internal)
RETURNS void
AS 'ivfflatrescan'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflatgettuple(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8414;
CREATE FUNCTION pg_catalog.ivfflatgettuple(internal, internal)
RETURNS boolean
AS 'ivfflatgettuple'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflatendscan(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8416;
CREATE FUNCTION pg_catalog.ivfflatendscan(internal)
RETURNS void
AS 'ivfflatendscan'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflathandler(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8206;
CREATE FUNCTION pg_catalog.ivfflathandler(internal)
RETURNS internal
AS 'ivfflathandler'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswbuild(internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8406;
CREATE FUNCTION pg_catalog.hnswbuild(internal, internal, internal)
RETURNS internal
AS 'hnswbuild'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswbuildempty(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8407;
CREATE FUNCTION pg_catalog.hnswbuildempty(internal)
RETURNS void
AS 'hnswbuildempty'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswinsert(internal, internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8401;
CREATE FUNCTION pg_catalog.hnswinsert(internal, internal, internal, internal, internal, internal)
RETURNS boolean
AS 'hnswinsert'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswbulkdelete(internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8408;
CREATE FUNCTION pg_catalog.hnswbulkdelete(internal, internal, internal, internal)
RETURNS internal
AS 'hnswbulkdelete'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswvacuumcleanup(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8409;
CREATE FUNCTION pg_catalog.hnswvacuumcleanup(internal, internal)
RETURNS internal
AS 'hnswvacuumcleanup'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswcostestimate(internal, internal, internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8410;
CREATE FUNCTION pg_catalog.hnswcostestimate(internal, internal, internal, internal, internal, internal, internal)
RETURNS void
AS 'hnswcostestimate'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswoptions(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8411;
CREATE FUNCTION pg_catalog.hnswoptions(internal, internal)
RETURNS internal
AS 'hnswoptions'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswvalidate(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8207;
CREATE FUNCTION pg_catalog.hnswvalidate(internal)
RETURNS boolean
AS 'hnswvalidate'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswbeginscan(internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8402;
CREATE FUNCTION pg_catalog.hnswbeginscan(internal, internal, internal)
RETURNS internal
AS 'hnswbeginscan'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswrescan(internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8404;
CREATE FUNCTION pg_catalog.hnswrescan(internal, internal, internal, internal, internal)
RETURNS void
AS 'hnswrescan'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswdelete(internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8429;
CREATE FUNCTION pg_catalog.hnswdelete(internal, internal, internal, internal, internal)
RETURNS boolean
AS 'hnswdelete'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswgettuple(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8403;
CREATE FUNCTION pg_catalog.hnswgettuple(internal, internal)
RETURNS boolean
AS 'hnswgettuple'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswendscan(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8405;
CREATE FUNCTION pg_catalog.hnswendscan(internal)
RETURNS void
AS 'hnswendscan'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnswhandler(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8208;
CREATE FUNCTION pg_catalog.hnswhandler(internal)
RETURNS internal
AS 'hnswhandler'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.ivfflat_bit_support(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8210;
CREATE FUNCTION pg_catalog.ivfflat_bit_support(internal)
RETURNS internal
AS 'ivfflat_bit_support'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnsw_bit_support(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8209;
CREATE FUNCTION pg_catalog.hnsw_bit_support(internal)
RETURNS internal
AS 'hnsw_bit_support'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hnsw_sparsevec_support(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8479;
CREATE FUNCTION pg_catalog.hnsw_sparsevec_support(internal)
RETURNS internal
AS 'hnsw_sparsevec_support'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.hamming_distance(bit, bit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8469;
CREATE FUNCTION pg_catalog.hamming_distance(bit, bit)
RETURNS float8
AS 'hamming_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.jaccard_distance(bit, bit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8468;
CREATE FUNCTION pg_catalog.jaccard_distance(bit, bit)
RETURNS float8
AS 'jaccard_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP TYPE IF EXISTS pg_catalog.sparsevec CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_TYPE, 8307, 8310, b;
CREATE TYPE sparsevec;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_in(cstring, oid, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8458;
CREATE FUNCTION pg_catalog.sparsevec_in(cstring, oid, int4)
RETURNS sparsevec
AS 'sparsevec_in'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_out(sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8459;
CREATE FUNCTION pg_catalog.sparsevec_out(sparsevec)
RETURNS cstring
AS 'sparsevec_out'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_typmod_in(_cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8460;
CREATE FUNCTION pg_catalog.sparsevec_typmod_in(_cstring)
RETURNS int4
AS 'sparsevec_typmod_in'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_recv(internal, oid, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8461;
CREATE FUNCTION pg_catalog.sparsevec_recv(internal, oid, int4)
RETURNS sparsevec
AS 'sparsevec_recv'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_send(sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8462;
CREATE FUNCTION pg_catalog.sparsevec_send(sparsevec)
RETURNS bytea
AS 'sparsevec_send'
LANGUAGE INTERNAL
STABLE STRICT;

CREATE TYPE sparsevec (
	INPUT     = sparsevec_in,
	OUTPUT    = sparsevec_out,
	TYPMOD_IN = sparsevec_typmod_in,
	RECEIVE   = sparsevec_recv,
	SEND      = sparsevec_send,
	STORAGE   = external
);

DROP FUNCTION IF EXISTS pg_catalog.l2_distance(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8465;
CREATE FUNCTION pg_catalog.l2_distance(sparsevec, sparsevec)
RETURNS float8
AS 'sparsevec_l2_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.inner_product(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8471;
CREATE FUNCTION pg_catalog.inner_product(sparsevec, sparsevec)
RETURNS float8
AS 'sparsevec_inner_product'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.cosine_distance(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8466;
CREATE FUNCTION pg_catalog.cosine_distance(sparsevec, sparsevec)
RETURNS float8
AS 'sparsevec_cosine_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.l1_distance(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8467;
CREATE FUNCTION pg_catalog.l1_distance(sparsevec, sparsevec)
RETURNS float8
AS 'sparsevec_l1_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.l2_norm(sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8478;
CREATE FUNCTION pg_catalog.l2_norm(sparsevec)
RETURNS float8
AS 'sparsevec_l2_norm'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.l2_normalize(sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8211;
CREATE FUNCTION pg_catalog.l2_normalize(sparsevec)
RETURNS sparsevec
AS 'sparsevec_l2_normalize'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_lt(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8472;
CREATE FUNCTION pg_catalog.sparsevec_lt(sparsevec, sparsevec)
RETURNS bool
AS 'sparsevec_lt'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_le(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8473;
CREATE FUNCTION pg_catalog.sparsevec_le(sparsevec, sparsevec)
RETURNS bool
AS 'sparsevec_le'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_eq(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8474;
CREATE FUNCTION pg_catalog.sparsevec_eq(sparsevec, sparsevec)
RETURNS bool
AS 'sparsevec_eq'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_ne(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8475;
CREATE FUNCTION pg_catalog.sparsevec_ne(sparsevec, sparsevec)
RETURNS bool
AS 'sparsevec_ne'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_ge(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8476;
CREATE FUNCTION pg_catalog.sparsevec_ge(sparsevec, sparsevec)
RETURNS bool
AS 'sparsevec_ge'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_gt(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8477;
CREATE FUNCTION pg_catalog.sparsevec_gt(sparsevec, sparsevec)
RETURNS bool
AS 'sparsevec_gt'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_cmp(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8464;
CREATE FUNCTION pg_catalog.sparsevec_cmp(sparsevec, sparsevec)
RETURNS int4
AS 'sparsevec_cmp'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_l2_squared_distance(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8470;
CREATE FUNCTION pg_catalog.sparsevec_l2_squared_distance(sparsevec, sparsevec)
RETURNS float8
AS 'sparsevec_l2_squared_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_negative_inner_product(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8463;
CREATE FUNCTION pg_catalog.sparsevec_negative_inner_product(sparsevec, sparsevec)
RETURNS float8
AS 'sparsevec_negative_inner_product'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec(sparsevec, int4, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8228;
CREATE FUNCTION pg_catalog.sparsevec(sparsevec, int4, boolean)
RETURNS sparsevec
AS 'sparsevec'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_to_sparsevec(vector, int4, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8229;
CREATE FUNCTION pg_catalog.vector_to_sparsevec(vector, int4, boolean)
RETURNS sparsevec
AS 'vector_to_sparsevec'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.sparsevec_to_vector(sparsevec, int4, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8230;
CREATE FUNCTION pg_catalog.sparsevec_to_vector(sparsevec, int4, boolean)
RETURNS vector
AS 'sparsevec_to_vector'
LANGUAGE INTERNAL
STABLE STRICT;

COMMENT ON FUNCTION pg_catalog.vector_in(cstring, oid, int4) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.vector_out(vector) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.vector_typmod_in(_cstring) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_recv(internal, oid, int4) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.vector_send(vector) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.l2_distance(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.inner_product(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.cosine_distance(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.l1_distance(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_dims(vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_norm(vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.l2_normalize(vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.binary_quantize(vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.subvector(vector, int, int) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_add(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_sub(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_mul(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_concat(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_lt(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_le(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_eq(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_ne(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_ge(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_gt(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_cmp(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_l2_squared_distance(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_negative_inner_product(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_spherical_distance(vector, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_accum(_float8, vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_avg(_float8) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_combine(_float8,_float8) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector(vector, int4, boolean) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.array_to_vector(_int4, int4, boolean) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.array_to_vector(_float4, int4, boolean) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.array_to_vector(_float8, int4, boolean) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.array_to_vector(_numeric, int4, boolean) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_to_float4(vector, int4, boolean) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_to_int4(vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_to_float8(vector) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_to_numeric(vector, int4) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflatbuild(internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflatbuildempty(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflatinsert(internal, internal, internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflatbulkdelete(internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflatvacuumcleanup(internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflatcostestimate(internal, internal, internal, internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflatoptions(internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflatvalidate(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflatbeginscan(internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflatrescan(internal, internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflatgettuple(internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflatendscan(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflathandler(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswbuild(internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswbuildempty(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswinsert(internal, internal, internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswbulkdelete(internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswvacuumcleanup(internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswcostestimate(internal, internal, internal, internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswoptions(internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswvalidate(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswbeginscan(internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswrescan(internal, internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswdelete(internal, internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswgettuple(internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswendscan(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnswhandler(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.ivfflat_bit_support(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnsw_bit_support(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hnsw_sparsevec_support(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.hamming_distance(bit, bit) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.jaccard_distance(bit, bit) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec_in(cstring, oid, int4) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.sparsevec_out(sparsevec) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.sparsevec_typmod_in(_cstring) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec_recv(internal, oid, int4) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.sparsevec_send(sparsevec) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.l2_distance(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.inner_product(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.cosine_distance(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.l1_distance(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.l2_norm(sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.l2_normalize(sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec_lt(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec_le(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec_eq(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec_ne(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec_ge(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec_gt(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec_cmp(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec_l2_squared_distance(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec_negative_inner_product(sparsevec, sparsevec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec(sparsevec, int4, boolean) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_to_sparsevec(vector, int4, boolean) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.sparsevec_to_vector(sparsevec, int4, boolean) IS 'NULL';

drop aggregate if exists pg_catalog.avg(vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8241;
create aggregate pg_catalog.avg(vector) (SFUNC=vector_accum, STYPE= _float8, finalfunc = vector_avg,CFUNC = vector_combine,INITCOND = '{0}');
COMMENT ON aggregate pg_catalog.avg(vector) IS 'concatenate aggregate input into an array';

drop aggregate if exists pg_catalog.sum(vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8242;
create aggregate pg_catalog.sum(vector) (SFUNC=vector_add, STYPE= vector, CFUNC = vector_add);
COMMENT ON aggregate pg_catalog.sum(vector) IS 'the average (arithmetic mean) as numeric of all bigint values';

DROP CAST IF EXISTS (vector AS vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8299;
CREATE CAST (vector AS vector)
	WITH FUNCTION vector(vector, int4, boolean) AS IMPLICIT;

DROP CAST IF EXISTS (vector AS _numeric) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8291;
CREATE CAST (vector AS _numeric)
	WITH FUNCTION vector_to_numeric(vector, int4) AS IMPLICIT;
	
DROP CAST IF EXISTS (vector AS _float8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8292;
CREATE CAST (vector AS _float8)
	WITH FUNCTION vector_to_float8(vector) AS IMPLICIT;

DROP CAST IF EXISTS (vector AS _int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8293;
CREATE CAST (vector AS _int4)
	WITH FUNCTION vector_to_int4(vector) AS IMPLICIT;

DROP CAST IF EXISTS (vector AS _float4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8294;
CREATE CAST (vector AS _float4)
	WITH FUNCTION vector_to_float4(vector, int4, boolean) AS IMPLICIT;

DROP CAST IF EXISTS (_int4 AS vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8298;
CREATE CAST (_int4 AS vector)
	WITH FUNCTION array_to_vector(_int4, int4, boolean) AS ASSIGNMENT;

DROP CAST IF EXISTS (_float4 AS vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8297;
CREATE CAST (_float4 AS vector)
	WITH FUNCTION array_to_vector(_float4, int4, boolean) AS ASSIGNMENT;

DROP CAST IF EXISTS (_float8 AS vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8296;
CREATE CAST (_float8 AS vector)
	WITH FUNCTION array_to_vector(_float8, int4, boolean) AS ASSIGNMENT;

DROP CAST IF EXISTS (_numeric AS vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8295;
CREATE CAST (_numeric AS vector)
	WITH FUNCTION array_to_vector(_numeric, int4, boolean) AS ASSIGNMENT;

DROP CAST IF EXISTS (sparsevec AS sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8285;
CREATE CAST (sparsevec AS sparsevec)
	WITH FUNCTION sparsevec(sparsevec, int4, boolean) AS IMPLICIT;

DROP CAST IF EXISTS (sparsevec AS vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8283;
CREATE CAST (sparsevec AS vector)
	WITH FUNCTION sparsevec_to_vector(sparsevec, int4, boolean) AS ASSIGNMENT;

DROP CAST IF EXISTS (vector AS sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8284;
CREATE CAST (vector AS sparsevec)
	WITH FUNCTION vector_to_sparsevec(vector, int4, boolean) AS IMPLICIT;

DROP ACCESS METHOD IF EXISTS ivfflat CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8301;
CREATE ACCESS METHOD ivfflat TYPE INDEX HANDLER ivfflathandler;

DROP ACCESS METHOD IF EXISTS hnsw CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8300;
CREATE ACCESS METHOD hnsw TYPE INDEX HANDLER hnswhandler;

DROP OPERATOR IF EXISTS pg_catalog.<->(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8311;
CREATE OPERATOR pg_catalog.<->(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

DROP OPERATOR IF EXISTS pg_catalog.<#>(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8312;
CREATE OPERATOR pg_catalog.<#>(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_negative_inner_product,
	COMMUTATOR = '<#>'
);

DROP OPERATOR IF EXISTS pg_catalog.<=>(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8313;
CREATE OPERATOR pg_catalog.<=>(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);

DROP OPERATOR IF EXISTS pg_catalog.<+>(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8314;
CREATE OPERATOR pg_catalog.<+>(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = l1_distance,
	COMMUTATOR = '<+>'
);

DROP OPERATOR IF EXISTS pg_catalog.+(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8325;
CREATE OPERATOR pg_catalog.+(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_add,
	COMMUTATOR = '+'
);

DROP OPERATOR IF EXISTS pg_catalog.-(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8326;
CREATE OPERATOR pg_catalog.-(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_sub,
	COMMUTATOR = '-'
);

DROP OPERATOR IF EXISTS pg_catalog.*(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8349;
CREATE OPERATOR pg_catalog.*(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_mul,
	COMMUTATOR = '*'
);

DROP OPERATOR IF EXISTS pg_catalog.||(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8339;
CREATE OPERATOR pg_catalog.||(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_concat
);

DROP OPERATOR IF EXISTS pg_catalog.<(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8327;
CREATE OPERATOR pg_catalog.<(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_lt,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.<=(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8328;
CREATE OPERATOR pg_catalog.<=(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_le,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.=(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8331;
CREATE OPERATOR pg_catalog.=(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_eq,
	COMMUTATOR = '=' ,
	RESTRICT = eqsel, JOIN = eqjoinsel, HASHES
);

DROP OPERATOR IF EXISTS pg_catalog.<>(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8332;
CREATE OPERATOR pg_catalog.<>(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_ne,
	COMMUTATOR = '<>' , NEGATOR = '=' ,
	RESTRICT = neqsel, JOIN = neqjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.>=(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8330;
CREATE OPERATOR pg_catalog.>=(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_ge,
	COMMUTATOR = '<=' , NEGATOR = '<' ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.>(vector, vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8329;
CREATE OPERATOR pg_catalog.>(
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_gt,
	COMMUTATOR = '<' , NEGATOR = '<=' ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.<~>(bit, bit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8323;
CREATE OPERATOR pg_catalog.<~>(
	LEFTARG = bit, RIGHTARG = bit, PROCEDURE = hamming_distance,
	COMMUTATOR = '<~>'
);

DROP OPERATOR IF EXISTS pg_catalog.<%>(bit, bit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8324;
CREATE OPERATOR pg_catalog.<%>(
	LEFTARG = bit, RIGHTARG = bit, PROCEDURE = jaccard_distance,
	COMMUTATOR = '<%>'
);

DROP OPERATOR IF EXISTS pg_catalog.<->(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8319;
CREATE OPERATOR pg_catalog.<->(
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

DROP OPERATOR IF EXISTS pg_catalog.<#>(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8320;
CREATE OPERATOR pg_catalog.<#>(
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_negative_inner_product,
	COMMUTATOR = '<#>'
);

DROP OPERATOR IF EXISTS pg_catalog.<=>(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8321;
CREATE OPERATOR pg_catalog.<=>(
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);

DROP OPERATOR IF EXISTS pg_catalog.<+>(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8322;
CREATE OPERATOR pg_catalog.<+>(
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = l1_distance,
	COMMUTATOR = '<+>'
);

DROP OPERATOR IF EXISTS pg_catalog.<(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8333;
CREATE OPERATOR pg_catalog.<(
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_lt,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.<=(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8334;
CREATE OPERATOR pg_catalog.<=(
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_le,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.=(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8337;
CREATE OPERATOR pg_catalog.=(
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_eq,
	COMMUTATOR = '=' ,
	RESTRICT = eqsel, JOIN = eqjoinsel, HASHES
);

DROP OPERATOR IF EXISTS pg_catalog.<>(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8338;
CREATE OPERATOR pg_catalog.<>(
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_ne,
	COMMUTATOR = '<>' , NEGATOR = '=' ,
	RESTRICT = neqsel, JOIN = neqjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.>=(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8336;
CREATE OPERATOR pg_catalog.>=(
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_ge,
	COMMUTATOR = '<=' , NEGATOR = '<' ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.>(sparsevec, sparsevec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8335;
CREATE OPERATOR pg_catalog.>(
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_gt,
	COMMUTATOR = '<' , NEGATOR = '<=' ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

COMMENT ON OPERATOR pg_catalog.<->(vector,vector) IS 'l2_distance';
COMMENT ON OPERATOR pg_catalog.<#>(vector,vector) IS 'vector_negative_inner_product';
COMMENT ON OPERATOR pg_catalog.<=>(vector,vector) IS 'cosine_distance';
COMMENT ON OPERATOR pg_catalog.<+>(vector,vector) IS 'l1_distance';
COMMENT ON OPERATOR pg_catalog.||(vector,vector) IS 'vector_concat';
COMMENT ON OPERATOR pg_catalog.+(vector,vector) IS 'vector_add';
COMMENT ON OPERATOR pg_catalog.-(vector,vector) IS 'vector_sub';
COMMENT ON OPERATOR pg_catalog.*(vector,vector) IS 'vector_mul';
COMMENT ON OPERATOR pg_catalog.<(vector,vector) IS 'vector less than';
COMMENT ON OPERATOR pg_catalog.<=(vector,vector) IS 'vector less than or equal';
COMMENT ON OPERATOR pg_catalog.>(vector,vector) IS 'vector greater than';
COMMENT ON OPERATOR pg_catalog.>=(vector,vector) IS 'vector greater than or equal';
COMMENT ON OPERATOR pg_catalog.=(vector,vector) IS 'vector equal';
COMMENT ON OPERATOR pg_catalog.<>(vector,vector) IS 'vector unequal';
COMMENT ON OPERATOR pg_catalog.<~>(bit,bit) IS 'hamming_distance';
COMMENT ON OPERATOR pg_catalog.<%>(bit,bit) IS 'jaccard_distance';
COMMENT ON OPERATOR pg_catalog.<->(sparsevec,sparsevec) IS 'sparsevec_l2_distance';
COMMENT ON OPERATOR pg_catalog.<#>(sparsevec,sparsevec) IS 'sparsevec_negative_inner_product';
COMMENT ON OPERATOR pg_catalog.<=>(sparsevec,sparsevec) IS 'sparsevec_cosine_distance';
COMMENT ON OPERATOR pg_catalog.<+>(sparsevec,sparsevec) IS 'sparsevec_l1_distance';
COMMENT ON OPERATOR pg_catalog.<(sparsevec,sparsevec) IS 'sparsevec less than';
COMMENT ON OPERATOR pg_catalog.<=(sparsevec,sparsevec) IS 'sparsevec less than or equal';
COMMENT ON OPERATOR pg_catalog.>(sparsevec,sparsevec) IS 'sparsevec greater than';
COMMENT ON OPERATOR pg_catalog.>=(sparsevec,sparsevec) IS 'sparsevec greater than or equal';
COMMENT ON OPERATOR pg_catalog.=(sparsevec,sparsevec) IS 'sparsevec equal';
COMMENT ON OPERATOR pg_catalog.<>(sparsevec,sparsevec) IS 'sparsevec unequal';

DROP OPERATOR FAMILY IF EXISTS vector_ops USING btree CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8392;
CREATE OPERATOR FAMILY vector_ops USING btree;

DROP OPERATOR CLASS IF EXISTS vector_ops USING btree CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8977;
CREATE OPERATOR CLASS vector_ops DEFAULT
    FOR TYPE vector USING btree as
    OPERATOR 1 pg_catalog.<(vector, vector),
    OPERATOR 2 pg_catalog.<=(vector, vector),
    OPERATOR 3 pg_catalog.=(vector, vector),
    OPERATOR 4 pg_catalog.>=(vector, vector),
    OPERATOR 5 pg_catalog.>(vector, vector),
    FUNCTION 1 pg_catalog.vector_cmp(vector,vector);

DROP OPERATOR FAMILY IF EXISTS vector_ubt_ops USING ubtree CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8375;
CREATE OPERATOR FAMILY vector_ubt_ops USING ubtree;

DROP OPERATOR CLASS IF EXISTS vector_ubt_ops USING ubtree CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8951;
CREATE OPERATOR CLASS vector_ubt_ops DEFAULT
	FOR TYPE vector USING ubtree AS
	OPERATOR 1 pg_catalog.<(vector, vector),
	OPERATOR 2 pg_catalog.<=(vector, vector),
	OPERATOR 3 pg_catalog.=(vector, vector),
	OPERATOR 4 pg_catalog.>=(vector, vector),
	OPERATOR 5 pg_catalog.>(vector, vector),
	FUNCTION 1 pg_catalog.vector_cmp(vector, vector);

DROP OPERATOR FAMILY IF EXISTS vector_l2_ops USING ivfflat CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8385;
CREATE OPERATOR FAMILY vector_l2_ops USING ivfflat;

DROP OPERATOR CLASS IF EXISTS vector_l2_ops USING ivfflat CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8914;
CREATE OPERATOR CLASS vector_l2_ops
	DEFAULT FOR TYPE vector USING ivfflat AS
	OPERATOR 1 pg_catalog.<->(vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.vector_l2_squared_distance(vector, vector),
	FUNCTION 3 pg_catalog.l2_distance(vector, vector);

DROP OPERATOR FAMILY IF EXISTS vector_ip_ops USING ivfflat CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8386;
CREATE OPERATOR FAMILY vector_ip_ops USING ivfflat;

DROP OPERATOR CLASS IF EXISTS vector_ip_ops USING ivfflat CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8915;
CREATE OPERATOR CLASS vector_ip_ops
	FOR TYPE vector USING ivfflat AS
	OPERATOR 1 pg_catalog.<#>(vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.vector_negative_inner_product(vector, vector),
	FUNCTION 3 pg_catalog.vector_spherical_distance(vector, vector),
	FUNCTION 4 pg_catalog.vector_norm(vector);

DROP OPERATOR FAMILY IF EXISTS vector_cosine_ops USING ivfflat CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8387;
CREATE OPERATOR FAMILY vector_cosine_ops USING ivfflat;

DROP OPERATOR CLASS IF EXISTS vector_cosine_ops USING ivfflat CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8916;
CREATE OPERATOR CLASS vector_cosine_ops
	FOR TYPE vector USING ivfflat AS
	OPERATOR 1 pg_catalog.<=>(vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.vector_negative_inner_product(vector, vector),
	FUNCTION 2 pg_catalog.vector_norm(vector),
	FUNCTION 3 pg_catalog.vector_spherical_distance(vector, vector),
	FUNCTION 4 pg_catalog.vector_norm(vector);

DROP OPERATOR FAMILY IF EXISTS vector_l2_ops USING hnsw CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8371;
CREATE OPERATOR FAMILY vector_l2_ops USING hnsw;

DROP OPERATOR CLASS IF EXISTS vector_l2_ops USING hnsw CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8900;
CREATE OPERATOR CLASS vector_l2_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 pg_catalog.<->(vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.vector_l2_squared_distance(vector, vector);

DROP OPERATOR FAMILY IF EXISTS vector_ip_ops USING hnsw CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8372;
CREATE OPERATOR FAMILY vector_ip_ops USING hnsw;

DROP OPERATOR CLASS IF EXISTS vector_ip_ops USING hnsw CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8999;
CREATE OPERATOR CLASS vector_ip_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 pg_catalog.<#>(vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.vector_negative_inner_product(vector, vector),
	FUNCTION 4 pg_catalog.vector_norm(vector);

DROP OPERATOR FAMILY IF EXISTS vector_cosine_ops USING hnsw CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8373;
CREATE OPERATOR FAMILY vector_cosine_ops USING hnsw;

DROP OPERATOR CLASS IF EXISTS vector_cosine_ops USING hnsw CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8902;
CREATE OPERATOR CLASS vector_cosine_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 pg_catalog.<=>(vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.vector_negative_inner_product(vector, vector),
	FUNCTION 2 pg_catalog.vector_norm(vector),
	FUNCTION 4 pg_catalog.vector_norm(vector);

DROP OPERATOR FAMILY IF EXISTS vector_l1_ops USING hnsw CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8374;
CREATE OPERATOR FAMILY vector_l1_ops USING hnsw;

DROP OPERATOR CLASS IF EXISTS vector_l1_ops USING hnsw CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8903;
CREATE OPERATOR CLASS vector_l1_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 pg_catalog.<+>(vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.l1_distance(vector, vector);

DROP OPERATOR FAMILY IF EXISTS bit_hamming_ops USING ivfflat CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8394;
CREATE OPERATOR FAMILY bit_hamming_ops USING ivfflat;

DROP OPERATOR CLASS IF EXISTS bit_hamming_ops USING ivfflat CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8923;
CREATE OPERATOR CLASS bit_hamming_ops
	FOR TYPE bit USING ivfflat AS
	OPERATOR 1 pg_catalog.<~>(bit, bit) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.hamming_distance(bit, bit),
	FUNCTION 3 pg_catalog.hamming_distance(bit, bit),
	FUNCTION 5 pg_catalog.ivfflat_bit_support(internal);

DROP OPERATOR FAMILY IF EXISTS bit_hamming_ops USING hnsw CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8380;
CREATE OPERATOR FAMILY bit_hamming_ops USING hnsw;

DROP OPERATOR CLASS IF EXISTS bit_hamming_ops USING hnsw CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8909;
CREATE OPERATOR CLASS bit_hamming_ops
	FOR TYPE bit USING hnsw AS
	OPERATOR 1 pg_catalog.<~>(bit, bit) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.hamming_distance(bit, bit),
	FUNCTION 3 pg_catalog.hnsw_bit_support(internal);

DROP OPERATOR FAMILY IF EXISTS bit_jaccard_ops USING hnsw CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8379;
CREATE OPERATOR FAMILY bit_jaccard_ops USING hnsw;

DROP OPERATOR CLASS IF EXISTS bit_jaccard_ops USING hnsw CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8908;
CREATE OPERATOR CLASS bit_jaccard_ops
	FOR TYPE bit USING hnsw AS
	OPERATOR 1 pg_catalog.<%>(bit, bit) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.jaccard_distance(bit, bit),
	FUNCTION 3 pg_catalog.hnsw_bit_support(internal);

DROP OPERATOR FAMILY IF EXISTS sparsevec_ops USING btree CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8397;
CREATE OPERATOR FAMILY sparsevec_ops USING btree;

DROP OPERATOR CLASS IF EXISTS sparsevec_ops USING btree CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8979;
CREATE OPERATOR CLASS sparsevec_ops
	DEFAULT FOR TYPE sparsevec USING btree AS
	OPERATOR 1 pg_catalog.<(sparsevec, sparsevec),
	OPERATOR 2 pg_catalog.<=(sparsevec, sparsevec),
	OPERATOR 3 pg_catalog.=(sparsevec, sparsevec),
	OPERATOR 4 pg_catalog.>=(sparsevec, sparsevec),
	OPERATOR 5 pg_catalog.>(sparsevec, sparsevec),
	FUNCTION 1 pg_catalog.sparsevec_cmp(sparsevec, sparsevec);

DROP OPERATOR FAMILY IF EXISTS sparsevec_ubt_ops USING ubtree CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8376;
CREATE OPERATOR FAMILY sparsevec_ubt_ops USING ubtree;

DROP OPERATOR CLASS IF EXISTS sparsevec_ubt_ops USING ubtree CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8952;
CREATE OPERATOR CLASS sparsevec_ubt_ops DEFAULT
	FOR TYPE sparsevec USING ubtree AS
	OPERATOR 1 pg_catalog.<(sparsevec, sparsevec),
	OPERATOR 2 pg_catalog.<=(sparsevec, sparsevec),
	OPERATOR 3 pg_catalog.=(sparsevec, sparsevec),
	OPERATOR 4 pg_catalog.>=(sparsevec, sparsevec),
	OPERATOR 5 pg_catalog.>(sparsevec, sparsevec),
	FUNCTION 1 sparsevec_cmp(sparsevec, sparsevec);

DROP OPERATOR FAMILY IF EXISTS sparsevec_l2_ops USING hnsw CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8381;
CREATE OPERATOR FAMILY sparsevec_l2_ops USING hnsw;

DROP OPERATOR CLASS IF EXISTS sparsevec_l2_ops USING hnsw CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8910;
CREATE OPERATOR CLASS sparsevec_l2_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 pg_catalog.<->(sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.sparsevec_l2_squared_distance(sparsevec, sparsevec),
	FUNCTION 3 pg_catalog.hnsw_sparsevec_support(internal);

DROP OPERATOR FAMILY IF EXISTS sparsevec_ip_ops USING hnsw CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8382;
CREATE OPERATOR FAMILY sparsevec_ip_ops USING hnsw;

DROP OPERATOR CLASS IF EXISTS sparsevec_ip_ops USING hnsw CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8911;
CREATE OPERATOR CLASS sparsevec_ip_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 pg_catalog.<#>(sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.sparsevec_negative_inner_product(sparsevec, sparsevec),
	FUNCTION 3 pg_catalog.hnsw_sparsevec_support(internal);

DROP OPERATOR FAMILY IF EXISTS sparsevec_cosine_ops USING hnsw CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8383;
CREATE OPERATOR FAMILY sparsevec_cosine_ops USING hnsw;

DROP OPERATOR CLASS IF EXISTS sparsevec_cosine_ops USING hnsw CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8912;
CREATE OPERATOR CLASS sparsevec_cosine_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 pg_catalog.<=>(sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.sparsevec_negative_inner_product(sparsevec, sparsevec),
	FUNCTION 2 pg_catalog.l2_norm(sparsevec),
	FUNCTION 3 pg_catalog.hnsw_sparsevec_support(internal);

DROP OPERATOR FAMILY IF EXISTS sparsevec_l1_ops USING hnsw CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8384;
CREATE OPERATOR FAMILY sparsevec_l1_ops USING hnsw;

DROP OPERATOR CLASS IF EXISTS sparsevec_l1_ops USING hnsw CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8913;
CREATE OPERATOR CLASS sparsevec_l1_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 pg_catalog.<+>(sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.l1_distance(sparsevec, sparsevec),
	FUNCTION 3 pg_catalog.hnsw_sparsevec_support(internal);
