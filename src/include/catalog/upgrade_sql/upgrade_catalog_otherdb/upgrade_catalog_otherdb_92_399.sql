SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 33, 1004, b;
CREATE TYPE pg_catalog.int2vector_extend;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2600;
CREATE OR REPLACE FUNCTION pg_catalog.int2vectorin_extend(cstring) RETURNS pg_catalog.int2vector_extend
    LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED SHIPPABLE AS 'int2vectorin_extend';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2601;
CREATE OR REPLACE FUNCTION pg_catalog.int2vectorout_extend(pg_catalog.int2vector_extend) RETURNS CSTRING
    LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED SHIPPABLE AS 'int2vectorout_extend';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2602;
CREATE OR REPLACE FUNCTION pg_catalog.int2vectorrecv_extend(internal) RETURNS pg_catalog.int2vector_extend
    LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED SHIPPABLE AS 'int2vectorrecv_extend';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2603;
CREATE OR REPLACE FUNCTION pg_catalog.int2vectorsend_extend(pg_catalog.int2vector_extend) RETURNS bytea
    LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED SHIPPABLE AS 'int2vectorsend_extend';

CREATE TYPE pg_catalog.int2vector_extend(input = int2vectorin_extend, output = int2vectorout_extend, receive = int2vectorrecv_extend,
    send = int2vectorsend_extend, internallength = VARIABLE, alignment = int4, storage = extended, element = int2, category = A);
