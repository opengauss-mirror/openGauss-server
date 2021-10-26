-- 1. create blockchain schema
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 4990;
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_namespace where nspname = 'blockchain' limit 1) into ans;
    if ans = false then
        create schema blockchain;
        COMMENT ON schema blockchain IS 'blockchain schema';
    end if;
END$$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

-- 2. 新增类型
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 5801, 5803, b;
CREATE TYPE pg_catalog.hash16;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5812;
CREATE OR REPLACE FUNCTION pg_catalog.hash16in(cstring) RETURNS pg_catalog.hash16 LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE AS 'hash16in';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5813;
CREATE OR REPLACE FUNCTION pg_catalog.hash16out(pg_catalog.hash16) RETURNS CSTRING LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE AS 'hash16out';

CREATE TYPE pg_catalog.hash16 (input=hash16in,output=pg_catalog.hash16out,internallength=8,passedbyvalue,alignment=double);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 5802, 5804, b;
CREATE TYPE pg_catalog.hash32;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5814;
CREATE OR REPLACE FUNCTION pg_catalog.hash32in(cstring) RETURNS pg_catalog.hash32 LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE AS 'hash32in';
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5815;
CREATE OR REPLACE FUNCTION pg_catalog.hash32out(pg_catalog.hash32) RETURNS CSTRING LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE AS 'hash32out';

CREATE TYPE pg_catalog.hash32 (input=hash32in,output=hash32out,internallength=16,alignment=char);

-- 3. 新增函数
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5820;
CREATE OR REPLACE FUNCTION pg_catalog.hash16_eq(hash16, hash16) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE as 'hash16_eq';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5819;
CREATE OR REPLACE FUNCTION pg_catalog.hash16_add(hash16, hash16) RETURNS hash16 LANGUAGE INTERNAL IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE as 'hash16_add';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5681;
CREATE OR REPLACE FUNCTION pg_catalog.get_dn_hist_relhash(text, text) RETURNS hash16 LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE as 'get_dn_hist_relhash';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5682;
CREATE OR REPLACE FUNCTION pg_catalog.ledger_gchain_check(text, text) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE as 'ledger_gchain_check';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5683;
CREATE OR REPLACE FUNCTION pg_catalog.ledger_hist_archive(text, text) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE as 'ledger_hist_archive';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5684;
CREATE OR REPLACE FUNCTION pg_catalog.ledger_gchain_archive() RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE as 'ledger_gchain_archive';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5722;
CREATE OR REPLACE FUNCTION pg_catalog.ledger_hist_check(text, text) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE as 'ledger_hist_check';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5685;
CREATE OR REPLACE FUNCTION pg_catalog.ledger_hist_repair(text, text) RETURNS hash16 LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE as 'ledger_hist_repair';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5686;
CREATE OR REPLACE FUNCTION pg_catalog.ledger_gchain_repair(text, text) RETURNS hash16 LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE as 'ledger_gchain_repair';

-- 4. create system relation gs_global_chain and its indexes.
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 5818, 5819, 5816, 5817;
CREATE TABLE pg_catalog.gs_global_chain
(
    blocknum int8 NOCOMPRESS not null,
    dbname name NOCOMPRESS not null,
    username name NOCOMPRESS not null,
    starttime timestamptz NOCOMPRESS not null,
    relid Oid NOCOMPRESS not null,
    relnsp name NOCOMPRESS not null,
    relname name NOCOMPRESS not null,
    relhash hash16 NOCOMPRESS not null,
    globalhash hash32 NOCOMPRESS not null,
    txcommand text NOCOMPRESS
) TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 5511;
CREATE INDEX gs_global_chain_relid_index ON pg_catalog.gs_global_chain USING BTREE(relid oid_ops);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
GRANT SELECT ON pg_catalog.gs_global_chain TO PUBLIC;
