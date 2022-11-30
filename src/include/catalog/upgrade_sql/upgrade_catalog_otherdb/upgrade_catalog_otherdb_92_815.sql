-- compress funcs
DROP FUNCTION IF EXISTS pg_catalog.compress_address_header(IN relname regclass, IN segment_no bigint, OUT extent bigint, OUT nblocks bigint, OUT alocated_chunks integer, OUT chunk_size integer, OUT algorithm bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1000;
CREATE OR REPLACE FUNCTION pg_catalog.compress_address_header(IN relname regclass, IN segment_no bigint, OUT extent bigint, OUT nblocks bigint, OUT alocated_chunks integer, OUT chunk_size integer, OUT algorithm bigint) RETURNS record LANGUAGE INTERNAL AS 'compress_address_header';

DROP FUNCTION IF EXISTS pg_catalog.compress_address_details(IN relname regclass, IN segment_no bigint, OUT extent bigint, OUT extent_block_number bigint, OUT block_number bigint, OUT alocated_chunks integer, OUT nchunks integer, OUT chunknos integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1001;
CREATE OR REPLACE FUNCTION pg_catalog.compress_address_details(IN relname regclass, IN segment_no bigint, OUT extent bigint, OUT extent_block_number bigint, OUT block_number bigint, OUT alocated_chunks integer, OUT nchunks integer, OUT chunknos integer) RETURNS record LANGUAGE INTERNAL AS 'compress_address_details';

DROP FUNCTION IF EXISTS pg_catalog.compress_buffer_stat_info(OUT ctrl_cnt bigint, OUT main_cnt bigint, OUT free_cnt bigint, OUT recycle_times bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1002;
CREATE OR REPLACE FUNCTION pg_catalog.compress_buffer_stat_info(OUT ctrl_cnt bigint, OUT main_cnt bigint, OUT free_cnt bigint, OUT recycle_times bigint) RETURNS record LANGUAGE INTERNAL AS 'compress_buffer_stat_info';

DROP FUNCTION IF EXISTS pg_catalog.compress_ratio_info(IN input_path text, OUT path text, OUT is_compress boolean, OUT file_count bigint, OUT logic_size bigint, OUT physic_size bigint, OUT compress_ratio text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1003;
CREATE OR REPLACE FUNCTION pg_catalog.compress_ratio_info(IN input_path text, OUT path text, OUT is_compress boolean, OUT file_count bigint, OUT logic_size bigint, OUT physic_size bigint, OUT compress_ratio text) RETURNS record LANGUAGE INTERNAL AS 'compress_ratio_info';

DROP FUNCTION IF EXISTS pg_catalog.compress_statistic_info(IN input_path text, IN step smallint, OUT path text, OUT extent_count bigint, OUT dispersion_count bigint, OUT void_count bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1005;
CREATE OR REPLACE FUNCTION pg_catalog.compress_statistic_info(IN input_path text, IN step smallint, OUT path text, OUT extent_count bigint, OUT dispersion_count bigint, OUT void_count bigint) RETURNS record LANGUAGE INTERNAL AS 'compress_statistic_info';

-- compress read page/file from remote
DROP FUNCTION IF EXISTS pg_catalog.gs_read_block_from_remote(int4, int4, int4, int2, int2, int4, xid, int4, xid, boolean, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5843;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_block_from_remote(oid, oid, oid, smallint, smallint, integer, xid, integer, xid, boolean, integer)
 RETURNS bytea
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS 'gs_read_block_from_remote';

DROP FUNCTION IF EXISTS pg_catalog.gs_read_file_from_remote(oid, oid, oid, integer, smallint, integer, integer, integer, xid, integer, OUT bytea, OUT xid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5844;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_file_from_remote(oid, oid, oid, integer, smallint, integer, integer, integer, xid, integer, OUT bytea, OUT xid)
 RETURNS record
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS 'gs_read_file_from_remote';

DROP FUNCTION IF EXISTS pg_catalog.gs_read_file_size_from_remote(oid, oid, oid, integer, smallint, integer, xid, integer, OUT bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5845;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_file_size_from_remote(oid, oid, oid, integer, smallint, integer, xid, integer, OUT bigint)
 RETURNS bigint
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS 'gs_read_file_size_from_remote';

DROP FUNCTION IF EXISTS pg_catalog.pg_read_binary_file_blocks(IN inputpath text, IN startblocknum int8, IN count int8,
                                           OUT path text,
                                           OUT blocknum int4,
                                           OUT len int4,
                                           OUT data bytea);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5846;
CREATE OR REPLACE FUNCTION pg_catalog.pg_read_binary_file_blocks(IN input text, IN blocknum bigint, IN blockcount bigint, OUT path text, OUT blocknum integer, OUT len integer, OUT algorithm integer, OUT chunk_size integer, OUT data bytea) RETURNS SETOF record AS 'pg_read_binary_file_blocks' LANGUAGE INTERNAL IMMUTABLE;
