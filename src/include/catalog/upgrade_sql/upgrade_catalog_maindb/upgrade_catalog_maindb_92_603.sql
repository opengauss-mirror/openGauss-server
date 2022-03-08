SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5843;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_block_from_remote
(  int4,
   int4,
   int4,
   int2, 
   int2,
   int4,
   xid,
   int4,
   xid,
   boolean,
   int4)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_read_block_from_remote_compress';
-- pg_read_binary_file_blocks()
--
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8413;
CREATE FUNCTION pg_catalog.pg_read_binary_file_blocks(IN inputpath text, IN startblocknum int8, IN count int8,
                                           OUT path text,
                                           OUT blocknum int4,
                                           OUT len int4,
                                           OUT data bytea)
    AS 'pg_read_binary_file_blocks' LANGUAGE INTERNAL IMMUTABLE STRICT;
