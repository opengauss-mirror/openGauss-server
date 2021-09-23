SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4767;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_block_from_remote
(  int4,
   int4,
   int4,
   int2, 
   int4,
   xid,
   int4,
   xid,
   boolean)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_read_block_from_remote';
