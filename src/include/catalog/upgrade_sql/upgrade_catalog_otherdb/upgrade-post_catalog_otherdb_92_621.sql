DROP FUNCTION IF EXISTS pg_catalog.gs_hot_standby_space_info() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6218;
CREATE OR REPLACE FUNCTION pg_catalog.gs_hot_standby_space_info
(  OUT base_page_file_num xid,
   OUT base_page_total_size xid,
   OUT lsn_info_meta_file_num xid,
   OUT lsn_info_meta_total_size xid,
   OUT block_info_meta_file_num xid,
   OUT block_info_meta_total_size xid
   )
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_hot_standby_space_info';