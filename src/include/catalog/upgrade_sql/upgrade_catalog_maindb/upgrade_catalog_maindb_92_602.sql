--create gs_uid
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 8666, 8667, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_uid
(
   relid OID NOCOMPRESS NOT NULL,
   uid_backup bigint NOCOMPRESS NOT NULL
);
GRANT SELECT ON pg_catalog.gs_uid TO public;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3499;
CREATE UNIQUE INDEX gs_uid_relid_index ON pg_catalog.gs_uid USING BTREE(relid OID_OPS);


SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
/*------ add sys fuction gs_stat_wal_entrytable ------*/
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2861;
CREATE FUNCTION pg_catalog.gs_stat_wal_entrytable(int8, OUT idx xid, OUT endlsn xid, OUT lrc int, OUT status xid32) 
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_stat_wal_entrytable';

/*------ add sys fuction gs_walwriter_flush_position ------*/
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2862;
CREATE FUNCTION pg_catalog.gs_walwriter_flush_position(out last_flush_status_entry int, out last_scanned_lrc int, out curr_lrc int, out curr_byte_pos xid, out prev_byte_size xid32, out flush_result xid, out send_result xid, out shm_rqst_write_pos xid, out shm_rqst_flush_pos xid, out shm_result_write_pos xid, out shm_result_flush_pos xid, out curr_time timestamptz)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_walwriter_flush_position';

/*------ add sys fuction gs_walwriter_flush_stat ------*/
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2863;
CREATE FUNCTION pg_catalog.gs_walwriter_flush_stat(int4, out write_times xid, out sync_times xid, out total_xlog_sync_bytes xid, out total_actual_xlog_sync_bytes xid, out avg_write_bytes xid32, out avg_actual_write_bytes xid32, out avg_sync_bytes xid32, out avg_actual_sync_bytes xid32, out total_write_time xid, out total_sync_time xid, out avg_write_time xid32, out avg_sync_time xid32, out curr_init_xlog_segno xid, out curr_open_xlog_segno xid, out last_reset_time timestamptz, out curr_time timestamptz)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_walwriter_flush_stat';

/*------ add sys fuction gs_stat_undo ------*/
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4434;
CREATE FUNCTION pg_catalog.gs_stat_undo(OUT curr_used_zone_count int4, OUT top_used_zones text, OUT curr_used_undo_size int4,
OUT undo_threshold int4, OUT oldest_xid_in_undo oid, OUT oldest_xmin oid, OUT total_undo_chain_len oid, OUT max_undo_chain_len oid,
OUT create_undo_file_count int4, OUT discard_undo_file_count int4)
RETURNS record LANGUAGE INTERNAL as 'gs_stat_undo';

--create system relation gs_db_privilege and its indexes
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 5566, 5567, 0, 0;
CREATE TABLE pg_catalog.gs_db_privilege
(
    roleid Oid NOCOMPRESS not null,
    privilege_type text NOCOMPRESS,
    admin_option boolean NOCOMPRESS not null
) WITH OIDS TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 5568;
CREATE UNIQUE INDEX gs_db_privilege_oid_index ON pg_catalog.gs_db_privilege USING BTREE(oid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 5569;
CREATE INDEX gs_db_privilege_roleid_index ON pg_catalog.gs_db_privilege USING BTREE(roleid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 5570;
CREATE UNIQUE INDEX gs_db_privilege_roleid_privilege_type_index ON pg_catalog.gs_db_privilege
    USING BTREE(roleid oid_ops, privilege_type text_ops);

GRANT SELECT ON pg_catalog.gs_db_privilege TO PUBLIC;

--create system function has_any_privilege(user, privilege)
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5571;
CREATE OR REPLACE FUNCTION pg_catalog.has_any_privilege(name, text) RETURNS boolean
  LANGUAGE INTERNAL STRICT STABLE
  AS 'has_any_privilege';

--create system view gs_db_privileges
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
CREATE VIEW pg_catalog.gs_db_privileges AS
    SELECT
        pg_catalog.pg_get_userbyid(roleid) AS rolename,
        privilege_type AS privilege_type,
        CASE
            WHEN admin_option THEN
                'yes'
            ELSE
                'no'
        END AS admin_option
    FROM pg_catalog.gs_db_privilege;

GRANT SELECT ON pg_catalog.gs_db_privileges TO PUBLIC;
/*------ add sys fuction gs_undo_record ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_record(int8, OUT undoptr oid, OUT xid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4439;
CREATE FUNCTION pg_catalog.gs_undo_record(int8, OUT undoptr oid, OUT xid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text)
RETURNS record LANGUAGE INTERNAL as 'gs_undo_record';

/*------ add sys fuction gs_undo_meta ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta(int4, int4, int4, OUT zoneId oid, OUT persistType oid, OUT insertptr text, OUT discard text, OUT endptr text, OUT used text, OUT lsn text);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4430;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_meta(int4, int4, int4, OUT zoneId oid, OUT persistType oid, OUT insertptr text, OUT discard text, OUT endptr text, OUT used text, OUT lsn text, OUT pid oid)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_undo_meta';

/* add sys fuction gs_undo_translot */
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot(int4, int4, OUT grpId oid, OUT xactId text, OUT startUndoPtr text, OUT endUndoPtr text, OUT lsn text);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4431;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_translot(int4, int4, OUT grpId oid, OUT xactId text, OUT startUndoPtr text, OUT endUndoPtr text, OUT lsn text, OUT slot_states oid)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_undo_translot';

/*------ add sys fuction gs_index_verify ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_index_verify(oid, oid, OUT ptype text, OUT blkno oid, OUT status text);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9150;
CREATE FUNCTION pg_catalog.gs_index_verify(oid, oid, OUT ptype text, OUT blkno oid, OUT status text) 
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_index_verify';

/*------ add sys fuction gs_index_recycle_queue ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_index_recycle_queue(oid, oid, oid, OUT rblkno oid, OUT item_offset oid, OUT xid text, OUT dblkno oid, OUT prev oid, OUT next oid);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9151;
CREATE FUNCTION pg_catalog.gs_index_recycle_queue(oid, oid, oid, OUT rblkno oid, OUT item_offset oid, OUT xid text, OUT dblkno oid, OUT prev oid, OUT next oid)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_index_recycle_queue';DROP FUNCTION IF EXISTS pg_catalog.pg_logical_get_area_changes() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4978;
CREATE OR REPLACE FUNCTION pg_catalog.pg_logical_get_area_changes(start_lsn text, upto_lsn text, upto_nchanges integer, plugin name DEFAULT '{}'::text[], xlog_path text, VARIADIC options text[], OUT location text, OUT xid xid, OUT data text)
 RETURNS SETOF record
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE COST 1000
AS $function$pg_logical_get_area_changes$function$;
