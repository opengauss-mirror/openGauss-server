DROP FUNCTION IF EXISTS pg_catalog.ss_buffer_ctrl() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4214;
CREATE FUNCTION pg_catalog.ss_buffer_ctrl(
    OUT bufferid integer,
    OUT is_remote_dirty tinyint,
    OUT lock_mode tinyint,
    OUT is_edp tinyint,
    OUT force_request tinyint,
    OUT need_flush tinyint,
    OUT buf_id integer,
    OUT state oid,
    OUT pblk_relno oid,
    OUT pblk_blkno oid,
    OUT pblk_lsn bigint,
    OUT seg_fileno tinyint,
    OUT seg_blockno oid
) 
RETURNS SETOF record 
LANGUAGE internal STABLE NOT FENCED NOT SHIPPABLE ROWS 100 
AS 'ss_buffer_ctrl';