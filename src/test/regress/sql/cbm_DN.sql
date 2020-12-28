create or replace FUNCTION disable_delay_ddl_recycle()
RETURNS table (i text,j text)
LANGUAGE plpgsql
AS
$$
DECLARE
        param1  text;

BEGIN
        EXECUTE('EXECUTE DIRECT ON (datanode1) ''SELECT pg_current_xlog_location()'' ') into param1;
        DROP TABLE cbmtst;
        DROP TABLE cbmtst1;
        CHECKPOINT;
        return query EXECUTE('EXECUTE DIRECT ON (datanode1) ''SELECT * FROM pg_disable_delay_ddl_recycle(''''' || param1 || ''''',false) '' ');
END;
$$
;

create or replace FUNCTION cbm_get_changed_block()
RETURNS table (merged_start_lsn text,merged_end_lsn text,tablespace_oid oid,database_oid oid, relfilenode oid, fork_number int,path text,rel_dropped bool,rel_created bool, rel_truncated bool,truncate_blocknum oid,changed_block_number oid, changed_block_list text)
LANGUAGE plpgsql
AS
$$
DECLARE
        param1  text;
        param2 text;
BEGIN
        EXECUTE('EXECUTE DIRECT ON (datanode1) ''SELECT pg_cbm_tracked_location()'' ') into param1;
        TRUNCATE cbmtst1;
        CREATE TABLE cbmtst2 (x INT);
        INSERT INTO cbmtst2 SELECT * FROM generate_series(1,1000);
        CREATE TABLE cbmtst3 (y INT);
        INSERT INTO cbmtst3 SELECT * FROM cbmtst2;
        CHECKPOINT;
        EXECUTE('EXECUTE DIRECT ON (datanode1) ''SELECT pg_cbm_tracked_location()'' ') into param2;

        return query EXECUTE('EXECUTE DIRECT ON (datanode1) ''SELECT * FROM pg_cbm_get_changed_block(''''' || param1 || ''''',''''' || param2 || ''''')  limit 1 '' ');
END;
$$
;


DROP TABLE IF EXISTS cbmtst;
DROP TABLE IF EXISTS cbmtst1;
DROP TABLE IF EXISTS cbmtst2;
DROP TABLE IF EXISTS cbmtst3;
DROP TABLE IF EXISTS timestst1;
DROP TABLE IF EXISTS timestst2;
DROP TABLE IF EXISTS timestst3;
CREATE TABLE timestst1 (lsn text);
CREATE TABLE timestst2 (lsn text);
CREATE TABLE timestst3 (lsn text);


set xc_maintenance_mode=on;

--pg_cbm_tracked_location
EXECUTE DIRECT ON (datanode1) 'INSERT INTO timestst1 SELECT pg_cbm_tracked_location()';

--pg_cbm_get_merged_file
CREATE TABLE cbmtst (i INT);
INSERT INTO cbmtst SELECT * FROM generate_series(1,1000);
CREATE TABLE cbmtst1 (j INT);
INSERT INTO cbmtst1 SELECT * FROM cbmtst;
CHECKPOINT;
EXECUTE DIRECT ON (datanode1) 'INSERT INTO timestst2 SELECT pg_cbm_tracked_location()';
EXECUTE DIRECT ON (datanode1) 'SELECT * FROM timestst1';
EXECUTE DIRECT ON (datanode1) 'SELECT * FROM timestst2';
EXECUTE DIRECT ON (datanode1) 'SELECT pg_cbm_get_merged_file(timestst1.lsn,timestst2.lsn) FROM timestst1 LEFT JOIN timestst2 ON 1=1';
--pG_cbm_get_changed_block
SELECT cbm_get_changed_block();
--pg_cbm_recycle_file
EXECUTE DIRECT ON (datanode1) 'SELECT pg_cbm_recycle_file(lsn) FROM timestst2';

--pg_cbm_force_track
DROP TABLE cbmtst2;
DROP TABLE cbmtst3;
CHECKPOINT;
EXECUTE DIRECT ON (datanode1) 'SELECT pg_cbm_tracked_location()';
EXECUTE DIRECT ON (datanode1) 'INSERT INTO timestst3 SELECT pg_current_xlog_location()';
EXECUTE DIRECT ON (datanode1) 'SELECT pg_cbm_force_track(lsn,10000) FROM timestst3';
EXECUTE DIRECT ON (datanode1) 'SELECT pg_cbm_tracked_location()';

EXECUTE DIRECT ON (datanode1) 'SELECT pg_enable_delay_ddl_recycle()';
EXECUTE DIRECT ON (datanode1) 'SELECT pg_enable_delay_xlog_recycle()';
select disable_delay_ddl_recycle();
EXECUTE DIRECT ON (datanode1) 'SELECT pg_disable_delay_xlog_recycle()';

--clean
DROP TABLE IF EXISTS cbmtst;
DROP TABLE IF EXISTS cbmtst1;
DROP TABLE IF EXISTS cbmtst2;
DROP TABLE IF EXISTS cbmtst3;
DROP TABLE IF EXISTS timestst1;
DROP TABLE IF EXISTS timestst2;
DROP TABLE IF EXISTS timestst3;
DROP FUNCTION IF EXISTS disable_delay_ddl_recycle;
DROP FUNCTION IF EXISTS cbm_get_changed_block;
