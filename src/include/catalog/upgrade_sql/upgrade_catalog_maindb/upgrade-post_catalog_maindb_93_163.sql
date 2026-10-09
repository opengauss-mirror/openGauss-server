/*------ builtin functions for dbe_perf OS disk view ------*/
DROP FUNCTION IF EXISTS pg_catalog.pg_os_disk_io_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3992;
CREATE FUNCTION pg_catalog.pg_os_disk_io_info
(
OUT major_number pg_catalog.int4,
OUT minor_number pg_catalog.int4,
OUT device_name pg_catalog.text,
OUT total_reads pg_catalog.int8,
OUT merge_read_num pg_catalog.int8,
OUT sector_read pg_catalog.int8,
OUT read_time_ms pg_catalog.int8,
OUT total_writes pg_catalog.int8,
OUT merge_write_num pg_catalog.int8,
OUT sector_write pg_catalog.int8,
OUT write_time_ms pg_catalog.int8,
OUT now_io_request pg_catalog.int8,
OUT time_inout_op_ms pg_catalog.int8,
OUT time_inout_opwei_ms pg_catalog.int8,
OUT discard_complete pg_catalog.int8,
OUT merge_discard_num pg_catalog.int8,
OUT sector_discard pg_catalog.int8,
OUT discard_time_ms pg_catalog.int8,
OUT sector_size pg_catalog.int4
)
RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 10 as 'pg_os_disk_io_info';

/*------ dbe_perf OS disk views: os_disk_io_info ------*/
CREATE OR REPLACE VIEW DBE_PERF.os_disk_io_info AS
  SELECT * FROM pg_os_disk_io_info();

CREATE OR REPLACE VIEW DBE_PERF.global_os_disk_io_info AS
  SELECT node_name,os_disk_io_info.* FROM dbe_perf.node_name,dbe_perf.os_disk_io_info;

GRANT SELECT ON dbe_perf.os_disk_io_info TO PUBLIC;
GRANT SELECT ON dbe_perf.global_os_disk_io_info TO PUBLIC;
