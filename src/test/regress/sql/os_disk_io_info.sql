-- OS disk IO statistics: builtin function and dbe_perf views
SELECT * FROM pg_catalog.pg_os_disk_io_info() LIMIT 0;
SELECT * FROM dbe_perf.os_disk_io_info LIMIT 0;
SELECT * FROM dbe_perf.global_os_disk_io_info LIMIT 0;
-- at least one block device must be reported (the root device always exists)
SELECT count(*) > 0 AS has_devices FROM dbe_perf.os_disk_io_info;
SELECT COALESCE(bool_and(total_reads >= 0 AND total_writes >= 0 AND sector_read >= 0 AND sector_write >= 0), true) AS counters_valid FROM dbe_perf.os_disk_io_info;
