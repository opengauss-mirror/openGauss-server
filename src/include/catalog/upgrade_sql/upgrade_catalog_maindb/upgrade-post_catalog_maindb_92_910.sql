DROP FUNCTION IF EXISTS pg_catalog.dss_io_stat() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6990;
CREATE FUNCTION pg_catalog.dss_io_stat
(
    int4,
    OUT read_kilobyte_per_sec   int8,
    OUT write_kilobyte_per_sec   int8,
    OUT io_times  int4
)
RETURNS record LANGUAGE INTERNAL as 'dss_io_stat';