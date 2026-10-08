-- OS net device statistics: builtin functions and dbe_perf views
SELECT * FROM pg_catalog.pg_os_net_dev_info() LIMIT 0;
SELECT * FROM pg_catalog.pg_os_net_dev_ext() LIMIT 0;
SELECT * FROM dbe_perf.os_net_dev_info LIMIT 0;
SELECT * FROM dbe_perf.global_os_net_dev_info LIMIT 0;
-- the loopback interface always exists
SELECT count(*) > 0 AS has_loopback FROM dbe_perf.os_net_dev_info WHERE interface_name = 'lo';
SELECT COALESCE(bool_and(rx_bytes >= 0 AND tx_bytes >= 0), true) AS counters_valid FROM dbe_perf.os_net_dev_info;
