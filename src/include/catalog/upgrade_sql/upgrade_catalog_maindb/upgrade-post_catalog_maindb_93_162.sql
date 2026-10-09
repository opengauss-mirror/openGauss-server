/*------ builtin functions for dbe_perf OS net dev views ------*/
DROP FUNCTION IF EXISTS pg_catalog.pg_os_net_dev_ext() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3993;
CREATE FUNCTION pg_catalog.pg_os_net_dev_ext
(
OUT interface_name pg_catalog.text,
OUT ip_address pg_catalog.text,
OUT link_speed_mbps pg_catalog.int4,
OUT dev_type pg_catalog.text
)
RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 10 as 'pg_os_net_dev_ext';

DROP FUNCTION IF EXISTS pg_catalog.pg_os_net_dev_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3994;
CREATE FUNCTION pg_catalog.pg_os_net_dev_info
(
OUT interface_name pg_catalog.text,
OUT rx_bytes pg_catalog.int8,
OUT rx_packets pg_catalog.int8,
OUT rx_errors pg_catalog.int8,
OUT rx_dropped pg_catalog.int8,
OUT rx_fifo pg_catalog.int8,
OUT rx_frame pg_catalog.int8,
OUT rx_multicast pg_catalog.int8,
OUT tx_bytes pg_catalog.int8,
OUT tx_packets pg_catalog.int8,
OUT tx_errors pg_catalog.int8,
OUT tx_dropped pg_catalog.int8,
OUT tx_fifo pg_catalog.int8,
OUT tx_colls pg_catalog.int8,
OUT tx_carrier pg_catalog.int8
)
RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 10 as 'pg_os_net_dev_info';

/*------ dbe_perf OS net dev views: os_net_dev_info ------*/
CREATE OR REPLACE VIEW DBE_PERF.os_net_dev_info AS
  SELECT info.interface_name,ext.ip_address,
      info.rx_bytes, info.rx_packets, info.rx_errors, info.rx_dropped, info.rx_fifo, info.rx_frame, info.rx_multicast,
      info.tx_bytes, info.tx_packets, info.tx_errors, info.tx_dropped, info.tx_fifo, info.tx_colls, info.tx_carrier,
      ext.link_speed_mbps,ext.dev_type
  FROM pg_os_net_dev_info() info LEFT JOIN pg_os_net_dev_ext() ext ON info.interface_name = ext.interface_name;

CREATE OR REPLACE VIEW DBE_PERF.global_os_net_dev_info AS
  SELECT node_name,net_dev.* FROM dbe_perf.node_name,dbe_perf.os_net_dev_info net_dev;

GRANT SELECT ON dbe_perf.os_net_dev_info TO PUBLIC;
GRANT SELECT ON dbe_perf.global_os_net_dev_info TO PUBLIC;
