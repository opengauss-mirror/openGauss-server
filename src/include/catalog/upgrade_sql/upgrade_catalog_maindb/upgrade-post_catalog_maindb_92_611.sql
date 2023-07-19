SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5169;
CREATE OR REPLACE FUNCTION pg_catalog.gs_validate_ext_listen_ip(clear cstring, validate_node_name cstring, validate_ip cstring, OUT pid bigint, OUT node_name text)
RETURNS SETOF record
LANGUAGE internal
STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
AS 'gs_validate_ext_listen_ip';