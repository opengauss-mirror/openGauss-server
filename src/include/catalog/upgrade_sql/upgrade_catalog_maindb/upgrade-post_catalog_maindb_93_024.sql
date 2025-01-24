set local inplace_upgrade_next_system_object_oids = IUO_PROC, 3148;
CREATE OR REPLACE FUNCTION pg_catalog.pg_terminate_active_session_socket
(IN threadid BIGINT,
  IN sessionid BIGINT)
RETURNS bool LANGUAGE INTERNAL NOT FENCED as 'pg_terminate_active_session_socket' STRICT;
