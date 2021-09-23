/* Add built-in function gs_comm_proxy_thread_status */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1995;
CREATE OR REPLACE FUNCTION gs_comm_proxy_thread_status(
OUT ProxyThreadId int8,
OUT ProxyCpuAffinity text,
OUT ThreadStartTime text,
OUT RxPckNums int8,
OUT TxPckNums int8,
OUT RxPcks float8,
OUT TxPcks float8)
RETURNS SETOF RECORD LANGUAGE INTERNAL STABLE ROWS 100 STRICT as 'gs_comm_proxy_thread_status';

CREATE VIEW gs_comm_proxy_thread_status AS
    SELECT DISTINCT * from gs_comm_proxy_thread_status();