#!/bin/sh

main()
{
    dsscmd_bin=$1
    dss_home=$2
    inst_count=$3
    last_id=`expr $inst_count - 1`

    export DSS_HOME=${dss_home}

    for inst_id in `seq 0 $last_id`
    do
        ${dsscmd_bin} mkdir +log${inst_id} pg_xlog${inst_id} UDS:${dss_home}/.dss_unix_d_socket
        ${dsscmd_bin} mkdir +log${inst_id}/pg_xlog${inst_id} archive_status UDS:${dss_home}/.dss_unix_d_socket
        ${dsscmd_bin} cp +data/pg_xlog${inst_id}/000000010000000000000001 +log${inst_id}/pg_xlog${inst_id}/000000010000000000000001 UDS:${dss_home}/.dss_unix_d_socket

        ${dsscmd_bin} rm +data/pg_xlog${inst_id}/000000010000000000000001 UDS:${dss_home}/.dss_unix_d_socket
        ${dsscmd_bin} rmdir +data/pg_xlog${inst_id}/archive_status UDS:${dss_home}/.dss_unix_d_socket
        ${dsscmd_bin} rmdir +data/pg_xlog${inst_id} UDS:${dss_home}/.dss_unix_d_socket
        ${dsscmd_bin} ln +log${inst_id}/pg_xlog${inst_id} +data/pg_xlog${inst_id} UDS:${dss_home}/.dss_unix_d_socket

        ${dsscmd_bin} mkdir +log${inst_id} pg_doublewrite${inst_id} UDS:${dss_home}/.dss_unix_d_socket
        ${dsscmd_bin} cp +data/pg_doublewrite${inst_id}/pg_dw_meta +log${inst_id}/pg_doublewrite${inst_id}/pg_dw_meta UDS:${dss_home}/.dss_unix_d_socket
        ${dsscmd_bin} cp +data/pg_doublewrite${inst_id}/pg_dw_0 +log${inst_id}/pg_doublewrite${inst_id}/pg_dw_0 UDS:${dss_home}/.dss_unix_d_socket
        ${dsscmd_bin} cp +data/pg_doublewrite${inst_id}/pg_dw_single +log${inst_id}/pg_doublewrite${inst_id}/pg_dw_single UDS:${dss_home}/.dss_unix_d_socket
        ${dsscmd_bin} rmdir +data/pg_doublewrite${inst_id} -r UDS:${dss_home}/.dss_unix_d_socket
        ${dsscmd_bin} ln +log${inst_id}/pg_doublewrite${inst_id} +data/pg_doublewrite${inst_id} UDS:${dss_home}/.dss_unix_d_socket

        ${dsscmd_bin} ls +data/pg_xlog${inst_id} UDS:${dss_home}/.dss_unix_d_socket
        ${dsscmd_bin} ls +data/pg_doublewrite${inst_id} UDS:${dss_home}/.dss_unix_d_socket
    done
}

main $@