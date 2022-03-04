/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 *  rto_statistic.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/rto_statistic.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <unistd.h>
#include "utils/elog.h"
#include "utils/builtins.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "instruments/instr_waitevent.h"
#include "replication/rto_statistic.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"

Datum node_name()
{
    return CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
}

void rto_get_standby_info_text(char *info, uint32 max_info_len)
{
    errno_t errorno;
    bool show_line = false;
    errorno = snprintf_s(info, max_info_len, max_info_len - 1, "%-30s%-20s%-20s%-20s", "standby_node_name",
                         "current_rto", "target_rto", "current_sleep_time");
    securec_check_ss(errorno, "", "");

    for (int i = 0; i < g_instance.attr.attr_storage.max_wal_senders; ++i) {
        if (strstr(g_instance.rto_cxt.rto_standby_data[i].id, "hadr_") != NULL) {
            continue;
        }
        if (strlen(g_instance.rto_cxt.rto_standby_data[i].id) == 0) {
            if (show_line == false) {
                errorno = snprintf_s(info + strlen(info), max_info_len - strlen(info), max_info_len - strlen(info) - 1,
                                     "\n%-30s%-20s%-20s%-20s", "", "", "", "");
                securec_check_ss(errorno, "", "");
                show_line = true;
            }
            continue;
        }

        errorno = snprintf_s(info + strlen(info), max_info_len - strlen(info), max_info_len - strlen(info) - 1,
                             "\n%-30s%-20lu%-20u%-20lu", g_instance.rto_cxt.rto_standby_data[i].id,
                             g_instance.rto_cxt.rto_standby_data[i].current_rto, u_sess->attr.attr_storage.target_rto,
                             g_instance.rto_cxt.rto_standby_data[i].current_sleep_time);
        securec_check_ss(errorno, "\0", "\0");
        show_line = true;
    }
}

Datum rto_get_standby_info()
{
    Datum value;
    const uint32 RTO_INFO_BUFFER_SIZE = 2048 * (1 + g_instance.attr.attr_storage.max_wal_senders);
    char *info = (char *)palloc0(sizeof(char) * RTO_INFO_BUFFER_SIZE);
    rto_get_standby_info_text(info, RTO_INFO_BUFFER_SIZE);
    value = CStringGetTextDatum(info);
    pfree_ext(info);
    return value;
}

#ifndef ENABLE_MULTIPLE_NODES
RTOStandbyData *GetDCFRTOStat(uint32 *num)
{
    RTOStandbyData *result =
        (RTOStandbyData *)palloc((int64)(DCF_MAX_NODE_NUM) * sizeof(RTOStandbyData));
    int i;
    int rc;
    int readDCFNode = 0;

    for (i = 0; i < DCF_MAX_NODE_NUM; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile DCFStandbyInfo *nodeinfo = &t_thrd.dcf_cxt.dcfCtxInfo->nodes_info[i];
        if (nodeinfo->isMember) {
            char *standby_names = (char *)(result[readDCFNode].id);
            rc = strncpy_s(standby_names, IP_LEN, g_instance.rto_cxt.dcf_rto_standby_data[i].id,
                           strlen(g_instance.rto_cxt.dcf_rto_standby_data[i].id));
            securec_check(rc, "\0", "\0");
            if (strstr(standby_names, "hadr_") != NULL) {
                continue;
            }
            char *local_ip = (char *)(result[readDCFNode].source_ip);
            rc = strncpy_s(local_ip, IP_LEN, (char *)g_instance.rto_cxt.dcf_rto_standby_data[i].source_ip,
                           strlen((char *)g_instance.rto_cxt.dcf_rto_standby_data[i].source_ip));
            securec_check(rc, "\0", "\0");

            char *remote_ip = (char *)(result[readDCFNode].dest_ip);
            rc = strncpy_s(remote_ip, IP_LEN, (char *)g_instance.rto_cxt.dcf_rto_standby_data[i].dest_ip,
                           strlen((char *)g_instance.rto_cxt.dcf_rto_standby_data[i].dest_ip));
            securec_check(rc, "\0", "\0");

            result[readDCFNode].source_port = g_instance.rto_cxt.dcf_rto_standby_data[i].source_port;
            result[readDCFNode].dest_port = g_instance.rto_cxt.dcf_rto_standby_data[i].dest_port;
            result[readDCFNode].current_rto = g_instance.rto_cxt.dcf_rto_standby_data[i].current_rto;
            result[readDCFNode].current_sleep_time = g_instance.rto_cxt.dcf_rto_standby_data[i].current_sleep_time;
            result[readDCFNode].target_rto = u_sess->attr.attr_storage.target_rto;
            readDCFNode++;
        }
    }

    *num = readDCFNode;
    return result;
}
#endif
RTOStandbyData *GetRTOStat(uint32 *num)
{
    RTOStandbyData *result =
        (RTOStandbyData *)palloc((int64)(g_instance.attr.attr_storage.max_wal_senders) * sizeof(RTOStandbyData));
    int i;
    int rc;
    int readWalSnd = 0;

    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile WalSnd *walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];
        SpinLockAcquire(&walsnd->mutex);
        if (walsnd->pid != 0) {
            char *standby_names = (char *)(result[readWalSnd].id);
            rc = strncpy_s(standby_names, IP_LEN, g_instance.rto_cxt.rto_standby_data[i].id,
                           strlen(g_instance.rto_cxt.rto_standby_data[i].id));
            securec_check(rc, "\0", "\0");
            if ((strstr(standby_names, "hadr_") != NULL) || (strstr(standby_names, "hass") != NULL)) {
                SpinLockRelease(&walsnd->mutex);
                continue;
            }

            char *local_ip = (char *)(result[readWalSnd].source_ip);
            rc = strncpy_s(local_ip, IP_LEN, (char *)walsnd->wal_sender_channel.localhost,
                           strlen((char *)walsnd->wal_sender_channel.localhost));
            securec_check(rc, "\0", "\0");

            char *remote_ip = (char *)(result[readWalSnd].dest_ip);
            rc = strncpy_s(remote_ip, IP_LEN, (char *)walsnd->wal_sender_channel.remotehost,
                           strlen((char *)walsnd->wal_sender_channel.remotehost));
            securec_check(rc, "\0", "\0");

            result[readWalSnd].source_port = walsnd->wal_sender_channel.localport;
            result[readWalSnd].dest_port = walsnd->wal_sender_channel.remoteport;
            result[readWalSnd].current_rto = g_instance.rto_cxt.rto_standby_data[i].current_rto;

            if (u_sess->attr.attr_storage.target_rto == 0) {
                result[readWalSnd].current_sleep_time = 0;
            } else {
                result[readWalSnd].current_sleep_time = g_instance.rto_cxt.rto_standby_data[i].current_sleep_time;
            }
            result[readWalSnd].target_rto = u_sess->attr.attr_storage.target_rto;
            readWalSnd++;
        }
        SpinLockRelease(&walsnd->mutex);
    }

    *num = readWalSnd;
    return result;
}

HadrRTOAndRPOData *HadrGetRTOStat(uint32 *num)
{
    HadrRTOAndRPOData *result =
        (HadrRTOAndRPOData *)palloc((int64)(g_instance.attr.attr_storage.max_wal_senders) * sizeof(HadrRTOAndRPOData));
    int i;
    int rc;
    int readWalSnd = 0;

    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile WalSnd *walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];
        SpinLockAcquire(&walsnd->mutex);
        if (walsnd->pid != 0 && ((strstr(g_instance.rto_cxt.rto_standby_data[i].id, "hadr_") != NULL) ||
            (strstr(g_instance.rto_cxt.rto_standby_data[i].id, "hass") != NULL))) {
            char *standby_names = (char *)(result[readWalSnd].id);
            rc = strncpy_s(standby_names, IP_LEN, g_instance.rto_cxt.rto_standby_data[i].id,
                           strlen(g_instance.rto_cxt.rto_standby_data[i].id));
            securec_check(rc, "\0", "\0");

            char *local_ip = (char *)(result[readWalSnd].source_ip);
            rc = strncpy_s(local_ip, IP_LEN, (char *)walsnd->wal_sender_channel.localhost,
                           strlen((char *)walsnd->wal_sender_channel.localhost));
            securec_check(rc, "\0", "\0");

            char *remote_ip = (char *)(result[readWalSnd].dest_ip);
            rc = strncpy_s(remote_ip, IP_LEN, (char *)walsnd->wal_sender_channel.remotehost,
                           strlen((char *)walsnd->wal_sender_channel.remotehost));
            securec_check(rc, "\0", "\0");

            result[readWalSnd].source_port = walsnd->wal_sender_channel.localport;
            result[readWalSnd].dest_port = walsnd->wal_sender_channel.remoteport;
            result[readWalSnd].current_rto = g_instance.rto_cxt.rto_standby_data[i].current_rto;
            result[readWalSnd].current_rpo = walsnd->log_ctrl.current_RPO < 0 ? 0 : walsnd->log_ctrl.current_RPO;

            if (u_sess->attr.attr_storage.hadr_recovery_time_target == 0) {
                result[readWalSnd].rto_sleep_time = 0;
            } else {
                result[readWalSnd].rto_sleep_time = g_instance.rto_cxt.rto_standby_data[i].current_sleep_time;
            }
            if (u_sess->attr.attr_storage.hadr_recovery_point_target == 0) {
                result[readWalSnd].rpo_sleep_time = 0;
            } else {
                result[readWalSnd].rpo_sleep_time = g_instance.streaming_dr_cxt.rpoSleepTime;
            }
            result[readWalSnd].target_rto = u_sess->attr.attr_storage.hadr_recovery_time_target;
            result[readWalSnd].target_rpo = u_sess->attr.attr_storage.hadr_recovery_point_target;
            readWalSnd++;
        }
        SpinLockRelease(&walsnd->mutex);
    }

    *num = readWalSnd;
    return result;
}

/* redo statistic view */
const RTOStatsViewObj g_rtoViewArr[RTO_VIEW_COL_SIZE] = {
    { "node_name", TEXTOID, node_name },
    { "rto_info", TEXTOID, rto_get_standby_info }
};
