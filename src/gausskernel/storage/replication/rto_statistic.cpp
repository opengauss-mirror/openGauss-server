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

void rto_get_standby_info_text(char* info, uint32 max_info_len)
{
    errno_t errorno;
    bool show_line = false;
    errorno = snprintf_s(info,
        max_info_len,
        max_info_len - 1,
        "%-30s%-20s%-20s%-20s",
        "standby_node_name",
        "current_rto",
        "target_rto",
        "current_sleep_time");
    securec_check_ss(errorno, "", "");

    for (int i = 0; i < g_instance.attr.attr_storage.max_wal_senders; ++i) {
        if (strlen(g_instance.rto_cxt.rto_standby_data[i].id) == 0) {
            if (show_line == false) {
                errorno = snprintf_s(info + strlen(info),
                    max_info_len - strlen(info),
                    max_info_len - strlen(info) - 1,
                    "\n%-30s%-20s%-20s%-20s",
                    "",
                    "",
                    "",
                    "");
                securec_check_ss(errorno, "", "");
                show_line = true;
            }
            continue;
        }

        errorno = snprintf_s(info + strlen(info),
            max_info_len - strlen(info),
            max_info_len - strlen(info) - 1,
            "\n%-30s%-20lu%-20u%-20lu",
            g_instance.rto_cxt.rto_standby_data[i].id,
            g_instance.rto_cxt.rto_standby_data[i].current_rto,
            g_instance.rto_cxt.rto_standby_data[i].target_rto,
            g_instance.rto_cxt.rto_standby_data[i].current_sleep_time);
        securec_check_ss(errorno, "", "");
        show_line = true;
    }
}

Datum rto_get_standby_info()
{
    Datum value;
    char* info = (char*)palloc0(sizeof(char) * RTO_INFO_BUFFER_SIZE);
    rto_get_standby_info_text(info, RTO_INFO_BUFFER_SIZE);
    value = CStringGetTextDatum(info);
    pfree_ext(info);
    return value;
}

RTOStandbyData* GetRTOStat(uint32* num)
{
    RTOStandbyData* result =
        (RTOStandbyData*)palloc((int64)(g_instance.attr.attr_storage.max_wal_senders) * sizeof(RTOStandbyData));
    int i;
    int rc;
    int readWalSnd = 0;

    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        if (g_instance.rto_cxt.rto_standby_data[i].id[0] != 0) {
            readWalSnd++;
            char* standby_names = (char*)(result[readWalSnd - 1].id);
            rc = strncpy_s(standby_names,
                STANDBY_NAME_SIZE,
                g_instance.rto_cxt.rto_standby_data[i].id,
                strlen(g_instance.rto_cxt.rto_standby_data[i].id));
            securec_check(rc, "", "");

            char* local_ip = (char*)(result[readWalSnd - 1].source_ip);
            rc = strncpy_s(local_ip,
                IP_LEN,
                g_instance.rto_cxt.rto_standby_data[i].source_ip,
                strlen(g_instance.rto_cxt.rto_standby_data[i].source_ip));
            securec_check(rc, "", "");
            result[readWalSnd - 1].source_port = g_instance.rto_cxt.rto_standby_data[i].source_port;

            char* remote_ip = (char*)(result[readWalSnd - 1].dest_ip);
            rc = strncpy_s(remote_ip,
                IP_LEN,
                g_instance.rto_cxt.rto_standby_data[i].dest_ip,
                strlen(g_instance.rto_cxt.rto_standby_data[i].dest_ip));
            securec_check(rc, "", "");

            result[readWalSnd - 1].dest_port = g_instance.rto_cxt.rto_standby_data[i].dest_port;

            result[readWalSnd - 1].current_rto = g_instance.rto_cxt.rto_standby_data[i].current_rto;
            result[readWalSnd - 1].current_sleep_time = g_instance.rto_cxt.rto_standby_data[i].current_sleep_time;
            result[readWalSnd - 1].target_rto = g_instance.rto_cxt.rto_standby_data[i].target_rto;
        }
    }

    *num = readWalSnd;
    return result;
}

/* redo statistic view */
const RTOStatsViewObj g_rtoViewArr[RTO_VIEW_COL_SIZE] = {
    {"node_name", TEXTOID, node_name}, {"rto_info", TEXTOID, rto_get_standby_info}};
