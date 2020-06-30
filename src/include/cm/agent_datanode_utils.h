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
 * agent_datanode_utils.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/agent_datanode_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef AGENT_DATANODE_UTILS_H
#define AGENT_DATANODE_UTILS_H

#include "cm/cm_c.h"
#include "cm/cm_msg.h"
#include "cm/agent_main.h"
#include "replication/replicainternal.h"
#include "libpq/libpq-fe.h"

#define close_and_reset_connection(conn) \
    if (conn != NULL) {                  \
        closePGconn(conn);               \
        freePGconn(conn);                \
        conn = NULL;                     \
    }

#define CLOSE_CONNECTION(con)            \
    do {                                 \
        close_and_reset_connection(con); \
        assert(NULL == con);             \
        return -1;                       \
    } while (0)

const int MAXCONNINFO = 1024;
extern PGconn* DN_conn[CM_MAX_DATANODE_PER_NODE];
extern THR_LOCAL PGconn* g_Conn;

extern void check_parallel_redo_status_by_file(
    agent_to_cm_datanode_status_report* report_msg, uint32 ii, char* redo_state_path);
extern int check_datanode_status_by_SQL0(agent_to_cm_datanode_status_report* report_msg, uint32 ii);
extern int check_datanode_status_by_SQL1(agent_to_cm_datanode_status_report* report_msg, uint32 ii);
extern int check_datanode_status_by_SQL2(agent_to_cm_datanode_status_report* report_msg, uint32 ii);
extern int check_datanode_status_by_SQL3(agent_to_cm_datanode_status_report* report_msg, uint32 ii);
extern int check_datanode_status_by_SQL4(agent_to_cm_datanode_status_report* report_msg, uint32 ii);
extern void check_datanode_status_by_SQL5(agent_to_cm_datanode_status_report* report_msg, uint32 ii, char* data_path);
extern int check_datanode_status_by_SQL6(
    agent_to_cm_datanode_status_report* report_msg, uint32 ii, const char* data_path);
extern int cmagent_execute_query(PGconn* db_connection, const char* run_command);
extern int cmagent_execute_query_for_lock(PGconn* db_connection, const char* run_command);

extern int cmagent_to_coordinator_connect(const char* pid_path);
extern uint32 find_cn_active_info_index(agent_to_cm_coordinate_status_report* report_msg, uint32 coordinatorId);
extern int is_cn_connect_ok(uint32 coordinatorId);
extern int datanode_rebuild_reason_enum_to_int(HaRebuildReason reason);
extern PGconn* get_connection(const char* pid_path, bool isCoordinater = false);
extern LocalMaxLsnMng g_LMLsn[CM_MAX_DATANODE_PER_NODE];
#endif /*AGENT_DATANODE_UTILS_H */
