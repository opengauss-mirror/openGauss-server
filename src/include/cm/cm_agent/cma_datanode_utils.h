/**
 * @file cma_datanode_utils.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-01
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMA_DATANODE_UTILS_H
#define CMA_DATANODE_UTILS_H

#include "libpq/libpq-int.h"
#include "libpq/libpq-fe.h"
#include "cma_main.h"


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

#define CLEAR_AND_CLOSE_CONNECTION(node_result, con) \
    do {                                             \
        PQclear(node_result);                        \
        close_and_reset_connection(con);             \
        assert(NULL == con);                         \
        return -1;                                   \
    } while (0)

#ifdef ENABLE_MULTIPLE_NODES
int GetAllDatabaseInfo(int index, DNDatabaseInfo **dnDatabaseInfo, int *dnDatabaseCount);
int GetDBTableFromSQL(int index, uint32 databaseId, uint32 tableId, uint32 tableIdSize,
                      DNDatabaseInfo *dnDatabaseInfo, int dnDatabaseCount, char* databaseName, char* tableName);
#endif
int cmagent_execute_query_and_check_result(PGconn* db_connection, const char* run_command);
int cmagent_execute_query(PGconn* db_connection, const char* run_command);

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

extern int check_datanode_status_by_SQL7(agent_to_cm_datanode_status_report* report_msg,
    uint32 ii, agent_to_cm_coordinate_barrier_status_report* barrier_info);
extern int check_datanode_status_by_SQL8(agent_to_cm_datanode_status_report* report_msg,
    uint32 ii, agent_to_cm_coordinate_barrier_status_report* barrier_info);
extern int CheckDatanodeStatusBySqL9(agent_to_cm_datanode_status_report* report_msg,
    uint32 ii, agent_to_cm_coordinate_barrier_status_report* barrier_info);
extern int CheckDatanodeSyncList(AgentToCmserverDnSyncList *syncListMsg, uint32 ii);
extern int cmagent_execute_query(PGconn* db_connection, const char* run_command);
extern int cmagent_execute_query_and_check_result(PGconn* db_connection, const char* run_command);

extern int cmagent_to_coordinator_connect(const char* pid_path);
uint32 find_cn_active_info_index(agent_to_cm_coordinate_status_report_old* report_msg, uint32 coordinatorId);
extern int is_cn_connect_ok(uint32 coordinatorId);
extern int datanode_rebuild_reason_enum_to_int(HaRebuildReason reason);
extern PGconn* get_connection(const char* pid_path, bool isCoordinater = false, int connectTimeOut = 5);
extern LocalMaxLsnMng g_LMLsn[CM_MAX_DATANODE_PER_NODE];
extern bool isUpgradeCluster();

#endif 
