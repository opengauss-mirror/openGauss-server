/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * rack_mem_def.h
 *        routines to support RackMemory
 *
 *
 * IDENTIFICATION
 *        src/include/storage/rack_mem_def.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef RACK_MEM_DEF_H
#define RACK_MEM_DEF_H
#include <cstring>

#ifdef __cplusplus
extern "C" {
#endif

#define MEM_MAX_ID_LENGTH 1024
#define MEM_INVALID_NODE_ID ("")
#define MEM_TOPOLOGY_MAX_HOSTS 32
#define MEM_MAX_NUMA_NUM_PER_ITEM 2
#define MEM_TOPOLOGY_MAX_TOTAL_NUMAS 16

typedef enum RackMemPerfLevel {
    L0,
    L1,
    L2
} PerfLevel;

struct NodeBorrowMemInfo {
    uint64_t availBorrowMemSize;
    uint64_t borrowedMemSize;
    uint64_t availLendMemSize;
    uint64_t lentMemSize;
};

typedef enum RackMemErrorCode : uint16_t {
    HOK = 0,
    INIT_ERROR,
    E_CODE_MEMLIB,
    E_CODE_UNKOWN_ERROR,

    /* INIT_ERROR */
    E_CODE_THREADPOOL_INIT_FAIL = 10,
    E_CODE_REGISTER_OP_CODE,
    E_CODE_REGISTER_HANDLER_NULL,
    E_CODE_RM_NODE_DATA_INT_ERROR,
    E_CODE_IPC_INIT_FAIL,
    E_CODE_OS_EVENT_INIT_FAIL,

    /* E_CODE_INVALID_PAR */
    E_CODE_INVALID_PAR = 20,
    E_CODE_SHM_REGIONS_INVALID,

    /* NET_ERROR */
    E_CODE_MESSAGE = 30,
    E_CODE_SERIALIZE_DESERIALIZE_ERROR,
    E_CODE_CRC_CHECK_ERROR,
    E_CODE_IPC_SYNC_CALL,
    E_CODE_IPC_RECEIVE_DATA_NULL,
    E_CODE_RPC_SYNC_CALL,
    E_CODE_RPC_RECEIVE_DATA_NULL,
    E_CODE_HCOM_INNER_SYNC_CALL,
    E_CODE_NET_REPLY,

    /* CONF_ERROR */
    E_CODE_CONF_INIT_FAIL = 40,
    E_CODE_CONF_GET_ERROR,
    E_CODE_CONF_SET_ERROR,

    /* NO_MEM_ERROR */
    E_CODE_NULLPTR = 50,
    E_CODE_NO_MEMORY,

    /* RM_ERROR */
    E_CODE_MANAGER = 60,
    E_CODE_NO_META,
    E_CODE_DATA_STORE,
    E_CODE_MANAGER_UPDATE_META_DATA,
    E_CODE_UPDATE_BORROW_ACCOUNT,
    E_CODE_MANAGER_NAME_EXIST,
    E_CODE_MANAGER_NAME_NOT_EXIST,
    E_CODE_MANAGER_DELETE_FAIL,
    E_CODE_UPDATE_NUMA_ACCOUNT_STATUS_ERR,
    E_CODE_IDLOC_TO_INDEXLOC_ERR,
    E_CODE_INDEX_TO_ID_ERR,
    E_CODE_GET_LEFT_SHM_SIZE,
    E_CODE_MANAGER_GET_META_DATA,
    E_CODE_TOPOLOGY_INIT_ERROR,

    /* RM_AGENT_ERROR */
    E_CODE_AGENT = 80,
    E_CODE_AGENT_UPDATE_META_DATA,
    E_CODE_AGENT_GET_META_DATA,
    E_CODE_AGENT_NAME_EXIST,
    E_CODE_AGENT_NAME_NOT_EXIST,
    E_CODE_AGENT_LOCAL_INSPECTION,
    E_CODE_OPEN_FILE,

    /* SERVER_ERROR */
    E_CODE_WRONG_USER_FAIL = 90,
    E_CODE_POSIX_IO_ERR,

    /* OBMM_ERROR */
    E_CODE_OBMM_EXPORT_ERROR = 100,
    E_CODE_OBMM_UNEXPORT_ERROR,
    E_CODE_OBMM_IMPORT_ERROR,
    E_CODE_OBMM_UNIMPORT_ERROR,
    E_CODE_OBMM_EVENT_MESSAGE,
    E_CODE_OBMM_ERROR,
    E_CODE_OBMM_GET_MESSAGE,

    /* SMAP_ERROR */
    E_CODE_SMAP_GET_PID_ERROR = 110,
    E_CODE_SMAP_OUT,
    E_CODE_SMAP_BACK,
    E_CODE_SMAP_INIT,
    E_CODE_SMAP_ERROR,

    /* STRATEGY_ERROR */
    E_CODE_INIT_STRATEGY = 120,
    E_CODE_STRATEGY_ERROR,
    E_CODE_STRATEGY_RESULT_INVALID,
} RmErrorCode;

typedef enum RackMemQueryType {
    QUERY_OBMM_CLEAR_CONF = 0,
    QUERY_MEM_NUMA_STATUS,
    QUERY_WATER_MARK_STATUS,
    QUERY_MEM_ACCOUNT,
    QUERY_SHM_ACCOUNT,
    QUERY_MEM_NODE_INFO,
} QueryType;

void DestroyRackMemLib();

/* Whether to enable application borrowing and sharing performance statistics */
void EnableRackMemHtrace();

#ifdef __cplusplus
}
#endif

#endif
