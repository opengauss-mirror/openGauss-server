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

#define MEM_MAX_ID_LENGTH 48
#define MEM_INVALID_NODE_ID ("")
#define MEM_TOPOLOGY_MAX_HOSTS 16
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

typedef enum RackMemErrorCode : int {
    HOK = 0,
    E_CODE_INVALID_PAR = 1000,
    E_CODE_MEMLIB_IPC = 1001,
    E_CODE_AGENT_RPC = 1002,
    E_CODE_MASTER_RPC = 1003,
    E_CODE_MEM_NOT_READY = 1004,
    E_CODE_MEMLIB = 1005,
    E_CODE_AGENT = 1006,
    E_CODE_MANAGER = 1007,
    E_CODE_RESOURCE_EXIST = 1008,
    E_CODE_RESOURCE_NOT_CREATE = 1009,
    E_CODE_RESOURCEATTACH = 1010,
    E_CODE_STRATEGY_ERROR = 1011,
    E_CODE_OBMM_OP_ERROR = 1012,
    E_CODE_SMAP_OP_ERROR = 1013,
    E_CODE_SCBUS_DAEMON = 1014,
    E_CODE_OPEN_FILE = 1015,
    E_CODE_MMAP_FILE = 1016,
    E_CODE_NULLPTR = 1017,
    E_CODE_SERIALIZE_DESERIALIZE_ERROR = 1018,
    E_CODE_CRC_CHECK_ERROR = 1019
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

const char* ErrCodeToStr(int errNum);

#ifdef __cplusplus
}
#endif

#endif
