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
 * -------------------------------------------------------------------------
 *
 * cl_state.h
 *
 * IDENTIFICATION
 *	  src\include\libpq\cl_state.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CL_STATE_H
#define CL_STATE_H

/* CLIENT LOGIC STATE */
#include "guc_vars.h"
#include <memory>
#include <unordered_map>
#include "client_logic_cache/cache_refresh_type.h"
#include "libpq-fe.h"
#include "postgres_fe.h"
#include "client_logic_processor/raw_values_list.h"
#if ((!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS)))
#include "client_logic/client_logic_enums.h"
#endif

typedef struct pg_conn PGconn;

struct ICachedColumns;
typedef struct CopyStateData CopyStateData;
class CStringsMap;
typedef CStringsMap StringArgs;
class StatementData;

class HookResource;
typedef struct objectFqdn {
    char data[NAMEDATALEN * 4];
} ObjectFqdn;

class PreparedStatementsList;

/*
    the PGClientLogic class maintains the per session state machine related to the Client Logic feature
*/
class PGClientLogic {
public:
    PGClientLogic(PGconn *conn);
    ~PGClientLogic();
    PGconn* m_conn;
    bool enable_client_encryption;
    bool disable_once;
    PreparedStatementsList *preparedStatements;
    PreparedStatementsList *pendingStatements;

    char lastStmtName[NAMEDATALEN];
#if ((!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS)))
    /* 
     * while create cmk, we will do :
     * (1) client : generate cmk key store files
     * (2) server : check operation is allowed, return result
     * (3) client : if server check failed, we will delete the cmk store files generated in step (1) 
     */
    CEQueryType query_type;
    char query_args[MAX_KEY_PATH_VALUE_LEN];
#endif
    ObjectFqdn *droppedSchemas;
    size_t droppedSchemas_size;
    size_t droppedSchemas_allocated;
    ObjectFqdn *droppedGlobalSettings;
    size_t droppedGlobalSettings_size;
    size_t droppedGlobalSettings_allocated;
    ObjectFqdn *droppedColumnSettings;
    size_t droppedColumnSettings_size;
    size_t droppedColumnSettings_allocated;
    ExecStatusType m_lastResultStatus;
    CacheRefreshType cacheRefreshType;
    std::unordered_map<std::string, HookResource *> m_hookResources;

    // GUC params
    GucParams gucParams;
    GucParams tmpGucParams;
    updateGucValues val_to_update;
    RawValuesList *rawValuesForReplace;
};
#endif /* CL_STATE_H */
