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
 * cl_state.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\cl_state.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "libpq-int.h"
#include "cl_state.h"
#include "client_logic_cache/types_to_oid.h"
#include "client_logic_hooks/hook_resource.h"
#include "client_logic_processor/raw_values_list.h"
#include "client_logic_processor/prepared_statements_list.h"
#include "client_logic_cache/cache_loader.h"

PGClientLogic::PGClientLogic(PGconn *conn)
    : m_conn(conn),
      enable_client_encryption(false),
      disable_once(false),
      preparedStatements(NULL),
      pendingStatements(NULL),
#if ((!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS)))
      query_type(CE_IGNORE),
#endif
      droppedSchemas(NULL),
      droppedSchemas_size(0),
      droppedSchemas_allocated(0),
      droppedGlobalSettings(NULL),
      droppedGlobalSettings_size(0),
      droppedGlobalSettings_allocated(0),
      droppedColumnSettings(NULL),
      droppedColumnSettings_size(0),
      droppedColumnSettings_allocated(0),
      cacheRefreshType(CacheRefreshType::ALL),
      m_hookResources(),
      val_to_update(updateGucValues::GUC_NONE)
{
    m_lastResultStatus = PGRES_EMPTY_QUERY;
    rawValuesForReplace = new RawValuesList();
    preparedStatements = new PreparedStatementsList;
    pendingStatements = new PreparedStatementsList;
    TypesMap::fill_types_map();
    lastStmtName[0] = '\0';
#if ((!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS)))
    query_args[0] = '\0';
#endif
}

PGClientLogic::~PGClientLogic()
{
    if (rawValuesForReplace) {
        delete rawValuesForReplace;
        rawValuesForReplace  = NULL;
    }
    for (auto it: m_hookResources) {
        if (it.second != NULL) {
            delete it.second;
            it.second = NULL;
        }
    }
    if (preparedStatements) { 
        delete preparedStatements;
        preparedStatements = NULL;
    }
    if (pendingStatements) { 
        delete pendingStatements;
        pendingStatements = NULL;
    }
    libpq_free(droppedGlobalSettings);
    libpq_free(droppedColumnSettings);
    libpq_free(droppedSchemas);
    CacheLoader::get_instance().clear();
}
