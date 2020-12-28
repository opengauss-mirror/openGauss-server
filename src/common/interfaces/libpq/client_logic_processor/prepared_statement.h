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
 * prepared_statement.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\prepared_statement.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PREPARED_STATEMENT_H
#define PREPARED_STATEMENT_H

#include "client_logic_cache/cache_refresh_type.h"
#include "libpq-fe.h"
#include "postgres_fe.h"
#include "client_logic_processor/raw_values_list.h"
#include "client_logic/cstrings_map.h"
#include "client_logic_cache/icached_columns.h"

typedef unsigned int Oid;
/*
 * every query is run in the context of a prepared statement.
 * Whether it is a named prepared statement or an unnamed statement created automatically.
 * We use the PreparedStatement context for the following purposes:
 * 1. trace an Execute statement to its original Prepare statement
 * 2. trace a response to its original request - we wait for the successful response from the server to triggers events
 * based on the queries parsed
 * 3. in the COPY processing, we save the state on the PreparedStatement
 */

typedef CStringsMap StringArgs;
struct ICachedColumns;
typedef struct CopyStateData CopyStateData;

class PreparedStatement {
private:
    static const int m_FUNCTION_NAME = 256;

public:
    PreparedStatement();
    ~PreparedStatement();

    ICachedColumns *cached_params;      /* params for PQexecPrepared and EXECUTE */
    ICachedColumns *cached_copy_columns; /* columns for COPY FROM STDIN */
    char *partial_csv_column;
    size_t partial_csv_column_size;
    size_t partial_csv_column_allocated;
    CopyStateData *copy_state;
    Oid *original_data_types_oids;
    size_t original_data_types_oids_size;

    CacheRefreshType cacheRefresh; /* frontend side effects of a successful DDL */

    char m_function_name[m_FUNCTION_NAME];
    StringArgs m_string_args;
};

#endif