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
 * prepared_statement.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\prepared_statement.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "prepared_statement.h"
#include "client_logic_cache/cached_columns.h"
#include "client_logic_fmt/gs_copy.h"

PreparedStatement::PreparedStatement()
    : cached_params(NULL),
      cached_copy_columns(NULL),
      partial_csv_column(NULL),
      partial_csv_column_size(0),
      partial_csv_column_allocated(0),
      copy_state(NULL),
      original_data_types_oids(NULL),
      original_data_types_oids_size(0),
      cacheRefresh(CacheRefreshType::CACHE_NONE),
      m_function_name("") {};

PreparedStatement::~PreparedStatement()
{
    if (cached_params != NULL) {
        delete (CachedColumns *)cached_params;
    }
    if (cached_copy_columns != NULL) {
        delete (CachedColumns *)cached_copy_columns;
    }
    libpq_free(original_data_types_oids);
    libpq_free(partial_csv_column);
    delete_copy_state(copy_state);
}
