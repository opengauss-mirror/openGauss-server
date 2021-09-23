/* -------------------------------------------------------------------------
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * cached_type.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_type.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cached_type.h"
#include "icached_column_manager.h"
#include "icached_column.h"
#include "cached_columns.h"
CachedType::CachedType(const Oid typid, const char* typname, const char* schema_name, const char* dbname,
    const ICachedColumnManager* cached_column_manager)
    : m_typid(typid), m_cached_column_manager(cached_column_manager), m_cached_columns(NULL)
{
    init(typname, schema_name, dbname);
}
void CachedType::init(const char* typname, const char* schema_name, const char* dbname)
{
    m_typname = typname ? strdup(typname) : NULL;
    m_schema_name = schema_name ? strdup(schema_name) : NULL;
    m_dbname = dbname ? strdup(dbname) : NULL;
    if (m_cached_columns == NULL) {
        m_cached_columns = new(std::nothrow) CachedColumns();
        if (m_cached_columns == NULL) {
            printf("out of memory\n");
            exit(EXIT_FAILURE);
        }
    }
    if (m_cached_column_manager != NULL) {
        m_cached_column_manager->get_cached_columns(m_dbname, m_schema_name, m_typname, m_cached_columns);
    }
    init();
}

void CachedType::init()
{
    if (m_original_ids == NULL) {
        m_original_ids = (int*)malloc(get_num_processed_args() * sizeof(int));
        if (m_original_ids == NULL) {
            printf("out of memory\n");
            exit(EXIT_FAILURE);
        }
        for (size_t i = 0; i < get_num_processed_args(); i++) {
            m_original_ids[i] = get_original_id(i);
        }
    }
}
CachedType::~CachedType()
{
    libpq_free(m_typname);
    libpq_free(m_schema_name);
    libpq_free(m_dbname);
    delete m_cached_columns;
    if (m_original_ids) {
        libpq_free(m_original_ids);
        m_original_ids = NULL;
    }
}

const bool CachedType::get_type_columns(ICachedColumns* cached_columns) const
{
    return m_cached_column_manager->get_cached_columns(m_dbname, m_schema_name, m_typname, cached_columns);
}


const Oid CachedType::get_original_id(const size_t idx) const
{
    if (idx >= m_cached_columns->size() || m_cached_columns->at(idx) == NULL) {
        return InvalidOid;
    }
    return m_cached_columns->at(idx)->get_origdatatype_oid();
}
