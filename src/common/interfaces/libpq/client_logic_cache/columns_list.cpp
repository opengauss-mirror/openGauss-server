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
 * columns_list.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\columns_list.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "columns_list.h"
#include "cached_column.h"
#include "libpq-int.h"

ColumnsList::ColumnsList() : m_columns_list(NULL), m_columns_list_size(0) {}
ColumnsList::~ColumnsList()
{
    clear();
}

void ColumnsList::clear()
{
    for (size_t i = 0; i < m_columns_list_size; ++i) {
        if (!m_columns_list[i]->is_in_prepare()) {
            delete m_columns_list[i];
            m_columns_list[i] = NULL;
        } else {
            m_columns_list[i]->remove_from_columnslist();
        }
    }
    libpq_free(m_columns_list);
    m_columns_list_size = 0;
}

bool ColumnsList::add(CachedColumn *cached_column)
{
    m_columns_list = (CachedColumn **)libpq_realloc(m_columns_list, sizeof(*m_columns_list) * m_columns_list_size,
        sizeof(*m_columns_list) * (m_columns_list_size + 1));
    if (m_columns_list == NULL) {
        return false;
    }
    cached_column->set_flag_columnslist();
    m_columns_list[m_columns_list_size] = cached_column;
    ++m_columns_list_size;
    return true;
}

const CachedColumn *ColumnsList::get_by_oid(Oid table_oid, unsigned int column_position) const
{
    for (size_t i = 0; i < m_columns_list_size; ++i) {
        if (m_columns_list[i]->get_table_oid() == table_oid && m_columns_list[i]->get_col_idx() == column_position) {
            return m_columns_list[i];
        }
    }
    return NULL;
}

const CachedColumn* ColumnsList::get_by_oid(Oid oid) const
{
    for (size_t i = 0; i < m_columns_list_size; ++i) {
        if (m_columns_list[i]->get_oid() == oid) {
            return m_columns_list[i];
        }
    }
    return NULL;
}

const CachedColumn **ColumnsList::get_by_schema(const char *schema_name, size_t &columns_list_size) const
{
    const CachedColumn **columns_list(NULL);
    columns_list_size = 0;
    for (size_t i = 0; i < m_columns_list_size; ++i) {
        if (pg_strcasecmp(m_columns_list[i]->get_schema_name(), schema_name) == 0) {
            columns_list = (const CachedColumn **)libpq_realloc(columns_list, sizeof(*columns_list) * columns_list_size,
                sizeof(*columns_list) * (columns_list_size + 1));
            if (columns_list == NULL) {
                return NULL;
            }
            columns_list[columns_list_size] = m_columns_list[i];
            ++columns_list_size;
        }
    }
    return columns_list;
}

const CachedColumn *ColumnsList::get_by_details(const char *database_name, const char *schema_name,
    const char *table_name, const char *column_name) const
{
    for (size_t i = 0; i < m_columns_list_size; ++i) {
        if ((pg_strcasecmp(m_columns_list[i]->get_catalog_name(), database_name) == 0) &&
            (pg_strcasecmp(m_columns_list[i]->get_schema_name(), schema_name) == 0) &&
            (pg_strcasecmp(m_columns_list[i]->get_table_name(), table_name) == 0) &&
            (pg_strcasecmp(m_columns_list[i]->get_col_name(), column_name) == 0)) {
            return m_columns_list[i];
        }
    }
    return NULL;
}

const CachedColumn **ColumnsList::get_by_details(const char *database_name, const char *schema_name,
    const char *table_name, size_t &columns_list_size) const
{
    const CachedColumn **columns_list(NULL);
    columns_list_size = 0;
    for (size_t i = 0; i < m_columns_list_size; ++i) {
        if ((pg_strcasecmp(m_columns_list[i]->get_catalog_name(), database_name) == 0) &&
            (pg_strcasecmp(m_columns_list[i]->get_schema_name(), schema_name) == 0) &&
            (pg_strcasecmp(m_columns_list[i]->get_table_name(), table_name) == 0)) {
            columns_list = (const CachedColumn **)libpq_realloc(columns_list, sizeof(*columns_list) * columns_list_size,
                sizeof(*columns_list) * (columns_list_size + 1));
            if (columns_list == NULL) {
                return NULL;
            }
            columns_list[columns_list_size] = m_columns_list[i];
            ++columns_list_size;
        }
    }
    return columns_list;
}


bool ColumnsList::empty() const
{
    return (m_columns_list_size == 0);
}
