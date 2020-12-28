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
 * search_path_list.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\search_path_list.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "search_path_list.h"
#include "libpq-int.h"

SearchPathList::SearchPathList() : m_schemas(NULL), m_schemas_size(0), m_user_schema_index(-1) {}

void SearchPathList::clear()
{
    free(m_schemas);
    m_schemas = NULL;
    m_schemas_size = 0;
    m_user_schema_index = -1;
}

bool SearchPathList::add(const char *schema_name)
{
    if (schema_name == NULL) {
        Assert(false);
        return false;
    }

    if (exists(schema_name)) {
        return true;
    }

    m_schemas = (NameData *)libpq_realloc(m_schemas, sizeof(*m_schemas) * m_schemas_size,
        sizeof(*m_schemas) * (m_schemas_size + 1));
    if (m_schemas == NULL) {
        return false;
    }
    check_strncpy_s(strncpy_s(m_schemas[m_schemas_size].data, sizeof(m_schemas[m_schemas_size].data), schema_name,
        strlen(schema_name)));
    ++m_schemas_size;
    return true;
}

bool SearchPathList::add_user_schema(const char *user_name)
{
    m_user_schema_index = m_schemas_size;
    return add(user_name);
}

void SearchPathList::set_user_schema(const char *user_name)
{
    if (m_user_schema_index == -1) {
        return;
    }
    check_strncpy_s(strncpy_s(m_schemas[m_user_schema_index].data, sizeof(m_schemas[m_user_schema_index].data),
        user_name, strlen(user_name)));
}
size_t SearchPathList::size() const
{
    return m_schemas_size;
}

const char *SearchPathList::at(size_t position) const
{
    if (position < m_schemas_size) {
        return m_schemas[position].data;
    }
    return NULL;
}

bool SearchPathList::exists(const char *schema_name) const
{
    for (size_t i = 0; i < m_schemas_size; ++i) {
        if (pg_strcasecmp(m_schemas[i].data, schema_name) == 0) {
            return true;
        }
    }

    return false;
}
