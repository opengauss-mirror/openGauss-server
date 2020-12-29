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
 * schemas_list.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\schemas_list.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "schemas_list.h"
#include "libpq-int.h"

SchemasList::SchemasList() : m_schemas(NULL), m_schemas_size(0) {}
SchemasList::~SchemasList()
{
    clear();
}
void SchemasList::clear()
{
    libpq_free(m_schemas);
}
bool SchemasList::exists(const char *schema_name) const
{
    for (size_t i = 0; i < m_schemas_size; ++i) {
        if (pg_strcasecmp(m_schemas[i].data, schema_name) == 0) {
            return true;
        }
    }
    return false;
}

bool SchemasList::add(const char *schema_name)
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

bool SchemasList::remove(const char *schema_name)
{
    if (schema_name == NULL) {
        Assert(false);
        return false;
    }

    size_t curr_index(0);
    size_t prev_index(0);
    bool is_found(false);
    for (; curr_index < m_schemas_size; ++curr_index) {
        if (!is_found) {
            /* search for schema to remove */
            if (pg_strcasecmp(m_schemas[curr_index].data, schema_name) == 0) {
                prev_index = curr_index;
                is_found = true;
            }
        } else {
            /* shift elements left in the array */
            m_schemas[prev_index] = m_schemas[curr_index];
            prev_index = curr_index;
        }
    }

    /* resize array */
    if (is_found) {
        if (m_schemas_size == 1) {
            free(m_schemas);
            m_schemas = NULL;
        } else {
            m_schemas = (NameData *)libpq_realloc(m_schemas, sizeof(*m_schemas) * m_schemas_size,
                sizeof(*m_schemas) * (m_schemas_size - 1));
            if (m_schemas == NULL) {
                return false;
            }
        }
        --m_schemas_size;
    }

    return true;
}
