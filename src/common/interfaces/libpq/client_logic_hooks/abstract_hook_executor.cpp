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
 * abstract_hook_executor.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\abstract_hook_executor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <algorithm>
#include "abstract_hook_executor.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "securec.h"


AbstractHookExecutor::AbstractHookExecutor(const char *function_name)
    : m_allowed_values(NULL), m_allowed_values_size(0), ref_count(0)
{
    const char *funcname = (function_name != NULL) ? function_name : "";
    check_strncpy_s(strncpy_s(m_function_name, sizeof(m_function_name), funcname, strlen(funcname)));
}

bool AbstractHookExecutor::add_argument(const char *key, const char *value)
{
    bool ret = true;
    unsigned char *unescaped_data(NULL);
    size_t unescapedDataSize(0);

    unescaped_data = PQunescapeBytea((unsigned char *)value, &unescapedDataSize);
    if (unescapedDataSize == 0) {
        if (strcmp(value, "\\x") != 0) {
            fprintf(stderr, "something went wrong in PQunescapeBytea of %s \t %s\n", key, value);
            if (unescaped_data != NULL) {
                PQfreemem(unescaped_data);
                unescaped_data = NULL;
            }
            return false;
        }
    }

    /* change key to lowercase */
    char *key_curr = (char *)key;
    size_t key_size = strlen(key);
    for (size_t i = 0; i < key_size; ++i) {
        key_curr[i] = tolower(key[i]);
    }

    /* add key/value to list */
    m_values_map.set(key_curr, (const char *)unescaped_data, unescapedDataSize);
    if (unescaped_data) {
        PQfreemem(unescaped_data);
        unescaped_data = NULL;
    }
    save_private_variables();
    return ret;
}

bool AbstractHookExecutor::get_argument(const char *key, const char **value, size_t &valueSize) const
{
    size_t i = 0;
    for (; i < m_allowed_values_size; i++) {
        if (pg_strncasecmp(m_allowed_values[i], key, strlen(m_allowed_values[i])) == 0) {
            break;
        }
    }
    if (i == m_allowed_values_size) {
        return false;
    }
    *value = m_values_map.find(key, &valueSize);
    if (*value) {
        return true;
    }
    return false;
}

void AbstractHookExecutor::add_allowed_value(const char * const value)
{
    m_allowed_values = (char **)libpq_realloc(m_allowed_values, m_allowed_values_size * sizeof(const char *),
        (1 + m_allowed_values_size) * sizeof(const char *));
    if (m_allowed_values == NULL) {
        return;
    }
    m_allowed_values[m_allowed_values_size] = strndup(value, NAMEDATALEN + 1);
    m_allowed_values_size++;
}

void AbstractHookExecutor::inc_ref_count()
{
    ref_count += 1;
}
void AbstractHookExecutor::dec_ref_count()
{
    Assert(ref_count != 0);
    ref_count -= 1;
}
const bool AbstractHookExecutor::safe_to_remove() const
{
    return ref_count == 0;
}
