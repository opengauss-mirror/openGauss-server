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
 * abstract_hook_executor.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\abstract_hook_executor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ABSTRACT_HOOK_EXECUTOR_H
#define ABSTRACT_HOOK_EXECUTOR_H

#include <string.h>
#include "client_logic/cstrings_map.h"
#include "libpq/libpq-fe.h"

/*
 * This class provides a basic framework for both the ColumnHookExecutor and the GlobalHookExecutor
 * at the momment, it's only an helper for adding arguments and getting arguments from/to the class instance
 */
class AbstractHookExecutor {
public:
    explicit AbstractHookExecutor(const char *function_name);
    bool add_argument(const char *key, const char *value);
    bool get_argument(const char *key, const char **value, size_t &valueSize) const;
    virtual ~AbstractHookExecutor() = 0;
    virtual void inc_ref_count();
    virtual void dec_ref_count();
    const bool safe_to_remove() const;

protected:
    char **m_allowed_values;
    size_t m_allowed_values_size;
    CStringsMap m_values_map;
    virtual void save_private_variables() = 0;
    static const int m_FUNCTION_NAME_MAX_SIZE = 256;
    void add_allowed_value(const char * const);
    size_t ref_count;

public:
    char m_function_name[m_FUNCTION_NAME_MAX_SIZE];
};

inline AbstractHookExecutor::~AbstractHookExecutor()
{
    for (size_t i = 0; i < m_allowed_values_size; i++) {
        libpq_free(m_allowed_values[i]);
    }
    libpq_free(m_allowed_values);
}

#endif
