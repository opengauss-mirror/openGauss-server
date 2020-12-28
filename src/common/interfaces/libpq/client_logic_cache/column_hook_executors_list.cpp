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
 * column_hook_executors_list.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\column_hook_executors_list.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "column_hook_executors_list.h"
#include "libpq-int.h"
#include "client_logic_hooks/column_hook_executor.h"
ColumnHookExecutorsList::ColumnHookExecutorsList() : m_column_hook_executors(NULL), m_column_hook_executors_size(0) {}
ColumnHookExecutorsList::~ColumnHookExecutorsList()
{
    for (size_t i = 0; i < m_column_hook_executors_size; ++i) {
        m_column_hook_executors[i]->dec_ref_count();
        if (m_column_hook_executors[i]->safe_to_remove()) {
            delete m_column_hook_executors[i];
        }
    }
    libpq_free(m_column_hook_executors);
}

bool ColumnHookExecutorsList::add(ColumnHookExecutor *column_hook_executor)
{
    if (!column_hook_executor) {
        Assert(false);
        return false;
    }
    column_hook_executor->inc_ref_count();
    m_column_hook_executors = (ColumnHookExecutor **)libpq_realloc(m_column_hook_executors,
        sizeof(*m_column_hook_executors) * m_column_hook_executors_size,
        sizeof(*m_column_hook_executors) * (m_column_hook_executors_size + 1));
    if (!m_column_hook_executors) {
        return false;
    }
    m_column_hook_executors[m_column_hook_executors_size] = column_hook_executor;
    ++m_column_hook_executors_size;
    return true;
}

ColumnHookExecutor *ColumnHookExecutorsList::at(size_t pos) const
{
    if (pos >= m_column_hook_executors_size) {
        return NULL;
    }
    return m_column_hook_executors[pos];
}

size_t ColumnHookExecutorsList::size() const
{
    return m_column_hook_executors_size;
}
