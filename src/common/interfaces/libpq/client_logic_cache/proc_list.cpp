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
 * proc_list.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\proc_list.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "proc_list.h"
#include "cached_proc.h"
#include "libpq-int.h"

ProcList::ProcList() : m_proc_list(NULL), m_proc_list_size(0) {}
ProcList::~ProcList()
{
    clear();
}

void ProcList::clear()
{
    for (size_t i = 0; i < m_proc_list_size; ++i) {
        m_proc_list[i]->m_refcount--;
        if (m_proc_list[i]->m_refcount == 0) {
            delete m_proc_list[i];
            m_proc_list[i] = NULL;
        }
    }
    libpq_free(m_proc_list);
    m_proc_list_size = 0;
}

bool ProcList::add(CachedProc* cached_proc)
{
    const int min_alloc_size = 10;
    CachedProc** new_proc_list = NULL;
    cached_proc->m_refcount++;
    if (m_proc_list_size % min_alloc_size == 0) {
        new_proc_list = (CachedProc**)libpq_realloc(m_proc_list, sizeof(*m_proc_list) * m_proc_list_size,
            sizeof(*m_proc_list) * (m_proc_list_size + min_alloc_size));
        if (new_proc_list == NULL) {
            return false;
        }
        m_proc_list = new_proc_list;
    }
    if (m_proc_list == NULL) {
        return false;
    }
    m_proc_list[m_proc_list_size] = cached_proc;
    ++m_proc_list_size;
    return true;
}

const CachedProc* ProcList::get_by_oid(Oid function_oid) const
{
    for (size_t i = 0; i < m_proc_list_size; ++i) {
        if (m_proc_list[i]->m_func_id == function_oid) {
            return m_proc_list[i];
        }
    }
    return NULL;
}

CachedProc* ProcList::get_by_details(const char* database_name, const char* schema_name,
    const char* function_name) const
{
    for (size_t i = 0; i < m_proc_list_size; ++i) {
        if ((pg_strcasecmp(m_proc_list[i]->m_dbname, database_name) == 0) &&
            (pg_strcasecmp(m_proc_list[i]->m_schema_name, schema_name) == 0) &&
            (pg_strcasecmp(m_proc_list[i]->m_proname, function_name) == 0)) {
            return m_proc_list[i];
        }
    }
    return NULL;
}


bool ProcList::empty() const
{
    return (m_proc_list_size == 0);
}

const CachedProc* ProcList::at(size_t i) const
{
    if (i >= m_proc_list_size) {
        return NULL;
    }
    return m_proc_list[i];
}