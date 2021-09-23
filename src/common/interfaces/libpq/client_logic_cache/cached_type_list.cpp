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
 * cached_type_list.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_type_list.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cached_type_list.h"
#include "cached_type.h"
#include "libpq-int.h"

CachedTypeList::CachedTypeList() : m_type_list(NULL), m_type_list_size(0) {}
CachedTypeList::~CachedTypeList()
{
    clear();
}

void CachedTypeList::clear()
{
    for (size_t i = 0; i < m_type_list_size; ++i) {
        delete m_type_list[i];
    }
    libpq_free(m_type_list);
    m_type_list_size = 0;
}

bool CachedTypeList::add(CachedType* cached_type)
{
    const int min_alloc_size = 10;
    if (m_type_list_size % min_alloc_size == 0) {
        CachedType** new_type_list = NULL;
        new_type_list = (CachedType**)libpq_realloc(m_type_list, sizeof(*m_type_list) * m_type_list_size,
            sizeof(*m_type_list) * (m_type_list_size + min_alloc_size));
        if (new_type_list == NULL) {
            return false;
        }
        m_type_list = new_type_list;
    }
    m_type_list[m_type_list_size] = cached_type;
    ++m_type_list_size;
    return true;
}

const CachedType* CachedTypeList::get_by_oid(Oid type_oid) const
{
    for (size_t i = 0; i < m_type_list_size; ++i) {
        if (m_type_list[i]->get_oid() == type_oid) {
            return m_type_list[i];
        }
    }
    return NULL;
}


bool CachedTypeList::empty() const
{
    return (m_type_list_size == 0);
}

const CachedType* CachedTypeList::at(size_t i) const
{
    if (i >= m_type_list_size) {
        return NULL;
    }
    return m_type_list[i];
}
