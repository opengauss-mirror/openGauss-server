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
 * cached_columns.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_columns.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cached_columns.h"
#include "cached_column.h"
#include "libpq-fe.h"
#include "libpq-int.h"

/* initial a empty CacheColumns */
CachedColumns::CachedColumns(bool order, bool dummies, bool is_cached_params)
    : m_is_in_scheme_order(order),
      m_is_to_process(false),
      m_columns(NULL),
      m_columns_size(0),
      m_dummies(dummies),
      m_is_cached_params(is_cached_params)
{}

/* delete CacheColumns */
CachedColumns::~CachedColumns()
{
    if (m_dummies) {
        /*
         * if the container was created for handling creating columns
         * which means it holds "dummny" columns that we create only for handling
         * columns that are not in server catalog tables yes
         */
        for (size_t i = 0; i < m_columns_size; i++) {
            delete m_columns[i]; // safe to delete
        }
    } else if (m_is_cached_params) {
        for (size_t i = 0; i < m_columns_size; i++) {
            if (m_columns[i]) {
                const_cast<ICachedColumn *>(m_columns[i])->set_use_in_prepare(false);
                if (!m_columns[i]->is_in_prepare() && !m_columns[i]->is_in_columns_list()) {
                    delete m_columns[i]; // safe to delete
                }
            }
        }
    }
    
    libpq_free(m_columns);
}

void CachedColumns::set_order(bool order)
{
    m_is_in_scheme_order = order;
}

size_t CachedColumns::size() const
{
    return m_columns_size;
}

size_t CachedColumns::not_null_size() const
{
    size_t re = 0;
    for (size_t i = 0; i < m_columns_size; i++) {
        if (m_columns[i] != NULL) {
            re++;
        }
    }
    return re;
}

const ICachedColumn *CachedColumns::at(size_t pos) const
{
    if (pos >= m_columns_size) {
        return NULL;
    }
    return m_columns[pos];
}

/*
 * push CacheColumns to CacheColumns
 * and assigning an address for new CacheColumns
 */
bool CachedColumns::push(const ICachedColumn *cached_column)
{
    m_columns = (const ICachedColumn **)libpq_realloc(m_columns, sizeof(*m_columns) * m_columns_size,
        sizeof(*m_columns) * (m_columns_size + 1));
    if (!m_columns) {
        return false;
    }
    m_columns[m_columns_size] = cached_column;
    ++m_columns_size;

    if (cached_column) {
        m_is_to_process = true;
    }

    return true;
}

/*
 * add a CacheColumn to CacheColumns at index
 * and assigning an address for new CacheColumn
 */
bool CachedColumns::set(size_t index, const ICachedColumn *cached_column)
{
    if (m_columns_size < index + 1) {
        m_columns = (const ICachedColumn **)libpq_realloc(m_columns, sizeof(*m_columns) * m_columns_size,
            sizeof(*m_columns) * (index + 1));
        if (!m_columns) {
            return false;
        }

        /* pad with NULLs all unused space */
        for (size_t i = m_columns_size; i < index; ++i) {
            m_columns[i] = NULL;
        }

        /* update size */
        m_columns_size = index + 1;
    }

    m_columns[index] = cached_column;
    if (cached_column) {
        m_is_to_process = true;
    }
    return true;
}

bool CachedColumns::is_empty() const
{
    return (m_columns_size == 0);
}

bool CachedColumns::is_to_process() const
{
    return (!is_empty() && m_is_to_process);
}

/*
 * append a CacheColumn to CacheColumns at the end
 * and assigning an address for new CacheColumn
 */
void CachedColumns::append(const ICachedColumns *other)
{
    if (!other || other->is_empty()) {
        return;
    }
    m_columns = (const ICachedColumn **)libpq_realloc(m_columns, sizeof(*m_columns) * m_columns_size,
        sizeof(*m_columns) * (m_columns_size + other->size()));
    if (!m_columns) {
        return;
    }
    for (size_t i = 0; i < other->size(); ++i) {
        m_columns[m_columns_size + i] = other->at(i);
    }
    m_columns_size += other->size();

    if (other->is_to_process()) {
        m_is_to_process = true;
    }

    if (!other->is_in_scheme_order()) {
        m_is_in_scheme_order = false;
    }
}

/* whether the list is ordered according to the original table order or is accorded according to the current DML query
 */
bool CachedColumns::is_in_scheme_order() const
{
    return m_is_in_scheme_order;
}
void CachedColumns::set_is_in_scheme_order(bool value)
{
    m_is_in_scheme_order = value;
}

/* A number that divides into another without a remainder. */
bool CachedColumns::is_divisor(size_t value) const
{
    return ((value % size()) == 0);
}
