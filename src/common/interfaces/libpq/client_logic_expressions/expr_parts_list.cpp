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
 * expr_parts_list.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_expressions\expr_parts_list.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include "expr_parts_list.h"
#include "expr_parts.h"
#include "libpq-int.h"
#define RESIZE_FACTOR 10

ExprPartsList::ExprPartsList() : m_expr_parts_list(NULL), m_expr_parts_list_size(0) {}
ExprPartsList::~ExprPartsList()
{
    clear();
}

bool ExprPartsList::add(ExprParts *expr_parts)
{
    if (m_expr_parts_list_size % RESIZE_FACTOR == 0) {
        m_expr_parts_list = (ExprParts *)libpq_realloc(m_expr_parts_list, 
            sizeof(*m_expr_parts_list) * m_expr_parts_list_size,
            sizeof(*m_expr_parts_list) * (m_expr_parts_list_size + RESIZE_FACTOR));
    }
    if (m_expr_parts_list == NULL) {
        return false;
    }
    /*
     * we just copy the entire structure at this point. we don't keep the pointer.
     * the structure is small enough for us to this.
     */
    m_expr_parts_list[m_expr_parts_list_size] = (*expr_parts);
    ++m_expr_parts_list_size;
    return true;
}

size_t ExprPartsList::size() const
{
    return m_expr_parts_list_size;
}

const ExprParts *ExprPartsList::at(size_t position) const
{
    if (position >= m_expr_parts_list_size) {
        return NULL;
    }
    return &m_expr_parts_list[position];
}

bool ExprPartsList::empty() const
{
    return (m_expr_parts_list_size == 0);
}

void ExprPartsList::clear()
{
    libpq_free(m_expr_parts_list);
}
