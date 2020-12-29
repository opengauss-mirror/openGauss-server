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
 * expr_parts_list.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_expressions\expr_parts_list.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef EXPR_PARTS_LIST_H
#define EXPR_PARTS_LIST_H

#include "c.h"

struct ExprParts;

class ExprPartsList {
public:
    ExprPartsList();
    ~ExprPartsList();
    bool add(ExprParts *expr_parts);
    size_t size() const;

    /* returns a pointer to a single object */
    const ExprParts *at(size_t position) const;
    void clear();
    bool empty() const;

private:
    ExprParts *m_expr_parts_list;
    size_t m_expr_parts_list_size;
};

#endif