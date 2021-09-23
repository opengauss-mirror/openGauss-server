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
 * expr_processor.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_expressions\expr_processor.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef EXPR_PROCESSOR_H
#define EXPR_PROCESSOR_H

#include <utility>
#include "nodes/parsenodes_common.h"
#include "client_logic_common/col_full_name.h"
#include "client_logic_cache/icached_column.h"
#include "expr_parts.h"
#include "expr_parts_list.h"
#include "func_name_data.h"

class ExprPartsList;
typedef struct StatementData StatementData;

class ColumnRefData;
class exprProcessor {
public:
    static bool expand_expr(const Node * const expr, StatementData *statement_data, ExprPartsList *expr_parts_list);
    static bool expand_column_ref(const ColumnRef *cref, ColumnRefData &column_ref_data);
    static bool expand_condition_expr(const Node * const expr, ExprPartsList *exprs_list);
    static bool expand_function_name(const FuncCall *callref, func_name_data &func_name);
private:
    static void expand_sub_link(const Node * const expr, StatementData *statement_data);
};

#endif
