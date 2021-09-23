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
 * func_processor.h
 *
 * IDENTIFICATION
 * src\common\interfaces\libpq\client_logic_processor\func_processor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef FUNC_PROCESSOR_H
#define FUNC_PROCESSOR_H
#include <cstddef>
#include "nodes/parsenodes_common.h"
#include "func_hardcoded_values.h"

class StatementData;
struct CreateFunctionStmt;
struct List;
class ExprPartsList;
class func_processor {
public:
    static bool run_pre_create_function_stmt(const List* options, StatementData* statement_data,
        bool is_do_stmt = false);
    static bool process(const ExprPartsList* target_expr_vec, StatementData* statement_data);
};
#endif