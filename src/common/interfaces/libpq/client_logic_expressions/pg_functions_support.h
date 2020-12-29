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
 * pg_functions_support.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_expressions\pg_functions_support.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef PG_FUNCTIONS_SUPPORT_H
#define PG_FUNCTIONS_SUPPORT_H

/* forward declerations */
struct FuncCall;

typedef struct StatementData StatementData;
class ExprPartsList;
bool handle_func_call(const FuncCall *funccall, ExprPartsList *expr_parts_list, StatementData *statement_data);

#endif