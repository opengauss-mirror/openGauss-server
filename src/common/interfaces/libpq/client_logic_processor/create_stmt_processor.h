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
 * create_stmt_processor.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\create_stmt_processor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CREATE_STMT_PROCESSOR_H
#define CREATE_STMT_PROCESSOR_H

#include "libpq-fe.h"
#include "stdint.h"
#include "nodes/parsenodes_common.h"
#include "client_logic_cache/icached_columns.h"

typedef struct PGClientLogicParams PGClientLogicParams;
typedef struct StatementData StatementData;

class ExprPartsList;
class RawValue;

class createStmtProcessor {
public:
    static bool run_pre_create_statement(const CreateStmt * const stmt, StatementData *statementData);
    
    static RawValue *add_cl_column_type(const ColumnDef * const column, StatementData *statement_data,
        ICachedColumn *cached_column, const int location);
    static RawValue *trans_column_definition(const ColumnDef * const column_def, ExprPartsList *expr_vec,
        ICachedColumns *cached_columns, ICachedColumns *cached_columns_for_defaults, StatementData *statement_data,
        const char *column_key_name, bool &error);
    static bool check_constraint(Constraint *constraint, const Oid type_id, char *name, ICachedColumns *cached_columns,
        StatementData* statement_data);
    static bool transform_expr(Node *expr, char *name, ICachedColumns *cached_columns, StatementData* statement_data);
    static char *get_column_name(ColumnRef *column);
    static bool process_column_defintion(ColumnDef *column, Node *element, ExprPartsList *expr_vec,
        ICachedColumns *cached_columns, ICachedColumns *cached_columns_for_defaults, StatementData *statement_data);
    static bool check_distributeby(const DistributeBy *distributeby, const char *colname,
        StatementData* statement_data);
};

#endif
