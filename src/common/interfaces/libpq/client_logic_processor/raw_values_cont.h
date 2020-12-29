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
 * raw_values_cont.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\raw_values_cont.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RAW_VALUES_CONT_H
#define RAW_VALUES_CONT_H

#include <cstddef>
#include <ctype.h>
#include <unistd.h>
#include <stdint.h>
#include "client_logic_common/statement_data.h"

typedef struct pg_conn PGconn;
struct InsertStmt;
struct SelectStmt;
struct UpdateStmt;
struct ExecuteStmt;
struct A_Expr;

struct ExprParts;
class ExprPartsList;
class RawValuesList;

class RawValues {
public:
    static bool get_raw_values_from_insert_statement(const InsertStmt * const insert_stmt,
        StatementData *statement_data, RawValuesList *raw_values_list);
    static bool get_raw_values_from_update_statement(const UpdateStmt * const update_stmt,
        StatementData *statement_data, RawValuesList *raw_values_list);
    static bool get_raw_values_from_execute_statement(const ExecuteStmt * const execute_stmt,
        StatementData *statement_data, RawValuesList *raw_values_list);
    static bool get_raw_values_from_consts_vec(const ExprPartsList *expr_parts_list, StatementData *statement_data,
        int offset, RawValuesList *raw_values_list);
    static bool get_unprocessed_data(const RawValuesList *raw_values_list, const unsigned char *processed_data,
        unsigned char *unprocessed_data, size_t &size);

private:
    static size_t count_literals(const char *str, size_t size);
    static bool get_cached_default_values(const char *db_name, const char *schema_name, const char *table_name,
        RawValuesList *raw_values_list);
    static bool get_raw_values_from_insert_select_statement(const SelectStmt * const select_stmt,
        StatementData *statement_data, RawValuesList *raw_values_list);
    static bool get_raw_values_from_value_lists(List * const exprList, StatementData *statement_data,
        RawValuesList *raw_values_list);
    static void get_raw_values_from_nodetag(const Value *value, int &end, int loc, const ExprParts *xpr,
        bool &is_emptyb, bool &is_str, const StatementData *statement_data);
    static RawValue *get_raw_values_from_ExprParts(
        const ExprParts *expr_parts, StatementData *statement_data, size_t offset);
};

#endif