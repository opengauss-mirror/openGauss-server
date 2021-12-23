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
 * raw_values_cont.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\raw_values_cont.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <iostream>
#include "raw_values_cont.h"
#include "raw_values_list.h"
#include "raw_value.h"
#include "postgres_fe.h"
#include "nodes/pg_list.h"
#include "parser/scanner.h"
#include "client_logic_expressions/expr_processor.h"
#include "client_logic_expressions/expr_parts_list.h"

#define MAX_POSTGRES_INT_CHARS 22
/* count number of escaped characters */
size_t RawValues::count_literals(const char *str, size_t size)
{
    size_t total = 0;
    for (unsigned int i = 0; i < size; i++) {
        if (str[i] == '\'') {
            total++;
        }
    }
    return total / 2;
}

/* no values to process, example query: "INSERT INTO products DEFAULT VALUES;" */
bool RawValues::get_cached_default_values(const char *db_name, const char *schema_name, const char *table_name,
    RawValuesList *raw_values_list)
{
    return true;
}

/* Process INSERT's VALUES portion or UPDATE's SET porition */
bool RawValues::get_raw_values_from_value_lists(List * const expr_list, StatementData *statement_data,
    RawValuesList *raw_values_list)
{
    if (raw_values_list == NULL) {
        Assert(false);
        return false;
    }

    ListCell *expr_iter = NULL;

    /*
     * iterate over expressions, find all constants and add them to the RawValues vector
     * together with all of the extra information needed to process the constants and re-rewrite the query
     */
    foreach (expr_iter, expr_list) {
        Node *node = (Node *)lfirst(expr_iter);
        if (!node) {
            fprintf(stderr, "node is null\n");
            continue;
        }

        ExprWithComma *expr_with_comma(NULL);
        ExprPartsList consts_vector;

        switch (nodeTag(node)) {
            case T_ExprWithComma: {
                expr_with_comma = (ExprWithComma *)node;
            } break;
            case T_ResTarget: {
                ResTarget *res_target = (ResTarget *)node;
                if (nodeTag(res_target->val) != T_ExprWithComma) {
                    fprintf(stderr, "unexpected nodeTag for res_target val\n");
                    break;
                }
                expr_with_comma = (ExprWithComma *)res_target->val;
            } break;
            case T_SetToDefault: {
                raw_values_list->add(NULL);
                break;
            }
            case T_A_Const: {
                A_Const *n = (A_Const *)node;
                ExprParts xp;
                xp.column_ref = NULL;
                xp.aconst = n;
                consts_vector.add(&xp);
                break;
            }
            default:
                fprintf(stderr, "unexpected nodeTag: %d\n", nodeTag(node));
                break;
        }

        /* if we found an Expression, we need now to convert it to Constants */
        if (expr_with_comma) {
            if (!exprProcessor::expand_expr((Node *)expr_with_comma->xpr, statement_data, &consts_vector)) {
                return false;
            }
            if (consts_vector.empty()) {
                raw_values_list->add(NULL);
                continue;
            }
        } else if (consts_vector.empty()) {
            continue;
        }

        if (expr_with_comma != NULL && expr_with_comma->comma_before_loc == 0) {
            expr_with_comma->comma_before_loc = strlen(statement_data->query);
            if (statement_data->query[expr_with_comma->comma_before_loc - 1] == ';') {
                --expr_with_comma->comma_before_loc;
            }
        }
        size_t consts_vector_size = consts_vector.size();
        for (size_t i = 0; i < consts_vector_size; ++i) {
            const ExprParts *expr_parts = consts_vector.at(i);
            RawValue *raw_value = get_raw_values_from_ExprParts(expr_parts, statement_data, 0);
            /* precaution */
            if (!raw_value) {
                continue;
            }
            raw_values_list->add(raw_value);
        }
    }
    list_free(expr_list);
    Assert(!raw_values_list->empty());
    return true;
}

bool RawValues::get_raw_values_from_insert_select_statement(const SelectStmt * const select_stmt,
    StatementData *statement_data, RawValuesList *raw_values_list)
{
    if (raw_values_list == NULL || select_stmt == NULL) {
        return false;
    }
    /*
     * The grammar allows attaching ORDER BY, LIMIT, FOR UPDATE, or WITH to a
     * VALUES clause.  If we have any of those, treat it as a general SELECT;
     * so it will work, but you can't use DEFAULT items together with those.
     */

    bool is_general_select = (
        select_stmt->valuesLists == NULL || select_stmt->sortClause != NULL || select_stmt->limitOffset != NULL ||
        select_stmt->limitCount != NULL || select_stmt->lockingClause != NULL || select_stmt->withClause != NULL);

    if (!is_general_select && select_stmt->valuesLists && list_length(select_stmt->valuesLists) > 1) {
        /* Process INSERT ... VALUES with multiple VALUES */
        ListCell *lc = NULL;
        foreach (lc, select_stmt->valuesLists) {
            List *sublist = (List *)lfirst(lc);
            if (!get_raw_values_from_value_lists(sublist, statement_data, raw_values_list)) {
                return false;
            }
        }
    } else if (select_stmt->valuesLists) {
        List *expr_list = (List *)linitial(select_stmt->valuesLists);
        if (!get_raw_values_from_value_lists(expr_list, statement_data, raw_values_list)) {
            return false;
        }
    }

    return true;
}

void RawValues::get_raw_values_from_nodetag(const Value *value, int &end, int loc, const ExprParts *xpr,
    bool &is_emptyb, bool &is_str, const StatementData *statement_data)
{
    errno_t rc = 0;
    switch (nodeTag(value)) {
        case T_Integer:
            char num_chars[MAX_POSTGRES_INT_CHARS];
            rc = memset_s(num_chars, MAX_POSTGRES_INT_CHARS, 0, MAX_POSTGRES_INT_CHARS);
            securec_check_c(rc, "\0", "\0");
            rc = sprintf_s(num_chars, MAX_POSTGRES_INT_CHARS, "%ld", intVal(value));
            securec_check_ss_c(rc, "\0", "\0");
            end = loc + strlen(num_chars);
            break;
        case T_Null:
            if (xpr->original_size != -1) {
                is_emptyb = true;
                break;
            } else {
                end = loc;
                break;
            }
        case T_String:
            is_str = true;
            if (strlen(strVal(value)) == 0) {
                /* make m_dataSize 0, for not processing this value */
                end = loc;
                break;
            }
            if (xpr->original_size == -1) {
                /* +2 is for the quote signs */
                end = loc + strlen(strVal(value)) + 2;
                end += count_literals(statement_data->query + loc + 1, end - loc - 1);
            }
            break;
        case T_Float:
            end = loc + strlen(strVal(value));
            break;

        default:
            fprintf(stderr, "unknown value tag: %d\n", nodeTag(value));
            break;
    }
}
RawValue *RawValues::get_raw_values_from_ExprParts(const ExprParts *xpr, StatementData *statement_data, size_t offset)
{
    RawValue *raw_value = new (std::nothrow) RawValue(statement_data->conn);
    if (raw_value == NULL) {
        fprintf(stderr, "failed to new RawValue object\n");
        exit(EXIT_FAILURE);
    }
    bool is_str = false;
    bool is_emptyb = false;
    errno_t rc = 0;
    if (xpr->aconst) {
        int loc = xpr->aconst->location;
        /* offset is used in case the query was already rewritten one time so we have to shift everything to the right
         */
        loc += offset;
        int end = 0;
        const Value *value = &(xpr->aconst->val);
        get_raw_values_from_nodetag(value, end, loc, xpr, is_emptyb, is_str, statement_data);
        if (xpr->original_size == -1) {
            raw_value->set_data((unsigned char *)(statement_data->query + loc), end - loc);
        } else {
            if (is_emptyb) {
                char *var = "NULL        ";
                rc = memcpy_s((unsigned char *)(statement_data->query + loc), xpr->original_size + 1, var,
                    xpr->original_size);
                securec_check_c(rc, "\0", "\0");
                raw_value->m_data_size = 0;
            } else if (is_str && strlen(strVal(value)) == 0) {
                char *var = "\'\'";
                size_t var_len = strlen(var);
                rc = memcpy_s((unsigned char *)(statement_data->query + loc), xpr->original_size + 1, var, var_len);
                securec_check_c(rc, "\0", "\0");
                rc = memset_s((unsigned char *)(statement_data->query + loc + var_len),
                    xpr->original_size + 1 - var_len, ' ', xpr->original_size - var_len);
                securec_check_c(rc, "\0", "\0");
                raw_value->m_data_size = 0;
            } else {
                raw_value->set_data((unsigned char *)(statement_data->query + loc), xpr->original_size);
            }
        }
        if (is_str)
            raw_value->set_data_value((const unsigned char *)strVal(value), strlen(strVal(value)));

        raw_value->m_empty_repeat = xpr->is_empty_repeat;

        raw_value->m_location = loc;
        raw_value->m_new_location = loc;
    } else if (xpr->param_ref) {
        /* used for prepared statements */
        size_t param_num = xpr->param_ref->number - 1;
        raw_value->m_is_param = true;
        raw_value->m_location = param_num;
        raw_value->m_new_location = param_num;

        if (param_num >= 0 && param_num < statement_data->nParams) {
            raw_value->set_data((const unsigned char *)statement_data->paramValues[param_num],
                statement_data->paramLengths ? statement_data->paramLengths[param_num] :
                                                strlen(statement_data->paramValues[param_num]));
            raw_value->m_data_value_format = statement_data->paramFormats ? statement_data->paramFormats[param_num] : 0;
        }
    }
    return raw_value;
}
/*
 * iterate over all of the const values in the query
 * and build a vector to be used to process the data and rewrite the original query
 */
bool RawValues::get_raw_values_from_consts_vec(const ExprPartsList *expr_parts_list, StatementData *statement_data,
    int offset, RawValuesList *raw_values_list)
{
    for (size_t i = 0; i < expr_parts_list->size(); i++) {
        raw_values_list->add(get_raw_values_from_ExprParts(expr_parts_list->at(i), statement_data, offset));
    }
    return true;
}

bool RawValues::get_raw_values_from_insert_statement(const InsertStmt * const insert_stmt,
    StatementData *statement_data, RawValuesList *raw_values_list)
{
    if (insert_stmt == nullptr || insert_stmt->relation == nullptr) {
        fprintf(stderr, "null param error\n");
        return false;
    }
    SelectStmt *select_stmt = (SelectStmt *)insert_stmt->selectStmt;
    /* if select statement is empty, then we have "INSERT .... DEFAULT VAULES" */
    if (select_stmt == nullptr) {
        return get_cached_default_values(insert_stmt->relation->catalogname, insert_stmt->relation->schemaname,
            insert_stmt->relation->relname, raw_values_list);
    }
    return get_raw_values_from_insert_select_statement(select_stmt, statement_data, raw_values_list);
}

bool RawValues::get_raw_values_from_update_statement(const UpdateStmt * const update_stmt,
    StatementData *statement_data, RawValuesList *raw_values_list)
{
    if (update_stmt == nullptr || update_stmt->targetList == nullptr) {
        fprintf(stderr, "not yet supported\n");
        return false;
    }
    return get_raw_values_from_value_lists(update_stmt->targetList, statement_data, raw_values_list);
}

bool RawValues::get_raw_values_from_execute_statement(const ExecuteStmt * const execute_stmt,
    StatementData *statement_data, RawValuesList *raw_values_list)
{
    if (execute_stmt == nullptr || execute_stmt->params == nullptr) {
        fprintf(stderr, "not yet supported\n");
        return false;
    }
    return get_raw_values_from_value_lists(execute_stmt->params, statement_data, raw_values_list);
}
bool RawValues::get_unprocessed_data(const RawValuesList *raw_values_list, const unsigned char *processed_data,
    unsigned char *unprocessed_data, size_t &size)
{
    for (size_t i = 0; i < raw_values_list->size(); i++) {
        RawValue *raw_value = raw_values_list->at(i);
        errno_t rc = EOK;
        if (raw_value == NULL || raw_value->m_processed_data == NULL || 
            raw_value->m_processed_data_size <= 3) {
            return false;
        }

        unsigned char offset = raw_value->m_is_param ? 0 : 1;
        int x = strncmp((const char *)raw_value->m_processed_data + offset, (const char *)processed_data,
            raw_value->m_processed_data_size - 3);
        if (x == 0) {
            if (raw_value->m_data[0] == '\'') {
                rc = memcpy_s(unprocessed_data, raw_value->m_data_size - 2, raw_value->m_data + 1,
                    raw_value->m_data_size - 2); /* ignoring quote signs */
                securec_check_c(rc, "\0", "\0");
                size = raw_value->m_data_size - 2; /* removing quotes signs */
            } else {
                rc = memcpy_s(unprocessed_data, raw_value->m_data_size, raw_value->m_data,
                    raw_value->m_data_size); /* ignoring quote signs */
                securec_check_c(rc, "\0", "\0");
                size = raw_value->m_data_size; /* removing quotes signs */
            }
            return true;
        }
    }
    return false;
}
