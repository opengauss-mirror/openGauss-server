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
 * expr_processor.cpp
 *	
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_expressions\expr_processor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "stdint.h"
#include "expr_processor.h"
#include "pg_functions_support.h"
#include "client_logic_common/client_logic_utils.h"
#include "client_logic_processor/stmt_processor.h"
#include "expr_parts_list.h"
#include "libpq-int.h"

/*
   get the column's full FQDN, as full as possible, depending on the amount of information in the ColumnRef
   column FQDN: "database"."schema"."table"."column"
   */

ColumnRefData::ColumnRefData()
{
    m_catalog_name.data[0] = '\0';
    m_schema_name.data[0] = '\0';
    m_table_name.data[0] = '\0';
    m_column_name.data[0] = '\0';
    m_alias_fqdn[0] = '\0';
}

ColumnRefData::~ColumnRefData() {}

/*
 * judge Cachecolumn by columnrefdata should include colname, tablename, schemaname and ...
 */
bool ColumnRefData::compare_with_cachecolumn(const ICachedColumn *cached_column) const
{
    if (cached_column) {
        if (cached_column->get_col_name() == NULL || cached_column->get_table_name() == NULL ||
            cached_column->get_schema_name() == NULL || cached_column->get_catalog_name() == NULL) {
            return false;
        }
        if (m_column_name.data[0] != '\0' && pg_strcasecmp(cached_column->get_col_name(), m_column_name.data) != 0) {
            return false;
        }
        if (m_table_name.data[0] != '\0' && pg_strcasecmp(cached_column->get_table_name(), m_table_name.data) != 0) {
            return false;
        }
        if (m_schema_name.data[0] != '\0' && pg_strcasecmp(cached_column->get_schema_name(), m_schema_name.data) != 0) {
            return false;
        }
        if (m_catalog_name.data[0] != '\0' &&
            pg_strcasecmp(cached_column->get_catalog_name(), m_catalog_name.data) != 0) {
            return false;
        }
    } else {
        return false;
    }
    return true;
}

bool exprProcessor::expand_column_ref(const ColumnRef *cref, ColumnRefData &column_ref_data)
{
    size_t fields_len = list_length(cref->fields);
    switch (fields_len) {
        case 1: {
            Node *field1 = (Node *)linitial(cref->fields);

            Assert(IsA(field1, String));
            check_strncpy_s(strncpy_s(column_ref_data.m_column_name.data, sizeof(column_ref_data.m_column_name.data),
                strVal(field1), strlen(strVal(field1))));
            break;
        }
        case 2: {
            Node *field1 = (Node *)linitial(cref->fields);
            Node *field2 = (Node *)lsecond(cref->fields);

            Assert(IsA(field1, String));
            check_strncpy_s(strncpy_s(column_ref_data.m_table_name.data, sizeof(column_ref_data.m_table_name.data),
                strVal(field1), strlen(strVal(field1))));
            Assert(IsA(field2, String));
            check_strncpy_s(strncpy_s(column_ref_data.m_column_name.data, sizeof(column_ref_data.m_column_name.data),
                strVal(field2), strlen(strVal(field2))));
            break;
        }
        case 3: {
            Node *field1 = (Node *)linitial(cref->fields);
            Node *field2 = (Node *)lsecond(cref->fields);
            Node *field3 = (Node *)lthird(cref->fields);

            Assert(IsA(field1, String));
            check_strncpy_s(strncpy_s(column_ref_data.m_schema_name.data, sizeof(column_ref_data.m_schema_name.data),
                strVal(field1), strlen(strVal(field1))));
            Assert(IsA(field2, String));
            check_strncpy_s(strncpy_s(column_ref_data.m_table_name.data, sizeof(column_ref_data.m_table_name.data),
                strVal(field2), strlen(strVal(field2))));
            Assert(IsA(field3, String));
            check_strncpy_s(strncpy_s(column_ref_data.m_column_name.data, sizeof(column_ref_data.m_column_name.data),
                strVal(field3), strlen(strVal(field3))));
            break;
        }
        case 4: {
            Node *field1 = (Node *)linitial(cref->fields);
            Node *field2 = (Node *)lsecond(cref->fields);
            Node *field3 = (Node *)lthird(cref->fields);
            Node *field4 = (Node *)lfourth(cref->fields);

            Assert(IsA(field1, String));
            check_strncpy_s(strncpy_s(column_ref_data.m_catalog_name.data, sizeof(column_ref_data.m_catalog_name.data),
                strVal(field1), strlen(strVal(field1))));
            Assert(IsA(field2, String));
            check_strncpy_s(strncpy_s(column_ref_data.m_schema_name.data, sizeof(column_ref_data.m_schema_name.data),
                strVal(field2), strlen(strVal(field2))));
            Assert(IsA(field3, String));
            check_strncpy_s(strncpy_s(column_ref_data.m_table_name.data, sizeof(column_ref_data.m_table_name.data),
                strVal(field3), strlen(strVal(field3))));
            Assert(IsA(field4, String));
            check_strncpy_s(strncpy_s(column_ref_data.m_column_name.data, sizeof(column_ref_data.m_column_name.data),
                strVal(field4), strlen(strVal(field4))));
            break;
        }
        default:
            Assert(false);
            return false;
            break;
    }

    return concat_col_fqdn(column_ref_data.m_catalog_name.data, column_ref_data.m_schema_name.data,
        column_ref_data.m_table_name.data, column_ref_data.m_column_name.data, column_ref_data.m_alias_fqdn);
}

/*
 *  search for expressions with relevant to Client Logic. relevant expressions add to the xprVec
 *  example: select * from t1 where col1 = 1
 *  "col1" -> column ref
 *  "=" -> operator
 *  "1" -> const
 */
void exprProcessor::expand_sub_link(const Node * const expr, StatementData *statement_data)
{
    switch (((SubLink *)expr)->subLinkType) {
        case EXPR_SUBLINK:
        case EXISTS_SUBLINK:
        case ARRAY_SUBLINK: {
            /* Get column name of the subquery's single target */
            SubLink *sublink = (SubLink *)expr;
            Processor::run_pre_statement(sublink->subselect, statement_data);
        } break;
        /* As with other operator-like nodes, these have no names */
        case ALL_SUBLINK:
        case ANY_SUBLINK: {
            /* Get column name of the subquery's single target */
            SubLink *sublink = (SubLink *)expr;
            Processor::run_pre_statement(sublink->subselect, statement_data);
        } break;
        case ROWCOMPARE_SUBLINK:
        case CTE_SUBLINK:
        default:
            break;
    }
}

bool exprProcessor::expand_expr(const Node * const expr, StatementData *statement_data, ExprPartsList *expr_parts_list)
{
    ExprParts expr_parts;
    if (!expr)
        return true;
    switch (nodeTag(expr)) {
        case T_ParamRef: {
            const ParamRef *param_ref = (ParamRef *)expr;
            expr_parts.column_ref = NULL;
            expr_parts.aconst = NULL;
            expr_parts.param_ref = param_ref;
            expr_parts_list->add(&expr_parts);
            return true;
        }
        case T_A_Const: {
            const A_Const *a_const = (A_Const *)expr;
            ExprParts expr_parts;
            expr_parts.column_ref = NULL;
            expr_parts.aconst = a_const;
            expr_parts_list->add(&expr_parts);
            return true;
        }
        case T_A_Expr: {
            A_Expr *a_expr = (A_Expr *)expr;
            ColumnRef *column_ref = NULL;
            ColumnRef *column_refl = NULL;
            ColumnRef *column_refr = NULL;
            A_Const *a_const = NULL;
            ParamRef *param_ref = NULL;
            switch (a_expr->kind) {
                case AEXPR_IN: {
                    if (a_expr->lexpr) {
                        if (IsA(a_expr->lexpr, ColumnRef)) {
                            column_ref = (ColumnRef *)a_expr->lexpr;
                        } else { /* might be not nessecary at all */
                            if (!expand_expr(a_expr->lexpr, statement_data, expr_parts_list)) {
                            }
                        }

                        if (column_ref) {
                            if (a_expr->rexpr && IsA(a_expr->rexpr, List)) {
                                List *f_list = (List *)a_expr->rexpr;
                                ListCell *fl = NULL;
                                foreach (fl, f_list) {
                                    if (IsA(lfirst(fl), A_Const)) {
                                        ExprParts expr_parts;
                                        expr_parts.column_ref = column_ref;
                                        expr_parts.aconst = (const A_Const * const)lfirst(fl);
                                        expr_parts.param_ref = NULL;
                                        expr_parts.operators = a_expr->name;
                                        expr_parts_list->add(&expr_parts);
                                    } 
                                }
                            }
                        }
                    }
                }
                case AEXPR_OP: {
                    if (a_expr->lexpr) {
                        if (IsA(a_expr->lexpr, ColumnRef)) {
                            column_ref = (ColumnRef *)a_expr->lexpr;
                            column_refl = (ColumnRef *)a_expr->lexpr;
                        } else if (IsA(a_expr->lexpr, A_Const)) {
                            a_const = (A_Const *)a_expr->lexpr;
                        } else if (IsA(a_expr->lexpr, ParamRef)) {
                            param_ref = (ParamRef *)a_expr->lexpr;
                        }
                    }
                    if (a_expr->rexpr) {
                        if (IsA(a_expr->rexpr, ColumnRef)) {
                            column_ref = (ColumnRef *)a_expr->rexpr;
                            column_refr = (ColumnRef *)a_expr->rexpr;
                        } else if (IsA(a_expr->rexpr, A_Const)) {
                            a_const = (A_Const *)a_expr->rexpr;
                        } else if (IsA(a_expr->rexpr, ParamRef)) {
                            param_ref = (ParamRef *)a_expr->rexpr;
                        } else if (IsA(a_expr->rexpr, SubLink)) {
                            expand_sub_link(a_expr->rexpr, statement_data);
                        }
                    }
                    if (column_ref && a_const) {
                        ExprParts expr_parts;
                        expr_parts.column_ref = column_ref;
                        expr_parts.aconst = a_const;
                        expr_parts.param_ref = NULL;
                        expr_parts.operators = a_expr->name;
                        expr_parts_list->add(&expr_parts);
                    } else if (column_ref && param_ref) {
                        ExprParts expr_parts;
                        expr_parts.column_ref = column_ref;
                        expr_parts.aconst = NULL;
                        expr_parts.param_ref = param_ref;
                        expr_parts.operators = a_expr->name;
                        expr_parts_list->add(&expr_parts);
                    } else if (column_refl && column_refr) { 
                        /*
                         * col1 = col2
                         */
                        ExprParts exprParts;
                        exprParts.column_refl = column_refl;
                        exprParts.column_refr = column_refr;
                        exprParts.column_ref = column_ref;
                        exprParts.aconst = NULL;
                        exprParts.param_ref = NULL;
                        exprParts.operators = a_expr->name;
                        expr_parts_list->add(&exprParts);
                    }
                    break;
                }
                case AEXPR_AND:
                case AEXPR_OR: {
                    if (!expand_expr(a_expr->lexpr, statement_data, expr_parts_list) ||
                        !expand_expr(a_expr->rexpr, statement_data, expr_parts_list)) {
                        return false;
                    }
                    break;
                }
                case AEXPR_NOT: {
                    if (!expand_expr(a_expr->rexpr, statement_data, expr_parts_list)) {
                        return false;
                    }
                    break;
                }
                default:
                    break;
            }
            break;
        }
        case T_FuncCall:
            return handle_func_call((const FuncCall *)expr, expr_parts_list, statement_data);
        case T_SubLink:
            expand_sub_link(expr, statement_data);
            break;

        default:
            break;
    }

    return true;
}

bool exprProcessor::expand_condition_expr(const Node * const expr, ExprPartsList *exprs_list)
{
    if (!exprs_list) {
        return true;
    }
    if (!expr) {
        return true;
    }
    switch (nodeTag(expr)) {
        case T_A_Expr: {
            A_Expr *aExpr = (A_Expr *)expr;
            if (!expand_condition_expr(aExpr->lexpr, exprs_list) ||
                !expand_condition_expr(aExpr->rexpr, exprs_list)) {
                return false;
            }
            break;
        }
        case T_ColumnRef: {
            ColumnRef *column_ref = (ColumnRef*)expr;
            ExprParts exprParts;
            exprParts.column_ref = column_ref;
            exprs_list->add(&exprParts);
            break;
        }
        default: {
            break;
        }
    }
    return true;
}
