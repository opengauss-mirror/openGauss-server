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
 * jit_plan_expr.cpp
 *    JIT execution plan expression helpers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/src/jit_plan_expr.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION: Be sure to include global.h before postgres.h to avoid conflict between libintl.h (included in global.h)
 * and c.h (included in postgres.h).
 */
#include "global.h"
#include "jit_plan_expr.h"

namespace JitExec {
IMPLEMENT_CLASS_LOGGER(ExpressionVisitor, JitExec)
DECLARE_LOGGER(JitPlanExpr, JitExec)

MOT::Table* getRealTable(const Query* query, int table_ref_id, int column_id)
{
    MOT::Table* table = nullptr;
    if (table_ref_id > list_length(query->rtable)) {  // varno index is 1-based
        MOT_LOG_TRACE("getRealTable(): Invalid table reference id %d", table_ref_id);
    } else {
        RangeTblEntry* rte = (RangeTblEntry*)list_nth(query->rtable, table_ref_id - 1);
        if (rte->rtekind == RTE_RELATION) {
            table = MOT::GetTableManager()->GetTableByExternal(rte->relid);
            if (table == nullptr) {
                MOT_LOG_TRACE("getRealTable(): Could not find table by external id %d", rte->relid);
            }
        } else if (rte->rtekind == RTE_JOIN) {
            Var* alias_var = (Var*)list_nth(rte->joinaliasvars, column_id - 1);  // this is zero-based!
            table_ref_id = alias_var->varno;
            if (table_ref_id > list_length(query->rtable)) {  // table_ref_id is 1-based
                MOT_LOG_TRACE("getRealTable(): Invalid indirect table ref index %d", table_ref_id);
            } else {
                rte = (RangeTblEntry*)list_nth(query->rtable, table_ref_id - 1);
                table = MOT::GetTableManager()->GetTableByExternal(rte->relid);
                if (table == nullptr) {
                    MOT_LOG_TRACE("getRealTable(): Could not find table by indirected external id %d", rte->relid);
                }
            }
        }
    }

    MOT_LOG_TRACE("getRealTable(): table_ref_id=%d, column_id=%d --> table=%p", table_ref_id, column_id, table);
    return table;
}

int getRealColumnId(const Query* query, int table_ref_id, int column_id, const MOT::Table* table)
{
    MOT_LOG_DEBUG("getRealColumnId(): table_ref_id = %d, column_id = %d", table_ref_id, column_id);
    if (table_ref_id > list_length(query->rtable)) {  // varno index is 1-based
        MOT_LOG_TRACE("getRealColumnId(): Invalid table reference id %d", table_ref_id);
        column_id = -1;  // signal error
    } else {
        RangeTblEntry* rte = (RangeTblEntry*)list_nth(query->rtable, table_ref_id - 1);
        if (rte->rtekind == RTE_RELATION) {
            if (rte->relid != table->GetTableExId()) {
                column_id = -2;  // signal irrelevant column
                MOT_LOG_TRACE("getRealColumnId(): Skipping var reference of another table %d", (int)rte->relid);
            }
        } else if (rte->rtekind == RTE_JOIN) {
            Var* alias_var = (Var*)list_nth(rte->joinaliasvars, column_id - 1);  // this is zero-based!
            table_ref_id = alias_var->varno;
            if (table_ref_id > list_length(query->rtable)) {  // table_ref_id is 1-based
                column_id = -1;                               // signal error
                MOT_LOG_TRACE("getRealColumnId(): Invalid indirect table ref index %d", table_ref_id);
            } else {
                rte = (RangeTblEntry*)list_nth(query->rtable, table_ref_id - 1);
                if (rte->relid != table->GetTableExId()) {
                    column_id = -2;  // signal irrelevant column
                    MOT_LOG_TRACE("getRealColumnId(): Skipping var reference of another table %d", (int)rte->relid);
                } else {
                    // take real column id and not column id from virtual join table
                    MOT_LOG_DEBUG("getRealColumnId(): Replacing join column id %d with real column id %d of table %s",
                        column_id,
                        (int)alias_var->varattno,
                        table->GetTableName().c_str());
                    column_id = alias_var->varattno;
                }
            }
        } else {
            column_id = -1;  // signal error
            MOT_LOG_TRACE("getRealColumnId(): Invalid relation kind %d", rte->rtekind);
        }
    }

    MOT_LOG_DEBUG("getRealColumnId(): RESULT - table_ref_id = %d, column_id = %d", table_ref_id, column_id);
    return column_id;
}

static JitExpr* parseConstExpr(const Const* const_expr, int arg_pos)
{
    if (!IsTypeSupported(const_expr->consttype)) {
        MOT_LOG_TRACE("Disqualifying constant expression: constant type %d is unsupported", (int)const_expr->consttype);
        return nullptr;
    }

    size_t alloc_size = sizeof(JitConstExpr);
    JitConstExpr* result = (JitConstExpr*)MOT::MemSessionAlloc(alloc_size);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for constant expression", alloc_size);
    } else {
        result->_expr_type = JIT_EXPR_TYPE_CONST;
        result->_const_type = const_expr->consttype;
        result->_value = const_expr->constvalue;
        result->_is_null = const_expr->constisnull;
        result->_arg_pos = arg_pos;
    }
    return (JitExpr*)result;
}

static JitExpr* parseParamExpr(const Param* param_expr, int arg_pos)
{
    if (!IsTypeSupported(param_expr->paramtype)) {
        MOT_LOG_TRACE(
            "Disqualifying parameter expression: parameter type %d is unsupported", (int)param_expr->paramtype);
        return nullptr;
    }

    size_t alloc_size = sizeof(JitParamExpr);
    JitParamExpr* result = (JitParamExpr*)MOT::MemSessionAlloc(alloc_size);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for parameter expression", alloc_size);
    } else {
        result->_expr_type = JIT_EXPR_TYPE_PARAM;
        result->_param_type = param_expr->paramtype;
        result->_param_id = param_expr->paramid - 1;  // move to zero-based index
        result->_arg_pos = arg_pos;
    }
    return (JitExpr*)result;
}

static JitExpr* parseVarExpr(Query* query, const Var* var_expr, int arg_pos)
{
    // make preliminary tests before memory allocation takes place
    if (!IsTypeSupported(var_expr->vartype)) {
        MOT_LOG_TRACE("Disqualifying var expression: var type %d is unsupported", (int)var_expr->vartype);
        return nullptr;
    }

    MOT::Table* table = getRealTable(query, var_expr->varno, var_expr->varattno);
    if (table == nullptr) {
        MOT_LOG_TRACE("parseVarExpr(): Failed to retrieve source table by table ref id %u and column id %d",
            var_expr->varno,
            var_expr->varattno);
        return nullptr;
    }
    int column_id = getRealColumnId(query, var_expr->varno, var_expr->varattno, table);
    if (column_id < 0) {
        MOT_LOG_TRACE("parseVarExpr(): Failed to retrieve column id by table ref id %d and column id %d (table %s)",
            var_expr->varno,
            var_expr->varattno,
            table->GetTableName().c_str());
        return nullptr;
    }

    size_t alloc_size = sizeof(JitVarExpr);
    JitVarExpr* result = (JitVarExpr*)MOT::MemSessionAlloc(alloc_size);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for var expression", alloc_size);
    } else {
        result->_expr_type = JIT_EXPR_TYPE_VAR;
        result->_column_type = var_expr->vartype;
        result->_table = table;
        result->_column_id = column_id;
        result->_arg_pos = arg_pos;
    }

    return (JitExpr*)result;
}

static JitExpr* parseRelabelExpr(Query* query, RelabelType* relabel_type, int arg_pos)
{
    JitExpr* result = nullptr;
    if (relabel_type->arg->type == T_Param) {
        result = parseParamExpr((Param*)relabel_type->arg, arg_pos);
        if (result != nullptr) {
            // replace result type with relabeled type
            ((JitParamExpr*)result)->_param_type = relabel_type->resulttype;
        }
    } else if (relabel_type->arg->type == T_Var) {
        result = parseVarExpr(query, (Var*)relabel_type->arg, arg_pos);
        if (result != nullptr) {
            // replace result type with relabeled type
            ((JitVarExpr*)result)->_column_type = relabel_type->resulttype;
        }
    } else {
        MOT_LOG_TRACE("parseRelabelExpr(): Unsupported relabel type %d", (int)relabel_type->arg->type);
        return nullptr;
    }

    return (JitExpr*)result;
}

static bool ValidateFuncCallExpr(Expr* expr)
{
    Oid resultType;
    Oid funcId;
    List* args;
    Oid oidValue;

    if (expr->type == T_OpExpr) {
        OpExpr* opExpr = (OpExpr*)expr;
        resultType = opExpr->opresulttype;
        funcId = opExpr->opfuncid;
        args = opExpr->args;
        oidValue = opExpr->opno;  // with operator expression we prefer printing the operator id for easier lookup
    } else if (expr->type == T_FuncExpr) {
        FuncExpr* funcExpr = (FuncExpr*)expr;
        resultType = funcExpr->funcresulttype;
        funcId = funcExpr->funcid;
        args = funcExpr->args;
        oidValue = funcExpr->funcid;
    } else {
        MOT_LOG_TRACE("ValidateFuncOpExpr(): Invalid expression type %u", expr->type);
        return false;
    }

    const char* exprName = (expr->type == T_OpExpr) ? "operator" : "function";
    if (!IsTypeSupported(resultType)) {
        MOT_LOG_TRACE("Disqualifying %s expression: result type %u is unsupported", exprName, resultType);
        return false;
    }

    if (!IsFuncIdSupported(funcId)) {
        MOT_LOG_TRACE("Disqualifying %s expression: operator function id %u is unsupported", exprName, funcId);
        return false;
    }

    if (list_length(args) > MOT_JIT_MAX_FUNC_EXPR_ARGS) {
        MOT_LOG_TRACE("Unsupported %s %u: too many arguments", exprName, oidValue);
        return false;
    }

    return true;
}

static JitExpr* parseOpExpr(Query* query, const OpExpr* op_expr, int arg_pos, int depth)
{
    if (!ValidateFuncCallExpr((Expr*)op_expr)) {
        MOT_LOG_TRACE("Disqualifying invalid operator expression");
        return nullptr;
    }

    JitExpr* args[MOT_JIT_MAX_FUNC_EXPR_ARGS] = {nullptr, nullptr, nullptr};
    int arg_num = 0;
    ListCell* lc = nullptr;
    foreach (lc, op_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        args[arg_num] = parseExpr(query, sub_expr, arg_pos + arg_num, depth + 1);
        if (args[arg_num] == nullptr) {
            MOT_LOG_TRACE("Failed to process operator sub-expression %d", arg_num);
            for (int i = 0; i < arg_num; ++i) {
                freeExpr(args[i]);
            }
            return nullptr;
        }
        if (++arg_num == MOT_JIT_MAX_FUNC_EXPR_ARGS) {
            break;
        }
    }

    size_t alloc_size = sizeof(JitOpExpr);
    JitOpExpr* result = (JitOpExpr*)MOT::MemSessionAlloc(alloc_size);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for operator expression", alloc_size);
        for (int i = 0; i < arg_num; ++i) {
            freeExpr(args[i]);
        }
        return nullptr;
    }

    result->_expr_type = JIT_EXPR_TYPE_OP;
    result->_op_no = op_expr->opno;
    result->_op_func_id = op_expr->opfuncid;
    result->_result_type = op_expr->opresulttype;
    result->_arg_count = arg_num;
    for (int i = 0; i < arg_num; ++i) {
        result->_args[i] = args[i];
    }
    result->_arg_pos = arg_pos;

    return (JitExpr*)result;
}

static JitExpr* parseFuncExpr(Query* query, const FuncExpr* func_expr, int arg_pos, int depth)
{
    if (!ValidateFuncCallExpr((Expr*)func_expr)) {
        MOT_LOG_TRACE("Disqualifying invalid function expression");
        return nullptr;
    }

    JitExpr* args[MOT_JIT_MAX_FUNC_EXPR_ARGS] = {nullptr, nullptr, nullptr};
    int arg_num = 0;
    ListCell* lc = nullptr;
    foreach (lc, func_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        args[arg_num] = parseExpr(query, sub_expr, arg_pos + arg_num, depth + 1);
        if (args[arg_num] == nullptr) {
            MOT_LOG_TRACE("Failed to process function sub-expression %d", arg_num);
            for (int i = 0; i < arg_num; ++i) {
                freeExpr(args[i]);
            }
            return nullptr;
        }
        if (++arg_num == MOT_JIT_MAX_FUNC_EXPR_ARGS) {
            break;
        }
    }

    size_t alloc_size = sizeof(JitFuncExpr);
    JitFuncExpr* result = (JitFuncExpr*)MOT::MemSessionAlloc(alloc_size);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for operator expression", alloc_size);
        for (int i = 0; i < arg_num; ++i) {
            freeExpr(args[i]);
        }
        return nullptr;
    }

    result->_expr_type = JIT_EXPR_TYPE_FUNC;
    result->_func_id = func_expr->funcid;
    result->_result_type = func_expr->funcresulttype;
    result->_arg_count = arg_num;
    for (int i = 0; i < arg_num; ++i) {
        result->_args[i] = args[i];
    }
    result->_arg_pos = arg_pos;

    return (JitExpr*)result;
}

static JitExpr* ParseSubLink(Query* query, const SubLink* subLink, int argPos, int depth)
{
    Query* subQuery = (Query*)subLink->subselect;

    // get the result type
    int resultType = 0;
    TargetEntry* targetEntry = (TargetEntry*)linitial(subQuery->targetList);
    if (targetEntry->expr->type == T_Var) {
        resultType = ((Var*)targetEntry->expr)->vartype;
    } else if (targetEntry->expr->type == T_Aggref) {
        resultType = ((Aggref*)targetEntry->expr)->aggtype;
    } else {
        MOT_LOG_TRACE("Disqualifying sub-link expression: unexpected target entry expression type: %d",
            (int)targetEntry->expr->type);
        return nullptr;
    }

    if (!IsTypeSupported(resultType)) {
        MOT_LOG_TRACE("Disqualifying sub-link expression: result type %d is unsupported", resultType);
        return nullptr;
    }

    size_t alloc_size = sizeof(JitSubLinkExpr);
    JitSubLinkExpr* result = (JitSubLinkExpr*)MOT::MemSessionAlloc(alloc_size);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for sub-link expression", alloc_size);
    } else {
        result->_expr_type = JIT_EXPR_TYPE_SUBLINK;
        result->_source_expr = (Expr*)subLink;
        result->_result_type = resultType;
        result->_arg_pos = argPos;
        result->_sub_query_index = 0;  // currently the only valid value for a single sub-query
    }

    return (JitExpr*)result;
}

static JitExpr* ParseBoolExpr(Query* query, const BoolExpr* boolExpr, int argPos, int depth)
{
    if (list_length(boolExpr->args) > MOT_JIT_MAX_BOOL_EXPR_ARGS) {
        MOT_LOG_TRACE("Unsupported Boolean operator: too many arguments");
        return nullptr;
    }

    JitExpr* args[MOT_JIT_MAX_BOOL_EXPR_ARGS] = {nullptr, nullptr};
    int argNum = 0;

    ListCell* lc = nullptr;
    foreach (lc, boolExpr->args) {
        Expr* subExpr = (Expr*)lfirst(lc);
        args[argNum] = parseExpr(query, subExpr, argPos + argNum, depth + 1);
        if (args[argNum] == nullptr) {
            MOT_LOG_TRACE("Failed to process operator sub-expression %d", argNum);
            for (int i = 0; i < argNum; ++i) {
                freeExpr(args[i]);
            }
            return nullptr;
        }
        if (++argNum == MOT_JIT_MAX_BOOL_EXPR_ARGS) {
            break;
        }
    }

    size_t allocSize = sizeof(JitBoolExpr);
    JitBoolExpr* result = (JitBoolExpr*)MOT::MemSessionAlloc(allocSize);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for boolean expression", allocSize);
        for (int i = 0; i < argNum; ++i) {
            freeExpr(args[i]);
        }
        return nullptr;
    }

    result->_expr_type = JIT_EXPR_TYPE_BOOL;
    result->_source_expr = (Expr*)boolExpr;
    result->_result_type = BOOLOID;
    result->_bool_expr_type = boolExpr->boolop;
    result->_arg_count = argNum;
    for (int i = 0; i < argNum; ++i) {
        result->_args[i] = args[i];
    }
    result->_arg_pos = argPos;

    return (JitExpr*)result;
}

JitExpr* parseExpr(Query* query, Expr* expr, int arg_pos, int depth)
{
    JitExpr* result = nullptr;

    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot parse expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (expr->type == T_Const) {
        result = parseConstExpr((Const*)expr, arg_pos);
    } else if (expr->type == T_Param) {
        result = parseParamExpr((Param*)expr, arg_pos);
    } else if (expr->type == T_Var) {
        result = parseVarExpr(query, (Var*)expr, arg_pos);
    } else if (expr->type == T_RelabelType) {
        result = parseRelabelExpr(query, (RelabelType*)expr, arg_pos);
    } else if (expr->type == T_OpExpr) {
        result = parseOpExpr(query, (OpExpr*)expr, arg_pos, depth);
    } else if (expr->type == T_FuncExpr) {
        result = parseFuncExpr(query, (FuncExpr*)expr, arg_pos, depth);
    } else if (expr->type == T_SubLink) {
        result = ParseSubLink(query, (SubLink*)expr, arg_pos, depth);
    } else if (expr->type == T_BoolExpr) {
        result = ParseBoolExpr(query, (BoolExpr*)expr, arg_pos, depth);
    } else {
        MOT_LOG_TRACE("Disqualifying expression: unsupported target expression type %d", (int)expr->type);
    }

    if (result != nullptr) {
        result->_source_expr = expr;
    }
    return result;
}

static void freeOpExpr(JitOpExpr* op_expr)
{
    for (int i = 0; i < op_expr->_arg_count; ++i) {
        freeExpr(op_expr->_args[i]);
    }
}

static void freeFuncExpr(JitFuncExpr* func_expr)
{
    for (int i = 0; i < func_expr->_arg_count; ++i) {
        freeExpr(func_expr->_args[i]);
    }
}

void freeExpr(JitExpr* expr)
{
    switch (expr->_expr_type) {
        case JIT_EXPR_TYPE_OP:
            freeOpExpr((JitOpExpr*)expr);
            break;

        case JIT_EXPR_TYPE_FUNC:
            freeFuncExpr((JitFuncExpr*)expr);
            break;

        default:
            break;
    }

    MOT::MemSessionFree(expr);
}

static bool containsExpr(const JitColumnExprArray* pkey_exprs, const Expr* expr)
{
    for (int i = 0; i < pkey_exprs->_count; ++i) {
        if (pkey_exprs->_exprs[i]._expr->_source_expr == expr) {
            return true;
        }
    }
    return false;
}

enum TableExprClass {
    TableExprNeutral,  // does not refer any table
    TableExprPKey,     // refers the specified table and is a pkey column reference
    TableExprFilter,   // refers the specified table and is a filter column reference
    TableExprInvalid,  // refers another table
    TableExprError     // error occurred while processing expression
};

static TableExprClass classifyTableExpr(Query* query, MOT::Table* table, MOT::Index* index, Expr* expr);

static TableExprClass classifyTableVarExpr(Query* query, MOT::Table* table, MOT::Index* index, const Var* var_expr)
{
    MOT::Table* real_table = getRealTable(query, var_expr->varno, var_expr->varattno);
    if (real_table == nullptr) {
        MOT_LOG_TRACE(
            "Failed to infer table for table ref id %d and column id %d", var_expr->varno, var_expr->varattno);
        return TableExprError;
    } else if (real_table == table) {
        int column_id = getRealColumnId(query, var_expr->varno, var_expr->varattno, table);
        int index_column_id = MapTableColumnToIndex(table, index, column_id);
        if (index_column_id >= 0) {
            MOT_LOG_TRACE(
                "classifyTableVarExpr(): seeing target table/index %s/%s pkey (column_id=%d, index_column_id=%d)",
                table->GetTableName().c_str(),
                index->GetName().c_str(),
                column_id,
                index_column_id);
            return TableExprPKey;
        } else {
            MOT_LOG_TRACE("classifyTableVarExpr(): seeing target table/index %s/%s filter (column_id=%d)",
                table->GetTableName().c_str(),
                index->GetName().c_str(),
                column_id);
            return TableExprFilter;
        }
    } else {
        MOT_LOG_TRACE(
            "Var expression referring to table %s, while looking for table %s (table_ref_id=%d, column_id=%d)",
            real_table->GetTableName().c_str(),
            table->GetTableName().c_str(),
            var_expr->varno,
            var_expr->varattno);
        return TableExprInvalid;
    }
}

static TableExprClass combineTableExprClass(TableExprClass tec1, TableExprClass tec2)
{
    // if either is error then this is error
    if ((tec1 == TableExprError) || (tec2 == TableExprError)) {
        return TableExprError;
    }

    // if either is invalid then this is invalid
    if ((tec1 == TableExprInvalid) || (tec2 == TableExprInvalid)) {
        return TableExprInvalid;
    }

    // if either is pkey then this is a pkey
    if ((tec1 == TableExprPKey) || (tec2 == TableExprPKey)) {
        return TableExprPKey;
    }

    // neither is pkey, so if either is filter then this is a filter
    if ((tec1 == TableExprFilter) || (tec2 == TableExprFilter)) {
        return TableExprFilter;
    }

    // by definition, both must be neutral
    return TableExprNeutral;
}

static TableExprClass clasifyTableExprArgs(Query* query, MOT::Table* table, MOT::Index* index, const List* args)
{
    TableExprClass result = TableExprInvalid;
    int nargs = (int)list_length(args);
    if (nargs == 1) {
        Expr* arg = (Expr*)linitial(args);
        result = classifyTableExpr(query, table, index, arg);
        MOT_LOG_TRACE("clasifyTableExprArgs(): single arg %d", result);
    } else if (nargs == 2) {
        Expr* lhs = (Expr*)linitial(args);
        Expr* rhs = (Expr*)lsecond(args);

        TableExprClass lhs_tec = classifyTableExpr(query, table, index, lhs);
        TableExprClass rhs_tec = classifyTableExpr(query, table, index, rhs);
        MOT_LOG_TRACE("clasifyTableExprArgs(): lhs_tec %d", lhs_tec);
        MOT_LOG_TRACE("clasifyTableExprArgs(): rhs_tec %d", rhs_tec);
        result = combineTableExprClass(lhs_tec, rhs_tec);
    } else if (nargs == 3) {
        Expr* arg1 = (Expr*)linitial(args);
        Expr* arg2 = (Expr*)lsecond(args);
        Expr* arg3 = (Expr*)lthird(args);

        TableExprClass tec1 = classifyTableExpr(query, table, index, arg1);
        TableExprClass tec2 = classifyTableExpr(query, table, index, arg2);
        TableExprClass tec3 = classifyTableExpr(query, table, index, arg3);
        MOT_LOG_TRACE("clasifyTableExprArgs(): tec1 %d", tec1);
        MOT_LOG_TRACE("clasifyTableExprArgs(): tec2 %d", tec2);
        MOT_LOG_TRACE("clasifyTableExprArgs(): tec3 %d", tec3);
        result = combineTableExprClass(combineTableExprClass(tec1, tec2), tec3);
    } else {
        result = TableExprError;
    }
    MOT_LOG_TRACE("clasifyTableExprArgs(): result %d", result);
    return result;
}

static TableExprClass classifyTableOpExpr(Query* query, MOT::Table* table, MOT::Index* index, OpExpr* op_expr)
{
    if (!IsFuncIdSupported(op_expr->opfuncid)) {
        MOT_LOG_TRACE("classifyTableOpExpr(): Unsupported function id %d", (int)op_expr->opfuncid);
        return TableExprError;
    }

    TableExprClass result = clasifyTableExprArgs(query, table, index, op_expr->args);
    if (result == TableExprError) {
        int nargs = (int)list_length(op_expr->args);
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Prepare JIT Plan",
            "Unexpected argument count %d in operator expression type %d",
            nargs,
            (int)op_expr->xpr.type);
    }

    return result;
}

static TableExprClass classifyTableFuncExpr(Query* query, MOT::Table* table, MOT::Index* index, FuncExpr* func_expr)
{
    if (!IsFuncIdSupported(func_expr->funcid)) {
        MOT_LOG_TRACE("classifyTableFuncExpr(): Unsupported function id %d", (int)func_expr->funcid);
        return TableExprError;
    }

    TableExprClass result = clasifyTableExprArgs(query, table, index, func_expr->args);
    if (result == TableExprError) {
        int nargs = (int)list_length(func_expr->args);
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Prepare JIT Plan",
            "Unexpected argument count %d in function expression type %d",
            nargs,
            (int)func_expr->xpr.type);
    }

    return result;
}

static TableExprClass classifyTableExpr(Query* query, MOT::Table* table, MOT::Index* index, Expr* expr)
{
    switch (expr->type) {
        case T_Const:
        case T_Param:
            MOT_LOG_TRACE("classifyTableExpr(): neutral const/param");
            return TableExprNeutral;

        case T_RelabelType:
            return classifyTableExpr(query, table, index, ((RelabelType*)expr)->arg);

        case T_Var:
            return classifyTableVarExpr(query, table, index, (Var*)expr);

        case T_OpExpr:
            return classifyTableOpExpr(query, table, index, (OpExpr*)expr);

        case T_FuncExpr:
            return classifyTableFuncExpr(query, table, index, (FuncExpr*)expr);

        case T_SubLink:
            MOT_LOG_TRACE("classifyTableExpr(): neutral sub-query");
            return TableExprNeutral;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Prepare JIT Plan",
                "Unexpected expression type %d while trying to determine if an expression refers only to a specific "
                "table");
            return TableExprError;
    }
}

static bool visitSearchOpExpression(Query* query, MOT::Table* table, MOT::Index* index, OpExpr* op_expr,
    bool include_pkey, ExpressionVisitor* visitor, bool include_join_exprs, JitColumnExprArray* pkey_exprs)
{
    int arg_count = list_length(op_expr->args);
    if (arg_count != 2) {
        MOT_LOG_TRACE("visitSearchOpExpression(): Invalid OpExpr in WHERE clause having %d arguments", arg_count);
        return false;
    }

    if (!IsWhereOperatorSupported(op_expr->opno)) {
        MOT_LOG_TRACE("visitSearchOpExpression(): Unsupported operator %d", op_expr->opno);
        return false;
    }

    int nargs = (int)list_length(op_expr->args);
    if (nargs != 2) {
        MOT_LOG_TRACE(
            "visitSearchOpExpression(): Unexpected number of arguments %d in operator %d", nargs, op_expr->opno);
        return false;
    }

    // when collecting filters we need to visit only those expressions not visited during pkey collection, but still
    // refer only to this table in addition, the where operator class is not EQUALS, then this is definitely a filter
    // (and nothing else than that), but we still need to verify that it belongs to this table/index
    if (pkey_exprs != nullptr) {
        MOT_LOG_TRACE(
            "visitSearchOpExpression(): Checking if expression %p contains a previously collected pkey expression",
            op_expr);
        Expr* lhs = (Expr*)linitial(op_expr->args);
        Expr* rhs = (Expr*)lsecond(op_expr->args);
        if (containsExpr(pkey_exprs, lhs) || containsExpr(pkey_exprs, rhs)) {
            MOT_LOG_TRACE("visitSearchOpExpression(): expression %p contains a pkey expression (LHS %p or RHS %p)",
                op_expr,
                lhs,
                rhs);
        } else {
            MOT_LOG_TRACE(
                "visitSearchOpExpression(): expression does not contain a pkey expression, classifying for filter");
            TableExprClass lhs_tec = classifyTableExpr(query, table, index, lhs);
            TableExprClass rhs_tec = classifyTableExpr(query, table, index, rhs);
            MOT_LOG_TRACE("visitSearchOpExpression(): LHS table expression is %d", (int)lhs_tec);
            MOT_LOG_TRACE("visitSearchOpExpression(): RHS table expression is %d", (int)rhs_tec);
            TableExprClass tec = combineTableExprClass(lhs_tec, rhs_tec);
            if (tec == TableExprError) {
                MOT_LOG_TRACE(
                    "visitSearchOpExpression(): Encountered error while classifying table expressions for filters");
                return false;
            } else if (tec == TableExprNeutral) {
                MOT_LOG_TRACE("visitSearchOpExpression(): Skipping neutral table expressions for filters");
            } else if (tec == TableExprInvalid) {
                MOT_LOG_TRACE("visitSearchOpExpression(): Skipping another table expressions for filters");
            } else if (tec == TableExprPKey) {
                MOT_LOG_TRACE("visitSearchOpExpression(): Skipping primary key table expressions for filters");
            } else {
                if (op_expr->opresulttype != BOOLOID) {
                    MOT_LOG_TRACE(
                        "visitSearchOpExpression(): Disqualifying query - filter result type %d is unsupported",
                        op_expr->opresulttype);
                } else {
                    MOT_LOG_TRACE("visitSearchOpExpression(): Collecting filter expression %p", op_expr);
                    if (!visitor->OnFilterExpr(op_expr->opno, op_expr->opfuncid, lhs, rhs)) {
                        MOT_LOG_TRACE("visitSearchOpExpression(): Expression collection failed");
                        return false;
                    }
                }
            }
        }
    }

    ListCell* lc1 = nullptr;
    int colid = -1;
    int vartype = -1;
    int index_colid = -1;
    Expr* expr = nullptr;

    bool join_expr = false;

    foreach (lc1, op_expr->args) {
        Expr* arg_expr = (Expr*)lfirst(lc1);
        if (arg_expr->type ==
            T_RelabelType) {  // sometimes relabel expression hides the inner expression, so we peel it off
            arg_expr = ((RelabelType*)arg_expr)->arg;
        }
        if (arg_expr->type == T_Var) {
            Var* var = (Var*)arg_expr;
            if (!IsTypeSupported(var->vartype)) {  // error: column type unsupported
                MOT_LOG_TRACE("visitSearchOpExpression(): Unsupported type %d", (int)var->vartype);
                return false;
            }
            // get real column id and also filter by source table (in case of JOIN, this maybe the column id
            // in the virtual JOIN table, so we need to follow it into the real table and get the real column id)
            // beware to not override colid from previous round
            int tmp_colid = getRealColumnId(query, var->varno, var->varattno, table);
            if (tmp_colid == -1) {  // error occurred
                MOT_LOG_TRACE("visitSearchOpExpression(): aborting after error");
                return false;              // query is not jittable, or even internal error occurred
            } else if (tmp_colid == -2) {  // column does not belong to table, this happens in implicit JOIN queries, so
                                           // we regard it as the expression part
                if (include_join_exprs) {
                    MOT_LOG_TRACE("visitSearchOpExpression(): Regarding column of another table as expression in "
                                  "(probably) implicit JOIN query");
                    expr = arg_expr;
                    join_expr = true;  // ATTENTION: when all is done, we still need to verify this refers to the other
                                       // table in the JOIN
                } else {
                    MOT_LOG_TRACE("visitSearchOpExpression(): Skipping column of another table");
                    return true;  // not an error, but we need to stop (this is an expression of another table)
                }
            } else {  // column belongs to table, but...
                if (colid >= 0) {
                    // this is very unexpected, is the user trying to compare two columns of the same table? we do not
                    // allow it at the moment
                    MOT_LOG_TRACE("visitSearchOpExpression(): Rejecting query with comparison between two columns of "
                                  "the same table");
                    return false;
                }
                colid = tmp_colid;
                index_colid = MapTableColumnToIndex(table, index, colid);
                MOT_LOG_TRACE(
                    "visitSearchOpExpression(): Found table column id %d and index column id %d", colid, index_colid);
                if (include_pkey && (index_colid < 0)) {  // ordered to include only pkey and column is non-pkey so skip
                    MOT_LOG_TRACE("visitSearchOpExpression(): Skipping non-index key column %d %s (ordered to include "
                                  "only index key columns)",
                        colid,
                        table->GetFieldName(colid));
                    return true;  // not an error, but we need to stop (this is a filter expression)
                } else if (!include_pkey &&
                           (index_colid >= 0)) {  // ordered to include only non-pkey and column is pkey so skip
                    MOT_LOG_TRACE("visitSearchOpExpression(): Skipping index key column %d, table column %d %s "
                                  "(ordered to include only non-index key columns)",
                        index_colid,
                        colid,
                        table->GetFieldName(colid));
                    return true;  // not an error, but we need to stop (this is a primary key expression)
                }
                vartype = var->vartype;
            }
            // no further processing
        } else {
            expr = arg_expr;
        }
    }

    if ((colid >= 0) && (expr != nullptr)) {
        MOT_LOG_TRACE(
            "visitSearchOpExpression(): Collecting expression %p for table column id %d, index column id %d (%s)",
            op_expr,
            colid,
            index_colid,
            table->GetFieldName(colid));
        return visitor->OnExpression(expr, vartype, colid, table, ClassifyWhereOperator(op_expr->opno), join_expr);
    }

    if (join_expr) {  // it is possible to see another table's column but not ours in implicit JOIN statements
        MOT_LOG_TRACE("visitSearchOpExpression(): Skipping expression %p of another table in implicit JOIN", op_expr);
        return true;
    }

    // last option: complex filter referring some table columns, so we need to analyze it is a valid filter expression
    if (classifyTableExpr(query, table, index, (Expr*)op_expr) == TableExprFilter) {
        MOT_LOG_TRACE("visitSearchOpExpression(): Enabling complex filter expression %p", op_expr);
        return true;
    }

    MOT_LOG_TRACE("visitSearchOpExpression(): Invalid OpExpr");
    return false;  // query is not jittable
}

static bool visitSearchBoolExpression(Query* query, MOT::Table* table, MOT::Index* index, const BoolExpr* bool_expr,
    bool include_pkey, ExpressionVisitor* visitor, bool include_join_exprs, JitColumnExprArray* pkey_exprs)
{
    bool result = false;
    if (bool_expr->boolop == AND_EXPR) {
        // now traverse args to get param index to build search key
        ListCell* lc = nullptr;

        foreach (lc, bool_expr->args) {
            // each element is Expr
            Expr* expr = (Expr*)lfirst(lc);
            result = visitSearchExpressions(
                query, table, index, expr, include_pkey, visitor, include_join_exprs, pkey_exprs);
            if (!result) {
                MOT_LOG_TRACE("visitSearchBoolExpression(): Failed to process operand");
                break;
            }
        }
    } else {
        MOT_LOG_TRACE("visitSearchBoolExpression(): Unsupported boolean operator %d", (int)bool_expr->boolop);
    }
    return result;
}

bool visitSearchExpressions(Query* query, MOT::Table* table, MOT::Index* index, Expr* expr, bool include_pkey,
    ExpressionVisitor* visitor, bool include_join_exprs, JitColumnExprArray* pkey_exprs)
{
    bool result = false;
    if (expr->type == T_OpExpr) {
        result = visitSearchOpExpression(
            query, table, index, (OpExpr*)expr, include_pkey, visitor, include_join_exprs, pkey_exprs);
    } else if (expr->type == T_BoolExpr) {
        result = visitSearchBoolExpression(
            query, table, index, (BoolExpr*)expr, include_pkey, visitor, include_join_exprs, pkey_exprs);
    } else {
        MOT_LOG_TRACE("Unsupported expression type %d while visiting search expressions", (int)expr->type);
    }
    return result;
}

bool getSearchExpressions(Query* query, MOT::Table* table, MOT::Index* index, bool include_pkey,
    JitColumnExprArray* search_exprs, int* count, bool use_join_clause)
{
    MOT_LOG_TRACE("Getting search expressions for table %s, index %s (include-pkey: %s, use-join-clause: %s)",
        table->GetTableName().c_str(),
        index->GetName().c_str(),
        include_pkey ? "yes" : "no",
        use_join_clause ? "yes" : "no");
    ExpressionCollector expr_collector(query, search_exprs, count);
    Node* quals = query->jointree->quals;
    bool result = true;
    if (quals == nullptr) {
        *count = 0;
    } else {
        result = visitSearchExpressions(
            query, table, index, (Expr*)&quals[0], include_pkey, &expr_collector, use_join_clause);
    }
    if (!result) {
        MOT_LOG_TRACE("Failed to get search expressions");
    } else {
        search_exprs->_count = *count;  // update actual number of expression used in search
        MOT_LOG_TRACE("Found %d expressions", *count);
    }
    return result;
}

static Node* getJoinQualifiers(const Query* query)
{
    Node* quals = nullptr;

    Expr* from_expr = (Expr*)linitial(query->jointree->fromlist);
    if (from_expr->type == T_JoinExpr) {
        JoinExpr* join_expr = (JoinExpr*)from_expr;
        quals = join_expr->quals;
    }

    return quals;
}

static const char* joinClauseTypeToString(JoinClauseType join_clause_type)
{
    switch (join_clause_type) {
        case JoinClauseNone:
            return "none";
        case JoinClauseExplicit:
            return "explicit";
        case JoinClauseImplicit:
            return "implicit";
        default:
            return "N/A";
    }
}

bool getRangeSearchExpressions(
    Query* query, MOT::Table* table, MOT::Index* index, JitIndexScan* index_scan, JoinClauseType join_clause_type)
{
    MOT_LOG_TRACE("Getting range search expressions for table %s, index %s (join_clause_type: %s)",
        table->GetTableName().c_str(),
        index->GetName().c_str(),
        joinClauseTypeToString(join_clause_type));
    bool result = false;
    RangeScanExpressionCollector expr_collector(query, table, index, index_scan);
    if (!expr_collector.Init()) {
        MOT_LOG_TRACE("Failed to initialize range search expression collector");
    } else {
        Node* quals = query->jointree->quals;
        if (quals == nullptr) {
            MOT_LOG_TRACE("No range search expressions collected (empty WHERE clause) - using a full index scan");
            index_scan->_scan_type = JIT_INDEX_SCAN_FULL;
            return true;
        }
        if (!visitSearchExpressions(
                query, table, index, (Expr*)&quals[0], true, &expr_collector, join_clause_type == JoinClauseImplicit)) {
            MOT_LOG_TRACE("Failed to collect range search expressions");
        } else {
            if (join_clause_type == JoinClauseExplicit) {
                quals = getJoinQualifiers(query);
                if (quals == nullptr) {
                    MOT_LOG_TRACE("Query is not jittable: JOIN clause has unexpectedly no qualifiers");
                    return result;
                } else {
                    MOT_LOG_TRACE("Adding JOIN clause qualifiers for WHERE clause classification");
                    if (!visitSearchExpressions(query, table, index, (Expr*)&quals[0], true, &expr_collector, true)) {
                        MOT_LOG_TRACE("Failed to collect range search expressions from JOIN qualifiers");
                        return result;
                    }
                }
            }
            expr_collector.EvaluateScanType();
            result = true;
        }
    }
    return result;
}

bool getTargetExpressions(Query* query, JitColumnExprArray* target_exprs)
{
    int i = 0;
    ListCell* lc = nullptr;

    foreach (lc, query->targetList) {
        TargetEntry* target_entry = (TargetEntry*)lfirst(lc);
        if (target_entry->resjunk) {
            MOT_LOG_TRACE("getTargetExpressions(): Skipping resjunk target entry");
            continue;
        }
        if (i < target_exprs->_count) {
            target_exprs->_exprs[i]._expr = parseExpr(query, target_entry->expr, 0, 0);
            if (target_exprs->_exprs[i]._expr == nullptr) {
                MOT_LOG_TRACE("getTargetExpressions(): Failed to parse target expression %d", i);
                return false;
            }
            target_exprs->_exprs[i]._table_column_id = target_entry->resno;  // update/insert
            if (target_entry->resorigtbl != 0) {                             // happens usually in INSERT
                target_exprs->_exprs[i]._table = MOT::GetTableManager()->GetTableByExternal(target_entry->resorigtbl);
                if (target_exprs->_exprs[i]._table == nullptr) {
                    MOT_LOG_TRACE("getTargetExpressions(): Failed to retrieve real table by id %d",
                        (int)target_entry->resorigtbl);
                    return false;
                }
            } else {
                // this is usually an update, and we retrieve the first table in rtable list
                RangeTblEntry* rte = (RangeTblEntry*)linitial(query->rtable);
                target_exprs->_exprs[i]._table = MOT::GetTableManager()->GetTableByExternal(rte->relid);
                if (target_exprs->_exprs[i]._table == nullptr) {
                    MOT_LOG_TRACE(
                        "getTargetExpressions(): Failed to retrieve real table by inferred id %d", (int)rte->relid);
                    return false;
                }
            }
            target_exprs->_exprs[i]._column_type = target_exprs->_exprs[i]._expr->_result_type;
            target_exprs->_exprs[i]._join_expr = false;
            ++i;
        } else {
            // this is unexpected and indicates internal error (we should have had enough items in the target expression
            // array)
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Prepare JIT Plan",
                "Exceeded number of target expressions %d",
                target_exprs->_count);
            return false;
        }
    }
    return true;
}

bool getSelectExpressions(Query* query, JitSelectExprArray* select_exprs)
{
    int i = 0;
    ListCell* lc = nullptr;

    foreach (lc, query->targetList) {
        TargetEntry* target_entry = (TargetEntry*)lfirst(lc);
        if (target_entry->resjunk) {
            MOT_LOG_TRACE("getSelectExpressions(): Skipping resjunk target entry");
            continue;
        }
        if (i < select_exprs->_count) {
            JitExpr* sub_expr = parseExpr(query, target_entry->expr, 0, 0);
            if (sub_expr == nullptr) {
                MOT_LOG_TRACE("getSelectExpressions(): Failed to parse select expression %d", i);
                return false;
            }
            if (sub_expr->_expr_type != JIT_EXPR_TYPE_VAR) {
                MOT_LOG_TRACE("getSelectExpressions(): Unexpected non-var expression");
                return false;
            }
            select_exprs->_exprs[i]._column_expr = (JitVarExpr*)sub_expr;
            select_exprs->_exprs[i]._tuple_column_id = target_entry->resno - 1;
            ++i;
        } else {
            // this is unexpected and indicates internal error (we should have had enough items in the target expression
            // array)
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Prepare JIT Plan",
                "Exceeded number of select expressions %d",
                select_exprs->_count);
            return false;
        }
    }
    return true;
}
}  // namespace JitExec
