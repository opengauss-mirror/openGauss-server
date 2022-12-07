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
 * jit_plan_sp_expr.cpp
 *    JIT SP execution plan expression helpers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_plan_sp_expr.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION: Be sure to include global.h before postgres.h to avoid conflict between libintl.h (included in global.h)
 * and c.h (included in postgres.h).
 */
#include "global.h"
#include "jit_plan_sp_expr.h"
#include "jit_common.h"
#include "jit_source_map.h"
#include "storage/mot/jit_exec.h"
#include "parser/analyze.h"
#include "catalog/pg_proc.h"

namespace JitExec {
DECLARE_LOGGER(JitPlanSpExpr, JitExec)

bool PrepareFuncParamInfoList(PLpgSQL_function* function, FuncParamInfoList* paramInfoList)
{
    MOT_LOG_TRACE("Preparing function parameter info list for function: %s", function->fn_signature);

    // count var datums
    MOT_LOG_TRACE("Seeing %d parameters", function->ndatums);
    paramInfoList->m_numParams = function->ndatums;
    paramInfoList->m_paramInfo = nullptr;

    // special case: function without parameters
    if (paramInfoList->m_numParams == 0) {
        return true;
    }

    size_t allocSize = sizeof(FuncParamInfo) * paramInfoList->m_numParams;
    paramInfoList->m_paramInfo = (FuncParamInfo*)MOT::MemSessionAlloc(allocSize);
    if (paramInfoList->m_paramInfo == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT Function Plan",
            "Failed to allocate %u bytes for function parameter information Oid array",
            (unsigned)allocSize);
        return false;
    }
    errno_t erc = memset_s(paramInfoList->m_paramInfo, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    for (int i = 0; i < function->ndatums; ++i) {
        PLpgSQL_datum* datum = function->datums[i];
        if (datum->dtype == PLPGSQL_DTYPE_VAR) {
            PLpgSQL_var* varDatum = (PLpgSQL_var*)datum;
            MOT_LOG_TRACE("Collecting var parameter with datum id %d name %s and type %d",
                i,
                varDatum->refname,
                varDatum->datatype->typoid);
            paramInfoList->m_paramInfo[i].m_datumId = i;
            paramInfoList->m_paramInfo[i].m_paramType = varDatum->datatype->typoid;
            allocSize = strlen(varDatum->datatype->typname) + 1;
            paramInfoList->m_paramInfo[i].m_paramName = (char*)MOT::MemSessionAlloc(allocSize);
            if (paramInfoList->m_paramInfo[i].m_paramName == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Prepare JIT Function Plan",
                    "Failed to allocate %u bytes for function parameter information type name",
                    (unsigned)allocSize);
                for (int j = 0; j < i; ++j) {
                    MOT::MemSessionFree(paramInfoList->m_paramInfo[j].m_paramName);
                }
                MOT::MemSessionFree(paramInfoList->m_paramInfo);
                return false;
            }
            erc = strcpy_s(paramInfoList->m_paramInfo[i].m_paramName, allocSize, varDatum->datatype->typname);
            securec_check(erc, "\0", "\0");
        } else {
            MOT_LOG_TRACE("Collecting other parameter with datum id %d", i);
            paramInfoList->m_paramInfo[i].m_datumId = i;
            paramInfoList->m_paramInfo[i].m_paramType = InvalidOid;
        }
    }
    return true;
}

void DestroyFuncParamInfoList(FuncParamInfoList* paramInfoList)
{
    for (int i = 0; i < paramInfoList->m_numParams; ++i) {
        MOT::MemSessionFree(paramInfoList->m_paramInfo[i].m_paramName);
    }
    MOT::MemSessionFree(paramInfoList->m_paramInfo);
}

static JitVisitResult VisitStmtListQueries(List* body, JitFunctionQueryVisitor* visitor, int* index);
static JitVisitResult VisitStmtBlockQueries(
    PLpgSQL_stmt_block* stmtBlock, JitFunctionQueryVisitor* visitor, int* index);

#define PROCESS_VISIT_RESULT(message, lineNo)               \
    do {                                                    \
        if (result == JitVisitResult::JIT_VISIT_ERROR) {    \
            visitor->OnError(message, lineNo);              \
        }                                                   \
        if (result != JitVisitResult::JIT_VISIT_CONTINUE) { \
            return result;                                  \
        }                                                   \
    } while (0)

static JitVisitResult VisitIfStmtQueries(PLpgSQL_stmt_if* ifStmt, JitFunctionQueryVisitor* visitor, int* index)
{
    JitVisitResult result = visitor->OnQuery(ifStmt->cond, nullptr, *index, false);
    PROCESS_VISIT_RESULT("IF statement condition", ifStmt->lineno);
    ++(*index);

    result = VisitStmtListQueries(ifStmt->then_body, visitor, index);
    PROCESS_VISIT_RESULT("IF statement THEN body", ifStmt->lineno);

    result = VisitStmtListQueries(ifStmt->else_body, visitor, index);
    PROCESS_VISIT_RESULT("IF statement ELSE body", ifStmt->lineno);

    ListCell* lc = nullptr;
    foreach (lc, ifStmt->elsif_list) {
        PLpgSQL_if_elsif* elseifStmt = (PLpgSQL_if_elsif*)lfirst(lc);
        result = VisitStmtListQueries(elseifStmt->stmts, visitor, index);
        PROCESS_VISIT_RESULT("IF statement ELSEIF body", ifStmt->lineno);
    }
    return result;
}

static JitVisitResult VisitCaseStmtQueries(PLpgSQL_stmt_case* caseStmt, JitFunctionQueryVisitor* visitor, int* index)
{
    JitVisitResult result = JitVisitResult::JIT_VISIT_CONTINUE;
    if (caseStmt->t_expr != nullptr) {
        result = visitor->OnQuery(caseStmt->t_expr, nullptr, *index, false);
        PROCESS_VISIT_RESULT("CASE statement expression", caseStmt->lineno);
        ++(*index);
    }
    ListCell* lc = nullptr;
    foreach (lc, caseStmt->case_when_list) {
        PLpgSQL_case_when* caseWhenStmt = (PLpgSQL_case_when*)lfirst(lc);
        result = VisitStmtListQueries(caseWhenStmt->stmts, visitor, index);
        PROCESS_VISIT_RESULT("CASE WHEN body", caseWhenStmt->lineno);
    }

    if (caseStmt->have_else && (caseStmt->else_stmts != nullptr)) {
        result = VisitStmtListQueries(caseStmt->else_stmts, visitor, index);
        PROCESS_VISIT_RESULT("CASE ELSE body", caseStmt->lineno);
    }
    return result;
}

static JitVisitResult VisitSqlStmtQuery(PLpgSQL_stmt_execsql* execSqlStmt, JitFunctionQueryVisitor* visitor, int* index)
{
    PLpgSQL_row* row = nullptr;
    if (execSqlStmt->into) {
        if (execSqlStmt->row == nullptr) {
            MOT_LOG_TRACE("Unsupported INTO record in EXECSQL statement: %s", execSqlStmt->sqlstmt->query);
            return JitVisitResult::JIT_VISIT_ERROR;
        }
        row = execSqlStmt->row;
    }
    JitVisitResult result = visitor->OnQuery(execSqlStmt->sqlstmt, row, *index, execSqlStmt->into);
    PROCESS_VISIT_RESULT("EXECSQL query", execSqlStmt->lineno);
    ++(*index);
    return result;
}

static JitVisitResult VisitWhileStmtQueries(PLpgSQL_stmt_while* whileStmt, JitFunctionQueryVisitor* visitor, int* index)
{
    JitVisitResult result = visitor->OnQuery(whileStmt->cond, nullptr, *index, false);
    PROCESS_VISIT_RESULT("WHILE statement condition", whileStmt->lineno);
    ++(*index);

    result = VisitStmtListQueries(whileStmt->body, visitor, index);
    PROCESS_VISIT_RESULT("WHILE statement body", whileStmt->lineno);
    return result;
}

static JitVisitResult VisitForiStmtQueries(PLpgSQL_stmt_fori* foriStmt, JitFunctionQueryVisitor* visitor, int* index)
{
    if (foriStmt->save_exceptions) {
        MOT_LOG_TRACE("Unsupported SAVE EXCEPTIONS clause in FOR-I statement");
        visitor->OnError("FOR-I statement message", foriStmt->lineno);
        return JitVisitResult::JIT_VISIT_ERROR;
    }

    JitVisitResult result = JitVisitResult::JIT_VISIT_CONTINUE;
    if (foriStmt->lower != nullptr) {
        result = visitor->OnQuery(foriStmt->lower, nullptr, *index, false);
        PROCESS_VISIT_RESULT("FOR statement lower bound expression", foriStmt->lineno);
        ++(*index);
    }
    if (foriStmt->upper != nullptr) {
        result = visitor->OnQuery(foriStmt->upper, nullptr, *index, false);
        PROCESS_VISIT_RESULT("FOR statement upper bound expression", foriStmt->lineno);
        ++(*index);
    }
    if (foriStmt->step != nullptr) {
        result = visitor->OnQuery(foriStmt->step, nullptr, *index, false);
        PROCESS_VISIT_RESULT("FOR statement step expression", foriStmt->lineno);
        ++(*index);
    }

    result = VisitStmtListQueries(foriStmt->body, visitor, index);
    PROCESS_VISIT_RESULT("FOR statement body", foriStmt->lineno);
    return result;
}

static JitVisitResult VisitRaiseStmt(PLpgSQL_stmt_raise* stmtRaise, JitFunctionQueryVisitor* visitor, int* index)
{
    // no query involved, just checking it is supported
    if ((strcmp(stmtRaise->message, MOT_JIT_PROFILE_BEGIN_MESSAGE) != 0) &&
        (strcmp(stmtRaise->message, MOT_JIT_PROFILE_END_MESSAGE) != 0)) {
        visitor->OnError("RAISE statement message", stmtRaise->lineno);
        return JitVisitResult::JIT_VISIT_ERROR;
    }
    ListCell* lc = nullptr;
    foreach (lc, stmtRaise->options) {
        PLpgSQL_raise_option* option = (PLpgSQL_raise_option*)lfirst(lc);
        if ((option->opt_type != PLPGSQL_RAISEOPTION_HINT) || (option->expr->query == nullptr)) {
            visitor->OnError("RAISE statement option", stmtRaise->lineno);
            return JitVisitResult::JIT_VISIT_ERROR;
        }
        MOT_LOG_TRACE("Seeing hint text: %s", option->expr->query);
    }
    return JitVisitResult::JIT_VISIT_CONTINUE;
}

static JitVisitResult VisitStmtListQueries(List* body, JitFunctionQueryVisitor* visitor, int* index)
{
    JitVisitResult result = JitVisitResult::JIT_VISIT_CONTINUE;
    ListCell* lc = nullptr;
    foreach (lc, body) {
        PLpgSQL_stmt* stmt = (PLpgSQL_stmt*)lfirst(lc);
        switch (stmt->cmd_type) {
            case PLPGSQL_STMT_EXECSQL:
                result = VisitSqlStmtQuery((PLpgSQL_stmt_execsql*)stmt, visitor, index);
                if (result != JitVisitResult::JIT_VISIT_CONTINUE) {
                    return result;
                }
                break;

            case PLPGSQL_STMT_PERFORM:
                result = visitor->OnQuery(((PLpgSQL_stmt_perform*)stmt)->expr, nullptr, *index, false);
                PROCESS_VISIT_RESULT("PERFORM statement", stmt->lineno);
                ++(*index);
                break;

            case PLPGSQL_STMT_BLOCK:
                result = VisitStmtBlockQueries((PLpgSQL_stmt_block*)stmt, visitor, index);
                if (result != JitVisitResult::JIT_VISIT_CONTINUE) {
                    return result;
                }
                break;

            case PLPGSQL_STMT_IF:
                result = VisitIfStmtQueries((PLpgSQL_stmt_if*)stmt, visitor, index);
                if (result != JitVisitResult::JIT_VISIT_CONTINUE) {
                    return result;
                }
                break;

            case PLPGSQL_STMT_CASE:
                result = VisitCaseStmtQueries((PLpgSQL_stmt_case*)stmt, visitor, index);
                if (result != JitVisitResult::JIT_VISIT_CONTINUE) {
                    return result;
                }
                break;

            case PLPGSQL_STMT_LOOP:
                result = VisitStmtListQueries(((PLpgSQL_stmt_loop*)stmt)->body, visitor, index);
                PROCESS_VISIT_RESULT("LOOP statement body", stmt->lineno);
                break;

            case PLPGSQL_STMT_WHILE:
                result = VisitWhileStmtQueries((PLpgSQL_stmt_while*)stmt, visitor, index);
                if (result != JitVisitResult::JIT_VISIT_CONTINUE) {
                    return result;
                }
                break;

            case PLPGSQL_STMT_FORI:
                result = VisitForiStmtQueries((PLpgSQL_stmt_fori*)stmt, visitor, index);
                if (result != JitVisitResult::JIT_VISIT_CONTINUE) {
                    return result;
                }
                break;

            case PLPGSQL_STMT_ASSIGN:
                result = visitor->OnQuery(((PLpgSQL_stmt_assign*)stmt)->expr, nullptr, *index, false);
                PROCESS_VISIT_RESULT("ASSIGN statement expression", stmt->lineno);
                ++(*index);
                break;

            case PLPGSQL_STMT_RETURN:
                if (((PLpgSQL_stmt_return*)stmt)->expr != nullptr) {
                    result = visitor->OnQuery(((PLpgSQL_stmt_return*)stmt)->expr, nullptr, *index, false);
                    PROCESS_VISIT_RESULT("RETURN statement expression", stmt->lineno);
                    ++(*index);
                }
                break;

            case PLPGSQL_STMT_EXIT:
                if (((PLpgSQL_stmt_exit*)stmt)->cond != nullptr) {
                    result = visitor->OnQuery(((PLpgSQL_stmt_exit*)stmt)->cond, nullptr, *index, false);
                    PROCESS_VISIT_RESULT("EXIT statement expression", stmt->lineno);
                    ++(*index);
                }
                break;

            case PLPGSQL_STMT_RAISE:
                result = VisitRaiseStmt((PLpgSQL_stmt_raise*)stmt, visitor, index);
                if (result != JitVisitResult::JIT_VISIT_CONTINUE) {
                    return result;
                }
                break;

            case PLPGSQL_STMT_NULL:
                return JitVisitResult::JIT_VISIT_CONTINUE;

            case PLPGSQL_STMT_GOTO:
                MOT_LOG_TRACE("Unsupported GOTO statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_FORS:
                MOT_LOG_TRACE("Unsupported FORS statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_FORC:
                MOT_LOG_TRACE("Unsupported FORC statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_FOREACH_A:
                MOT_LOG_TRACE("Unsupported FOREACH statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_RETURN_NEXT:
                MOT_LOG_TRACE("Unsupported GOTO statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_RETURN_QUERY:
                MOT_LOG_TRACE("Unsupported RETURN-NEXT statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_DYNEXECUTE:
                MOT_LOG_TRACE("Unsupported DYNEXECUTE statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_DYNFORS:
                MOT_LOG_TRACE("Unsupported DYNCFORS statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_GETDIAG:
                MOT_LOG_TRACE("Unsupported GETDIAG statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_OPEN:
                MOT_LOG_TRACE("Unsupported OPEN statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_FETCH:
                MOT_LOG_TRACE("Unsupported FETCH statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_CLOSE:
                MOT_LOG_TRACE("Unsupported CLOSE statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_COMMIT:
                MOT_LOG_TRACE("Unsupported COMMIT statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            case PLPGSQL_STMT_ROLLBACK:
                MOT_LOG_TRACE("Unsupported ROLLBACK statement");
                return JitVisitResult::JIT_VISIT_ERROR;

            default:
                MOT_LOG_TRACE("Unsupported unknown statement: %u", stmt->cmd_type);
                return JitVisitResult::JIT_VISIT_ERROR;
        }
    }
    return JitVisitResult::JIT_VISIT_CONTINUE;
}

static JitVisitResult VisitStmtBlockQueries(PLpgSQL_stmt_block* stmtBlock, JitFunctionQueryVisitor* visitor, int* index)
{
    JitVisitResult result = VisitStmtListQueries(stmtBlock->body, visitor, index);
    PROCESS_VISIT_RESULT("STATEMENT block body", stmtBlock->lineno);
    if (stmtBlock->exceptions && (list_length(stmtBlock->exceptions->exc_list) > 0)) {
        ListCell* lc = nullptr;
        foreach (lc, stmtBlock->exceptions->exc_list) {
            PLpgSQL_exception* exceptionBlock = (PLpgSQL_exception*)lfirst(lc);
            result = VisitStmtListQueries(exceptionBlock->action, visitor, index);
            PROCESS_VISIT_RESULT("STATEMENT block exception body", exceptionBlock->lineno);
        }
    }
    return result;
}

extern bool VisitFunctionQueries(PLpgSQL_function* function, JitFunctionQueryVisitor* visitor)
{
    int index = 0;
    if (function->action != nullptr) {
        JitVisitResult result = VisitStmtBlockQueries(function->action, visitor, &index);
        if (result == JitVisitResult::JIT_VISIT_ERROR) {
            MOT_LOG_TRACE("Failed to visit function");
            return false;
        } else if (result == JitVisitResult::JIT_VISIT_STOP) {
            MOT_LOG_TRACE("Visit function stopped early by visitor");
        }
    }
    return true;
}

int CountFunctionQueries(PLpgSQL_function* function)
{
    int queryCount = 0;
    MOT_LOG_TRACE("Counting function queries");
    JitFunctionQueryCounter queryCounter;
    if (!VisitFunctionQueries(function, &queryCounter)) {
        // we already passed verification, so this can't happen
        MOT_ASSERT(false);
        MOT_LOG_TRACE("Failed to count number of queries (probable internal error)");
    } else {
        queryCount = queryCounter.GetQueryCount();
        MOT_LOG_TRACE("Counted %d function queries", queryCount);
    }
    return queryCount;
}

static bool IsPLpgSQLFunction(Oid functionOid)
{
    // search function in pg_proc
    HeapTuple procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
    if ((procTuple == nullptr) || !HeapTupleIsValid(procTuple)) {
        MOT_LOG_TRACE("Cannot determine funciton type: Oid %u not found in pg_proc", functionOid);
        return false;
    }

    Form_pg_proc procStruct = (Form_pg_proc)GETSTRUCT(procTuple);
    MOT_LOG_TRACE("Function %u language id: %u", (unsigned)functionOid, (unsigned)procStruct->prolang)

    // get object id for PLPGSQL validator function
    const char* language = "plpgsql";
    HeapTuple languageTuple = SearchSysCache1(LANGNAME, PointerGetDatum(language));
    if ((languageTuple == nullptr) || !HeapTupleIsValid(languageTuple)) {
        MOT_LOG_TRACE("Cannot find plpgsql language tuple");
        ReleaseSysCache(procTuple);
        return false;
    }

    bool result = false;
    Oid languageOid = HeapTupleGetOid(languageTuple);
    ReleaseSysCache(languageTuple);
    MOT_LOG_TRACE("Retrieved PLPGSQL language id %u", (unsigned)languageOid);
    if (procStruct->prolang != languageOid) {
        MOT_LOG_TRACE("Function %u is not a PLpgSQL function", (unsigned)functionOid);
    } else {
        result = true;
        MOT_LOG_TRACE("Function %u is a PLpgSQL function", (unsigned)functionOid);
    }
    ReleaseSysCache(procTuple);
    return result;
}

bool IsFunctionSupported(FuncExpr* funcExpr)
{
    if (!IsTypeSupported(funcExpr->funcresulttype)) {
        MOT_LOG_TRACE(
            "Disqualifying function expression: result type %d is unsupported", (int)funcExpr->funcresulttype);
        return false;
    }

    if (list_length(funcExpr->args) > MOT_JIT_MAX_FUNC_EXPR_ARGS) {
        MOT_LOG_TRACE("Unsupported function %d: too many arguments", funcExpr->funcid);
        return false;
    }

    // each argument is checked separately by T_List node tag below
    return true;
}

static bool IsSimpleNode(Node* node, bool* failed, bool* isConst = nullptr, Oid* constType = nullptr)
{
    if (node == nullptr) {
        return true;
    }

    switch (nodeTag(node)) {
        case T_Const:
            if (isConst != nullptr) {
                *isConst = true;
            }
            if (constType != nullptr) {
                *constType = ((Const*)node)->consttype;
            }
            return true;

        case T_Param:
            return true;

        case T_FuncExpr: {
            FuncExpr* expr = (FuncExpr*)node;
            if (expr->funcretset) {
                *failed = true;
                MOT_LOG_TRACE("Unsupported function returning set");
                return false;
            }
            if (!IsSimpleNode((Node*)expr->args, failed)) {
                MOT_LOG_TRACE("Unsupported function arguments");
                return false;
            }
            if (!IsFunctionSupported(expr)) {
                *failed = true;
                MOT_LOG_TRACE("Unsupported function expression");
                return false;
            }
            // we currently disqualify hybrid expressions, either involving jittable or non-jittable function call
            if (HasJitFunctionSource(expr->funcid) || IsPLpgSQLFunction(expr->funcid)) {
                *failed = true;
                MOT_LOG_TRACE("Unsupported hybrid expression with function call %u", (unsigned)expr->funcid);
                return false;
            }
            return true;
        }

        case T_OpExpr: {
            OpExpr* expr = (OpExpr*)node;
            if (expr->opretset) {
                MOT_LOG_TRACE("Unsupported operator returning set");
                return false;
            }
            if (!IsSimpleNode((Node*)expr->args, failed)) {
                MOT_LOG_TRACE("Unsupported operator arguments");
                return false;
            }
            return true;
        }

        case T_RelabelType:
            return IsSimpleNode((Node*)((RelabelType*)node)->arg, failed);

        case T_List: {
            List* expr = (List*)node;
            ListCell* l = nullptr;
            foreach (l, expr) {
                if (!IsSimpleNode((Node*)lfirst(l), failed)) {
                    MOT_LOG_TRACE("Unsupported list node");
                    return false;
                }
            }
            return true;
        }

        case T_ArrayRef:
            *failed = true;
            MOT_LOG_TRACE("Unsupported ARRAY-REF Node");
            return false;

        case T_DistinctExpr:
            MOT_LOG_TRACE("Simple DISTINCT expression");
            return true;

        case T_NullIfExpr:
            MOT_LOG_TRACE("Simple NULL-IF expression");
            return true;

        case T_ScalarArrayOpExpr:
            MOT_LOG_TRACE("Simple SCALAR-ARRAY-OP expression");
            return true;

        case T_BoolExpr:
            MOT_LOG_TRACE("Simple BOOL expression");
            return true;

        case T_FieldSelect:
            *failed = true;
            MOT_LOG_TRACE("Unsupported FIELD-SELECT expression");
            return false;

        case T_FieldStore:
            *failed = true;
            MOT_LOG_TRACE("Unsupported FIELD-STORE expression");
            return false;

        case T_CoerceViaIO:
            *failed = true;
            MOT_LOG_TRACE("Unsupported COERCE-VIA-IO Node");
            return false;

        case T_ArrayCoerceExpr:
            *failed = true;
            MOT_LOG_TRACE("Unsupported ARRAY-COERCE-EXPR Node");
            return false;

        case T_ConvertRowtypeExpr:
            *failed = true;
            MOT_LOG_TRACE("Unsupported CONVERT-ROWTYPE-EXPR Node");
            return false;

        case T_CaseExpr:
            *failed = true;
            MOT_LOG_TRACE("Unsupported CASE-EXPR Node");
            return false;

        case T_CaseWhen:
            *failed = true;
            MOT_LOG_TRACE("Unsupported CASE-WHEN Node");
            return false;

        case T_CaseTestExpr:
            *failed = true;
            MOT_LOG_TRACE("Unsupported CASE-TEST Node");
            return false;

        case T_ArrayExpr:
            *failed = true;
            MOT_LOG_TRACE("Unsupported ARRAY-EXPR Node");
            return false;

        case T_RowExpr:
            *failed = true;
            MOT_LOG_TRACE("Unsupported ROW-EXPR Node");
            return false;

        case T_RowCompareExpr:
            *failed = true;
            MOT_LOG_TRACE("Unsupported ROW-COMPARE-EXPR Node");
            return false;

        case T_CoalesceExpr:
            MOT_LOG_TRACE("Simple COALESCE-EXPR Node");
            return false;

        case T_MinMaxExpr:
            *failed = true;
            MOT_LOG_TRACE("Unsupported MINMAX-EXPR Node");
            return false;

        case T_XmlExpr:
            *failed = true;
            MOT_LOG_TRACE("Unsupported XML-EXPR Node");
            return false;

        case T_NullTest:
            MOT_LOG_TRACE("Simple NULL-TEST Node");
            return true;

        case T_HashFilter:
            *failed = true;
            MOT_LOG_TRACE("Unsupported HASH-FILTER Node");
            return false;

        case T_BooleanTest:
            MOT_LOG_TRACE("Simple BOOLEAN-TEST Node");
            return true;

        case T_CoerceToDomain:
            *failed = true;
            MOT_LOG_TRACE("Unsupported COERCE-TO-DOMAIN Node");
            return false;

        case T_CoerceToDomainValue:
            *failed = true;
            MOT_LOG_TRACE("Unsupported COERCE-TO-DOMAIN-VALUE Node");
            return false;

        default:
            *failed = true;
            MOT_LOG_TRACE("Unsupported Node type: %d", (int)nodeTag(node));
            return false;
    }
}

static bool IsSimpleExpr(CachedPlan* cplan, bool* failed, bool* isConst = nullptr, Oid* constType = nullptr)
{
    // 1. There must be one single plan tree
    if (list_length(cplan->stmt_list) != 1) {
        return false;
    }
    PlannedStmt* stmt = (PlannedStmt*)linitial(cplan->stmt_list);

    // 2. It must be a RESULT plan --> no scan's required
    if (!IsA(stmt, PlannedStmt) || (stmt->commandType != CMD_SELECT)) {
        return false;
    }
    Plan* plan = stmt->planTree;
    if (!IsA(plan, BaseResult)) {
        return false;
    }

    // 3. Can't have any sub-plan or qual clause, either
    if ((plan->lefttree != nullptr) || (plan->righttree != nullptr) || (plan->initPlan != nullptr) ||
        (plan->qual != nullptr) || (((BaseResult*)plan)->resconstantqual != nullptr)) {
        return false;
    }

    // 4. The plan must have a single attribute as result
    if (list_length(plan->targetlist) != 1) {
        return false;
    }
    TargetEntry* tle = (TargetEntry*)linitial(plan->targetlist);

    // 5. First check special case: maybe this is a function call into a jitted function
    if (tle->expr->type == T_FuncExpr) {
        FuncExpr* funcExpr = (FuncExpr*)tle->expr;
        if (HasJitFunctionSource(funcExpr->funcid)) {
            // this is a jitted function invocation
            MOT_LOG_TRACE("Found non-simple invocation to jitted function %u", (unsigned)funcExpr->funcid);
            return false;
        }
        // since JIT-compilation is postponed until PREPARE, we may have not yet compiled this function, so we also
        // check whether it is a PLpgSQL stored procedure
        if (IsPLpgSQLFunction(funcExpr->funcid)) {
            // this is a jitted function function that we nhave never attempted to compile
            MOT_LOG_TRACE("Found non-simple invocation to PLPGsql stored procedure %u", (unsigned)funcExpr->funcid);
            return false;
        }
    }

    // 6. Check that all the nodes in the expression are non-scary.
    if (!IsSimpleNode((Node*)tle->expr, failed, isConst, constType)) {
        return false;
    }

    return true;
}

bool IsSimpleExpr(
    SPIPlanPtr spiPlan, Query* query, bool* failed, bool* isConst /* = nullptr */, Oid* constType /* = nullptr */)
{
    *failed = false;
    // It must be a plain SELECT query without any input tables
    if (!IsA(query, Query) || (query->commandType != CMD_SELECT) || (query->rtable != NIL)) {
        return false;
    }

    // Can't have any sub-plans, aggregates, qual clauses either
    bool hasComplexClause = (query->hasAggs || query->hasWindowFuncs || query->hasSubLinks || query->hasForUpdate ||
                             query->cteList || query->jointree->quals || query->groupClause || query->havingQual ||
                             query->windowClause || query->distinctClause || query->sortClause || query->limitOffset ||
                             query->limitCount || query->setOperations);
    if (hasComplexClause) {
        return false;
    }

    // The query must have a single attribute as result
    if (list_length(query->targetList) != 1) {
        return false;
    }

    // OK, it seems worth constructing a plan for more careful checking
    // NOTE: every variable used after catch needs to be volatile (see longjmp() man page)
    volatile CachedPlan* cplan = nullptr;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile bool result = false;
    PG_TRY();
    {
        cplan = SPI_plan_get_cached_plan(spiPlan);
        if (cplan == nullptr) {
            MOT_LOG_TRACE("Failed to determine expression type - unable to get cached plan");
            *failed = true;
        } else {
            // Share the remaining work with recheck code path
            result = IsSimpleExpr((CachedPlan*)cplan, failed, isConst, constType);

            // Release our plan ref-count
            ReleaseCachedPlan((CachedPlan*)cplan, false);
        }
    }
    PG_CATCH();
    {
        CurrentMemoryContext = origCxt;
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while retrieving cached plan: %s", edata->message);
        ereport(WARNING,
            (errmodule(MOD_MOT),
                errmsg("Caught exception while retrieving cached plan: %s", edata->message),
                errdetail("%s", edata->detail)));
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();

    return result;
}

bool IsArgParam(PLpgSQL_function* func, int index, int* argIndex)
{
    *argIndex = -1;
    int argCount = func->fn_nargs;
    for (int i = 0; i < argCount; ++i) {
        if (func->fn_argvarnos[i] == index) {
            *argIndex = i;
            return true;
        }
    }
    return false;
}

TupleDesc PrepareTupleDescFromRow(PLpgSQL_row* row, PLpgSQL_function* func)
{
    TupleDesc tupDesc = CreateTemplateTupleDesc(row->nfields, false);
    for (int i = 0; i < row->nfields; i++) {
        int varNo = row->varnos[i];
        PLpgSQL_datum* datum = func->datums[varNo];
        if (datum->dtype == PLPGSQL_DTYPE_VAR) {
            PLpgSQL_var* var = (PLpgSQL_var*)datum;
            TupleDescInitEntry(tupDesc, i + 1, var->refname, var->datatype->typoid, var->datatype->atttypmod, 0);
            TupleDescInitEntryCollation(tupDesc, i + 1, var->datatype->collation);
        }
    }
    return tupDesc;
}

extern SPIPlanPtr GetSpiPlan(PLpgSQL_function* function, PLpgSQL_expr* expr)
{
    ExprQueryAttrs attrs;
    if (!GetExprQueryAttrs(expr, function, &attrs)) {
        MOT_LOG_TRACE("Failed to get query attributes for query: %s", expr->query);
        return nullptr;
    }

    SPIPlanPtr plan = nullptr;
    if (attrs.m_spiPlan == nullptr) {
        MOT_LOG_TRACE("Query plan is null: %s", expr->query);
    } else {
        int rc = SPI_keepplan(attrs.m_spiPlan);
        if (rc != 0) {
            MOT_LOG_TRACE("Failed to keep SPI plan: %s (%d)", SPI_result_code_string(rc), rc);
        } else {
            // move the plan to the result
            plan = attrs.m_spiPlan;
            attrs.m_spiPlan = nullptr;
        }
    }
    CleanupExprQueryAttrs(&attrs);
    return plan;
}

extern SPIPlanPtr PrepareSpiPlan(JitFunctionContext* functionContext, int exprIndex, PLpgSQL_expr** expr)
{
    PLpgSQL_function* function = ((JitFunctionExecState*)functionContext->m_execState)->m_function;
    MOT_ASSERT(function != nullptr);

    SPIAutoConnect spiAutoConnect;
    if (!spiAutoConnect.IsConnected()) {
        int rc = spiAutoConnect.GetErrorCode();
        MOT_LOG_TRACE("Failed to connect to SPI while generating plan for SP non-jittable expression %u: %s (%u)",
            exprIndex,
            SPI_result_code_string(rc),
            rc);
        return nullptr;
    }

    // NOTE: every variable used after catch needs to be volatile (see longjmp() man page)
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile SPIPlanPtr result = nullptr;
    JitSubQueryPlanGenerator subqPlanGen((PLpgSQL_function*)function, expr, exprIndex);
    PG_TRY();
    {
        if (!VisitFunctionQueries((PLpgSQL_function*)function, &subqPlanGen)) {
            MOT_LOG_TRACE("Failed to traverse function while generating plan for expression %u", exprIndex);
        } else {
            result = subqPlanGen.GetPlan();
        }
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while generating plan for non-jittable SP sub-query: %s", edata->message);
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();

    if (result == nullptr) {
        MOT_LOG_TRACE("Could not find plan for expression with index %u", exprIndex);
    }
    return result;
}

void DestroyCallSitePlan(JitCallSitePlan* callSitePlan)
{
    if (callSitePlan->m_queryPlan != nullptr) {
        JitDestroyPlan(callSitePlan->m_queryPlan);
        callSitePlan->m_queryPlan = nullptr;
    }
    if (callSitePlan->m_spiPlan != nullptr) {
        (void)SPI_freeplan(callSitePlan->m_spiPlan);
        callSitePlan->m_spiPlan = nullptr;
    }
    if (callSitePlan->m_callParamInfo != nullptr) {
        MOT::MemSessionFree(callSitePlan->m_callParamInfo);
        callSitePlan->m_callParamInfo = nullptr;
    }
    if (callSitePlan->m_queryString != nullptr) {
        MOT::MemSessionFree(callSitePlan->m_queryString);
        callSitePlan->m_queryString = nullptr;
    }
    if (callSitePlan->m_tupDesc != nullptr) {
        FreeTupleDesc(callSitePlan->m_tupDesc);
        callSitePlan->m_tupDesc = nullptr;
    }
}

static bool ValidateFuncArgList(List* argList)
{
    ListCell* lc = nullptr;
    int i = 0;
    foreach (lc, argList) {
        Expr* arg = (Expr*)lfirst(lc);
        if (IsA(arg, NamedArgExpr)) {
            NamedArgExpr* namedArg = (NamedArgExpr*)arg;
            MOT_LOG_TRACE("Rejecting invoke query with named argument %d: %s", i, namedArg->name);
            return false;
        }
    }
    return true;
}

FuncExpr* ValidateDirectInvokePlan(Query* query)
{
    if ((query->jointree) && (query->jointree->fromlist || query->jointree->quals)) {
        MOT_LOG_TRACE("ValidateDirectInvokePlan(): Disqualifying invoke query - FROM clause is not empty");
        return nullptr;
    }
    if (list_length(query->targetList) != 1) {
        MOT_LOG_TRACE(
            "ValidateDirectInvokePlan(): Disqualifying invoke query - target list does not contain exactly one entry");
        return nullptr;
    }

    TargetEntry* targetEntry = (TargetEntry*)linitial(query->targetList);
    if (nodeTag(targetEntry->expr) != T_FuncExpr) {
        MOT_LOG_TRACE("ValidateDirectInvokePlan(): Disqualifying invoke query - single target entry is not a function "
                      "expression");
        return nullptr;
    }
    FuncExpr* result = (FuncExpr*)targetEntry->expr;
    if (!ValidateFuncArgList(result->args)) {
        result = nullptr;
    }
    return result;
}

FuncExpr* ValidateSelectInvokePlan(Query* query)
{
    RangeTblEntry* tableEntry = (RangeTblEntry*)linitial(query->rtable);
    if (tableEntry->rtekind != RTE_FUNCTION) {
        MOT_LOG_TRACE("ValidateSelectInvokePlan(): Disqualifying invoke query - table entry type is not FUNCTION");
        return nullptr;
    }
    if (nodeTag(tableEntry->funcexpr) != T_FuncExpr) {
        MOT_LOG_TRACE(
            "ValidateSelectInvokePlan(): Disqualifying invoke query - invalid table entry function expression type");
        return nullptr;
    }
    FuncExpr* result = (FuncExpr*)tableEntry->funcexpr;
    if (!ValidateFuncArgList(result->args)) {
        result = nullptr;
    }
    return result;
}

bool IsTargetListSingleRecordFunc(List* targetList)
{
    bool result = false;
    if (list_length(targetList) == 1) {
        TargetEntry* targetEntry = (TargetEntry*)linitial(targetList);
        if (nodeTag(targetEntry->expr) == T_FuncExpr) {
            FuncExpr* expr = (FuncExpr*)targetEntry->expr;
            if (expr->funcresulttype == RECORDOID) {
                result = true;
            }
        }
    }
    return result;
}

bool IsCallSiteJittedFunction(JitCallSite* callSite)
{
    bool result = false;
    if ((callSite->m_queryContext != nullptr) && (callSite->m_queryContext->m_commandType == JIT_COMMAND_INVOKE)) {
        JitQueryContext* queryContext = (JitQueryContext*)callSite->m_queryContext;
        if (queryContext->m_invokeContext != nullptr) {
            result = true;
        }
    }
    MOT_LOG_DEBUG("Call-site is composite: %s", result ? "true" : "false");
    return result;
}

static bool IsCallSitePlanJittedFunction(JitCallSitePlan* callSitePlan)
{
    bool result = false;
    if ((callSitePlan->m_queryPlan != nullptr) && (callSitePlan->m_queryPlan->_command_type == JIT_COMMAND_INVOKE)) {
        JitInvokePlan* invokePlan = (JitInvokePlan*)callSitePlan->m_queryPlan;
        if (invokePlan->m_functionPlan != nullptr) {
            result = true;
        }
    }
    MOT_LOG_DEBUG("Call-site is jitted function: %s", result ? "true" : "false");
    return result;
}

static uint8_t IsUnjittableInvokeQuery(JitCallSitePlan* callSitePlan, Query* query)
{
    uint8_t result = 0;
    if (callSitePlan->m_queryPlan == nullptr) {
        FuncExpr* funcExpr = nullptr;
        int tableCount = list_length(query->rtable);
        if (tableCount == 0) {
            funcExpr = ValidateDirectInvokePlan(query);
            result = 1;
        } else if (tableCount == 1) {
            funcExpr = ValidateSelectInvokePlan(query);
        }
        if (funcExpr != nullptr) {
            result = 1;
        }
    }
    return result;
}

static uint8_t IsModStmt(Query* query)
{
    uint8_t modStmt = 0;
    if (query->canSetTag) {
        if (query->commandType == CMD_INSERT || query->commandType == CMD_UPDATE || query->commandType == CMD_DELETE ||
            query->commandType == CMD_MERGE) {
            modStmt = 1;
        }
    }
    return modStmt;
}

static bool PrepareCallParamInfo(JitCallSitePlan* callSitePlan, FuncParamInfoList* paramInfoList,
    PLpgSQL_function* function, const ExprQueryAttrs& attrs, char* queryString)
{
    MOT_LOG_TRACE("Creating call param info of size %d for sub-query: %s", paramInfoList->m_numParams, queryString);
    callSitePlan->m_callParamCount = bms_num_members(attrs.m_paramNos);
    if (callSitePlan->m_callParamCount > 0) {
        size_t allocSize = sizeof(JitCallParamInfo) * callSitePlan->m_callParamCount;
        callSitePlan->m_callParamInfo = (JitCallParamInfo*)MOT::MemSessionAlloc(allocSize);
        if (callSitePlan->m_callParamInfo == nullptr) {
            MOT_LOG_ERROR(
                "Failed to allocate %u bytes for %d call parameters", allocSize, callSitePlan->m_callParamCount);
            return false;
        }
        errno_t erc = memset_s(callSitePlan->m_callParamInfo, allocSize, 0, allocSize);
        securec_check(erc, "\0", "\0");
    } else {
        callSitePlan->m_callParamInfo = nullptr;
    }

    // setup only used parameters
    int callParamIndex = 0;
    if (attrs.m_paramNos && !bms_is_empty(attrs.m_paramNos)) {
        int paramId = bms_next_member(attrs.m_paramNos, -1);
        while (paramId >= 0) {
            MOT_ASSERT(paramId < paramInfoList->m_numParams);
            MOT_ASSERT(callParamIndex < callSitePlan->m_callParamCount);
            callSitePlan->m_callParamInfo[callParamIndex].m_paramIndex = paramId;
            callSitePlan->m_callParamInfo[callParamIndex].m_paramType = paramInfoList->m_paramInfo[paramId].m_paramType;
            int datumIndex = paramInfoList->m_paramInfo[paramId].m_datumId;
            int argIndex = -1;
            // for SP parameters we save parameter index, and for locals we save datum index
            if (IsArgParam(function, datumIndex, &argIndex)) {
                MOT_LOG_TRACE("Query param %d, mapped to datum %d, is argument with original index %d",
                    paramId,
                    datumIndex,
                    argIndex);
                callSitePlan->m_callParamInfo[callParamIndex].m_paramKind = JitCallParamKind::JIT_CALL_PARAM_ARG;
                callSitePlan->m_callParamInfo[callParamIndex].m_invokeArgIndex = argIndex;
                callSitePlan->m_callParamInfo[callParamIndex].m_invokeDatumIndex = -1;
            } else {
                MOT_LOG_TRACE("Query param %d, mapped to datum %d, is local var", paramId, datumIndex);
                callSitePlan->m_callParamInfo[callParamIndex].m_paramKind = JitCallParamKind::JIT_CALL_PARAM_LOCAL;
                callSitePlan->m_callParamInfo[callParamIndex].m_invokeArgIndex = -1;
                callSitePlan->m_callParamInfo[callParamIndex].m_invokeDatumIndex = datumIndex;
            }
            paramId = bms_next_member(attrs.m_paramNos, paramId);
            ++callParamIndex;
        }
    }

    return true;
}

static bool PrepareStmtQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, FuncParamInfoList* paramInfoList,
    PLpgSQL_function* function, JitCallSitePlan* callSitePlanList, int queryCount, int exprIndex, bool into,
    int* queryIndex)
{
    char* queryString = expr->query;

    // parse query and try to check if jittable and then create code for it
    ExprQueryAttrs attrs;
    if (!GetExprQueryAttrs(expr, function, &attrs)) {
        MOT_LOG_TRACE("Failed to get query attributes from expression: %s", expr->query);
        return false;
    }
    MOT_LOG_TRACE("Preparing function query: %s", queryString);

    // we now check for a simple expression, which is OK
    bool failed = false;
    bool isSimpleExpr = IsSimpleExpr(attrs.m_spiPlan, attrs.m_query, &failed);
    if (failed) {
        CleanupExprQueryAttrs(&attrs);
        MOT_LOG_TRACE("Failed to prepare query - cannot determine expression type");
        return false;
    }
    if (isSimpleExpr) {
        CleanupExprQueryAttrs(&attrs);
        MOT_LOG_TRACE("Found simple expression: %s", queryString);
        return true;
    }

    // at this point, if there are no vacant slots in the call site list, then we have a bug in query counting
    JitCallSitePlan* callSitePlan = &callSitePlanList[*queryIndex];
    if (*queryIndex >= queryCount) {
        CleanupExprQueryAttrs(&attrs);
        MOT_LOG_TRACE("Missing vacant slot in call site plan list");
        return false;
    }

    // prepare called query plan (in case of non-jittable MOT query, we prepare a call site without a query plan)
    callSitePlan->m_queryPlan = IsJittableQuery(attrs.m_query, queryString, true);
    if (callSitePlan->m_queryPlan == nullptr) {
        // if this is an MOT query we still continue with unjittable call site, otherwise we disqualify
        // stored-procedure with non-MOT query
        MOT_LOG_TRACE("Sub-query is not jittable, checking for MOT query: %s", queryString);
        StorageEngineType storageEngineType = SE_TYPE_UNSPECIFIED;
        CheckTablesStorageEngine(attrs.m_query, &storageEngineType);
        if (storageEngineType != SE_TYPE_MOT) {
            DestroyCallSitePlan(callSitePlan);
            CleanupExprQueryAttrs(&attrs);
            MOT_LOG_TRACE("Sub-query is not MOT query (%d): %s", (int)storageEngineType, queryString);
            return false;
        }
    }

    // prepare call parameters
    if (!PrepareCallParamInfo(callSitePlan, paramInfoList, function, attrs, queryString)) {
        MOT_LOG_ERROR("Failed to prepare call parameters for sub-query: %s", queryString);
        DestroyCallSitePlan(callSitePlan);
        CleanupExprQueryAttrs(&attrs);
        return false;
    }

    // set new attributes
    callSitePlan->m_queryString = DupString(queryString, JitContextUsage::JIT_CONTEXT_LOCAL);
    if (callSitePlan->m_queryString == nullptr) {
        MOT_LOG_ERROR("Failed to duplicate called query string: %s", queryString);
        DestroyCallSitePlan(callSitePlan);
        CleanupExprQueryAttrs(&attrs);
        return false;  // safe cleanup
    }

    // save the expression for later parsing, it will be still valid by the time we reach code-generation phase
    callSitePlan->m_expr = expr;
    callSitePlan->m_exprIndex = exprIndex;
    MOT_LOG_TRACE("Assigned call site plan %d expression %p at index %d with query %s",
        *queryIndex,
        expr,
        exprIndex,
        expr->query);

    // we keep this also for jittable sub-query as it might evolve into unjittable after revalidation
    callSitePlan->m_queryCmdType = attrs.m_query->commandType;

    // prepare a global tuple descriptor
    MemoryContext oldCtx = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
    if (IsTargetListSingleRecordFunc(attrs.m_query->targetList) && IsCallSitePlanJittedFunction(callSitePlan)) {
        // this is a call into a jitted function returning record (without "* from"), so we build tuple desc according
        // to the row datum receiving the result of this call query (so that composite tuple is not used in this case)
        if (row != nullptr) {
            callSitePlan->m_tupDesc = PrepareTupleDescFromRow(row, function);
        } else {
            MOT_LOG_TRACE("PrepareStmtQuery(): Disqualifying query - missing row when query returns record");
            DestroyCallSitePlan(callSitePlan);
            CleanupExprQueryAttrs(&attrs);
            return false;
        }
    } else {
        callSitePlan->m_tupDesc = ExecCleanTypeFromTL(attrs.m_query->targetList, false);
    }
    (void)MemoryContextSwitchTo(oldCtx);
    if (callSitePlan->m_tupDesc == nullptr) {
        MOT_LOG_TRACE("Failed to create stored-procedure sub-query tuple descriptor from target list");
        DestroyCallSitePlan(callSitePlan);
        CleanupExprQueryAttrs(&attrs);
        return false;  // safe cleanup during destroy
    }
    Assert(callSitePlan->m_tupDesc->tdrefcount == -1);
    callSitePlan->m_isUnjittableInvoke = IsUnjittableInvokeQuery(callSitePlan, attrs.m_query);
    callSitePlan->m_isModStmt = IsModStmt(attrs.m_query);
    callSitePlan->m_isInto = static_cast<uint8_t>(into);
    callSitePlan->m_spiPlan = attrs.m_spiPlan;
    attrs.m_spiPlan = nullptr;  // keep plan alive
    ++(*queryIndex);
    CleanupExprQueryAttrs(&attrs);
    return true;
}

JitVisitResult JitFunctionQueryCounter::OnQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, int index, bool into)
{
    MOT_LOG_TRACE("Encountered expression %d at %p with query: %s", index, expr, expr->query);
    MOT_ASSERT(m_count == index);
    ++m_count;
    return JitVisitResult::JIT_VISIT_CONTINUE;
}

void JitFunctionQueryCounter::OnError(const char* stmtName, int lineNo)
{
    // impossible to arrive here, we just passed validation
    MOT_ASSERT(false);
    MOT_LOG_TRACE("Encountered error in '%s' statement at line %d, while counting queries", stmtName, lineNo);
}

JitVisitResult JitFunctionQueryCollector::OnQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, int index, bool into)
{
    bool result = PrepareStmtQuery(
        expr, row, m_paramInfoList, m_function, m_callSitePlanList, m_queryCount, index, into, &m_actualQueryCount);
    return (result ? JitVisitResult::JIT_VISIT_CONTINUE : JitVisitResult::JIT_VISIT_ERROR);
}

void JitFunctionQueryCollector::OnError(const char* stmtType, int lineNo)
{
    MOT_LOG_TRACE("Unsupported %s at line %d", stmtType, lineNo);
}

JitVisitResult JitFunctionQueryVerifier::OnQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, int index, bool into)
{
    // during planning we consider every expression as jittable, it will fail during code-generation if it is not
    // supported
    return JitVisitResult::JIT_VISIT_CONTINUE;
}

void JitFunctionQueryVerifier::OnError(const char* stmtType, int lineNo)
{
    MOT_LOG_TRACE("Failed to verify query in '%s' statement at line number %d", stmtType, lineNo);
}

JitVisitResult JitSubQueryPlanGenerator::OnQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, int index, bool into)
{
    if (index != m_exprIndex) {
        return JitVisitResult::JIT_VISIT_CONTINUE;
    }

    MOT_LOG_TRACE("Generating SPI plan from expression %d at %p for query: %s", index, expr, expr->query);
    m_plan = GetSpiPlan(m_function, expr);
    if (m_plan == nullptr) {
        return JitVisitResult::JIT_VISIT_ERROR;
    }
    *m_expr = expr;
    return JitVisitResult::JIT_VISIT_STOP;
}

void JitSubQueryPlanGenerator::OnError(const char* stmtType, int lineNo)
{
    MOT_LOG_TRACE("Failed to generate SP query plan, during statement '%s' at line %d", stmtType, lineNo);
}
}  // namespace JitExec
