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
 * jit_plan_sp.cpp
 *    JIT execution plan generation for stored procedures.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_plan_sp.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION: Be sure to include global.h before postgres.h to avoid conflict between libintl.h (included in global.h)
 * and c.h (included in postgres.h).
 */
#include "global.h"
#include "jit_plan.h"
#include "jit_plan_expr.h"
#include "jit_common.h"
#include "jit_source_map.h"
#include "mm_session_api.h"
#include "mm_global_api.h"
#include "utilities.h"
#include "jit_profiler.h"

#include "nodes/pg_list.h"
#include "catalog/pg_aggregate.h"
#include "storage/mot/jit_exec.h"

#include "utils/plpgsql.h"
#include "catalog/pg_language.h"
#include "parser/analyze.h"
#include "executor/executor.h"
#include "catalog/pg_proc.h"
#include "utils/builtins.h"

#include "jit_plan_sp.h"

#include <regex>

namespace JitExec {
DECLARE_LOGGER(JitPlanSp, JitExec)

static bool NameMatchesRegExList(const std::string& name, const std::string& regExList)
{
    std::string::size_type pos = 0;
    std::string::size_type nextPos = regExList.find(';');

    MOT_LOG_TRACE("Matching name %s against reg-ex list: %s", name.c_str(), regExList.c_str());
    while (pos < regExList.length()) {
        std::string regExStr =
            (nextPos != std::string::npos) ? regExList.substr(pos, nextPos - pos) : regExList.substr(pos);
        std::regex regEx(regExStr);
        if (std::regex_match(name, regEx)) {
            MOT_LOG_TRACE("Name %s matches regular expression %s", name.c_str(), regExStr.c_str());
            return true;
        }
        if (nextPos == std::string::npos) {
            break;
        }
        pos = nextPos + 1;
        nextPos = regExList.find(';', pos);
    }
    MOT_LOG_TRACE("Name %s does not match regular expression list %s", name.c_str(), regExList.c_str());
    return false;
}

static bool IsFunctionAllowed(const std::string& functionName)
{
    return NameMatchesRegExList(functionName, MOT::GetGlobalConfiguration().m_spCodegenAllowed);
}

static bool IsFunctionProhibited(const std::string& functionName)
{
    return NameMatchesRegExList(functionName, MOT::GetGlobalConfiguration().m_spCodegenProhibited);
}

static bool IsPureFunctionAllowed(const std::string& functionName)
{
    return NameMatchesRegExList(functionName, MOT::GetGlobalConfiguration().m_pureSPCodegen);
}

static JitFunctionPlan* GetFunctionPlan(const char* functionName, Oid functionOid)
{
    // we embed function id in qualified function name to distinguish between overrides of the same function
    MOT::mot_string qualifiedFunctionName;
    if (!qualifiedFunctionName.format("%s.%u", functionName, (unsigned)functionOid)) {
        MOT_LOG_TRACE("Failed to format qualified function name");
        return nullptr;
    }

    // enforce white-list with regular expressions here for allowed function names
    std::string fname(functionName);
    if (!IsFunctionAllowed(fname)) {
        MOT_LOG_TRACE("Invoked SP %s disqualified for JIT compilation by allowed white-list", functionName);
        return nullptr;
    }

    // enforce black list with regular expressions here for disallowed function names
    if (IsFunctionProhibited(fname)) {
        MOT_LOG_TRACE("Invoked SP %s disqualified for JIT compilation by prohibited black-list", functionName);
        return nullptr;
    }

    if (u_sess->mot_cxt.jit_compile_depth > MOT_JIT_MAX_COMPILE_DEPTH) {
        MOT_LOG_TRACE("GetFunctionPlan(): Reached maximum compile depth");
        return nullptr;
    }

    SPIAutoConnect spiAutoConn;
    if (!spiAutoConn.IsConnected()) {
        int rc = spiAutoConn.GetErrorCode();
        MOT_LOG_TRACE("Failed to connect to SPI while triggering function %s/%u compilation: %s (%u)",
            functionName,
            functionOid,
            SPI_result_code_string(rc),
            rc);
        return nullptr;
    }

    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile HeapTuple procTuple = nullptr;
    volatile PLpgSQL_function* func = nullptr;
    volatile JitPlan* plan = nullptr;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    PG_TRY();
    {
        // take read lock first before we try to get compiled function, this avoids race between us and committer
        procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
        if (!HeapTupleIsValid(procTuple)) {
            MOT_LOG_TRACE("SP %s disqualified: Oid %u not found in pg_proc", functionName, functionOid);
        } else {
            // now we trigger compilation of the function at this point unconditionally
            // attention: if compilation failed then ereport is thrown.
            func = GetPGCompiledFunction(functionOid, functionName);
            if (func == nullptr) {
                MOT_LOG_TRACE("Invoked SP %s disqualified for JIT compilation: No compilation result for function %u",
                    functionName,
                    functionOid);
            } else {
                ++func->use_count;
                MOT_LOG_TRACE("GetFunctionPlan(): Increased use count of function %p to %lu: %s",
                    func,
                    func->use_count,
                    functionName);
                // now we try to prepare plan for the function
                plan = IsJittableFunction((PLpgSQL_function*)func, procTuple, functionOid, true);
            }
        }
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN(
            "Caught exception while preparing function %s/%u JIT plan: %s", functionName, functionOid, edata->message);
        ereport(WARNING,
            (errmodule(MOD_MOT),
                errmsg("Caught exception while preparing function %s/%u JIT plan: %s",
                    functionName,
                    functionOid,
                    edata->message),
                errdetail("%s", edata->detail)));
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();

    // cleanup
    if (procTuple != nullptr) {
        ReleaseSysCache(procTuple);
    }

    // function use count is kept positive until code-generation phase finishes
    if ((func != nullptr) && (plan == nullptr)) {
        --func->use_count;
        MOT_LOG_TRACE(
            "GetFunctionPlan(): Decreased use count of function %p to %lu: %s", func, func->use_count, functionName);
    }
    return (JitFunctionPlan*)plan;
}

static List* GetFuncDefaultParams(
    Oid functionOid, int passedArgCount, int** defaultParamPosArray, int* defaultParamCount)
{
    // code adapted from GetDefaultVale() called by func_get_detail()
    HeapTuple tuple = nullptr;
    Oid* argTypes = nullptr;
    char** argNames = nullptr;
    char* argModes = nullptr;
    char* defaultsStr = nullptr;
    List* defaults = nullptr;
    int allArgCount = 0;

    do {  // instead of goto
        tuple = SearchSysCache1(PROCOID, functionOid);
        if (!HeapTupleIsValid(tuple)) {
            MOT_LOG_TRACE("Failed to find function by id: %u", functionOid);
            break;
        }

        // get defaults list
        bool isNull = false;
        Datum proArgDefaults = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_proargdefaults, &isNull);
        if (isNull) {
            MOT_LOG_TRACE("Unexpected null attribute 'proargdefaults' in function %u", functionOid);
            break;
        }
        defaultsStr = TextDatumGetCString(proArgDefaults);
        MOT_LOG_TRACE("Seeing default parameter string: %s", defaultsStr);
        defaults = (List*)stringToNode(defaultsStr);
        if (!IsA(defaults, List)) {
            MOT_LOG_TRACE("Unexpected non-list type for attribute 'proargdefaults' in function %u", functionOid);
            break;
        }

        // get defaults position list
        allArgCount = get_func_arg_info(tuple, &argTypes, &argNames, &argModes);
        Form_pg_proc procStruct = (Form_pg_proc)GETSTRUCT(tuple);
        int argCount = procStruct->pronargs;
        *defaultParamCount = procStruct->pronargdefaults;

        int2vector* defaultArgPos = nullptr;
        if (argCount <= FUNC_MAX_ARGS_INROW) {
            Datum defArgPosDatum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prodefaultargpos, &isNull);
            if (isNull) {
                MOT_LOG_TRACE("Unexpected null attribute 'prodefaultargpos' in function %u", functionOid);
                break;
            }
            defaultArgPos = (int2vector*)DatumGetPointer(defArgPosDatum);
        } else {
            Datum defArgPosDatum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prodefaultargposext, &isNull);
            if (isNull) {
                MOT_LOG_TRACE("Unexpected null attribute 'prodefaultargposext' in function %u", functionOid);
                break;
            }
            defaultArgPos = (int2vector*)PG_DETOAST_DATUM(defArgPosDatum);
        }

        // do some sanity checks
        if (passedArgCount > argCount) {
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
                "JIT Plan",
                "Too many arguments (%d) passed to function %u accepting at most %u arguments",
                passedArgCount,
                functionOid,
                argCount);
            defaults = nullptr;
            break;
        }
        if (allArgCount < argCount) {
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
                "JIT Plan",
                "Inconsistent function %u state: all argument count %d is less than argument count %d",
                functionOid,
                allArgCount,
                argCount);
            defaults = nullptr;
            break;
        }

        // fetch the position array
        FetchDefaultArgumentPos(defaultParamPosArray, defaultArgPos, argModes, allArgCount);
    } while (0);

    pfree_ext(argTypes);
    pfree_ext(argModes);
    if (argNames != nullptr) {
        for (int i = 0; i < allArgCount; i++) {
            pfree_ext(argNames[i]);
        }
        pfree_ext(argNames);
    }
    pfree_ext(defaultsStr);

    if (tuple != nullptr) {
        ReleaseSysCache(tuple);
    }
    return defaults;
}

static Oid GetExprTypeAndCollation(Expr* expr, Oid* collation)
{
    switch (nodeTag(expr)) {
        case T_Const: {
            Const* constExpr = (Const*)expr;
            *collation = constExpr->constcollid;
            return constExpr->consttype;
        }

        case T_Var: {
            Var* varExpr = (Var*)expr;
            *collation = varExpr->varcollid;
            return varExpr->vartype;
        }

        case T_Param: {
            Param* paramExpr = (Param*)expr;
            *collation = paramExpr->paramcollid;
            return paramExpr->paramtype;
        }

        case T_RelabelType: {
            RelabelType* relabelType = (RelabelType*)expr;
            *collation = relabelType->resultcollid;
            return relabelType->resulttype;
        }

        case T_OpExpr: {
            OpExpr* opExpr = (OpExpr*)expr;
            *collation = opExpr->opcollid;
            return opExpr->opresulttype;
        }

        case T_FuncExpr: {
            FuncExpr* funcExpr = (FuncExpr*)expr;
            *collation = funcExpr->funccollid;
            return funcExpr->funcresulttype;
        }

        case T_BoolExpr: {
            *collation = InvalidOid;
            return BOOLOID;
        }

        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* arrayExpr = (ScalarArrayOpExpr*)expr;
            *collation = arrayExpr->inputcollid;
            return BOOLOID;
        }

        default:
            break;
    }

    *collation = InvalidOid;
    return InvalidOid;
}

static JitExpr* MakeNullExpr(Oid resultType, Oid collationId)
{
    uint32_t allocSize = sizeof(JitConstExpr);
    JitConstExpr* jitExpr = (JitConstExpr*)MOT::MemSessionAlloc(allocSize);
    if (jitExpr == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "JIT Plan", "Failed to allocate %u bytes for constant expression", allocSize);
        return nullptr;
    }
    jitExpr->_expr_type = JIT_EXPR_TYPE_CONST;
    jitExpr->_source_expr = nullptr;
    jitExpr->_const_type = resultType;
    jitExpr->m_collationId = collationId;
    jitExpr->_value = PointerGetDatum(nullptr);
    jitExpr->_is_null = true;
    return (JitExpr*)jitExpr;
}

static JitExpr* MakeNullExpr(Expr* expr)
{
    Oid collationId = InvalidOid;
    Oid resultType = GetExprTypeAndCollation(expr, &collationId);
    if (resultType == InvalidOid) {
        MOT_LOG_TRACE("Failed to retrieve expression result type and collation");
        return nullptr;
    }
    return MakeNullExpr(resultType, collationId);
}

extern FuncExpr* GetFuncExpr(Query* query)
{
    FuncExpr* funcExpr = nullptr;
    int tableCount = list_length(query->rtable);
    if (tableCount == 0) {
        funcExpr = ValidateDirectInvokePlan(query);
    } else if (tableCount == 1) {
        funcExpr = ValidateSelectInvokePlan(query);
    }
    return funcExpr;
}

extern JitPlan* JitPrepareInvokePlan(Query* query)
{
    if (!IsMotSPCodegenEnabled()) {
        MOT_LOG_TRACE("Disqualifying invoke query - code generation for stored procedures is disabled");
        return nullptr;
    }

    JitPlan* plan = nullptr;
    MOT_LOG_TRACE("Preparing INVOKE plan");
    uint64_t startTime = GetSysClock();

    if (query->commandType != CMD_SELECT) {
        MOT_LOG_TRACE("JitPrepareInvokePlan(): Disqualifying invoke query - not a SELECT command");
        return nullptr;
    }
    if (!CheckQueryAttributes(query, false, false, false)) {
        MOT_LOG_TRACE("JitPrepareInvokePlan(): Disqualifying invoke query - Invalid query attributes");
        return nullptr;
    }

    // disallow invoke that returns a record (tuple with single composite attribute)
    // this happens when the SP has more than one output parameters, and we invoke the SP like this:
    //      "select SP_NAME(...)"
    // we support only this kind of invocation:
    //      "select * from SP_NAME(...)"
    if (IsTargetListSingleRecordFunc(query->targetList)) {
        if (u_sess->mot_cxt.jit_compile_depth == 0) {
            MOT_LOG_TRACE("JitPrepareInvokePlan(): Disqualifying query - Return type is record in top-level query");
            return nullptr;
        }
    }

    FuncExpr* funcExpr = GetFuncExpr(query);
    if (funcExpr == nullptr) {
        MOT_LOG_TRACE("JitPrepareInvokePlan(): Disqualifying invoke query - Invalid invoke plan");
        return nullptr;
    }

    HeapTuple procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcExpr->funcid));
    if (!HeapTupleIsValid(procTuple)) {
        MOT_LOG_TRACE(
            "JitPrepareInvokePlan(): Disqualifying invoke query - Failed to find function by id: %u", funcExpr->funcid);
        return nullptr;
    }

    // Validate the procedure first
    Form_pg_proc procStruct = (Form_pg_proc)GETSTRUCT(procTuple);
    char* funcName = pstrdup(NameStr(procStruct->proname));
    bool isNull = false;
    Datum prokindDatum = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_prokind, &isNull);
    bool proIsProcedure = isNull ? false : PROC_IS_PRO(CharGetDatum(prokindDatum));
    Datum packageDatum = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_package, &isNull);
    bool isPackage = isNull ? false : DatumGetBool(packageDatum);
    TransactionId funcXmin = HeapTupleGetRawXmin(procTuple);
    ReleaseSysCache(procTuple);

    if (!proIsProcedure) {
        MOT_LOG_TRACE(
            "JitPrepareInvokePlan(): Disqualifying invoke query - Skipping function %s as it not a SP", funcName);
        pfree(funcName);
        return nullptr;
    }

    if (isPackage) {
        MOT_LOG_TRACE(
            "JitPrepareInvokePlan(): Disqualifying invoke query - Skipping SP %s as it is a package", funcName);
        pfree(funcName);
        return nullptr;
    }

    JitFunctionPlan* funcPlan = nullptr;
    JitExpr** args = nullptr;
    JitExpr** defaultParams = nullptr;
    char* funcNameCopy = nullptr;
    int argNum = 0;
    int usedDefaultParamCount = 0;

    do {  // instead of goto
        Oid functionId = funcExpr->funcid;
        Oid* argTypes = nullptr;
        int nargs = 0;
        funcPlan = GetFunctionPlan(funcName, functionId);
        if (funcPlan == nullptr) {
            if (GetActiveNamespaceFunctionId() == InvalidOid) {
                MOT_LOG_TRACE("JitPrepareInvokePlan(): Disqualifying invoke query - Failed to prepare JIT plan for"
                              " function %s on top-level query",
                    funcName);
                break;
            }
            MOT_LOG_TRACE("JitPrepareInvokePlan(): Preparing plan for unjittable invoked function %s by id %u",
                funcName,
                functionId);
            (void)get_func_signature(functionId, &argTypes, &nargs);
        } else {
            MOT_LOG_TRACE("Using function %u plan with txn id %" PRIu64, functionId, funcPlan->m_function->fn_xmin);
        }

        // add default parameters if needed
        // NOTE: we do not support yet NamedArgExpr for CallableStatement or named-notation call style
        int passedArgCount = list_length(funcExpr->args);
        int requiredArgCount = funcPlan ? (int)funcPlan->m_argCount : nargs;
        if (passedArgCount < requiredArgCount) {
            // get the default parameter list with positions
            MOT_LOG_TRACE("Preparing invoke query with default parameters");
            int* defaultParamPosArray = nullptr;
            int defaultParamCount = 0;
            List* defaultParamList =
                GetFuncDefaultParams(functionId, passedArgCount, &defaultParamPosArray, &defaultParamCount);
            if (defaultParamList == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_UNAVAILABLE,
                    "JIT Plan",
                    "Failed to get default parameters for function %u: %s",
                    functionId,
                    funcName);
                break;
            }

            // we do our best to skip any holes in the default parameter list, leaving unspecified parameters as nulls
            // for now we do so only for positional call-style (i.e. parameters are NOT named),
            // UNLIKE PG, we do not just append the required amount of defaults, but we actually append the defaults in
            // their expected position
            usedDefaultParamCount = requiredArgCount - passedArgCount;  // including any holes

            // allocate default parameter array
            size_t allocSize = sizeof(JitExpr*) * usedDefaultParamCount;
            defaultParams = (JitExpr**)MOT::MemSessionAlloc(allocSize);
            if (defaultParams == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Prepare JIT Invoke Plan",
                    "Failed to allocate %u bytes for %d function default parameters",
                    (unsigned)allocSize,
                    usedDefaultParamCount);
                break;
            }
            errno_t erc = memset_s(defaultParams, allocSize, 0, allocSize);
            securec_check(erc, "\0", "\0");

            int i = 0;
            ListCell* lc = nullptr;
            int defaultParamIndex = 0;
            bool failed = false;
            foreach (lc, defaultParamList) {
                // skip initial defaults already passed as arguments
                Expr* subExpr = (Expr*)lfirst(lc);
                int pos = defaultParamPosArray[i];
                if (pos < passedArgCount) {
                    MOT_LOG_TRACE("Skipping default parameter %d", i);
                    ++i;
                    continue;
                }
                // skip the hole
                int defaultPos = passedArgCount + defaultParamIndex;
                while (defaultPos < pos) {
                    MOT_LOG_TRACE("Skipping missing default parameter at position %d", defaultPos);
                    JitExpr* defaultParam = MakeNullExpr(subExpr);
                    if (defaultParam == nullptr) {
                        MOT_LOG_TRACE("Failed to parse default parameter expression");
                        failed = true;
                        break;
                    }
                    defaultParams[defaultParamIndex++] = defaultParam;
                    defaultPos = passedArgCount + defaultParamIndex;
                }
                if (failed) {
                    break;
                }
                MOT_ASSERT(defaultPos == pos);
                MOT_LOG_TRACE("Using default parameter %d in position %d", i, defaultPos);
                JitExpr* defaultParam = parseExpr(query, subExpr, 0);
                if (defaultParam == nullptr) {
                    MOT_LOG_TRACE("Failed to parse default parameter expression");
                    failed = true;
                    break;
                }
                defaultParams[defaultParamIndex++] = defaultParam;
                ++i;
            }
            if (failed) {
                break;
            }
            // fill in any missing trailing default parameters
            for (int j = defaultParamIndex; j < usedDefaultParamCount; ++j) {
                Oid argType = (funcPlan != nullptr) ? funcPlan->m_paramTypes[j] : argTypes[j];
                JitExpr* defaultParam = MakeNullExpr(argType, InvalidOid);
                if (defaultParam == nullptr) {
                    MOT_LOG_TRACE("Failed to prepare null default parameter");
                    failed = true;
                    break;
                }
                defaultParams[j] = defaultParam;
            }
        }

        if (passedArgCount > 0) {
            size_t allocSize = sizeof(JitExpr*) * passedArgCount;
            args = (JitExpr**)MOT::MemSessionAlloc(allocSize);
            if (args == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Prepare JIT Invoke Plan",
                    "Failed to allocate %u bytes for %d function arguments",
                    (unsigned)allocSize,
                    passedArgCount);
                break;
            }

            ListCell* lc;
            foreach (lc, funcExpr->args) {
                Expr* subExpr = (Expr*)lfirst(lc);
                // each parameter is a top-level expression at position 0
                JitExpr* arg = parseExpr(query, subExpr, 0);
                if (arg == nullptr) {
                    MOT_LOG_TRACE("JitPrepareInvokePlan(): Disqualifying invoke query - Failed to process function "
                                  "sub-expression %d",
                        argNum);
                    break;
                } else {
                    args[argNum++] = arg;
                }
            }

            if (argNum < passedArgCount) {  // failure
                break;
            }
        }

        // allocate function name and copy it
        size_t funcNameLen = strlen(funcName) + 1;
        funcNameCopy = (char*)MOT::MemSessionAlloc(funcNameLen);
        if (funcNameCopy == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Prepare JIT Invoke Plan",
                "Failed to allocate %u bytes for function name",
                (unsigned)funcNameLen);
            break;
        }
        errno_t erc = strcpy_s(funcNameCopy, funcNameLen, funcName);
        securec_check(erc, "\0", "\0");

        // allocate plan
        size_t allocSize = sizeof(JitInvokePlan);
        JitInvokePlan* invokePlan = (JitInvokePlan*)MOT::MemSessionAlloc(allocSize);
        if (invokePlan == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Prepare JIT Invoke Plan",
                "Failed to allocate %u bytes for invoke plan",
                (unsigned)allocSize);
            break;
        }
        erc = memset_s(invokePlan, allocSize, 0, allocSize);
        securec_check(erc, "\0", "\0");

        invokePlan->_plan_type = JIT_PLAN_INVOKE;
        invokePlan->_command_type = JIT_COMMAND_INVOKE;
        invokePlan->m_functionPlan = funcPlan;
        invokePlan->_function_id = functionId;
        invokePlan->_function_name = funcNameCopy;
        invokePlan->m_functionTxnId = funcXmin;
        invokePlan->_args = args;
        invokePlan->_arg_count = passedArgCount;
        invokePlan->m_defaultParams = defaultParams;
        invokePlan->m_defaultParamCount = usedDefaultParamCount;
        plan = (JitPlan*)invokePlan;
    } while (0);

    if (plan == nullptr) {  // cleanup on failure
        if (funcNameCopy != nullptr) {
            MOT::MemSessionFree(funcNameCopy);
        }
        if (args != nullptr) {
            for (int i = 0; i < argNum; ++i) {
                if (args[i] != nullptr) {
                    freeExpr(args[i]);
                }
            }
            MOT::MemSessionFree(args);
        }
        if (defaultParams != nullptr) {
            for (int i = 0; i < usedDefaultParamCount; ++i) {
                if (defaultParams[i] != nullptr) {
                    freeExpr(defaultParams[i]);
                }
            }
            MOT::MemSessionFree(defaultParams);
        }
        if (funcPlan != nullptr) {
            JitDestroyPlan((JitPlan*)funcPlan);
        }
    }
    pfree(funcName);

    if (plan != nullptr) {
        uint64_t endTime = GetSysClock();
        uint64_t planTimeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
        MOT_LOG_TRACE("INVOKE Plan time %" PRIu64 " micros", planTimeMicros);
    }
    return plan;
}

static bool RegenerateCallParamInfo(JitCallSite* callSite, FuncParamInfoList* paramInfoList,
    PLpgSQL_function* function, const ExprQueryAttrs& attrs, char* queryString, JitContextUsage usage)
{
    callSite->m_callParamCount = bms_num_members(attrs.m_paramNos);
    MOT_LOG_TRACE("Creating call param info of size %d/%d for sub-query: %s",
        callSite->m_callParamCount,
        paramInfoList->m_numParams,
        queryString);
    if (callSite->m_callParamCount > 0) {
        size_t allocSize = sizeof(JitCallParamInfo) * callSite->m_callParamCount;
        callSite->m_callParamInfo = (JitCallParamInfo*)JitMemAlloc(allocSize, usage);
        if (callSite->m_callParamInfo == nullptr) {
            MOT_LOG_ERROR("Failed to allocate %u bytes for %d call parameters", allocSize, callSite->m_callParamCount);
            return false;
        }
        errno_t erc = memset_s(callSite->m_callParamInfo, allocSize, 0, allocSize);
        securec_check(erc, "\0", "\0");
    } else {
        callSite->m_callParamInfo = nullptr;
    }

    int callParamIndex = 0;
    if (attrs.m_paramNos && !bms_is_empty(attrs.m_paramNos)) {
        int paramId = bms_next_member(attrs.m_paramNos, -1);
        while (paramId >= 0) {
            MOT_ASSERT(paramId < paramInfoList->m_numParams);
            MOT_ASSERT(callParamIndex < callSite->m_callParamCount);
            callSite->m_callParamInfo[callParamIndex].m_paramIndex = paramId;
            callSite->m_callParamInfo[callParamIndex].m_paramType = paramInfoList->m_paramInfo[paramId].m_paramType;
            int datumIndex = paramInfoList->m_paramInfo[paramId].m_datumId;
            int argIndex = -1;
            // for SP parameters we save parameter index, and for locals we save datum index
            if (IsArgParam(function, datumIndex, &argIndex)) {
                MOT_LOG_TRACE("Query param %d, mapped to datum %d, is argument with original index %d",
                    paramId,
                    datumIndex,
                    argIndex);
                callSite->m_callParamInfo[callParamIndex].m_paramKind = JitCallParamKind::JIT_CALL_PARAM_ARG;
                callSite->m_callParamInfo[callParamIndex].m_invokeArgIndex = argIndex;
                callSite->m_callParamInfo[callParamIndex].m_invokeDatumIndex = -1;
            } else {
                MOT_LOG_TRACE("Query param %d, mapped to datum %d, is local var", paramId, datumIndex);
                callSite->m_callParamInfo[callParamIndex].m_paramKind = JitCallParamKind::JIT_CALL_PARAM_LOCAL;
                callSite->m_callParamInfo[callParamIndex].m_invokeArgIndex = -1;
                callSite->m_callParamInfo[callParamIndex].m_invokeDatumIndex = datumIndex;
            }
            paramId = bms_next_member(attrs.m_paramNos, paramId);
            ++callParamIndex;
        }
    }

    return true;
}

static bool RegenerateSetNewQueryContext(JitCallSite* callSite, MotJitContext* jitContext, char* queryString,
    JitContextUsage usage, const ExprQueryAttrs& attrs)
{
    // cleanup old attributes
    if (callSite->m_queryContext != nullptr) {
        DestroyJitContext(callSite->m_queryContext);
        callSite->m_queryContext = nullptr;
    }
    if (callSite->m_queryString != nullptr) {
        MOT_ASSERT(IsJitContextUsageGlobal(usage));
        JitMemFree(callSite->m_queryString, usage);
        callSite->m_queryString = nullptr;
    }

    // set new attributes
    callSite->m_queryContext = jitContext;
    if (jitContext != nullptr) {
        callSite->m_queryString = nullptr;
        callSite->m_queryContext->m_validState = JIT_CONTEXT_VALID;
    } else {
        callSite->m_queryString = DupString(queryString, usage);
        if (callSite->m_queryString == nullptr) {
            return false;
        }
        MOT_LOG_TRACE("RegenerateStmtQuery(): Cloned string %p on %s scope into call site %p: %s",
            callSite->m_queryString,
            JitContextUsageToString(usage),
            callSite,
            callSite->m_queryString);
    }
    // we keep this also for jittable sub-query as it might evolve into unjittable after revalidation
    callSite->m_queryCmdType = attrs.m_query->commandType;
    return true;
}

static bool RegenerateTupleDesc(
    JitCallSite* callSite, PLpgSQL_row* row, PLpgSQL_function* function, const ExprQueryAttrs& attrs)
{
    MemoryContext oldCtx = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
    if (IsTargetListSingleRecordFunc(attrs.m_query->targetList) && IsCallSiteJittedFunction(callSite)) {
        // this is a call into a jitted function returning record (without "* from"), so we build tuple desc according
        // to the row datum receiving the result of this call query (so that composite tuple is not used in this case)
        if (row != nullptr) {
            callSite->m_tupDesc = PrepareTupleDescFromRow(row, function);
        } else {
            MOT_LOG_TRACE("RegenerateStmtQuery(): Disqualifying query - missing row when query returns record");
            return false;
        }
    } else {
        callSite->m_tupDesc = ExecCleanTypeFromTL(attrs.m_query->targetList, false);
    }
    (void)MemoryContextSwitchTo(oldCtx);
    if (callSite->m_tupDesc == nullptr) {
        MOT_LOG_TRACE("Failed to create stored-procedure sub-query tuple descriptor from target list");
        return false;  // safe cleanup during destroy
    }
    Assert(callSite->m_tupDesc->tdrefcount == -1);
    return true;
}

extern bool RegenerateStmtQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, FuncParamInfoList* paramInfoList,
    PLpgSQL_function* function, JitCallSite* callSite, int queryCount, int subQueryIndex)
{
    // NOTE: This function is used for regenerating expired sub-query context.
    char* queryString = expr->query;
    MOT_LOG_TRACE("Regenerating sub-query plan: %s", queryString);

    // parse query and try to check if jittable and then create code for it
    ExprQueryAttrs attrs;
    if (!GetExprQueryAttrs(expr, function, &attrs)) {
        MOT_LOG_TRACE("Failed to get query attributes from expression: %s", queryString);
        return false;
    }

    // we now check for a simple expression, which is OK
    bool failed = false;
    bool isSimpleExpr = IsSimpleExpr(attrs.m_spiPlan, attrs.m_query, &failed);
    if (failed) {
        CleanupExprQueryAttrs(&attrs);
        MOT_LOG_TRACE("Failed to regenerate sub-query plan - cannot determine expression type");
        return false;
    }
    if (isSimpleExpr) {
        CleanupExprQueryAttrs(&attrs);
        MOT_LOG_TRACE("Found simple expression: %s", queryString);
        // by no means should this happen during code regeneration, nevertheless for release build we declare failure
        MOT_ASSERT(false);
        return false;
    }

    // at this point, if there are no vacant slots in the call site list, then we have a bug in query counting
    if (subQueryIndex >= queryCount) {
        MOT_ASSERT(false);  // by ne means should this happen during code regeneration
        CleanupExprQueryAttrs(&attrs);
        MOT_LOG_TRACE("Missing vacant slot in call site list");
        return false;
    }

    JitPlan* jitPlan = IsJittableQuery(attrs.m_query, queryString);
    if (jitPlan == nullptr) {
        // if this is an MOT query we still continue
        StorageEngineType storageEngineType = SE_TYPE_UNSPECIFIED;
        CheckTablesStorageEngine(attrs.m_query, &storageEngineType);
        if (storageEngineType != SE_TYPE_MOT) {
            CleanupExprQueryAttrs(&attrs);
            MOT_LOG_TRACE("Sub-query is not jittable: %s", queryString);
            return false;
        }
    }

    JitContextUsage usage = JitContextUsage::JIT_CONTEXT_LOCAL;
    if (callSite->m_queryContext != nullptr) {
        usage = callSite->m_queryContext->m_usage;
    }
    MotJitContext* jitContext = nullptr;
    if (jitPlan != nullptr) {
        jitContext = JitCodegenQuery(attrs.m_query, queryString, jitPlan, callSite->m_queryContext->m_usage);
        if (jitContext == nullptr) {
            CleanupExprQueryAttrs(&attrs);
            JitDestroyPlan(jitPlan);
            MOT_LOG_TRACE("Failed to generate code for sub-query: %s", queryString);
            return false;
        }
        JitDestroyPlan(jitPlan);
        jitPlan = nullptr;

        // get rid of parameter list (child signature may change...)
        if (callSite->m_callParamInfo != nullptr) {
            JitMemFree(callSite->m_callParamInfo, usage);
            callSite->m_callParamInfo = nullptr;
        }

        // cleanup tuple descriptor
        if (callSite->m_tupDesc != nullptr) {
            FreeTupleDesc(callSite->m_tupDesc);
            callSite->m_tupDesc = nullptr;
        }

        MOT_ASSERT(jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY);
    }

    if (!RegenerateCallParamInfo(callSite, paramInfoList, function, attrs, queryString, usage)) {
        MOT_LOG_ERROR("Failed to create call parameters for sub-query: %s", queryString);
        CleanupExprQueryAttrs(&attrs);
        if (jitContext != nullptr) {
            DestroyJitContext(jitContext);
        }
        return false;
    }

    // cleanup old attributes and set new ones
    if (!RegenerateSetNewQueryContext(callSite, jitContext, queryString, usage, attrs)) {
        MOT_LOG_TRACE("Failed to set new sub-query context and query string");
        CleanupExprQueryAttrs(&attrs);
        // Even though RegenerateSetNewQueryContext() can fail only when jitContext is nullptr, we still add here
        // checking and destroy context for safety (if some code is changed in future).
        if (jitContext != nullptr) {
            DestroyJitContext(jitContext);
            callSite->m_queryContext = nullptr;
        }
        return false;  // safe cleanup during destroy
    }

    // prepare a global tuple descriptor
    if (!RegenerateTupleDesc(callSite, row, function, attrs)) {
        MOT_LOG_TRACE("RegenerateStmtQuery(): Disqualifying query - failed to setup tuple descriptor");
        CleanupExprQueryAttrs(&attrs);
        if (jitContext != nullptr) {
            DestroyJitContext(jitContext);
            callSite->m_queryContext = nullptr;
        }
        return false;  // safe cleanup during destroy
    }

    CleanupExprQueryAttrs(&attrs);
    return true;
}

static void DestroyCallSitePlanList(JitCallSitePlan* callSitePlanList, int queryCount)
{
    if (callSitePlanList == nullptr) {
        return;
    }

    for (int i = 0; i < queryCount; ++i) {
        DestroyCallSitePlan(&callSitePlanList[i]);
    }
    MOT::MemSessionFree(callSitePlanList);
}

inline bool CollectFunctionQueries(PLpgSQL_function* function, FuncParamInfoList* paramInfoList,
    JitCallSitePlan* callSitePlanList, int queryCount, int* actualQueryCount)
{
    JitFunctionQueryCollector queryCollector(paramInfoList, function, callSitePlanList, queryCount);
    bool result = VisitFunctionQueries(function, &queryCollector);
    if (result) {
        *actualQueryCount = queryCollector.GetActualQueryCount();
    }
    return result;
}

static JitCallSitePlan* PrepareFunctionQueries(
    PLpgSQL_function* function, FuncParamInfoList* paramInfoList, int queryCount, int* actualQueryCount)
{
    MOT_LOG_TRACE("Preparing %d function queries", queryCount);
    size_t allocSize = queryCount * sizeof(JitCallSitePlan);
    JitCallSitePlan* callSitePlanList = (JitCallSitePlan*)MOT::MemSessionAlloc(allocSize);
    if (callSitePlanList == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT Function Plan",
            "Failed to allocate %u bytes for function query plan list",
            (unsigned)allocSize);
        return nullptr;
    }

    errno_t erc = memset_s(callSitePlanList, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    // actual number of queries can be less than we expect, as there could be simple expressions, but we still expect
    // at least one MOT jitted query
    int preparedQueryCount = 0;
    bool result = CollectFunctionQueries(function, paramInfoList, callSitePlanList, queryCount, &preparedQueryCount);
    if (!result || (preparedQueryCount == 0)) {
        if (!result) {
            MOT_LOG_TRACE("Failed to prepare SP sub-queries (%d prepared successfully though)", preparedQueryCount);
        }
        DestroyCallSitePlanList(callSitePlanList, queryCount);
        callSitePlanList = nullptr;
    }

    // communicate back number of parsed queries only on success
    if (result) {
        *actualQueryCount = preparedQueryCount;
    }

    return callSitePlanList;
}

static bool CheckReturnType(PLpgSQL_function* function, const char* functionName)
{
    if (!IsTypeSupported(function->fn_rettype) && (function->fn_rettype != RECORDOID) &&
        (function->fn_rettype != VOIDOID)) {
        MOT_LOG_TRACE("CheckReturnType(): Disqualifying function %s with unsupported return type %d",
            functionName,
            function->fn_rettype);
        return false;
    }
    if (function->fn_retset) {
        MOT_LOG_TRACE("CheckReturnType(): Disqualifying function %s with return set", functionName);
        return false;
    }
    return true;
}

static bool IsConstExpr(PLpgSQL_expr* expr, PLpgSQL_function* function, bool* failed)
{
    // parse query and try to check if it is a simple constant
    ExprQueryAttrs attrs;
    if (!GetExprQueryAttrs(expr, function, &attrs)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Prepare JIT Function Plan", "Failed to get query from expression");
        return false;
    }
    char* queryString = expr->query;
    MOT_LOG_TRACE("Checking if expression query is constant: %s", queryString);

    // we now check for a simple expression, which is OK
    bool isConst = false;
    Oid constType = InvalidOid;
    bool isSimple = IsSimpleExpr(attrs.m_spiPlan, attrs.m_query, failed, &isConst, &constType);
    if (*failed) {
        MOT_LOG_TRACE("Failed to determine expression type");
        return false;
    }

    CleanupExprQueryAttrs(&attrs);
    if (isSimple && isConst && IsTypeSupported(constType)) {
        MOT_LOG_TRACE("Found simple constant expression: %s", queryString);
        return true;
    }

    return false;
}

static bool FunctionStmtSupported(PLpgSQL_function* function)
{
    JitFunctionQueryVerifier queryVerifier;
    return VisitFunctionQueries(function, &queryVerifier);
}

static bool CheckParameters(PLpgSQL_function* function, const char* functionName)
{
    // check all datum types
    for (int i = 0; i < function->ndatums; ++i) {
        if (i == function->sql_bulk_exceptions_varno) {
            // This datum is used only for SAVE EXCEPTIONS and SQL%BULK_EXCEPTION, which we don't support.
            continue;
        }

        PLpgSQL_datum* datum = function->datums[i];
        if (datum->dtype == PLPGSQL_DTYPE_VAR) {
            PLpgSQL_var* var = (PLpgSQL_var*)datum;
            if (!IsTypeSupported(var->datatype->typoid)) {
                MOT_LOG_TRACE("CheckParameters(): Disqualifying function %s with unsupported datum %d (dno %d) type %d",
                    functionName,
                    i,
                    datum->dno,
                    var->datatype->typoid);
                return false;
            }
        }
    }

    // check argument default values are constants
    for (int i = 0; i < function->fn_nargs; ++i) {
        int varno = function->fn_argvarnos[i];
        PLpgSQL_datum* datum = function->datums[varno];
        if (datum->dtype == PLPGSQL_DTYPE_VAR) {
            PLpgSQL_var* var = (PLpgSQL_var*)datum;
            if (var->default_val != nullptr) {
                bool failed = false;
                bool isConst = IsConstExpr(var->default_val, function, &failed);
                if (failed) {
                    MOT_LOG_TRACE("CheckParameters(): Disqualifying function %s - cannot determine constant expression"
                                  " type",
                        functionName);
                    return false;
                }
                if (!isConst) {
                    MOT_LOG_TRACE("CheckParameters(): Disqualifying function %s with unsupported default value for "
                                  "argument %d, datum %d (dno %d)",
                        functionName,
                        i,
                        varno,
                        datum->dno);
                    return false;
                }
            }
        }
    }

    return true;
}

extern JitPlan* JitPrepareFunctionPlan(
    PLpgSQL_function* function, const char* functionSource, const char* functionName, Oid functionOid, bool isStrict)
{
    MOT_LOG_TRACE("Preparing plan for function: %s with %d args", functionName, function->fn_nargs);
    uint64_t startTime = GetSysClock();

    // enforce white-list with regular expressions here for allowed function names
    std::string fname(functionName);
    if (!IsFunctionAllowed(fname)) {
        MOT_LOG_TRACE("Function disqualified for JIT compilation by allowed white-list");
        return nullptr;
    }

    // enforce black list with regular expressions here for disallowed function names
    if (IsFunctionProhibited(fname)) {
        MOT_LOG_TRACE("Function disqualified for JIT compilation by prohibited black-list");
        return nullptr;
    }

    // verify function return type is supported
    if (!CheckReturnType(function, functionName)) {
        MOT_LOG_TRACE("JitPreparePlan(): Disqualifying function %s with unsupported return type", functionName);
        return nullptr;
    }

    // make sure we have a valid SPI connection
    SPIAutoConnect spiAutoConn;
    if (!spiAutoConn.IsConnected()) {
        int rc = spiAutoConn.GetErrorCode();
        MOT_LOG_TRACE("Failed to connect to SPI while preparing plan for function %s: %s (%u)",
            functionName,
            SPI_result_code_string(rc),
            rc);
        return nullptr;
    }

    // verify function variable types are supported
    if (!CheckParameters(function, functionName)) {
        MOT_LOG_TRACE("JitPreparePlan(): Disqualifying function %s with unsupported parameters types", functionName);
        return nullptr;
    }

    // verify function statements are jittable
    if (!FunctionStmtSupported(function)) {
        MOT_LOG_TRACE("JitPreparePlan(): Disqualifying function %s with unsupported statement", functionName);
        return nullptr;
    }

    // disqualify functions that do not execute sub-queries at all
    int queryCount = CountFunctionQueries(function);
    if (queryCount == 0) {
        MOT_LOG_TRACE("JitPreparePlan(): Disqualifying function %s: no MOT sub-queries", functionName);
        return nullptr;
    }

    // prepare function parameters info for parsing sub-queries
    FuncParamInfoList paramInfoList = {};
    if (!PrepareFuncParamInfoList(function, &paramInfoList)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Prepare JIT Function Plan",
            "Failed to prepare parameter information for stored procedure %s execution plan",
            functionName);
        return nullptr;
    }

    // for profiler to work correctly, we need to reserve parent function id, and also setup name-space
    if (MOT::GetGlobalConfiguration().m_enableCodegenProfile) {
        MOT::mot_string qualifiedFunctionName;
        if (!qualifiedFunctionName.format("%s.%u", functionName, (unsigned)functionOid)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Prepare JIT Function Plan",
                "Failed to prepare qualified function name for stored procedure %s execution plan",
                functionName);
            DestroyFuncParamInfoList(&paramInfoList);
            return nullptr;
        }
        if (!PushJitSourceNamespace(functionOid, qualifiedFunctionName.c_str())) {
            MOT_LOG_TRACE(
                "Failed to push JIT source namespace for preparing stored procedure %s execution plan", functionName);
            DestroyFuncParamInfoList(&paramInfoList);
            return nullptr;
        }
        // reserve slot for parent function id early enough, before child queries are processed
        (void)JitProfiler::GetInstance()->GetProfileFunctionId(GetActiveNamespace(), functionOid);
    }

    // verify all sub-queries are jittable
    int actualQueryCount = -1;
    JitCallSitePlan* callSitePlanList = PrepareFunctionQueries(function, &paramInfoList, queryCount, &actualQueryCount);

    if (MOT::GetGlobalConfiguration().m_enableCodegenProfile) {
        PopJitSourceNamespace();
    }

    bool failed = ((callSitePlanList == nullptr) && (actualQueryCount == -1));
    if (failed) {
        MOT_LOG_TRACE("Failed to prepare sub-query plans for stored procedure %s execution plan", functionName);
        DestroyFuncParamInfoList(&paramInfoList);
        return nullptr;
    }

    // disqualify functions that execute queries on non-MOT tables, unless they are in the white list
    bool unallowedPureFunction = ((actualQueryCount == 0) && !IsPureFunctionAllowed(functionName));
    if (unallowedPureFunction) {
        MOT_LOG_TRACE("JitPreparePlan(): Disqualifying function %s: no MOT sub-queries actually found", functionName);
        DestroyFuncParamInfoList(&paramInfoList);
        return nullptr;
    }

    // save all JIT contexts of all sub-queries in a map inside the plan.
    size_t allocSize = sizeof(JitFunctionPlan);
    JitFunctionPlan* functionPlan = (JitFunctionPlan*)MOT::MemSessionAlloc(allocSize);
    if (functionPlan == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT Function Plan",
            "Failed to allocate %u bytes for stored procedure %s execution plan",
            (unsigned)allocSize,
            functionName);
        DestroyCallSitePlanList(callSitePlanList, queryCount);
        DestroyFuncParamInfoList(&paramInfoList);
        return nullptr;
    }
    errno_t erc = memset_s(functionPlan, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    // allocate function name and copy it
    size_t funcNameLen = strlen(functionName) + 1;
    functionPlan->_function_name = (char*)MOT::MemSessionAlloc(funcNameLen);
    if (!functionPlan->_function_name) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT Function Plan",
            "Failed to allocate %u bytes for function name",
            (unsigned)funcNameLen);
        MOT::MemSessionFree(functionPlan);
        DestroyCallSitePlanList(callSitePlanList, queryCount);
        DestroyFuncParamInfoList(&paramInfoList);
        return nullptr;
    }
    erc = strcpy_s(functionPlan->_function_name, funcNameLen, functionName);
    securec_check(erc, "\0", "\0");

    functionPlan->_plan_type = JIT_PLAN_FUNCTION;
    functionPlan->_command_type = JIT_COMMAND_FUNCTION;
    functionPlan->_function_id = functionOid;
    functionPlan->m_function = function;
    allocSize = sizeof(Oid) * paramInfoList.m_numParams;
    functionPlan->m_paramTypes = (Oid*)MOT::MemSessionAlloc(allocSize);
    if (functionPlan->m_paramTypes == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT Function Plan",
            "Failed to allocate %u bytes for function parameter type array",
            (unsigned)funcNameLen);
        MOT::MemSessionFree(functionPlan->_function_name);
        MOT::MemSessionFree(functionPlan);
        DestroyCallSitePlanList(callSitePlanList, queryCount);
        DestroyFuncParamInfoList(&paramInfoList);
        return nullptr;
    }
    for (int i = 0; i < paramInfoList.m_numParams; ++i) {
        functionPlan->m_paramTypes[i] = paramInfoList.m_paramInfo[i].m_paramType;
    }
    functionPlan->m_paramCount = (uint32_t)paramInfoList.m_numParams;
    functionPlan->m_argCount = (uint32_t)function->fn_nargs;
    functionPlan->_query_count = actualQueryCount;
    functionPlan->m_callSitePlanList = callSitePlanList;

    DestroyFuncParamInfoList(&paramInfoList);

    uint64_t endTime = GetSysClock();
    uint64_t planTimeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
    MOT_LOG_TRACE("Plan time %" PRIu64 " micros for SP: %s", planTimeMicros, functionPlan->_function_name);
    return (JitPlan*)functionPlan;
}

extern void JitDestroyFunctionPlan(JitFunctionPlan* plan)
{
    if (plan->m_function != nullptr) {
        --plan->m_function->use_count;
        MOT_LOG_TRACE("JitDestroyFunctionPlan(): Decreased use count of function %p to %lu: %s",
            plan->m_function,
            plan->m_function->use_count,
            plan->_function_name);
        plan->m_function = nullptr;
    }
    if (plan->_function_name != nullptr) {
        MOT::MemSessionFree(plan->_function_name);
        plan->_function_name = nullptr;
    }
    if (plan->m_paramTypes != nullptr) {
        MOT::MemSessionFree(plan->m_paramTypes);
        plan->m_paramTypes = nullptr;
    }
    if (plan->m_callSitePlanList != nullptr) {
        DestroyCallSitePlanList(plan->m_callSitePlanList, plan->_query_count);
        plan->m_callSitePlanList = nullptr;
    }
    MOT::MemSessionFree(plan);
}

extern void JitDestroyInvokePlan(JitInvokePlan* plan)
{
    if (plan->_function_name != nullptr) {
        MOT::MemSessionFree(plan->_function_name);
    }
    if (plan->_args != nullptr) {
        for (int i = 0; i < plan->_arg_count; ++i) {
            if (plan->_args[i] != nullptr) {
                freeExpr(plan->_args[i]);
            }
        }
        MOT::MemSessionFree(plan->_args);
    }
    if (plan->m_defaultParams != nullptr) {
        for (int i = 0; i < plan->m_defaultParamCount; ++i) {
            if (plan->m_defaultParams[i] != nullptr) {
                freeExpr(plan->m_defaultParams[i]);
            }
        }
        MOT::MemSessionFree(plan->m_defaultParams);
    }
    if (plan->m_functionPlan != nullptr) {
        JitDestroyPlan((JitPlan*)plan->m_functionPlan);
    }
    MOT::MemSessionFree(plan);
}
}  // namespace JitExec
