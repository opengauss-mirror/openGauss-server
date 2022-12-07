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
 * jit_plan_sp_expr.h
 *    JIT SP execution plan expression helpers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_plan_sp_expr.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_PLAN_SP_EXPR_H
#define JIT_PLAN_SP_EXPR_H

#include "utils/plpgsql.h"
#include "nodes/parsenodes.h"
#include "jit_plan.h"

namespace JitExec {
struct FuncParamInfo {
    Oid m_paramType;
    char* m_paramName;
    int m_datumId;
};

struct FuncParamInfoList {
    int m_numParams;
    FuncParamInfo* m_paramInfo;
};

enum class JitVisitResult { JIT_VISIT_CONTINUE, JIT_VISIT_STOP, JIT_VISIT_ERROR };

class JitFunctionQueryVisitor {
public:
    JitFunctionQueryVisitor()
    {}

    virtual ~JitFunctionQueryVisitor()
    {}

    virtual JitVisitResult OnQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, int index, bool into) = 0;

    virtual void OnError(const char* stmtType, int lineNo) = 0;
};

class JitFunctionQueryCounter : public JitFunctionQueryVisitor {
public:
    JitFunctionQueryCounter() : m_count(0)
    {}

    ~JitFunctionQueryCounter() final
    {}

    JitVisitResult OnQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, int index, bool into) final;

    void OnError(const char* stmtName, int lineNo) final;

    inline int GetQueryCount() const
    {
        return m_count;
    }

private:
    int m_count;
};

class JitFunctionQueryCollector : public JitFunctionQueryVisitor {
public:
    JitFunctionQueryCollector(
        FuncParamInfoList* paramInfoList, PLpgSQL_function* function, JitCallSitePlan* callSitePlanList, int queryCount)
        : m_paramInfoList(paramInfoList),
          m_function(function),
          m_callSitePlanList(callSitePlanList),
          m_queryCount(queryCount),
          m_actualQueryCount(0)
    {}

    ~JitFunctionQueryCollector() final
    {
        m_paramInfoList = nullptr;
        m_function = nullptr;
        m_callSitePlanList = nullptr;
    }

    JitVisitResult OnQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, int index, bool into) final;

    void OnError(const char* stmtType, int lineNo) final;

    inline int GetActualQueryCount() const
    {
        return m_actualQueryCount;
    }

private:
    FuncParamInfoList* m_paramInfoList;
    PLpgSQL_function* m_function;
    JitCallSitePlan* m_callSitePlanList;
    int m_queryCount;
    int m_actualQueryCount;
};

class JitFunctionQueryVerifier : public JitFunctionQueryVisitor {
public:
    JitFunctionQueryVerifier()
    {}

    ~JitFunctionQueryVerifier() final
    {}

    JitVisitResult OnQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, int index, bool into) final;

    void OnError(const char* stmtType, int lineNo) final;
};

class JitSubQueryPlanGenerator : public JitFunctionQueryVisitor {
public:
    JitSubQueryPlanGenerator(PLpgSQL_function* function, PLpgSQL_expr** expr, int exprIndex)
        : m_function(function), m_expr(expr), m_exprIndex(exprIndex), m_plan(nullptr)
    {}

    ~JitSubQueryPlanGenerator() final
    {
        m_function = nullptr;
        m_expr = nullptr;
        m_plan = nullptr;
    }

    JitVisitResult OnQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, int index, bool into) final;

    void OnError(const char* stmtType, int lineNo) final;

    inline SPIPlanPtr GetPlan()
    {
        return m_plan;
    }

private:
    PLpgSQL_function* m_function;
    PLpgSQL_expr** m_expr;
    int m_exprIndex;
    SPIPlanPtr m_plan;
};

extern bool PrepareFuncParamInfoList(PLpgSQL_function* function, FuncParamInfoList* paramInfoList);
extern void DestroyFuncParamInfoList(FuncParamInfoList* paramInfoList);

extern int CountFunctionQueries(PLpgSQL_function* function);

extern bool IsSimpleExpr(
    SPIPlanPtr spiPlan, Query* query, bool* failed, bool* isConst = nullptr, Oid* constType = nullptr);

extern bool IsArgParam(PLpgSQL_function* func, int index, int* argIndex);

extern TupleDesc PrepareTupleDescFromRow(PLpgSQL_row* row, PLpgSQL_function* func);

/** @brief Prepare SPI plan for a function expression by index. */
extern SPIPlanPtr PrepareSpiPlan(JitExec::JitFunctionContext* functionContext, int exprIndex, PLpgSQL_expr** expr);

/** @brief Prepare SPI plan for a function expression. */
extern SPIPlanPtr GetSpiPlan(PLpgSQL_function* function, PLpgSQL_expr* expr);

extern void DestroyCallSitePlan(JitCallSitePlan* callSitePlan);

extern FuncExpr* ValidateDirectInvokePlan(Query* query);

extern FuncExpr* ValidateSelectInvokePlan(Query* query);

extern bool IsTargetListSingleRecordFunc(List* targetList);

extern bool IsCallSiteJittedFunction(JitCallSite* callSite);

extern bool IsFunctionSupported(FuncExpr* funcExpr);
}  // namespace JitExec

#endif /* JIT_PLAN_SP_EXPR_H */
