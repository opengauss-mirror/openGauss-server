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
 * jit_plan_sp.h
 *    JIT execution plan generation for stored procedures.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_plan_sp.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_PLAN_SP_H
#define JIT_PLAN_SP_H

#include "jit_plan_sp_expr.h"

namespace JitExec {
extern JitPlan* JitPrepareInvokePlan(Query* query);

extern void JitDestroyFunctionPlan(JitFunctionPlan* plan);

extern void JitDestroyInvokePlan(JitInvokePlan* plan);

extern bool VisitFunctionQueries(PLpgSQL_function* function, JitFunctionQueryVisitor* visitor);

extern bool RegenerateStmtQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, FuncParamInfoList* paramInfoList,
    PLpgSQL_function* function, JitCallSite* callSite, int queryCount, int subQueryIndex);

extern FuncExpr* GetFuncExpr(Query* query);
}  // namespace JitExec

#endif /* JIT_PLAN_SP_H */