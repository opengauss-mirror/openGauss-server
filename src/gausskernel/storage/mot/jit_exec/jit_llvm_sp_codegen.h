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
 * jit_llvm_sp_codegen.h
 *    LLVM JIT-compiled code generation for stored procedures.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm_sp_codegen.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_LLVM_SP_CODEGEN_H
#define JIT_LLVM_SP_CODEGEN_H

#include "postgres.h"
#include "nodes/parsenodes.h"

#include "storage/mot/jit_def.h"
#include "mot_engine.h"

namespace JitExec {
// forward declarations
struct MotJitContext;
struct JitPlan;

/**
 * @brief Generates a native LLVM JIT-compiled code for a stored procedure.
 * @param function The parsed function.
 * @param procTuple The stored procedure entry in the system catalog.
 * @param functionOid The function identifier.
 * @param returnSetInfo Return set information for the function (required during parsing).
 * @param plan The plan resulted from the analysis phase.
 * @param[out] codegenStats Code generation statistics.
 * @return The resulting JIT context, or NULL if failed.
 */
extern MotJitContext* JitCodegenLlvmFunction(PLpgSQL_function* function, HeapTuple procTuple, Oid functionOid,
    ReturnSetInfo* returnSetInfo, JitPlan* plan, JitCodegenStats& codegenStats);

/**
 * @brief Executed a previously LLVM-jitted stored procedure.
 * @param jitContext The context produced by a previous call to @ref QueryCodegen().
 * @param params The list of bound parameters passed to the query.
 * @param[out] slot The slot used for reporting select result.
 * @param[out] tuplesProcessed The variable used to report the number of processed tuples.
 * @param[out] scanEnded The variable used to report if a range scan ended.
 * @return Zero if succeeded, otherwise an error code.
 * @note This function may cause transaction abort.
 */
extern int JitExecLlvmFunction(JitFunctionContext* jitContext, ParamListInfo params, TupleTableSlot* slot,
    uint64_t* tuplesProcessed, int* scanEnded);
}  // namespace JitExec

#endif /* JIT_LLVM_SP_CODEGEN_H */