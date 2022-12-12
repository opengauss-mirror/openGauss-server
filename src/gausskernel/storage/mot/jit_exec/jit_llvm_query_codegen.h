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
 * jit_llvm_query_codegen.h
 *    LLVM JIT-compiled code generation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm_query_codegen.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_LLVM_QUERY_CODEGEN_H
#define JIT_LLVM_QUERY_CODEGEN_H

#include "postgres.h"
#include "nodes/parsenodes.h"

#include "storage/mot/jit_def.h"
#include "mot_engine.h"

namespace JitExec {
// forward declarations
struct MotJitContext;
struct JitPlan;

/**
 * @brief Generates a native LLVM JIT-compiled code for the given query.
 * @param query The query to JIT-compile.
 * @param queryString The query string.
 * @param plan The plan resulted from the analysis phase.
 * @param[out] codegenStats Code generation statistics.
 * @return The resulting JIT context, or NULL if failed.
 */
MotJitContext* JitCodegenLlvmQuery(Query* query, const char* queryString, JitPlan* plan, JitCodegenStats& codegenStats);

/**
 * @brief Executed a previously LLVM-jitted query.
 * @param jitContext The context produced by a previous call to @ref QueryCodegen().
 * @param params The list of bound parameters passed to the query.
 * @param[out] slot The slot used for reporting select result.
 * @param[out] tuplesProcessed The variable used to report the number of processed tuples.
 * @param[out] scanEnded The variable used to report if a range scan ended.
 * @param newScan Specifies whether this is a new scan or a continued previous scan.
 * @return Zero if succeeded, otherwise an error code.
 * @note This function may cause transaction abort.
 */
extern int JitExecLlvmQuery(JitQueryContext* jitContext, ParamListInfo params, TupleTableSlot* slot,
    uint64_t* tuplesProcessed, int* scanEnded, int newScan);
}  // namespace JitExec

#endif /* JIT_LLVM_QUERY_CODEGEN_H */
