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
struct JitContext;
struct JitPlan;

/**
 * @brief Generates a native LLVM JIT-compiled code for the given query.
 * @param query The query to JIT-compile.
 * @param queryString The query string.
 * @param analysis_result The information gathered during query analysis during a previous call to
 * @ref IsJittable().
 * @param plan The plan resulted from the analysis phase.
 * @return The resulting JIT context, or NULL if failed.
 */
JitContext* JitCodegenLlvmQuery(Query* query, const char* queryString, JitPlan* plan);
}  // namespace JitExec

#endif /* JIT_LLVM_QUERY_CODEGEN_H */
