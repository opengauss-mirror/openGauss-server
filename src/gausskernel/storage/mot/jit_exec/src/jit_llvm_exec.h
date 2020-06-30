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
 * jit_llvm_exec.h
 *    LLVM JIT-compiled execution.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/src/jit_llvm_exec.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_LLVM_H
#define JIT_LLVM_H

#include "postgres.h"
#include "nodes/params.h"
#include "executor/tuptable.h"
#include "nodes/parsenodes.h"

#include "storage/mot/jit_def.h"

#include "mot_engine.h"

namespace dorado {
class GsCodeGen;
};

namespace JitExec {
// forward declarations
struct JitContext;
struct JitPlan;

/**
 * @typedef Jitted function prototype.
 * @param table The main table used for the query.
 * @param key The search key used for the main table.
 * @param bitmapSet The bitmap set used for incremental redo.
 * @param params The list of bound parameters passed to the query.
 * @param[out] slot The slot used for reporting select result.
 * @param[out] tuplesProcessed The variable used to report the number of processed rows.
 * @param[out] scanEnded Signifies in range scans whether scan ended.
 * @param endIteratorKey The key used for the end iterator in range scans.
 * @param innerTable The inner scan table used during JOIN queries.
 * @param innerKey The search key used for the inner scan table during JOIN queries.
 * @param innerEndIteratorKey The key used for the end iterator of the inner table scan during JOIN queries.
 * @return Zero if succeeded, otherwise an error code.
 * @note This function may cause transaction abort.
 */
typedef int (*JitFunc)(MOT::Table* table, MOT::Index* index, MOT::Key* key, MOT::BitmapSet* bitmapSet,
    ParamListInfo params, TupleTableSlot* slot, uint64_t* tuplesProcessed, int* scanEnded, MOT::Key* endIteratorKey,
    MOT::Table* innerTable, MOT::Index* innerIndex, MOT::Key* innerKey, MOT::Key* innerEndIteratorKey);

// the number of arguments in the jitted function
#define MOT_JIT_FUNC_ARG_COUNT 13

#ifdef __aarch64__
/**
 * @brief Initializes the global spin-lock used for synchronizing native-LLVM compilation.
 * @return Truen if operation succeeded, otherwise false.
 */
extern bool InitArmCompileLock();

/** @brief Destroys the global spin-lock used for synchronizing native-LLVM compilation. */
extern void DestroyArmCompileLock();
#endif

/** @brief Prints startup information regarding LLVM version. */
extern void PrintNativeLlvmStartupInfo();

/**
 * @brief Generates a native LLVM JIT-compiled code for the given query.
 * @param query The query to JIT-compile.
 * @param queryString The query string.
 * @param analysis_result The information gathered during query analysis during a previous call to
 * @ref IsJittable().
 * @param plan The plan resulted from the analysis phase.
 * @return The resulting JIT context, or NULL if failed.
 */
extern JitContext* JitCodegenLlvmQuery(Query* query, const char* queryString, JitPlan* plan);

/** @brief Checks whether the current platforms natively supports LLVM. */
extern bool JitCanInitThreadCodeGen();

/**
 * @brief Deallocates a GsCodeGen object, previously allocated during a call to @ref
 * JitCodegenLlvmQuery().
 * @param codeGen The GsCodeGen object to free.
 */
extern void FreeGsCodeGen(dorado::GsCodeGen* codeGen);
}  // namespace JitExec

#endif
