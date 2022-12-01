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
 * jit_llvm.h
 *    JIT LLVM.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_LLVM_H
#define JIT_LLVM_H

#include "postgres.h"
#include "nodes/params.h"
#include "executor/tuptable.h"

#include "storage/mot/jit_def.h"
#include "mot_engine.h"

namespace dorado {
class GsCodeGen;
};

namespace JitExec {
/**
 * @typedef Jitted function prototype.
 * @param table The main table used for the query.
 * @param key The search key used for the main table.
 * @param bitmapSet The bitmap set used for incremental redo.
 * @param params The list of bound parameters passed to the query.
 * @param[out] slot The slot used for reporting select result.
 * @param[out] tuplesProcessed The variable used to report the number of processed rows.
 * @param[out] scanEnded Signifies in range scans whether scan ended.
 * @param newScan Specifies whether this is a new scan or a continued previous scan.
 * @param endIteratorKey The key used for the end iterator in range scans.
 * @param innerTable The inner scan table used during JOIN queries.
 * @param innerKey The search key used for the inner scan table during JOIN queries.
 * @param innerEndIteratorKey The key used for the end iterator of the inner table scan during JOIN queries.
 * @return Zero if succeeded, otherwise an error code.
 * @note This function may cause transaction abort.
 */
typedef int (*JitQueryFunc)(MOT::Table* table, MOT::Index* index, MOT::Key* key, MOT::BitmapSet* bitmapSet,
    ParamListInfo params, TupleTableSlot* slot, uint64_t* tuplesProcessed, int* scanEnded, int newScan,
    MOT::Key* endIteratorKey, MOT::Table* innerTable, MOT::Index* innerIndex, MOT::Key* innerKey,
    MOT::Key* innerEndIteratorKey);

// the number of arguments in the jitted function
#define MOT_JIT_QUERY_ARG_COUNT 14

/**
 * @typedef Jitted stored procedure function prototype.
 * @param params The list of bound parameters passed to the query.
 * @param[out] slot The slot used for reporting select result.
 * @param[out] tuplesProcessed The variable used to report the number of processed rows.
 * @param[out] scanEnded Signifies in range scans whether scan ended.
 * @return Zero if succeeded, otherwise an error code.
 * @note This function may cause transaction abort.
 */
typedef int (*JitSPFunc)(ParamListInfo params, TupleTableSlot* slot, uint64_t* tuplesProcessed, int* scanEnded);

// the number of arguments in the jitted function
#define MOT_JIT_FUNC_ARG_COUNT 4

/** @brief Prints startup information regarding LLVM version. */
void PrintNativeLlvmStartupInfo();

/** @brief Checks whether the current platforms natively supports LLVM. */
bool JitCanInitThreadCodeGen();

/** @brief Allocates a GsCodeGen object. */
dorado::GsCodeGen* SetupCodegenEnv();

/**
 * @brief De-allocates a GsCodeGen object, previously allocated by @ref SetupCodegenEnv()
 * during a call to @ref JitCodegenLlvmQuery().
 * @param codeGen The GsCodeGen object to free.
 */
void FreeGsCodeGen(dorado::GsCodeGen* codeGen);
}  // namespace JitExec

#endif /* JIT_LLVM_H */
