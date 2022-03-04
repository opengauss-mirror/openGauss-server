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
 * ---------------------------------------------------------------------------------------
 * 
 * vecsortcodegen.h
 *        Declarations of code generation for vecsort funtions.
 *
 * Since the most cpu intensive part of sort function locates
 * at CompareMultiColumn, we mainly do code generation for CompareMultiColumn
 * and the functions it calls.
 * 
 * 
 * IDENTIFICATION
 *        src/include/codegen/vecsortcodegen.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LLVM_VECSORT_H
#define LLVM_VECSORT_H
#include "codegen/gscodegen.h"
#include "vecexecutor/vecnodesort.h"
#include "nodes/execnodes.h"
#include "vecexecutor/vecnodes.h"
#include "utils/batchsort.h"
#include "access/tuptoaster.h"

namespace dorado {
/*
 * VecSortCodeGen class implements specific optimization by using LLVM
 */
#ifdef ENABLE_LLVM_COMPILE
class VecSortCodeGen : public BaseObject {
public:
    /*
     * @Brief		: Check the validation of VecSortState.
     * @Description	: Check if the current sort node could be codegened
     *				  or not. We only support the codegen on CompareMultiColumn
     *				  and functions it calls. The data type of sort keys
     *				  could only be int4, int8, char, text, varchar or numeric.
     * @in node		: VecSortState, the node to be checked.
     * @return		: return true if current sort node could be codegened.
     */
    static bool JittableCompareMultiColumn(VecSortState* node);

    /*
     * @Brief		: Code generation for CompareMultiColumn
     * @Description	: Codegen for the hot path in sort
     * @in node		: VecSortState, the node to be codegened
     * @return		: The codegeneration of part of CompareMultiColumn
     *				  function.
     */
    static llvm::Function* CompareMultiColumnCodeGen(VecSortState* node, bool use_prefetch);

    /*
     * @Brief		: Code generation for CompareMultiColumn under Top N sort
     * @Description	: Codegen for the hot path in sort for Top N
     * @in node		: VecSortState, the node to be codegened
     * @return		: The codegeneration of part of CompareMultiColumn
     *				  function.
     */
    static llvm::Function* CompareMultiColumnCodeGen_TOPN(VecSortState* node, bool use_prefetch);

    /*
     * @Brief		: Code generation for bpcharcmp function
     * @Description	: Codegen for bpcharcmp and all functions it calls.
     *				  If bpchar length is greater than 16 bytes, use bpcharcmpCodeGen_long()
     *				  Otherwise, use bpcharcmpCodeGen_short()
     * 				  the generated function will be called by CompareMultiColumn
     * @return		: The codegeneration of bpcharcmp
     *				  function.
     */
    static llvm::Function* bpcharcmpCodeGen_long();
    static llvm::Function* bpcharcmpCodeGen_short();

    /*
     * @Brief		: Code generation for memcmp function.
     * @Description	: Codegen for memcmp and all functions it calls. In sort
     *				  node this function is called by varstr_cmp inlined
     *				  into bpcharcmp function.
     * @return		: The LLVM IR function generated for memcmp function
     *				  called by varstr_cmp inlined into bpcharcmp.
     */
    static llvm::Function* LLVMIRmemcmp_CMC_CodeGen();

    /*
     * @Brief		: Code generation for text_cmp function
     * @return		: The LLVM IR function generated for text_cmp function
     */
    static llvm::Function* textcmpCodeGen();

    /*
     * @Brief		: Code generation for numeric_cmp function
     * @return		: The LLVM IR function generated for numeric compare.
     */
    static llvm::Function* numericcmpCodeGen();
    static llvm::Function* numericcmpCodeGen_fastpath();

    /*
     * @Brief		: Check the validation of VecAggState.
     * @Description	: Check if the match_key function in sort aggregation
     *				  could be codegened or not.
     * @in node		: VecAggState, the node to be checked.
     * @return		: true if it's jittable, otherwise, false.
     */
    static bool JittableSortAggMatchKey(VecAggState* node);

    /*
     * @Brief		: Codegeneration for match_key function in Sort Aggregation.
     * @Description	: Codegeneration for match_key function in sort aggregation,
     *				  The data type of sort keys could only be int4, int8,
     *				  char, text, varchar or numeric, and we only support the
     *				  group by rollup case without considering multiple phases.
     * @in node		: VecAggState, the node to be codegened.
     * @return		: The LLVM IR function generated for match_key function.
     */
    static llvm::Function* SortAggMatchKeyCodeGen(VecAggState* node);

    /*
     * @Description	: Code generation for bpchareq function called by
     *				  match_key function in Sort Aggregation.
     * @in length	: The length of the bpchar definition.
     * @return		: The LLVM IR function generated for bpchareq function
     *				  in match_key of sortagg.
     */
    static llvm::Function* SortAggBpchareqCodeGen(int length);

    /*
     * @Description	: Code generation for memcmp function called by bpchareq
     *			      in match_key function of Sort Aggregation. If the bpchar
     *			      length is >= 16, call SortAggMemcmpCodeGen_long(),
     *			      otherwise, call SortAggMemcmpCodeGen_short().
     * @return		: The LLVM IR function generated for memcmp function
     *				  called by bpchareq in match_key of sortagg.
     */
    static llvm::Function* SortAggMemcmpCodeGen_long();
    static llvm::Function* SortAggMemcmpCodeGen_short();

    /*
     * @Description	: Code generation for texteq function in match_key of sortagg.
     * @return		: The LLVM IR function generated for texteq function.
     */
    static llvm::Function* SortAggTexteqCodeGen();
};
#endif
}  // namespace dorado
#endif
