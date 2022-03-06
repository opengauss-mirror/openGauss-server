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
 * vechashaggcodegen.h
 *        Declarations of code generation for vechashagg funtions.
 *
 * For each vechashagg function, they have the same functionality as the ones
 * defined in vechashagg.h. Code generation will not change the logical of the
 * the whole function. 
 * 
 * IDENTIFICATION
 *        src/include/codegen/vechashaggcodegen.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LLVM_VECHASHAGG_H
#define LLVM_VECHASHAGG_H
#include "codegen/gscodegen.h"
#include "executor/node/nodeAgg.h"
#include "nodes/execnodes.h"
#include "vecexecutor/vecnodes.h"

namespace dorado {
/*
 * VecHashAggCodeGen class implements specific optimization by using LLVM
 */
#ifdef ENABLE_LLVM_COMPILE
class VecHashAggCodeGen : public BaseObject {
public:
    /*
     * @Brief		: Calculate scale of expression.
     * @Description	: Give expression node, calculate the scale of current,
     *				  this function only be used when we can use fast codegen.
     * @in node		: The expression that we need to evaluate scale.
     * @return		: return the scale of current node expr.
     */
    static int GetAlignedScale(Expr* node);

    /*
     * @Brief		: Check the expressions we supported
     * @Description	: Since we only support simple expression in agg
     *				  operator, we should restrict the ones we support.
     * @in state	: Input expression state node.
     * @return		: If the current node is allowed to do codegen or not.
     */
    static bool AggRefJittable(ExprState* state);

    /*
     * @Brief		: Check if current expression can use special codegen path.
     * @Description	: For simple var expression and simple algorithm expression
     *				  with numeric data type, if we could represent it by
     *				  big integer, use fast path at the beginning.
     * @in state 	: Input expression state node.
     * @return		: Return true if the expression can use fast codegen path.
     */
    static bool AggRefFastJittable(ExprState* state);

    /*
     * @Brief		: Check the validation of VecAggState for agghashing.
     * @Description	: Check if the current agg node could be codegened
     *				  or not. We only support sum and avg operation. The
     *				  data type could only be int8 and numeric with
     *				  precision less than or equal to 18.
     * @in node		: vecaggstate, the node to be checked.
     * @return		: Return true if current agg node could be codegened.
     */
    static bool AgghashingJittable(VecAggState* node);

    /*
     * @Brief		: Main function process to build LLVM ir function used to generate
     *			: machine code.
     * @Description	: Build LLVM ir function with respect to special query according to
     *				: current agg state, including build function and agg function.
     * @in node		: vector aggregate state information.
     */
    static void HashAggCodeGen(VecAggState* node);

    /*
     * @Brief		: Main function process to build LLVM IR function used to generate
     *			: machine code.
     * @Description	: Build LLVM ir function with respect to special query according to
     *				: current sonic agg state, including agg function.
     * @in node		: vector aggregate state information, special for sonic case.
     */
    static void SonicHashAggCodeGen(VecAggState* node);

    /*
     * @Brief		: Code generation for Hashing process
     * @Description	: The whole HashAggRunner::BuildAggTbl functions
     *				  can be divieded into three part. AgghashingCodeGen
     *				  contains (a)computing hash value & compare hash value
     *				  & match key  and (b)allocate new hash cell.
     * @in node		: vecaggstate, the node to be codegened
     * @return		: The codegeneration of part of HashAggRunner::BuildAggTbl
     *				  function.
     */
    template <bool isSglTbl>
    static llvm::Function* AgghashingCodeGenorSglTbl(VecAggState* node);

    /*
     * @Description	: Same as above function except the prefetching hashCell.
     */
    template <bool isSglTbl>
    static llvm::Function* AgghashingWithPrefetchCodeGenorSglTbl(VecAggState* node);
    /*
     * @Brief		: Code generation for vechashtable::hashBatch function.
     * @Description	: Code generation for hashBatch function with the fixed
     *				  row number 'idx'. The original function calculate the
     *				  whole hash values of that batch with respect to the
     *				  group by clauses. hashBatchCodeGen only calculate the
     *				  hash value of one row.
     * @in node		: node which contains aggstate information.
     * @in idx		: the row number of the batch.
     * @in rehash	: check if we need to rehash the row.
     * @return		: LLVM function pointer.
     *
     */
    static llvm::Function* HashBatchCodeGen(VecAggState* node, int idx, bool rehash);

    /*
     * @Brief		: Code generation for match_key function with respect to
     *				  one key.
     * @Description	: Code generation for match_key function with the fixed
     *				  row number 'idx' and fixed key. We only consider the
     *				  idx-th cell and the idx-th tuple in batch.
     * @in node		: Node which contains aggstate information.
     * @in idx		: The row number of the batch.
     * @return		: LLVM function pointer.
     */
    static llvm::Function* MatchOneKeyCodeGen(VecAggState* node, int idx);

    /*
     * @Brief		: Check the validation of VecAggState for batch aggregation.
     * @Description	: Check if the current node can be codegened for aggregation
     *				  or not. Since we only support sum and avg operation with
     *				  data type int8 and numeric with allowed precision(<=18).
     * @in node		: vecaggstate, the node to be codegened
     * @return		: return true if current node is allowed to do codegen of
     *				  batch aggregation.
     */
    static bool BatchAggJittable(VecAggState* node, bool isSonic);

    /*
     * @Brief		: Code generation for BatchAggregation.
     * @Description	: Do codegen for function BaseAggRunner::BatchAggregation.
     *				  The main process contains projection of expressions and
     *				  aggregation of the agg operator.
     * @in node		: vecaggstate, the node to be codegened
     * @in use_prefetch	: Flag used to decide if we need to prefetch data.
     * @return		: LLVM function pointer that point to the codegeneration of
     *				  BatchAggregation.
     */
    static llvm::Function* BatchAggregationCodeGen(VecAggState* node, bool use_prefetch);

    /*
     * @Brief		: LLVM function used to evaluation numeric expression in
     *				  fast path.
     * @Description	: Evaluate numeric expression in fast path if the mid-result
     *				  can be represented in BI64 by avoiding make numeric
     *				  result all the time.
     * @in state	: ExprState node
     * @in builder	: LLVM Builder associated with the current module.
     * @in jitted_func	: the current llvm function that we need to generate.
     * @in bb_null	: null basicblock used for deal with null value.
     * @in bb_last	: maks parent basic block dynamically to get value from
     *				  last basicblock.
     * @bb_outofbound	: basicblock used to deal with the case where the result
     *				  can not be represented in BI64.
     * @econtext	: ExprContext of the current operator.
     * @argVector	: the input scalevector.
     * @phi_idx		: the idxth row that we need to evaluation.
     * @return		: the result of this value if no outofbound or null is meet.
     */
    static llvm::Value* EvalFastExprInBatchAgg(ExprState* state, GsCodeGen::LlvmBuilder& builder,
        llvm::Function* jitted_func, llvm::BasicBlock** bb_null, llvm::BasicBlock** bb_last,
        llvm::BasicBlock** bb_outofbound, llvm::Value* econtext, llvm::Value* argVector, llvm::Value* phi_idx);

    /*
     * @Brief		: Code generation for expressions in agg function.
     * @Description	: Do codegen for ExecVecProject function in BatchAggregation.
     *				  Since we only deal with simple expressions, we could list
     *			   	  these simple cases one by one.
     * @in node		: ExprState node.
     * @in builder	: LLVM Builder associated with the current module.
     * @in econtext	: Expr Context with respect to current aggref Expr.
     * @in phi_idx	: the index of the current tuple.
     * @in isNull	: flag used to mask the return value is null or not.
     * @return		: return the result of this expression.
     */
    static llvm::Value* EvalSimpleExprInBatchAgg(ExprState* node, GsCodeGen::LlvmBuilder& builder,
        llvm::Value* econtext, llvm::Value* phi_idx, llvm::Value* isNull);

    static llvm::Function* SonicBatchAggregationCodeGen(VecAggState* node, bool use_prefetch);

    /*
     * @Brief		: Code generation for allocating a new hash cell and initialization.
     * @Description	: Code generation for allocating hash cell and initilize the
     *				  hash cell. It has the same functionality as the orginal one.
     * @in ptrbuilder	: LLVM Builder associated with the current module.
     * @in hAggRunner	: HashAggRunner information.
     * @in batch	: vectorbatch which needed to creat hash cell.
     * @in idx		: the index of the current tuple.
     * @in keysimple	: mark whether the key is simple or not.
     */
    static void WarpAllocHashSlotCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* hAggRunner,
        llvm::Value* batch, llvm::Value* idx, llvm::Value* keysimple);
    static void WarpSglTblAllocHashSlotCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* hAggRunner,
        llvm::Value* batch, llvm::Value* idx, llvm::Value* keysimple);

    /*
     * @Brief		: Reset current expr context.
     * @Description	: Codeg generation for reset expr context in LLVM assemble.
     * @in econtext	: Current expr context.
     */
    static void WrapResetEContextCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* econtext);
};
#endif

}  // namespace dorado
#endif
