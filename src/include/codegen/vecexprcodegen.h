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
 * vecexprcodegen.h
 *        Declarations of code generation for vecexpression funtions.
 *
 * For each expression, first decide if this expression can be LLVM optimized
 * or not according to the detail information. The expression functions
 * accomplished here have the same functionality as the ones defined in
 * vecexpression.h, though more constraints are used. 
 * 
 * IDENTIFICATION
 *        src/include/codegen/vecexprcodegen.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LLVM_VECEXPRESSION_H
#define LLVM_VECEXPRESSION_H
#include "optimizer/clauses.h"
#include "nodes/execnodes.h"
#include "codegen/gscodegen.h"

namespace dorado {
#ifdef ENABLE_LLVM_COMPILE
/*
 * @Description	: Arguments used for Vectorized Expression CodeGen Engine.
 */
typedef struct {
    ExprState* exprstate;            /* Expression information used to
                                        generate IR function */
    PlanState* parent;               /* The parent planstate information */
    GsCodeGen::LlvmBuilder* builder; /* LLVM builder in upper level */
    llvm::Value** llvm_args;         /* LLVM parameters*/
} ExprCodeGenArgs;

/*
 * @Description : Parameters needed to pass to the original c-function.
 */
typedef struct {
    int nargs;              /* number of args */
    llvm::Value** args;     /* values of parameters with length nargs */
    llvm::Value** argnulls; /* flags of parameters with length nargs */
} LLVMFuncCallInfo;

typedef llvm::Value* (*VectorExprCodeGen)(ExprCodeGenArgs* args);

/*
 * LlvmExecQual class implements specific optimization by using LLVM.
 */
class VecExprCodeGen : public BaseObject {
public:
    /*
     * @Brief       : Check whether state could be  legal or not.
     * @Description : Check is the current state->expr could be codegened
     *				  or not.
     * @in state	: exprstate, the node to be checked.
     * @return		: return true is state could be codegened.
     */
    static bool ExprJittable(ExprState* state);

    /*
     * The following functions are codegen functions and their guardian function:
     * Since each expression has its own restricts, we should consider them
     * independently. For each expression, we have one guardian function and
     * one codegen function.
     */

    /*
     * @Brief		: Check if the whole qual list can be codegened or not.
     * @Description : Check if the qual list can be codegened or not. The qual
     *				  list can be joinqual, hashjoin_clauses and so on.
     * @in qual		: the expr node list need to be checked.
     * @return 		: return true if the whole qual can be codegened.
     */
    static bool QualJittable(List* qual);

    /*
     * @Brief		: Code generation for ExecEvalQual.
     * @Description : Since ExprState stored the plan-tree information, ExprContext
     *				  stored the actual data information during execution. We need
     *				  to separate these two parameters in 'InitNode' and 'ExecNode'
     *                status. So during expression processes, we should pass through
     *				  'Expr' as parameter in 'InitNode' and pass through 'econtext'
     *                as LLVM parameter in 'ExecNode'.
     * @in qual		: the expr node list need to be codegened.
     * @in parent	: the whole planstate of the expr node.
     * @return 		: ScalarVector result of the qual list in LLVM assemble.
     * @notes       : firstly, econtext is passed when qual is needed; secondly, for
     *				  each LLVM function in ExecVecQual, the interface is
     *				  LLVM_Expr_Func(ExprCodeGenArgs);
     */
    static llvm::Function* QualCodeGen(List* qual, PlanState* parent, bool isReset = true);

    /*
     * @Description	: Check if the scalararrayop expr can be codegened or not.
     * @in state	: ExprState of ScalarArrayOpExpr.
     * @return		: return true if scalararrayop expr can be codegened.
     */
    static bool ScalarArrayJittable(ExprState* state);

    /*
     * @Brief		: Codegen for scalararrayop expression.
     * @Description	: Codegeneration of the ExecEvalVecScalarArrayOp:
     *				  For integer/float/date/timestamp, we use switch
     *				  structure in LLVM, for bpchar/varchar/text, we use
     *				  codegen function.
     * @in args		: arguments of vector expr codegen engine.
     * @return		: return true when tuple satisfies the condition
     * @Notes        : Support ...in (...) and any(array[....]) only.
     */
    static llvm::Value* ScalarArrayCodeGen(ExprCodeGenArgs* args);

    /*
     * @Description : Check if the boolean test expr can be codegened or not.
     * @in state	: ExprState of BooleanTest Expr.
     * @Output		: return true if boolean test expr can be codegened.
     */
    static bool BooleanTestJittable(ExprState* state);

    /*
     * @Brief 		: Codegen for boolean test expression.
     * @Description : Codegeneration of the ExecEvalBooleanTest.
     * @in args		: arguments of vector expr codegen engine.
     * @return		: return the result of Boolean Test Expr.
     */
    static llvm::Value* BooleanTestCodeGen(ExprCodeGenArgs* args);

    /*
     * @Description : Check if the null test expr can be codegened or not.
     *
     * @in state	: NullTest ExprState.
     * @return      : return true if nulltest expr can be codegened.
     */
    static bool NullTestJittable(ExprState* state);

    /*
     * @Brief       : Codegen for NullTest expression.
     * @Description : Codegeneration of the ExecEvalNullTest: contains
     *				  IS_NULL, IS_NOT_NULL.
     * @in args		: arguments of vector expr codegen engine.
     * @return 		: return true only if argument is nulll, else return false.
     */
    static llvm::Value* NullTestCodeGen(ExprCodeGenArgs* args);

    /*
     * @Brief       : Check if the oper func can be codegened or not.
     * @Description : Check if the FuncExprState can be codegened or not :
     *				  right now most of the oper expr can be codegened, but
     *				  some of them still have some constraints.
     * @in state	: ExprState representation of FuncExprSate.
     * @return 		: return true if the operation function can be codegened.
     */
    static bool OpJittable(ExprState* state);

    /*
     * @Brief			: Check if the oper func with numeric data can be codegened in special path.
     * @Description	: Check if the Numeric OperExpr can be codegened in a special way:
     *				  the data we considered here are all can be represented in BI64.
     * @in state		: ExprState representation of OperExprSate.
     * @return 		: return true if the operation function can be codegened in fast path.
     */
    static bool NumericOpFastJittable(ExprState* state);

    /*
     * @Brief       : Codegen for vecoper expression.
     * @Description : Codegeneration of the ExecEvalVecOp: Right row
     *				  we only support several cases with data type
     *				  int/float/date/timestamp and text(eq,gt)/bpchar(eq)
     * @in args		: arguments of vector expr codegen engine.
     * @return		: return the result of oper expr in LLVM assemble.
     * @Notes       : Since we have not codegen ExecMakeVecFunctionResult
     *				  as we do in vecexpression.cpp, only seveal cases
     *				  are supported.
     */
    static llvm::Value* OpCodeGen(ExprCodeGenArgs* args);

    /*
     * @Brief       : Codegen for numeric type operation expression.
     * @Description : Codegeneration for numeric operation expression: we make a special
     *			path for data that can be transformed to BI64. While for other cases,
     *			we use normal codegen path(NormalNumericOpCodeGen).
     * @in ptrbuilder	: LLVM Builder associated with the current module.
     * @in estate	: opexprstate associated with current operation.
     * @in funcoid	: function oid of current numeric operation.
     * @in larg	: left argument passed to llvm.
     * @in rarg	: right argument passed to llvm.
     */
    static llvm::Value* FastNumericOpCodeGen(
        GsCodeGen::LlvmBuilder* ptrbuilder, ExprState* estate, llvm::Value* larg, llvm::Value* rarg);
    static llvm::Value* NormalNumericOpCodeGen(
        GsCodeGen::LlvmBuilder* ptrbuilder, Oid funcoid, llvm::Value* larg, llvm::Value* rarg);

    /*
     * @Brief       : Check if the FuncExpr can be codegened or not.
     * @Description : Check if the FuncExprState can be codegened or not :
     *				  right now most of the func expr can be codegened, but
     *				  some of them still have some constraints.
     * @in state	: ExprState representation of FuncExprSate.
     * @return		: return true if the func can be codegen.
     * @Notes        : We only support C / UTF-8 encoding.
     */
    static bool FuncJittable(ExprState* state);

    /*
     * @Brief       : Codegen for vecfunc expression.
     * @Description : Codegeneration of the ExecEvalVecFunc: Only part of them
     *				  have been codegened, the rest would be used by
     *				  call original C-Function.
     * @in args		: arguments of vector expr codegen engine.
     * @return		: result of ExecEvalFunc in LLVM assemble.
     * @Notes       : We only support C / UTF-8 encoding.
     */
    static llvm::Value* FuncCodeGen(ExprCodeGenArgs* args);

    /*
     * @Brief       : Check if veccase expression can be codegened or not.
     * @Description : Codegeneration of the ExecEvalVecCase: The logic here
     *				  is the same as ExecEvalVecCase. Since operation is only
     *				  partly supported, the exprs in case arg/when expr/default
     *				  expr are the ones supported by OpCodeGen.
     * @in state	: context of CaseExprState
     * @return      : return true if veccase expr can be codegened.
     */
    static bool CaseJittable(ExprState* state);

    /*
     * @Brief        : Codegen for veccase expression.
     * @Description  : Codegeneration of the ExecEvalVecCase: The logic here
     *				  is the same as ExecEvalVecCase. Since operation is only
     *				  partly supported, the exprs in case arg/when expr/default
     *				  expr are the ones supported by OpCodeGen.
     * @in args		: arguments of vector expr codegen engine.
     * @return      : wclause->result or default->result in LLVM assemble.
     */
    static llvm::Value* CaseCodeGen(ExprCodeGenArgs* args);

    /*
     * @Brief       : Check if nullif expression can be codegened or not.
     * @Description : Codegeneration of the ExecEvalVecNullif checking.
     * @in state    : ExprState
     * @return      : return true if nullif expr can be codegened.
     */
    static bool NullIfJittable(ExprState* state);

    /*
     * Brief       : Codegen for nullif expression.
     * Description : NullIfCodeGen is  the codegeneration of the ExecEvalVecNullIf.
     * Input       : ExprCodeGenArgs
     * Output      : when expr1=expr2 return null, else return expr1.
     * Notes       : None.
     */
    static llvm::Value* NullIfCodeGen(ExprCodeGenArgs* args);

    /*
     * @Brief       : Check if and/or/not expr can be codegened or not.
     * @Description : Check if the bool expr can be codegened or not. The
     *				  expr type contatins and, or and not expr. While
     *				  and/or expression and not expression should be
     *				  considered independently.
     * @in state    : the context of bool expr state
     * @return      : return true the expr in state can be codegened.
     */
    static bool BoolJittable(ExprState* state);

    /*
     * @Brief       : Codegen for vector and/or expression.
     * @Description : Codegeneration of the ExecEvalProcessAndOrLogic: The
     *				  logic here is the same as ExecEvalProcessAndOrLogic.
     *				  BoolJittable would check and/or/not expression argument,
     *				  but and/or expression and not expression would be
     *				  considered independently.
     * @in args     : arguments of vector expr codegen engine.
     * @return		: the and/or expr's result in LLVM assemble.
     */
    static llvm::Value* AndOrLogicCodeGen(ExprCodeGenArgs* args);

    /*
     * @Brief 		: Codegen for vector not expression.
     * @Description	: Codegeneration of the ExecEvalVecNot.
     * @in args		: arguments of vector expr codegen engine.
     * @return		: the not expr's result in LLVM assemble.
     */
    static llvm::Value* NotCodeGen(ExprCodeGenArgs* args);

    /*
     * @Brief       : Check if vecrelabel expr can be codegened or not.
     * @Description : Check if the relabel expr in genericexprstate can be
     *				: codegened or not. Only support the case when arg
     *				: is a var or casetestexpr.
     * @in state    : context of relabel expr.
     * @return		: return true if relabel expr can be codegened.
     */
    static bool RelabelJittable(ExprState* state);

    /*
     * @Brief       : Codegen for vecrelabel expression.
     * @Description : Codegeneration of the ExecEvalVecRelabelType:
     *				  Support the case when arg is a var or casetestexpr.
     * @in args     : arguments of vector expr codegen engine.
     * @return		: result of relabel expr in LLVM assemble
     */
    static llvm::Value* RelabelCodeGen(ExprCodeGenArgs* args);

    /*
     * @Brief       : Check if the var expr can be codegened or not.
     * @Description : Check if the Var in state can be codegened or not,
     *				: only need to consider the var type.
     * @in state    : context of Var Expr State
     * @return      : return true if const expr can be codegened.
     */
    static bool VarJittable(ExprState* state);

    /*
     * @Brief       : Codegen for evalvar expr.
     * @Description : Codegeneration of the ExecEvalVecVar : Get the
     *				  values from econtext->vectorbatch with fixed
     *				  column index and row index(loop_index). Also we
     *				  should restore the null flag.
     * @in args		: arguments of vector expr codegen engine.
     * @out isNull  : null flag of batch->m_arr[varattno-1]
     * @return		: return true if const expr can be codegened.
     */
    static llvm::Value* VarCodeGen(ExprCodeGenArgs* args);

    /*
     * @Brief       : Codegen for vecconst expression.
     * @Description : Check is const expr could be codegened or not.
     * @in state	: const expr state.
     * @return		: return true if const expr can be codegened.
     */
    static bool ConstJittable(ExprState* state);

    /*
     * @Brief       : Codegen for vecconst expression.
     * @Description : Codegeneration of the ExecEvalVecConst
     * @in args		: arguments of vector expr codegen engine.
     * @return		: the value in const expr.
     */
    static llvm::Value* ConstCodeGen(ExprCodeGenArgs* args);

    /*
     * @Brief		: Check if the targetlist could be codegened or not.
     * @Description : Check the jittable of the ExecVecTargetList, which
     *				: evaluates a targetlist with respect to the given
     *				: expression context in LLVM assemble.
     * @in targetlist	: the expr node list need to be checked.
     * @return		: return true if we were able to codegen the list.
     */
    static bool TargetListJittable(List* targetlist);

    /*
     * @Brief       : Codegen for TargetList expression.
     * @Description : Codegeneration of the ExecVecTargetList, which
     *				: evaluates a targetlist with respect to the given
     *				: expression context in LLVM assemble.
     * @in targetlist	: the expr node list need to be codegened.
     * @in parent	: PlanState of the whole targetlist.
     * @return 		: LLVM function of targetlist expr.
     */
    static llvm::Function* TargetListCodeGen(List* targetlist, PlanState* parent);

    /*
     * @Description	: CodeGen routine for Expression.
     * @in args		: arguments of vector expr codegen engine.
     * @return		: the result of current args->exprstate.
     */
    static llvm::Value* CodeGen(ExprCodeGenArgs* args);

    /*
     * @Brief		: CodeGen for general function call.
     * @Description	: For functions we do not codegened, invoke these
     *				  function in LLVM assemble, and reduce the logical
     *				  branches in dealing with arguments and return vals
     *				  by using LLVM optimization.
     * @in ptrbuilder	: LLVM Builder associated with the current module.
     * @in fcache	: The function expr dealed with.
     * @in isNull	: used to record the null flag of the return value.
     * @in lfcinfo	: arguments about fcache.
     *
     */
    static llvm::Value* EvalFuncResultCodeGen(
        GsCodeGen::LlvmBuilder* ptrbuilder, FuncExprState* fcache, llvm::Value* isNull, LLVMFuncCallInfo* lfcinfo);

    /*
     * @Brief		: Make sure the ExtractFunc according the datum type.
     * @Description	: Wrap all the Extract Func in LLVM assemble, which could
     *				: easily deal with input argument according the different
     *				: datum type.
     * @in ptrbuilder	: LLVM Builder associated with the current module.
     * @in argtyp	: input argument datum type.type
     * @in data		: input data
     * @return		: the result after extraction.
     */
    static llvm::Value* WrapChooExtFunCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, Oid argtyp, llvm::Value* argval);
    static llvm::Value* WrapExtFixedTypeCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data);
    static llvm::Value* WrapExtAddrTypeCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data);
    static llvm::Value* WrapExtCStrTypeCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data);
    static llvm::Value* WrapExtVarTypeCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data);

    /*
     * @Brief		: Wrap Datum to Scalar Func according the datum type.
     * @Description	: Wrap all the Convert Func in LLVM assemble.
     * @in ptrbuilder	: LLVM Builder associated with the current module.
     * @in val		: input data argument.
     * @in len		: the argument data len with respect to datum type.
     * @return		: the result after conversation.
     */
    static llvm::Value* WrapDFixLToScalCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* val, llvm::Value* len);
    static llvm::Value* WrapDCStrToScalCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* val);

    /*
     * @Description : Wrap the c-function : GetVectorBatch, which is used to
     *				  get the vectorbatch during vectargetlist-codegeneration.
     * @in ptrbuilder	: LLVM Builder associated with the current module.
     * @in econtext	: exprcontext in LLVM assemble.
     */
    static llvm::Value* WrapGetVectorBatchCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* econtext);

    /*
     * @Brief		: Wrap the Invoke function.
     * @Description	: Codegen the interface, used to call the original
     * 				: C-Function - FunctionCallInvoke, only consider strict
     *				: function, where we do not consider the flag of args.
     * @in ptrbuilder	: LLVM Builder associated with the current module.
     * @in fcinfo	: data with FunctionCallInfo type in LLVM assemble.
     * @in arg		: array of arguments, contain the arguments of fcinfo
     *				: and the flag of the result.
     */
    static llvm::Value* WrapStrictOpFuncCodeGen(
        GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* fcinfo, llvm::Value* arg, llvm::Value* isNull);

    /*
     * @Brief		: Wrap the Invoke function.
     * @Description	: Codegen the interface, used to call the original
     * 				: C-Function - FunctionCallInvoke, only consider non-strict
     *				: function.
     * @in ptrbuilder	: LLVM Builder associated with the current module.
     * @in fcinfo	: data with FunctionCallInfo type in LLVM assemble.
     * @in arg		: array of arguments, contain the arguments of fcinfo
     *				: and the flag of the result.
     * @in argnull	: the flag of the input args.
     * @in isNull	: the flag of the result.
     */
    static llvm::Value* WrapNonStrictOpFuncCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* fcinfo,
        llvm::Value* arg, llvm::Value* argnull, llvm::Value* isNull);

    /*
     * @Brief		: Wrap the Invoke function.
     * @Description	: Codegen the interface, used to call the original
     * 				: C-Function - MemoryContextSwitchTo.
     * @in ptrbuilder	: LLVM Builder associated with the current module.
     * @in context	: MemoryContext that we want to access.
     */
    static llvm::Value* MemCxtSwitToCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* context);
};
#endif
}  // namespace dorado
#endif
