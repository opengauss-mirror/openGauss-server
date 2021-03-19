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
 * floatcodegen.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/codegenutil/floatcodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/builtinscodegen.h"

namespace dorado {
static llvm::Function* float8_is_nan_codegen();

/**
 * @Description : Generate IR function to codegen float8eq.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* float8eq_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(float8Type, FLOAT8OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::Value* llvmargs[2];
    llvm::Value* lhs_value = NULL;
    llvm::Value* rhs_value = NULL;
    llvm::Value* lhs_isnan = NULL;
    llvm::Value* rhs_isnan = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Value* res3 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_float8eq", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_float8eq = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_value = llvmargs[0];
    rhs_value = llvmargs[1];

    /* Define basic block structure */
    llvm::BasicBlock* entry = &jitted_float8eq->getEntryBlock();
    llvm::BasicBlock* not_equal = llvm::BasicBlock::Create(context, "not_equal", jitted_float8eq);
    llvm::BasicBlock* may_nan = llvm::BasicBlock::Create(context, "may_nan", jitted_float8eq);
    llvm::BasicBlock* either_nan = llvm::BasicBlock::Create(context, "either_nan", jitted_float8eq);
    llvm::BasicBlock* both_nan = llvm::BasicBlock::Create(context, "both_nan", jitted_float8eq);
    llvm::BasicBlock* ret_bb = llvm::BasicBlock::Create(context, "ret_bb", jitted_float8eq);

    /* first convert it to float-point type */
    lhs_value = builder.CreateBitCast(lhs_value, float8Type);
    rhs_value = builder.CreateBitCast(rhs_value, float8Type);

    /* enter into the control flow */
    builder.SetInsertPoint(entry);
    /* yields true if either operand is a NaN or lhs_value equal to rhs_value */
    llvm::Value* nan_eq = builder.CreateFCmpUEQ(lhs_value, rhs_value);

    /*
     * when both parameters are not NaN and not equal to each other, we get false,
     * else we should first check which parameter is NaN.
     */
    builder.CreateCondBr(nan_eq, may_nan, not_equal);

    builder.SetInsertPoint(not_equal);
    res1 = Datum_0;
    builder.CreateBr(ret_bb);

    builder.SetInsertPoint(may_nan);
    /* check if the parameters are null or not */
    llvm::Function* func_isnan = llvmCodeGen->module()->getFunction("Jitted_float8isnan");
    if (func_isnan == NULL) {
        func_isnan = float8_is_nan_codegen();
    }
    lhs_isnan = builder.CreateCall(func_isnan, lhs_value, "flag1");
    rhs_isnan = builder.CreateCall(func_isnan, rhs_value, "flag2");

    /* do 'and'(&) operation : if both are NaN, we get true, else false */
    llvm::Value* and_nan = builder.CreateAnd(lhs_isnan, rhs_isnan);
    and_nan = builder.CreateTrunc(and_nan, int1Type);
    builder.CreateCondBr(and_nan, both_nan, either_nan);

    /* NaN equals to NaN */
    builder.SetInsertPoint(both_nan);
    res2 = Datum_1;
    builder.CreateBr(ret_bb);

    /* when both are not NaN, we get true, else we get false. */
    builder.SetInsertPoint(either_nan);
    llvm::Value* or_nan = builder.CreateOr(lhs_isnan, rhs_isnan);
    or_nan = builder.CreateTrunc(or_nan, int1Type);
    res3 = builder.CreateSelect(or_nan, Datum_0, Datum_1);
    builder.CreateBr(ret_bb);

    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 3);
    Phi->addIncoming(res1, not_equal);
    Phi->addIncoming(res2, both_nan);
    Phi->addIncoming(res3, either_nan);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_float8eq);
    return jitted_float8eq;
}

/**
 * @Description : Generate IR function to codegen float8ne.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* float8ne_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(float8Type, FLOAT8OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::Value* llvmargs[2];
    llvm::Value* lhs_value = NULL;
    llvm::Value* rhs_value = NULL;
    llvm::Value* lhs_isnan = NULL;
    llvm::Value* rhs_isnan = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_float8ne", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_float8ne = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_value = llvmargs[0];
    rhs_value = llvmargs[1];

    /* Define basic block structure */
    llvm::BasicBlock* entry = &jitted_float8ne->getEntryBlock();
    llvm::BasicBlock* not_nan = llvm::BasicBlock::Create(context, "not_nan", jitted_float8ne);
    llvm::BasicBlock* may_nan = llvm::BasicBlock::Create(context, "may_nan", jitted_float8ne);
    llvm::BasicBlock* ret_bb = llvm::BasicBlock::Create(context, "ret_bb", jitted_float8ne);

    lhs_value = builder.CreateBitCast(lhs_value, float8Type);
    rhs_value = builder.CreateBitCast(rhs_value, float8Type);

    /* enter into the control flow */
    builder.SetInsertPoint(entry);
    /* yields true if either operand is a NaN */
    llvm::Value* cmp_nan = builder.CreateFCmpUNO(lhs_value, rhs_value);
    /*
     * when both parameters are not NaN, use FCmpONE to yield the result,
     * else we get 'true' when only one of the operands is NaN.
     */
    builder.CreateCondBr(cmp_nan, may_nan, not_nan);
    builder.SetInsertPoint(not_nan);
    llvm::Value* tmp_ne = builder.CreateFCmpONE(lhs_value, rhs_value);
    res1 = builder.CreateZExt(tmp_ne, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(may_nan);
    /* check if the parameters are null or not */
    llvm::Function* func_isnan = llvmCodeGen->module()->getFunction("Jitted_float8isnan");
    if (func_isnan == NULL) {
        func_isnan = float8_is_nan_codegen();
    }
    lhs_isnan = builder.CreateCall(func_isnan, lhs_value, "flag1");
    rhs_isnan = builder.CreateCall(func_isnan, rhs_value, "flag2");
    llvm::Value* tmp_nan_ne = builder.CreateAnd(lhs_isnan, rhs_isnan);
    tmp_nan_ne = builder.CreateTrunc(tmp_nan_ne, int1Type);
    res2 = builder.CreateSelect(tmp_nan_ne, Datum_0, Datum_1);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, not_nan);
    Phi->addIncoming(res2, may_nan);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_float8ne);
    return jitted_float8ne;
}

/**
 * @Description : Generate IR function to codegen float8lt.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* float8lt_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(float8Type, FLOAT8OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::Value* llvmargs[2];
    llvm::Value* lhs_value = NULL;
    llvm::Value* rhs_value = NULL;
    llvm::Value* lhs_isnan = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_float8lt", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_float8lt = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_value = llvmargs[0];
    rhs_value = llvmargs[1];

    /* Define basic block structure */
    llvm::BasicBlock* entry = &jitted_float8lt->getEntryBlock();
    llvm::BasicBlock* not_nan = llvm::BasicBlock::Create(context, "not_nan", jitted_float8lt);
    llvm::BasicBlock* may_nan = llvm::BasicBlock::Create(context, "may_nan", jitted_float8lt);
    llvm::BasicBlock* ret_bb = llvm::BasicBlock::Create(context, "ret_bb", jitted_float8lt);

    lhs_value = builder.CreateBitCast(lhs_value, float8Type);
    rhs_value = builder.CreateBitCast(rhs_value, float8Type);

    /* enter into the control flow */
    builder.SetInsertPoint(entry);
    /* yields true if either operand is a NaN */
    llvm::Value* cmp_nan = builder.CreateFCmpUNO(lhs_value, rhs_value);
    /*
     * when both parameters are not NaN, use FCmpOLT to yield the result,
     * else we get 'true' only when the left operand is not NaN.
     */
    builder.CreateCondBr(cmp_nan, may_nan, not_nan);
    builder.SetInsertPoint(not_nan);
    llvm::Value* tmp_lt = builder.CreateFCmpOLT(lhs_value, rhs_value);
    res1 = builder.CreateZExt(tmp_lt, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(may_nan);
    /* check if the parameters are null or not */
    llvm::Function* func_isnan = llvmCodeGen->module()->getFunction("Jitted_float8isnan");
    if (func_isnan == NULL) {
        func_isnan = float8_is_nan_codegen();
    }
    lhs_isnan = builder.CreateCall(func_isnan, lhs_value, "flag1");
    lhs_isnan = builder.CreateTrunc(lhs_isnan, int1Type);
    res2 = builder.CreateSelect(lhs_isnan, Datum_0, Datum_1);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, not_nan);
    Phi->addIncoming(res2, may_nan);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_float8lt);
    return jitted_float8lt;
}

/**
 * @Description : Generate IR function to codegen float8le.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* float8le_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(float8Type, FLOAT8OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[2];
    llvm::Value* lhs_value = NULL;
    llvm::Value* rhs_value = NULL;
    llvm::Value* rhs_isnan = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_float8le", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_float8le = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_value = llvmargs[0];
    rhs_value = llvmargs[1];

    /* Define basic block structure */
    llvm::BasicBlock* entry = &jitted_float8le->getEntryBlock();
    llvm::BasicBlock* not_nan = llvm::BasicBlock::Create(context, "not_nan", jitted_float8le);
    llvm::BasicBlock* may_nan = llvm::BasicBlock::Create(context, "may_nan", jitted_float8le);
    llvm::BasicBlock* ret_bb = llvm::BasicBlock::Create(context, "ret_bb", jitted_float8le);

    lhs_value = builder.CreateBitCast(lhs_value, float8Type);
    rhs_value = builder.CreateBitCast(rhs_value, float8Type);

    /* enter into the control flow */
    builder.SetInsertPoint(entry);
    /* yields true if either operand is a NaN */
    llvm::Value* cmp_nan = builder.CreateFCmpUNO(lhs_value, rhs_value);
    /*
     * when both parameters are not NaN, use FCmpOLE to yield the result, else
     * we get 'true' when the right operand is NaN, regardless of the left operand.
     */
    builder.CreateCondBr(cmp_nan, may_nan, not_nan);
    builder.SetInsertPoint(not_nan);
    llvm::Value* tmp_le = builder.CreateFCmpOLE(lhs_value, rhs_value);
    res1 = builder.CreateZExt(tmp_le, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(may_nan);
    /* check if the parameters are null or not */
    llvm::Function* func_isnan = llvmCodeGen->module()->getFunction("Jitted_float8isnan");
    if (func_isnan == NULL) {
        func_isnan = float8_is_nan_codegen();
    }
    rhs_isnan = builder.CreateCall(func_isnan, rhs_value, "flag2");
    res2 = rhs_isnan;
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, not_nan);
    Phi->addIncoming(res2, may_nan);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_float8le);
    return jitted_float8le;
}

/**
 * @Description : Generate IR function to codegen float8gt.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* float8gt_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(float8Type, FLOAT8OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::Value* llvmargs[2];
    llvm::Value* lhs_value = NULL;
    llvm::Value* rhs_value = NULL;
    llvm::Value* rhs_isnan = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_float8gt", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_float8gt = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_value = llvmargs[0];
    rhs_value = llvmargs[1];

    /* Define basic block structure */
    llvm::BasicBlock* entry = &jitted_float8gt->getEntryBlock();
    llvm::BasicBlock* not_nan = llvm::BasicBlock::Create(context, "not_nan", jitted_float8gt);
    llvm::BasicBlock* may_nan = llvm::BasicBlock::Create(context, "may_nan", jitted_float8gt);
    llvm::BasicBlock* ret_bb = llvm::BasicBlock::Create(context, "ret_bb", jitted_float8gt);

    lhs_value = builder.CreateBitCast(lhs_value, float8Type);
    rhs_value = builder.CreateBitCast(rhs_value, float8Type);

    /* enter into the control flow */
    builder.SetInsertPoint(entry);
    /* yields true if either operand is a NaN */
    llvm::Value* cmp_nan = builder.CreateFCmpUNO(lhs_value, rhs_value);
    /*
     * when both parameters are not NaN, use FCmpOGT to yield the result,
     * else we get 'true' only when the left operand is NaN.
     */
    builder.CreateCondBr(cmp_nan, may_nan, not_nan);
    builder.SetInsertPoint(not_nan);
    llvm::Value* tmp_gt = builder.CreateFCmpOGT(lhs_value, rhs_value);
    res1 = builder.CreateZExt(tmp_gt, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(may_nan);
    /* check if the parameters are null or not */
    llvm::Function* func_isnan = llvmCodeGen->module()->getFunction("Jitted_float8isnan");
    if (func_isnan == NULL) {
        func_isnan = float8_is_nan_codegen();
    }
    rhs_isnan = builder.CreateCall(func_isnan, rhs_value, "flag1");
    rhs_isnan = builder.CreateTrunc(rhs_isnan, int1Type);
    res2 = builder.CreateSelect(rhs_isnan, Datum_0, Datum_1);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, not_nan);
    Phi->addIncoming(res2, may_nan);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_float8gt);
    return jitted_float8gt;
}

/**
 * @Description : Generate IR function to codegen float8ge.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* float8ge_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(float8Type, FLOAT8OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[2];
    llvm::Value* lhs_value = NULL;
    llvm::Value* rhs_value = NULL;
    llvm::Value* lhs_isnan = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_float8ge", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_float8ge = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_value = llvmargs[0];
    rhs_value = llvmargs[1];

    /* Define basic block structure */
    llvm::BasicBlock* entry = &jitted_float8ge->getEntryBlock();
    llvm::BasicBlock* not_nan = llvm::BasicBlock::Create(context, "not_nan", jitted_float8ge);
    llvm::BasicBlock* may_nan = llvm::BasicBlock::Create(context, "may_nan", jitted_float8ge);
    llvm::BasicBlock* ret_bb = llvm::BasicBlock::Create(context, "ret_bb", jitted_float8ge);

    lhs_value = builder.CreateBitCast(lhs_value, float8Type);
    rhs_value = builder.CreateBitCast(rhs_value, float8Type);

    /* enter into the control flow */
    builder.SetInsertPoint(entry);
    /* yields true if either operand is a NaN */
    llvm::Value* cmp_nan = builder.CreateFCmpUNO(lhs_value, rhs_value);
    /*
     * when both parameters are not NaN, use FCmpOGE to yield the result, else we
     * get 'true' when the left operand is NaN, regardless of the right operand.
     */
    builder.CreateCondBr(cmp_nan, may_nan, not_nan);
    builder.SetInsertPoint(not_nan);
    llvm::Value* tmp_lt = builder.CreateFCmpOGE(lhs_value, rhs_value);
    res1 = builder.CreateZExt(tmp_lt, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(may_nan);
    /* check if the parameters are null or not */
    llvm::Function* func_isnan = llvmCodeGen->module()->getFunction("Jitted_float8isnan");
    if (func_isnan == NULL) {
        func_isnan = float8_is_nan_codegen();
    }
    lhs_isnan = builder.CreateCall(func_isnan, lhs_value, "flag1");
    res2 = lhs_isnan;
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, not_nan);
    Phi->addIncoming(res2, may_nan);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_float8ge);
    return jitted_float8ge;
}
/**
 * @Description : Generate IR function to codegen float8_is_nan.
 *				  Since fcmpuno yields true if either operand is a
 *				  QNAN, we could compare the input parameter 'input_val'
 *				  with a fixed value to check if 'input_val' is NaN or not.
 * @return      : LLVM IR Function
 */
static llvm::Function* float8_is_nan_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(float8Type, FLOAT8OID);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::Value* llvmargs[1];
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_float8isnan", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("input_val", float8Type));
    llvm::Function* jitted_isnan = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* val = llvmargs[0];

    llvm::Value* tmp = builder.CreateBitCast(Datum_1, float8Type);

    llvm::Value* cmp = builder.CreateFCmpUNO(val, tmp);
    result = builder.CreateZExt(cmp, int64Type);
    (void)builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_isnan);
    return jitted_isnan;
}
}  // namespace dorado
