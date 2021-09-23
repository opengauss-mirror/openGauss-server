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
 *  bigintegercodegen.cpp
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/codegenutil/bigintegercodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/builtinscodegen.h"

extern const int64 ScaleMultipler[20];
extern const int64 Int64MultiOutOfBound[20];

void WrapReplValWithOldVal(bictl* ctl, ScalarValue val);

namespace dorado {
/**
 * @Description : Generate IR function to codegen bi64 add 64.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* bi64add64_codegen(bool use_ctl)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_NINTTYP(int128Type, 128);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
    DEFINE_CG_PTRTYPE(bictlPtrType, "struct.bictl");

    DEFINE_CGVAR_INT16(val_scalemask, NUMERIC_BI_SCALEMASK);
    DEFINE_CGVAR_INT32(val_numeric64, NUMERIC_64);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);

    llvm::Value* cmpval = NULL;
    llvm::Value* tmpval = NULL;
    llvm::Value* tmpval1 = NULL;
    llvm::Value* tmpval2 = NULL;
    llvm::Value* phi_val = NULL;
    llvm::Value* multi_bound = NULL;
    llvm::Value* left_scaled1 = NULL;
    llvm::Value* left_scaled2 = NULL;
    llvm::Value* right_scaled1 = NULL;
    llvm::Value* right_scaled2 = NULL;
    llvm::Value* resscale1 = NULL;
    llvm::Value* resscale2 = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Value* res3 = NULL;
    llvm::Value* res4 = NULL;
    llvm::Value* res5 = NULL;
    llvm::Value* llvmargs[3];
    llvm::PHINode* left_scaled = NULL;
    llvm::PHINode* right_scaled = NULL;
    llvm::PHINode* resscale = NULL;
    llvm::PHINode* big_res = NULL;
    llvm::Value* ctl = NULL;

    llvm::Type* Intrinsic_Tys[] = {int64Type};
    llvm::Value* Vals[2] = {int64_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /*
     * make a difference between agg sum case (sum(bi64)) and simple expression
     * case (bi64 + bi64). bictl structure is not needed in later case.
     */
    llvm::Function* jitted_bi64add64 = NULL;
    if (use_ctl) {
        GsCodeGen::FnPrototype func_type(llvmCodeGen, "Jitted_bi64add64", int64Type);
        func_type.addArgument(GsCodeGen::NamedVariable("larg", numericPtrType));
        func_type.addArgument(GsCodeGen::NamedVariable("rarg", numericPtrType));
        func_type.addArgument(GsCodeGen::NamedVariable("ctl", bictlPtrType));
        jitted_bi64add64 = func_type.generatePrototype(&builder, &llvmargs[0]);
    } else {
        GsCodeGen::FnPrototype func_type(llvmCodeGen, "Jitted_bi64add64s", int64Type);
        func_type.addArgument(GsCodeGen::NamedVariable("larg", numericPtrType));
        func_type.addArgument(GsCodeGen::NamedVariable("rarg", numericPtrType));
        jitted_bi64add64 = func_type.generatePrototype(&builder, &llvmargs[0]);
    }

    llvm::Value* larg = llvmargs[0];
    llvm::Value* rarg = llvmargs[1];
    if (use_ctl) {
        ctl = llvmargs[2];
    }

    /* get scale and real val */
    /* get lvalscale : NUMERIC_BI_SCALE(larg) */
    Vals4[0] = int64_0;
    Vals4[1] = int32_1;
    Vals4[2] = int32_0;
    Vals4[3] = int32_0;
    tmpval = builder.CreateInBoundsGEP(larg, Vals4);
    tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
    llvm::Value* lvalscale = builder.CreateAnd(tmpval, val_scalemask);
    lvalscale = builder.CreateSExt(lvalscale, int32Type, "lscale");

    /* get rvalsacle : NUMERIC_BI_SCALE(rarg) */
    tmpval = builder.CreateInBoundsGEP(rarg, Vals4);
    tmpval = builder.CreateLoad(int16Type, tmpval, "rheader");
    llvm::Value* rvalscale = builder.CreateAnd(tmpval, val_scalemask);
    rvalscale = builder.CreateSExt(rvalscale, int32Type, "rscale");

    /* get leftval : NUMERIC_64VALUE(larg) */
    Vals4[3] = int32_1;
    tmpval = builder.CreateInBoundsGEP(larg, Vals4);
    tmpval = builder.CreateBitCast(tmpval, int64PtrType);
    llvm::Value* leftval = builder.CreateLoad(int64Type, tmpval, "lval");

    /* get rightval : NUMERIC_64VALUE(rarg) */
    tmpval = builder.CreateInBoundsGEP(rarg, Vals4);
    tmpval = builder.CreateBitCast(tmpval, int64PtrType);
    llvm::Value* rightval = builder.CreateLoad(int64Type, tmpval, "rval");

    /* Adjust leftval and rightval to the same scale */
    DEFINE_BLOCK(delta_large, jitted_bi64add64);
    DEFINE_BLOCK(delta_small, jitted_bi64add64);
    DEFINE_BLOCK(adjust_true, jitted_bi64add64);
    DEFINE_BLOCK(adjust_overflow_bb, jitted_bi64add64);
    DEFINE_BLOCK(adjust_normal_bb, jitted_bi64add64);
    DEFINE_BLOCK(right_out_bound, jitted_bi64add64);
    DEFINE_BLOCK(left_out_bound, jitted_bi64add64);
    DEFINE_BLOCK(check_use_ctl, jitted_bi64add64);
    DEFINE_BLOCK(ret_bb, jitted_bi64add64);

    llvm::Value* delta_scale = builder.CreateSub(lvalscale, rvalscale, "delta_scale");
    /* check delta_scale >= 0 or not */
    cmpval = builder.CreateICmpSGE(delta_scale, int32_0, "cmp_delta_scale");
    builder.CreateCondBr(cmpval, delta_large, delta_small);

    /* corresponding to delta_scale >= 0 */
    builder.SetInsertPoint(delta_large);
    /* tmpval = y < 0 ? y : -y */
    tmpval = builder.CreateSub(int64_0, rightval);
    cmpval = builder.CreateICmpSLT(rightval, int64_0, "negative_cmp");
    phi_val = builder.CreateSelect(cmpval, rightval, tmpval);

    left_scaled1 = leftval;
    llvm::Value* mulscale = ScaleMultiCodeGen(&builder, delta_scale);
    /* corresponding to y_scaled = y * ScaleMultipler[delta_scale] */
    right_scaled1 = builder.CreateMul(rightval, mulscale);

    resscale1 = lvalscale;
    multi_bound = GetInt64MulOutofBoundCodeGen(&builder, delta_scale);
    cmpval = builder.CreateICmpSGE(phi_val, multi_bound, "bound_check");
    builder.CreateCondBr(cmpval, adjust_true, right_out_bound);

    builder.SetInsertPoint(delta_small);
    /* tmpval = x < 0 ? x : -x */
    tmpval = builder.CreateSub(int64_0, leftval);
    cmpval = builder.CreateICmpSLT(leftval, int64_0, "negative_cmp");
    phi_val = builder.CreateSelect(cmpval, leftval, tmpval);

    /* corresponding to x_scaled = x * ScaleMultipler[-delta_scale] */
    llvm::Value* mdelta_scale = builder.CreateSub(int32_0, delta_scale);
    llvm::Value* mmulscale = ScaleMultiCodeGen(&builder, mdelta_scale);
    left_scaled2 = builder.CreateMul(leftval, mmulscale);
    right_scaled2 = rightval;

    resscale2 = rvalscale;
    multi_bound = GetInt64MulOutofBoundCodeGen(&builder, mdelta_scale);
    cmpval = builder.CreateICmpSGE(phi_val, multi_bound, "bound_check");
    builder.CreateCondBr(cmpval, adjust_true, left_out_bound);

    builder.SetInsertPoint(adjust_true);
    left_scaled = builder.CreatePHI(int64Type, 2);
    right_scaled = builder.CreatePHI(int64Type, 2);
    resscale = builder.CreatePHI(int32Type, 2);

    left_scaled->addIncoming(left_scaled1, delta_large);
    left_scaled->addIncoming(left_scaled2, delta_small);

    right_scaled->addIncoming(right_scaled1, delta_large);
    right_scaled->addIncoming(right_scaled2, delta_small);

    resscale->addIncoming(resscale1, delta_large);
    resscale->addIncoming(resscale2, delta_small);

    /* do bi64 add bi64, need to check if there is any overflow */
    llvm::Function* func_sadd_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
    if (func_sadd_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
    }
    llvm::Value* res = builder.CreateCall(func_sadd_overflow, {left_scaled, right_scaled}, "sadd");
    llvm::Value* overflow_flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(overflow_flag, adjust_overflow_bb, adjust_normal_bb);

    builder.SetInsertPoint(adjust_normal_bb);
    if (use_ctl) {
        /* do Numeric res= (Numeric)ctl->store_pos */
        Vals[0] = int64_0;
        Vals[1] = int32_0;
        llvm::Value* store_pos = builder.CreateInBoundsGEP(ctl, Vals);
        store_pos = builder.CreateLoad(int64Type, store_pos, "store_pos");
        llvm::Value* resnum = builder.CreateIntToPtr(store_pos, numericPtrType);

        /* set result scale */
        tmpval = builder.CreateOr(resscale, val_numeric64);
        tmpval = builder.CreateTrunc(tmpval, int16Type);
        Vals4[0] = int64_0;
        Vals4[1] = int32_1;
        Vals4[2] = int32_0;
        Vals4[3] = int32_0;
        llvm::Value* res_header = builder.CreateInBoundsGEP(resnum, Vals4);
        builder.CreateStore(tmpval, res_header);

        /* set result value */
        Vals4[3] = int32_1;
        llvm::Value* res_data = builder.CreateInBoundsGEP(resnum, Vals4);
        res_data = builder.CreateBitCast(res_data, int64PtrType);
        llvm::Value* result = builder.CreateExtractValue(res, 0);
        builder.CreateStore(result, res_data);
        res1 = int64_0;
    } else {
        res1 = builder.CreateExtractValue(res, 0);
        llvm::Value* tmpscale = builder.CreateTrunc(resscale, int8Type);
        res1 = WrapmakeNumeric64CodeGen(&builder, res1, tmpscale);
    }
    builder.CreateBr(ret_bb);

    /* adjust true without out_of_bound */
    builder.SetInsertPoint(adjust_overflow_bb);
    tmpval1 = builder.CreateSExt(left_scaled, int128Type);
    tmpval2 = builder.CreateSExt(right_scaled, int128Type);
    res2 = builder.CreateAdd(tmpval1, tmpval2, "int128add");
    tmpval = builder.CreateTrunc((llvm::Value*)resscale, int8Type);
    res2 = WrapmakeNumeric128CodeGen(&builder, res2, tmpval);
    builder.CreateBr(check_use_ctl);

    /* right_scaled is out of bound */
    builder.SetInsertPoint(right_out_bound);
    tmpval1 = builder.CreateSExt(leftval, int128Type);
    tmpval = GetScaleMultiCodeGen(&builder, delta_scale);
    tmpval = builder.CreateSExt(tmpval, int128Type);
    tmpval2 = builder.CreateSExt(rightval, int128Type);
    tmpval2 = builder.CreateMul(tmpval2, tmpval, "right_scale");
    res3 = builder.CreateAdd(tmpval1, tmpval2, "right_int128");
    lvalscale = builder.CreateTrunc(lvalscale, int8Type);
    res3 = WrapmakeNumeric128CodeGen(&builder, res3, lvalscale);
    builder.CreateBr(check_use_ctl);

    /* leftval_scaled is out of bound */
    builder.SetInsertPoint(left_out_bound);
    tmpval = GetScaleMultiCodeGen(&builder, mdelta_scale);
    tmpval = builder.CreateSExt(tmpval, int128Type);
    tmpval1 = builder.CreateSExt(leftval, int128Type);
    tmpval1 = builder.CreateMul(tmpval1, tmpval, "left_scale");
    tmpval2 = builder.CreateSExt(rightval, int128Type);
    res4 = builder.CreateAdd(tmpval1, tmpval2);
    rvalscale = builder.CreateTrunc(rvalscale, int8Type);
    res4 = WrapmakeNumeric128CodeGen(&builder, res4, rvalscale);
    builder.CreateBr(check_use_ctl);

    builder.SetInsertPoint(check_use_ctl);
    big_res = builder.CreatePHI(int64Type, 3);
    big_res->addIncoming(res2, adjust_overflow_bb);
    big_res->addIncoming(res3, right_out_bound);
    big_res->addIncoming(res4, left_out_bound);
    if (use_ctl) {
        tmpval = (llvm::Value*)big_res;
        WrapReplVarWithOldCodeGen(&builder, ctl, tmpval);
        res5 = int64_0;
    } else {
        /* return big_res directly */
        res5 = (llvm::Value*)big_res;
    }
    builder.CreateBr(ret_bb);

    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* phi_ret = builder.CreatePHI(int64Type, 2);
    phi_ret->addIncoming(res1, adjust_normal_bb);
    phi_ret->addIncoming(res5, check_use_ctl);
    (void)builder.CreateRet(phi_ret);

    llvmCodeGen->FinalizeFunction(jitted_bi64add64);
    return jitted_bi64add64;
}

/**
 * @Description : Generate IR function to bi64 sub 64
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* bi64sub64_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_NINTTYP(int128Type, 128);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");

    DEFINE_CGVAR_INT16(val_scalemask, NUMERIC_BI_SCALEMASK);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);

    llvm::Value* cmpval = NULL;
    llvm::Value* tmpval = NULL;
    llvm::Value* tmpval1 = NULL;
    llvm::Value* tmpval2 = NULL;
    llvm::Value* phi_val = NULL;
    llvm::Value* multi_bound = NULL;
    llvm::Value* left_scaled1 = NULL;
    llvm::Value* left_scaled2 = NULL;
    llvm::Value* right_scaled1 = NULL;
    llvm::Value* right_scaled2 = NULL;
    llvm::Value* resscale1 = NULL;
    llvm::Value* resscale2 = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Value* res3 = NULL;
    llvm::Value* res4 = NULL;
    llvm::Value* llvmargs[2];
    llvm::PHINode* left_scaled = NULL;
    llvm::PHINode* right_scaled = NULL;
    llvm::PHINode* resscale = NULL;

    llvm::Type* Intrinsic_Tys[] = {int64Type};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    GsCodeGen::FnPrototype func_type(llvmCodeGen, "Jitted_bi64sub64", int64Type);
    func_type.addArgument(GsCodeGen::NamedVariable("larg", numericPtrType));
    func_type.addArgument(GsCodeGen::NamedVariable("rarg", numericPtrType));
    llvm::Function* jitted_bi64sub64 = func_type.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* larg = llvmargs[0];
    llvm::Value* rarg = llvmargs[1];

    /* get scale and real val */
    /* get lvalscale : NUMERIC_BI_SCALE(larg) */
    Vals4[0] = int64_0;
    Vals4[1] = int32_1;
    Vals4[2] = int32_0;
    Vals4[3] = int32_0;
    tmpval = builder.CreateInBoundsGEP(larg, Vals4);
    tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
    llvm::Value* lvalscale = builder.CreateAnd(tmpval, val_scalemask);
    lvalscale = builder.CreateSExt(lvalscale, int32Type, "lscale");

    /* get rvalsacle : NUMERIC_BI_SCALE(rarg) */
    tmpval = builder.CreateInBoundsGEP(rarg, Vals4);
    tmpval = builder.CreateLoad(int16Type, tmpval, "rheader");
    llvm::Value* rvalscale = builder.CreateAnd(tmpval, val_scalemask);
    rvalscale = builder.CreateSExt(rvalscale, int32Type, "rscale");

    /* get leftval : NUMERIC_64VALUE(larg) */
    Vals4[3] = int32_1;
    tmpval = builder.CreateInBoundsGEP(larg, Vals4);
    tmpval = builder.CreateBitCast(tmpval, int64PtrType);
    llvm::Value* leftval = builder.CreateLoad(int64Type, tmpval, "lval");

    /* get rightval : NUMERIC_64VALUE(rarg) */
    tmpval = builder.CreateInBoundsGEP(rarg, Vals4);
    tmpval = builder.CreateBitCast(tmpval, int64PtrType);
    llvm::Value* rightval = builder.CreateLoad(int64Type, tmpval, "rval");

    /* Adjust leftval and rightval to the same scale */
    DEFINE_BLOCK(delta_large, jitted_bi64sub64);
    DEFINE_BLOCK(delta_small, jitted_bi64sub64);
    DEFINE_BLOCK(adjust_true, jitted_bi64sub64);
    DEFINE_BLOCK(adjust_overflow_bb, jitted_bi64sub64);
    DEFINE_BLOCK(adjust_normal_bb, jitted_bi64sub64);
    DEFINE_BLOCK(right_out_bound, jitted_bi64sub64);
    DEFINE_BLOCK(left_out_bound, jitted_bi64sub64);
    DEFINE_BLOCK(ret_bb, jitted_bi64sub64);

    llvm::Value* delta_scale = builder.CreateSub(lvalscale, rvalscale, "delta_scale");
    /* check delta_scale >= 0 or not */
    cmpval = builder.CreateICmpSGE(delta_scale, int32_0, "cmp_delta_scale");
    builder.CreateCondBr(cmpval, delta_large, delta_small);

    /* corresponding to delta_scale >= 0 */
    builder.SetInsertPoint(delta_large);
    /* tmpval = y < 0 ? y : -y */
    tmpval = builder.CreateSub(int64_0, rightval);
    cmpval = builder.CreateICmpSLT(rightval, int64_0, "negative_cmp");
    phi_val = builder.CreateSelect(cmpval, rightval, tmpval);

    left_scaled1 = leftval;
    llvm::Value* mulscale = ScaleMultiCodeGen(&builder, delta_scale);
    /* corresponding to y_scaled = y * ScaleMultipler[delta_scale] */
    right_scaled1 = builder.CreateMul(rightval, mulscale);

    resscale1 = lvalscale;
    multi_bound = GetInt64MulOutofBoundCodeGen(&builder, delta_scale);
    cmpval = builder.CreateICmpSGE(phi_val, multi_bound, "bound_check");
    builder.CreateCondBr(cmpval, adjust_true, right_out_bound);

    builder.SetInsertPoint(delta_small);
    /* tmpval = x < 0 ? x : -x */
    tmpval = builder.CreateSub(int64_0, leftval);
    cmpval = builder.CreateICmpSLT(leftval, int64_0, "negative_cmp");
    phi_val = builder.CreateSelect(cmpval, leftval, tmpval);

    /* corresponding to x_scaled = x * ScaleMultipler[-delta_scale] */
    llvm::Value* mdelta_scale = builder.CreateSub(int32_0, delta_scale);
    llvm::Value* mmulscale = ScaleMultiCodeGen(&builder, mdelta_scale);
    left_scaled2 = builder.CreateMul(leftval, mmulscale);
    right_scaled2 = rightval;

    resscale2 = rvalscale;
    multi_bound = GetInt64MulOutofBoundCodeGen(&builder, mdelta_scale);
    cmpval = builder.CreateICmpSGE(phi_val, multi_bound, "bound_check");
    builder.CreateCondBr(cmpval, adjust_true, left_out_bound);

    /* leftval_scaled and right_scaled both don't overflow, add directly */
    builder.SetInsertPoint(adjust_true);
    left_scaled = builder.CreatePHI(int64Type, 2);
    right_scaled = builder.CreatePHI(int64Type, 2);
    resscale = builder.CreatePHI(int32Type, 2);

    left_scaled->addIncoming(left_scaled1, delta_large);
    left_scaled->addIncoming(left_scaled2, delta_small);

    right_scaled->addIncoming(right_scaled1, delta_large);
    right_scaled->addIncoming(right_scaled2, delta_small);

    resscale->addIncoming(resscale1, delta_large);
    resscale->addIncoming(resscale2, delta_small);

    /* do bi64 add bi64, need to check if there is any overflow */
    llvm::Function* func_ssub_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_ssub_with_overflow, Intrinsic_Tys);
    if (func_ssub_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::sasub_with_overflow function!\n")));
    }
    llvm::Value* res = builder.CreateCall(func_ssub_overflow, {left_scaled, right_scaled}, "sadd");
    llvm::Value* overflow_flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(overflow_flag, adjust_overflow_bb, adjust_normal_bb);

    builder.SetInsertPoint(adjust_normal_bb);
    /* if not out of bound, return makeNumeric64(result, resScale) */
    res1 = builder.CreateExtractValue(res, 0);
    tmpval = builder.CreateTrunc(resscale, int8Type);
    res1 = WrapmakeNumeric64CodeGen(&builder, res1, tmpval);
    builder.CreateBr(ret_bb);

    /* if overflow, turn to int128 type first */
    builder.SetInsertPoint(adjust_overflow_bb);
    tmpval1 = builder.CreateSExt(left_scaled, int128Type);
    tmpval2 = builder.CreateSExt(right_scaled, int128Type);
    res2 = builder.CreateSub(tmpval1, tmpval2, "int128add");
    tmpval = builder.CreateTrunc((llvm::Value*)resscale, int8Type);
    res2 = WrapmakeNumeric128CodeGen(&builder, res2, tmpval);
    builder.CreateBr(ret_bb);

    /* right_scaled is out of bound */
    builder.SetInsertPoint(right_out_bound);
    tmpval1 = builder.CreateSExt(leftval, int128Type);
    tmpval = GetScaleMultiCodeGen(&builder, delta_scale);
    tmpval = builder.CreateSExt(tmpval, int128Type);
    tmpval2 = builder.CreateSExt(rightval, int128Type);
    tmpval2 = builder.CreateMul(tmpval2, tmpval, "right_scale");
    res3 = builder.CreateSub(tmpval1, tmpval2, "right_int128");
    lvalscale = builder.CreateTrunc(lvalscale, int8Type);
    res3 = WrapmakeNumeric128CodeGen(&builder, res3, lvalscale);
    builder.CreateBr(ret_bb);

    /* leftval_scaled is out of bound */
    builder.SetInsertPoint(left_out_bound);
    tmpval = GetScaleMultiCodeGen(&builder, mdelta_scale);
    tmpval = builder.CreateSExt(tmpval, int128Type);
    tmpval1 = builder.CreateSExt(leftval, int128Type);
    tmpval1 = builder.CreateMul(tmpval1, tmpval, "left_scale");
    tmpval2 = builder.CreateSExt(rightval, int128Type);
    res4 = builder.CreateSub(tmpval1, tmpval2);
    rvalscale = builder.CreateTrunc(rvalscale, int8Type);
    res4 = WrapmakeNumeric128CodeGen(&builder, res4, rvalscale);
    builder.CreateBr(ret_bb);

    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* phi_ret = builder.CreatePHI(int64Type, 4);
    phi_ret->addIncoming(res1, adjust_normal_bb);
    phi_ret->addIncoming(res2, adjust_overflow_bb);
    phi_ret->addIncoming(res3, right_out_bound);
    phi_ret->addIncoming(res4, left_out_bound);
    (void)builder.CreateRet(phi_ret);

    llvmCodeGen->FinalizeFunction(jitted_bi64sub64);
    return jitted_bi64sub64;
}

/**
 * @Description : Generate IR function to bi64 mul 64
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* bi64mul64_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_NINTTYP(int128Type, 128);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
    DEFINE_CGVAR_INT32(maxInt64digitsNum, MAXINT64DIGIT);
    DEFINE_CGVAR_INT16(val_scalemask, NUMERIC_BI_SCALEMASK);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* tmpval = NULL;
    llvm::Value* cmpval = NULL;
    llvm::Value* templeftVal = NULL;
    llvm::Value* temprightVal = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;

    llvm::Type* Intrinsic_Tys[] = {int64Type};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    GsCodeGen::FnPrototype func_type(llvmCodeGen, "Jitted_bi64mul64", int64Type);
    func_type.addArgument(GsCodeGen::NamedVariable("larg", numericPtrType));
    func_type.addArgument(GsCodeGen::NamedVariable("rarg", numericPtrType));
    llvm::Function* jitted_bi64mul64 = func_type.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* larg = llvmargs[0];
    llvm::Value* rarg = llvmargs[1];

    /* get lvalsacle : NUMERIC_BI_SCALE(larg) */
    Vals4[1] = int32_1;
    tmpval = builder.CreateInBoundsGEP(larg, Vals4);
    tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
    llvm::Value* lvalscale = builder.CreateAnd(tmpval, val_scalemask);
    lvalscale = builder.CreateSExt(lvalscale, int32Type, "lscale");

    /* get rvalsacle : NUMERIC_BI_SCALE(rarg) */
    tmpval = builder.CreateInBoundsGEP(rarg, Vals4);
    tmpval = builder.CreateLoad(int16Type, tmpval, "rheader");
    llvm::Value* rvalscale = builder.CreateAnd(tmpval, val_scalemask);
    rvalscale = builder.CreateSExt(rvalscale, int32Type, "rscale");

    /* get leftval : NUMERIC_64VALUE(larg) */
    Vals4[3] = int32_1;
    tmpval = builder.CreateInBoundsGEP(larg, Vals4);
    tmpval = builder.CreateBitCast(tmpval, int64PtrType);
    llvm::Value* leftval = builder.CreateLoad(int64Type, tmpval, "lval");

    /* get rightval : NUMERIC_64VALUE(rarg) */
    tmpval = builder.CreateInBoundsGEP(rarg, Vals4);
    tmpval = builder.CreateBitCast(tmpval, int64PtrType);
    llvm::Value* rightval = builder.CreateLoad(int64Type, tmpval, "rval");

    llvm::Value* result_scale = builder.CreateAdd(lvalscale, rvalscale, "resullt_scale");

    DEFINE_BLOCK(mul64, jitted_bi64mul64);
    DEFINE_BLOCK(mul128, jitted_bi64mul64);
    DEFINE_BLOCK(ret_bb, jitted_bi64mul64);

    /* check delta_scale >= MAX DIGITS INT64 NUMBER or not */
    cmpval = builder.CreateICmpSLE(result_scale, maxInt64digitsNum, "cmp_delta_scale");

    /* do bi64 mul bi64, need to check if there is any overflow */
    llvm::Function* func_smul_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_smul_with_overflow, Intrinsic_Tys);
    if (func_smul_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::smul_with_overflow function!\n")));
    }
    llvm::Value* res = builder.CreateCall(func_smul_overflow, {leftval, rightval}, "smul");
    llvm::Value* overflow_flag = builder.CreateExtractValue(res, 1);
    llvm::Value* res_64 = builder.CreateExtractValue(res, 0);
    cmpval = builder.CreateAnd(cmpval, builder.CreateNot(overflow_flag));

    result_scale = builder.CreateTrunc(result_scale, int8Type);
    builder.CreateCondBr(cmpval, mul64, mul128);

    builder.SetInsertPoint(mul64);
    res1 = WrapmakeNumeric64CodeGen(&builder, res_64, result_scale);
    builder.CreateBr(ret_bb);

    /* if overflow, turn to int128 type first */
    builder.SetInsertPoint(mul128);
    templeftVal = builder.CreateSExt(leftval, int128Type);
    temprightVal = builder.CreateSExt(rightval, int128Type);
    templeftVal = builder.CreateMul(templeftVal, temprightVal, "res_val");
    res2 = WrapmakeNumeric128CodeGen(&builder, templeftVal, result_scale);
    builder.CreateBr(ret_bb);

    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* phi_ret = builder.CreatePHI(int64Type, 2);
    phi_ret->addIncoming(res1, mul64);
    phi_ret->addIncoming(res2, mul128);
    (void)builder.CreateRet(phi_ret);

    llvmCodeGen->FinalizeFunction(jitted_bi64mul64);

    return jitted_bi64mul64;
}

/**
 * @Description : Generate IR function to bi64 div 64
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* bi64div64_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");

    llvm::Value* llvmargs[2];

    GsCodeGen::FnPrototype func_type(llvmCodeGen, "Jitted_bi64div64", int64Type);
    func_type.addArgument(GsCodeGen::NamedVariable("larg", numericPtrType));
    func_type.addArgument(GsCodeGen::NamedVariable("rarg", numericPtrType));
    llvm::Function* jitted_bi64div64 = func_type.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* larg = llvmargs[0];
    llvm::Value* rarg = llvmargs[1];

    llvm::Value* result = Wrapbi64div64CodeGen(&builder, larg, rarg);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_bi64div64);

    return jitted_bi64div64;
}

/**
 * @Description	: Get the element of array Int64MultiOutOfBound according to the scale.
 * @in ptrbuilder	: LLVM Builder associated with the current module.
 * @in deltascale	: The delta scale between two int64 data.
 * @return		: multipler used for comparing whether the result of arg * ScaleMultipler
 *				  is out of int64 bound.
 */
llvm::Value* GetInt64MulOutofBoundCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* deltascale)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(int64_0, 0);

    /* get the global Int64MultiOutOfBound array (int64) from current module */
    llvm::GlobalVariable* global_mbound = llvmCodeGen->module()->getNamedGlobal("Int64MultiOutOfBound");
    llvm::Value* Vals[2] = {int64_0, int64_0};

    Vals[1] = ptrbuilder->CreateSExt(deltascale, int64Type);
    llvm::Value* tmpval = ptrbuilder->CreateInBoundsGEP(global_mbound, Vals);
    llvm::Value* multi_bound = ptrbuilder->CreateLoad(int64Type, tmpval, "multibound");
    return multi_bound;
}

/**
 * @Description	: Get the element of array ScaleMultpler according to the scale.
 * @in ptrbuilder	: LLVM Builder associated with the current module.
 * @in deltascale	: The delta scale between two int64 data.
 * @return		: multipler used for aligning two int64 data.
 */
llvm::Value* ScaleMultiCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* deltascale)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(int64_0, 0);

    /* get the global ScaleMultiplier array (int64) from current module */
    llvm::GlobalVariable* global_smul = llvmCodeGen->module()->getNamedGlobal("ScaleMultipler");
    llvm::Value* Vals[2] = {int64_0, int64_0};

    Vals[1] = ptrbuilder->CreateSExt(deltascale, int64Type);
    llvm::Value* tmpval = ptrbuilder->CreateInBoundsGEP(global_smul, Vals);
    llvm::Value* scalemul = ptrbuilder->CreateLoad(int64Type, tmpval, "scalemul");
    return scalemul;
}

/**
 * @Description	: Get the element of array ScaleMultpler according to the scale.
 * @in ptrbuilder	: LLVM Builder associated with the current module.
 * @in deltascale	: The delta scale needed for aligning two int128 data.
 * @return		: The factorial of 10 in LLVM assemble.
 */
llvm::Value* GetScaleMultiCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* deltascale)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_NINTTYP(int128Type, 128);
    DEFINE_CGVAR_INT64(int64_0, 0);

    /* get the global getScaleMultiplier array (int128) from current module */
    llvm::GlobalVariable* global_gsmul = llvmCodeGen->module()->getNamedGlobal("_ZZ18getScaleMultiplieriE6values");
    llvm::Value* Vals[2] = {int64_0, int64_0};

    Vals[1] = ptrbuilder->CreateSExt(deltascale, int64Type);
    llvm::Value* tmpval = ptrbuilder->CreateInBoundsGEP(global_gsmul, Vals);
    llvm::Value* gscalemul = ptrbuilder->CreateLoad(int128Type, tmpval, "getscalemul");
    return gscalemul;
}

/**
 * @Description	: Copy a new value according to val. If we have enough space,
 *				  reuse that space to create that value.
 * @in ptrbuilder	: LLVM Builder associated with the current module.
 * @in ctl		: bictl data in LLVM assemble.
 * @in Val		: The value needed to create new value.
 */
void WrapReplVarWithOldCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* ctl, llvm::Value* Val)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(bictlPtrType, "struct.bictl");

    llvm::Function* jitted_cpypoldval = llvmCodeGen->module()->getFunction("LLVMWrapCopyVarPOldVal");
    if (jitted_cpypoldval == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapCopyVarPOldVal", voidType);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("ctl", bictlPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("val", int64Type));
        jitted_cpypoldval = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapCopyVarPOldVal", (void*)WrapReplValWithOldVal);
    }
    llvmCodeGen->FinalizeFunction(jitted_cpypoldval);

    ptrbuilder->CreateCall(jitted_cpypoldval, {ctl, Val});

    return;
}

/**
 * @Description	: Interface of bi64div64 in LLVM assemble.
 * @in ptrbuilder	: LLVM Builder associated with the current module.
 * @in larg		: The left numeric argument in LLVM assemble.
 * @in rarg		: The right numeric argument in LLVM assemble.
 * @return		: The rusult of (bi64)larg/rarg in LLVM assemble.
 */
llvm::Value* Wrapbi64div64CodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* larg, llvm::Value* rarg)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");

    llvm::Function* jitted_wbi64div64 = llvmCodeGen->module()->getFunction("LLVMWrapbi64div64");
    if (jitted_wbi64div64 == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapbi64div64", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("larg", numericPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("rarg", numericPtrType));
        jitted_wbi64div64 = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapbi64div64", (void*)Simplebi64div64);
    }
    llvmCodeGen->FinalizeFunction(jitted_wbi64div64);

    return ptrbuilder->CreateCall(jitted_wbi64div64, {larg, rarg});
}
}  // namespace dorado

/**
 * @Description	: Create a new value according to val.
 * @in ctl	: bictl value.
 * @in val	: value needed to creae new value.
 * @return	: void.
 */
void WrapReplValWithOldVal(bictl* ctl, ScalarValue val)
{
    ctl->store_pos = replaceVariable(ctl->context, ctl->store_pos, val);
}
