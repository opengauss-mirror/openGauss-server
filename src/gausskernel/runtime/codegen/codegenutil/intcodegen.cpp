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
 * intcodegen.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/codegenutil/intcodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/builtinscodegen.h"
#include "codegen/codegendebuger.h"

namespace dorado {
/*
 * LLVM provides three kinds of intrinsics for fast arithmetic
 * overflow checking: sadd/uadd, ssub/usub, smul/umul. Each
 * of these intrinsics returns a two-element struct, the first
 * element of this struct contains the result of the corresponding
 * arithmetic operation modulo 2^n, where n is the bit width of the
 * result. The second element of the result is an i1 that is 1 if
 * the arithmetic operation overflowed and 0 otherwise.
 *
 * For the LLVM IR functions accomplished below, we will use these
 * intrinsics. When we meet with an overflow, we will cast an error.
 */
/**
 * @Description : Generate IR function to codegen int4pl.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int4pl_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Type* Intrinsic_Tys[] = {int32Type};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int4pl", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int4pl = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(overflow_bb, jitted_int4pl);
    DEFINE_BLOCK(normal_bb, jitted_int4pl);
    DEFINE_BLOCK(ret_bb, jitted_int4pl);

    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    rhs_value = builder.CreateTrunc(rhs_value, int32Type);
    const char* errorstr = "integer out of range";

    llvm::Function* func_sadd_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
    if (func_sadd_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
    }

    llvm::Value* res = builder.CreateCall(func_sadd_overflow, {lhs_value, rhs_value}, "sadd");
    llvm::Value* flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(flag, overflow_bb, normal_bb);
    builder.SetInsertPoint(overflow_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(normal_bb);
    res2 = builder.CreateExtractValue(res, 0);
    res2 = builder.CreateSExt(res2, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, overflow_bb);
    Phi->addIncoming(res2, normal_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int4pl);
    return jitted_int4pl;
}

/**
 * @Description : Generate IR function to codegen int4mi.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int4mi_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Type* Intrinsic_Tys[] = {int32Type};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int4mi", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int4mi = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(overflow_bb, jitted_int4mi);
    DEFINE_BLOCK(normal_bb, jitted_int4mi);
    DEFINE_BLOCK(ret_bb, jitted_int4mi);

    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    rhs_value = builder.CreateTrunc(rhs_value, int32Type);
    const char* errorstr = "integer out of range";

    llvm::Function* func_ssub_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_ssub_with_overflow, Intrinsic_Tys);
    if (func_ssub_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::ssub_with_overflow function!\n")));
    }

    llvm::Value* res = builder.CreateCall(func_ssub_overflow, {lhs_value, rhs_value}, "ssub");
    llvm::Value* flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(flag, overflow_bb, normal_bb);
    builder.SetInsertPoint(overflow_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(normal_bb);
    res2 = builder.CreateExtractValue(res, 0);
    res2 = builder.CreateSExt(res2, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, overflow_bb);
    Phi->addIncoming(res2, normal_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int4mi);
    return jitted_int4mi;
}

/**
 * @Description : Generate IR function to codegen int4mul.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int4mul_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Type* Intrinsic_Tys[] = {int32Type};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int4mul", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int4mul = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(overflow_bb, jitted_int4mul);
    DEFINE_BLOCK(normal_bb, jitted_int4mul);
    DEFINE_BLOCK(ret_bb, jitted_int4mul);

    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    rhs_value = builder.CreateTrunc(rhs_value, int32Type);
    const char* errorstr = "integer out of range";

    llvm::Function* func_smul_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_smul_with_overflow, Intrinsic_Tys);
    if (func_smul_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::smul_with_overflow function!\n")));
    }

    llvm::Value* res = builder.CreateCall(func_smul_overflow, {lhs_value, rhs_value}, "smul");
    llvm::Value* flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(flag, overflow_bb, normal_bb);
    builder.SetInsertPoint(overflow_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(normal_bb);
    res2 = builder.CreateExtractValue(res, 0);
    res2 = builder.CreateSExt(res2, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, overflow_bb);
    Phi->addIncoming(res2, normal_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int4mul);
    return jitted_int4mul;
}

/**
 * @Description : Generate IR function to codegen int4div.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int4div_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(float8Type, FLOAT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT32(int32_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int4div", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int4div = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(zero_bb, jitted_int4div);
    DEFINE_BLOCK(nonzero_bb, jitted_int4div);
    DEFINE_BLOCK(ret_bb, jitted_int4div);

    /* First get the right values by truncating */
    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    rhs_value = builder.CreateTrunc(rhs_value, int32Type);
    const char* errorstr = "division by zero";

    /* Second make sure the denominator is not zero */
    llvm::Value* tmp = builder.CreateICmpEQ(rhs_value, int32_0, "decide");
    builder.CreateCondBr(tmp, zero_bb, nonzero_bb);
    builder.SetInsertPoint(nonzero_bb);
    /* Turn integer to double precision to promise the accuracy */
    lhs_value = builder.CreateSIToFP(lhs_value, float8Type);
    rhs_value = builder.CreateSIToFP(rhs_value, float8Type);
    /* Do division by calling float point division */
    res2 = builder.CreateFDiv(lhs_value, rhs_value, "int4_div");
    res2 = builder.CreateBitCast(res2, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(zero_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, zero_bb);
    Phi->addIncoming(res2, nonzero_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int4div);
    return jitted_int4div;
}

/**
 * @Description : Generate IR function to codegen int8pl.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int8pl_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Type* Intrinsic_Tys[] = {int64Type};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int8pl", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int8pl = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(overflow_bb, jitted_int8pl);
    DEFINE_BLOCK(normal_bb, jitted_int8pl);
    DEFINE_BLOCK(ret_bb, jitted_int8pl);

    const char* errorstr = "bigint out of range";

    llvm::Function* func_sadd_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
    if (func_sadd_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
    }

    llvm::Value* res = builder.CreateCall(func_sadd_overflow, {lhs_value, rhs_value}, "sadd");
    llvm::Value* flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(flag, overflow_bb, normal_bb);
    builder.SetInsertPoint(overflow_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(normal_bb);
    res2 = builder.CreateExtractValue(res, 0);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, overflow_bb);
    Phi->addIncoming(res2, normal_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int8pl);
    return jitted_int8pl;
}

/**
 * @Description : Generate IR function to codegen int8mi.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int8mi_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Type* Intrinsic_Tys[] = {int64Type};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int8mi", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int8mi = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(overflow_bb, jitted_int8mi);
    DEFINE_BLOCK(normal_bb, jitted_int8mi);
    DEFINE_BLOCK(ret_bb, jitted_int8mi);

    const char* errorstr = "bigint out of range";

    llvm::Function* func_ssub_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_ssub_with_overflow, Intrinsic_Tys);
    if (func_ssub_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::ssub_with_overflow function!\n")));
    }

    llvm::Value* res = builder.CreateCall(func_ssub_overflow, {lhs_value, rhs_value}, "ssub");
    llvm::Value* flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(flag, overflow_bb, normal_bb);
    builder.SetInsertPoint(overflow_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(normal_bb);
    res2 = builder.CreateExtractValue(res, 0);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, overflow_bb);
    Phi->addIncoming(res2, normal_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int8mi);
    return jitted_int8mi;
}

/**
 * @Description : Generate IR function to codegen int8mul.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int8mul_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Type* Intrinsic_Tys[] = {int64Type};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int8mul", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int8mul = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(overflow_bb, jitted_int8mul);
    DEFINE_BLOCK(normal_bb, jitted_int8mul);
    DEFINE_BLOCK(ret_bb, jitted_int8mul);

    const char* errorstr = "bigint out of range";

    llvm::Function* func_smul_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_smul_with_overflow, Intrinsic_Tys);
    if (func_smul_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::smul_with_overflow function!")));
    }

    llvm::Value* res = builder.CreateCall(func_smul_overflow, {lhs_value, rhs_value}, "ssub");
    llvm::Value* flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(flag, overflow_bb, normal_bb);
    builder.SetInsertPoint(overflow_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(normal_bb);
    res2 = builder.CreateExtractValue(res, 0);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, overflow_bb);
    Phi->addIncoming(res2, normal_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int8mul);
    return jitted_int8mul;
}

/**
 * @Description : Generate IR function to codegen int8div.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int8div_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(float8Type, FLOAT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int8div", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int8div = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(zero_bb, jitted_int8div);
    DEFINE_BLOCK(nonzero_bb, jitted_int8div);
    DEFINE_BLOCK(ret_bb, jitted_int8div);

    const char* errorstr = "division by zero";

    llvm::Value* tmp = builder.CreateICmpEQ(rhs_value, Datum_0, "decide");
    builder.CreateCondBr(tmp, zero_bb, nonzero_bb);
    builder.SetInsertPoint(nonzero_bb);
    lhs_value = builder.CreateSIToFP(lhs_value, float8Type);
    rhs_value = builder.CreateSIToFP(rhs_value, float8Type);
    res2 = builder.CreateFDiv(lhs_value, rhs_value, "int8_div");
    res2 = builder.CreateBitCast(res2, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(zero_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, zero_bb);
    Phi->addIncoming(res2, nonzero_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int8div);
    return jitted_int8div;
}

/*
 * Since LLVM could only deal with operations between values that have the
 * same data type, we need to truncate one of the operands to get the right
 * value and then do sign extension to make the two operands have the same
 * data type for all the IR functions below.
 */
/**
 * @Description : Generate IR function to codegen int48pl.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int48pl_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Type* Intrinsic_Tys[] = {int64Type};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int48pl", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int48pl = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(overflow_bb, jitted_int48pl);
    DEFINE_BLOCK(normal_bb, jitted_int48pl);
    DEFINE_BLOCK(ret_bb, jitted_int48pl);

    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    lhs_value = builder.CreateSExt(lhs_value, int64Type);
    const char* errorstr = "bigint out of range";

    llvm::Function* func_sadd_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
    if (func_sadd_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
    }

    llvm::Value* res = builder.CreateCall(func_sadd_overflow, {lhs_value, rhs_value}, "sadd");
    llvm::Value* flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(flag, overflow_bb, normal_bb);
    builder.SetInsertPoint(overflow_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(normal_bb);
    res2 = builder.CreateExtractValue(res, 0);
    res2 = builder.CreateSExt(res2, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, overflow_bb);
    Phi->addIncoming(res2, normal_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int48pl);
    return jitted_int48pl;
}

/**
 * @Description : Generate IR function to codegen int84pl.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int84pl_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Type* Intrinsic_Tys[] = {int64Type};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int84pl", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int84pl = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(overflow_bb, jitted_int84pl);
    DEFINE_BLOCK(normal_bb, jitted_int84pl);
    DEFINE_BLOCK(ret_bb, jitted_int84pl);

    rhs_value = builder.CreateTrunc(rhs_value, int32Type);
    rhs_value = builder.CreateSExt(rhs_value, int64Type);
    const char* errorstr = "bigint out of range";

    llvm::Function* func_sadd_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
    if (func_sadd_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
    }

    llvm::Value* res = builder.CreateCall(func_sadd_overflow, {lhs_value, rhs_value}, "sadd");
    llvm::Value* flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(flag, overflow_bb, normal_bb);
    builder.SetInsertPoint(overflow_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(normal_bb);
    res2 = builder.CreateExtractValue(res, 0);
    res2 = builder.CreateSExt(res2, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, overflow_bb);
    Phi->addIncoming(res2, normal_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int84pl);
    return jitted_int84pl;
}

/**
 * @Description : Generate IR function to codegen int48mi.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int48mi_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Type* Intrinsic_Tys[] = {int64Type};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int48mi", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int48mi = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(overflow_bb, jitted_int48mi);
    DEFINE_BLOCK(normal_bb, jitted_int48mi);
    DEFINE_BLOCK(ret_bb, jitted_int48mi);

    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    lhs_value = builder.CreateSExt(lhs_value, int64Type);
    const char* errorstr = "bigint out of range";

    llvm::Function* func_ssub_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_ssub_with_overflow, Intrinsic_Tys);
    if (func_ssub_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::ssub_with_overflow function!\n")));
    }

    llvm::Value* res = builder.CreateCall(func_ssub_overflow, {lhs_value, rhs_value}, "ssub");
    llvm::Value* flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(flag, overflow_bb, normal_bb);
    builder.SetInsertPoint(overflow_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(normal_bb);
    res2 = builder.CreateExtractValue(res, 0);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, overflow_bb);
    Phi->addIncoming(res2, normal_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int48mi);
    return jitted_int48mi;
}

/**
 * @Description : Generate IR function to codegen int84mi.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int84mi_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Type* Intrinsic_Tys[] = {int64Type};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int84mi", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int84mi = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(overflow_bb, jitted_int84mi);
    DEFINE_BLOCK(normal_bb, jitted_int84mi);
    DEFINE_BLOCK(ret_bb, jitted_int84mi);

    rhs_value = builder.CreateTrunc(rhs_value, int32Type);
    rhs_value = builder.CreateSExt(rhs_value, int64Type);
    const char* errorstr = "bigint out of range";

    llvm::Function* func_ssub_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_ssub_with_overflow, Intrinsic_Tys);
    if (func_ssub_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::ssub_with_overflow function!\n")));
    }

    llvm::Value* res = builder.CreateCall(func_ssub_overflow, {lhs_value, rhs_value}, "ssub");
    llvm::Value* flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(flag, overflow_bb, normal_bb);
    builder.SetInsertPoint(overflow_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(normal_bb);
    res2 = builder.CreateExtractValue(res, 0);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, overflow_bb);
    Phi->addIncoming(res2, normal_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int84mi);
    return jitted_int84mi;
}

/**
 * @Description : Generate IR function to codegen int48mul.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int48mul_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Type* Intrinsic_Tys[] = {int64Type};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int48mul", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int48mul = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(overflow_bb, jitted_int48mul);
    DEFINE_BLOCK(normal_bb, jitted_int48mul);
    DEFINE_BLOCK(ret_bb, jitted_int48mul);

    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    lhs_value = builder.CreateSExt(lhs_value, int64Type);
    const char* errorstr = "bigint out of range";

    llvm::Function* func_smul_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_smul_with_overflow, Intrinsic_Tys);
    if (func_smul_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::smul_with_overflow function!\n")));
    }

    llvm::Value* res = builder.CreateCall(func_smul_overflow, {lhs_value, rhs_value}, "smul");
    llvm::Value* flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(flag, overflow_bb, normal_bb);
    builder.SetInsertPoint(overflow_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(normal_bb);
    res2 = builder.CreateExtractValue(res, 0);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, overflow_bb);
    Phi->addIncoming(res2, normal_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int48mul);
    return jitted_int48mul;
}

/**
 * @Description : Generate IR function to codegen int84mul.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int84mul_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Type* Intrinsic_Tys[] = {int64Type};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int84mul", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int84mul = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(overflow_bb, jitted_int84mul);
    DEFINE_BLOCK(normal_bb, jitted_int84mul);
    DEFINE_BLOCK(ret_bb, jitted_int84mul);

    rhs_value = builder.CreateTrunc(rhs_value, int32Type);
    rhs_value = builder.CreateSExt(rhs_value, int64Type);
    const char* errorstr = "bigint out of range";

    llvm::Function* func_smul_overflow =
        llvm::Intrinsic::getDeclaration(mod, llvm_smul_with_overflow, Intrinsic_Tys);
    if (func_smul_overflow == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Cannot get the llvm::Intrinsic::smul_with_overflow function!\n")));
    }

    llvm::Value* res = builder.CreateCall(func_smul_overflow, {lhs_value, rhs_value}, "smul");
    llvm::Value* flag = builder.CreateExtractValue(res, 1);
    builder.CreateCondBr(flag, overflow_bb, normal_bb);
    builder.SetInsertPoint(overflow_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(normal_bb);
    res2 = builder.CreateExtractValue(res, 0);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, overflow_bb);
    Phi->addIncoming(res2, normal_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int84mul);
    return jitted_int84mul;
}

/**
 * @Description : Generate IR function to codegen int48div.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int48div_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(float8Type, FLOAT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int48div", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int48div = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(zero_bb, jitted_int48div);
    DEFINE_BLOCK(nonzero_bb, jitted_int48div);
    DEFINE_BLOCK(ret_bb, jitted_int48div);

    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    lhs_value = builder.CreateSExt(lhs_value, int64Type);
    const char* errorstr = "division by zero";

    llvm::Value* tmp = builder.CreateICmpEQ(rhs_value, Datum_0, "decide");
    builder.CreateCondBr(tmp, zero_bb, nonzero_bb);
    builder.SetInsertPoint(nonzero_bb);
    lhs_value = builder.CreateSIToFP(lhs_value, float8Type);
    rhs_value = builder.CreateSIToFP(rhs_value, float8Type);
    res2 = builder.CreateFDiv(lhs_value, rhs_value, "int48_div");
    res2 = builder.CreateBitCast(res2, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(zero_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, zero_bb);
    Phi->addIncoming(res2, nonzero_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int48div);
    return jitted_int48div;
}

/**
 * @Description : Generate IR function to codegen int84div.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* int84div_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(float8Type, FLOAT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[2];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int84div", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_int84div = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    DEFINE_BLOCK(zero_bb, jitted_int84div);
    DEFINE_BLOCK(nonzero_bb, jitted_int84div);
    DEFINE_BLOCK(ret_bb, jitted_int84div);

    rhs_value = builder.CreateTrunc(rhs_value, int32Type);
    rhs_value = builder.CreateSExt(rhs_value, int64Type);
    const char* errorstr = "division by zero";

    llvm::Value* tmp = builder.CreateICmpEQ(rhs_value, Datum_0, "decide");
    builder.CreateCondBr(tmp, zero_bb, nonzero_bb);
    builder.SetInsertPoint(nonzero_bb);
    lhs_value = builder.CreateSIToFP(lhs_value, float8Type);
    rhs_value = builder.CreateSIToFP(rhs_value, float8Type);
    res2 = builder.CreateFDiv(lhs_value, rhs_value, "int84_div");
    res2 = builder.CreateBitCast(res2, int64Type);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(zero_bb);
    res1 = Datum_0;
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi = builder.CreatePHI(int64Type, 2);
    Phi->addIncoming(res1, zero_bb);
    Phi->addIncoming(res2, nonzero_bb);
    (void)builder.CreateRet(Phi);

    llvmCodeGen->FinalizeFunction(jitted_int84div);
    return jitted_int84div;
}
}  // namespace dorado
