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
 * datecodegen.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/codegenutil/datecodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/datecodegen.h"
#include "codegen/gscodegen.h"

#include "catalog/pg_type.h"

namespace dorado {
/**
 * @Description : Generate IR function to codegen date_eq.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* date_eq_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[2];
    llvm::Value* lhs_value = NULL;
    llvm::Value* rhs_value = NULL;
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_dateeq", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_dateeq = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_value = llvmargs[0];
    rhs_value = llvmargs[1];

    /* Truncate the arguments to int32 to against DateADT */
    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    rhs_value = builder.CreateTrunc(rhs_value, int32Type);

    /*
     * Call LLVM API builder function to do equal comparision.
     * CreateICmpEQ is used to compare two integers and the return
     * type only accounts for one bit. We need to extend it to int64Type.
     */
    result = builder.CreateICmpEQ(lhs_value, rhs_value, "date_eq");
    result = builder.CreateZExt(result, int64Type);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_dateeq);
    return jitted_dateeq;
}

/**
 * @Description : Generate IR function to codegen date_eq.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* date_ne_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_datene", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_datene = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    /* Truncate the arguments to int32 to against DateADT */
    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    rhs_value = builder.CreateTrunc(rhs_value, int32Type);

    /*
     * Call LLVM API builder function to do unequal comparision.
     * CreateICmpNE is used to compare two integers and the return
     * type only accounts for one bit. We need to extend it to int64Type.
     */
    result = builder.CreateICmpNE(lhs_value, rhs_value, "date_ne");
    result = builder.CreateZExt(result, int64Type);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_datene);
    return jitted_datene;
}

/**
 * @Description : Generate IR function to codegen date_ne.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* date_lt_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_datelt", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_datelt = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    /* Truncate the arguments to int32 to against DateADT */
    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    rhs_value = builder.CreateTrunc(rhs_value, int32Type);

    /* Call LLVM API builder function to do less than comparision.
     * CreateICmpSLT is used to compare two integers and the return
     * type only accounts for one bit. We need to extend it to int64Type.
     */
    result = builder.CreateICmpSLT(lhs_value, rhs_value, "date_lt");
    result = builder.CreateZExt(result, int64Type);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_datelt);
    return jitted_datelt;
}

/**
 * @Description : Generate IR function to codegen date_le.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* date_le_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_datele", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_datele = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    /* Truncate the arguments to int32 to against DateADT */
    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    rhs_value = builder.CreateTrunc(rhs_value, int32Type);

    /* Call LLVM API builder function to do less than or equal comparision.
     * CreateICmpSLE is used to compare two integers and the return
     * type only accounts for one bit. We need to extend it to int64Type.
     */
    result = builder.CreateICmpSLE(lhs_value, rhs_value, "date_le");
    result = builder.CreateZExt(result, int64Type);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_datele);
    return jitted_datele;
}

/**
 * @Description : Generate IR function to codegen date_gt.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* date_gt_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_dategt", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_dategt = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    /* Truncate the arguments to int32 to against DateADT */
    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    rhs_value = builder.CreateTrunc(rhs_value, int32Type);

    /* Call LLVM API builder function to do great than comparision.
     * CreateICmpSGT is used to compare two integers and the return
     * type only accounts for one bit. We need to extend it to int64Type.
     */
    result = builder.CreateICmpSGT(lhs_value, rhs_value, "date_gt");
    result = builder.CreateZExt(result, int64Type);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_dategt);
    return jitted_dategt;
}

/**
 * @Description : Generate IR function to codegen date_ge.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* date_ge_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_datege", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_datege = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    /* Truncate the arguments to int32 to against DateADT */
    lhs_value = builder.CreateTrunc(lhs_value, int32Type);
    rhs_value = builder.CreateTrunc(rhs_value, int32Type);

    /* Call LLVM API builder function to do great than or equal comparision.
     * CreateICmpNE is used to compare two integers and the return
     * type only accounts for one bit. We need to extend it to int64Type.
     */
    result = builder.CreateICmpSGE(lhs_value, rhs_value, "date_ge");
    result = builder.CreateZExt(result, int64Type);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_datege);
    return jitted_datege;
}
}  // namespace dorado
