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
 *  timestampcodegen.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/codegenutil/timestampcodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/timestampcodegen.h"

#include "catalog/pg_type.h"

namespace dorado {
/**
 * Description : Generate IR function to codegen timestamp_eq.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * return      : LLVM IR Function
 */
llvm::Function* timestamp_eq_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define LLVM data type and variables */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_timestampeq", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_timestampeq = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    /*
     * The both arguments are int64 type, call LLVM equal comparision function
     * without truncation. Call CreateSelect to choose the result, when tmp is
     * true, return Datum_1, else return Datum_0.
     */
    llvm::Value* tmp = builder.CreateICmpEQ(lhs_value, rhs_value);
    result = builder.CreateSelect(tmp, Datum_1, Datum_0);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_timestampeq);
    return jitted_timestampeq;
}

/**
 * Description : Generate IR function to codegen timestamp_ne.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * return      : LLVM IR Function
 */
llvm::Function* timestamp_ne_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define LLVM data type and variables */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_timestampne", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_timestampne = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    /*
     * The both arguments are int64 type, call LLVM unequal comparision function
     * without truncation. Call CreateSelect to choose the result, when tmp is
     * true, return Datum_1, else return Datum_0.
     */
    llvm::Value* tmp = builder.CreateICmpNE(lhs_value, rhs_value);
    result = builder.CreateSelect(tmp, Datum_1, Datum_0);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_timestampne);
    return jitted_timestampne;
}

/**
 * Description : Generate IR function to codegen timestamp_lt.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * return      : LLVM IR Function
 */
llvm::Function* timestamp_lt_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define LLVM data type and variables */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_timestamplt", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_timestamplt = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    /*
     * The both arguments are int64 type, call LLVM less than comparision function
     * without truncation. Call CreateSelect to choose the result, when tmp is
     * true, return Datum_1, else return Datum_0.
     */
    llvm::Value* tmp = builder.CreateICmpSLT(lhs_value, rhs_value);
    result = builder.CreateSelect(tmp, Datum_1, Datum_0);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_timestamplt);
    return jitted_timestamplt;
}

/**
 * Description : Generate IR function to codegen timestamp_le.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * return      : LLVM IR Function
 */
llvm::Function* timestamp_le_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define LLVM data type and variables */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_timestample", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_timestample = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    /*
     * The both arguments are int64 type, call LLVM less than or equal comparision
     * function without truncation. Call CreateSelect to choose the result, when
     * tmp is true, return Datum_1, else return Datum_0.
     */
    llvm::Value* tmp = builder.CreateICmpSLE(lhs_value, rhs_value);
    result = builder.CreateSelect(tmp, Datum_1, Datum_0);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_timestample);
    return jitted_timestample;
}

/**
 * Description : Generate IR function to codegen timestamp_gt.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * return      : LLVM IR Function
 */
llvm::Function* timestamp_gt_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define LLVM data type and variables */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_timestampgt", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_timestampgt = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    /*
     * The both arguments are int64 type, call LLVM greate than comparision
     * function without truncation. Call CreateSelect to choose the result, when
     * tmp is true, return Datum_1, else return Datum_0.
     */
    llvm::Value* tmp = builder.CreateICmpSGT(lhs_value, rhs_value);
    result = builder.CreateSelect(tmp, Datum_1, Datum_0);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_timestampgt);
    return jitted_timestampgt;
}

/**
 * Description : Generate IR function to codegen timestamp_ge.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * return      : LLVM IR Function
 */
llvm::Function* timestamp_ge_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define LLVM data type and variables */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_timestampge", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_timestampge = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* lhs_value = llvmargs[0];
    llvm::Value* rhs_value = llvmargs[1];

    /*
     * The both arguments are int64 type, call LLVM greate than or equal comparision
     * function without truncation. Call CreateSelect to choose the result, when
     * tmp is true, return Datum_1, else return Datum_0.
     */
    llvm::Value* tmp = builder.CreateICmpSGE(lhs_value, rhs_value);
    result = builder.CreateSelect(tmp, Datum_1, Datum_0);
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_timestampge);
    return jitted_timestampge;
}
}  // namespace dorado
