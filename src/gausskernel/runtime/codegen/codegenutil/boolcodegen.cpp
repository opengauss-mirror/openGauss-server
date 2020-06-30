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
 *  boolcodegen.cpp
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/codegenutil/boolcodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/builtinscodegen.h"

namespace dorado {
/**
 * @Description : Generate IR function to codegen booleq.
 *				 'arg1' and 'arg2' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* booleq_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(boolType, BOOLOID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    llvm::Value* llvmargs[2];

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_booleq", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int64Type));
    llvm::Function* jitted_booleq = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* arg1 = llvmargs[0];
    llvm::Value* arg2 = llvmargs[1];

    /*
     * Truncate arguments to INT16 to get the right values and extend
     * the result to INT64, which is corresponding to DATUM in Gauss200.
     */
    arg1 = builder.CreateTrunc(arg1, boolType);
    arg2 = builder.CreateTrunc(arg2, boolType);

    llvm::Value* res = builder.CreateICmpEQ(arg1, arg2, "bool_eq");
    res = builder.CreateZExt(res, int64Type);
    (void)builder.CreateRet(res);

    llvmCodeGen->FinalizeFunction(jitted_booleq);
    return jitted_booleq;
}
}  // namespace dorado
