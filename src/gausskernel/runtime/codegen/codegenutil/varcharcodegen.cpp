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
 * varcharcodegen.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/codegenutil/varcharcodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/builtinscodegen.h"

namespace dorado {
/*
 * @Description	: The simple case of pg_mbstrlen_with_len. Since we only support
 *				: ASCII and UTF-8, we could make a clear different between them.
 * @in strlen	: the length of strdata in bytes.
 * @in strdata	: the cstring data.
 * @return 		: the length counted in wchars.
 */
int Wrapmbstrlen(int str_len, char* str_data)
{
    int reslen = 0;

    while (str_len > 0 && *str_data) {
        int l = 0;
        MB_LEN(l, str_data);

        str_len -= l;
        str_data += l;
        reslen++;
    }

    return reslen;
}

/*
 * @Description	: Wrap the 'Wrapmbstrlen' function in LLVM
 * @in ptrbuilder : LLVM builder structure used to call the IR function.
 * @in strlen	: the length of the cstring in LLVM assemble.
 * @in strdata	: the cstring data in LLVM assemble.
 */
llvm::Value* WrapmbstrlenCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* str_len, llvm::Value* str_data)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);

    llvm::Value* result = NULL;

    llvm::Function* jitted_wrapmbstrlen = llvmCodeGen->module()->getFunction("LLVMIRWrapmbstrlen");
    if (jitted_wrapmbstrlen == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMIRWrapmbstrlen", int32Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("str_len", int32Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("str_data", int8PtrType));
        jitted_wrapmbstrlen = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRWrapmbstrlen", (void*)Wrapmbstrlen);
    }
    result = ptrbuilder->CreateCall(jitted_wrapmbstrlen, {str_len, str_data});

    return result;
}

/**
 * @Description : Generate IR function to codegen bpchareq.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* bpchareq_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[2];
    llvm::Value* lhs_arg = NULL;
    llvm::Value* rhs_arg = NULL;
    llvm::Value* result = NULL;
    llvm::Value* str_len1 = NULL;
    llvm::Value* str_data1 = NULL;
    llvm::Value* str_len2 = NULL;
    llvm::Value* str_data2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_bpchareq", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_bpchareq = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_arg = llvmargs[0];
    rhs_arg = llvmargs[1];

    /*
     * Convert args to {i32, i8*} type to get the length and the real value.
     */
    llvm::Function* func_varlena = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
    if (func_varlena == NULL) {
        func_varlena = VarlenaCvtCodeGen();
    }
    llvm::Value* lhs_val = builder.CreateCall(func_varlena, lhs_arg, "lval");
    str_len1 = builder.CreateExtractValue(lhs_val, 0);
    str_data1 = builder.CreateExtractValue(lhs_val, 1);
    llvm::Value* rhs_val = builder.CreateCall(func_varlena, rhs_arg, "rval");
    str_len2 = builder.CreateExtractValue(rhs_val, 0);
    str_data2 = builder.CreateExtractValue(rhs_val, 1);

    /*
     * Since we codegen bpchareq in favor of clang, the IR function is stored
     * in an IR file. After we load the IR file, we could load the IR function
     * from module.
     */
    llvm::Function* func_bpchareq_cc = llvmCodeGen->module()->getFunction("LLVMIRbpchareq");
    if (func_bpchareq_cc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Failed on getting IR function : LLVMIRbpchareq!\n")));
    }

    result = builder.CreateCall(func_bpchareq_cc, {str_len1, str_data1, str_len2, str_data2}, "bpchareq");
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_bpchareq);
    return jitted_bpchareq;
}

/**
 * @Description : Generate IR function to codegen bpcharne.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return		: LLVM IR Function
 */
llvm::Function* bpcharne_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[2];
    llvm::Value* lhs_arg = NULL;
    llvm::Value* rhs_arg = NULL;
    llvm::Value* result = NULL;
    llvm::Value* str_len1 = NULL;
    llvm::Value* str_data1 = NULL;
    llvm::Value* str_len2 = NULL;
    llvm::Value* str_data2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_bpcharne", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_bpcharne = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_arg = llvmargs[0];
    rhs_arg = llvmargs[1];

    /*
     * Convert args to {i32, i8*} type to get the length and the real value.
     */
    llvm::Function* func_varlena = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
    if (func_varlena == NULL) {
        func_varlena = VarlenaCvtCodeGen();
    }
    llvm::Value* lhs_val = builder.CreateCall(func_varlena, lhs_arg, "lval");
    str_len1 = builder.CreateExtractValue(lhs_val, 0);
    str_data1 = builder.CreateExtractValue(lhs_val, 1);
    llvm::Value* rhs_val = builder.CreateCall(func_varlena, rhs_arg, "rval");
    str_len2 = builder.CreateExtractValue(rhs_val, 0);
    str_data2 = builder.CreateExtractValue(rhs_val, 1);

    /*
     * Since we codegen bpcharne in favor of clang, the IR function is stored
     * in an IR file. After we load the IR file, we could load the IR function
     * from module.
     */
    llvm::Function* func_bpcharne_cc = llvmCodeGen->module()->getFunction("LLVMIRbpcharne");
    if (func_bpcharne_cc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Failed on getting IR function : LLVMIRbpcharne!\n")));
    }

    result = builder.CreateCall(func_bpcharne_cc, {str_len1, str_data1, str_len2, str_data2}, "bpcharne");
    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_bpcharne);
    return jitted_bpcharne;
}

/**
 * @Description : Generate IR function to codegen bpcharlen.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* bpcharlen_codegen(int current_encoding)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[1];
    llvm::Value* lhs_arg = NULL;
    llvm::Value* result = NULL;
    llvm::Value* str_len = NULL;
    llvm::Value* str_data = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_bpcharlen", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    llvm::Function* jitted_bpcharlen = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_arg = llvmargs[0];

    /*
     * Convert args to {i32, i8*} type to get the length and the real value.
     */
    llvm::Function* func_varlena = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
    if (func_varlena == NULL) {
        func_varlena = VarlenaCvtCodeGen();
    }
    llvm::Value* lhs_val = builder.CreateCall(func_varlena, lhs_arg, "lval");
    str_len = builder.CreateExtractValue(lhs_val, 0);

    if (current_encoding == PG_UTF8) {
        str_data = builder.CreateExtractValue(lhs_val, 1);
        str_len = WrapmbstrlenCodeGen(&builder, str_len, str_data);
    }

    str_len = builder.CreateZExt(str_len, int64Type);

    result = str_len;
    (void)builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_bpcharlen);
    return jitted_bpcharlen;
}
}  // namespace dorado
