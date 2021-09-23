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
 *  varlenacodegen.cpp
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/codegen/codegenutil/varlenacodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/builtinscodegen.h"
#include "utils/builtins.h"

namespace dorado {
/**
 * @Description : Warp up the function which is used to convert
 *				  cstring to text with len.
 * @in len		: The length of cstring.
 * @in data		: The actual context of cstring.
 * @return		: datum representation for a pointer.
 */
Datum WrapVarlenaGetDatum(int len, const char* data)
{
    text* result = (text*)palloc(len + VARHDRSZ);

    SET_VARSIZE(result, len + VARHDRSZ);
    if (len > 0) {
        errno_t rc = memcpy_s(VARDATA(result), len, data, len);
        securec_check(rc, "\0", "\0");
    }
    return PointerGetDatum(result);
}

/**
 * @Description	: Define the IR function to convert a var-len data
 *				  to a {i32, i8*} struct data, which could be easily
 *				  dealed with in LLVM. The first element represents
 *				  the length of this string, the second element
 *				  represents the actural context.
 * @return		: LLVM IR Function
 */
llvm::Function* VarlenaCvtCodeGen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* define data type and variables */
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_ARRTYPE(int8ArrType, int8Type, 1);

    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_4, 4);

    llvm::Value* llvmargs[1];

    /* define the data type corresponding to varattrib_1b and varattrib_4b in PG */
    llvm::Type* Elements[] = {int8Type, int8ArrType};
    llvm::Type* varattrib_1bType = llvm::StructType::create(context, Elements, "varattrib_1b");
    DEFINE_CG_PTRTYPE(varattrib_1bPtrType, varattrib_1bType);
    Elements[0] = int32Type;
    llvm::Type* varattrib_4bType = llvm::StructType::create(context, Elements, "varattrib_4b");
    DEFINE_CG_PTRTYPE(varattrib_4bPtrType, varattrib_4bType);
    Elements[1] = int8PtrType;
    llvm::Type* varattribType = llvm::StructType::create(context, Elements, "varattrib");

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedEvalVarlena", varattribType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", int64Type));
    llvm::Function* jitted_varlena = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* arg = llvmargs[0];

    /* Basic blocks in the generated function */
    llvm::BasicBlock* entry = &jitted_varlena->getEntryBlock();
    llvm::BasicBlock* if_then = llvm::BasicBlock::Create(context, "if_then", jitted_varlena);
    llvm::BasicBlock* if_else = llvm::BasicBlock::Create(context, "if_else", jitted_varlena);
    llvm::BasicBlock* if_end = llvm::BasicBlock::Create(context, "if_end", jitted_varlena);

    builder.SetInsertPoint(entry);
    llvm::Value* Val0 = builder.CreateIntToPtr(arg, varattrib_1bPtrType);
    llvm::Value* argVals2[] = {int64_0, int32_0};
    llvm::Value* Val_header = builder.CreateInBoundsGEP(Val0, argVals2);
    llvm::Value* Val1 = builder.CreateLoad(int8Type, Val_header);

    llvm::Value* Val_conv = builder.CreateZExt(Val1, int32Type);
    Val1 = builder.CreateAnd(Val_conv, 1);
    /* to check it is 1-byte header or 4-byte header */
    llvm::Value* Val_cmp = builder.CreateICmpEQ(Val1, int32_0);
    builder.CreateCondBr(Val_cmp, if_else, if_then);

    /* 1-byte header */
    builder.SetInsertPoint(if_then);
    llvm::Value* argVals3[] = {int64_0, int32_1, int64_0};
    llvm::Value* data1 = builder.CreateInBoundsGEP(Val0, argVals3);
    llvm::Value* len1 = builder.CreateLShr(Val_conv, 1);
    len1 = builder.CreateSub(len1, int32_1);
    builder.CreateBr(if_end);

    /* 4-byte header */
    builder.SetInsertPoint(if_else);
    Val0 = builder.CreateIntToPtr(arg, varattrib_4bPtrType);
    llvm::Value* data2 = builder.CreateInBoundsGEP(Val0, argVals3);
    Val_header = builder.CreateInBoundsGEP(Val0, argVals2);
    llvm::Value* len2 = builder.CreateLoad(int32Type, Val_header);
    len2 = builder.CreateLShr(len2, 2);
    len2 = builder.CreateSub(len2, int32_4);
    builder.CreateBr(if_end);

    /*
     * get the length and the actual context according to the
     * different header type.
     */
    builder.SetInsertPoint(if_end);
    llvm::PHINode* Phi_len = builder.CreatePHI(int32Type, 2);
    Phi_len->addIncoming(len1, if_then);
    Phi_len->addIncoming(len2, if_else);
    llvm::PHINode* Phi_data = builder.CreatePHI(int8PtrType, 2);
    Phi_data->addIncoming(data1, if_then);
    Phi_data->addIncoming(data2, if_else);

    /*
     * Since varattribType is not an original structure data type in LLVM,
     * we should define the return value as an undefined value, and then
     * fill each element.
     * Note : Undefined values are useful because they indicate
     * to the compiler that the program is well defined no matter what
     * value is used. This gives the compiler more freedom to optimize.
     */
    llvm::Value* ret_result = llvm::UndefValue::get(varattribType);
    ret_result = builder.CreateInsertValue(ret_result, Phi_len, 0);
    ret_result = builder.CreateInsertValue(ret_result, Phi_data, 1);

    (void)builder.CreateRet(ret_result);

    llvmCodeGen->FinalizeFunction(jitted_varlena);
    return jitted_varlena;
}

/**
 * @Description	: Convert a {i32, i8*} type data to Datum.
 * 				  When we arrived at a {i32, i8*} type after all instructions,
 *				  we should turn back to a Datum.
 * @in ptrbuilder	: LLVM builder structure used to call the IR function.
 * @in len		: The length of the string data
 * @in data		: The actual context of the string data
 * @return		: A llvm value with the data type int64Type, which is
 *				  corresponding to datum representation for a pointer.
 */
llvm::Value* VarlenaGetDatumCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* len, llvm::Value* data)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);

    llvm::Value* result = NULL;

    llvm::Function* jitted_vargetdatum = llvmCodeGen->module()->getFunction("LLVMIRVarlenaGetDatum");
    if (jitted_vargetdatum == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMIRVarlenaGetDatum", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("strlen", int32Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("strdata", int8PtrType));
        jitted_vargetdatum = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRVarlenaGetDatum", (void*)WrapVarlenaGetDatum);
    }

#ifdef __aarch64__
    len = ptrbuilder->CreateTrunc(len, ptrbuilder->getInt32Ty());
    data = ptrbuilder->CreateIntToPtr(data, ptrbuilder->getInt8PtrTy());
#endif
    result = ptrbuilder->CreateCall(jitted_vargetdatum, {len, data});
    return result;
}

/**
 * @Description : Generate IR function to codegen texteq.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* texteq_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[2];
    llvm::Value* lhs_arg = NULL;
    llvm::Value* rhs_arg = NULL;
    llvm::Value* result = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_texteq", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    llvm::Function* jitted_texteq = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_arg = llvmargs[0];
    rhs_arg = llvmargs[1];

    llvm::Value* len1 = NULL;
    llvm::Value* len2 = NULL;
    llvm::Value* data1 = NULL;
    llvm::Value* data2 = NULL;

    /*
     * Convert args to {i32, i8*} type to get the length and the real value.
     * The args may come from a const or come from a Var/RelabelType Node.
     * The arg of RelabelType should be a Var with type varchar.
     */
    llvm::Function* func_varlena = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
    if (func_varlena == NULL) {
        func_varlena = VarlenaCvtCodeGen();
    }
    llvm::Value* lhs_val = builder.CreateCall(func_varlena, lhs_arg, "lval");
    len1 = builder.CreateExtractValue(lhs_val, 0);
    data1 = builder.CreateExtractValue(lhs_val, 1);
    llvm::Value* rhs_val = builder.CreateCall(func_varlena, rhs_arg, "rval");
    len2 = builder.CreateExtractValue(rhs_val, 0);
    data2 = builder.CreateExtractValue(rhs_val, 1);

    /*
     * Since we codegen texteq in favor of clang, the IR function is stored
     * in an IR file. After we load the IR file, we could load the IR function
     * from module.
     */
    llvm::Function* func_texteq_cc = llvmCodeGen->module()->getFunction("LLVMIRtexteq");
    if (func_texteq_cc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Failed on getting IR function : LLVMIRtexteq!\n")));
    }
    result = builder.CreateCall(func_texteq_cc, {len1, data1, len2, data2}, "texteq");

    builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_texteq);
    return jitted_texteq;
}

/**
 * @Description : Generate IR function to codegen textlt.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* textlt_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[3];
    llvm::Value* lhs_arg = NULL;
    llvm::Value* rhs_arg = NULL;
    llvm::Value* collid = NULL;
    llvm::Value* result = NULL;
    llvm::Value* len1 = NULL;
    llvm::Value* len2 = NULL;
    llvm::Value* data1 = NULL;
    llvm::Value* data2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_textlt", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("collation", int32Type));
    llvm::Function* jitted_textlt = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_arg = llvmargs[0];
    rhs_arg = llvmargs[1];
    collid = llvmargs[2];

    /*
     * Convert args to {i32, i8*} type to get the length and the real value.
     * The args may come from a const or come from a Var/RelabelType Node.
     * The arg of RelabelType should be a Var with type varchar.
     */
    llvm::Function* func_varlena = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
    if (func_varlena == NULL) {
        func_varlena = VarlenaCvtCodeGen();
    }
    llvm::Value* lhs_val = builder.CreateCall(func_varlena, lhs_arg, "lval");
    len1 = builder.CreateExtractValue(lhs_val, 0);
    data1 = builder.CreateExtractValue(lhs_val, 1);
    llvm::Value* rhs_val = builder.CreateCall(func_varlena, rhs_arg, "rval");
    len2 = builder.CreateExtractValue(rhs_val, 0);
    data2 = builder.CreateExtractValue(rhs_val, 1);

    /*
     * Since we codegen textlt in favor of clang, the IR function is stored
     * in an IR file. After we load the IR file, we could load the IR function
     * from module.
     */
    llvm::Function* func_textlt_cc = llvmCodeGen->module()->getFunction("LLVMIRtextlt");
    if (func_textlt_cc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Failed on getting IR function : LLVMIRtextlt!\n")));
    }
    result = builder.CreateCall(func_textlt_cc, {len1, data1, len2, data2, collid}, "textlt");
    (void)builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_textlt);
    return jitted_textlt;
}

/**
 * @Description : Generate IR function to codegen textgt.
 *				 'lhs_arg' and 'rhs_arg' are the parameters used by LLVM function.
 * @return      : LLVM IR Function
 */
llvm::Function* textgt_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* llvmargs[3];
    llvm::Value* lhs_arg = NULL;
    llvm::Value* rhs_arg = NULL;
    llvm::Value* collid = NULL;
    llvm::Value* result = NULL;
    llvm::Value* len1 = NULL;
    llvm::Value* len2 = NULL;
    llvm::Value* data1 = NULL;
    llvm::Value* data2 = NULL;

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_textgt", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("lhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("rhs_arg", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("collation", int32Type));
    llvm::Function* jitted_textgt = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    lhs_arg = llvmargs[0];
    rhs_arg = llvmargs[1];
    collid = llvmargs[2];

    /*
     * Convert args to {i32, i8*} type to get the length and the real value.
     * The args may come from a const or come from a Var/RelabelType Node.
     * The arg of RelabelType should be a Var with type varchar.
     */
    llvm::Function* func_varlena = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
    if (func_varlena == NULL) {
        func_varlena = VarlenaCvtCodeGen();
    }
    llvm::Value* lhs_val = builder.CreateCall(func_varlena, lhs_arg, "lval");
    len1 = builder.CreateExtractValue(lhs_val, 0);
    data1 = builder.CreateExtractValue(lhs_val, 1);
    llvm::Value* rhs_val = builder.CreateCall(func_varlena, rhs_arg, "rval");
    len2 = builder.CreateExtractValue(rhs_val, 0);
    data2 = builder.CreateExtractValue(rhs_val, 1);

    /*
     * Since we codegen textgt in favor of clang, the IR function is stored
     * in an IR file. After we load the IR file, we could load the IR function
     * from module.
     */
    llvm::Function* func_textgt_cc = llvmCodeGen->module()->getFunction("LLVMIRtextgt");
    if (func_textgt_cc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Failed on getting IR function : LLVMIRtextgt!\n")));
    }
    result = builder.CreateCall(func_textgt_cc, {len1, data1, len2, data2, collid}, "textgt");

    (void)builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_textgt);
    return jitted_textgt;
}

/**
 * @Description : Generate IR function to codegen substr.
 * @return      : LLVM IR Function
 */
llvm::Function* substr_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
#ifndef __aarch64__
    DEFINE_CGVAR_INT32(int32_0, 0);
#endif

    DEFINE_CGVAR_INT64(INT64_0, 0);
    DEFINE_CGVAR_INT8(null_false, 0);
    DEFINE_CGVAR_INT8(null_true, 1);

    llvm::Value* str = NULL;
    llvm::Value* start = NULL;
    llvm::Value* len = NULL;
    llvm::Value* isNull = NULL;
    llvm::Value* str_len = NULL;
    llvm::Value* str_data = NULL;
    llvm::Value* res_len = NULL;
    llvm::Value* res_data = NULL;
    llvm::Value* llvmargs[4];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Value* result = NULL;

    /*
     * Define llvm function
     * Since we adapt A db's suibstrb(text str, interget start, integer length),
     * we only need to deal with the first arg.
     */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jittedsubstr", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("str", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("start", int32Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("length", int32Type));
    /*
     * in case of A db compatible format we have to prepare a flag to indicate whether
     * result string is NULL, so we add one more parameter.
     */
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        fn_prototype.addArgument(GsCodeGen::NamedVariable("isNull", int8PtrType));

    llvm::Function* jitted_substr = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    str = llvmargs[0];
    start = llvmargs[1];
    len = llvmargs[2];

    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        isNull = llvmargs[3];

    /*
     * Convert the first arg to {i32, i8*} type to get the length and the real value.
     */
    llvm::Function* func_varlena = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
    if (func_varlena == NULL) {
        func_varlena = VarlenaCvtCodeGen();
    }
    llvm::Value* inputstr = builder.CreateCall(func_varlena, str, "str");
    str_len = builder.CreateExtractValue(inputstr, 0);
    str_data = builder.CreateExtractValue(inputstr, 1);

    /* load substring ir function from IR file*/
    llvm::Function* func_substr_cc = NULL;
    int current_encoding = GetDatabaseEncoding();
    func_substr_cc = (current_encoding == PG_SQL_ASCII) ? llvmCodeGen->module()->getFunction("LLVMIRsubstring_ASCII")
                                                        : llvmCodeGen->module()->getFunction("LLVMIRsubstring_UTF8");
    if (func_substr_cc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Failed on getting IR function : LLVMIRsubstring!\n")));
    }

    /*
     * Since the returned type is varchar_type, we should first extract
     * the value and then fill them in the alloced buffer space.
     */
    result = builder.CreateCall(func_substr_cc, {str_len, str_data, start, len}, "substr");
    res_len = builder.CreateExtractValue(result, 0);
    res_data = builder.CreateExtractValue(result, 1);
    /*
     *here we should consider the sql_compatibility setting
     *in case of ORC, we should set isNull to True if res_len == 0;
     *otherwise, just return the result.
     */
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        DEFINE_BLOCK(be_null, jitted_substr);
        DEFINE_BLOCK(bnot_null, jitted_substr);
        DEFINE_BLOCK(ret_bb, jitted_substr);

#ifdef __aarch64__
        llvm::Value* flag = builder.CreateICmpEQ(res_len, INT64_0, "check");
#else
        llvm::Value* flag = builder.CreateICmpEQ(res_len, int32_0, "check");
#endif
        builder.CreateCondBr(flag, be_null, bnot_null);

        builder.SetInsertPoint(be_null);
        builder.CreateStore(null_true, isNull);
        res1 = INT64_0;
        builder.CreateBr(ret_bb);

        builder.SetInsertPoint(bnot_null);
        builder.CreateStore(null_false, isNull);
        res2 = VarlenaGetDatumCodeGen(&builder, res_len, res_data);
        builder.CreateBr(ret_bb);

        builder.SetInsertPoint(ret_bb);
        llvm::PHINode* Phi_ret = builder.CreatePHI(int64Type, 2);
        Phi_ret->addIncoming(res1, be_null);
        Phi_ret->addIncoming(res2, bnot_null);
        (void)builder.CreateRet(Phi_ret);
    } else {
        result = VarlenaGetDatumCodeGen(&builder, res_len, res_data);
        (void)builder.CreateRet(result);
    }

    llvmCodeGen->FinalizeFunction(jitted_substr);
    return jitted_substr;
}

/**
 * @Description : Generate IR function to codegen rtrim1.
 *				  Only need one parameter, since rtrim1 equal
 *				  to rtrim with set fixed as ' '.
 * @return      : LLVM IR Function
 */
llvm::Function* rtrim1_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* define data type and variables */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, BOOLOID);
    DEFINE_CGVAR_INT8(null_false, 0);
    DEFINE_CGVAR_INT8(null_true, 1);
#ifndef __aarch64__
    DEFINE_CGVAR_INT32(int32_0, 0);
#endif

    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* argval = NULL;
    llvm::Value* result = NULL;
    llvm::Value* isNull = NULL;
    llvm::Value* llvmargs[2];

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jittedrtrim1", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("str", int64Type));
    /*
     * in case of A db compatible format we have to prepare a flag to indicate whether
     * result string is NULL, so we add one more parameter.
     */
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        fn_prototype.addArgument(GsCodeGen::NamedVariable("isNull", int8PtrType));

    llvm::Function* jitted_rtrim1 = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    argval = llvmargs[0];
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        isNull = llvmargs[1];

    /* load rtrim1 ir function from module */
    llvm::Function* func_rtrim1_cc = llvmCodeGen->module()->getFunction("LLVMIRrtrim1");
    if (func_rtrim1_cc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Failed on getting IR function : LLVMIRrtrim1!\n")));
    }

    /*
     * Since the returned type is varchar_type, we should first extract
     * the value and then fill them in the alloced buffer space.
     */
    result = builder.CreateCall(func_rtrim1_cc, argval, "rtrim1");
    llvm::Value* res_len = builder.CreateExtractValue(result, 0);
    llvm::Value* res_data = builder.CreateExtractValue(result, 1);

    /*
     *here we should consider the u_sess->attr.attr_sql.sql_compatibility setting
     *in case of ORC, we should set isNull to True if res_len == 0;
     *otherwise, just return the result.
     */
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        DEFINE_BLOCK(be_null, jitted_rtrim1);
        DEFINE_BLOCK(bnot_null, jitted_rtrim1);
        DEFINE_BLOCK(ret_bb, jitted_rtrim1);

#ifdef __aarch64__
        llvm::Value* flag = builder.CreateICmpEQ(res_len, Datum_0, "check");
#else
        llvm::Value* flag = builder.CreateICmpEQ(res_len, int32_0, "check");
#endif
        builder.CreateCondBr(flag, be_null, bnot_null);

        builder.SetInsertPoint(be_null);
        builder.CreateStore(null_true, isNull);
        llvm::Value* res1 = Datum_0;
        builder.CreateBr(ret_bb);

        builder.SetInsertPoint(bnot_null);
        builder.CreateStore(null_false, isNull);
        llvm::Value* res2 = VarlenaGetDatumCodeGen(&builder, res_len, res_data);
        builder.CreateBr(ret_bb);

        builder.SetInsertPoint(ret_bb);
        llvm::PHINode* Phi_ret = builder.CreatePHI(int64Type, 2);
        Phi_ret->addIncoming(res1, be_null);
        Phi_ret->addIncoming(res2, bnot_null);
        (void)builder.CreateRet(Phi_ret);
    } else {
        result = VarlenaGetDatumCodeGen(&builder, res_len, res_data);
        (void)builder.CreateRet(result);
    }

    llvmCodeGen->FinalizeFunction(jitted_rtrim1);
    return jitted_rtrim1;
}

/**
 * @Description : Generate IR function to codegen btrim1.
 *				  Only need one parameter 'str', since btrim1 equal
 *				  to btrim with set fixed as ' '.
 * @return      : LLVM IR Function
 */
llvm::Function* btrim1_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* define data type and variables */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, BOOLOID);

#ifndef __aarch64__
    DEFINE_CGVAR_INT32(int32_0, 0);
#endif

    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT8(null_false, 0);
    DEFINE_CGVAR_INT8(null_true, 1);

    llvm::Value* argval = NULL;
    llvm::Value* result = NULL;
    llvm::Value* isNull = NULL;
    llvm::Value* llvmargs[2];

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jittedbtrim1", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("str", int64Type));
    /*
     * in case of A db compatible format we have to prepare a flag to indicate whether
     * result string is NULL, so we add one more parameter.
     */
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        fn_prototype.addArgument(GsCodeGen::NamedVariable("isNull", int8PtrType));
    llvm::Function* jitted_btrim1 = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    argval = llvmargs[0];
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        isNull = llvmargs[1];

    /* load rtrim1 ir function from IR file*/
    llvm::Function* func_btrim1_cc = llvmCodeGen->module()->getFunction("LLVMIRbtrim1");
    if (func_btrim1_cc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Failed on getting IR function : LLVMIRbtrim1!\n")));
    }

    /*
     * Since the returned type is varchar_type, we should first extract
     * the value and then fill them in the alloced buffer space.
     */
    result = builder.CreateCall(func_btrim1_cc, argval, "btrim1");
    llvm::Value* res_len = builder.CreateExtractValue(result, 0);
    llvm::Value* res_data = builder.CreateExtractValue(result, 1);

    /*
     *here we should consider the u_sess->attr.attr_sql.sql_compatibility setting
     *in case of ORC, we should set isNull to True if res_len == 0;
     *otherwise, just return the result.
     */
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        DEFINE_BLOCK(be_null, jitted_btrim1);
        DEFINE_BLOCK(bnot_null, jitted_btrim1);
        DEFINE_BLOCK(ret_bb, jitted_btrim1);

#ifdef __aarch64__
        llvm::Value* flag = builder.CreateICmpEQ(res_len, Datum_0, "check");
#else
        llvm::Value* flag = builder.CreateICmpEQ(res_len, int32_0, "check");
#endif
        builder.CreateCondBr(flag, be_null, bnot_null);

        builder.SetInsertPoint(be_null);
        builder.CreateStore(null_true, isNull);
        llvm::Value* res1 = Datum_0;
        builder.CreateBr(ret_bb);

        builder.SetInsertPoint(bnot_null);
        builder.CreateStore(null_false, isNull);
        llvm::Value* res2 = VarlenaGetDatumCodeGen(&builder, res_len, res_data);
        builder.CreateBr(ret_bb);

        builder.SetInsertPoint(ret_bb);
        llvm::PHINode* Phi_ret = builder.CreatePHI(int64Type, 2);
        Phi_ret->addIncoming(res1, be_null);
        Phi_ret->addIncoming(res2, bnot_null);
        (void)builder.CreateRet(Phi_ret);
    } else {
        result = VarlenaGetDatumCodeGen(&builder, res_len, res_data);
        (void)builder.CreateRet(result);
    }

    llvmCodeGen->FinalizeFunction(jitted_btrim1);
    return jitted_btrim1;
}

/**
 * @Description : Generate IR function to codegen textlike.
 * @return      : LLVM IR Function
 */
llvm::Function* textlike_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the llvm data types corresponding to database types. */
    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* str = NULL;
    llvm::Value* ptn = NULL;
    llvm::Value* str_len = NULL;
    llvm::Value* str_data = NULL;
    llvm::Value* ptn_len = NULL;
    llvm::Value* ptn_data = NULL;
    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function for textlike(,,,,). */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_textlike", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("str", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("ptn", int64Type));
    llvm::Function* jitted_textlike = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    str = llvmargs[0];
    ptn = llvmargs[1];

    /* Convert the first arg to {i32, i8*} type to get the length and the real value. */
    llvm::Function* func_varlena = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
    if (func_varlena == NULL) {
        func_varlena = VarlenaCvtCodeGen();
    }
    llvm::Value* inputstr1 = builder.CreateCall(func_varlena, str, "str");
    llvm::Value* inputstr2 = builder.CreateCall(func_varlena, ptn, "ptn");
    str_len = builder.CreateExtractValue(inputstr1, 0);
    str_data = builder.CreateExtractValue(inputstr1, 1);
    ptn_len = builder.CreateExtractValue(inputstr2, 0);
    ptn_data = builder.CreateExtractValue(inputstr2, 1);

    /* Load textlike ir function from IR file*/
    llvm::Function* func_textlike_cc = llvmCodeGen->module()->getFunction("LLVMIRtextlike");
    if (func_textlike_cc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Failed on getting IR function : LLVMIRtextlike!\n")));
    }

    result = builder.CreateCall(func_textlike_cc, {str_len, str_data, ptn_len, ptn_data}, "textlike");
    (void)builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_textlike);
    return jitted_textlike;
}

/**
 * @Description : Generate IR function to codegen text_not_like.
 * @return      : LLVM IR Function
 */
llvm::Function* textnlike_codegen()
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the llvm data types corresponding to database types. */
    DEFINE_CG_TYPE(int64Type, INT8OID);

    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::Value* str = NULL;
    llvm::Value* ptn = NULL;
    llvm::Value* str_len = NULL;
    llvm::Value* str_data = NULL;
    llvm::Value* ptn_len = NULL;
    llvm::Value* ptn_data = NULL;
    llvm::Value* llvmargs[2];
    llvm::Value* result = NULL;

    /* Define llvm function for text_not_like(,,,,). */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_textnotlike", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("str", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("ptn", int64Type));
    llvm::Function* jitted_textnlike = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    str = llvmargs[0];
    ptn = llvmargs[1];

    /* Convert the first arg to {i32, i8*} type to get the length and the real value. */
    llvm::Function* func_varlena = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
    if (func_varlena == NULL) {
        func_varlena = VarlenaCvtCodeGen();
    }
    llvm::Value* inputstr1 = builder.CreateCall(func_varlena, str, "str");
    llvm::Value* inputstr2 = builder.CreateCall(func_varlena, ptn, "ptn");
    str_len = builder.CreateExtractValue(inputstr1, 0);
    str_data = builder.CreateExtractValue(inputstr1, 1);
    ptn_len = builder.CreateExtractValue(inputstr2, 0);
    ptn_data = builder.CreateExtractValue(inputstr2, 1);

    /* Load text_not_like ir function from IR file*/
    llvm::Function* func_textnlike_cc = llvmCodeGen->module()->getFunction("LLVMIRtextnotlike");
    if (func_textnlike_cc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Failed on getting IR function : LLVMIRtextnlike!\n")));
    }

    result = builder.CreateCall(func_textnlike_cc, {str_data, str_len, ptn_data, ptn_len}, "textnlike");
    result = builder.CreateICmpNE(result, Datum_1);
    result = builder.CreateZExt(result, int64Type);
    (void)builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_textnlike);
    return jitted_textnlike;
}
}  // namespace dorado
