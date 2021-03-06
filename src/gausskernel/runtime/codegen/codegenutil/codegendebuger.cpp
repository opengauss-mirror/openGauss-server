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
 *  codegendebuger.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/codegenutil/codegendebuger.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/codegendebuger.h"
#include "catalog/pg_type.h"

namespace dorado {
void DebugerCodeGen::CodeGenDebugInfo(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* value, Datum flag)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvm::LLVMContext& context = llvmCodeGen->context();
    llvm::Type* int64Type = llvm::IntegerType::getInt64Ty(context);
    llvm::Value* Val = value;
    llvm::Type* type = value->getType();
    llvm::Value* flagVal = llvm::ConstantInt::get(context, llvm::APInt(64, (long long)flag, true));

    llvm::Function* IRdebugInfo = llvmCodeGen->module()->getFunction("LLVMIRDebugInfo");
    if (IRdebugInfo == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMIRDebugInfo", llvm::Type::getVoidTy(context));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("value", int64Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("flag", int64Type));
        IRdebugInfo = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRDebugInfo", (void*)WrapCodeGenDebuger);
    }

    /* If the type is a pointer type, convert it to integer */
    if (type->isPointerTy()) {
        Val = ptrbuilder->CreatePtrToInt(value, int64Type);
    } else if (type->isIntegerTy()) {
        /*
         * Since the type of parameter in WrapCodeGenDebuger is Datum,
         * we need to make the type of value be int64Type.
         */
        int bits = ((llvm::IntegerType*)type)->getBitWidth();
        if (bits < 64) {
            Val = ptrbuilder->CreateSExt(value, int64Type);
        } else if (bits > 64) {
            Val = ptrbuilder->CreateTrunc(value, int64Type);
        }
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_LLVM), errmsg("Unsupported LLVM debug type!\n")));
    }

    ptrbuilder->CreateCall(IRdebugInfo, {Val, flagVal});
    return;
}

void DebugerCodeGen::CodeGenDebugString(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* value, Datum flag)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();

    /* define value type */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);

    /* convert the flag value to LLVM value */
    DEFINE_CGVAR_INT64(flagVal, flag);

    llvm::Function* IRdebugStr = llvmCodeGen->module()->getFunction("LLVMIRDebugString");
    if (IRdebugStr == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMIRDebugString", llvm::Type::getVoidTy(context));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("value", int8PtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("flag", int64Type));
        IRdebugStr = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRDebugString", (void*)WrapCodeGenString);
    }

    ptrbuilder->CreateCall(IRdebugStr, {value, flagVal});
    return;
}

void DebugerCodeGen::CodeGenElogInfo(GsCodeGen::LlvmBuilder* ptrbuilder, Datum elevel, const char* cvalue)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);

    llvm::Value* Val = llvm::ConstantInt::get(context, llvm::APInt(64, (long long)elevel, true));
    llvm::Value* data = llvm::ConstantInt::get(context, llvm::APInt(64, (uintptr_t)cvalue, true));
    data = builder.CreateIntToPtr(data, int8PtrType);

    llvm::Function* jitted_elog = llvmCodeGen->module()->getFunction("Jitted_simple_elog");
    if (jitted_elog == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_simple_elog", llvm::Type::getVoidTy(context));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("level", int64Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("data", int8PtrType));
        jitted_elog = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("Jitted_simple_elog", (void*)WrapCodeGenElog);
    }
    ptrbuilder->CreateCall(jitted_elog, {Val, data});
    return;
}

void DebugerCodeGen::WrapCodeGenDebuger(Datum value, Datum flag)
{
    ereport(LOG, (errmodule(MOD_LLVM), errmsg("LLVM Debug at Pos%lld : %lld\n", (long long)flag, (long long)value)));
}

void DebugerCodeGen::WrapCodeGenString(char* string, Datum flag)
{
    ereport(LOG, (errmodule(MOD_LLVM), errmsg("LLVM Debug at Pos%lld : %s\n", (long long)flag, string)));
}

void DebugerCodeGen::WrapCodeGenElog(Datum elevel, char* strdata)
{
    switch (elevel) {
        case ERROR:
            ereport(ERROR, (errcode(ERRCODE_CODEGEN_ERROR), errmodule(MOD_LLVM), errmsg("%s", strdata)));
            break;
        case LOG:
            ereport(LOG, (errmodule(MOD_LLVM), errmsg("%s", strdata)));
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_LLVM), errmsg("Unsupported LLVM report type!")));
            break;
    }
}
}  // namespace dorado
