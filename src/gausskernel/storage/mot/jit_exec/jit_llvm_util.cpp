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
 * -------------------------------------------------------------------------
 *
 * jit_llvm_util.cpp
 *    LLVM JIT utilities.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm_util.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "jit_llvm_util.h"
#include "global.h"
#include "utilities.h"
#include "knl/knl_session.h"
#include "utils/elog.h"
#include "securec.h"

// Global definitions
#define IF_STACK u_sess->mot_cxt.jit_llvm_if_stack
#define LOOP_STACK u_sess->mot_cxt.jit_llvm_loop_stack
#define WHILE_STACK u_sess->mot_cxt.jit_llvm_while_stack
#define DO_WHILE_STACK u_sess->mot_cxt.jit_llvm_do_while_stack
#define FOR_STACK u_sess->mot_cxt.jit_llvm_for_stack
#define SWITCH_CASE_STACK u_sess->mot_cxt.jit_llvm_switch_case_stack
#define TRY_CATCH_STACK u_sess->mot_cxt.jit_llvm_try_catch_stack

namespace llvm_util {
DECLARE_LOGGER(JitUtil, JitExec);

extern const char* LlvmRuntimeFaultToString(int faultCode)
{
    if (JitExec::IsJitRuntimeFaultCode(faultCode)) {
        return JitExec::JitRuntimeFaultToString(faultCode);
    }

    switch (faultCode) {
        case LLVM_FAULT_ACCESS_VIOLATION:
            return "Access violation";

        case LLVM_FAULT_UNHANDLED_EXCEPTION:
            return "Unhandled exception";

        case LLVM_FAULT_RESOURCE_LIMIT:
            return "Resource limit";

        default:
            return "Applicative fault code";
    }
}

extern void OpenCompileFrame(LlvmCodeGenContext* ctx)
{
    ctx->m_currentCompileFrame = new (std::nothrow) CompileFrame(ctx->m_currentCompileFrame);
    if (ctx->m_currentCompileFrame == nullptr) {
        MOT_LOG_TRACE("Failed to allocate compile frame");
    }
}

extern void CloseCompileFrame(LlvmCodeGenContext* ctx)
{
    // cleanup current frame
    if (ctx->m_currentCompileFrame != nullptr) {
        CompileFrame* compileFrame = ctx->m_currentCompileFrame->GetNext();
        delete ctx->m_currentCompileFrame;
        ctx->m_currentCompileFrame = compileFrame;
    }
}

extern void CleanupCompileFrames(LlvmCodeGenContext* ctx)
{
    while (ctx->m_currentCompileFrame != nullptr) {
        CloseCompileFrame(ctx);
    }
}

extern void JitResetCompileState()
{
    u_sess->mot_cxt.jit_llvm_if_stack = nullptr;
    u_sess->mot_cxt.jit_llvm_loop_stack = nullptr;
    u_sess->mot_cxt.jit_llvm_while_stack = nullptr;
    u_sess->mot_cxt.jit_llvm_do_while_stack = nullptr;
    u_sess->mot_cxt.jit_llvm_for_stack = nullptr;
    u_sess->mot_cxt.jit_llvm_switch_case_stack = nullptr;
    u_sess->mot_cxt.jit_llvm_try_catch_stack = nullptr;
}

extern llvm::FunctionCallee DefineFunction(llvm::Module* module, llvm::Type* returnType, const char* name, ...)
{
    va_list vargs;
    va_start(vargs, name);
    std::vector<llvm::Type*> args;
    llvm::Type* argType = va_arg(vargs, llvm::Type*);
    while (argType != nullptr) {
        args.push_back(argType);
        argType = va_arg(vargs, llvm::Type*);
    }
    va_end(vargs);
    llvm::ArrayRef<llvm::Type*> argsRef(args);
    llvm::FunctionType* funcType = llvm::FunctionType::get(returnType, argsRef, false);
    return module->getOrInsertFunction(name, funcType);
}

extern llvm::Value* AddFunctionCall(LlvmCodeGenContext* ctx, llvm::FunctionCallee func, ...)
{
    // prepare arguments
    va_list vargs;
    va_start(vargs, func);
    std::vector<llvm::Value*> args;
    llvm::Value* argValue = va_arg(vargs, llvm::Value*);
    while (argValue != nullptr) {
        args.push_back(argValue);
        argValue = va_arg(vargs, llvm::Value*);
    }
    va_end(vargs);
    llvm::ArrayRef<llvm::Value*> argsRef(args);

    // make the call
    return ctx->m_builder->CreateCall(func, argsRef);
}

extern llvm::Value* InsertFunctionCall(
    llvm::Instruction* before, LlvmCodeGenContext* ctx, llvm::FunctionCallee func, ...)
{
    // find insert position
    llvm::BasicBlock* block = ctx->m_builder->GetInsertBlock();
    llvm::BasicBlock::InstListType& instList = block->getInstList();
    llvm::BasicBlock::iterator itr = block->begin();
    while (itr != block->end()) {
        if (itr == before->getIterator()) {
            break;
        }
        ++itr;
    }
    if (itr == block->end()) {
        MOT_LOG_ERROR("Failed to insert function call before instruction: instruction not found in current block");
        return nullptr;
    }

    // prepare arguments
    va_list vargs;
    va_start(vargs, func);
    std::vector<llvm::Value*> args;
    llvm::Value* argValue = va_arg(vargs, llvm::Value*);
    while (argValue != nullptr) {
        args.push_back(argValue);
        argValue = va_arg(vargs, llvm::Value*);
    }
    va_end(vargs);
    llvm::ArrayRef<llvm::Value*> argsRef(args);

    // prepare the call
    llvm::CallInst* callInst = llvm::CallInst::Create(func, args);

    // an insert the call before the required instruction
    instList.insert(itr, callInst);
    return callInst;
}

extern llvm::Value* AddInvokePGFunction0(LlvmCodeGenContext* ctx, PGFunction funcPtr, Oid collationId)
{
    llvm::ConstantInt* funcValue = llvm::ConstantInt::get(ctx->INT64_T, (int64_t)funcPtr, true);
    llvm::Value* funcPtrValue = llvm::ConstantExpr::getIntToPtr(funcValue, ctx->STR_T);
    return AddFunctionCall(ctx, ctx->m_invokePGFunction0Func, funcPtrValue, JIT_CONST_INT32(collationId), nullptr);
}

extern llvm::Value* AddInvokePGFunction1(LlvmCodeGenContext* ctx, PGFunction funcPtr, Oid collationId, bool isStrict,
    llvm::Value* arg, llvm::Value* isNull, Oid argType)
{
    llvm::ConstantInt* funcValue = llvm::ConstantInt::get(ctx->INT64_T, (int64_t)funcPtr, true);
    llvm::Value* funcPtrValue = llvm::ConstantExpr::getIntToPtr(funcValue, ctx->STR_T);
    return AddFunctionCall(ctx,
        ctx->m_invokePGFunction1Func,
        funcPtrValue,
        JIT_CONST_INT32(collationId),
        JIT_CONST_INT32((isStrict ? 1 : 0)),
        arg,
        isNull,
        JIT_CONST_INT32(argType),
        nullptr);
}

extern llvm::Value* AddInvokePGFunction2(LlvmCodeGenContext* ctx, PGFunction funcPtr, Oid collationId, bool isStrict,
    llvm::Value* arg1, llvm::Value* isNull1, Oid argType1, llvm::Value* arg2, llvm::Value* isNull2, Oid argType2)
{
    llvm::ConstantInt* funcValue = llvm::ConstantInt::get(ctx->INT64_T, (int64_t)funcPtr, true);
    llvm::Value* funcPtrValue = llvm::ConstantExpr::getIntToPtr(funcValue, ctx->STR_T);
    return AddFunctionCall(ctx,
        ctx->m_invokePGFunction2Func,
        funcPtrValue,
        JIT_CONST_INT32(collationId),
        JIT_CONST_INT32((isStrict ? 1 : 0)),
        arg1,
        isNull1,
        JIT_CONST_INT32(argType1),
        arg2,
        isNull2,
        JIT_CONST_INT32(argType2),
        nullptr);
}

extern llvm::Value* AddInvokePGFunction3(LlvmCodeGenContext* ctx, PGFunction funcPtr, Oid collationId, bool isStrict,
    llvm::Value* arg1, llvm::Value* isNull1, Oid argType1, llvm::Value* arg2, llvm::Value* isNull2, Oid argType2,
    llvm::Value* arg3, llvm::Value* isNull3, Oid argType3)
{
    llvm::ConstantInt* funcValue = llvm::ConstantInt::get(ctx->INT64_T, (int64_t)funcPtr, true);
    llvm::Value* funcPtrValue = llvm::ConstantExpr::getIntToPtr(funcValue, ctx->STR_T);
    return AddFunctionCall(ctx,
        ctx->m_invokePGFunction3Func,
        funcPtrValue,
        JIT_CONST_INT32(collationId),
        JIT_CONST_INT32((isStrict ? 1 : 0)),
        arg1,
        isNull1,
        JIT_CONST_INT32(argType1),
        arg2,
        isNull2,
        JIT_CONST_INT32(argType2),
        arg3,
        isNull3,
        JIT_CONST_INT32(argType3),
        nullptr);
}

extern llvm::Value* AddInvokePGFunctionN(LlvmCodeGenContext* ctx, PGFunction funcPtr, Oid collationId, bool isStrict,
    llvm::Value** args, llvm::Value** isNull, Oid* argTypes, int argCount)
{
    llvm::ConstantInt* funcValue = llvm::ConstantInt::get(ctx->INT64_T, (int64_t)funcPtr, true);
    llvm::Value* funcPtrValue = llvm::ConstantExpr::getIntToPtr(funcValue, ctx->STR_T);

    // allocate arrays
    llvm::Value* argArray = AddMemSessionAlloc(ctx, JIT_CONST_INT32(sizeof(Datum) * argCount));
    llvm::Value* nullArray = AddMemSessionAlloc(ctx, JIT_CONST_INT32(sizeof(uint32_t) * argCount));
    llvm::Value* typeArray = AddMemSessionAlloc(ctx, JIT_CONST_INT32(sizeof(uint32_t) * argCount));

    // assign arrays
    for (int i = 0; i < argCount; ++i) {
        llvm::Value* elmIndex = JIT_CONST_INT32(i);
        llvm::Value* elm = ctx->m_builder->CreateGEP(argArray, elmIndex);
        ctx->m_builder->CreateStore(args[i], elm, true);
        elm = ctx->m_builder->CreateGEP(nullArray, elmIndex);
        ctx->m_builder->CreateStore(isNull[i], elm, true);
        elm = ctx->m_builder->CreateGEP(typeArray, elmIndex);
        ctx->m_builder->CreateStore(JIT_CONST_INT32(argTypes[i]), elm, true);
    }

    // make the call
    llvm::Value* result = AddFunctionCall(ctx,
        ctx->m_invokePGFunctionNFunc,
        funcPtrValue,
        JIT_CONST_INT32(collationId),
        JIT_CONST_INT32((isStrict ? 1 : 0)),
        argArray,
        nullArray,
        typeArray,
        JIT_CONST_INT32(argCount),
        nullptr);

    // clean up
    AddMemSessionFree(ctx, argArray);
    AddMemSessionFree(ctx, nullArray);
    AddMemSessionFree(ctx, typeArray);

    return result;
}

extern llvm::Value* AddMemSessionAlloc(LlvmCodeGenContext* ctx, llvm::Value* allocSize)
{
    return AddFunctionCall(ctx, ctx->m_memSessionAllocFunc, allocSize);
}

extern void AddMemSessionFree(LlvmCodeGenContext* ctx, llvm::Value* ptr)
{
    AddFunctionCall(ctx, ctx->m_memSessionFreeFunc, ptr);
}

inline void DefineLlvmPushExceptionFrame(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmPushExceptionFrameFunc =
        DefineFunction(module, ctx->INT8_T->getPointerTo(), "LlvmPushExceptionFrame", nullptr);
}

inline void DefineLlvmGetCurrentExceptionFrame(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmGetCurrentExceptionFrameFunc =
        DefineFunction(module, ctx->INT8_T->getPointerTo(), "LlvmGetCurrentExceptionFrame", nullptr);
}

inline void DefineLlvmPopExceptionFrame(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmPopExceptionFrameFunc = DefineFunction(module, ctx->INT32_T, "LlvmPopExceptionFrame", nullptr);
}

inline void DefineLlvmThrowException(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmThrowExceptionFunc = DefineFunction(module, ctx->VOID_T, "LlvmThrowException", ctx->INT32_T, nullptr);
}

inline void DefineLlvmRethrowException(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmRethrowExceptionFunc = DefineFunction(module, ctx->VOID_T, "LlvmRethrowException", nullptr);
}

inline void DefineLlvmGetExceptionValue(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmGetExceptionValueFunc = DefineFunction(module, ctx->INT32_T, "LlvmGetExceptionValue", nullptr);
}

inline void DefineLlvmGetExceptionStatus(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmGetExceptionStatusFunc = DefineFunction(module, ctx->INT32_T, "LlvmGetExceptionStatus", nullptr);
}

inline void DefineLlvmResetExceptionStatus(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmResetExceptionStatusFunc = DefineFunction(module, ctx->VOID_T, "LlvmResetExceptionStatus", nullptr);
}

inline void DefineLlvmResetExceptionValue(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmResetExceptionValueFunc = DefineFunction(module, ctx->VOID_T, "LlvmResetExceptionValue", nullptr);
}

inline void DefineLlvmUnwindExceptionFrame(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmUnwindExceptionFrameFunc = DefineFunction(module, ctx->VOID_T, "LlvmUnwindExceptionFrame", nullptr);
}

inline void DefineLlvmSetJmp(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmSetJmpFunc = llvm_util::DefineFunction(
        module, ctx->INT32_T, "__sigsetjmp", ctx->INT8_T->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void DefineLlvmLongJmp(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmLongJmpFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "siglongjmp", ctx->INT8_T->getPointerTo(), ctx->INT32_T, nullptr);
}

#ifdef MOT_JIT_DEBUG
inline void DefineLlvmDebugPrint(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmDebugPrintFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "debugLog", ctx->STR_T, ctx->STR_T, nullptr);
}

inline void DefineLlvmDebugPrintFrame(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmDebugPrintFrameFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "LLvmPrintFrame", ctx->STR_T, ctx->INT8_T->getPointerTo(), nullptr);
}
#endif

inline void DefineInvokePGFunction0(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_invokePGFunction0Func = DefineFunction(
        module, ctx->DATUM_T, "JitInvokePGFunction0", ctx->INT8_T->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void DefineInvokePGFunction1(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_invokePGFunction1Func = DefineFunction(module,
        ctx->DATUM_T,
        "JitInvokePGFunction1",
        ctx->INT8_T->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->DATUM_T,
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void DefineInvokePGFunction2(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_invokePGFunction2Func = DefineFunction(module,
        ctx->DATUM_T,
        "JitInvokePGFunction2",
        ctx->INT8_T->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->DATUM_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->DATUM_T,
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void DefineInvokePGFunction3(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_invokePGFunction3Func = DefineFunction(module,
        ctx->DATUM_T,
        "JitInvokePGFunction3",
        ctx->INT8_T->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->DATUM_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->DATUM_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->DATUM_T,
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void DefineInvokePGFunctionN(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_invokePGFunctionNFunc = DefineFunction(module,
        ctx->DATUM_T,
        "JitInvokePGFunctionN",
        ctx->INT8_T->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->DATUM_T->getPointerTo(),
        ctx->INT32_T->getPointerTo(),
        ctx->INT32_T->getPointerTo(),
        ctx->INT32_T,
        nullptr);
}

inline void DefineMemSessionAlloc(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_memSessionAllocFunc =
        DefineFunction(module, ctx->INT8_T->getPointerTo(), "JitMemSessionAlloc", ctx->INT32_T, nullptr);
}

inline void DefineMemSessionFree(LlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_memSessionFreeFunc =
        DefineFunction(module, ctx->VOID_T, "JitMemSessionFree", ctx->INT8_T->getPointerTo(), nullptr);
}

inline llvm::Value* AddLlvmPushExceptionFrame(LlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->m_llvmPushExceptionFrameFunc, nullptr);
}

inline llvm::Value* AddLlvmGetCurrentExceptionFrame(LlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->m_llvmGetCurrentExceptionFrameFunc, nullptr);
}

inline llvm::Value* AddLlvmPopExceptionFrame(LlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->m_llvmPopExceptionFrameFunc, nullptr);
}

inline llvm::Value* AddLlvmThrowException(LlvmCodeGenContext* ctx, llvm::Value* exceptionValue)
{
    return AddFunctionCall(ctx, ctx->m_llvmThrowExceptionFunc, exceptionValue, nullptr);
}

inline llvm::Value* AddLlvmRethrowException(LlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->m_llvmRethrowExceptionFunc, nullptr);
}

inline llvm::Value* AddLlvmGetExceptionValue(LlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->m_llvmGetExceptionValueFunc, nullptr);
}

inline llvm::Value* AddLlvmGetExceptionStatus(LlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->m_llvmGetExceptionStatusFunc, nullptr);
}

inline void AddLlvmResetExceptionStatus(LlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->m_llvmResetExceptionStatusFunc, nullptr);
}

inline void AddLlvmResetExceptionValue(LlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->m_llvmResetExceptionValueFunc, nullptr);
}

inline llvm::Value* AddLlvmUnwindExceptionFrame(LlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->m_llvmUnwindExceptionFrameFunc, nullptr);
}

#ifdef MOT_JIT_DEBUG
inline void AddLlvmDebugPrintImpl(LlvmCodeGenContext* ctx, const char* function, const char* msg)
{
    llvm::Value* functionValue = ctx->m_builder->CreateGlobalStringPtr(function);
    llvm::Value* msgValue = ctx->m_builder->CreateGlobalStringPtr(msg);

    // a debug call may sometimes be emitted after terminator (return, branch, etc.), so we need to inject it before
    // the terminator
    llvm::Instruction* terminator = ctx->m_builder->GetInsertBlock()->getTerminator();
    if (terminator == nullptr) {
        llvm_util::AddFunctionCall(ctx, ctx->m_llvmDebugPrintFunc, functionValue, msgValue, nullptr);
    } else {
        llvm_util::InsertFunctionCall(terminator, ctx, ctx->m_llvmDebugPrintFunc, functionValue, msgValue, nullptr);
    }
}

inline void AddLlvmDebugPrintFrameImpl(LlvmCodeGenContext* ctx, const char* msg, llvm::Value* frame)
{
    llvm::Value* msgValue = ctx->m_builder->CreateGlobalStringPtr(msg);

    // a debug call may sometimes be emitted after terminator (return, branch, etc.), so we need to inject it before
    // the terminator
    llvm::Instruction* terminator = ctx->m_builder->GetInsertBlock()->getTerminator();
    if (terminator == nullptr) {
        llvm_util::AddFunctionCall(ctx, ctx->m_llvmDebugPrintFrameFunc, msgValue, frame, nullptr);
    } else {
        llvm_util::InsertFunctionCall(terminator, ctx, ctx->m_llvmDebugPrintFrameFunc, msgValue, frame, nullptr);
    }
}
#define LLVM_DEBUG_PRINT(ctx, function, msg) AddLlvmDebugPrintImpl(ctx, function, msg)
#define LLVM_DEBUG_PRINT_FRAME(ctx, msg, frame) AddLlvmDebugPrintFrameImpl(ctx, msg, frame)
#else
#define LLVM_DEBUG_PRINT(ctx, function, msg)
#define LLVM_DEBUG_PRINT_FRAME(ctx, msg, frame)
#endif

inline llvm::Value* AddLlvmSetJmp(LlvmCodeGenContext* ctx, llvm::Value* setJumpBuf)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_llvmSetJmpFunc, setJumpBuf, JIT_CONST_INT32(1), nullptr);
}

inline void AddLlvmLongJmp(LlvmCodeGenContext* ctx, llvm::Value* setJumpBuf, llvm::Value* exceptionValue)
{
    llvm_util::AddFunctionCall(ctx, ctx->m_llvmLongJmpFunc, setJumpBuf, exceptionValue, nullptr);
}

extern void InitLlvmCodeGenContext(LlvmCodeGenContext* ctx, GsCodeGen* codeGen, GsCodeGen::LlvmBuilder* builder)
{
    ctx->m_codeGen = codeGen;
    ctx->m_builder = builder;

    llvm::LLVMContext& context = ctx->m_codeGen->context();

    // primitive types
    ctx->INT1_T = llvm::Type::getInt1Ty(context);
    ctx->INT8_T = llvm::Type::getInt8Ty(context);
    ctx->INT16_T = llvm::Type::getInt16Ty(context);
    ctx->INT32_T = llvm::Type::getInt32Ty(context);
    ctx->INT64_T = llvm::Type::getInt64Ty(context);
    ctx->VOID_T = llvm::Type::getVoidTy(context);
    ctx->STR_T = llvm::Type::getInt8Ty(context)->getPointerTo();
    ctx->FLOAT_T = llvm::Type::getFloatTy(context);
    ctx->DOUBLE_T = llvm::Type::getDoubleTy(context);
    ctx->BOOL_T = ctx->INT8_T;
    ctx->DATUM_T = ctx->INT64_T;

    llvm::Module* module = ctx->m_codeGen->module();

    // exception helper functions
    DefineLlvmPushExceptionFrame(ctx, module);
    DefineLlvmGetCurrentExceptionFrame(ctx, module);
    DefineLlvmPopExceptionFrame(ctx, module);
    DefineLlvmThrowException(ctx, module);
    DefineLlvmRethrowException(ctx, module);
    DefineLlvmGetExceptionValue(ctx, module);
    DefineLlvmGetExceptionStatus(ctx, module);
    DefineLlvmResetExceptionStatus(ctx, module);
    DefineLlvmResetExceptionValue(ctx, module);
    DefineLlvmUnwindExceptionFrame(ctx, module);
    DefineLlvmSetJmp(ctx, module);
    DefineLlvmLongJmp(ctx, module);
#ifdef MOT_JIT_DEBUG
    DefineLlvmDebugPrint(ctx, module);
    DefineLlvmDebugPrintFrame(ctx, module);
#endif
    DefineInvokePGFunction0(ctx, module);
    DefineInvokePGFunction1(ctx, module);
    DefineInvokePGFunction2(ctx, module);
    DefineInvokePGFunction3(ctx, module);
    DefineInvokePGFunctionN(ctx, module);

    DefineMemSessionAlloc(ctx, module);
    DefineMemSessionFree(ctx, module);

    ctx->m_currentCompileFrame = nullptr;
}

extern void DestroyLlvmCodeGenContext(LlvmCodeGenContext* ctx)
{
    CleanupCompileFrames(ctx);
    if (ctx->m_codeGen != nullptr) {
        ctx->m_codeGen->releaseResource();
        delete ctx->m_codeGen;
        ctx->m_codeGen = nullptr;
    }
}

char* JitUtils::FormatBlockName(char* buf, size_t size, const char* baseName, const char* suffix)
{
    errno_t erc = snprintf_s(buf, size, size - 1, "%s%s", baseName, suffix);
    securec_check_ss(erc, "\0", "\0");
    return buf;
}

llvm::Value* JitUtils::ExecCompare(
    GsCodeGen::LlvmBuilder* builder, llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp)
{
    llvm::Value* res = nullptr;
    switch (cmpOp) {
        case JitExec::JIT_ICMP_EQ:
            res = builder->CreateICmpEQ(lhs, rhs);
            break;

        case JitExec::JIT_ICMP_NE:
            res = builder->CreateICmpNE(lhs, rhs);
            break;

        case JitExec::JIT_ICMP_GT:
            res = builder->CreateICmpSGT(lhs, rhs);
            break;

        case JitExec::JIT_ICMP_GE:
            res = builder->CreateICmpSGE(lhs, rhs);
            break;

        case JitExec::JIT_ICMP_LT:
            res = builder->CreateICmpSLT(lhs, rhs);
            break;

        case JitExec::JIT_ICMP_LE:
            res = builder->CreateICmpSLE(lhs, rhs);
            break;

        default:
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Execute Pseudo-JIT Function", "Invalid compare operator %d", (int)cmpOp);
            MOT_ASSERT(false);
            return nullptr;
    }
    return res;
}

llvm::Value* JitUtils::GetTypedZero(const llvm::Value* value)
{
    llvm::Value* typedZero = nullptr;
    if (value->getType()->isPointerTy()) {
        typedZero = llvm::ConstantPointerNull::get((llvm::PointerType*)value->getType());
    } else {
        typedZero = llvm::ConstantInt::get(value->getType(), 0, true);
    }
    return typedZero;
}

JitStatement::JitStatement(LlvmCodeGenContext* codegenContext)
    : m_codegenContext(codegenContext),
      m_context(m_codegenContext->m_codeGen->context()),
      m_builder(m_codegenContext->m_builder)
{}

JitStatement::~JitStatement()
{
    m_codegenContext = nullptr;
    m_builder = nullptr;
}

llvm::BasicBlock* JitStatement::MakeBlock(const char* blockBaseName, const char* suffix)
{
    constexpr size_t bufSize = 64;
    char buf[bufSize];
    return llvm::BasicBlock::Create(
        m_context, JitUtils::FormatBlockName(buf, bufSize, blockBaseName, suffix), m_codegenContext->m_jittedFunction);
}

void JitStatement::EvalCmp(llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp, llvm::BasicBlock* successBlock,
    llvm::BasicBlock* failBlock)
{
    // evaluate condition and start if-true block
    llvm::Value* res = JitUtils::ExecCompare(m_builder, lhs, rhs, cmpOp);
    m_builder->CreateCondBr(res, successBlock, failBlock);
    m_builder->SetInsertPoint(successBlock);
}

void JitStatement::Eval(llvm::Value* value, llvm::BasicBlock* successBlock, llvm::BasicBlock* failBlock)
{
    llvm::Value* typedZero = JitUtils::GetTypedZero(value);
    EvalCmp(value, typedZero, JitExec::JIT_ICMP_NE, successBlock, failBlock);
}

void JitStatement::EvalNot(llvm::Value* value, llvm::BasicBlock* successBlock, llvm::BasicBlock* failBlock)
{
    llvm::Value* typedZero = JitUtils::GetTypedZero(value);
    EvalCmp(value, typedZero, JitExec::JIT_ICMP_EQ, successBlock, failBlock);
}

bool JitStatement::CurrentBlockEndsInBranch()
{
    bool result = false;
    llvm::BasicBlock* currentBlock = m_builder->GetInsertBlock();
    if (!currentBlock->empty()) {
        result = currentBlock->back().isTerminator();
    }
    return result;
}

JitIf::JitIf(LlvmCodeGenContext* codegenContext, const char* blockName)
    : JitStatement(codegenContext), m_elseBlockUsed(false)
{
    m_ifBlock = MakeBlock(blockName, "_if_exec_bb");
    m_elseBlock = MakeBlock(blockName, "_else_exec_bb");
    m_postIfBlock = MakeBlock(blockName, "_post_if_bb");

    // push this object on top of the compile-time if-stack
    m_next = IF_STACK;
    IF_STACK = this;
}

JitIf::~JitIf()
{
    // pop this object from the compile-time if-stack
    IF_STACK = IF_STACK->m_next;
    m_ifBlock = nullptr;
    m_elseBlock = nullptr;
    m_postIfBlock = nullptr;
    m_next = nullptr;
}

JitIf* JitIf::GetCurrentStatement()
{
    return IF_STACK;
}

void JitIf::JitIfCmp(llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp)
{
    EvalCmp(lhs, rhs, cmpOp, m_ifBlock, m_elseBlock);
    OpenCompileFrame(m_codegenContext);  // open if-block compile frame
}

void JitIf::JitIfEval(llvm::Value* value)
{
    Eval(value, m_ifBlock, m_elseBlock);
    OpenCompileFrame(m_codegenContext);  // open if-block compile frame
}

void JitIf::JitIfNot(llvm::Value* value)
{
    EvalNot(value, m_ifBlock, m_elseBlock);
    OpenCompileFrame(m_codegenContext);  // open if-block compile frame
}

void JitIf::JitElse()
{
    // end current block first (jump to post-if point), and then start else block
    // attention: current block might be something else than the if-true block
    if (!CurrentBlockEndsInBranch()) {
        m_builder->CreateBr(m_postIfBlock);
    }
    CloseCompileFrame(m_codegenContext);  // close if-block compile frame
    m_builder->SetInsertPoint(m_elseBlock);
    OpenCompileFrame(m_codegenContext);  // open else-block compile frame
    m_elseBlockUsed = true;
}

void JitIf::JitEnd()
{
    // end current block (if not already ended)
    // attention: current block might be something else than the if-true or if-false blocks
    if (!CurrentBlockEndsInBranch()) {
        m_builder->CreateBr(m_postIfBlock);
    }
    CloseCompileFrame(m_codegenContext);  // close recent compile frame

    if (!m_elseBlockUsed) {
        // user did not use else block, so we must generate an empty one, otherwise it will be removed
        // during function finalization (since it is empty), and if the condition evaluation fails, then
        // during run-time we will jump to a block that was deleted (core dump)
        m_builder->SetInsertPoint(m_elseBlock);
        m_builder->CreateBr(m_postIfBlock);
    }

    // now start the post-if block
    m_builder->SetInsertPoint(m_postIfBlock);
}

JitLoop::JitLoop(LlvmCodeGenContext* codegenContext, const char* name) : JitStatement(codegenContext), m_name(name)
{
    // push this object on top of the compile-time loop-stack
    m_next = LOOP_STACK;
    LOOP_STACK = this;
}

JitLoop::~JitLoop()
{
    // pop this object from the compile-time loop-stack
    LOOP_STACK = LOOP_STACK->m_next;
}

JitLoop* JitLoop::GetCurrentStatement()
{
    return LOOP_STACK;
}

JitWhile::JitWhile(LlvmCodeGenContext* codegenContext, const char* blockName) : JitLoop(codegenContext, blockName)
{
    m_condWhileBlock = MakeBlock(blockName, "_cond_while_bb");
    m_execWhileBlock = MakeBlock(blockName, "_exec_while_bb");
    m_postWhileBlock = MakeBlock(blockName, "_post_while_bb");

    // we end current block and start the condition evaluation block
    m_builder->CreateBr(m_condWhileBlock);
    m_builder->SetInsertPoint(m_condWhileBlock);

    // push this object on top of the compile-time while-stack
    m_next = WHILE_STACK;
    WHILE_STACK = this;
}

JitWhile::~JitWhile()
{
    // pop this object from the compile-time while-stack
    WHILE_STACK = WHILE_STACK->m_next;
    m_condWhileBlock = nullptr;
    m_execWhileBlock = nullptr;
    m_postWhileBlock = nullptr;
    m_next = nullptr;
}

JitWhile* JitWhile::GetCurrentStatement()
{
    return WHILE_STACK;
}

void JitWhile::JitWhileCmp(llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp)
{
    // evaluate condition and start while exec block
    EvalCmp(lhs, rhs, cmpOp, m_execWhileBlock, m_postWhileBlock);
    OpenCompileFrame(m_codegenContext);  // open while-block compile frame
}

void JitWhile::JitWhileEval(llvm::Value* value)
{
    Eval(value, m_execWhileBlock, m_postWhileBlock);
    OpenCompileFrame(m_codegenContext);  // open while-block compile frame
}

void JitWhile::JitWhileNot(llvm::Value* value)
{
    EvalNot(value, m_execWhileBlock, m_postWhileBlock);
    OpenCompileFrame(m_codegenContext);  // open while-block compile frame
}

void JitWhile::JitContinue()
{
    // jump to test evaluation block
    m_builder->CreateBr(m_condWhileBlock);
}

void JitWhile::JitBreak()
{
    // jump to post while block
    m_builder->CreateBr(m_postWhileBlock);
}

void JitWhile::JitEnd()
{
    // insert instruction to jump back to test loop and begin code after loop
    m_builder->CreateBr(m_condWhileBlock);
    CloseCompileFrame(m_codegenContext);  // close while-block compile frame
    m_builder->SetInsertPoint(m_postWhileBlock);
}

JitDoWhile::JitDoWhile(LlvmCodeGenContext* codegenContext, const char* blockName) : JitLoop(codegenContext, blockName)
{
    m_execWhileBlock = MakeBlock(blockName, "_exec_while_bb");
    m_condWhileBlock = MakeBlock(blockName, "_cond_while_bb");
    m_postWhileBlock = MakeBlock(blockName, "_post_while_bb");

    // we end current block and start the do-while execution block
    m_builder->CreateBr(m_execWhileBlock);
    m_builder->SetInsertPoint(m_execWhileBlock);
    OpenCompileFrame(m_codegenContext);  // open do-while-block compile frame

    // push this object on top of the compile-time do-while-stack
    m_next = DO_WHILE_STACK;
    DO_WHILE_STACK = this;
}

JitDoWhile::~JitDoWhile()
{
    // pop this object from the compile-time do-while-stack
    DO_WHILE_STACK = DO_WHILE_STACK->m_next;
    m_execWhileBlock = nullptr;
    m_condWhileBlock = nullptr;
    m_postWhileBlock = nullptr;
    m_next = nullptr;
}

JitDoWhile* JitDoWhile::GetCurrentStatement()
{
    return DO_WHILE_STACK;
}

void JitDoWhile::JitContinue()
{
    // jump to test condition block
    m_builder->CreateBr(m_condWhileBlock);
}

void JitDoWhile::JitBreak()
{
    // jump to post while block
    m_builder->CreateBr(m_postWhileBlock);
}

void JitDoWhile::JitCond()
{
    // end previous block and start test block
    m_builder->CreateBr(m_condWhileBlock);
    m_builder->SetInsertPoint(m_condWhileBlock);
}

void JitDoWhile::JitWhileCmp(llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp)
{
    EvalCmp(lhs, rhs, cmpOp, m_execWhileBlock, m_postWhileBlock);
}

void JitDoWhile::JitWhileEval(llvm::Value* value)
{
    Eval(value, m_execWhileBlock, m_postWhileBlock);
}

void JitDoWhile::JitWhileNot(llvm::Value* value)
{
    EvalNot(value, m_execWhileBlock, m_postWhileBlock);
}

void JitDoWhile::JitEnd()
{
    // no need to jump to cond block
    CloseCompileFrame(m_codegenContext);  // close do-while-block compile frame
    m_builder->SetInsertPoint(m_postWhileBlock);
}

JitFor::JitFor(LlvmCodeGenContext* codegenContext, const char* blockName, const char* counterName,
    llvm::Value* initValue, llvm::Value* step, llvm::Value* bound, JitExec::JitICmpOp cmpOp)
    : JitLoop(codegenContext, blockName), m_step(step), m_bound(bound), m_cmpOp(cmpOp)
{
    Init(blockName, counterName, nullptr, initValue);
}

JitFor::JitFor(LlvmCodeGenContext* codegenContext, const char* blockName, llvm::Value* counterVar,
    llvm::Value* initValue, llvm::Value* step, llvm::Value* bound, JitExec::JitICmpOp cmpOp)
    : JitLoop(codegenContext, blockName), m_step(step), m_bound(bound), m_cmpOp(cmpOp)
{
    Init(blockName, nullptr, counterVar, initValue);
}

JitFor::~JitFor()
{
    // pop this object from the compile-time for-stack
    FOR_STACK = FOR_STACK->m_next;
}

JitFor* JitFor::GetCurrentStatement()
{
    return FOR_STACK;
}

llvm::Value* JitFor::GetCounter()
{
    return m_builder->CreateLoad(m_counterValue, true);
}

void JitFor::JitContinue()
{
    // jump to test condition block
    m_builder->CreateBr(m_incrementForCounterBlock);
}

void JitFor::JitBreak()
{
    m_builder->CreateBr(m_postForBlock);
}

void JitFor::JitEnd()
{
    // jump to test condition block
    m_builder->CreateBr(m_incrementForCounterBlock);
    CloseCompileFrame(m_codegenContext);  // close for-block compile frame
    m_builder->SetInsertPoint(m_postForBlock);
}

void JitFor::Init(const char* blockName, const char* counterName, llvm::Value* counterVar, llvm::Value* initValue)
{
    m_initForCounterBlock = MakeBlock(blockName, "_init_for_counter_bb");
    m_condForBlock = MakeBlock(blockName, "_cond_for_bb");
    m_execForBodyBlock = MakeBlock(blockName, "_exec_for_body_bb");
    m_incrementForCounterBlock = MakeBlock(blockName, "_increment_for_counter_bb");
    m_postForBlock = MakeBlock(blockName, "_post_while_bb");

    // we end current block and start the do-while execution block
    m_builder->CreateBr(m_initForCounterBlock);
    m_builder->SetInsertPoint(m_initForCounterBlock);
    OpenCompileFrame(m_codegenContext);  // open for-block compile frame

    // generate create local variable and assign instructions
    if (counterName) {
        m_counterValue = m_builder->CreateAlloca(initValue->getType(), 0, nullptr, counterName);
    } else {
        MOT_ASSERT(counterVar);
        m_counterValue = counterVar;
    }
    m_builder->CreateStore(initValue, m_counterValue);
    m_builder->CreateBr(m_condForBlock);

    // generate check counter block
    m_builder->SetInsertPoint(m_condForBlock);
    {
        JitIf jitIf(m_codegenContext, "for_cond");
        llvm::LoadInst* counterValue = m_builder->CreateLoad(m_counterValue);
        jitIf.JitIfCmp(counterValue, m_bound, m_cmpOp);
        m_builder->CreateBr(m_execForBodyBlock);
        jitIf.JitElse();
        m_builder->CreateBr(m_postForBlock);
        jitIf.JitEnd();
    }

    // generate increment counter block
    m_builder->CreateBr(m_incrementForCounterBlock);
    m_builder->SetInsertPoint(m_incrementForCounterBlock);
    llvm::LoadInst* counterValue = m_builder->CreateLoad(m_counterValue);
    llvm::Value* newCounterValue = m_builder->CreateAdd(counterValue, m_step);
    m_builder->CreateStore(newCounterValue, m_counterValue);
    m_builder->CreateBr(m_condForBlock);

    // allow user now to define for body
    m_builder->SetInsertPoint(m_execForBodyBlock);

    // push this object on top of the compile-time for-stack
    m_next = FOR_STACK;
    FOR_STACK = this;
}

JitSwitchCase::JitSwitchCase(LlvmCodeGenContext* codegenContext, const char* blockName, llvm::Value* switchValue)
    : JitStatement(codegenContext),
      m_blockName(blockName),
      m_switchValue(switchValue),
      m_caseBlock(nullptr),
      m_postCaseBlock(nullptr)
{
    m_defaultCaseBlock = MakeBlock(blockName, "_default_case_bb");
    m_postSwitchBlock = MakeBlock(blockName, "_post_switch_bb");

    // push this object on top of the compile-time switch-stack
    m_next = SWITCH_CASE_STACK;
    SWITCH_CASE_STACK = this;
}

JitSwitchCase::~JitSwitchCase()
{
    // pop this object from the compile-time switch-stack
    SWITCH_CASE_STACK = SWITCH_CASE_STACK->m_next;
}

JitSwitchCase* JitSwitchCase::GetCurrentStatement()
{
    return SWITCH_CASE_STACK;
}

void JitSwitchCase::JitCase(llvm::Value* caseValue)
{
    JitCaseMany(caseValue);
    JitCaseManyTerm();
}

void JitSwitchCase::JitCaseMany(llvm::Value* caseValue)
{
    // generate block for case body
    if (m_caseBlock == nullptr) {
        MOT_ASSERT(m_postCaseBlock == nullptr);
        m_caseBlock = MakeBlock(m_blockName.c_str(), "_case_bb");
        m_postCaseBlock = MakeBlock(m_blockName.c_str(), "_post_case_bb");
    }

    // generate block for next case value in this case block
    llvm::BasicBlock* nextBlock = MakeBlock(m_blockName.c_str(), "_next_case_bb");
    EvalCmp(m_switchValue, caseValue, JitExec::JIT_ICMP_EQ, m_caseBlock, nextBlock);
    m_builder->SetInsertPoint(nextBlock);
}

void JitSwitchCase::JitCaseManyTerm()
{
    MOT_ASSERT(m_caseBlock != nullptr);
    MOT_ASSERT(m_postCaseBlock != nullptr);
    MOT_ASSERT(!CurrentBlockEndsInBranch());

    // if none of the values matches, we need to jump to next CASE clause
    m_builder->CreateBr(m_postCaseBlock);

    // now start this CASE body
    m_builder->SetInsertPoint(m_caseBlock);
    OpenCompileFrame(m_codegenContext);
    m_caseBlock = nullptr;
}

void JitSwitchCase::JitBreak()
{
    // case handled, so we jump to code after the switch-case statement
    if (!CurrentBlockEndsInBranch()) {
        m_builder->CreateBr(m_postSwitchBlock);
    }
}

void JitSwitchCase::JitCaseEnd()
{
    // default label does not have a post case block
    if (m_postCaseBlock != nullptr) {
        // must be already set to null previously
        MOT_ASSERT(m_caseBlock == nullptr);
        m_builder->SetInsertPoint(m_postCaseBlock);
        m_postCaseBlock = nullptr;
    }
}

void JitSwitchCase::JitDefault()
{
    MOT_ASSERT(!CurrentBlockEndsInBranch());
    m_builder->CreateBr(m_defaultCaseBlock);
    m_builder->SetInsertPoint(m_defaultCaseBlock);
    OpenCompileFrame(m_codegenContext);  // open default-block compile frame
}

void JitSwitchCase::JitEnd()
{
    // if last case statement did not call "break", we do it ourselves
    if (!CurrentBlockEndsInBranch()) {
        m_builder->CreateBr(m_postSwitchBlock);
    }
    m_builder->SetInsertPoint(m_postSwitchBlock);
}

JitTryCatch::JitTryCatch(LlvmCodeGenContext* codegenContext, const char* blockName)
    : JitStatement(codegenContext), m_blockName(blockName), m_ifTry(nullptr), m_exceptionSwitchCase(nullptr)
{
    m_beginTryBlock = MakeBlock(blockName, "_begin_try_bb");
    m_postTryCatchBlock = MakeBlock(blockName, "_post_try_catch_bb");

    // terminate previous block and start a new one (at least for code readability)
    m_builder->CreateBr(m_beginTryBlock);
    m_builder->SetInsertPoint(m_beginTryBlock);

    // push frame and call setjmp intrinsic
    llvm::Value* setJumpBuf = AddLlvmPushExceptionFrame(m_codegenContext);
    LLVM_DEBUG_PRINT_FRAME(m_codegenContext, "Calling setjmp on frame", setJumpBuf);
    llvm::Value* setJumpRes = AddLlvmSetJmp(m_codegenContext, setJumpBuf);

    // if result is zero, then this is normal execution, otherwise some exception was thrown
    m_ifTry = new (std::nothrow) JitIf(m_codegenContext, (m_blockName + "_begin_try").c_str());
    if (m_ifTry == nullptr) {
        MOT_LOG_TRACE("Failed to allocate JIT IF statement for TRY-CATCH");
    } else {
        m_ifTry->JitIfNot(setJumpRes);
    }
    LLVM_DEBUG_PRINT(m_codegenContext, "JitTryCatch::JitTryCatch()", "Starting normal flow");

    // push this object on top of the try-catch-stack
    m_next = TRY_CATCH_STACK;
    TRY_CATCH_STACK = this;
}

JitTryCatch::~JitTryCatch()
{
    // cleanup if needed
    if (m_ifTry != nullptr) {
        MOT_LOG_TRACE("Releasing dangling if in try/catch statement, missing catch handler?");
        delete m_ifTry;
        m_ifTry = nullptr;
    }
    if (m_exceptionSwitchCase != nullptr) {
        MOT_LOG_TRACE("Releasing dangling switch-case in try/catch statement, missing catch handler?");
        delete m_exceptionSwitchCase;
        m_exceptionSwitchCase = nullptr;
    }
    // pop this object from the compile-time try-catch-stack
    TRY_CATCH_STACK = TRY_CATCH_STACK->m_next;
}

JitTryCatch* JitTryCatch::GetCurrentStatement()
{
    return TRY_CATCH_STACK;
}

void JitTryCatch::JitThrow(llvm::Value* exceptionValue)
{
    // save fault code and raise exception status
    LLVM_DEBUG_PRINT(m_codegenContext, "JitTryCatch::JitThrow()", "Throwing exception");
    AddLlvmThrowException(m_codegenContext, exceptionValue);
}

void JitTryCatch::JitRethrow()
{
    // raise exception status
    LLVM_DEBUG_PRINT(m_codegenContext, "JitTryCatch::JitRethrow()", "Re-throwing exception");
    AddLlvmRethrowException(m_codegenContext);
}

void JitTryCatch::JitBeginCatch(llvm::Value* value)
{
    if (m_exceptionSwitchCase == nullptr) {
        PrepareExceptionBlock();
    }
    if (m_exceptionSwitchCase != nullptr) {
        m_exceptionSwitchCase->JitCase(value);
    }

    // mark exception handled (but leave exception value register intact, to allow re-throw, it will be reset later)
    LLVM_DEBUG_PRINT(m_codegenContext, "JitTryCatch::JitBeginCatch()", "Entering single-value catch handler");
    AddLlvmResetExceptionStatus(m_codegenContext);
}

void JitTryCatch::JitBeginCatchMany(llvm::Value* value)
{
    if (m_exceptionSwitchCase == nullptr) {
        PrepareExceptionBlock();
    }
    if (m_exceptionSwitchCase != nullptr) {
        m_exceptionSwitchCase->JitCaseMany(value);
    }
}

void JitTryCatch::JitCatchManyTerm()
{
    if (m_exceptionSwitchCase != nullptr) {
        m_exceptionSwitchCase->JitCaseManyTerm();
    }

    // mark exception handled (but leave exception value register intact, to allow re-throw, it will be reset later)
    LLVM_DEBUG_PRINT(m_codegenContext, "JitTryCatch::JitEndCatchMany()", "Entering multi-value catch handler");
    AddLlvmResetExceptionStatus(m_codegenContext);
}

void JitTryCatch::JitEndCatch()
{
    LLVM_DEBUG_PRINT(m_codegenContext, "JitTryCatch::JitEndCatch()", "Exiting catch handler");
    if (m_exceptionSwitchCase != nullptr) {
        m_exceptionSwitchCase->JitBreak();
        m_exceptionSwitchCase->JitCaseEnd();
    }
}

void JitTryCatch::JitCatchAll()
{
    if (m_exceptionSwitchCase == nullptr) {
        PrepareExceptionBlock();
    }

    if (m_exceptionSwitchCase != nullptr) {
        m_exceptionSwitchCase->JitDefault();
    }

    // mark exception handled (but leave exception value register intact, to allow re-throw, it will be reset later)
    LLVM_DEBUG_PRINT(m_codegenContext, "JitTryCatch::JitCatchAll()", "Entering catch-all handler");
    AddLlvmResetExceptionStatus(m_codegenContext);
}

void JitTryCatch::JitEnd()
{
    // end switch-case block
    if (m_exceptionSwitchCase != nullptr) {
        m_exceptionSwitchCase->JitEnd();
        delete m_exceptionSwitchCase;
        m_exceptionSwitchCase = nullptr;
    }

    // end if block
    MOT_ASSERT(m_ifTry);
    if (m_ifTry != nullptr) {
        m_ifTry->JitEnd();
        delete m_ifTry;
        m_ifTry = nullptr;
    }

    LLVM_DEBUG_PRINT(m_codegenContext, "JitTryCatch::JitEnd()", "Exiting try-catch block");
    // if exception not handled then re-throw
    {
        JitIf jitIf(m_codegenContext, "check_exception_not_handled");
        llvm::Value* exceptionStatus = AddLlvmGetExceptionStatus(m_codegenContext);
        llvm::ConstantInt* statusValue =
            llvm::ConstantInt::get(m_codegenContext->INT32_T, LLVM_EXCEPTION_STATUS_NONE, true);
        jitIf.JitIfCmp(exceptionStatus, statusValue, JitExec::JIT_ICMP_NE);
        LLVM_DEBUG_PRINT(m_codegenContext, "JitTryCatch::JitEnd()", "Exception unhandled, unwinding stack");
        AddLlvmUnwindExceptionFrame(m_codegenContext);
        jitIf.JitElse();
        // if we reached here, it means that the exception was handled, so we clear the exception register
        // pay attention that the exception status register is supposed to be already reset by some catch handler
        LLVM_DEBUG_PRINT(m_codegenContext, "JitTryCatch::JitEnd()", "Exception handled, resetting value");
        AddLlvmResetExceptionValue(m_codegenContext);  // clear off exception value register
        jitIf.JitEnd();
    }

    // start exception post-processing
    m_builder->CreateBr(m_postTryCatchBlock);
    m_builder->SetInsertPoint(m_postTryCatchBlock);
    LLVM_DEBUG_PRINT(m_codegenContext, "JitTryCatch::JitEnd()", "Starting post-try-catch block");
}

llvm::Value* JitTryCatch::JitGetExceptionStatus()
{
    return AddLlvmGetExceptionStatus(m_codegenContext);
}

void JitTryCatch::PrepareExceptionBlock()
{
    // try block ended without exception being thrown, so jump to post block
    if (!CurrentBlockEndsInBranch()) {
        AddLlvmPopExceptionFrame(m_codegenContext);
        m_builder->CreateBr(m_postTryCatchBlock);
    } else {
        // in case of return instruction, the SP code is fine (but not in general case...)
        // in case of some other bizarre goto, the result is undefined.
        // for now we do nothing, assuming caller is aware of the effect of his actions

        // although we should insert pop-exception frame instruction before the branch/return instruction
        // in reality, the caller is responsible for popping all exception frames before calling return.
    }

    // handle siglongjmp() result not zero - some exception was thrown
    MOT_ASSERT(m_ifTry != nullptr);
    if (m_ifTry != nullptr) {
        m_ifTry->JitElse();
    }
    LLVM_DEBUG_PRINT(
        m_codegenContext, "JitTryCatch::PrepareExceptionBlock()", "Entering else block of exception handler");

    // pop exception frame and get the exception value
    llvm::Value* exceptionValue = AddLlvmPopExceptionFrame(m_codegenContext);

    // now begin handle exceptions
    m_exceptionSwitchCase =
        new (std::nothrow) JitSwitchCase(m_codegenContext, (m_blockName + "_catch").c_str(), exceptionValue);
    if (m_exceptionSwitchCase == nullptr) {
        MOT_LOG_TRACE("Failed to allocate JIT SWITCH-CASE statement for TRY-CATCH");
    }
}
}  // namespace llvm_util
