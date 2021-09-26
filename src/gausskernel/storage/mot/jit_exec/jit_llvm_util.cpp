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

using namespace dorado;

namespace llvm {
DECLARE_LOGGER(JitUtil, JitExec);

// Globals
#define IF_STACK u_sess->mot_cxt.jit_llvm_if_stack
#define WHILE_STACK u_sess->mot_cxt.jit_llvm_while_stack
#define DO_WHILE_STACK u_sess->mot_cxt.jit_llvm_do_while_stack

char* JitUtils::FormatBlockName(char* buf, size_t size, const char* baseName, const char* suffix)
{
    errno_t erc = snprintf_s(buf, size, size - 1, "%s%s", baseName, suffix);
    securec_check_ss(erc, "\0", "\0");
    return buf;
}

llvm::Value* JitUtils::ExecCompare(
    GsCodeGen::LlvmBuilder* builder, llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp)
{
    llvm::Value* res = NULL;
    switch (cmpOp) {
        case JitExec::JIT_ICMP_EQ:
            res = builder->CreateICmpEQ(lhs, rhs);
            break;

        case JitExec::JIT_ICMP_NE:
            res = builder->CreateICmpNE(lhs, rhs);
            break;

        case JitExec::JIT_ICMP_GT:
            res = builder->CreateICmpUGT(lhs, rhs);
            break;

        case JitExec::JIT_ICMP_GE:
            res = builder->CreateICmpUGE(lhs, rhs);
            break;

        case JitExec::JIT_ICMP_LT:
            res = builder->CreateICmpULT(lhs, rhs);
            break;

        case JitExec::JIT_ICMP_LE:
            res = builder->CreateICmpULE(lhs, rhs);
            break;

        default:
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Execute Pseudo-JIT Function", "Invalid compare operator %d", (int)cmpOp);
            MOT_ASSERT(false);
            return NULL;
    }
    return res;
}

llvm::Value* JitUtils::GetTypedZero(const llvm::Value* value)
{
    llvm::Value* typedZero = NULL;
    if (value->getType()->isPointerTy()) {
        typedZero = llvm::ConstantPointerNull::get((llvm::PointerType*)value->getType());
    } else {
        typedZero = llvm::ConstantInt::get(value->getType(), 0, true);
    }
    return typedZero;
}

JitStatement::JitStatement(llvm::LLVMContext& context, GsCodeGen::LlvmBuilder* builder)
    : m_context(context), m_builder(builder)
{}

JitStatement::~JitStatement()
{}

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

JitIf::JitIf(
    llvm::LLVMContext& context, GsCodeGen::LlvmBuilder* builder, llvm::Function* jittedFunction, const char* blockName)
    : JitStatement(context, builder), m_elseBlockUsed(false)
{
    const unsigned int bufSize = 64;
    char buf[bufSize];
    m_condBlock = llvm::BasicBlock::Create(
        m_context, JitUtils::FormatBlockName(buf, bufSize, blockName, "_if_cond_bb"), jittedFunction);
    m_ifBlock = llvm::BasicBlock::Create(
        m_context, JitUtils::FormatBlockName(buf, bufSize, blockName, "_if_exec_bb"), jittedFunction);
    m_elseBlock = llvm::BasicBlock::Create(
        m_context, JitUtils::FormatBlockName(buf, bufSize, blockName, "_else_exec_bb"), jittedFunction);
    m_postIfBlock = llvm::BasicBlock::Create(
        m_context, JitUtils::FormatBlockName(buf, bufSize, blockName, "_post_if_bb"), jittedFunction);

    // we end current block and start the condition evaluation block
    m_builder->CreateBr(m_condBlock);
    m_builder->SetInsertPoint(m_condBlock);

    // push this object on top of the if-stack
    m_next = IF_STACK;
    IF_STACK = this;
}

JitIf::~JitIf()
{
    // pop this object from the if-stack
    IF_STACK = IF_STACK->m_next;
    m_condBlock = nullptr;
    m_ifBlock = nullptr;
    m_elseBlock = nullptr;
    m_postIfBlock = nullptr;
    m_builder = nullptr;
    m_next = nullptr;
}

JitIf* JitIf::GetCurrentStatement()
{
    return IF_STACK;
}

void JitIf::JitIfCmp(llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp)
{
    EvalCmp(lhs, rhs, cmpOp, m_ifBlock, m_elseBlock);
}

void JitIf::JitIfEval(llvm::Value* value)
{
    Eval(value, m_ifBlock, m_elseBlock);
}

void JitIf::JitIfNot(llvm::Value* value)
{
    EvalNot(value, m_ifBlock, m_elseBlock);
}

void JitIf::JitElse()
{
    // end current block first (jump to post-if point), and then start else block
    // attention: current block might be something else than the if-true block
    if (!CurrentBlockEndsInBranch()) {
        m_builder->CreateBr(m_postIfBlock);
    }
    m_builder->SetInsertPoint(m_elseBlock);
    m_elseBlockUsed = true;
}

void JitIf::JitEnd()
{
    // end current block (if not already ended)
    // attention: current block might be something else than the if-true or if-false blocks
    if (!CurrentBlockEndsInBranch()) {
        m_builder->CreateBr(m_postIfBlock);
    }

    if (!m_elseBlockUsed) {
        // user did not use else block, so we must generate an empty one, otherwise it will be removed
        // during function finalization (since it is empty), and if the condition evaluation fails, then
        // during runtime we will jump to a block that was deleted (core dump)
        m_builder->SetInsertPoint(m_elseBlock);
        m_builder->CreateBr(m_postIfBlock);
    }

    // now start the post-if block
    m_builder->SetInsertPoint(m_postIfBlock);
}

JitWhile::JitWhile(
    llvm::LLVMContext& context, GsCodeGen::LlvmBuilder* builder, llvm::Function* jittedFunction, const char* blockName)
    : JitStatement(context, builder)
{
    const unsigned int bufSize = 64;
    char buf[bufSize];
    m_condWhileBlock = llvm::BasicBlock::Create(
        m_context, JitUtils::FormatBlockName(buf, bufSize, blockName, "_cond_while_bb"), jittedFunction);
    m_execWhileBlock = llvm::BasicBlock::Create(
        m_context, JitUtils::FormatBlockName(buf, bufSize, blockName, "_exec_while_bb"), jittedFunction);
    m_postWhileBlock = llvm::BasicBlock::Create(
        m_context, JitUtils::FormatBlockName(buf, bufSize, blockName, "_post_while_bb"), jittedFunction);

    // we end current block and start the condition evaluation block
    m_builder->CreateBr(m_condWhileBlock);
    m_builder->SetInsertPoint(m_condWhileBlock);

    // push this object on top of the while-stack
    m_next = WHILE_STACK;
    WHILE_STACK = this;
}

JitWhile::~JitWhile()
{
    // pop this object from the while-stack
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
}

void JitWhile::JitWhileEval(llvm::Value* value)
{
    Eval(value, m_execWhileBlock, m_postWhileBlock);
}

void JitWhile::JitWhileNot(llvm::Value* value)
{
    EvalNot(value, m_execWhileBlock, m_postWhileBlock);
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
    m_builder->SetInsertPoint(m_postWhileBlock);
}

JitDoWhile::JitDoWhile(
    llvm::LLVMContext& context, GsCodeGen::LlvmBuilder* builder, llvm::Function* jittedFunction, const char* blockName)
    : JitStatement(context, builder)
{
    const unsigned int bufSize = 64;
    char buf[bufSize];
    m_execWhileBlock = llvm::BasicBlock::Create(
        m_context, JitUtils::FormatBlockName(buf, bufSize, blockName, "_exec_while_bb"), jittedFunction);
    m_condWhileBlock = llvm::BasicBlock::Create(
        m_context, JitUtils::FormatBlockName(buf, bufSize, blockName, "_cond_while_bb"), jittedFunction);
    m_postWhileBlock = llvm::BasicBlock::Create(
        m_context, JitUtils::FormatBlockName(buf, bufSize, blockName, "_post_while_bb"), jittedFunction);

    // we end current block and start the do-while execution block
    m_builder->CreateBr(m_execWhileBlock);
    m_builder->SetInsertPoint(m_execWhileBlock);

    // push this object on top of the do-while-stack
    m_next = DO_WHILE_STACK;
    DO_WHILE_STACK = this;
}

JitDoWhile::~JitDoWhile()
{
    // pop this object from the while-stack
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
    m_builder->SetInsertPoint(m_postWhileBlock);
}
}  // namespace llvm
