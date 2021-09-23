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
 * jit_tvm_util.cpp
 *    TVM-jitted query execution utilities.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_tvm_util.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "jit_tvm_util.h"
#include "jit_tvm.h"
#include "knl/knl_session.h"
#include <stdio.h>

namespace tvm {
// Globals
#define IF_STACK u_sess->mot_cxt.jit_tvm_if_stack
#define WHILE_STACK u_sess->mot_cxt.jit_tvm_while_stack
#define DO_WHILE_STACK u_sess->mot_cxt.jit_tvm_do_while_stack

char* JitUtils::FormatBlockName(char* buf, size_t size, const char* baseName, const char* suffix)
{
    errno_t erc = snprintf_s(buf, size, size - 1, "%s%s", baseName, suffix);
    securec_check_ss(erc, "\0", "\0");
    return buf;
}

JitStatement::JitStatement(Builder* builder) : m_builder(builder)
{}

JitStatement::~JitStatement()
{
    m_builder = nullptr;
}

void JitStatement::EvalCmp(
    Instruction* lhs, Instruction* rhs, JitExec::JitICmpOp cmpOp, BasicBlock* successBlock, BasicBlock* failBlock)
{
    // evaluate condition and start if-true block
    Instruction* res = m_builder->CreateICmp(lhs, rhs, cmpOp);
    m_builder->CreateCondBr(res, successBlock, failBlock);
    m_builder->SetInsertPoint(successBlock);
}

void JitStatement::Eval(Instruction* value, BasicBlock* successBlock, BasicBlock* failBlock)
{
    // if value is not null (i.e. not equal to zero) we branch to success
    EvalCmp(value, m_builder->CreateConst(0), JitExec::JIT_ICMP_NE, successBlock, failBlock);
}

void JitStatement::EvalNot(Instruction* value, BasicBlock* successBlock, BasicBlock* failBlock)
{
    // if value is null (i.e. equal to zero) we branch to success
    EvalCmp(value, m_builder->CreateConst(0), JitExec::JIT_ICMP_EQ, successBlock, failBlock);
}

JitIf::JitIf(Builder* builder, const char* blockName) : JitStatement(builder), m_elseBlockUsed(false)
{
    constexpr size_t bufSize = 64;
    char buf[bufSize];
    m_condBlock = m_builder->CreateBlock(JitUtils::FormatBlockName(buf, bufSize, blockName, "_if_cond_bb"));
    m_ifBlock = m_builder->CreateBlock(JitUtils::FormatBlockName(buf, bufSize, blockName, "_if_exec_bb"));
    m_elseBlock = m_builder->CreateBlock(JitUtils::FormatBlockName(buf, bufSize, blockName, "_else_exec_bb"));
    m_postIfBlock = m_builder->CreateBlock(JitUtils::FormatBlockName(buf, bufSize, blockName, "_post_if_bb"));

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
    m_next = nullptr;
}

JitIf* JitIf::GetCurrentStatement()
{
    return IF_STACK;
}

void JitIf::JitIfCmp(Instruction* lhs, Instruction* rhs, JitExec::JitICmpOp cmpOp)
{
    EvalCmp(lhs, rhs, cmpOp, m_ifBlock, m_elseBlock);
}

void JitIf::JitIfEval(Instruction* value)
{
    Eval(value, m_ifBlock, m_elseBlock);
}

void JitIf::JitIfNot(Instruction* value)
{
    EvalNot(value, m_ifBlock, m_elseBlock);
}

void JitIf::JitElse()
{
    // end if-true block first (jump to post-if point), and then start else block
    if (!m_builder->currentBlockEndsInBranch()) {
        m_builder->CreateBr(m_postIfBlock);
    }
    m_builder->SetInsertPoint(m_elseBlock);
    m_elseBlockUsed = true;
}

void JitIf::JitEnd()
{
    // end previous block (if not already ended)
    if (!m_builder->currentBlockEndsInBranch()) {
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

JitWhile::JitWhile(Builder* builder, const char* blockName) : JitStatement(builder)
{
    constexpr int bufSize = 64;
    char buf[bufSize];
    m_condWhileBlock = m_builder->CreateBlock(JitUtils::FormatBlockName(buf, bufSize, blockName, "_cond_while_bb"));
    m_execWhileBlock = m_builder->CreateBlock(JitUtils::FormatBlockName(buf, bufSize, blockName, "_exec_while_bb"));
    m_postWhileBlock = m_builder->CreateBlock(JitUtils::FormatBlockName(buf, bufSize, blockName, "_post_while_bb"));

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

void JitWhile::JitWhileCmp(Instruction* lhs, Instruction* rhs, JitExec::JitICmpOp cmpOp)
{
    EvalCmp(lhs, rhs, cmpOp, m_execWhileBlock, m_postWhileBlock);
}

void JitWhile::JitWhileEval(Instruction* value)
{
    Eval(value, m_execWhileBlock, m_postWhileBlock);
}

void JitWhile::JitWhileNot(Instruction* value)
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

JitDoWhile::JitDoWhile(Builder* builder, const char* blockName) : JitStatement(builder)
{
    const unsigned int bufSize = 64;
    char buf[bufSize];
    m_execWhileBlock = m_builder->CreateBlock(JitUtils::FormatBlockName(buf, bufSize, blockName, "_exec_while_bb"));
    m_condWhileBlock = m_builder->CreateBlock(JitUtils::FormatBlockName(buf, bufSize, blockName, "_cond_while_bb"));
    m_postWhileBlock = m_builder->CreateBlock(JitUtils::FormatBlockName(buf, bufSize, blockName, "_post_while_bb"));

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

void JitDoWhile::JitWhileCmp(Instruction* lhs, Instruction* rhs, JitExec::JitICmpOp cmpOp)
{
    EvalCmp(lhs, rhs, cmpOp, m_execWhileBlock, m_postWhileBlock);
}

void JitDoWhile::JitWhileEval(Instruction* value)
{
    Eval(value, m_execWhileBlock, m_postWhileBlock);
}

void JitDoWhile::JitWhileNot(Instruction* value)
{
    EvalNot(value, m_execWhileBlock, m_postWhileBlock);
}

void JitDoWhile::JitEnd()
{
    // no need to jump to cond block
    m_builder->SetInsertPoint(m_postWhileBlock);
}
}  // namespace tvm
