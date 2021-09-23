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
 * jit_tvm_util.h
 *    TVM-jitted query execution utilities.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_tvm_util.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_TVM_UTIL_H
#define JIT_TVM_UTIL_H

#include <stddef.h>
#include "jit_common.h"  // for JitICmpOp, should be refactored?

namespace tvm {
// forward declarations
class Builder;
class BasicBlock;
class Instruction;

/** @class Static helpers. */
class JitUtils {
public:
    /**
     * @brief Formats a block name.
     * @param buf The format buffer.
     * @param size The format buffer size.
     * @param baseName The base name of the block.
     * @param suffix The suffix to append.
     * @return The resulting block name.
     */
    static char* FormatBlockName(char* buf, size_t size, const char* baseName, const char* suffix);

private:
    JitUtils()
    {}

    virtual ~JitUtils()
    {}
};

/** @class Implements a generic statement. */
class JitStatement {
protected:
    /** @var The TVM builder. */
    Builder* m_builder;

    /**
     * @brief Constructor.
     * @param builder The TVM builder.
     */
    explicit JitStatement(Builder* builder);

    /** @brief Destructor. */
    virtual ~JitStatement();

    /**
     * @brief Compares two values. If the values pass the condition then the success-block begins,
     * otherwise the fail block begins.
     * @param lhs The left-hand-side value to compare.
     * @param rhs The right-hand-side value to compare.
     * @param cmpOp The compare operation to perform.
     * @param successBlock The success block.
     * @param failBlock The fail block.
     */
    void EvalCmp(
        Instruction* lhs, Instruction* rhs, JitExec::JitICmpOp cmpOp, BasicBlock* successBlock, BasicBlock* failBlock);

    /**
     * @brief Tests whether a value is null or zero. If the value passes the condition then the
     * success-block begins, otherwise the fail block begins.
     * @param value The value to evaluate.
     * @param successBlock The success block.
     * @param failBlock The fail block.
     */
    void Eval(Instruction* value, BasicBlock* successBlock, BasicBlock* failBlock);

    /**
     * @brief Tests whether a value is NOT null or zero. If the value passes the condition then the
     * success-block begins, otherwise the fail block begins.
     * @param value The value to evaluate.
     * @param successBlock The success block.
     * @param failBlock The fail block.
     */
    void EvalNot(Instruction* value, BasicBlock* successBlock, BasicBlock* failBlock);
};

/** @class Implements an if construct. */
class JitIf : public JitStatement {
public:
    /**
     * @brief Constructor.
     * @param builder The TVM builder.
     * @param blockName The if-block name.
     */
    JitIf(Builder* builder, const char* blockName);

    /** @brief Destructor. */
    ~JitIf() override;

    /** @brief Retrieves the current (innermost if nested) if-block. */
    static JitIf* GetCurrentStatement();

    /**
     * @brief Begins an if-block by comparing two values. If the values pass the condition then the
     * if-block begins.
     * @param lhs The left-hand-side value to compare.
     * @param rhs The right-hand-side value to compare.
     * @param cmpOp The compare operation to perform.
     */
    void JitIfCmp(Instruction* lhs, Instruction* rhs, JitExec::JitICmpOp cmpOp);

    /**
     * @brief Begins an if-block by checking if a value is null or zero. If the value passes the
     * condition then the if-block begins.
     * @param value The value to evaluate.
     */
    void JitIfEval(Instruction* value);

    /**
     * @brief Begins an if-block by checking if a value is not null or zero. If the value passes the
     * condition then the if-block begins.
     * @param value The value to evaluate.
     */
    void JitIfNot(Instruction* value);

    /** @brief Begins the else-block, in case the two values are not equal. Calling this function is optional. */
    void JitElse();

    /** @brief Terminates the if block. Calling this function is mandatory. */
    void JitEnd();

    /** @brief Retrieves the condition evaluation block. */
    inline BasicBlock* GetCondBlock()
    {
        return m_condBlock;
    };

    /** @brief Retrieves the condition success execution block. */
    inline BasicBlock* GetIfBlock()
    {
        return m_ifBlock;
    };

    /** @brief Retrieves the condition failure execution block. */
    inline BasicBlock* GetElseBlock()
    {
        return m_elseBlock;
    };

    /** @brief Retrieves the post-if execution block. */
    inline BasicBlock* GetPostBlock()
    {
        return m_postIfBlock;
    };

private:
    BasicBlock* m_condBlock;
    BasicBlock* m_ifBlock;
    BasicBlock* m_elseBlock;
    BasicBlock* m_postIfBlock;
    bool m_elseBlockUsed;

    /** @brief The next if-block (outer with respect to this block). */
    JitIf* m_next;
};

/** @class Implements a while construct. */
class JitWhile : public JitStatement {
public:
    /**
     * @brief Constructor.
     * @param builder The TVM builder.
     * @param blockName The while-block name.
     */
    JitWhile(Builder* builder, const char* blockName);

    /** @brief Destructor. */
    ~JitWhile() override;

    /** @brief Retrieves the current (innermost if nested) while-block. */
    static JitWhile* GetCurrentStatement();

    /**
     * @brief Performs the while-block test by comparing two values. If the values pass the condition
     * then the while exec-block begins. Calling this function is mandatory.
     * @param lhs The left-hand-side value to compare.
     * @param rhs The right-hand-side value to compare.
     * @param cmpOp The compare operation to perform.
     */
    void JitWhileCmp(Instruction* lhs, Instruction* rhs, JitExec::JitICmpOp cmpOp);

    /**
     * @brief Performs the while-block test by checking if a value is null or zero. If the value
     * passes the condition then the while exec-block begins.
     * @param value The value to evaluate.
     */
    void JitWhileEval(Instruction* value);

    /**
     * @brief Performs the while-block test by checking if a value is not null or zero. If the value
     * passes the condition then the while exec-block begins.
     * @param value The value to evaluate.
     */
    void JitWhileNot(Instruction* value);

    /** @brief Stops the current iteration and begins a new one. */
    void JitContinue();

    /** @brief Breaks out of the while loop. */
    void JitBreak();

    /** @brief Terminates the while block. Calling this function is mandatory. */
    void JitEnd();

    /** @brief Retrieves the condition evaluation block. */
    inline BasicBlock* GetCondBlock()
    {
        return m_condWhileBlock;
    };

    /** @brief Retrieves the condition success execution block. */
    inline BasicBlock* GetExecBlock()
    {
        return m_execWhileBlock;
    };

    /** @brief Retrieves the post-if execution block. */
    inline BasicBlock* GetPostBlock()
    {
        return m_postWhileBlock;
    };

private:
    BasicBlock* m_condWhileBlock;
    BasicBlock* m_execWhileBlock;
    BasicBlock* m_postWhileBlock;

    /** @brief The next while-block (outer with respect to this block). */
    JitWhile* m_next;
};

/** @class Implements a do-while construct. */
class JitDoWhile : public JitStatement {
public:
    /**
     * @brief Constructor.
     * @param builder The TVM builder.
     * @param blockName The do-while-block name.
     */
    JitDoWhile(Builder* builder, const char* blockName);

    /** @brief Destructor. */
    ~JitDoWhile() override;

    /** @brief Retrieves the current (innermost if nested) do-while-block. */
    static JitDoWhile* GetCurrentStatement();

    /** @brief Stops the current iteration and begins a new one by first evaluating the condition. */
    void JitContinue();

    /** @brief Breaks out of the do-while loop. */
    void JitBreak();

    /**
     * @brief Marks the beginning of the test condition block. This is followed by code to compute the
     * two values used in the test.
     */
    void JitCond();

    /**
     * @brief Performs the while-block test by comparing two values. If the values pass the condition
     * then the while exec-block begins. Calling this function is mandatory.
     * @param lhs The left-hand-side value to compare.
     * @param rhs The right-hand-side value to compare.
     * @param cmpOp The compare operation to perform.
     */
    void JitWhileCmp(Instruction* lhs, Instruction* rhs, JitExec::JitICmpOp cmpOp);

    /**
     * @brief Performs the while-block test by checking if a value is null or zero. If the value
     * passes the condition then the while exec-block begins.
     * @param value The value to evaluate.
     */
    void JitWhileEval(Instruction* value);

    /**
     * @brief Performs the while-block test by checking if a value is not null or zero. If the value
     * passes the condition then the while exec-block begins.
     * @param value The value to evaluate.
     */
    void JitWhileNot(Instruction* value);

    /** @brief Terminates the do-while block. Calling this function is mandatory. */
    void JitEnd();

    /** @brief Retrieves the condition evaluation block. */
    inline BasicBlock* GetCondBlock()
    {
        return m_condWhileBlock;
    };

    /** @brief Retrieves the condition success execution block. */
    inline BasicBlock* GetExecBlock()
    {
        return m_execWhileBlock;
    };

    /** @brief Retrieves the post-if execution block. */
    inline BasicBlock* GetPostBlock()
    {
        return m_postWhileBlock;
    };

private:
    BasicBlock* m_execWhileBlock;
    BasicBlock* m_condWhileBlock;
    BasicBlock* m_postWhileBlock;

    /** @brief The next do-while-block (outer with respect to this block). */
    JitDoWhile* m_next;
};
}  // namespace tvm

/********************************************
 * TVM helper macros
 ********************************************/
/** @define Helper macro for a constant instruction. */
#define JIT_CONST(value) ctx->_builder->CreateConst((uint64_t)(value))

/** @define Helper macro for a return statement. */
#define JIT_RETURN(value) ctx->_builder->CreateRet(value)

/** @define Helper macro for a return constant statement. */
#define JIT_RETURN_CONST(value) ctx->_builder->CreateRet(JIT_CONST(value))

/** @define Helper macro for a branch/goto statement. */
#define JIT_GOTO(block) ctx->_builder->CreateBr(block)

/** @define Macro for getting the current block. */
#define JIT_CURRENT_BLOCK() ctx->_builder->GetInsertBlock()

/********************************************
 * TVM If Statement
 ********************************************/
/**
 * @define Macro for starting a pseudo-LLVM if statement. Make sure to put condition evaluation code
 * after this statement if you wish to jump back to the condition evaluation block.
 */
#define JIT_IF_BEGIN(name) \
    {                      \
        tvm::JitIf jitIf(ctx->_builder, #name);

/** @define Macro for referring the current if-block. */
#define JIT_IF_CURRENT() tvm::JitIf::GetCurrentStatement()

/********************************************
 * TVM While Statement
 ********************************************/
/** @define Macro for starting a native while statement. */
#define JIT_WHILE_BEGIN(name) \
    {                         \
        tvm::JitWhile jitWhile(ctx->_builder, #name);

/** @define Macro for referring the current while-block. */
#define JIT_WHILE_CURRENT() tvm::JitWhile::GetCurrentStatement()

/********************************************
 * TVM Do-While Statement
 ********************************************/
/** @define Macro for starting a native do-while statement. */
#define JIT_DO_WHILE_BEGIN(name) \
    {                            \
        tvm::JitDoWhile jitDoWhile(ctx->_builder, #name);

/** @define Macro for referring the current do-while-block. */
#define JIT_DO_WHILE_CURRENT() tvm::JitDoWhile::GetCurrentStatement()

// the other macros for emitting JIT code are found in jit_util.h
// make sure to include that file after including this file
#endif /* JIT_TVM_UTIL_H */
