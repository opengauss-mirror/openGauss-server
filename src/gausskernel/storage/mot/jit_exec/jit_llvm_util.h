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
 * jit_llvm_util.h
 *    LLVM JIT utilities.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm_util.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_LLVM_UTIL_H
#define JIT_LLVM_UTIL_H

/*
 * ATTENTION:
 * 1. Be sure to include gscodegen.h before anything else to avoid clash with PM definition in datetime.h.
 * 2. Be sure to include libintl.h before gscodegen.h to avoid problem with gettext.
 */
#include "libintl.h"
#include "codegen/gscodegen.h"
#include "jit_common.h"

// ATTENTION: In order to define all macros just once, we use the same class names. In order to avoid
// name clashes, we include these classes in the llvm name space. Nevertheless, be careful not to
// include this file and jit_util.h as well.
namespace llvm {
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

    /**
     * @brief Executes a comparison between two values.
     * @param builder The LLVM builder.
     * @param lhs The left-hand-side value to compare.
     * @param rhs The right-hand-side value to compare.
     * @param cmpOp The compare operation to perform.
     * @return The comparison result (1 if comparison is true, otherwise 0).
     */
    static llvm::Value* ExecCompare(
        dorado::GsCodeGen::LlvmBuilder* builder, llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp);

    /**
     * @brief Constructs a typed zero value by another value.
     * @param value The value whose type is to be considered.
     * @return The zero value with a type like the input parameter.
     */
    static llvm::Value* GetTypedZero(const llvm::Value* value);

private:
    JitUtils()
    {}

    virtual ~JitUtils()
    {}
};

/****************************
 *         Statement
 ****************************/
/** @class Implements a generic statement. */
class JitStatement {
protected:
    /** @var The LLVM context. */
    llvm::LLVMContext& m_context;

    /** @var The LLVM builder. */
    dorado::GsCodeGen::LlvmBuilder* m_builder;

    /**
     * @brief Constructor.
     * @param context The LLVM context.
     * @param builder The LLVM builder.
     */
    JitStatement(llvm::LLVMContext& context, dorado::GsCodeGen::LlvmBuilder* builder);

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
    void EvalCmp(llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp, llvm::BasicBlock* successBlock,
        llvm::BasicBlock* failBlock);

    /**
     * @brief Tests whether a value is null or zero. If the value passes the condition then the
     * success-block begins, otherwise the fail block begins.
     * @param value The value to evaluate.
     * @param successBlock The success block.
     * @param failBlock The fail block.
     */
    void Eval(llvm::Value* value, llvm::BasicBlock* successBlock, llvm::BasicBlock* failBlock);

    /**
     * @brief Tests whether a value is NOT null or zero. If the value passes the condition then the
     * success-block begins, otherwise the fail block begins.
     * @param value The value to evaluate.
     * @param successBlock The success block.
     * @param failBlock The fail block.
     */
    void EvalNot(llvm::Value* value, llvm::BasicBlock* successBlock, llvm::BasicBlock* failBlock);

    /**
     * @brief Queries whether the last instruction of the currently built block is a terminator.
     * @return True if the currently built block ends with a terminator.
     */
    bool CurrentBlockEndsInBranch();
};

/** @class Implements an if construct. */
class JitIf : public JitStatement {
public:
    /**
     * @brief Constructor.
     * @param context The LLVM context.
     * @param builder The LLVM builder.
     * @param jittedFunction The jitted function.
     * @param blockName The if-block name.
     */
    JitIf(llvm::LLVMContext& context, dorado::GsCodeGen::LlvmBuilder* builder, llvm::Function* jittedFunction,
        const char* blockName);

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
    void JitIfCmp(llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp);

    /**
     * @brief Begins an if-block by checking if a value is null or zero. If the value passes the
     * condition then the if-block begins.
     * @param value The value to evaluate.
     */
    void JitIfEval(llvm::Value* value);

    /**
     * @brief Begins an if-block by checking if a value is not null or zero. If the value passes the
     * condition then the if-block begins.
     * @param value The value to evaluate.
     */
    void JitIfNot(llvm::Value* value);

    /** @brief Begins the else-block, in case the two values are not equal. Calling this function is optional. */
    void JitElse();

    /** @brief Terminates the if block. Calling this function is mandatory. */
    void JitEnd();

    /** @brief Retrieves the condition evaluation block. */
    inline llvm::BasicBlock* GetCondBlock()
    {
        return m_condBlock;
    };

    /** @brief Retrieves the condition success execution block. */
    inline llvm::BasicBlock* GetIfBlock()
    {
        return m_ifBlock;
    };

    /** @brief Retrieves the condition failure execution block. */
    inline llvm::BasicBlock* GetElseBlock()
    {
        return m_elseBlock;
    };

    /** @brief Retrieves the post-if execution block. */
    inline llvm::BasicBlock* GetPostBlock()
    {
        return m_postIfBlock;
    };

private:
    llvm::BasicBlock* m_condBlock;
    llvm::BasicBlock* m_ifBlock;
    llvm::BasicBlock* m_elseBlock;
    llvm::BasicBlock* m_postIfBlock;
    bool m_elseBlockUsed;

    /** @brief The next if-block (outer with respect to this block). */
    JitIf* m_next;
};

/** @class Implements a while construct. */
class JitWhile : public JitStatement {
public:
    /**
     * @brief Constructor.
     * @param context The LLVM context.
     * @param builder The LLVM builder.
     * @param jittedFunction The jitted function.
     * @param blockName The if-block name.
     */
    JitWhile(llvm::LLVMContext& context, dorado::GsCodeGen::LlvmBuilder* builder, llvm::Function* jittedFunction,
        const char* blockName);

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
    void JitWhileCmp(llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp);

    /**
     * @brief Performs the while-block test by checking if a value is null or zero. If the value
     * passes the condition then the while exec-block begins.
     * @param value The value to evaluate.
     */
    void JitWhileEval(llvm::Value* value);

    /**
     * @brief Performs the while-block test by checking if a value is not null or zero. If the value
     * passes the condition then the while exec-block begins.
     * @param value The value to evaluate.
     */
    void JitWhileNot(llvm::Value* value);

    /** @brief Stops the current iteration and begins a new one. */
    void JitContinue();

    /** @brief Breaks out of the while loop. */
    void JitBreak();

    /** @brief Terminates the while block. Calling this function is mandatory. */
    void JitEnd();

    /** @brief Retrieves the condition evaluation block. */
    inline llvm::BasicBlock* GetCondBlock()
    {
        return m_condWhileBlock;
    };

    /** @brief Retrieves the condition success execution block. */
    inline llvm::BasicBlock* GetExecBlock()
    {
        return m_execWhileBlock;
    };

    /** @brief Retrieves the post-if execution block. */
    inline llvm::BasicBlock* GetPostBlock()
    {
        return m_postWhileBlock;
    };

private:
    llvm::BasicBlock* m_condWhileBlock;
    llvm::BasicBlock* m_execWhileBlock;
    llvm::BasicBlock* m_postWhileBlock;

    /** @brief The next while-block (outer with respect to this block). */
    JitWhile* m_next;
};

/** @class Implements a do-while construct. */
class JitDoWhile : public JitStatement {
public:
    /**
     * @brief Constructor.
     * @param context The LLVM context.
     * @param builder The LLVM builder.
     * @param jittedFunction The jitted function.
     * @param blockName The if-block name.
     */
    JitDoWhile(llvm::LLVMContext& context, dorado::GsCodeGen::LlvmBuilder* builder, llvm::Function* jittedFunction,
        const char* blockName);

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
    void JitWhileCmp(llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp);

    /**
     * @brief Performs the while-block test by checking if a value is null or zero. If the value
     * passes the condition then the while exec-block begins.
     * @param value The value to evaluate.
     */
    void JitWhileEval(llvm::Value* value);

    /**
     * @brief Performs the while-block test by checking if a value is not null or zero. If the value
     * passes the condition then the while exec-block begins.
     * @param value The value to evaluate.
     */
    void JitWhileNot(llvm::Value* value);

    /** @brief Terminates the do-while block. Calling this function is mandatory. */
    void JitEnd();

    /** @brief Retrieves the condition evaluation block. */
    inline llvm::BasicBlock* GetCondBlock()
    {
        return m_condWhileBlock;
    };

    /** @brief Retrieves the condition success execution block. */
    inline llvm::BasicBlock* GetExecBlock()
    {
        return m_execWhileBlock;
    };

    /** @brief Retrieves the post-if execution block. */
    inline llvm::BasicBlock* GetPostBlock()
    {
        return m_postWhileBlock;
    };

private:
    llvm::BasicBlock* m_execWhileBlock;
    llvm::BasicBlock* m_condWhileBlock;
    llvm::BasicBlock* m_postWhileBlock;

    /** @brief The next do-while-block (outer with respect to this block). */
    JitDoWhile* m_next;
};
}  // namespace llvm

/**************************************
 *      Pseudo return Statement
 **************************************/
/** @define Helper macro for a constant instruction. */
#define JIT_CONST(value) llvm::ConstantInt::get(ctx->INT32_T, (value), true)

/** @define Helper macro for a return statement. */
#define JIT_RETURN(value) ctx->_builder->CreateRet(value)

/** @define Helper macro for a return constant statement. */
#define JIT_RETURN_CONST(value) ctx->_builder->CreateRet(JIT_CONST(value))

/** @define Helper macro for a branch/goto statement. */
#define JIT_GOTO(block) ctx->_builder->CreateBr(block)

/** @define Macro for getting the current block. */
#define JIT_CURRENT_BLOCK() ctx->_builder->GetInsertBlock()

/**************************************
 *            If Statement
 **************************************/
/**
 * @define Macro for starting a native if statement. Make sure to put condition evaluation code
 * after this statement if you wish to jump back to the condition evaluation block.
 */
#define JIT_IF_BEGIN(name) \
    {                      \
        llvm::JitIf jitIf##name(ctx->_code_gen->context(), ctx->_builder, ctx->m_jittedQuery, #name);

/** @define Macro for referring the current if-block. */
#define JIT_IF_CURRENT() llvm::JitIf::GetCurrentStatement()

/**************************************
 *           While Statement
 **************************************/
/** @define Macro for starting a native while statement. */
#define JIT_WHILE_BEGIN(name) \
    {                         \
        llvm::JitWhile jitWhile##name(ctx->_code_gen->context(), ctx->_builder, ctx->m_jittedQuery, #name);

/** @define Macro for referring the current while-block. */
#define JIT_WHILE_CURRENT() llvm::JitWhile::GetCurrentStatement()

/**************************************
 *         Do-While Statement
 **************************************/
/** @define Macro for starting a native do-while statement. */
#define JIT_DO_WHILE_BEGIN(name) \
    {                            \
        llvm::JitDoWhile jitDoWhile##name(ctx->_code_gen->context(), ctx->_builder, ctx->m_jittedQuery, #name);

/** @define Macro for referring the current do-while-block. */
#define JIT_DO_WHILE_CURRENT() llvm::JitDoWhile::GetCurrentStatement()

// the other macros for emitting JIT code are found in jit_util.h
// make sure to include that file after including this file
#endif /* JIT_LLVM_UTIL_H */
