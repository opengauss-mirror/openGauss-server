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

/** @define Access violation run-time fault (attempt to access null pointer, or array index out of bounds). */
#define LLVM_FAULT_ACCESS_VIOLATION 1

/** @define Unhandled exception run-time fault (thrown exception not handled). */
#define LLVM_FAULT_UNHANDLED_EXCEPTION 2

/** @define Resource limit reached during function execution. */
#define LLVM_FAULT_RESOURCE_LIMIT 3

/*---------------------- Exception Codes -------------------*/
/** @define Denotes that there is no exception presently being thrown or handled. */
#define LLVM_EXCEPTION_STATUS_NONE 0

/** @define Denotes that an exception has just been thrown. */
#define LLVM_EXCEPTION_STATUS_THROWN 1

/** @define Denotes that an exception handler is being searched for a thrown exception. */
#define LLVM_EXCEPTION_STATUS_HANDLING 2

// ATTENTION: In order to define all macros just once, we use the same class names. In order to avoid
// name clashes, we include these classes in the llvm name space. Nevertheless, be careful not to
// include this file and jit_util.h as well.
namespace llvm_util {
using namespace dorado;

/** @brief Converts LLVM run-time fault code to string.*/
extern const char* LlvmRuntimeFaultToString(int faultCode);

/** @class RuntimeFaultHandler An interface for handling run-time faults at the function level. */
class LlvmRuntimeFaultHandler {
public:
    /**
     * @brief Handles a run-time fault.
     * @param faultCode The fault code.
     */
    virtual void OnRuntimeFault(uint64_t faultCode) = 0;

protected:
    /** @brief Constructor. */
    LlvmRuntimeFaultHandler()
    {}

    /** @brief Destructor. */
    virtual ~LlvmRuntimeFaultHandler()
    {}
};

/** @class CompileFrame. */
class CompileFrame {
public:
    /** @brief Constructor. */
    explicit CompileFrame(CompileFrame* next) : m_next(next)
    {}

    /** @brief Destructor. */
    ~CompileFrame()
    {}

    inline CompileFrame* GetNext()
    {
        return m_next;
    }

    /**
     * @brief Queries whether the compile frame already contains a local variable by the given name.
     * @param name The local variable name.
     * @return True if this compile frame contains a local variable by the given name, otherwise false.
     */
    inline bool ContainsLocalVariable(const char* name)
    {
        return m_localVarMap.find(name) != m_localVarMap.end();
    }

    inline const char* GetUniqueName()
    {
        char buf[MAX_VAR_NAME_LEN];
        errno_t erc = snprintf_s(buf, MAX_VAR_NAME_LEN, MAX_VAR_NAME_LEN - 1, "unnamed_%u", m_nextVarId++);
        securec_check_ss(erc, "\0", "\0");
        m_uniqueVarName = buf;
        return m_uniqueVarName.c_str();
    }

    /**
     * @brief Defines local variable on the local frame.
     * @param localVar The local variable to define.
     * @return True if succeeded, otherwise false (if frame already contains local variable with the same name).
     */
    bool AddLocalVariable(const char* name, llvm::Value* localVar)
    {
        return m_localVarMap.insert(LocalVarMap::value_type(name, localVar)).second;
    }

    /**
     * @brief Searches a local variable in the frame.
     * @param name The name of the local variable.
     * @return The local variable, or null if not found.
     */
    llvm::Value* GetLocalVariable(const char* name)
    {
        llvm::Value* result = nullptr;
        LocalVarMap::iterator itr = m_localVarMap.find(name);
        if (itr != m_localVarMap.end()) {
            result = itr->second;
        }
        return result;
    }

private:
    static constexpr uint32_t MAX_VAR_NAME_LEN = 32;

    CompileFrame* m_next;

    /** @typedef Local variable map type. */
    using LocalVarMap = std::map<std::string, llvm::Value*>;

    /** @var Local variable map. */
    LocalVarMap m_localVarMap;

    /** @var Buffer for next unique variable name. */
    std::string m_uniqueVarName;

    /** @var Identifier for next unique variable name. */
    static uint32_t m_nextVarId;
};

/** @struct Utility structure for LLVM code-generation. */
struct LlvmCodeGenContext {
    /** @var The Gauss code-generation context. */
    GsCodeGen* m_codeGen;

    /** @var The LLVM builder. */
    GsCodeGen::LlvmBuilder* m_builder;

    /** @var The currently built function. */
    llvm::Function* m_jittedFunction;

    // primitive types
    llvm::IntegerType* BOOL_T;
    llvm::IntegerType* INT1_T;  // for IR assembly "select"
    llvm::IntegerType* INT8_T;
    llvm::IntegerType* INT16_T;
    llvm::IntegerType* INT32_T;
    llvm::IntegerType* INT64_T;
    llvm::Type* VOID_T;
    llvm::Type* FLOAT_T;
    llvm::Type* DOUBLE_T;
    llvm::PointerType* STR_T;
    llvm::IntegerType* DATUM_T;

    // helper functions required to support LLVM simple exceptions
    llvm::FunctionCallee m_llvmPushExceptionFrameFunc;
    llvm::FunctionCallee m_llvmGetCurrentExceptionFrameFunc;
    llvm::FunctionCallee m_llvmPopExceptionFrameFunc;
    llvm::FunctionCallee m_llvmThrowExceptionFunc;
    llvm::FunctionCallee m_llvmRethrowExceptionFunc;
    llvm::FunctionCallee m_llvmGetExceptionValueFunc;
    llvm::FunctionCallee m_llvmGetExceptionStatusFunc;
    llvm::FunctionCallee m_llvmResetExceptionStatusFunc;
    llvm::FunctionCallee m_llvmResetExceptionValueFunc;
    llvm::FunctionCallee m_llvmUnwindExceptionFrameFunc;
    llvm::FunctionCallee m_llvmSetJmpFunc;
    llvm::FunctionCallee m_llvmLongJmpFunc;
#ifdef MOT_JIT_DEBUG
    llvm::FunctionCallee m_llvmDebugPrintFunc;
    llvm::FunctionCallee m_llvmDebugPrintFrameFunc;
#endif

    // invoke built-ins
    llvm::FunctionCallee m_invokePGFunction0Func;
    llvm::FunctionCallee m_invokePGFunction1Func;
    llvm::FunctionCallee m_invokePGFunction2Func;
    llvm::FunctionCallee m_invokePGFunction3Func;
    llvm::FunctionCallee m_invokePGFunctionNFunc;

    // invoke MOT memory allocation
    llvm::FunctionCallee m_memSessionAllocFunc;
    llvm::FunctionCallee m_memSessionFreeFunc;

    /** @var The current compile-time frame. */
    CompileFrame* m_currentCompileFrame;
};

extern void OpenCompileFrame(LlvmCodeGenContext* ctx);

extern void CloseCompileFrame(LlvmCodeGenContext* ctx);

extern void CleanupCompileFrames(LlvmCodeGenContext* ctx);

/** @brief Reset compile state for LLVM (cleanup in case of error). */
extern void JitResetCompileState();

/**
 * @brief Helper function for defining LLVM external function.
 * @param module The LLVM module into which the function declaration is to be added.
 * @param returnType The return type of the function.
 * @param name The name of the function (must match the actual function name, otherwise linking fails).
 * @param ... Function parameter list. List must be terminated with NULL.
 * @return The resulting function definition.
 */
extern llvm::FunctionCallee DefineFunction(llvm::Module* module, llvm::Type* returnType, const char* name, ...);

/**
 * @brief Adds a function call.
 * @param ctx The code-generation context.
 * @param func The invoked function definition.
 * @param ... Function argument list. List must be terminated with NULL.
 * @return The return value of the function.
 */
extern llvm::Value* AddFunctionCall(LlvmCodeGenContext* ctx, llvm::FunctionCallee func, ...);

/**
 * @brief Adds a function call at the specified position.
 * @param before The instruction before which the function call is to be added.
 * @param ctx The code-generation context.
 * @param func The invoked function definition.
 * @param ... Function argument list. List must be terminated with NULL.
 * @return The return value of the function.
 */
extern llvm::Value* InsertFunctionCall(
    llvm::Instruction* before, LlvmCodeGenContext* ctx, llvm::FunctionCallee func, ...);

/** @brief Adds invocation of PG function with no arguments. */
extern llvm::Value* AddInvokePGFunction0(LlvmCodeGenContext* ctx, PGFunction funcPtr, Oid collationId);

/** @brief Adds invocation of PG function with 1 argument. */
extern llvm::Value* AddInvokePGFunction1(LlvmCodeGenContext* ctx, PGFunction funcPtr, Oid collationId, bool isStrict,
    llvm::Value* arg, llvm::Value* isNull, Oid argType);

/** @brief Adds invocation of PG function with 2 arguments. */
extern llvm::Value* AddInvokePGFunction2(LlvmCodeGenContext* ctx, PGFunction funcPtr, Oid collationId, bool isStrict,
    llvm::Value* arg1, llvm::Value* isNull1, Oid argType1, llvm::Value* arg2, llvm::Value* isNull2, Oid argType2);

/** @brief Adds invocation of PG function with 3 arguments. */
extern llvm::Value* AddInvokePGFunction3(LlvmCodeGenContext* ctx, PGFunction funcPtr, Oid collationId, bool isStrict,
    llvm::Value* arg1, llvm::Value* isNull1, Oid argType1, llvm::Value* arg2, llvm::Value* isNull2, Oid argType2,
    llvm::Value* arg3, llvm::Value* isNull3, Oid argType3);

/** @brief Adds invocation of PG function with 4 or more arguments. */
extern llvm::Value* AddInvokePGFunctionN(LlvmCodeGenContext* ctx, PGFunction funcPtr, Oid collationId, bool isStrict,
    llvm::Value** args, llvm::Value** isNull, Oid* argTypes, int argCount);

/** @brief Adds call to allocate session memory. */
extern llvm::Value* AddMemSessionAlloc(LlvmCodeGenContext* ctx, llvm::Value* allocSize);

/** @brief Adds call to free session memory. */
extern void AddMemSessionFree(LlvmCodeGenContext* ctx, llvm::Value* ptr);

/**
 * @brief Initializes a code-generation context.
 * @param ctx The context object to initialize.
 * @param codeGen The GaussDB code-generation object for LLVM.
 * @param builder The LLVM builder.
 */
extern void InitLlvmCodeGenContext(LlvmCodeGenContext* ctx, GsCodeGen* codeGen, GsCodeGen::LlvmBuilder* builder);

/** @brief Destroys a code-generation context. */
extern void DestroyLlvmCodeGenContext(LlvmCodeGenContext* ctx);

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
        GsCodeGen::LlvmBuilder* builder, llvm::Value* lhs, llvm::Value* rhs, JitExec::JitICmpOp cmpOp);

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
    /** @var The code-generation context. */
    LlvmCodeGenContext* m_codegenContext;

    /** @var The LLVM context. */
    llvm::LLVMContext& m_context;

    /** @var The LLVM builder. */
    GsCodeGen::LlvmBuilder* m_builder;

    /**
     * @brief Constructor.
     * @param codegenContext The code-generation context.
     */
    explicit JitStatement(LlvmCodeGenContext* codegenContext);

    /** @brief Destructor. */
    virtual ~JitStatement();

    /**
     * @brief Creates an instruction block/
     * @param blockBaseName The base-name of the block.
     * @param suffix The suffix to add to the block name.
     * @return The created block, or null if failed.
     */
    llvm::BasicBlock* MakeBlock(const char* blockBaseName, const char* suffix);

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
     * @param codegenContext The code-generation context.
     * @param blockName The if-block name.
     */
    JitIf(LlvmCodeGenContext* codegenContext, const char* blockName);

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
    llvm::BasicBlock* m_ifBlock;
    llvm::BasicBlock* m_elseBlock;
    llvm::BasicBlock* m_postIfBlock;
    bool m_elseBlockUsed;

    /** @brief The next if-block (outer with respect to this block). */
    JitIf* m_next;
};

class JitLoop : public JitStatement {
public:
    /** @brief Retrieves the name of the loop. */
    inline const char* GetName() const
    {
        return m_name.c_str();
    }

    /** @brief Retrieves the current (innermost if nested) loop. */
    static JitLoop* GetCurrentStatement();

    /** @brief Stops the current iteration and begins a new one. */
    virtual void JitContinue() = 0;

    /** @brief Breaks out of the while loop. */
    virtual void JitBreak() = 0;

    /** @brief Retrieves the condition evaluation block of the loop. */
    virtual llvm::BasicBlock* GetCondBlock() = 0;

    /** @brief Retrieves the post-loop execution block. */
    virtual llvm::BasicBlock* GetPostBlock() = 0;

protected:
    /**
     * @brief Constructor.
     * @param codegenContext The code-generation context.
     * @param name The loop name. Be careful not to give duplicate names to different loops in the same chain of
     * nested loops (causes debug-build assert).
     */
    JitLoop(LlvmCodeGenContext* codegenContext, const char* name);

    /** @brief Destructor. */
    ~JitLoop() override;

private:
    /** @brief The loop name. */
    std::string m_name;

    /** @brief The next loop-block (outer with respect to this block). */
    JitLoop* m_next;
};

/** @class Implements a while construct. */
class JitWhile : public JitLoop {
public:
    /**
     * @brief Constructor.
     * @param codegenContext The code-generation context.
     * @param blockName The if-block name.
     */
    JitWhile(LlvmCodeGenContext* codegenContext, const char* blockName);

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
    void JitContinue() override;

    /** @brief Breaks out of the while loop. */
    void JitBreak() override;

    /** @brief Terminates the while block. Calling this function is mandatory. */
    void JitEnd();

    /** @brief Retrieves the condition evaluation block. */
    inline llvm::BasicBlock* GetCondBlock()
    {
        return m_condWhileBlock;
    }

    /** @brief Retrieves the condition success execution block. */
    inline llvm::BasicBlock* GetExecBlock()
    {
        return m_execWhileBlock;
    }

    /** @brief Retrieves the post-if execution block. */
    inline llvm::BasicBlock* GetPostBlock()
    {
        return m_postWhileBlock;
    }

private:
    llvm::BasicBlock* m_condWhileBlock;
    llvm::BasicBlock* m_execWhileBlock;
    llvm::BasicBlock* m_postWhileBlock;

    /** @brief The next while-block (outer with respect to this block). */
    JitWhile* m_next;
};

/** @class Implements a do-while construct. */
class JitDoWhile : public JitLoop {
public:
    /**
     * @brief Constructor.
     * @param codegenContext The code-generation context.
     * @param blockName The if-block name.
     */
    JitDoWhile(LlvmCodeGenContext* codegenContext, const char* blockName);

    /** @brief Destructor. */
    ~JitDoWhile() override;

    /** @brief Retrieves the current (innermost if nested) do-while-block. */
    static JitDoWhile* GetCurrentStatement();

    /** @brief Stops the current iteration and begins a new one by first evaluating the condition. */
    void JitContinue() override;

    /** @brief Breaks out of the do-while loop. */
    void JitBreak() override;

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

/** @class Implements a for-loop construct. */
class JitFor : public JitLoop {
public:
    /**
     * @brief Constructor.
     * @param codegenContext The code-generation context.
     * @param blockName The for-loop block name.
     * @param counterName The loop counter name (type is derived from init_value).
     * @param initValue The initial counter value.
     * @param step The increment step of the loop counter.
     * @param bound The loop counter bound.
     * @param cmpOp The comparison operation to use when comparing the loop counter with the loop bound.
     */
    JitFor(LlvmCodeGenContext* codegenContext, const char* blockName, const char* counterName, llvm::Value* initValue,
        llvm::Value* step, llvm::Value* bound, JitExec::JitICmpOp cmpOp);

    /**
     * @brief Constructor.
     * @param codegenContext The code-generation context.
     * @param blockName The for-loop block name.
     * @param counterVar The loop counter variable, externally provided by the caller (type should match type of
     * initValue).
     * @param initValue The initial counter value.
     * @param step The increment step of the loop counter.
     * @param bound The loop counter bound.
     * @param cmpOp The comparison operation to use when comparing the loop counter with the loop bound.
     */
    JitFor(LlvmCodeGenContext* codegenContext, const char* blockName, llvm::Value* counterVar, llvm::Value* initValue,
        llvm::Value* step, llvm::Value* bound, JitExec::JitICmpOp cmpcmpOp_op);

    /** @brief Destructor. */
    ~JitFor() override;

    /** @brief Retrieves the current (innermost if nested) for-loop-block. */
    static JitFor* GetCurrentStatement();

    /** @brief Retrieves the counter value. */
    llvm::Value* GetCounter();

    /**
     * @brief Stops the current iteration and begins a new one by incrementing the loop counter and then evaluating
     * the loop condition.
     */
    void JitContinue() override;

    /** @brief Breaks out of the for loop. */
    void JitBreak() override;

    /** @brief Terminates the do-while block. Calling this function is mandatory. */
    void JitEnd();

    /** @brief Retrieves the counter initialization block. */
    inline llvm::BasicBlock* GetInitBlock()
    {
        return m_initForCounterBlock;
    }

    /** @brief Retrieves the condition evaluation block. */
    llvm::BasicBlock* GetCondBlock() override
    {
        return m_condForBlock;
    }

    /** @brief Retrieves the for-loop body execution block. */
    inline llvm::BasicBlock* GetExecBlock()
    {
        return m_execForBodyBlock;
    }

    /** @brief Retrieves the counter increment block. */
    inline llvm::BasicBlock* GetIncrementBlock()
    {
        return m_incrementForCounterBlock;
    }

    /** @brief Retrieves the post-for execution block. */
    llvm::BasicBlock* GetPostBlock() override
    {
        return m_postForBlock;
    }

private:
    llvm::BasicBlock* m_initForCounterBlock;
    llvm::BasicBlock* m_condForBlock;
    llvm::BasicBlock* m_execForBodyBlock;
    llvm::BasicBlock* m_incrementForCounterBlock;
    llvm::BasicBlock* m_postForBlock;

    llvm::Value* m_counterValue;
    llvm::Value* m_step;
    llvm::Value* m_bound;
    JitExec::JitICmpOp m_cmpOp;

    /** @brief The next for-loop-block (outer with respect to this block). */
    JitFor* m_next;

    void Init(const char* blockName, const char* counterName, llvm::Value* counterVar, llvm::Value* initValue);
};

/** @class Implements a switch-case construct. */
class JitSwitchCase : public JitStatement {
public:
    /**
     * @brief Constructor.
     * @param codegenContext The code-generation context.
     * @param blockName The do-while-block name.
     * @param switchValue The tested value.
     */
    JitSwitchCase(LlvmCodeGenContext* codegenContext, const char* blockName, llvm::Value* switchValue);

    /** @brief Destructor. */
    ~JitSwitchCase() override;

    /** @brief Retrieves the current (innermost if nested) for-loop-block. */
    static JitSwitchCase* GetCurrentStatement();

    /** @brief Starts a switch-case-block. */
    void JitCase(llvm::Value* caseValue);

    /** @brief Adds a case value to a switch-case block with multiple values (batch a few cases together). */
    void JitCaseMany(llvm::Value* caseValue);

    /** @brief Ends a switch-case block with multiple values. */
    void JitCaseManyTerm();

    /** @brief Terminates a switch-case block. */
    void JitBreak();

    /** @brief Ends a case block. */
    void JitCaseEnd();

    /** @brief Starts the default switch-case-block. */
    void JitDefault();

    /** @brief Terminates the switch-case statement. Calling this function is mandatory. */
    void JitEnd();

    /** @brief Retrieves the counter initialization block. */
    inline llvm::BasicBlock* GetDefaultCaseBlock()
    {
        return m_defaultCaseBlock;
    }

    /** @brief Retrieves the post-for execution block. */
    inline llvm::BasicBlock* GetPostBlock()
    {
        return m_postSwitchBlock;
    }

private:
    std::string m_blockName;
    llvm::Value* m_switchValue;
    llvm::BasicBlock* m_caseBlock;
    llvm::BasicBlock* m_postCaseBlock;
    llvm::BasicBlock* m_defaultCaseBlock;
    llvm::BasicBlock* m_postSwitchBlock;

    /** @brief The next switch-case-block (outer with respect to this block). */
    JitSwitchCase* m_next;
};

/** @class Implements a try-catch construct. */
class JitTryCatch : public JitStatement {
public:
    /**
     * @brief Constructor.
     * @param codegenContext The code-generation context.
     * @param blockName The try-catch-block name.
     */
    JitTryCatch(LlvmCodeGenContext* codegenContext, const char* blockName);

    /** @brief Destructor. */
    ~JitTryCatch() override;

    /** @brief Retrieves the current (innermost if nested) try-catch-block. */
    static JitTryCatch* GetCurrentStatement();

    /** @brief Throws a value. */
    void JitThrow(llvm::Value* exceptionValue);

    /** @brief Re-throws the recent exception value (use only inside catch block). */
    void JitRethrow();

    /** @brief Begins a catch block. */
    void JitBeginCatch(llvm::Value* value);

    /**
     * @brief Begins a catch block of multiple values. Can be called several times with multiple values. When all case
     * values are specified, a call to @ref JitTryCatch::JitEndCatchMany() should be made (after which the case body
     * should be entered. This call should be finally matched with a call to @ref JitTryCatch::JitEndCatch().
     */
    void JitBeginCatchMany(llvm::Value* value);

    /** @brief Ends a "catch multiple values" statement and begins the catch body block. */
    void JitCatchManyTerm();

    /** @brief Terminates a catch block. */
    void JitEndCatch();

    /** @brief Starts the catch-all block. */
    void JitCatchAll();

    /** @brief Terminates the try-catch statement. Calling this function is mandatory. */
    void JitEnd();

    /** @brief Retrieves the post-for execution block. */
    inline llvm::BasicBlock* GetPostBlock()
    {
        return m_postTryCatchBlock;
    }

    /** @brief Queries whether there is a pending exception. */
    llvm::Value* JitGetExceptionStatus();

private:
    std::string m_blockName;
    JitIf* m_ifTry;
    JitSwitchCase* m_exceptionSwitchCase;
    llvm::BasicBlock* m_beginTryBlock;
    llvm::BasicBlock* m_postTryCatchBlock;

    /** @brief The next try-catch-block (outer with respect to this block). */
    JitTryCatch* m_next;

    /** @brief Make preparations for exception block. */
    void PrepareExceptionBlock();
};
}  // namespace llvm_util

#define JIT_ASSERT_LLVM_CODEGEN_UTIL_VALID()           \
    do {                                               \
        MOT_ASSERT(JIT_IF_CURRENT() == nullptr);       \
        MOT_ASSERT(JIT_WHILE_CURRENT() == nullptr);    \
        MOT_ASSERT(JIT_DO_WHILE_CURRENT() == nullptr); \
        MOT_ASSERT(JIT_FOR_CURRENT() == nullptr);      \
        MOT_ASSERT(JIT_SWITCH_CURRENT() == nullptr);   \
        MOT_ASSERT(JIT_TRY_CURRENT() == nullptr);      \
    } while (0)

/**************************************
 *      Return Statement
 **************************************/
/** @define Helper macro for a constant instruction. */
#define JIT_CONST_CHAR(value) llvm::ConstantInt::get(ctx->INT8_T, (value), true)
#define JIT_CONST_BOOL(value) llvm::ConstantInt::get(ctx->BOOL_T, (value), true)
#define JIT_CONST_INT1(value) llvm::ConstantInt::get(ctx->INT1_T, (value), true)
#define JIT_CONST_INT8(value) llvm::ConstantInt::get(ctx->INT8_T, (value), true)
#define JIT_CONST_INT16(value) llvm::ConstantInt::get(ctx->INT16_T, (value), true)
#define JIT_CONST_INT32(value) llvm::ConstantInt::get(ctx->INT32_T, (value), true)
#define JIT_CONST_INT64(value) llvm::ConstantInt::get(ctx->INT64_T, (value), true)
#define JIT_CONST_UINT8(value) llvm::ConstantInt::get(ctx->INT8_T, (value), false)
#define JIT_CONST_UINT16(value) llvm::ConstantInt::get(ctx->INT16_T, (value), false)
#define JIT_CONST_UINT32(value) llvm::ConstantInt::get(ctx->INT32_T, (value), false)
#define JIT_CONST_UINT64(value) llvm::ConstantInt::get(ctx->INT64_T, (value), false)
#define JIT_CONST_FLOAT(value) llvm::ConstantFP::get(ctx->FLOAT_T, (value), true)
#define JIT_CONST_DOUBLE(value) llvm::ConstantFP::get(ctx->DOUBLE_T, (value), true)

/** @define Helper macro for a return statement. */
#define JIT_RETURN(value) ctx->m_builder->CreateRet(value)

/** @define Helper macro for defining a basic block. */
#define JIT_DEFINE_BLOCK(blockName) \
    llvm::BasicBlock* blockName = llvm::BasicBlock::Create(ctx->m_codeGen->context(), #blockName)

/** @define Helper macro for a branch/goto statement. */
#define JIT_GOTO(block) ctx->m_builder->CreateBr(block)

/** @define Macro for getting the current block. */
#define JIT_CURRENT_BLOCK() ctx->m_builder->GetInsertBlock()

/**************************************
 *            If Statement
 **************************************/
/**
 * @define Macro for starting a native if statement. Make sure to put condition evaluation code
 * after this statement if you wish to jump back to the condition evaluation block.
 */
#define JIT_IF_BEGIN(name) \
    {                      \
        llvm_util::JitIf jitIf(ctx, #name);

/** @define Macro similar to @ref JIT_FOR_BEGIN(), but with string values (either quoted constant or pointer). */
#define JIT_IF_BEGIN_V(name) \
    {                        \
        llvm_util::JitIf jitIf(ctx, name);

/** @define Macro for referring the current if-block. */
#define JIT_IF_CURRENT() llvm_util::JitIf::GetCurrentStatement()

/**************************************
 *           While Statement
 **************************************/
/** @define Macro for starting a native while statement. */
#define JIT_WHILE_BEGIN(name) \
    {                         \
        llvm_util::JitWhile jitWhile(ctx, #name);

/** @define Macro similar to @ref JIT_WHILE_BEGIN(), but with string values (either quoted constant or pointer). */
#define JIT_WHILE_BEGIN_V(name) \
    {                           \
        llvm_util::JitWhile jitWhile(ctx, name);

/** @define Macro for referring the current while-block. */
#define JIT_WHILE_CURRENT() llvm_util::JitWhile::GetCurrentStatement()

/**************************************
 *         Do-While Statement
 **************************************/
/** @define Macro for starting a native do-while statement. */
#define JIT_DO_WHILE_BEGIN(name) \
    {                            \
        llvm::JitDoWhile jitDoWhile(ctx, #name);

/** @define Macro similar to @ref JIT_DO_WHILE_BEGIN(), but with string values (either quoted constant or pointer). */
#define JIT_DO_WHILE_BEGIN_V(name) \
    {                              \
        llvm_util::JitDoWhile jitDoWhile(ctx, name);

/** @define Macro for referring the current do-while-block. */
#define JIT_DO_WHILE_CURRENT() llvm_util::JitDoWhile::GetCurrentStatement()

/**************************************
 *         For Statement
 **************************************/

/** @define Macro for starting a LLVM for statement. */
#define JIT_FOR_BEGIN(name, counterName, initValue, step, bound, cmpOp) \
    {                                                                   \
        llvm_util::JitFor jitFor(ctx, #name, #counterName, initValue, step, bound, cmpOp);

/** @define Macro similar to @ref JIT_FOR_BEGIN(), but with string values (either quoted constant or pointer). */
#define JIT_FOR_BEGIN_V(name, counterName, initValue, step, bound, cmpOp) \
    {                                                                     \
        llvm_util::JitFor jitFor(ctx, name, counterName, initValue, step, bound, cmpOp);

/** @define Macro similar to @ref JIT_FOR_BEGIN(), but with pointer to counter variable. */
#define JIT_FOR_BEGIN_X(name, counterVar, initValue, step, bound, cmpOp) \
    {                                                                    \
        llvm_util::JitFor jitFor(ctx, #name, counterVar, initValue, step, bound, cmpOp);

/** @define Macro for referring the current for-loop-block. */
#define JIT_FOR_CURRENT() llvm_util::JitFor::GetCurrentStatement()

/** @define Macro for getting the condition evaluation block of the current for statement. */
#define JIT_FOR_INCREMENT_BLOCK() JIT_FOR_CURRENT()->GetIncrementBlock()

/** @define Macro for getting the block after the current for statement. */
#define JIT_FOR_POST_BLOCK() JIT_FOR_CURRENT()->GetPostBlock()

/**************************************
 *         Loop Macros
 **************************************/

/** @define Macro for getting the current (innermost if nested) loop. */
#define JIT_LOOP_CURRENT() llvm_util::JitLoop::GetCurrentStatement()

/**************************************
 *         Switch-Case Statement
 **************************************/

/** @define Macro for starting a LLVM for statement. */
#define JIT_SWITCH_BEGIN(name, value) \
    {                                 \
        llvm_util::JitSwitchCase jitSwitchCase(ctx, #name, value);

/** @define Macro similar to @ref JIT_SWITCH_BEGIN(), but with string values (either quoted constant or pointer). */
#define JIT_SWITCH_BEGIN_V(name, value) \
    {                                   \
        llvm_util::JitSwitchCase jitSwitchCase(ctx, name, value);

/** @define Macro for referring the current switch-case-block. */
#define JIT_SWITCH_CURRENT() llvm_util::JitSwitchCase::GetCurrentStatement()

/**************************************
 *         Try-Catch Statement
 **************************************/

/** @define Macro for starting a LLVM for statement. */
#define JIT_TRY_BEGIN(name) \
    {                       \
        llvm_util::JitTryCatch jitTryCatch((llvm_util::LlvmCodeGenContext*)(ctx), #name);

/** @define Macro similar to @ref JIT_TRY_BEGIN(), but with string values (either quoted constant or pointer). */
#define JIT_TRY_BEGIN_V(name) \
    {                         \
        llvm_util::JitTryCatch jitTryCatch(ctx, name);

/** @define Macro for referring the current switch-case-block. */
#define JIT_TRY_CURRENT() llvm_util::JitTryCatch::GetCurrentStatement()

/** @define Helper for managing exceptions signaled in helper functions. */
#define JIT_EXCEPTION_STATUS() JIT_TRY_CURRENT()->JitGetExceptionStatus()

// the other macros for emitting JIT code are found in jit_util.h
// make sure to include that file after including this file
#endif /* JIT_LLVM_UTIL_H */
