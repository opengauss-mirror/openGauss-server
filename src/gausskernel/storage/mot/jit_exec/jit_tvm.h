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
 * jit_tvm.h
 *    JIT TVM (Tiny Virtual Machine).
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_tvm.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_TVM_H
#define JIT_TVM_H

#include <stdint.h>
#include "postgres.h"
#include "nodes/params.h"
#include "executor/tuptable.h"
#include "storage/mot/jit_def.h"
#include "debug_utils.h"
#include "logger.h"

#include <vector>
#include <list>
#include <assert.h>

// forward declaration
namespace JitExec {
struct JitContext;
}

namespace tvm {  // Tiny Virtual Machine
class Function;
class BasicBlock;

/****************************************
 * Execution Context and Register API
 ****************************************/
/** @struct PTVM execution context. */
struct ExecContext {
    // call parameters
    /** @var The original execution context. */
    JitExec::JitContext* _jit_context;

    /** @var The parameters with which the query was invoked. */
    ParamListInfo _params;

    /** @var The result tuple, into which a result record is written. */
    TupleTableSlot* _slot;

    /** @var The number of tuples processed. */
    uint64_t* _tp_processed;

    /** @var Specifies whether a range scan ended. */
    int* _scan_ended;

    /** @var Specifies whether this is a new scan or a continued previous scan. */
    int m_newScan;

    // Pseudo-LLVM execution info
    /** @var The result code of last expression evaluation. */
    int64_t _expr_rc;

    /** @var Local variable for the number of rows processed. */
    uint64_t _rows_processed;

    /** @var The number of registers used by the function. */
    uint64_t _register_count;

    /** @var The function registers. */
    uint64_t _registers[0];
};

/**
 * @brief Allocates an execution context for a Pseudo-LLVM function on the memory allocator of the
 * current session.
 * @param register_count The number of registers used by the function.
 * @return The allocated context or NULL if failed.
 */
extern ExecContext* allocExecContext(uint64_t register_count);

/**
 * @brief Deallocates a pseudo-LLVM execution context.
 * @param ctx The context to deallocate.
 */
extern void freeExecContext(ExecContext* ctx);

/**
 * @brief Sets a value in the register array for the given execution context.
 * @param exec_context The execution context.
 * @param register_ref The register index.
 * @param value The value to set.
 */
extern void setRegisterValue(ExecContext* exec_context, int register_ref, uint64_t value);

/**
 * @brief Gets a value from the register array in the given execution context.
 * @param exec_context The execution context.
 * @param register_ref The register index.
 * @return The value of the register.
 */
extern uint64_t getRegisterValue(ExecContext* exec_context, int register_ref);

/****************************************
 *        TVM Building Blocks
 ****************************************/
/*-------------------------------  Instruction -------------------------------*/
/**
 * @class Instruction. An instruction is an executable object. An instruction can return a value
 * that is saved in a register.
 */
class Instruction {
public:
    /** @enum Instruction type. */
    enum Type {
        /** @var Regular instruction returning a value. */
        Regular,

        /** @var Instruction without return value. */
        Void,

        /** @var A branch instruction (flow control). */
        Branch,

        /** @var A return statement. */
        Return,

        /** @var A register reference instruction. */
        RegisterRef,

        /** @var A return-next-tuple instruction (next function invocation continues from the same place). */
        ReturnNext
    };

    /** @brief Destructor. */
    virtual ~Instruction()
    {}

    /**
     * @brief Executes the instruction.
     * @param execContext The execution context.
     * @return The execution result, or zero if instruction does not return a value. The returned
     * value is saved in the register associated with the instruction.
     */
    virtual uint64_t Exec(ExecContext* execContext);

    /** @brief Retrieves the instruction type. */
    inline Type GetType() const
    {
        return _type;
    }

    /** @brief Dumps the instruction into the standard error stream. */
    virtual void Dump();

    /**
     * @brief Sets the index of the register with which the instruction is associated.
     * @param registerRef
     */
    inline void SetRegisterRef(int registerRef)
    {
        _register_ref = registerRef;
    }

    /** @brief Retrieves the index of the register with which the instruction is associated. */
    inline int GetRegisterRef() const
    {
        return _register_ref;
    }

    /** @brief Retrieves the number of sub-instructions referred to by this instruction. */
    inline int GetSubInstructionCount() const
    {
        return _sub_instructions.size();
    }

    /**
     * @brief Retrieves the sub-instruction of this instruction at the specified index.
     * @param index The sub-instruction index.
     * @return The sub-instruction.
     */
    inline Instruction* GetSubInstructionAt(int index)
    {
        MOT_ASSERT(index < (int)_sub_instructions.size());
        return _sub_instructions[index];
    }

protected:
    /**
     * @brief Constructor.
     * @param type The instruction type.
     */
    explicit Instruction(Type type = Regular) : _type(type), _register_ref(-1)
    {}

    /**
     * @brief Implements the execution of the instruction. Must be overridden by sub-classes.
     * @param execContext
     * @return
     */
    virtual uint64_t ExecImpl(ExecContext* execContext)
    {
        MOT_ASSERT(false);
        return 0;
    }

    /** @brief Implements dumping the instruction to standard error stream. */
    virtual void DumpImpl()
    {}

    /**
     * @brief Adds a sub-instruction to this instruction.
     * @param sub_instruction The sub-instruction to add.
     * @note Every derived class who refers to sub-instruction must update the @ref Instruction
     * base-class, otherwise function verification might fail, or result in unexpected runtime behavior.
     */
    inline void AddSubInstruction(Instruction* sub_instruction)
    {
        _sub_instructions.push_back(sub_instruction);
    }

    DECLARE_CLASS_LOGGER()

private:
    /** @var The instruction type. */
    Type _type;

    /** @var The register with which the instruction is associated. */
    int _register_ref;

    /** @typedef Instruction list type. */
    typedef std::vector<Instruction*> InstructionList;

    /** @var The list of sub-instruction to which this instruction refers. */
    InstructionList _sub_instructions;
};

/*---------------------------------  Expression --------------------------------*/
/**
 * @class Expression. An expression is an executable object that results in a Datum value. The
 * result of the execution is NOT saved in a register.
 */
class Expression {
public:
    /** @brief Destructor. */
    virtual ~Expression()
    {}

    /** @brief Queries whether the evaluation of the expression can fail. */
    inline bool canFail() const
    {
        return (_eval_result == CanFail);
    }

    /**
     * @brief Evaluates the expression.
     * @param exec_context The execution context.
     * @return The evaluation result.
     */
    virtual Datum eval(ExecContext* exec_context) = 0;

    /** @brief Dumps the expression to the standard error stream. */
    virtual void dump() = 0;

protected:
    /** @enum Fallibility Constants that specify whether expression evaluation can fail. */
    enum EvalResult {
        /** @var Denotes that expression evaluation can fail. */
        CanFail,

        /** @var Denotes that expression evaluation cannot fail. */
        CannotFail
    };

    /**
     * @brief Constructor.
     * @param eval_result Specifies whether the evaluation of the expression can fail.
     */
    explicit Expression(EvalResult eval_result) : _eval_result(eval_result)
    {}

    DECLARE_CLASS_LOGGER()

private:
    /** @var Specifies whether the evaluation of the expression can fail. */
    EvalResult _eval_result;
};

/*---------------------------------  ConstExpression --------------------------------*/
/** @class ConstExpression. An expression denoting a constant value. */
class ConstExpression : public Expression {
public:
    /**
     * @brief Constructor.
     * @param datum The constant value.
     * @param arg_pos The position of the expression in a compound expression.
     * @param is_null Specifies whether this is a null value.
     */
    ConstExpression(Datum datum, int arg_pos, int is_null)
        : Expression(Expression::CannotFail), _datum(datum), _arg_pos(arg_pos), _is_null(is_null)
    {}

    /** @brief Destructor. */
    ~ConstExpression() override
    {}

    /**
     * @brief Evaluates the constant expression.
     * @param exec_context The execution context.
     * @return The constant value. As a side effect the argument-is-null array in the JIT execution
     * context is updated at @ref _arg_pos position according the null-status of the result.
     */
    Datum eval(ExecContext* exec_context) override;

    /** @brief Dumps the expression to the standard error stream. */
    void dump() override;

private:
    /** @var The constant value. */
    Datum _datum;

    /** @var The position of the expression in a compound expression. */
    int _arg_pos;

    /** @var Specifies whether this is a null value. */
    int _is_null;
};

/*---------------------------------  ParamExpression --------------------------------*/
/** @class ParamExpression. An expression that evaluates to the parameter at the specified index. */
class ParamExpression : public Expression {
public:
    /**
     * @brief Constructor.
     * @param param_id The zero-based index of the parameter.
     * @param arg_pos The position of the expression in a compound expression.
     */
    ParamExpression(int param_id, int arg_pos) : Expression(Expression::CanFail), _param_id(param_id), _arg_pos(arg_pos)
    {}

    /** @brief Destructor. */
    ~ParamExpression() override
    {}

    /**
     * @brief Evaluates the parameter expression.
     * @param exec_context The execution context.
     * @return The parameter value. As a side effect the argument-is-null array in the JIT execution
     * context is updated at @ref _arg_pos position according the null-status of the result.
     */
    Datum eval(ExecContext* exec_context) override;

    /** @brief Dumps the expression to the standard error stream. */
    void dump() override;

private:
    /** @var The zero-based index of the parameter. */
    int _param_id;  // already zero-based

    /** @var The position of the expression in a compound expression. */
    int _arg_pos;
};

/*---------------------------------  AndExpression --------------------------------*/
/** @class AndExpression. An expression that evaluates to boolean AND of two sub-expressions. */
class AndExpression : public Expression {
public:
    /**
     * @brief Constructor.
     * @param lhs The left-hand-side sub-expression.
     * @param rhs The right-hand-side sub-expression.
     */
    AndExpression(Expression* lhs, Expression* rhs) : Expression(Expression::CanFail), _lhs(lhs), _rhs(rhs)
    {}

    /** @brief Destructor. */
    ~AndExpression() override
    {}

    /**
     * @brief Evaluates the boolean expression.
     * @param exec_context The execution context.
     * @return The boolean result.
     */
    Datum eval(ExecContext* exec_context) override;

    /** @brief Dumps the expression to the standard error stream. */
    void dump() override;

private:
    /** @var The left-hand-side sub-expression. */
    Expression* _lhs;

    /** @var The right-hand-side sub-expression. */
    Expression* _rhs;
};

/*---------------------------------  OrExpression --------------------------------*/
/** @class OrExpression. An expression that evaluates to boolean OR of two sub-expressions. */
class OrExpression : public Expression {
public:
    /**
     * @brief Constructor.
     * @param lhs The left-hand-side sub-expression.
     * @param rhs The right-hand-side sub-expression.
     */
    OrExpression(Expression* lhs, Expression* rhs) : Expression(Expression::CanFail), _lhs(lhs), _rhs(rhs)
    {}

    /** @brief Destructor. */
    ~OrExpression() override
    {}

    /**
     * @brief Evaluates the boolean expression.
     * @param exec_context The execution context.
     * @return The boolean result.
     */
    Datum eval(ExecContext* exec_context) override;

    /** @brief Dumps the expression to the standard error stream. */
    void dump() override;

private:
    /** @var The left-hand-side sub-expression. */
    Expression* _lhs;

    /** @var The right-hand-side sub-expression. */
    Expression* _rhs;
};

/*---------------------------------  NotExpression --------------------------------*/
/** @class NotExpression. An expression that evaluates to boolean NOT of a sub-expression. */
class NotExpression : public Expression {
public:
    /**
     * @brief Constructor.
     * @param arg The sub-expression.
     */
    explicit NotExpression(Expression* arg) : Expression(Expression::CanFail), _arg(arg)
    {}

    /** @brief Destructor. */
    ~NotExpression() override
    {}

    /**
     * @brief Evaluates the boolean expression.
     * @param exec_context The execution context.
     * @return The boolean result.
     */
    Datum eval(ExecContext* exec_context) override;

    /** @brief Dumps the expression to the standard error stream. */
    void dump() override;

private:
    /** @var The sub-expression. */
    Expression* _arg;
};

/*---------------------------------  RegisterRefInstruction --------------------------------*/
/**
 * @class RegisterRefInstruction. This instruction retrieves the cached result of a previously
 * executed instruction. It avoids re-execution of the instruction (which is both performance-wise
 * counter-productive and semantically wrong, especially if a side-effect exists).
 */
class RegisterRefInstruction : public Instruction {
public:
    /**
     * @brief Constructor.
     * @param register_ref The index of the register in which the cached result is found.
     */
    explicit RegisterRefInstruction(int register_ref) : Instruction(Instruction::RegisterRef)
    {
        SetRegisterRef(register_ref);
    }

    /** @brief Destructor. */
    ~RegisterRefInstruction() override
    {}

    /**
     * @brief Executes the instruction by the retrieving the cached result from the appropriate register.
     * @param execContext The execution context.
     * @return The execution result.
     * @note The returned value is NOT saved in any register (as it is by itself being retrieved from
     * a register).
     */
    uint64_t Exec(ExecContext* execContext) override;

    /** @brief Dumps the instruction to the standard error stream. */
    void Dump() override;
};

/*---------------------------------  ReturnInstruction --------------------------------*/
/** @class ReturnInstruction. An instruction that ends the execution of the function. */
class ReturnInstruction : public Instruction {
public:
    /**
     * @brief Constructor.
     * @param return_value The return value instruction.
     */
    explicit ReturnInstruction(Instruction* return_value)
        : Instruction(Instruction::Return), _return_value(return_value)
    {
        AddSubInstruction(return_value);
    }

    /** @brief Destructor. */
    ~ReturnInstruction() override
    {
        _return_value = nullptr;
    }

    /**
     * @brief Executes the instruction by evaluating the sub-instruction and returning the resulting
     * value.
     * @param execContext The execution context.
     * @return The execution result.
     * @note The returned value is saved in NOT saved in any register, since it is executed only once
     * (although the underlying sub-instruction could be very well a @ref RegisterRefInstruction
     * instruction).
     */
    uint64_t Exec(ExecContext* execContext) override;

    /** @brief Dumps the instruction to the standard error stream. */
    void Dump() override;

private:
    /** @brief The sub-instruction that should be evaluated as the return value. */
    Instruction* _return_value;
};

/*---------------------------------  ReturnNextInstruction --------------------------------*/
/**
 * @class ReturnNextInstruction. An instruction that returns a value but does not end the execution
 * of the function.
 */
class ReturnNextInstruction : public Instruction {
public:
    /**
     * @brief Constructor.
     * @param return_value The return value instruction.
     */
    ReturnNextInstruction(Instruction* return_value) : Instruction(Instruction::ReturnNext), _return_value(return_value)
    {
        AddSubInstruction(return_value);
    }

    /** @brief Destructor. */
    ~ReturnNextInstruction() override
    {
        _return_value = nullptr;
    }

    /**
     * @brief Executes the instruction by evaluating the sub-instruction and returning the resulting
     * value.
     * @param execContext The execution context.
     * @return The execution result.
     * @note The returned value is saved in NOT saved in any register, since it is executed only once
     * (although the underlying sub-instruction could be very well a @ref RegisterRefInstruction
     * instruction).
     */
    uint64_t Exec(ExecContext* execContext) override;

    /** @brief Dumps the instruction to the standard error stream. */
    void Dump() override;

private:
    /** @brief The sub-instruction that should be evaluated as the return value. */
    Instruction* _return_value;
};

/*---------------------------------  ConstInstruction --------------------------------*/
/**
 * @class ConstInstruction. A instruction that evaluates to a constant value. This kind of
 * instruction is required in cases where an instruction is required (and therefore a @ref
 * ConstExpression cannot be used).
 */
class ConstInstruction : public Instruction {
public:
    /**
     * @brief Constructor.
     * @param value The constant value.
     */
    explicit ConstInstruction(uint64_t value) : Instruction(Instruction::Void), _value(value)
    {}

    /** @brief Destructor. */
    ~ConstInstruction() override
    {}

    /**
     * @brief Executes the instruction by returning the constant value.
     * @param execContext The execution context.
     * @return The execution result.
     * @note The returned value is NOT saved in any register.
     */
    uint64_t Exec(ExecContext* exec_cContext) override;

    /** @brief Dumps the instruction to the standard error stream. */
    void Dump() override;

private:
    /** @var The constant value. */
    uint64_t _value;
};

/*---------------------------------  ExpressionInstruction --------------------------------*/
/**
 * @class ExpressionInstruction. A wrapper for @ref Expression. The result is cached in a register,
 * along with a RegisterRefInstruction as a result. This ensures the expression is executed only
 * once, as well as in cases where an @ref Instruction is required and @ref Expression cannot be used.
 */
class ExpressionInstruction : public Instruction {
public:
    /**
     * @brief Constructor.
     * @param expr The sub-expression.
     */
    explicit ExpressionInstruction(Expression* expr) : _expr(expr)
    {}

    /** @brief Destructor. */
    ~ExpressionInstruction() override;

protected:
    /**
     * @brief Implements the execution of the instruction by evaluating the sub-expression.
     * @param execContext The execution context.
     * @return The execution result.
     * @note The returned value is saved in a register.
     */
    uint64_t ExecImpl(ExecContext* execContext) override;

    /** @brief Dumps the instruction to the standard error stream. */
    void DumpImpl() override;

private:
    /** @var The sub-expression. */
    Expression* _expr;
};

/*---------------------------------  GetExpressionRCInstruction --------------------------------*/
/**
 * @class GetExpressionRCInstruction. An instruction used for retrieving the execution result of the
 * recently evaluated expression. Since expressions return a Datum value, there is no way to tell
 * whether execution succeeded or failed. Each expression can communicate its execution success
 * status through the execution context. This instruction retrieves the execution result of the
 * recently evaluated expression.
 */
class GetExpressionRCInstruction : public Instruction {
public:
    /** @brief Constructor. */
    GetExpressionRCInstruction()
    {}

    /** @brief Destructor. */
    ~GetExpressionRCInstruction() override
    {}

protected:
    /**
     * @brief Implements the execution of the instruction by retrieving the execution result of the
     * recently evaluated expression from the execution context.
     * @param execContext The execution context.
     * @return The execution result.
     * @note The returned value is saved in a register, since a subsequent expression evaluation will
     * override the recent expression evaluation execution result in the execution context.
     */
    uint64_t ExecImpl(ExecContext* execContext) override;

    /** @brief Dumps the instruction to the standard error stream. */
    void DumpImpl() override;
};

/*---------------------------------  DebugLogInstruction --------------------------------*/
/** @class DebugLogInstruction. Issues a debug log message. */
class DebugLogInstruction : public Instruction {
public:
    /**
     * @brief Constructor.
     * @param function The issuing function.
     * @param msg The debug log message.
     */
    DebugLogInstruction(const char* function, const char* msg)
        : Instruction(Instruction::Void), _function(function), _msg(msg)
    {}

    /** @brief Destructor. */
    ~DebugLogInstruction() override
    {
        _function = nullptr;
        _msg = nullptr;
    }

    /**
     * @brief Executes the instruction by printing the debug message.
     * @param execContext The execution context.
     * @return Not used.
     */
    uint64_t Exec(ExecContext* execContext) override;

    /** @brief Dumps the instruction to the standard error stream. */
    void Dump() override;

private:
    /** @brief The issuing function. */
    const char* _function;

    /** @var The debug log message. */
    const char* _msg;
};

/*---------------------------------  AbstractBranchInstruction --------------------------------*/
/** @class AbstractBranchInstruction. The parent class for all branch instruction. */
class AbstractBranchInstruction : public Instruction {
public:
    /** @enum The branch type. */
    enum BranchType {
        /** @var This is a condition branch. */
        Conditional,

        /** @var This is an unconditional branch. */
        Unconditional
    };

    /** @brief Retrieves the branch instruction type. */
    inline BranchType getBranchType() const
    {
        return _branch_type;
    }

    /** @brief Retrieves the number of sub-branches in this branch instruction. */
    virtual int getBranchCount() = 0;

    /** @brief Retrieves the execution block associated with the branch at the specified index. */
    virtual BasicBlock* getBranchAt(int index) = 0;

    /**
     * @brief Assistance in function finalization. Replaces a trivial block in this branch
     * instruction with the next block (in case this branch instruction refers to the trivial block).
     * @param trivial_block The trivial block to replace.
     * @param next_block The next block to use instead.
     * @note A trivial block is a block with only one unconditional branch instruction.
     */
    virtual void replaceTrivialBlockReference(BasicBlock* trivial_block, BasicBlock* next_block) = 0;

    /** @brief Destructor. */
    ~AbstractBranchInstruction() override
    {}

protected:
    /**
     * @brief Constructor.
     * @param branch_type The branch instruction type.
     */
    explicit AbstractBranchInstruction(BranchType branch_type)
        : Instruction(Instruction::Branch), _branch_type(branch_type)
    {}

private:
    /** @var The branch instruction type. */
    BranchType _branch_type;
};

/*---------------------------------  BasicBlock --------------------------------*/
/** @class BasicBlock. Represents a labeled block of instructions. */
class BasicBlock {
public:
    /**
     * @brief Constructor.
     * @param name The name of the block.
     */
    explicit BasicBlock(const char* name) : _name(name), _next_instruction_valid(false), _current_block(nullptr)
    {}

    /** @brief Destructor. */
    ~BasicBlock()
    {
        _current_block = nullptr;
    }

    /**
     * @brief Sets the block count. Used for producing a unique block name, in case more than one
     * block has the same name.
     * @param count The count value.
     */
    void setCount(int count);

    /** @brief Retrieves the name of the block. */
    inline const char* getName() const
    {
        return _name.c_str();
    }

    /**
     * @brief Adds an instruction to the block.
     * @param instruction The instruction to add.
     */
    void addInstruction(Instruction* instruction);

    /**
     * @brief Records a register reference definition (required for function validation).
     * @param register_index The register index.
     */
    void recordRegisterReferenceDefinition(int register_index);

    /** @brief Queries whether this block is empty. */
    inline bool isEmpty() const
    {
        return _instructions.empty();
    }

    /**
     * @brief Explicitly adds a predecessor to this block.
     * @param block The predecessor block to add.
     */
    inline void addPredecessor(BasicBlock* block)
    {
        _predecessors.push_back(block);
    }

    /**
     * @brief Adds to this block all the predecessors of the given trivial block.
     * @param trivial_block The trivial block.
     * @note This is the block that replaces a trivial block, therefore it needs to "inherit" all the
     * blocks preceeding the trivial block. The trivial block will subsequently be removed and destroyed.
     */
    void addTrivialBlockPredecessors(BasicBlock* trivial_block);

    /**
     * @brief Queries whether this block is trivial (i.e. contains only one unconditional branch
     * instruction.
     */
    bool isTrivial() const;

    /**
     * @brief Retrieves the next of this block, assuming this block is trivial.
     * @return The next block if this is a trivial block, otherwise null.
     */
    BasicBlock* getNextTrivalBlock();

    /**
     * @brief Replaces in all block instructions, all references to the trivial block with the next
     * block.
     * @param trivial_block The trivial block to replace.
     * @param next_block The next block to use instead.
     */
    void replaceTrivialBlockReference(BasicBlock* trivial_block, BasicBlock* next_block);

    /**
     * @brief Executes the instruction block.
     * @param exec_context The execution context.
     * @return The result of the first return statement encountered in this or any inner block.
     */
    uint64_t exec(ExecContext* exec_context);

    /**
     * @brief Starts or continues executes of the instruction block form the last point it stopped.
     * @param exec_context The execution context.
     * @return The result of the first return statement encountered in this or any inner block.
     */
    uint64_t execNext(ExecContext* exec_context);

    /** @brief Queries whether this block ends in a branch instruction. */
    bool endsInBranch();

    /** @brief Verifies the validity of the instruction block. */
    bool verify();

    /** @brief Dumps the instruction block to the standard error stream. */
    void dump();

private:
    /** @brief The block name. */
    std::string _name;

    /** @typedef Instruction list type. */
    typedef std::list<Instruction*> InstructionList;

    /** @typedef BasicBlock list type. */
    typedef std::list<BasicBlock*> BasicBlockList;

    /** @typedef Register reference definition list type. */
    typedef std::list<int> RegRefDefList;

    /** @var The instructions of the block. */
    InstructionList _instructions;

    /**
     * @var The blocks preceeding this block (i.e. that branch into this block). The predecessor
     * block list is used to validate the function.
     */
    BasicBlockList _predecessors;

    /**
     * @var The list of registers referenced by instructions in this block. This list is used for
     * validating the function.
     */
    RegRefDefList _register_ref_definitions;

    /** @var The next instruction to execute in continued execution mode. */
    InstructionList::iterator _next_instruction;

    /** @var Specifies whether the @ref _next_instruction member variable is valid. */
    bool _next_instruction_valid;

    /** @var The current block being executed in continued execution mode. */
    BasicBlock* _current_block;

    /** @brief Dumps the list of predecessors. */
    void dumpPredecessors();

    /** @brief Dumps the list of instructions. */
    void dumpInstructions();

    /**
     * @brief Queries whether the block contains an instruction that refers to a register invisible
     * from this block. This can happen if the register is initialized in a block that does not
     * precede this block. In such a case, referring to the register refers to uninitialized memory,
     * which would normally lead to crash.
     */
    bool hasIllegalInstruction();

    /**
     * @brief Queries whether all registers referred to by the instruction are visible.
     * @param instruction The instruction to validate.
     * @return True if the instruction is valid.
     */
    bool isInstructionLegal(Instruction* instruction);

    /**
     * Queries whether an instruction has some invisible sub-instruction.
     * @param instruction The instruction to examine.
     * @return True if a visibility problem was detected with any sub-instruction.
     */
    bool hasInvisibleRegisterRef(Instruction* instruction);

    /**
     * @brief Queries whether a register reference is visible. All preceeding blocks are scanned in a
     * DFS manner.
     * @param ref_ref_value The register index.
     * @param visited_blocks The blocks already visited during this scan (avoid endless recurrence).
     * @return True if the register definition resides in a block preceeding to this one, otherwise false.
     */
    bool isRegisterRefVisible(int ref_ref_value, BasicBlockList* visited_blocks);

    /**
     * @brief Queries whether this block contains a specific register definition instruction.
     * @param ref_ref_value The register index.
     * @return True if the block contains the register definition, otherwise false.
     */
    bool containsRegRefDefinition(int ref_ref_value);
};

/*---------------------------------  ICmpInstruction --------------------------------*/
/** @class ICmpEQInstruction. An instruction to compare two values. */
class ICmpInstruction : public Instruction {
public:
    /**
     * @brief Constructor.
     * @param lhs_instruction The left-hand side operand.
     * @param rhs_instruction The right-hand side operand.
     * @param cmp_op The comparison operator.
     */
    ICmpInstruction(Instruction* lhs_instruction, Instruction* rhs_instruction, JitExec::JitICmpOp cmp_op)
        : _lhs_instruction(lhs_instruction), _rhs_instruction(rhs_instruction), _cmp_op(cmp_op)
    {}

    /** @brief Destructor. */
    ~ICmpInstruction() override
    {
        _lhs_instruction = nullptr;
        _rhs_instruction = nullptr;
    }

protected:
    /**
     * @brief Implements the execution of the instruction.
     * @param execContext The execution context.
     * @return Non-zero value if the comparison resulted in true, otherwise zero.
     */
    uint64_t ExecImpl(ExecContext* execContext) override;

    /** @brief Dumps the instruction to the standard error stream. */
    void DumpImpl() override;

private:
    /** @var The left-hand side operand. */
    Instruction* _lhs_instruction;

    /** @var The right-hand side operand. */
    Instruction* _rhs_instruction;

    /** @var The comparison operator. */
    JitExec::JitICmpOp _cmp_op;

    /**
     * @brief Executes the comparison between two integer values.
     * @param lhs_res The left-hand size value.
     * @param rhs_res The right-hand size value.
     * @return Non-zero value if the comparison resulted in true, otherwise zero.
     */
    int exec_cmp(uint64_t lhs_res, uint64_t rhs_res);
};

/*---------------------------------  CondBranchInstruction --------------------------------*/
/** @class CondBranchInstruction. Conditional branch instruction.  */
class CondBranchInstruction : public AbstractBranchInstruction {
public:
    /**
     * @brief Constructor.
     * @param if_test The condition to test.
     * @param true_branch The block to execute if the condition evaluated to true.
     * @param false_branch The block to execute if the condition evaluated to false.
     */
    CondBranchInstruction(Instruction* if_test, BasicBlock* true_branch, BasicBlock* false_branch)
        : AbstractBranchInstruction(AbstractBranchInstruction::Conditional),
          _if_test(if_test),
          _true_branch(true_branch),
          _false_branch(false_branch)
    {}

    /** @brief Destructor. */
    ~CondBranchInstruction() override
    {
        _if_test = nullptr;
        _true_branch = nullptr;
        _false_branch = nullptr;
    }

    /** @brief Implements base class. Conditional branch instruction has two outcome branches. */
    int getBranchCount() override;

    /**
     * @brief Implements base class. At index 0 is the true branch, and at index 1 is the false branch.
     * @param index The branch index.
     * @return The requested branch.
     */
    BasicBlock* getBranchAt(int index) override;

    /**
     * @brief Assistance in function finalization. Replaces a trivial block in this branch
     * instruction with the next block (in case this branch instruction refers to the trivial block).
     * @param trivial_block The trivial block to replace.
     * @param next_block The next block to use instead.
     * @note A trivial block is a block with only one unconditional branch instruction.
     */
    void replaceTrivialBlockReference(BasicBlock* trivial_block, BasicBlock* next_block) override;

    /**
     * @brief Executes the instruction. Evaluates the condition and returns the next block to execute.
     * @param execContext The execution context.
     * @return The next block to execute.
     */
    uint64_t Exec(ExecContext* execContext) override;

    /** @brief Dumps the instruction to the standard error stream. */
    void Dump() override;

private:
    /** @var The condition to test. */
    Instruction* _if_test;

    /** @var The block to execute if the condition evaluated to true. */
    BasicBlock* _true_branch;

    /** @var The block to execute if the condition evaluated to false. */
    BasicBlock* _false_branch;
};

/*---------------------------------  BranchInstruction --------------------------------*/
/** @class BranchInstruction. Unconditional branch instruction. */
class BranchInstruction : public AbstractBranchInstruction {
public:
    /**
     * @brief Constructor.
     * @param target_branch The next block to execute.
     */
    explicit BranchInstruction(BasicBlock* target_branch)
        : AbstractBranchInstruction(AbstractBranchInstruction::Unconditional), _target_branch(target_branch)
    {}

    /** @brief Destructor. */
    ~BranchInstruction() override
    {
        _target_branch = nullptr;
    }

    /** @brief Implements base class. Unconditional branch instruction has only one outcome branch. */
    int getBranchCount() override;

    /**
     * @brief Implements base class. At index 0 is the target branch.
     * @param index The branch index.
     * @return The requested branch.
     */
    BasicBlock* getBranchAt(int index) override;

    /**
     * @brief Assistance in function finalization. Replaces a trivial block in this branch
     * instruction with the next block (in case this branch instruction refers to the trivial block).
     * @param trivial_block The trivial block to replace.
     * @param next_block The next block to use instead.
     * @note A trivial block is a block with only one unconditional branch instruction.
     */
    void replaceTrivialBlockReference(BasicBlock* trivial_block, BasicBlock* next_block) override;

    /**
     * @brief Executes the instruction. Returns the next block to execute.
     * @param exec_context The execution context.
     * @return The next block to execute.
     */
    uint64_t Exec(ExecContext* exec_context) override;

    /** @brief Dumps the block to the standard error stream. */
    void Dump() override;

private:
    /** @var The next block to execute. */
    BasicBlock* _target_branch;
};

/*---------------------------------  TVM Function --------------------------------*/
/** @class Function. A Pseudo-LLVM function. */
class Function {
public:
    /**
     * @brief Constructor.
     * @param function_name The name of the function.
     * @param query_string The query/function string being executed.
     * @param entry_block The first block to execute.
     */
    Function(const char* function_name, const char* query_string, BasicBlock* entry_block);

    /** @brief Destructor. */
    ~Function();

    /** @brief Retrieves the name of the function. */
    inline const char* getName() const
    {
        return _function_name.c_str();
    }

    /** @brief Retrieves the maximum number of registers used by the function. */
    inline uint64_t getRegisterCount() const
    {
        return _max_register_ref + 1;
    }

    /**
     * @brief Executes the function.
     * @param exec_context The execution context.
     * @return The value evaluated by the first return instruction encountered.
     */
    uint64_t exec(ExecContext* exec_context);

    /**
     * @brief Executes the function up to the next "Return-Next" instruction or "Return" instruction.
     * @param exec_context The execution context.
     * @return The value evaluated by the first return instruction encountered.
     * @note This is used to assist in executing efficiently "Return Next" in stored procedures.
     */
    uint64_t execNext(ExecContext* exec_context);

    /**
     * @brief Adds instruction to the function. This is in addition to adding the instruction to its
     * containing block.
     * @param instruction The instruction to add.
     */
    void addInstruction(Instruction* instruction);

    /**
     * @brief Adds a block to the function.
     * @param block The block to add.
     */
    void addBlock(BasicBlock* block);

    /**
     * @brief Finalizes the function.
     * @return True if finalization finished successfully, otherwise false.
     */
    bool finalize();

    /** @brief Verifies the validity of the function. */
    bool verify();

    /** @brief Dumps the block to the standard error stream. */
    void dump();

private:
    /** @typedef Block name map type. */
    typedef std::map<std::string, int> BlockNameMap;

    /** @typedef Instruction list type. */
    typedef std::list<Instruction*> InstructionList;

    /** @typedef Block list type. */
    typedef std::list<BasicBlock*> BasicBlockList;

    /** @var The function name. */
    std::string _function_name;

    /** @var The query/function string being executed. */
    std::string _query_string;

    /** @var The first block to execute. */
    BasicBlock* _entry_block;

    /** @var A map to count the number of times each block name was used. */
    BlockNameMap _block_name_count;

    /** @var The maximum number of registers used by the function. */
    int _max_register_ref;

    /** @var The list of all instructions used by all blocks in this function. */
    InstructionList _all_instructions;

    /** @var The list of all blocks used by this function. */
    BasicBlockList _all_blocks;

    /** @var The currently executing block. */
    BasicBlock* _current_block;

    /** @brief Removes all empty blocks from execution. */
    void removeEmptyBlocks();

    /** @brief Remove all trivial blocks (and rewire all predecessors to the next block). */
    void rewireTrivialBlocks();

    /** @brief Searches for a trivial block. */
    BasicBlock* findTrivialBlock();

    /** @brief Requires a trivial block. */
    void rewireTrivialBlock(BasicBlock* trivial_block);

    /**
     * @brief Replaces all references to a trivial block with its next block.
     * @param trivial_block The trivial block.
     * @param next_block The next block to use instead.
     */
    void replaceTrivialBlockReference(BasicBlock* trivial_block, BasicBlock* next_block);

    /**
     * @brief Removes and destroys a trivial block.
     * @param trivial_block The trivial block to cleanup.
     */
    void removeTrivialBlock(BasicBlock* trivial_block);
};

/*---------------------------------  TVM Builder --------------------------------*/
/** @class Builder. Helper class to build Pseudo-LLVM functions. */
class Builder {
public:
    /** @brief Constructor. */
    Builder();

    /** @brief Destructor. */
    ~Builder();

    /**
     * @brief Creates a function. This is the first step to do.
     * @param functionName The name of the function.
     * @param queryString The string of the query/function it executes.
     * @return The resulting object or NULL if allocation failed.
     * @note The function is initialized with an initial empty entry blocks. New instructions will be
     * added to the entry block until a new block is started.
     */
    Function* createFunction(const char* functionName, const char* queryString);

    /**
     * @brief Adds an instruction to the current block.
     * @param instruction The instruction to add.
     * @return The resulting instruction.
     * @note If this is a regular instruction then a @ref RegisterRefInstruction is created for it and
     * returned instead, so that all other instructions using it will access in runtime the cached
     * result of the original instruction.
     */
    Instruction* addInstruction(Instruction* instruction);

    /**
     * @brief Adds an @ref ExpressionInstruction for the given expression.
     * @param expr The expression to add.
     * @return The resulting instruction. This is actually a @ref RegisterRefInstruction instruction
     * pointing to the result of the original @ref ExpressionInstruction.
     */
    Instruction* addExpression(Expression* expr);

    /**
     * @brief Adds a @ref GetExpressionRCInstruction to retrieve the execution result of the recently
     * evaluated expression.
     * @return The resulting instruction. No @ref RegisterRefInstruction is produced.
     */
    Instruction* addGetExpressionRC();

    /**
     * @brief Creates a new block of execution. The current block is not replaced yet. In order to do
     * so, call @ref SetInsertPoint().
     * @param name The name of the block. If this name was used in the past, then an integer suffix
     * will be added to it to ensure uniqueness.
     * @return The new block.
     */
    BasicBlock* CreateBlock(const char* name);

    /**
     * @brief Creates an integer comparison instruction.
     * @param lhsInstruction The left-hand side operand.
     * @param rhsInstruction The right-hand side operand.
     * @param cmpOp The comparison operator.
     * @return The resulting instruction. This is a @ref RegisterRefInstruction to the cached result
     * of the original instruction.
     */
    Instruction* CreateICmp(Instruction* lhsInstruction, Instruction* rhsInstruction, JitExec::JitICmpOp cmpOp);

    /**
     * @brief Creates an integer EQUALS comparison instruction.
     * @param lhs_instruction The left-hand side operand.
     * @param rhs_instruction The right-hand side operand.
     * @return The resulting instruction. This is a @ref RegisterRefInstruction to the cached result
     * of the original instruction.
     */
    inline Instruction* CreateICmpEQ(Instruction* lhs_instruction, Instruction* rhs_instruction)
    {
        return CreateICmp(lhs_instruction, rhs_instruction, JitExec::JIT_ICMP_EQ);
    }

    /**
     * @brief Creates (and adds to the current block) a conditional branch instruction.
     * @param if_test The condition test instruction.
     * @param true_branch The block to execute if the condition evaluates to true.
     * @param false_branch The block to execute if the condition evaluates to false.
     */
    void CreateCondBr(Instruction* if_test, BasicBlock* true_branch, BasicBlock* false_branch);

    /**
     * @brief Creates (and adds to the current block) an unconditional branch instruction.
     * @param target_branch The next block to execute.
     */
    void CreateBr(BasicBlock* target_branch);

    /**
     * @brief Sets the current block into which new instructions are added.
     * @param block The block to use.
     */
    void SetInsertPoint(BasicBlock* block);

    /**
     * @brief Creates (and adds to the current block) a return statement.
     * @param return_value The instruction to be evaluated as the return value.
     */
    void CreateRet(Instruction* return_value);

    /**
     * @brief Creates a @ref ConstInstruction. The instruction is not added to the current block as it
     * has no effect.
     * @param value The constant value.
     * @return The resulting instruction.
     */
    Instruction* CreateConst(uint64_t value);

    /** @brief Retrieves the currently built block. */
    BasicBlock* GetInsertBlock();

    /** @brief Queries whether the current block ends with a branch instruction. */
    bool currentBlockEndsInBranch();

private:
    /** @var The function being built. */
    Function* _current_function;

    /** @var The current block into which instructions are added. */
    BasicBlock* _current_block;

    /** @var The index of the next register to use. */
    int _next_register_ref;
};
}  // namespace tvm

#endif /* JIT_TVM_H */
