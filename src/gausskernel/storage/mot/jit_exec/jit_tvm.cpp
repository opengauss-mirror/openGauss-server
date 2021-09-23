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
 * jit_tvm.cpp
 *    JIT TVM (Tiny Virtual Machine).
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_tvm.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "jit_tvm.h"
#include "jit_helpers.h"
#include "mot_engine.h"
#include "utilities.h"
#include "mot_error.h"

#include <algorithm>

DECLARE_LOGGER(TVM, JitExec)

namespace tvm {
IMPLEMENT_CLASS_LOGGER(Instruction, JitExec)
IMPLEMENT_CLASS_LOGGER(Expression, JitExec)

extern ExecContext* allocExecContext(uint64_t register_count)
{
    size_t alloc_size = sizeof(ExecContext) + register_count * sizeof(uint64_t);
    // let's use faster MOT session-local allocation
    ExecContext* ctx = (ExecContext*)MOT::MemSessionAlloc(alloc_size);
    if (ctx != nullptr) {
        errno_t erc = memset_s(ctx, alloc_size, 0, alloc_size);
        securec_check(erc, "\0", "\0");
        ctx->_register_count = register_count;
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Execute JIT",
            "Failed to allocate %u bytes for pseudo-jit execution context",
            (unsigned)alloc_size);
    }
    return ctx;
}

extern void freeExecContext(ExecContext* ctx)
{
    if (ctx != nullptr) {
        MOT::MemSessionFree(ctx);
    }
}

extern void setRegisterValue(ExecContext* exec_context, int register_ref, uint64_t value)
{
    if (register_ref < (int)exec_context->_register_count) {
        exec_context->_registers[register_ref] = value;
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "Execute JIT",
            "Invalid register access %d exceeds limit %d",
            register_ref,
            (int)exec_context->_register_count);
        // NOTE: should throw PG exception through report error?
        // NOTE: all DirectFunction() calls should be surrounded with PG_TRY and error should be reported somehow
        MOT_ASSERT(false);
    }
}

extern uint64_t getRegisterValue(ExecContext* exec_context, int register_ref)
{
    uint64_t result = 0;
    if (register_ref < (int)exec_context->_register_count) {
        result = exec_context->_registers[register_ref];
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "Execute JIT",
            "Invalid register access %d exceeds limit %d",
            register_ref,
            (int)exec_context->_register_count);
        // NOTE: should throw PG exception through report error?
        MOT_ASSERT(false);
    }
    return result;
}

uint64_t Instruction::Exec(ExecContext* exec_context)
{
#ifdef MOT_JIT_DEBUG
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_DEBUG)) {
        MOT_LOG_DEBUG("Executing instruction:");
        Dump();
        fprintf(stderr, "\n");
    }
#endif
    uint64_t value = ExecImpl(exec_context);
    if (_type == Regular) {
        setRegisterValue(exec_context, _register_ref, value);
    }
    return value;
}

void Instruction::Dump()
{
    if (_register_ref != -1) {
        fprintf(stderr, "%%%d = ", _register_ref);
    }
    DumpImpl();
}

Datum ConstExpression::eval(ExecContext* exec_context)
{
    setExprArgIsNull(_arg_pos, _is_null);
    exec_context->_expr_rc = MOT_NO_ERROR;
    return _datum;
}

void ConstExpression::dump()
{
    fprintf(stderr, "%" PRIu64, (uint64_t)_datum);
}

Datum ParamExpression::eval(ExecContext* exec_context)
{
    Datum result = PointerGetDatum(NULL);
    if (_param_id < exec_context->_params->numParams) {
        exec_context->_expr_rc = MOT_NO_ERROR;
        result = getDatumParam(exec_context->_params, _param_id, _arg_pos);
    } else {
        exec_context->_expr_rc = MOT_ERROR_INDEX_OUT_OF_RANGE;
    }
    return result;
}

void ParamExpression::dump()
{
    fprintf(stderr, "getDatumParam(%%params, param_id=%d, arg_pos=%d)", _param_id, _arg_pos);
}

Datum AndExpression::eval(ExecContext* exec_context)
{
    Datum result = PointerGetDatum(NULL);
    Datum lhs = _lhs->eval(exec_context);
    if (exec_context->_expr_rc == MOT_NO_ERROR) {
        Datum rhs = _rhs->eval(exec_context);
        if (exec_context->_expr_rc == MOT_NO_ERROR) {
            result = BoolGetDatum(DatumGetBool(lhs) && DatumGetBool(rhs));
        }
    }
    return result;
}

void AndExpression::dump()
{
    _lhs->dump();
    fprintf(stderr, " AND ");
    _rhs->dump();
}

Datum OrExpression::eval(ExecContext* exec_context)
{
    Datum result = PointerGetDatum(NULL);
    Datum lhs = _lhs->eval(exec_context);
    if (exec_context->_expr_rc == MOT_NO_ERROR) {
        Datum rhs = _rhs->eval(exec_context);
        if (exec_context->_expr_rc == MOT_NO_ERROR) {
            result = BoolGetDatum(DatumGetBool(lhs) || DatumGetBool(rhs));
        }
    }
    return result;
}

void OrExpression::dump()
{
    _lhs->dump();
    fprintf(stderr, " OR ");
    _rhs->dump();
}

Datum NotExpression::eval(ExecContext* exec_context)
{
    Datum result = PointerGetDatum(NULL);
    Datum arg = _arg->eval(exec_context);
    if (exec_context->_expr_rc == MOT_NO_ERROR) {
        result = BoolGetDatum(!DatumGetBool(arg));
    }
    return result;
}

void NotExpression::dump()
{
    fprintf(stderr, "NOT ");
    _arg->dump();
}

uint64_t RegisterRefInstruction::Exec(ExecContext* exec_context)
{
    return getRegisterValue(exec_context, GetRegisterRef());
}

void RegisterRefInstruction::Dump()
{
    fprintf(stderr, "%%%d", GetRegisterRef());
}

uint64_t ReturnInstruction::Exec(ExecContext* exec_context)
{
    return _return_value->Exec(exec_context);
}

void ReturnInstruction::Dump()
{
    fprintf(stderr, "return ");
    _return_value->Dump();
}

uint64_t ReturnNextInstruction::Exec(ExecContext* exec_context)
{
    return _return_value->Exec(exec_context);
}

void ReturnNextInstruction::Dump()
{
    fprintf(stderr, "return next ");
    _return_value->Dump();
}

uint64_t ConstInstruction::Exec(ExecContext* exec_context)
{
    return _value;
}

void ConstInstruction::Dump()
{
    fprintf(stderr, "%" PRIu64, _value);
}

ExpressionInstruction::~ExpressionInstruction()
{
    MOT_LOG_DEBUG("Deleting sub-expr %p in expression instruction %p", _expr, this);
    delete _expr;
}

uint64_t ExpressionInstruction::ExecImpl(ExecContext* exec_context)
{
    return _expr->eval(exec_context);
}

void ExpressionInstruction::DumpImpl()
{
    _expr->dump();
}

uint64_t GetExpressionRCInstruction::ExecImpl(ExecContext* exec_context)
{
    return exec_context->_expr_rc;
}

void GetExpressionRCInstruction::DumpImpl()
{
    fprintf(stderr, "getExpressionRC()");
}

uint64_t DebugLogInstruction::Exec(ExecContext* exec_context)
{
    debugLog(_function, _msg);
    return (uint64_t)MOT::RC_OK;
}

void DebugLogInstruction::Dump()
{
    fprintf(stderr, "debugLog(function='%s', msg='%s')", _function, _msg);
}

void BasicBlock::setCount(int count)
{
    char id[32];
    errno_t erc = snprintf_s(id, sizeof(id), sizeof(id) - 1, "%d", count);
    securec_check_ss(erc, "\0", "\0");
    _name += id;
}

void BasicBlock::addInstruction(Instruction* instruction)
{
    _instructions.push_back(instruction);

    // update predecessors if necessary
    if (instruction->GetType() == Instruction::Branch) {
        AbstractBranchInstruction* branch_instruction = (AbstractBranchInstruction*)instruction;
        int branch_count = branch_instruction->getBranchCount();
        for (int i = 0; i < branch_count; ++i) {
            BasicBlock* block = branch_instruction->getBranchAt(i);
            block->addPredecessor(this);
        }
    }
}

void BasicBlock::recordRegisterReferenceDefinition(int register_index)
{
    MOT_LOG_TRACE("Adding register %d ref-def to block %s", register_index, getName());
    _register_ref_definitions.push_back(register_index);
}

void BasicBlock::addTrivialBlockPredecessors(BasicBlock* trivial_block)
{
    // inherit all the predecessors of the trivial block
    BasicBlockList::iterator block_itr = trivial_block->_predecessors.begin();
    while (block_itr != trivial_block->_predecessors.end()) {
        addPredecessor(*block_itr);
        ++block_itr;
    }

    // and remove the trivial block itself from the list of predecessors
    block_itr = find(_predecessors.begin(), _predecessors.end(), trivial_block);
    MOT_ASSERT(block_itr != _predecessors.end());
    if (block_itr != _predecessors.end()) {
        _predecessors.erase(block_itr);
    }
}

bool BasicBlock::isTrivial() const
{
    bool result = false;
    if (_instructions.size() == 1) {
        Instruction* instruction = _instructions.front();
        if (instruction->GetType() == Instruction::Branch) {
            AbstractBranchInstruction* branch_instruction = (AbstractBranchInstruction*)instruction;
            if (branch_instruction->getBranchType() == AbstractBranchInstruction::Unconditional) {
                result = true;
            }
        }
    }
    return result;
}

BasicBlock* BasicBlock::getNextTrivalBlock()
{
    BasicBlock* result = NULL;
    MOT_ASSERT(_instructions.size() == 1);
    if (_instructions.size() == 1) {
        Instruction* instruction = _instructions.front();
        MOT_ASSERT(instruction->GetType() == Instruction::Branch);
        if (instruction->GetType() == Instruction::Branch) {
            AbstractBranchInstruction* branch_instruction = (AbstractBranchInstruction*)instruction;
            MOT_ASSERT(branch_instruction->getBranchType() == AbstractBranchInstruction::Unconditional);
            if (branch_instruction->getBranchType() == AbstractBranchInstruction::Unconditional) {
                result = branch_instruction->getBranchAt(0);
            }
        }
    }
    return result;
}

void BasicBlock::replaceTrivialBlockReference(BasicBlock* trivial_block, BasicBlock* next_block)
{
    InstructionList::iterator itr = _instructions.begin();
    while (itr != _instructions.end()) {
        Instruction* instruction = *itr;
        if (instruction->GetType() == Instruction::Branch) {
            AbstractBranchInstruction* branch_instruction = (AbstractBranchInstruction*)instruction;
            branch_instruction->replaceTrivialBlockReference(trivial_block, next_block);
        }
        ++itr;
    }
}

uint64_t BasicBlock::exec(ExecContext* exec_context)
{
#ifdef MOT_JIT_DEBUG
    MOT_LOG_DEBUG("Executing block: %s", getName());
#endif
    InstructionList::iterator itr = _instructions.begin();
    while (itr != _instructions.end()) {
        Instruction* instruction = *itr;
        uint64_t rc = instruction->Exec(exec_context);
        Instruction::Type itype = instruction->GetType();
        if (itype == Instruction::Return) {
            return rc;
        }
        if (itype == Instruction::Branch) {
            BasicBlock* next_block = (BasicBlock*)(uintptr_t)rc;
            return next_block->exec(exec_context);  // allow tail call optimization
        }
        ++itr;
    }
    MOT_ASSERT(false);
    MOT_REPORT_ERROR(
        MOT_ERROR_INTERNAL, "Execute JIT", "Reached end of block %s without terminating instruction", getName());
    return (uint64_t)MOT::RC_ERROR;
}

uint64_t BasicBlock::execNext(ExecContext* exec_context)
{
#ifdef MOT_JIT_DEBUG
    MOT_LOG_DEBUG("Executing block %s from last point", getName());
#endif

    // initialize instruction if required
    if (!_next_instruction_valid) {
        _next_instruction = _instructions.begin();
    }

    // if we already dived into the next block then continue to execute it from where it stopped
    if (_current_block != nullptr) {
        return _current_block->execNext(exec_context);
    }

    // otherwise continue executing instructions
    while (_next_instruction != _instructions.end()) {
        Instruction* instruction = *_next_instruction;
        ++_next_instruction;
        uint64_t rc = instruction->Exec(exec_context);

        // check instruction type to understand what happened
        Instruction::Type itype = instruction->GetType();

        // case 1: function execution terminated
        if (itype == Instruction::Return) {
            _next_instruction_valid = false;
            return rc;
        }

        // case 2: function execution stopped for now, and will continue later
        if (itype == Instruction::ReturnNext) {
            return rc;
        }

        // case 3: branch instruction leads us to another branch
        if (itype == Instruction::Branch) {
            _next_instruction_valid =
                false;  // instruction for current block will no longer be executed until function restarts
            _current_block = (BasicBlock*)(uintptr_t)rc;
            return _current_block->exec(exec_context);  // allow tail call optimization if possible
        }
    }

    // guard in runtime (should not happen if function verification was correct)
    MOT_ASSERT(false);
    MOT_REPORT_ERROR(
        MOT_ERROR_INTERNAL, "Execute JIT", "Reached end of block %s without terminating instruction", getName());
    return (uint64_t)MOT::RC_ERROR;
}

bool BasicBlock::endsInBranch()
{
    bool result = false;
    if (!_instructions.empty()) {
        Instruction::Type last_instr_type = _instructions.back()->GetType();
        result = (last_instr_type == Instruction::Branch) || (last_instr_type == Instruction::Return);
    }
    return result;
}

bool BasicBlock::verify()
{
    bool result = false;
    MOT_LOG_TRACE("Verifying block: %s", getName());
    if (!endsInBranch()) {
        MOT_LOG_ERROR(
            "Block %s is invalid: last instruction is not a terminating instruction (return or branch)", getName());
    } else {
        // verify that all register references in this block are visible (i.e. are defined at this or
        // a predecessor block).
        if (hasIllegalInstruction()) {
            MOT_LOG_ERROR(
                "Block %s is invalid: Found invisible register reference (definition unreachable from current block)",
                getName());
        } else {
            result = true;
        }
    }
    return result;
}

void BasicBlock::dump()
{
    int padding = 64 - (int)_name.size();
    fprintf(stderr, "%s:%*s; preds = ", getName(), padding, "");

    dumpPredecessors();
    fprintf(stderr, "\n");

    dumpInstructions();
    fprintf(stderr, "\n");
}

void BasicBlock::dumpPredecessors()
{
    bool is_first = true;
    BasicBlockList::iterator itr = _predecessors.begin();
    while (itr != _predecessors.end()) {
        BasicBlock* block = *itr;
        if (is_first) {
            fprintf(stderr, "%%%s", block->getName());
            is_first = false;
        } else {
            fprintf(stderr, ", %%%s", block->getName());
        }
        ++itr;
    }
}

void BasicBlock::dumpInstructions()
{
    InstructionList::iterator itr = _instructions.begin();
    while (itr != _instructions.end()) {
        fprintf(stderr, "  ");
        (*itr)->Dump();
        fprintf(stderr, ";\n");
        ++itr;
    }
}

bool BasicBlock::hasIllegalInstruction()
{
    MOT_LOG_TRACE("Checking whether block %s has illegal instruction", getName());
    InstructionList::iterator itr = _instructions.begin();
    while (itr != _instructions.end()) {
        Instruction* instruction = *itr;
        if (!isInstructionLegal(instruction)) {
            MOT_LOG_ERROR("Found illegal instruction:");
            instruction->Dump();
            fprintf(stderr, "\n");
            return true;
        }
        ++itr;
    }
    return false;
}

bool BasicBlock::isInstructionLegal(Instruction* instruction)
{
    // if this is a register ref instruction then check the register is visible from this block
    // otherwise check that all sub-instructions are visible
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
        MOT_LOG_TRACE("Checking whether instruction is legal:");
        instruction->Dump();
        fprintf(stderr, "\n");
    }

    if (instruction->GetType() == Instruction::RegisterRef) {
        BasicBlockList visited_blocks;
        if (!isRegisterRefVisible(instruction->GetRegisterRef(), &visited_blocks)) {
            MOT_LOG_ERROR("Register reference instruction is invisible from current block %s:", getName());
            instruction->Dump();
            fprintf(stderr, "\n");
            return false;
        }
    } else if (hasInvisibleRegisterRef(instruction)) {
        MOT_LOG_ERROR("Found instruction with invisible register ref:");
        instruction->Dump();
        fprintf(stderr, "\n");
        return false;
    }
    return true;
}

bool BasicBlock::hasInvisibleRegisterRef(Instruction* instruction)
{
    MOT_LOG_TRACE("Checking if instruction contains invisible register reference");
    int sub_inst_count = instruction->GetSubInstructionCount();
    for (int i = 0; i < sub_inst_count; ++i) {
        Instruction* sub_instruction = instruction->GetSubInstructionAt(i);
        if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
            MOT_LOG_TRACE("Checking sub-instruction:")
            sub_instruction->Dump();
            fprintf(stderr, "\n");
        }
        if (!isInstructionLegal(sub_instruction)) {
            MOT_LOG_ERROR("Found illegal sub-instruction:");
            sub_instruction->Dump();
            fprintf(stderr, "\n");
            return true;
        }
    }
    MOT_LOG_TRACE("Instruction does NOT contain invisible register reference");
    return false;
}

bool BasicBlock::isRegisterRefVisible(int ref_ref_value, BasicBlockList* visited_blocks)
{
    // we search the register definition in this block and each predecessor block
    // we assume that graph connectivity has been checked already (otherwise we need proper backwards DFS)
    bool result = false;
    bool is_first = false;
    MOT_LOG_TRACE(
        "Checking if register reference %d instruction is visible from current block %s", ref_ref_value, getName());

    // avoid endless recursion
    BasicBlockList::iterator itr = find(visited_blocks->begin(), visited_blocks->end(), this);
    if (itr != visited_blocks->end()) {
        return true;
    } else {
        if (visited_blocks->empty()) {
            is_first = true;
        }
        visited_blocks->push_back(this);
    }

    // check if current block contains the definition for the register index
    if (containsRegRefDefinition(ref_ref_value)) {
        result = true;
    } else {
        // other-wise search backwards in all predecessors
        BasicBlockList::iterator block_itr = _predecessors.begin();
        while (block_itr != _predecessors.end()) {
            BasicBlock* block = *block_itr;
            if (block->isRegisterRefVisible(ref_ref_value, visited_blocks)) {
                result = true;
                break;
            }
            ++block_itr;
        }
    }

    // issue error only after recurrence ends
    if (!result && is_first) {
        MOT_LOG_ERROR("Register reference %d is invisible from current block %s", ref_ref_value, getName());
    }
    return result;
}

bool BasicBlock::containsRegRefDefinition(int ref_ref_value)
{
    bool result = false;
    RegRefDefList::iterator itr =
        find(_register_ref_definitions.begin(), _register_ref_definitions.end(), ref_ref_value);
    if (itr != _register_ref_definitions.end()) {
        result = true;
    }
    MOT_LOG_TRACE(
        "Block %s contains register reference %d definition: %s", getName(), ref_ref_value, result ? "Yes" : "No");
    return result;
}

uint64_t ICmpInstruction::ExecImpl(ExecContext* exec_context)
{
    uint64_t lhs_res = _lhs_instruction->Exec(exec_context);
    uint64_t rhs_res = _rhs_instruction->Exec(exec_context);
    return exec_cmp(lhs_res, rhs_res);
}

void ICmpInstruction::DumpImpl()
{
    fprintf(stderr, "icmp ");
    _lhs_instruction->Dump();

    switch (_cmp_op) {
        case JitExec::JIT_ICMP_EQ:
            fprintf(stderr, " == ");
            break;
        case JitExec::JIT_ICMP_NE:
            fprintf(stderr, " != ");
            break;
        case JitExec::JIT_ICMP_GT:
            fprintf(stderr, " > ");
            break;
        case JitExec::JIT_ICMP_GE:
            fprintf(stderr, " >= ");
            break;
        case JitExec::JIT_ICMP_LT:
            fprintf(stderr, " < ");
            break;
        case JitExec::JIT_ICMP_LE:
            fprintf(stderr, " <= ");
            break;
        default:
            fprintf(stderr, " cmp? ");
    }

    _rhs_instruction->Dump();
}

int ICmpInstruction::exec_cmp(uint64_t lhs_res, uint64_t rhs_res)
{
    switch (_cmp_op) {
        case JitExec::JIT_ICMP_EQ:
            return (lhs_res == rhs_res) ? 1 : 0;
        case JitExec::JIT_ICMP_NE:
            return (lhs_res != rhs_res) ? 1 : 0;
        case JitExec::JIT_ICMP_GT:
            return (lhs_res > rhs_res) ? 1 : 0;
        case JitExec::JIT_ICMP_GE:
            return (lhs_res >= rhs_res) ? 1 : 0;
        case JitExec::JIT_ICMP_LT:
            return (lhs_res < rhs_res) ? 1 : 0;
        case JitExec::JIT_ICMP_LE:
            return (lhs_res <= rhs_res) ? 1 : 0;
        default:
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Execute Pseudo-JIT Function", "Invalid compare operator %d", (int)_cmp_op);
            MOT_ASSERT(false);
            return 0;
    }
}

int CondBranchInstruction::getBranchCount()
{
    return 2;
}

BasicBlock* CondBranchInstruction::getBranchAt(int index)
{
    BasicBlock* result = NULL;
    if (index == 0) {
        result = _true_branch;
    } else if (index == 1) {
        result = _false_branch;
    } else {
        MOT_ASSERT(false);
    }
    return result;
}

void CondBranchInstruction::replaceTrivialBlockReference(BasicBlock* trivial_block, BasicBlock* next_block)
{
    if (_true_branch == trivial_block) {
        MOT_LOG_TRACE(
            "Replacing trivial true-branch %s with next block %s", _true_branch->getName(), next_block->getName());
        _true_branch = next_block;
    }
    if (_false_branch == trivial_block) {
        MOT_LOG_TRACE(
            "Replacing trivial false-branch %s with next block %s", _false_branch->getName(), next_block->getName());
        _false_branch = next_block;
    }
}

uint64_t CondBranchInstruction::Exec(ExecContext* exec_context)
{
    BasicBlock* result = NULL;
    uint64_t rc = _if_test->Exec(exec_context);
    if (rc) {
        result = _true_branch;
    } else {
        result = _false_branch;
    }
    return (uint64_t)result;
}

void CondBranchInstruction::Dump()
{
    fprintf(stderr, "br ");  // format compatible with IR (meaning if non-zero jump to true branch)
    _if_test->Dump();
    fprintf(stderr, ", label %%%s, label %%%s", _true_branch->getName(), _false_branch->getName());
}

int BranchInstruction::getBranchCount()
{
    return 1;
}

BasicBlock* BranchInstruction::getBranchAt(int index)
{
    BasicBlock* result = NULL;
    if (index == 0) {
        result = _target_branch;
    } else {
        MOT_ASSERT(false);
    }
    return result;
}

void BranchInstruction::replaceTrivialBlockReference(BasicBlock* trivial_block, BasicBlock* next_block)
{
    if (_target_branch == trivial_block) {
        MOT_LOG_TRACE(
            "Replacing trivial target-branch %s with next block %s", _target_branch->getName(), next_block->getName());
        _target_branch = next_block;
    }
}

uint64_t BranchInstruction::Exec(ExecContext* exec_context)
{
    return (uint64_t)_target_branch;
}

void BranchInstruction::Dump()
{
    fprintf(stderr, "br label %%%s", _target_branch->getName());
}

Function::Function(const char* function_name, const char* query_string, BasicBlock* entry_block)
    : _function_name(function_name),
      _query_string(query_string),
      _entry_block(entry_block),
      _max_register_ref(0),
      _current_block(entry_block)
{
    addBlock(entry_block);
}

Function::~Function()
{
#ifdef MOT_JIT_DEBUG
    MOT_LOG_DEBUG("Deleting function %s", _function_name.c_str());
#endif
    InstructionList::iterator instr_itr = _all_instructions.begin();
    while (instr_itr != _all_instructions.end()) {
        Instruction* instruction = *instr_itr;
        MOT_LOG_DEBUG("Deleting instruction %p", instruction);
        delete instruction;
        instr_itr = _all_instructions.erase(instr_itr);
    }
    BasicBlockList::iterator block_itr = _all_blocks.begin();
    while (block_itr != _all_blocks.end()) {
        BasicBlock* block = *block_itr;
        MOT_LOG_DEBUG("Deleting block %s at %p", block->getName(), block);
        delete block;
        block_itr = _all_blocks.erase(block_itr);
    }
}

uint64_t Function::exec(ExecContext* exec_context)
{
    MOT_LOG_DEBUG(
        "Executing TVM jitted function %s for query string: %s", _function_name.c_str(), _query_string.c_str());
    uint64_t res = _entry_block->exec(exec_context);
    MOT_LOG_DEBUG("Function %s return code: %" PRIu64, _function_name.c_str(), res);
    return res;
}

uint64_t Function::execNext(ExecContext* exec_context)
{
    MOT_LOG_DEBUG("Starting/Continuing execution of pseudo-LLVM jitted function %s for query string: %s",
        _function_name.c_str(),
        _query_string.c_str());
    uint64_t res = _entry_block->execNext(exec_context);
    MOT_LOG_DEBUG("Function %s return code: %" PRIu64, _function_name.c_str(), res);
    return res;
}

void Function::addInstruction(Instruction* instruction)
{
    _all_instructions.push_back(instruction);
    if (instruction->GetRegisterRef() > _max_register_ref) {
        _max_register_ref = instruction->GetRegisterRef();
    }
}

void Function::addBlock(BasicBlock* block)
{
    _all_blocks.push_back(block);
    std::pair<BlockNameMap::iterator, bool> pairib =
        _block_name_count.insert(BlockNameMap::value_type(block->getName(), 0));
    if (!pairib.second) {
        int count = ++pairib.first->second;
        block->setCount(count);
    }
}

bool Function::finalize()
{
    // cleanup empty blocks (and in the future maybe optimize)
    removeEmptyBlocks();
    rewireTrivialBlocks();
    return true;  // currently no reason for error
}

bool Function::verify()
{
    bool result = false;
    MOT_LOG_TRACE("Verifying function: %s", getName());
    if (getRegisterCount() >= MOT_JIT_MAX_FUNC_REGISTERS) {
        MOT_LOG_TRACE("Function %s is invalid: register count %u exceeds maximum allowed %u",
            getName(),
            (unsigned)getRegisterCount(),
            (unsigned)MOT_JIT_MAX_FUNC_REGISTERS);
    } else {
        BasicBlockList::iterator block_itr = _all_blocks.begin();
        while (block_itr != _all_blocks.end()) {
            BasicBlock* block = *block_itr;
            if (!block->verify()) {
                MOT_LOG_ERROR("Function %s is invalid: Block %s verification failed", getName(), block->getName());
                return false;
            }
            ++block_itr;
        }
        result = true;
    }
    return result;
}

void Function::dump()
{
    fprintf(stderr, "Function %s() {\n", getName());
    BasicBlockList::iterator block_itr = _all_blocks.begin();
    while (block_itr != _all_blocks.end()) {
        (*block_itr)->dump();
        ++block_itr;
    }
    fprintf(stderr, "}\n\n");
}

void Function::removeEmptyBlocks()
{
    BasicBlockList::iterator block_itr = _all_blocks.begin();
    while (block_itr != _all_blocks.end()) {
        BasicBlock* block = *block_itr;
        if (block->isEmpty()) {
            block_itr = _all_blocks.erase(block_itr);
            MOT_LOG_TRACE("Function %s: removing empty block %s", getName(), block->getName());
            delete block;
        } else {
            ++block_itr;
        }
    }
}

void Function::rewireTrivialBlocks()
{
    BasicBlock* trivial_block = findTrivialBlock();
    while (trivial_block != NULL) {
        rewireTrivialBlock(trivial_block);
        trivial_block = findTrivialBlock();
    }
}

BasicBlock* Function::findTrivialBlock()
{
    BasicBlockList::iterator block_itr = _all_blocks.begin();
    while (block_itr != _all_blocks.end()) {
        BasicBlock* block = *block_itr;
        if (block->isTrivial()) {
            return block;
        }
        ++block_itr;
    }
    return NULL;
}

void Function::rewireTrivialBlock(BasicBlock* trivial_block)
{
    // retrieve the only next block of this block
    BasicBlock* next_block = trivial_block->getNextTrivalBlock();

    // 1. fix all instructions in previous blocks referring to this block with the next block
    replaceTrivialBlockReference(trivial_block, next_block);

    // 2. add to the predecessors of the next block the predecessors of this block
    next_block->addTrivialBlockPredecessors(trivial_block);

    // 3. remove the block from the block list of this function
    if (trivial_block == _entry_block) {
        _entry_block = next_block;
    }
    removeTrivialBlock(trivial_block);
}

void Function::replaceTrivialBlockReference(BasicBlock* trivial_block, BasicBlock* next_block)
{
    BasicBlockList::iterator block_itr = _all_blocks.begin();
    while (block_itr != _all_blocks.end()) {
        BasicBlock* block = *block_itr;
        if (block != trivial_block) {
            block->replaceTrivialBlockReference(trivial_block, next_block);
        }
        ++block_itr;
    }
}

void Function::removeTrivialBlock(BasicBlock* trivial_block)
{
    BasicBlockList::iterator block_itr = find(_all_blocks.begin(), _all_blocks.end(), trivial_block);
    if (block_itr != _all_blocks.end()) {
        _all_blocks.erase(block_itr);
        delete trivial_block;
    }
}

Builder::Builder() : _current_function(NULL), _current_block(NULL), _next_register_ref(0)
{}

Builder::~Builder()
{}

Function* Builder::createFunction(const char* function_name, const char* query_string)
{
    _current_block = new (std::nothrow) BasicBlock("entry");
    _current_function = new (std::nothrow) Function(function_name, query_string, _current_block);
    return _current_function;
}

Instruction* Builder::addInstruction(Instruction* instruction)
{
    Instruction* result = instruction;
    if (instruction->GetType() == Instruction::Regular) {
        instruction->SetRegisterRef(_next_register_ref);
        result = new (std::nothrow) RegisterRefInstruction(_next_register_ref);
        if (result == nullptr) {
            return nullptr;
        }
        _current_block->recordRegisterReferenceDefinition(_next_register_ref);  // for later validation
        _current_function->addInstruction(result);                              // for cleanup only
        ++_next_register_ref;
    }
    _current_block->addInstruction(instruction);
    _current_function->addInstruction(instruction);
    return result;
}

Instruction* Builder::addExpression(Expression* expr)
{
    return addInstruction(new (std::nothrow) ExpressionInstruction(expr));
}

Instruction* Builder::addGetExpressionRC()
{
    return addInstruction(new (std::nothrow) GetExpressionRCInstruction());
}

BasicBlock* Builder::CreateBlock(const char* name)
{
    BasicBlock* block = new (std::nothrow) BasicBlock(name);
    _current_function->addBlock(block);
    return block;
}

Instruction* Builder::CreateICmp(Instruction* lhs_instruction, Instruction* rhs_instruction, JitExec::JitICmpOp cmp_op)
{
    return addInstruction(new (std::nothrow) ICmpInstruction(lhs_instruction, rhs_instruction, cmp_op));
}

void Builder::CreateCondBr(Instruction* if_test, BasicBlock* true_branch, BasicBlock* false_branch)
{
    addInstruction(new (std::nothrow) CondBranchInstruction(if_test, true_branch, false_branch));
}

void Builder::CreateBr(BasicBlock* target_branch)
{
    addInstruction(new (std::nothrow) BranchInstruction(target_branch));
}

void Builder::SetInsertPoint(BasicBlock* block)
{
    _current_block = block;
}

void Builder::CreateRet(Instruction* instruction)
{
    addInstruction(new (std::nothrow) ReturnInstruction(instruction));
}

Instruction* Builder::CreateConst(uint64_t value)
{
    Instruction* result = new (std::nothrow) ConstInstruction(value);
    _current_function->addInstruction(result);
    return result;
}

BasicBlock* Builder::GetInsertBlock()
{
    return _current_block;
}

bool Builder::currentBlockEndsInBranch()
{
    return _current_block->endsInBranch();
}
}  // namespace tvm
