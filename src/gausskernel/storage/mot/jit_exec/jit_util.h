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
 * jit_util.h
 *    Definitions for emitting LLVM code.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_util.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_UTIL_H
#define JIT_UTIL_H

// This file contains definitions for emitting LLVM code.
// The macros are identical for both flavors.
// In order to use these macros properly, include either jit_llvm_util.h before including this file.
// They contain definitions for the following essential macros:
//
// - JIT_IF_BEGIN/CURRENT
// - JIT_WHILE_BEGIN/CURRENT
// - JIT_DO_WHILE_BEGIN/CURRENT
// - JIT_FOR_BEGIN/CURRENT
// - JIT_SWITCH_BEGIN/CURRENT
// - JIT_TRY_BEGIN/CURRENT
//
// If statements should be used as follows:
//
// JIT_IF_BEGIN(label_name)
//  ... emit code for evaluating while condition value
// JIT_IF_EVAL(cond) // or JIT_IF_EVAL_NOT(), or JIT_IF_EVAL_CMP()
//   ... // emit code for success
// JIT_ELSE() // begin else block - optional
//  ... // emit code for failure
// JIT_IF_END()
//
// While statements should be used as follows:
//
// JIT_WHILE_BEGIN(label_name)
//  ... emit code for evaluating while condition value
// JIT_WHILE_EVAL(cond) // or JIT_WHILE_EVAL_NOT(), or JIT_WHILE_EVAL_CMP()
//  ... // emit code for while body
//  JIT_WHILE_BREAK() // emit code to break from while loop
//  JIT_WHILE_CONTINUE()  // emit code to stop current iteration and re-evaluate while condition
// JIT_WHILE_END()
//
// Do-while statements are similar:
//
// JIT_DO_WHILE_BEGIN(label_name)
//  ... // emit code for do-while body
//  JIT_DO_WHILE_BREAK() // emit code to break from do-while loop
//  JIT_DO_WHILE_CONTINUE()  // emit code to stop current iteration and re-evaluate do-while condition
//  ... emit code for evaluating while condition value
// JIT_DO_WHILE_EVAL(cond) // or JIT_DO_WHILE_EVAL_NOT(), or JIT_DO_WHILE_EVAL_CMP()
// JIT_DO_WHILE_END()
//
// For loop statements should be used as follows:
//
// JIT_FOR_BEGIN(label_name, counter_name, init_value, step, bound, cmpOp)
//    ... // emit code for body of for-loop
//    JIT_FOR_COUNTER() // get counter current value (INT32)
// JIT_FOR_END()
//
// Switch-case statements should be used as follows:
//
// JIT_SWITCH_BEGIN(label_name, value)
//  JIT_CASE(value1)
//    ... // emit code for this case
//    JIT_CASE_BREAK() // break from case (mandatory, no fall-through is available, use many-case instead)
//  ...
//  JIT_CASE_MANY(value2)
//  JIT_CASE_MANY(value3)
//  ...
//  JIT_CASE_MANY(valueN)
//  JIT_END_CASE_MANY()
//    ... // emit code for cases value2 through valueN
//    JIT_CASE_BREAK() // break from case (mandatory)
//  ...
//  JIT_CASE_DEFAULT()
//    ... // emit code for default case
//    JIT_CASE_BREAK() // break from case (mandatory)
// JIT_SWITCH_END()
//
// Try-catch statements should be used as follows:
//
// JIT_TRY_BEGIN(label_name)
//    ... // emit code for try-block, can call JIT_THROW(value) to emit "throw" instruction
// JIT_CATCH(value1)
//    ... // emit code for catch-block, can call JIT_RETHROW() to emit "rethrow" instruction
// JIT_END_CATCH()  // end catch-block (mandatory)
// ...
// JIT_BEGIN_CATCH_MANY(value2)
// JIT_BEGIN_CATCH_MANY(value3)
// ...
// JIT_BEGIN_CATCH_MANY(valueN)
// JIT_END_CATCH_MANY()
//    ... // emit code for multiple value catch-block, can call JIT_RETHROW() to emit "rethrow" instruction
// JIT_END_CATCH()  // end catch-block (mandatory)
// ...
// JIT_CATCH_ALL()  // optional, catches all values
//    ... // emit code for default catch-block, can call JIT_RETHROW() to emit "rethrow" instruction
// JIT_END_CATCH()  // end catch-block (mandatory)
// JIT_TRY_END()
////////////////////////////////////////////////////////////////////////////////
// If Statement Macros
////////////////////////////////////////////////////////////////////////////////
/** @define Macro for evaluating the condition of a JIT if statement. */
#define JIT_IF_EVAL_CMP(lhs, rhs, cmpOp) JIT_IF_CURRENT()->JitIfCmp(lhs, rhs, cmpOp)

/** @define Macro for evaluating the condition of a JIT if statement. */
#define JIT_IF_EVAL(value) JIT_IF_CURRENT()->JitIfEval(value)

/** @define Macro for evaluating the condition of a JIT if statement. */
#define JIT_IF_EVAL_NOT(value) JIT_IF_CURRENT()->JitIfNot(value)

/** @define Macro for starting the else block in a JIT if statement. */
#define JIT_ELSE() JIT_IF_CURRENT()->JitElse()

/** @define Macro for ending a JIT if statement. */
#define JIT_IF_END()            \
    JIT_IF_CURRENT()->JitEnd(); \
    }

/** @define Macro for getting the condition success execution block of the current if statement. */
#define JIT_IF_EXEC_BLOCK() JIT_IF_CURRENT()->GetIfBlock()

/** @define Macro for getting the condition failure execution block of the current if statement. */
#define JIT_IF_ELSE_BLOCK() JIT_IF_CURRENT()->GetElseBlock()

/** @define Macro for getting the block after the current if statement. */
#define JIT_IF_POST_BLOCK() JIT_IF_CURRENT()->GetPostBlock()

////////////////////////////////////////////////////////////////////////////////
// While Statement Macros
////////////////////////////////////////////////////////////////////////////////
/** @define Macro for evaluating the condition of a JIT while statement. */
#define JIT_WHILE_EVAL_CMP(lhs, rhs, cmpOp) JIT_WHILE_CURRENT()->JitWhileCmp(lhs, rhs, cmpOp)

/** @define Macro for evaluating the condition of a JIT while statement. */
#define JIT_WHILE_EVAL(value) JIT_WHILE_CURRENT()->JitWhileEval(value)

/** @define Macro for evaluating the condition of a JIT while statement. */
#define JIT_WHILE_EVAL_NOT(value) JIT_WHILE_CURRENT()->JitWhileNot(value)

/** @define Macro for emitting a "continue" instruction within a JIT while statement. */
#define JIT_WHILE_CONTINUE() JIT_WHILE_CURRENT()->JitContinue()

/** @define Macro for emitting a "break" instruction within a JIT while statement. */
#define JIT_WHILE_BREAK() JIT_WHILE_CURRENT()->JitBreak()

/** @define Macro for ending a JIT while statement. */
#define JIT_WHILE_END()            \
    JIT_WHILE_CURRENT()->JitEnd(); \
    }

/** @define Macro for getting the condition evaluation block of the current while statement. */
#define JIT_WHILE_COND_BLOCK() JIT_WHILE_CURRENT()->GetCondBlock()

/** @define Macro for getting the condition success execution block of the current while statement. */
#define JIT_WHILE_EXEC_BLOCK() JIT_WHILE_CURRENT()->GetExecBlock()

/** @define Macro for getting the block after the current while statement. */
#define JIT_WHILE_POST_BLOCK() JIT_WHILE_CURRENT()->GetPostBlock()

////////////////////////////////////////////////////////////////////////////////
// Do-While Statement Macros
////////////////////////////////////////////////////////////////////////////////
/** @define Macro for emitting a "continue" instruction within a JIT do-while statement. */
#define JIT_DO_WHILE_CONTINUE() JIT_DO_WHILE_CURRENT()->JitContinue()

/** @define Macro for emitting a "break" instruction within a JIT do-while statement. */
#define JIT_DO_WHILE_BREAK() JIT_DO_WHILE_CURRENT()->JitBreak()

/** @define Macro for marking start of condition evaluation in a JIT do-while statement. */
#define JIT_DO_WHILE_COND() JIT_DO_WHILE_CURRENT()->JitCond()

/** @define Macro for evaluating the condition of a JIT do-while statement. */
#define JIT_DO_WHILE_EVAL_CMP(lhs, rhs, cmpOp) JIT_DO_WHILE_CURRENT()->JitWhileCmp(lhs, rhs, cmpOp)

/** @define Macro for evaluating the condition of a JIT do-while statement. */
#define JIT_DO_WHILE_EVAL(value) JIT_DO_WHILE_CURRENT()->JitWhileEval(value)

/** @define Macro for evaluating the condition of a JIT do-while statement. */
#define JIT_DO_WHILE_EVAL_NOT(value) JIT_DO_WHILE_CURRENT()->JitWhileNot(value)

/** @define Macro for ending a JIT while statement. */
#define JIT_DO_WHILE_END()            \
    JIT_DO_WHILE_CURRENT()->JitEnd(); \
    }

/** @define Macro for getting the condition evaluation block of the current while statement. */
#define JIT_DO_WHILE_COND_BLOCK() JIT_DO_WHILE_CURRENT()->GetCondBlock()

/** @define Macro for getting the condition success execution block of the current while statement. */
#define JIT_DO_WHILE_EXEC_BLOCK() JIT_DO_WHILE_CURRENT()->GetExecBlock()

/** @define Macro for getting the block after the current while statement. */
#define JIT_DO_WHILE_POST_BLOCK() JIT_DO_WHILE_CURRENT()->GetPostBlock()

////////////////////////////////////////////////////////////////////////////////
// For Statement Macros
////////////////////////////////////////////////////////////////////////////////

/** @define Macro for emitting a "continue" instruction within a JIT for statement. */
#define JIT_FOR_CONTINUE() JIT_FOR_CURRENT()->JitContinue()

/** @define Macro for emitting a "break" instruction within a JIT for statement. */
#define JIT_FOR_BREAK() JIT_FOR_CURRENT()->JitBreak()

/** @define Macro for emitting a "break" instruction within a JIT for statement. */
#define JIT_FOR_COUNTER() JIT_FOR_CURRENT()->GetCounter()

/** @define Macro for ending a JIT for statement. */
#define JIT_FOR_END()            \
    JIT_FOR_CURRENT()->JitEnd(); \
    }

/** @define Macro for getting the condition evaluation block of the current for statement. */
#define JIT_FOR_INCREMENT_BLOCK() JIT_FOR_CURRENT()->GetIncrementBlock()

/** @define Macro for getting the block after the current for statement. */
#define JIT_FOR_POST_BLOCK() JIT_FOR_CURRENT()->GetPostBlock()

////////////////////////////////////////////////////////////////////////////////
// Common Loop Statement Macros
////////////////////////////////////////////////////////////////////////////////

/** @define Macro for breaking from current loop. */
#define JIT_LOOP_BREAK() JIT_LOOP_CURRENT()->JitBreak()

/** @define Macro for continuing to next iteration in current loop. */
#define JIT_LOOP_CONTINUE() JIT_LOOP_CURRENT()->JitContinue()

/** @define Macro for getting the condition checking block of the current loop. */
#define JIT_LOOP_COND_BLOCK() JIT_LOOP_CURRENT()->GetCondBlock()

/** @define Macro for getting the post-block of the current loop (code after the loop). */
#define JIT_LOOP_POST_BLOCK() JIT_LOOP_CURRENT()->GetPostBlock()

////////////////////////////////////////////////////////////////////////////////
// Switch-Case Statement Macros
////////////////////////////////////////////////////////////////////////////////

/** @define Macro for emitting a "case value:" block within a JIT switch-case statement. */
#define JIT_CASE(value) JIT_SWITCH_CURRENT()->JitCase(value)

/** @define Macro for emitting a "case value:" with multiple values. */
#define JIT_CASE_MANY(value) JIT_SWITCH_CURRENT()->JitCaseMany(value)

/** @define Macro for terminating multiple "case value:" labels.  */
#define JIT_CASE_MANY_TERM() JIT_SWITCH_CURRENT()->JitCaseManyTerm()

/** @define Macro for emitting a "break" instruction within a JIT switch-case statement. */
#define JIT_CASE_BREAK() JIT_SWITCH_CURRENT()->JitBreak()

/** @define Macro for ending a case block within a JIT switch-case statement. */
#define JIT_CASE_END() JIT_SWITCH_CURRENT()->JitCaseEnd()

/** @define Macro for emitting a "default:" case block within a JIT switch-case statement. */
#define JIT_CASE_DEFAULT() JIT_SWITCH_CURRENT()->JitDefault()

/** @define Macro for ending a JIT for statement. */
#define JIT_SWITCH_END()            \
    JIT_SWITCH_CURRENT()->JitEnd(); \
    }

////////////////////////////////////////////////////////////////////////////////
// Try-Catch Statement Macros
////////////////////////////////////////////////////////////////////////////////

/** @define Macro for emitting a "throw value" instruction within a JIT try-catch statement. */
#define JIT_THROW(value) JIT_TRY_CURRENT()->JitThrow(value)

/** @define Macro for emitting a "catch (value)" block within a JIT try-catch statement. */
#define JIT_BEGIN_CATCH(value) JIT_TRY_CURRENT()->JitBeginCatch(value)

/** @define Macro for emitting multiple "catch (value)" block within a JIT try-catch statement. */
#define JIT_BEGIN_CATCH_MANY(value) JIT_TRY_CURRENT()->JitBeginCatchMany(value)

/** @define Macro for ending a "catch multiple values" statement and starting the catch body block. */
#define JIT_CATCH_MANY_TERM() JIT_TRY_CURRENT()->JitCatchManyTerm()

/** @define Macro for ending a "catch [multiple] (value)" block within a JIT try-catch statement. */
#define JIT_END_CATCH() JIT_TRY_CURRENT()->JitEndCatch()

/** @define Macro for emitting a "catch(...)" block within a JIT try-catch statement. */
#define JIT_CATCH_ALL() JIT_TRY_CURRENT()->JitCatchAll()

/** @define Macro for emitting a "throw" instruction within a catch-block in a JIT try-catch statement. */
#define JIT_RETHROW() JIT_TRY_CURRENT()->JitRethrow()

/** @define Macro for ending a JIT try-catch statement. */
#define JIT_TRY_END()            \
    JIT_TRY_CURRENT()->JitEnd(); \
    }

/** @define Macro for getting the block after the current for statement. */
#define JIT_TRY_POST_BLOCK() JIT_TRY_CURRENT()->GetPostBlock()

#endif /* JIT_UTIL_H */
