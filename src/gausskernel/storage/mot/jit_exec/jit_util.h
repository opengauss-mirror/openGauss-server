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
 *    Definitions for emitting LLVM or TVM code.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_util.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_UTIL_H
#define JIT_UTIL_H

// this file contains definitions for emitting LLVM or TVM code
// the macros are identical for both flavors.
// in order to use these macros properly, include either jit_llvm_util.h or
// jit_tvm_util.h before including this file.
// They contain definitions for the following essential macros:
//
// - JIT_IF_BEGIN/CURRENT
// - JIT_WHILE_BEGIN/CURRENT
// - JIT_DO_WHILE_BEGIN/CURRENT
// - JIT_FOR_BEGIN/CURRENT
//
// If statements should be used as follows:
//
// JIT_IF_BEGIN(label_name)
// JIT_IF_EVAL(some_computed_value) // or JIT_IF_EVAL_NOT(), or JIT_IF_EVAL_CMP()
//   ... // emit code for success
// JIT_ELSE() // begin else block - optional
//  ... // emit code for failure
// JIT_IF_END()
//
// While statements should be used as follows:
//
// JIT_WHILE_BEGIN(label_name)
// JIT_WHILE_EVAL(some_computed_value) // or JIT_WHILE_EVAL_NOT(), or JIT_WHILE_EVAL_CMP()
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
// JIT_DO_WHILE_EVAL(some_computed_value) // or JIT_DO_WHILE_EVAL_NOT(), or JIT_DO_WHILE_EVAL_CMP()
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
//  JIT_CASE_BEGIN(value1)
//    ... // emit code for this case
//    JIT_CASE_BREAK() // break from case (optional, might fall-through)
//  JIT_CASE_END()  // required? if not then change name of JIT_CASE_BEGIN() to JIT_CASE()
//  ...
//  JIT_CASE_BEGIN(value1)
//    ... // emit code for this case
//    JIT_CASE_BREAK() // break from case (optional, might fall-through)
//  JIT_CASE_END()  // required?
//  ...
//  JIT_CASE_DEFAULT()
//    ... // emit code for default case
//    JIT_CASE_BREAK()  // mandatory?
//  JIT_CASE_END()  // required?
// JIT_SWITCH_END()
////////////////////////////////////////////////////////////////////////////////
// If Statement Macros (both LLVM and TVM)
////////////////////////////////////////////////////////////////////////////////
/** @define Macro for evaluating the condition of a JIT if statement. */
#define JIT_IF_EVAL_CMP(lhs, rhs, cmpOp) JIT_IF_CURRENT()->JitIfCmp(lhs, rhs, cmpOp);

/** @define Macro for evaluating the condition of a JIT if statement. */
#define JIT_IF_EVAL(value) JIT_IF_CURRENT()->JitIfEval(value);

/** @define Macro for evaluating the condition of a JIT if statement. */
#define JIT_IF_EVAL_NOT(value) JIT_IF_CURRENT()->JitIfNot(value);

/** @define Macro for starting the else block in a JIT if statement. */
#define JIT_ELSE() JIT_IF_CURRENT()->JitElse();

/** @define Macro for ending a JIT if statement. */
#define JIT_IF_END()            \
    JIT_IF_CURRENT()->JitEnd(); \
    }

/** @define Macro for getting the condition evaluation block of the current if statement. */
#define JIT_IF_COND_BLOCK() JIT_IF_CURRENT()->GetCondBlock()

/** @define Macro for getting the condition success execution block of the current if statement. */
#define JIT_IF_EXEC_BLOCK() JIT_IF_CURRENT()->GetIfBlock()

/** @define Macro for getting the condition failure execution block of the current if statement. */
#define JIT_IF_ELSE_BLOCK() JIT_IF_CURRENT()->GetElseBlock()

/** @define Macro for getting the block after the current if statement. */
#define JIT_IF_POST_BLOCK() JIT_IF_CURRENT()->GetPostBlock()

////////////////////////////////////////////////////////////////////////////////
// While Statement Macros (both LLVM and TVM)
////////////////////////////////////////////////////////////////////////////////
/** @define Macro for evaluating the condition of a JIT while statement. */
#define JIT_WHILE_EVAL_CMP(lhs, rhs, cmpOp) JIT_WHILE_CURRENT()->JitWhileCmp(lhs, rhs, cmpOp);

/** @define Macro for evaluating the condition of a JIT while statement. */
#define JIT_WHILE_EVAL(value) JIT_WHILE_CURRENT()->JitWhileEval(value);

/** @define Macro for evaluating the condition of a JIT while statement. */
#define JIT_WHILE_EVAL_NOT(value) JIT_WHILE_CURRENT()->JitWhileNot(value);

/** @define Macro for emitting a "continue" instruction within a JIT while statement. */
#define JIT_WHILE_CONTINUE() JIT_WHILE_CURRENT()->JitContinue();

/** @define Macro for emitting a "break" instruction within a JIT while statement. */
#define JIT_WHILE_BREAK() JIT_WHILE_CURRENT()->JitBreak();

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
// Do-While Statement Macros (both LLVM and TVM)
////////////////////////////////////////////////////////////////////////////////
/** @define Macro for emitting a "continue" instruction within a JIT do-while statement. */
#define JIT_DO_WHILE_CONTINUE() JIT_DO_WHILE_CURRENT()->JitContinue();

/** @define Macro for emitting a "break" instruction within a JIT do-while statement. */
#define JIT_DO_WHILE_BREAK() JIT_DO_WHILE_CURRENT()->JitBreak();

/** @define Macro for marking start of condition evaluation in a JIT do-while statement. */
#define JIT_DO_WHILE_COND() JIT_DO_WHILE_CURRENT()->JitCond();

/** @define Macro for evaluating the condition of a JIT do-while statement. */
#define JIT_DO_WHILE_EVAL_CMP(lhs, rhs, cmpOp) JIT_DO_WHILE_CURRENT()->JitWhileCmp(lhs, rhs, cmpOp);

/** @define Macro for evaluating the condition of a JIT do-while statement. */
#define JIT_DO_WHILE_EVAL(value) JIT_DO_WHILE_CURRENT()->JitWhileEval(value);

/** @define Macro for evaluating the condition of a JIT do-while statement. */
#define JIT_DO_WHILE_EVAL_NOT(value) JIT_DO_WHILE_CURRENT()->JitWhileNot(value);

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

#endif /* JIT_UTIL_H */
