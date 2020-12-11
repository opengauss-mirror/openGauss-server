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
 * jit_def.h
 *    Jit execution constants.
 *
 * IDENTIFICATION
 *    src/include/storage/mot/jit_def.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_DEF_H
#define JIT_DEF_H

#include <stdint.h>

/** @define Impose a hard coded limit on depth of parsed expression. */
#define MOT_JIT_MAX_EXPR_DEPTH 10

/** @define We support up to 8 arguments for NULL management in expressions. */
#define MOT_JIT_EXPR_ARG_COUNT 8

/** @define The maximum number of registers used in a pseudo-function execution. */
#define MOT_JIT_MAX_FUNC_REGISTERS 4096

/** @define The maximum number of arguments in a Boolean expression. */
#define MOT_JIT_MAX_BOOL_EXPR_ARGS 2

/** @define The maximum number of arguments in a function call expression. */
#define MOT_JIT_MAX_FUNC_EXPR_ARGS 3

namespace JitExec {

// To debug JIT execution, #define MOT_JIT_DEBUG
// To use advanced WHERE clause operators, #define MOT_JIT_ADVANCED_WHERE_OP
// To enable features required for JIT testing, #define MOT_JIT_TEST

/** @enum JIT execution mode constants. */
enum JitExecMode : uint32_t {
    /** @var Invalid execution mode. */
    JIT_EXEC_MODE_INVALID,

    /** @var LLVM execution mode. */
    JIT_EXEC_MODE_LLVM,

    /** @var TVM execution mode. */
    JIT_EXEC_MODE_TVM
};

/** @enum JitCommandType Command types supported by jitted queries. */
enum JitCommandType : uint8_t {
    /** @var Invalid command type. */
    JIT_COMMAND_INVALID,

    /** @var Simple insert command. */
    JIT_COMMAND_INSERT,

    /** @var Simple (point-query) update command. */
    JIT_COMMAND_UPDATE,

    /** @var Simple (point-query) select command. */
    JIT_COMMAND_SELECT,

    /** @var Simple (point-query) delete command. */
    JIT_COMMAND_DELETE,

    /** @var Range update command. */
    JIT_COMMAND_RANGE_UPDATE,

    /** @var Unordered range select command. */
    JIT_COMMAND_RANGE_SELECT,

    /** @var Unordered full-scan select command. */
    JIT_COMMAND_FULL_SELECT,

    /** @var Aggregate range select command. */
    JIT_COMMAND_AGGREGATE_RANGE_SELECT,

    /** @var Join command resulting in a single tuple. */
    JIT_COMMAND_POINT_JOIN,

    /** @var Join range select command. */
    JIT_COMMAND_RANGE_JOIN,

    /** @var Join aggregate command. */
    JIT_COMMAND_AGGREGATE_JOIN,

    /** @var Compound select command (point-select with sub-queries). */
    JIT_COMMAND_COMPOUND_SELECT
};

/** @enum JIT context usage constants. */
enum JitContextUsage : uint8_t {
    /** @var JIT context is used in global context. */
    JIT_CONTEXT_GLOBAL,

    /** @var JIT context is used in session-local context. */
    JIT_CONTEXT_LOCAL
};

/** @enum Constants for classifying operators in WHERE clause. */
enum JitWhereOperatorClass : uint8_t {
    /** @var Invalid where operator class. */
    JIT_WOC_INVALID,

    /** @var "Equals" where operator class. */
    JIT_WOC_EQUALS,

    /** @var "Less than" where operator class. */
    JIT_WOC_LESS_THAN,

    /* @var "Greater than" where operator class. */
    JIT_WOC_GREATER_THAN,

    /** @var "Less than" or "equals to" where operator class. */
    JIT_WOC_LESS_EQUALS,

    /** @var "Greater than" or "equals to" where operator class */
    JIT_WOC_GREATER_EQUALS
};

/** @enum Integer-comparison operator types. */
enum JitICmpOp {
    /** @var Designates "equals" operator. */
    JIT_ICMP_EQ,

    /** @var Designates "not-equals" operator. */
    JIT_ICMP_NE,

    /** @var Designates "greater-than" operator. */
    JIT_ICMP_GT,

    /** @var Designates "greater-equals" operator. */
    JIT_ICMP_GE,

    /** @var Designates "less-than" operator. */
    JIT_ICMP_LT,

    /** @var Designates "less-equals" operator. */
    JIT_ICMP_LE,
};

/** @enum Range scan type constants. */
enum JitRangeScanType {
    /** @var No range scan. */
    JIT_RANGE_SCAN_NONE,

    /** @var Main range scan. In JOIN queries this designates the outer loop scan. */
    JIT_RANGE_SCAN_MAIN,

    /** @var Designates inner loop range scan. Can be specified only on JOIN queries.*/
    JIT_RANGE_SCAN_INNER,

    /** @var Sub-query range scan. */
    JIT_RANGE_SCAN_SUB_QUERY
};

/** @enum Range bound mode constants. */
enum JitRangeBoundMode {
    /** @var Designates no range bound mode specification. */
    JIT_RANGE_BOUND_NONE,

    /** @var Designates to include the range bound in the range scan. */
    JIT_RANGE_BOUND_INCLUDE,

    /** @var Designates to exclude the range bound from the range scan. */
    JIT_RANGE_BOUND_EXCLUDE
};

/** @enum Range iterator type. */
enum JitRangeIteratorType {
    /** @var Designated no range iterator specification. */
    JIT_RANGE_ITERATOR_NONE,

    /** @var Designates the iterator pointing to the start of the range. */
    JIT_RANGE_ITERATOR_START,

    /** @var Designates the iterator pointing to the end of the range. */
    JIT_RANGE_ITERATOR_END
};

/** @enum Query sort order */
enum JitQuerySortOrder {
    /** @var Invalid query sort order. */
    JIT_QUERY_SORT_INVALID,

    /** @var Ascending query sort order. */
    JIT_QUERY_SORT_ASCENDING,

    /** @var Descending query sort order. */
    JIT_QUERY_SORT_DESCENDING
};
}  // namespace JitExec

#endif
