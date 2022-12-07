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

/*---------------------- Generic Constants -------------------*/
/** @define Impose a hard coded limit on depth of parsed expression. */
#define MOT_JIT_MAX_EXPR_DEPTH 10

/** @define We support up to 8 arguments for NULL management in expressions. */
#define MOT_JIT_EXPR_ARG_COUNT 8

/** @define The maximum number of arguments in a Boolean expression. */
#define MOT_JIT_MAX_BOOL_EXPR_ARGS 2

/** @define The maximum number of arguments in a function call expression. */
#define MOT_JIT_MAX_FUNC_EXPR_ARGS 3

/** @define The global query name-space name. */
#define MOT_JIT_GLOBAL_QUERY_NS "GLOBAL"

/** @define The maximum nesting level of compiled stored procedures. */
#define MOT_JIT_MAX_COMPILE_DEPTH 10

/** @define The maximum nesting level of compiled stored procedure blocks. */
#define MOT_JIT_MAX_BLOCK_DEPTH 10

/*---------------------- Optional Compile-Time Configuration -------------------*/
/*
 * To enable generating JIT code for full-scan queries, #define MOT_JIT_FULL_SCAN
 */

/*
 * To debug JIT execution, #define MOT_JIT_DEBUG
 */

/*
 * To enable features required for JIT testing, #define MOT_JIT_TEST
 */

/** @define Enable advanced WHERE clause operators (comment to disable). */
#define MOT_JIT_ADVANCED_WHERE_OP

/*---------------------- JIT Parse Errors -------------------*/
/** @define JIT encountered generic parsing error. */
#define MOT_JIT_GENERIC_PARSE_ERROR 1

/** @define JIT encountered "table not found" error. */
#define MOT_JIT_TABLE_NOT_FOUND 2

/*---------------------- JIT Profiler Constants -------------------*/
/** @define Invalid profile function identifier. */
#define MOT_JIT_PROFILE_INVALID_FUNCTION_ID ((uint32_t)-1)

/** @define Invalid profile region identifier. */
#define MOT_JIT_PROFILE_INVALID_REGION_ID ((uint32_t)-1)

/** @define The predefined "total" profile region. */
#define MOT_JIT_PROFILE_REGION_TOTAL "total"

/** @define The predefined "def-vars" profile region. */
#define MOT_JIT_PROFILE_REGION_DEF_VARS "def-vars"

/** @define The predefined "init-vars" profile region. */
#define MOT_JIT_PROFILE_REGION_INIT_VARS "init-vars"

/** @define The predefined "child-call" profile region. */
#define MOT_JIT_PROFILE_REGION_CHILD_CALL "child-call"

namespace JitExec {
/** @enum JIT context state constants. */
enum JitContextState : uint8_t {
    /** @var Denotes initial context state.*/
    JIT_CONTEXT_STATE_INIT,

    /** @var Denotes JIT context is ready for use. */
    JIT_CONTEXT_STATE_READY,

    /** @var Denotes JIT context is pending compilation to finish. */
    JIT_CONTEXT_STATE_PENDING,

    /** @var Denotes JIT context finished compilation successfully. */
    JIT_CONTEXT_STATE_DONE,

    /** @var Denotes JIT context is invalid and needs revalidation. */
    JIT_CONTEXT_STATE_INVALID,

    /** @var Denotes JIT context revalidation or compilation failed. */
    JIT_CONTEXT_STATE_ERROR,

    /** @var Denotes final context state. */
    JIT_CONTEXT_STATE_FINAL
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

    /** @var Range delete command. */
    JIT_COMMAND_RANGE_DELETE,

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
    JIT_COMMAND_COMPOUND_SELECT,

    /** @var Function (stored-procedure) execution command. */
    JIT_COMMAND_FUNCTION,

    /** @var Invoke stored-procedure command. */
    JIT_COMMAND_INVOKE
};

/** @enum JIT context usage constants. */
enum JitContextUsage : uint8_t {
    /** @var JIT context is used in global context. */
    JIT_CONTEXT_GLOBAL,

    /** @var JIT context is used in global context (but not as the primary stencil). */
    JIT_CONTEXT_GLOBAL_SECONDARY,

    /** @var JIT context is used in session-local context. */
    JIT_CONTEXT_LOCAL
};

/** @enum JIT purge scope constants. */
enum JitPurgeScope {
    /** @var Denotes that JIT query source objects are directly affected by the purge action. */
    JIT_PURGE_SCOPE_QUERY,

    /** @var Denotes that JIT stored procedure source objects are directly affected by the purge action. */
    JIT_PURGE_SCOPE_SP
};

/** @enum JIT purge action constants. */
enum JitPurgeAction {
    /** @var Denotes purge-only action. */
    JIT_PURGE_ONLY,

    /** @var Denotes purge and set-as-expired action. */
    JIT_PURGE_EXPIRE,

    /** @var Denotes purge and set-as-replaced action (stored procedures only). */
    JIT_PURGE_REPLACE
};

/** @brief Helper function to determine whether JIT context usage is global. */
inline bool IsJitContextUsageGlobal(JitContextUsage usage)
{
    return ((usage == JIT_CONTEXT_GLOBAL) || (usage == JIT_CONTEXT_GLOBAL_SECONDARY));
}

inline const char* JitContextUsageToString(JitContextUsage usage)
{
    return (usage == JIT_CONTEXT_GLOBAL)
               ? "global"
               : ((usage == JIT_CONTEXT_GLOBAL_SECONDARY) ? "secondary-global" : "session-local");
}

inline const char* JitPurgeScopeToString(JitPurgeScope purgeScope)
{
    return (purgeScope == JIT_PURGE_SCOPE_QUERY) ? "query" : "sp";
}

inline const char* JitPurgeActionToString(JitPurgeAction purgeAction)
{
    return (purgeAction == JIT_PURGE_ONLY) ? "purge" : ((purgeAction == JIT_PURGE_EXPIRE) ? "expire" : "replace");
}

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
