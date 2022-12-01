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
 * jit_source.h
 *    A stencil used for caching a compiled function and cloning other context objects with identical query.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_source.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_SOURCE_H
#define JIT_SOURCE_H

#include <pthread.h>

#include "jit_context.h"
#include "pgstat.h"
#include "jit_common.h"
#include "jit_plan.h"
#include "mot_list.h"

namespace JitExec {

/**
 * @enum Constant values denoting code-generation state of the JIT source.. */
enum class JitCodegenState : uint8_t {
    /** @var Generated code is ready for use (the source context can be cloned). */
    JIT_CODEGEN_READY,

    /** @var Generated code is unavailable yet (code is being generated at the moment). */
    JIT_CODEGEN_UNAVAILABLE,

    /** @var Code generation failed. This query will not be JIT-ted again until invalidation. */
    JIT_CODEGEN_ERROR,

    /**
     * @var The generated code expired since a DDL caused this query to be invalidated. Caller is required to
     * re-generate code for the query. This is a temporary state, and the first session to revalidate the JIT source
     * will atomically change state to unavailable.
     */
    JIT_CODEGEN_EXPIRED,

    /** @var The generated code is no longer usable, since the stored procedure was dropped. */
    JIT_CODEGEN_DROPPED,

    /** @var The generated code is temporarily unusable, since the stored procedure is about to be replaced. */
    JIT_CODEGEN_REPLACED,

    /** @var The generated code is deprecate. */
    JIT_CODEGEN_DEPRECATE,

    /** @var The invoked query is pending an invoked stored procedure to finish compilation. */
    JIT_CODEGEN_PENDING,

    /** @brief Code for non-jittable sub-query. */
    JIT_CODEGEN_NONE
};

/** @enum Constants denoting jittable status. */
enum class JitSourceStatus : uint8_t {
    /** @var Denotes JIT source is in invalid state (temporary state after invalidation due to DDL). */
    JIT_SOURCE_INVALID,

    /** @var Denotes JIT source is in jittable state. */
    JIT_SOURCE_JITTABLE,

    /** @var Denotes JIT source is in unjittable state. */
    JIT_SOURCE_UNJITTABLE
};

/** @enum Constants denoting source operation. */
enum class JitSourceOp {
    /** @var No special action should be taken during clone. */
    JIT_SOURCE_NO_OP,

    /** @var Purge all moved contexts. */
    JIT_SOURCE_PURGE_ONLY,

    /** @var Purge and invalidate all moved contexts. */
    JIT_SOURCE_INVALIDATE,

    /** @var Mark functions replaced for all moved contexts. */
    JIT_SOURCE_REPLACE_FUNCTION,

    /** @var Mark functions dropped for all moved contexts. */
    JIT_SOURCE_DROP_FUNCTION
};

/**
 * @struct JitSource A stencil used for caching a compiled function and cloning other context
 * objects with identical query.
 */
struct __attribute__((packed)) JitSource {
    /** @var Lock used for concurrent compilation. */
    pthread_mutex_t m_lock;  // L1 offset 0 (40 bytes)

    /** @var Keep noisy lock in its own cache line. */
    uint8_t m_padding1[24];  // L1 offset 40

    /** @var The compiled function. */
    MotJitContext* m_sourceJitContext;  // L1 offset 0

    /** @var Tells whether the object was initialized successfully. */
    uint8_t m_initialized;  // L1 offset 8

    /** @var Specifies whether this JIT source is pending for child compilation to finish. */
    uint8_t m_pendingChildCompile;  // L1 offset 9

    /** @var The code generation state of the context. */
    JitCodegenState m_codegenState;  // L1 offset 10

    /** @var The jittable state of the context. */
    JitSourceStatus m_status;  // L1 offset 11

    /** @var The type of context being managed. */
    JitContextType m_contextType;  // L1 offset 12

    /** @var Specified the command type of the contained JIT context*/
    JitCommandType m_commandType;  // L1 offset 13

    /** @var JIT source usage. */
    JitContextUsage m_usage;  // L1 offset 14

    /**
     * @var Flag to indicate that the source was dropped concurrently during compilation.
     * This source should be deprecated once the compilation finishes.
     */
    uint8_t m_deprecatedPendingCompile;  // L1 offset 15

    /** @var Manage a list of cached context source objects for each session. */
    JitSource* m_next;  // L1 offset 16

    /** @var The source query string. */
    char* m_queryString;  // L1 offset 24

    /** @var JIT context list for cleanup during relation modification. */
    MotJitContext* m_contextList;  // L1 offset 32

    /** @var Time of last change. */
    TimestampTz m_timestamp;  // L1 offset 40

    /** @var The function id in case of a function or an invoke query source. */
    Oid m_functionOid;  // L1 offset 48

    /** @var The table id in case of a query source. */
    uint32_t m_tableId;  // L1 offset 52

    /** @var The inner table id in case of a join query source. */
    uint32_t m_innerTableId;  // L1 offset 56

    /** @var Unique identifier of the source in the source pool. */
    uint32_t m_sourceId;  // L1 offset 60

    /** @var Code generation statistics. */
    JitCodegenStats m_codegenStats;  // L1 offset 0

    /** @var JIT context list for delaying source context destruction until safe point in time. */
    MotJitContext* m_deprecateContextList;  // L1 offset 32

    /** @var The function txn id in case of a function or an invoke query source. */
    TransactionId m_functionTxnId;  // L1 offset 40

    /** @var The Txn Id which expired this source (applicable only for function or an invoke query source). */
    TransactionId m_expireTxnId;  // L1 offset 48

    /** @var Manage a list of deprecated sources. */
    JitSource* m_nextDeprecate;  // L1 offset 56
};

/** @class A simple linked list of JIT source objects. */
class JitSourceList {
public:
    /** @brief Default constructor. */
    JitSourceList() : m_head(nullptr), m_tail(nullptr)
    {}

    /** @brief Default destructor. */
    ~JitSourceList()
    {
        m_head = nullptr;
        m_tail = nullptr;
    }

    /** @brief Pushes an element on back of list. */
    inline void PushBack(JitSource* jitSource)
    {
        if (m_tail == nullptr) {
            m_head = m_tail = jitSource;
        } else {
            m_tail->m_next = jitSource;
            m_tail = jitSource;
        }
        jitSource->m_next = nullptr;
    }

    /** @brief Pushes an element on back of list. */
    inline void PushBackDeprecate(JitSource* jitSource)
    {
        if (m_tail == nullptr) {
            m_head = m_tail = jitSource;
        } else {
            m_tail->m_nextDeprecate = jitSource;
            m_tail = jitSource;
        }
        jitSource->m_nextDeprecate = nullptr;
    }

    /** @brief Starts iterating over list. */
    inline JitSource* Begin()
    {
        return m_head;
    }

    /** @brief Queries whether list is empty. */
    inline bool Empty() const
    {
        return (m_head == nullptr);
    }

private:
    /** @var The list head element. */
    JitSource* m_head;

    /** @var The list tail element. */
    JitSource* m_tail;
};

inline bool IsInvokeQuerySource(JitSource* jitSource)
{
    return ((jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) &&
            (jitSource->m_commandType == JIT_COMMAND_INVOKE));
}

inline bool IsSimpleQuerySource(JitSource* jitSource)
{
    return ((jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) &&
            (jitSource->m_commandType != JIT_COMMAND_INVOKE));
}

inline bool IsJitSourceRecyclable(JitSource* jitSource)
{
    return ((jitSource->m_contextList == nullptr) && (jitSource->m_deprecateContextList == nullptr) &&
            (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_DEPRECATE));
}

/**
 * @brief Initializes a JIT source object.
 * @param jitSource The JIT source to initialize.
 * @param queryString The query string of the JIT source.
 * @param usage The JIT source usage (global, secondary global, or local)
 * @return True if initialization succeeded, otherwise false. Consult @ref mm_get_root_error() for further details.
 */
extern bool InitJitSource(JitSource* jitSource, const char* queryString, JitContextUsage usage);

/**
 * @brief Releases all resources associated with a JIT source.
 * @param jitSource The JIT source to destroy.
 */
extern void DestroyJitSource(JitSource* jitSource);

/**
 * @brief Re-initializes an already initialized JIT source object.
 * @detail Since JIT source objects are pooled, a quick re-init function is provided for reusing
 * reclaimed JIT source objects. Only partial initialization is required in this case.
 * @param jitSource The JIT source to re-initialize.
 * @param queryString The query string of the JIT source.
 * @param usage Specifies the usage of the source.
 */
extern void ReInitJitSource(JitSource* jitSource, const char* queryString, JitContextUsage usage);

/** @brief Locks the JIT source for concurrent access. */
extern void LockJitSource(JitSource* jitSource);

/** @brief Unlocks the JIT source for concurrent access. */
extern void UnlockJitSource(JitSource* jitSource);

/**
 * @brief Gets a valid and ready to use JIT context from the JIT source. If the JIT source is ready, then its
 * contained context will be cloned, otherwise only current state is returned without any cloning.
 * @param jitSource The JIT source to wait upon.
 * @param[out] readySourceJitContext The resulting ready JIT context (already cloned).
 * @param usage The required resulting context usage.
 * @param jitPlan The JIT plan
 * @return The resulting context status.
 */
extern JitCodegenState GetReadyJitContext(
    JitSource* jitSource, MotJitContext** readySourceJitContext, JitContextUsage usage, JitPlan* jitPlan);

/** @brief Queries whether a JIT source is ready. */
extern bool IsJitSourceReady(JitSource* jitSource);

/** @brief Retrieves the valid state of the contained JIT context. */
extern int GetJitSourceValidState(JitSource* jitSource);

/**
 * @brief Re-validates a JIT source after DDL that caused invalidation. In case if another thread is already
 * validating the source, then unavailable state will be returned.
 * @param jitSource The JIT source to re-validate.
 * @param[optional] functionTxnId The function txn id in case of a function or an invoke query source.
 * @return The resulting context status.
 */
extern JitCodegenState RevalidateJitSource(JitSource* jitSource, TransactionId functionTxnId = InvalidTransactionId);

/**
 * @brief Sets the cached context status to error.
 * jitSource The JIT source to set its error status.
 * @param errorCode The error code of the root cause for the failure.
 * @param[out,optional] newState On return specifies the current JIT source state (usually error as specified, but on
 * race conditions could be deprecate).
 */
extern void SetJitSourceError(JitSource* jitSource, int errorCode, JitCodegenState* newState = nullptr);

/**
 * @brief Sets the cached context status to error.
 * @param relationId The external identifier of the relation that caused this JIT source to expire.
 * @param functionReplace Specifies whether this call was triggered by "REPLACE FUNCTION".
 */
extern void SetJitSourceExpired(JitSource* jitSource, uint64_t relationId, bool functionReplace = false);

/**
 * @brief Installs a ready source JIT context.
 * @param jitSource The JIT source to set its ready status.
 * @param readySourceJitContext The ready source JIT context to install.
 * @param codegenStats Code generation statistics.
 * @param[out,optional] newState On return specifies the current JIT source state (usually ready as specified, but on
 * race conditions could be deprecate).
 * @return True if operation succeeded, or false in case the JIT source is already ready with a valid JIT context.
 */
extern bool SetJitSourceReady(JitSource* jitSource, MotJitContext* readySourceJitContext, JitCodegenStats* codegenStats,
    JitCodegenState* newState = nullptr);

/**
 * @brief Registers JIT context for cleanup when its JIT source gets purged due to relation modification.
 * @param jitSource The originating JIT source.
 * @param jitContext The JIT context to register.
 */
extern void AddJitSourceContext(JitSource* jitSource, MotJitContext* jitContext);

/**
 * @brief Un-registers JIT context from cleanup when its JIT source gets purged due to relation modification.
 * @param jitSource The originating JIT source.
 * @param cleanupContext The JIT context to un-register.
 */
extern void RemoveJitSourceContext(JitSource* jitSource, MotJitContext* cleanupContext);

/** @brief Queries whether a JIT source refers to a given relation. */
extern bool JitSourceRefersRelation(JitSource* jitSource, uint64_t relationId, bool searchDeprecate);

/** @brief Queries whether a JIT source refers to a given function. */
extern bool JitSourceRefersSP(JitSource* jitSource, Oid functionId);

/** @brief Retrieves the number of non-jittable sub-queries in this source. */
extern uint32_t GetJitSourceNonJitQueryCount(JitSource* jitSource);

/** @brief Purges the JIT source from all key/index references to the given relation. */
extern void PurgeJitSource(JitSource* jitSource, uint64_t relationId);

/** @brief Marks the JIT source as deprecate. */
extern void DeprecateJitSource(JitSource* jitSource, bool markOnly);

/** @brief Retrieves a string representation of the current source code generation status. */
extern const char* JitCodegenStateToString(JitCodegenState state);

/** @brief Retrieves a string representation of the current source status. */
extern const char* JitSourceStatusToString(JitSourceStatus status);

/** @brief Make a session-local clone of the given JIT source. */
extern JitSource* CloneLocalJitSource(JitSource* jitSource, bool cloneContext, JitSourceOp sourceOp);

/** @brief Make a global clone of the given JIT source. */
extern JitSource* CloneGlobalJitSource(JitSource* jitSource);

/** @brief Merges a session-local JIT source its counterpart global source. */
extern void MergeJitSource(
    JitSource* localSource, JitSource* globalSource, bool applyLocalChanges, JitContextList* rollbackInvokeContextList);

/** @brief Move current session-local context objects from global source to local source. */
extern void MoveCurrentSessionContexts(JitSource* source, JitSource* target, JitSourceOp sourceOp);

/** @brief Recycles deprecate source context that can be recycled. */
extern bool CleanUpDeprecateJitSourceContexts(JitSource* source);

/** @brief Queries whether this is a premature revalidation attempt on the given JIT source. */
extern bool IsPrematureRevalidation(JitSource* jitSource, JitPlan* jitPlan);
extern bool IsPrematureRevalidation(JitSource* jitSource, TransactionId functionTxnId);

/** @brief Unlinks all the session-local context objects from local source. */
extern void UnlinkLocalJitSourceContexts(JitSource* localSource);
}  // namespace JitExec

#endif
