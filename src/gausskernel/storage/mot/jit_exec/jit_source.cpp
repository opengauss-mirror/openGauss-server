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
 * jit_source.cpp
 *    A stencil used for caching a compiled function and cloning other context objects with identical query.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_source.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "jit_source.h"
#include "utilities.h"
#include "utils/memutils.h"
#include "jit_statistics.h"
#include "debug_utils.h"
#include "mm_global_api.h"

namespace JitExec {
DECLARE_LOGGER(JitSource, JitExec);

// forward declarations
static char* CloneQueryString(const char* queryString);
static void FreeQueryString(char* queryString);
static char* ReallocQueryString(char* oldQueryString, const char* newQueryString);
static void SetJitSourceStatus(JitSource* jitSource, JitContext* readySourceJitContext, JitContextStatus status,
    bool notifyAll = true, int errorCode = 0, uint64_t relationId = 0, bool* invalidState = nullptr);
static const char* JitContextStatusToString(JitContextStatus status);
static void JitSourcePurgeContextList(JitSource* jitSource, uint64_t relationId);
static void RemoveJitSourceContextImpl(JitSource* jitSource, JitContext* jitContext);

#ifdef MOT_DEBUG
static void VerifyJitSourceStateTrans(JitSource* jitSource, JitContext* readySourceJitContext, JitContextStatus status);
#define MOT_JIT_SOURCE_VERIFY_STATE(jitSource, readySourceJitContext, status) \
    VerifyJitSourceStateTrans(jitSource, readySourceJitContext, status)
#else
#define MOT_JIT_SOURCE_VERIFY_STATE(jitSource, readySourceJitContext, status)
#endif

extern bool InitJitSource(JitSource* jitSource, const char* queryString)
{
    jitSource->_query_string = NULL;
    jitSource->_source_jit_context = NULL;
    jitSource->_initialized = 0;

    jitSource->_status = JIT_CONTEXT_UNAVAILABLE;
    jitSource->_next = NULL;
    jitSource->m_contextList = nullptr;

    int res = pthread_mutex_init(&jitSource->_lock, NULL);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(
            res, pthread_mutex_init, "Code Generation", "Failed to create mutex for jit-source");
    } else {
        res = pthread_cond_init(&jitSource->_cond, NULL);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(
                res, pthread_cond_init, "Code Generation", "Failed to create mutex for jit-source");
            pthread_mutex_destroy(&jitSource->_lock);
        } else {
            jitSource->_query_string = CloneQueryString(queryString);
            jitSource->_initialized = 1;
        }
    }

    return jitSource->_initialized != 0 ? true : false;
}

extern void DestroyJitSource(JitSource* jitSource)
{
    MOT_LOG_TRACE("Destroying JIT source %p with query string: %s", jitSource, jitSource->_query_string);
    if (jitSource->m_contextList != nullptr) {
        MOT_LOG_WARN("JIT source still contains registered JIT context objects, while destroying JIT source");
        JitContext* jitContext = jitSource->m_contextList;
        while (jitContext != nullptr) {
            jitContext->m_jitSource = nullptr;  // prevent crash during DestroyJitContext()
            jitContext = jitContext->m_nextInSource;
        }
        jitSource->m_contextList = nullptr;
    }

    if (jitSource->_source_jit_context != nullptr) {
        // this code is executed during shutdown, so the underlying
        // GsCodeGen object was already destroyed by the top memory context
        DestroyJitContext(jitSource->_source_jit_context);
        jitSource->_source_jit_context = NULL;
    }
    if (jitSource->_query_string != nullptr) {
        FreeQueryString(jitSource->_query_string);
        jitSource->_query_string = NULL;
    }
    if (jitSource->_initialized) {
        pthread_cond_destroy(&jitSource->_cond);
        pthread_mutex_destroy(&jitSource->_lock);
        jitSource->_initialized = 0;
    }
}

extern void ReInitJitSource(JitSource* jitSource, const char* queryString)
{
    jitSource->_query_string = ReallocQueryString(jitSource->_query_string, queryString);
    jitSource->_source_jit_context = NULL;
    jitSource->_status = JIT_CONTEXT_UNAVAILABLE;
    jitSource->_next = NULL;
}

extern JitContextStatus WaitJitContextReady(JitSource* jitSource, JitContext** readySourceJitContext)
{
    MOT_LOG_TRACE("%p: Waiting for jit-source to become ready on query: %s", jitSource, jitSource->_query_string);

    *readySourceJitContext = NULL;

    pthread_mutex_lock(&jitSource->_lock);

    while ((jitSource->_status == JIT_CONTEXT_UNAVAILABLE) && (jitSource->_source_jit_context == NULL)) {
        pthread_cond_wait(&jitSource->_cond, &jitSource->_lock);
    }

    JitContextStatus result = jitSource->_status;
    if (jitSource->_status == JIT_CONTEXT_EXPIRED) {
        // special case: we return to caller status-expired and change internal status to unavailable
        jitSource->_status = JIT_CONTEXT_UNAVAILABLE;
    } else if (jitSource->_status == JIT_CONTEXT_READY) {
        // we need to re-fetch index objects in case TRUNCATE TABLE was issued
        if ((jitSource->_source_jit_context->m_index == nullptr) &&
            (jitSource->_source_jit_context->m_commandType != JIT_COMMAND_INSERT)) {
            if (!ReFetchIndices(jitSource->_source_jit_context)) {
                MOT_LOG_TRACE("Failed to re-fetch index objects for source %p with query: %s",
                    jitSource,
                    jitSource->_query_string);
                result = JIT_CONTEXT_ERROR;
            }
        }
        if (result != JIT_CONTEXT_ERROR) {
            *readySourceJitContext = CloneJitContext(jitSource->_source_jit_context);
            if (*readySourceJitContext != nullptr) {
                ++u_sess->mot_cxt.jit_context_count;
                (*readySourceJitContext)->m_nextInSource = jitSource->m_contextList;
                jitSource->m_contextList = *readySourceJitContext;
                (*readySourceJitContext)->m_jitSource = jitSource;
                MOT_LOG_TRACE(
                    "Registered JIT context %p in JIT source %p for cleanup", *readySourceJitContext, jitSource);
                JitStatisticsProvider::GetInstance().AddCodeCloneQuery();
            } else {
                MOT_LOG_TRACE("Failed to clone ready source context %p", jitSource->_source_jit_context);
                JitStatisticsProvider::GetInstance().AddCodeCloneErrorQuery();
            }
        }
    }
    pthread_mutex_unlock(&jitSource->_lock);

    MOT_LOG_TRACE("%p: Found ready context %p with query %s (result: %s, status: %s, source context: %p)",
        jitSource,
        *readySourceJitContext,
        jitSource->_query_string,
        JitContextStatusToString(result),
        JitContextStatusToString(jitSource->_status),
        jitSource->_source_jit_context);
    return result;
}

extern void SetJitSourceError(JitSource* jitSource, int errorCode)
{
    SetJitSourceStatus(jitSource, NULL, JIT_CONTEXT_ERROR, true, errorCode);
}

extern void SetJitSourceExpired(JitSource* jitSource, uint64_t relationId)
{
    SetJitSourceStatus(jitSource, NULL, JIT_CONTEXT_EXPIRED, false, 0, relationId);
    JitStatisticsProvider::GetInstance().AddCodeExpiredQuery();
}

extern bool SetJitSourceReady(JitSource* jitSource, JitContext* readySourceJitContext)
{
    bool result = true;
    bool invalidState = false;
    SetJitSourceStatus(jitSource, readySourceJitContext, JIT_CONTEXT_READY, true, 0, 0, &invalidState);
    if (invalidState) {
        result = false;
    }
    return result;
}

static char* CloneQueryString(const char* queryString)
{
    char* newQueryString = NULL;
    if (queryString != nullptr) {
        size_t len = strlen(queryString);
        if (len > 0) {
            newQueryString = (char*)MOT::MemGlobalAlloc(len + 1);
            if (newQueryString != nullptr) {
                errno_t erc = strcpy_s(newQueryString, len + 1, queryString);
                securec_check(erc, "\0", "\0");
            }
        }
    }
    return newQueryString;
}

static void FreeQueryString(char* queryString)
{
    if (queryString != nullptr) {
        MOT::MemGlobalFree(queryString);
    }
}

static char* ReallocQueryString(char* oldQueryString, const char* newQueryString)
{
    if (oldQueryString == nullptr) {
        oldQueryString = CloneQueryString(newQueryString);
    } else if (newQueryString == nullptr) {
        FreeQueryString(oldQueryString);
        oldQueryString = NULL;
    } else {
        size_t oldLength = strlen(oldQueryString);
        size_t newLength = strlen(newQueryString);
        if (newLength < oldLength) {
            errno_t erc =
                strncpy_s(oldQueryString, oldLength, newQueryString, newLength + 1);  // copy terminating null too
            securec_check(erc, "\0", "\0");
        } else {
            newQueryString = (char*)MOT::MemGlobalRealloc(oldQueryString, newLength, MOT::MEM_REALLOC_COPY_ZERO);
        }
    }
    return oldQueryString;
}

static void SetJitSourceStatus(JitSource* jitSource, JitContext* readySourceJitContext, JitContextStatus status,
    bool notifyAll /* = true */, int errorCode /* = 0 */, uint64_t relationId /* = 0 */,
    bool* invalidState /* = nullptr */)
{
    MOT::Table* table = readySourceJitContext ? readySourceJitContext->m_table : NULL;
    MOT_LOG_TRACE("Jit source %p: Installing ready JIT context %p with status %s, table %p and query: %s",
        jitSource,
        readySourceJitContext,
        JitContextStatusToString(status),
        table,
        jitSource->_query_string);

    pthread_mutex_lock(&jitSource->_lock);

    if ((jitSource->_source_jit_context != NULL) && (status == JIT_CONTEXT_READY)) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_STATE, "JIT Compile", "Cannot set context as ready: already set");
        if (invalidState) {
            *invalidState = true;
        }
    } else {
        MOT_JIT_SOURCE_VERIFY_STATE(jitSource, readySourceJitContext, status);
        if (jitSource->_status == JIT_CONTEXT_EXPIRED) {
            if (status != JIT_CONTEXT_EXPIRED) {
                MOT_LOG_TRACE("Fixed expired JIT source %p with context %p (query string: %s)",
                    jitSource,
                    readySourceJitContext,
                    jitSource->_query_string);
            } else {
                MOT_LOG_TRACE("Duplicate attempt to set JIT source %p as expired (query string: %s)",
                    jitSource,
                    jitSource->_query_string);
            }
        } else if (status == JIT_CONTEXT_EXPIRED) {
            MOT_LOG_TRACE("Setting JIT source %p as expired (query string: %s)", jitSource, jitSource->_query_string);
        } else {
            MOT_LOG_TRACE("Installing JIT source %p with context %p (query string: %s)",
                jitSource,
                readySourceJitContext,
                jitSource->_query_string);
        }
        // cleanup old context (avoid resource leak)
        if (jitSource->_source_jit_context != nullptr) {
            DestroyJitContext(jitSource->_source_jit_context);
        }
        jitSource->_source_jit_context = readySourceJitContext;
        jitSource->_status = status;
        if (jitSource->_source_jit_context != nullptr) {
            // copy query string from source to context
            jitSource->_source_jit_context->m_queryString = jitSource->_query_string;
            jitSource->m_contextList = nullptr;
        } else {  // expired context: cleanup all related JIT context objects
            JitSourcePurgeContextList(jitSource, relationId);
        }
    }
    pthread_mutex_unlock(&jitSource->_lock);

    // in any case we notify change (even error or expired status)
    MOT_LOG_TRACE("%p: Notifying JIT context %p is ready (query: %s)",
        jitSource,
        jitSource->_source_jit_context,
        jitSource->_query_string);

    if (notifyAll) {
        pthread_cond_broadcast(&jitSource->_cond);
    } else {
        pthread_cond_signal(&jitSource->_cond);
    }
}

static const char* JitContextStatusToString(JitContextStatus status)
{
    switch (status) {
        case JIT_CONTEXT_READY:
            return "Ready";
        case JIT_CONTEXT_UNAVAILABLE:
            return "Unavailable";
        case JIT_CONTEXT_ERROR:
            return "Error";
        case JIT_CONTEXT_EXPIRED:
            return "Expired";
        default:
            return "N/A";
    }
}

extern void AddJitSourceContext(JitSource* jitSource, JitContext* cleanupContext)
{
    pthread_mutex_lock(&jitSource->_lock);
    cleanupContext->m_nextInSource = jitSource->m_contextList;
    jitSource->m_contextList = cleanupContext;
    pthread_mutex_unlock(&jitSource->_lock);
}

extern void RemoveJitSourceContext(JitSource* jitSource, JitContext* cleanupContext)
{
    pthread_mutex_lock(&jitSource->_lock);
    RemoveJitSourceContextImpl(jitSource, cleanupContext);
    pthread_mutex_unlock(&jitSource->_lock);
}

extern bool JitSourceRefersRelation(JitSource* jitSource, uint64_t relationId)
{
    bool result = false;
    JitContext* srcContext = jitSource->_source_jit_context;
    if (srcContext != nullptr) {  // context not expired
        if ((srcContext->m_table != nullptr) && (srcContext->m_table->GetTableExId() == relationId)) {
            result = true;
        } else if ((srcContext->m_innerTable != nullptr) && (srcContext->m_innerTable->GetTableExId() == relationId)) {
            result = true;
        } else if (srcContext->m_subQueryCount > 0) {
            for (uint32_t i = 0; i < srcContext->m_subQueryCount; ++i) {
                if ((srcContext->m_subQueryData[i].m_table != nullptr) &&
                    (srcContext->m_subQueryData[i].m_table->GetTableExId() == relationId)) {
                    result = true;
                    break;
                }
            }
        }
    }

    return result;
}

extern void PurgeJitSource(JitSource* jitSource, uint64_t relationId)
{
    pthread_mutex_lock(&jitSource->_lock);
    PurgeJitContext(jitSource->_source_jit_context, relationId);
    JitSourcePurgeContextList(jitSource, relationId);
    pthread_mutex_unlock(&jitSource->_lock);
}

static void JitSourcePurgeContextList(JitSource* jitSource, uint64_t relationId)
{
    MOT_LOG_TRACE("Purging all JIT context objects by relation id %" PRIu64, relationId);
    JitContext* itr = jitSource->m_contextList;
    while (itr != nullptr) {
        PurgeJitContext(itr, relationId);
        itr = itr->m_nextInSource;
    }
}

static void RemoveJitSourceContextImpl(JitSource* jitSource, JitContext* jitContext)
{
    MOT_LOG_TRACE("Removing JIT context %p from source %p", jitContext, jitSource);
    JitContext* prev = nullptr;
    JitContext* curr = jitSource->m_contextList;
    while ((curr != nullptr) && (jitContext != curr)) {
        prev = curr;
        curr = curr->m_nextInSource;
    }
    if (curr != nullptr) {
        MOT_LOG_TRACE("JIT context %p found in source %p, now removing", jitContext, jitSource);
        if (prev != nullptr) {
            prev->m_nextInSource = curr->m_nextInSource;
        } else {  // curr is head
            MOT_ASSERT(curr == jitSource->m_contextList);
            jitSource->m_contextList = curr->m_nextInSource;
        }
    } else {
        MOT_LOG_WARN("Cannot remove JIT context %p from source %p: context not found", jitContext, jitSource);
    }
}

#ifdef MOT_DEBUG
static void VerifyJitSourceStateTrans(JitSource* jitSource, JitContext* readySourceJitContext, JitContextStatus status)
{
    // first verify JIT source state
    MOT_ASSERT(jitSource != nullptr);
    if (jitSource->_status == JIT_CONTEXT_READY) {
        MOT_ASSERT(jitSource->_source_jit_context != nullptr);
    } else {
        MOT_ASSERT(jitSource->_source_jit_context == nullptr);
    }

    // next verify target state
    if (status == JIT_CONTEXT_READY) {
        MOT_ASSERT(readySourceJitContext != nullptr);
    } else {
        MOT_ASSERT(readySourceJitContext == nullptr);
    }

    // now verify state transition
    if (jitSource->_status == JIT_CONTEXT_UNAVAILABLE) {
        MOT_ASSERT((status == JIT_CONTEXT_READY) || (status == JIT_CONTEXT_ERROR));
    } else {
        MOT_ASSERT(status == JIT_CONTEXT_EXPIRED);
    }
}
#endif
}  // namespace JitExec
