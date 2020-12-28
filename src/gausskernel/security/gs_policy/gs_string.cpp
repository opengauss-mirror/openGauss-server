/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * pg_common.cpp
 * Containers common parts and string implementation
 *
 *
 * IDENTIFICATION
 * src/gausskernel/security/gs_policy/gs_string.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "gs_policy/gs_string.h"
#include "access/xact.h"
#include <strings.h>
#include "knl/knl_thread.h"
namespace gs_stl {
#define MIN_STR_CAPACITY 16

MemoryContext GetStringMemory()
{
    if (t_thrd.security_policy_cxt.StringMemoryContext == NULL) {
        t_thrd.security_policy_cxt.StringMemoryContext =
            AllocSetContextCreate(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY), "StringMemory",
                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    }
    return t_thrd.security_policy_cxt.StringMemoryContext;
}

void DeleteStringMemory()
{
    if (t_thrd.security_policy_cxt.StringMemoryContext != NULL) {
        MemoryContextDelete(t_thrd.security_policy_cxt.StringMemoryContext);
        t_thrd.security_policy_cxt.StringMemoryContext = NULL;
    }
}

MemoryContext GetVectorMemory()
{
    if (t_thrd.security_policy_cxt.VectorMemoryContext == NULL) {
        t_thrd.security_policy_cxt.VectorMemoryContext =
            AllocSetContextCreate(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY), "VectorMemory",
                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    }
    return t_thrd.security_policy_cxt.VectorMemoryContext;
}

void DeleteVectorMemory()
{
    if (t_thrd.security_policy_cxt.VectorMemoryContext != NULL) {
        MemoryContextDelete(t_thrd.security_policy_cxt.VectorMemoryContext);
        t_thrd.security_policy_cxt.VectorMemoryContext = NULL;
    }
}

MemoryContext GetMapMemory()
{
    if (!t_thrd.security_policy_cxt.MapMemoryContext) {
        t_thrd.security_policy_cxt.MapMemoryContext = AllocSetContextCreate(TopMemoryContext, "MapMemory",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    }
    return t_thrd.security_policy_cxt.MapMemoryContext;
}

void DeleteMapMemory()
{
    if (t_thrd.security_policy_cxt.MapMemoryContext) {
        MemoryContextDelete(t_thrd.security_policy_cxt.MapMemoryContext);
        t_thrd.security_policy_cxt.MapMemoryContext = nullptr;
    }
}

void *_HashMapAllocFunc(Size request)
{
    return MemoryContextAlloc(GetMapMemory(), request);
}

MemoryContext GetSetMemory()
{
    if (!t_thrd.security_policy_cxt.SetMemoryContext) {
        t_thrd.security_policy_cxt.SetMemoryContext = AllocSetContextCreate(TopMemoryContext, "SetMemory",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    }
    return t_thrd.security_policy_cxt.SetMemoryContext;
}

void DeleteSetMemory()
{
    if (t_thrd.security_policy_cxt.SetMemoryContext) {
        MemoryContextDelete(t_thrd.security_policy_cxt.SetMemoryContext);
        t_thrd.security_policy_cxt.SetMemoryContext = nullptr;
    }
}

void *_HashSetAllocFunc(Size request)
{
    return MemoryContextAlloc(GetSetMemory(), request);
}

int matchStr(const void *key1, const void *key2, Size keysize)
{
    return strncasecmp((const char *)key1, (const char *)key2, keysize - 1);
}

int gs_stringCompareKeyFunc(const void *keyA, const void *keyB)
{
    if (*(const gs_string *)keyA < *(const gs_string *)keyB) {
        return -1;
    } else if (*(const gs_string *)keyB < *(const gs_string *)keyA) {
        return 1;
    } else {
        return 0;
    }
}

// string implementation
inline bool gs_string::InitBuff(const char *str, size_t len)
{
    if (m_buff == NULL) {
        size_t init_len = (len > 0) ? (len + 1) : (strlen(str) + 1);
        m_capacity = Max(MIN_STR_CAPACITY, init_len);
        m_buff = AllocFunc(m_capacity);
        errno_t ret = snprintf_s(m_buff, m_capacity, init_len - 1, "%.*s", (int)(init_len - 1), str);
        securec_check_ss(ret, "\0", "\0");
        m_len = (size_t)ret;
        return true;
    }
    return false;
}
gs_string::gs_string(const char *str, size_t len) : m_buff(NULL), m_len(0), m_capacity(0)
{
    (void)InitBuff(str, len);
}

gs_string::~gs_string()
{
    /*
     * Note that: container destruction will be called by system depending on
     * runtime result. as dealloc of container use thread-level memory context
     * we must make sure the mem_cxt is valid in any scenario when dealloc memory.
     * there are two scenarios we may concern:
     * thread is existing(longjmp), destruction func is called when out of scope,
     * it's safe here as all the memory resources are released accordingly.
     */
    if ((m_buff != NULL) && (t_thrd.port_cxt.thread_is_exiting == false)) {
        Assert(t_thrd.top_mem_cxt != NULL);
        pfree(m_buff);
        m_buff = NULL;
    }
}

gs_string::gs_string(const gs_string &arg) : m_buff(NULL), m_len(0), m_capacity(0)
{
    operator = (arg);
}

gs_string &gs_string::operator = (const gs_string &arg)
{
    if (&arg == this) {
        return *this;
    }

    /* m_buff should always be free if not NULL as will be taken place with arg */
    if (m_buff != NULL) {
        pfree(m_buff);
        m_buff = NULL;
    }

    size_t len = arg.size();
    if (len > 0) {
        (void)InitBuff(arg.c_str(), arg.size());
    } else {
        (void)InitBuff("", 0);
    }
    return *this;
}

int gs_string::operator - (const gs_string &arg) const
{
    if (this == &arg) {
        return 0;
    }

    if (*this < arg)
        return -1;
    else if (arg < *this)
        return 1;
    else
        return 0;
}

gs_string &gs_string::append(const gs_string &str)
{
    return append(str.c_str(), str.size());
}

gs_string &gs_string::append(const char *str, size_t len)
{
    if (!InitBuff(str)) {
        size_t init_len = (len > 0) ? (len + 1) : (strlen(str) + 1);
        if (init_len > (m_capacity - m_len)) {
            m_buff = ReallocFunc(m_capacity + init_len);
        }
        errno_t ret = snprintf_s(m_buff + m_len, m_capacity - m_len, init_len - 1, "%.*s", (int)(init_len - 1), str);
        securec_check_ss(ret, "\0", "\0");
        m_len += (size_t)ret;
    }
    return *this;
}

void gs_string::push_back(char ch)
{
    char t_chr[2] = {0};
    t_chr[1] = ch;
    if (!InitBuff(t_chr)) {
        if ((m_len + 1) >= m_capacity) {
            m_buff = ReallocFunc(m_capacity * 2);
        }
        m_buff[m_len++] = ch;
        m_buff[m_len] = '\0';
    }
}

void gs_string::pop_back()
{
    if (m_len > 0) {
        m_buff[--m_len] = '\0';
    }
}

char gs_string::operator[](int idx) const
{
    if (idx > (int)m_len) {
        return '\0';
    }
    return m_buff[idx];
}

void gs_string::clear()
{
    if (m_buff != NULL) {
        m_buff[0] = '\0';
        m_len = 0;
    }
}

size_t gs_string::find(char arg, size_t start) const
{
    for (; start < m_len; ++start) {
        if (m_buff[start] == arg) {
            return start;
        }
    }
    return npos;
}

char gs_string::back() const
{
    if (m_len > 0) {
        return m_buff[m_len - 1];
    }
    return m_buff[0];
}

gs_string gs_string::substr(size_t pos, size_t len) const
{
    if ((pos + len) < m_len) {
        return gs_string((const char *)(m_buff + pos), len);
    }
    if (pos < m_len) {
        return gs_string((const char *)(m_buff + pos), m_len - pos);
    }
    return gs_string((const char *)m_buff, m_len);
}

gs_string &gs_string::replace(size_t pos, size_t len, const char *s)
{
    if (pos < m_len) {
        size_t rep_len = strlen(s) + 1;
        size_t replace_len = pos + len;
        size_t jump = (rep_len - len);
        errno_t ret = EOK;
        if ((m_len + jump) >= m_capacity) {
            (void)ReallocFunc(m_capacity + jump);
        }

        if (replace_len < m_len) {
            m_len = pos;
            gs_string tmp(m_buff + replace_len);
            ret = snprintf_s(m_buff + m_len, m_capacity - m_len, rep_len - 1, "%s", s);
            securec_check_ss(ret, "\0", "\0");
            m_len += (size_t)ret;
            ret = snprintf_s(m_buff + m_len, m_capacity - m_len, tmp.size(), "%s", tmp.c_str());
            securec_check_ss(ret, "\0", "\0");
            m_len += (size_t)ret;
        } else {
            m_len = pos;
            ret = snprintf_s(m_buff + m_len, m_capacity - m_len, rep_len - 1, "%s", s);
            securec_check_ss(ret, "\0", "\0");
            m_len += (size_t)ret;
        }
    }
    return *this;
}

void gs_string::erase(size_t pos, size_t len)
{
    if (m_len == 0 || (pos >= m_len)) {
        return;
    }
    if ((pos + len) < m_len) {
        size_t idx = pos + len;
        while (idx < m_len) {
            m_buff[pos++] = m_buff[idx++];
        }
        m_len = pos;
    } else {
        m_len = pos;
    }
    m_buff[m_len] = '\0';
}

bool gs_string::operator == (const gs_string &arg) const
{
    if (m_len != arg.m_len) {
        return false;
    }
    if (m_len > 0) {
        return (strcasecmp(m_buff, arg.m_buff) == 0);
    }
    return m_len == 0;
}

bool gs_string::operator < (const gs_string &arg) const
{
    return strcasecmp(m_buff, arg.m_buff) < 0;
}

inline char *gs_string::AllocFunc(size_t _size) const
{
    return (char *)MemoryContextAlloc(GetStringMemory(), _size);
}

inline char *gs_string::ReallocFunc(size_t _size)
{
    m_capacity = _size;

    char *buff = AllocFunc(m_capacity);
    /* copy old data */
    if (m_buff != NULL) {
        errno_t ret = snprintf_s(buff, m_capacity, strlen(m_buff), "%s", m_buff);
        securec_check_ss(ret, "\0", "\0");
        m_len = (size_t)ret;
        pfree(m_buff);
    }

    m_buff = buff;
    return m_buff;
}
}
