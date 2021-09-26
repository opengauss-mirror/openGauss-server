/* -------------------------------------------------------------------------
 *
 * pthread-win32.cpp
 *	 partial pthread implementation for win32
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2004-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/common/interfaces/libpq/pthread-win32.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#define _WINSOCKAPI_
#include <windows.h>
#if !defined(_MINGW32)
#include "pthread-win32.h"
#else
#include "libpq/libpq-int.h"
#endif

#if defined(WIN32) && !defined(_MINGW32)
DWORD
pthread_self(void)
{
    return GetCurrentThreadId();
}

void pthread_setspecific(pthread_key_t key, void* val)
{}

void* pthread_getspecific(pthread_key_t key)
{
    return NULL;
}

int pthread_mutex_init(pthread_mutex_t* mp, void* attr)
{
    *mp = (CRITICAL_SECTION*)malloc(sizeof(CRITICAL_SECTION));
    if (!*mp)
        return 1;
    InitializeCriticalSection(*mp);
    return 0;
}

int pthread_mutex_lock(pthread_mutex_t* mp)
{
    if (!*mp)
        return 1;
    EnterCriticalSection(*mp);
    return 0;
}

int pthread_mutex_unlock(pthread_mutex_t* mp)
{
    if (!*mp)
        return 1;
    LeaveCriticalSection(*mp);
    return 0;
}
#endif
