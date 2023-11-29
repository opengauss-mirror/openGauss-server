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
 * gs_env_r.cpp
 *
 * IDENTIFICATION
 *    src/common/port/gs_env_r.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdlib.h>
#include "utils/syscall_lock.h"

#ifdef WIN32
extern void pgwin32_unsetenv(const char*);

#define unsetenv(x) pgwin32_unsetenv(x)

int gs_putenv_r(char* envvar);
char* gs_getenv_r(const char* name);
int gs_setenv_r(const char* name, const char* envvar, int overwrite);
int gs_unsetenv_r(const char* name);
#endif

int gs_putenv_r(char* envvar)
{
    int ret;
    (void)syscalllockAcquire(&env_lock);
    ret = putenv(envvar);
    (void)syscalllockRelease(&env_lock);
    return ret;
}

char* gs_getenv_r(const char* name)
{
    (void)syscalllockAcquire(&env_lock);
    char* ret = getenv(name);
    (void)syscalllockRelease(&env_lock);
    return ret;
}

int gs_setenv_r(const char* name, const char* envvar, int overwrite)
{
    int ret;
    (void)syscalllockAcquire(&env_lock);
    ret = setenv(name, envvar, overwrite);
    (void)syscalllockRelease(&env_lock);
    return ret;
}

int gs_unsetenv_r(const char* name)
{
    int ret;
    (void)syscalllockAcquire(&env_lock);
#ifdef WIN32
    unsetenv(name);
    ret = 0;
#else
    ret = unsetenv(name);
#endif

    (void)syscalllockRelease(&env_lock);
    return ret;
}
