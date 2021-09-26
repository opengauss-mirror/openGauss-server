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
 * ---------------------------------------------------------------------------------------
 * 
 * syscall_lock.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/syscall_lock.h
 *
 * NOTE
 *    some function in lib has not thread safe version , use lock to protect them 
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SYSCALL_LOCK_H_
#define SYSCALL_LOCK_H_

#ifndef WIN32
#include "pthread.h"
#else
#define _WINSOCKAPI_
#include <windows.h>
#ifndef _MINGW32
#include "pthread-win32.h"
#else
#include "libpq/libpq-int.h"
#endif
#endif

typedef pthread_mutex_t syscalllock;

#define syscalllockInit(lock) pthread_mutex_init(lock, NULL)
#define syscalllockAcquire(lock) pthread_mutex_lock(lock)
#define syscalllockRelease(lock) pthread_mutex_unlock(lock)
#define syscalllockFree(lock) pthread_mutex_destroy(lock)

/*
 * the locks declared here should be initialized  in syscall_lock_init function
 */
extern syscalllock getpwuid_lock;
extern syscalllock env_lock;
extern syscalllock dlerror_lock;
extern syscalllock kerberos_conn_lock;
extern syscalllock read_cipher_lock;

#endif /* SYSCALL_LOCK_H_ */
