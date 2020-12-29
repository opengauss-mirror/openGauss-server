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
 * gs_syscall_lock.cpp
 *
 * IDENTIFICATION
 *    src/common/port/gs_syscall_lock.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "utils/syscall_lock.h"
#include "c.h"
/*
 * syscall lock, should not be  TLS
 */

syscalllock getpwuid_lock;
syscalllock env_lock;
syscalllock dlerror_lock;
syscalllock kerberos_conn_lock;
syscalllock read_cipher_lock;

/*
 * @Description: Atomic set val into *ptr in a 32-bit address, and return the previous pointed by ptr
 * @IN ptr: int32 pointer
 * @IN val: value to set
 * @Return: old value
 * @See also:
 */
#ifndef WIN32
int32 gs_syscall_atomic_test_and_set(volatile int32* ptr, int32 val)
{
    int32 oldValue = __sync_lock_test_and_set(ptr, val);

    return oldValue;
}

/*
 * @Description: Atomic increment in a 32-bit address, and return the incremented value.
 * @IN ptr: int32 pointer
 * @IN inc: increase value
 * @Return: new value
 * @See also:
 */
int32 gs_syscall_atomic_add_32(volatile int32* ptr, int32 inc)
{
    volatile int32 newValue = 0;

    int32 oldValue = __sync_fetch_and_add(ptr, inc);
    newValue = oldValue + inc;

    return newValue;
}

/*
 * @Description: Atomic increment in a 64-bit address, and return the incremented value.
 * @IN ptr: int64 pointer
 * @IN inc: increase value
 * @Return: new value
 * @See also:
 */
int64 gs_syscall_atomic_add_64(int64* ptr, int64 inc)
{
    volatile int64 newValue = 0;

    int64 oldValue = __sync_fetch_and_add(ptr, inc);
    newValue = oldValue + inc;

    return newValue;
}
#endif
