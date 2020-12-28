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
 * lwlock_be.cpp
 *        bridge between lwlock.cpp and pgstat.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/lmgr/lwlock_be.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "storage/lock/lwlock_be.h"
#include "pgstat.h"

/*
 * remember lwlock to require before entering to lwlock
 * waiting loop.
 */
void remember_lwlock_acquire(LWLock *lock)
{
    if (t_thrd.shemem_ptr_cxt.MyBEEntry) {
        volatile PgBackendStatus *beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
        ++(beentry->lw_count);
        /*
         * the statement "Assert ( CHANGECOUNT_IS_EVEN(beentry->lw_count) );" does not hold
         * because this function maybe called before pgstat_bestart() function.
         */
        beentry->lw_want_lock = lock;
    }
}

/*
 * forget lwlock id to require after exiting from lwlock
 * waiting loop. that means it's successfull to hold lwlock.
 */
void forget_lwlock_acquire(void)
{
    if (t_thrd.shemem_ptr_cxt.MyBEEntry) {
        volatile PgBackendStatus *beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
        ++(beentry->lw_count);
        /*
         * the statement "Assert ( !CHANGECOUNT_IS_EVEN(beentry->lw_count) );" does not hold
         * because this function may be called before pgstat_bestart() function.
         */
        beentry->lw_want_lock = NULL;
    }
}
