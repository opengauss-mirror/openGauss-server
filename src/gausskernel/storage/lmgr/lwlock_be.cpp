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
void remember_lwlock_acquire(LWLock *lock, LWLockMode mode)
{
    if (t_thrd.shemem_ptr_cxt.MyBEEntry) {
        volatile PgBackendStatus *beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
        ++(beentry->lw_count);
        /*
         * the statement "Assert ( CHANGECOUNT_IS_EVEN(beentry->lw_count) );" does not hold
         * because this function maybe called before pgstat_bestart() function.
         */
        beentry->lw_want_lock = lock;
        beentry->lw_want_mode = mode;
        beentry->lw_want_start_time = 
            (u_sess->attr.attr_common.pgstat_track_activities ? GetCurrentTimestamp() : (TimestampTz)0);
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
         *
         * mode and start time does not need to be reset, because it is only meaningful if 'lock' is not null.
         */
        beentry->lw_want_lock = NULL;
    }
}

/*
 * find lwlock we held and return the index.
 */
int find_lwlock_hold(LWLock *lock)
{
    int i;

    /* Remove lock from list of locks held.  Usually, but not always, it will
     * be the latest-acquired lock; so search array backwards. */
    for (i = t_thrd.storage_cxt.num_held_lwlocks - 1; i >= 0; --i) {
        if (lock == t_thrd.storage_cxt.held_lwlocks[i].lock) {
            break;
        }
    }

    if (unlikely(i < 0)) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("lock %s is not held", T_NAME(lock))));
    }

    /* if lwlock is held longer than 1min, ereport the detail and backtrace */
    TimestampTz now = (u_sess->attr.attr_common.pgstat_track_activities ? GetCurrentTimestamp() : (TimestampTz)0);
    if (u_sess->attr.attr_common.pgstat_track_activities &&
        TimestampDifferenceExceeds(t_thrd.storage_cxt.lwlock_held_times[i], now, MSECS_PER_MIN)) {
        force_backtrace_messages = true;
        int old_backtrace_min_messages = u_sess->attr.attr_common.backtrace_min_messages;

        u_sess->attr.attr_common.backtrace_min_messages = LOG;
        ereport(LOG,
            (errmsg("lwlock %s mode %d is held for %ld us longer than 1 min",
                    T_NAME(lock),
                    (int)(t_thrd.storage_cxt.held_lwlocks[i].mode),
                    now - t_thrd.storage_cxt.lwlock_held_times[i])));

        u_sess->attr.attr_common.backtrace_min_messages = old_backtrace_min_messages;
    }

    return i;
}

/*
 * forget lwlock id when the lwlock is releasing.
 * return the mode of the lwlock we hold.
 */
LWLockMode forget_lwlock_hold(LWLock *lock)
{
    int i = find_lwlock_hold(lock);

    LWLockMode mode = t_thrd.storage_cxt.held_lwlocks[i].mode;

    t_thrd.storage_cxt.num_held_lwlocks--;
    for (; i < t_thrd.storage_cxt.num_held_lwlocks; i++) {
        t_thrd.storage_cxt.held_lwlocks[i] = t_thrd.storage_cxt.held_lwlocks[i + 1];
        t_thrd.storage_cxt.lwlock_held_times[i] = t_thrd.storage_cxt.lwlock_held_times[i + 1];
    }

    return mode;
}
