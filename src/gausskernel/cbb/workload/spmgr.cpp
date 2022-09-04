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
 * spmgr.cpp
 *
 * The file is used to provide the interfaces of storage space management.
 * It includes the following interfaces:
 * a: The caculation of space based on user ID:
 *    void perm_space_increase(Oid ownerID, uint64 size, DataSpaceType type);
 *    void perm_space_decrease(Oid ownerID, uint64 size, DataSpaceType type);
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/workload/spmgr.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <time.h>
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_authid.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "miscadmin.h"
#include "utils/aset.h"
#include "utils/atomic.h"
#include "utils/memprot.h"
#include "pgxc/pgxc.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/relcache.h"
#include "catalog/pg_class.h"

#include "workload/workload.h"

#include "commands/async.h"
#include "storage/sinval.h"

typedef struct TmptableCacheKey {
    Oid relNode; /* relation */
} TmptableCacheKey;

typedef struct TmpTableCacheEntry {
    TmptableCacheKey key;
    bool isTmptable;
    int128 autoinc;
} TmptableCacheEntry;

inline bool do_not_statistic_space(Oid userId)
{
    return (!IsUnderPostmaster || !OidIsValid(userId) || userId == BOOTSTRAP_SUPERUSERID);
}

inline bool enable_space_limit()
{
    return (ENABLE_WORKLOAD_CONTROL && g_instance.attr.attr_resource.enable_perm_space &&
            u_sess->wlm_cxt->wlm_params.iostate == IOSTATE_WRITE);
}

inline void check_space_reach_limit(int64 limit, int64 cursize, char* type, bool group)
{
    if (!enable_space_limit()) {
        return;
    }

    if (limit > 0 && cursize > limit) {
        if (strcmp(type, "spill") == 0) {
            u_sess->wlm_cxt->spill_limit_error = true;
        }
        ereport(ERROR,
            (errcode(ERRCODE_QUERY_CANCELED),
                errmsg("%s space is out of %s's %s space limit", type, group ? "group" : "user", type)));
    }
}

inline void do_space_atomic_add(Oid userid, int64* globalsize, int64* totalsize, uint64 size, char* type)
{
    int64 origin = *totalsize;
    gs_atomic_add_64(totalsize, size);
    gs_atomic_add_64(globalsize, size);
    ereport(DEBUG2,
        (errmodule(MOD_WLM),
            errmsg("increase owerid = %u, %s space = %lu, %s orign = %ld, result = %ld",
                userid,
                type,
                size,
                type,
                origin,
                *totalsize)));
}

inline bool NeedComputeDnUsedSize()
{
    if (u_sess->debug_query_id != 0 && g_instance.attr.attr_resource.enable_perm_space &&
        u_sess->attr.attr_resource.sqlUseSpaceLimit >= 0 && IS_PGXC_DATANODE) {
        return true;
    } else {
        return false;
    }
}

void perm_space_increase_internal(UserData* userdata, uint64 size, DataSpaceType type)
{
    switch (type) {
        case SP_PERM: {
            check_space_reach_limit(userdata->spacelimit, userdata->global_totalspace, "perm", false);
            if (userdata->parent) {
                check_space_reach_limit(
                    userdata->parent->spacelimit, userdata->parent->global_totalspace, "perm", true);
            }
            do_space_atomic_add(userdata->userid, &userdata->global_totalspace, &userdata->totalspace, size, "perm");
            userdata->spaceUpdate = true;
            break;
        }
        case SP_TEMP: {
            check_space_reach_limit(userdata->tmpSpaceLimit, userdata->globalTmpSpace, "temp", false);
            if (userdata->parent) {
                check_space_reach_limit(
                    userdata->parent->tmpSpaceLimit, userdata->parent->globalTmpSpace, "temp", true);
            }
            do_space_atomic_add(userdata->userid, &userdata->globalTmpSpace, &userdata->tmpSpace, size, "temp");
            userdata->spaceUpdate = true;
            break;
        }
        case SP_SPILL: {
            if (u_sess->wlm_cxt->spill_limit_error) {
                return;
            }
            check_space_reach_limit(userdata->spillSpaceLimit, userdata->globalSpillSpace, "spill", false);
            if (userdata->parent) {
                check_space_reach_limit(
                    userdata->parent->spillSpaceLimit, userdata->parent->globalSpillSpace, "spill", true);
            }
            do_space_atomic_add(userdata->userid, &userdata->globalSpillSpace, &userdata->spillSpace, size, "spill");
            break;
        }
    }
}

/*
 * @Description: used to update the user space when data increasing
 *    The function is used when loading data or copying data
 * @IN rel: the table relation
 * @IN size: the increasing size
 * @Return: void
 * @See also:
 */
void perm_space_increase(Oid ownerID, uint64 size, DataSpaceType type)
{
    UserData* userdata = u_sess->wlm_cxt->spmgr_userdata;

    if (do_not_statistic_space(ownerID)) {
        return;
    }

    /* retrieve the user information from hash table */
    if (userdata == NULL || userdata->userid != ownerID) {
        USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);
        /* retrieve the user information based on ownerid from hash table */
        userdata = GetUserDataFromHTab(ownerID, true);
    }

    if (userdata != NULL) {
        perm_space_increase_internal(userdata, size, type);
    }

    /* we need to update the according dn used space in the hash table according to the debug query id */
    if (NeedComputeDnUsedSize()) {
        uint64 space = size >> BITS_IN_KB;
        bool found = false;
        DnUsedSpaceHashEntry* dnUsedEntry = NULL;
        (void)LWLockAcquire(DnUsedSpaceHashLock, LW_EXCLUSIVE);

        dnUsedEntry = (DnUsedSpaceHashEntry*)hash_search(
            g_instance.comm_cxt.usedDnSpace, &u_sess->debug_query_id, HASH_ENTER, &found);

        if (!found) {
            dnUsedEntry->dnUsedSpace = 0;
        }

        if (dnUsedEntry->dnUsedSpace + space > (uint64)u_sess->attr.attr_resource.sqlUseSpaceLimit) {
            LWLockRelease(DnUsedSpaceHashLock);
            ereport(ERROR,
                (errcode(ERRCODE_QUERY_CANCELED),
                    errmsg("the space used on DN has exceeded the sql use space limit (%d kB).",
                        u_sess->attr.attr_resource.sqlUseSpaceLimit)));
        } else {
            dnUsedEntry->dnUsedSpace += space;
            LWLockRelease(DnUsedSpaceHashLock);
        }
    }

    return;
}

int128* find_tmptable_cache_autoinc(Oid relNode)
{
    TmptableCacheKey key = {0};
    TmptableCacheEntry* tmptable_entry = NULL;
    MemoryContext old_mem = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));
    if (u_sess->wlm_cxt->TmptableCacheHash == NULL) {
        return NULL;
    }

    key.relNode = relNode;

    /* Look for an existing entry */
    tmptable_entry = (TmptableCacheEntry*)hash_search(u_sess->wlm_cxt->TmptableCacheHash, (void*)&key, HASH_FIND, NULL);
    (void)MemoryContextSwitchTo(old_mem);
    if (tmptable_entry == NULL) {
        return NULL;
    }

    return &tmptable_entry->autoinc;
}

int128 tmptable_autoinc_nextval(Oid relnode, int128 *autoinc_next)
{
    int128 autoinc;
    if (autoinc_next == NULL) {
        autoinc_next = find_tmptable_cache_autoinc(relnode);
        AssertEreport(autoinc_next != NULL, MOD_OPT, "failed to get temp table auto_increment counter");
    }
    autoinc = *autoinc_next;
    if (autoinc + 1 > autoinc) {
        *autoinc_next = autoinc + 1;
    }
    return autoinc;
}

void tmptable_autoinc_setval(Oid relnode, int128 *autoinc_next, int128 value, bool iscalled)
{
    if (autoinc_next == NULL) {
        autoinc_next = find_tmptable_cache_autoinc(relnode);
        AssertEreport(autoinc_next != NULL, MOD_OPT, "failed to get temp table auto_increment counter");
    }
    if (iscalled) {
        value = (value < INT128_MAX) ? value + 1 : value;
    }
    if (*autoinc_next < value) {
        *autoinc_next = value;
    }
}

void tmptable_autoinc_reset(Oid relnode, int128 value)
{
    int128* autoinc_next = find_tmptable_cache_autoinc(relnode);
    if (autoinc_next == NULL) { /* Shouldn't happen */
        make_tmptable_cache_key(relnode);
        autoinc_next = find_tmptable_cache_autoinc(relnode);
        Assert(autoinc_next);
    }
    *autoinc_next = value;
}

bool find_tmptable_cache_key(Oid relNode)
{
    TmptableCacheKey key = {0};
    TmptableCacheEntry* tmptable_entry = NULL;
    MemoryContext old_mem = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));
    if (u_sess->wlm_cxt->TmptableCacheHash == NULL) {
        return false;
    }

    key.relNode = relNode;

    /* Look for an existing entry */
    tmptable_entry = (TmptableCacheEntry*)hash_search(u_sess->wlm_cxt->TmptableCacheHash, (void*)&key, HASH_FIND, NULL);
    (void)MemoryContextSwitchTo(old_mem);
    if (tmptable_entry == NULL) {
        return false;
    }

    return tmptable_entry->isTmptable;
}

void make_tmptable_cache_key(Oid relNode)
{
    TmptableCacheKey key = {0};
    TmptableCacheEntry* tmptable_entry = NULL;
    MemoryContext old_mem = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
    if (u_sess->wlm_cxt->TmptableCacheHash == NULL) {
        /* First time through: initialize the hash table */
        HASHCTL ctl;
        errno_t rc;

        rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "\0", "\0");
        ctl.keysize = sizeof(TmptableCacheKey);
        ctl.entrysize = sizeof(TmptableCacheEntry);
        ctl.hash = tag_hash;
        ctl.hcxt = u_sess->cache_mem_cxt;

        u_sess->wlm_cxt->TmptableCacheHash = hash_create("Tmptable lookup cache", 256, &ctl,
            HASH_CONTEXT | HASH_ELEM | HASH_FUNCTION);
    }
    key.relNode = relNode;

    tmptable_entry =
        (TmptableCacheEntry*)hash_search(u_sess->wlm_cxt->TmptableCacheHash, (void*)&key, HASH_ENTER, NULL);
    tmptable_entry->isTmptable = true;
    tmptable_entry->autoinc = 1;
    (void)MemoryContextSwitchTo(old_mem);
}

inline void update_user_space(UserData* userdata, uint64 size, DataSpaceType type)
{
    /* update user space data in the hash table */
    int64 origin = 0;
    int64* target = NULL;
    int64 result = 0;
    switch (type) {
        case SP_PERM: {
            origin = userdata->totalspace;
            target = &userdata->totalspace;
            break;
        }
        case SP_TEMP: {
            origin = userdata->tmpSpace;
            target = &userdata->tmpSpace;
            break;
        }
        case SP_SPILL: {
            origin = userdata->spillSpace;
            target = &userdata->spillSpace;
            break;
        }
    }
    result = (uint64)origin >= size ? origin - size : 0;
    while (true) {
        if (gs_compare_and_swap_64(target, origin, result)) {
            break;
        }
        origin = *target;
    }

    userdata->spaceUpdate = (type != SP_SPILL ? true : false);

    if (type == SP_PERM) {
        ereport(DEBUG2,
            (errmodule(MOD_WLM),
                errmsg("decrease owerid = %u, perm space = %lu, perm orign = %ld, result = %ld",
                    userdata->userid,
                    size,
                    origin,
                    userdata->totalspace)));
    } else if (type == SP_TEMP) {
        ereport(DEBUG2,
            (errmodule(MOD_WLM),
                errmsg("decrease owerid = %u, temp space = %lu, temp orign = %ld, result = %ld",
                    userdata->userid,
                    size,
                    origin,
                    userdata->tmpSpace)));
    } else {
        ereport(DEBUG2,
            (errmodule(MOD_WLM),
                errmsg("decrease owerid = %u, spill space = %lu, spill orign = %ld, result = %ld",
                    userdata->userid,
                    size,
                    origin,
                    userdata->spillSpace)));
    }
}

/*
 * @Description: used to update the user space when data decreasing
 *    The function is used when dropping or deleting table
 * @IN ownerid:user id
 * @IN ownerid:size to decrease for user
 * @Return: void
 */
void perm_space_decrease(Oid ownerID, uint64 size, DataSpaceType type)
{
    /* we must use workload manager to update user space */
    if (do_not_statistic_space(ownerID) || size == 0) {
        return;
    }

    UserData* userdata = u_sess->wlm_cxt->spmgr_userdata;
    /* retrieve the user information from hash table */
    if (userdata == NULL || userdata->userid != ownerID) {
        USE_AUTO_LWLOCK(WorkloadUserInfoLock, LW_SHARED);
        /* retrieve the user information based on ownerid from hash table */
        userdata = GetUserDataFromHTab(ownerID, true);
    }

    if (userdata != NULL) {
        update_user_space(userdata, size, type);
    }

    /* we need to update the according dn used space in the hash table according to the debug query id */
    if (NeedComputeDnUsedSize()) {
        uint64 space = (uint64)size >> BITS_IN_KB;
        bool found = false;
        DnUsedSpaceHashEntry* dnUsedEntry = NULL;

        (void)LWLockAcquire(DnUsedSpaceHashLock, LW_EXCLUSIVE);
        dnUsedEntry = (DnUsedSpaceHashEntry*)hash_search(
            g_instance.comm_cxt.usedDnSpace, &u_sess->debug_query_id, HASH_ENTER, &found);

        if (found) {
            if (dnUsedEntry->dnUsedSpace > space) {
                dnUsedEntry->dnUsedSpace -= space;
            } else {
                dnUsedEntry->dnUsedSpace = 0;
            }
        }
        LWLockRelease(DnUsedSpaceHashLock);
    }

    return;
}

/*
 * @Description: reset space parameters
 * @IN void
 * @Return: void
 * @See also:
 */
void perm_space_value_reset(void)
{
    u_sess->wlm_cxt->spmgr_space_bytes = 0;
    u_sess->wlm_cxt->spmgr_userdata = NULL;
    u_sess->wlm_cxt->spill_limit_error = false;

    if (IS_PGXC_DATANODE) {
        WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

        /* update simple query count */
        if (g_wlm_params->complicate) {
            int comp_count = 0;
            int result = 0;

            do {
                comp_count = g_instance.wlm_cxt->stat_manager.comp_count;
                result = (comp_count >= 1) ? comp_count - 1 : 0;
            } while (!gs_compare_and_swap_32(&g_instance.wlm_cxt->stat_manager.comp_count, comp_count, result));

            g_wlm_params->complicate = 0;

            if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry) {
                if (u_sess->wlm_cxt->local_foreign_respool == NULL) {
                    WLMAutoLWLock user_lock(WorkloadUserInfoLock, LW_SHARED);

                    if (!LWLockHeldByMe(WorkloadUserInfoLock)) {
                        user_lock.AutoLWLockAcquire();
                    }

                    if (g_wlm_params->userdata) {
                        UserData* udata = (UserData*)g_wlm_params->userdata;

                        USE_CONTEXT_LOCK(&udata->mutex);

                        udata->entry_list =
                            list_delete_ptr(udata->entry_list, t_thrd.shemem_ptr_cxt.mySessionMemoryEntry);

                        ereport(DEBUG1,
                            (errmsg("release memory entry, uid: %u, length: %d",
                                udata->userid,
                                list_length(udata->entry_list))));
                    }

                    user_lock.AutoLWLockRelease();
                } else {
                    USE_CONTEXT_LOCK(&u_sess->wlm_cxt->local_foreign_respool->mutex);

                    u_sess->wlm_cxt->local_foreign_respool->entry_list = list_delete_ptr(
                        u_sess->wlm_cxt->local_foreign_respool->entry_list, t_thrd.shemem_ptr_cxt.mySessionMemoryEntry);
                }
            }
        }
    }

    /* reset io state for backend status for the thread */
    pgstat_set_io_state(IOSTATE_NONE);
}

/*
 * @Description: check space limitation
 * @IN void
 * @Return: void
 * @See also:
 */
void WLMCheckSpaceLimit(void)
{
    int save_errno = errno;

    /*
     * Don't joggle the elbow of proc_exit
     */
    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        InterruptPending = true;
        t_thrd.int_cxt.QueryCancelPending = true;

        u_sess->wlm_cxt->cancel_from_space_limit = true;

        /*
         * in libcomm interrupt is not allow,
         * gs_r_cancel will signal libcomm and
         * libcomm will then check for interrupt.
         */
        gs_r_cancel();

        /*
         * If it's safe to interrupt, and we're waiting for input or a lock,
         * service the interrupt immediately
         */
        if (t_thrd.int_cxt.ImmediateInterruptOK && t_thrd.int_cxt.InterruptHoldoffCount == 0 &&
            t_thrd.int_cxt.CritSectionCount == 0) {
            /* bump holdoff count to make ProcessInterrupts() a no-op */
            /* until we are done getting ready for it */
            t_thrd.int_cxt.InterruptHoldoffCount++;
            LockErrorCleanup(); /* prevent CheckDeadLock from running */
            t_thrd.int_cxt.InterruptHoldoffCount--;
            ProcessInterrupts();
        }
    }

    /* If we're still here, waken anything waiting on the process latch */
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = save_errno;
}

