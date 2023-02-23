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
 * instr_workload.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/instruments/workload/instr_workload.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "instruments/instr_workload.h"
#include "instruments/snapshot.h"
#include "instruments/percentile.h"
#include "pgstat.h"
#include "utils/dynahash.h"
#include "securec.h"
#include "access/xact.h"
#include "funcapi.h"
#include "utils/acl.h"
#include "workload/workload.h"
#include "job/job_scheduler.h"
#include "catalog/pg_resource_pool.h"
#include "catalog/pg_authid.h"
#include "utils/snapmgr.h"
#include "postmaster/snapcapturer.h"
#include "postmaster/cfs_shrinker.h"
#include "postmaster/rbcleaner.h"

const int RESOURCE_POOL_HASH_SIZE = 32;

static WorkloadXactInfo* InstrWorkloadInfoGeneral(int* num);

static bool CheckInstrIsAvailable()
{
    if (GetCurrentTransactionStartTimestamp() == 0 || !u_sess->attr.attr_resource.enable_resource_track ||
        !(IS_PGXC_COORDINATOR || IS_SINGLE_NODE) || u_sess->wlm_cxt == NULL ||
        g_instance.stat_cxt.workload_info_hashtbl == NULL ||
        !g_instance.wlm_cxt->stat_manager.infoinit || u_sess->proc_cxt.MyProcPort == NULL ||
        u_sess->proc_cxt.MyProcPort->user_name == NULL) {
        return false;
    }
    return true;
}

static bool IsBackgroundXact()
{
    if (IsJobSchedulerProcess() || t_thrd.role == WLM_WORKER || t_thrd.role == WLM_MONITOR ||
        !OidIsValid(u_sess->wlm_cxt->wlm_params.rpdata.rpoid) || t_thrd.role == AUTOVACUUM_WORKER ||
        t_thrd.role == WLM_ARBITER || IsJobSnapshotProcess() ||
        (IsTxnSnapCapturerProcess() || IsTxnSnapWorkerProcess() || IsRbCleanerProcess() || IsRbWorkerProcess() ||
         IsCfsShrinkerProcess()) ||
        strncmp(u_sess->attr.attr_common.application_name, "gs_clean", strlen("gs_clean")) == 0) {
        return true;
    }
    return false;
}

/*
 * updateMaxValueForAtomicType - using atomic type to store max value,
 * we need update the max value by using atomic method
 */
static void updateMaxValueForAtomicType(int64 new_val, int64* max)
{
    int64 prev;
    do
        prev = *max;
    while (prev < new_val && !gs_compare_and_swap_64(max, prev, new_val));
}

/*
 * updateMinValueForAtomicType - update ming value for atomic type
 */
static void updateMinValueForAtomicType(int64 new_val, int64* mix)
{
    int64 prev;
    do
        prev = *mix;
    while ((prev == 0 || prev > new_val) && !gs_compare_and_swap_64(mix, prev, new_val));
}

static void instr_report_responstime(WorkloadXactInfo* wlmInfo)
{
    TimestampTz xact_stop_timestamp = GetCurrentTimestamp();
    TimestampTz xact_start_timestamp = t_thrd.xact_cxt.xactStartTimestamp;
    TimestampTz duration = xact_stop_timestamp - xact_start_timestamp;
    if (duration <= 0) {
        ereport(LOG,
            (errmsg("duration is invalid xac_start_timestamp %ld, xac_stop_timestamp %ld, duration %ld.",
                xact_start_timestamp,
                xact_stop_timestamp,
                duration)));
        return;
    }
    if (IsBackgroundXact()) {
        updateMinValueForAtomicType(duration, &(wlmInfo->bg_xact_info.responstime.min));
        updateMaxValueForAtomicType(duration, &(wlmInfo->bg_xact_info.responstime.max));
        gs_atomic_add_64(&(wlmInfo->bg_xact_info.responstime.total), duration);
    } else {
        updateMinValueForAtomicType(duration, &(wlmInfo->transaction_info.responstime.min));
        updateMaxValueForAtomicType(duration, &(wlmInfo->transaction_info.responstime.max));
        gs_atomic_add_64(&(wlmInfo->transaction_info.responstime.total), duration);
    }
}

/* ----------
 * pg_check_authid              - Check a user by roleid whether exist
 * ----------
 */
static bool pg_check_authid(Oid authid)
{
    HeapTuple roletup;
    roletup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(authid));
    if (HeapTupleIsValid(roletup)) {
        ReleaseSysCache(roletup);
        return true;
    } else {
        return false;
    }
}

/*
 * Check whether the user exists, and remove user transaction if not exist.
 */
static bool InstrCheckUserExist(Oid userId)
{
    if (!OidIsValid(userId)) {
        return false;
    }

    bool isExist = pg_check_authid(ObjectIdGetDatum(userId));
    if (!isExist) {
        /* remove the user transaction from hash table */
        LWLockRelease(InstrWorkloadLock);
        (void)LWLockAcquire(InstrWorkloadLock, LW_EXCLUSIVE);

        WLMWorkLoadKey key;
        key.user_id = userId;

        hash_search(g_instance.stat_cxt.workload_info_hashtbl, &key, HASH_REMOVE, NULL);

        LWLockRelease(InstrWorkloadLock);
        (void)LWLockAcquire(InstrWorkloadLock, LW_SHARED);
    }

    return isExist;
}

static WorkloadXactInfo* InstrWorkloadInfoGeneral(int* num)
{
    if (!(IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        ereport(LOG, (errcode(ERRCODE_WARNING), (errmsg("Instr workload transaction is not allowed on datanode."))));
        return NULL;
    }

    LWLockAcquire(InstrWorkloadLock, LW_SHARED);
    HASH_SEQ_STATUS hash_seq;
    int i = 0;
    *num = hash_get_num_entries(g_instance.stat_cxt.workload_info_hashtbl);
    WorkloadXactInfo* info = NULL;
    WorkloadXactInfo* infoEle = NULL;
    WorkloadXactInfo* infoArray = (WorkloadXactInfo*)palloc0_noexcept(*num * sizeof(WorkloadXactInfo));
    if (infoArray == NULL) {
        LWLockRelease(InstrWorkloadLock);
        ereport(LOG, (errmsg("out of memory during allocating entry.")));
        return NULL;
    }

    hash_seq_init(&hash_seq, g_instance.stat_cxt.workload_info_hashtbl);
    while ((info = (WorkloadXactInfo*)hash_seq_search(&hash_seq)) != NULL) {
        /* check user exists */
        if (!InstrCheckUserExist(info->user_id)) {
            continue;
        }

        /* only superuser could see all user transaction info */
        if (!superuser() && !isMonitoradmin(GetUserId()) && info->user_id != GetCurrentUserId()) {
            continue;
        }

        infoEle = infoArray + i;
        /* Get user transaction info */
        infoEle->user_id = info->user_id;
        infoEle->transaction_info.commit_counter = info->transaction_info.commit_counter;
        infoEle->transaction_info.rollback_counter = info->transaction_info.rollback_counter;
        infoEle->transaction_info.responstime.max = info->transaction_info.responstime.max;
        infoEle->transaction_info.responstime.min = info->transaction_info.responstime.min;
        if (info->transaction_info.commit_counter + info->transaction_info.rollback_counter == 0) {
            infoEle->transaction_info.responstime.average = 0;
        } else {
            infoEle->transaction_info.responstime.average =
                info->transaction_info.responstime.total /
                (info->transaction_info.commit_counter + info->transaction_info.rollback_counter);
        }
        infoEle->transaction_info.responstime.total = info->transaction_info.responstime.total;

        infoEle->bg_xact_info.commit_counter = info->bg_xact_info.commit_counter;
        infoEle->bg_xact_info.rollback_counter = info->bg_xact_info.rollback_counter;
        infoEle->bg_xact_info.responstime.max = info->bg_xact_info.responstime.max;
        infoEle->bg_xact_info.responstime.min = info->bg_xact_info.responstime.min;
        if (info->bg_xact_info.commit_counter + info->bg_xact_info.rollback_counter == 0) {
            infoEle->bg_xact_info.responstime.average = 0;
        } else {
            infoEle->bg_xact_info.responstime.average =
                info->bg_xact_info.responstime.total /
                (info->bg_xact_info.commit_counter + info->bg_xact_info.rollback_counter);
        }
        infoEle->bg_xact_info.responstime.total = info->bg_xact_info.responstime.total;
        ++i;
    }

    /* actually number entry */
    *num = i;
    LWLockRelease(InstrWorkloadLock);

    return infoArray;
}

void InitInstrOneUserTransaction(Oid userId)
{
    if (!(IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        return;
    }

    bool found = true;
    WLMWorkLoadKey key;
    key.user_id = userId;

    WorkloadXactInfo* wlmInfo =
        (WorkloadXactInfo*)hash_search(g_instance.stat_cxt.workload_info_hashtbl, &key, HASH_ENTER_NULL, &found);
    if (wlmInfo == NULL) {
        ereport(LOG, (errcode(ERRCODE_WARNING), (errmsg("out of memory, could not report workload xact info"))));
        return;
    }

    if (!found) {
        wlmInfo->user_id = key.user_id;

        errno_t rc;
        rc = memset_s(
            &wlmInfo->transaction_info, sizeof(wlmInfo->transaction_info), 0, sizeof(wlmInfo->transaction_info));
        securec_check(rc, "\0", "\0");
    }
}

/*
 * Init all user entry for workload transaction.
 */
static void InitInstrWorkloadTransactionUser(void)
{
    if (!(IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        return;
    }

    ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    ResourceOwner tmpOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "ForWorkloadTransaction",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));

    Relation relation = heap_open(AuthIdRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);
    HeapTuple tup;

    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        Oid roleid = HeapTupleGetOid(tup);
        InitInstrOneUserTransaction(roleid);
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
    tmpOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = NULL;
    ResourceOwnerDelete(tmpOwner);
    t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
}

static void init_instr_workload_hashtbl(void)
{
    if (!(IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        return;
    }

    HASHCTL hash_ctl;
    int rc;
    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");
    hash_ctl.keysize = sizeof(WLMWorkLoadKey);
    hash_ctl.hcxt = g_instance.wlm_cxt->workload_manager_mcxt;
    hash_ctl.entrysize = sizeof(WorkloadXactInfo);
    hash_ctl.alloc = WLMAlloc0NoExcept4Hash; /* use alloc function without exception */
    hash_ctl.hash = oid_hash;
    hash_ctl.dealloc = pfree;

    g_instance.stat_cxt.workload_info_hashtbl = hash_create("workload info hash table",
        WORKLOAD_STAT_HASH_SIZE,
        &hash_ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_ALLOC | HASH_DEALLOC);
}

/*
 * Create instrument workload transaction hashtbl, and add
 * all user entry into workload_info_hashtbl.
 */
void InitInstrWorkloadTransaction(void)
{
    if (!(IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        return;
    }
    LWLockAcquire(InstrWorkloadLock, LW_EXCLUSIVE);
    if (g_instance.stat_cxt.workload_info_hashtbl != NULL) {
        LWLockRelease(InstrWorkloadLock);
        return;
    }

    /* Create hashtbl workload_info_hashtbl */
    init_instr_workload_hashtbl();

    /* Init all user entry for workload transaction */
    InitInstrWorkloadTransactionUser();

    LWLockRelease(InstrWorkloadLock);
}

void instr_report_workload_xact_info(bool isCommit)
{
    if (!CheckInstrIsAvailable()) {
        return;
    }

    Oid userId = GetCurrentUserId();
    bool found = true;
    WLMWorkLoadKey key;
    key.user_id = userId;
    LWLockAcquire(InstrWorkloadLock, LW_SHARED);
    WorkloadXactInfo* wlmInfo =
        (WorkloadXactInfo*)hash_search(g_instance.stat_cxt.workload_info_hashtbl, &key, HASH_FIND, NULL);
    if (wlmInfo == NULL) {
        LWLockRelease(InstrWorkloadLock);
        LWLockAcquire(InstrWorkloadLock, LW_EXCLUSIVE);
        wlmInfo =
            (WorkloadXactInfo*)hash_search(g_instance.stat_cxt.workload_info_hashtbl, &key, HASH_ENTER_NULL, &found);
        if (wlmInfo == NULL) {
            ereport(LOG, (errcode(ERRCODE_WARNING), (errmsg("out of memory, could not report workload xact info"))));
            LWLockRelease(InstrWorkloadLock);
            return;
        }
        if (!found) {
            wlmInfo->user_id = key.user_id;
        }
    }
    LWLockRelease(InstrWorkloadLock);
    if (isCommit) {
        if (IsBackgroundXact()) {
            pg_atomic_fetch_add_u64(&(wlmInfo->bg_xact_info.commit_counter), 1);
        } else {
            pg_atomic_fetch_add_u64(&(wlmInfo->transaction_info.commit_counter), 1);
        }
        instr_report_responstime(wlmInfo);
    } else {
        if (IsBackgroundXact()) {
            pg_atomic_fetch_add_u64(&(wlmInfo->bg_xact_info.rollback_counter), 1);
        } else {
            pg_atomic_fetch_add_u64(&(wlmInfo->transaction_info.rollback_counter), 1);
        }
    }
}

static void create_tuple_entry(TupleDesc tupdesc)
{
    int i = 0;
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "user_oid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "commit_counter", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "rollback_counter", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "resp_min", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "resp_max", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "resp_avg", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "resp_total", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "bg_commit_counter", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "bg_rollback_counter", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "bg_resp_min", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "bg_resp_max", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "bg_resp_avg", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "bg_resp_total", INT8OID, -1, 0);
}

static void set_tuple_value(WorkloadXactInfo* statistics, Datum* values, int attnum, int values_size)
{
    int i = -1;
    if (i + attnum < values_size) {
        values[++i] = Int64GetDatum(statistics->user_id);
        values[++i] = Int64GetDatum(statistics->transaction_info.commit_counter);
        values[++i] = Int64GetDatum(statistics->transaction_info.rollback_counter);
        values[++i] = Int64GetDatum(statistics->transaction_info.responstime.min);
        values[++i] = Int64GetDatum(statistics->transaction_info.responstime.max);
        values[++i] = Int64GetDatum(statistics->transaction_info.responstime.average);
        values[++i] = Int64GetDatum(statistics->transaction_info.responstime.total);
        values[++i] = Int64GetDatum(statistics->bg_xact_info.commit_counter);
        values[++i] = Int64GetDatum(statistics->bg_xact_info.rollback_counter);
        values[++i] = Int64GetDatum(statistics->bg_xact_info.responstime.min);
        values[++i] = Int64GetDatum(statistics->bg_xact_info.responstime.max);
        values[++i] = Int64GetDatum(statistics->bg_xact_info.responstime.average);
        values[++i] = Int64GetDatum(statistics->bg_xact_info.responstime.total);
    }
}

Datum get_instr_workload_info(PG_FUNCTION_ARGS)
{
    const int INSTR_WORKLOAD_ATTRUM = 13;
    FuncCallContext* funcctx = NULL;
    int num = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc = NULL;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        tupdesc = CreateTemplateTupleDesc(INSTR_WORKLOAD_ATTRUM, false);

        create_tuple_entry(tupdesc);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        if (!u_sess->attr.attr_resource.enable_resource_track) {
            ereport(WARNING, (errcode(ERRCODE_WARNING), (errmsg("GUC parameter 'enable_resource_track' is off"))));
            MemoryContextSwitchTo(oldcontext);
            SRF_RETURN_DONE(funcctx);
        }

        if (g_instance.stat_cxt.workload_info_hashtbl == NULL) {
            ereport(WARNING, (errcode(ERRCODE_WARNING), (errmsg("workload_info_hashtbl is uninitialized"))));
            MemoryContextSwitchTo(oldcontext);
            SRF_RETURN_DONE(funcctx);
        }

        funcctx->user_fctx = InstrWorkloadInfoGeneral(&num);
        funcctx->max_calls = num;

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx != NULL && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[INSTR_WORKLOAD_ATTRUM];
        bool nulls[INSTR_WORKLOAD_ATTRUM];
        HeapTuple tuple = NULL;
        Datum result;

        WorkloadXactInfo* statistics = (WorkloadXactInfo*)funcctx->user_fctx + funcctx->call_cntr;

        errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        set_tuple_value(statistics, values, INSTR_WORKLOAD_ATTRUM, sizeof(values));
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    pfree_ext(funcctx->user_fctx);
    funcctx->user_fctx = NULL;

    SRF_RETURN_DONE(funcctx);
}
