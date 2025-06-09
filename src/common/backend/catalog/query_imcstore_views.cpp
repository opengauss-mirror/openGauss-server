/*
* Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * query_imcstore_views.cpp
 *      routines to support IMColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/htap/query_imcstore_views.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "funcapi.h"
#include "utils/hsearch.h"
#ifdef ENABLE_HTAP
#include "access/htap/imcs_hash_table.h"
#endif
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "catalog/query_imcstore_views.h"

#ifdef ENABLE_HTAP
constexpr int IMCSTORE_DELTA_PAGE_SIZE = 8192;

static void FillIMCStoreViewsValues(Datum* imcstoreViewsValues, bool* imcstoreViewsNulls, IMCStoreView *imcstoreView)
{
    imcstoreViewsValues[Anum_imcstore_views_reloid - 1] = ObjectIdGetDatum(imcstoreView->relOid);
    imcstoreViewsValues[Anum_imcstore_views_relname - 1] = NameGetDatum(imcstoreView->relname);
    if (imcstoreView->imcsAttsNum) {
        imcstoreViewsValues[Anum_imcstore_views_imcs_attrs - 1] = PointerGetDatum(imcstoreView->imcsAttsNum);
    } else {
        imcstoreViewsNulls[Anum_imcstore_views_imcs_attrs - 1] = true;
    }
    imcstoreViewsValues[Anum_imcstore_views_imcs_nattrs - 1] = Int16GetDatum(imcstoreView->imcsNatts);
    imcstoreViewsValues[Anum_imcstore_views_imcs_status - 1] = NameGetDatum(imcstoreView->imcsStatus);
    imcstoreViewsValues[Anum_imcstore_views_is_partition - 1] = BoolGetDatum(imcstoreView->isPartition);
    imcstoreViewsValues[Anum_imcstore_views_parent_oid - 1] = ObjectIdGetDatum(imcstoreView->parentOid);
    imcstoreViewsValues[Anum_imcstore_views_cu_size_in_mem - 1] = Int32GetDatum(pg_atomic_read_u64(
        &imcstoreView->cuSizeInMem));
    imcstoreViewsValues[Anum_imcstore_views_cu_num_in_mem - 1] = Int32GetDatum(pg_atomic_read_u64(
        &imcstoreView->cuNumsInMem));
    imcstoreViewsValues[Anum_imcstore_views_cu_size_in_disk - 1] = Int32GetDatum(pg_atomic_read_u64(
        &imcstoreView->cuSizeInDisk));
    imcstoreViewsValues[Anum_imcstore_views_cu_num_in_disk - 1] = Int32GetDatum(pg_atomic_read_u64(
        &imcstoreView->cuNumsInDisk));
    imcstoreViewsValues[Anum_imcstore_views_delta_in_mem - 1] = Int32GetDatum(pg_atomic_read_u64(
        &imcstoreView->deltaInMem));
}

IMCStoreView* SearchAllIMCStoreViews(uint32 *num)
{
    uint32 viewIndex = 0;
    bool found = false;
    Oid* relOid = NULL;
    HASH_SEQ_STATUS hashSeq;
    IMCSDesc *imcsDesc = NULL;
    IMCStoreView* results = (IMCStoreView*)palloc(sizeof(IMCStoreView) * IMCSTORE_HASH_TAB_CAPACITY);

    hash_seq_init(&hashSeq, IMCS_HASH_TABLE->m_imcs_hash);
    while ((relOid = (Oid*)hash_seq_search(&hashSeq)) != NULL) {
        LWLockAcquire(IMCS_HASH_TABLE->m_imcs_lock, LW_SHARED);
        imcsDesc = (IMCSDesc*)hash_search(IMCS_HASH_TABLE->m_imcs_hash, relOid, HASH_FIND, &found);
        if (!found) {
            LWLockRelease(IMCS_HASH_TABLE->m_imcs_lock);
            continue;
        }
        results[viewIndex].relOid = imcsDesc->relOid;
        results[viewIndex].relname = imcsDesc->relname;
        results[viewIndex].imcsAttsNum = int2vectorCopy(imcsDesc->imcsAttsNum);
        results[viewIndex].imcsNatts = imcsDesc->imcsNatts;
        results[viewIndex].imcsStatus = IMCSStatusMap[imcsDesc->imcsStatus];
        results[viewIndex].isPartition = imcsDesc->isPartition;
        results[viewIndex].parentOid = imcsDesc->parentOid;
        results[viewIndex].cuSizeInMem = imcsDesc->cuSizeInMem;
        results[viewIndex].cuNumsInMem = imcsDesc->cuNumsInMem;
        results[viewIndex].cuSizeInDisk = imcsDesc->cuSizeInDisk;
        results[viewIndex].cuNumsInDisk = imcsDesc->cuNumsInDisk;
        results[viewIndex].deltaInMem = 0;
        for (uint32 i = 0; i < imcsDesc->curMaxRowGroupId + 1; i++) {
            if (!imcsDesc->rowGroups || !imcsDesc->rowGroups[i] || !imcsDesc->rowGroups[i]->m_delta) {
                continue;
            }
            results[viewIndex].deltaInMem += list_length(imcsDesc->rowGroups[i]->m_delta->pages) *
                                             IMCSTORE_DELTA_PAGE_SIZE;
        }
        viewIndex++;
        LWLockRelease(IMCS_HASH_TABLE->m_imcs_lock);
    }

    *num = viewIndex;
    return results;
}
#endif

Datum query_imcstore_views(PG_FUNCTION_ARGS)
{
    Datum result;
#ifndef ENABLE_HTAP
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("htap not enabled.")));
#else
    IMCStoreView *imcstoreView = NULL;
    FuncCallContext *funcctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc = NULL;
        MemoryContext oldcontext = NULL;
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        tupdesc = CreateTemplateTupleDesc(Natts_imcstore_views, false, TableAmHeap);

        TupleDescInitEntry(tupdesc, (AttrNumber)Anum_imcstore_views_reloid, "reloid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)Anum_imcstore_views_relname, "relname", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)Anum_imcstore_views_imcs_attrs, "imcs_attrs", INT2VECTOROID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)Anum_imcstore_views_imcs_nattrs, "imcs_nattrs", INT2OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)Anum_imcstore_views_imcs_status, "imcs_status", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)Anum_imcstore_views_is_partition, "is_partition", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)Anum_imcstore_views_parent_oid, "parent_oid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)Anum_imcstore_views_cu_size_in_mem, "cu_size_in_mem", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)Anum_imcstore_views_cu_num_in_mem, "cu_num_in_mem", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)Anum_imcstore_views_cu_size_in_disk, "cu_size_in_disk", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)Anum_imcstore_views_cu_num_in_disk, "cu_num_in_disk", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)Anum_imcstore_views_delta_in_mem, "delta_in_mem", INT8OID, -1, 0);

        funcctx->user_fctx = SearchAllIMCStoreViews(&(funcctx->max_calls));
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        (void)MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    imcstoreView = (IMCStoreView*)funcctx->user_fctx;
    if (funcctx->call_cntr < funcctx->max_calls) {
        HeapTuple tuple = NULL;
        Datum imcstoreViewsValues[Natts_imcstore_views];
        bool imcstoreViewsNulls[Natts_imcstore_views] = {false};

        imcstoreView += funcctx->call_cntr;
        FillIMCStoreViewsValues(imcstoreViewsValues, imcstoreViewsNulls, imcstoreView);
        tuple = heap_form_tuple(funcctx->tuple_desc, imcstoreViewsValues, imcstoreViewsNulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    } else {
        SRF_RETURN_DONE(funcctx);
    }
#endif
}

