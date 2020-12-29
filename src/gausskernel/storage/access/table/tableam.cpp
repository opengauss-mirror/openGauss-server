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
 * tableam.cpp
 *      Table access methods
 *
 * IDENTIFICATION
 *      src/gausskernel/storage/access/table/tableam.cpp
 *
 * NOTES
 *      Note that most function in here are documented in tableam.h, rather than
 *      here. That's because there's a lot of inline functions in tableam.h and
 *      it'd be harder to understand if one constantly had to switch between files.
 *
 *----------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/htup.h"
#include "access/xact.h"
#include "executor/executor.h"
#include "commands/cluster.h"
#include "storage/buf/bufmgr.h"
#include "storage/item/itemptr.h"
#include "storage/shmem.h"

#include "catalog/index.h"
         
    
/* ------------------------------------------------------------------------
 * HEAP TABLE SLOT AM APIs
 * ------------------------------------------------------------------------
 */

/*
 * Implementation of heap accessor methods.
 */

Datum heapam_tops_getsysattr(Tuple tup, int attnum, TupleDesc tuple_desc, bool* isnull, Buffer buff) {
    Assert(TUPLE_IS_HEAP_TUPLE(HeapTuple(tup)));
    return heap_getsysattr((HeapTuple)tup, attnum, tuple_desc, isnull);
}

/*
 * Clears the contents of the table slot that contains heap table tuple data.
 */
void heapam_tslot_clear(TupleTableSlot *slot)
{
    return heap_slot_clear(slot);
}

HeapTuple heapam_tslot_materialize(TupleTableSlot *slot)
{
    heap_slot_materialize(slot);
    return (HeapTuple)slot->tts_tuple;
}

MinimalTuple heapam_tslot_get_minimal_tuple(TupleTableSlot *slot)
{
    return heap_slot_get_minimal_tuple(slot);
}

MinimalTuple heapam_tslot_copy_minimal_tuple(TupleTableSlot *slot)
{
    return heap_slot_copy_minimal_tuple(slot);
}

void heapam_tslot_store_minimal_tuple(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree)
{
    heap_slot_store_minimal_tuple(mtup, slot, shouldFree);
}

HeapTuple heapam_tslot_get_heap_tuple(TupleTableSlot* slot)
{
    return heap_slot_get_heap_tuple(slot);
}

HeapTuple heapam_tslot_copy_heap_tuple(TupleTableSlot *slot)
{
    return heap_slot_copy_heap_tuple(slot);
}

void heapam_tslot_store_heap_tuple(Tuple tuple, TupleTableSlot* slot, Buffer buffer, bool should_free)
{
    heap_slot_store_heap_tuple((HeapTuple)tuple, slot, buffer, should_free);
}

void heapam_tslot_getsomeattrs(TupleTableSlot *slot, int natts)
{
    heap_slot_getsomeattrs(slot, natts);
}

Datum heapam_tslot_getattr(TupleTableSlot* slot, int attnum, bool* isnull)
{
    return heap_slot_getattr(slot, attnum, isnull);
}

void heapam_tslot_getallattrs(TupleTableSlot* slot)
{
    heap_slot_getallattrs(slot);
}

bool heapam_tslot_attisnull(TupleTableSlot* slot, int attnum)
{
    return heap_slot_attisnull(slot, attnum);
}

/* ------------------------------------------------------------------------
 * TABLE TUPLE AM APIs
 * ------------------------------------------------------------------------
 */

MinimalTuple heapam_tops_form_minimal_tuple(
    TupleDesc tuple_descriptor, Datum* values, const bool* isnull, MinimalTuple in_tuple)
{
    return heap_form_minimal_tuple(tuple_descriptor, values, isnull, in_tuple);
}

Tuple heapam_tops_form_tuple(TupleDesc tuple_descriptor, Datum* values, bool* isnull)
{
    return (Tuple)heap_form_tuple(tuple_descriptor, values, isnull);
}

HeapTuple heapam_tops_form_cmprs_tuple(TupleDesc tuple_descriptor, FormCmprTupleData* cmprs_info)
{
    return heap_form_cmprs_tuple(tuple_descriptor, cmprs_info);
}

void heapam_tops_deform_tuple(Tuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull)
{
    Assert(TUPLE_IS_HEAP_TUPLE(HeapTuple(tuple)));
    return heap_deform_tuple((HeapTuple)tuple, tuple_desc, values, isnull);
}

void heapam_tops_deform_cmprs_tuple(Tuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull, char* cmprs_info)
{
    Assert(TUPLE_IS_HEAP_TUPLE(HeapTuple(tuple)));
    heap_deform_cmprs_tuple((HeapTuple)tuple, tuple_desc, values, isnull, cmprs_info);
}

Size heapam_tops_computedatasize_tuple(TupleDesc tuple_desc, Datum* values, const bool* isnull, uint32 hoff)
{
    return heap_compute_data_size(tuple_desc, values, isnull);
}

void heapam_tops_fill_tuple(TupleDesc tuple_desc, Datum* values, const bool* isnull, char* data, Size data_size, uint16* infomask, bits8* bit)
{
    heap_fill_tuple(tuple_desc, values, isnull, data, data_size, infomask, bit);
}

Tuple heapam_tops_modify_tuple(Tuple tuple, TupleDesc tuple_desc, Datum* repl_values, const bool* repl_isnull, const bool* do_replace)
{
    return (Tuple)heap_modify_tuple((HeapTuple)tuple, tuple_desc, repl_values, repl_isnull, do_replace);
}

Datum heapam_tops_tuple_getattr(Tuple tuple, int att_num, TupleDesc tuple_desc, bool* is_null)
{
    return heap_getattr((HeapTuple)tuple, att_num, tuple_desc, is_null);
}

bool heapam_tops_tuple_attisnull(Tuple tup, int attnum, TupleDesc tuple_desc)
{
    return heap_attisnull((HeapTuple)tup, attnum, tuple_desc);
}

Tuple heapam_tops_copy_tuple(Tuple tuple)
{
    Assert(TUPLE_IS_HEAP_TUPLE(HeapTuple(tuple)));
    return heap_copytuple((HeapTuple)tuple);
}


/* ------------------------------------------------------------------------
 * DQL AM APIs
 * ------------------------------------------------------------------------
 */
bool heapam_tuple_fetch(Relation relation, Snapshot snapshot, 
                                                  HeapTuple tuple, Buffer *userbuf, bool keep_buf, Relation stats_relation) {
    return heap_fetch(relation, snapshot, tuple, userbuf, keep_buf, stats_relation);
}

bool heapam_tuple_satisfies_snapshot(Relation relation, HeapTuple tuple, 
                                                             Snapshot snapshot, Buffer buffer) {
    return HeapTupleSatisfiesVisibility(tuple, snapshot, buffer);
}

void heapam_tuple_get_latest_tid(Relation relation, Snapshot snapshot, 
                                                        ItemPointer tid) {
    return heap_get_latest_tid(relation, snapshot, tid);
}

/* ------------------------------------------------------------------------
 * DML AM APIs
 * ------------------------------------------------------------------------
 */

Oid heapam_tuple_insert (Relation relation, Tuple tup, CommandId cid, 
                           int options, struct BulkInsertStateData *bistate) {
    return heap_insert(relation, (HeapTuple)tup, cid, options, bistate);
}

int heapam_tuple_multi_insert(Relation relation, Relation parent, 
                                  Tuple* tuples, int ntuples, CommandId cid, 
                                  int options, struct BulkInsertStateData *bistate, 
                                  HeapMultiInsertExtraArgs *args) {
    return heap_multi_insert(relation, parent, (HeapTuple*)tuples, ntuples, cid, options, bistate, args);
}

TM_Result heapam_tuple_delete(Relation relation, ItemPointer tid,
                                    CommandId cid, Snapshot crosscheck, Snapshot snapshot,
                                    bool wait, TM_FailureData *tmfd,
                                    bool allow_delete_self) {

    return heap_delete(relation, tid, cid, crosscheck, wait, tmfd, allow_delete_self);
}

/* 
 * Since both heapam and uheapam must have the same params, the params
 * here are the union of both versions.  Therefore, some parms are not used.
 */
/* -------------------------------------------------------------------------- */
TM_Result heapam_tuple_update(Relation relation, Relation parentRelation,
                             ItemPointer otid, Tuple newtup, CommandId cid,
                             Snapshot crosscheck, Snapshot snapshot, bool wait,
                             TM_FailureData *tmfd, bool *update_indexes, bool allow_update_self) {
    TM_Result result = heap_update(relation, parentRelation, otid, (HeapTuple)newtup,
        cid, crosscheck, wait, tmfd, allow_update_self);

    /* make update_indexes optional */
    if(update_indexes) {
        *update_indexes = (result == TM_Ok && !HeapTupleIsHeapOnly((HeapTuple)newtup));
    }

    return result;
}

TM_Result heapam_tuple_lock(Relation relation, Tuple tuple, Buffer *buffer, 
                            CommandId cid, LockTupleMode mode, bool nowait, TM_FailureData *tmfd,
                            bool allow_lock_self, bool follow_updates, bool eval, Snapshot snapshot, 
                            ItemPointer tid, bool isSelectForUpdate)
{
    return heap_lock_tuple(relation, (HeapTuple)tuple, buffer, cid, mode, nowait, tmfd, allow_lock_self);
}

HeapTuple heapam_tuple_lock_updated(CommandId cid, Relation relation, 
                           int lockmode, ItemPointer tid, TransactionId priorXmax) {
    return heap_lock_updated(cid, relation, lockmode, tid, priorXmax);
}


/* -----------------------------------------------------------------------
 * SCAN AM APIS FOR HEAP
 * ------------------------------------------------------------------------
 */

IndexFetchTableData * heapam_scan_index_fetch_begin(Relation rel)
{
    return heapam_index_fetch_begin(rel);
}

void heapam_scan_index_fetch_reset(IndexFetchTableData *scan)
{
    return heapam_index_fetch_reset(scan);
}

void heapam_scan_index_fetch_end(IndexFetchTableData *scan)
{
    return heapam_index_fetch_end(scan);
}

HeapTuple heapam_scan_index_fetch_tuple(IndexScanDesc scan, bool *all_dead)
{
    return heapam_index_fetch_tuple(scan, all_dead);
}

TableScanDesc heapam_scan_begin(Relation relation, Snapshot snapshot, int nkeys, ScanKey key, RangeScanInRedis rangeScanInRedis)
{
    return heap_beginscan(relation, snapshot, nkeys, key, rangeScanInRedis);
}
    
TableScanDesc heapam_scan_begin_bm(Relation relation, Snapshot snapshot, int nkeys, ScanKey key)
{
    return heap_beginscan_bm(relation, snapshot, nkeys, key);
}

TableScanDesc heapam_scan_begin_sampling(Relation relation, Snapshot snapshot, int nkeys, ScanKey key, bool allow_strat, bool allow_sync, RangeScanInRedis rangeScanInRedis)
{
    return heap_beginscan_sampling(relation, snapshot, nkeys, key, allow_strat, allow_sync, rangeScanInRedis);
}

Tuple heapam_scan_getnexttuple(TableScanDesc sscan, ScanDirection direction)
{
    return (Tuple) heap_getnext(sscan, direction);
}

void heapam_scan_getpage(TableScanDesc sscan, BlockNumber page)
{
    return heapgetpage(sscan, page);
}

void heapam_scan_end(TableScanDesc sscan)
{
    return heap_endscan(sscan);
}
    
void heapam_scan_rescan(TableScanDesc sscan, ScanKey key)
{
     return heap_rescan(sscan, key);
}

void heapam_scan_restrpos(TableScanDesc sscan)
{
    return heap_restrpos(sscan);
}

void heapam_scan_markpos(TableScanDesc sscan)
{
    return heap_markpos(sscan);
}

void heapam_scan_init_parallel_seqscan(TableScanDesc sscan, int32 dop, ScanDirection dir)
{
    return heap_init_parallel_seqscan(sscan, dop, dir);
}


double heapam_index_build_scan(Relation heapRelation, Relation indexRelation, 
                            IndexInfo* indexInfo, bool allow_sync, 
                            IndexBuildCallback callback, void* callback_state) {
    return IndexBuildHeapScan(heapRelation, indexRelation, indexInfo, allow_sync, callback, callback_state);
}


void heapam_index_validate_scan (Relation heapRelation, Relation indexRelation, 
                             IndexInfo* indexInfo, Snapshot snapshot, v_i_state* state) {
    return validate_index_heapscan(heapRelation, indexRelation, indexInfo, snapshot, state);
}

double heapam_relation_copy_for_cluster(Relation OldHeap, Relation OldIndex, 
                                 Relation NewHeap, TransactionId OldestXmin, 
                                 TransactionId FreezeXid, bool verbose, 
                                 bool use_sort, AdaptMem* memUsage, TransactionId* ptrFreezeXid) {
    return copy_heap_data_internal(OldHeap, OldIndex, NewHeap, OldestXmin, FreezeXid, verbose, use_sort, memUsage);
}


const TableAmRoutine g_heapam_methods = {
    /* ------------------------------------------------------------------------
     * TABLE SLOT AM APIs
     * ------------------------------------------------------------------------
     */
    tslot_clear : heapam_tslot_clear,
    tslot_materialize : heapam_tslot_materialize,
    tslot_get_minimal_tuple : heapam_tslot_get_minimal_tuple,
    tslot_copy_minimal_tuple : heapam_tslot_copy_minimal_tuple,
    tslot_store_minimal_tuple : heapam_tslot_store_minimal_tuple,
    tslot_get_heap_tuple : heapam_tslot_get_heap_tuple,
    tslot_copy_heap_tuple : heapam_tslot_copy_heap_tuple,
    tslot_store_tuple : heapam_tslot_store_heap_tuple,
    tslot_getsomeattrs : heapam_tslot_getsomeattrs,
    tslot_getattr : heapam_tslot_getattr,
    tslot_getallattrs : heapam_tslot_getallattrs,
    tslot_attisnull : heapam_tslot_attisnull,


    /* ------------------------------------------------------------------------
     * TABLE TUPLE AM APIs
     * ------------------------------------------------------------------------
     */
    tops_getsysattr : heapam_tops_getsysattr,
    tops_form_minimal_tuple : heapam_tops_form_minimal_tuple,
    tops_form_tuple : heapam_tops_form_tuple,
    tops_form_cmprs_tuple : heapam_tops_form_cmprs_tuple,
    tops_deform_tuple : heapam_tops_deform_tuple,
    tops_deform_cmprs_tuple : heapam_tops_deform_cmprs_tuple,
    tops_computedatasize_tuple : heapam_tops_computedatasize_tuple,
    tops_fill_tuple : heapam_tops_fill_tuple,
    tops_modify_tuple : heapam_tops_modify_tuple,
    tops_tuple_getattr : heapam_tops_tuple_getattr,
    tops_tuple_attisnull : heapam_tops_tuple_attisnull,
    tops_copy_tuple : heapam_tops_copy_tuple,

    /* -----------------------------------------------------------------------
     * SCAN AM APIS
     * -----------------------------------------------------------------------
     */
    scan_index_fetch_begin : heapam_scan_index_fetch_begin,
    scan_index_fetch_reset : heapam_scan_index_fetch_reset,
    scan_index_fetch_end : heapam_scan_index_fetch_end,
    scan_index_fetch_tuple : heapam_scan_index_fetch_tuple,
    scan_begin : heapam_scan_begin,
    scan_begin_bm : heapam_scan_begin_bm,
    scan_begin_sampling : heapam_scan_begin_sampling,
    scan_rescan : heapam_scan_rescan,
    scan_restrpos : heapam_scan_restrpos,
    scan_markpos : heapam_scan_markpos,
    scan_init_parallel_seqscan : heapam_scan_init_parallel_seqscan,
    scan_getnexttuple : heapam_scan_getnexttuple,
    scan_getpage : heapam_scan_getpage,
    scan_end : heapam_scan_end,

    /* ------------------------------------------------------------------------
     * DQL AM APIs
     * ------------------------------------------------------------------------
     */
    tuple_fetch : heapam_tuple_fetch,
    tuple_satisfies_snapshot : heapam_tuple_satisfies_snapshot,
    tuple_get_latest_tid : heapam_tuple_get_latest_tid,

    /* ------------------------------------------------------------------------
     * DML AM APIs
     * ------------------------------------------------------------------------
     */
    tuple_insert : heapam_tuple_insert,
    tuple_multi_insert : heapam_tuple_multi_insert,
    tuple_delete : heapam_tuple_delete,
    tuple_update : heapam_tuple_update,
    tuple_lock : heapam_tuple_lock,
    tuple_lock_updated : heapam_tuple_lock_updated,


    /* ------------------------------------------------------------------------
     * DDL AM APIs
     * ------------------------------------------------------------------------
     */
    index_build_scan : heapam_index_build_scan,
    index_validate_scan : heapam_index_validate_scan,
    relation_copy_for_cluster : heapam_relation_copy_for_cluster
};

/* extern TableAmRoutine g_heapam_methods; */
const TableAmRoutine * const g_tableam_routines[] = {
    &g_heapam_methods
};

