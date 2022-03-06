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
#include "access/ustore/knl_uam.h"
#include "access/ustore/knl_uscan.h"
#include "access/ustore/knl_uheap.h" 
#include "access/xact.h"
#include "executor/executor.h"
#include "executor/node/nodeTidscan.h"
#include "commands/cluster.h"
#include "commands/vacuum.h"
#include "storage/buf/bufmgr.h"
#include "storage/item/itemptr.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/tcap.h"
#include "catalog/index.h"

#include "access/ustore/knl_utuple.h"
#include "access/ustore/knl_uvisibility.h"

/* ------------------------------------------------------------------------
 * HEAP TABLE SLOT AM APIs
 * ------------------------------------------------------------------------
 */


const TableAmRoutine *GetTableAmRoutine(TableAmType type)
{
    return g_tableam_routines[type];
}

/*
 * Clears the contents of the table slot that contains heap table tuple data.
 */
void tableam_tslot_clear(TupleTableSlot *slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_clear(slot);
}

HeapTuple tableam_tslot_materialize(TupleTableSlot *slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_materialize(slot);
}

MinimalTuple tableam_tslot_get_minimal_tuple(TupleTableSlot *slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_get_minimal_tuple(slot);
}


MinimalTuple tableam_tslot_copy_minimal_tuple(TupleTableSlot *slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_copy_minimal_tuple(slot);
}

void tableam_tslot_store_minimal_tuple(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree)
{
    g_tableam_routines[slot->tts_tupslotTableAm]->tslot_store_minimal_tuple(mtup, slot, shouldFree);
}

HeapTuple tableam_tslot_get_heap_tuple(TupleTableSlot *slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_get_heap_tuple(slot);
}

HeapTuple tableam_tslot_copy_heap_tuple(TupleTableSlot *slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_copy_heap_tuple(slot);
}

void tableam_tslot_store_tuple(Tuple tuple, TupleTableSlot *slot, Buffer buffer, bool shouldFree, bool batchMode)
{
    g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tslot_store_tuple(tuple, slot, buffer, shouldFree, batchMode);
}

void tableam_tslot_getsomeattrs(TupleTableSlot *slot, int natts)
{
    g_tableam_routines[slot->tts_tupslotTableAm]->tslot_getsomeattrs(slot, natts);
}

void tableam_tslot_formbatch(TupleTableSlot* slot, VectorBatch* batch, int cur_rows, int natts)
{
    g_tableam_routines[slot->tts_tupslotTableAm]->tslot_formbatch(slot, batch, cur_rows, natts);
}

Datum tableam_tslot_getattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_getattr(slot, attnum, isnull);
}

void tableam_tslot_getallattrs(TupleTableSlot *slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_getallattrs(slot);
}

bool tableam_tslot_attisnull(TupleTableSlot *slot, int attnum)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_attisnull(slot, attnum);
}

Tuple tableam_tslot_get_tuple_from_slot(Relation relation, TupleTableSlot *slot)
{
    slot->tts_tupleDescriptor->tdhasuids = RELATION_HAS_UIDS(relation);
    return g_tableam_routines[relation->rd_tam_type]->tslot_get_tuple_from_slot(slot);
}

/* ------------------------------------------------------------------------
 * TABLE TUPLE AM APIs
 * ------------------------------------------------------------------------
 */

Datum tableam_tops_getsysattr(Tuple tup, int attnum, TupleDesc tuple_desc, bool *isnull,
    Buffer buf)
{
    AssertValidTuple(tup);
    return g_tableam_routines[GetTabelAmIndexTuple(tup)]->tops_getsysattr(tup, attnum, tuple_desc, isnull, buf);
}

MinimalTuple tableam_tops_form_minimal_tuple(TupleDesc tuple_descriptor, Datum *values,
    const bool *isnull, MinimalTuple in_tuple, uint32 tupTableType)
{
    AssertValidTupleType(tupTableType);
    return g_tableam_routines[GetTableAMIndex(tupTableType)]->tops_form_minimal_tuple(tuple_descriptor, values, isnull,
        in_tuple);
}

Tuple tableam_tops_form_tuple(TupleDesc tuple_descriptor, Datum *values, bool *isnull,
    uint32 tupTableType)
{
    AssertValidTupleType(tupTableType);
    return g_tableam_routines[GetTableAMIndex(tupTableType)]->tops_form_tuple(tuple_descriptor, values, isnull);
}

Tuple tableam_tops_form_cmprs_tuple(TupleDesc tuple_descriptor, FormCmprTupleData *cmprs_info,
    uint32 tupTableType)
{
    AssertValidTupleType(tupTableType);
    return g_tableam_routines[GetTableAMIndex(tupTableType)]->tops_form_cmprs_tuple(tuple_descriptor, cmprs_info);
}


void tableam_tops_deform_tuple(Tuple tuple, TupleDesc tuple_desc, Datum *values, bool *isnull)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_deform_tuple(tuple, tuple_desc, values, isnull);
}

void tableam_tops_deform_tuple2(Tuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull, Buffer buffer)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_deform_tuple2(tuple, tupleDesc, values, isnull, buffer);
}

void tableam_tops_deform_cmprs_tuple(Tuple tuple, TupleDesc tuple_desc, Datum *values, bool *isnull,
    char *cmprs_info)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_deform_cmprs_tuple(tuple, tuple_desc, values, isnull,
        cmprs_info);
}

void tableam_tops_fill_tuple(TupleDesc tuple_desc, Datum *values, const bool *isnull, char *data,
    Size data_size, uint16 *infomask, bits8 *bit)
{
    return g_tableam_routines[tuple_desc->tdTableAmType]->tops_fill_tuple(tuple_desc, values, isnull, data, data_size,
        infomask, bit);
}

/*
 * there is no uheapam_tops_modify_tuple
 * but this is done for completeness
 */
Tuple tableam_tops_modify_tuple(Tuple tuple, TupleDesc tuple_desc, Datum *repl_values,
    const bool *repl_isnull, const bool *do_replace)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_modify_tuple(tuple, tuple_desc, repl_values,
        repl_isnull, do_replace);
}

Tuple tableam_tops_opfusion_modify_tuple(Tuple tuple, TupleDesc tuple_desc,
    Datum* repl_values, bool* repl_isnull, UpdateFusion* opf)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_opfusion_modify_tuple(tuple, tuple_desc,
        repl_values, repl_isnull, opf);
}



Datum tableam_tops_tuple_getattr(Tuple tuple, int att_num, TupleDesc tuple_desc, bool *is_null)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_tuple_getattr(tuple, att_num,
        tuple_desc, is_null);
}

Datum tableam_tops_tuple_fast_getattr(Tuple tuple, int att_num, TupleDesc tuple_desc, bool *is_null)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_tuple_fast_getattr(tuple, att_num,
        tuple_desc, is_null);
}

bool tableam_tops_tuple_attisnull(Tuple tuple, int attnum, TupleDesc tuple_desc)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_tuple_attisnull(tuple, attnum, tuple_desc);
}

bool tableam_tops_page_get_item(Relation rel, Tuple tuple, Page page,
    OffsetNumber tupleNo, BlockNumber destBlocks)
{
    return g_tableam_routines[rel->rd_tam_type]->tops_page_get_item(rel, tuple, page, tupleNo, destBlocks);
}
OffsetNumber tableam_tops_page_get_max_offsetnumber(Relation rel, Page page)
{
    return g_tableam_routines[rel->rd_tam_type]->tops_page_get_max_offsetnumber(page);
}
Size tableam_tops_page_get_freespace(Relation rel, Page page)
{
    return g_tableam_routines[rel->rd_tam_type]->tops_page_get_freespace(page);
}
bool tableam_tops_tuple_fetch_row_version(TidScanState* node, Relation relation, ItemPointer tid,
        Snapshot snapshot, TupleTableSlot *slot)
{
    return g_tableam_routines[relation->rd_tam_type]->tops_tuple_fetch_row_version(node, relation, tid, snapshot, slot);
}

Tuple tableam_tops_copy_tuple(Tuple tuple)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_copy_tuple(tuple);
}

MinimalTuple tableam_tops_copy_minimal_tuple(MinimalTuple mtup)
{
    return heap_copy_minimal_tuple(mtup);
}

void tableam_tops_free_minimal_tuple(MinimalTuple mtup)
{
    heap_free_minimal_tuple(mtup);
}

Tuple tableam_tops_new_tuple(Relation relation, ItemPointer tid)
{
    return g_tableam_routines[relation->rd_tam_type]->tops_new_tuple(relation, tid);
}

TransactionId tableam_tops_get_conflictXid(Relation relation, Tuple tup)
{
    return g_tableam_routines[relation->rd_tam_type]->tops_get_conflictXid(tup);
}

void tableam_tops_destroy_tuple(Relation relation, Tuple tuple)
{
    g_tableam_routines[relation->rd_tam_type]->tops_destroy_tuple(tuple);
}

void tableam_tops_add_to_bulk_insert_select(Relation relation, CopyFromBulk bulk, Tuple tup,
    bool needCopy)
{
    g_tableam_routines[relation->rd_tam_type]->tops_add_to_bulk_insert_select(bulk, tup, needCopy);
}

void tableam_tops_add_to_bulk(Relation relation,
                                                CopyFromBulk bulk, Tuple tup, bool needCopy)
{
    g_tableam_routines[relation->rd_tam_type]->tops_add_to_bulk(bulk, tup, needCopy);
}

ItemPointer tableam_tops_get_t_self(Relation relation, Tuple tup)
{
    return g_tableam_routines[relation->rd_tam_type]->tops_get_t_self(tup);
}

void tableam_tops_exec_delete_index_tuples(TupleTableSlot *slot, Relation relation,
    ModifyTableState *node, ItemPointer tupleid, ExecIndexTuplesState exec_index_tuples_state,
    Bitmapset *modifiedIdxAttrs)
{
    g_tableam_routines[relation->rd_tam_type]->tops_exec_delete_index_tuples(slot, relation, node, tupleid,
        exec_index_tuples_state, modifiedIdxAttrs);
}

List *tableam_tops_exec_update_index_tuples(TupleTableSlot *slot, TupleTableSlot *oldslot,
    Relation relation, ModifyTableState *node, Tuple tuple, ItemPointer tupleid,
    ExecIndexTuplesState exec_index_tuples_state, int2 bucketid, Bitmapset *modifiedIdxAttrs)
{
    return g_tableam_routines[relation->rd_tam_type]->tops_exec_update_index_tuples(slot, oldslot, relation, node,
        tuple, tupleid, exec_index_tuples_state, bucketid, modifiedIdxAttrs);
}

uint32 tableam_tops_get_tuple_type(Relation relation)
{
    return g_tableam_routines[relation->rd_tam_type]->tops_get_tuple_type();
}

void tableam_tops_copy_from_insert_batch(Relation rel, EState* estate, CommandId mycid, int hiOptions,
        ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, BulkInsertState bistate, int nBufferedTuples,
        Tuple* bufferedTuples, Partition partition, int2 bucketId)
{
    g_tableam_routines[rel->rd_tam_type]->tops_copy_from_insert_batch(rel, estate, mycid, hiOptions, resultRelInfo,
        myslot, bistate, nBufferedTuples, bufferedTuples, partition, bucketId);
}

void tableam_tops_update_tuple_with_oid(Relation relation, Tuple tup, TupleTableSlot *slot)
{
    g_tableam_routines[relation->rd_tam_type]->tops_update_tuple_with_oid(relation, tup, slot);
}

Datum HeapamTopsTupleFastGetattr(Tuple tuple, int att_num, TupleDesc tuple_desc, bool *is_null)
{
    return fastgetattr((HeapTuple)tuple, att_num, tuple_desc, is_null);
}

/* ------------------------------------------------------------------------
 * DQL AM APIs
 * ------------------------------------------------------------------------
 */
bool tableam_tuple_fetch(Relation relation, Snapshot snapshot, HeapTuple tuple, Buffer *userbuf,
    bool keep_buf, Relation stats_relation)
{
    return g_tableam_routines[relation->rd_tam_type]->tuple_fetch(relation, snapshot, tuple, userbuf, keep_buf,
        stats_relation);
}

bool tableam_tuple_satisfies_snapshot(Relation relation, HeapTuple tuple, Snapshot snapshot,
    Buffer buffer)
{
    return g_tableam_routines[relation->rd_tam_type]->tuple_satisfies_snapshot(relation, tuple, snapshot, buffer);
}

void tableam_tuple_get_latest_tid(Relation relation, Snapshot snapshot, ItemPointer tid)
{
    return g_tableam_routines[relation->rd_tam_type]->tuple_get_latest_tid(relation, snapshot, tid);
}

Oid tableam_tuple_insert(Relation relation, Tuple tup, CommandId cid, int options,
    struct BulkInsertStateData *bistate)
{
    return g_tableam_routines[relation->rd_tam_type]->tuple_insert(relation, tup, cid, options, bistate);
}

int tableam_tuple_multi_insert(Relation relation, Relation parent, Tuple *tuples, int ntuples,
    CommandId cid, int options, struct BulkInsertStateData *bistate, HeapMultiInsertExtraArgs *args)
{
    return g_tableam_routines[relation->rd_tam_type]->tuple_multi_insert(relation, parent, tuples, ntuples, cid,
        options, bistate, args);
}

TM_Result tableam_tuple_delete(Relation relation, ItemPointer tid, CommandId cid, Snapshot crosscheck,
    Snapshot snapshot, bool wait, TupleTableSlot **oldslot, TM_FailureData *tmfd, bool allow_delete_self)
{
    return g_tableam_routines[relation->rd_tam_type]->tuple_delete(relation, tid, cid, crosscheck, snapshot, wait,
        oldslot, tmfd, allow_delete_self);
}


TM_Result tableam_tuple_update(Relation relation, Relation parentRelation, ItemPointer otid, Tuple newtup,
    CommandId cid, Snapshot crosscheck, Snapshot snapshot, bool wait, TupleTableSlot **oldslot, TM_FailureData *tmfd,
    bool *update_indexes, Bitmapset **modifiedIdxAttrs, bool allow_update_self,
    bool allow_inplace_update, LockTupleMode *lockmode)
{
    return g_tableam_routines[relation->rd_tam_type]->tuple_update(relation, parentRelation, otid, newtup, cid,
        crosscheck, snapshot, wait, oldslot, tmfd, lockmode, update_indexes, modifiedIdxAttrs, allow_update_self,
        allow_inplace_update);
}

TM_Result tableam_tuple_lock(Relation relation, Tuple tuple, Buffer *buffer, CommandId cid,
    LockTupleMode mode, bool nowait, TM_FailureData *tmfd, bool allow_lock_self, bool follow_updates, bool eval,
    Snapshot snapshot, ItemPointer tid, bool isSelectForUpdate, bool isUpsert, TransactionId conflictXid,
    int waitSec)
{
    return g_tableam_routines[relation->rd_tam_type]->tuple_lock(relation, tuple, buffer, cid, mode, nowait, tmfd,
        allow_lock_self, follow_updates, eval, snapshot, tid, isSelectForUpdate, isUpsert, conflictXid,
        waitSec);
}

Tuple tableam_tuple_lock_updated(CommandId cid, Relation relation, int lockmode, ItemPointer tid,
    TransactionId priorXmax, Snapshot snapshot, bool isSelectForUpdate)
{
    return (Tuple)g_tableam_routines[relation->rd_tam_type]->tuple_lock_updated(cid, relation, lockmode, tid, priorXmax,
        snapshot, isSelectForUpdate);
}

void tableam_tuple_check_visible(Relation relation, Snapshot snapshot, Tuple tuple, Buffer buffer)
{
    g_tableam_routines[relation->rd_tam_type]->tuple_check_visible(snapshot, tuple, buffer);
}

void tableam_tuple_abort_speculative(Relation relation, Tuple tuple)
{
    g_tableam_routines[relation->rd_tam_type]->tuple_abort_speculative(relation, tuple);
}

/* -----------------------------------------------------------------------
 * SCAN AM APIS FOR HEAP
 * ------------------------------------------------------------------------
 */

IndexFetchTableData *tableam_scan_index_fetch_begin(Relation rel)
{
    return HeapamScanIndexFetchBegin(rel);
}

void tableam_scan_index_fetch_reset(IndexFetchTableData *scan)
{
    heapam_index_fetch_reset(scan);
}

void tableam_scan_index_fetch_end(IndexFetchTableData *scan)
{
    HeapamScanIndexFetchEnd(scan);
}

Tuple tableam_scan_index_fetch_tuple(IndexScanDesc scan, bool *all_dead)
{
    return g_tableam_routines[scan->heapRelation->rd_tam_type]->scan_index_fetch_tuple(scan, all_dead);
}

TableScanDesc tableam_scan_begin(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
    RangeScanInRedis rangeScanInRedis)
{
    return g_tableam_routines[relation->rd_tam_type]->scan_begin(relation, snapshot, nkeys, key, rangeScanInRedis);
}

TableScanDesc tableam_scan_begin_bm(Relation relation, Snapshot snapshot, int nkeys, ScanKey key)
{
    return g_tableam_routines[relation->rd_tam_type]->scan_begin_bm(relation, snapshot, nkeys, key);
}

TableScanDesc tableam_scan_begin_sampling(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
    bool allow_strat, bool allow_sync, RangeScanInRedis rangeScanInRedis)
{
    return g_tableam_routines[relation->rd_tam_type]->scan_begin_sampling(relation, snapshot, nkeys, key, allow_strat,
        allow_sync, rangeScanInRedis);
}

Tuple tableam_scan_getnexttuple(TableScanDesc sscan, ScanDirection direction)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_getnexttuple(sscan, direction);
}

bool tableam_scan_gettuplebatchmode(TableScanDesc sscan, ScanDirection direction)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_GetNextBatch(sscan, direction);
}

void tableam_scan_getpage(TableScanDesc sscan, BlockNumber page)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_getpage(sscan, page);
}

Tuple tableam_scan_gettuple_for_verify(TableScanDesc sscan, ScanDirection direction, bool isValidRelationPage)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_gettuple_for_verify(sscan,
        direction, isValidRelationPage);
}

void tableam_scan_end(TableScanDesc sscan)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_end(sscan);
}

void tableam_scan_rescan(TableScanDesc sscan, ScanKey key)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_rescan(sscan, key);
}

void tableam_scan_restrpos(TableScanDesc sscan)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_restrpos(sscan);
}

void tableam_scan_markpos(TableScanDesc sscan)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_markpos(sscan);
}

void tableam_scan_init_parallel_seqscan(TableScanDesc sscan, int32 dop, ScanDirection dir)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_init_parallel_seqscan(sscan, dop, dir);
}

double tableam_index_build_scan(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
    bool allow_sync, IndexBuildCallback callback, void *callback_state, TableScanDesc scan)
{
    return g_tableam_routines[heapRelation->rd_tam_type]->index_build_scan(heapRelation, indexRelation, indexInfo,
        allow_sync, callback, callback_state, scan);
}


void tableam_index_validate_scan(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
    Snapshot snapshot, v_i_state *state)
{
    return g_tableam_routines[heapRelation->rd_tam_type]->index_validate_scan(heapRelation, indexRelation, indexInfo,
        snapshot, state);
}

double tableam_relation_copy_for_cluster(Relation OldHeap, Relation OldIndex, Relation NewHeap,
    TransactionId OldestXmin, TransactionId FreezeXid, bool verbose, bool use_sort, AdaptMem *memUsage)
{
    return g_tableam_routines[OldHeap->rd_tam_type]->relation_copy_for_cluster(OldHeap, OldIndex, NewHeap, OldestXmin,
        FreezeXid, verbose, use_sort, memUsage);
}

/*
 * Implementation of heap accessor methods.
 */

Datum HeapamTopsGetsysattr(Tuple tup, int attnum, TupleDesc tuple_desc, bool* isnull, Buffer buff) {
    Assert(TUPLE_IS_HEAP_TUPLE(HeapTuple(tup)));
    return heap_getsysattr((HeapTuple)tup, attnum, tuple_desc, isnull);
}

Datum HeapamTopsTupleGetattr(Tuple tuple, int att_num, TupleDesc tuple_desc, bool* is_null)
{
    return heap_getattr((HeapTuple)tuple, att_num, tuple_desc, is_null);
}

bool HeapamTopsTupleAttisnull(Tuple tup, int attnum, TupleDesc tuple_desc)
{
    return heap_attisnull((HeapTuple)tup, attnum, tuple_desc);
}

bool HeapamTopsPageGetItem(Relation rel, Tuple tuple, Page page, OffsetNumber tupleNo, BlockNumber destBlocks)
{
    HeapTupleHeader tupleHeader = NULL;
    ItemId tupleItem = NULL;
    tupleItem = PageGetItemId(page, tupleNo);
    if (!ItemIdHasStorage(tupleItem)) {
        return false;
    }
    tupleHeader = (HeapTupleHeader)PageGetItem((Page)page, tupleItem);
    /* set block number */
    ItemPointerSetBlockNumber(
        &(tupleHeader->t_ctid), destBlocks + ItemPointerGetBlockNumber(&(tupleHeader->t_ctid)));

    ((HeapTuple)tuple)->t_data = tupleHeader;
    ((HeapTuple)tuple)->t_len = ItemIdGetLength(tupleItem);
    ((HeapTuple)tuple)->t_self = tupleHeader->t_ctid;
    ((HeapTuple)tuple)->tupTableType = HEAP_TUPLE;
    return true;
}

Tuple HeapamTopsCopyTuple(Tuple tuple)
{
    Assert(TUPLE_IS_HEAP_TUPLE(HeapTuple(tuple)));
    return heap_copytuple((HeapTuple)tuple);
}

Tuple HeapamTopsNewTuple(Relation relation, ItemPointer tid)
{
    HeapTuple tuple = heaptup_alloc(HEAPTUPLESIZE);
    tuple->t_data = NULL;
    if (tid != NULL) {
        tuple->t_self = *tid;
    }
    return (Tuple)tuple;
}

TransactionId HeapamTopsGetConflictXid(Tuple tup)
{
    return InvalidTransactionId;
}

void HeapamTopsDestroyTuple(Tuple tuple)
{
    HeapTuple tup = (HeapTuple)tuple;
    if (tup->tupInfo == 1) {
        pfree_ext(tup->t_data);
    }
    pfree_ext(tup);
}

void HeapamTopsAddToBulkInsertSelect(CopyFromBulk bulk, Tuple tup, bool needCopy)
{
    HeapAddToBulkInsertSelect(bulk, tup, needCopy);
}

void HeapamTopsAddToBulk(CopyFromBulk bulk, Tuple tup, bool needCopy)
{
    HeapAddToBulk(bulk, tup, needCopy);
}

void HeapamTopsUpdateTupleWithOid (Relation rel, Tuple tuple, TupleTableSlot *slot)
{
    return;
}

ItemPointer HeapamTopsGetTSelf(Tuple tup)
{
    return &(((HeapTuple)tup)->t_self);
}

void HeapamTopsExecDeleteIndexTuples(TupleTableSlot *slot, Relation relation, ModifyTableState *node,
    ItemPointer tupleid, ExecIndexTuplesState exec_index_tuples_state, Bitmapset *modifiedIdxAttrs)
{
    return;
}

List *HeapamTopsExecUpdateIndexTuples(TupleTableSlot *slot, TupleTableSlot *oldslot, Relation relation,
    ModifyTableState *node, Tuple tuple, ItemPointer tupleid, ExecIndexTuplesState exec_index_tuples_state,
    int2 bucketid, Bitmapset *modifiedIdxAttrs)
{
    List *recheckIndexes = NULL;
    /*
     * insert index entries for tuple
     */
    recheckIndexes = ExecInsertIndexTuples(slot, &(((HeapTuple)tuple)->t_self), exec_index_tuples_state.estate,
        exec_index_tuples_state.targetPartRel, exec_index_tuples_state.p, bucketid, exec_index_tuples_state.conflict,
        modifiedIdxAttrs);
    return recheckIndexes;
}

uint32 HeapamTopsGetTupleType()
{
    return HEAP_TUPLE;
}

void HeapamTopsCopyFromInsertBatch(Relation rel, EState* estate, CommandId mycid, int hiOptions,
        ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, BulkInsertState bistate, int nBufferedTuples,
        Tuple* bufferedTuples, Partition partition, int2 bucketId)
{
    CopyFromInsertBatch(rel, estate, mycid, hiOptions, resultRelInfo, myslot, bistate, nBufferedTuples,
        (HeapTuple*) bufferedTuples, partition, bucketId);
}

/*
 * Clears the contents of the table slot that contains heap table tuple data.
 */
void HeapamTslotClear(TupleTableSlot *slot)
{
    return heap_slot_clear(slot);
}

HeapTuple HeapamTslotMaterialize(TupleTableSlot *slot)
{
    heap_slot_materialize(slot);
    return (HeapTuple)slot->tts_tuple;
}

MinimalTuple HeapamTslotGetMinimalTuple(TupleTableSlot *slot)
{
    return heap_slot_get_minimal_tuple(slot);
}

MinimalTuple HeapamTslotCopyMinimalTuple(TupleTableSlot *slot)
{
    return heap_slot_copy_minimal_tuple(slot);
}

void HeapamTslotStoreMinimalTuple(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree)
{
    heap_slot_store_minimal_tuple(mtup, slot, shouldFree);
}

HeapTuple HeapamTslotGetHeapTuple(TupleTableSlot* slot)
{
    return heap_slot_get_heap_tuple(slot);
}

HeapTuple HeapamTslotCopyHeapTuple(TupleTableSlot *slot)
{
    return heap_slot_copy_heap_tuple(slot);
}

void HeapamTslotStoreHeapTuple(Tuple tuple, TupleTableSlot* slot, Buffer buffer, bool shouldFree, bool batchMode)
{
    heap_slot_store_heap_tuple((HeapTuple)tuple, slot, buffer, shouldFree, batchMode);
}

void HeapamTslotGetsomeattrs(TupleTableSlot *slot, int natts)
{
    heap_slot_getsomeattrs(slot, natts);
}

void HeapamTslotFormBatch(TupleTableSlot *slot, VectorBatch* batch, int cur_rows, int natts)
{
    heap_slot_formbatch(slot, batch, cur_rows, natts);
}

Datum HeapamTslotGetattr(TupleTableSlot* slot, int attnum, bool* isnull)
{
    return heap_slot_getattr(slot, attnum, isnull);
}

void HeapamTslotGetallattrs(TupleTableSlot* slot)
{
    heap_slot_getallattrs(slot);
}

bool HeapamTslotAttisnull(TupleTableSlot* slot, int attnum)
{
    return heap_slot_attisnull(slot, attnum);
}

FORCE_INLINE Tuple HeapamTslotGetTupleFromSlot(TupleTableSlot* slot)
{
    HeapTuple tuple = ExecMaterializeSlot(slot);
    tuple->tupInfo = 0;
    return (Tuple) tuple;
}

/* ------------------------------------------------------------------------
 * TABLE TUPLE AM APIs
 * ------------------------------------------------------------------------
 */

MinimalTuple HeapamTopsFormMinimalTuple(
    TupleDesc tuple_descriptor, Datum* values, const bool* isnull, MinimalTuple in_tuple)
{
    return heap_form_minimal_tuple(tuple_descriptor, values, isnull, in_tuple);
}

Tuple HeapamTopsFormTuple(TupleDesc tuple_descriptor, Datum* values, bool* isnull)
{
    return (Tuple)heap_form_tuple(tuple_descriptor, values, isnull);
}

HeapTuple HeapamTopsFormCmprsTuple(TupleDesc tuple_descriptor, FormCmprTupleData* cmprs_info)
{
    return heap_form_cmprs_tuple(tuple_descriptor, cmprs_info);
}

void HeapamTopsDeformTuple(Tuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull)
{
    Assert(TUPLE_IS_HEAP_TUPLE(HeapTuple(tuple)));
    return heap_deform_tuple((HeapTuple)tuple, tuple_desc, values, isnull);
}

void HeapamTopsDeformTuple2(Tuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull, Buffer buffer)
{
    Assert(TUPLE_IS_HEAP_TUPLE(HeapTuple(tuple)));
    return heap_deform_tuple2((HeapTuple)tuple, tupleDesc, values, isnull, buffer);
}

void HeapamTopsDeformCmprsTuple(Tuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull, char* cmprs_info)
{
    Assert(TUPLE_IS_HEAP_TUPLE(HeapTuple(tuple)));
    heap_deform_cmprs_tuple((HeapTuple)tuple, tuple_desc, values, isnull, cmprs_info);
}

void HeapamTopsFillTuple(TupleDesc tuple_desc, Datum* values, const bool* isnull, char* data, Size data_size, uint16* infomask, bits8* bit)
{
    heap_fill_tuple(tuple_desc, values, isnull, data, data_size, infomask, bit);
}

Tuple HeapamTopsModifyTuple(Tuple tuple, TupleDesc tuple_desc, Datum* repl_values, const bool* repl_isnull, const bool* do_replace)
{
    return (Tuple)heap_modify_tuple((HeapTuple)tuple, tuple_desc, repl_values, repl_isnull, do_replace);
}

Tuple HeapamTopsOpFusionModifyTuple(Tuple tuple, TupleDesc tuple_desc, Datum* repl_values,
    bool* repl_isnull, UpdateFusion* opf)
{
    Tuple newTuple;
    tableam_tops_deform_tuple(tuple, tuple_desc, repl_values, repl_isnull);
    opf->refreshTargetParameterIfNecessary();
    /*
     * create a new tuple from the values and isnull arrays
     */
    newTuple = tableam_tops_form_tuple(tuple_desc, repl_values, repl_isnull, HEAP_TUPLE);
    /*
     * copy the identification info of the old tuple: t_ctid, t_self, and OID
     * (if any)
     */
    ((HeapTuple)newTuple)->t_data->t_ctid = ((HeapTuple)tuple)->t_data->t_ctid;
    ((HeapTuple)newTuple)->t_self = ((HeapTuple)tuple)->t_self;
    ((HeapTuple)newTuple)->t_tableOid = ((HeapTuple)tuple)->t_tableOid;
    ((HeapTuple)newTuple)->t_bucketId = ((HeapTuple)tuple)->t_bucketId;
    HeapTupleCopyBase((HeapTuple)newTuple, (HeapTuple)tuple);
#ifdef PGXC
    ((HeapTuple)newTuple)->t_xc_node_id = ((HeapTuple)tuple)->t_xc_node_id;
#endif
    Assert(tuple_desc->tdhasoid == false);
    return newTuple;
}

/* ------------------------------------------------------------------------
 * DQL AM APIs
 * ------------------------------------------------------------------------
 */
bool HeapamTupleFetch(Relation relation, Snapshot snapshot,
                                                  HeapTuple tuple, Buffer *userbuf, bool keep_buf, Relation stats_relation) {
    return heap_fetch(relation, snapshot, tuple, userbuf, keep_buf, stats_relation);
}

bool HeapamTupleSatisfiesSnapshot(Relation relation, HeapTuple tuple,
                                                             Snapshot snapshot, Buffer buffer) {
    return HeapTupleSatisfiesVisibility(tuple, snapshot, buffer);
}

void HeapamTupleGetLatestTid(Relation relation, Snapshot snapshot,
                                                        ItemPointer tid) {
    return heap_get_latest_tid(relation, snapshot, tid);
}

/* ------------------------------------------------------------------------
 * DML AM APIs
 * ------------------------------------------------------------------------
 */

Oid HeapamTupleInsert (Relation relation, Tuple tup, CommandId cid,
                           int options, struct BulkInsertStateData *bistate) {
    return heap_insert(relation, (HeapTuple)tup, cid, options, bistate);
}

int HeapamTupleMultiInsert(Relation relation, Relation parent,
                                  Tuple* tuples, int ntuples, CommandId cid, 
                                  int options, struct BulkInsertStateData *bistate, 
                                  HeapMultiInsertExtraArgs *args) {
    return heap_multi_insert(relation, parent, (HeapTuple*)tuples, ntuples, cid, options, bistate, args);
}

TM_Result HeapamTupleDelete(Relation relation, ItemPointer tid,
                                    CommandId cid, Snapshot crosscheck, Snapshot snapshot,
                                    bool wait, TupleTableSlot** oldslot, TM_FailureData *tmfd,
                                    bool allow_delete_self) {

    return heap_delete(relation, tid, cid, crosscheck, wait, tmfd, allow_delete_self);
}

/* 
 * Since both heapam and uheapam must have the same params, the params
 * here are the union of both versions.  Therefore, some parms are not used.
 */
/* -------------------------------------------------------------------------- */
TM_Result HeapamTupleUpdate(Relation relation, Relation parentRelation, ItemPointer otid, Tuple newtup, CommandId cid,
    Snapshot crosscheck, Snapshot snapshot, bool wait, TupleTableSlot **oldslot, TM_FailureData *tmfd,
    LockTupleMode* lockmode, bool *update_indexes, Bitmapset **modifiedIdxAttrs, bool allow_update_self,
    bool allow_inplace_update)
{
    TM_Result result = heap_update(relation, parentRelation, otid, (HeapTuple)newtup,
        cid, crosscheck, wait, tmfd, lockmode, allow_update_self);

    /* make update_indexes optional */
    if(update_indexes) {
        *update_indexes = (result == TM_Ok && !HeapTupleIsHeapOnly((HeapTuple)newtup));
    }

    return result;
}

TM_Result HeapamTupleLock(Relation relation, Tuple tuple, Buffer *buffer,
    CommandId cid, LockTupleMode mode, bool nowait, TM_FailureData *tmfd,
    bool allow_lock_self, bool follow_updates, bool eval, Snapshot snapshot,
    ItemPointer tid, bool isSelectForUpdate, bool isUpsert, TransactionId conflictXid,
    int waitSec)
{
    return heap_lock_tuple(relation, (HeapTuple)tuple, buffer, cid, mode, nowait, follow_updates, tmfd,
                           allow_lock_self, waitSec);
}

Tuple HeapamTupleLockUpdated(CommandId cid, Relation relation, int lockmode, ItemPointer tid,
    TransactionId priorXmax, Snapshot snapshot, bool isSelectForUpdate)
{
    return (Tuple)heap_lock_updated(cid, relation, lockmode, tid, priorXmax);
}

void HeapamTupleCheckVisible(Snapshot snapshot, Tuple tuple, Buffer buffer)
{
    HeapTupleCheckVisible(snapshot, (HeapTuple)tuple, buffer);
}

/* -----------------------------------------------------------------------
 * SCAN AM APIS FOR HEAP
 * ------------------------------------------------------------------------
 */

IndexFetchTableData * HeapamScanIndexFetchBegin(Relation rel)
{
    return heapam_index_fetch_begin(rel);
}

void HeapamScanIndexFetchReset(IndexFetchTableData *scan)
{
    return heapam_index_fetch_reset(scan);
}

void HeapamScanIndexFetchEnd(IndexFetchTableData *scan)
{
    return heapam_index_fetch_end(scan);
}

Tuple HeapamScanIndexFetchTuple(IndexScanDesc scan, bool *all_dead)
{
    return (Tuple)heapam_index_fetch_tuple(scan, all_dead);
}

TableScanDesc HeapamScanBegin(Relation relation, Snapshot snapshot, int nkeys, ScanKey key, RangeScanInRedis rangeScanInRedis)
{
    return heap_beginscan(relation, snapshot, nkeys, key, rangeScanInRedis);
}
    
TableScanDesc HeapamScanBeginBm(Relation relation, Snapshot snapshot, int nkeys, ScanKey key)
{
    return heap_beginscan_bm(relation, snapshot, nkeys, key);
}

TableScanDesc HeapamScanBeginSampling(Relation relation, Snapshot snapshot, int nkeys, ScanKey key, bool allow_strat, bool allow_sync, RangeScanInRedis rangeScanInRedis)
{
    return heap_beginscan_sampling(relation, snapshot, nkeys, key, allow_strat, allow_sync, rangeScanInRedis);
}

// The heap am implementation of abstract method scan_getnexttuple
Tuple HeapamScanGetnexttuple(TableScanDesc sscan, ScanDirection direction)
{
    return (Tuple) heap_getnext(sscan, direction);
}

void HeapamScanGetpage(TableScanDesc sscan, BlockNumber page)
{
    return heapgetpage(sscan, page);
}

Tuple HeapamGetNextForVerify(TableScanDesc sscan, ScanDirection direction, bool isValidRelationPage)
{
    return (Tuple)heapGetNextForVerify(sscan, direction, isValidRelationPage);
}

void HeapamScanEnd(TableScanDesc sscan)
{
    return heap_endscan(sscan);
}
    
void HeapamScanRescan(TableScanDesc sscan, ScanKey key)
{
     return heap_rescan(sscan, key);
}

void HeapamScanRestrpos(TableScanDesc sscan)
{
    return heap_restrpos(sscan);
}

void HeapamScanMarkpos(TableScanDesc sscan)
{
    return heap_markpos(sscan);
}

void HeapamScanInitParallelSeqscan(TableScanDesc sscan, int32 dop, ScanDirection dir)
{
    return heap_init_parallel_seqscan(sscan, dop, dir);
}

double HeapamIndexBuildScan(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo, bool allow_sync,
                            IndexBuildCallback callback, void *callback_state, TableScanDesc scan)
{
    return IndexBuildHeapScan(heapRelation, indexRelation, indexInfo, allow_sync, callback, callback_state, scan);
}

void HeapamIndexValidateScan (Relation heapRelation, Relation indexRelation,
                             IndexInfo* indexInfo, Snapshot snapshot, v_i_state* state) {
    return validate_index_heapscan(heapRelation, indexRelation, indexInfo, snapshot, state);
}

double HeapamRelationCopyForCluster(Relation OldHeap, Relation OldIndex,
                                 Relation NewHeap, TransactionId OldestXmin, 
                                 TransactionId FreezeXid, bool verbose, 
                                 bool use_sort, AdaptMem* memUsage) {
    return copy_heap_data_internal(OldHeap, OldIndex, NewHeap, OldestXmin, FreezeXid, verbose, use_sort, memUsage);
}

void HeapamAbortSpeculative(Relation relation, Tuple tuple)
{
    heap_abort_speculative(relation, (HeapTuple) tuple);
}

void HeapamTcapPromoteLock (Relation relation, LOCKMODE *lockmode)
{
    /* Protect old versions from recycling during timecapsule. */
    *lockmode = AccessExclusiveLock;
}

bool HeapamTcapValidateSnap(Relation relation, Snapshot snap)
{
    if (RelationIsUstoreIndex(relation)) {
        return true;
    }
    return snap->xmin >= GetGlobalOldestXmin();
}

void HeapamTcapDeleteDelta(Relation relation, Snapshot snap)
{
    TvDeleteDelta(RelationGetRelid(relation), snap);
}

void HeapamTcapInsertLost(Relation relation, Snapshot snap)
{
    TvInsertLost(RelationGetRelid(relation), snap);
}

const TableAmRoutine g_heapam_methods = {
    /* ------------------------------------------------------------------------
     * TABLE SLOT AM APIs
     * ------------------------------------------------------------------------
     */
    tslot_clear : HeapamTslotClear,
    tslot_materialize : HeapamTslotMaterialize,
    tslot_get_minimal_tuple : HeapamTslotGetMinimalTuple,
    tslot_copy_minimal_tuple : HeapamTslotCopyMinimalTuple,
    tslot_store_minimal_tuple : HeapamTslotStoreMinimalTuple,
    tslot_get_heap_tuple : HeapamTslotGetHeapTuple,
    tslot_copy_heap_tuple : HeapamTslotCopyHeapTuple,
    tslot_store_tuple : HeapamTslotStoreHeapTuple,
    tslot_getsomeattrs : HeapamTslotGetsomeattrs,
    tslot_formbatch :  HeapamTslotFormBatch,
    tslot_getattr : HeapamTslotGetattr,
    tslot_getallattrs : HeapamTslotGetallattrs,
    tslot_attisnull : HeapamTslotAttisnull,
    tslot_get_tuple_from_slot : HeapamTslotGetTupleFromSlot,


    /* ------------------------------------------------------------------------
     * TABLE TUPLE AM APIs
     * ------------------------------------------------------------------------
     */
    tops_getsysattr : HeapamTopsGetsysattr,
    tops_form_minimal_tuple : HeapamTopsFormMinimalTuple,
    tops_form_tuple : HeapamTopsFormTuple,
    tops_form_cmprs_tuple : HeapamTopsFormCmprsTuple,
    tops_deform_tuple : HeapamTopsDeformTuple,
    tops_deform_tuple2 : HeapamTopsDeformTuple2,
    tops_deform_cmprs_tuple : HeapamTopsDeformCmprsTuple,
    tops_fill_tuple : HeapamTopsFillTuple,
    tops_modify_tuple : HeapamTopsModifyTuple,
    tops_opfusion_modify_tuple : HeapamTopsOpFusionModifyTuple,
    tops_tuple_getattr : HeapamTopsTupleGetattr,
    tops_tuple_fast_getattr : HeapamTopsTupleFastGetattr,
    tops_tuple_attisnull : HeapamTopsTupleAttisnull,
    tops_copy_tuple : HeapamTopsCopyTuple,
    tops_new_tuple : HeapamTopsNewTuple,
    tops_get_conflictXid : HeapamTopsGetConflictXid,
    tops_destroy_tuple : HeapamTopsDestroyTuple,
    tops_add_to_bulk_insert_select : HeapamTopsAddToBulkInsertSelect,
    tops_add_to_bulk : HeapamTopsAddToBulk,
    tops_update_tuple_with_oid : HeapamTopsUpdateTupleWithOid,
    tops_get_t_self : HeapamTopsGetTSelf,
    tops_exec_delete_index_tuples : HeapamTopsExecDeleteIndexTuples,
    tops_exec_update_index_tuples : HeapamTopsExecUpdateIndexTuples,
    tops_get_tuple_type : HeapamTopsGetTupleType,
    tops_copy_from_insert_batch : HeapamTopsCopyFromInsertBatch,
    tops_page_get_item : HeapamTopsPageGetItem,
    tops_page_get_max_offsetnumber : PageGetMaxOffsetNumber,
    tops_page_get_freespace : PageGetHeapFreeSpace,
    tops_tuple_fetch_row_version : HeapFetchRowVersion,
    

    /* -----------------------------------------------------------------------
     * SCAN AM APIS
     * -----------------------------------------------------------------------
     */
    scan_index_fetch_begin : HeapamScanIndexFetchBegin,
    scan_index_fetch_reset : HeapamScanIndexFetchReset,
    scan_index_fetch_end : HeapamScanIndexFetchEnd,
    scan_index_fetch_tuple : HeapamScanIndexFetchTuple,
    scan_begin : HeapamScanBegin,
    scan_begin_bm : HeapamScanBeginBm,
    scan_begin_sampling : HeapamScanBeginSampling,
    scan_rescan : HeapamScanRescan,
    scan_restrpos : HeapamScanRestrpos,
    scan_markpos : HeapamScanMarkpos,
    scan_init_parallel_seqscan : HeapamScanInitParallelSeqscan,
    scan_getnexttuple : HeapamScanGetnexttuple,
    scan_GetNextBatch : HeapamGetNextBatchMode,
    scan_getpage : HeapamScanGetpage,
    scan_gettuple_for_verify : HeapamGetNextForVerify,
    scan_end : HeapamScanEnd,

    /* ------------------------------------------------------------------------
     * DQL AM APIs
     * ------------------------------------------------------------------------
     */
    tuple_fetch : HeapamTupleFetch,
    tuple_satisfies_snapshot : HeapamTupleSatisfiesSnapshot,
    tuple_get_latest_tid : HeapamTupleGetLatestTid,

    /* ------------------------------------------------------------------------
     * DML AM APIs
     * ------------------------------------------------------------------------
     */
    tuple_insert : HeapamTupleInsert,
    tuple_multi_insert : HeapamTupleMultiInsert,
    tuple_delete : HeapamTupleDelete,
    tuple_update : HeapamTupleUpdate,
    tuple_lock : HeapamTupleLock,
    tuple_lock_updated : HeapamTupleLockUpdated,
    tuple_check_visible: HeapamTupleCheckVisible,
    tuple_abort_speculative: HeapamAbortSpeculative,

    /* ------------------------------------------------------------------------
     * DDL AM APIs
     * ------------------------------------------------------------------------
     */
    index_build_scan : HeapamIndexBuildScan,
    index_validate_scan : HeapamIndexValidateScan,
    relation_copy_for_cluster : HeapamRelationCopyForCluster,

    /* ------------------------------------------------------------------------
     * TIMECAPSULE AM APIs
     * ------------------------------------------------------------------------
     */
    tcap_promote_lock : HeapamTcapPromoteLock,
    tcap_validate_snap : HeapamTcapValidateSnap,
    tcap_delete_delta : HeapamTcapDeleteDelta,
    tcap_insert_lost : HeapamTcapInsertLost
};

/*
 * Implementation of uheap accessor methods.
 */

/* ------------------------------------------------------------------------
 * UHEAP TABLE SLOT AM APIs
 * ------------------------------------------------------------------------
 */

/*
 * Clears the contents of the table slot that contains heap table tuple data.
 */
void UHeapamTslotClear(TupleTableSlot *slot)
{
    return UHeapSlotClear(slot);
}

HeapTuple UHeapamTslotMaterialize(TupleTableSlot *slot)
{
    return UHeapCopyHeapTuple(slot);
}

MinimalTuple UHeapamTslotGetMinimalTuple(TupleTableSlot *slot)
{
    return UHeapSlotGetMinimalTuple(slot);
}

MinimalTuple UHeapamTslotCopyMinimalTuple(TupleTableSlot *slot)
{
    return UHeapSlotCopyMinimalTuple(slot);
}

void UHeapamTslotStoreMinimalTuple(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree)
{
    UHeapSlotStoreMinimalTuple(mtup, slot, shouldFree);
}

HeapTuple UHeapamTslotGetHeapTuple(TupleTableSlot *slot)
{
    return UHeapCopyHeapTuple(slot);
}

HeapTuple UHeapamTslotCopyHeapTuple(TupleTableSlot *slot)
{
    return UHeapCopyHeapTuple(slot);
}

void UHeapamTslotGetsomeattrs(TupleTableSlot *slot, int natts)
{
    UHeapSlotGetSomeAttrs(slot, natts);
}

void UHeapamTslotGetallattrs(TupleTableSlot *slot)
{
    UHeapSlotGetAllAttrs(slot);
}

void UHeapamTslotFormBatch(TupleTableSlot *slot, VectorBatch* batch, int cur_rows, int natts)
{
    UHeapSlotFormBatch(slot, batch, cur_rows, natts);
}

Datum UHeapamTslotGetattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
    return UHeapSlotGetAttr(slot, attnum, isnull);
}

bool UHeapamTslotAttisnull(TupleTableSlot *slot, int attnum)
{
    return UHeapSlotAttIsNull(slot, attnum);
}

Tuple uheapam_tslot_get_tuple_from_slot(TupleTableSlot* slot)
{
    UHeapTuple utuple = NULL;
    if (slot->tts_tupslotTableAm != TAM_USTORE) {
        tableam_tslot_getallattrs(slot); // here has some main difference.
        utuple = (UHeapTuple)tableam_tops_form_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull,
            UHEAP_TUPLE);
        slot->tts_tupslotTableAm = TAM_USTORE;
        utuple->tupInfo = 1;
        ExecStoreTuple((Tuple)utuple, slot, InvalidBuffer, true);
    } else {
        utuple =  ExecGetUHeapTupleFromSlot(slot);
        utuple->tupInfo = 1;
    }
    if (utuple != NULL)
    {
        Assert(utuple->tupTableType == UHEAP_TUPLE);
    }
    return (Tuple) utuple;
}

/* ------------------------------------------------------------------------
 * UHEAP TABLE TUPLE AM APIs
 * ------------------------------------------------------------------------
 */
Datum UHeapamTopsGetsysattr(Tuple tup, int attnum, TupleDesc tuple_desc, bool *isnull, Buffer buff)
{
    Assert(TUPLE_IS_UHEAP_TUPLE(UHeapTuple(tup)));
    return UHeapGetSysAttr((UHeapTuple)tup, buff, attnum, tuple_desc, isnull);
}

MinimalTuple UHeapamTopsFormMinimalTuple(TupleDesc tuple_descriptor, Datum *values, const bool *isnull,
    MinimalTuple in_tuple)
{
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("form_minimal_tuple for uheap not implemented yet")));

    return NULL;
}

Tuple UHeapamTopsFormTuple(TupleDesc tuple_descriptor, Datum *values, bool *isnull)
{
    return (Tuple)UHeapFormTuple(tuple_descriptor, values, isnull);
}

HeapTuple UHeapamTopsFormCmprsTuple(TupleDesc tuple_descriptor, FormCmprTupleData *cmprs_info)
{
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("form_cmprs_tuple for uheap not implemented yet")));

    return NULL;
}

void UHeapamTopsDeformTuple(Tuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull)
{
    Assert(TUPLE_IS_UHEAP_TUPLE(UHeapTuple(tuple)));

    return UHeapDeformTuple((UHeapTuple)tuple, tupleDesc, values, isnull);
}

void UHeapamTopsDeformTuple2(Tuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull, Buffer buffer)
{
    Assert(TUPLE_IS_UHEAP_TUPLE(UHeapTuple(tuple)));

    return UHeapDeformTuple((UHeapTuple)tuple, tupleDesc, values, isnull);
}

void UHeapamTopsDeformCmprsTuple(Tuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull, char *cmprsInfo)
{
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("deform_cmprs_tuple for uheap not implemented yet")));
}

void UHeapamTopsFillTuple(TupleDesc tupleDesc, Datum *values, const bool *isnull, char *data, Size dataSize,
    uint16 *infomask, bits8 *bit)
{
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("fill_tuple for uheap not implemented yet")));
}

Tuple UHeapamTopsModifyTuple(Tuple tuple, TupleDesc tuple_desc,
    Datum* repl_values, const bool* repl_isnull, const bool* do_replace)
{
    return (Tuple)UHeapModifyTuple((UHeapTuple)tuple, tuple_desc, repl_values, repl_isnull, do_replace);
}
Tuple UHeapamTopsOpFusionModifyTuple(Tuple tuple, TupleDesc tuple_desc, Datum* repl_values,
    bool* repl_isnull, UpdateFusion* opf)
{
    Tuple newTuple;
    tableam_tops_deform_tuple(tuple, tuple_desc, repl_values, repl_isnull);
    opf->refreshTargetParameterIfNecessary();
    /*
     * create a new tuple from the values and isnull arrays
     */
    newTuple = tableam_tops_form_tuple(tuple_desc, repl_values, repl_isnull, UHEAP_TUPLE);
    /*
     * copy the identification info of the old tuple: t_ctid, t_self, and OID
     * (if any)
     */
    ((UHeapTuple)newTuple)->ctid = ((UHeapTuple)tuple)->ctid;
    ((UHeapTuple)newTuple)->table_oid = ((UHeapTuple)tuple)->table_oid;
#ifdef PGXC
    ((UHeapTuple)newTuple)->xc_node_id = ((UHeapTuple)tuple)->xc_node_id;
#endif
    Assert(tuple_desc->tdhasoid == false);
    return newTuple;
}


Datum UHeapamTopsTupleGetattr(Tuple tuple, int att_num, TupleDesc tuple_desc, bool *is_null)
{
    return uheap_getattr((UHeapTuple)tuple, att_num, tuple_desc, is_null);
}

Datum UheapamTopsTupleFastGetattr(Tuple tuple, int att_num, TupleDesc tuple_desc, bool *is_null)
{
    return UHeapFastGetAttr((UHeapTuple)tuple, att_num, tuple_desc, is_null);
}

bool UHeapamTopsTupleAttisnull(Tuple tuple, int attnum, TupleDesc tupleDesc)
{
    return UHeapAttIsNull((UHeapTuple)tuple, attnum, tupleDesc);
}

bool UHeapamTopsPageGetItem(Relation rel, Tuple tuple, Page page, OffsetNumber tupleNo, BlockNumber destBlocks)
{
    UHeapDiskTuple uDiskTuple = NULL;
    RowPtr *uTupleItem = NULL;
    uTupleItem = UPageGetRowPtr(page, tupleNo);
    if (!RowPtrHasStorage(uTupleItem)) {
        return false;
    }
    uDiskTuple = (UHeapDiskTuple)UPageGetRowData(page, uTupleItem);
    /* set freeze options for rows in merging file */
    UHeapTupleHeaderSetTDSlot(uDiskTuple, UHEAPTUP_SLOT_FROZEN);

    ((UHeapTuple)tuple)->disk_tuple = uDiskTuple;
    ((UHeapTuple)tuple)->disk_tuple_size = RowPtrGetLen(uTupleItem);
    ((UHeapTuple)tuple)->tupTableType = UHEAP_TUPLE;
    return true;
}

Tuple UHeapamTopsCopyTuple(Tuple tuple)
{
    Assert(TUPLE_IS_UHEAP_TUPLE(UHeapTuple(tuple)));

    return UHeapCopyTuple((UHeapTuple)tuple);
}

Tuple UHeapamTopsNewTuple(Relation relation, ItemPointer tid)
{
    UHeapTuple tuple = uheaptup_alloc(sizeof(UHeapTupleData));
    tuple->disk_tuple = (UHeapDiskTupleData *)palloc0(MaxUHeapTupleSize(relation) - sizeof(UHeapTupleData));
    tuple->tupInfo = 1;
    if (tid != NULL) {
        tuple->ctid = *tid;
    }
    return (Tuple)tuple;
}

TransactionId UHeapamTopsGetConflictXid(Tuple tup)
{
    return UHeapTupleHasMultiLockers(((UHeapTuple)tup)->disk_tuple->flag) ? 
                                    ((UHeapTuple)tup)->t_xid_base : ((UHeapTuple)tup)->t_multi_base;
}

void UHeapamTopsDestroyTuple(Tuple tuple)
{
    UHeapTuple utuple = (UHeapTuple)tuple;
    if (utuple->tupInfo == 1) {
        pfree_ext(utuple->disk_tuple);
    }
    pfree_ext(utuple);
}

void UHeapamTopsAddToBulkInsertSelect(CopyFromBulk bulk, Tuple tup, bool needCopy)
{
    UHeapAddToBulkInsertSelect(bulk, tup, needCopy);
}

void UHeapamTopsAddToBulk(CopyFromBulk bulk, Tuple tup, bool needCopy)
{
    UHeapAddToBulk(bulk, tup, needCopy);
}

void UHeapamTopsUpdateTupleWithOid (Relation rel, Tuple tuple, TupleTableSlot *slot)
{
    /* Update the tuple with table oid */
    if (RelationGetRelid(rel) != InvalidOid)
        ((UHeapTuple)tuple)->table_oid = RelationGetRelid(rel);

    if (slot->tts_tupslotTableAm != TAM_USTORE) {
        /*
            * Global Partition Index stores the partition's tableOid with the index
            * tuple which is extracted from heap tuple of the slot in this case.
            */
            ((HeapTuple)slot->tts_tuple)->t_tableOid = RelationGetRelid(rel);
    }
    ((UHeapTuple)tuple)->xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
}


ItemPointer UHeapamTopsGetTSelf(Tuple tup)
{
    return &(((UHeapTuple)tup)->ctid);
}

void UHeapamTopsExecDeleteIndexTuples(TupleTableSlot *slot, Relation relation, ModifyTableState *node,
    ItemPointer tupleid, ExecIndexTuplesState execIndexTuplesState, Bitmapset *modifiedIdxAttrs)
{
    ExecUHeapDeleteIndexTuplesGuts(slot, relation, node, tupleid, execIndexTuplesState, modifiedIdxAttrs, false);
}

List *UHeapamTopsExecUpdateIndexTuples(TupleTableSlot *slot, TupleTableSlot *oldslot, Relation relation,
    ModifyTableState *node, Tuple tuple, ItemPointer tupleid, ExecIndexTuplesState exec_index_tuples_state,
    int2 bucketid, Bitmapset *modifiedIdxAttrs)
{
    List *recheckIndexes = NULL;
    bool inplaceUpdated = UHeapTupleIsInPlaceUpdated(((UHeapTuple)tuple)->disk_tuple->flag);

    /* If an index column is updated, then we delete the old index entry and insert a new index entry. */
    ExecUHeapDeleteIndexTuplesGuts(oldslot, relation, node, tupleid, exec_index_tuples_state,
        modifiedIdxAttrs, inplaceUpdated);
    recheckIndexes = ExecInsertIndexTuples(slot, &(((UHeapTuple)tuple)->ctid), exec_index_tuples_state.estate,
        exec_index_tuples_state.targetPartRel, exec_index_tuples_state.p, InvalidBktId,
        exec_index_tuples_state.conflict, modifiedIdxAttrs, inplaceUpdated);

    return recheckIndexes;
}

uint32 UHeapamTopsGetTupleType()
{
    return UHEAP_TUPLE;
}

void UHeapamTopsCopyFromInsertBatch(Relation rel, EState* estate, CommandId mycid, int hiOptions,
        ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, BulkInsertState bistate, int nBufferedTuples,
        Tuple* bufferedTuples, Partition partition, int2 bucketId)
{
    UHeapCopyFromInsertBatch(rel, estate, mycid, hiOptions, resultRelInfo, myslot, bistate, nBufferedTuples,
        (UHeapTuple*) bufferedTuples, partition, bucketId);
}

/* -----------------------------------------------------------------------
 * SCAN AM APIS FOR USTORE
 * ------------------------------------------------------------------------
 */
TableScanDesc UHeapamScanBegin(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
    RangeScanInRedis rangeScanInRedis)
{
    return UHeapBeginScan(relation, snapshot, nkeys);
}

TableScanDesc UHeapamScanBeginBm(Relation relation, Snapshot snapshot, int nkeys, ScanKey key)
{
    return UHeapBeginScan(relation, snapshot, nkeys);
}

TableScanDesc UHeapBeginScanSampling(Relation relation, Snapshot snapshot, int nkeys, ScanKey key, 
    bool allow_strat, bool allow_sync, RangeScanInRedis rangeScanInRedis)
{
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("Ustore not support table sampling yet")));
    return NULL;
}

IndexFetchTableData * UHeapamScanIndexFetchBegin(Relation rel)
{
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("not supported in ustore yet")));
    return NULL;
}

void UHeapamScanIndexFetchReset(IndexFetchTableData *scan)
{
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("not suppoted in ustore yet")));
}

void UHeapamScanIndexFetchEnd(IndexFetchTableData *scan)
{
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("not supported in ustore yet")));
}

Tuple UHeapamScanIndexFetchTuple(IndexScanDesc scan, bool *all_dead)
{
    return (Tuple)UHeapamIndexFetchTuple(scan, all_dead);
}

void UHeapamScanRescan(TableScanDesc sscan, ScanKey key)
{
    return UHeapRescan(sscan, key);
}

void UHeapamScanRestrpos(TableScanDesc sscan)
{
    return UHeapRestRpos(sscan);
}

void UHeapamScanMarkpos(TableScanDesc sscan)
{
    return UHeapMarkPos(sscan);
}


void UHeapamScanEndscan(TableScanDesc sscan)
{
    return UHeapEndScan(sscan);
}

/* uheap implementation of the scan_getnexttuple abstract method */
Tuple UHeapamScanGetnexttuple(TableScanDesc sscan, ScanDirection direction)
{
    return (Tuple)UHeapGetNext(sscan, direction);
}

bool UHeapamGetNextBatchMode(TableScanDesc sscan, ScanDirection direction)
{
    /* Note: no locking manipulations needed */
    bool finished = false;
    UHeapScanDesc scan = (UHeapScanDesc)sscan;

    scan->rs_base.rs_ctupRows = 0;
    Assert(ScanDirectionIsForward(direction));
    if (likely(scan->rs_base.rs_pageatatime)) {
        finished = UHeapGetTupPageBatchmode(scan, direction);
    } else {
        ereport(ERROR, (errcode(ERRCODE_RELATION_OPEN_ERROR),
            errmsg("relation %s is temporarily unavalible", RelationGetRelationName(scan->rs_base.rs_rd))));
    }
    return finished;
}

void UHeapamScanGetpage(TableScanDesc sscan, BlockNumber page)
{
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("not supported in ustore yet")));
}

Tuple UHeapamGetNextForVerify(TableScanDesc sscan, ScanDirection direction, bool isValidRelationPage)
{
    return (Tuple)UHeapGetNextForVerify(sscan, direction, isValidRelationPage);
}

void UHeapamTslotStoreUHeapTuple(Tuple tuple, TupleTableSlot *slot, Buffer buffer, bool shouldFree, bool batchMode)
{
    UHeapSlotStoreUHeapTuple((UHeapTuple)tuple, slot, shouldFree, batchMode);
}

/* ------------------------------------------------------------------------
 * DDL AM APIs
 * ------------------------------------------------------------------------
 */

double UHeapamIndexBuildScan(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo, bool allowSync,
    IndexBuildCallback callback, void *callback_state, TableScanDesc scan)
{
    return IndexBuildUHeapScan(heapRelation, indexRelation, indexInfo, allowSync, callback, callback_state, scan);
}

void UHeapamIndexValidateScan (Relation heapRelation, Relation indexRelation,
                             IndexInfo* indexInfo, Snapshot snapshot, v_i_state* state) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("not supported in ustore yet")));
}
    
void UHeapamTupleGetLatestTid(Relation relation, Snapshot snapshot, ItemPointer tid) 
{
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("not supported in ustore yet")));
}

double UHeapamRelationCopyForCluster(Relation oldHeap, Relation oldIndex, Relation newHeap, TransactionId oldestXmin,
    TransactionId freezeXid, bool verbose, bool useSort, AdaptMem *memUsage)
{
    return CopyUHeapDataInternal(oldHeap, oldIndex, newHeap, oldestXmin, freezeXid, verbose, useSort, memUsage);
}


bool UHeapamTupleFetch(Relation relation, Snapshot snapshot, HeapTuple tuple, Buffer *userbuf, bool keepBuf,
    Relation statsRelation)
{
    ItemPointer tid = &tuple->t_self;

    UHeapTupleData uheaptupdata;
    UHeapTuple uheaptup = &uheaptupdata;
    struct {
        UHeapDiskTupleData hdr;
        char data[MaxPossibleUHeapTupleSize];
    } tbuf;

    errno_t errorNo = EOK;
    errorNo = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check(errorNo, "\0", "\0");

    uheaptup->disk_tuple = &tbuf.hdr;
    uheaptup->ctid = *tid;

    if (UHeapFetch(relation, snapshot, tid, uheaptup, userbuf, keepBuf, true)) {
        /* successfully fetched a UHeap tuple, now construct a heap tuple copy out of it */
        TupleDesc tupdesc = relation->rd_att;

        HeapTuple heapTupleTemp = UHeapToHeap(tupdesc, uheaptup);
        heapTupleTemp->t_self = *tid;
        heapTupleTemp->t_tableOid = RelationGetRelid(relation);
        heapTupleTemp->t_xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;

        HeapCopyTupleNoAlloc(tuple, heapTupleTemp);
        pfree(heapTupleTemp);

        return true;
    }

    return false;
}


/* -----------------------------------------------------------------------
 * DML AM APIS FOR USTORE
 * ------------------------------------------------------------------------
 */

Oid UHeapamTupleInsert(Relation relation, Tuple tup, CommandId cid, int options, struct BulkInsertStateData *bistate)
{
    return UHeapInsert(relation, (UHeapTupleData *)tup, cid, bistate);
}

int UHeapamTupleMultiInsert(Relation relation, Relation parent, Tuple *tuples, int ntuples, CommandId cid, int options,
    struct BulkInsertStateData *bistate, HeapMultiInsertExtraArgs *args)
{
    (void)UHeapMultiInsert(relation, (UHeapTuple *)tuples, ntuples, cid, options, bistate);
    return 0;
}

TM_Result UHeapamTupleDelete(Relation relation, ItemPointer tid, CommandId cid, Snapshot crosscheck, Snapshot snapshot,
    bool wait, TupleTableSlot** slot, TM_FailureData *tmfd, bool allow_delete_self)
{
    return UHeapDelete(relation, tid, cid, crosscheck, snapshot, wait, slot, tmfd, false, allow_delete_self);
}

TM_Result UHeapamTupleUpdate(Relation relation, Relation parentRelation, ItemPointer otid, Tuple newtup, CommandId cid,
    Snapshot crosscheck, Snapshot snapshot, bool wait, TupleTableSlot **oldslot, TM_FailureData *tmfd,
    LockTupleMode *mode, bool *update_indexes, Bitmapset **modifiedIdxAttrs, bool allow_update_self,
    bool allow_inplace_update)
{
    TM_Result result = UHeapUpdate(relation, parentRelation, otid, (UHeapTuple)newtup, cid, crosscheck, snapshot, wait,
        oldslot, tmfd, update_indexes, modifiedIdxAttrs, allow_inplace_update);

    /* the LockTupleMode of ustore-updating must be LockTupleExclusive now */
    if (mode != NULL) {
        *mode = LockTupleExclusive;
    }

    return result;
}


TM_Result UHeapamTupleLock(Relation relation, Tuple tuple, Buffer *buffer, CommandId cid, LockTupleMode mode,
    bool nowait, TM_FailureData *tmfd, bool allow_lock_self, bool follow_updates, bool eval, Snapshot snapshot,
    ItemPointer tid, bool isSelectForUpdate, bool isUpsert, TransactionId conflictXid, int waitSec)
{
    return UHeapLockTuple(relation, (UHeapTuple)tuple, buffer, cid, mode, nowait, tmfd, follow_updates, eval, snapshot,
        isSelectForUpdate, allow_lock_self, isUpsert, conflictXid, waitSec);
}

Tuple UHeapamTupleLockUpdated(CommandId cid, Relation relation, int lockmode, ItemPointer tid, TransactionId priorXmax,
    Snapshot snapshot, bool isSelectForUpdate)
{
    return (Tuple)UHeapLockUpdated(cid, relation, (LockTupleMode)lockmode, tid, priorXmax, snapshot, isSelectForUpdate);
}

void UHeapamAbortSpeculative(Relation relation, Tuple tuple)
{
    UHeapAbortSpeculative(relation, (UHeapTuple)tuple);
}

void UHeapamTupleCheckVisible(Snapshot snapshot, Tuple tuple, Buffer buffer)
{
    UHeapTupleCheckVisible(snapshot, (UHeapTuple)tuple, buffer);
}

bool UHeapamTupleSatisfiesSnapshot(Relation relation, HeapTuple tuple, Snapshot snapshot, Buffer buffer)
{
    UHeapTuple utuple = (UHeapTuple)tuple;
    if (tuple->tupTableType == HEAP_TUPLE) {
        utuple = HeapToUHeap(relation->rd_att, (HeapTuple)tuple);
        utuple->ctid = tuple->t_self;
    }

    return UHeapTupleSatisfiesVisibility(utuple, snapshot, buffer);
}

void UheapamTcapPromoteLock(Relation relation, LOCKMODE *lockmode)
{
    /* nothing to do */
}

bool UheapamTcapValidateSnap(Relation relation, Snapshot snap)
{
    /* nothing to do */
    return true;
}

void UheapamTcapDeleteDelta(Relation relation, Snapshot snap)
{
    TvUheapDeleteDelta(RelationGetRelid(relation), snap);
}

void UheapamTcapInsertLost(Relation relation, Snapshot snap)
{
    TvUheapInsertLost(RelationGetRelid(relation), snap);
}

/* All the function is pointer to heap function now, need to abstract the logic and replace with ustore function
 * after. */
const TableAmRoutine g_ustoream_methods = {

    // XXXTAM: Currently heapam* methods are hacked to deal with uheap table methods.
    // separate them out into uheapam* and assign them below to the right am function pointer.
    /* ------------------------------------------------------------------------
     * TABLE SLOT AM APIs
     * ------------------------------------------------------------------------
     */

    tslot_clear : UHeapamTslotClear,
    tslot_materialize : UHeapamTslotMaterialize,
    tslot_get_minimal_tuple : UHeapamTslotGetMinimalTuple,
    tslot_copy_minimal_tuple : UHeapamTslotCopyMinimalTuple,
    tslot_store_minimal_tuple : UHeapamTslotStoreMinimalTuple,
    tslot_get_heap_tuple : UHeapamTslotGetHeapTuple,
    tslot_copy_heap_tuple : UHeapamTslotCopyHeapTuple,
    tslot_store_tuple : UHeapamTslotStoreUHeapTuple,
    tslot_getsomeattrs : UHeapSlotGetSomeAttrs,
    tslot_formbatch :  UHeapamTslotFormBatch,
    tslot_getattr : UHeapamTslotGetattr,
    tslot_getallattrs : UHeapamTslotGetallattrs,
    tslot_attisnull : UHeapamTslotAttisnull,
    tslot_get_tuple_from_slot : uheapam_tslot_get_tuple_from_slot,

    /* ------------------------------------------------------------------------
     * TABLE TUPLE AM APIs
     * ------------------------------------------------------------------------
     */
    tops_getsysattr : UHeapamTopsGetsysattr,
    tops_form_minimal_tuple : UHeapamTopsFormMinimalTuple,
    tops_form_tuple : UHeapamTopsFormTuple,
    tops_form_cmprs_tuple : UHeapamTopsFormCmprsTuple, /* Not implemented yet */
    tops_deform_tuple : UHeapamTopsDeformTuple,
    tops_deform_tuple2 : UHeapamTopsDeformTuple2,
    tops_deform_cmprs_tuple : UHeapamTopsDeformCmprsTuple,        /* Not implemented yet */
    tops_fill_tuple : HeapamTopsFillTuple,                     /* Not implemented yet */
    tops_modify_tuple : UHeapamTopsModifyTuple,
    tops_opfusion_modify_tuple : UHeapamTopsOpFusionModifyTuple,
    tops_tuple_getattr : UHeapamTopsTupleGetattr,
    tops_tuple_fast_getattr : UheapamTopsTupleFastGetattr,
    tops_tuple_attisnull : UHeapamTopsTupleAttisnull,
    tops_copy_tuple : UHeapamTopsCopyTuple,
    tops_new_tuple : UHeapamTopsNewTuple,
    tops_get_conflictXid : UHeapamTopsGetConflictXid,
    tops_destroy_tuple : UHeapamTopsDestroyTuple,
    tops_add_to_bulk_insert_select : UHeapamTopsAddToBulkInsertSelect,
    tops_add_to_bulk : UHeapamTopsAddToBulk,
    tops_update_tuple_with_oid : UHeapamTopsUpdateTupleWithOid,
    tops_get_t_self : UHeapamTopsGetTSelf,
    tops_exec_delete_index_tuples : UHeapamTopsExecDeleteIndexTuples,
    tops_exec_update_index_tuples : UHeapamTopsExecUpdateIndexTuples,
    tops_get_tuple_type : UHeapamTopsGetTupleType,
    tops_copy_from_insert_batch : UHeapamTopsCopyFromInsertBatch,
    tops_page_get_item : UHeapamTopsPageGetItem,
    tops_page_get_max_offsetnumber : UHeapPageGetMaxOffsetNumber,
    tops_page_get_freespace : PageGetUHeapFreeSpace,
    tops_tuple_fetch_row_version : UHeapFetchRowVersion,

    /* -----------------------------------------------------------------------
     * SCAN AM APIS
     * -----------------------------------------------------------------------
     */

    scan_index_fetch_begin : UHeapamScanIndexFetchBegin,
    scan_index_fetch_reset : UHeapamScanIndexFetchReset,
    scan_index_fetch_end : UHeapamScanIndexFetchEnd,
    scan_index_fetch_tuple : UHeapamScanIndexFetchTuple,
    scan_begin : UHeapamScanBegin,
    scan_begin_bm : UHeapamScanBeginBm,
    scan_begin_sampling : UHeapBeginScanSampling,
    scan_rescan : UHeapamScanRescan,
    scan_restrpos : UHeapamScanRestrpos,
    scan_markpos : UHeapamScanMarkpos,

    scan_init_parallel_seqscan : HeapamScanInitParallelSeqscan,
    scan_getnexttuple : UHeapamScanGetnexttuple,
    scan_GetNextBatch : UHeapamGetNextBatchMode,
    scan_getpage : UHeapamScanGetpage,

    scan_gettuple_for_verify : UHeapamGetNextForVerify,
    scan_end : UHeapamScanEndscan,

    tuple_fetch : UHeapamTupleFetch,
    tuple_satisfies_snapshot : UHeapamTupleSatisfiesSnapshot,
    tuple_get_latest_tid : UHeapamTupleGetLatestTid,
    tuple_insert : UHeapamTupleInsert,
    tuple_multi_insert : UHeapamTupleMultiInsert,
    tuple_delete : UHeapamTupleDelete,
    tuple_update : UHeapamTupleUpdate,
    tuple_lock : UHeapamTupleLock,
    tuple_lock_updated : UHeapamTupleLockUpdated,
    tuple_check_visible : UHeapamTupleCheckVisible,
    tuple_abort_speculative : UHeapamAbortSpeculative,


    /* ------------------------------------------------------------------------
     * DDL AM APIs
     * ------------------------------------------------------------------------
     */
    index_build_scan : UHeapamIndexBuildScan,

    index_validate_scan : UHeapamIndexValidateScan,
    relation_copy_for_cluster : UHeapamRelationCopyForCluster,
    
    /* ------------------------------------------------------------------------
     * TIMECAPSULE AM APIs
     * ------------------------------------------------------------------------
     */
    tcap_promote_lock : UheapamTcapPromoteLock,
    tcap_validate_snap : UheapamTcapValidateSnap,
    tcap_delete_delta : UheapamTcapDeleteDelta,
    tcap_insert_lost : UheapamTcapInsertLost
};

const TableAmRoutine * const g_tableam_routines[] = {
    &g_heapam_methods,
    &g_ustoream_methods
};
