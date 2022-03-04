/* -------------------------------------------------------------------------
 *
 * tableam.h
 * openGauss table access method definitions.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tableam.h
 *
 * NOTES
 * See tableam.sgml for higher level documentation.
 *
 * -------------------------------------------------------------------------
 */

#ifndef TABLEAM_H
#define TABLEAM_H

#include "access/hbindex_am.h"
#include "access/hbucket_am.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "catalog/index.h"
#include "executor/executor.h"
#include "opfusion/opfusion_update.h"
#include "optimizer/bucketinfo.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapshot.h"
#include "nodes/execnodes.h"
#include "commands/copy.h"
#include "commands/copypartition.h"

/*
 * * Bitmask values for the flags argument to the scan_begin callback.
 *    */
typedef enum ScanOptions {
    /* one of SO_TYPE_* may be specified */
    SO_TYPE_SEQSCAN = 1 << 0,
    SO_TYPE_BITMAPSCAN = 1 << 1,
    SO_TYPE_SAMPLESCAN = 1 << 2,
    SO_TYPE_ANALYZE = 1 << 3,
    SO_TYPE_TIDSCAN = 1 << 8,

    /* several of SO_ALLOW_* may be specified */
    /* allow or disallow use of access strategy */
    SO_ALLOW_STRAT = 1 << 4,
    /* report location to syncscan logic? */
    SO_ALLOW_SYNC = 1 << 5,
    /* verify visibility page-at-a-time? */
    SO_ALLOW_PAGEMODE = 1 << 6,

    /* unregister snapshot at scan end? */
    SO_TEMP_SNAPSHOT = 1 << 7
} ScanOptions;


/* ------------------------------------------------------------------------
 * Table Access Method Layer
 * ------------------------------------------------------------------------
 */

/* "options" flag bits for tuple_insert */
#define TABLE_INSERT_SKIP_WAL 0x0001
#define TABLE_INSERT_SKIP_FSM 0x0002
#define TABLE_INSERT_FROZEN 0x0004
#define TABLE_INSERT_SPECULATIVE 0x0008

/*
 * Macros on tuple assertions from tuple and typeType
 * and retrieve the index for table type from tuple type.
 */
#define TUPLE_IS_HEAP_TUPLE(tup) (((HeapTuple)tup)->tupTableType == HEAP_TUPLE)
#define TUPLE_IS_UHEAP_TUPLE(tup) (((UHeapTuple)tup)->tupTableType == UHEAP_TUPLE)
#define AssertValidTupleType(tupType) Assert(tupType == HEAP_TUPLE || tupType == UHEAP_TUPLE)
#define AssertValidTuple(tup) Assert(TUPLE_IS_HEAP_TUPLE(tup) || TUPLE_IS_UHEAP_TUPLE(tup))
#define GetTableAMIndex(tupType) tupType >> 1
#define GetTabelAmIndexTuple(tup) GetTableAMIndex(((HeapTuple)tup)->tupTableType)

/*
 * Common interface for table AM.
 * This abstracts away the accessor methods to different kinds of tables.
 */

typedef struct TableAmRoutine {
    /* ------------------------------------------------------------------------
     * TABLE SLOT AM APIs
     * ------------------------------------------------------------------------
     */

    /*
     * Clear the contents of the slot. Only the contents are expected to be
     * cleared and not the tuple descriptor. Typically an implementation of
     * this callback should free the memory allocated for the tuple(minimal/physical/Datum and isnull array)
     * contained in the slot.
     */
    void (*tslot_clear)(TupleTableSlot *slot);

    /*
     * Make the contents of the slot solely depend on the slot(make them a local copy),
     * and not on underlying external resources like another memory context, buffers etc for HeapTable.
     * or return a copy of HeapTuple from slots's content for other Tables.
     *
     * @pram slot: slot to be materialized.
     */
    HeapTuple (*tslot_materialize)(TupleTableSlot *slot);

    /*
     * Return a minimal tuple "owned" by the slot. It is slot's responsibility
     * to free the memory consumed by the minimal tuple. If the slot can not
     * "own" a minimal tuple, it should not implement this callback and should
     * set it as NULL.
     *
     * @param slot: slot from minimal tuple to fetch.
     * @return slot's minimal tuple.
     *
     */
    MinimalTuple (*tslot_get_minimal_tuple)(TupleTableSlot *slot);

    /*
     * Return a copy of minimal tuple representing the contents of the slot.
     * The copy needs to be palloc'd in the current memory context. The slot
     * itself is expected to remain unaffected. It is *not* expected to have
     * meaningful "system columns" in the copy. The copy is not be "owned" by
     * the slot i.e. the caller has to take responsibility to free memory
     * consumed by the slot.
     *
     * @param slot: slot from which minimal tuple to be copied.
     * @return slot's tuple minimal tuple copy
     */
    MinimalTuple (*tslot_copy_minimal_tuple)(TupleTableSlot *slot);

    /*
     * Stores heaps minimal tuple in the TupleTableSlot. Release the current slots buffer and Free's any slot's
     * minimal and heap tuple.
     *
     * @param mtup: minimal tuple to be stored.
     * @param slot: slot to store tuple.
     * @param: should_free true if clear the slot's tuple contents by pfree_ext() during  ExecClearTuple.
     */
    void (*tslot_store_minimal_tuple)(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree);

    /*
     * Returns a heap tuple "owned" by the slot. It is the slot's responsibility to free the memory
     * associated with this tuple. If the slot cannot own the tuple constructed or returned, it should
     * not implement this method, and should return NULL.
     *
     * @param slot: slot from tuple to fetch.
     * @return slot's tuple.
     */
    HeapTuple (*tslot_get_heap_tuple)(TupleTableSlot *slot);


    /*
     * Return a copy of heap tuple representing the contents of the slot. The
     * copy needs to be palloc'd in the current memory context. The slot
     * itself is expected to remain unaffected. It is *not* expected to have
     * meaningful "system columns" in the copy. The copy is not be "owned" by
     * the slot i.e. the caller has to take responsibility to free memory
     * consumed by the slot.
     *
     * @param slot: slot from which tuple to be copied.
     * @return slot's tuple copy
     */
    HeapTuple (*tslot_copy_heap_tuple)(TupleTableSlot *slot);

    /*
     * Stores the physical tuple into a slot.
     *
     * @param tuple: tuple to be stored.
     * @param slot: slot to store tuple.
     * @param: should_free true if clear the slot's tuple contents by pfree_ext() during  ExecClearTuple
     * @return: none
     */
    void (*tslot_store_tuple)(Tuple tuple, TupleTableSlot *slot, Buffer buffer, bool shouldFree, bool batchMode);

    /*
     * Fill up first natts entries of tts_values and tts_isnull arrays with
     * values from the tuple contained in the slot. The function may be called
     * with natts more than the number of attributes available in the tuple,
     * in which case it should set tts_nvalid to the number of returned
     * columns.
     *
     * @param slot:input Tuple Table slot from which attributes are extracted.
     * @param attnum: index until which slots attributes are extracted.
     */
    void (*tslot_getsomeattrs)(TupleTableSlot *slot, int natts);


    void (*tslot_formbatch)(TupleTableSlot* slot, VectorBatch* batch, int cur_rows, int natts);

    /*
     * Fetches a given attribute from the slot's current tuple.
     * attnums beyond the slot's tupdesc's last attribute will be considered NULL
     * even when the physical tuple is longer than the tupdesc.
     *
     * @param slot: TableTuple slot from this attribute is extracted
     * @param attnum: index of the atribute to be extracted.
     * @param isnull: set to true, if the attribute is NULL.
     */
    Datum (*tslot_getattr)(TupleTableSlot *slot, int attnum, bool *isnull);

    /*
     * This function forces all the entries of slot's Datum/isnull array to be valid
     * The caller may then extract data directly
     * from those arrays instead of using getattr.
     *
     * @param slot: TableTuple slot from this attributes are extracted
     */
    void (*tslot_getallattrs)(TupleTableSlot *slot);

    /*
     * Detects if specified attribute is null without actually fetching it.
     *
     * @param slot: Tabletuple slot
     * @para attnum: attribute index that should be checked for null value.
     */
    bool (*tslot_attisnull)(TupleTableSlot *slot, int attnum);

    /*
     * Get tuple from a slot or materialize the tuple.
     *
     * @para attnum: current relation
     * @param slot: Tabletuple slot
     */
    Tuple (*tslot_get_tuple_from_slot)(TupleTableSlot *slot);

    /* ------------------------------------------------------------------------
     * TABLE TUPLE AM APIs
     * ------------------------------------------------------------------------
     */

    /*
     * Returns value of the given system attribute as a datum and sets isnull
     * to false, if it's not NULL. Throws an error if the slot type does not
     * support system attributes.
     */
    Datum (*tops_getsysattr)(Tuple tup, int attnum, TupleDesc tuple_desc, bool *isnull, Buffer buf);

    /*
     * form_minimal_tuple
     * construct a MinimalTuple from the given values[] and isnull[] arrays,
     * which are of the length indicated by tupleDescriptor->natts
     *
     * The result is allocated in the current memory context.
     */
    MinimalTuple (*tops_form_minimal_tuple)(TupleDesc tuple_descriptor, Datum *values, const bool *isnull,
        MinimalTuple in_tuple);


    /*
     * form_tuple
     * construct a tuple from the given values[] and isnull[] arrays,
     * which are of the length indicated by tupleDescriptor->natts
     *
     * The result is allocated in the current memory context.
     */
    Tuple (*tops_form_tuple)(TupleDesc tuple_descriptor, Datum *values, bool *isnull);

    /*
     * form_cmprs_tuple
     * construct a compressed tuple from the given cmprsInfo.
     *
     * The result is allocated in the current memory context.
     */
    HeapTuple (*tops_form_cmprs_tuple)(TupleDesc tuple_descriptor, FormCmprTupleData *cmprs_info);

    /*
     * deform_tuple
     * Given a tuple, extract data into values/isnull arrays; this is
     * the inverse of heap_form_tuple.
     */
    void (*tops_deform_tuple)(Tuple tuple, TupleDesc tuple_desc, Datum *values, bool *isnull);

    void (*tops_deform_tuple2)(Tuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull, Buffer buffer);

    /*
     * deform_cmprs_tuple
     * Given a tuple, extract data into values/isnull arrays; this is
     * the inverse of heap_form_cmprs_tuple.
     */
    void (*tops_deform_cmprs_tuple)(Tuple tuple, TupleDesc tuple_desc, Datum *values, bool *isnull, char *cmprs_info);

    /*
     * fill_tuple
     * Load data portion of a tuple from values/isnull arrays
     */
    void (*tops_fill_tuple)(TupleDesc tuple_desc, Datum *values, const bool *isnull, char *data, Size data_size,
        uint16 *infomask, bits8 *bit);

    /*
     * modify_tuple
     * form a new tuple from an old tuple and a set of replacement values.
     *
     * The result is allocated in the current memory context.
     */
    Tuple (*tops_modify_tuple)(Tuple tuple, TupleDesc tuple_desc, Datum *repl_values, const bool *repl_isnull,
        const bool *do_replace);

    /*
     * opfusion_modify_tuple
     * form a new tuple from an old tuple and a set of replacement values.
     *
     * The interface is used inside opfusion
     */
    Tuple (*tops_opfusion_modify_tuple)(Tuple tuple, TupleDesc tuple_desc,
        Datum* repl_values, bool* repl_isnull, UpdateFusion* opf);


    /*
     * tuple_getattr
     * Fetches the attribute for a given index..
     */
    Datum (*tops_tuple_getattr)(Tuple tuple, int att_num, TupleDesc tuple_desc, bool *is_null);

    Datum (*tops_tuple_fast_getattr)(Tuple tuple, int attnum, TupleDesc tupleDesc, bool *isnull);

    /*
     * tuple_attisnull
     * Fetches if the attribute is null for a given index.
     */
    bool (*tops_tuple_attisnull)(Tuple tup, int attnum, TupleDesc tuple_desc);

    /*
     * tops_copy_tuple
     * Returns a copy of the entire tuple
     */
    Tuple (*tops_copy_tuple)(Tuple tuple);

    /*
     * tops_create_empty_tuple
     * create a empty tuple
     */
    Tuple (*tops_new_tuple)(Relation relation, ItemPointer tid);
    TransactionId (*tops_get_conflictXid)(Tuple tup);
    void (*tops_destroy_tuple)(Tuple tuple);
    void (*tops_add_to_bulk_insert_select)(CopyFromBulk bulk, Tuple tup, bool needCopy);
    void (*tops_add_to_bulk)(CopyFromBulk bulk, Tuple tup, bool needCopy);
    void (*tops_update_tuple_with_oid)(Relation rel, Tuple tuple, TupleTableSlot *slot);
    ItemPointer (*tops_get_t_self)(Tuple tup);
    void (*tops_exec_delete_index_tuples)(TupleTableSlot *slot, Relation relation, ModifyTableState *node,
        ItemPointer tupleid, ExecIndexTuplesState exec_index_tuples_state, Bitmapset *modifiedIdxAttrs);
    List *(*tops_exec_update_index_tuples)(TupleTableSlot *slot, TupleTableSlot *oldslot, Relation relation,
        ModifyTableState *node, Tuple tuple, ItemPointer tupleid, ExecIndexTuplesState exec_index_tuples_state,
        int2 bucketid, Bitmapset *modifiedIdxAttrs);
    uint32 (*tops_get_tuple_type)();
    void (*tops_copy_from_insert_batch)(Relation rel, EState* estate, CommandId mycid, int hiOptions,
        ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, BulkInsertState bistate, int nBufferedTuples,
        Tuple* bufferedTuples, Partition partition, int2 bucketId);
    bool (*tops_page_get_item)(Relation rel, Tuple tuple, Page page, OffsetNumber tupleNo, BlockNumber destBlocks);
    OffsetNumber (*tops_page_get_max_offsetnumber)(Page page);
    Size (*tops_page_get_freespace)(Page page);
    bool (*tops_tuple_fetch_row_version)(TidScanState* node, Relation relation, ItemPointer tid,
        Snapshot snapshot, TupleTableSlot *slot);


    /* ---------------------------------------------------------------------
     * SCAN AM API
     * ---------------------------------------------------------------------
     */

    IndexFetchTableData *(*scan_index_fetch_begin)(Relation rel);

    void (*scan_index_fetch_reset)(IndexFetchTableData *scan);

    void (*scan_index_fetch_end)(IndexFetchTableData *scan);

    Tuple (*scan_index_fetch_tuple)(IndexScanDesc scan, bool *all_dead);

    /*
     * begin relation scan
     */
    TableScanDesc (*scan_begin)(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
        RangeScanInRedis rangeScanInRedis);

    /*
     * begin relation scan for bit map
     */
    TableScanDesc (*scan_begin_bm)(Relation relation, Snapshot snapshot, int nkeys, ScanKey key);

    /*
     * begin relation scan for sampling
     */
    TableScanDesc (*scan_begin_sampling)(Relation relation, Snapshot snapshot, int nkeys, ScanKey key, bool allow_strat,
        bool allow_sync, RangeScanInRedis rangeScanInRedis);

    /*
     * Re scan
     */
    void (*scan_rescan)(TableScanDesc sscan, ScanKey key);

    /*
     * Restore scan position
     */
    void (*scan_restrpos)(TableScanDesc sscan);

    /*
     * Mark scan position
     */
    void (*scan_markpos)(TableScanDesc sscan);

    /*
     * init parallel seq scan
     */
    void (*scan_init_parallel_seqscan)(TableScanDesc sscan, int32 dop, ScanDirection dir);

    /*
     * Get next tuple
     * Will return a Generic "Tuple" type
     */
    Tuple (*scan_getnexttuple)(TableScanDesc sscan, ScanDirection direction);

    bool (*scan_GetNextBatch)(TableScanDesc scan, ScanDirection direction);

    /*
     * Get next page
     */
    void (*scan_getpage)(TableScanDesc sscan, BlockNumber page);

    /*
     * Get next page
     */
    Tuple (*scan_gettuple_for_verify)(TableScanDesc sscan, ScanDirection direction, bool isValidRelationPage);

    /*
     * end relation scan
     */
    void (*scan_end)(TableScanDesc sscan);


    /* ------------------------------------------------------------------------
     * DQL AM APIs
     * ------------------------------------------------------------------------
     */

    bool (*tuple_fetch)(Relation relation, Snapshot snapshot, HeapTuple tuple, Buffer *userbuf, bool keep_buf,
        Relation stats_relation);

    bool (*tuple_satisfies_snapshot)(Relation relation, HeapTuple tuple, Snapshot snapshot, Buffer buffer);

    void (*tuple_get_latest_tid)(Relation relation, Snapshot snapshot, ItemPointer tid);

    /* ------------------------------------------------------------------------
     * DML AM APIs
     * ------------------------------------------------------------------------
     */
    Oid (*tuple_insert)(Relation relation, Tuple tup, CommandId cid, int options, struct BulkInsertStateData *bistate);

    int (*tuple_multi_insert)(Relation relation, Relation parent, Tuple *tuples, int ntuples, CommandId cid,
        int options, struct BulkInsertStateData *bistate, HeapMultiInsertExtraArgs *args);

    TM_Result (*tuple_delete)(Relation relation, ItemPointer tid, CommandId cid, Snapshot crosscheck, Snapshot snapshot,
        bool wait, TupleTableSlot **oldslot, TM_FailureData *tmfd, bool allow_delete_self);

    TM_Result (*tuple_update)(Relation relation, Relation parentRelation, ItemPointer otid, Tuple newtup, CommandId cid,
        Snapshot crosscheck, Snapshot snapshot, bool wait, TupleTableSlot **oldslot, TM_FailureData *tmfd,
        LockTupleMode *mode, bool *update_indexes, Bitmapset **modifiedIdxAttrs, bool allow_update_self,
        bool allow_inplace_update);

    TM_Result (*tuple_lock)(Relation relation, Tuple tuple, Buffer *buffer, CommandId cid, LockTupleMode mode,
        bool nowait, TM_FailureData *tmfd, bool allow_lock_self, bool follow_updates, bool eval, Snapshot snapshot,
        ItemPointer tid, bool isSelectForUpdate, bool isUpsert, TransactionId conflictXid,
        int waitSec);

    Tuple (*tuple_lock_updated)(CommandId cid, Relation relation, int lockmode, ItemPointer tid,
        TransactionId priorXmax, Snapshot snapshot, bool isSelectForUpdate);

    void (*tuple_check_visible)(Snapshot snapshot, Tuple tuple, Buffer buffer);

    void (*tuple_abort_speculative)(Relation relation, Tuple tuple);

    /* ------------------------------------------------------------------------
     * DDL AM APIs
     * ------------------------------------------------------------------------
     */

    double (*index_build_scan)(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo, bool allow_sync,
        IndexBuildCallback callback, void *callback_state, TableScanDesc scan);

    void (*index_validate_scan)(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo, Snapshot snapshot,
        v_i_state *state);

    double (*relation_copy_for_cluster)(Relation OldHeap, Relation OldIndex, Relation NewHeap, TransactionId OldestXmin,
        TransactionId FreezeXid, bool verbose, bool use_sort, AdaptMem *memUsage);

    /* ------------------------------------------------------------------------
     * TIMECAPSULE AM APIs
     * ------------------------------------------------------------------------
     */
    void (*tcap_promote_lock)(Relation relation, LOCKMODE *lockmode);
    bool (*tcap_validate_snap)(Relation relation, Snapshot snap);
    void (*tcap_delete_delta)(Relation relation, Snapshot snap);
    void (*tcap_insert_lost)(Relation relation, Snapshot snap);
} TableAmRoutine;

extern const TableAmRoutine * const g_tableam_routines[];
extern void HeapamScanIndexFetchEnd(IndexFetchTableData *scan);
extern void heapam_index_fetch_reset(IndexFetchTableData *scan);
extern IndexFetchTableData *HeapamScanIndexFetchBegin(Relation rel);

extern const TableAmRoutine *GetTableAmRoutine(TableAmType type);
extern void tableam_tslot_clear(TupleTableSlot *slot);
extern HeapTuple tableam_tslot_materialize(TupleTableSlot *slot);
extern MinimalTuple tableam_tslot_get_minimal_tuple(TupleTableSlot *slot);
extern MinimalTuple tableam_tslot_copy_minimal_tuple(TupleTableSlot *slot);
extern void tableam_tslot_store_minimal_tuple(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree);
extern HeapTuple tableam_tslot_get_heap_tuple(TupleTableSlot *slot);
extern HeapTuple tableam_tslot_copy_heap_tuple(TupleTableSlot *slot);
extern void tableam_tslot_store_tuple(Tuple tuple, TupleTableSlot *slot, Buffer buffer, bool shouldFree, bool batchMode);
extern void tableam_tslot_getsomeattrs(TupleTableSlot *slot, int natts);
extern Datum tableam_tslot_getattr(TupleTableSlot *slot, int attnum, bool *isnull);
extern void tableam_tslot_getallattrs(TupleTableSlot *slot);
extern void tableam_tslot_formbatch(TupleTableSlot* slot, VectorBatch* batch, int cur_rows,  int natts);
extern bool tableam_tslot_attisnull(TupleTableSlot *slot, int attnum);
extern Tuple tableam_tslot_get_tuple_from_slot(Relation relation, TupleTableSlot *slot);
extern Datum tableam_tops_getsysattr(Tuple tup, int attnum, TupleDesc tuple_desc, bool *isnull,
    Buffer buf = InvalidBuffer);
extern MinimalTuple tableam_tops_form_minimal_tuple(TupleDesc tuple_descriptor, Datum *values,
    const bool *isnull, MinimalTuple in_tuple, uint32 tupTableType);
extern Tuple tableam_tops_form_tuple(TupleDesc tuple_descriptor, Datum *values, bool *isnull,
    uint32 tupTableType);
extern Tuple tableam_tops_form_cmprs_tuple(TupleDesc tuple_descriptor, FormCmprTupleData *cmprs_info,
    uint32 tupTableType);
extern void tableam_tops_deform_tuple(Tuple tuple, TupleDesc tuple_desc, Datum *values, bool *isnull);
extern void tableam_tops_deform_tuple2(Tuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull, Buffer buffer);
extern void tableam_tops_deform_cmprs_tuple(Tuple tuple, TupleDesc tuple_desc, Datum *values, bool *isnull,
    char *cmprs_info);
extern void tableam_tops_fill_tuple(TupleDesc tuple_desc, Datum *values, const bool *isnull, char *data,
    Size data_size, uint16 *infomask, bits8 *bit);
extern Tuple tableam_tops_modify_tuple(Tuple tuple, TupleDesc tuple_desc, Datum *repl_values,
    const bool *repl_isnull, const bool *do_replace);
extern Tuple tableam_tops_opfusion_modify_tuple(Tuple tuple, TupleDesc tuple_desc,
    Datum* repl_values, bool* repl_isnull, UpdateFusion* opf);

#define tableam_tops_free_tuple(tup) \
    do {                                    \
        if ((tup) != NULL) {                \
            if (((UHeapTuple)tup)->tupTableType == UHEAP_TUPLE) { \
                UHeapFreeTuple(tup);                  \
            } else {                                               \
                heap_freetuple_ext(tup);                          \
            }                                                      \
        }                                                          \
    } while (0)

extern Datum tableam_tops_tuple_getattr(Tuple tuple, int att_num, TupleDesc tuple_desc, bool *is_null);
extern Datum tableam_tops_tuple_fast_getattr(Tuple tuple, int att_num, TupleDesc tuple_desc, bool *is_null);
extern bool tableam_tops_tuple_attisnull(Tuple tuple, int attnum, TupleDesc tuple_desc);
extern Tuple tableam_tops_copy_tuple(Tuple tuple);
extern MinimalTuple tableam_tops_copy_minimal_tuple(MinimalTuple mtup);
extern void tableam_tops_free_minimal_tuple(MinimalTuple mtup);
extern Tuple tableam_tops_new_tuple(Relation relation, ItemPointer tid);
extern TransactionId tableam_tops_get_conflictXid(Relation relation, Tuple tup);
extern void tableam_tops_destroy_tuple(Relation relation, Tuple tuple);
extern void tableam_tops_add_to_bulk_insert_select(Relation relation, CopyFromBulk bulk, Tuple tup,
    bool needCopy);
extern void tableam_tops_add_to_bulk(Relation relation,
                                                CopyFromBulk bulk, Tuple tup, bool needCopy);
extern ItemPointer tableam_tops_get_t_self(Relation relation, Tuple tup);
extern void tableam_tops_exec_delete_index_tuples(TupleTableSlot *slot, Relation relation,
    ModifyTableState *node, ItemPointer tupleid, ExecIndexTuplesState exec_index_tuples_state,
    Bitmapset *modifiedIdxAttrs);
extern List *tableam_tops_exec_update_index_tuples(TupleTableSlot *slot, TupleTableSlot *oldslot,
    Relation relation, ModifyTableState *node, Tuple tuple, ItemPointer tupleid,
    ExecIndexTuplesState exec_index_tuples_state, int2 bucketid, Bitmapset *modifiedIdxAttrs);
extern uint32 tableam_tops_get_tuple_type(Relation relation);
extern void tableam_tops_copy_from_insert_batch(Relation rel, EState* estate, CommandId mycid, int hiOptions,
        ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, BulkInsertState bistate, int nBufferedTuples,
        Tuple* bufferedTuples, Partition partition, int2 bucketId);
extern bool tableam_tops_page_get_item(Relation rel, Tuple tuple, Page page,
    OffsetNumber tupleNo, BlockNumber destBlocks);
extern OffsetNumber tableam_tops_page_get_max_offsetnumber(Relation rel, Page page);
extern Size tableam_tops_page_get_freespace(Relation rel, Page page);
extern bool tableam_tops_tuple_fetch_row_version(TidScanState* node, Relation relation, ItemPointer tid,
        Snapshot snapshot, TupleTableSlot *slot);
extern void tableam_tops_update_tuple_with_oid(Relation relation, Tuple tup, TupleTableSlot *slot);
extern bool tableam_tuple_fetch(Relation relation, Snapshot snapshot, HeapTuple tuple, Buffer *userbuf,
    bool keep_buf, Relation stats_relation);
extern bool tableam_tuple_satisfies_snapshot(Relation relation, HeapTuple tuple, Snapshot snapshot,
    Buffer buffer);
extern void tableam_tuple_get_latest_tid(Relation relation, Snapshot snapshot, ItemPointer tid);
extern Oid tableam_tuple_insert(Relation relation, Tuple tup, CommandId cid, int options,
    struct BulkInsertStateData *bistate);
extern int tableam_tuple_multi_insert(Relation relation, Relation parent, Tuple *tuples, int ntuples,
    CommandId cid, int options, struct BulkInsertStateData *bistate, HeapMultiInsertExtraArgs *args);
extern TM_Result tableam_tuple_delete(Relation relation, ItemPointer tid, CommandId cid, Snapshot crosscheck,
    Snapshot snapshot, bool wait, TupleTableSlot **oldslot, TM_FailureData *tmfd, bool allow_delete_self = false);
extern TM_Result tableam_tuple_update(Relation relation, Relation parentRelation, ItemPointer otid, Tuple newtup,
    CommandId cid, Snapshot crosscheck, Snapshot snapshot, bool wait, TupleTableSlot **oldslot, TM_FailureData *tmfd,
    bool *update_indexes, Bitmapset **modifiedIdxAttrs, bool allow_update_self = false,
    bool allow_inplace_update = true, LockTupleMode *lockmode = NULL);
extern TM_Result tableam_tuple_lock(Relation relation, Tuple tuple, Buffer *buffer, CommandId cid,
    LockTupleMode mode, bool nowait, TM_FailureData *tmfd, bool allow_lock_self, bool follow_updates, bool eval,
    Snapshot snapshot, ItemPointer tid, bool isSelectForUpdate, bool isUpsert = false, 
    TransactionId conflictXid = InvalidTransactionId, int waitSec = 0);
extern Tuple tableam_tuple_lock_updated(CommandId cid, Relation relation, int lockmode, ItemPointer tid,
    TransactionId priorXmax, Snapshot snapshot = NULL, bool isSelectForUpdate = false);
extern void tableam_tuple_check_visible(Relation relation, Snapshot snapshot, Tuple tuple, Buffer buffer);
extern void tableam_tuple_abort_speculative(Relation relation, Tuple tuple);

/* -----------------------------------------------------------------------
 * SCAN AM APIS FOR HEAP
 * ------------------------------------------------------------------------
 */

extern IndexFetchTableData *tableam_scan_index_fetch_begin(Relation rel);
extern void tableam_scan_index_fetch_reset(IndexFetchTableData *scan);
extern void tableam_scan_index_fetch_end(IndexFetchTableData *scan);
extern Tuple tableam_scan_index_fetch_tuple(IndexScanDesc scan, bool *all_dead);
extern TableScanDesc tableam_scan_begin(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
    RangeScanInRedis rangeScanInRedis = { false, 0, 0 });
extern TableScanDesc tableam_scan_begin_bm(Relation relation, Snapshot snapshot, int nkeys, ScanKey key);
extern TableScanDesc tableam_scan_begin_sampling(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
    bool allow_strat, bool allow_sync, RangeScanInRedis rangeScanInRedis = { false, 0, 0 });
extern Tuple tableam_scan_getnexttuple(TableScanDesc sscan, ScanDirection direction);
extern bool tableam_scan_gettuplebatchmode(TableScanDesc sscan, ScanDirection direction);
extern void tableam_scan_getpage(TableScanDesc sscan, BlockNumber page);
extern Tuple tableam_scan_gettuple_for_verify(TableScanDesc sscan, ScanDirection direction, bool isValidRelationPage);
extern void tableam_scan_end(TableScanDesc sscan);
extern void tableam_scan_rescan(TableScanDesc sscan, ScanKey key);
extern void tableam_scan_restrpos(TableScanDesc sscan);
extern void tableam_scan_markpos(TableScanDesc sscan);
extern void tableam_scan_init_parallel_seqscan(TableScanDesc sscan, int32 dop, ScanDirection dir);
extern double tableam_index_build_scan(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
    bool allow_sync, IndexBuildCallback callback, void *callback_state, TableScanDesc scan);
extern void tableam_index_validate_scan(Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
    Snapshot snapshot, v_i_state *state);
extern double tableam_relation_copy_for_cluster(Relation OldHeap, Relation OldIndex, Relation NewHeap,
    TransactionId OldestXmin, TransactionId FreezeXid, bool verbose, bool use_sort, AdaptMem *memUsage);
static inline void tableam_tcap_promote_lock(Relation relation, LOCKMODE *lockmode)
{
    return g_tableam_routines[relation->rd_tam_type]->tcap_promote_lock(relation, lockmode);
}

static inline bool tableam_tcap_validate_snap(Relation relation, Snapshot snap)
{
    return g_tableam_routines[relation->rd_tam_type]->tcap_validate_snap(relation, snap);
}

static inline void tableam_tcap_delete_delta(Relation relation, Snapshot snap)
{
    return g_tableam_routines[relation->rd_tam_type]->tcap_delete_delta(relation, snap);
}

static inline void tableam_tcap_insert_lost(Relation relation, Snapshot snap)
{
    return g_tableam_routines[relation->rd_tam_type]->tcap_insert_lost(relation, snap);
}

extern TM_Result HeapamTupleUpdate(Relation relation, Relation parentRelation, ItemPointer otid, Tuple newtup,
    CommandId cid, Snapshot crosscheck, Snapshot snapshot, bool wait, TM_FailureData *tmfd, bool *update_indexes,
    Bitmapset **modifiedIdxAttrs, bool allow_update_self = false, bool allow_inplace_update = true);

#endif /* TABLEAM_H */
