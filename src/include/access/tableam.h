/*-------------------------------------------------------------------------
 *
 * tableam.h
 *    POSTGRES table access method definitions.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tableam.h
 *
 * NOTES
 *      See tableam.sgml for higher level documentation.
 *
 *-------------------------------------------------------------------------
 */

#ifndef TABLEAM_H
#define TABLEAM_H

#include "access/hbindex_am.h"
#include "access/hbucket_am.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "catalog/index.h"
#include "executor/executor.h"
#include "optimizer/bucketinfo.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapshot.h"
#include "nodes/execnodes.h"

/*
 *  * Bitmask values for the flags argument to the scan_begin callback.
 *   */
typedef enum ScanOptions
{
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

extern RangeScanInRedis reset_scan_qual(Relation currHeapRel, ScanState * node);

/*
 * Macros on tuple assertions from tuple and typeType
 * and retrieve the index for table type from tuple type.
 */
#define TUPLE_IS_HEAP_TUPLE(tup) (((HeapTuple)tup)->tupTableType == HEAP_TUPLE)
#define AssertValidTupleType(tupType) Assert(tupType == HEAP_TUPLE || tupType == UHEAP_TUPLE)
#define AssertValidTuple(tup) Assert(TUPLE_IS_HEAP_TUPLE(tup))
#define GetTableAMIndex(tupType) tupType >> 1
#define GetTabelAmIndexTuple(tup) GetTableAMIndex(((HeapTuple)tup)->tupTableType)

/*
 * Common interface for table AM.
 * This abstracts away the accessor methods to different kinds of tables.
 */

typedef struct TableAmRoutine
{
    /* ------------------------------------------------------------------------
     * TABLE SLOT AM APIs
     * ------------------------------------------------------------------------
     */

    /*
     * Clear the contents of the slot. Only the contents are expected to be
     * cleared and not the tuple descriptor. Typically an implementation of
     * this callback should free the memory allocated for the tuple(minimal/physical/Datum and isnull array)
     *contained in the slot.
     */
    void (*tslot_clear) (TupleTableSlot *slot);

    /*
     * Make the contents of the slot solely depend on the slot(make them a local copy),
     *  and not on underlying external resources like another memory context, buffers etc for HeapTable.
     *  or return a copy of HeapTuple from slots's content for other Tables.
     *
     * @pram slot: slot to be materialized.
     */
    HeapTuple (*tslot_materialize) (TupleTableSlot *slot);

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
    MinimalTuple (*tslot_get_minimal_tuple) (TupleTableSlot *slot);

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
    MinimalTuple (*tslot_copy_minimal_tuple) (TupleTableSlot *slot);

    /*
     * Stores heaps minimal tuple in the TupleTableSlot. Release the current slots buffer and Free's any slot's
     * minimal and heap tuple.
     *
     * @param mtup: minimal tuple to be stored.
     * @param slot: slot to store tuple.
     * @param: should_free true if clear the slot's tuple contents by pfree_ext() during  ExecClearTuple.
     */
    void (*tslot_store_minimal_tuple) (MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree);

    /*
     * Returns a heap tuple "owned" by the slot. It is the slot's responsibility to free the memory
     * associated with this tuple. If the slot cannot own the tuple constructed or returned, it should
     * not implement this method, and should return NULL.
     *
     * @param slot: slot from tuple to fetch.
     * @return slot's tuple.
     */
    HeapTuple (*tslot_get_heap_tuple) (TupleTableSlot* slot);


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
    HeapTuple (*tslot_copy_heap_tuple) (TupleTableSlot *slot);

    /*
     * Stores the physical tuple into a slot.
     *
     * @param tuple: tuple to be stored.
     * @param slot: slot to store tuple.
     * @param: should_free true if clear the slot's tuple contents by pfree_ext() during  ExecClearTuple
     * @return: none
     */
    void (*tslot_store_tuple)(Tuple tuple, TupleTableSlot* slot, Buffer buffer, bool should_free);

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
    void (*tslot_getsomeattrs) (TupleTableSlot *slot, int natts);

    /*
     * Fetches a given attribute from the slot's current tuple.
     *  attnums beyond the slot's tupdesc's last attribute will be considered NULL
     *  even when the physical tuple is longer than the tupdesc.
     *
     *  @param slot: TableTuple slot from this attribute is extracted
     *  @param attnum: index of the atribute to be extracted.
     *  @param isnull: set to true, if the attribute is NULL.
     */
    Datum (*tslot_getattr)(TupleTableSlot* slot, int attnum, bool* isnull);

    /*
     * This function forces all the entries of slot's Datum/isnull array to be valid
     * The caller may then extract data directly
     *  from those arrays instead of using getattr.
     *
     *  @param slot: TableTuple slot from this attributes are extracted
     */
    void (*tslot_getallattrs)(TupleTableSlot* slot);

    /*
     * Detects if specified attribute is null without actually fetching it.
     *
     * @param slot: Tabletuple slot
     * @para attnum: attribute index that should be checked for null value.
     */
    bool (*tslot_attisnull)(TupleTableSlot* slot, int attnum);

    /* ------------------------------------------------------------------------
     * TABLE TUPLE AM APIs
     * ------------------------------------------------------------------------
     */

    /*
     * Returns value of the given system attribute as a datum and sets isnull
     * to false, if it's not NULL. Throws an error if the slot type does not
     * support system attributes.
     */
    Datum (*tops_getsysattr) (Tuple tup, int attnum, TupleDesc tuple_desc, bool* isnull, Buffer buf);

    /*
     * form_minimal_tuple
     *      construct a MinimalTuple from the given values[] and isnull[] arrays,
     *      which are of the length indicated by tupleDescriptor->natts
     *
     * The result is allocated in the current memory context.
     */
    MinimalTuple (*tops_form_minimal_tuple)(TupleDesc tuple_descriptor, Datum* values, const bool* isnull, MinimalTuple in_tuple);


    /*
     * form_tuple
     *      construct a tuple from the given values[] and isnull[] arrays,
     *      which are of the length indicated by tupleDescriptor->natts
     *
     * The result is allocated in the current memory context.
     */
    Tuple (*tops_form_tuple)(TupleDesc tuple_descriptor, Datum* values, bool* isnull);

    /*
     * form_cmprs_tuple
     *      construct a compressed tuple from the given cmprsInfo.
     *
     * The result is allocated in the current memory context.
     */
    HeapTuple (*tops_form_cmprs_tuple)(TupleDesc tuple_descriptor, FormCmprTupleData* cmprs_info);

    /*
     * deform_tuple
     *      Given a tuple, extract data into values/isnull arrays; this is
     *      the inverse of heap_form_tuple.
     */
    void (*tops_deform_tuple)(Tuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull);

    /*
     * deform_cmprs_tuple
     *      Given a tuple, extract data into values/isnull arrays; this is
     *      the inverse of heap_form_cmprs_tuple.
     */
    void (*tops_deform_cmprs_tuple)(Tuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull, char* cmprs_info);

    /*
     * compute_data_size
     *      Determine size of the data area of a tuple to be constructed
     */
    Size (*tops_computedatasize_tuple)(TupleDesc tuple_desc, Datum* values, const bool* isnull, uint32 hoff);

    /*
     * fill_tuple
     *      Load data portion of a tuple from values/isnull arrays
     */
    void (*tops_fill_tuple)(TupleDesc tuple_desc, Datum* values, const bool* isnull, char* data, Size data_size, uint16* infomask, bits8* bit);

    /*
     * modify_tuple
     *      form a new tuple from an old tuple and a set of replacement values.
     *
     * The result is allocated in the current memory context.
     */
    Tuple (*tops_modify_tuple)(Tuple tuple, TupleDesc tuple_desc, Datum* repl_values, const bool* repl_isnull, const bool* do_replace);

    /*
     *  tuple_getattr
     *      Fetches the attribute for a given index..
     */
    Datum (*tops_tuple_getattr)(Tuple tuple, int att_num, TupleDesc tuple_desc, bool* is_null);

    /*
     * tuple_attisnull
     *      Fetches if the attribute is null for a given index.
     */
    bool (*tops_tuple_attisnull)(Tuple tup, int attnum, TupleDesc tuple_desc);

    /*
     * tops_copy_tuple
     *   Returns a copy of the entire tuple
     */
    Tuple (*tops_copy_tuple) (Tuple tuple);

    /* ---------------------------------------------------------------------
     * SCAN AM API
     * ---------------------------------------------------------------------
     */       

    IndexFetchTableData * (*scan_index_fetch_begin) (Relation rel);

    void (*scan_index_fetch_reset) (IndexFetchTableData *scan);

    void (*scan_index_fetch_end) (IndexFetchTableData *scan);

    HeapTuple (*scan_index_fetch_tuple) (IndexScanDesc scan, bool *all_dead);

    /*
     * begin relation scan
     */
    TableScanDesc (*scan_begin) (Relation relation, Snapshot snapshot, int nkeys, ScanKey key, RangeScanInRedis rangeScanInRedis);

    /*
     * begin relation scan for bit map
     */
    TableScanDesc (*scan_begin_bm) (Relation relation, Snapshot snapshot, int nkeys, ScanKey key);

    /*
     * begin relation scan for sampling
     */
    TableScanDesc (*scan_begin_sampling) (Relation relation, Snapshot snapshot, int nkeys, ScanKey key, bool allow_strat, bool allow_sync, RangeScanInRedis rangeScanInRedis);

    /*
     * Re scan
     */
    void (*scan_rescan) (TableScanDesc sscan, ScanKey key);

    /*
     * Restore scan position
     */
    void (*scan_restrpos) (TableScanDesc sscan);

    /*
     * Mark scan position
     */     
    void (*scan_markpos) (TableScanDesc sscan);
    
    /*
     * init parallel seq scan
     */
    void (*scan_init_parallel_seqscan) (TableScanDesc sscan, int32 dop, ScanDirection dir);

    /*
     * Get next tuple
     * Will return a Generic "Tuple" type
     */
    Tuple (*scan_getnexttuple) (TableScanDesc sscan, ScanDirection direction);
    
    /*
     * Get next page
     */
    void (*scan_getpage) (TableScanDesc sscan, BlockNumber page);

    /*
     * end relation scan
     */
    void (*scan_end) (TableScanDesc sscan);


    /* ------------------------------------------------------------------------
     * DQL AM APIs
     * ------------------------------------------------------------------------
     */

    bool (*tuple_fetch) (Relation relation, Snapshot snapshot, HeapTuple tuple, 
                         Buffer *userbuf, bool keep_buf, Relation stats_relation);

    bool (*tuple_satisfies_snapshot) (Relation relation, HeapTuple tuple, 
                                      Snapshot snapshot, Buffer buffer);

    void (*tuple_get_latest_tid) (Relation relation, Snapshot snapshot, 
                                  ItemPointer tid);

    /* ------------------------------------------------------------------------
     * DML AM APIs
     * ------------------------------------------------------------------------
     */
    Oid (*tuple_insert) (Relation relation, Tuple tup, CommandId cid,
                         int options, struct BulkInsertStateData *bistate);

    int (*tuple_multi_insert) (Relation relation, Relation parent,
                               Tuple* tuples, int ntuples, CommandId cid,
                               int options, struct BulkInsertStateData *bistate,
                               HeapMultiInsertExtraArgs *args);

    TM_Result (*tuple_delete) (Relation relation, ItemPointer tid, CommandId cid,
                                 Snapshot crosscheck, Snapshot snapshot, bool wait, 
                                 TM_FailureData *tmfd, bool allow_delete_self);

    TM_Result (*tuple_update) (Relation relation, Relation parentRelation, 
                                 ItemPointer otid, Tuple newtup, CommandId cid, 
                                 Snapshot crosscheck, Snapshot snapshot, bool wait, 
                                 TM_FailureData *tmfd, bool *update_indexes, bool allow_update_self);

    TM_Result (*tuple_lock) (Relation relation, Tuple tuple, Buffer *buffer, 
                             CommandId cid, LockTupleMode mode, bool nowait, TM_FailureData *tmfd,
                             bool allow_lock_self, bool follow_updates, bool eval, Snapshot snapshot,
                             ItemPointer tid, bool isSelectForUpdate);

    HeapTuple (*tuple_lock_updated)(CommandId cid, Relation relation, 
                               int lockmode, ItemPointer tid, TransactionId priorXmax);


    /* ------------------------------------------------------------------------
     * DDL AM APIs
     * ------------------------------------------------------------------------
     */

    double (*index_build_scan) (Relation heapRelation, Relation indexRelation, 
                                IndexInfo* indexInfo, bool allow_sync, 
                                IndexBuildCallback callback, void* callback_state);

    void (*index_validate_scan) (Relation heapRelation, Relation indexRelation, 
                                 IndexInfo* indexInfo, Snapshot snapshot, v_i_state* state);

    double (*relation_copy_for_cluster) (Relation OldHeap, Relation OldIndex, 
                                         Relation NewHeap, TransactionId OldestXmin, 
                                         TransactionId FreezeXid, bool verbose, 
                                         bool use_sort, AdaptMem* memUsage,
                                         TransactionId* ptrFreezeXid);

} TableAmRoutine;

extern const TableAmRoutine * const g_tableam_routines[];

static inline const TableAmRoutine *GetTableAmRoutine(TableAmType type) {
    return g_tableam_routines[type];
}

/*
 * Clears the contents of the table slot that contains heap table tuple data.
 */
static inline void tableam_tslot_clear(TupleTableSlot *slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_clear(slot);
}

static inline HeapTuple tableam_tslot_materialize(TupleTableSlot *slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_materialize(slot);
}

static inline MinimalTuple tableam_tslot_get_minimal_tuple(TupleTableSlot *slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_get_minimal_tuple(slot);
}


static inline MinimalTuple tableam_tslot_copy_minimal_tuple(TupleTableSlot *slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_copy_minimal_tuple(slot);
}

static inline void tableam_tslot_store_minimal_tuple(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree)
{
    g_tableam_routines[slot->tts_tupslotTableAm]->tslot_store_minimal_tuple(mtup, slot, shouldFree);
}

static inline HeapTuple tableam_tslot_get_heap_tuple(TupleTableSlot* slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_get_heap_tuple(slot);
}

static inline HeapTuple tableam_tslot_copy_heap_tuple(TupleTableSlot *slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_copy_heap_tuple(slot);
}

static inline void tableam_tslot_store_tuple(Tuple tuple, TupleTableSlot* slot, Buffer buffer, bool should_free)
{
    g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tslot_store_tuple(tuple, slot, buffer, should_free);
}


static inline void tableam_tslot_getsomeattrs(TupleTableSlot *slot, int natts)
{
     g_tableam_routines[slot->tts_tupslotTableAm]->tslot_getsomeattrs(slot, natts);
}

static inline Datum tableam_tslot_getattr(TupleTableSlot* slot, int attnum, bool* isnull)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_getattr(slot, attnum, isnull);
}

static inline void tableam_tslot_getallattrs(TupleTableSlot* slot)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_getallattrs(slot);
}

static inline bool tableam_tslot_attisnull(TupleTableSlot* slot, int attnum)
{
    return g_tableam_routines[slot->tts_tupslotTableAm]->tslot_attisnull(slot, attnum);
}

/* ------------------------------------------------------------------------
 * TABLE TUPLE AM APIs
 * ------------------------------------------------------------------------
 */

static inline Datum tableam_tops_getsysattr(Tuple tup, int attnum, TupleDesc tuple_desc, bool* isnull, Buffer buf = InvalidBuffer) {
    AssertValidTuple(tup);
    return g_tableam_routines[GetTabelAmIndexTuple(tup)]->tops_getsysattr(tup, attnum, tuple_desc, isnull, buf);
}

static inline MinimalTuple tableam_tops_form_minimal_tuple(
    TupleDesc tuple_descriptor, Datum* values, const bool* isnull, MinimalTuple in_tuple, uint32 tupTableType)
{
    AssertValidTupleType(tupTableType);
    return g_tableam_routines[GetTableAMIndex(tupTableType)]->tops_form_minimal_tuple(tuple_descriptor, values, isnull, in_tuple);
}

static inline Tuple tableam_tops_form_tuple(TupleDesc tuple_descriptor, Datum* values, bool* isnull, uint32 tupTableType)
{
    AssertValidTupleType(tupTableType);
    return g_tableam_routines[GetTableAMIndex(tupTableType)]->tops_form_tuple(tuple_descriptor, values, isnull);
}

static inline Tuple tableam_tops_form_cmprs_tuple(TupleDesc tuple_descriptor, FormCmprTupleData* cmprs_info, uint32 tupTableType)
{
    AssertValidTupleType(tupTableType);
    return g_tableam_routines[GetTableAMIndex(tupTableType)]->tops_form_cmprs_tuple(tuple_descriptor, cmprs_info);
}


static inline void tableam_tops_deform_tuple(Tuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_deform_tuple(tuple, tuple_desc, values, isnull);
}

static inline void tableam_tops_deform_cmprs_tuple(Tuple tuple, TupleDesc tuple_desc, Datum* values, bool* isnull, char* cmprs_info)
{    
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_deform_cmprs_tuple(tuple, tuple_desc, values, isnull, cmprs_info);
}

static inline Size tableam_tops_computedatasize_tuple(TupleDesc tuple_desc, Datum* values, const bool* isnull, uint32 hoff, uint32 tupTableType)
{
    AssertValidTupleType(tupTableType);
    return g_tableam_routines[GetTableAMIndex(tupTableType)]->tops_computedatasize_tuple(tuple_desc, values, isnull, hoff);
}

static inline void tableam_tops_fill_tuple(TupleDesc tuple_desc, Datum* values, const bool* isnull, char* data, Size data_size, uint16* infomask, bits8* bit)
{
    return g_tableam_routines[tuple_desc->tdTableAmType]->tops_fill_tuple(tuple_desc, values, isnull, data, data_size, infomask, bit);
}

/*
 * there is no uheapam_tops_modify_tuple
 * but this is done for completeness
 */
static inline Tuple tableam_tops_modify_tuple(Tuple tuple, TupleDesc tuple_desc, Datum* repl_values, const bool* repl_isnull, const bool* do_replace)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_modify_tuple(tuple, tuple_desc, repl_values, repl_isnull, do_replace);
}

static inline void tableam_tops_free_tuple(HeapTuple &htup)
{
    heap_freetuple_ext(htup);
}

static inline Datum tableam_tops_tuple_getattr(Tuple tuple, int att_num, TupleDesc tuple_desc, bool* is_null)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_tuple_getattr(tuple, att_num, tuple_desc, is_null);
}

static inline bool tableam_tops_tuple_attisnull(Tuple tuple, int attnum, TupleDesc tuple_desc)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_tuple_attisnull(tuple, attnum, tuple_desc);
}

static inline Tuple tableam_tops_copy_tuple(Tuple tuple)
{
    AssertValidTuple(tuple);
    return g_tableam_routines[GetTabelAmIndexTuple(tuple)]->tops_copy_tuple(tuple);
}

static inline MinimalTuple tableam_tops_copy_minimal_tuple(MinimalTuple mtup)
{
    return heap_copy_minimal_tuple(mtup);
}

static inline void tableam_tops_free_minimal_tuple(MinimalTuple mtup)
{
    heap_free_minimal_tuple(mtup);
}


/* ------------------------------------------------------------------------
 * DQL AM APIs
 * ------------------------------------------------------------------------
 */
static inline bool tableam_tuple_fetch(Relation relation, Snapshot snapshot, 
                                                  HeapTuple tuple, Buffer *userbuf, bool keep_buf, Relation stats_relation) {
    return g_tableam_routines[relation->rd_tam_type]->tuple_fetch(relation, snapshot, tuple, userbuf, keep_buf, stats_relation);
}

static inline bool tableam_tuple_satisfies_snapshot(Relation relation, HeapTuple tuple, 
                                                             Snapshot snapshot, Buffer buffer) {
    return g_tableam_routines[relation->rd_tam_type]->tuple_satisfies_snapshot(relation, tuple, snapshot, buffer);
}

static inline void tableam_tuple_get_latest_tid(Relation relation, Snapshot snapshot, 
                                                        ItemPointer tid) {
    return g_tableam_routines[relation->rd_tam_type]->tuple_get_latest_tid(relation, snapshot, tid);
}

static inline Oid tableam_tuple_insert (Relation relation, Tuple tup, CommandId cid,
                           int options, struct BulkInsertStateData *bistate) {
    return g_tableam_routines[relation->rd_tam_type]->tuple_insert(relation, tup, cid, options, bistate);
}

static inline int tableam_tuple_multi_insert(Relation relation, Relation parent,
                                  Tuple* tuples, int ntuples, CommandId cid,
                                  int options, struct BulkInsertStateData *bistate,
                                  HeapMultiInsertExtraArgs *args) {
    return g_tableam_routines[relation->rd_tam_type]->tuple_multi_insert(relation, parent, tuples, ntuples, cid, options, bistate, args);
}

static inline TM_Result tableam_tuple_delete(Relation relation, ItemPointer tid,
                                    CommandId cid, Snapshot crosscheck, Snapshot snapshot,
                                    bool wait, TM_FailureData *tmfd, bool allow_delete_self = false) {
    return g_tableam_routines[relation->rd_tam_type]->tuple_delete(relation, tid, cid, crosscheck, snapshot, wait, tmfd, allow_delete_self);
}


static inline TM_Result tableam_tuple_update(Relation relation, Relation parentRelation,
                             ItemPointer otid, Tuple newtup, CommandId cid,
                             Snapshot crosscheck, Snapshot snapshot, bool wait,
                             TM_FailureData *tmfd, bool *update_indexes, bool allow_update_self = false) {
    return g_tableam_routines[relation->rd_tam_type]->tuple_update(relation, parentRelation, otid, newtup, cid, crosscheck,
        snapshot, wait, tmfd, update_indexes, allow_update_self);
}

static inline TM_Result tableam_tuple_lock(Relation relation, Tuple tuple, Buffer *buffer,
                                           CommandId cid, LockTupleMode mode, bool nowait, TM_FailureData *tmfd,
                                           bool allow_lock_self, bool follow_updates, bool eval, Snapshot snapshot,
                                           ItemPointer tid, bool isSelectForUpdate) {
    return g_tableam_routines[relation->rd_tam_type]->tuple_lock(relation, tuple, buffer, cid, mode, nowait, tmfd,
                                                                 allow_lock_self, follow_updates, eval, snapshot, 
                                                                 tid, isSelectForUpdate);
}

static inline HeapTuple tableam_tuple_lock_updated(CommandId cid, Relation relation, 
                           int lockmode, ItemPointer tid, TransactionId priorXmax) {
    return g_tableam_routines[relation->rd_tam_type]->tuple_lock_updated(cid, relation, lockmode, tid, priorXmax);
}


/* -----------------------------------------------------------------------
 * SCAN AM APIS FOR HEAP
 * ------------------------------------------------------------------------
 */

extern IndexFetchTableData * heapam_scan_index_fetch_begin(Relation rel);
static inline IndexFetchTableData * tableam_scan_index_fetch_begin(Relation rel)
{
    return heapam_scan_index_fetch_begin(rel);
}

extern void heapam_index_fetch_reset(IndexFetchTableData *scan);
static inline void tableam_scan_index_fetch_reset(IndexFetchTableData *scan)
{
    heapam_index_fetch_reset(scan);
}

extern void heapam_scan_index_fetch_end(IndexFetchTableData *scan);
static inline void tableam_scan_index_fetch_end(IndexFetchTableData *scan)
{
    heapam_scan_index_fetch_end(scan);
}

static inline HeapTuple tableam_scan_index_fetch_tuple(IndexScanDesc scan, bool *all_dead)
{
    return g_tableam_routines[scan->heapRelation->rd_tam_type]->scan_index_fetch_tuple(scan, all_dead);
}

static inline TableScanDesc tableam_scan_begin(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
    RangeScanInRedis rangeScanInRedis = {false, 0, 0})
{
    return g_tableam_routines[relation->rd_tam_type]->scan_begin(relation, snapshot, nkeys, key, rangeScanInRedis);
}

static inline TableScanDesc tableam_scan_begin_bm(Relation relation, Snapshot snapshot, int nkeys, ScanKey key)
{
    return g_tableam_routines[relation->rd_tam_type]->scan_begin_bm(relation, snapshot, nkeys, key);
}

static inline TableScanDesc tableam_scan_begin_sampling(Relation relation, Snapshot snapshot, int nkeys, ScanKey key, bool allow_strat,
    bool allow_sync, RangeScanInRedis rangeScanInRedis = {false, 0, 0})
{
    return g_tableam_routines[relation->rd_tam_type]->scan_begin_sampling(relation, snapshot, nkeys, key, allow_strat, allow_sync, rangeScanInRedis);
}

static inline Tuple tableam_scan_getnexttuple(TableScanDesc sscan, ScanDirection direction)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_getnexttuple(sscan, direction);
}

static inline void tableam_scan_getpage(TableScanDesc sscan, BlockNumber page)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_getpage(sscan, page);
}

static inline void tableam_scan_end(TableScanDesc sscan)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_end(sscan);
}

static inline void tableam_scan_rescan(TableScanDesc sscan, ScanKey key)
{
     return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_rescan(sscan, key);
}

static inline void tableam_scan_restrpos(TableScanDesc sscan)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_restrpos(sscan);
}

static inline void tableam_scan_markpos(TableScanDesc sscan)
{
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_markpos(sscan);
}

static inline void tableam_scan_init_parallel_seqscan(TableScanDesc sscan, int32 dop, ScanDirection dir)
{    
    return g_tableam_routines[sscan->rs_rd->rd_tam_type]->scan_init_parallel_seqscan(sscan, dop, dir);
}

static inline double tableam_index_build_scan(Relation heapRelation, Relation indexRelation, 
                            IndexInfo* indexInfo, bool allow_sync, 
                            IndexBuildCallback callback, void* callback_state) {
    return g_tableam_routines[heapRelation->rd_tam_type]->index_build_scan(heapRelation, indexRelation, indexInfo, allow_sync, callback, callback_state);
}


static inline void tableam_index_validate_scan(Relation heapRelation, Relation indexRelation, 
                             IndexInfo* indexInfo, Snapshot snapshot, v_i_state* state) {
    return g_tableam_routines[heapRelation->rd_tam_type]->index_validate_scan(heapRelation, indexRelation, indexInfo, snapshot, state);
}

static inline double tableam_relation_copy_for_cluster(Relation OldHeap, Relation OldIndex, 
                                 Relation NewHeap, TransactionId OldestXmin, 
                                 TransactionId FreezeXid, bool verbose, 
                                 bool use_sort, AdaptMem* memUsage,
                                 TransactionId* ptrFreezeXid) {
    return g_tableam_routines[OldHeap->rd_tam_type]->relation_copy_for_cluster(OldHeap, OldIndex, NewHeap, OldestXmin, FreezeXid, verbose, use_sort, memUsage, ptrFreezeXid);
}


extern TM_Result heapam_tuple_update(Relation relation, Relation parentRelation,
                             ItemPointer otid, Tuple newtup, CommandId cid,
                             Snapshot crosscheck, Snapshot snapshot, bool wait,
                             TM_FailureData *tmfd, bool *update_indexes, bool allow_update_self = false);

#endif /* TABLEAM_H */
