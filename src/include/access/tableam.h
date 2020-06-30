/*-------------------------------------------------------------------------
 *
 * tableam.h
 *    POSTGRES table access method definitions.
 *
 *
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
#include "optimizer/bucketinfo.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapshot.h"
#include "nodes/execnodes.h"

extern bool reset_scan_qual(Relation currHeapRel, ScanState * node);


typedef void (*table_endscan_t)(AbsTblScanDesc scan);

typedef void (*table_rescan_t)(AbsTblScanDesc scan, struct ScanKeyData* key);

typedef HeapTuple (*table_getnext_t)(AbsTblScanDesc scan, ScanDirection direction);

typedef void (*table_markpos_t)(AbsTblScanDesc scan);

typedef void (*table_restrpos_t)(AbsTblScanDesc scan);

typedef void (*table_getpage_t)(AbsTblScanDesc scan, BlockNumber page);

typedef void(*table_init_parallel_seqscan_t)(AbsTblScanDesc scan, int dop, ScanDirection dir);

/*
 * API struct for a table AM.  Note this must be allocated in a
 * server-lifetime manner, typically as a static const struct, which then gets
 * returned by FormData_pg_am.amhandler.
 *
 * In most cases it's not appropriate to call the callbacks directly, use the
 * table_* wrapper functions instead.
 *
 * GetTableAmRoutine() asserts that required callbacks are filled in, remember
 * to update when adding a callback.
 */
typedef struct TableAm {
    /*
	 * Release resources and deallocate scan. If AbsTblScanDesc.temp_snap,
	 * AbsTblScanDesc.rs_snapshot needs to be unregistered.
	 */
    table_endscan_t table_endscan;

    /*
	 * Restart relation scan.  If set_params is set to true, allow_{strat,
	 * sync, pagemode} (see scan_begin) changes should be taken into account.
	 */
    table_rescan_t table_rescan;

    /*
	 * Return next tuple from `scan`, store in slot.
	 */
    table_getnext_t table_getnext;

    table_markpos_t table_markpos;

    table_restrpos_t table_restrpos;

    table_getpage_t table_getpage;

	table_init_parallel_seqscan_t table_init_parallel_seqscan;
} TableAm;

static inline const TableAm* TblScanAm(AbsTblScanDesc scan)
{
    return scan->tblAm;
}


/* Create AbsTblScanDesc with scan state. For hash-bucket table, it scans the
 * specified buckets in ScanState */
static inline AbsTblScanDesc abs_tbl_beginscan(Relation relation, Snapshot snapshot,
    int nkeys, ScanKey key, ScanState* sstate = NULL)
{
    if (RELATION_CREATE_BUCKET(relation)) {
        return (AbsTblScanDesc)hbkt_tbl_beginscan(relation, snapshot,
            nkeys, key, sstate);
    }

    bool isRangeScanInRedis = reset_scan_qual(relation, sstate);
    return (AbsTblScanDesc)heap_beginscan(relation, snapshot,
        nkeys, key, isRangeScanInRedis);
}

static inline AbsTblScanDesc abs_tbl_begin_tidscan(Relation relation, ScanState* state)
{
	if (RELATION_CREATE_BUCKET(relation)) {
		return (AbsTblScanDesc)hbkt_tbl_begin_tidscan(relation, state);
	}

	return NULL;
}

static inline void abs_tbl_end_tidscan(AbsTblScanDesc scan)
{
	if (scan == NULL) {
		return;
	}

	Assert(scan->type == T_ScanDesc_HBucket);
	hbkt_tbl_end_tidscan(scan);
}

static inline
AbsTblScanDesc abs_tbl_beginscan_bm(Relation relation, Snapshot snapshot,
	int nkeys, ScanKey key, ScanState* sstate)
{
	if (RELATION_CREATE_BUCKET(relation)) {
		return (AbsTblScanDesc)hbkt_tbl_beginscan_bm(relation, snapshot, nkeys, key, sstate);
	}

	return (AbsTblScanDesc)heap_beginscan_bm(relation, snapshot, nkeys, key);
}

static inline AbsTblScanDesc abs_tbl_beginscan_sampling(Relation relation, Snapshot snapshot, 
	int nkeys, ScanKey key, bool allow_strat, bool allow_sync, ScanState* sstate)
{
    if (RelationIsCUFormat(relation)) {
        return (AbsTblScanDesc)heap_beginscan_sampling(relation, snapshot,
            nkeys, key, allow_strat, allow_sync, sstate->isRangeScanInRedis);
    }

    if (RELATION_CREATE_BUCKET(relation)) {
        return (AbsTblScanDesc)hbkt_tbl_beginscan_sampling(relation, snapshot,
            nkeys, key, allow_strat, allow_sync, sstate);
    }

    bool isRangeScanInRedis = reset_scan_qual(relation, sstate);
    return (AbsTblScanDesc)heap_beginscan_sampling(relation, snapshot,
        nkeys, key, allow_strat, allow_sync, isRangeScanInRedis);
}

static inline HeapTuple abs_tbl_getnext(AbsTblScanDesc scan, ScanDirection direction)
{
    return TblScanAm(scan)->table_getnext(scan, direction);
}

static inline 
void abs_tbl_endscan(AbsTblScanDesc scan)
{
    TblScanAm(scan)->table_endscan(scan);
}

static inline 
void abs_tbl_rescan(AbsTblScanDesc scan, struct ScanKeyData* key)
{
    TblScanAm(scan)->table_rescan(scan, key);
}

static inline
void abs_tbl_markpos(AbsTblScanDesc scan)
{
    TblScanAm(scan)->table_markpos(scan);
}

static inline
void abs_tbl_restrpos(AbsTblScanDesc scan)
{
    TblScanAm(scan)->table_restrpos(scan);
}

static inline
void abs_tbl_getpage(AbsTblScanDesc scan, BlockNumber page)
{
	TblScanAm(scan)->table_getpage(scan, page);
}

static inline void abs_tbl_init_parallel_seqscan(AbsTblScanDesc scan, int dop, ScanDirection dir)
{
	TblScanAm(scan)->table_init_parallel_seqscan(scan, dop, dir);
}

static inline HeapScanDesc GetHeapScanDesc(AbsTblScanDesc scan)
{
    if (scan != NULL && scan->type == T_ScanDesc_HBucket) {
        HBktTblScanDesc hpScan = (HBktTblScanDesc)scan;
        return hpScan->currBktScan;
    }

    return (HeapScanDesc)scan;
}

static inline void table_getpage(AbsTblScanDesc scan, BlockNumber page)
{
    TblScanAm(scan)->table_getpage(scan, page);
}

typedef void (*idx_rescan_t)(AbsIdxScanDesc scan,
    ScanKey keys, int nkeys,
    ScanKey orderbys, int norderbys);

typedef void(*idx_rescan_local_t)(AbsIdxScanDesc scan,
	ScanKey keys, int nkeys,
	ScanKey orderbys, int norderbys);

typedef void(*idx_rescan_bitmap_t)(AbsIdxScanDesc scan,
	ScanKey keys, int nkeys,
	ScanKey orderbys, int norderbys);

typedef void (*idx_endscan_t)(AbsIdxScanDesc scan);

typedef void (*idx_markpos_t)(AbsIdxScanDesc scan);

typedef void (*idx_restrpos_t)(AbsIdxScanDesc scan);

typedef ItemPointer (*idx_getnext_tid_t)(AbsIdxScanDesc scan, ScanDirection direction);

typedef HeapTuple (*idx_fetch_heap_t)(AbsIdxScanDesc scan);

typedef HeapTuple (*idx_getnext_t)(AbsIdxScanDesc scan, ScanDirection direction);

typedef int64 (*idx_getbitmap_t)(AbsIdxScanDesc scan, TIDBitmap *bitmap);

typedef struct IndexAm {
    idx_rescan_t idx_rescan;

	idx_rescan_local_t idx_rescan_local;

	idx_rescan_bitmap_t idx_rescan_bitmap;

    idx_endscan_t idx_endscan;

    idx_markpos_t idx_markpos;

    idx_restrpos_t idx_restrpos;

    idx_getnext_tid_t idx_getnext_tid;

    idx_fetch_heap_t idx_fetch_heap;

    idx_getnext_t idx_getnext;

	idx_getbitmap_t idx_getbitmap;
} IndexAm;

static inline const IndexAm* IdxScanAm(AbsIdxScanDesc scan)
{
    return scan->idxAm;
}

static inline void abs_idx_rescan(AbsIdxScanDesc scan,
    ScanKey keys, int nkeys,
    ScanKey orderbys, int norderbys)
{
    IdxScanAm(scan)->idx_rescan(scan, keys, nkeys, orderbys, norderbys);
}

static inline void abs_idx_rescan_local(AbsIdxScanDesc scan,
    ScanKey keys, int nkeys,
    ScanKey orderbys, int norderbys)
{
    IdxScanAm(scan)->idx_rescan_local(scan, keys, nkeys, orderbys, norderbys);
}

static inline void abs_idx_endscan(AbsIdxScanDesc scan)
{
    IdxScanAm(scan)->idx_endscan(scan);
}

static inline void abs_idx_markpos(AbsIdxScanDesc scan)
{
    IdxScanAm(scan)->idx_markpos(scan);
}

static inline void abs_idx_restrpos(AbsIdxScanDesc scan)
{
    return IdxScanAm(scan)->idx_restrpos(scan);
}

static inline HeapTuple abs_idx_fetch_heap(AbsIdxScanDesc scan)
{
    return IdxScanAm(scan)->idx_fetch_heap(scan);
}

static inline HeapTuple abs_idx_getnext(AbsIdxScanDesc scan, ScanDirection direction)
{
    return IdxScanAm(scan)->idx_getnext(scan, direction);
}

static inline ItemPointer abs_idx_getnext_tid(AbsIdxScanDesc scan, ScanDirection direction)
{
    return IdxScanAm(scan)->idx_getnext_tid(scan, direction);
}

static inline AbsIdxScanDesc abs_idx_beginscan(Relation heapRelation,
    Relation indexRelation,
    Snapshot snapshot,
    int nkeys, int norderbys,
    ScanState* scanState)
{
    if (RELATION_CREATE_BUCKET(heapRelation)) {
        return (AbsIdxScanDesc)hbkt_idx_beginscan(heapRelation, indexRelation,
            snapshot, nkeys, norderbys, scanState);
    }

    return (AbsIdxScanDesc)index_beginscan(heapRelation, indexRelation, snapshot, nkeys, norderbys);
}

static inline IndexScanDesc GetIndexScanDesc(AbsIdxScanDesc scan)
{
    if (scan != NULL && scan->type == T_ScanDesc_HBucketIndex) {
        HBktIdxScanDesc hpScan = (HBktIdxScanDesc)scan;
        return hpScan->currBktIdxScan;
    }

    return (IndexScanDesc)scan;
}

static inline AbsIdxScanDesc abs_idx_beginscan_bitmap(Relation indexRelation,
    Snapshot snapshot,
    int nkeys,
    ScanState* scanState)
{
    if (RELATION_CREATE_BUCKET(indexRelation)) {
        return (AbsIdxScanDesc)hbkt_idx_beginscan_bitmap(indexRelation, snapshot, nkeys, scanState);
    }
    return (AbsIdxScanDesc)index_beginscan_bitmap(indexRelation, snapshot, nkeys);
}

static inline void abs_idx_rescan_bitmap(AbsIdxScanDesc scan, ScanKey keys, int nkeys,
	ScanKey orderbys, int norderbys)
{
	IdxScanAm(scan)->idx_rescan_bitmap(scan, keys, nkeys, orderbys, norderbys);
}

static inline int64 abs_idx_getbitmap(AbsIdxScanDesc scan, TIDBitmap *bitmap)
{
	return IdxScanAm(scan)->idx_getbitmap(scan, bitmap);
}

#endif /* TABLEAM_H */
