/*-------------------------------------------------------------------------
 *
 * amapi.h
 *	  API for Postgres index access methods.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2015-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
* IDENTIFICATION
 *        src/include/access/amapi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AMAPI_H
#define AMAPI_H

#include "access/genam.h"

struct IndexInfo;

typedef IndexBuildResult *(*ambuild_function) (Relation heapRelation,
											   Relation indexRelation,
											   struct IndexInfo *indexInfo);

typedef void (*ambuildempty_function) (Relation indexRelation);

typedef bool (*aminsert_function) (Relation indexRelation,
								   Datum *values,
								   const bool *isnull,
								   ItemPointer heap_tid,
								   Relation heapRelation,
								   IndexUniqueCheck checkUniqueo);

typedef IndexBulkDeleteResult *(*ambulkdelete_function) (IndexVacuumInfo *info,
														 IndexBulkDeleteResult *stats,
														 IndexBulkDeleteCallback callback,
														 const void *callback_state);

typedef IndexBulkDeleteResult *(*amvacuumcleanup_function) (IndexVacuumInfo *info,
															IndexBulkDeleteResult *stats);

typedef bool (*amcanreturn_function) ();


typedef void (*amcostestimate_function) (struct PlannerInfo *root,
										 struct IndexPath *path,
										 double loop_count,
										 Cost *indexStartupCost,
										 Cost *indexTotalCost,
										 Selectivity *indexSelectivity,
										 double *indexCorrelation);

typedef bytea *(*amoptions_function) (Datum reloptions,
									  bool validate);

typedef bool (*amvalidate_function) (Oid opclassoid);

typedef IndexScanDesc (*ambeginscan_function) (Relation indexRelation,
											   int nkeys,
											   int norderbys);

typedef void (*amrescan_function) (IndexScanDesc scan,
								   ScanKey keys);

typedef bool (*amgettuple_function) (IndexScanDesc scan,
									 ScanDirection direction);

typedef int64 (*amgetbitmap_function) (IndexScanDesc scan,
									   TIDBitmap *tbm);

typedef void (*amendscan_function) (IndexScanDesc scan);

typedef void (*ammarkpos_function) (IndexScanDesc scan);

typedef void (*amrestrpos_function) (IndexScanDesc scan);

typedef IndexBuildResult* (*ammerge_function) (Relation dest_idx_rel, List *src_idx_rel_scans,
                                              List *src_part_merge_offsets);

typedef struct IndexAmRoutine
{
	NodeTag		type;

	ambuild_function ambuild;
	ambuildempty_function ambuildempty;
	aminsert_function aminsert;
	ambulkdelete_function ambulkdelete;
	amvacuumcleanup_function amvacuumcleanup;
	amcanreturn_function amcanreturn;	
	amcostestimate_function amcostestimate;
	amoptions_function amoptions;
	amvalidate_function amvalidate;
	ambeginscan_function ambeginscan;
	amrescan_function amrescan;
	amgettuple_function amgettuple; 
	amgetbitmap_function amgetbitmap;	
	amendscan_function amendscan;
	ammarkpos_function ammarkpos;	
	amrestrpos_function amrestrpos; 
    ammerge_function ammerge;
} IndexAmRoutine;

typedef IndexAmRoutine *AmRoutine;

extern IndexAmRoutine *get_index_amroutine_for_nbtree();

#endif