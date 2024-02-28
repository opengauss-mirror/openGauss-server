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
#include "pg_config_manual.h"

#define PG_AM_INSERT_ARGS_NUM			6
#define PG_AM_BEGINSCAN_ARGS_NUM		3
#define PG_AM_GETTUPLE_ARGS_NUM			2
#define PG_AM_RESCAN_ARGS_NUM			5
#define PG_AM_ENDSCAN_ARGS_NUM			1
#define PG_AM_BUILD_ARGS_NUM			3
#define PG_AM_BUILDEMPTY_ARGS_NUM		1
#define PG_AM_BULKDELETE_ARGS_NUM		4
#define PG_AM_VACUUMCLEANUP_ARGS_NUM	2
#define PG_AM_COSTESTIMATE_ARGS_NUM		7
#define PG_AM_OPTIONS_ARGS_NUM			2
#define PG_AM_FUNC_MAX_ARGS_NUM			PG_AM_COSTESTIMATE_ARGS_NUM

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

	/*
	 * Total number of strategies (operators) by which we can traverse/search
	 * this AM.  Zero if AM does not have a fixed set of strategy assignments.
	 */
	uint16		amstrategies;
	/* total number of support functions that this AM uses */
	uint16		amsupport;
	/* opclass options support function number or 0 */
	uint16		amoptsprocnum;
	/* does AM support ORDER BY indexed column's value? */
	bool		amcanorder;
	/* does AM support ORDER BY result of an operator on indexed column? */
	bool		amcanorderbyop;
	/* does AM support backward scanning? */
	bool		amcanbackward;
	/* does AM support UNIQUE indexes? */
	bool		amcanunique;
	/* does AM support multi-column indexes? */
	bool		amcanmulticol;
	/* does AM require scans to have a constraint on the first index column? */
	bool		amoptionalkey;
	/* does AM handle ScalarArrayOpExpr quals? */
	bool		amsearcharray;
	/* does AM handle IS NULL/IS NOT NULL quals? */
	bool		amsearchnulls;
	/* can index storage data type differ from column data type? */
	bool		amstorage;
	/* can an index of this type be clustered on? */
	bool		amclusterable;
	/* does AM handle predicate locks? */
	bool		ampredlocks;
	/* does AM support parallel scan? */
	bool		amcanparallel;
	/* does AM support columns included with clause INCLUDE? */
	bool		amcaninclude;
	/* does AM use maintenance_work_mem? */
	bool		amusemaintenanceworkmem;
	/* OR of parallel vacuum flags.  See vacuum.h for flags. */
	uint8		amparallelvacuumoptions;
	/* type of data stored in index, or InvalidOid if variable */
	Oid			amkeytype;

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

	char ambuildfuncname[NAMEDATALEN];
	char ambuildemptyfuncname[NAMEDATALEN];
	char aminsertfuncname[NAMEDATALEN];
	char ambulkdeletefuncname[NAMEDATALEN];
	char amvacuumcleanupfuncname[NAMEDATALEN];
	char amcanreturnfuncname[NAMEDATALEN];
	char amcostestimatefuncname[NAMEDATALEN];
	char amoptionsfuncname[NAMEDATALEN];
	char amvalidatefuncname[NAMEDATALEN];
	char ambeginscanfuncname[NAMEDATALEN];
	char amrescanfuncname[NAMEDATALEN];
	char amgettuplefuncname[NAMEDATALEN];
	char amgetbitmapfuncname[NAMEDATALEN];
	char amendscanfuncname[NAMEDATALEN];
	char ammarkposfuncname[NAMEDATALEN];
	char amrestrposfuncname[NAMEDATALEN];
    char ammergefuncname[NAMEDATALEN];
} IndexAmRoutine;

typedef IndexAmRoutine *AmRoutine;

extern IndexAmRoutine *get_index_amroutine_for_nbtree();

#endif