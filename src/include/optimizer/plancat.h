/* -------------------------------------------------------------------------
 *
 * plancat.h
 *	  prototypes for plancat.c.
 *
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/plancat.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PLANCAT_H
#define PLANCAT_H

#include "nodes/relation.h"
#include "utils/relcache.h"

/* Hook for plugins to get control in get_relation_info() */
typedef void (*get_relation_info_hook_type) (PlannerInfo *root, Oid relationObjectId, bool inhparent, RelOptInfo *rel);
extern THR_LOCAL PGDLLIMPORT get_relation_info_hook_type get_relation_info_hook;

extern List* build_index_tlist(PlannerInfo* root, IndexOptInfo* index, Relation heapRelation);

extern void get_relation_info(PlannerInfo* root, Oid relationObjectId, bool inhparent, RelOptInfo* rel);

extern void estimate_rel_size(Relation rel, int32* attr_widths, RelPageType* pages, double* tuples, double* allvisfrac,
    List** sampledPartitionIds);

extern int32 get_relation_data_width(Oid relid, Oid partitionid, int32* attr_widths, bool vectorized = false);

extern int32 getPartitionDataWidth(Relation partRel, int32* attr_widths);

extern int32 getIdxDataWidth(Relation rel, IndexInfo* info, bool vectorized);

extern bool relation_excluded_by_constraints(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);

extern List* build_physical_tlist(PlannerInfo* root, RelOptInfo* rel);

extern bool IsRteForStartWith(PlannerInfo *root, RangeTblEntry *rte);

extern bool has_unique_index(RelOptInfo* rel, AttrNumber attno);

extern Selectivity restriction_selectivity(
    PlannerInfo* root, Oid operatorid, List* args, Oid inputcollid, int varRelid);

extern Selectivity join_selectivity(
    PlannerInfo* root, Oid operatorid, List* args, Oid inputcollid, JoinType jointype, SpecialJoinInfo* sjinfo);
extern void estimatePartitionSize(
    Relation relation, Oid partitionid, int32* attr_widths, RelPageType* pages, double* tuples, double* allvisfrac);

extern bool HasStoredGeneratedColumns(const PlannerInfo *root, Index rti);

extern PlannerInfo *get_cte_root(PlannerInfo *root, int levelsup, char *ctename);

#ifdef USE_SPQ
extern double spq_estimate_partitioned_numtuples(Relation rel);
#endif

#endif /* PLANCAT_H */
