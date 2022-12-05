/* -------------------------------------------------------------------------
 *
 * postgres_fdw.c
 * 		  Foreign-data wrapper for remote openGauss servers
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 		  contrib/postgres_fdw/postgres_fdw.c
 *
 * -------------------------------------------------------------------------
 */

#ifndef _PGFDW_INTERNAL_INTERFACE_H_
#define _PGFDW_INTERNAL_INTERFACE_H_

#include "c.h"
#include "datatypes.h"
#include "nodes/bitmapset.h"
#include "nodes/relation.h"
#include "nodes/nodes.h"
#include "nodes/execnodes.h"
#include "executor/executor.h"
#include "utils/lsyscache.h"
#include "postgres_fdw.h"

/*
 * This macro embodies the correct way to test whether a RestrictInfo is
 * "pushed down" to a given outer join, that is, should be treated as a filter
 * clause rather than a join clause at that outer join.  This is certainly so
 * if is_pushed_down is true; but examining that is not sufficient anymore,
 * because outer-join clauses will get pushed down to lower outer joins when
 * we generate a path for the lower outer join that is parameterized by the
 * LHS of the upper one.  We can detect such a clause by noting that its
 * required_relids exceed the scope of the join.
 */
#define RINFO_IS_PUSHED_DOWN(rinfo, joinrelids) \
    ((rinfo)->is_pushed_down || !bms_is_subset((rinfo)->required_relids, joinrelids))

extern char *psprintf(const char *fmt, ...);
extern size_t pvsnprintf(char *buf, size_t len, const char *fmt, va_list args);

extern Path *GetExistingLocalJoinPath(RelOptInfo *joinrel);
extern ForeignPath *create_foreign_join_path(PlannerInfo *root, RelOptInfo *rel, List *target, double rows,
    Cost startup_cost, Cost total_cost, List *pathkeys, Relids required_outer, Path *fdw_outerpath, List *fdw_private);
ForeignPath *create_foreign_upper_path(PlannerInfo *root, RelOptInfo *rel, List *target, double rows,
    Cost startup_cost, Cost total_cost, List *pathkeys, Path *fdw_outerpath, List *fdw_private);
extern List *ExecInitExprList(List *nodes, PlanState *parent);
extern char *get_namespace_name_or_temp(Oid nspid);
extern RelOptInfo *fetch_upper_rel(FDWUpperRelCxt *ufdwCxt, UpperRelationKind kind);
extern RelOptInfo *make_upper_rel(FDWUpperRelCxt *ufdwCxt, PgFdwRelationInfo *fpinfo);
extern void adjust_limit_rows_costs(double *rows, Cost *startup_cost, Cost *total_cost, int64 offset_est, int64 count_est);
extern Plan *change_plan_targetlist(PlannerInfo *root, Plan *subplan, List *tlist);
extern void apply_tlist_labeling(List *dest_tlist, List *src_tlist);
extern List* extract_target_from_tel(FDWUpperRelCxt *ufdw_cxt, PgFdwRelationInfo *fpinfo);

#define boolVal(v)		((bool)(((Value*)(v))->val.ival))

extern SortGroupClause *get_sortgroupref_clause_noerr(Index sortref, List *clauses);

/* Control flags for format_type_extended */
#define FORMAT_TYPE_TYPEMOD_GIVEN	0x01	/* typemod defined by caller */
#define FORMAT_TYPE_ALLOW_INVALID	0x02	/* allow invalid types */
#define FORMAT_TYPE_FORCE_QUALIFY	0x04	/* force qualification of type */
#define FORMAT_TYPE_INVALID_AS_NULL	0x08	/* NULL if undefined */

#endif