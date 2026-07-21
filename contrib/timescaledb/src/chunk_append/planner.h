/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_APPEND_PLANNER_H
#define TIMESCALEDB_CHUNK_APPEND_PLANNER_H

#include <postgres.h>
#include "compat.h"

Plan *ts_chunk_append_plan_create(PlannerInfo *root, RelOptInfo *rel, ExtensiblePath *path, List *tlist,
								  List *clauses, List *custom_plans);
Scan *ts_chunk_append_get_scan_plan(Plan *plan);

void _chunk_append_init(void);

extern void _planner_init(void);
extern void _planner_fini(void);

#endif /* TIMESCALEDB_CHUNK_APPEND_PLANNER_H */
