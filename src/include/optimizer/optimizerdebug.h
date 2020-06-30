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
 * ---------------------------------------------------------------------------------------
 * 
 * optimizerdebug.h
 *        prototypes for optimizerdebug.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/optimizer/optimizerdebug.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef OPTIMIZERDEBUG_H
#define OPTIMIZERDEBUG_H

#include "nodes/relation.h"
#include "nodes/print.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"

extern char* debug1_print_relids(Relids relids, List* rtable);

extern void debug1_print_rel(PlannerInfo* root, RelOptInfo* rel);
extern char* debug1_print_path(PlannerInfo* root, Path* path, int indent);
extern void debug_print_hashjoin_detail(PlannerInfo* root, HashPath* path, double virtualbuckets,
    Selectivity innerbucketsize, double outer_scan_ratio, Cost startup_cost, Cost total_cost);
extern void debug_print_agg_detail(PlannerInfo* root, AggStrategy aggstrategy, SAggMethod aggmethod, Path* path1,
    Path* path2 = NULL, Path* path3 = NULL, Path* path4 = NULL, Path* path5 = NULL);

extern inline bool is_errmodule_enable(int elevel, ModuleId mod_id);
#define OPT_LOG(level, format, ...)                                                                      \
    do {                                                                                                 \
        if (is_errmodule_enable(level, MOD_OPT)) {                                                       \
            ereport(level, (errmodule(MOD_OPT), errmsg(format, ##__VA_ARGS__), ignore_interrupt(true))); \
        }                                                                                                \
    } while (0)

#endif /* OPTIMIZERDEBUG_H */
