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
 * bucketpruning.h
 *    functions related to bucketpruning
 *
 * IDENTIFICATION
 *     src/include/optimizer/bucketpruning.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef BUCKETPRUNING_H
#define BUCKETPRUNING_H

#include "nodes/relation.h"
#include "nodes/parsenodes.h"
#include "nodes/execnodes.h"
#include "utils/plancache.h"
#include "utils/globalplancache.h"


extern void set_rel_bucketinfo(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);
extern void setPlanBucketId(Plan* plan, ParamListInfo params, MemoryContext cxt);
extern void setCachedPlanBucketId(CachedPlan *cplan, ParamListInfo boundParams);
extern BucketInfo* CalBucketInfo(ScanState* state);

#endif
