/* -------------------------------------------------------------------------
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * src/include/optimizer/hybrid_recall_path.h
 *
 * [P2-1] HybridRecall planner entry: recognize ogai.hybrid_rrf_marker query
 * shapes and (in later P2 tasks) build the two-branch HybridRecallPath
 * (hybrid recall design doc §7).
 * -------------------------------------------------------------------------
 */
#ifndef HYBRID_RECALL_PATH_H
#define HYBRID_RECALL_PATH_H

#include "nodes/relation.h"

extern void TryAddHybridRecallPath(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);

#endif /* HYBRID_RECALL_PATH_H */
