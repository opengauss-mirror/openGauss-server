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
 * pruningboundary.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/optimizer/pruningboundary.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PRUNINGBOUNDARY_H_
#define PRUNINGBOUNDARY_H_

#include "nodes/parsenodes.h"
#include "nodes/relation.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "utils/relcache.h"

extern void destroyPruningBoundary(PruningBoundary* boundary);
extern PruningBoundary* makePruningBoundary(int partKeyNum);
extern PruningBoundary* copyBoundary(PruningBoundary* boundary);

#endif /* PRUNINGBOUNDARY_H_ */
