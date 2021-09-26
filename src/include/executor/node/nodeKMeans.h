/**
Copyright (c) 2021 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

  http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
---------------------------------------------------------------------------------------

nodeKMeans.h
      Functions related to the k-means operator

IDENTIFICATION
    src/include/executor/nodeKMeans.h

---------------------------------------------------------------------------------------
**/

#ifndef DB4AI_NODEKMEANS_H
#define DB4AI_NODEKMEANS_H

#include "nodes/execnodes.h"

/*
 * do not touch this unless you know what you're doing (the implications on
 * nodeKMeans.cpp and create_model.cpp). you are warned.
 */
uint32_t constexpr NUM_ATTR_OUTPUT = 15U;

extern KMeansState* ExecInitKMeans(KMeans* node, EState* estate, int eflags);

extern TupleTableSlot* ExecKMeans(KMeansState* node);

extern void ExecEndKMeans(KMeansState* node);

#endif //DB4AI_NODEKMEANS_H
