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
 * pruningslice.h
 *  some routines that be used for slice pruning for list/range distributed table.
 * 
 * 
 * IDENTIFICATION
 *        src/include/pgxc/pruningslice.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef PRUNINGSLICE_H_
#define PRUNINGSLICE_H_

void PruningSliceForExecNodes(ExecNodes* exec_nodes, ParamListInfo boundParams);
void PruningSliceForQuals(ExecNodes* execNodes, Index varno, Node* quals, ParamListInfo boundParams);

void InitDistColIndexMap(int* colMap, int size, List* distColIdxList);
Oid GetRangeNodeFromValue(RelationLocInfo* relLocInfo, Datum* datums, const bool* nulls, Oid* attrs,
    int* colMap, int len);
Oid GetListNodeFromValue(RelationLocInfo* relLocInfo, Datum* datums, const bool* nulls, Oid* attrs,
    int* colMap, int len);

void PruningRangeSlice(ExecNodes* execNodes, RelationLocInfo* relLocInfo, Index varno, RelationAccessType relaccess,
     ListCell* lastDistcolCell, Node* quals, ParamListInfo boundParams, bool useDynReduce);

void ConstructConstFromValues(Datum* datums, const bool* nulls, Oid* attrs,
    const int* colMap, int len, Const* consts, Const** constPointers);
void ConstructSliceBoundaryInner(ExecNodes* en);
uint2 GetTargetConsumerNodeIdx(ExecBoundary* enBoundary, Const** distValues, int distLen);


#endif /* PRUNINGSLICE_H_ */

