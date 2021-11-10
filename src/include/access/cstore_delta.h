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
 * cstore_delta.h
 *        routines to support ColStore delta
 *
 *
 * IDENTIFICATION
 *        src/include/access/cstore_delta.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CSTORE_DELTA_H
#define CSTORE_DELTA_H

#include "postgres.h"

#include "nodes/parsenodes.h"
#include "nodes/execnodes.h"
#include "utils/relcache.h"

extern void MoveDeltaDataToCU(Relation rel, Relation parentRel=NULL);
extern void DefineDeltaUniqueIndex(Oid relationId, IndexStmt* stmt, Oid indexRelationId, Relation parentRel=NULL);
extern Oid CreateDeltaUniqueIndex(Relation deltaRel, const char* deltaIndexName, IndexInfo* indexInfo,
    List* indexElemList, List* indexColNames, bool isPrimary);
extern void BuildIndexOnNewDeltaTable(Oid oldColTable, Oid newColTable, Oid parentId=InvalidOid);
extern void ReindexDeltaIndex(Oid indexId, Oid indexPartId);
extern void ReindexPartDeltaIndex(Oid indexOid, Oid partOid);

extern Oid GetDeltaIdxFromCUIdx(Oid CUIndexOid, bool isPartitioned, bool suppressMiss = false);
extern char* GetCUIdxNameFromDeltaIdx(Relation deltaIdx);

#endif
