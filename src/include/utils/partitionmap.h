/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * partitionmap.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/partitionmap.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PARTITIONMAP_H_
#define PARTITIONMAP_H_

#include "postgres.h"
#include "access/htup.h"
#include "catalog/pg_type.h"
#include "nodes/primnodes.h"
#include "storage/lock/lock.h"
#include "utils/hsearch.h"
#include "utils/relcache.h"

typedef enum PartitionType {
    PART_TYPE_NONE = 0,
    PART_TYPE_RANGE,
    PART_TYPE_INTERVAL,
    PART_TYPE_HASH,
    PART_TYPE_LIST,
    PART_TYPE_VALUE
} PartitionType;

// describe abstract partition map
typedef struct PartitionMap {
    PartitionType type;
    int refcount;
    bool isDirty;
} PartitionMap;

// describe range partition
#define RANGE_PARTKEYMAXNUM 4
#define PARTITION_PARTKEYMAXNUM 4
#define VALUE_PARTKEYMAXNUM 4
#define INTERVAL_PARTKEYMAXNUM 1

#define LIST_PARTKEYMAXNUM 1

#define HASH_PARTKEYMAXNUM 1

#define PartitionLogicalExist(partitionIdentifier) ((partitionIdentifier)->partSeq >= 0)

#define PartitionPhysicalExist(partitionIdentifier) \
                    ((partitionIdentifier)->partArea != PART_AREA_NONE && ((partitionIdentifier)->fileExist)

void incre_partmap_refcount(PartitionMap* map);
void decre_partmap_refcount(PartitionMap* map);

#define PartitionMapGetKey(partmap) (((RangePartitionMap*)(partmap))->partitionKey)

#define PartitionMapGetType(partmap) (((RangePartitionMap*)(partmap))->partitionKeyDataType)

#define PartitionMapIsRange(partmap) (PART_TYPE_RANGE == ((RangePartitionMap*)(partmap))->type.type)

#define PartitionMapIsList(partmap) (PART_TYPE_LIST == ((ListPartitionMap*)(partmap))->type.type)

#define PartitionMapIsHash(partmap) (PART_TYPE_HASH == ((HashPartitionMap*)(partmap))->type.type)

#define PartitionMapIsInterval(partmap) (PART_TYPE_INTERVAL == ((RangePartitionMap*)(partmap))->type.type)

#define FAKERELATIONCACHESIZE 100

#define BoundaryIsNull(boundary) !(PointerIsValid(boundary))

#define BoundaryIsFull(boundary) (PointerIsValid(boundary) && (boundary)->state == PRUNING_RESULT_FULL)

#define BoundaryIsEmpty(boundary) (PointerIsValid(boundary) && (boundary)->state == PRUNING_RESULT_EMPTY)

#define BoundaryIsSubset(boundary) (PointerIsValid(boundary) && (boundary)->state == PRUNING_RESULT_SUBSET)

#define PruningResultIsFull(pruningRes) (PointerIsValid(pruningRes) && (pruningRes)->state == PRUNING_RESULT_FULL)

#define PruningResultIsEmpty(pruningRes) (!PointerIsValid(pruningRes) || (pruningRes)->state == PRUNING_RESULT_EMPTY)

#define PruningResultIsSubset(pruningRes) (PointerIsValid(pruningRes) && (pruningRes)->state == PRUNING_RESULT_SUBSET)

extern void RelationInitPartitionMap(Relation relation, bool isSubPartition = false);

extern int partOidGetPartSequence(Relation rel, Oid partOid);
extern Oid getListPartitionOid(PartitionMap* partitionmap, Const** partKeyValue, int* partIndex, bool topClosed);
extern Oid getHashPartitionOid(PartitionMap* partitionmap, Const** partKeyValue, int* partIndex, bool topClosed);
extern Oid getRangePartitionOid(PartitionMap* partitionmap, Const** partKeyValue, int* partIndex, bool topClosed);
extern Oid GetPartitionOidByParam(Relation relation, Param *paramArg, ParamExternData *prm);
extern List* getRangePartitionBoundaryList(Relation rel, int sequence);
extern List* getListPartitionBoundaryList(Relation rel, int sequence);
extern List* getHashPartitionBoundaryList(Relation rel, int sequence);
extern Oid partitionKeyValueListGetPartitionOid(Relation rel, List* partKeyValueList, bool topClosed);
extern int getNumberOfRangePartitions(Relation rel);
extern int getNumberOfListPartitions(Relation rel);
extern int getNumberOfHashPartitions(Relation rel);
extern int getNumberOfPartitions(Relation rel);
extern Const* transformDatum2Const(TupleDesc tupledesc, int16 attnum, Datum datumValue, bool isnull, Const* cnst);

extern int2vector* getPartitionKeyAttrNo(
    Oid** typeOids, HeapTuple pg_part_tup, TupleDesc tupledsc, TupleDesc rel_tupledsc);
extern void unserializePartitionStringAttribute(Const** outMax, int outMaxLen, Oid* partKeyType, int partKeyTypeLen,
    Oid relid, int2vector* partkey, HeapTuple pg_part_tup, int att_num, TupleDesc tupledsc);
extern void unserializeListPartitionAttribute(int *len, Const*** listValues, Oid* partKeyType, int partKeyTypeLen,
    Oid relid, int2vector* partkey, HeapTuple pg_part_tup, int att_num, TupleDesc tupledsc);
extern void unserializeHashPartitionAttribute(Const** outMax, int outMaxLen,
    Oid relid, int2vector* partkey, HeapTuple pg_part_tup, int att_num, TupleDesc tupledsc);

extern int partitonKeyCompare(Const** value1, Const** value2, int len);
extern int getPartitionNumber(PartitionMap* map);
extern int GetSubPartitionNumber(Relation rel);

extern bool targetListHasPartitionKey(List* targetList, Oid partitiondtableid);

extern int constCompare_constType(Const* value1, Const* value2);

extern bool partitionHasToast(Oid partOid);

extern void constCompare(Const* value1, Const* value2, int& compare);

extern struct ListPartElement* CopyListElements(ListPartElement* src, int elementNum);
extern struct HashPartElement* CopyHashElements(HashPartElement* src, int elementNum, int partkeyNum);

#endif /* PARTITIONMAP_H_ */
