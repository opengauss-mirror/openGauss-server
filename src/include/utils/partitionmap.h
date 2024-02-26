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

/*
 * ---------------------------------------------------------------------------------------
 *
 * Table partitioning related defines
 *
 * ---------------------------------------------------------------------------------------
 */

/* ALL types partition allows 4 columns at most */
#define MAX_PARTKEY_NUMS           16

/* range-partition allows 4 columns at most */
#define MAX_RANGE_PARTKEY_NUMS     16

/* interval-partition allows 1 columns at most */
#define MAX_INTERVAL_PARTKEY_NUMS  1

/* list-partition allows 1 columns at most */
#define MAX_LIST_PARTKEY_NUMS      16

/* hash-partition allows 1 columns at most */
#define MAX_HASH_PARTKEY_NUMS      1

/* value-partition allows 4 columns at most */
#define MAX_VALUE_PARTKEY_NUMS     4

typedef struct {
    Datum value;
    bool isNull;
} PartKeyExprResult;

/* describe table partition type */
typedef enum PartitionType {
    PART_TYPE_NONE = 0,
    PART_TYPE_RANGE,
    PART_TYPE_INTERVAL,
    PART_TYPE_HASH,
    PART_TYPE_LIST,
    PART_TYPE_VALUE
} PartitionType;

/* describe abstract PartitionMap class */
typedef struct PartitionMap {
    PartitionType type;
    int refcount;
    bool isDirty;

    Oid relOid;                /* oid of partitioned table a.w.k the pg_class.oid */
    int2vector *partitionKey;  /* partition key */
    Oid *partitionKeyDataType; /* the data type of partition key */
} PartitionMap;

typedef struct PartitionKey {
    int count;      /* partition key values count */
    Const **values;
} PartitionKey;

#define PartitionLogicalExist(partitionIdentifier) ((partitionIdentifier)->partSeq >= 0)

#define PartitionPhysicalExist(partitionIdentifier) \
                    ((partitionIdentifier)->partArea != PART_AREA_NONE && ((partitionIdentifier)->fileExist)

void incre_partmap_refcount(PartitionMap* map);
void decre_partmap_refcount(PartitionMap* map);

/* macro to check type of partition type */
#define PartitionMapIsRange(partMap)    (((PartitionMap *)partMap)->type == PART_TYPE_RANGE)
#define PartitionMapIsList(partMap)     (((PartitionMap *)partMap)->type == PART_TYPE_LIST)
#define PartitionMapIsHash(partMap)     (((PartitionMap *)partMap)->type == PART_TYPE_HASH)
#define PartitionMapIsInterval(partMap) (((PartitionMap *)partMap)->type == PART_TYPE_INTERVAL)

#define PartitionMapTypeIsValid(partMap) (  \
    PartitionMapIsRange(partMap) ||         \
    PartitionMapIsList(partMap)  ||         \
    PartitionMapIsHash(partMap)  ||         \
    PartitionMapIsInterval(partMap)         \
)

inline int2vector* PartitionMapGetPartKeyArray(PartitionMap *partMap)
{
    Assert (partMap != NULL && PartitionMapTypeIsValid(partMap));
    return partMap->partitionKey;
}

inline int PartitionMapGetPartKeyNum(PartitionMap *partMap)
{
    Assert (partMap != NULL && PartitionMapTypeIsValid(partMap));
    return partMap->partitionKey->dim1;
}

#define constIsMaxValue(value) ((value)->ismaxvalue)

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
extern void partitionKeyCompareForRouting(Const **partkey_value, Const **partkey_bound, uint32 partKeyColumnNum,
                                          int &compare);
extern Oid getListPartitionOid(PartitionMap* partitionmap, Const** partKeyValue, int partKeyCount, int* partIndex);
extern Oid getHashPartitionOid(PartitionMap* partitionmap, Const** partKeyValue, int* partIndex);
extern Oid getRangePartitionOid(PartitionMap* partitionmap, Const** partKeyValue, int* partIndex, bool topClosed);
extern Oid GetPartitionOidByParam(PartitionMap* partitionmap, Param *paramArg, ParamExternData *prm);
extern List* getRangePartitionBoundaryList(Relation rel, int sequence);
extern List* getListPartitionBoundaryList(Relation rel, int sequence);
extern List* getHashPartitionBoundaryList(Relation rel, int sequence);
extern Oid partitionKeyValueListGetPartitionOid(Relation rel, List* partKeyValueList, bool topClosed);
extern int getNumberOfRangePartitions(Relation rel);
extern int getNumberOfListPartitions(Relation rel);
extern int getNumberOfHashPartitions(Relation rel);
extern int getNumberOfPartitions(Relation rel);
extern Const* transformDatum2Const(TupleDesc tupledesc, int16 attnum, Datum datumValue, bool isnull, Const* cnst);
Const* transformDatum2ConstForPartKeyExpr(PartitionMap* partMap, PartKeyExprResult* result, Const* cnst);

extern int2vector* getPartitionKeyAttrNo(
    Oid** typeOids, HeapTuple pg_part_tup, TupleDesc tupledsc, TupleDesc rel_tupledsc);
extern void unserializePartitionStringAttribute(Const** outMax, int outMaxLen, Oid* partKeyType, int partKeyTypeLen,
    Oid relid, int2vector* partkey, HeapTuple pg_part_tup, int att_num, TupleDesc tupledsc);
extern void unserializeListPartitionAttribute(int *len, PartitionKey** listValues, Oid* partKeyType, int partKeyTypeLen,
    Oid relid, int2vector* partkey, HeapTuple pg_part_tup, int att_num, TupleDesc tupledsc);
extern void unserializeHashPartitionAttribute(Const** outMax, int outMaxLen,
    Oid relid, int2vector* partkey, HeapTuple pg_part_tup, int att_num, TupleDesc tupledsc);

extern int partitonKeyCompare(Const** value1, Const** value2, int len, bool nullEqual = false);
extern int getPartitionNumber(PartitionMap* map);
extern int GetSubPartitionNumber(Relation rel);

extern bool targetListHasPartitionKey(List* targetList, Oid partitiondtableid);

extern int constCompare_constType(Const* value1, Const* value2, Oid collation);

extern bool partitionHasToast(Oid partOid);

extern void constCompare(Const* value1, Const* value2, Oid collation, int& compare);

extern struct ListPartElement* CopyListElements(ListPartElement* src, int elementNum);
extern struct HashPartElement* CopyHashElements(HashPartElement* src, int elementNum, int partkeyNum);

#endif /* PARTITIONMAP_H_ */
