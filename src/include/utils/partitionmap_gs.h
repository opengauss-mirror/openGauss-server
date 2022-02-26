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
 * partitionmap_gs.h
 * 
 *        this file separates from partitionmap.h, extract major code from partitionmap.h
 * 		  to here
 * 
 * IDENTIFICATION
 *        src/include/utils/partitionmap_gs.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PARTITIONMAP_GS_H_
#define PARTITIONMAP_GS_H_

#include "postgres.h"
#include "access/heapam.h"
#include "knl/knl_variable.h"
#include "access/htup.h"
#include "access/ustore/knl_uheap.h"
#include "catalog/pg_type.h"
#include "nodes/primnodes.h"
#include "storage/lock/lock.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/partcache.h"
#include "utils/partitionmap.h"

typedef enum PartitionArea {
    PART_AREA_NONE = 0,
    PART_AREA_RANGE,
    PART_AREA_INTERVAL,
    PART_AREA_LIST,
    PART_AREA_HASH
} PartitionArea;

/*
 * Partition position in partitioned-table
 * partition sequence is numbered in range area and interval area separately.
 * partition sequec start with 0
 */
typedef struct PartitionIdentifier {
    PartitionArea partArea;
    int partSeq;
    bool fileExist;
    Oid partitionId;
} PartitionIdentifier;

/**
 *partition map is used to  find which partition a record is mapping to.
 *and  pruning the unused partition when querying.the map has two part:
 *and  range part and interval part.
 *range part is a array of RangeElement which is sorted by RangeElement.boundary
 *interval part is array of IntervalElement which is sorted by IntervalElement.sequenceNum
 *binary search is used to routing in  range part of map , and in interval part
 *we use (recordValue-lowBoundary_of_interval)/interval to get the sequenceNum of
 *a interval partition
 *
 */
typedef struct RangeElement {
    Oid partitionOid;                     /*the oid of partition*/
    int len;                              /*the length of partition key number*/
    Const* boundary[RANGE_PARTKEYMAXNUM]; /*upper bond of partition */
    bool isInterval;                      /* is interval partition */
} RangeElement;

typedef struct IntervalElement {
    Oid partitionOid; /* the oid of partition */
    int sequenceNum;  /* the logic number of interval partition. */
} IntervalElement;

typedef struct ListPartElement {
    Oid partitionOid;                     /* the oid of partition */
    int len;                              /* the length of values */
    Const** boundary;                      /* list values */
} ListPartElement;

typedef struct HashPartElement {
    Oid partitionOid;                     /* the oid of partition */
    Const* boundary[1];                   /* hash bucket */
} HashPartElement;

// describe partition info of Value  Partitioned-Table
typedef struct ValuePartitionMap {
    PartitionMap type;
    Oid relid;      /* Oid of partitioned table */
    List* partList; /* List<OID> Partition key List */
} ValuePartitionMap;

// describe partition info of  Range Partitioned-Table
typedef struct RangePartitionMap {
    PartitionMap type;
    Oid relid;                 /*oid of partitioned table*/
    int2vector* partitionKey;  /*partition key*/
    Oid* partitionKeyDataType; /*the data type of partition key*/
    /*section 1: range partition specific*/
    int rangeElementsNum;          /* the number of range partition*/
    RangeElement* rangeElements;   /* array of RangeElement */
    Interval* intervalValue;       /* valid for interval partition */
    oidvector* intervalTablespace; /* valid for interval partition */
} RangePartitionMap;

bool ValueSatisfyLowBoudary(Const** partKeyValue, RangeElement* partition, Interval* intervalValue, bool topClosed);
extern int2vector* GetPartitionKey(const PartitionMap* partMap);

typedef struct ListPartitionMap {
    PartitionMap type;
    Oid relid;      /* Oid of partitioned table */
    int2vector* partitionKey;  /* partition key */
    Oid* partitionKeyDataType; /* the data type of partition key */
    /* section 1: list partition specific */
    int listElementsNum;          /* the number of list partition */
    ListPartElement* listElements;   /* array of listElement */
} ListPartitionMap;

typedef struct HashPartitionMap {
    PartitionMap type;
    Oid relid;      /* Oid of partitioned table */
    int2vector* partitionKey;  /* partition key */
    Oid* partitionKeyDataType; /* the data type of partition key */
    /* section 1: hash partition specific */
    int hashElementsNum;          /* the number of hash partition */
    HashPartElement* hashElements;   /* array of hashElement */
} HashPartitionMap;

#define PartitionkeyTypeGetIntervalType(type)                                          \
    do {                                                                               \
        if (DATEOID == (type) || TIMESTAMPOID == (type) || TIMESTAMPTZOID == (type)) { \
            (type) = INTERVALOID;                                                      \
        }                                                                              \
    } while (0)

#define partitionRoutingForTuple(rel, tuple, partIdentfier)                                                           \
    do {                                                                                                              \
        TupleDesc tuple_desc = NULL;                                                                                  \
        int2vector *partkey_column = NULL;                                                                            \
        int partkey_column_n = 0;                                                                                     \
        static THR_LOCAL Const consts[PARTITION_PARTKEYMAXNUM];                                                       \
        static THR_LOCAL Const *values[PARTITION_PARTKEYMAXNUM];                                                      \
        bool isnull = false;                                                                                          \
        bool is_ustore = RelationIsUstoreFormat(rel);                                                                 \
        Datum column_raw;                                                                                             \
        int i = 0;                                                                                                    \
        partkey_column = GetPartitionKey((rel)->partMap);                                                             \
        partkey_column_n = partkey_column->dim1;                                                                      \
        tuple_desc = (rel)->rd_att;                                                                                   \
        for (i = 0; i < partkey_column_n; i++) {                                                                      \
            isnull = false;                                                                                           \
            column_raw = (is_ustore)                                                                                  \
                             ? UHeapFastGetAttr((UHeapTuple)(tuple), partkey_column->values[i], tuple_desc, &isnull)  \
                             : fastgetattr((HeapTuple)(tuple), partkey_column->values[i], tuple_desc, &isnull);       \
            values[i] =                                                                                               \
                transformDatum2Const((rel)->rd_att, partkey_column->values[i], column_raw, isnull, &consts[i]);       \
        }                                                                                                             \
        if (PartitionMapIsInterval((rel)->partMap) && values[0]->constisnull) {                                       \
            ereport(ERROR,                                                                                            \
                    (errcode(ERRCODE_INTERNAL_ERROR), errmsg("inserted partition key does not map to any partition"), \
                     errdetail("inserted partition key cannot be NULL for interval-partitioned table")));             \
        }                                                                                                             \
        partitionRoutingForValue((rel), values, partkey_column_n, true, false, (partIdentfier));                      \
    } while (0)

#define partitionRoutingForValue(rel, keyValue, valueLen, topClosed, missIsOk, result)                                 \
    do {                                                                                                               \
        if ((rel)->partMap->type == PART_TYPE_RANGE || (rel)->partMap->type == PART_TYPE_INTERVAL) {                   \
            if ((rel)->partMap->type == PART_TYPE_RANGE) {                                                             \
                (result)->partArea = PART_AREA_RANGE;                                                                  \
            } else {                                                                                                   \
                Assert((valueLen) == 1);                                                                               \
                (result)->partArea = PART_AREA_INTERVAL;                                                               \
            }                                                                                                          \
            (result)->partitionId = getRangePartitionOid((rel)->partMap, (keyValue), &((result)->partSeq), topClosed); \
            if ((result)->partSeq < 0) {                                                                               \
                (result)->fileExist = false;                                                                           \
            } else {                                                                                                   \
                (result)->fileExist = true;                                                                            \
                RangePartitionMap *partMap = (RangePartitionMap *)((rel)->partMap);                                    \
                if (partMap->rangeElements[(result)->partSeq].isInterval &&                                            \
                    !ValueSatisfyLowBoudary(keyValue, &partMap->rangeElements[(result)->partSeq],                      \
                                            partMap->intervalValue, topClosed)) {                                      \
                    (result)->partSeq = -1;                                                                            \
                    (result)->partitionId = InvalidOid;                                                                \
                    (result)->fileExist = false;                                                                       \
                }                                                                                                      \
            }                                                                                                          \
        } else if ((rel)->partMap->type == PART_TYPE_LIST) {                                                           \
            (result)->partArea = PART_AREA_LIST;                                                                       \
            (result)->partitionId =                                                                                    \
                getListPartitionOid(((rel)->partMap), (keyValue), &((result)->partSeq), topClosed);                    \
            if ((result)->partSeq < 0) {                                                                               \
                (result)->fileExist = false;                                                                           \
            } else {                                                                                                   \
                (result)->fileExist = true;                                                                            \
            }                                                                                                          \
        } else if ((rel)->partMap->type == PART_TYPE_HASH) {                                                           \
            (result)->partArea = PART_AREA_HASH;                                                                       \
            (result)->partitionId =                                                                                    \
                getHashPartitionOid(((rel)->partMap), (keyValue), &((result)->partSeq), topClosed);                    \
            if ((result)->partSeq < 0) {                                                                               \
                (result)->fileExist = false;                                                                           \
            } else {                                                                                                   \
                (result)->fileExist = true;                                                                            \
            }                                                                                                          \
        } else {                                                                                                       \
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),                                                           \
                            errmsg("Unsupported partition strategy:%d", (rel)->partMap->type)));                       \
        }                                                                                                              \
    } while (0)

/*
 * search >/>= keyValue partition if direction is true
 * search </<= keyValue partition if direction is false
 */
#define partitionRoutingForValueRange(cxt, keyValue, valueLen, topClosed, direction, result)                           \
    do {                                                                                                               \
        if ((GetPartitionMap(cxt))->type == PART_TYPE_RANGE) {                                                                 \
            (result)->partArea = PART_AREA_RANGE;                                                                      \
        } else if ((GetPartitionMap(cxt))->type == PART_TYPE_INTERVAL) {                                                       \
            Assert((valueLen) == 1);                                                                                   \
            (result)->partArea = PART_AREA_INTERVAL;                                                                   \
        } else {                                                                                                       \
            ereport(ERROR,                                                                                             \
                (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Unsupported partition strategy:%d", (GetPartitionMap(cxt))->type))); \
        }                                                                                                              \
        (result)->partitionId = getRangePartitionOid((GetPartitionMap(cxt)), (keyValue), &((result)->partSeq), topClosed);              \
        if ((result)->partSeq < 0) {                                                                                   \
            (result)->fileExist = false;                                                                               \
        } else {                                                                                                       \
            (result)->fileExist = true;                                                                                \
            RangePartitionMap* partMap = (RangePartitionMap*)(GetPartitionMap(cxt));                                           \
            if (partMap->rangeElements[(result)->partSeq].isInterval && !direction &&                                  \
                !ValueSatisfyLowBoudary(                                                                               \
                    keyValue, &partMap->rangeElements[(result)->partSeq], partMap->intervalValue, topClosed)) {        \
                --((result)->partSeq);                                                                                 \
                (result)->partitionId = partMap->rangeElements[(result)->partSeq].partitionOid;                        \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)

#define partitionRoutingForValueEqual(rel, keyValue, valueLen, topClosed, result)                                      \
    do {                                                                                                               \
        if ((rel)->partMap->type == PART_TYPE_LIST) {                                                                 \
            (result)->partArea = PART_AREA_LIST;                                                                      \
            (result)->partitionId = getListPartitionOid(((rel)->partMap), (keyValue), &((result)->partSeq), topClosed);          \
            if ((result)->partSeq < 0) {                                                                               \
                (result)->fileExist = false;                                                                           \
            } else {                                                                                                   \
                (result)->fileExist = true;                                                                            \
            }                                                                                                          \
        } else if ((rel)->partMap->type == PART_TYPE_HASH) {                                                           \
            (result)->partArea = PART_AREA_HASH;                                                                      \
            (result)->partitionId = getHashPartitionOid(((rel)->partMap), (keyValue), &((result)->partSeq), topClosed);          \
            if ((result)->partSeq < 0) {                                                                               \
                (result)->fileExist = false;                                                                           \
            } else {                                                                                                   \
                (result)->fileExist = true;                                                                            \
            }                                                                                                          \
        } else {                                                                                                       \
            ereport(ERROR,                                                                                             \
                (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Unsupported partition strategy:%d", (rel)->partMap->type))); \
        }                                                                                                              \
    } while (0)

typedef enum PruningResultState { PRUNING_RESULT_EMPTY, PRUNING_RESULT_SUBSET, PRUNING_RESULT_FULL } PruningResultState;

/*
 * PruningBoundary is NULL if pruning result is EMPTY,
 * all min item and max item are NULLs if pruning result is FULL.
 */
typedef struct PruningBoundary {
    PruningResultState state;
    int partitionKeyNum;
    Datum* min;
    bool* minClose;
    Datum* max;
    bool* maxClose;
} PruningBoundary;

/*
 * hash table that index the fakeRelation cache
 */

typedef struct partrelidcachekey {
    Oid  partoid;
    int4 bucketid;
} PartRelIdCacheKey;

typedef struct partrelidcacheent {
    Oid  partoid;
    int4 bucketid;
    Relation reldesc;
    Partition partdesc;
} PartRelIdCacheEnt;

void getFakeReationForPartitionOid(HTAB **fakeRels, MemoryContext cxt, Relation rel, Oid partOid,
                                          Relation *fakeRelation, Partition *partition, LOCKMODE lmode);


#define searchFakeReationForPartitionOid(fakeRels, cxt, rel, partOid, fakeRelation, partition, lmode)              \
    do {                                                                                                           \
        PartRelIdCacheKey _key = {partOid, -1};                                                                    \
        Relation partParentRel = rel;                                                                              \
        Relation partRelForSubPart = NULL;                                                                         \
        if (PointerIsValid(partition)) {                                                                           \
            break;                                                                                                 \
        }                                                                                                          \
        if (RelationIsSubPartitioned(rel) && !RelationIsIndex(rel)) {                                              \
            Oid parentOid = partid_get_parentid(partOid);                                                          \
            if (parentOid != rel->rd_id) {                                                                         \
                Partition partForSubPart = NULL;                                                                   \
                getFakeReationForPartitionOid(&fakeRels, cxt, rel, parentOid, &partRelForSubPart, &partForSubPart, \
                                              lmode);                                                              \
                partParentRel = partRelForSubPart;                                                                 \
            }                                                                                                      \
        }                                                                                                          \
        if (RelationIsNonpartitioned(partParentRel)) {                                                             \
            fakeRelation = NULL;                                                                                   \
            partition = NULL;                                                                                      \
            break;                                                                                                 \
        }                                                                                                          \
        if (PointerIsValid(fakeRels)) {                                                                            \
            FakeRelationIdCacheLookup(fakeRels, _key, fakeRelation, partition);                                    \
            if (!RelationIsValid(fakeRelation)) {                                                                  \
                partition = partitionOpen(partParentRel, partOid, lmode);                                          \
                fakeRelation = partitionGetRelation(partParentRel, partition);                                     \
                FakeRelationCacheInsert(fakeRels, fakeRelation, partition, -1);                                    \
            }                                                                                                      \
        } else {                                                                                                   \
            HASHCTL ctl;                                                                                           \
            errno_t errorno = EOK;                                                                                 \
            errorno = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));                                                 \
            securec_check_c(errorno, "\0", "\0");                                                                  \
            ctl.keysize = sizeof(PartRelIdCacheKey);                                                               \
            ctl.entrysize = sizeof(PartRelIdCacheEnt);                                                             \
            ctl.hash = tag_hash;                                                                                   \
            ctl.hcxt = cxt;                                                                                        \
            fakeRels = hash_create("fakeRelationCache by OID", FAKERELATIONCACHESIZE, &ctl,                        \
                                   HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);                                      \
            partition = partitionOpen(partParentRel, partOid, lmode);                                              \
            fakeRelation = partitionGetRelation(partParentRel, partition);                                         \
            FakeRelationCacheInsert(fakeRels, fakeRelation, partition, -1);                                        \
        }                                                                                                          \
    } while (0)

#define searchHBucketFakeRelation(fakeRels, cxt, rel, bucketid, fakeRelation)                                            \
    do {                                                                                                                 \
        PartRelIdCacheKey _key = {(rel->rd_id), bucketid};                                                               \
        Partition partition;                                                                                             \
        Relation  parentRel = rel;                                                                                       \
        Assert(RELATION_OWN_BUCKET(rel));                                                                                \
        if (bucketid == -1) {                                                                                            \
            ereport(ERROR,                                                                                               \
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),	                                                                 \
            errmsg("Invaild Oid when open hash bucket relation, the target bucket may not exist on current datanode"))); \
        }                                                                                                                \
        if (PointerIsValid(fakeRels)) {                                                                                  \
            FakeRelationIdCacheLookup(fakeRels, _key, fakeRelation, partition);                                          \
            if (!RelationIsValid(fakeRelation)) {                                                                        \
                fakeRelation = bucketGetRelation(parentRel, NULL, bucketid);                                             \
                FakeRelationCacheInsert(fakeRels, fakeRelation, NULL, bucketid);                                         \
            }                                                                                                            \
        } else {                                                                                                         \
            HASHCTL ctl;                                                                                                 \
            errno_t errorno = EOK;                                                                                       \
            errorno = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));                                                       \
            securec_check_c(errorno, "\0", "\0");                                                                        \
            ctl.keysize = sizeof(PartRelIdCacheKey);                                                                     \
            ctl.entrysize = sizeof(PartRelIdCacheEnt);                                                                   \
            ctl.hash = tag_hash;                                                                                         \
            ctl.hcxt = cxt;                                                                                              \
            fakeRels = hash_create("fakeRelationCache by OID",                                                           \
                                    FAKERELATIONCACHESIZE,                                                               \
                                    &ctl,                                                                                \
                                    HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);                                           \
            fakeRelation = bucketGetRelation(parentRel, NULL, bucketid);                                                 \
            FakeRelationCacheInsert(fakeRels, fakeRelation, NULL, bucketid);                                             \
        }                                                                                                                \
    } while (0)

#define FakeRelationIdCacheLookup(CACHE, KEY, RELATION, PARTITION)                      \
    do {                                                                                \
        PartRelIdCacheEnt* hentry;                                                      \
        hentry = (PartRelIdCacheEnt*)hash_search(CACHE, (void*)&(KEY), HASH_FIND, NULL);\
        if (hentry) {                                                                   \
            RELATION = hentry->reldesc;                                                 \
            PARTITION = hentry->partdesc;                                               \
        } else {                                                                        \
            RELATION = NULL;                                                            \
            PARTITION = NULL;                                                           \
        }                                                                               \
    } while (0)

#define FakeRelationCacheInsert(CACHE, RELATION, PARTITION, BUCKET)                                       \
    do {                                                                                                  \
        PartRelIdCacheEnt* idhentry;                                                                      \
        bool found;                                                                                       \
        PartRelIdCacheKey key={(RELATION->rd_id), BUCKET};                                                \
        idhentry = (PartRelIdCacheEnt*)hash_search(CACHE, (void*)&(key), HASH_ENTER, &found);             \
        /* used to give notice if found -- now just keep quiet */                                         \
        idhentry->reldesc = RELATION;                                                                     \
        idhentry->partdesc = PARTITION;                                                                   \
    } while (0)

#define FakeRelationCacheDestroy(HTAB)                                                           \
    do {                                                                                         \
        HASH_SEQ_STATUS status;                                                                  \
        PartRelIdCacheEnt *idhentry;                                                             \
        Relation parentRelation = NULL;                                                          \
        hash_seq_init(&status, (HTAB));                                                          \
        while ((idhentry = (PartRelIdCacheEnt *)hash_seq_search(&status)) != NULL) {             \
            if (idhentry->bucketid == InvalidBktId) {                                            \
                if (PartitionIsTableSubPartition(idhentry->partdesc)) {                          \
                    Oid parentPartOid = idhentry->reldesc->parentId;                             \
                    Oid grandParentRelOid = partid_get_parentid(parentPartOid);                  \
                    Relation grandParentRel = relation_open(grandParentRelOid, NoLock);          \
                    Partition parentPart = partitionOpen(grandParentRel, parentPartOid, NoLock); \
                    Relation parentRelation = partitionGetRelation(grandParentRel, parentPart);  \
                    releaseDummyRelation(&idhentry->reldesc);                                    \
                    partitionClose(parentRelation, idhentry->partdesc, NoLock);                  \
                    releaseDummyRelation(&parentRelation);                                       \
                    partitionClose(grandParentRel, parentPart, NoLock);                          \
                    relation_close(grandParentRel, NoLock);                                      \
                } else {                                                                         \
                    parentRelation = relation_open(idhentry->reldesc->parentId, NoLock);         \
                    releaseDummyRelation(&idhentry->reldesc);                                    \
                    partitionClose(parentRelation, idhentry->partdesc, NoLock);                  \
                    relation_close(parentRelation, NoLock);                                      \
                }                                                                                \
            } else {                                                                             \
                bucketCloseRelation(idhentry->reldesc);                                          \
            }                                                                                    \
        }                                                                                        \
        hash_destroy((HTAB));                                                                    \
        (HTAB) = NULL;                                                                           \
    } while (0)

typedef struct SubPartitionPruningResult {
    NodeTag type;
    int partSeq;
    Bitmapset* bm_selectedSubPartitions;
    List* ls_selectedSubPartitions;
} SubPartitionPruningResult;

typedef struct PruningResult {
    NodeTag type;
    PruningResultState state;
    PruningBoundary* boundary;
    Bitmapset* bm_rangeSelectedPartitions;
    int intervalOffset; /*the same as intervalMinSeq*/
                        /*if interval partitions is empty, intervalOffset=-1*/
    Bitmapset* intervalSelectedPartitions;
    List* ls_rangeSelectedPartitions;
    List* ls_selectedSubPartitions;
    Param* paramArg;
    OpExpr* exprPart;
    Expr* expr;
    /* This variable applies only to single-partition key range partition tables in PBE mode. */
    bool isPbeSinlePartition = false;
} PruningResult;

extern Oid partIDGetPartOid(Relation relation, PartitionIdentifier* partID);
extern PartitionIdentifier* partOidGetPartID(Relation rel, Oid partOid);

extern void RebuildPartitonMap(PartitionMap* oldMap, PartitionMap* newMap);
extern void RebuildRangePartitionMap(RangePartitionMap* oldMap, RangePartitionMap* newMap);

bool isPartKeyValuesInPartition(RangePartitionMap* partMap, Const** partKeyValues, int partkeyColumnNum, int partSeq);

extern int comparePartitionKey(RangePartitionMap* partMap, Const** values1, Const** values2, int partKeyNum);

extern int lookupHBucketid(oidvector *buckets, int low, int2 bucket_id);

extern int2 computeTupleBucketId(Relation rel, HeapTuple tuple);

extern Oid GetNeedDegradToRangePartOid(Relation rel, Oid partOid);
extern RangeElement* CopyRangeElementsWithoutBoundary(const RangeElement* src, int elementNum);
extern char* ReadIntervalStr(HeapTuple tuple, TupleDesc tupleDesc);
extern oidvector* ReadIntervalTablespace(HeapTuple tuple, TupleDesc tupleDesc);
int ValueCmpLowBoudary(Const** partKeyValue, const RangeElement* partition, Interval* intervalValue);
extern void get_typlenbyval(Oid typid, int16 *typlen, bool *typbyval);
extern RangeElement* copyRangeElements(RangeElement* src, int elementNum, int partkeyNum);
extern int rangeElementCmp(const void* a, const void* b);
extern int HashElementCmp(const void* a, const void* b);
extern void DestroyListElements(ListPartElement* src, int elementNum);
extern void PartitionMapDestroyHashArray(HashPartElement* hashArray, int arrLen);
extern void partitionMapDestroyRangeArray(RangeElement* rangeArray, int arrLen);
extern void RelationDestroyPartitionMap(PartitionMap* partMap);

#endif /* PARTITIONMAP_GS_H_ */
