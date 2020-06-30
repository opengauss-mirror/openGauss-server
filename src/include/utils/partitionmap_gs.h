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
#include "catalog/pg_type.h"
#include "nodes/primnodes.h"
#include "storage/lock.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/partcache.h"
#include "utils/partitionmap.h"

typedef enum PartitionArea { PART_AREA_NONE = 0, PART_AREA_RANGE, PART_AREA_INTERVAL } PartitionArea;

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
} RangeElement;

typedef struct IntervalElement {
    Oid partitionOid; /*the oid of partition*/
    int sequenceNum;  /*the logic number of interval partition.*/
} IntervalElement;

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
    int rangeElementsNum;        /*the number of range partition*/
    RangeElement* rangeElements; /*array of RangeElement*/
} RangePartitionMap;

/*
 * describe partition info of Interval Partitioned-Table
 * intervalElements: starting from min existing interval partition, to max existing interval partition.
                and holes for un-created between them
 * sequenceMap: bitmap representation of intervalElements
 */
typedef struct IntervalPartitionMap {
    RangePartitionMap rangePartitionMap;
    /*section 2: interval partition specific*/
    int intervalElementsNum;
    IntervalElement* intervalElements; /*array of IntervalElement, it's bitmap represetation is sequenceMap belowing*/
    Datum transitionPoint;             /*the low boundary of interval part*/
    Datum intervalValue;
    int intervalMaxSeq;     /*sequence number of last interval element in intervalElements*/
    int intervalMinSeq;     /*sequence number of first interval element in intervalElements*/
    Bitmapset* sequenceMap; /*a bit map represent which interval partition is created*/
    bool transitionPointByVal;
    bool intervalValuePointByVal;
} IntervalPartitionMap;

#define PartitionkeyTypeGetIntervalType(type)                                          \
    do {                                                                               \
        if (DATEOID == (type) || TIMESTAMPOID == (type) || TIMESTAMPTZOID == (type)) { \
            (type) = INTERVALOID;                                                      \
        }                                                                              \
    } while (0)

#define partitionRoutingForTuple(rel, tuple, partIdentfier)                                                     \
    do {                                                                                                        \
        TupleDesc tuple_desc = NULL;                                                                            \
        int2vector* partkey_column = NULL;                                                                      \
        int partkey_column_n = 0;                                                                               \
        static THR_LOCAL Const consts[RANGE_PARTKEYMAXNUM];                                                     \
        static THR_LOCAL Const* values[RANGE_PARTKEYMAXNUM];                                                    \
        bool isnull = false;                                                                                    \
        Datum column_raw;                                                                                       \
        int i = 0;                                                                                              \
        partkey_column = ((RangePartitionMap*)(rel)->partMap)->partitionKey;                                    \
        partkey_column_n = partkey_column->dim1;                                                                \
        tuple_desc = (rel)->rd_att;                                                                             \
        for (i = 0; i < partkey_column_n; i++) {                                                                \
            isnull = false;                                                                                     \
            column_raw = fastgetattr((tuple), partkey_column->values[i], tuple_desc, &isnull);                  \
            values[i] =                                                                                         \
                transformDatum2Const((rel)->rd_att, partkey_column->values[i], column_raw, isnull, &consts[i]); \
        }                                                                                                       \
        if (PartitionMapIsInterval((rel)->partMap) && values[0]->constisnull) {                                 \
            ereport(ERROR,                                                                                      \
                (errcode(ERRCODE_INTERNAL_ERROR),                                                               \
                    errmsg("inserted partition key does not map to any partition"),                             \
                    errdetail("inserted partition key cannot be NULL for interval-partitioned table")));        \
        }                                                                                                       \
        partitionRoutingForValue((rel), values, partkey_column_n, true, false, (partIdentfier));                \
    } while (0)

#define partitionRoutingForValue(rel, keyValue, valueLen, topClosed, missIsOk, result)                                 \
    do {                                                                                                               \
        if ((rel)->partMap->type == PART_TYPE_RANGE) {                                                                 \
            (result)->partArea = PART_AREA_RANGE;                                                                      \
            (result)->partitionId = getRangePartitionOid((rel), (keyValue), &((result)->partSeq), topClosed);          \
            if ((result)->partSeq < 0) {                                                                               \
                (result)->fileExist = false;                                                                           \
            } else {                                                                                                   \
                (result)->fileExist = true;                                                                            \
            }                                                                                                          \
        } else if (rel->partMap->type == PART_TYPE_INTERVAL) {                                                         \
            Assert((valueLen) == 1);                                                                                   \
            (result)->partitionId = getIntervalPartitionOid((IntervalPartitionMap*)((rel)->partMap),                   \
                (keyValue),                                                                                            \
                &((result)->partSeq),                                                                                  \
                &((result)->partArea),                                                                                 \
                topClosed,                                                                                             \
                missIsOk);                                                                                             \
            if (OidIsValid((result)->partitionId)) {                                                                   \
                (result)->fileExist = true;                                                                            \
            } else {                                                                                                   \
                (result)->fileExist = false;                                                                           \
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

#define searchFakeReationForPartitionOid(fakeRels, cxt, rel, partOid, fakeRelation, partition, lmode)               \
    do {                                                                                                            \
        PartRelIdCacheKey _key = {partOid, -1};                                                                     \
        if (PointerIsValid(partition)) {                                                                            \
            break;                                                                                                  \
        }                                                                                                           \
        if (RelationIsNonpartitioned(rel)) {                                                                        \
            fakeRelation = NULL;                                                                                    \
            partition = NULL;                                                                                       \
            break;                                                                                                  \
        }                                                                                                           \
        if (PointerIsValid(fakeRels)) {                                                                             \
            FakeRelationIdCacheLookup(fakeRels, _key, fakeRelation, partition);                                     \
            if (!RelationIsValid(fakeRelation)) {                                                                   \
                partition = partitionOpen(rel, partOid, lmode);                                                     \
                fakeRelation = partitionGetRelation(rel, partition);                                                \
                FakeRelationCacheInsert(fakeRels, fakeRelation, partition, -1);                                     \
            }                                                                                                       \
        } else {                                                                                                    \
            HASHCTL ctl;                                                                                            \
            errno_t errorno = EOK;                                                                                  \
            errorno = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));                                                  \
            securec_check_c(errorno, "\0", "\0");                                                                   \
            ctl.keysize = sizeof(PartRelIdCacheKey);                                                                \
            ctl.entrysize = sizeof(PartRelIdCacheEnt);                                                              \
            ctl.hash = tag_hash;                                                                                    \
            ctl.hcxt = cxt;                                                                                         \
            fakeRels = hash_create(                                                                                 \
                "fakeRelationCache by OID", FAKERELATIONCACHESIZE, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT); \
            partition = partitionOpen(rel, partOid, lmode);                                                         \
            fakeRelation = partitionGetRelation(rel, partition);                                                    \
            FakeRelationCacheInsert(fakeRels, fakeRelation, partition, -1);                                         \
        }                                                                                                           \
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

#define FakeRelationCacheDestroy(HTAB)                                              \
    do {                                                                            \
        HASH_SEQ_STATUS status;                                                     \
        PartRelIdCacheEnt* idhentry;                                                \
        Relation parentRelation = NULL;                                             \
        hash_seq_init(&status, (HTAB));                                             \
        while ((idhentry = (PartRelIdCacheEnt*)hash_seq_search(&status)) != NULL) { \
            if (idhentry->bucketid == InvalidBktId) {                               \
                parentRelation = relation_open(idhentry->reldesc->parentId, NoLock);\
                releaseDummyRelation(&idhentry->reldesc);                           \
                partitionClose(parentRelation, idhentry->partdesc, NoLock);         \
                relation_close(parentRelation, NoLock);                             \
            } else {                                                                \
                bucketCloseRelation(idhentry->reldesc);                             \
            }                                                                       \
        }                                                                           \
        hash_destroy((HTAB));                                                       \
        (HTAB) = NULL;                                                              \
    } while (0)

typedef struct PruningResult {
    NodeTag type;
    PruningResultState state;
    PruningBoundary* boundary;
    Bitmapset* bm_rangeSelectedPartitions;
    int intervalOffset; /*the same as intervalMinSeq*/
                        /*if interval partitions is empty, intervalOffset=-1*/
    Bitmapset* intervalSelectedPartitions;
    List* ls_rangeSelectedPartitions;
} PruningResult;

extern Oid partIDGetPartOid(Relation relation, PartitionIdentifier* partID);
extern PartitionIdentifier* partOidGetPartID(Relation rel, Oid partOid);

extern void RebuildPartitonMap(PartitionMap* oldMap, PartitionMap* newMap);
extern Oid getIntervalPartitionOid(IntervalPartitionMap* intervalPartMap, Const** partKeyValue, int32* partIndex,
    PartitionArea* partArea, bool topClosed, bool missIsOk);
bool isPartKeyValuesInPartition(RangePartitionMap* partMap, Const** partKeyValues, int partkeyColumnNum, int partSeq);

extern int comparePartitionKey(RangePartitionMap* partMap, Const** values1, Const** values2, int partKeyNum);

extern int lookupHBucketid(oidvector *buckets, int low, int2 bucket_id);

extern int2 computeTupleBucketId(Relation rel, HeapTuple tuple);
		
#endif /* PARTITIONMAP_GS_H_ */
