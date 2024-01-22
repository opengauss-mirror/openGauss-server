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
    Const consts[MAX_PARTKEY_NUMS];
    Const *values[MAX_PARTKEY_NUMS];
} PartitionIdentifier;

/*
 * partition map is used to  find which partition a record is mapping to.
 * and  pruning the unused partition when querying.the map has two part:
 * and  range part and interval part.
 * range part is a array of RangeElement which is sorted by RangeElement
 */
typedef struct RangeElement {
    Oid partitionOid;                        /* the oid of partition */
    int partitionno;                         /* the partitionno of partition */
    int len;                                 /* the length of partition key number */
    Const* boundary[MAX_RANGE_PARTKEY_NUMS];    /* upper bond of partition */
    bool isInterval;                         /* is interval partition */
} RangeElement;

typedef struct ListPartElement {
    Oid partitionOid;                     /* the oid of partition */
    int partitionno;                      /* the partitionno of partition */
    int len;                              /* the length of values */
    PartitionKey* boundary;
} ListPartElement;

typedef struct HashPartElement {
    Oid partitionOid;                     /* the oid of partition */
    int partitionno;                      /* the partitionno of partition */
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
    PartitionMap base;
    /*section 1: range partition specific*/
    int rangeElementsNum;          /* the number of range partition*/
    RangeElement* rangeElements;   /* array of RangeElement */
    Interval* intervalValue;       /* valid for interval partition */
    oidvector* intervalTablespace; /* valid for interval partition */
} RangePartitionMap;

#define IS_NULL_CONST(x) (x->constisnull)

typedef struct PartEntryKey {
    Oid parentRelOid;
    PartitionKey partKey;
} PartEntryKey;

typedef struct PartElementHashEntry {
    /* hash table's KEY part */
    PartEntryKey key;

    /* value part */
    Oid rootRelOid; /* Oid entry in pg_class, equal to key.parentRelOid in none-subpartition case */
    Oid partRelOid;
    int partSeq;

    /* confilct case to lookup party entry */
    struct PartElementHashEntry *next;
} ListElementHashEntry;

#define INVALID_PARTREL_OID   InvalidOid
#define INVALID_PARTREL_SEQNO -1
extern Const **transformConstIntoPartkeyType(FormData_pg_attribute* attrs, int2vector* partitionKey, Const **boundary,
    int len);

typedef struct ListPartitionMap {
    PartitionMap base;
    /* section 1: list partition specific */
    int listElementsNum;          /* the number of list partition */
    ListPartElement* listElements;   /* array of listElement */

    /* list value to partRelOid */
    HTAB *ht;
    Oid   defaultPartRelOid;
    int   defaultPartSeqNo;
} ListPartitionMap;

typedef struct HashPartitionMap {
    PartitionMap base;
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

extern void InsertPartKeyHashTable(ListPartitionMap *listMap, ListPartElement *partElem, int partSeqNo);
extern HTAB *BuildPartKeyHashTable(ListPartitionMap *listMap);
extern bool IsDefaultValueListPartition(ListPartitionMap *listMap, ListPartElement *partElem);
char *PartKeyGetCstring(PartitionKey* partkeys);
extern bool ConstEqual(Const *c1, Const *c2);
extern char *PartKeyGetCstring(Const* c);
void partitionRoutingForTuple(Relation rel, void *tuple, PartitionIdentifier *partIdentfier, bool canIgnore,
                              bool partExprKeyIsNull);
void partitionRoutingForValue(Relation rel, Const **keyValue, int valueLen, bool topClosed, bool missIsOk,
                              PartitionIdentifier *result);

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

#define partitionRoutingForValueEqual(rel, keyValue, valueLen, topClosed, result)                               \
    do {                                                                                                        \
        (keyValue) = transformConstIntoPartkeyType(((rel)->rd_att->attrs), GetPartitionKey((rel)->partMap),     \
                                                   (keyValue), (valueLen));                                     \
        if ((rel)->partMap->type == PART_TYPE_LIST) {                                                           \
            (result)->partArea = PART_AREA_LIST;                                                                \
            (result)->partitionId =                                                                             \
                getListPartitionOid(((rel)->partMap), (keyValue), (valueLen), &((result)->partSeq));            \
            if ((result)->partSeq < 0) {                                                                        \
                (result)->fileExist = false;                                                                    \
            } else {                                                                                            \
                (result)->fileExist = true;                                                                     \
            }                                                                                                   \
        } else if ((rel)->partMap->type == PART_TYPE_HASH) {                                                    \
            (result)->partArea = PART_AREA_HASH;                                                                \
            (result)->partitionId = getHashPartitionOid(((rel)->partMap), (keyValue), &((result)->partSeq));    \
            if ((result)->partSeq < 0) {                                                                        \
                (result)->fileExist = false;                                                                    \
            } else {                                                                                            \
                (result)->fileExist = true;                                                                     \
            }                                                                                                   \
        } else {                                                                                                \
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),                                                    \
                            errmsg("Unsupported partition strategy:%d", (rel)->partMap->type)));                \
        }                                                                                                       \
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


/* search fake relation with partOid. The partitionno is used to retry search. In some cases we don't need partitionno,
 * such as index search/ddl operation, just input INVALID_PARTITION_NO */
#define searchFakeReationForPartitionOid(fakeRels, cxt, rel, partOid, partitionno, fakeRelation, partition, lmode) \
    do {                                                                                                           \
        PartRelIdCacheKey _key = {partOid, -1};                                                                    \
        Relation partParentRel = rel;                                                                              \
        Relation partRelForSubPart = NULL;                                                                         \
        if (PointerIsValid(partition)) {                                                                           \
            break;                                                                                                 \
        }                                                                                                          \
        if (RelationIsSubPartitioned(rel) && !RelationIsIndex(rel)) {                                              \
            Oid parentOid = partid_get_parentid(partOid);                                                          \
            if (!OidIsValid(parentOid)) {                                                                          \
                ereport(ERROR,                                                                                     \
                        (errcode(ERRCODE_PARTITION_ERROR),                                                         \
                         errmsg("partition %u does not exist on relation \"%s\" when search the fake relation",    \
                                partOid, RelationGetRelationName(rel)),                                            \
                         errdetail("this partition may have already been dropped")));                              \
            }                                                                                                      \
            if (parentOid != rel->rd_id) {                                                                         \
                Partition partForSubPart = NULL;                                                                   \
                getFakeReationForPartitionOid                                                                      \
                    (&fakeRels, cxt, rel, parentOid, &partRelForSubPart, &partForSubPart, lmode);                  \
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
                partition = PartitionOpenWithPartitionno(partParentRel, partOid, partitionno, lmode);              \
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
            partition = PartitionOpenWithPartitionno(partParentRel, partOid, partitionno, lmode);                  \
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
    int partitionno;
    Bitmapset* bm_selectedSubPartitions;
    List* ls_selectedSubPartitions;
    List* ls_selectedSubPartitionnos;
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
    List* ls_selectedPartitionnos;
    List* ls_selectedSubPartitions;
    Param* paramArg;
    OpExpr* exprPart;
    Expr* expr;
    /* This variable applies only to single-partition key range partition tables in PBE mode. */
    bool isPbeSinlePartition = false;
} PruningResult;

extern bool ValueSatisfyLowBoudary(Const** partKeyValue, RangeElement* partition, Interval* intervalValue,
    bool topClosed);
extern int2vector* GetPartitionKey(const PartitionMap* partMap);

extern Oid partIDGetPartOid(Relation relation, PartitionIdentifier* partID);
extern PartitionIdentifier* partOidGetPartID(Relation rel, Oid partOid);

extern void RebuildPartitonMap(PartitionMap* oldMap, PartitionMap* newMap);
extern void RebuildRangePartitionMap(RangePartitionMap* oldMap, RangePartitionMap* newMap);
extern bool EqualPartitonMap(const PartitionMap* partMap1, const PartitionMap* partMap2);

bool isPartKeyValuesInPartition(RangePartitionMap* partMap, Const** partKeyValues, int partkeyColumnNum, int partSeq);

extern int comparePartitionKey(RangePartitionMap* partMap, Const** partkey_value, Const** partkey_bound, int partKeyNum);

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
extern int ListElementCmp(const void* a, const void* b);
extern int HashElementCmp(const void* a, const void* b);
extern void DestroyListElements(ListPartElement* src, int elementNum);
extern void PartitionMapDestroyHashArray(HashPartElement* hashArray, int arrLen);
extern void partitionMapDestroyRangeArray(RangeElement* rangeArray, int arrLen);
extern void DestroyPartitionMap(PartitionMap* partMap);
/* search fake relation with partOid, if no need partitionno, just input 0 */
extern bool trySearchFakeReationForPartitionOid(HTAB** fakeRels, MemoryContext cxt, Relation rel, Oid partOid,
    int partitionno, Relation* fakeRelation, Partition* partition, LOCKMODE lmode, bool checkSubPart = true);

/* partitoin map copy functions */
extern ListPartitionMap *CopyListPartitionMap(ListPartitionMap *src_lpm);
/* more! other hash/range and its underlaying element data structores will add here later */

#endif /* PARTITIONMAP_GS_H_ */
