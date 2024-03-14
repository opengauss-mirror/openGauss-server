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
 * -------------------------------------------------------------------------
 * File Name	: partrouting.cpp
 * Target		: data partition
 * Brief		:
 * Description	:
 * History	:
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/partition/partrouting.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "executor/executor.h"
#include "nodes/value.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/catcache.h"
#include "utils/syscache.h"
#include "utils/array.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/datetime.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "utils/partitionkey.h"
#include "utils/date.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "fmgr.h"
#include "utils/memutils.h"
#include "utils/datum.h"
#include "utils/knl_relcache.h"

/*****************************************************************************
 *
 *	   partition routing entry point
 *
 * To support tuple to partitionRel mapping mechanism
 *****************************************************************************/
/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: get special table partition for a tuple
 *			: create a partition if necessary
 * Description	:
 * Notes		:
 */
Oid heapTupleGetPartitionOid(Relation rel, void *tuple, int *partitionno, bool isDDL, bool canIgnore, bool partExprKeyIsNull)
{
    Oid partitionOid = InvalidOid;
    
    /* get routing result */
    partitionRoutingForTuple(rel, tuple, u_sess->catalog_cxt.route, canIgnore, partExprKeyIsNull);

    /* if the partition exists, return partition's oid */
    if (u_sess->catalog_cxt.route->fileExist) {
        Assert(OidIsValid(u_sess->catalog_cxt.route->partitionId));
        partitionOid = u_sess->catalog_cxt.route->partitionId;
        if (PointerIsValid(partitionno)) {
            *partitionno = GetPartitionnoFromSequence(rel->partMap, u_sess->catalog_cxt.route->partSeq);
        }
        return partitionOid;
    }

    /*
     * feedback for non-existing table partition.
     *   If the routing result indicates a range partition, give error report
     */
    int level = canIgnore ? WARNING : ERROR;
    switch (u_sess->catalog_cxt.route->partArea) {
        /*
         * If it is a range partition, give error report
         */
        case PART_AREA_INTERVAL: {
            return AddNewIntervalPartition(rel, tuple, partitionno, isDDL);
        } break;
        case PART_AREA_RANGE:
        case PART_AREA_LIST:
        case PART_AREA_HASH: {
            ereport(
                level,
                (errcode(ERRCODE_NO_DATA_FOUND), errmsg("inserted partition key does not map to any table partition")));
        } break;
        /* never happen; just to be self-contained */
        default: {
            ereport(
                level,
                (errcode(ERRCODE_NO_DATA_FOUND), errmsg("Inserted partition key does not map to any table partition"),
                 errdetail("Unrecognized PartitionArea %d", u_sess->catalog_cxt.route->partArea)));
        } break;
    }

    return partitionOid;
}

Oid heapTupleGetSubPartitionOid(Relation rel, void *tuple, EState* estate, TupleTableSlot* slot)
{
    Oid partitionId = InvalidOid;
    Oid subPartitionId = InvalidOid;
    int partitionno = INVALID_PARTITION_NO;
    Partition part = NULL;
    Relation partRel = NULL;
    /* get partititon oid for the record */
    partitionId = getPartitionIdFromTuple(rel, tuple, estate, slot, NULL);
    part = PartitionOpenWithPartitionno(rel, partitionId, partitionno, RowExclusiveLock);
    partRel = partitionGetRelation(rel, part);
    /* get subpartititon oid for the record */
    subPartitionId = getPartitionIdFromTuple(partRel, tuple, estate, slot, NULL);
    
    releaseDummyRelation(&partRel);
    partitionClose(rel, part, RowExclusiveLock);

    return subPartitionId;
}

void partitionRoutingForTuple(Relation rel, void *tuple, PartitionIdentifier *partIdentfier, bool canIgnore,
                              bool partExprKeyIsNull)
{
    TupleDesc tuple_desc = NULL;
    int2vector *partkey_column = NULL;
    int partkey_column_n = 0;
    static THR_LOCAL Const consts[MAX_PARTKEY_NUMS];
    static THR_LOCAL Const *values[MAX_PARTKEY_NUMS];
    bool isnull = false;
    bool is_ustore = RelationIsUstoreFormat(rel);
    Datum column_raw;
    int i = 0;
    partkey_column = GetPartitionKey((rel)->partMap);
    partkey_column_n = partkey_column->dim1;
    tuple_desc = (rel)->rd_att;
    for (i = 0; i < partkey_column_n; i++) {
        isnull = false;
        if (partExprKeyIsNull) {
            column_raw = (is_ustore)
                             ? UHeapFastGetAttr((UHeapTuple)(tuple), partkey_column->values[i], tuple_desc, &isnull)
                             : fastgetattr((HeapTuple)(tuple), partkey_column->values[i], tuple_desc, &isnull);
            values[i] = transformDatum2Const((rel)->rd_att, partkey_column->values[i], column_raw, isnull, &consts[i]);
        } else {
            values[i] = transformDatum2ConstForPartKeyExpr((rel)->partMap, (PartKeyExprResult*)tuple, &consts[i]);
        }
    }
    if (PartitionMapIsInterval((rel)->partMap) && values[0]->constisnull) {
        if (canIgnore) { /* treat type as PART_TYPE_RANGE because PART_TYPE_INTERVAL will create a new partition. \ this
                          * will be handled by caller and directly return */
            (partIdentfier)->partArea = PART_AREA_RANGE;
            (partIdentfier)->fileExist = false;
            (partIdentfier)->partitionId = InvalidOid;
            return;
        }
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("inserted partition key does not map to any partition"),
                        errdetail("inserted partition key cannot be NULL for interval-partitioned table")));
    }
    partitionRoutingForValue((rel), values, partkey_column_n, true, false, partIdentfier);
}

void partitionRoutingForValue(Relation rel, Const **keyValue, int valueLen, bool topClosed, bool missIsOk,
                              PartitionIdentifier *result)
{
    if ((rel)->partMap->type == PART_TYPE_RANGE || (rel)->partMap->type == PART_TYPE_INTERVAL) {
        if ((rel)->partMap->type == PART_TYPE_RANGE) {
            (result)->partArea = PART_AREA_RANGE;
        } else {
            Assert((valueLen) == 1);
            (result)->partArea = PART_AREA_INTERVAL;
        }
        (result)->partitionId = getRangePartitionOid((rel)->partMap, (keyValue), &((result)->partSeq), topClosed);
        if ((result)->partSeq < 0) {
            (result)->fileExist = false;
        } else {
            (result)->fileExist = true;
            RangePartitionMap *partMap = (RangePartitionMap *)((rel)->partMap);
            if (partMap->rangeElements[(result)->partSeq].isInterval &&
                !ValueSatisfyLowBoudary(keyValue, &partMap->rangeElements[(result)->partSeq], partMap->intervalValue,
                                        topClosed)) {
                (result)->partSeq = -1;
                (result)->partitionId = InvalidOid;
                (result)->fileExist = false;
            }
        }
    } else if ((rel)->partMap->type == PART_TYPE_LIST) {
        (result)->partArea = PART_AREA_LIST;
        (result)->partitionId =
            getListPartitionOid(((rel)->partMap), (keyValue), (valueLen), &((result)->partSeq));
        if ((result)->partSeq < 0) {
            (result)->fileExist = false;
        } else {
            (result)->fileExist = true;
        }
    } else if ((rel)->partMap->type == PART_TYPE_HASH) {
        (result)->partArea = PART_AREA_HASH;
        (result)->partitionId = getHashPartitionOid(rel->partMap, keyValue, &result->partSeq);
        if ((result)->partSeq < 0) {
            (result)->fileExist = false;
        } else {
            (result)->fileExist = true;
        }
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Unsupported partition strategy:%d", (rel)->partMap->type)));
    }
}

/*
 * @@GaussDB@@
 * Brief		:
 * Description	:give the tuple, return oid of the corresponding partition
 * Notes		:
 */
Oid getRangePartitionOid(PartitionMap *partitionmap, Const** partKeyValue, int32* partSeq, bool topClosed)
{
    RangeElement* rangeElementIterator = NULL;
    RangePartitionMap* rangePartMap = NULL;
    Oid result = InvalidOid;
    int keyNums;
    int hit = -1;
    int min_part_id = 0;
    int max_part_id = 0;
    int local_part_id = 0;
    int compare = 0;
    Const** boundary = NULL;
    
    Assert(PointerIsValid(partitionmap));
    Assert(PointerIsValid(partKeyValue));

    incre_partmap_refcount(partitionmap);
    rangePartMap = (RangePartitionMap*)(partitionmap);

    keyNums = rangePartMap->base.partitionKey->dim1;
    max_part_id = rangePartMap->rangeElementsNum - 1;
    boundary = rangePartMap->rangeElements[max_part_id].boundary;
    partitionKeyCompareForRouting(partKeyValue, boundary, (uint32)keyNums, compare);

    if (compare == 0) {
        if (topClosed) {
            hit = -1;
        } else {
            hit = max_part_id;
        }
    } else if (compare > 0) {
        hit = -1;
    } else {
        /* semi-seek */
        while (max_part_id > min_part_id) {
            local_part_id = (uint32)(max_part_id + min_part_id) >> 1;
            rangeElementIterator = &(rangePartMap->rangeElements[local_part_id]);

            boundary = rangeElementIterator->boundary;
            partitionKeyCompareForRouting(partKeyValue, boundary, (uint32)keyNums, compare);

            if (compare == 0) {
                hit = local_part_id;

                if (topClosed) {
                    hit += 1;
                }
                break;
            } else if (compare > 0) {
                min_part_id = local_part_id + 1;
            } else {
                max_part_id = local_part_id;
            }
        }
        /* get it */
        if (max_part_id == min_part_id) {
            hit = max_part_id;
        }
    }

    if (PointerIsValid(partSeq)) {
        *partSeq = hit;
    }

    if (hit >= 0) {
        result = rangePartMap->rangeElements[hit].partitionOid;
    }

    decre_partmap_refcount(partitionmap);
    return result;
}

Oid getListPartitionOid(PartitionMap *partMap, Const **partKeyValue, int partKeyCount, int32 *partSeq)
{
    Assert(PointerIsValid(partMap));
    Assert(PointerIsValid(partKeyValue));
    
    ListPartitionMap* listMap = (ListPartitionMap *)partMap;
    Oid result = InvalidOid;
    
    incre_partmap_refcount(partMap);

    PartEntryKey key;
    key.parentRelOid = listMap->base.relOid;
    key.partKey.values = partKeyValue;
    key.partKey.count = partKeyCount;
    bool found = false;
    PartElementHashEntry *entry = (PartElementHashEntry *)hash_search(listMap->ht, &key, HASH_FIND, &found);

    if (found) {
        /* found targe value from the hash bucket list */
        while (entry != NULL) {
            if (ListPartKeyCompare(&key.partKey, &entry->key.partKey) == 0) {
                break;
            }

            entry = entry->next;
        }

        /* found the target partition othewise set the value */
        if (entry != NULL) {
            *partSeq = entry->partSeq;
            result = entry->partRelOid;
        } else {
            *partSeq = listMap->defaultPartSeqNo;
            result = listMap->defaultPartRelOid;
        }
    } else {
        /* target partition is not found case check if we have default partition */
        *partSeq = listMap->defaultPartSeqNo;
        result = listMap->defaultPartRelOid;
    }

    decre_partmap_refcount(partMap);
    return result;
}

/* the 2nd parameter must be partition boundary */
void partitionKeyCompareForRouting(Const **partkey_value, Const **partkey_bound, uint32 partKeyColumnNum, int &compare)
{
    uint32 i = 0;
    Const *kv = NULL;
    Const *bv = NULL;
    for (; i < partKeyColumnNum; i++) {
        kv = *((partkey_value) + i);
        bv = *(partkey_bound + i);
        if (kv == NULL || bv == NULL) {
            if (kv == NULL && bv == NULL) {
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("NULL can not be compared with NULL")));
            } else if (kv == NULL) {
                compare = -1;
            } else {
                compare = 1;
            }
            break;
        }
        if (constIsMaxValue(kv) || constIsMaxValue(bv)) {
            if (constIsMaxValue(kv) && constIsMaxValue(bv)) {
                compare = 0;
                continue;
            } else if (constIsMaxValue(kv)) {
                compare = 1;
            } else {
                compare = -1;
            }
            break;
        }
        if (kv->constisnull || bv->constisnull) {
            if (kv->constisnull && bv->constisnull) {
                ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                errmsg("null value can not be compared with null value.")));
            } else if (kv->constisnull) {
                compare = DB_IS_CMPT(B_FORMAT) ? -1 : 1;
            } else {
                compare = DB_IS_CMPT(B_FORMAT) ? 1 : -1;
            }
            break;
        }
        constCompare(kv, bv, bv->constcollid, compare);
        if ((compare) != 0) {
            break;
        }
    }
}

Oid getHashPartitionOid(PartitionMap* partMap, Const** partKeyValue, int32* partSeq)
{
    HashPartitionMap* hashPartMap = NULL;
    Oid result = InvalidOid;
    int keyNums = 0;
    int hit = -1;
    
    Assert(PointerIsValid(partMap));
    Assert(PointerIsValid(partKeyValue));

    incre_partmap_refcount(partMap);
    hashPartMap = (HashPartitionMap*)(partMap);

    keyNums = hashPartMap->base.partitionKey->dim1;
    
    int i = 0;
    uint32 hash_value = 0;
    while (i < keyNums) {
        if (partKeyValue[i]->constisnull) {
            if (PointerIsValid(partSeq)) {
                *partSeq = hit;
            }
            decre_partmap_refcount(partMap);
            return result;
        }
        hash_value = hashValueCombination(hash_value, partKeyValue[i]->consttype, partKeyValue[i]->constvalue,
                                          false, LOCATOR_TYPE_HASH, partKeyValue[i]->constcollid);
        i++;
    }

    hit = hash_value % (uint32)(hashPartMap->hashElementsNum);
    hit = hashPartMap->hashElementsNum - hit - 1;

    if (PointerIsValid(partSeq)) {
        *partSeq = hit;
    }

    if (hit >= 0) {
        result = hashPartMap->hashElements[hit].partitionOid;
    }

    decre_partmap_refcount(partMap);
    return result;
}
