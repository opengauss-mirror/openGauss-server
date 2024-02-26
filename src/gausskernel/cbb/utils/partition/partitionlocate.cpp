/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021 All rights reserved.
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
 * File Name	: partitionlocate.cpp
 * Target		: data partition
 * Brief		:
 * Description	:
 * History	:
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/partition/partitionlocate.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/tableam.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "utils/partitionkey.h"

bool isPartKeyValuesInListPartition(
    ListPartitionMap *partMap, Const **partKeyValues, const int partkeyColumnNum, const int partSeq)
{
    Assert(partMap && partKeyValues);
    Assert(partkeyColumnNum == partMap->base.partitionKey->dim1);

    int sourcePartSeq = -1;
    Oid sourceOid = getListPartitionOid(&partMap->base, partKeyValues, partkeyColumnNum, &sourcePartSeq);
    if (sourcePartSeq < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Can't find list partition oid when checking tuple is in the partition.")));
    }

    Oid targetOid = partMap->listElements[partSeq].partitionOid;
    if (sourceOid == targetOid) {
        return true;
    } else {
        return false;
    }
}

bool isPartKeyValuesInHashPartition(Relation partTableRel, const HashPartitionMap *partMap, Const **partKeyValues,
    const int partkeyColumnNum, const int partSeq)
{
    Assert(partMap && partKeyValues);
    Assert(partkeyColumnNum == partMap->base.partitionKey->dim1);

    int sourcePartSeq = -1;
    Oid sourceOid = getHashPartitionOid(partTableRel->partMap, partKeyValues, &sourcePartSeq);
    if (sourcePartSeq < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Can't find hash partition oid when checking tuple is in the partition.")));
    }

    Oid targetOid = partMap->hashElements[partSeq].partitionOid;
    if (sourceOid == targetOid) {
        return true;
    } else {
        return false;
    }
}

static bool checkTupleIsInListPartition(Relation partTableRel, int partSeq, TupleDesc tupleDesc, void* tuple)
{
    int2vector* partkeyColumns = NULL;
    int partkeyColumnNum = 0;
    int i = 0;
    Const* tuplePartKeyValues[MAX_LIST_PARTKEY_NUMS];
    Const consts[MAX_LIST_PARTKEY_NUMS];
    Datum tuplePartKeyValue;
    bool isNull = false;
    bool isInPartition = false;
    ListPartitionMap *partMap = NULL;

    if (partSeq >= ((ListPartitionMap *)(partTableRel->partMap))->listElementsNum) {
        return false;
    }

    incre_partmap_refcount(partTableRel->partMap);
    partMap = (ListPartitionMap *)(partTableRel->partMap);

    partkeyColumns = partMap->base.partitionKey;
    partkeyColumnNum = partkeyColumns->dim1;

    for (i = 0; i < partkeyColumnNum; i++) {
        isNull = false;
        tuplePartKeyValue = tableam_tops_tuple_getattr(tuple, (int)partkeyColumns->values[i], tupleDesc, &isNull);
        tuplePartKeyValues[i] =
            transformDatum2Const(tupleDesc, partkeyColumns->values[i], tuplePartKeyValue, isNull, &consts[i]);
    }

    isInPartition = isPartKeyValuesInListPartition(partMap, tuplePartKeyValues, partkeyColumnNum, partSeq);

    decre_partmap_refcount(partTableRel->partMap);

    return isInPartition;
}

static bool checkTupleIsInHashPartition(Relation partTableRel, int partSeq, TupleDesc tupleDesc, void* tuple)
{
    int2vector* partkeyColumns = NULL;
    int partkeyColumnNum = 0;
    int i = 0;
    Const* tuplePartKeyValues[MAX_HASH_PARTKEY_NUMS];
    Const consts[MAX_HASH_PARTKEY_NUMS];
    Datum tuplePartKeyValue;
    bool isNull = false;
    bool isInPartition = false;
    HashPartitionMap *partMap = NULL;
    if (partSeq >= ((HashPartitionMap *)(partTableRel->partMap))->hashElementsNum) {
        return false;
    }
    incre_partmap_refcount(partTableRel->partMap);
    partMap = (HashPartitionMap *)(partTableRel->partMap);

    partkeyColumns = partMap->base.partitionKey;
    partkeyColumnNum = partkeyColumns->dim1;

    for (i = 0; i < partkeyColumnNum; i++) {
        isNull = false;
        tuplePartKeyValue = tableam_tops_tuple_getattr(tuple, (int)partkeyColumns->values[i], tupleDesc, &isNull);
        tuplePartKeyValues[i] =
            transformDatum2Const(tupleDesc, partkeyColumns->values[i], tuplePartKeyValue, isNull, &consts[i]);
    }

    isInPartition =
        isPartKeyValuesInHashPartition(partTableRel, partMap, tuplePartKeyValues, partkeyColumnNum, partSeq);

    decre_partmap_refcount(partTableRel->partMap);

    return isInPartition;
}

static bool checkTupleIsInRangePartition(Relation partTableRel, int partSeq, TupleDesc tupleDesc, void* tuple)
{
    int2vector* partkeyColumns = NULL;
    int partkeyColumnNum = 0;
    int i = 0;
    Const* tuplePartKeyValues[MAX_RANGE_PARTKEY_NUMS];
    Const consts[MAX_RANGE_PARTKEY_NUMS];
    Datum tuplePartKeyValue;
    bool isNull = false;
    bool isInPartition = false;
    RangePartitionMap *partMap = NULL;
    if (partSeq >= ((RangePartitionMap *)(partTableRel->partMap))->rangeElementsNum) {
        return false;
    }
    incre_partmap_refcount(partTableRel->partMap);
    partMap = (RangePartitionMap *)(partTableRel->partMap);

    partkeyColumns = partMap->base.partitionKey;
    partkeyColumnNum = partkeyColumns->dim1;

    for (i = 0; i < partkeyColumnNum; i++) {
        isNull = false;
        tuplePartKeyValue = tableam_tops_tuple_getattr(tuple, (int)partkeyColumns->values[i], tupleDesc, &isNull);
        tuplePartKeyValues[i] =
            transformDatum2Const(tupleDesc, partkeyColumns->values[i], tuplePartKeyValue, isNull, &consts[i]);
    }

    isInPartition = isPartKeyValuesInPartition(partMap, tuplePartKeyValues, partkeyColumnNum, partSeq);

    decre_partmap_refcount(partTableRel->partMap);

    return isInPartition;
}

// Description : Check the tuple whether locate the partition
bool isTupleLocatePartition(Relation partTableRel, int partSeq, TupleDesc tupleDesc, void* tuple)
{
    bool isInPartition = false;

    if (partSeq < 0) {
        return false;
    }
    switch (partTableRel->partMap->type) {
        case PART_TYPE_LIST: {
            isInPartition = checkTupleIsInListPartition(partTableRel, partSeq, tupleDesc, tuple);
            break;
        }
        case PART_TYPE_HASH: {
            isInPartition = checkTupleIsInHashPartition(partTableRel, partSeq, tupleDesc, tuple);
            break;
        }
        case PART_TYPE_RANGE: 
        case PART_TYPE_INTERVAL: {
            isInPartition = checkTupleIsInRangePartition(partTableRel, partSeq, tupleDesc, tuple);
            break;
        }
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("Only the List/Hash/Range partitioned table support finding partitions for tuples.")));
            break;
    }

    return isInPartition;
}
