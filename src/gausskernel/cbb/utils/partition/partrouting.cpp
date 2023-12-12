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

Oid getListPartitionOid(PartitionMap *partMap, Const **partKeyValue, int partKeyCount, int32 *partSeq, bool topClosed)
{
    ListPartitionMap *listPartMap = NULL;
    Oid result = InvalidOid;
    int hit = -1;
    PartitionKey partKey;
    PartitionKey *boundary = NULL;
    Oid defaultPartitionOid = InvalidOid;
    bool existDefaultPartition = false;
    int defaultPartitionHit = -1;

    Assert(PointerIsValid(partMap));
    Assert(PointerIsValid(partKeyValue));

    incre_partmap_refcount(partMap);
    listPartMap = (ListPartitionMap *)(partMap);
    partKey.values = partKeyValue;
    partKey.count = partKeyCount;

    int i = 0;
    while (i < listPartMap->listElementsNum && hit < 0) {
        boundary = listPartMap->listElements[i].boundary;
        int list_len = listPartMap->listElements[i].len;
        if (list_len == 1 && ((Const *)boundary[0].values[0])->ismaxvalue) {
            defaultPartitionOid = listPartMap->listElements[i].partitionOid;
            existDefaultPartition = true;
            defaultPartitionHit = i;
        }
        int j = 0;
        while (j < list_len) {
            if (ListPartKeyCompare(&partKey, &boundary[j]) == 0) {
                hit = i;
                break;
            }
            j++;
        }
        i++;
    }

    if (PointerIsValid(partSeq)) {
        *partSeq = hit;
    }

    if (hit >= 0) {
        result = listPartMap->listElements[hit].partitionOid;
    } else if (existDefaultPartition) {
        result = defaultPartitionOid;
        *partSeq = defaultPartitionHit;
    }

    decre_partmap_refcount(partMap);
    return result;
}

Oid getHashPartitionOid(PartitionMap* partMap, Const** partKeyValue, int32* partSeq, bool topClosed)
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