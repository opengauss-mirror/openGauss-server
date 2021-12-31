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
 * -------------------------------------------------------------------------
 *
 * File Name	: partitionkey.cpp
 * Target		: data partition
 * Brief		:
 * Description	:
 * History	:
 * 
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/partition/partitionkey.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_partition_fn.h"
#include "catalog/pg_type.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/partitionkey.h"


/*
 * @@GaussDB@@
 * Brief
 * Description	: 	transform the list of maxvalue
 *				  	(partition's boundary, Const node form) into TEXT array
 * input		: 	List of Const
 * return value	: 	TEXT array
 * Note			:
 * Review       : 	xuzhongqing 67238
 */
#define constIsMaxValue(value) ((value)->ismaxvalue)

Datum transformPartitionBoundary(List* bondary, const bool* isTimestamptz)
{
    Datum result;
    ArrayBuildState* astate = NULL;
    ListCell* cell = NULL;
    int16 typlen = 0;
    bool typbyval = false;
    char typalign;
    char typdelim;
    Oid typioparam = InvalidOid;
    Oid outfunc = InvalidOid;

    int partKeyIdx = 0;

    /* no change if empty list */
    if (bondary == NIL) {
        return (Datum)0;
    }

    /* We build new array using accumArrayResult */
    astate = NULL;

    /*
     * If CREATE/SET, add new options to array; if RESET, just check that the
     * user didn't say RESET (option=val).  (Must do this because the grammar
     * doesn't enforce it.)
     */
    foreach (cell, bondary) {
        Node* partKeyFld = (Node*)lfirst(cell);
        Const* maxValueItem = NULL;
        text* t = NULL;
        char* maxValue = NULL;
        Size len = 0;
        errno_t rc = 0;
        Datum datumValue = (Datum)0;

        Assert(nodeTag(partKeyFld) == T_Const);
        maxValueItem = (Const*)partKeyFld;

        if (!constIsMaxValue(maxValueItem)) {
            /* get outfunc for consttype, excute the corresponding typeout function
             * transform Const->constvalue into string format.
             */
            get_type_io_data(maxValueItem->consttype,
                IOFunc_output,
                &typlen,
                &typbyval,
                &typalign,
                &typdelim,
                &typioparam,
                &outfunc);
            maxValue =
                DatumGetCString(OidFunctionCall1Coll(outfunc, maxValueItem->constcollid, maxValueItem->constvalue));

            if (isTimestamptz[partKeyIdx]) {
                int tmp = u_sess->time_cxt.DateStyle;
                u_sess->time_cxt.DateStyle = USE_ISO_DATES;
                datumValue = DirectFunctionCall3(
                    timestamptz_in, CStringGetDatum(maxValue), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));

                maxValue = (char*)DirectFunctionCall1(timestamptz_out, datumValue);
                u_sess->time_cxt.DateStyle = tmp;
            }

            /* turn typeout function's output into string */
            len = VARHDRSZ + strlen(maxValue);

            t = (text*)palloc(len + 1);
            SET_VARSIZE(t, len);

            rc = snprintf_s(VARDATA(t), (len + 1 - VARHDRSZ), strlen(maxValue), "%s", maxValue);
            securec_check_ss(rc, "\0", "\0");

            astate = accumArrayResult(astate, PointerGetDatum(t), false, TEXTOID, CurrentMemoryContext);
        } else {
            maxValue = NULL;
            astate = accumArrayResult(astate, (Datum)NULL, true, TEXTOID, CurrentMemoryContext);
        }

        partKeyIdx++;
    }

    result = makeArrayResult(astate, CurrentMemoryContext);
    return result;
}

Datum transformListBoundary(List* bondary, const bool* isTimestamptz)
{
    Datum result;
    ArrayBuildState* astate = NULL;
    ListCell* cell = NULL;
    int16 typlen = 0;
    bool typbyval = false;
    char typalign;
    char typdelim;
    Oid typioparam = InvalidOid;
    Oid outfunc = InvalidOid;

    int partKeyIdx = 0;

    /* no change if empty list */
    if (bondary == NIL) {
        return (Datum)0;
    }

    /* We build new array using accumArrayResult */
    astate = NULL;

    /*
 *      * If CREATE/SET, add new options to array; if RESET, just check that the
 *           * user didn't say RESET (option=val).  (Must do this because the grammar
 *                * doesn't enforce it.)
 *                     */
    foreach (cell, bondary) {
        Node* partKeyFld = (Node*)lfirst(cell);
        Const* maxValueItem = NULL;
        text* t = NULL;
        char* maxValue = NULL;
        Size len = 0;
        errno_t rc = 0;
        Datum datumValue = (Datum)0;

        Assert(nodeTag(partKeyFld) == T_Const);
        maxValueItem = (Const*)partKeyFld;

        if (!constIsMaxValue(maxValueItem)) {
            /* get outfunc for consttype, excute the corresponding typeout function
 *              * transform Const->constvalue into string format.
 *                           */
            get_type_io_data(maxValueItem->consttype,
                IOFunc_output,
                &typlen,
                &typbyval,
                &typalign,
                &typdelim,
                &typioparam,
                &outfunc);
            maxValue =
                DatumGetCString(OidFunctionCall1Coll(outfunc, maxValueItem->constcollid, maxValueItem->constvalue));

            if (isTimestamptz[partKeyIdx]) {
                int tmp = u_sess->time_cxt.DateStyle;
                u_sess->time_cxt.DateStyle = USE_ISO_DATES;
                datumValue = DirectFunctionCall3(
                    timestamptz_in, CStringGetDatum(maxValue), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));

                maxValue = (char*)DirectFunctionCall1(timestamptz_out, datumValue);
                u_sess->time_cxt.DateStyle = tmp;
            }

            /* turn typeout function's output into string */
            len = VARHDRSZ + strlen(maxValue);

            t = (text*)palloc(len + 1);
            SET_VARSIZE(t, len);

            rc = snprintf_s(VARDATA(t), (len + 1 - VARHDRSZ), strlen(maxValue), "%s", maxValue);
            securec_check_ss(rc, "\0", "\0");

            astate = accumArrayResult(astate, PointerGetDatum(t), false, TEXTOID, CurrentMemoryContext);
        } else {
            maxValue = NULL;
            astate = accumArrayResult(astate, (Datum)NULL, true, TEXTOID, CurrentMemoryContext);
        }
    }

    result = makeArrayResult(astate, CurrentMemoryContext);
    return result;
}

/*
 * @@GaussDB@@
 * Brief
 * Description	: transform TEXT array into a list of Value
 * input		: TEXT Array
 * return value	: List of Value
 * Note			:
 */
List* untransformPartitionBoundary(Datum options)
{
    List* result = NIL;
    ArrayType* array = NULL;
    Datum* optiondatums = NULL;
    int noptions;
    int i;
    bool* isnull = NULL;

    /* Nothing to do if no options */
    if (!PointerIsValid(DatumGetPointer(options))) {
        return result;
    }

    array = DatumGetArrayTypeP(options);

    Assert(ARR_ELEMTYPE(array) == TEXTOID);

    deconstruct_array(array, TEXTOID, -1, false, 'i', &optiondatums, &isnull, &noptions);

    for (i = 0; i < noptions; i++) {
        char* s = NULL;

        if (!isnull[i]) {
            s = TextDatumGetCString(optiondatums[i]);
        } else {
            s = NULL;
        }
        result = lappend(result, makeString(s));
    }

    pfree_ext(isnull);

    return result;
}

int partitonKeyCompare(Const** value1, Const** value2, int len)
{
    uint8 i = 0;
    int compare = 0;
    Const* v1 = NULL;
    Const* v2 = NULL;

    for (; i < len; i++) {
        v1 = *(value1 + i);
        v2 = *(value2 + i);

        if (v1 == NULL && v2 == NULL)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("NULL can not be compared with NULL")));
        if (v1 == NULL || v2 == NULL) {
            compare = (v1 == NULL) ? -1 : 1;
            break;
        }

        if (constIsMaxValue(v1) && constIsMaxValue(v2)) {
            compare = 0;
            continue;
        }
        if (constIsMaxValue(v1) || constIsMaxValue(v2)) {
            compare = (constIsMaxValue(v1)) ? 1 : -1;
            break;
        }

        if (v1->constisnull && v2->constisnull)
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("null value can not be compared with null value.")));
        if (v1->constisnull || v2->constisnull) {
            compare = (v1->constisnull) ? 1 : -1;
            break;
        }

        constCompare(v1, v2, compare);
        if (compare != 0) {
            break;
        }
    }

    return compare;
}

bool IsPartkeyInTargetList(Relation rel, List* targetList)
{
    PartitionMap* map = NULL;
    int2vector* partKey = NULL;
    ListCell *lc = NULL;
    int j = 0;

    map = rel->partMap;
    if (map->type == PART_TYPE_LIST) {
        partKey = ((ListPartitionMap *)map)->partitionKey;
    } else if (map->type == PART_TYPE_HASH) {
        partKey = ((HashPartitionMap *)map)->partitionKey;
    } else {
        partKey = ((RangePartitionMap *)map)->partitionKey;
    }
    foreach (lc, targetList) {
        TargetEntry *entry = (TargetEntry *)lfirst(lc);

        if (entry->resjunk) {
            continue;
        }

        /* check partkey has the column */
        for (j = 0; j < partKey->dim1; j++) {
            if (partKey->values[j] == entry->resno) {
                return true;
            }
        }
    }
    return false;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: check the partition key in the targetlist
 */
bool targetListHasPartitionKey(List* targetList, Oid partitiondtableid)
{
    bool ret = false;
    Relation rel;

    rel = heap_open(partitiondtableid, NoLock);
    if (RELATION_IS_PARTITIONED(rel)) {
        if (RelationIsSubPartitioned(rel)) {
            ret = IsPartkeyInTargetList(rel, targetList);
            if (!ret) {
                List *partOidList = relationGetPartitionOidList(rel);
                Oid partOid = list_nth_oid(partOidList, 0);
                Partition part = partitionOpen(rel, partOid, NoLock);
                Relation partRel = partitionGetRelation(rel, part);

                ret = IsPartkeyInTargetList(partRel, targetList);

                releaseDummyRelation(&partRel);
                partitionClose(rel, part, NoLock);
            }
        } else {
            ret = IsPartkeyInTargetList(rel, targetList);
        }
    }
    heap_close(rel, NoLock);
    return ret;
}

bool isPartKeyValuesInPartition(RangePartitionMap* partMap, Const** partKeyValues, int partkeyColumnNum, int partSeq)
{
    Assert(partMap && partKeyValues);
    Assert(partkeyColumnNum == partMap->partitionKey->dim1);

    int compareBottom = 0;
    int compareTop = 0;
    bool greaterThanBottom = false;
    bool lessThanTop = false;

    if (0 == partSeq) {
        /* is in first partition (-inf, boundary) */
        greaterThanBottom = true;
        partitonKeyCompareForRouting(partKeyValues, partMap->rangeElements[0].boundary, (uint32)partkeyColumnNum,
                                     compareTop);
        if (compareTop < 0) {
            lessThanTop = true;
        }
    } else {
        /* is in [last_partiton_boundary,  boundary) */
        partitonKeyCompareForRouting(
            partKeyValues, partMap->rangeElements[partSeq - 1].boundary, (uint32)partkeyColumnNum, compareBottom);

        if (compareBottom >= 0) {
            greaterThanBottom = true;
        }

        partitonKeyCompareForRouting(
            partKeyValues, partMap->rangeElements[partSeq].boundary, (uint32)partkeyColumnNum, compareTop);

        if (compareTop < 0) {
            lessThanTop = true;
        }
    }

    return greaterThanBottom && lessThanTop;
}

int comparePartitionKey(RangePartitionMap* partMap, Const** values1, Const** values2, int partKeyNum)
{
    int compare = 0;

    incre_partmap_refcount((PartitionMap*)partMap);
    partitonKeyCompareForRouting(values1, values2, (uint32)partKeyNum, compare);
    decre_partmap_refcount((PartitionMap*)partMap);

    return compare;
}

int2vector* GetPartitionKey(const PartitionMap* partMap)
{
    if (partMap->type == PART_TYPE_LIST) {
        return ((ListPartitionMap*)partMap)->partitionKey;
    } else if (partMap->type == PART_TYPE_HASH) {
        return ((HashPartitionMap*)partMap)->partitionKey;
    } else {
        return ((RangePartitionMap*)partMap)->partitionKey;
    }
}
