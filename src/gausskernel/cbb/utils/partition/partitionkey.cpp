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
#include "commands/tablecmds.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include "parser/parse_utilcmd.h"
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

static Oid GetPartitionOidFromPartitionKeyValuesList(Relation rel, List *partitionKeyValuesList, ParseState *pstate,
                                                     RangeTblEntry *rte);
static void CheckPartitionValuesList(Relation rel, List *subPartitionKeyValuesList);

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

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: select * from partition (partition_name)
 *				: or select from partition for (partition_values_list)
 * Description	: get partition oid for rte->partitionOid
 */
Oid getPartitionOidForRTE(RangeTblEntry* rte, RangeVar* relation, ParseState* pstate, Relation rel)
{
    Oid partitionOid = InvalidOid;

    if (!PointerIsValid(rte) || !PointerIsValid(relation) || !PointerIsValid(pstate) || !PointerIsValid(rel)) {
        return InvalidOid;
    }

    /* relation is not partitioned table. */
    if (!rte->ispartrel || rte->relkind != RELKIND_RELATION) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                        (errmsg("relation \"%s\" is not partitioned table", relation->relname), errdetail("N/A."),
                         errcause("System error."), erraction("Contact engineer to support."))));
    } else {
        /* relation is partitioned table, from clause is partition (partition_name). */
        if (PointerIsValid(relation->partitionname)) {
            partitionOid = partitionNameGetPartitionOid(rte->relid,
                relation->partitionname,
                PART_OBJ_TYPE_TABLE_PARTITION,
                AccessShareLock,
                true,
                false,
                NULL,
                NULL,
                NoLock);
            /* partiton does not exist. */
            if (!OidIsValid(partitionOid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("partition \"%s\" of relation \"%s\" does not exist",
                            relation->partitionname,
                            relation->relname)));
            }

            rte->pname = makeAlias(relation->partitionname, NIL);
        } else {
            partitionOid =
                GetPartitionOidFromPartitionKeyValuesList(rel, relation->partitionKeyValuesList, pstate, rte);
        }
    }

    return partitionOid;
}


static Oid GetPartitionOidFromPartitionKeyValuesList(Relation rel, List *partitionKeyValuesList, ParseState *pstate,
                                                     RangeTblEntry *rte)
{
    CheckPartitionValuesList(rel, partitionKeyValuesList);

    Oid partitionOid = InvalidOid;

    if (rel->partMap->type == PART_TYPE_LIST) {
        ListPartitionDefState *listPartDef = NULL;
        listPartDef = makeNode(ListPartitionDefState);
        listPartDef->boundary = partitionKeyValuesList;
        listPartDef->boundary = transformListPartitionValue(pstate, listPartDef->boundary, false, true);
        listPartDef->boundary = transformIntoTargetType(
            rel->rd_att->attrs, (((ListPartitionMap *)rel->partMap)->partitionKey)->values[0], listPartDef->boundary);

        rte->plist = listPartDef->boundary;

        partitionOid = partitionValuesGetPartitionOid(rel, listPartDef->boundary, AccessShareLock, true, true, false);

        pfree_ext(listPartDef);
    } else if (rel->partMap->type == PART_TYPE_HASH) {
        HashPartitionDefState *hashPartDef = NULL;
        hashPartDef = makeNode(HashPartitionDefState);
        hashPartDef->boundary = partitionKeyValuesList;
        hashPartDef->boundary = transformListPartitionValue(pstate, hashPartDef->boundary, false, true);
        hashPartDef->boundary = transformIntoTargetType(
            rel->rd_att->attrs, (((HashPartitionMap *)rel->partMap)->partitionKey)->values[0], hashPartDef->boundary);

        rte->plist = hashPartDef->boundary;

        partitionOid = partitionValuesGetPartitionOid(rel, hashPartDef->boundary, AccessShareLock, true, true, false);

        pfree_ext(hashPartDef);
    } else if (rel->partMap->type == PART_TYPE_RANGE || rel->partMap->type == PART_TYPE_INTERVAL) {
        RangePartitionDefState *rangePartDef = NULL;
        rangePartDef = makeNode(RangePartitionDefState);
        rangePartDef->boundary = partitionKeyValuesList;

        transformPartitionValue(pstate, (Node *)rangePartDef, false);

        rangePartDef->boundary = transformConstIntoTargetType(
            rel->rd_att->attrs, ((RangePartitionMap *)rel->partMap)->partitionKey, rangePartDef->boundary);

        rte->plist = rangePartDef->boundary;

        partitionOid = partitionValuesGetPartitionOid(rel, rangePartDef->boundary, AccessShareLock, true, true, false);

        pfree_ext(rangePartDef);
    } else {
        /* shouldn't happen. */
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        (errmsg("unsupported partition type"), errdetail("N/A."), errcause("System error."),
                         erraction("Contact engineer to support."))));
    }

    /* partition does not exist. */
    if (!OidIsValid(partitionOid)) {
        if (rel->partMap->type == PART_TYPE_RANGE || rel->partMap->type == PART_TYPE_LIST ||
            rel->partMap->type == PART_TYPE_HASH) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                            (errmsg("Cannot find partition by the value"), errdetail("N/A."),
                             errcause("The value is incorrect."), erraction("Use the correct value."))));
        }
    }

    return partitionOid;
}

static void SplitValuesList(List *ValuesList, List **partitionKeyValuesList, List **subPartitionKeyValuesList,
                            Relation rel)
{
    uint len = 0;
    uint cnt = 0;
    Node* elem = NULL;
    ListCell *cell = NULL;

    if (rel->partMap->type == PART_TYPE_RANGE) {
        len = (((RangePartitionMap *)rel->partMap)->partitionKey)->dim1;
    } else if (rel->partMap->type == PART_TYPE_LIST) {
        len = (((ListPartitionMap *)rel->partMap)->partitionKey)->dim1;
    } else if (rel->partMap->type == PART_TYPE_HASH) {
        len = (((HashPartitionMap *)rel->partMap)->partitionKey)->dim1;
    } else {
        /* shouldn't happen */
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        (errmsg("unsupported partition type"), errdetail("N/A."), errcause("System error."),
                         erraction("Contact engineer to support."))));
    }
    /* The subpartition table supports only one partition key. */
    Assert(len == 1);

    foreach(cell, ValuesList) {
        elem = (Node*)lfirst(cell);
        cnt++;
        if (cnt <= len) {
            *partitionKeyValuesList = lappend(*partitionKeyValuesList, elem);
        } else {
            *subPartitionKeyValuesList = lappend(*subPartitionKeyValuesList, elem);
        }
    }
}

static void CheckPartitionValuesList(Relation rel, List *subPartitionKeyValuesList)
{
    int len = 0;

    if (rel->partMap->type == PART_TYPE_RANGE) {
        len = (((RangePartitionMap *)rel->partMap)->partitionKey)->dim1;
    } else if (rel->partMap->type == PART_TYPE_LIST) {
        len = (((ListPartitionMap *)rel->partMap)->partitionKey)->dim1;
    } else if (rel->partMap->type == PART_TYPE_HASH) {
        len = (((HashPartitionMap *)rel->partMap)->partitionKey)->dim1;
    } else {
        /* shouldn't happen */
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        (errmsg("unsupported partition type"), errdetail("N/A."), errcause("System error."),
                         erraction("Contact engineer to support."))));
    }

    if (subPartitionKeyValuesList == NIL || len != subPartitionKeyValuesList->length) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                        (errmsg("number of partitionkey values is not equal to the number of partitioning columns"),
                         errdetail("N/A."), errcause("The value is incorrect."), erraction("Use the correct value."))));
    }
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: select * from subpartition (subpartition_name)
 * Description	: get partition oid for rte->partitionOid
 */
Oid GetSubPartitionOidForRTE(RangeTblEntry *rte, RangeVar *relation, ParseState *pstate, Relation rel, Oid *partOid)
{
    Oid subPartitionOid = InvalidOid;

    if (!PointerIsValid(rte) || !PointerIsValid(relation) || !PointerIsValid(pstate) || !PointerIsValid(rel)) {
        return InvalidOid;
    }

    /* relation is not partitioned table. */
    if (!rte->ispartrel || rte->relkind != RELKIND_RELATION || !RelationIsSubPartitioned(rel)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                        (errmsg("relation \"%s\" is not subpartitioned table", relation->relname), errdetail("N/A."),
                         errcause("System error."), erraction("Contact engineer to support."))));
    } else {
        /* relation is partitioned table, from clause is subpartition (subpartition_name). */
        if (PointerIsValid(relation->subpartitionname)) {
            subPartitionOid = partitionNameGetPartitionOid(rte->relid,
                relation->subpartitionname,
                PART_OBJ_TYPE_TABLE_SUB_PARTITION,
                AccessShareLock,
                true,
                false,
                NULL,
                NULL,
                NoLock,
                partOid);
            /* partiton does not exist. */
            if (!OidIsValid(subPartitionOid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("subpartition \"%s\" of relation \"%s\" does not exist",
                            relation->subpartitionname,
                            relation->relname)));
            }

            rte->pname = makeAlias(relation->subpartitionname, NIL);
        } else {
            List *partitionKeyValuesList = NIL;
            List *subPartitionKeyValuesList = NIL;
            List *tmpList = NIL;
            SplitValuesList(relation->partitionKeyValuesList, &partitionKeyValuesList, &subPartitionKeyValuesList, rel);
            *partOid = GetPartitionOidFromPartitionKeyValuesList(rel, partitionKeyValuesList, pstate, rte);
            tmpList = rte->plist;
            Partition part = partitionOpen(rel, *partOid, AccessShareLock);
            Relation partRel = partitionGetRelation(rel, part);
            CheckPartitionValuesList(partRel, subPartitionKeyValuesList);
            subPartitionOid =
                GetPartitionOidFromPartitionKeyValuesList(partRel, subPartitionKeyValuesList, pstate, rte);
            releaseDummyRelation(&partRel);
            partitionClose(rel, part, AccessShareLock);
            rte->plist = list_concat(tmpList, rte->plist);
        }
    }
    return subPartitionOid;
}
