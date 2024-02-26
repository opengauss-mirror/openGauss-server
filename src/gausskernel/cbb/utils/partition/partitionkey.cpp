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
static Oid GetPartitionOidFromPartitionKeyValuesList(Relation rel, List *partitionKeyValuesList, ParseState *pstate,
                                                     RangeTblEntry *rte);
static void CheckPartitionValuesList(Relation rel, List *subPartitionKeyValuesList);
static char* ListRowBoundaryGetString(RowExpr* bound, const bool* isTimestamptz);

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

        Assert(nodeTag(partKeyFld) == T_Const || nodeTag(partKeyFld) == T_RowExpr);
        maxValueItem = (Const*)partKeyFld;

        if (IsA(partKeyFld, RowExpr)) {
            /*
             * Outputs a set of key values in the bounds of a multikey list partition as a cstring array,
             * and then outputs the array as text. This text will be an element of the boundary array.
             */
            maxValue = ListRowBoundaryGetString((RowExpr*)partKeyFld, isTimestamptz);
            astate = accumArrayResult(astate, CStringGetTextDatum(maxValue), false, TEXTOID, CurrentMemoryContext);
        } else if (!constIsMaxValue(maxValueItem)) {
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

/* Use value1's collation first, so the table partition key boundary must be the first parameter! */
int partitonKeyCompare(Const** value1, Const** value2, int len, bool nullEqual)
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

        if (v1->constisnull && v2->constisnull) {
            /*
             * List partition key value can be null. In some cases, two null const should be considered equal,
             * such as when checking list partition boundary values.
             */
            if (nullEqual) {
                compare = 0;
                continue;
            }
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("null value can not be compared with null value.")));
        }
        if (v1->constisnull || v2->constisnull) {
            compare = (v1->constisnull) ? 1 : -1;
            break;
        }

        constCompare(v1, v2, v1->constcollid, compare);
        if (compare != 0) {
            break;
        }
    }

    return compare;
}

bool IsPartkeyInTargetList(Relation rel, List* targetList)
{
    Assert (rel != NULL && rel->partMap != NULL);
    int2vector* partKey = rel->partMap->partitionKey;
    ListCell *lc = NULL;
    
    foreach (lc, targetList) {
        TargetEntry *entry = (TargetEntry *)lfirst(lc);

        if (entry->resjunk) {
            continue;
        }

        /* check partkey has the column */
        for (int j = 0; j < partKey->dim1; j++) {
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

    rel = heap_open(partitiondtableid, AccessShareLock);
    if (RELATION_IS_PARTITIONED(rel)) {
        if (RelationIsSubPartitioned(rel)) {
            ret = IsPartkeyInTargetList(rel, targetList);
            if (!ret) {
                List *partOidList = relationGetPartitionOidList(rel);
                ListCell *cell = NULL;
                Oid partOid = InvalidOid;
                foreach (cell, partOidList) {
                    partOid = lfirst_oid(cell);
                    if (ConditionalLockPartitionOid(rel->rd_id, partOid, AccessShareLock)) {
                        break;
                    }
                    if (cell->next == NULL) {
                        ereport(ERROR,
                            (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                                errmsg("cannot check partkey for relation \"%s\"", RelationGetRelationName(rel)),
                                errdetail("all partitions of the target relation hold the lock")));
                    }
                }

                Partition part = partitionOpen(rel, partOid, NoLock);
                Relation partRel = partitionGetRelation(rel, part);

                ret = IsPartkeyInTargetList(partRel, targetList);

                releaseDummyRelation(&partRel);
                partitionClose(rel, part, AccessShareLock);
            }
        } else {
            ret = IsPartkeyInTargetList(rel, targetList);
        }
    }
    heap_close(rel, AccessShareLock);
    return ret;
}

bool isPartKeyValuesInPartition(RangePartitionMap* partMap, Const** partKeyValues, int partkeyColumnNum, int partSeq)
{
    Assert(partMap && partKeyValues);
    Assert(partkeyColumnNum == partMap->base.partitionKey->dim1);

    int compareBottom = 0;
    int compareTop = 0;
    bool greaterThanBottom = false;
    bool lessThanTop = false;

    if (0 == partSeq) {
        /* is in first partition (-inf, boundary) */
        greaterThanBottom = true;
        partitionKeyCompareForRouting(partKeyValues, partMap->rangeElements[0].boundary, (uint32)partkeyColumnNum,
                                     compareTop);
        if (compareTop < 0) {
            lessThanTop = true;
        }
    } else {
        /* is in [last_partiton_boundary,  boundary) */
        partitionKeyCompareForRouting(
            partKeyValues, partMap->rangeElements[partSeq - 1].boundary, (uint32)partkeyColumnNum, compareBottom);

        if (compareBottom >= 0) {
            greaterThanBottom = true;
        }

        partitionKeyCompareForRouting(
            partKeyValues, partMap->rangeElements[partSeq].boundary, (uint32)partkeyColumnNum, compareTop);

        if (compareTop < 0) {
            lessThanTop = true;
        }
    }

    return greaterThanBottom && lessThanTop;
}

int comparePartitionKey(RangePartitionMap* partMap, Const** partkey_value, Const** partkey_bound, int partKeyNum)
{
    int compare = 0;

    incre_partmap_refcount((PartitionMap*)partMap);
    partitionKeyCompareForRouting(partkey_value, partkey_bound, (uint32)partKeyNum, compare);
    decre_partmap_refcount((PartitionMap*)partMap);

    return compare;
}

int2vector* GetPartitionKey(const PartitionMap* partMap)
{
    return partMap->partitionKey;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: select * from partition (partition_name)
 *				: or select from partition for (partition_values_list)
 * Description	: get partition oid for rte->partitionOidList
 */
bool GetPartitionOidForRTE(RangeTblEntry* rte, RangeVar* relation, ParseState* pstate, Relation rel)
{
    Oid partitionOid = InvalidOid;

    if (!PointerIsValid(rte) || !PointerIsValid(relation) || !PointerIsValid(pstate) || !PointerIsValid(rel)) {
        return false;
    }

    /* cannot lock heap in case deadlock, we need process invalid messages here */
    AcceptInvalidationMessages();

    /* relation is not partitioned table. */
    if (!rte->ispartrel || rte->relkind != RELKIND_RELATION) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                        (errmsg("relation \"%s\" is not partitioned table", relation->relname), errdetail("N/A."),
                         errcause("System error."), erraction("Contact engineer to support."))));
    } else {
        /* relation is partitioned table, from clause is partition (partition_name). */
        if (PointerIsValid(relation->partitionname)) {
            partitionOid = PartitionNameGetPartitionOid(rte->relid,
                relation->partitionname,
                PART_OBJ_TYPE_TABLE_PARTITION,
                NoLock,
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
        rte->partitionOid = partitionOid;
        rte->partitionOidList = lappend_oid(rte->partitionOidList, partitionOid);
        /* InvalidOid means all subpartitions */
        rte->subpartitionOidList = lappend_oid(rte->subpartitionOidList, InvalidOid);
    }

    return true;
}


static Oid GetPartitionOidFromPartitionKeyValuesList(Relation rel, List *partitionKeyValuesList, ParseState *pstate,
                                                     RangeTblEntry *rte)
{
    CheckPartitionValuesList(rel, partitionKeyValuesList);

    Oid partitionOid = InvalidOid;

    if (rel->partMap->type == PART_TYPE_LIST) {
        ListPartitionDefState *listPartDef = NULL;
        listPartDef = makeNode(ListPartitionDefState);
        listPartDef->boundary = (List *)copyObject(partitionKeyValuesList);
        listPartDef->boundary = transformListPartitionValue(pstate, listPartDef->boundary, false, true);
        listPartDef->boundary = transformConstIntoTargetType(
            rel->rd_att->attrs, rel->partMap->partitionKey, listPartDef->boundary);

        rte->plist = listPartDef->boundary;

        partitionOid = PartitionValuesGetPartitionOid(rel, listPartDef->boundary, NoLock, true, true, false);

        pfree_ext(listPartDef);
    } else if (rel->partMap->type == PART_TYPE_HASH) {
        HashPartitionDefState *hashPartDef = NULL;
        hashPartDef = makeNode(HashPartitionDefState);
        hashPartDef->boundary = (List *)copyObject(partitionKeyValuesList);
        hashPartDef->boundary = transformListPartitionValue(pstate, hashPartDef->boundary, false, true);
        hashPartDef->boundary = transformIntoTargetType(
            rel->rd_att->attrs, rel->partMap->partitionKey->values[0], hashPartDef->boundary);

        rte->plist = hashPartDef->boundary;

        partitionOid = PartitionValuesGetPartitionOid(rel, hashPartDef->boundary, NoLock, true, true, false);

        pfree_ext(hashPartDef);
    } else if (rel->partMap->type == PART_TYPE_RANGE || rel->partMap->type == PART_TYPE_INTERVAL) {
        RangePartitionDefState *rangePartDef = NULL;
        rangePartDef = makeNode(RangePartitionDefState);
        rangePartDef->boundary = (List *)copyObject(partitionKeyValuesList);

        transformPartitionValue(pstate, (Node *)rangePartDef, false);

        rangePartDef->boundary = transformConstIntoTargetType(
            rel->rd_att->attrs, ((RangePartitionMap *)rel->partMap)->base.partitionKey, rangePartDef->boundary);

        rte->plist = rangePartDef->boundary;

        partitionOid = PartitionValuesGetPartitionOid(rel, rangePartDef->boundary, NoLock, true, true, false);

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
        len = rel->partMap->partitionKey->dim1;
    } else if (rel->partMap->type == PART_TYPE_LIST) {
        len = rel->partMap->partitionKey->dim1;
    } else if (rel->partMap->type == PART_TYPE_HASH) {
        len = rel->partMap->partitionKey->dim1;
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
    Assert (rel != NULL && rel->partMap != NULL);
    if (!PartitionMapTypeIsValid(rel->partMap)) {
        /* shouldn't happen */
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        (errmsg("unsupported partition type"), errdetail("N/A."), errcause("System error."),
                         erraction("Contact engineer to support."))));
    }

    int len = rel->partMap->partitionKey->dim1;
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
 * Description	: get partition oid for rte->subpartitionOidList
 */
bool GetSubPartitionOidForRTE(RangeTblEntry *rte, RangeVar *relation, ParseState *pstate, Relation rel)
{
    Oid subPartitionOid = InvalidOid;
    Oid partitionOid;

    if (!PointerIsValid(rte) || !PointerIsValid(relation) || !PointerIsValid(pstate) || !PointerIsValid(rel)) {
        return false;
    }

    /* cannot lock heap in case deadlock, we need process invalid messages here */
    AcceptInvalidationMessages();

    /* relation is not partitioned table. */
    if (!rte->ispartrel || rte->relkind != RELKIND_RELATION || !RelationIsSubPartitioned(rel)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                        (errmsg("relation \"%s\" is not subpartitioned table", relation->relname), errdetail("N/A."),
                         errcause("System error."), erraction("Contact engineer to support."))));
    } else {
        /* relation is partitioned table, from clause is subpartition (subpartition_name). */
        if (PointerIsValid(relation->subpartitionname)) {
            subPartitionOid = SubPartitionNameGetSubPartitionOid(rte->relid,
                relation->subpartitionname,
                NoLock,
                NoLock,
                true,
                false,
                NULL,
                NULL,
                NoLock,
                &partitionOid);
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
            partitionOid = GetPartitionOidFromPartitionKeyValuesList(rel, partitionKeyValuesList, pstate, rte);
            tmpList = rte->plist;
            Partition part = partitionOpen(rel, partitionOid, NoLock);
            Relation partRel = partitionGetRelation(rel, part);
            CheckPartitionValuesList(partRel, subPartitionKeyValuesList);
            subPartitionOid =
                GetPartitionOidFromPartitionKeyValuesList(partRel, subPartitionKeyValuesList, pstate, rte);
            releaseDummyRelation(&partRel);
            partitionClose(rel, part, NoLock);
            rte->plist = list_concat(tmpList, rte->plist);
        }
        rte->partitionOid = partitionOid;
        rte->subpartitionOid = subPartitionOid;
        rte->partitionOidList = lappend_oid(rte->partitionOidList, partitionOid);
        rte->subpartitionOidList = lappend_oid(rte->subpartitionOidList, subPartitionOid);
    }
    return true;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: delete from table partition (partition_name, subpartition_name, ...)
 * Description	: get (sub)partition oids for rte->partitionOidList and rte->subpartitionOidList
 */
void GetPartitionOidListForRTE(RangeTblEntry *rte, RangeVar *relation)
{
    ListCell *cell = NULL;
    Oid partitionOid;
    Oid subpartitionOid;

    /* cannot lock heap in case deadlock, we need process invalid messages here */
    AcceptInvalidationMessages();

    foreach(cell, relation->partitionNameList) {
        const char* name = strVal(lfirst(cell));
        partitionOid = PartitionNameGetPartitionOid(rte->relid,
            name,
            PART_OBJ_TYPE_TABLE_PARTITION,
            NoLock,
            true,
            false,
            NULL,
            NULL,
            NoLock);
        if (OidIsValid(partitionOid)) {
            rte->isContainPartition = true;
            rte->partitionOidList = lappend_oid(rte->partitionOidList, partitionOid);
            /* InvalidOid means full subpartitions */
            rte->subpartitionOidList = lappend_oid(rte->subpartitionOidList, InvalidOid);
            continue;
        }

        /* name is not a partiton name, try to get oid using it as a subpartition. */
        subpartitionOid = SubPartitionNameGetSubPartitionOid(rte->relid,
            name,
            NoLock,
            NoLock,
            true,
            false,
            NULL,
            NULL,
            NoLock,
            &partitionOid);
        /* partiton does not exist. */
        if (!OidIsValid(subpartitionOid)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("partition or subpartition \"%s\" of relation \"%s\" does not exist",
                        name,
                        relation->relname)));
        }
        rte->isContainSubPartition = true;
        rte->partitionOidList = lappend_oid(rte->partitionOidList, partitionOid);
        rte->subpartitionOidList = lappend_oid(rte->subpartitionOidList, subpartitionOid);
    }
}

/* function to check whether two partKey are identical */
int ConstCompareWithNull(Const *c1, Const *c2, Oid collation)
{
    if (constIsNull(c1) && constIsNull(c2)) {
        return 0;
    }
    if (constIsNull(c1) || constIsNull(c2)) {
        return (c1->constisnull) ? -1 : 1;
    }

    int compare = -1;
    constCompare(c1, c2, collation, compare);

    return compare;
}

int ListPartKeyCompare(PartitionKey* k1, PartitionKey* k2)
{
    if (k1->count != k2->count) {
        return (k1->count < k2->count) ? 1 : -1;
    }
    if (constIsMaxValue(k1->values[0]) || constIsMaxValue(k2->values[0])) {
        if (constIsMaxValue(k1->values[0]) && constIsMaxValue(k2->values[0])) {
            return 0;
        } else {
            return constIsMaxValue(k1->values[0]) ? 1 : -1;
        }
    }
    int res;
    for (int i = 0; i < k1->count; i++) {
        res = ConstCompareWithNull(k1->values[i], k2->values[i], k2->values[i]->constcollid);
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

static char* ConstBondaryGetString(Const* con, bool isTimestamptz)
{
    char* result;
    int16 typlen = 0;
    bool typbyval = false;
    char typalign;
    char typdelim;
    Oid typioparam = InvalidOid;
    Oid outfunc = InvalidOid;

    /*
     * get outfunc for consttype, excute the corresponding typeout
     * function transform Const->constvalue into string format.
     */
    get_type_io_data(con->consttype,
        IOFunc_output,
        &typlen,
        &typbyval,
        &typalign,
        &typdelim,
        &typioparam,
        &outfunc);
    result = DatumGetCString(OidFunctionCall1Coll(outfunc, con->constcollid, con->constvalue));

    if (isTimestamptz) {
        int tmp = u_sess->time_cxt.DateStyle;
        u_sess->time_cxt.DateStyle = USE_ISO_DATES;
        Datum datumValue = DirectFunctionCall3(
            timestamptz_in, CStringGetDatum(result), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
        pfree_ext(result);
        result = DatumGetCString(DirectFunctionCall1(timestamptz_out, datumValue));
        u_sess->time_cxt.DateStyle = tmp;
    }

    return result;
}

static char* ListRowBoundaryGetString(RowExpr* bound, const bool* isTimestamptz)
{
    ArrayBuildState* astate = NULL;
    ListCell* cell = NULL;
    Node* item = NULL;
    char* outValue = NULL;
    int partKeyIdx = 0;
    FmgrInfo flinfo;
    Datum result;

    foreach (cell, bound->args) {
        item = (Node*)lfirst(cell);
        Assert(IsA(item, Const));
        if (constIsNull((Const*)item)) { /* const in RowExpr may be NULL but will not be DEFAULT */
            astate = accumArrayResult(astate, (Datum)NULL, true, CSTRINGOID, CurrentMemoryContext);
        } else {
            outValue = ConstBondaryGetString((Const*)item, isTimestamptz[partKeyIdx]);
            astate = accumArrayResult(astate, CStringGetDatum(outValue), false, CSTRINGOID, CurrentMemoryContext);
        }
    }
    result = makeArrayResult(astate, CurrentMemoryContext);
    /* output array string */
    errno_t rc = memset_s(&flinfo, sizeof(FmgrInfo), 0, sizeof(FmgrInfo));
    securec_check(rc, "\0", "\0");
    flinfo.fn_mcxt = CurrentMemoryContext;
    flinfo.fn_addr = array_out;
    result = FunctionCall1(&flinfo, result);
    return DatumGetCString(result);
}