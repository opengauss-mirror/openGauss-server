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
 * pgxc_slice.cpp
 *  system catalog table that storing slice information for list/range distributed table.
 * 
 * 
 * IDENTIFICATION
 *        src/common/backend/catalog/pgxc_slice.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/pgxc_slice.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "nodes/value.h"
#include "nodes/parsenodes.h"
#include "storage/lock/lock.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/partitionkey.h"
#include "utils/syscache.h"


static void PgxcSliceTupleInsert(Datum *values, bool *nulls)
{
    Relation rel = NULL;
    HeapTuple htup = NULL;

    rel = heap_open(PgxcSliceRelationId, RowExclusiveLock);
    htup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    (void)simple_heap_insert(rel, htup);

    CatalogUpdateIndexes(rel, htup);
    heap_freetuple(htup);
    heap_close(rel, RowExclusiveLock);
}

static void AddReferencedSlices(Oid relid, DistributeBy *distributeby)
{
    int i;
    HeapTuple tup, newtup;
    Relation relation;
    CatCList *slicelist = NULL;
    Datum values[Natts_pgxc_slice] = { 0 };
    bool nulls[Natts_pgxc_slice] = { false };
    bool replaces[Natts_pgxc_slice] = { false };

    slicelist = SearchSysCacheList1(PGXCSLICERELID, ObjectIdGetDatum(distributeby->referenceoid));
    if (slicelist->n_members == 0) {
        ReleaseSysCacheList(slicelist);
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("referenced table does not exists "
                       "when constructing references table")));
    }

    relation = heap_open(PgxcSliceRelationId, RowExclusiveLock);
    for (i = 0; i < slicelist->n_members; i++) {
        tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(slicelist, i);
        bool isnull = false;
        Datum val = fastgetattr(tup, Anum_pgxc_slice_type, RelationGetDescr(relation), &isnull);
        if (DatumGetChar(val) == PGXC_SLICE_TYPE_TABLE) {
            continue;
        }

        values[Anum_pgxc_slice_relid - 1] = relid;
        replaces[Anum_pgxc_slice_relid - 1] = true;
        newtup = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);
        simple_heap_insert(relation, newtup);
        CatalogUpdateIndexes(relation, newtup);
        heap_freetuple_ext(newtup);
    }

    ReleaseSysCacheList(slicelist);
    heap_close(relation, RowExclusiveLock);
}

static void GetDistribColsTzFlag(DistributeBy *distributeby, TupleDesc desc, bool *isTimestampTz)
{
    int i = 0;
    char *colname = NULL;
    ListCell *cell = NULL;
    
    foreach (cell, distributeby->colname) {
        colname = strVal(lfirst(cell));
        isTimestampTz[i] = false;
        for (int j = 0; j < desc->natts; j++) {
            if (desc->attrs[j].atttypid == TIMESTAMPTZOID &&
                strcmp(colname, desc->attrs[j].attname.data) == 0) {
                isTimestampTz[i] = true;
                break;
            }
        }
        i++;
    }
}

static void AddNewTupleForTable(const char *relname, Oid relid, DistributeBy *distributeby)
{
    Datum values[Natts_pgxc_slice];
    bool nulls[Natts_pgxc_slice] = { false };
    NameData name;

    (void)namestrcpy(&name, relname);
    values[Anum_pgxc_slice_relname - 1] = NameGetDatum(&name);
    values[Anum_pgxc_slice_type - 1] = CharGetDatum(PGXC_SLICE_TYPE_TABLE);
    values[Anum_pgxc_slice_strategy - 1] = CharGetDatum(distributeby->distState->strategy);
    values[Anum_pgxc_slice_relid - 1] = ObjectIdGetDatum(relid);
    nulls[Anum_pgxc_slice_referenceoid - 1] = true;
    values[Anum_pgxc_slice_sindex - 1] = UInt32GetDatum(0);
    nulls[Anum_pgxc_slice_interval - 1] = true;
    nulls[Anum_pgxc_slice_transitboundary - 1] = true;
    nulls[Anum_pgxc_slice_transitno - 1] = true;
    nulls[Anum_pgxc_slice_nodeoid - 1] = true;
    nulls[Anum_pgxc_slice_boundaries - 1] = true;
    values[Anum_pgxc_slice_specified - 1] = BoolGetDatum(false);
    nulls[Anum_pgxc_slice_sliceorder - 1] = true;

    PgxcSliceTupleInsert(values, nulls);
    return;
}

static void AddNewTupleForSlice(Oid relid, const RangePartitionDefState *sliceDef,
    uint32 sindex, Oid nodeOid, const bool *isTimestampTz, bool specified, uint32 order)
{
    Datum values[Natts_pgxc_slice];
    bool nulls[Natts_pgxc_slice] = { false };
    NameData name;
    Datum boundary = (Datum)0;

    (void)namestrcpy(&name, sliceDef->partitionName);
    values[Anum_pgxc_slice_relname - 1] = NameGetDatum(&name);
    values[Anum_pgxc_slice_type - 1] = CharGetDatum(PGXC_SLICE_TYPE_SLICE);
    nulls[Anum_pgxc_slice_strategy - 1] = true;
    values[Anum_pgxc_slice_relid - 1] = ObjectIdGetDatum(relid);
    nulls[Anum_pgxc_slice_referenceoid - 1] = true;
    values[Anum_pgxc_slice_sindex - 1] = UInt32GetDatum(sindex);
    
    nulls[Anum_pgxc_slice_interval - 1] = true;
    nulls[Anum_pgxc_slice_transitboundary - 1] = true;
    nulls[Anum_pgxc_slice_transitno - 1] = true;

    values[Anum_pgxc_slice_nodeoid - 1] = ObjectIdGetDatum(nodeOid);

    boundary = transformPartitionBoundary(sliceDef->boundary, isTimestampTz);
    if (boundary == UInt32GetDatum(0)) {
        nulls[Anum_pgxc_slice_boundaries - 1] = true;
    } else {
        values[Anum_pgxc_slice_boundaries - 1] = boundary;
    }

    values[Anum_pgxc_slice_specified - 1] = BoolGetDatum(specified);
    values[Anum_pgxc_slice_sliceorder - 1] = UInt32GetDatum(order);

    PgxcSliceTupleInsert(values, nulls);
}

static void PreprocessDefaultSlice(ListSliceDefState *listslice, int len)
{
    List* firstboundary = (List *)linitial(listslice->boundaries);
    List* newboundary = NIL; 
    Const* m = NULL;
    int i;

    if (((Const *)linitial(firstboundary))->ismaxvalue) {
        m = makeNode(Const);
        m->ismaxvalue = true;
        newboundary = list_make1((void *)m);
        for (i = 0; i < len - 1; i++) { /* construct a boundary with n DEFAULTs */
            m = makeNode(Const);
            m->ismaxvalue = true;
            newboundary = lappend(newboundary, m);
        }
        listslice->boundaries = list_make1((void *)newboundary); /* one item in slice */
    }
}

static void GetSliceTargetNode(const Oid* nodeoids, uint32 nodenum, const char* dnName,
    uint32* pStartpos, Oid* targetOid, bool* specified)
{
    if (dnName == NULL) {
        if (nodenum == 0) {
            ereport(ERROR,
            (errcode(ERRCODE_DIVISION_BY_ZERO),
                errmsg("node number is zero "
                       "when computing datanode oid for List/Range distribution.")));
        }
        *targetOid = nodeoids[(*pStartpos) % nodenum];
        (*pStartpos)++;
        *specified = false;
    } else {
        *targetOid = get_pgxc_datanodeoid(dnName, false);
        *specified = true;
    }
}

static void AddNewTupleForAllSlices(Oid relid, DistributeBy *distributeby,
    const Oid *nodeoids, uint32 nodenum, uint32 startpos, const bool *isTimestampTz)
{
    ListCell *cell = NULL;
    DistState *distState = distributeby->distState;
    Oid targetOid;
    bool specified;
    uint32 order = 0;

    /* Slice References case */
    if (OidIsValid(distributeby->referenceoid)) {
        AddReferencedSlices(relid, distributeby);
        return;
    }

    foreach (cell, distState->sliceList) {
        Node *n = (Node *)lfirst(cell);
        switch (n->type) {
            case T_RangePartitionDefState: {
                RangePartitionDefState *sliceDef = (RangePartitionDefState *)n;
                GetSliceTargetNode(nodeoids, nodenum, sliceDef->tablespacename, &startpos, &targetOid, &specified);
                AddNewTupleForSlice(relid, sliceDef, 0, targetOid, isTimestampTz, specified, order);
                break;
            }
            case T_ListSliceDefState: {
                ListSliceDefState *listslice = (ListSliceDefState *)n;
                PreprocessDefaultSlice(listslice, list_length(distributeby->colname));

                RangePartitionDefState *sliceDef = makeNode(RangePartitionDefState);
                ListCell *cell = NULL;
                uint32 sindex = 0;

                GetSliceTargetNode(nodeoids, nodenum, listslice->datanode_name, &startpos, &targetOid, &specified);

                /* we wrap every list boundary to RangePartitionDefState, then write to pgxc_slice */
                foreach (cell, listslice->boundaries) {
                    sliceDef->partitionName = listslice->name;
                    sliceDef->boundary = (List *)lfirst(cell);
                    AddNewTupleForSlice(relid, sliceDef, sindex, targetOid, isTimestampTz, specified, order);
                    sindex++;
                }

                pfree(sliceDef);
                break;
            }
            default:
                break;
        }

        order++;
    }

    return;
}

void PgxcSliceCreate(const char* relname, Oid relid, DistributeBy* distributeby, TupleDesc desc,
    const Oid* nodeoids, uint32 nodenum, uint32 startpos)
{
    Relation pgxcslicerel = heap_open(PgxcSliceRelationId, RowExclusiveLock);
    bool *isTimestamptz = (bool *)palloc(list_length(distributeby->colname) * sizeof(bool));

    /* get every distribute column timestamptz flag unless in slice reference case */
    if (!OidIsValid(distributeby->referenceoid)) {
        GetDistribColsTzFlag(distributeby, desc, isTimestamptz);
    }

    /* write table entry tuple to pgxc_slice */
    AddNewTupleForTable(relname, relid, distributeby);

    /* write every definition slice tuple to pgxc_slice */
    AddNewTupleForAllSlices(relid, distributeby, nodeoids, nodenum, startpos, isTimestamptz);

    pfree(isTimestamptz);
    heap_close(pgxcslicerel, RowExclusiveLock);
}

void RemovePgxcSlice(Oid relid)
{
    Relation relation;
    HeapTuple tup;
    relation = heap_open(PgxcSliceRelationId, RowExclusiveLock);

    CatCList *slicelist = SearchSysCacheList1(PGXCSLICERELID, ObjectIdGetDatum(relid));
    for (int i = 0; i < slicelist->n_members; i++) {
        tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(slicelist, i);
        simple_heap_delete(relation, &tup->t_self);
    }
    ReleaseSysCacheList(slicelist);
    
    heap_close(relation, RowExclusiveLock);
}
