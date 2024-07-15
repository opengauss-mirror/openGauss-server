/* -------------------------------------------------------------------------
 *
 * pg_proc_ext.cpp
 * routines to support manipulation of the pg_proc_ext relation
 *
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
 *
 * IDENTIFICATION
 * src/common/backend/catalog/pg_proc_ext.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/pg_proc_ext.h"
#include "catalog/indexing.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/syscache.h"
#include "access/heapam.h"

static inline ArrayType* getPartKeysArr(List* partitionCols);

/*
 * @Description: Insert a new record to pg_proc_ext.
 */
void InsertPgProcExt(Oid oid, FunctionPartitionInfo* partInfo)
{
    Datum values[Natts_pg_proc_ext];
    bool nulls[Natts_pg_proc_ext];
    bool replaces[Natts_pg_proc_ext];
    HeapTuple tuple = NULL;
    HeapTuple oldtuple = NULL;
    Relation rel = NULL;
    errno_t rc = 0;

    rel = heap_open(ProcedureExtensionRelationId, RowExclusiveLock);

    oldtuple = SearchSysCache1(PROCEDUREEXTENSIONOID, ObjectIdGetDatum(oid));
    if (partInfo == NULL) {
        if (HeapTupleIsValid(oldtuple)) {
            simple_heap_delete(rel, &oldtuple->t_self);
            ReleaseSysCache(oldtuple);
        }
        heap_close(rel, RowExclusiveLock);
        return;
    }

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), true, sizeof(replaces));
    securec_check_c(rc, "\0", "\0");

    values[Anum_pg_proc_ext_proc_oid - 1] = ObjectIdGetDatum(oid);
    values[Anum_pg_proc_ext_parallel_cursor_seq - 1] = UInt64GetDatum(partInfo->partitionCursorIndex);
    values[Anum_pg_proc_ext_parallel_cursor_strategy - 1] = Int16GetDatum(partInfo->strategy);
    values[Anum_pg_proc_ext_parallel_cursor_partkey - 1] = PointerGetDatum(getPartKeysArr(partInfo->partitionCols));

    if (HeapTupleIsValid(oldtuple)) {
        replaces[Anum_pg_proc_ext_proc_oid - 1] = false;
        tuple = heap_modify_tuple(oldtuple, RelationGetDescr(rel), values, nulls, replaces);
        simple_heap_update(rel, &tuple->t_self, tuple);
        ReleaseSysCache(oldtuple);
    } else {
        tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
        (void)simple_heap_insert(rel, tuple);
    }
    CatalogUpdateIndexes(rel, tuple);
    heap_freetuple_ext(tuple);
    heap_close(rel, RowExclusiveLock);
}

void DeletePgProcExt(Oid oid)
{
    Relation relation = NULL;
    HeapTuple tup = NULL;

    relation = heap_open(ProcedureExtensionRelationId, RowExclusiveLock);

    tup = SearchSysCache1(PROCEDUREEXTENSIONOID, ObjectIdGetDatum(oid));
    if (HeapTupleIsValid(tup)) {
        simple_heap_delete(relation, &tup->t_self);
        ReleaseSysCache(tup);
    }
    heap_close(relation, RowExclusiveLock);
}

static inline ArrayType* getPartKeysArr(List* partitionCols)
{
    Datum* partKeys = (Datum*)palloc0(list_length(partitionCols) * sizeof(Datum));
    ArrayType* partKeysArr = NULL;
    ListCell* lc = NULL;
    int i = 0;
    foreach (lc, partitionCols) {
        char* col = (char*)lfirst(lc);
        partKeys[i++] = CStringGetTextDatum(col);
    }
    partKeysArr = construct_array(partKeys, list_length(partitionCols), TEXTOID, -1, false, 'i');
    return partKeysArr;
}

int2 GetParallelCursorSeq(Oid oid)
{
    HeapTuple tuple = SearchSysCache1(PROCEDUREEXTENSIONOID, oid);
    if (!HeapTupleIsValid(tuple)) {
        return -1;
    }

    bool isNull;
    Datum dat = SysCacheGetAttr(PROCEDUREEXTENSIONOID, tuple, Anum_pg_proc_ext_parallel_cursor_seq, &isNull);
    if (isNull) {
        ReleaseSysCache(tuple);
        return -1;
    }
    ReleaseSysCache(tuple);
    return DatumGetInt16(dat);
}

FunctionPartitionStrategy GetParallelStrategyAndKey(Oid oid, List** partkey)
{
    FunctionPartitionStrategy strategy = FUNC_PARTITION_ANY;
    bool isNull;
    HeapTuple tuple = SearchSysCache1(PROCEDUREEXTENSIONOID, ObjectIdGetDatum(oid));
    
    if (!HeapTupleIsValid(tuple)) {
        return strategy;
    }

    Datum dat = SysCacheGetAttr(PROCEDUREEXTENSIONOID, tuple, Anum_pg_proc_ext_parallel_cursor_strategy, &isNull);
    if (isNull) {
        ReleaseSysCache(tuple);
        return strategy;
    }
    strategy = (FunctionPartitionStrategy)DatumGetInt16(dat);

    dat = SysCacheGetAttr(PROCEDUREEXTENSIONOID, tuple, Anum_pg_proc_ext_parallel_cursor_partkey, &isNull);

    if (!isNull) {
        ArrayType* arr = DatumGetArrayTypeP(dat);
        Datum* argnames = NULL;
        int numargs;
        deconstruct_array(arr, TEXTOID, -1, false, 'i', &argnames, NULL, &numargs);
        for (int i = 0; i < numargs; i++) {
            *partkey = lappend(*partkey, TextDatumGetCString(argnames[i]));
        }
    }

    ReleaseSysCache(tuple);
    return strategy;
}
