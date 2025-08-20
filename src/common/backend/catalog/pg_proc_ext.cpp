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
#include "access/tableam.h"
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
void InsertPgProcExt(Oid oid, FunctionPartitionInfo* partInfo, Oid proprocoid, bool resultCache)
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
    if (partInfo == NULL && !OidIsValid(proprocoid) && !resultCache) {
        if (HeapTupleIsValid(oldtuple)) {
            simple_heap_delete(rel, &oldtuple->t_self);
            ReleaseSysCache(oldtuple);
        }
        heap_close(rel, RowExclusiveLock);
        return;
    }

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), true, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), true, sizeof(replaces));
    securec_check_c(rc, "\0", "\0");

    values[Anum_pg_proc_ext_proc_oid - 1] = ObjectIdGetDatum(oid);
    nulls[Anum_pg_proc_ext_proc_oid - 1] = false;
    if (partInfo != NULL) {
        values[Anum_pg_proc_ext_parallel_cursor_seq - 1] = UInt64GetDatum(partInfo->partitionCursorIndex);
        values[Anum_pg_proc_ext_parallel_cursor_strategy - 1] = Int16GetDatum(partInfo->strategy);
        values[Anum_pg_proc_ext_parallel_cursor_partkey - 1] = PointerGetDatum(getPartKeysArr(partInfo->partitionCols));
        nulls[Anum_pg_proc_ext_parallel_cursor_seq - 1] = false;
        nulls[Anum_pg_proc_ext_parallel_cursor_strategy - 1] = false;
        nulls[Anum_pg_proc_ext_parallel_cursor_partkey - 1] = false;
    }
    values[Anum_pg_proc_ext_procoid - 1] = ObjectIdGetDatum(proprocoid);
    nulls[Anum_pg_proc_ext_procoid - 1] = OidIsValid(proprocoid) ? false : true;
    values[Anum_pg_proc_ext_result_cache - 1] = BoolGetDatum(resultCache);
    nulls[Anum_pg_proc_ext_result_cache - 1] = false;

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

void UpdatePgProcExt(Oid funcOid, DefElem* result_cache_item, bool needCleanParallelEnableInfo)
{
    bool needResultCache = (result_cache_item != NULL && intVal(result_cache_item->arg));
    if (needResultCache) {
        HeapTuple tup = SearchSysCache1(PROCEDUREEXTENSIONOID, ObjectIdGetDatum(funcOid));
        if (!HeapTupleIsValid(tup)) {
            /* tuple not exists, insert it */
            InsertPgProcExt(funcOid, NULL, InvalidOid, true);
            return;
        }

        Relation rel = heap_open(ProcedureExtensionRelationId, RowExclusiveLock);
        Datum repl_val[Natts_pg_proc_ext];
        bool repl_null[Natts_pg_proc_ext];
        bool repl_repl[Natts_pg_proc_ext];
        errno_t rc = EOK;

        rc = memset_s(repl_repl, sizeof(repl_repl), false, sizeof(repl_repl));
        securec_check(rc, "\0", "\0");
    
        if (result_cache_item != NULL) {
            repl_repl[Anum_pg_proc_ext_result_cache - 1] = true;
            repl_val[Anum_pg_proc_ext_result_cache - 1] = BoolGetDatum(intVal(result_cache_item->arg));
            repl_null[Anum_pg_proc_ext_result_cache - 1] = false;
        }

        if (needCleanParallelEnableInfo) {
            repl_repl[Anum_pg_proc_ext_parallel_cursor_seq - 1] = true;
            repl_null[Anum_pg_proc_ext_parallel_cursor_seq - 1] = true;
            repl_repl[Anum_pg_proc_ext_parallel_cursor_strategy - 1] = true;
            repl_null[Anum_pg_proc_ext_parallel_cursor_strategy - 1] = true;
            repl_repl[Anum_pg_proc_ext_parallel_cursor_partkey - 1] = true;
            repl_null[Anum_pg_proc_ext_parallel_cursor_partkey - 1] = true;
        }

        HeapTuple newtup = heap_modify_tuple(tup, RelationGetDescr(rel), repl_val, repl_null, repl_repl);

        simple_heap_update(rel, &newtup->t_self, newtup);
        CatalogUpdateIndexes(rel, newtup);

        ReleaseSysCache(tup);
        heap_freetuple(newtup);
        heap_close(rel, RowExclusiveLock);
        return;
    }

    if (needCleanParallelEnableInfo) {
        DeletePgProcExt(funcOid);
    }
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

Oid GetProprocoidByOid(Oid oid)
{
    bool isNull;
    HeapTuple tuple = SearchSysCache1(PROCEDUREEXTENSIONOID, ObjectIdGetDatum(oid));
    if (!HeapTupleIsValid(tuple)) {
        return InvalidOid;
    }

    Datum proprocoid_datum = SysCacheGetAttr(PROCEDUREEXTENSIONOID, tuple, Anum_pg_proc_ext_procoid, &isNull);
    if (isNull) {
        ReleaseSysCache(tuple);
        return InvalidOid;
    }
    Oid proprocoid = ObjectIdGetDatum(proprocoid_datum);
    ReleaseSysCache(tuple);
    return proprocoid;
}

bool GetResultCacheByOid(Oid oid)
{
    bool isNull;
    HeapTuple tuple = SearchSysCache1(PROCEDUREEXTENSIONOID, ObjectIdGetDatum(oid));
    if (!HeapTupleIsValid(tuple)) {
        return InvalidOid;
    }

    Datum resultcache_datum = SysCacheGetAttr(PROCEDUREEXTENSIONOID, tuple, Anum_pg_proc_ext_result_cache, &isNull);
    if (isNull) {
        ReleaseSysCache(tuple);
        return false;
    }
    bool resultcache = BoolGetDatum(resultcache_datum);
    ReleaseSysCache(tuple);
    return resultcache;
}

void check_func_can_cache_result(CreateFunctionStmt* n, bool notsupport)
{
#ifndef ENABLE_MULTIPLE_NODES
    if (IsInitdb || !IsNormalProcessingMode()) {
        return;
    }

    bool hasResultCache = false;
    bool volatilitySupport = false;
    DefElem* val = NULL;
    DefElem* valretcache = NULL;
    ListCell* lc = NULL;
    foreach (lc, n->options) {
        val = (DefElem*)lfirst(lc);
        if (pg_strcasecmp(val->defname, "result_cache") == 0) {
            hasResultCache = true;
            valretcache = val;
            continue;
        }
        if (pg_strcasecmp(val->defname, "volatility") == 0 &&
                (pg_strcasecmp(strVal(val->arg), "stable") == 0 ||
                 pg_strcasecmp(strVal(val->arg), "immutable") == 0)) {
            volatilitySupport = true;
            continue;
        }
    }

    /* check options support */
    if (hasResultCache) {
        if (notsupport) {
            list_delete(n->options, valretcache);
            ereport(WARNING,
                (errmsg("Function result cache cannot support table result type or subprogram.")));
            return;
        }
        /* no support volatility */
        if (!volatilitySupport) {
            list_delete(n->options, valretcache);
            ereport(WARNING,
                (errmsg("Function result cache cannot use when function is not stable/immutable, ignore it.")));
            return;
        }
        /* check function has not in parameters */
        FunctionParameter* p;
        foreach (lc, n->parameters) {
            p = (FunctionParameter*)lfirst(lc);
            if (p->mode != FUNC_PARAM_IN) {
                list_delete(n->options, valretcache);
                ereport(WARNING,
                    (errmsg("Function result cache cannot use when function has not in parameters, ignore it.")));
                return;
            }
        }
    }
#endif
}
