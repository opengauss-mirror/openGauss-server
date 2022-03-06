/* -------------------------------------------------------------------------
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * client_logic_proc.cpp
 *
 * IDENTIFICATION
 * src\common\backend\client_logic\client_logic_proc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "client_logic/client_logic_proc.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/gs_encrypted_proc.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_proc.h"
#include "utils/array.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "access/xact.h"


/*
 * @Description: save record in pg_depend for the dependency between clietn logic proc table and pg_proc
 * @param[IN] func_id Oid of function in pg_proc
 * @param[IN] gs_encrypted_proc_id  Oid of function in gs_encrypted_proc
 * @return: void
 */
void record_proc_depend(const Oid func_id, const Oid gs_encrypted_proc_id)
{
    ObjectAddress pg_proc_addr;
    ObjectAddress gs_encrypted_proc_addr;
    pg_proc_addr.classId = ProcedureRelationId;
    pg_proc_addr.objectId = func_id;
    pg_proc_addr.objectSubId = 0;
    gs_encrypted_proc_addr.classId = ClientLogicProcId;
    gs_encrypted_proc_addr.objectId = gs_encrypted_proc_id;
    gs_encrypted_proc_addr.objectSubId = 0;
    recordDependencyOn(&gs_encrypted_proc_addr, &pg_proc_addr, DEPENDENCY_INTERNAL);
}

void verify_out_param(HeapTuple oldtup, int *out_param_id)
{
    bool isNull = false;
    Datum proargmodes;
#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        proargmodes = SysCacheGetAttr(PROCNAMEARGSNSP, oldtup, Anum_pg_proc_proargmodes, &isNull);
    } else {
        proargmodes = SysCacheGetAttr(PROCALLARGS, oldtup, Anum_pg_proc_proargmodes, &isNull);
    }
#else
    proargmodes = SysCacheGetAttr(PROCNAMEARGSNSP, oldtup, Anum_pg_proc_proargmodes, &isNull);
#endif
    if (isNull) {
        return;
    }
    ArrayType *arr = DatumGetArrayTypeP(proargmodes); /* ensure not toasted */
    if (arr == NULL) {
        return;
    }
    int n_modes = ARR_DIMS(arr)[0];
    char *argmodes = (char*)ARR_DATA_PTR(arr);
    for (int i = 0; i < n_modes; i++) {
        if (argmodes[i] == PROARGMODE_OUT || argmodes[i] == PROARGMODE_INOUT ||
            argmodes[i] == PROARGMODE_TABLE) {
            if (*out_param_id == -1) {
                *out_param_id = i;
            } else {
                /* there is more than one out param - ignore */
                *out_param_id = -1;
                break;
            }
        }
    }
}

/*
 * @Description: inserting into gs tale the original return type - calclation by out params might happen for PLPGSQL
 * @param[IN] func_id Oid of function in pg_proc
 * @param[IN] ret_type original return type
 * @param[IN] res_type client logic data type
 * @return: void
 */
void add_rettype_orig(const Oid func_id, const Oid ret_type, const Oid res_type)
{
    bool nulls[Natts_pg_proc] = {0};
    Datum values[Natts_pg_proc] = {0};
    bool replaces[Natts_pg_proc] = {0};
    bool gs_nulls[Natts_gs_encrypted_proc] = {0};
    Datum gs_values[Natts_gs_encrypted_proc] = {0};
    bool gs_replaces[Natts_gs_encrypted_proc] = {0};
    values[Anum_pg_proc_prorettype - 1] = ObjectIdGetDatum(res_type);
    replaces[Anum_pg_proc_prorettype - 1] = true;
    bool isNull = false;

    HeapTuple oldtup = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_id));
    if (!HeapTupleIsValid(oldtup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmodule(MOD_EXECUTOR),
            errmsg("cache lookup failed for function %u when initialize function cache.", func_id)));
    }
    /* for dynamic plpgsql there is not enough to replace ret value
     * also need to replace out parameter through which this value returned
     */
    int out_param_id = -1;
    Oid out_param_type = InvalidOid;
    int allnumargs = 0;

    /* since return type must match OUT params, if there is only 1 OUT param, so its data type is the return type
    * if there are more than 1 out param, the return type is going to be recoerd which is beign processed in another
    *  function
    */
    verify_out_param(oldtup, &out_param_id);
    if (out_param_id > -1) {
        /* there is one out param - replace allargs as well */
        Datum proallargtypes = SysCacheGetAttr(PROCOID, oldtup, Anum_pg_proc_proallargtypes, &isNull);
        if (!isNull) {
            Oid* allargtypes;
            ArrayType* arr_all_types = DatumGetArrayTypeP(proallargtypes); /* ensure not toasted */
            allnumargs = ARR_DIMS(arr_all_types)[0];

            Assert(allnumargs > out_param_id);
            allargtypes = (Oid*)ARR_DATA_PTR(arr_all_types);
            out_param_type = allargtypes[out_param_id];
            allargtypes[out_param_id] = res_type;
            values[Anum_pg_proc_proallargtypes - 1] = PointerGetDatum(arr_all_types);
            replaces[Anum_pg_proc_proallargtypes - 1] = true;
        }
    }

    Relation rel = heap_open(ProcedureRelationId, RowExclusiveLock);
    TupleDesc tupDesc = RelationGetDescr(rel);
    HeapTuple tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);
    ReleaseSysCache(oldtup);
    heap_close(rel, RowExclusiveLock);
    CommandCounterIncrement();
    HeapTuple gs_tup;
    HeapTuple gs_oldtup = SearchSysCache1(GSCLPROCID, ObjectIdGetDatum(func_id));
    Relation gs_rel = heap_open(ClientLogicProcId, RowExclusiveLock);
    TupleDesc gs_tupDesc = RelationGetDescr(gs_rel);
    gs_values[Anum_gs_encrypted_proc_last_change - 1] =
         DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
    gs_values[Anum_gs_encrypted_proc_prorettype_orig - 1] = ObjectIdGetDatum(ret_type);
    if (!HeapTupleIsValid(gs_oldtup)) {
        gs_values[Anum_gs_encrypted_proc_func_id - 1] = ObjectIdGetDatum(func_id);
        gs_values[Anum_gs_encrypted_proc_prorettype_orig - 1] = ObjectIdGetDatum(ret_type);
        if (allnumargs > 0 && out_param_id > -1 && out_param_type != InvalidOid) {
            Datum* gs_all_args_orig = (Datum*)palloc(allnumargs * sizeof(Datum));
            for (int i = 0; i < allnumargs; i++) {
                gs_all_args_orig[i] = ObjectIdGetDatum(-1);
            }
            gs_all_args_orig[out_param_id] = ObjectIdGetDatum(out_param_type);
            ArrayType* arr_gs_all_types_orig =
                construct_array(gs_all_args_orig, allnumargs, INT4OID, sizeof(int4), true, 'i');
            gs_values[Anum_gs_encrypted_proc_proallargtypes_orig - 1] = PointerGetDatum(arr_gs_all_types_orig);
            gs_values[Anum_gs_encrypted_proc_last_change - 1] = PointerGetDatum(arr_gs_all_types_orig);
            pfree_ext(gs_all_args_orig);
        } else {
            gs_nulls[Anum_gs_encrypted_proc_proallargtypes_orig - 1] = true;
        }
        gs_nulls[Anum_gs_encrypted_proc_proargcachedcol - 1] = true;
        gs_tup = heap_form_tuple(gs_tupDesc, gs_values, gs_nulls);
        const Oid gs_encrypted_proc_id = simple_heap_insert(gs_rel, gs_tup);
        record_proc_depend(func_id, gs_encrypted_proc_id);
    } else {
        gs_values[Anum_gs_encrypted_proc_prorettype_orig - 1] = ObjectIdGetDatum(ret_type);
        gs_replaces[Anum_gs_encrypted_proc_prorettype_orig - 1] = true;
        gs_replaces[Anum_gs_encrypted_proc_last_change - 1] = true;
        Datum gs_all_types_orig =
            SysCacheGetAttr(GSCLPROCID, gs_oldtup, Anum_gs_encrypted_proc_proallargtypes_orig, &isNull);
        if (!isNull) {
            ArrayType* arr_gs_all_types_orig = DatumGetArrayTypeP(gs_all_types_orig);
            Assert(allnumargs == ARR_DIMS(arr_gs_all_types_orig)[0]);
            Oid* vec_gs_all_types_orig = (Oid*)ARR_DATA_PTR(arr_gs_all_types_orig);
            vec_gs_all_types_orig[out_param_id] = ObjectIdGetDatum(out_param_type);
            gs_values[Anum_gs_encrypted_proc_proallargtypes_orig - 1] = PointerGetDatum(arr_gs_all_types_orig);
            gs_replaces[Anum_gs_encrypted_proc_proallargtypes_orig - 1] = true;
        }

        /* Okay, do it... */
        gs_tup = heap_modify_tuple(gs_oldtup, gs_tupDesc, gs_values, gs_nulls, gs_replaces);
        simple_heap_update(gs_rel, &gs_tup->t_self, gs_tup);
        ReleaseSysCache(gs_oldtup);
    }
    CatalogUpdateIndexes(gs_rel, gs_tup);
    heap_close(gs_rel, RowExclusiveLock);
    CommandCounterIncrement();
    ce_cache_refresh_type |= 0x20; /* update PROC cache */
}


/*
 * @Description: get the pg_type Oid related to specific pg_class record
 * @param[IN] relid Oid of the relation that we want to get its related type
 * @return: type related to relid
 */
static Oid get_reltype_from_relid(const Oid relid)
{
    Oid reltypid = InvalidOid;
    if (relid != InvalidOid) {
        HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if (HeapTupleIsValid(tuple)) {
            bool is_null;
            Datum typid_datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reltype, &is_null);
            if(!is_null) {
                reltypid = DatumGetObjectId(typid_datum);
            }
            ReleaseSysCache(tuple);
        }
    }
    return reltypid;
}

/*
 * @Description: adding to the original parameters data types to gs_encrypted_proc
 * @param[IN] func_id pg_proc Oid of the function
 * @param[IN] all_types_orig Datum array of original data types in case of client logic, else -1
 * @param[IN] all_types real types of the pg_proc record for this functions
 * @param[IN] tup_natts number of attributes that will return in the function result type
 * @param[IN] relid relation id of the table that the columns returned from
 * @return: void
 */
void add_allargtypes_orig(const Oid func_id,
                          Datum* all_types_orig,  Datum* all_types, const int tup_natts, const Oid relid)
{
    Datum proargmodes = 0;
    Datum gs_all_types_orig = 0;
    ArrayType* arr_all_arg_types = NULL;
    ArrayType* arr_proargmodes = NULL;
    ArrayType* arr_gs_all_types_orig = NULL;
    Oid* vec_all_arg_types = NULL;
    Oid* vec_gs_all_types_orig = NULL;
    char* vec_proargmodes = NULL;
    int allnumargs = 0;
    bool gs_nulls[Natts_gs_encrypted_proc] = {0};
    Datum gs_values[Natts_gs_encrypted_proc] = {0};
    bool gs_replaces[Natts_gs_encrypted_proc] = {0};
    bool nulls[Natts_pg_proc] = {0};
    Datum values[Natts_pg_proc] = {0};
    bool replaces[Natts_pg_proc] = {0};
    const Oid reltypid = get_reltype_from_relid(relid);
    HeapTuple oldtup = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_id));
    if (!HeapTupleIsValid(oldtup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmodule(MOD_EXECUTOR),
            errmsg("cache lookup failed for function %u when initialize function cache.", func_id)));
    }
    bool isNull = false;
    Datum proAllArgTypes = SysCacheGetAttr(PROCOID, oldtup, Anum_pg_proc_proallargtypes, &isNull);
    if (isNull) {
        if (reltypid != InvalidOid) {
            values[Anum_pg_proc_prorettype - 1] = ObjectIdGetDatum(reltypid);
            replaces[Anum_pg_proc_prorettype-1] = true;
            Relation rel = heap_open(ProcedureRelationId, RowExclusiveLock);
            TupleDesc tupDesc = RelationGetDescr(rel);
            HeapTuple tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
            simple_heap_update(rel, &tup->t_self, tup);
            CatalogUpdateIndexes(rel, tup);
            heap_close(rel, RowExclusiveLock);
            CommandCounterIncrement();
        }
        ReleaseSysCache(oldtup);
        return; /* nothing to update */
    }

    arr_all_arg_types = DatumGetArrayTypeP(proAllArgTypes); /* ensure not toasted */
    allnumargs = ARR_DIMS(arr_all_arg_types)[0];
    bool is_oid_oid_array = ARR_NDIM(arr_all_arg_types) != 1 || allnumargs < 0 || ARR_HASNULL(arr_all_arg_types) ||
        ARR_ELEMTYPE(arr_all_arg_types) != OIDOID;
    if (is_oid_oid_array) {
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
    }
    if (allnumargs == 0) {
        ReleaseSysCache(oldtup);
        return;
    }
    /* for replacing the content of arr_all_arg_types it is easier to work with */
    vec_all_arg_types = (Oid*)ARR_DATA_PTR(arr_all_arg_types);

#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.proc->workingVersionNum < 92470) {
        proargmodes = SysCacheGetAttr(PROCNAMEARGSNSP, oldtup, Anum_pg_proc_proargmodes, &isNull);
    } else {
        proargmodes = SysCacheGetAttr(PROCALLARGS, oldtup, Anum_pg_proc_proargmodes, &isNull);
    }
#else
    proargmodes = SysCacheGetAttr(PROCNAMEARGSNSP, oldtup, Anum_pg_proc_proargmodes, &isNull);
#endif
    if (!isNull) {
        arr_proargmodes = DatumGetArrayTypeP(proargmodes); /* ensure not toasted */
        bool is_char_oid_array = ARR_NDIM(arr_proargmodes) != 1 || ARR_DIMS(arr_proargmodes)[0] != allnumargs ||
            ARR_HASNULL(arr_proargmodes) || ARR_ELEMTYPE(arr_proargmodes) != CHAROID;
        if (is_char_oid_array) {
            ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
        }

        vec_proargmodes = (char*)ARR_DATA_PTR(arr_proargmodes);
    } else {
        vec_proargmodes = NULL;
    }
    HeapTuple gs_tup;
    HeapTuple gs_oldtup = SearchSysCache1(GSCLPROCID, ObjectIdGetDatum(func_id));
    if (HeapTupleIsValid(gs_oldtup)) {
        gs_all_types_orig = SysCacheGetAttr(GSCLPROCID, gs_oldtup, Anum_gs_encrypted_proc_proallargtypes_orig, &isNull);
        if (!isNull) {
            arr_gs_all_types_orig = DatumGetArrayTypeP(gs_all_types_orig);
            Assert(allnumargs == ARR_DIMS(arr_gs_all_types_orig)[0]);
            vec_gs_all_types_orig = (Oid*)ARR_DATA_PTR(arr_gs_all_types_orig);
        }
    }
    if (vec_gs_all_types_orig == NULL) {
        vec_gs_all_types_orig = (Oid*)palloc(allnumargs * sizeof(Oid));
        for (int i = 0; i < allnumargs; i++) {
            vec_gs_all_types_orig[i] = ObjectIdGetDatum(-1);
        }
    }

    /* Replace orig data types with replaced data types */
    if (vec_proargmodes) {
        int col = 0;
        for (int i = 0; i < tup_natts; i++) {
            for(; col < allnumargs; col++) {
                if(vec_proargmodes[col] == PROARGMODE_OUT  || vec_proargmodes[col] == PROARGMODE_INOUT ||
                    vec_proargmodes[col] == PROARGMODE_TABLE) {
                    vec_all_arg_types[col] = DatumGetObjectId(all_types[i]);
                    vec_gs_all_types_orig[col] = DatumGetObjectId(all_types_orig[i]);
                    col++; /* force move to the next arg */
                    break;
                }
            }
        }
    }
    values[Anum_pg_proc_proallargtypes - 1] = PointerGetDatum(arr_all_arg_types);
    replaces[Anum_pg_proc_proallargtypes - 1] = true;
    if (reltypid != InvalidOid) {
        values[Anum_pg_proc_prorettype - 1] = ObjectIdGetDatum(reltypid);
        replaces[Anum_pg_proc_prorettype-1] = true;
    }

    Relation rel = heap_open(ProcedureRelationId, RowExclusiveLock);
    TupleDesc tupDesc = RelationGetDescr(rel);
    HeapTuple tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);
    heap_close(rel, RowExclusiveLock);
    CommandCounterIncrement();
    Relation gs_rel = heap_open(ClientLogicProcId, RowExclusiveLock);
    TupleDesc gs_tupDesc = RelationGetDescr(gs_rel);
    gs_values[Anum_gs_encrypted_proc_last_change - 1] =
        DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
    if (!HeapTupleIsValid(gs_oldtup)) {
        gs_values[Anum_gs_encrypted_proc_func_id - 1] = ObjectIdGetDatum(func_id);
        Datum* all_types_orig_datum = (Datum*)palloc(allnumargs * sizeof(Datum));
        for (int i = 0; i < allnumargs; i++) {
            all_types_orig_datum[i] = ObjectIdGetDatum(vec_gs_all_types_orig[i]);
        }
        arr_gs_all_types_orig = construct_array(all_types_orig_datum, allnumargs, INT4OID, sizeof(int4), true, 'i');
        gs_nulls[Anum_gs_encrypted_proc_proargcachedcol - 1] = true;
        gs_values[Anum_gs_encrypted_proc_proallargtypes_orig - 1] = PointerGetDatum(arr_gs_all_types_orig);
        gs_tup = heap_form_tuple(gs_tupDesc, gs_values, gs_nulls);
        Oid gs_encrypted_proc_id = simple_heap_insert(gs_rel, gs_tup);
        record_proc_depend(func_id, gs_encrypted_proc_id);
        pfree_ext(all_types_orig_datum);
    } else {
        gs_values[Anum_gs_encrypted_proc_proallargtypes_orig - 1] = PointerGetDatum(arr_gs_all_types_orig);
        gs_replaces[Anum_gs_encrypted_proc_proallargtypes_orig - 1] = true;
        gs_replaces[Anum_gs_encrypted_proc_last_change - 1] = true;
        /* Okay, do it... */
        gs_tup = heap_modify_tuple(gs_oldtup, gs_tupDesc, gs_values, gs_nulls, gs_replaces);
        simple_heap_update(gs_rel, &gs_tup->t_self, gs_tup);
        ReleaseSysCache(gs_oldtup);
    }
    CatalogUpdateIndexes(gs_rel, gs_tup);
    heap_close(gs_rel, RowExclusiveLock);
    ce_cache_refresh_type |= 0x20; /* update PROC cache */
    ReleaseSysCache(oldtup);
    CommandCounterIncrement();
}


void verify_rettype_for_out_param(const Oid func_id)
{
    bool nulls[Natts_pg_proc] = {0};
    Datum values[Natts_pg_proc] = {0};
    bool replaces[Natts_pg_proc] = {0};
    bool gs_nulls[Natts_gs_encrypted_proc] = {0};
    Datum gs_values[Natts_gs_encrypted_proc] = {0};
    bool gs_replaces[Natts_gs_encrypted_proc] = {0};
    replaces[Anum_pg_proc_prorettype - 1] = true;
    bool isNull = false;
    int out_param_id = -1;
    Oid out_param_type = InvalidOid;
    int allnumargs = 0;

    HeapTuple oldtup = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_id));
    if (!HeapTupleIsValid(oldtup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmodule(MOD_EXECUTOR),
            errmsg("cache lookup failed for function %u when initialize function cache.", func_id)));
    }
    /* for dynamic plpgsql there is not enough to replace ret value
     * also need to replace out parameter through wicj this value returned
     */
    Datum ret_type = SysCacheGetAttr(PROCOID, oldtup, Anum_pg_proc_prorettype, &isNull);
    if (isNull || IsClientLogicType(ObjectIdGetDatum(ret_type))) {
        /* No ret type or rettype is already replaced */
        ReleaseSysCache(oldtup);
        return;
    }
    verify_out_param(oldtup, &out_param_id);
    if (out_param_id == -1) {
        ReleaseSysCache(oldtup);
        return;
    }
    /* there is one out param - verify return type is the same as type param */
    Datum proallargtypes = SysCacheGetAttr(PROCOID, oldtup, Anum_pg_proc_proallargtypes, &isNull);
    if (isNull) {
        ReleaseSysCache(oldtup);
        return;
    }
    Oid *allargtypes = NULL;
    ArrayType *arr_all_types = DatumGetArrayTypeP(proallargtypes); /* ensure not toasted */
    allnumargs = ARR_DIMS(arr_all_types)[0];
    Assert(allnumargs > out_param_id);
    allargtypes = (Oid*)ARR_DATA_PTR(arr_all_types);
    out_param_type = allargtypes[out_param_id];
    if (!IsClientLogicType(out_param_type)) {
        ReleaseSysCache(oldtup);
        return;
    }
    values[Anum_pg_proc_prorettype - 1] = out_param_type;

    Relation rel = heap_open(ProcedureRelationId, RowExclusiveLock);
    TupleDesc tupDesc = RelationGetDescr(rel);
    HeapTuple tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);
    ReleaseSysCache(oldtup);
    heap_close(rel, RowExclusiveLock);
    CommandCounterIncrement();
    /* keep original rettype in gs_encrypted_proc */
    HeapTuple gs_tup;
    HeapTuple gs_oldtup = SearchSysCache1(GSCLPROCID, ObjectIdGetDatum(func_id));
    Relation gs_rel = heap_open(ClientLogicProcId, RowExclusiveLock);
    TupleDesc gs_tupDesc = RelationGetDescr(gs_rel);
    gs_values[Anum_gs_encrypted_proc_last_change - 1] =
        DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
    if (!HeapTupleIsValid(gs_oldtup)) {
        gs_values[Anum_gs_encrypted_proc_func_id - 1] = ObjectIdGetDatum(func_id);
        gs_values[Anum_gs_encrypted_proc_prorettype_orig - 1] = ObjectIdGetDatum(ret_type);
        if (allnumargs > 0 && out_param_id > -1 && out_param_type != InvalidOid) {
            Datum *gs_all_args_orig = (Datum*)palloc(allnumargs * sizeof(Datum));
            for (int i = 0; i < allnumargs; i++) {
                gs_all_args_orig[i] = ObjectIdGetDatum(-1);
            }
            gs_all_args_orig[out_param_id] = out_param_type;
            ArrayType *arr_gs_all_types_orig =
                construct_array(gs_all_args_orig, allnumargs, INT4OID, sizeof(int4), true, 'i');
            gs_values[Anum_gs_encrypted_proc_proallargtypes_orig - 1] = PointerGetDatum(arr_gs_all_types_orig);
            pfree_ext(gs_all_args_orig);
        } else {
            gs_nulls[Anum_gs_encrypted_proc_proallargtypes_orig - 1] = true;
        }
        gs_nulls[Anum_gs_encrypted_proc_proargcachedcol - 1] = true;
        gs_tup = heap_form_tuple(gs_tupDesc, gs_values, gs_nulls);
        const Oid gs_encrypted_proc_id = simple_heap_insert(gs_rel, gs_tup);
        record_proc_depend(func_id, gs_encrypted_proc_id);
    } else {
        gs_values[Anum_gs_encrypted_proc_prorettype_orig - 1] = ObjectIdGetDatum(ret_type);
        gs_replaces[Anum_gs_encrypted_proc_prorettype_orig - 1] = true;
        gs_replaces[Anum_gs_encrypted_proc_last_change - 1] = true;
        /* Okay, do it... */
        gs_tup = heap_modify_tuple(gs_oldtup, gs_tupDesc, gs_values, gs_nulls, gs_replaces);
        simple_heap_update(gs_rel, &gs_tup->t_self, gs_tup);
        ReleaseSysCache(gs_oldtup);
    }
    CatalogUpdateIndexes(gs_rel, gs_tup);
    heap_close(gs_rel, RowExclusiveLock);
    CommandCounterIncrement();
    ce_cache_refresh_type |= 0x20; /* update PROC cache */
}

/*
 * @Description: delete all procedure client logic info and remove dependencies
 * @param[IN] tuple of gs_encrypted_proc to be removed
 * @return: void
 */
void delete_proc_client_info(HeapTuple old_gs_tup)
{
    deleteDependencyRecordsFor(ClientLogicProcId, HeapTupleGetOid(old_gs_tup), true);
    Relation gs_rel = heap_open(ClientLogicProcId, RowExclusiveLock);
    simple_heap_delete(gs_rel, &old_gs_tup->t_self);
    heap_close(gs_rel, RowExclusiveLock);
    ReleaseSysCache(old_gs_tup);
    CommandCounterIncrement();
}

/*
 * @Description: delete all procedure client logic info and remove dependencies
 * @param[IN] func_id Oid of functions in pg_proc
 * @return: void
 */
void delete_proc_client_info(Oid func_id)
{
    HeapTuple old_gs_tup = SearchSysCache1(GSCLPROCID, ObjectIdGetDatum(func_id));
    if (HeapTupleIsValid(old_gs_tup)) {
        ereport(DEBUG5, (errmsg("removing client logic procedure info for %u", func_id)));
        delete_proc_client_info(old_gs_tup);
    }
}
