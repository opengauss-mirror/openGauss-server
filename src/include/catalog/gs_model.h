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
 * gs_model.h
 *
 * IDENTIFICATION
 *    src/include/catalog/gs_model.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_MODEL_H
#define GS_MODEL_H

#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/genbki.h"
#include "catalog/indexing.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/date.h"

#define ModelRelationId 3991
#define ModelRelation_Rowtype_Id 3994

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

CATALOG(gs_model_warehouse,3991) BKI_ROWTYPE_OID(3994) BKI_SCHEMA_MACRO
{
    NameData    modelname;         /* model name */
    Oid         modelowner;        /* model owner */
    timestamp   createtime;     /* Model storage time */
    int4        processedtuples;
    int4        discardedtuples;
    float4      preprocesstime;
    float4      exectime;
    int4        iterations;
    Oid         outputtype;
#ifdef CATALOG_VARLEN /* variable-length fields start here */
    text        modeltype;          /* svm、kmeans、invalid */
    text        query;
    bytea       modeldata;          /* OPTION  just for ModelBinary*/
    float4      weight[1];
    text        hyperparametersnames[1];
    text        hyperparametersvalues[1];
    Oid         hyperparametersoids[1];
    text        coefnames[1];
    text        coefvalues[1];
    Oid         coefoids[1];
    text        trainingscoresname[1];
    float4      trainingscoresvalue[1];
    text        modeldescribe[1];
#endif /* CATALOG_VARLEN */
} FormData_gs_model_warehouse;

typedef FormData_gs_model_warehouse *Form_gs_model_warehouse;

#define Natts_gs_model_warehouse            22
#define Anum_gs_model_model_name            1
#define Anum_gs_model_owner_oid             2
#define Anum_gs_model_create_time           3
#define Anum_gs_model_processedTuples       4
#define Anum_gs_model_discardedTuples       5
#define Anum_gs_model_process_time_secs     6
#define Anum_gs_model_exec_time_secs        7
#define Anum_gs_model_iterations            8
#define Anum_gs_model_outputType            9

#define Anum_gs_model_model_type            10
#define Anum_gs_model_query                 11
#define Anum_gs_model_modelData             12
#define Anum_gs_model_weight                13
#define Anum_gs_model_hyperparametersNames  14
#define Anum_gs_model_hyperparametersValues 15
#define Anum_gs_model_hyperparametersOids   16
#define Anum_gs_model_coefNames             17
#define Anum_gs_model_coefValues            18
#define Anum_gs_model_coefOids              19
#define Anum_gs_model_trainingScoresName    20
#define Anum_gs_model_trainingScoresValue   21
#define Anum_gs_model_modeldescribe         22



// Locate the oid for a given model name
inline Oid get_model_oid(const char* model_name, bool missing_ok)
{
    Oid oid;

    oid = GetSysCacheOid1(DB4AI_MODEL, CStringGetDatum(model_name));
    if (!OidIsValid(oid) && !missing_ok) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("model \"%s\" does not exist", model_name)));
    }
    return oid;
}



// Remove a model by oid
inline void remove_model_by_oid(Oid model_oid)
{
    Relation rel;
    HeapTuple tup;
    ScanKeyData skey[1];
    SysScanDesc scan;

    if (t_thrd.proc->workingVersionNum < 92366)
        return;

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(model_oid));

    rel = heap_open(ModelRelationId, RowExclusiveLock);

    scan = systable_beginscan(rel, GsModelOidIndexId, true, SnapshotNow, 1, skey);

    /* we expect exactly one match */
    tup = systable_getnext(scan);
    if (!HeapTupleIsValid(tup))
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("could not find tuple for model entry %u", model_oid)));

    simple_heap_delete(rel, &tup->t_self);

    systable_endscan(scan);
    heap_close(rel, RowExclusiveLock);
}

#endif
