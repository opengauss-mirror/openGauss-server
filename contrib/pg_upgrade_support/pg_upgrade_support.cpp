/*
 *	pg_upgrade_support.c
 *
 *	server-side functions to set backend global variables
 *	to control oid and relfilenode assignment, and do other special
 *	hacks needed for pg_upgrade.
 *
 *	Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *	contrib/pg_upgrade_support/pg_upgrade_support.c
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "catalog/pg_partition_fn.h"

/* THIS IS USED ONLY FOR PG >= 9.0 */

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern bool InplaceUpgradePrecommit;

extern "C" {

Datum set_next_pg_type_oid(PG_FUNCTION_ARGS);
Datum set_next_array_pg_type_oid(PG_FUNCTION_ARGS);
Datum set_next_toast_pg_type_oid(PG_FUNCTION_ARGS);

Datum set_next_heap_pg_class_oid(PG_FUNCTION_ARGS);
Datum set_next_index_pg_class_oid(PG_FUNCTION_ARGS);
Datum set_next_toast_pg_class_oid(PG_FUNCTION_ARGS);

Datum set_next_etbl_pg_type_oid(PG_FUNCTION_ARGS);
Datum set_next_etbl_array_pg_type_oid(PG_FUNCTION_ARGS);
Datum set_next_etbl_toast_pg_type_oid(PG_FUNCTION_ARGS);
Datum set_next_etbl_heap_pg_class_oid(PG_FUNCTION_ARGS);
Datum set_next_etbl_index_pg_class_oid(PG_FUNCTION_ARGS);
Datum set_next_etbl_toast_pg_class_oid(PG_FUNCTION_ARGS);

Datum set_next_pg_enum_oid(PG_FUNCTION_ARGS);
Datum set_next_pg_authid_oid(PG_FUNCTION_ARGS);

Datum set_next_partrel_pg_partition_oid(PG_FUNCTION_ARGS);

Datum set_next_part_pg_partition_oids(PG_FUNCTION_ARGS);
Datum setnext_part_toast_pg_class_oids(PG_FUNCTION_ARGS);
Datum set_next_part_toast_pg_type_oids(PG_FUNCTION_ARGS);
Datum setnext_part_index_pg_class_oids(PG_FUNCTION_ARGS);

Datum set_next_cstore_psort_oid(PG_FUNCTION_ARGS);
Datum set_next_cstore_psort_typoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_psort_atypoid(PG_FUNCTION_ARGS);

Datum set_next_cstore_delta_oid(PG_FUNCTION_ARGS);
Datum set_next_cstore_delta_typoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_delta_atypoid(PG_FUNCTION_ARGS);

Datum set_next_cstore_cudesc_oid(PG_FUNCTION_ARGS);
Datum set_next_cstore_cudesc_typoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_cudesc_atypoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_cudesc_idx_oid(PG_FUNCTION_ARGS);

Datum set_next_cstore_cudesc_toast_oid(PG_FUNCTION_ARGS);
Datum set_next_cstore_cudesc_toast_typoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_cudesc_toast_idx_oid(PG_FUNCTION_ARGS);

Datum set_next_cstore_delta_toast_oid(PG_FUNCTION_ARGS);
Datum set_next_cstore_delta_toast_typoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_delta_toast_idx_oid(PG_FUNCTION_ARGS);

Datum create_empty_extension(PG_FUNCTION_ARGS);

Datum start_upgrade_functions_manually(PG_FUNCTION_ARGS);
Datum stop_upgrade_functions_manually(PG_FUNCTION_ARGS);

Datum set_next_heap_pg_class_rfoid(PG_FUNCTION_ARGS);
Datum set_next_index_pg_class_rfoid(PG_FUNCTION_ARGS);
Datum set_next_toast_pg_class_rfoid(PG_FUNCTION_ARGS);
Datum set_next_etbl_heap_pg_class_rfoid(PG_FUNCTION_ARGS);
Datum set_next_etbl_index_pg_class_rfoid(PG_FUNCTION_ARGS);
Datum set_next_etbl_toast_pg_class_rfoid(PG_FUNCTION_ARGS);
Datum set_next_partrel_pg_partition_rfoid(PG_FUNCTION_ARGS);
Datum set_next_part_pg_partition_rfoids(PG_FUNCTION_ARGS);
Datum setnext_part_toast_pg_class_rfoids(PG_FUNCTION_ARGS);
Datum setnext_part_index_pg_class_rfoids(PG_FUNCTION_ARGS);
Datum set_next_cstore_psort_rfoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_delta_rfoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_cudesc_rfoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_cudesc_idx_rfoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_cudesc_toast_rfoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_cudesc_toast_idx_rfoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_delta_toast_rfoid(PG_FUNCTION_ARGS);
Datum set_next_cstore_delta_toast_idx_rfoid(PG_FUNCTION_ARGS);
Datum estimate_global_distinct(PG_FUNCTION_ARGS);
Datum adj_mcv(PG_FUNCTION_ARGS);
Datum start_inplace_upgrade_functions_manually(PG_FUNCTION_ARGS);
Datum stop_inplace_upgrade_functions_manually(PG_FUNCTION_ARGS);
}

PG_FUNCTION_INFO_V1(set_next_pg_type_oid);
PG_FUNCTION_INFO_V1(set_next_array_pg_type_oid);
PG_FUNCTION_INFO_V1(set_next_toast_pg_type_oid);

PG_FUNCTION_INFO_V1(set_next_heap_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_index_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_toast_pg_class_oid);

PG_FUNCTION_INFO_V1(set_next_etbl_pg_type_oid);
PG_FUNCTION_INFO_V1(set_next_etbl_array_pg_type_oid);
PG_FUNCTION_INFO_V1(set_next_etbl_toast_pg_type_oid);

PG_FUNCTION_INFO_V1(set_next_etbl_heap_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_etbl_index_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_etbl_toast_pg_class_oid);

PG_FUNCTION_INFO_V1(set_next_partrel_pg_partition_oid);
PG_FUNCTION_INFO_V1(set_next_part_pg_partition_oids);
PG_FUNCTION_INFO_V1(setnext_part_toast_pg_class_oids);
PG_FUNCTION_INFO_V1(set_next_part_toast_pg_type_oids);
PG_FUNCTION_INFO_V1(setnext_part_index_pg_class_oids);

PG_FUNCTION_INFO_V1(set_next_cstore_psort_oid);
PG_FUNCTION_INFO_V1(set_next_cstore_psort_typoid);
PG_FUNCTION_INFO_V1(set_next_cstore_psort_atypoid);

PG_FUNCTION_INFO_V1(set_next_cstore_delta_oid);
PG_FUNCTION_INFO_V1(set_next_cstore_delta_typoid);
PG_FUNCTION_INFO_V1(set_next_cstore_delta_atypoid);

PG_FUNCTION_INFO_V1(set_next_cstore_cudesc_oid);
PG_FUNCTION_INFO_V1(set_next_cstore_cudesc_typoid);
PG_FUNCTION_INFO_V1(set_next_cstore_cudesc_atypoid);
PG_FUNCTION_INFO_V1(set_next_cstore_cudesc_idx_oid);

PG_FUNCTION_INFO_V1(set_next_cstore_cudesc_toast_oid);
PG_FUNCTION_INFO_V1(set_next_cstore_cudesc_toast_typoid);

PG_FUNCTION_INFO_V1(set_next_cstore_cudesc_toast_idx_oid);

PG_FUNCTION_INFO_V1(set_next_cstore_delta_toast_oid);
PG_FUNCTION_INFO_V1(set_next_cstore_delta_toast_typoid);

PG_FUNCTION_INFO_V1(set_next_cstore_delta_toast_idx_oid);

PG_FUNCTION_INFO_V1(set_next_pg_enum_oid);
PG_FUNCTION_INFO_V1(set_next_pg_authid_oid);

PG_FUNCTION_INFO_V1(create_empty_extension);

PG_FUNCTION_INFO_V1(start_upgrade_functions_manually);
PG_FUNCTION_INFO_V1(stop_upgrade_functions_manually);

PG_FUNCTION_INFO_V1(set_next_heap_pg_class_rfoid);
PG_FUNCTION_INFO_V1(set_next_index_pg_class_rfoid);
PG_FUNCTION_INFO_V1(set_next_toast_pg_class_rfoid);

PG_FUNCTION_INFO_V1(set_next_etbl_heap_pg_class_rfoid);
PG_FUNCTION_INFO_V1(set_next_etbl_index_pg_class_rfoid);
PG_FUNCTION_INFO_V1(set_next_etbl_toast_pg_class_rfoid);

PG_FUNCTION_INFO_V1(set_next_partrel_pg_partition_rfoid);
PG_FUNCTION_INFO_V1(set_next_part_pg_partition_rfoids);
PG_FUNCTION_INFO_V1(setnext_part_toast_pg_class_rfoids);

PG_FUNCTION_INFO_V1(setnext_part_index_pg_class_rfoids);
PG_FUNCTION_INFO_V1(set_next_cstore_psort_rfoid);
PG_FUNCTION_INFO_V1(set_next_cstore_delta_rfoid);

PG_FUNCTION_INFO_V1(set_next_cstore_cudesc_rfoid);
PG_FUNCTION_INFO_V1(set_next_cstore_cudesc_idx_rfoid);
PG_FUNCTION_INFO_V1(set_next_cstore_cudesc_toast_rfoid);

PG_FUNCTION_INFO_V1(set_next_cstore_cudesc_toast_idx_rfoid);
PG_FUNCTION_INFO_V1(set_next_cstore_delta_toast_rfoid);
PG_FUNCTION_INFO_V1(set_next_cstore_delta_toast_idx_rfoid);
PG_FUNCTION_INFO_V1(estimate_global_distinct);
PG_FUNCTION_INFO_V1(adj_mcv);
PG_FUNCTION_INFO_V1(start_inplace_upgrade_functions_manually);
PG_FUNCTION_INFO_V1(stop_inplace_upgrade_functions_manually);

extern "C" {

Datum start_upgrade_functions_manually(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        ereport(ERROR, (errmodule(MOD_INSTR), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("[upgrade_functions] only system admin can start upgrade functions"))));
    }

    u_sess->proc_cxt.IsBinaryUpgrade = true;

    PG_RETURN_VOID();
}

Datum stop_upgrade_functions_manually(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        ereport(ERROR, (errmodule(MOD_INSTR), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("[upgrade_functions] only system admin can stop upgrade functions"))));
    }

    u_sess->proc_cxt.IsBinaryUpgrade = false;

    PG_RETURN_VOID();
}

Datum set_next_pg_type_oid(PG_FUNCTION_ARGS)
{
    Oid typoid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_pg_type_oid = typoid;

    PG_RETURN_VOID();
}

Datum set_next_array_pg_type_oid(PG_FUNCTION_ARGS)
{
    Oid typoid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_array_pg_type_oid = typoid;

    PG_RETURN_VOID();
}

Datum set_next_toast_pg_type_oid(PG_FUNCTION_ARGS)
{
    Oid typoid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_toast_pg_type_oid = typoid;

    PG_RETURN_VOID();
}

Datum set_next_heap_pg_class_oid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_oid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_index_pg_class_oid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_index_pg_class_oid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_toast_pg_class_oid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_oid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_etbl_pg_type_oid(PG_FUNCTION_ARGS)
{
    Oid typoid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_etbl_pg_type_oid = typoid;

    PG_RETURN_VOID();
}

Datum set_next_etbl_array_pg_type_oid(PG_FUNCTION_ARGS)
{
    Oid typoid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_etbl_array_pg_type_oid = typoid;

    PG_RETURN_VOID();
}

Datum set_next_etbl_toast_pg_type_oid(PG_FUNCTION_ARGS)
{
    Oid typoid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_etbl_toast_pg_type_oid = typoid;

    PG_RETURN_VOID();
}

Datum set_next_etbl_heap_pg_class_oid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_etbl_heap_pg_class_oid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_etbl_index_pg_class_oid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_etbl_index_pg_class_oid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_etbl_toast_pg_class_oid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_etbl_toast_pg_class_oid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_partrel_pg_partition_oid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_partrel_pg_partition_oid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_part_pg_partition_oids(PG_FUNCTION_ARGS)
{
    ArrayType* partition_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums = 0;
    int idx = 0;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    partition_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(partition_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(partition_oids));

    if (ARR_ELEMTYPE(partition_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(partition_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_pg_partition_oid) {
        u_sess->upg_cxt.binary_upgrade_next_part_pg_partition_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.binary_upgrade_cur_part_pg_partition_oid = 0;
    u_sess->upg_cxt.binary_upgrade_max_part_pg_partition_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.binary_upgrade_next_part_pg_partition_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum setnext_part_toast_pg_class_oids(PG_FUNCTION_ARGS)
{
    ArrayType* part_toast_class_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    part_toast_class_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(part_toast_class_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(part_toast_class_oids));

    if (ARR_ELEMTYPE(part_toast_class_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(part_toast_class_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_class_oid) {
        u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_class_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_class_oid = 0;
    u_sess->upg_cxt.binary_upgrade_max_part_toast_pg_class_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_class_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_part_toast_pg_type_oids(PG_FUNCTION_ARGS)
{
    ArrayType* part_toast_type_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    part_toast_type_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(part_toast_type_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(part_toast_type_oids));

    if (ARR_ELEMTYPE(part_toast_type_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(part_toast_type_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_type_oid) {
        u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_type_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_type_oid = 0;
    u_sess->upg_cxt.binary_upgrade_max_part_toast_pg_type_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_type_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum setnext_part_index_pg_class_oids(PG_FUNCTION_ARGS)
{
    ArrayType* part_index_class_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    part_index_class_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(part_index_class_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(part_index_class_oids));

    if (ARR_ELEMTYPE(part_index_class_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(part_index_class_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_index_pg_class_oid) {
        u_sess->upg_cxt.binary_upgrade_next_part_index_pg_class_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.binary_upgrade_cur_part_index_pg_class_oid = 0;
    u_sess->upg_cxt.binary_upgrade_max_part_index_pg_class_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.binary_upgrade_next_part_index_pg_class_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_pg_enum_oid(PG_FUNCTION_ARGS)
{
    Oid enumoid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_pg_enum_oid = enumoid;

    PG_RETURN_VOID();
}

Datum set_next_pg_authid_oid(PG_FUNCTION_ARGS)
{
    Oid authoid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_pg_authid_oid = authoid;
    PG_RETURN_VOID();
}

Datum create_empty_extension(PG_FUNCTION_ARGS)
{
    text* extName = PG_GETARG_TEXT_PP(0);
    text* schemaName = PG_GETARG_TEXT_PP(1);
    bool relocatable = PG_GETARG_BOOL(2);
    text* extVersion = PG_GETARG_TEXT_PP(3);
    Datum extConfig;
    Datum extCondition;
    List* requiredExtensions = NIL;

    if (PG_ARGISNULL(4))
        extConfig = PointerGetDatum(NULL);
    else
        extConfig = PG_GETARG_DATUM(4);

    if (PG_ARGISNULL(5))
        extCondition = PointerGetDatum(NULL);
    else
        extCondition = PG_GETARG_DATUM(5);

    requiredExtensions = NIL;
    if (!PG_ARGISNULL(6)) {
        ArrayType* textArray = PG_GETARG_ARRAYTYPE_P(6);
        Datum* textDatums = NULL;
        int ndatums;
        int i;

        deconstruct_array(textArray, TEXTOID, -1, false, 'i', &textDatums, NULL, &ndatums);
        for (i = 0; i < ndatums; i++) {
            text* txtname = DatumGetTextPP(textDatums[i]);
            char* extName = text_to_cstring(txtname);
            Oid extOid = get_extension_oid(extName, false);

            requiredExtensions = lappend_oid(requiredExtensions, extOid);
        }
    }

    InsertExtensionTuple(text_to_cstring(extName),
        GetUserId(),
        get_namespace_oid(text_to_cstring(schemaName), false),
        relocatable,
        text_to_cstring(extVersion),
        extConfig,
        extCondition,
        requiredExtensions);

    PG_RETURN_VOID();
}

Datum set_next_cstore_psort_oid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_psort_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_psort_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_psort_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_psort_oids));

    if (ARR_ELEMTYPE(cs_psort_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_psort_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_psort_pg_class_oid) {
        u_sess->upg_cxt.bupgrade_next_psort_pg_class_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_psort_pg_class_oid = 0;
    u_sess->upg_cxt.bupgrade_max_psort_pg_class_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_psort_pg_class_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_psort_typoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_psort_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_psort_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_psort_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_psort_oids));

    if (ARR_ELEMTYPE(cs_psort_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_psort_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_psort_pg_type_oid) {
        u_sess->upg_cxt.bupgrade_next_psort_pg_type_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_psort_pg_type_oid = 0;
    u_sess->upg_cxt.bupgrade_max_psort_pg_type_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_psort_pg_type_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_psort_atypoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_psort_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_psort_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_psort_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_psort_oids));

    if (ARR_ELEMTYPE(cs_psort_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_psort_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_psort_array_pg_type_oid) {
        u_sess->upg_cxt.bupgrade_next_psort_array_pg_type_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_psort_array_pg_type_oid = 0;
    u_sess->upg_cxt.bupgrade_max_psort_array_pg_type_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_psort_array_pg_type_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_delta_oid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_oids));

    if (ARR_ELEMTYPE(cs_delta_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_delta_pg_class_oid) {
        u_sess->upg_cxt.bupgrade_next_delta_pg_class_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_delta_pg_class_oid = 0;
    u_sess->upg_cxt.bupgrade_max_delta_pg_class_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_delta_pg_class_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_delta_typoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_oids));

    if (ARR_ELEMTYPE(cs_delta_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_delta_pg_type_oid) {
        u_sess->upg_cxt.bupgrade_next_delta_pg_type_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_delta_pg_type_oid = 0;
    u_sess->upg_cxt.bupgrade_max_delta_pg_type_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_delta_pg_type_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_delta_atypoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_oids));

    if (ARR_ELEMTYPE(cs_delta_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_delta_array_pg_type_oid) {
        u_sess->upg_cxt.bupgrade_next_delta_array_pg_type_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_delta_array_pg_type_oid = 0;
    u_sess->upg_cxt.bupgrade_max_delta_array_pg_type_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_delta_array_pg_type_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_cudesc_oid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_cudesc_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_cudesc_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_cudesc_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_cudesc_oids));

    if (ARR_ELEMTYPE(cs_cudesc_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_cudesc_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_oid) {
        u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_cudesc_pg_class_oid = 0;
    u_sess->upg_cxt.bupgrade_max_cudesc_pg_class_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_cudesc_typoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_cudesc_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_cudesc_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_cudesc_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_cudesc_oids));

    if (ARR_ELEMTYPE(cs_cudesc_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_cudesc_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_cudesc_pg_type_oid) {
        u_sess->upg_cxt.bupgrade_next_cudesc_pg_type_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_cudesc_pg_type_oid = 0;
    u_sess->upg_cxt.bupgrade_max_cudesc_pg_type_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_cudesc_pg_type_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_cudesc_atypoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_cudesc_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_cudesc_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_cudesc_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_cudesc_oids));

    if (ARR_ELEMTYPE(cs_cudesc_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_cudesc_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_cudesc_array_pg_type_oid) {
        u_sess->upg_cxt.bupgrade_next_cudesc_array_pg_type_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_cudesc_array_pg_type_oid = 0;
    u_sess->upg_cxt.bupgrade_max_cudesc_array_pg_type_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_cudesc_array_pg_type_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_cudesc_idx_oid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_oids));

    if (ARR_ELEMTYPE(cs_delta_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_cudesc_index_oid) {
        u_sess->upg_cxt.bupgrade_next_cudesc_index_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_cudesc_index_oid = 0;
    u_sess->upg_cxt.bupgrade_max_cudesc_index_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_cudesc_index_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_cudesc_toast_oid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_oids));

    if (ARR_ELEMTYPE(cs_delta_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_class_oid) {
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_class_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_class_oid = 0;
    u_sess->upg_cxt.bupgrade_max_cudesc_toast_pg_class_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_class_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_cudesc_toast_typoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_oids));

    if (ARR_ELEMTYPE(cs_delta_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_type_oid) {
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_type_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_type_oid = 0;
    u_sess->upg_cxt.bupgrade_max_cudesc_toast_pg_type_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_type_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_cudesc_toast_idx_oid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_oids));

    if (ARR_ELEMTYPE(cs_delta_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_cudesc_toast_index_oid) {
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_index_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_cudesc_toast_index_oid = 0;
    u_sess->upg_cxt.bupgrade_max_cudesc_toast_index_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_index_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_delta_toast_oid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_oids));

    if (ARR_ELEMTYPE(cs_delta_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_delta_toast_pg_class_oid) {
        u_sess->upg_cxt.bupgrade_next_delta_toast_pg_class_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_class_oid = 0;
    u_sess->upg_cxt.bupgrade_max_delta_toast_pg_class_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_delta_toast_pg_class_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_delta_toast_typoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_oids));

    if (ARR_ELEMTYPE(cs_delta_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_delta_toast_pg_type_oid) {
        u_sess->upg_cxt.bupgrade_next_delta_toast_pg_type_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_type_oid = 0;
    u_sess->upg_cxt.bupgrade_max_delta_toast_pg_type_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_delta_toast_pg_type_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_delta_toast_idx_oid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_oids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_oids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_oids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_oids));

    if (ARR_ELEMTYPE(cs_delta_oids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_oids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_delta_toast_index_oid) {
        u_sess->upg_cxt.bupgrade_next_delta_toast_index_oid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_delta_toast_index_oid = 0;
    u_sess->upg_cxt.bupgrade_max_delta_toast_index_oid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_delta_toast_index_oid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_heap_pg_class_rfoid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_rfoid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_index_pg_class_rfoid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_index_pg_class_rfoid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_toast_pg_class_rfoid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_rfoid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_etbl_heap_pg_class_rfoid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_etbl_heap_pg_class_rfoid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_etbl_index_pg_class_rfoid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_etbl_index_pg_class_rfoid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_etbl_toast_pg_class_rfoid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_etbl_toast_pg_class_rfoid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_partrel_pg_partition_rfoid(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);

    u_sess->upg_cxt.binary_upgrade_next_partrel_pg_partition_rfoid = reloid;

    PG_RETURN_VOID();
}

Datum set_next_part_pg_partition_rfoids(PG_FUNCTION_ARGS)
{
    ArrayType* partition_rfoids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    partition_rfoids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(partition_rfoids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(partition_rfoids));

    if (ARR_ELEMTYPE(partition_rfoids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(partition_rfoids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_pg_partition_rfoid) {
        u_sess->upg_cxt.binary_upgrade_next_part_pg_partition_rfoid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.binary_upgrade_cur_part_pg_partition_rfoid = 0;
    u_sess->upg_cxt.binary_upgrade_max_part_pg_partition_rfoid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.binary_upgrade_next_part_pg_partition_rfoid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum setnext_part_toast_pg_class_rfoids(PG_FUNCTION_ARGS)
{
    ArrayType* part_toast_class_rfoids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    part_toast_class_rfoids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(part_toast_class_rfoids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(part_toast_class_rfoids));

    if (ARR_ELEMTYPE(part_toast_class_rfoids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(part_toast_class_rfoids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_class_rfoid) {
        u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_class_rfoid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.binary_upgrade_cur_part_toast_pg_class_rfoid = 0;
    u_sess->upg_cxt.binary_upgrade_max_part_toast_pg_class_rfoid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.binary_upgrade_next_part_toast_pg_class_rfoid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum setnext_part_index_pg_class_rfoids(PG_FUNCTION_ARGS)
{
    ArrayType* part_index_class_rfoids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    part_index_class_rfoids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(part_index_class_rfoids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(part_index_class_rfoids));

    if (ARR_ELEMTYPE(part_index_class_rfoids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(part_index_class_rfoids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.binary_upgrade_next_part_index_pg_class_rfoid) {
        u_sess->upg_cxt.binary_upgrade_next_part_index_pg_class_rfoid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.binary_upgrade_cur_part_index_pg_class_rfoid = 0;
    u_sess->upg_cxt.binary_upgrade_max_part_index_pg_class_rfoid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.binary_upgrade_next_part_index_pg_class_rfoid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_psort_rfoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_psort_rfoids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_psort_rfoids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_psort_rfoids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_psort_rfoids));

    if (ARR_ELEMTYPE(cs_psort_rfoids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_psort_rfoids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_psort_pg_class_rfoid) {
        u_sess->upg_cxt.bupgrade_next_psort_pg_class_rfoid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_psort_pg_class_rfoid = 0;
    u_sess->upg_cxt.bupgrade_max_psort_pg_class_rfoid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_psort_pg_class_rfoid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_delta_rfoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_rfoids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_rfoids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_rfoids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_rfoids));

    if (ARR_ELEMTYPE(cs_delta_rfoids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_rfoids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_delta_pg_class_rfoid) {
        u_sess->upg_cxt.bupgrade_next_delta_pg_class_rfoid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_delta_pg_class_rfoid = 0;
    u_sess->upg_cxt.bupgrade_max_delta_pg_class_rfoid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_delta_pg_class_rfoid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_cudesc_rfoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_cudesc_rfoids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_cudesc_rfoids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_cudesc_rfoids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_cudesc_rfoids));

    if (ARR_ELEMTYPE(cs_cudesc_rfoids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_cudesc_rfoids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_rfoid) {
        u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_rfoid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_cudesc_pg_class_rfoid = 0;
    u_sess->upg_cxt.bupgrade_max_cudesc_pg_class_rfoid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_rfoid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_cudesc_idx_rfoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_rfoids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_rfoids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_rfoids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_rfoids));

    if (ARR_ELEMTYPE(cs_delta_rfoids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_rfoids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_cudesc_index_rfoid) {
        u_sess->upg_cxt.bupgrade_next_cudesc_index_rfoid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_cudesc_index_rfoid = 0;
    u_sess->upg_cxt.bupgrade_max_cudesc_index_rfoid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_cudesc_index_rfoid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_cudesc_toast_rfoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_rfoids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_rfoids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_rfoids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_rfoids));

    if (ARR_ELEMTYPE(cs_delta_rfoids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_rfoids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_class_rfoid) {
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_class_rfoid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_class_rfoid = 0;
    u_sess->upg_cxt.bupgrade_max_cudesc_toast_pg_class_rfoid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_class_rfoid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_cudesc_toast_idx_rfoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_rfoids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_rfoids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_rfoids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_rfoids));

    if (ARR_ELEMTYPE(cs_delta_rfoids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_rfoids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_cudesc_toast_index_rfoid) {
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_index_rfoid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_cudesc_toast_index_rfoid = 0;
    u_sess->upg_cxt.bupgrade_max_cudesc_toast_index_rfoid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_index_rfoid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_delta_toast_rfoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_rfoids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_rfoids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_rfoids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_rfoids));

    if (ARR_ELEMTYPE(cs_delta_rfoids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_rfoids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_delta_toast_pg_class_rfoid) {
        u_sess->upg_cxt.bupgrade_next_delta_toast_pg_class_rfoid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_class_rfoid = 0;
    u_sess->upg_cxt.bupgrade_max_delta_toast_pg_class_rfoid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_delta_toast_pg_class_rfoid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum set_next_cstore_delta_toast_idx_rfoid(PG_FUNCTION_ARGS)
{
    ArrayType* cs_delta_rfoids = NULL;
    Datum* oiddatums = NULL;
    int ndatums;
    int idx;

    if (PG_ARGISNULL(0) || PG_GETARG_POINTER(0) == NULL)
        PG_RETURN_VOID();

    cs_delta_rfoids = PG_GETARG_ARRAYTYPE_P(0);

    if (ARR_NDIM(cs_delta_rfoids) != 1)
        elog(ERROR, "array must be one-dimensional, not %d dimensions", ARR_NDIM(cs_delta_rfoids));

    if (ARR_ELEMTYPE(cs_delta_rfoids) != OIDOID)
        elog(ERROR, "array must contain tsquery elements");

    deconstruct_array(cs_delta_rfoids, OIDOID, sizeof(Oid), true, 'i', &oiddatums, NULL, &ndatums);

    if (NULL == u_sess->upg_cxt.bupgrade_next_delta_toast_index_rfoid) {
        u_sess->upg_cxt.bupgrade_next_delta_toast_index_rfoid = (Oid*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), MAX_PARTITION_NUM * sizeof(Oid));
    }

    u_sess->upg_cxt.bupgrade_cur_delta_toast_index_rfoid = 0;
    u_sess->upg_cxt.bupgrade_max_delta_toast_index_rfoid = ndatums;
    for (idx = 0; idx < ndatums; idx++) {
        u_sess->upg_cxt.bupgrade_next_delta_toast_index_rfoid[idx] = oiddatums[idx];
    }

    PG_RETURN_VOID();
}

Datum estimate_global_distinct(PG_FUNCTION_ARGS)
{
    float dndistinct = PG_GETARG_FLOAT4(0);
    double dntuples = PG_GETARG_FLOAT8(1);
    int dnnum = PG_GETARG_INT32(2);
    bool diskey = PG_GETARG_BOOL(3);
    float globaldistinct = 0.0;

    if (dntuples == 0)
        dntuples = 1;

    if (0.0 == dndistinct)
        dndistinct = 1.0;
    else if (dndistinct < 0)
        dndistinct = -dntuples * dndistinct;

    if (dndistinct > dntuples)
        dndistinct = dntuples;

    if (diskey)
        globaldistinct = dndistinct * dnnum;
    else {
        double descRatio = 1.0;
        descRatio = dndistinct / dntuples;
        if (descRatio >= 1.0)
            globaldistinct = dndistinct * dnnum;
        else {
            double temp = DatumGetFloat8(DirectFunctionCall2(dpow, descRatio, dnnum));
            globaldistinct = dndistinct * (1 - temp) / (1 - descRatio);
        }
    }
    if (globaldistinct < dndistinct)
        globaldistinct = dndistinct;
    else if (globaldistinct > dntuples * dnnum)
        globaldistinct = dntuples * dnnum;

    if (globaldistinct >= dntuples * dnnum * 0.1)
        globaldistinct = -globaldistinct / (dntuples * dnnum);
    else
        globaldistinct = rint(globaldistinct);
    PG_RETURN_FLOAT4(globaldistinct);
}

Datum adj_mcv(PG_FUNCTION_ARGS)
{
    ArrayType* mcv_array = (ArrayType*)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    int dnnum = PG_GETARG_INT32(1);
    int i;
    float4* arrdata = (float4*)ARR_DATA_PTR(mcv_array);

    if (ARR_NDIM(mcv_array) != 1)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("array of weight must be one-dimensional")));

    if (dnnum == 0)
        dnnum = 1;
    for (i = 0; i < (int)ARR_SIZE(mcv_array); i++) {
        arrdata[i] = arrdata[i] / dnnum;
    }
    PG_RETURN_ARRAYTYPE_P(mcv_array);
}

Datum start_inplace_upgrade_functions_manually(PG_FUNCTION_ARGS)
{
    u_sess->upg_cxt.InplaceUpgradeSwitch = true;

    PG_RETURN_VOID();
}

Datum stop_inplace_upgrade_functions_manually(PG_FUNCTION_ARGS)
{
    u_sess->upg_cxt.InplaceUpgradeSwitch = false;
    InplaceUpgradePrecommit = false;

    PG_RETURN_VOID();
}
}