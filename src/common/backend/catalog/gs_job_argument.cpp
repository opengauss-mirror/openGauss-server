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
 *
 * gs_job_argument.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/catalog/catalog/gs_job_argument.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include <limits.h>
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/alter.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/schemacmds.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/dbe_scheduler.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/pg_job.h"
#include "catalog/pg_job_proc.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/gs_job_argument.h"
#include "catalog/gs_job_attribute.h"
#include "fmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "pgxc/execRemote.h"

/* Function definitions */
static void check_define_argument_valid(const Datum job_name, const Datum argument_position);

/*
 * @brief search_by_sysscan_1
 *  Perform sysscan.
 * @param rel
 * @param scan_key_info
 * @return List*
 */
List *search_by_sysscan_1(Relation rel, ScanKeyInfo *scan_key_info)
{
    ScanKeyData key[1];
    ScanKeyInit(&key[0], scan_key_info->attribute_number, BTEqualStrategyNumber, scan_key_info->procedure,
                scan_key_info->attribute_value);
    SysScanDesc index_scan = systable_beginscan(rel, InvalidOid, false, SnapshotNow, 1, key);
    HeapTuple tuple;
    List *tuples = NIL;
    while (HeapTupleIsValid(tuple = systable_getnext(index_scan))) {
        tuples = lappend(tuples, heap_copytuple(tuple));
    }
    systable_endscan(index_scan);
    return tuples;
}

/*
 * @brief search_by_sysscan_2
 *  Perform sysscan with two keys.
 * @param rel
 * @param scan_key_info1
 * @param scan_key_info2
 * @return List*
 */
List *search_by_sysscan_2(Relation rel, ScanKeyInfo *scan_key_info1, ScanKeyInfo *scan_key_info2)
{
    ScanKeyData key[2];
    ScanKeyInit(&key[0], scan_key_info1->attribute_number, BTEqualStrategyNumber, scan_key_info1->procedure,
                scan_key_info1->attribute_value);
    ScanKeyInit(&key[1], scan_key_info2->attribute_number, BTEqualStrategyNumber, scan_key_info2->procedure,
                scan_key_info2->attribute_value);
    SysScanDesc index_scan = systable_beginscan(rel, InvalidOid, false, SnapshotNow, 2, key);
    HeapTuple tuple;
    List *tuples = NIL;
    while (HeapTupleIsValid(tuple = systable_getnext(index_scan))) {
        tuples = lappend(tuples, heap_copytuple(tuple));
    }
    systable_endscan(index_scan);
    return tuples;
}

/*
 * @brief search_by_argument_position
 *  Get the tuple from gs_job_argument with given position.
 */
static HeapTuple search_by_argument_position(Relation gs_job_argument_rel, Datum job_name, Datum argument_position)
{
    ScanKeyInfo scan_key_info1;
    scan_key_info1.attribute_value = job_name;
    scan_key_info1.attribute_number = Anum_gs_job_argument_job_name;
    scan_key_info1.procedure = F_TEXTEQ;
    ScanKeyInfo scan_key_info2;
    scan_key_info2.attribute_value = argument_position;
    scan_key_info2.attribute_number = Anum_gs_job_argument_argument_position;
    scan_key_info2.procedure = F_INT4EQ;

    List *tuples = search_by_sysscan_2(gs_job_argument_rel, &scan_key_info1, &scan_key_info2);
    Assert(list_length(tuples) <= 1);
    HeapTuple tuple = NULL;
    if (list_length(tuples) > 1) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("find %d rows match the search in system table gs_job_argument.", list_length(tuples)),
                        errdetail("N/A"), errcause("job_name or argument not match"),
                        erraction("Please check job_name and argument")));
    } else if (list_length(tuples) == 1) {
        tuple = (HeapTuple)linitial(tuples);
        list_free_ext(tuples);
    }
    return tuple;
}

/*
 * @brief search_by_argument_name
 *  Get the tuple from gs_job_argument with given name for PROGRAM ONLY.
 */
static HeapTuple search_by_argument_name(Relation gs_job_argument_rel, Datum program_name, Datum argument_name)
{
    ScanKeyInfo scan_key_info1;
    scan_key_info1.attribute_value = program_name;
    scan_key_info1.attribute_number = Anum_gs_job_argument_job_name;
    scan_key_info1.procedure = F_TEXTEQ;
    ScanKeyInfo scan_key_info2;
    scan_key_info2.attribute_value = argument_name;
    scan_key_info2.attribute_number = Anum_gs_job_argument_argument_name;
    scan_key_info2.procedure = F_TEXTEQ;

    List *tuples = search_by_sysscan_2(gs_job_argument_rel, &scan_key_info1, &scan_key_info2);
    Assert(list_length(tuples) <= 1);
    HeapTuple tuple = NULL;
    if (list_length(tuples) > 1) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Cannot set job argument value."), errdetail("Arguments cannot have the same name."),
                        errcause("Multiple arguments found."), erraction("Please check program's argument name.")));
    } else if (list_length(tuples) == 1) {
        tuple = (HeapTuple)linitial(tuples);
        list_free_ext(tuples);
    }
    return tuple;
}

/*
 * @brief get_default_argument_name
 *  Get the default argument name.
 */
static Datum get_default_argument_name(Datum argument_position)
{
#define MAX_ARGUMENT_NAME_LEN 1024
    char name[MAX_ARGUMENT_NAME_LEN] = {0};
    int position = DatumGetInt32(argument_position);
    errno_t rc = snprintf_s(name, MAX_ARGUMENT_NAME_LEN, MAX_ARGUMENT_NAME_LEN, "default_argument_%d", position);
    securec_check_ss_c(rc, "\0", "\0");

    return CStringGetTextDatum(name);
}

/*
 * @brief get_program_argument_type
 *  Get argument type from associated program.
 */
static Datum get_program_argument_type(Relation gs_job_argument_rel, Datum job_name, Datum arg_position, Datum *values)
{
    Datum program_name = get_attribute_value(job_name, "program_name", AccessShareLock, false, false);
    HeapTuple oldtuple = SearchSysCache2(JOBARGUMENTPOSITION, program_name, arg_position);
    if (oldtuple == NULL) {
        return get_default_argument_type();
    }
    bool isnull = false;
    Datum argument_type = heap_getattr(oldtuple, Anum_gs_job_argument_argument_type,
                                       RelationGetDescr(gs_job_argument_rel), &isnull);
    Datum argument_type_dup = DirectFunctionCall1(namein, CStringGetDatum(pstrdup(DatumGetPointer(argument_type))));
    ReleaseSysCache(oldtuple);
    pfree(DatumGetPointer(program_name));
    return argument_type_dup;
}

/*
 * @brief insert_argument_value
 *  Insert argument value base on position.
 */
static void insert_argument_value(Relation gs_job_argument_rel, Datum job_name, Datum arg_position, Datum arg_value)
{
    Relation attr_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    Datum program_name = CStringGetTextDatum("program_name");
    bool isnull = false;
    Datum program_name_value = lookup_job_attribute(attr_rel, job_name, program_name, &isnull, false);

    check_define_argument_valid(program_name_value, arg_position);
    heap_close(attr_rel, NoLock);
    pfree(DatumGetPointer(program_name_value));
    pfree(DatumGetPointer(program_name));

    Datum values[Natts_gs_job_argument];
    bool nulls[Natts_gs_job_argument];
    error_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    Datum arg_type = get_program_argument_type(gs_job_argument_rel, job_name, arg_position, values);
    values[Anum_gs_job_argument_argument_name - 1] = get_default_argument_name(arg_position);
    values[Anum_gs_job_argument_argument_type - 1] = arg_type;
    values[Anum_gs_job_argument_job_name - 1] = job_name;
    values[Anum_gs_job_argument_argument_position - 1] = arg_position;
    values[Anum_gs_job_argument_argument_value - 1] = arg_value;
    nulls[Anum_gs_job_argument_default_value - 1] = true;

    HeapTuple tuple = heap_form_tuple(RelationGetDescr(gs_job_argument_rel), values, nulls);
    (void)simple_heap_insert(gs_job_argument_rel, tuple);
    CatalogUpdateIndexes(gs_job_argument_rel, tuple);
    heap_freetuple_ext(tuple);
    pfree(DatumGetPointer(values[Anum_gs_job_argument_argument_name - 1]));
}

/*
 * @brief update_argument_value
 *  Update the job argument value.
 */
static void update_argument_value(Relation gs_job_argument_rel, HeapTuple oldtuple, Datum job_name,
                                  Datum argument_position, Datum argument_value)
{
    Datum values[Natts_gs_job_argument];
    bool nulls[Natts_gs_job_argument];
    bool replaces[Natts_gs_job_argument];
    error_t rc = memset_s(replaces, sizeof(replaces), 0, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    Datum argument_type = get_program_argument_type(gs_job_argument_rel, job_name, argument_position, values);
    replaces[Anum_gs_job_argument_argument_type - 1] = true;
    values[Anum_gs_job_argument_argument_type - 1] = argument_type;
    nulls[Anum_gs_job_argument_argument_type - 1] = false;

    replaces[Anum_gs_job_argument_argument_value - 1] = true;
    values[Anum_gs_job_argument_argument_value - 1] = argument_value;
    nulls[Anum_gs_job_argument_argument_value - 1] = false;

    HeapTuple newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(gs_job_argument_rel), values, nulls, replaces);
    simple_heap_update(gs_job_argument_rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(gs_job_argument_rel, newtuple);
    heap_freetuple_ext(newtuple);
}

/*
 * @brief set_job_argument_value_1_internal
 *  Set the job argument value.
 */
void set_job_argument_value_1_internal(PG_FUNCTION_ARGS)
{
    Datum job_name = PG_GETARG_DATUM(0);
    Datum argument_position = PG_GETARG_DATUM(1);
    Datum argument_value = PG_GETARG_DATUM(2);
    check_object_type_matched(job_name, "job");
    check_object_is_visible(job_name);

    Relation gs_job_argument_rel = heap_open(GsJobArgumentRelationId, RowExclusiveLock);
    HeapTuple oldtuple = search_by_argument_position(gs_job_argument_rel, job_name, argument_position);
    if (oldtuple != NULL) {
        update_argument_value(gs_job_argument_rel, oldtuple, job_name, argument_position, argument_value);
        pfree(oldtuple);
    } else {
        insert_argument_value(gs_job_argument_rel, job_name, argument_position, argument_value);
    }
    heap_close(gs_job_argument_rel, NoLock);
}

/*
 * @brief get_argument_position_from_name
 *  Get arguemnt position base on its argument name.
 * @param gs_job_argument_rel   relation
 * @param job_name              job name
 * @param argument_name         argument name
 * @return Datum                argument position
 */
static Datum get_argument_position_from_name(Relation gs_job_argument_rel, Datum job_name, Datum argument_name)
{
    Datum program_name = get_attribute_value(job_name, "program_name", AccessShareLock, false, false);
    HeapTuple tuple = search_by_argument_name(gs_job_argument_rel, program_name, argument_name);
    if (tuple == NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Fail to set job argument value."),
                        errdetail("Argument %s was never defined.", TextDatumGetCString(argument_name)),
                        errcause("Trying to set argument value without defining it."),
                        erraction("Please define program argument before set value.")));
    }
    bool isnull = true;
    Datum argument_position = heap_getattr(tuple, Anum_gs_job_argument_argument_position,
                                           RelationGetDescr(gs_job_argument_rel), &isnull);
    int position = DatumGetInt32(argument_position);
    pfree(tuple);
    return Int32GetDatum(position);
}

/*
 * @brief set_job_argument_value_2_internal
 *  Set the job argument value. This is a common overload of the previous one.
 *  Note that we do not actually save the name for job entries.
 */
void set_job_argument_value_2_internal(PG_FUNCTION_ARGS)
{
    Datum job_name = PG_GETARG_DATUM(0);
    Datum argument_name = PG_GETARG_DATUM(1);
    Datum argument_value = PG_GETARG_DATUM(2);
    check_object_type_matched(job_name, "job");
    check_object_is_visible(job_name);

    Relation gs_job_argument_rel = heap_open(GsJobArgumentRelationId, RowExclusiveLock);
    Datum argument_position = get_argument_position_from_name(gs_job_argument_rel, job_name, argument_name);
    HeapTuple oldtuple = search_by_argument_position(gs_job_argument_rel, job_name, argument_position);
    if (oldtuple != NULL) {
        update_argument_value(gs_job_argument_rel, oldtuple, job_name, argument_position, argument_value);
        pfree(oldtuple);
    } else {
        insert_argument_value(gs_job_argument_rel, job_name, argument_position, argument_value);
    }
    heap_close(gs_job_argument_rel, NoLock);
}

/*
 * @brief delete_argument_by_position
 *  Delete argument entry by its position.
 * @param gs_job_argument_rel
 * @param program_name
 * @param argument_position
 */
static void delete_argument_by_position(Relation gs_job_argument_rel, Datum program_name, Datum argument_position)
{
    ScanKeyData key[2];
    ScanKeyInit(&key[0], Anum_gs_job_argument_job_name, BTEqualStrategyNumber, F_TEXTEQ, program_name);
    ScanKeyInit(&key[1], Anum_gs_job_argument_argument_position, BTEqualStrategyNumber, F_INT4EQ, argument_position);
    SysScanDesc index_scan = systable_beginscan(gs_job_argument_rel, InvalidOid, false, SnapshotNow, 2, key);
    HeapTuple tuple;
    while (HeapTupleIsValid(tuple = systable_getnext(index_scan))) {
        simple_heap_delete(gs_job_argument_rel, &tuple->t_self);
    }
    systable_endscan(index_scan);
}

/*
 * @brief delete_deprecated_program_arguments
 *  Clean up program arguments and related job's argument. Then disable the program.
 * @param program_name
 * @param delete_self   delete program's argument if true.
 */
void delete_deprecated_program_arguments(Datum program_name, bool delete_self)
{
    if (delete_self) {
        delete_from_argument(program_name);
        enable_program(program_name, CStringGetTextDatum("false"));
    }
    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    List *tuples = search_related_attribute(gs_job_attribute_rel, CStringGetTextDatum("program_name"), program_name);
    ListCell *lc = NULL;
    foreach(lc, tuples) {
        HeapTuple tuple = (HeapTuple)lfirst(lc);
        bool isnull = false;
        Datum job_name = heap_getattr(tuple, Anum_gs_job_attribute_job_name, gs_job_attribute_rel->rd_att, &isnull);
        delete_from_argument(job_name);
    }
    heap_close(gs_job_attribute_rel, NoLock);
    list_free_deep(tuples);
}

/*
 * @brief check_define_argument_valid
 *  Check if argument number is within range.
 * @param program_name
 * @param argument_position
 */
static void check_define_argument_valid(const Datum program_name, const Datum argument_position)
{
    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    bool isNull = false;
    Datum number_of_arguments = lookup_job_attribute(gs_job_attribute_rel, program_name,
                                                     CStringGetTextDatum("number_of_arguments"), &isNull, false);
    int32_t max_number = DatumGetInt32(TextToInt32(number_of_arguments));
    int32 position = DatumGetInt32(argument_position);
    if (position <= 0 || position > max_number) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Argument position specified is out of range."),
                        errdetail("Argument %d should within range (0, %d].", position, max_number), errcause("N/A"),
                        erraction("Please check argument_position")));
    }
    heap_close(gs_job_attribute_rel, NoLock);
}

/*
 * @brief define_program_argument
 *  Define arguments.
 */
static void define_program_argument(PG_FUNCTION_ARGS)
{
    Datum values[Natts_gs_job_argument];
    bool nulls[Natts_gs_job_argument];
    error_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    Datum program_name = PG_GETARG_DATUM(0);
    check_object_is_visible(PG_GETARG_DATUM(0));
    check_object_type_matched(program_name, "program");
    if (is_inlined_program(program_name)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid program name"), errdetail("Cannot modify inlined program."),
                        errcause("Try to modify inlined object."), erraction("Please enter a valid program name")));
    }
    Datum argument_position = PG_GETARG_DATUM(1);
    check_define_argument_valid(program_name, argument_position);

    Datum argument_name = PG_GETARG_DATUM(2);
    Datum argument_type = DirectFunctionCall1(namein, CStringGetDatum(TextDatumGetCString(PG_GETARG_DATUM(3))));
    if (PG_NARGS() == 6) {
        Datum default_value = PG_GETARG_DATUM(4);
        values[Anum_gs_job_argument_default_value - 1] = default_value;
    } else {
        nulls[Anum_gs_job_argument_default_value - 1] = true;
    }

    values[Anum_gs_job_argument_job_name - 1] = program_name;
    values[Anum_gs_job_argument_argument_name - 1] = PG_ARGISNULL(2) ? get_default_argument_name(argument_position) : \
                                                                       argument_name;
    values[Anum_gs_job_argument_argument_position - 1] = argument_position;
    values[Anum_gs_job_argument_argument_type - 1] = argument_type;
    nulls[Anum_gs_job_argument_argument_value - 1] = true;

    Relation gs_job_argument_rel = heap_open(GsJobArgumentRelationId, RowExclusiveLock);
    delete_argument_by_position(gs_job_argument_rel, program_name, argument_position);
    HeapTuple tuple = heap_form_tuple(RelationGetDescr(gs_job_argument_rel), values, nulls);
    (void)simple_heap_insert(gs_job_argument_rel, tuple);
    CatalogUpdateIndexes(gs_job_argument_rel, tuple);
    heap_freetuple_ext(tuple);
    heap_close(gs_job_argument_rel, NoLock);
    delete_deprecated_program_arguments(program_name, false);
}

/*
 * @brief define_program_argument_1_internal
 *  Wrapper.
 */
void define_program_argument_1_internal(PG_FUNCTION_ARGS)
{
    define_program_argument(fcinfo);
}

/*
 * @brief define_program_argument_2_internal
 *  Wrapper.
 */
void define_program_argument_2_internal(PG_FUNCTION_ARGS)
{
    define_program_argument(fcinfo);
}