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
 * gs_job_manager.cpp
 *    Functions to run/stop/execute/drop dbe jobs.
 *
 * IDENTIFICATION
 *    src/gausskernel/process/job/gs_job_manager.cpp
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
#include "pgxc/execRemote.h"

/* Run job methods */
static bool run_sql_job(Datum job_name, StringInfoData *buf);
static bool run_procedure_job(Datum job_name, StringInfoData *buf);
static char *run_external_job(Datum job_name);

/*
 * @brief delete_by_syscache
 *  Perform a simple heap delete by searching syscache.
 * @param rel           Target relation
 * @param object_name   Delete by key
 * @param cache_id      Cache ID
 */
static void delete_by_syscache(Relation rel, const Datum object_name, SysCacheIdentifier cache_id)
{
    CatCList *tuples = SearchSysCacheList1(cache_id, object_name);
    if (tuples == NULL) {
        return;
    }
    for (int i = 0; i < tuples->n_members; i++) {
        HeapTuple tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(tuples, i);
        simple_heap_delete(rel, &tuple->t_self);
    }
    ReleaseSysCacheList(tuples);
}

/*
 * @brief delete_from_attribute
 *  Delete from gs_job_attribute.
 * @param object_name
 */
void delete_from_attribute(const Datum object_name)
{
    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    delete_by_syscache(gs_job_attribute_rel, object_name, JOBATTRIBUTENAME);
    heap_close(gs_job_attribute_rel, NoLock);
}

/*
 * @brief delete_from_argument
 *  Delete from gs_job_argument.
 * @param job_name
 */
void delete_from_argument(const Datum object_name)
{
    Relation rel = heap_open(GsJobArgumentRelationId, RowExclusiveLock);
    delete_by_syscache(rel, object_name, JOBARGUMENTNAME);
    heap_close(rel, NoLock);
}

/*
 * @brief delete_from_job
 *  Delete from pg_job.
 * @param job_name
 */
void delete_from_job(const Datum job_name)
{
    Relation rel = heap_open(PgJobRelationId, RowExclusiveLock);
    HeapTuple tuple = search_from_pg_job(rel, job_name);
    if (tuple != NULL) {
        simple_heap_delete(rel, &tuple->t_self);
    }
    heap_close(rel, NoLock);
}

/*
 * @brief delete_from_job_proc
 *  Delete from pg_job_proc.
 * @param job_name
 */
void delete_from_job_proc(const Datum job_name)
{
    Relation rel = heap_open(PgJobProcRelationId, RowExclusiveLock);
    HeapTuple tuple = search_from_pg_job_proc_no_exception(rel, job_name);
    if (tuple != NULL) {
        simple_heap_delete(rel, &tuple->t_self);
    }
    heap_close(rel, NoLock);
}

HeapTuple search_from_pg_job(Relation pg_job_rel, Datum job_name)
{
    ScanKeyInfo scan_key_info1;
    scan_key_info1.attribute_value = job_name;
    scan_key_info1.attribute_number = Anum_pg_job_job_name;
    scan_key_info1.procedure = F_TEXTEQ;
    ScanKeyInfo scan_key_info2;
    scan_key_info2.attribute_value = PointerGetDatum(u_sess->proc_cxt.MyProcPort->database_name);
    scan_key_info2.attribute_number = Anum_pg_job_dbname;
    scan_key_info2.procedure = F_NAMEEQ;
    
    List *tuples = search_by_sysscan_2(pg_job_rel, &scan_key_info1, &scan_key_info2);
    if (tuples == NIL) {
        return NULL;
    }
    Assert(list_length(tuples) == 1);
    if (list_length(tuples) != 1) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("find %d tuples match job_name %s in system table pg_job.", list_length(tuples),
                        TextDatumGetCString(job_name)),
                    errdetail("N/A"), errcause("job name is not exist"), erraction("Please check job_name")));
    }
    HeapTuple tuple = (HeapTuple)linitial(tuples);
    list_free_ext(tuples);
    return tuple;
}

/*
 * @brief update_pg_job
 *  Update pg_job.
 * @param job_name
 * @param attribute_number
 * @param attribute_value
 */
void update_pg_job(Datum job_name, int attribute_number, Datum attribute_value, bool isnull)
{
    Datum values[Natts_pg_job];
    bool nulls[Natts_pg_job];
    bool replaces[Natts_pg_job];
    errno_t rc =  memset_s(replaces, sizeof(replaces), 0, sizeof(replaces));
    securec_check_c(rc, "\0", "\0");
    replaces[attribute_number - 1] = true;
    if (!isnull) {
        values[attribute_number - 1] = attribute_value;
        nulls[attribute_number - 1] = false;
    } else {
        nulls[attribute_number - 1] = true;
    }

    Relation pg_job_rel = heap_open(PgJobRelationId, RowExclusiveLock);
    HeapTuple oldtuple = search_from_pg_job(pg_job_rel, job_name);
    if (!HeapTupleIsValid(oldtuple)) {
        heap_close(pg_job_rel, NoLock);
        return;
    }

    HeapTuple newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(pg_job_rel), values, nulls, replaces);
    simple_heap_update(pg_job_rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(pg_job_rel, newtuple);
    heap_close(pg_job_rel, NoLock);
    heap_freetuple_ext(newtuple);
    heap_freetuple_ext(oldtuple);
}

/*
 * @brief update_pg_job_multi_columns
 *  Update multple columns from pg_job.
 * @param job_name
 * @param attribute_numbers attributes needed to be updated
 * @param attribute_values  attribute values(corresponds to attribute numbers)
 * @param n                 number of columns
 */
void update_pg_job_multi_columns(const Datum job_name, const int *attribute_numbers, const Datum *attribute_values,
                                 const bool *isnull, int n)
{
    Datum values[Natts_pg_job];
    bool nulls[Natts_pg_job];
    bool replaces[Natts_pg_job];
    error_t rc = memset_s(replaces, sizeof(replaces), 0, sizeof(replaces));
    securec_check_c(rc, "\0", "\0");
    for (int i = 0; i < n; i++) {
        replaces[attribute_numbers[i] - 1] = true;
        if (!isnull[i]) {
            values[attribute_numbers[i] - 1] = attribute_values[i];
            nulls[attribute_numbers[i] - 1] = false;
        } else {
            nulls[attribute_numbers[i] - 1] = true;
        }
    }


    Relation pg_job_rel = heap_open(PgJobRelationId, RowExclusiveLock);
    HeapTuple oldtuple = search_from_pg_job(pg_job_rel, job_name);
    if (!HeapTupleIsValid(oldtuple)) {
        heap_close(pg_job_rel, NoLock);
        return;
    }

    HeapTuple newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(pg_job_rel), values, nulls, replaces);
    simple_heap_update(pg_job_rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(pg_job_rel, newtuple);
    heap_close(pg_job_rel, NoLock);
    heap_freetuple_ext(newtuple);
    heap_freetuple_ext(oldtuple);
}

/*
 * @brief search_related_attribute
 *  Search all attributes that is somewhat related to the object.
 * @param gs_job_attribute_rel
 * @param attribute_name
 * @param attribute_value
 * @return List*            List of related attribute tuples.
 */
List *search_related_attribute(Relation gs_job_attribute_rel, Datum attribute_name, Datum attribute_value)
{
    /* Job itself does not need related attributes */
    if (attribute_name == (Datum)0) {
        return NIL;
    }
    ScanKeyInfo scan_key_info1;
    scan_key_info1.attribute_value = attribute_name;
    scan_key_info1.attribute_number = Anum_gs_job_attribute_attribute_name;
    scan_key_info1.procedure = F_TEXTEQ;
    ScanKeyInfo scan_key_info2;
    scan_key_info2.attribute_value = attribute_value;
    scan_key_info2.attribute_number = Anum_gs_job_attribute_attribute_value;
    scan_key_info2.procedure = F_TEXTEQ;
    List *tuples = search_by_sysscan_2(gs_job_attribute_rel, &scan_key_info1, &scan_key_info2);
    return tuples;
}

/*
 * @brief disable_related_jobs_force
 */
static void disable_related_jobs_force(Relation gs_job_attribute_rel, List *disable_job_names)
{
    ListCell *lc = NULL;
    foreach (lc, disable_job_names) {
        Datum job_name = PointerGetDatum(lfirst(lc));
        update_pg_job(job_name, Anum_pg_job_enable, BoolGetDatum(false));
    }
}

/*
 * @brief reset_job_class
 *
 * @param gs_job_attribute_rel
 * @param disable_job_names
 * @param attribute_name
 */
static void reset_job_class(Relation gs_job_attribute_rel, List *disable_job_names, Datum attribute_name, bool force)
{
    if (!force && disable_job_names) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Job class is refered by at least one job."),
                        errdetail("N/A"), errcause("job class is used"),
                        erraction("Please pass force true or drop job first")));
    }

    ListCell *lc = NULL;
    foreach (lc, disable_job_names) {
        Datum job_name = PointerGetDatum(lfirst(lc));
        update_attribute(job_name, attribute_name, CStringGetTextDatum("DEFAULT_JOB_CLASS"));
    }
}

/*
 * @brief search_related_jobs
 *  search all related jobs.
 * @param object_name
 * @param attribute_name
 * @param force
 * @return List*
 */
static List *search_related_jobs(Relation gs_job_attribute_rel, Datum object_name, Datum attribute_name, bool force)
{
    List *tuples = search_related_attribute(gs_job_attribute_rel, attribute_name, object_name);
    // If force is set to FALSE, a class being dropped must not be referenced by any jobs, otherwise an error occurs.
    if (!force && list_length(tuples) != 0) {
        HeapTuple tuple = (HeapTuple)linitial(tuples);
        bool isNull = false;
        Datum related_name =
            heap_getattr(tuple, Anum_gs_job_attribute_job_name, RelationGetDescr(gs_job_attribute_rel), &isNull);
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OBJECT_IN_USE),
                        errmsg("%s %s refered by job %s", TextDatumGetCString(attribute_name),
                                TextDatumGetCString(object_name), TextDatumGetCString(related_name)),
                        errdetail("N/A"), errcause("attribute is used"),
                        erraction("Please drop object %s", DatumGetPointer(object_name))));
    }
    List *disable_job_names = NIL;
    ListCell *lc = NULL;
    foreach (lc, tuples) {
        HeapTuple tuple = (HeapTuple)lfirst(lc);
        bool isNull = false;
        Datum disable_job_name =
            heap_getattr(tuple, Anum_gs_job_attribute_job_name, RelationGetDescr(gs_job_attribute_rel), &isNull);
        Assert(!isNull);
        disable_job_names = lappend(disable_job_names, DatumGetPointer(PG_DETOAST_DATUM_COPY(disable_job_name)));
    }
    list_free_deep(tuples);
    return disable_job_names;
}

/*
 * @brief drop_inline_program
 *  Drop inline program if exists.
 * @param job_name
 */
void drop_inline_program(const Datum job_name)
{
    Datum attribute_name = CStringGetTextDatum("program_name");

    Relation rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    bool isnull = false;
    Datum program_name = lookup_job_attribute(rel, job_name, attribute_name, &isnull, true);
    heap_close(rel, NoLock);
    if (!PointerIsValid(program_name)) {
        return;
    }

    char *program_name_str = TextDatumGetCString(program_name);
    if (strncmp(INLINE_JOB_PROGRAM_PREFIX, program_name_str, strlen(INLINE_JOB_PROGRAM_PREFIX)) == 0) {
        delete_from_attribute(program_name);
        delete_from_job_proc(program_name);
        delete_from_argument(program_name);
    }
}

/*
 * @brief drop_single_object_name
 *  Drop one object, disable all related objects.
 * @param object_name
 * @param object_type
 * @param force
 * @param simple        when set to true, do not disable relatied objects
 */
static void drop_single_object_name(Datum object_name, const char *object_type, bool force)
{
    check_object_type_matched(object_name, object_type);
    delete_from_attribute(object_name);
    delete_from_job_proc(object_name);
    delete_from_argument(object_name);

    Datum attribute_name;
    if (pg_strcasecmp(object_type, "job_class") == 0) {
        attribute_name = CStringGetTextDatum("job_class");
    } else if (pg_strcasecmp(object_type, "program") == 0) {
        attribute_name = CStringGetTextDatum("program_name");
    } else if (pg_strcasecmp(object_type, "schedule") == 0) {
        attribute_name = CStringGetTextDatum("schedule_name");
    } else {
        Assert(false);
        return;
    }

    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    List *disable_job_names = search_related_jobs(gs_job_attribute_rel, object_name, attribute_name, force);
    disable_related_jobs_force(gs_job_attribute_rel, disable_job_names);
    if (pg_strcasecmp(object_type, "job_class") == 0 && list_length(disable_job_names) > 0) {
        reset_job_class(gs_job_attribute_rel, disable_job_names, attribute_name, force);
    }
    heap_close(gs_job_attribute_rel, NoLock);
    list_free_deep(disable_job_names);
    disable_job_names = NIL;
    pfree(DatumGetPointer(attribute_name));
}

/*
 * @brief drop_single_job_class_internal
 *  Drop a single job class.
 * Note:
 *  Dropping a job class requires the MANAGE SCHEDULER system privilege.
 */
void drop_single_job_class_internal(PG_FUNCTION_ARGS)
{
    check_object_is_visible(PG_GETARG_DATUM(0), false);
    Datum job_class_name = PG_GETARG_DATUM(0);
    char *job_class_str = TextDatumGetCString(job_class_name);
    if (pg_strcasecmp(job_class_str, "DEFAULT_JOB_CLASS") == 0) {
        return;
    }
    bool force = PG_GETARG_BOOL(1);
    drop_single_object_name(job_class_name, "job_class", force);
}

/*
 * @brief drop_single_program_internal
 *  Drop a single program.
 */
void drop_single_program_internal(PG_FUNCTION_ARGS)
{
    check_object_is_visible(PG_GETARG_DATUM(0), false);
    Datum program_name = PG_GETARG_DATUM(0);

    char *program_type_str = get_attribute_value_str(program_name, "program_type", AccessShareLock, true);
    if (program_type_str == NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Cannot find program type of program %s.", TextDatumGetCString(program_name)),
                errdetail("Invalid program format."), errcause("N/A"),
                erraction("Please check the program name.")));
    }

    if (pg_strcasecmp(program_type_str, EXTERNAL_JOB_TYPE) == 0) {
        check_privilege(get_role_name_str(), CREATE_EXTERNAL_JOB_PRIVILEGE);
    }
    pfree_ext(program_type_str);

    bool force = PG_GETARG_BOOL(1);
    drop_single_object_name(program_name, "program", force);
}

/*
 * @brief drop_single_schedule_internal
 *  Drop a single schedule.
 */
void drop_single_schedule_internal(PG_FUNCTION_ARGS)
{
    check_object_is_visible(PG_GETARG_DATUM(0), false);
    Datum schedule_name = PG_GETARG_DATUM(0);
    bool force = PG_GETARG_BOOL(1);
    drop_single_object_name(schedule_name, "schedule", force);
}

/*
 * @brief drop_credential_internal
 *  Drop a single credential.
 */
void drop_credential_internal(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("Fail to drop credential."),
                        errdetail("Insufficient privilege to drop credential."), errcause("N/A"),
                        erraction("Please login in with initial user or contact database administrator.")));
    }
    const Datum credential_name = PG_GETARG_DATUM(0);
    check_object_type_matched(credential_name, "credential");
    bool force = PG_GETARG_BOOL(1);
    if (!force) {
        Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
        Datum credential_attribute_name = CStringGetTextDatum("credential_name");
        List *tuples = search_related_attribute(gs_job_attribute_rel, credential_attribute_name, credential_name);
        pfree(DatumGetPointer(credential_attribute_name));
        credential_attribute_name = Datum(0);
        if (list_length(tuples) > 0) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OBJECT_IN_USE),
                        errmsg("Fail to drop credential."),
                        errdetail("Credential %s is refered by jobs.", TextDatumGetCString(credential_name)),
                        errcause("Credential is in use."),
                        erraction("Please set force flag to true or drop all associated jobs.")));
        }
        heap_close(gs_job_attribute_rel, NoLock);
    }
    delete_from_attribute(credential_name);
}

/*
 * @brief revoke_authorization
 *  revoke authorization.
 * @param username
 * @param privilege
 * @param skip_disable  for drop role only, we do not need to disable anything since all
 *                      objects are dropped by then
 */
static void revoke_authorization(Datum username, Datum privilege, bool skip_disable = false)
{
    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    HeapTuple old_tuple = SearchSysCache2(JOBATTRIBUTENAME, username, privilege);
    if (old_tuple == NULL) {
        heap_close(gs_job_attribute_rel, NoLock);
        return;
    }
    simple_heap_delete(gs_job_attribute_rel, &old_tuple->t_self);
    ReleaseSysCache(old_tuple);
    heap_close(gs_job_attribute_rel, NoLock);

    if (skip_disable) {
        return;
    }

    char *privilege_str = TextDatumGetCString(privilege);
    char *username_str = TextDatumGetCString(username);
    if (pg_strcasecmp(privilege_str, EXECUTE_ANY_PROGRAM_PRIVILEGE) == 0) {
        disable_shared_job_from_owner(username_str);
    } else if (pg_strcasecmp(privilege_str, RUN_EXTERNAL_JOB_PRIVILEGE) == 0) {
        disable_external_job_from_owner(username_str);
    }
    pfree_ext(username_str);
    pfree_ext(privilege_str);
}

/*
 * @brief revoke_user_authorization_internal
 *  Revoke a user's authorization.
 */
void revoke_user_authorization_internal(PG_FUNCTION_ARGS)
{
    const Datum username = get_role_datum(PG_GETARG_DATUM(0));
    const Datum privilege = PG_GETARG_DATUM(1);
    if (!superuser()) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("Fail to revoke authorization."),
                        errdetail("Insufficient privilege to revoke authorization."), errcause("N/A"),
                        erraction("Please login in with initial user or contact database administrator.")));
    }
    check_authorization_valid(privilege);
    revoke_authorization(username, privilege);
}

/*
 * @brief lookup_job_attribute
 *  Look up one attribute.
 * @param rel               gs_job_attribute relation
 * @param object_name       Object name(can be any job related objects)
 * @param attribute         The attribute we looking for
 * @param isnull            null pointer, mark whether the look up result is null
 * @return Datum            return the attribute value
 */
Datum lookup_job_attribute(Relation rel, Datum object_name, Datum attribute, bool *isnull, bool miss_ok)
{
    HeapTuple attribute_tuple = SearchSysCache2(JOBATTRIBUTENAME, object_name, attribute);
    if (!HeapTupleIsValid(attribute_tuple)) {
        if (miss_ok) {
            return Datum(0);
        }
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Can not find attribute \'%s\' of object name \'%s\'.", TextDatumGetCString(attribute),
                               TextDatumGetCString(object_name)),
                        errdetail("N/A"), errcause("attribute is not exist"), erraction("Please check object name")));
    }
    Datum attr = heap_getattr(attribute_tuple, Anum_gs_job_attribute_attribute_value, rel->rd_att, isnull);
    Assert((*isnull && !PointerIsValid(attr)) || (!(*isnull) && PointerIsValid(attr)));
    Datum attr_cpy = Datum(0);
    if (PointerIsValid(attr)) {
        attr_cpy = PointerGetDatum(PG_DETOAST_DATUM_COPY(attr));
    }
    ReleaseSysCache(attribute_tuple);

    return attr_cpy;
}

/*
 * @brief batch_lookup_job_attribute
 *  Look up attribute in a batch with given attribute look up names.
 * @param attributes        Job attributes with attribute name pre-filled
 * @param n                 Number of attributes wanted
 */
void batch_lookup_job_attribute(JobAttribute *attributes, int n)
{
    if (attributes == NULL) {
        return;
    }

    Relation rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    for (int i = 0; i < n; i++) {
        attributes[i].value = lookup_job_attribute(rel, attributes[i].object_name, attributes[i].name,
                                                   &attributes[i].null, false);
    }
    heap_close(rel, NoLock);
}

/*
 * @brief lookup_program_argument
 *  Look up one program argument.
 * @param rel           gs_job_argument relation
 * @param job_name      Job name
 * @param program_name  Program name
 * @param position      Position of the argument
 * @param argument      Job argument context
 */
void lookup_program_argument(Relation rel, Datum job_name, Datum program_name, int position, JobArgument *argument)
{
    HeapTuple argument_tuple = SearchSysCache2(JOBARGUMENTPOSITION, job_name, Int32GetDatum(position));
    if (!HeapTupleIsValid(argument_tuple)) {
        argument_tuple = SearchSysCache2(JOBARGUMENTPOSITION, program_name, Int32GetDatum(position));
        if (!HeapTupleIsValid(argument_tuple)) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Can not find argument info of object \'%s\'.", TextDatumGetCString(job_name)),
                        errdetail("N/A"), errcause("argument information not found"),
                        erraction("Please check object name")));
        }
    }
    bool isnull = true;
    /* Name: type */
    Datum type = heap_getattr(argument_tuple, Anum_gs_job_argument_argument_type, rel->rd_att, &isnull);
    if (!isnull) {
        argument->argument_type = pstrdup((char *)type);
    } else {
        argument->argument_type = NULL;
    }

    /* Datum: argument_value */
    Datum argument_value = heap_getattr(argument_tuple, Anum_gs_job_argument_argument_value, rel->rd_att, &isnull);
    if (!isnull) {
        argument->argument_value = TextDatumGetCString(argument_value);
    } else {
        argument->argument_value = NULL;
    }

    if (argument->argument_value != NULL) {
        ReleaseSysCache(argument_tuple);
        return;
    }

    /* Datum: default_value */
    Datum default_value = heap_getattr(argument_tuple, Anum_gs_job_argument_default_value, rel->rd_att, &isnull);
    if (!isnull) {
        argument->argument_value = TextDatumGetCString(default_value);
    } else {
        argument->argument_value = NULL;
    }

    if (argument->argument_value == NULL) {
        ReleaseSysCache(argument_tuple);
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Cannot get argument value of job %s.", TextDatumGetCString(job_name)),
                        errdetail("N/A"), errcause("argument information not found"),
                        erraction("Please check object name")));
    }
    ReleaseSysCache(argument_tuple);
}

/*
 * @brief batch_lookup_program_argument
 *  Allow us to look up multiple arguments(all the available arguments) in a batch.
 * @param job_name              Job name
 * @param program_name          Program name
 * @param number_of_arguments   Number of arguments
 */
JobArgument *batch_lookup_program_argument(Datum job_name, Datum program_name, int number_of_arguments)
{
    if (number_of_arguments <= 0) {
        return NULL;
    }
    JobArgument *arguments = (JobArgument *)palloc0(number_of_arguments * sizeof(JobArgument));
    Relation rel = heap_open(GsJobArgumentRelationId, AccessShareLock);
    for (int i = 0; i < number_of_arguments; i++) {
        lookup_program_argument(rel, job_name, program_name, i + 1, arguments + i);
        if (arguments[i].argument_value == NULL) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("Can not find argument %d in system table gs_job_argument.", i + 1),
                            errdetail("N/A"), errcause("argument %d is not defined", i + 1),
                            erraction("Please check program arguments")));
        }
    }
    heap_close(rel, NoLock);
    return arguments;
}

/*
 * @brief lookup_credential_username
 *  Look up credential username.
 * @param credential_name
 * @return char*
 */
char *lookup_credential_username(Datum job_name)
{
    bool use_default = false;
    char *job_type_str = get_job_type(job_name, false);
    bool is_shell_job = pg_strcasecmp(job_type_str, EXTERNAL_JOB_TYPE) == 0;
    pfree_ext(job_type_str);
    if (!is_shell_job) {
        return NULL;
    }
    Datum credential_name = get_attribute_value(job_name, "credential_name", AccessShareLock, true, true);
    if (credential_name == Datum(0)) {
        use_default = true;
        credential_name = CStringGetTextDatum(DEFAULT_CREDENTIAL_NAME);
    }
    char *attribute_value_str = get_attribute_value_str(credential_name, "object_type", AccessShareLock, true, true);
    if (attribute_value_str == NULL || pg_strcasecmp(attribute_value_str, "credential") != 0) {
        if (use_default) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("No database wise credential found."),
                            errdetail("Need to create default credential for database with name 'db_credential'"),
                            errcause("No credential found."),
                            erraction("Please create a default or custom credential.")));
            return NULL; /* compiler happy */
        }
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Fail to look up credential."),
                        errdetail("Credential specified does not exist."),
                        errcause("No credential found."),
                        erraction("Please check credential name.")));
    }
    pfree_ext(attribute_value_str);
    char *username = get_attribute_value_str(credential_name, "username", AccessShareLock, false, false);
    return username;
}

/*
 * @brief lookup_job_value
 *  Look up job values.
 * @param job_name
 * @return JobValue*
 */
JobValue *lookup_job_value(Datum job_name)
{
    Relation pg_job_rel = heap_open(PgJobRelationId, AccessShareLock);
    HeapTuple tuple = search_from_pg_job(pg_job_rel, job_name);
    if (!HeapTupleIsValid(tuple)) {
        heap_close(pg_job_rel, AccessShareLock);
        return NULL;
    }
    JobValue *job_value = (JobValue *)palloc0(sizeof(JobValue));
    bool isnull = false;

    job_value->end_date = DatumGetTimestamp(heap_getattr(tuple, Anum_pg_job_end_date, pg_job_rel->rd_att, &isnull));
    if (isnull) {
        job_value->end_date = get_scheduler_max_timestamp();
    }
    job_value->nspname = DatumGetPointer(heap_getattr(tuple, Anum_pg_job_nspname, pg_job_rel->rd_att, &isnull));
    if (isnull) {
        job_value->nspname = NULL;
    } else {
        job_value->nspname = pstrdup(job_value->nspname);
    }
    job_value->interval = TextDatumGetCString(heap_getattr(tuple, Anum_pg_job_interval, pg_job_rel->rd_att, &isnull));
    if (isnull || pg_strcasecmp(job_value->interval, "null") == 0) {
        pfree(job_value->interval);
        job_value->interval = NULL;
    }
    job_value->fail_count = DatumGetInt16(heap_getattr(tuple, Anum_pg_job_failure_count, pg_job_rel->rd_att, &isnull));
    if (isnull) {
        job_value->fail_count = 0;
    }
    heap_close(pg_job_rel, AccessShareLock);
    return job_value;
}

/*
 * @brief make_job_proc_value
 *  Context struct initialization.
 * @param job_action        Job action datum
 * @return JobProcValue*    Out context
 */
JobProcValue *make_job_proc_value(Datum job_action)
{
    JobProcValue *job_proc_value = (JobProcValue *)palloc0(sizeof(JobProcValue));
    job_proc_value->action = job_action;
    job_proc_value->action_str = TextDatumGetCString(job_action);
    return job_proc_value;
}

/*
 * @brief make_job_attribute_value
 *  Context struct initialization.
 * @param attributes            Attribute keys
 * @return JobAttributeValue*   Out context
 */
JobAttributeValue *make_job_attribute_value(JobAttribute *attributes)
{
    enum {PROGRAM_TYPE = 0, PROGRAM_ENABLED, NUM_OF_ARGS, AUTO_DROP, JOB_CLASS};
    JobAttributeValue *job_attribute_value = (JobAttributeValue *)palloc0(sizeof(JobAttributeValue));
    job_attribute_value->job_type = attributes[PROGRAM_TYPE].value;
    attributes[PROGRAM_TYPE].value = Datum(0);
    job_attribute_value->program_enable = TextToBool(attributes[PROGRAM_ENABLED].value);
    job_attribute_value->auto_drop = TextToBool(attributes[AUTO_DROP].value);
    job_attribute_value->number_of_arguments = TextToInt32(attributes[NUM_OF_ARGS].value);
    job_attribute_value->job_class = attributes[JOB_CLASS].value;

    return job_attribute_value;
}

/*
 * @brief make_job_target
 *  Make the job target by put attributes into a well organized structure.
 * @param job_name          Job name
 * @param program_name      Associated program name(can be a inlined program)
 * @param attributes        The job attributes
 * @return JobTarget*       The structure created
 */
static JobTarget *make_job_target(Datum job_name, Datum program_name, JobAttribute *attributes)
{
    Datum job_id;
    Datum job_action;

    /* Lookup id, action attributes */
    lookup_pg_job_proc(job_name, &job_id, &job_action);
    if (job_action == Datum(0)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("job %s's action is undefined.", TextDatumGetCString(job_name)),
                errdetail("N/A"), errcause("job's action is undefined"), erraction("Please check job_name")));
    }

    /* Fill up job targets */
    JobTarget *job_target = (JobTarget *)palloc0(sizeof(JobTarget));
    job_target->job_name = job_name;
    job_target->id = job_id;
    job_target->job_proc_value = make_job_proc_value(job_action);
    job_target->job_attribute_value = make_job_attribute_value(attributes);
    job_target->job_attribute_value->username = lookup_credential_username(job_name);
    job_target->job_attribute_value->program_name = program_name;
    job_target->arguments = batch_lookup_program_argument(job_name, program_name,
                                                          job_target->job_attribute_value->number_of_arguments);
    job_target->job_value = lookup_job_value(job_name);
    return job_target;
}

/*
 * @brief get_job_target
 *  Get the Job Target object.
 * @param job_name      Job name datum
 * @return JobTarget*   Out context
 */
static JobTarget *get_job_target(Datum job_name)
{
    Datum program_name = get_attribute_value(job_name, "program_name", AccessShareLock);

    /* Lookup corresponding attributes */

    const char *attribute_names[] = {"program_type", "enabled", "number_of_arguments", "auto_drop", "job_class"};
    const Datum object_names[] = {program_name, program_name, program_name, job_name, job_name};
    int count = lengthof(attribute_names);
    JobAttribute *lookup_attributes = (JobAttribute *)palloc0(sizeof(JobAttribute) * count);
    for (int i = 0; i < count; i++) {
        lookup_attributes[i].object_name = object_names[i];
        lookup_attributes[i].name = CStringGetTextDatum(attribute_names[i]);
    }
    batch_lookup_job_attribute(lookup_attributes, count);

    /* Job prep */
    JobTarget *job_target = make_job_target(job_name, program_name, lookup_attributes);
    for (int i = 0; i < count; i++) {
        lookup_attributes[i].object_name = object_names[i];
        pfree(DatumGetPointer(lookup_attributes[i].name));
        if (lookup_attributes[i].value != Datum(0)) {
            pfree(DatumGetPointer(lookup_attributes[i].value));
        }
    }
    pfree_ext(lookup_attributes);
    return job_target;
}

/*
 * @brief check_program_type_argument
 *  Perform a simple check on program type argument.
 * @param program_type
 * @param number_of_arguments
 */
void check_program_type_argument(Datum program_type, int number_of_arguments)
{
    if (pg_strcasecmp(TextDatumGetCString(program_type), PLSQL_JOB_TYPE) == 0 && number_of_arguments != 0) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid program type or argument"),
                        errdetail("Program type PLSQL_BLOCK must has no argument"),
                        errcause("Program type is PLSQL_BLOCK, so the number of arguments must be 0"),
                        erraction("Please check program type or argument param")));
    }
    const int max_arguments = 255;
    if (number_of_arguments > max_arguments || number_of_arguments < 0) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid program arguments"),
                        errdetail("Program arguments must less euqal than 255 and greater euqal than zero"),
                        errcause("Program arguments must less euqal than 255"),
                        erraction("Please check program argument param")));
    }
}

/*
 * @brief check_str_valid
 *  Check if a user input string contains danger characters.
 * @param str
 */
static char *replace_all_danger_character(const char *str)
{
    if (strstr(str, "\\\"") != NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                        errmsg("Fail to parse script parameter."),
                        errdetail("Parameter contains escaped double quotes '\\\"'."), errcause("Command is invalid"),
                        erraction("Please enter a valid command")));
    }
    StringInfoData buffer;
    initStringInfo(&buffer);
    appendStringInfoString(&buffer, "\\\"");    /* \" for start of param */
    int len = strlen(str);
    for (int i = 0; i < len; i++) {
        if (str[i] == '"') {
            appendStringInfoString(&buffer, "\\\\\\\""); /* \\\" for escaping the escaped double quote */
        } else if (str[i] == '\\' || str[i] == '|') {
            appendStringInfoString(&buffer, "\\");    /* \ for escaping other dangerous str[i] character */
            appendStringInfoChar(&buffer, str[i]);
        } else {
            appendStringInfoChar(&buffer, str[i]);
        }
    }
    appendStringInfoString(&buffer, "\\\"");    /* \" */
    return buffer.data;
}

/*
 * @brief check_real_path_valid
 *  Check if a user input string is a valid real path.
 * @param file_name
 */
static void check_real_path_valid(const Datum file_name)
{
    char *command = TextDatumGetCString(file_name);
    char path_name_arr[PATH_MAX + 1] = {0};
    char *path_name = realpath(command, path_name_arr);
    if (path_name == NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                        errmsg("Fail to execute external job."),
                        errdetail("Cannot execute external script with given path."), errcause("Command is invalid"),
                        erraction("Please enter a valid command")));
    }
    const char *danger_character_list[] = {"|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"", "{",
                                           "}", "(", ")", "[", "]", "~", "*", "?",  "!", "\n", NULL};
    for (int i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr(path_name, danger_character_list[i]) != NULL) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_NAME),
                            errmsg("invalid token \"%s\"", danger_character_list[i]),
                            errdetail("str contains invalid character"), errcause("str is invalid"),
                            erraction("Please enter a valid str")));
        }
    }
    pfree(command);
}

/*
 * @brief run_backend_job_internal
 *  Runs a job immediately by reset job_status and next_run_time.
 *  Note: use_current_session is not functioning.
 */
void run_backend_job_internal(PG_FUNCTION_ARGS)
{
    check_object_is_visible(PG_GETARG_DATUM(0));
    Datum job_name = PG_GETARG_DATUM(0);
    check_object_type_matched(job_name, "job");
    if (is_job_running(job_name)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Job is still running."),
                        errdetail("Cannot run job when job is already running."),
                        errcause("Job is still running."),
                        erraction("Please try again later.")));
    }
    JobTarget *job_target = get_job_target(job_name); /* for check only */
    pfree_ext(job_target);

    /* Run job */
    Datum current_time = DirectFunctionCall1(timestamptz_timestamp, TimestampTzGetDatum(GetCurrentTimestamp()));
    Datum job_related_attr[] = {current_time, CharGetDatum(PGJOB_SUCC_STATUS)};
    int job_related_num[] = {Anum_pg_job_next_run_date, Anum_pg_job_job_status};
    bool isnull[] = {false, false};
    update_pg_job_multi_columns(job_name, (int *)job_related_num, job_related_attr, isnull, lengthof(job_related_attr));
}

/*
 * @brief check_sql_job_ready
 *  Check if job is ready to run.
 * @param job_target
 */
static void check_sql_job_ready(JobTarget *job_target)
{
    if (!job_target->job_attribute_value->program_enable) {
        StartTransactionCommand();
        update_pg_job(job_target->job_name, Anum_pg_job_enable, BoolGetDatum(false));
        CommitTransactionCommand();
        char *program_name_str = TextDatumGetCString(job_target->job_attribute_value->program_name);
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("program %s of job %s is disabled.",
                                program_name_str, TextDatumGetCString(job_target->job_name)),
                        errdetail("N/A"), errcause("program of job is disabled"),
                        erraction("Please enable program %s of job first", program_name_str)));
    }
    check_program_type_argument(job_target->job_attribute_value->job_type,
                                job_target->job_attribute_value->number_of_arguments);
    check_object_is_visible(job_target->job_attribute_value->program_name);
}

/*
 * @brief check_external_job_ready
 *
 * @param job_target
 */
static void check_external_job_ready(JobTarget *job_target)
{
    check_sql_job_ready(job_target);
    check_real_path_valid(PointerGetDatum(job_target->job_proc_value->action));

    for (int i = 0; i < job_target->job_attribute_value->number_of_arguments; i++) {
        job_target->arguments[i].argument_value = replace_all_danger_character(job_target->arguments[i].argument_value);
    }
    if (job_target->job_attribute_value->username == NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("job %s's credential username is undefined.",
                        TextDatumGetCString(job_target->job_name)),
                        errdetail("N/A"), errcause("credential username is undefined"),
                        erraction("Please check job_name")));
    }
    check_privilege(get_role_name_str(), RUN_EXTERNAL_JOB_PRIVILEGE);
}

/*
 * @brief expire_backend_job
 *  Disable or drop job when job expired.
 */
void expire_backend_job(Datum job_name, bool auto_drop)
{
    start_xact_command();
    if (auto_drop) {
        drop_inline_program(job_name);
        delete_from_attribute(job_name);
        delete_from_job_proc(job_name);
        delete_from_argument(job_name);
        delete_from_job(job_name);
    } else {
        update_pg_job(job_name, Anum_pg_job_enable, BoolGetDatum(false));
    }
    finish_xact_command();
}

/*
 * @brief refresh_backend_job_status
 */
static bool refresh_backend_job_status(JobTarget *job_target, bool after_run)
{
    /* after run */
    if (after_run && job_target->job_value->interval == NULL) {
        expire_backend_job(job_target->job_name, job_target->job_attribute_value->auto_drop);
        return true;
    }

    /* before run */
    TimestampTz cur_timestamp = GetCurrentTimestamp();
    bool expire = DatumGetBool(DirectFunctionCall2(timestamptz_gt_timestamp, cur_timestamp,
                                                   job_target->job_value->end_date));
    if (expire) {
        expire_backend_job(job_target->job_name, job_target->job_attribute_value->auto_drop);
        return true;
    }
    return false;
}

/*
 * @brief run_job_internal
 *  Runs a job immediately, only when external job, others ereport error
 *  Note: use_current_session is not functioning.
 */
Datum run_foreground_job_internal(PG_FUNCTION_ARGS)
{
    check_object_is_visible(PG_GETARG_DATUM(0));
    Datum job_name = PG_GETARG_DATUM(0);
    check_object_type_matched(job_name, "job");
    char *program_type = get_job_type(job_name, false);
    if (pg_strcasecmp(program_type, EXTERNAL_JOB_TYPE) != 0) { // shell
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("when run current session job, program_type %s is not supported.", program_type),
                        errdetail("program_type not supported"), errcause("N/A"),
                        erraction("please check program type")));
    }
    pfree(program_type);
    char *res = run_external_job(job_name);
    Assert(res != NULL);
    Datum res_text = CStringGetTextDatum(res);
    pfree_ext(res);
    return res_text;
}

/*
 * @brief run_sql_job
 *  Run sql job.
 * @param job_name
 * @param buf
 * @return true
 * @return false
 */
static bool run_sql_job(Datum job_name, StringInfoData *buf)
{
    JobTarget *job_target = get_job_target(job_name);
    check_sql_job_ready(job_target);
    if (t_thrd.role == JOB_WORKER) {
        if (refresh_backend_job_status(job_target, false)) {
            return true;
        }
    }
    if (buf->data[0] == '\0' && job_target->job_value->nspname != NULL) {
        appendStringInfo(buf, "set current_schema=%s;", quote_identifier(job_target->job_value->nspname));
    }
    appendStringInfo(buf, "%s", job_target->job_proc_value->action_str);
    execute_simple_query(buf->data);
    if (t_thrd.role == JOB_WORKER) {
        (void)refresh_backend_job_status(job_target, true);
    }
    return true;
}

/*
 * @brief run_procedure_job
 *  Run stored procedure job.
 * @param job_name
 * @param buf
 * @return true
 * @return false
 */
static bool run_procedure_job(Datum job_name, StringInfoData *buf)
{
    JobTarget *job_target = get_job_target(job_name);
    check_sql_job_ready(job_target);
    if (t_thrd.role == JOB_WORKER) {
        if (refresh_backend_job_status(job_target, false)) {
            return true;
        }
    }
    if (buf->data[0] == '\0' && job_target->job_value->nspname != NULL) {
        appendStringInfo(buf, "set current_schema=%s;", quote_identifier(job_target->job_value->nspname));
    }
    appendStringInfoString(buf, "call ");
    appendStringInfoString(buf, job_target->job_proc_value->action_str);
    appendStringInfoString(buf, "(");
    for (int i = 0; i < job_target->job_attribute_value->number_of_arguments; i++) {
        if (i > 0) {
            appendStringInfoString(buf, ", ");
        }
        appendStringInfoString(buf, "'");
        appendStringInfoString(buf, job_target->arguments[i].argument_value);
        appendStringInfoString(buf, "'::");
        appendStringInfoString(buf, job_target->arguments[i].argument_type);
    }
    appendStringInfoString(buf, ");");
    execute_simple_query(buf->data);
    if (t_thrd.role == JOB_WORKER) {
        (void)refresh_backend_job_status(job_target, true);
    }
    return true;
}

/*
 * @brief ssh_run_external_program
 *  Create a SSH connection string for execute a program
 * @param username
 * @param action    Program action
 * @return char*
 */
static char *ssh_run_external_program(const char *username, const char *action)
{
    StringInfoData ssh_buf;
    initStringInfo(&ssh_buf);
    appendStringInfoString(&ssh_buf, "ssh ");
    appendStringInfoString(&ssh_buf, username);
    appendStringInfoString(&ssh_buf, "@localhost \"");
    appendStringInfoString(&ssh_buf, action);
    appendStringInfoString(&ssh_buf, "\"");
    appendStringInfoString(&ssh_buf, " 2>&1");

    FILE *ssh_stream = popen(ssh_buf.data, "r");
    if (ssh_stream == NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("ssh to username failed"), errdetail("cannot ssh to username, popen fail"),
                        errcause("N/A"),
                        erraction("Please append ~/.ssh/id_rsa.pub to username's homepath/.ssh/authorized_keys")));
    }
    StringInfoData res_buf;
    initStringInfo(&res_buf);
    const int bufferLen = 1024;
    char buffer[bufferLen];
    while (fgets(buffer, sizeof(buffer), ssh_stream) != NULL) {
        buffer[bufferLen - 1] = '\0';
        appendStringInfoString(&res_buf, buffer);
    }
    pclose(ssh_stream);
    pfree(ssh_buf.data);
    return res_buf.data;
}

/*
 * @brief run_external_job
 * @param job_name
 */
static char *run_external_job(Datum job_name)
{
    JobTarget *job_target = get_job_target(job_name);
    Assert(job_target->job_proc_value->action != Datum(0));
    check_external_job_ready(job_target);
    if (t_thrd.role == JOB_WORKER) {
        if (refresh_backend_job_status(job_target, false)) {
            return NULL;
        }
    }
    StringInfoData action_buf;
    initStringInfo(&action_buf);
    appendStringInfoString(&action_buf, job_target->job_proc_value->action_str);
    for (int i = 0; i < job_target->job_attribute_value->number_of_arguments; i++) {
        appendStringInfoString(&action_buf, " ");
        appendStringInfoString(&action_buf, job_target->arguments[i].argument_value);
    }
    char *res = ssh_run_external_program(job_target->job_attribute_value->username, action_buf.data);
    if (t_thrd.role == JOB_WORKER) {
        (void)refresh_backend_job_status(job_target, true);
    }
    return res;
}

/*
 * @brief execute_backend_scheduler_job
 *  Execute job main.
 */
bool execute_backend_scheduler_job(Datum job_name, StringInfoData *buf)
{
    if (job_name == Datum(0)) {
        return false;
    }
    char *program_type = get_job_type(job_name, true);
    if (!PointerIsValid(program_type)) {
        return false;
    }
    if (pg_strcasecmp(program_type, PLSQL_JOB_TYPE) == 0) {
        return run_sql_job(job_name, buf);
    }
    if (pg_strcasecmp(program_type, PRECEDURE_JOB_TYPE) == 0) {
        return run_procedure_job(job_name, buf);
    }
    if (pg_strcasecmp(program_type, EXTERNAL_JOB_TYPE) != 0) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("program_type %s cannot be recognized.", program_type),
                        errdetail("program_type crashed"), errcause("N/A"),
                        erraction("please check program type")));
    }
    pfree(program_type);
    (void)run_external_job(job_name);
    return true;
}

/*
 * @brief stop_single_job_force
 *  Send a SIGINT/SIGTERM to job worker.
 * @param job_name
 * @param force
 */
static void stop_single_job_force(Datum job_name, bool force)
{
    Relation pg_job_rel = heap_open(PgJobRelationId, RowExclusiveLock);
    HeapTuple tuple = search_from_pg_job(pg_job_rel, job_name);
    if (!HeapTupleIsValid(tuple)) {
        heap_close(pg_job_rel, NoLock);
        return;
    }
    Form_pg_job pg_job_value = (Form_pg_job)GETSTRUCT(tuple);
    if (pg_job_value->job_status != PGJOB_RUN_STATUS || pg_job_value->current_postgres_pid == -1) {
        heap_close(pg_job_rel, NoLock);
        heap_freetuple_ext(tuple);
        return;
    }
    if (!force) {
        gs_signal_send(pg_job_value->current_postgres_pid, SIGINT);
    } else {
        gs_signal_send(pg_job_value->current_postgres_pid, SIGTERM);
    }

    heap_close(pg_job_rel, NoLock);
    heap_freetuple_ext(tuple);
}

/*
 * @brief stop_job_by_job_class_force
 *  Stop jobs by its job class.
 * @param job_class_name
 * @param force
 */
static void stop_job_by_job_class_force(Datum job_class_name, bool force)
{
    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, RowExclusiveLock);
    Datum job_class_attribute_name = CStringGetTextDatum("job_class");
    List *tuples = search_related_attribute(gs_job_attribute_rel, job_class_attribute_name, job_class_name);
    pfree(DatumGetPointer(job_class_attribute_name));
    job_class_attribute_name = Datum(0);
    if (list_length(tuples) == 0) {
        heap_close(gs_job_attribute_rel, NoLock);
        return;
    }
    ListCell *lc = NULL;
    foreach(lc, tuples) {
        HeapTuple tuple = (HeapTuple)lfirst(lc);
        bool isnull = false;
        Datum job_name = heap_getattr(tuple, Anum_gs_job_attribute_job_name, gs_job_attribute_rel->rd_att, &isnull);
        stop_single_job_force(job_name, force);
    }
    heap_close(gs_job_attribute_rel, NoLock);
    list_free_deep(tuples);
}

/*
 * @brief stop_single_job_internal
 *  Stop the job by sending SIGINT/SIGTERM signals to job worker.
 */
void stop_single_job_internal(PG_FUNCTION_ARGS)
{
    Datum job_name = PG_GETARG_DATUM(0);
    bool force = PG_GETARG_BOOL(1);
    char *object_type = get_attribute_value_str(job_name, "object_type", RowExclusiveLock, true);
    if (object_type == NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Undefined object %s.", TextDatumGetCString(job_name)),
                        errdetail("Job or job class %s does not exist", TextDatumGetCString(job_name)),
                        errcause("N/A"), erraction("Please check the job name.")));
    }

    if (pg_strcasecmp(object_type, "job") == 0) {
        stop_single_job_force(job_name, force);
    } else if (pg_strcasecmp(object_type, "job_class") == 0) {
        stop_job_by_job_class_force(job_name, force);
    } else {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("object_type can only be job or job_class.")));
    }
}

/*
 * @brief drop_single_job_name
 *  Drop a single job with specified job name.
 * @param job_name
 * @param defer
 * @param force
 */
static void drop_single_job_name(Datum job_name, bool defer, bool force)
{
    check_object_type_matched(job_name, "job");
    char *job_type = get_job_type(job_name, true);
    if (job_type == NULL || pg_strcasecmp(job_type, EXTERNAL_JOB_TYPE) == 0) {
        check_privilege(get_role_name_str(), CREATE_EXTERNAL_JOB_PRIVILEGE);
    }
    pfree_ext(job_type);

    if (force && defer) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Cannot defer drop_job when 'force' is on."),
                        errdetail("N/A"), errcause("Conflicting condition."),
                        erraction("Please recheck the parameters.")));
    }
    bool is_running = is_job_running(job_name);
    if (is_running && defer) {
        /* if defer is set, expire the job immediately */
        Datum attribute_name_1 = CStringGetTextDatum("auto_drop");
        Datum attribute_value_1 = BoolToText(true);
        set_job_attribute(job_name, attribute_name_1, attribute_value_1);
        Datum attribute_name_2 = CStringGetTextDatum("end_date");
        Datum attribute_value_2 = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
        attribute_value_2 = DirectFunctionCall1(timestamp_text, attribute_value_2);
        set_job_attribute(job_name, attribute_name_2, attribute_value_2);
        return;
    }
    if (is_running && !force) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Cannot drop job %s when job is running.", TextDatumGetCString(job_name)),
                        errdetail("Job is running."), errcause("N/A"),
                        erraction("Please check the job status.")));
    }
    if (is_running && force) {
        DirectFunctionCall2((PGFunction)stop_single_job_internal, job_name, BoolGetDatum(true));
    }
    drop_inline_program(job_name);
    delete_from_attribute(job_name);
    delete_from_job_proc(job_name);
    delete_from_argument(job_name);
    delete_from_job(job_name);
}

/*
 * @brief drop_scheduler_jobs_from_class
 *  Drop all jobs with specified job class.
 */
void drop_scheduler_jobs_from_class(Datum job_class_name, bool defer, bool force)
{
    Datum attribute_name = CStringGetTextDatum("job_class");
    Relation rel = heap_open(GsJobAttributeRelationId, AccessShareLock);
    List *tuples = search_related_attribute(rel, attribute_name, job_class_name);
    List *drop_job_names = NIL;
    ListCell *lc = NULL;
    foreach(lc, tuples) {
        HeapTuple tuple = (HeapTuple)lfirst(lc);
        bool isnull = false;
        Datum job_name = heap_getattr(tuple, Anum_gs_job_attribute_job_name, rel->rd_att, &isnull);
        Assert(!isnull);
        drop_job_names = lappend(drop_job_names, DatumGetPointer(PG_DETOAST_DATUM_COPY(job_name)));
    }
    heap_close(rel, AccessShareLock);

    /* call disable */
    lc = NULL;
    foreach(lc, drop_job_names) {
        Datum job_name = (Datum)lfirst(lc);
        check_object_is_visible(job_name, false);
        drop_single_job_name(job_name, defer, force);
    }
}

/*
 * @brief drop_job_internal
 *  Drop the job and all relative objects(inline program etc.)
 *  Note: defer and commit_semantic has no purpose yet.
 */
void drop_single_job_internal(PG_FUNCTION_ARGS)
{
    check_object_is_visible(PG_GETARG_DATUM(0), false);
    Datum job_name = PG_GETARG_DATUM(0);
    bool force = PG_GETARG_BOOL(1);
    bool defer = PG_GETARG_BOOL(2);

    char *object_type = get_attribute_value_str(job_name, "object_type", RowExclusiveLock, true);
    if (object_type == NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Undefined object %s.", TextDatumGetCString(job_name)),
                errdetail("Job or job class %s does not exist", TextDatumGetCString(job_name)),
                errcause("N/A"), erraction("Please check the job name.")));
    }

    if (pg_strcasecmp(object_type, "job") == 0) {
        drop_single_job_name(job_name, defer, force);
    } else if (pg_strcasecmp(object_type, "job_class") == 0) {
        drop_scheduler_jobs_from_class(job_name, defer, force);
    } else {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("name %s is %s.", TextDatumGetCString(job_name), object_type),
                errdetail("N/A"), errcause("name has wrong type"),
                erraction("Please check %s name", TextDatumGetCString(job_name))));
    }
    return;
}

/*
 * @brief is_private_job
 *  Is a privatejob or not? job use log_user instead of owner, so we made a seperate function.
 * @param job_name
 * @param user_str
 */
static bool is_private_job(Datum job_name, const char *user_str)
{
    bool isnull = false;
    Relation pg_job_rel = heap_open(PgJobRelationId, AccessShareLock);
    HeapTuple tuple = search_from_pg_job(pg_job_rel, job_name);
    if (!HeapTupleIsValid(tuple)) {
        heap_close(pg_job_rel, AccessShareLock);
        return false;
    }
    Datum creator = heap_getattr(tuple, Anum_pg_job_log_user, pg_job_rel->rd_att, &isnull);
    char *creator_str = DatumGetCString(creator);
    if (pg_strcasecmp(creator_str, user_str) == 0) {
        heap_close(pg_job_rel, AccessShareLock);
        return true;
    }
    heap_close(pg_job_rel, AccessShareLock);
    return false;
}

/*
 * @brief is_private_scheduler_object
 *  Check if an object is fully owned by given user.
 * @param rel           gs_job_attribute relation
 * @param object_name   object name
 * @param user_str      given user name
 * @return true         is fully owned
 * @return false        not fully owned
 */
bool is_private_scheduler_object(Relation rel, Datum object_name, const char *user_str)
{
    bool isnull = true;
    Datum attr = lookup_job_attribute(rel, object_name, CStringGetTextDatum("object_type"), &isnull, false);
    char *attr_str = TextDatumGetCString(attr);
    if (pg_strcasecmp(attr_str, "job") == 0) {
        pfree_ext(attr_str);
        if (!is_private_job(object_name, user_str)) {
            return false;
        }
        Datum program_name = \
            lookup_job_attribute(rel, object_name, CStringGetTextDatum("program_name"), &isnull, false);
        if (is_private_scheduler_object(rel, program_name, user_str)) {
            return true;    /* program is private */
        }
        return false;
    }
    pfree_ext(attr_str);
    Datum owner = lookup_job_attribute(rel, object_name, CStringGetTextDatum("owner"), &isnull, true);
    if (owner == Datum(0)) {
        /* credential does not have owner */
        return true;
    }
    char *owner_str = TextDatumGetCString(owner);
    if (pg_strcasecmp(owner_str, user_str) != 0) {
        pfree_ext(owner_str);
        return false;
    }
    pfree_ext(owner_str);
    return true;
}

/*
 * @brief disable_job_from_owner
 *  Disable all shared objects base on its owner.
 * @param user_str      usually current_user
 */
void disable_shared_job_from_owner(const char *user_str)
{
    Datum attribute_name = CStringGetTextDatum("owner");
    Datum attribute_value = CStringGetTextDatum(user_str);
    Relation rel = heap_open(GsJobAttributeRelationId, AccessShareLock);
    List *tuples = search_related_attribute(rel, attribute_name, attribute_value);
    List *disable_job_names = NIL;
    ListCell *lc = NULL;
    foreach(lc, tuples) {
        HeapTuple tuple = (HeapTuple)lfirst(lc);
        bool isnull = false;
        Datum object_name = heap_getattr(tuple, Anum_gs_job_attribute_job_name, rel->rd_att, &isnull);
        Assert(!isnull);
        if (is_private_scheduler_object(rel, object_name, user_str)) {
            continue;
        }
        /* here should be all jobs, which its program is not private */
        disable_job_names = lappend(disable_job_names, DatumGetPointer(PG_DETOAST_DATUM_COPY(object_name)));
    }
    heap_close(rel, AccessShareLock);

    /* call disable */
    lc = NULL;
    foreach(lc, disable_job_names) {
        Datum program_name = (Datum)lfirst(lc);
        Datum enable_value = BoolToText(false);
        enable_single_force(program_name, enable_value, true);
    }
    list_free_deep(disable_job_names);
}

/*
 * @brief is_internal_scheduler_object
 *  Is object an internel object?
 * @param rel
 * @param object_name
 * @param user_str
 */
bool is_internal_scheduler_object(Relation rel, Datum object_name)
{
    bool isnull = true;
    Datum attr = lookup_job_attribute(rel, object_name, CStringGetTextDatum("object_type"), &isnull, false);
    char *attr_str = TextDatumGetCString(attr);
    if (pg_strcasecmp(attr_str, "job") == 0) {
        pfree_ext(attr_str);
        Datum program_name = \
            lookup_job_attribute(rel, object_name, CStringGetTextDatum("program_name"), &isnull, false);
        if (is_internal_scheduler_object(rel, program_name)) {
            return true;    /* program is internal */
        }
        return false;
    }
    pfree_ext(attr_str);
    Datum program_type = lookup_job_attribute(rel, object_name, CStringGetTextDatum("program_type"), &isnull, true);
    if (program_type == Datum(0)) {
        return true;
    }
    char *program_type_str = TextDatumGetCString(program_type);
    if (pg_strcasecmp(program_type_str, EXTERNAL_JOB_TYPE) == 0) {
        pfree_ext(program_type_str);
        return false;
    }
    pfree_ext(program_type_str);
    return true;
}

/*
 * @brief disable_external_job_from_owner
 *  Disable all external objects base on its owner.
 * @param user_str      usually current_user
 */
void disable_external_job_from_owner(const char *user_str)
{
    Datum attribute_name = CStringGetTextDatum("owner");
    Datum attribute_value = CStringGetTextDatum(user_str);
    Relation rel = heap_open(GsJobAttributeRelationId, AccessShareLock);
    List *tuples = search_related_attribute(rel, attribute_name, attribute_value);
    List *disable_object_names = NIL;
    ListCell *lc = NULL;
    foreach(lc, tuples) {
        HeapTuple tuple = (HeapTuple)lfirst(lc);
        bool isnull = false;
        Datum object_name = heap_getattr(tuple, Anum_gs_job_attribute_job_name, rel->rd_att, &isnull);
        Assert(!isnull);
        if (is_internal_scheduler_object(rel, object_name)) {
            continue;
        }
        Datum attr = lookup_job_attribute(rel, object_name, CStringGetTextDatum("object_type"), &isnull, false);
        char *attr_str = TextDatumGetCString(attr);
        if (pg_strcasecmp(attr_str, "job") != 0) {
            pfree_ext(attr_str);
            continue;   /* we skip non-job for now */
        }
        /* here should be a job with external program */
        disable_object_names = lappend(disable_object_names, DatumGetPointer(PG_DETOAST_DATUM_COPY(object_name)));
    }
    heap_close(rel, AccessShareLock);

    /* call disable */
    lc = NULL;
    foreach(lc, disable_object_names) {
        Datum program_name = (Datum)lfirst(lc);
        Datum enable_value = BoolToText(false);
        enable_single_force(program_name, enable_value, true);
    }
    list_free_deep(disable_object_names);
}

/*
 * @brief disable_related_job_with_program
 *  Disable all related jobs created by users other than 'user_str' with a given program.
 * @param program_name
 * @param type_str
 * @param user_str
 */
static void disable_related_job_with_program(Datum program_name, const char *type_str, const char *user_str = NULL)
{
    if (pg_strcasecmp(type_str, "program") != 0) {
        return;
    }

    ListCell *lc = NULL;
    Datum attribute_name = CStringGetTextDatum("program_name");
    Relation gs_job_attribute_rel = heap_open(GsJobAttributeRelationId, AccessShareLock);
    List *disable_job_names = search_related_jobs(gs_job_attribute_rel, program_name, attribute_name, true);
    heap_close(gs_job_attribute_rel, AccessShareLock);

    foreach (lc, disable_job_names) {
        Datum job_name = PointerGetDatum(lfirst(lc));
        char *owner = get_attribute_value_str(job_name, "owner", AccessShareLock, false, false);
        if (user_str != NULL && pg_strcasecmp(user_str, owner) == 0) {
            pfree_ext(owner);
            continue;   /* skip current user's job */
        }
        pfree_ext(owner);
        update_pg_job(job_name, Anum_pg_job_enable, BoolGetDatum(false));
    }
    list_free_deep(disable_job_names);
    disable_job_names = NIL;
}

/*
 * @brief remove_scheduler_objects_from_owner
 *  Remove all external objects, including privileges from its owner.
 * @param user_str
 */
void remove_scheduler_objects_from_owner(const char *user_str)
{
    Datum attribute_name = CStringGetTextDatum("owner");
    Datum attribute_value = CStringGetTextDatum(user_str);
    Relation rel = heap_open(GsJobAttributeRelationId, AccessShareLock);
    List *tuples = search_related_attribute(rel, attribute_name, attribute_value);
    List *drop_object_names = NIL;
    List *drop_object_types = NIL;
    ListCell *lc = NULL;
    foreach(lc, tuples) {
        HeapTuple tuple = (HeapTuple)lfirst(lc);
        bool isnull = false;
        Datum object_name = heap_getattr(tuple, Anum_gs_job_attribute_job_name, rel->rd_att, &isnull);
        Assert(!isnull);
        Datum type = lookup_job_attribute(rel, object_name, CStringGetTextDatum("object_type"), &isnull, false);
        drop_object_names = lappend(drop_object_names, DatumGetPointer(PG_DETOAST_DATUM_COPY(object_name)));
        drop_object_types = lappend(drop_object_types, DatumGetPointer(type));
    }
    heap_close(rel, AccessShareLock);

    /* call disable */
    lc = NULL;
    ListCell *lc2 = NULL;
    forboth(lc, drop_object_names, lc2, drop_object_types) {
        Datum object_name = (Datum)lfirst(lc);
        Datum object_type = (Datum)lfirst(lc2);
        char *type_str = TextDatumGetCString(object_type);
        check_object_type_matched(object_name, type_str);
        delete_from_attribute(object_name);
        delete_from_job_proc(object_name);
        delete_from_argument(object_name);
        delete_from_job(object_name);

        /* If it is a program, we need to disable all related jobs from other users */
        disable_related_job_with_program(object_name, type_str, user_str);
        pfree(type_str);
    }
    const char *privilege_arr[] = {EXECUTE_ANY_PROGRAM_PRIVILEGE, CREATE_JOB_PRIVILEGE,
                                   CREATE_EXTERNAL_JOB_PRIVILEGE, RUN_EXTERNAL_JOB_PRIVILEGE};
    int len = lengthof(privilege_arr);
    for (int i = 0; i < len; i++) {
        revoke_authorization(CStringGetTextDatum(user_str), CStringGetTextDatum(privilege_arr[i]), true);
    }
    list_free_deep(drop_object_names);
    list_free_deep(drop_object_types);
}