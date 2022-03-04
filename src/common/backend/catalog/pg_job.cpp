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
 * pg_job.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/catalog/catalog/pg_job.cpp
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
#include "catalog/gs_job_attribute.h"
#include "fmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "pgxc/execRemote.h"

/* Mask of jobid and nodeid for generate mix jobid and nodeid. */
#define JOBID_MASK 0xffff
#define NODEID_MASK 0xffffffffffff
#define GENERATE_MIX_ID(node_id, job_id) ((int64)((((node_id)&NODEID_MASK) << 16) | ((uint4)(job_id)&JOBID_MASK)))
#define booltostr(x) ((x) ? "true" : "false")
#define JOB_MAX_FAIL_COUNT 16
#define InvalidJobId 0

extern void syn_command_to_other_node(int4 job_id, Pgjob_Command_Type command, Datum what = 0, Datum next_date = 0,
    Datum job_interval = 0, bool broken = 0);
extern void syn_command_to_other_node_internal(Datum node_name, Datum database, int4 job_id, Pgjob_Command_Type command,
    Datum what, Datum next_date, Datum job_interval, bool broken);
static void elog_job_detail(int4 job_id, char* what, Update_Pgjob_Status status, char* errmsg);
static char* query_with_update_job(int4 job_id, Datum job_status, int64 pid, Datum last_start_date, Datum last_end_date,
    Datum last_suc_date, Datum this_run_date, Datum next_run_date, int2 failure_count, Datum node_name, Datum fail_msg);
static bool is_internal_perf_job(int64 job_id);
static bool is_job_aborted(Datum job_status);
static HeapTuple get_job_tup_from_rel(Relation job_rel, int job_id);
static char* get_job_what(int4 job_id, bool throw_not_found_error = true);

/*
 * @brief is_scheduler_job_id
 *  Check if a job is scheduler job by searching its job name.
 */
bool is_scheduler_job_id(Relation relation, int64 job_id)
{
    bool is_regular_job = true;
    HeapTuple tuple = NULL;
    tuple = get_job_tup_from_rel(relation, job_id);
    if (!HeapTupleIsValid(tuple)) {
        return false;
    }
    (void)heap_getattr(tuple, Anum_pg_job_job_name, relation->rd_att, &is_regular_job);

    return !is_regular_job;
}

/*
 * @Description: Insert a new record to pg_job_proc.
 * @in job_id - Job id.
 * @in what - Job task.
 * @returns - void
 */
static void insert_pg_job_proc(int4 job_id, Datum what)
{
    Datum values[Natts_pg_job_proc];
    bool nulls[Natts_pg_job_proc];
    HeapTuple tuple = NULL;
    Relation rel = NULL;
    errno_t rc = 0;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");

    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");
    nulls[Anum_pg_job_proc_job_name - 1] = true;

    rel = heap_open(PgJobProcRelationId, RowExclusiveLock);

    values[Anum_pg_job_proc_job_id - 1] = ObjectIdGetDatum(job_id);
    values[Anum_pg_job_proc_what - 1] = what;

    tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    (void)simple_heap_insert(rel, tuple);

    CatalogUpdateIndexes(rel, tuple);

    heap_freetuple_ext(tuple);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Encapsulating a function with the job interface will cause the function's schema be 
 * inserted into the pg_job system table. 
 * In order to avoid the situation that the job function in the dbe_task schema can only
 * call functions or procedures under the dbe_task schema, convert the dbe_task to public.
 */
char* get_real_search_schema()
{
    char *cur_schema = get_namespace_name(SchemaNameGetSchemaOid(NULL));
    if (strcmp(cur_schema, "dbe_task") == 0) {
        pfree_ext(cur_schema);
        cur_schema = "public";
    }
    return cur_schema;
}
/*
 * Description: Insert a new record to pg_job.
 *
 * Parameters:
 *	@in rel: pg_job relation.
 *	@in job_id: Job id.
 *	@in next_date: Next execute time.
 *	@in job_interval: Time interval.
 *	@in node_id: If local cn received isubmit from original cn, the node_id identify original cn.
 * Returns: void
 */
static void insert_pg_job(Relation rel, int job_id, Datum next_date, Datum job_interval, int4 node_id,
    Datum database_name, Datum node_name, char* job_nspname = NULL)
{
    /* just check in cluster mode */
    if (!(IsConnFromCoord() && IS_PGXC_DATANODE) && !IS_SINGLE_NODE && !IS_PGXC_COORDINATOR &&
        !(isSingleMode && IS_PGXC_DATANODE)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Submit or Isubmit job only can be operate on coordinator.")));
    }

    HeapTuple tuple = NULL;
    Datum values[Natts_pg_job];
    bool nulls[Natts_pg_job];
    errno_t rc = 0;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");

    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");

    values[Anum_pg_job_job_id - 1] = Int64GetDatum(job_id);
    values[Anum_pg_job_log_user - 1] = DirectFunctionCall1(namein, CStringGetDatum(GetUserNameFromId(GetUserId())));
    values[Anum_pg_job_priv_user - 1] = DirectFunctionCall1(namein, CStringGetDatum(GetUserNameFromId(GetUserId())));

    if (database_name == 0) {
        values[Anum_pg_job_dbname - 1] =
            DirectFunctionCall1(namein, CStringGetDatum(get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true)));
    } else {
        values[Anum_pg_job_dbname - 1] = database_name;
    }

    if (PointerIsValid(job_nspname)) {
        values[Anum_pg_job_nspname - 1] =
            DirectFunctionCall1(namein, CStringGetDatum(job_nspname));
    } else {
        values[Anum_pg_job_nspname - 1] =
            DirectFunctionCall1(namein, CStringGetDatum(get_real_search_schema()));
    }

    if (node_name != 0) {
        /* submit_job_on_nodes */
        values[Anum_pg_job_node_name - 1] = node_name;
    } else {
        /* Set local g_instance.attr.attr_common.PGXCNodeName if call Submit or ISubmit by client. */
        if (!IsConnFromCoord())
            values[Anum_pg_job_node_name - 1] =
                DirectFunctionCall1(namein, CStringGetDatum(g_instance.attr.attr_common.PGXCNodeName));
        else if (IS_PGXC_COORDINATOR)/* Get node_name by node_id if come from original coordinator. */
            values[Anum_pg_job_node_name - 1] =
                DirectFunctionCall1(namein, CStringGetDatum(get_pgxc_node_name_by_node_id(node_id)));
        else {
            values[Anum_pg_job_node_name - 1] =
                DirectFunctionCall1(namein, CStringGetDatum(""));
        }
    }

    values[Anum_pg_job_start_date - 1] = next_date;
    values[Anum_pg_job_job_status - 1] = CharGetDatum(PGJOB_SUCC_STATUS);
    values[Anum_pg_job_current_postgres_pid - 1] = Int64GetDatum(-1);
    /*
     * Create a job for the first time, we should set
     * last_start_date/last_end_date/last_suc_date/this_run_date as null .
     */
    nulls[Anum_pg_job_last_start_date - 1] = true;
    nulls[Anum_pg_job_last_end_date - 1] = true;
    nulls[Anum_pg_job_last_suc_date - 1] = true;
    nulls[Anum_pg_job_this_run_date - 1] = true;
    values[Anum_pg_job_next_run_date - 1] = next_date;
    values[Anum_pg_job_interval - 1] = job_interval;
    values[Anum_pg_job_failure_count - 1] = Int16GetDatum(0);

    /*
     * These entries are exclusive for dbms package extensions.
     */
    nulls[Anum_pg_job_job_name - 1] = true;
    nulls[Anum_pg_job_end_date - 1] = true;
    nulls[Anum_pg_job_enable - 1] = true;
    nulls[Anum_pg_job_failure_msg - 1] = PointerGetDatum(cstring_to_text(""));

    tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

    (void)simple_heap_insert(rel, tuple);

    CatalogUpdateIndexes(rel, tuple);

    heap_freetuple_ext(tuple);
}

/*
 * Description: Get all values of job tuple from pg_job by job_id.
 *
 * Parameters:
 *	@in job_id: Job id.
 *	@in tup: HeapTuple for pg_job.
 *	@in relation: Relation for pg_job.
 *	@in values: Column values for pg_job.
 *	@in visnull: Identify the each value is null or not.
 * Returns: HeapTuple
 */
void get_job_values(int4 job_id, HeapTuple tup, Relation relation, Datum* values, bool* visnull)
{
    errno_t rc = 0;

    rc = memset_s(values, sizeof(Datum) * Natts_pg_job, 0, sizeof(Datum) * Natts_pg_job);
    securec_check(rc, "\0", "\0");

    rc = memset_s(visnull, sizeof(bool) * Natts_pg_job, false, sizeof(bool) * Natts_pg_job);
    securec_check_c(rc, "\0", "\0");

    for (int i = 0; i < Natts_pg_job; i++) {
        values[i] = heap_getattr(tup, i + 1, relation->rd_att, &visnull[i]);
    }
}

/*
 * Description: Get job tuple from pg_job by job_id.
 *
 * Parameters:
 *	@in job_id: Job id.
 * Returns: HeapTuple
 */
static HeapTuple get_job_tup(int job_id)
{
    HeapTuple tup = NULL;
    char* myrolename = NULL;

    tup = SearchSysCache1(PGJOBID, Int64GetDatum(job_id));
    if (!HeapTupleIsValid(tup)) {
        /* in old version pg_job, job can be existed in CN, but not DN */
        if (IS_PGXC_DATANODE && IsConnFromCoord())
            return NULL;

        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Can not find job id %d in system table pg_job.", job_id)));
    }

    Form_pg_job job = ((Form_pg_job)GETSTRUCT(tup));

    myrolename = GetUserNameFromId(GetUserId());
    /* Database Security:  Support separation of privilege. */
    if (!(superuser_arg(GetUserId()) || systemDBA_arg(GetUserId())) &&
        0 != strcmp(NameStr(job->log_user), myrolename)) {
        ReleaseSysCache(tup);
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Permission for current user to get this job. current_user: %s, job_user: %s, job_id: %ld",
                    myrolename,
                    NameStr(job->log_user),
                    job->job_id)));
    }

    return tup;
}

/*
 * Description: Delete job from pg_job_proc.
 *
 * Parameters:
 *	@in job_id: Job id.
 * Returns: void
 */
static void delete_job_proc(int4 job_id)
{
    Relation relation = NULL;
    HeapTuple tup = NULL;

    relation = heap_open(PgJobProcRelationId, RowExclusiveLock);

    tup = SearchSysCache1(PGJOBPROCID, Int32GetDatum(job_id));
    if (!HeapTupleIsValid(tup)) {
        heap_close(relation, RowExclusiveLock);
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Can not find jobid %d in system table pg_job_proc.", job_id)));
    }

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);
    heap_close(relation, RowExclusiveLock);
}

/*
 * Description: Get job task info from pg_job_proc.
 *
 * Parameters:
 *	@in job_id: Job id.
 * Returns: char*
 */
static char* get_job_what(int4 job_id, bool throw_not_found_error)
{
    Relation job_proc_rel = NULL;
    HeapTuple proc_tup = NULL;
    char* what = NULL;
    Datum what_datum;
    bool isnull = false;

    job_proc_rel = heap_open(PgJobProcRelationId, AccessShareLock);
    proc_tup = SearchSysCache1(PGJOBPROCID, Int32GetDatum(job_id));

    if (!HeapTupleIsValid(proc_tup)) {
        heap_close(job_proc_rel, AccessShareLock);
        if (throw_not_found_error) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Can not find jobid %d in system table pg_job_proc.", job_id)));
        } else {
            return NULL;
        }
    }

    what_datum = heap_getattr(proc_tup, Anum_pg_job_proc_what, job_proc_rel->rd_att, &isnull);
    what = text_to_cstring(DatumGetTextP(what_datum));

    ReleaseSysCache(proc_tup);
    heap_close(job_proc_rel, AccessShareLock);

    return what;
}

static void update_pg_job_on_remote(const char* update_query, int64 job_id, MemoryContext current_context)
{
    PG_TRY();
    {
        ExecUtilityStmtOnNodes(update_query, NULL, false, false, EXEC_ON_COORDS, false);
    }
    PG_CATCH();
    {
        /* Save error info */
        (void)MemoryContextSwitchTo(current_context);
        ErrorData* edata = CopyErrorData();
        FlushErrorState();

        ereport(WARNING,
            (errcode(ERRCODE_OPERATE_FAILED),
                errmsg("Synchronize job info to other coordinator failed, job_id: %ld.", job_id),
                errdetail("Synchronize fail reason: %s.", edata->message)));
    }
    PG_END_TRY();
}

void update_pg_job_dbname(Oid jobid, const char* dbname)
{
    Relation job_relation = NULL;
    HeapTuple tup = NULL;
    HeapTuple newtuple = NULL;
    Datum values[Natts_pg_job];
    bool nulls[Natts_pg_job];
    bool replaces[Natts_pg_job];
    errno_t rc = 0;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");

    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");
 
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check_c(rc, "\0", "\0");

    replaces[Anum_pg_job_dbname - 1] = true;
    values[Anum_pg_job_dbname - 1] = CStringGetDatum(dbname);

    job_relation = heap_open(PgJobRelationId, RowExclusiveLock);

    tup = get_job_tup(jobid);

    newtuple = heap_modify_tuple(tup, RelationGetDescr(job_relation), values, nulls, replaces);

    simple_heap_update(job_relation, &newtuple->t_self, newtuple);

    CatalogUpdateIndexes(job_relation, newtuple);
    ReleaseSysCache(tup);
    heap_freetuple_ext(newtuple);

    heap_close(job_relation, RowExclusiveLock);
}

void update_pg_job_username(Oid jobid, const char* username)
{
    Relation job_relation = NULL;
    HeapTuple tup = NULL;
    HeapTuple newtuple = NULL;
    Datum values[Natts_pg_job];
    bool nulls[Natts_pg_job];
    bool replaces[Natts_pg_job];
    errno_t rc = 0;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");

    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");

    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check_c(rc, "\0", "\0");

    replaces[Anum_pg_job_log_user - 1] = true;
    values[Anum_pg_job_log_user - 1] = CStringGetDatum(username);

    replaces[Anum_pg_job_nspname - 1] = true;
    values[Anum_pg_job_nspname - 1] = CStringGetDatum(username);

    job_relation = heap_open(PgJobRelationId, RowExclusiveLock);

    tup = get_job_tup(jobid);

    newtuple = heap_modify_tuple(tup, RelationGetDescr(job_relation), values, nulls, replaces);

    simple_heap_update(job_relation, &newtuple->t_self, newtuple);

    CatalogUpdateIndexes(job_relation, newtuple);
    ReleaseSysCache(tup);
    heap_freetuple_ext(newtuple);

    heap_close(job_relation, RowExclusiveLock);
}

/*
 * Description: Update job info to pg_job according to job execute status.
 *
 * Parameters:
 *	@in job_id: Job id.
 *	@in status: Job status.
 *	@in start_date: Job start time.
 *	@in next_date: Job next time.
 * Returns: void
 */
static void update_pg_job_info(int job_id, Update_Pgjob_Status status, Datum start_date, Datum next_date,
    const char* failure_msg, bool is_scheduler_job)
{
    Relation relation = NULL;
    HeapTuple tup = NULL;
    HeapTuple newtuple = NULL;
    Datum curtime;
    Datum values[Natts_pg_job], old_value[Natts_pg_job];
    bool nulls[Natts_pg_job], visnull[Natts_pg_job];
    bool replaces[Natts_pg_job];
    errno_t rc = 0;
    int2 failure_count;
    char* update_query = NULL;
    /* aborted job can not change to 'r'/'f'/'s' */
    bool is_job_abort = false;
    bool is_perf_job = false;
    MemoryContext current_context = CurrentMemoryContext;
    ResourceOwner save = t_thrd.utils_cxt.CurrentResourceOwner;

    if (Pgjob_Fail == status) {
        /*
         * Abort transaction or subtransaction when execute job fail be
         * ensure the state of transaction is ok.
         */
        AbortOutOfAnyTransaction();
    }

    StartTransactionCommand();
    is_perf_job = is_internal_perf_job(job_id);
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");

    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");

    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check_c(rc, "\0", "\0");

    /* Update pg_job system table. */
    relation = heap_open(PgJobRelationId, RowExclusiveLock);
    tup = get_job_tup(job_id);
    get_job_values(job_id, tup, relation, old_value, visnull);
    is_job_abort = is_job_aborted(old_value[Anum_pg_job_job_status - 1]);

    /* Get current timestamp. */
    curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
    replaces[Anum_pg_job_job_status - 1] = !is_job_abort;
    replaces[Anum_pg_job_current_postgres_pid - 1] = true;
    failure_count = DatumGetInt16(old_value[Anum_pg_job_failure_count - 1]);

    switch (status) {
        case Pgjob_Run: {
            replaces[Anum_pg_job_this_run_date - 1] = true;
            values[Anum_pg_job_job_status - 1] = CharGetDatum(is_job_abort ? PGJOB_ABORT_STATUS : PGJOB_RUN_STATUS);
            values[Anum_pg_job_current_postgres_pid - 1] = Int64GetDatum(t_thrd.proc_cxt.MyProcPid);
            values[Anum_pg_job_this_run_date - 1] = start_date;

            /* Update start_date if this is the first execute job. */
            if (visnull[Anum_pg_job_this_run_date - 1]) {
                replaces[Anum_pg_job_start_date - 1] = true;
                values[Anum_pg_job_start_date - 1] = start_date;
            }

            /* Construct query with update the job info and synchronize to other coordinator. */
            update_query = query_with_update_job(job_id,
                values[Anum_pg_job_job_status - 1],
                values[Anum_pg_job_current_postgres_pid - 1],
                visnull[Anum_pg_job_last_start_date - 1] ? 0 : old_value[Anum_pg_job_last_start_date - 1],
                visnull[Anum_pg_job_last_end_date - 1] ? 0 : old_value[Anum_pg_job_last_end_date - 1],
                visnull[Anum_pg_job_last_suc_date - 1] ? 0 : old_value[Anum_pg_job_last_suc_date - 1],
                values[Anum_pg_job_this_run_date - 1],
                old_value[Anum_pg_job_next_run_date - 1],
                failure_count, 
                values[Anum_pg_job_node_name - 1],
                visnull[Anum_pg_job_failure_msg - 1] ? 0 : old_value[Anum_pg_job_failure_msg - 1]);
            break;
        }
        case Pgjob_Succ: {
            replaces[Anum_pg_job_last_start_date - 1] = true;
            replaces[Anum_pg_job_last_end_date - 1] = true;
            replaces[Anum_pg_job_last_suc_date - 1] = true;
            replaces[Anum_pg_job_failure_count - 1] = true;
            replaces[Anum_pg_job_next_run_date - 1] = true;
            replaces[Anum_pg_job_failure_msg - 1] = true;
            values[Anum_pg_job_job_status - 1] = CharGetDatum(is_job_abort ? PGJOB_ABORT_STATUS : PGJOB_SUCC_STATUS);
            values[Anum_pg_job_current_postgres_pid - 1] = Int64GetDatum(-1);
            values[Anum_pg_job_last_start_date - 1] = start_date;
            values[Anum_pg_job_last_end_date - 1] = curtime;
            values[Anum_pg_job_last_suc_date - 1] = start_date;

            /* Clear failure count if success. */
            failure_count = 0;
            values[Anum_pg_job_failure_count - 1] = Int16GetDatum(failure_count);

            /* Only execute the job once and set status to 'd' if interval is 'null'. */
            if (next_date == 0) {
                values[Anum_pg_job_job_status - 1] = is_scheduler_job ? values[Anum_pg_job_job_status - 1] : \
                                                                        CharGetDatum(PGJOB_ABORT_STATUS);
                values[Anum_pg_job_next_run_date - 1] = DirectFunctionCall2(to_timestamp,
                    DirectFunctionCall1(textin, CStringGetDatum("4000-1-1")),
                    DirectFunctionCall1(textin, CStringGetDatum("yyyy-mm-dd")));
            } else {
                values[Anum_pg_job_next_run_date - 1] = next_date;
            }
            values[Anum_pg_job_failure_msg - 1] = PointerGetDatum(cstring_to_text(""));

            update_query = query_with_update_job(job_id,
                values[Anum_pg_job_job_status - 1],
                values[Anum_pg_job_current_postgres_pid - 1],
                values[Anum_pg_job_last_start_date - 1],
                values[Anum_pg_job_last_end_date - 1],
                values[Anum_pg_job_last_suc_date - 1],
                visnull[Anum_pg_job_this_run_date - 1] ? 0 : old_value[Anum_pg_job_this_run_date - 1],
                values[Anum_pg_job_next_run_date - 1],
                failure_count,
                values[Anum_pg_job_node_name - 1],
                values[Anum_pg_job_failure_msg - 1]);
            break;
        }
        case Pgjob_Fail: {

            replaces[Anum_pg_job_last_start_date - 1] = true;
            replaces[Anum_pg_job_last_end_date - 1] = true;
            replaces[Anum_pg_job_failure_count - 1] = true;
            replaces[Anum_pg_job_next_run_date - 1] = true;
            replaces[Anum_pg_job_failure_msg - 1] = true;
            values[Anum_pg_job_job_status - 1] = CharGetDatum(is_job_abort ? PGJOB_ABORT_STATUS : PGJOB_FAIL_STATUS);
            values[Anum_pg_job_current_postgres_pid - 1] = Int64GetDatum(-1);
            values[Anum_pg_job_last_start_date - 1] = start_date;
            values[Anum_pg_job_last_end_date - 1] = curtime;
            failure_count = failure_count + 1;
            values[Anum_pg_job_failure_count - 1] = Int16GetDatum(failure_count);

            /*
             * Set job_status as 'd' if failure_count more or equal to 16 or
             * execute the job once if interval is 'null'.
             * Then set next_run_date as default date "4000-1-1".
             *
             * Scheduler jobs is an exception, since scheduler has proprietary enable flag which does
             * not rely on PGJOB_ABORT_STATUS.
             */
            if (next_date == 0) {
                values[Anum_pg_job_job_status - 1] = is_scheduler_job ? values[Anum_pg_job_job_status - 1] : \
                                                                        CharGetDatum(PGJOB_ABORT_STATUS);
                values[Anum_pg_job_next_run_date - 1] = DirectFunctionCall2(to_timestamp,
                    DirectFunctionCall1(textin, CStringGetDatum("4000-1-1")),
                    DirectFunctionCall1(textin, CStringGetDatum("yyyy-mm-dd")));
            } else if (failure_count >= JOB_MAX_FAIL_COUNT) {
                ereport(WARNING,
                    (errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("job with id % is abnormal, fail exceeds %d times", job_id, JOB_MAX_FAIL_COUNT)));
                values[Anum_pg_job_job_status - 1] = is_scheduler_job ? values[Anum_pg_job_job_status - 1] : \
                                                                        CharGetDatum(PGJOB_ABORT_STATUS);
                values[Anum_pg_job_next_run_date - 1] = next_date;
            } else {
                values[Anum_pg_job_next_run_date - 1] = next_date;
            }
            values[Anum_pg_job_failure_msg - 1] = PointerGetDatum(cstring_to_text(failure_msg));

            update_query = query_with_update_job(job_id,
                values[Anum_pg_job_job_status - 1],
                values[Anum_pg_job_current_postgres_pid - 1],
                values[Anum_pg_job_last_start_date - 1],
                values[Anum_pg_job_last_end_date - 1],
                visnull[Anum_pg_job_last_suc_date - 1] ? 0 : old_value[Anum_pg_job_last_suc_date - 1],
                visnull[Anum_pg_job_this_run_date - 1] ? 0 : old_value[Anum_pg_job_this_run_date - 1],
                values[Anum_pg_job_next_run_date - 1],
                failure_count,
                values[Anum_pg_job_node_name - 1],
                values[Anum_pg_job_failure_msg - 1]);
            break;
        }
        default: {
            ReleaseSysCache(tup);
            heap_close(relation, is_perf_job ? NoLock : RowExclusiveLock);
            AbortOutOfAnyTransaction();
            ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("Invalid job status, job_id: %d.", job_id)));
        }
    }

    newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);

    simple_heap_update(relation, &newtuple->t_self, newtuple);

    CatalogUpdateIndexes(relation, newtuple);

    ReleaseSysCache(tup);
    heap_freetuple_ext(newtuple);

    if (IS_PGXC_COORDINATOR && update_query != NULL) {
        Assert(!IS_SINGLE_NODE);

        /*
         * If execute job in local success and only synchronize to other coordinator fail,
         * we should consider the worker success. Because worker thread should finish this
         * transaction to update job_status and failure_count local. Otherwise, the job will
         * always restart again and couldn't finish.
         */
        update_pg_job_on_remote(update_query, job_id, current_context);
        pfree_ext(update_query);
    }

    /* avoid update concurrently between perf job management */
    heap_close(relation, is_perf_job ? NoLock : RowExclusiveLock);

    CommitTransactionCommand();

    (void)MemoryContextSwitchTo(current_context);
    t_thrd.utils_cxt.CurrentResourceOwner = save;
}

static void check_job_id(int64 job_id, int64 job_max_number = JOBID_MAX_NUMBER)
{
    if (job_id <= InvalidJobId || job_id > job_max_number) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid job_id: %ld", job_id),
                errdetail("The scope of jobid should between 1 and %ld", job_max_number)));
    }
}

void check_job_status(Datum job_status)
{
    char status = DatumGetChar(job_status);

    if (status != PGJOB_RUN_STATUS &&
        status != PGJOB_ABORT_STATUS &&
        status != PGJOB_FAIL_STATUS &&
        status != PGJOB_SUCC_STATUS) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid job_status: \'%c\' ", status),
                errdetail("The job status should be 'r','f','s','d'.")));
    }
}

/*
 * Description: Update pg_job synchronize from original coordinator.
 *
 * Parameters:
 *	@in PG_FUNCTION_ARGS: Arguments passed to function.
 * Returns: void
 */
void syn_update_pg_job(PG_FUNCTION_ARGS)
{
    int64 job_id = PG_GETARG_INT64(0);
    Datum job_status = PG_GETARG_DATUM(1);
    Datum pid = PG_GETARG_DATUM(2);
    Datum last_start_date = PG_GETARG_DATUM(3);
    Datum last_end_date = PG_GETARG_DATUM(4);
    Datum last_suc_date = PG_GETARG_DATUM(5);
    Datum this_run_date = PG_GETARG_DATUM(6);
    Datum next_run_date = PG_GETARG_DATUM(7);
    Datum failure_count = PG_GETARG_DATUM(8);
    Datum failure_msg = PG_GETARG_DATUM(9);

    Relation relation;
    HeapTuple tup = NULL;
    HeapTuple newtuple = NULL;
    Datum values[Natts_pg_job], old_value[Natts_pg_job];
    bool nulls[Natts_pg_job], visnull[Natts_pg_job];
    bool replaces[Natts_pg_job];
    errno_t rc = 0;

    check_job_id(job_id);
    check_job_status(job_status);
    tup = get_job_tup(job_id);
    if (!HeapTupleIsValid(tup)) {
        return;
    }

    check_job_permission(tup);

    /* Update pg_job system table. */
    relation = heap_open(PgJobRelationId, RowExclusiveLock);
    if (is_scheduler_job_id(relation, job_id)) {
        heap_close(relation, RowExclusiveLock);
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Cannot modify job with jobid:%ld.", job_id),
                        errdetail("Cannot modify scheduler job with current method."), errcause("Forbidden operation."),
                        erraction("Please use scheduler interface to operate this action.")));
    }

    get_job_values(job_id, tup, relation, old_value, visnull);

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");

    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");

    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check_c(rc, "\0", "\0");

    replaces[Anum_pg_job_job_status - 1] = true;
    replaces[Anum_pg_job_current_postgres_pid - 1] = true;
    replaces[Anum_pg_job_next_run_date - 1] = true;
    replaces[Anum_pg_job_failure_count - 1] = true;
    values[Anum_pg_job_job_status - 1] = job_status;
    values[Anum_pg_job_current_postgres_pid - 1] = pid;
    values[Anum_pg_job_next_run_date - 1] = next_run_date;
    values[Anum_pg_job_failure_count - 1] = failure_count;
    values[Anum_pg_job_failure_msg - 1] = failure_msg;

    if (!PG_ARGISNULL(3)) {
        replaces[Anum_pg_job_last_start_date - 1] = true;
        values[Anum_pg_job_last_start_date - 1] = last_start_date;
    }

    if (!PG_ARGISNULL(4)) {
        replaces[Anum_pg_job_last_end_date - 1] = true;
        values[Anum_pg_job_last_end_date - 1] = last_end_date;
    }

    if (!PG_ARGISNULL(5)) {
        replaces[Anum_pg_job_last_suc_date - 1] = true;
        values[Anum_pg_job_last_suc_date - 1] = last_suc_date;
    }

    if (!PG_ARGISNULL(6)) {
        replaces[Anum_pg_job_this_run_date - 1] = true;
        values[Anum_pg_job_this_run_date - 1] = this_run_date;

        /* Synchronize update start_date if this is the first execute job. */
        if (visnull[Anum_pg_job_this_run_date - 1]) {
            replaces[Anum_pg_job_start_date - 1] = true;
            values[Anum_pg_job_start_date - 1] = this_run_date;
        }
    }

    newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);

    simple_heap_update(relation, &newtuple->t_self, newtuple);

    CatalogUpdateIndexes(relation, newtuple);

    ReleaseSysCache(tup);
    heap_freetuple_ext(newtuple);

    heap_close(relation, RowExclusiveLock);
}

/*
 * Description: Get interval and next_date by call spi interface.
 *
 * Parameters:
 *	@in job_id: Job id.
 *	@in ischeck: We don't need get value if only check the interval is valid.
 *	@in job_interval: character of job_interval.
 *	@in start_date: Old start_date value.
 *	@out new_interval: New interval value compute by call spi interface.
 *	@out new_next_date: New next_date value compute by interval.
 *	@in current_context: Current memory context.
 * Returns: void
 */
static void get_interval_nextdate_by_spi(int4 job_id, bool ischeck, const char* job_interval, Datum start_date,
    Datum* new_next_date, MemoryContext current_context)
{
    int ret, interval_len;
    bool isnull = false;
    char* exec_job_interval = NULL;
    Datum current_date;
    TimestampTz result;
    struct timeval tp;

    /* Get current time without usec. */
    gettimeofday(&tp, NULL);
    result = (TimestampTz)tp.tv_sec - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);
#ifdef HAVE_INT64_TIMESTAMP
    result = result * USECS_PER_SEC;
#endif

    current_date = DirectFunctionCall1(timestamptz_timestamp, result);

    /* Construct query as 'select interval' to get next_date. */
    interval_len = strlen(job_interval) + strlen("select ") + 1;
    exec_job_interval = (char*)palloc(interval_len);
    ret = snprintf_s(exec_job_interval, interval_len, interval_len - 1, "select %s", job_interval);
    securec_check_ss_c(ret, "\0", "\0");

    SPI_STACK_LOG("connect", NULL, NULL);
    if (SPI_OK_CONNECT != SPI_connect()) {
        ereport(ERROR,
            (errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("Unable to connect to execute internal query, job_id: %d.", job_id)));
    }

    ret = SPI_execute(exec_job_interval, true, 1);
    if (ret < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_SPI_EXECUTE_FAILURE),
                errmsg("Call SPI_execute execute job interval fail, job_id: %d.", job_id)));
    }

    pfree_ext(exec_job_interval);
    exec_job_interval = NULL;

    /* The result should be timestamp type or interval type. */
    if (!(SPI_tuptable && SPI_tuptable->tupdesc &&
            (SPI_tuptable->tupdesc->attrs[0]->atttypid == TIMESTAMPOID ||
                SPI_tuptable->tupdesc->attrs[0]->atttypid == INTERVALOID))) {
        ereport(ERROR,
            (errcode(ERRCODE_SPI_ERROR), errmsg("Execute job interval for get next_date error, job_id: %d.", job_id)));
    }

    /* We don't need get value if only check the interval is valid. */
    if (!ischeck) {
        /* If INTERVALOID, start_date+INTERVALOID=next_date */
        if (INTERVALOID == SPI_tuptable->tupdesc->attrs[0]->atttypid) {
            Datum new_interval = heap_getattr(SPI_tuptable->vals[0], 1, SPI_tuptable->tupdesc, &isnull);

            MemoryContext oldcontext = MemoryContextSwitchTo(current_context);
            new_interval = datumCopy(
                new_interval, SPI_tuptable->tupdesc->attrs[0]->attbyval, SPI_tuptable->tupdesc->attrs[0]->attlen);
            *new_next_date = DirectFunctionCall2(timestamp_pl_interval, start_date, new_interval);
            (void)MemoryContextSwitchTo(oldcontext);
        } else {
            *new_next_date = heap_getattr(SPI_tuptable->vals[0], 1, SPI_tuptable->tupdesc, &isnull);
        }
    } else {
        /* The interval should greater than current time if it is timestamp. */
        if (TIMESTAMPOID == SPI_tuptable->tupdesc->attrs[0]->atttypid) {
            Datum check_next_date;

            check_next_date = heap_getattr(SPI_tuptable->vals[0], 1, SPI_tuptable->tupdesc, &isnull);
            if (DatumGetBool(DirectFunctionCall2(timestamp_lt, check_next_date, current_date))) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Interval: %s must evaluate to a time in the future for job_id: %d.",
                            quote_literal_cstr(job_interval),
                            job_id)));
            }
        }
    }

    SPI_STACK_LOG("finish", NULL, NULL);
    SPI_finish();
}

/*
 * @brief get_interval_nextdate
 *  Get next job run date base on interval string (can either be a INTERVAL type or a calendaring syntax)
 */
static Datum get_interval_nextdate(int4 job_id, bool ischeck, const char* interval, Datum start_date, Datum base_date,
                                   MemoryContext current_context)
{
    Datum new_next_date;
    if (pg_strncasecmp(interval, "freq=", strlen("freq=")) != 0) {
        get_interval_nextdate_by_spi(job_id, ischeck, interval, start_date, &new_next_date, current_context);
    } else {
        /* scheduler job uses timestamp with time zone all the way, so convert it */
        Datum start_date_tz = DirectFunctionCall1(timestamp_timestamptz, start_date);
        Datum base_date_tz = DirectFunctionCall1(timestamp_timestamptz, base_date);
        Datum next_date_tz = evaluate_repeat_interval(CStringGetTextDatum(interval), base_date_tz, start_date_tz);
        new_next_date = DirectFunctionCall1(timestamptz_timestamp, next_date_tz);
    }
    return new_next_date;
}

/*
 * Description: Begin to execute the job.
 *
 * Parameters:
 *	@in job_id: Job id.
 * Returns: void
 */
void execute_job(int4 job_id)
{
    Datum start_date = 0;
    Datum base_date = 0;    /* The actual start date of the scheduler job */
    Datum old_next_date = 0;
    /*
     * In RELEASE version, some of these variables will be optimized into
     * registers. Be extra careful with updating the variables inside PG_TRY(),
     * since PG_CATCH() will always flush and restore those registers to its
     * original state(right before PG_CATCH()). An unexpected optimization would
     * change how these codes behaves.
     */
    volatile Datum new_next_date = 0;
    Datum job_name = 0;
    char* what = NULL;
    char* job_interval = NULL;
    char* nspname = NULL;
    HeapTuple job_tup = NULL;
    Relation job_rel = NULL;
    Datum values[Natts_pg_job];
    bool nulls[Natts_pg_job];
    bool is_scheduler_job = false;
    ResourceOwner save = t_thrd.utils_cxt.CurrentResourceOwner; /* MessagesContext */
    MemoryContext current_context = CurrentMemoryContext;
    StringInfoData buf;

    initStringInfo(&buf);
    job_tup = get_job_tup(job_id);
    job_rel = heap_open(PgJobRelationId, AccessShareLock);
    get_job_values(job_id, job_tup, job_rel, values, nulls);

    is_scheduler_job = !nulls[Anum_pg_job_job_name - 1]; /* scheduler job flag */
    job_name = is_scheduler_job ? PointerGetDatum(PG_DETOAST_DATUM_COPY(values[Anum_pg_job_job_name - 1])) : Datum(0);
    job_interval = text_to_cstring(DatumGetTextP(values[Anum_pg_job_interval - 1]));
    old_next_date = values[Anum_pg_job_next_run_date - 1];
    new_next_date = old_next_date;  /* Set initial value for the new next_date. */
    what = get_job_what(job_id);
    start_date = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
    base_date = values[Anum_pg_job_start_date - 1];
    if (!nulls[Anum_pg_job_nspname - 1]) {
        nspname = DatumGetCString(DirectFunctionCall1(nameout, NameGetDatum(values[Anum_pg_job_nspname - 1])));
    }

    heap_close(job_rel, AccessShareLock);
    ReleaseSysCache(job_tup);

    PG_TRY();
    {
        /* Update the job state to 'r' before starting to execute it. */
        update_pg_job_info(job_id, Pgjob_Run, start_date, 0, NULL, is_scheduler_job);

        /*
         * Compute next_date if interval is not 'null'.
         * Only execute the job once and set job_status as 'd' if interval is 'null'.
         */
        if (0 != pg_strcasecmp(job_interval, "null")) {
            save = t_thrd.utils_cxt.CurrentResourceOwner;

            StartTransactionCommand();
            /* Get new interval by execute 'select interval' for computing new next_date. */
            new_next_date = get_interval_nextdate(job_id, false, job_interval, start_date, base_date, current_context);
            CommitTransactionCommand();

            t_thrd.utils_cxt.CurrentResourceOwner = save;

            /*
             * There is a condition: we create a new job which the interval is a fixed timestamp.
             * we should execute the job when the first times, and we don't need execute the job
             * later because the new next_date always equal to old next_date.
             */
            if (DatumGetBool(DirectFunctionCall2(timestamp_eq, new_next_date, old_next_date)) &&
                !nulls[Anum_pg_job_last_end_date - 1]) {
                pfree_ext(job_interval);
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("It is invalid job interval for the reason that it is a fixed timestamp, job_id: %d, "
                               "interval: %s.",
                            job_id,
                            job_interval)));
            }
        } else {
            new_next_date = 0;
            elog(WARNING,
                "The interval of current job is \'null\', we can execute this job only one times and set job_status as "
                "\'d\', job_id: %d.",
                job_id);
        }

        save = t_thrd.utils_cxt.CurrentResourceOwner;

        if (nspname != NULL)
            appendStringInfo(&buf, "set current_schema=%s;", quote_identifier(nspname));
        if (!execute_backend_scheduler_job(job_name, &buf)) {
            appendStringInfo(&buf, "%s", what);
            execute_simple_query(buf.data);
        }

        pfree_ext(buf.data);
        pfree_ext(job_interval);
        t_thrd.utils_cxt.CurrentResourceOwner = save;
    }
    PG_CATCH();
    {
        /* Save error info */
        MemoryContext ecxt = MemoryContextSwitchTo(current_context);
        ErrorData* edata = CopyErrorData();
        FlushErrorState();

        t_thrd.utils_cxt.CurrentResourceOwner = save;
        /* Update last_end_date and  job_status='f' and failure_count++ */
        update_pg_job_info(job_id, Pgjob_Fail, start_date, new_next_date, edata->message, is_scheduler_job);
        elog_job_detail(job_id, what, Pgjob_Fail, edata->message);

        (void)MemoryContextSwitchTo(ecxt);

        pfree_ext(job_interval);
        pfree_ext(what);

        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_FAILED),
                errmsg("Execute job failed, job_id: %d.", job_id),
                errdetail("%s", edata->message)));
    }
    PG_END_TRY();

    /* Update last_suc_date and  job_status='s' */
    update_pg_job_info(job_id, Pgjob_Succ, start_date, new_next_date, NULL, is_scheduler_job);
    elog_job_detail(job_id, what, Pgjob_Succ, NULL);
    pfree_ext(what);

    (void)MemoryContextSwitchTo(current_context);
}

/*
 * Description: Check the interval is valid or not.
 *
 * Parameters:
 *	@in job_id: Job id.
 *	@in rel: Relation for pg_job.
 *	@in interval: Interval for the job.
 *	@in next_date: Next_date for the job.
 * Returns: void
 */
void check_interval_valid(int4 job_id, Relation rel, Datum interval)
{
    char* job_interval = text_to_cstring(DatumGetTextP(interval));
    MemoryContext current_context = CurrentMemoryContext;
    ResourceOwner current_resource = t_thrd.utils_cxt.CurrentResourceOwner;

    /* Check if param interval is valid or not by execute 'select interval'. */
    PG_TRY();
    {
        (void)get_interval_nextdate(job_id, true, job_interval, 0, 0, CurrentMemoryContext);
    }
    PG_CATCH();
    {
        t_thrd.utils_cxt.CurrentResourceOwner = current_resource;
        heap_close(rel, RowExclusiveLock);

        /* Save error info */
        MemoryContext ecxt = MemoryContextSwitchTo(current_context);
        ErrorData* edata = CopyErrorData();
        FlushErrorState();

        (void)MemoryContextSwitchTo(ecxt);
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("Invalid parameter interval: \'%s\'.", job_interval),
                errdetail("%s", edata->message)));
    }
    PG_END_TRY();
}

/*
 * Description: Add an job to pg_job.
 *
 * Parameters:
 *	@in what: Task string.
 *	@in next_date: Next execute time.
 *	@in interval_time: Time interval.
 *	@in job_id: job id. default null.
 * Returns: int
 */

Datum job_submit(PG_FUNCTION_ARGS)
{
    Datum what = PG_GETARG_DATUM(1);
    Datum next_date = PG_GETARG_DATUM(2);
    Datum interval_time = PG_GETARG_DATUM(3);
    int64 job_id = 0;
    int4 real_job_id = 0;
    int4 node_id = 0;
    Relation rel = NULL;
    char *c_what = NULL;
    char *c_interval_time = NULL;
    if (PG_ARGISNULL(1)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Parameter what can not be null.")));
    }

    if (PG_ARGISNULL(2)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Parameter next date can not be null.")));
    }

    rel = heap_open(PgJobRelationId, RowExclusiveLock);
    if (PG_ARGISNULL(0)) {
        uint16 id = 0;
        /* Alloc valid job_id. */
        int ret = jobid_alloc(&id);
        if (JOBID_ALLOC_ERROR == ret) {
            heap_close(rel, RowExclusiveLock);
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("All 32768 jobids have alloc, and there is no free jobid")));
        }
        job_id = id;
    } else {
        job_id = PG_GETARG_INT64(0);
        /* If come from original cn, we should get real job_id and node_id. */
        if ((IS_PGXC_DATANODE || IS_PGXC_COORDINATOR) && IsConnFromCoord()) {
            real_job_id = (uint64)job_id & JOBID_MASK;
            node_id = ((uint64)job_id >> 16) & NODEID_MASK;
            job_id = real_job_id;
        }
        check_job_id(job_id);
    }
    c_what = text_to_cstring(DatumGetTextP(what));
    if (PG_ARGISNULL(3)) {
        /* Set interval as "null" if it is null. */
        interval_time = CStringGetTextDatum("null");
    }
    c_interval_time = text_to_cstring(DatumGetTextP(interval_time));
    if (0 != pg_strcasecmp(c_interval_time, "null") && !IsConnFromCoord()) {
        /* Check if param interval is valid or not by execute 'select interval'. */
        check_interval_valid(job_id, rel, interval_time);
    }
    /* Insert a job to pg_job.*/
    insert_pg_job(rel, job_id, next_date, interval_time, node_id, 0, 0);

    /* Insert job id and what into jog_proc system table.*/
    insert_pg_job_proc(job_id, what);

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        syn_command_to_other_node(job_id, Job_ISubmit, what, next_date, interval_time);
    }
#endif

    heap_close(rel, RowExclusiveLock);
    elog(LOG,
        "Success to Submit job, job_id: %ld, what: %s, next_date: %s, job_interval: %s.",
        job_id,
        quote_literal_cstr(c_what),
        quote_literal_cstr(DatumGetCString(DirectFunctionCall1(timestamp_out, DatumGetTimestamp(next_date)))),
        quote_literal_cstr(c_interval_time));
    pfree_ext(c_what);
    pfree_ext(c_interval_time);
    PG_RETURN_INT32(job_id);
}


static void check_parameter_for_nodes(PG_FUNCTION_ARGS, int node_name_idx, int database_idx, int what_idx,
    int next_date_idx)
{
    if (!superuser() && !isMonitoradmin(GetUserId()))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("only system/monitor admin can submit multi-node jobs!")));

    if (PG_ARGISNULL(node_name_idx)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Parameter node_name can not be null.")));
    }

    /* now node name can only be 'ALL_NODE' */
    if (strcmp(DatumGetCString(PG_GETARG_DATUM(node_name_idx)), PGJOB_TYPE_ALL) != 0 &&
        strcmp(DatumGetCString(PG_GETARG_DATUM(node_name_idx)), PGJOB_TYPE_CCN) != 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Parameter node_name can only be 'ALL_NODE' or 'CCN' for multi-node jobs.")));
    }

    if (PG_ARGISNULL(database_idx)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Parameter Database can not be null.")));
    }

    if (strcmp(DatumGetCString(PG_GETARG_DATUM(database_idx)), DEFAULT_DATABASE) != 0 &&
        strcmp(DatumGetCString(PG_GETARG_DATUM(node_name_idx)), PGJOB_TYPE_CCN) != 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Parameter Database can only be 'postgres' except for jobs on CCN.")));
    }

    (void) get_database_oid(DatumGetCString(PG_GETARG_DATUM(database_idx)), false);

    if (PG_ARGISNULL(what_idx)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Parameter what can not be null.")));
    }

    if (PG_ARGISNULL(next_date_idx)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Parameter next date can not be null.")));
    }
}

static void check_job_interval(PG_FUNCTION_ARGS, Relation rel, int job_id, Datum *job_interval, int interval_idx)
{
    if (PG_ARGISNULL(4)) {
        /* Set interval as "null" if it is null. */
        *job_interval = CStringGetTextDatum("null");
    } else {
        if (0 != pg_strcasecmp(text_to_cstring(DatumGetTextP(*job_interval)), "null") && !IsConnFromCoord()) {
            /* Check if param interval is valid or not by execute 'select interval'. */
            check_interval_valid(job_id, rel, *job_interval);
        }
    }
}

/*
 * support sumbit job with id on specific CN or DN/ALL_DN/ALL_CN/ALL,
 * the job record will be inserted to the pg_job table on all Nodes,
 */
Datum isubmit_job_on_nodes(PG_FUNCTION_ARGS)
{
    Datum job_id = PG_GETARG_INT64(0);
    Datum node_name = PG_GETARG_DATUM(1);
    Datum database = PG_GETARG_DATUM(2);
    Datum what = PG_GETARG_DATUM(3);
    Datum next_date = PG_GETARG_DATUM(4);
    Datum job_interval = PG_GETARG_DATUM(5);
    Relation rel = NULL;

    check_parameter_for_nodes(fcinfo, 1, 2, 3, 4);
    check_job_id(job_id);

    rel = heap_open(PgJobRelationId, RowExclusiveLock);
    check_job_interval(fcinfo, rel, job_id, &job_interval, 5);

    insert_pg_job(rel, job_id, next_date, job_interval, 0, database, node_name, "public");
    insert_pg_job_proc(job_id, what);

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        syn_command_to_other_node_internal(node_name, database, job_id, Job_ISubmit_Node, what, next_date, job_interval,
            false);
    }
#endif
    heap_close(rel, RowExclusiveLock);
    PG_RETURN_INT32(job_id);
}


/*
 * support sumbit tsdb job with id on ALL NODE ,
 * the job record will be inserted to the pg_job table on all Nodes,
 */
Datum isubmit_job_on_nodes_internal(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3) || PG_ARGISNULL(4) || PG_ARGISNULL(5)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Parameter can not be null.")));
    }
    Datum job_id = PG_GETARG_INT64(0);
    Datum node_name = PG_GETARG_DATUM(1);
    Datum database = PG_GETARG_DATUM(2);
    Datum what = PG_GETARG_DATUM(3);
    Datum next_date = PG_GETARG_DATUM(4);
    Datum job_interval = PG_GETARG_DATUM(5);
    Relation rel = NULL;

    check_job_id(job_id);

    rel = heap_open(PgJobRelationId, RowExclusiveLock);
    check_job_interval(fcinfo, rel, job_id, &job_interval, 5);

    insert_pg_job(rel, job_id, next_date, job_interval, 0, database, node_name);
    insert_pg_job_proc(job_id, what);

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        syn_command_to_other_node_internal(node_name, database, job_id, Job_ISubmit_Node_Internal, 
            what, next_date, job_interval, false);
    }
#endif
    heap_close(rel, RowExclusiveLock);
    PG_RETURN_INT32(job_id);
}

/*
 * support sumbit job on specific CN or DN/ALL_DN/ALL_CN/ALL
 *
 * Parameters:
 *  @in node: specifc node name/ALL_CN/ALL_DN/ALL
 *  @in database: database name
 *  @in what: task string
 *  @in next_data: next execute time.
 *  @in job_interval: Time interval(seconds)
 */
Datum submit_job_on_nodes(PG_FUNCTION_ARGS)
{
    Datum node_name = PG_GETARG_DATUM(0);
    Datum database = PG_GETARG_DATUM(1);
    Datum what = PG_GETARG_DATUM(2);
    Datum next_date = PG_GETARG_DATUM(3);
    Datum job_interval = PG_GETARG_DATUM(4);
    uint16 job_id = 0;
    int ret = 0;
    Relation rel = NULL;

    check_parameter_for_nodes(fcinfo, 0, 1, 2, 3);
    rel = heap_open(PgJobRelationId, RowExclusiveLock);

    /* Alloc valid job_id. */
    ret = jobid_alloc(&job_id);
    if (ret == JOBID_ALLOC_ERROR) {
        heap_close(rel, RowExclusiveLock);
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("All 32768 jobids have alloc, and there is no free jobid")));
    }
    check_job_interval(fcinfo, rel, job_id, &job_interval, 4);

    insert_pg_job(rel, job_id, next_date, job_interval, 0, database, node_name);
    insert_pg_job_proc(job_id, what);

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        syn_command_to_other_node_internal(node_name, database, job_id, Job_ISubmit_Node, what,
            next_date, job_interval, false);
    }
#endif
    heap_close(rel, RowExclusiveLock);

    PG_RETURN_INT32(job_id);
}

/*
 * Description: Delete job from pg_job and pg_job_proc and tell other cn delete.
 *
 * Parameters:
 *	@in pg_job: pg_job relation.
 *	@in job_id: Job id.
 *  @in local: remove job locally if true
 * Returns: void
 */
static void remove_job_internal(Relation pg_job, int4 job_id, bool ischeck, bool local)
{
    HeapTuple tup = NULL;

    check_job_id(job_id);

    tup = get_job_tup_from_rel(pg_job, job_id);
    if (!HeapTupleIsValid(tup)) {
        return;
    }

    /* If remove job by function job_cancel, we should check the permission. */
    if (ischeck) {
        check_job_permission(tup, !is_internal_perf_job(job_id));
    }

    simple_heap_delete(pg_job, &tup->t_self);

    heap_freetuple_ext(tup);

    /* Delete from pg_job_proc. */
    delete_job_proc(job_id);

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !local) {
        /* Synchronize remove to other coordinater. */
        syn_command_to_other_node(job_id, Job_Remove);
    }
#endif

    elog(LOG, "Success to remove job, job_id: %d.", job_id);
}

/*
 * Description: Remove job related to Oid, Database Oid or User Oid.
 *
 * Parameters:
 *	@in oid: Database Oid or User Oid.
 *	@in oidFlag: Identify Database Oid or User Oid.
 *  @in local: remove job locally if true
 * Returns: void
 */
void remove_job_by_oid(Oid oid, Delete_Pgjob_Oid oidFlag, bool local)
{
    Relation pg_job_tbl = NULL;
    TableScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    bool is_regular_job = true;
    bool object_not_found = true;
    const char *objname = NULL;
    Oid jobid = InvalidOid;

    /* Get object names from oid base on oidFlag */
    switch (oidFlag) {
        case DbOid:
            objname = get_database_name(oid);
            object_not_found = (objname == NULL);
            break;
        case UserOid:
            objname = GetUserNameFromId(oid);
            object_not_found = (objname == NULL);
            break;
        case RelOid:
            jobid = oid;
            object_not_found = false;
            break;
        default:
            object_not_found = true;
    }

    if (object_not_found) {
        return;
    }

    pg_job_tbl = heap_open(PgJobRelationId, ExclusiveLock);
    scan = heap_beginscan(pg_job_tbl, SnapshotNow, 0, NULL);

    while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
        /* non-scheduler jobs does not come with a job name */
        (void)heap_getattr(tuple, Anum_pg_job_job_name, pg_job_tbl->rd_att, &is_regular_job);
        Form_pg_job pg_job = (Form_pg_job)GETSTRUCT(tuple);
        if (((oidFlag == DbOid && 0 == strcmp(NameStr(pg_job->dbname), objname)) ||
            (oidFlag == UserOid && 0 == strcmp(NameStr(pg_job->log_user), objname)) ||
            (oidFlag == RelOid && pg_job->job_id == jobid)) &&
            (oidFlag != UserOid || is_regular_job)) {
            remove_job_internal(pg_job_tbl, pg_job->job_id, false, local);
        }
    }

    heap_endscan(scan);
    heap_close(pg_job_tbl, ExclusiveLock);

    /* always remove scheduler objects when drop role */
    if (oidFlag == UserOid) {
        remove_scheduler_objects_from_owner(get_role_name_str(oid));
    }
}

/*
 * Description: Delete a job by job_id.
 *
 * Parameters:
 *	@in job_id: Job id.
 * Returns: void
 */
Datum job_cancel(PG_FUNCTION_ARGS)
{
    int64 job_id = PG_GETARG_INT64(0);
    Relation relation = NULL;
    MemoryContext current_context = CurrentMemoryContext;
    bool ischeck = (IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IS_SINGLE_NODE;
    bool is_perf_job = is_internal_perf_job(job_id);
    LOCKMODE lock_mode = is_perf_job ? ExclusiveLock : RowExclusiveLock;

    relation = heap_open(PgJobRelationId, lock_mode);

    PG_TRY();
    {
        if (is_scheduler_job_id(relation, job_id)) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_STATUS),
                            errmsg("Cannot remove job with jobid:%ld.", job_id),
                            errdetail("Cannot remove scheduler job with job_cancel"), errcause("Forbidden operation."),
                            erraction("Please use scheduler interface to operate this action.")));
        }
        remove_job_internal(relation, job_id, ischeck, false);
    }
    PG_CATCH();
    {
        ErrorData* edata = NULL;

        /* Save error info */
        MemoryContext ecxt = MemoryContextSwitchTo(current_context);
        edata = CopyErrorData();
        FlushErrorState();

        heap_close(relation, lock_mode);

        (void)MemoryContextSwitchTo(ecxt);
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("Remove jobid:%ld failed.", job_id),
                errdetail("%s", edata->message)));
    }
    PG_END_TRY();

    heap_close(relation, is_perf_job ? NoLock : lock_mode);
    PG_RETURN_VOID();
}

/*
 * Remove from pg_job and pg_job_proc by pg_job.
 */
void RemoveJobById(Oid objectId)
{
    int64 job_id = (int64)(objectId);
    Relation relation;
    MemoryContext current_context = CurrentMemoryContext;
    bool ischeck = (IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IS_SINGLE_NODE;

    relation = heap_open(PgJobRelationId, RowExclusiveLock);
    PG_TRY();
    {
        check_job_id(job_id);
        TableScanDesc scan = NULL;
        char* myrolename = NULL;
        HeapTuple tuple = NULL;
        HeapTuple cp_tuple = NULL;
        scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
        while (HeapTupleIsValid(tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
            Form_pg_job job = (Form_pg_job)GETSTRUCT(tuple);
            if (job->job_id == job_id) {
                myrolename = GetUserNameFromId(GetUserId());
                /* Database Security:  Support separation of privilege.*/
                if (!(superuser_arg(GetUserId()) || systemDBA_arg(GetUserId())) &&
                    0 != strcmp(NameStr(job->log_user), myrolename)) {
                    tableam_scan_end(scan);
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("Permission for current user to get this job. current_user: %s, job_user: %s, job_id: %ld",
                                myrolename, NameStr(job->log_user), job->job_id)));
                }
                cp_tuple = heap_copytuple(tuple);
                break;
            }
        }
        heap_endscan(scan);
        if (!HeapTupleIsValid(cp_tuple)) {
            heap_close(relation, RowExclusiveLock);
            return;
        }
        /* If remove job by function remove_job, we should check the permission. */
        if (ischeck) {
            check_job_permission(cp_tuple, !is_internal_perf_job(job_id));
        }
        simple_heap_delete(relation, &cp_tuple->t_self);
        heap_freetuple_ext(cp_tuple);
        /* Delete from pg_job_proc.*/
        delete_job_proc(job_id);

    #ifdef ENABLE_MULTIPLE_NODES
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
            /* Synchronize remove to other coordinater. */
            syn_command_to_other_node(job_id, Job_Remove);
        }
    #endif
        elog(LOG, "Success to remove job, job_id: %ld.", job_id);
    }
    PG_CATCH();
    {
        ErrorData* edata = NULL;

        /* Save error info */
        MemoryContext ecxt = MemoryContextSwitchTo(current_context);
        edata = CopyErrorData();
        FlushErrorState();

        heap_close(relation, RowExclusiveLock);

        (void)MemoryContextSwitchTo(ecxt);
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("Remove jobid:%ld failed.", job_id),
                errdetail("%s", edata->message)));
    }
    PG_END_TRY();

    heap_close(relation, RowExclusiveLock);
}

/*
 * Description: Update job status to aborted('d') or successfully('s').
 *
 * Parameters:
 *	@in job_id: Job id.
 *	@in next_date: Next execute time.
 * Returns: void
 */

//int64 job_id, bool finish, Datum next_date
void job_finish(PG_FUNCTION_ARGS)
{
    int64 job_id = PG_GETARG_INT64(0);
    bool finished = PG_GETARG_BOOL(1);
    Datum next_time = PG_GETARG_DATUM(2);

    HeapTuple tup = NULL;
    HeapTuple newtuple = NULL;
    Datum values[Natts_pg_job], oldvalues[Natts_pg_job];
    bool nulls[Natts_pg_job], oldvisnulls[Natts_pg_job];
    bool replaces[Natts_pg_job];
    errno_t rc = 0;
    Relation relation = NULL;
    bool is_perf_job = is_internal_perf_job(job_id);
    /* using ExclusiveLock to avoid 'd' to be overwritten by Job Worker */
    LOCKMODE lock_mode = is_perf_job ? ExclusiveLock : RowExclusiveLock;

    check_job_id(job_id);

    /* Update pg_job system table.*/
    relation = heap_open(PgJobRelationId, lock_mode);

    tup = get_job_tup_from_rel(relation, job_id);
    if (!HeapTupleIsValid(tup)) {
        heap_close(relation, lock_mode);
        return;
    }
    check_job_permission(tup, !is_perf_job);
    if (is_scheduler_job_id(relation, job_id)) {
        heap_close(relation, RowExclusiveLock);
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Cannot execute job with jobid:%ld.", job_id),
                        errdetail("Cannot execute scheduler job with current method."),
                        errcause("Forbidden operation."),
                        erraction("Please use scheduler interface.")));
    }

    get_job_values(job_id, tup, relation, oldvalues, oldvisnulls);

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");

    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");

    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check_c(rc, "\0", "\0");

    if (!PG_ARGISNULL(1)) {
        replaces[Anum_pg_job_job_status - 1] = true;
    } else {
        finished = (PGJOB_ABORT_STATUS == DatumGetChar(oldvalues[Anum_pg_job_job_status - 1]) ? true : false);
    }

    if (finished) {
        replaces[Anum_pg_job_next_run_date - 1] = true;
        values[Anum_pg_job_job_status - 1] = CharGetDatum(PGJOB_ABORT_STATUS);
        values[Anum_pg_job_next_run_date - 1] = DirectFunctionCall2(to_timestamp,
            DirectFunctionCall1(textin, CStringGetDatum("4000-1-1")),
            DirectFunctionCall1(textin, CStringGetDatum("yyyy-mm-dd")));
    } else {
        values[Anum_pg_job_job_status - 1] = CharGetDatum(PGJOB_SUCC_STATUS);
        if (next_time != 0) {
            replaces[Anum_pg_job_next_run_date - 1] = true;
            values[Anum_pg_job_next_run_date - 1] = next_time;
        }
    }

    newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);

    simple_heap_update(relation, &newtuple->t_self, newtuple);

    CatalogUpdateIndexes(relation, newtuple);
    heap_freetuple_ext(tup);
    heap_freetuple_ext(newtuple);

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /* Synchronize broken to other nodes. */
        syn_command_to_other_node(job_id, Job_Finish, 0, next_time, 0, finished);
    }
#endif

    heap_close(relation, is_perf_job ? NoLock : lock_mode);

    elog(LOG,
        "Success to Finish job, job_id: %ld, finished: %s, next_time: %s.",
        job_id,
        booltostr(finished),
        quote_literal_cstr(DatumGetCString(DirectFunctionCall1(timestamp_out, DatumGetTimestamp(next_time)))));
}

/*
 * Description: Update pg_job_proc.
 *
 * Parameters:
 *	@in job_id: Job id.
 *	@in task: Job task.
 * Returns: void
 */
static void update_job_proc_what(int4 job_id, Datum task)
{
    Relation job_pro_relation = NULL;
    HeapTuple tup = NULL;
    HeapTuple newtuple = NULL;
    Datum values[Natts_pg_job_proc];
    bool nulls[Natts_pg_job_proc];
    bool replaces[Natts_pg_job_proc];
    errno_t rc = 0;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");

    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");

    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check_c(rc, "\0", "\0");

    replaces[Anum_pg_job_proc_what - 1] = true;
    values[Anum_pg_job_proc_what - 1] = task;

    job_pro_relation = heap_open(PgJobProcRelationId, RowExclusiveLock);

    tup = SearchSysCache1(PGJOBPROCID, Int32GetDatum(job_id));
    if (!HeapTupleIsValid(tup)) {
        heap_close(job_pro_relation, RowExclusiveLock);
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("can not find jobid in the job queue %d", job_id)));
    }

    newtuple = heap_modify_tuple(tup, RelationGetDescr(job_pro_relation), values, nulls, replaces);

    simple_heap_update(job_pro_relation, &newtuple->t_self, newtuple);

    CatalogUpdateIndexes(job_pro_relation, newtuple);
    ReleaseSysCache(tup);
    heap_freetuple_ext(newtuple);

    heap_close(job_pro_relation, RowExclusiveLock);
}

/*
 * Description: Update pg_job what, next_date or interval.
 *
 * Parameters:
 *	@in job_id: Job id.
 *	@in what: Task.
 *	@in next_date: Next time.
 *	@in interval_time: Interval.
 * Returns: void
 */
void job_update(PG_FUNCTION_ARGS)
{
    int64 job_id = PG_GETARG_INT64(0);
    Datum next_time = PG_GETARG_DATUM(1);
    Datum interval_time = PG_GETARG_DATUM(2);
    Datum content = PG_GETARG_DATUM(3);
    if (PG_ARGISNULL(0)) {
        // wrong input
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Parameter id for job_update is unexpected.")));
    }
    Relation relation = NULL;
    HeapTuple tup = NULL;
    /* Don't change any parameter if all are null. */
    if (PG_ARGISNULL(1) && PG_ARGISNULL(2) && PG_ARGISNULL(3)) {
        elog(LOG, "All parameters are NULL for Change, then leave the values.");
        return;
    }

    check_job_id(job_id);

    tup = get_job_tup(job_id);
    if (!HeapTupleIsValid(tup)) {
        return;
    }

    check_job_permission(tup);

    /* Update pg_job system table.*/
    relation = heap_open(PgJobRelationId, RowExclusiveLock);
    if (is_scheduler_job_id(relation, job_id)) {
        heap_close(relation, RowExclusiveLock);
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Cannot update job with jobid:%ld.", job_id),
                        errdetail("Cannot update scheduler job with current method."), errcause("Forbidden operation."),
                        erraction("Please use scheduler interface.")));
    }

    if (!PG_ARGISNULL(3)) {
        /* Update pg_job_proc if content is not null.*/
        update_job_proc_what(job_id, content);
    }

    /* Update next_time or interval_time if either of them is not NULL. */
    if (!PG_ARGISNULL(1) || !PG_ARGISNULL(2)) {
        HeapTuple newtuple = NULL;
        Datum values[Natts_pg_job];
        bool nulls[Natts_pg_job];
        bool replaces[Natts_pg_job];
        errno_t rc = 0;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check_c(rc, "\0", "\0");

        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check_c(rc, "\0", "\0");

        if (!PG_ARGISNULL(1)) {
            replaces[Anum_pg_job_next_run_date - 1] = true;
            values[Anum_pg_job_next_run_date - 1] = next_time;
        }

        if (!PG_ARGISNULL(2)) {
            if (0 != pg_strcasecmp(text_to_cstring(DatumGetTextP(interval_time)), "null")
                && !IsConnFromCoord()) {
                /* Check if param interval is valid or not by execute 'select interval'. */
                check_interval_valid(job_id, relation, interval_time);
            }

            replaces[Anum_pg_job_interval - 1] = true;
            values[Anum_pg_job_interval - 1] = interval_time;
        }

        newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);

        CatalogUpdateIndexes(relation, newtuple);
        ReleaseSysCache(tup);
        heap_freetuple_ext(newtuple);
    } else {
        ReleaseSysCache(tup);
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /* Synchronize broken to other nodes. */
        syn_command_to_other_node(job_id, Job_Update, content, next_time, interval_time);
    }
#endif
    heap_close(relation, RowExclusiveLock);

    elog(LOG,
        "Success to Update job, job_id: %ld, content: %s, next_time: %s, interval_time: %s.",
        job_id,
        0 == content ? "null" : quote_literal_cstr(text_to_cstring(DatumGetTextP(content))),
        0 == next_time
            ? "null"
            : quote_literal_cstr(DatumGetCString(DirectFunctionCall1(timestamp_out, DatumGetTimestamp(next_time)))),
        0 == interval_time ? "null" : quote_literal_cstr(text_to_cstring(DatumGetTextP(interval_time))));
}


/*
 * Description: Check job status is 'r' and update to 'f' when start job scheduler thread
 *
 * Returns: void
 */
void update_run_job_to_fail()
{
    Relation pg_job_tbl = NULL;
    TableScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    HeapTuple newtuple = NULL;
    Datum values[Natts_pg_job], old_value[Natts_pg_job];
    bool nulls[Natts_pg_job], visnull[Natts_pg_job];
    bool replaces[Natts_pg_job];
    errno_t rc;
    MemoryContext current_context = CurrentMemoryContext;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");

    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    rc = memset_s(replaces, sizeof(replaces), 0, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pg_job_job_status - 1] = true;
    replaces[Anum_pg_job_failure_count - 1] = true;

    pg_job_tbl = heap_open(PgJobRelationId, ExclusiveLock);
    scan = heap_beginscan(pg_job_tbl, SnapshotNow, 0, NULL);

    while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
        Form_pg_job pg_job = (Form_pg_job)GETSTRUCT(tuple);
        /* Every coordinator should update all tuples which job_status is 'r'. */
        if (pg_job->job_status == PGJOB_RUN_STATUS &&
            0 == strcmp(pg_job->node_name.data, g_instance.attr.attr_common.PGXCNodeName)) {
            get_job_values(pg_job->job_id, tuple, pg_job_tbl, old_value, visnull);
            values[Anum_pg_job_failure_count - 1] = Int16GetDatum(pg_job->failure_count + 1);

            if (pg_job->failure_count + 1 >= JOB_MAX_FAIL_COUNT) {
                values[Anum_pg_job_job_status - 1] = CharGetDatum(PGJOB_ABORT_STATUS);
                replaces[Anum_pg_job_next_run_date - 1] = true;
                values[Anum_pg_job_next_run_date - 1] = DirectFunctionCall2(to_timestamp,
                    DirectFunctionCall1(textin, CStringGetDatum("4000-1-1")),
                    DirectFunctionCall1(textin, CStringGetDatum("yyyy-mm-dd")));
            } else
                values[Anum_pg_job_job_status - 1] = CharGetDatum(PGJOB_FAIL_STATUS);

            newtuple = heap_modify_tuple(tuple, RelationGetDescr(pg_job_tbl), values, nulls, replaces);
            simple_heap_update(pg_job_tbl, &newtuple->t_self, newtuple);

            CatalogUpdateIndexes(pg_job_tbl, newtuple);

            if (IS_SINGLE_NODE) {
                /* skip to sync update action to other coordinator in single node mode */
                continue;
            }

            /* Send update to other coordinator. */
            char* update_query = query_with_update_job(pg_job->job_id,
                values[Anum_pg_job_job_status - 1],
                pg_job->current_postgres_pid,
                visnull[Anum_pg_job_last_start_date - 1] ? 0 : old_value[Anum_pg_job_last_start_date - 1],
                visnull[Anum_pg_job_last_end_date - 1] ? 0 : old_value[Anum_pg_job_last_end_date - 1],
                visnull[Anum_pg_job_last_suc_date - 1] ? 0 : old_value[Anum_pg_job_last_suc_date - 1],
                visnull[Anum_pg_job_this_run_date - 1] ? 0 : old_value[Anum_pg_job_this_run_date - 1],
                values[Anum_pg_job_next_run_date - 1],
                values[Anum_pg_job_failure_count - 1],
                values[Anum_pg_job_node_name - 1],
                visnull[Anum_pg_job_failure_msg - 1] ? 0 : old_value[Anum_pg_job_failure_msg - 1]);

            /*
             * If update job status in local success and only synchronize to other coordinator fail,
             * we should consider the worker success. Because it will result in job scheduler thread
             * start failed.
             */
            if (update_query != NULL) {
                update_pg_job_on_remote(update_query, pg_job->job_id, current_context);
                pfree_ext(update_query);
            }
        }
    }

    heap_endscan(scan);
    heap_close(pg_job_tbl, ExclusiveLock);
}

/*
 * Used in tsdb. Generate a random job id. pg_job is checked to ensure the generated id is not conflicted.
 * CAUTION: the function only tries JOBID_MAX_NUMBER times. So if there have been a lot of jobs in pg_job,
 * the function is likely to fail, although there is still valid id to use.
 */
static int get_random_job_id(int64 job_max_number = JOBID_MAX_NUMBER) 
{
    if (job_max_number < InvalidJobId) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Cannot generate job id."), errdetail("N/A"), errcause("Invalid job id range set."),
                        erraction("Please recheck job status.")));
    }
    int job_id = gs_random() % job_max_number;
    HeapTuple tup = NULL;
    uint32 loop_count = 0;
    while (loop_count < job_max_number) {
        if (likely(job_id > 0)) {
            tup = SearchSysCache1(PGJOBID, Int64GetDatum(job_id));
            /* Find a invalid jobid. */
            if (!HeapTupleIsValid(tup)) {
                break;
            }
            loop_count++;
            ReleaseSysCache(tup);
        }
        job_id = gs_random() % job_max_number;
    }

    if (loop_count == job_max_number) {
        ereport(LOG,
            (errmodule(MOD_TIMESERIES), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Cannot find a valid job_id randomly, try to search one by one.")));
        return 0;
    }
    return job_id;
}

/*
 * Description: Alloc a valid jobid.
 *
 * Parameters:
 *	@in pusJobId: Return job id.
 * Returns: int
 */
int jobid_alloc(uint16* pusJobId, int64 job_max_number)
{
    int job_id = get_random_job_id(job_max_number);
    if (job_id != 0) {
        *pusJobId = job_id;
        return JOBID_ALLOC_OK;
    }
    HeapTuple tup = NULL;

    for (int job_id = 1; job_id <= job_max_number; job_id++) {
        tup = SearchSysCache1(PGJOBID, Int64GetDatum(job_id));
        /* Find a invalid jobid. */
        if (!HeapTupleIsValid(tup)) {
            *pusJobId = job_id;
            return JOBID_ALLOC_OK;
        }

        ReleaseSysCache(tup);
    }

    return JOBID_ALLOC_ERROR;
}

/*
 * Description: Check whether current user have authority to operate the job.
 *
 * Parameters:
 *	@in tuple: pg_job tuple.
 * Returns: void
 */
void check_job_permission(HeapTuple tuple, bool check_running)
{
    char *mydbname = NULL, *myrolename = NULL;
    Form_pg_job job = NULL;

    job = (Form_pg_job)GETSTRUCT(tuple);

    myrolename = GetUserNameFromId(GetUserId());
    if (!(superuser_arg(GetUserId()) || systemDBA_arg(GetUserId())) &&
        0 != strcmp(NameStr(job->log_user), myrolename)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("Permission for current user to operate the job. current_user: %s, job_user: %s, job_id: %ld",
                    myrolename,
                    NameStr(job->log_user),
                    job->job_id)));
    }

    Oid dboid = get_database_oid(NameStr(job->dbname), true);
    if (OidIsValid(dboid)) {
        mydbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        if (mydbname && 0 != strcmp(NameStr(job->dbname), mydbname)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                    errmsg(
                        "Permission for current database to operate the job. current_dboid: %s, job_dboid: %s, job_id: %ld",
                        mydbname,
                        NameStr(job->dbname),
                        job->job_id)));
        }
    } else {
        /* skip permission check since the database of job does not exist */
        ereport(LOG, (errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database \"%s\" of job %ld does not exist", NameStr(job->dbname), job->job_id)));
    }

    if (check_running && job->job_status == PGJOB_RUN_STATUS) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_STATUS),
                errmsg("Can not operate this job due to running status, job_id: %ld. ", job->job_id)));
    }
}

/*
 * Description: Output detail info of job execute to log.
 *
 * Parameters:
 *	@in job_id: Job id.
 *	@in what: Job task info.
 *	@in status: Job status.
 *	@in errmsg: The error message if execute job failed.
 * Returns: void
 */
static void elog_job_detail(int4 job_id, char* what, Update_Pgjob_Status status, char* errmsg)
{
    HeapTuple tup = NULL;
    StringInfoData buf;
    Relation relation = NULL;
    Datum values[Natts_pg_job];
    bool nulls[Natts_pg_job];

    StartTransactionCommand();
    tup = get_job_tup(job_id);
    relation = heap_open(PgJobRelationId, AccessShareLock);

    get_job_values(job_id, tup, relation, values, nulls);

    initStringInfo(&buf);
    appendStringInfoString(&buf, "Execute Job Detail: \n");
    appendStringInfo(&buf, "job_id: %d \n", job_id);
    appendStringInfo(&buf, "what: %s \n", what);
    appendStringInfo(&buf,
        "start_date: %s \n",
        DatumGetCString(
            DirectFunctionCall1(timestamp_out, DatumGetTimestamp(values[Anum_pg_job_last_start_date - 1]))));

    if (Pgjob_Succ == status) {
        appendStringInfoString(&buf, "job_status: success \n");
    } else {
        appendStringInfoString(&buf, "job_status: failed \n");
        appendStringInfo(&buf, "detail error msg: %s \n", errmsg);
    }

    appendStringInfo(&buf,
        "end_date: %s \n",
        DatumGetCString(DirectFunctionCall1(timestamp_out, DatumGetTimestamp(values[Anum_pg_job_last_end_date - 1]))));
    appendStringInfo(&buf,
        "next_run_date: %s \n",
        DatumGetCString(DirectFunctionCall1(timestamp_out, DatumGetTimestamp(values[Anum_pg_job_next_run_date - 1]))));

    elog(LOG, "%s", buf.data);

    ReleaseSysCache(tup);
    pfree_ext(buf.data);
    heap_close(relation, AccessShareLock);

    CommitTransactionCommand();
}

/*
 * Description: Construct query with update the job info.
 *
 * Parameters:
 *	@in job_id: Job id.
 *	@in job_status: Job status.
 *	@in pid: Job belong thread id.
 *	@in last_start_date: Job last start time.
 *	@in last_end_date: Job last end time.
 *	@in last_suc_date: Job last success time.
 *	@in this_run_date: Job current run time.
 *	@in next_run_date: Job next run time.
 *	@in failure_count: Job failure count.
 * Returns: char*
 */
static char* query_with_update_job(int4 job_id, Datum job_status, int64 pid, Datum last_start_date, Datum last_end_date,
    Datum last_suc_date, Datum this_run_date, Datum next_run_date, int2 failure_count, Datum node_name, Datum fail_msg)
{
    StringInfoData queryString;

    if (!IS_PGXC_COORDINATOR ||
        node_name == 0 ||
        strcmp(DatumGetName(node_name)->data, PGJOB_TYPE_ALL) == 0 ||
        strcmp(DatumGetName(node_name)->data, PGJOB_TYPE_ALL_CN) == 0 ||
        strcmp(DatumGetName(node_name)->data, PGJOB_TYPE_ALL_DN) == 0) {
        /*
         * ALL_NODE/ALL_CN/ALL_DN won't sync status to other,
         * one CN only sync job status to other CNs(specfic job),
         * DN won't sync job status to others.
         */
        return NULL;
    }

    initStringInfo(&queryString);
    if (t_thrd.proc->workingVersionNum >= 92473) {
        appendStringInfo(&queryString,
            "select * from update_pgjob(%d, \'%c\', %ld, %s, %s, %s, %s, %s, %d , %s);",
            job_id,
            DatumGetChar(job_status),
            pid,
            last_start_date == 0 ? "null"
                                 : quote_literal_cstr(DatumGetCString(
                                       DirectFunctionCall1(timestamp_out, DatumGetTimestamp(last_start_date)))),
            last_end_date == 0
                ? "null"
                : quote_literal_cstr(DatumGetCString(DirectFunctionCall1(timestamp_out,
                                                                          DatumGetTimestamp(last_end_date)))),
            last_suc_date == 0
                ? "null"
                : quote_literal_cstr(DatumGetCString(DirectFunctionCall1(timestamp_out,
                                                                          DatumGetTimestamp(last_suc_date)))),
            this_run_date == 0
                ? "null"
                : quote_literal_cstr(DatumGetCString(DirectFunctionCall1(timestamp_out,
                                                                          DatumGetTimestamp(this_run_date)))),
            quote_literal_cstr(DatumGetCString(DirectFunctionCall1(timestamp_out, DatumGetTimestamp(next_run_date)))),
            failure_count,
            fail_msg == 0
                ? "null"
                : quote_literal_cstr(DatumGetCString(fail_msg)));
    } else {
        appendStringInfo(&queryString,
            "select * from update_pgjob(%d, \'%c\', %ld, %s, %s, %s, %s, %s, %d);",
            job_id,
            DatumGetChar(job_status),
            pid,
            last_start_date == 0 ? "null"
                                 : quote_literal_cstr(DatumGetCString(
                                       DirectFunctionCall1(timestamp_out, DatumGetTimestamp(last_start_date)))),
            last_end_date == 0
                ? "null"
                : quote_literal_cstr(DatumGetCString(DirectFunctionCall1(timestamp_out,
                                                                          DatumGetTimestamp(last_end_date)))),
            last_suc_date == 0
                ? "null"
                : quote_literal_cstr(DatumGetCString(DirectFunctionCall1(timestamp_out,
                                                                          DatumGetTimestamp(last_suc_date)))),
            this_run_date == 0
                ? "null"
                : quote_literal_cstr(DatumGetCString(DirectFunctionCall1(timestamp_out,
                                                                          DatumGetTimestamp(this_run_date)))),
            quote_literal_cstr(DatumGetCString(DirectFunctionCall1(timestamp_out, DatumGetTimestamp(next_run_date)))),
            failure_count);

    }


    elog(LOG, "query with update job: %s", queryString.data);

    return queryString.data;
}

/*
 * check the job is internal perf job or not.
 */
static bool is_internal_perf_job(int64 job_id)
{
    bool result = false;
    const char *what = get_job_what(job_id, false);
    if (what != NULL) {
        result = strcasestr(what, " capture_view_to_json(") != NULL;
        pfree_ext(what);
    }

    return result;
}

static bool is_job_aborted(Datum job_status)
{
    return (DatumGetChar(job_status) == PGJOB_ABORT_STATUS);
}

static HeapTuple get_job_tup_from_rel(Relation job_rel, int job_id)
{
    TableScanDesc scan = NULL;
    char* myrolename = NULL;
    HeapTuple tuple = NULL;
    HeapTuple cp_tuple = NULL;

    scan = tableam_scan_begin(job_rel, SnapshotNow, 0, NULL);
    while (HeapTupleIsValid(tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        Form_pg_job job = (Form_pg_job)GETSTRUCT(tuple);

        if (job->job_id == job_id) {
            myrolename = GetUserNameFromId(GetUserId());
            /* Database Security:  Support separation of privilege.*/
            if (!(superuser_arg(GetUserId()) || systemDBA_arg(GetUserId())) &&
                0 != strcmp(NameStr(job->log_user), myrolename)) {
                heap_endscan(scan);
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Permission for current user to get this job. current_user: %s, job_user: %s, job_id: %ld",
                            myrolename,
                            NameStr(job->log_user),
                            job->job_id)));
            }
            cp_tuple = heap_copytuple(tuple);
            break;
        }
    }

    tableam_scan_end(scan);
    if (cp_tuple != NULL) {
        return cp_tuple;
    }

    /* in old version pg_job, job can be existed in CN, but not DN */
    if (IS_PGXC_DATANODE && IsConnFromCoord())
        return NULL;

    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Can not find job id %d in system table pg_job.", job_id)));
    return NULL;
}
