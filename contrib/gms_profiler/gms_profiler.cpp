/*
 * gms_profiler.cpp
 *
 * IDENTIFICATION
 *    contrib/gms_profiler/gms_profiler.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/plpgsql.h"
#include "access/hash.h"
#include "utils/numeric.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include "catalog/pg_authid.h"
#include "funcapi.h"
#include "catalog/pg_proc.h"
#include "commands/extension.h"
#include "utils/lsyscache.h"
#include "gms_profiler.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(start_profiler);
PG_FUNCTION_INFO_V1(start_profiler_1);
PG_FUNCTION_INFO_V1(start_profiler_ext);
PG_FUNCTION_INFO_V1(start_profiler_ext_1);
PG_FUNCTION_INFO_V1(stop_profiler);
PG_FUNCTION_INFO_V1(flush_data);
PG_FUNCTION_INFO_V1(pause_profiler);
PG_FUNCTION_INFO_V1(resume_profiler);

static uint32 profiler_index;

static void plpgsql_cb_func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void plpgsql_cb_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void plpgsql_cb_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);
static void plpgsql_cb_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);

static PLpgSQL_plugin plugin_funcs = {
	NULL,
	plpgsql_cb_func_beg,
	plpgsql_cb_func_end,
	plpgsql_cb_stmt_beg,
	plpgsql_cb_stmt_end,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
};

void set_extension_index(uint32 index)
{
    profiler_index = index;
}

void init_session_vars(void)
{
    RepallocSessionVarsArrayIfNecessary();

    ProfilerContext* psc =
        (ProfilerContext*)MemoryContextAllocZero(u_sess->self_mem_cxt, sizeof(ProfilerContext));
    u_sess->attr.attr_common.extension_session_vars_array[profiler_index] = psc;

    psc->mem_cxt = u_sess->self_mem_cxt;
    psc->state = PROFILER_INACTIVE;

    u_sess->plsql_cxt.plugin_ptr = (PLpgSQL_plugin **) find_rendezvous_variable("PLpgSQL_plugin");
    *u_sess->plsql_cxt.plugin_ptr = &plugin_funcs;
}

ProfilerContext* get_session_context()
{
    if (u_sess->attr.attr_common.extension_session_vars_array[profiler_index] == NULL) {
        init_session_vars();
    }
    return (ProfilerContext*)u_sess->attr.attr_common.extension_session_vars_array[profiler_index];
}

static bool check_profiler_table_exists(void)
{
    Oid profiler_table_oid = InvalidOid;

    Oid namespaceId = LookupExplicitNamespace("gms_profiler", true);
    if (namespaceId == InvalidOid)
        return false;

    profiler_table_oid = get_relname_relid("plsql_profiler_runnumber", namespaceId);
    if (profiler_table_oid == InvalidOid)
        return false;

    profiler_table_oid = get_relname_relid("plsql_profiler_runs", namespaceId);
    if (profiler_table_oid == InvalidOid)
        return false;

    profiler_table_oid = get_relname_relid("plsql_profiler_units", namespaceId);
    if (profiler_table_oid == InvalidOid)
        return false;

    profiler_table_oid = get_relname_relid("plsql_profiler_data", namespaceId);
    if (profiler_table_oid == InvalidOid)
        return false;

    return true;
}

static uint32 get_runid_from_profiler_sequence(bool curr_val)
{
    StringInfoData buf;
    int ret;
    uint32 runid = 0;

    if (SPI_connect() < 0)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_connect failed")));

    initStringInfo(&buf);
    if (curr_val)
        appendStringInfo(&buf, "select currval('gms_profiler.plsql_profiler_runnumber');");
    else
        appendStringInfo(&buf, "select nextval('gms_profiler.plsql_profiler_runnumber');");

    ret = SPI_exec(buf.data, 0, NULL);
    pfree(buf.data);

    if ((ret == SPI_OK_SELECT) && (SPI_processed == 1)) {
        char *value = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
        char *end;
        runid = strtoul(value, &end, 10);
    }
    SPI_finish();

    elog(DEBUG1, "get_runid_from_profiler_sequence get runid %u", runid);

    if (!runid)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("can not get runid from plsql_profiler_runnumber")));

    return runid;
}

static void update_profiler_unit_entry(ProfilerUnitHashEntry *unit_entry, SPIPlanPtr plan)
{
    ProfilerContext *profiler_cxt = get_session_context();
    Datum values[3];
    char nulls[3] = {' ', ' ', ' '};
    StringInfoData buf;
    int ret;

    /* values 0 total_time */
    initStringInfo(&buf);
    appendStringInfo(&buf, "%ld", unit_entry->total_time);
    values[0] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 1 runid */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%u", profiler_cxt->runid);
    values[1] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 2: unit_number */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%u", unit_entry->unit_number);
    values[2] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree(buf.data);

    ret = SPI_execute_plan(plan, values, nulls, false, 1);
    if (ret != SPI_OK_UPDATE)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("can't update total_time of plsql_profiler_units")));

}

static void insert_profiler_unit_entry(ProfilerUnitHashEntry *unit_entry, SPIPlanPtr plan)
{
    HeapTuple proc_tup;
    HeapTuple auth_tp;
    bool is_null;
    Datum kind_datum;
    char proc_type;
    StringInfoData buf;
    Form_pg_proc proc_struct;
    Form_pg_authid authid_struct;
    char *unit_name;
    char *usename;
    ProfilerContext *profiler_cxt = get_session_context();
    Datum values[7];
    char nulls[7] = {' ', ' ', ' ', ' ', ' ', ' ', ' '};
    Oid proc_owner;
    int ret;

    /* values 0: runid */
    initStringInfo(&buf);
    appendStringInfo(&buf, "%u", profiler_cxt->runid);
    values[0] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 1: unit_number */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%u", unit_entry->unit_number);
    values[1] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    if (unit_entry->unit_oid == InvalidOid) {
        /* values 2: unit_type when unit_oid is 0, unit_number must be 1,
         * regard it as an anonymous block */
        resetStringInfo(&buf);
        appendStringInfo(&buf, "%s", "ANONYMOUS BLOCK");
        values[2] = DirectFunctionCall1(varcharin, CStringGetDatum(buf.data));

        /* values 3: unit_owner anonymous*/
        resetStringInfo(&buf);
        appendStringInfo(&buf, "%s", "<anonymous>");
        values[3] = DirectFunctionCall1(varcharin, CStringGetDatum(buf.data));

        /* values 4: unit_name anonymous*/
        resetStringInfo(&buf);
        appendStringInfo(&buf, "%s", "<anonymous>");
        values[4] = DirectFunctionCall1(varcharin, CStringGetDatum(buf.data));

    } else {
        /* values 2: unit_type PROCEDURE or FUNCTION
         * should get prokind attr from pg_proc */
        proc_tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(unit_entry->unit_oid));
        if (!HeapTupleIsValid(proc_tup)) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("cache lookup failed for function %u", unit_entry->unit_oid)));
        }

        kind_datum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_prokind, &is_null);
        proc_type = DatumGetChar(kind_datum);

        resetStringInfo(&buf);

        if (proc_type == 'f') {
            appendStringInfo(&buf, "%s", "FUNCTION");
        } else if (proc_type == 'p') {
            appendStringInfo(&buf, "%s", "PROCEDURE");
        } else {
            appendStringInfo(&buf, "%s", "UNKNOWN");
        }

        values[2] = DirectFunctionCall1(varcharin, CStringGetDatum(buf.data));

        /* values 4: unit_name
         * unit_name is in the pg_proc system table,
         * so get it before unit_owner */

        proc_struct = (Form_pg_proc) GETSTRUCT(proc_tup);
        unit_name = NameStr(proc_struct->proname);

        resetStringInfo(&buf);
        appendStringInfo(&buf, "%s", unit_name);
        values[4] = DirectFunctionCall1(varcharin, CStringGetDatum(buf.data));

        /* get proc_owner oid before release cache */
        proc_owner = proc_struct->proowner;
        ReleaseSysCache(proc_tup);

        /* values 3: unit_owner */
        auth_tp = SearchSysCache1(AUTHOID, ObjectIdGetDatum(proc_owner));
        if (!HeapTupleIsValid(auth_tp)) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("cache lookup failed for proc owner %u", proc_owner)));
        }

        authid_struct = (Form_pg_authid) GETSTRUCT(auth_tp);
        usename = NameStr(authid_struct->rolname);

        resetStringInfo(&buf);
        appendStringInfo(&buf, "%s", usename);
        values[3] = DirectFunctionCall1(varcharin, CStringGetDatum(buf.data));
        ReleaseSysCache(auth_tp);
    }

    /* unit5 unit_timestamp  */
    Datum timestamp;
    Datum timestamp_scaled;
    timestamp = DirectFunctionCall1(timestamptz_timestamp, unit_entry->unit_timestamp);
    timestamp_scaled = DirectFunctionCall2(timestamp_scale, timestamp, Int32GetDatum(0));
    values[5] = timestamp_scaled;

    /* unit6 total_time */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%ld", unit_entry->total_time);
    values[6] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    pfree(buf.data);

    ret = SPI_execute_plan(plan, values, nulls, false, 1);
    if (ret != SPI_OK_INSERT)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("can't insert data into plsql_profiler_units")));

}

static void flush_profiler_units(void)
{
    SPIPlanPtr	insert_plan = NULL;
    SPIPlanPtr	update_plan = NULL;
    Oid	insert_argtypes[] = {NUMERICOID, NUMERICOID, VARCHAROID, VARCHAROID, VARCHAROID, TIMESTAMPOID, NUMERICOID};
    Oid	update_argtypes[] = {NUMERICOID, NUMERICOID, NUMERICOID};
    ProfilerContext *profiler_cxt = get_session_context();
    HASH_SEQ_STATUS scan;
    ProfilerUnitHashEntry *unit_entry;

    insert_plan = SPI_prepare("insert into gms_profiler.plsql_profiler_units (runid, unit_number, "
                       "unit_type, unit_owner, unit_name, unit_timestamp, total_time) "
                       "values ($1, $2, $3, $4, $5, $6, $7)", 7, insert_argtypes);
    if (insert_plan == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_prepare insert into plsql_profiler_units failed")));

    update_plan = SPI_prepare("update gms_profiler.plsql_profiler_units set total_time = $1 "
                              "where runid = $2 and unit_number = $3",
                              3, update_argtypes);
    if (update_plan == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_prepare update plsql_profiler_units failed")));

    hash_seq_init(&scan, profiler_cxt->unit_hash);
    while ((unit_entry = (ProfilerUnitHashEntry *)hash_seq_search(&scan))) {
        if (unit_entry->unit_flushed) {
            elog(DEBUG1, "update_profiler_unit_entry %u %u ", profiler_cxt->runid, unit_entry->unit_number);
            update_profiler_unit_entry(unit_entry, update_plan);
        } else {
            elog(DEBUG1, "insert_profiler_unit_entry %u %u ", profiler_cxt->runid, unit_entry->unit_number);
            insert_profiler_unit_entry(unit_entry, insert_plan);
            unit_entry->unit_flushed = true;
        }
    }
}

static void insert_profiler_data_entry(ProfilerDataHashEntry *data_entry, SPIPlanPtr plan)
{
    StringInfoData buf;
    ProfilerContext *profiler_cxt = get_session_context();
    Datum values[7];
    char nulls[7] = {' ', ' ', ' ', ' ', ' ', ' ', ' '};
    int ret;

    /* values 0: runid */
    initStringInfo(&buf);
    appendStringInfo(&buf, "%u", profiler_cxt->runid);
    values[0] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 1: unit_number */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%u", data_entry->unit_number);
    values[1] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 2: lineno */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%u", data_entry->lineno);
    values[2] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 3: total occur */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%u", data_entry->total_occur);
    values[3] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 4: total time */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%ld", data_entry->total_time);
    values[4] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 5: min time */
    resetStringInfo(&buf);
    if (data_entry->min_time == INT64_MAX)
        appendStringInfo(&buf, "%d", 0);
    else
        appendStringInfo(&buf, "%ld", data_entry->min_time);
    values[5] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 6: max time */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%ld", data_entry->max_time);
    values[6] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    pfree(buf.data);

    ret = SPI_execute_plan(plan, values, nulls, false, 1);
    if (ret != SPI_OK_INSERT)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("can't insert data into plsql_profiler_data")));
}

static void update_profiler_data_entry(ProfilerDataHashEntry *data_entry, SPIPlanPtr plan)
{
    StringInfoData buf;
    ProfilerContext *profiler_cxt = get_session_context();
    Datum values[7];
    char nulls[7] = {' ', ' ', ' ', ' ', ' ', ' ', ' '};
    int ret;

    /* values 0: total occur */
    initStringInfo(&buf);
    appendStringInfo(&buf, "%u", data_entry->total_occur);
    values[0] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 1: total time */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%ld", data_entry->total_time);
    values[1] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 2: min time */
    resetStringInfo(&buf);
    if (data_entry->min_time == INT64_MAX)
        appendStringInfo(&buf, "%d", 0);
    else
        appendStringInfo(&buf, "%ld", data_entry->min_time);
    values[2] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 3: max time */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%ld", data_entry->max_time);
    values[3] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 4: runid */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%u", profiler_cxt->runid);
    values[4] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 5: unit_number */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%u", data_entry->unit_number);
    values[5] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 6: lineno */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%u", data_entry->lineno);
    values[6] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    pfree(buf.data);

    ret = SPI_execute_plan(plan, values, nulls, false, 1);
    if (ret != SPI_OK_UPDATE)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("can't update plsql_profiler_data data")));
}

static void flush_profiler_data(void)
{
    SPIPlanPtr	insert_plan = NULL;
    SPIPlanPtr	update_plan = NULL;
    Oid	argtypes[] = {NUMERICOID, NUMERICOID, NUMERICOID, NUMERICOID, NUMERICOID, NUMERICOID, NUMERICOID};
    ProfilerContext *profiler_cxt = get_session_context();
    HASH_SEQ_STATUS scan;
    ProfilerDataHashEntry *data_entry;

    insert_plan = SPI_prepare("insert into gms_profiler.plsql_profiler_data (runid, unit_number, "
                       "line#, total_occur, total_time, min_time, max_time) "
                       "values ($1, $2, $3, $4, $5, $6, $7)", 7, argtypes);
    if (insert_plan == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_prepare insert into plsql_profiler_data failed")));

    update_plan = SPI_prepare("update gms_profiler.plsql_profiler_data set total_occur = $1, "
                              "total_time = $2, min_time = $3, max_time = $4 where runid = $5"
                              " and unit_number = $6 and line# = $7", 7, argtypes);
    if (update_plan == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_prepare update plsql_profiler_data failed")));

    hash_seq_init(&scan, profiler_cxt->data_hash);
    while ((data_entry = (ProfilerDataHashEntry *)hash_seq_search(&scan))) {
        if (data_entry->data_flushed) {
            elog(DEBUG1, "update data %u %u %u", profiler_cxt->runid, data_entry->unit_number, data_entry->lineno);
            update_profiler_data_entry(data_entry, update_plan);
        } else {
            elog(DEBUG1, "insert data %u %u %u", profiler_cxt->runid, data_entry->unit_number, data_entry->lineno);
            insert_profiler_data_entry(data_entry, insert_plan);
            data_entry->data_flushed = true;
        }
    }
}

static void update_profiler_runs()
{
    StringInfoData buf;
    int ret;
    ProfilerContext *profiler_cxt = get_session_context();
    SPIPlanPtr	plan = NULL;
    Datum values[2];
    char nulls[2] = {' ', ' '};
    Oid	argtypes[2] = {NUMERICOID, NUMERICOID};

    plan = SPI_prepare("update gms_profiler.plsql_profiler_runs set run_total_time = $1 where runid = $2", 2, argtypes);
    if (plan == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_prepare update plsql_profiler_runs failed")));

    /* values 0: run_total_time */
    initStringInfo(&buf);
    appendStringInfo(&buf, "%lu", profiler_cxt->run_total_time);
    values[0] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 1: runid */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%u", profiler_cxt->runid);
    values[1] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    pfree(buf.data);

    ret = SPI_execute_plan(plan, values, nulls, false, 1);
    if (ret != SPI_OK_UPDATE)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("can't update run_total_time of plsql_profiler_runs")));
}

static void insert_profiler_runs(void)
{
    StringInfoData buf;
    int ret;
    ProfilerContext *profiler_cxt = get_session_context();
    HeapTuple auth_tp;
    Form_pg_authid authid_struct;
    SPIPlanPtr	plan = NULL;
    Datum values[6];
    char nulls[6] = {' ', ' ', ' ', ' ', ' ', ' '};
    Oid	argtypes[] = {NUMERICOID, VARCHAROID, TIMESTAMPOID, VARCHAROID, NUMERICOID, VARCHAROID};

    plan = SPI_prepare("insert into gms_profiler.plsql_profiler_runs (runid, run_owner, "
                       "run_date, run_comment, run_total_time, run_comment1) "
                       "values ($1, $2, $3, $4, $5, $6)", 6, argtypes);
    if (plan == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_prepare insert into plsql_profiler_runs failed")));

    /* values 0: runid */
    initStringInfo(&buf);
    appendStringInfo(&buf, "%u", profiler_cxt->runid);
    values[0] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 1: run_owner */
    if (profiler_cxt->run_owner != InvalidOid) {
        auth_tp = SearchSysCache1(AUTHOID, ObjectIdGetDatum(profiler_cxt->run_owner));
        if (!HeapTupleIsValid(auth_tp)) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("cache lookup failed for proc owner %u", profiler_cxt->run_owner)));
        }

        authid_struct = (Form_pg_authid) GETSTRUCT(auth_tp);
        resetStringInfo(&buf);
        appendStringInfo(&buf, "%s", NameStr(authid_struct->rolname));
        values[1] = DirectFunctionCall1(varcharin, CStringGetDatum(buf.data));
        ReleaseSysCache(auth_tp);
    } else {
        nulls[1] = 'n';
    }

    /* values 2: run_date */
    Datum timestamp;
    Datum timestamp_scaled;
    timestamp = DirectFunctionCall1(timestamptz_timestamp, profiler_cxt->run_timestamp);
    timestamp_scaled = DirectFunctionCall2(timestamp_scale, timestamp, Int32GetDatum(0));
    values[2] = timestamp_scaled;

    /* values 3: run_comment */
    if (profiler_cxt->run_comment) {
        resetStringInfo(&buf);
        appendStringInfo(&buf, "%s", profiler_cxt->run_comment);
        values[3] = DirectFunctionCall1(varcharin, CStringGetDatum(buf.data));
    } else {
        nulls[3] = 'n';
    }

    /* values 4: run_total_time */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "%lu", profiler_cxt->run_total_time);
    values[4] = DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
                                    ObjectIdGetDatum(0), Int32GetDatum(-1));

    /* values 5: run_comment1 */
    if (profiler_cxt->run_comment1) {
        resetStringInfo(&buf);
        appendStringInfo(&buf, "%s", profiler_cxt->run_comment1);
        values[5] = DirectFunctionCall1(varcharin, CStringGetDatum(buf.data));
    } else {
        nulls[5] = 'n';
    }

    pfree(buf.data);

    ret = SPI_execute_plan(plan, values, nulls, false, 1);
    if (ret != SPI_OK_INSERT)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("can't insert data into plsql_profiler_runs")));

}

static void flush_profiler_runs() {
    ProfilerContext *profiler_cxt = get_session_context();

    if (profiler_cxt->runinfo_flushed) {
        elog(DEBUG1, "update_profiler_runs %u", profiler_cxt->runid);
        update_profiler_runs();
    } else {
        elog(DEBUG1, "insert_profiler_runs %u ", profiler_cxt->runid);
        insert_profiler_runs();
        profiler_cxt->runinfo_flushed = true;
    }
}

static void flush_profiler_to_table(void)
{
    if (SPI_connect() < 0)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_connect failed")));

    flush_profiler_runs();
    flush_profiler_units();
    flush_profiler_data();

    SPI_finish();
}

static int start_profiler_internal(FunctionCallInfo fcinfo, uint32 *runid)
{
    char *run_comment = NULL;
    char *run_comment1 = NULL;
    HASHCTL unit_hash_ctl;
    HASHCTL data_hash_ctl;
    ProfilerUnitHashKey unit_key;
    ProfilerUnitHashEntry *unit_entry;
    bool found;
    errno_t rc;
    int32 result = PROFILER_ERROR_OK;
    ProfilerContext *profiler_cxt = get_session_context();

    if (RecoveryInProgress())
        elog(ERROR, "Standby do not support gms_profiler, please do it in primary!");

    if ((!u_sess->plsql_cxt.inited) || (!profiler_cxt))
        return PROFILER_ERROR_PARAM;

    /* start_profiler can not thow any exception */
    PG_TRY();
    {
        if ((PG_NARGS() >= 1) && (!PG_ARGISNULL(0)))
            run_comment = text_to_cstring(PG_GETARG_VARCHAR_PP(0));
        else {
            Datum timestamp;
            Datum timestamp_scaled;
            timestamp = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());
            timestamp_scaled = DirectFunctionCall2(timestamp_scale, timestamp, Int32GetDatum(0));
            run_comment = DatumGetCString(DirectFunctionCall1(timestamp_out, timestamp_scaled));
        }

        if ((PG_NARGS() >= 2) && (!PG_ARGISNULL(1)))
            run_comment1 = text_to_cstring(PG_GETARG_VARCHAR_PP(1));


        /* If call start_profiler twice without call stop profiler, do not get new runnid */
        if ((profiler_cxt->state == PROFILER_ACTIVE) || (profiler_cxt->state == PROFILER_PAUSED))
            profiler_cxt->runid = get_runid_from_profiler_sequence(true);
        else
            profiler_cxt->runid = get_runid_from_profiler_sequence(false);

        profiler_cxt->state = PROFILER_ACTIVE;
        profiler_cxt->current_unit_number = 1;
        profiler_cxt->max_unit_number = 1;
        profiler_cxt->run_owner = GetUserId();
        profiler_cxt->run_timestamp = GetCurrentTimestamp();
        profiler_cxt->run_comment = MemoryContextStrdup(profiler_cxt->mem_cxt, run_comment);
        profiler_cxt->run_comment1 = MemoryContextStrdup(profiler_cxt->mem_cxt, run_comment1);
        profiler_cxt->runinfo_flushed = false;
        profiler_cxt->in_stmt = 0;

        if (profiler_cxt->unit_stack)
            pfree_ext(profiler_cxt->unit_stack);

        profiler_cxt->unit_stack = (uint32 *) MemoryContextAllocZero(profiler_cxt->mem_cxt,
                                                                     sizeof(uint32) * PROFILER_UNIT_STACK_SIZE);
        /* Push first unit number */
        profiler_cxt->unit_stack[0] = profiler_cxt->current_unit_number;
        profiler_cxt->unit_stack_top = 1;

        if (profiler_cxt->unit_hash)
            hash_destroy(profiler_cxt->unit_hash);

        profiler_cxt->unit_hash = NULL;

        rc = memset_s(&unit_hash_ctl, sizeof(unit_hash_ctl), 0, sizeof(unit_hash_ctl));
        securec_check(rc, "\0", "\0");

        unit_hash_ctl.keysize = sizeof(ProfilerUnitHashKey);
        unit_hash_ctl.entrysize = sizeof(ProfilerUnitHashEntry);
        unit_hash_ctl.hash = tag_hash;
        unit_hash_ctl.hcxt = profiler_cxt->mem_cxt;
        profiler_cxt->unit_hash = hash_create("dmbs profiler unit hash", PROFILER_UNIT_HASH_NUMBER,
                                              &unit_hash_ctl,
                                              HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

        if (profiler_cxt->data_hash)
            hash_destroy(profiler_cxt->data_hash);

        profiler_cxt->data_hash = NULL;

        rc = memset_s(&data_hash_ctl, sizeof(data_hash_ctl), 0, sizeof(data_hash_ctl));
        securec_check(rc, "\0", "\0");

        data_hash_ctl.keysize = sizeof(ProfilerDataHashKey);
        data_hash_ctl.entrysize = sizeof(ProfilerDataHashEntry);
        data_hash_ctl.hash = tag_hash;
        data_hash_ctl.hcxt = profiler_cxt->mem_cxt;
        profiler_cxt->data_hash = hash_create("dmbs profiler data hash", PROFILER_DATA_HASH_NUMBER,
                                              &data_hash_ctl,
                                              HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

        /* unit_number 1 is anonymous block */
        unit_key.unit_oid = 0;
        unit_entry = (ProfilerUnitHashEntry *) hash_search(profiler_cxt->unit_hash, (void *) &unit_key,
                                                           HASH_ENTER, &found);
        Assert(unit_entry);
        unit_entry->unit_oid = 0;
        unit_entry->unit_number = profiler_cxt->current_unit_number;
        unit_entry->unit_timestamp = GetCurrentTimestamp();
        unit_entry->total_time = 0;
        unit_entry->unit_flushed = false;

        elog(DEBUG1, "start_profiler state %d runid %d current_unit_number %u max_unit_number %u",
             profiler_cxt->state, profiler_cxt->runid,
             profiler_cxt->current_unit_number,
             profiler_cxt->max_unit_number);

        *runid = profiler_cxt->runid;
    }
    PG_CATCH();
    {
        pfree_ext(profiler_cxt->unit_stack);

        hash_destroy(profiler_cxt->data_hash);
        profiler_cxt->data_hash = NULL;

        hash_destroy(profiler_cxt->data_hash);
        profiler_cxt->data_hash = NULL;

        pfree_ext(profiler_cxt->run_comment);
        pfree_ext(profiler_cxt->run_comment1);

        FlushErrorState();
        result = PROFILER_ERROR_IO;
    }
    PG_END_TRY();
    return result;
}

static Datum make_profiler_result(uint32 runid, int32 result)
{
    TupleDesc tupdesc;
    Datum   values[2];
    bool    nulls[2];
    HeapTuple htup;

    tupdesc = CreateTemplateTupleDesc(2, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "run_number",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "result",
                       INT4OID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    values[0] = UInt32GetDatum(runid);
    values[1] = Int32GetDatum(result);
    nulls[0] = false;
    nulls[1] = false;

    htup = heap_form_tuple(tupdesc, values, nulls);

    return HeapTupleGetDatum(htup);
}

/*
 * begin_profile_block
 * At the begining of execute plpgsql procedure block
 * call this function to generate new unit number if this block is not called before
 * get it's unit number from unit_hash if block is called.
 *
 */
static void begin_profile_block(Oid block_oid)
{
    bool is_null;
    char *proc_name;
    char proc_type;
    Oid proc_owner;
    Form_pg_proc proc_struct;
    Form_pg_authid authid_struct;
    char *usename;
    ProfilerContext *profiler_cxt = get_session_context();
    ProfilerUnitHashKey unit_key;
    ProfilerUnitHashEntry *unit_entry;
    bool found;

    if ((!u_sess->plsql_cxt.inited) || !check_profiler_table_exists() || (!profiler_cxt)
        || (profiler_cxt->state != PROFILER_ACTIVE))
        return;

    unit_key.unit_oid = block_oid;
    unit_entry = (ProfilerUnitHashEntry*)hash_search(profiler_cxt->unit_hash, (void*)&unit_key, HASH_ENTER, &found);
    Assert(unit_entry);
    if (!found) {
        unit_entry->unit_oid = block_oid;
        profiler_cxt->max_unit_number += 1;
        profiler_cxt->current_unit_number = profiler_cxt->max_unit_number;
        unit_entry->unit_number = profiler_cxt->current_unit_number;
        unit_entry->unit_timestamp = GetCurrentTimestamp();
        unit_entry->total_time = 0;
        unit_entry->unit_flushed = false;
    } else {
        profiler_cxt->current_unit_number = unit_entry->unit_number;
        unit_entry->unit_timestamp = GetCurrentTimestamp();
    }

    if (profiler_cxt->unit_stack_top >= PROFILER_UNIT_STACK_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("procedure stack depth is too much")));
    }

    profiler_cxt->unit_stack[profiler_cxt->unit_stack_top++] = profiler_cxt->current_unit_number;

    HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(block_oid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("cache lookup failed for function %u", block_oid)));
    }

    proc_struct = (Form_pg_proc)GETSTRUCT(tp);
    proc_name = NameStr(proc_struct->proname);
    proc_owner = proc_struct->proowner;

    Datum kind_datum = SysCacheGetAttr(PROCOID, tp, Anum_pg_proc_prokind, &is_null);
    proc_type = DatumGetChar(kind_datum);
    ReleaseSysCache(tp);

    tp = SearchSysCache1(AUTHOID, ObjectIdGetDatum(proc_owner));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("cache lookup failed for function %u", proc_owner)));
    }

    authid_struct = (Form_pg_authid)GETSTRUCT(tp);
    usename = NameStr(authid_struct->rolname);
    ReleaseSysCache(tp);

    elog(DEBUG1, "block begin oid %u unit_number %u, proc_name %s proc_type %c",
         block_oid,
         profiler_cxt->current_unit_number,
         proc_name,
         proc_type);
}

/*
 * end_profile_block
 * At the end of execute plpgsql procedure block
 * call this function to record this block execute time
 *
 */
static void end_profile_block(Oid block_oid)
{
    TimestampTz current_timestamp;
    ProfilerContext *profiler_cxt = get_session_context();
    ProfilerUnitHashKey unit_key;
    ProfilerUnitHashEntry *unit_entry;
    bool found;

    if ((!u_sess->plsql_cxt.inited) || !check_profiler_table_exists() || (!profiler_cxt)
        || (profiler_cxt->state != PROFILER_ACTIVE))
        return;

    unit_key.unit_oid = block_oid;

    unit_entry = (ProfilerUnitHashEntry*)hash_search(profiler_cxt->unit_hash,
                                                     (void*)&unit_key, HASH_ENTER, &found);
    if (!found) {
        elog(ERROR, "can not find unit oid %u", block_oid);
    } else {
        current_timestamp = GetCurrentTimestamp();
        unit_entry->total_time += current_timestamp - unit_entry->unit_timestamp;
    }

    /* pop unit number from unit stack */
    profiler_cxt->unit_stack_top--;

    /* restore previous unit number */
    if (profiler_cxt->unit_stack_top >= 1) {
        profiler_cxt->current_unit_number = profiler_cxt->unit_stack[profiler_cxt->unit_stack_top-1];
    }

    if (profiler_cxt->current_unit_number < 1 ||
            profiler_cxt->current_unit_number > profiler_cxt->max_unit_number) {
        elog(DEBUG1, "Wrong unit_number %u", profiler_cxt->current_unit_number);
        return;
    }

    elog(DEBUG1, "block end oid %u current_unit_number %u",
         block_oid,
         profiler_cxt->current_unit_number);
}

/*
 * begin_profile_stmt
 * At the begining of execute plpgsql statement
 * call this function to record statement start timestamp
 * and occures
 */
static void begin_profile_stmt(int lineno)
{
    ProfilerContext *profiler_cxt = get_session_context();
    ProfilerDataHashKey line_key;
    ProfilerDataHashEntry *line_entry;
    bool found;

    if ((!u_sess->plsql_cxt.inited) || !check_profiler_table_exists() || (!profiler_cxt)
        || (profiler_cxt->state != PROFILER_ACTIVE))
        return;

    /* use in_stmt to assure begin_profile_stmt is called
     * before end_profile_stmt is called */
    profiler_cxt->in_stmt += 1;

    line_key.unit_number = profiler_cxt->current_unit_number;
    line_key.lineno = lineno;
    line_entry = (ProfilerDataHashEntry*)hash_search(profiler_cxt->data_hash,
                                                     (void*)&line_key, HASH_ENTER, &found);
    Assert(line_entry);
    if (!found) {
        line_entry->unit_number = profiler_cxt->current_unit_number;
        line_entry->lineno = lineno;
        line_entry->stmt_timestamp = GetCurrentTimestamp();
        line_entry->total_occur = 1;
        line_entry->total_time = 0;
        line_entry->min_time = INT64_MAX;
        line_entry->max_time = 0;
        line_entry->data_flushed = false;
    } else {
        line_entry->stmt_timestamp = GetCurrentTimestamp();
        line_entry->total_occur += 1;
    }

    elog(DEBUG1, "stmt begin runid %u unit_number %u lineno %d  "
              "stmt_timestamp %ld total_occur %u total_time %ld min_time %ld max_time %ld",
         profiler_cxt->runid, profiler_cxt->current_unit_number, lineno,
         line_entry->stmt_timestamp, line_entry->total_occur, line_entry->total_time,
         line_entry->min_time, line_entry->max_time);
}

/*
 * end_profile_stmt
 * At the end of execute plpgsql statement
 * call this function to record statement exuecte time
 */
static void end_profile_stmt(int lineno)
{
    ProfilerContext *profiler_cxt = get_session_context();
    ProfilerDataHashKey line_key;
    ProfilerDataHashEntry *line_entry;
    bool found;
    int64 stmt_time;

    if ((!u_sess->plsql_cxt.inited) || !check_profiler_table_exists() || (!profiler_cxt)
        || (profiler_cxt->state != PROFILER_ACTIVE))
        return;

    /* if in_stmt < 1 means begin_profile_stmt is not called before */
    if (profiler_cxt->in_stmt < 1)
        return;

    profiler_cxt->in_stmt -= 1;

    line_key.unit_number = profiler_cxt->current_unit_number;
    line_key.lineno = lineno;
    line_entry = (ProfilerDataHashEntry*)hash_search(profiler_cxt->data_hash, (void*)&line_key, HASH_ENTER, &found);
    Assert(line_entry);
    if (!found) {
        line_entry->unit_number = profiler_cxt->current_unit_number;
        line_entry->lineno = lineno;
        line_entry->stmt_timestamp = GetCurrentTimestamp();
        line_entry->total_occur = 1;
        line_entry->total_time = 0;
        line_entry->min_time = INT64_MAX;
        line_entry->max_time = 0;
        line_entry->data_flushed = false;
    } else {
        stmt_time = GetCurrentTimestamp() - line_entry->stmt_timestamp;
        line_entry->total_time += stmt_time;

        if (stmt_time < line_entry->min_time)
            line_entry->min_time = stmt_time;

        if (stmt_time > line_entry->max_time)
            line_entry->max_time = stmt_time;
    }

    elog(DEBUG1, "stmt end runid %u unit_number %u lineno %d  "
              "stmt_timestamp %ld total_occur %u total_time %ld min_time %ld max_time %ld",
         profiler_cxt->runid, profiler_cxt->current_unit_number, lineno,
         line_entry->stmt_timestamp, line_entry->total_occur, line_entry->total_time,
         line_entry->min_time, line_entry->max_time);
}

/*
 * profile_stmt_once
 * For loop/while/for statement, only record statement occures
 * do not record statement execute time.
 */
static void profile_stmt_once(int lineno)
{
    ProfilerContext *profiler_cxt = get_session_context();
    ProfilerDataHashKey line_key;
    ProfilerDataHashEntry *line_entry;
    bool found;

    if ((!u_sess->plsql_cxt.inited) || !check_profiler_table_exists() || (!profiler_cxt)
        || (profiler_cxt->state != PROFILER_ACTIVE))
        return;

    line_key.unit_number = profiler_cxt->current_unit_number;
    line_key.lineno = lineno;
    line_entry = (ProfilerDataHashEntry*)hash_search(profiler_cxt->data_hash, (void*)&line_key, HASH_ENTER, &found);
    Assert(line_entry);
    if (!found) {
        line_entry->unit_number = profiler_cxt->current_unit_number;
        line_entry->lineno = lineno;
        line_entry->stmt_timestamp = GetCurrentTimestamp();
        line_entry->total_occur = 1;
        line_entry->total_time = 0;
        line_entry->min_time = INT64_MAX;
        line_entry->max_time = 0;
        line_entry->data_flushed = false;
    } else {
        line_entry->total_occur += 1;
    }

    elog(DEBUG1, "stmt once runid %u unit_number %u lineno %d",
         profiler_cxt->runid, profiler_cxt->current_unit_number, lineno);
}

static bool cmdtype_is_loop(int cmdtype)
{
    if ((cmdtype == PLPGSQL_STMT_LOOP) || (cmdtype == PLPGSQL_STMT_WHILE)  || (cmdtype == PLPGSQL_STMT_FORI)
        || (cmdtype == PLPGSQL_STMT_FORS) || (cmdtype == PLPGSQL_STMT_FORC) || (cmdtype == PLPGSQL_STMT_FOREACH_A))
        return true;
    else
        return false;

}

static void plpgsql_cb_func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
    //elog(NOTICE, "function begin: \"%s\"", func->fn_signature);
    begin_profile_block(func->fn_oid);
}
static void plpgsql_cb_func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
    //elog(NOTICE, "function end: \"%s\"", func->fn_signature);
    end_profile_block(func->fn_oid);
}

static void plpgsql_cb_stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
    //elog(NOTICE, "statement [%20s] begin - [ln: %d]", plpgsql_stmt_typename(stmt), stmt->lineno);
    if (!cmdtype_is_loop((enum PLpgSQL_stmt_types)stmt->cmd_type)) {
        begin_profile_stmt(stmt->lineno);
    } else {
        profile_stmt_once(stmt->lineno);
    }
}

static void plpgsql_cb_stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
    //elog(NOTICE, "statement [%20s] end   - [ln: %d]", plpgsql_stmt_typename(stmt), stmt->lineno);
    if (!cmdtype_is_loop((enum PLpgSQL_stmt_types)stmt->cmd_type)) {
        end_profile_stmt(stmt->lineno);
    }
}

Datum start_profiler(PG_FUNCTION_ARGS)
{
    uint32 runid = 0;
    int result;

    result = start_profiler_internal(fcinfo, &runid);

    PG_RETURN_INT32(result);
}

Datum start_profiler_1(PG_FUNCTION_ARGS)
{
    uint32 runid = 0;
    int result;

    result = start_profiler_internal(fcinfo, &runid);

    PG_RETURN_VOID();
}

Datum start_profiler_ext(PG_FUNCTION_ARGS)
{
    uint32 runid = 0;
    int result;

    result = start_profiler_internal(fcinfo, &runid);

    if (result == PROFILER_ERROR_OK)
        return make_profiler_result(runid, result);
    else
        return make_profiler_result(0, result);
}

Datum start_profiler_ext_1(PG_FUNCTION_ARGS)
{
    uint32 runid = 0;
    int result;

    result = start_profiler_internal(fcinfo, &runid);

    if (result == PROFILER_ERROR_OK)
        PG_RETURN_INT32(runid);
    else
        PG_RETURN_INT32(0);
}

Datum stop_profiler(PG_FUNCTION_ARGS)
{
    ProfilerContext *profiler_cxt = get_session_context();
    int32 result = PROFILER_ERROR_OK;

    if (RecoveryInProgress())
        elog(ERROR, "Standby do not support gms_profiler, please do it in primary!");

    if ((!u_sess->plsql_cxt.inited) || (!profiler_cxt))
        PG_RETURN_INT32(PROFILER_ERROR_PARAM);

    elog(DEBUG1, "stop_profiler state %d runid %d current_unit_number %u max_unit_number %u",
         profiler_cxt->state, profiler_cxt->runid, profiler_cxt->current_unit_number, profiler_cxt->max_unit_number);

    /* If not call start_profiler before, do nothing! */
    if (profiler_cxt->state == PROFILER_INACTIVE)
        PG_RETURN_INT32(PROFILER_ERROR_OK);

    profiler_cxt->state = PROFILER_INACTIVE;
    profiler_cxt->run_total_time = GetCurrentTimestamp() - profiler_cxt->run_timestamp;

    PG_TRY();
    {
        flush_profiler_to_table();
    }
    PG_CATCH();
    {
        result = PROFILER_ERROR_IO;
        FlushErrorState();
    }
    PG_END_TRY();

    /* clean memory when stop profiler */
    pfree_ext(profiler_cxt->unit_stack);
    profiler_cxt->unit_stack_top = 0;

    hash_destroy(profiler_cxt->unit_hash);
    profiler_cxt->unit_hash = NULL;

    hash_destroy(profiler_cxt->data_hash);
    profiler_cxt->data_hash = NULL;

    pfree_ext(profiler_cxt->run_comment);
    pfree_ext(profiler_cxt->run_comment1);

    profiler_cxt->runinfo_flushed = false;
    profiler_cxt->in_stmt = 0;

    PG_RETURN_INT32(result);
}

Datum flush_data(PG_FUNCTION_ARGS)
{
    ProfilerContext *profiler_cxt = get_session_context();
    int32 result = PROFILER_ERROR_OK;

    if (RecoveryInProgress())
        elog(ERROR, "Standby do not support gms_profiler, please do it in primary!");

    if ((!u_sess->plsql_cxt.inited) || (!profiler_cxt))
        PG_RETURN_INT32(PROFILER_ERROR_PARAM);

    elog(DEBUG1, "flush_data state %d runid %d current_unit_number %u max_unit_number %u",
         profiler_cxt->state, profiler_cxt->runid, profiler_cxt->current_unit_number, profiler_cxt->max_unit_number);

    PG_TRY();
    {
        flush_profiler_to_table();
    }
    PG_CATCH();
    {
        result = PROFILER_ERROR_IO;
        FlushErrorState();
    }
    PG_END_TRY();

    PG_RETURN_INT32(result);
}

Datum pause_profiler(PG_FUNCTION_ARGS)
{
    ProfilerContext *profiler_cxt = get_session_context();

    if (RecoveryInProgress())
        elog(ERROR, "Standby do not support gms_profiler, please do it in primary!");

    if ((!u_sess->plsql_cxt.inited) || (!profiler_cxt))
        PG_RETURN_INT32(PROFILER_ERROR_PARAM);

    elog(DEBUG1, "pause_profiler state %d runid %d current_unit_number %u max_unit_number %u",
         profiler_cxt->state, profiler_cxt->runid, profiler_cxt->current_unit_number, profiler_cxt->max_unit_number);


    /* PROFILER_INACTIVE state can not become PROFILER_PAUSED state  */
    if (profiler_cxt->state == PROFILER_ACTIVE)
        profiler_cxt->state = PROFILER_PAUSED;

    PG_RETURN_INT32(PROFILER_ERROR_OK);
}

Datum resume_profiler(PG_FUNCTION_ARGS)
{
    ProfilerContext *profiler_cxt = get_session_context();

    if (RecoveryInProgress())
        elog(ERROR, "Standby do not support gms_profiler, please do it in primary!");

    if ((!u_sess->plsql_cxt.inited) || (!profiler_cxt))
        PG_RETURN_INT32(PROFILER_ERROR_PARAM);

    elog(DEBUG1, "resume_profiler state %d runid %d current_unit_number %u max_unit_number %u",
         profiler_cxt->state, profiler_cxt->runid, profiler_cxt->current_unit_number, profiler_cxt->max_unit_number);

    /* PROFILER_INACTIVE state can not become PROFILER_ACTIVE state  */
    if (profiler_cxt->state == PROFILER_PAUSED)
        profiler_cxt->state = PROFILER_ACTIVE;

    PG_RETURN_INT32(PROFILER_ERROR_OK);
}

