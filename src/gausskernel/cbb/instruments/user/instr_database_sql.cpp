/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * instr_database_sql.cpp
 *   Per-database SQL n_calls / total_elapse aggregation
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/user/instr_database_sql.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "commands/dbcommands.h"
#include "funcapi.h"
#include "instruments/instr_unique_sql.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/atomic.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "instruments/instr_database_sql.h"

const int DATABASE_SQL_STAT_MAX_HASH_SIZE = 256;
const int DATABASE_SQL_STAT_ATTRNUM = 5;

typedef struct {
    Oid datid;
    pg_atomic_uint64 n_calls;
    int64 total_elapse_time; /* microseconds */
} DatabaseSQLStat;

typedef struct {
    Oid datid;
    uint64 n_calls;
    int64 total_elapse_time;
} DatabaseSQLStatSnapshot;

static uint32 database_sql_stat_hash_code(const void* key, Size size)
{
    return hash_uint32(*((const uint32*)key));
}

static int database_sql_stat_match(const void* key1, const void* key2, Size key_size)
{
    if (key1 != NULL && key2 != NULL && *((const Oid*)key1) == *((const Oid*)key2)) {
        return 0;
    }
    return 1;
}

void InitDatabaseSQLStat()
{
    g_instance.stat_cxt.DatabaseSQLStatContext = AllocSetContextCreate(g_instance.instance_context,
        "DatabaseSQLStatContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    HASHCTL ctl;
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check_c(rc, "\0", "\0");

    ctl.hcxt = g_instance.stat_cxt.DatabaseSQLStatContext;
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(DatabaseSQLStat);
    ctl.hash = database_sql_stat_hash_code;
    ctl.match = database_sql_stat_match;

    g_instance.stat_cxt.DatabaseSQLStatHTAB = hash_create("database sql stat hash table",
        DATABASE_SQL_STAT_MAX_HASH_SIZE,
        &ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_NOEXCEPT);

    SpinLockInit(&g_instance.stat_cxt.DatabaseSQLStatLock);
}

/*
 * UpdateDatabaseSQLStat - accumulate one finished SQL into per-database counters.
 * Called on the same path as UniqueSQL calls/elapse update (CN / single node).
 */
void UpdateDatabaseSQLStat(int64 elapse_start)
{
    if (!is_unique_sql_enabled() || elapse_start == 0) {
        return;
    }
    if (g_instance.stat_cxt.DatabaseSQLStatHTAB == NULL) {
        return;
    }

    Oid datid = u_sess->proc_cxt.MyDatabaseId;
    if (!OidIsValid(datid)) {
        return;
    }

    TimestampTz elapse_time = GetCurrentTimestamp() - elapse_start;
    elapse_time = (elapse_time == 0) ? 1 : elapse_time;

    SpinLockAcquire(&g_instance.stat_cxt.DatabaseSQLStatLock);
    bool found = false;
    DatabaseSQLStat* entry = (DatabaseSQLStat*)hash_search(
        g_instance.stat_cxt.DatabaseSQLStatHTAB, &datid, HASH_ENTER, &found);
    if (entry == NULL) {
        SpinLockRelease(&g_instance.stat_cxt.DatabaseSQLStatLock);
        ereport(WARNING, (errmodule(MOD_INSTR), errmsg("[db_sql_stat] out of memory when allocating entry")));
        return;
    }
    if (!found) {
        entry->datid = datid;
        pg_atomic_write_u64(&entry->n_calls, 0);
        entry->total_elapse_time = 0;
    }
    pg_atomic_fetch_add_u64(&entry->n_calls, 1);
    gs_atomic_add_64(&entry->total_elapse_time, elapse_time);
    SpinLockRelease(&g_instance.stat_cxt.DatabaseSQLStatLock);
}

static DatabaseSQLStatSnapshot* GetDatabaseSqlStatSnapshot(long* num)
{
    *num = 0;
    if (g_instance.stat_cxt.DatabaseSQLStatHTAB == NULL) {
        return NULL;
    }

    SpinLockAcquire(&g_instance.stat_cxt.DatabaseSQLStatLock);
    *num = hash_get_num_entries(g_instance.stat_cxt.DatabaseSQLStatHTAB);
    if (*num == 0) {
        SpinLockRelease(&g_instance.stat_cxt.DatabaseSQLStatLock);
        return NULL;
    }

    DatabaseSQLStatSnapshot* rows =
        (DatabaseSQLStatSnapshot*)palloc0_noexcept(*num * sizeof(DatabaseSQLStatSnapshot));
    if (rows == NULL) {
        SpinLockRelease(&g_instance.stat_cxt.DatabaseSQLStatLock);
        ereport(ERROR, (errmodule(MOD_INSTR), errmsg("[db_sql_stat] cannot alloc memory")));
        return NULL;
    }

    HASH_SEQ_STATUS hash_seq;
    DatabaseSQLStat* entry = NULL;
    int i = 0;
    hash_seq_init(&hash_seq, g_instance.stat_cxt.DatabaseSQLStatHTAB);
    while ((entry = (DatabaseSQLStat*)hash_seq_search(&hash_seq)) != NULL) {
        rows[i].datid = entry->datid;
        rows[i].n_calls = pg_atomic_read_u64(&entry->n_calls);
        rows[i].total_elapse_time = entry->total_elapse_time;
        i++;
    }
    SpinLockRelease(&g_instance.stat_cxt.DatabaseSQLStatLock);

    *num = i;
    return rows;
}

static TupleDesc CreateDatabaseSqlStatTupleDesc(void)
{
    int i = 0;
    TupleDesc tupdesc = CreateTemplateTupleDesc(DATABASE_SQL_STAT_ATTRNUM, false);

    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "datid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "datname", NAMEOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "n_calls", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "total_elapse_time", INT8OID, -1, 0);

    return BlessTupleDesc(tupdesc);
}

static HeapTuple FormDatabaseSqlStatTuple(TupleDesc tupdesc, DatabaseSQLStatSnapshot* row)
{
    Datum values[DATABASE_SQL_STAT_ATTRNUM];
    bool nulls[DATABASE_SQL_STAT_ATTRNUM] = {false};
    int i = 0;
    errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[i++] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
    values[i++] = ObjectIdGetDatum(row->datid);

    char* dbname = get_database_name(row->datid);
    if (dbname != NULL) {
        values[i++] = DirectFunctionCall1(namein, CStringGetDatum(dbname));
    } else {
        values[i++] = DirectFunctionCall1(namein, CStringGetDatum("*REMOVED_DB*"));
    }

    values[i++] = UInt64GetDatum(row->n_calls);
    values[i++] = Int64GetDatum(row->total_elapse_time);

    return heap_form_tuple(tupdesc, values, nulls);
}

Datum get_database_sql_stat(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("only system/monitor admin can get database sql statistics"))));
    }

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        long num = 0;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        funcctx->tuple_desc = CreateDatabaseSqlStatTupleDesc();
        funcctx->user_fctx = GetDatabaseSqlStatSnapshot(&num);
        funcctx->max_calls = num;
        MemoryContextSwitchTo(oldcontext);

        if (funcctx->max_calls == 0) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        DatabaseSQLStatSnapshot* row =
            (DatabaseSQLStatSnapshot*)funcctx->user_fctx + funcctx->call_cntr;
        HeapTuple tuple = FormDatabaseSqlStatTuple(funcctx->tuple_desc, row);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}
