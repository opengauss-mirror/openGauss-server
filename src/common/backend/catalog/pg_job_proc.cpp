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
 * pg_job_proc.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/catalog/catalog/pg_job_proc.cpp
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

/*
 * @brief get_random_program_proc_id
 *  Get large random program proc id. used only for program
 * @return uint32_t
 */
static uint32_t get_random_program_proc_id()
{
    uint32_t job_id;
    const int RANDOM_LOOP_MAX = 1024;
    for (int i = 0; i < RANDOM_LOOP_MAX; i++) {
        job_id = gs_random() % INT32_MAX_VALUE;
        job_id += INT32_MAX_VALUE;
        bool exist = SearchSysCacheExists1(PGJOBPROCID, UInt32GetDatum(job_id));
        if (!exist) {
            return job_id;
        }
    }

    for (job_id = INT32_MAX_VALUE; job_id > 0; job_id--) {
        /* Find a invalid jobid. */
        bool exist = SearchSysCacheExists1(PGJOBPROCID, UInt32GetDatum(job_id));
        if (!exist) {
            return job_id;
        }
    }
    if (!OidIsValid(job_id)) {
        ereport(ERROR,
            (errmodule(MOD_JOB), errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("All virtual jobids have alloc, and there is no free jobid."),
                errdetail("N/A"),
                errcause("cannot alloc job id"),
                erraction("Please drop some job")));
    }
    return 0;
}

/*
 * @brief dbe_insert_pg_job_proc
 *
 * @param job_id
 * @param job_action
 * @param job_name
 * @return Datum
 */
Datum dbe_insert_pg_job_proc(Datum job_id, Datum job_action, const Datum job_name)
{
    Datum values[Natts_pg_job_proc];
    bool nulls[Natts_pg_job_proc];
    errno_t rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");

    Relation rel = heap_open(PgJobProcRelationId, RowExclusiveLock);
    if (!OidIsValid(job_id)) {
        job_id = ObjectIdGetDatum(get_random_program_proc_id());
    }

    values[Anum_pg_job_proc_job_id - 1] = job_id;
    if (!PointerIsValid(job_action)) {
        nulls[Anum_pg_job_proc_what - 1] = true;
    }
    values[Anum_pg_job_proc_what - 1] = job_action;
    values[Anum_pg_job_proc_job_name - 1] = job_name;

    HeapTuple tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    (void)simple_heap_insert(rel, tuple);
    CatalogUpdateIndexes(rel, tuple);

    heap_close(rel, RowExclusiveLock);
    heap_freetuple_ext(tuple);

    return job_id;
}

int4 get_job_id_from_pg_job(Datum job_name)
{
    Relation pg_job_rel = heap_open(PgJobRelationId, AccessShareLock);
    HeapTuple tuple = search_from_pg_job(pg_job_rel, job_name);
    int4 job_id = 0;
    if (tuple != NULL) {
        bool isnull = false;
        job_id = heap_getattr(tuple, Anum_pg_job_job_id, pg_job_rel->rd_att, &isnull);
        heap_freetuple_ext(tuple);
    }
    
    heap_close(pg_job_rel, AccessShareLock);
    return job_id;
}

/*
 * @brief search_from_pg_job_proc_no_exception
 *
 * @param rel
 * @param job_name
 * @return HeapTuple
 */
HeapTuple search_from_pg_job_proc_no_exception(Relation rel, Datum job_name)
{
    int4 job_id = get_job_id_from_pg_job(job_name);
    ScanKeyInfo scan_key_info1;
    scan_key_info1.attribute_value = job_name;
    scan_key_info1.attribute_number = Anum_pg_job_proc_job_name;
    scan_key_info1.procedure = F_TEXTEQ;
    ScanKeyInfo scan_key_info2;
    scan_key_info2.attribute_value = job_id;
    scan_key_info2.attribute_number = Anum_pg_job_proc_job_id;
    scan_key_info2.procedure = F_INT4EQ;
    List *tuples = search_by_sysscan_2(rel, &scan_key_info1, &scan_key_info2);
    if (tuples == NIL) {
        return NULL;
    }
    if (list_length(tuples) != 1) {
        list_free_deep(tuples);
        return NULL;
    }
    HeapTuple tuple = (HeapTuple)linitial(tuples);
    list_free_ext(tuples);
    return tuple;
}

/*
 * @brief search_from_pg_job_proc
 *
 * @param rel
 * @param name
 * @return HeapTuple
 */
static HeapTuple search_from_pg_job_proc(Relation rel, Datum name)
{
    HeapTuple tuple = search_from_pg_job_proc_no_exception(rel, name);
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("Can not find job with object name \'%s\'.", TextDatumGetCString(name)),
                        errdetail("N/A"), errcause("attribute does not exist"), erraction("Please check object name")));
    }
    return tuple;
}

/*
 * @brief lookup_pg_job_proc
 *
 * @param name
 * @param job_id
 * @param job_action
 */
void lookup_pg_job_proc(Datum name, Datum *job_id, Datum *job_action)
{
    bool isnull = false;
    Relation rel = heap_open(PgJobProcRelationId, AccessShareLock);
    HeapTuple tuple = search_from_pg_job_proc(rel, name);

    /* integer job id */
    if (job_id != NULL) {
        *job_id = heap_getattr(tuple, Anum_pg_job_proc_job_id, rel->rd_att, &isnull);
        if (isnull) {
            *job_id = 0;
        }
    }

    if (job_action != NULL) {
        /* text: job action */
        Datum job_action_src = heap_getattr(tuple, Anum_pg_job_proc_what, rel->rd_att, &isnull);
        if (isnull) {
            *job_action = Datum(0);
        } else {
            *job_action = PointerGetDatum(PG_DETOAST_DATUM_COPY(job_action_src));
        }
    }
    heap_freetuple_ext(tuple);

    heap_close(rel, AccessShareLock);
}

/*
 * @brief dbe_update_pg_job_proc
 *
 * @param job_action
 * @param job_name
 */
void dbe_update_pg_job_proc(Datum job_action, const Datum job_name)
{
    Datum values[Natts_pg_job_proc];
    bool nulls[Natts_pg_job_proc];

    bool replaces[Natts_pg_job_proc];
    (void)memset_s(replaces, sizeof(replaces), 0, sizeof(replaces));

    Relation rel = heap_open(PgJobProcRelationId, RowExclusiveLock);

    replaces[Anum_pg_job_proc_what - 1] = true;
    values[Anum_pg_job_proc_what - 1] = job_action;
    nulls[Anum_pg_job_proc_what - 1] = false;

    HeapTuple oldtuple = search_from_pg_job_proc(rel, job_name);

    HeapTuple newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(rel), values, nulls, replaces);
    simple_heap_update(rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(rel, newtuple);

    heap_close(rel, RowExclusiveLock);
    heap_freetuple_ext(oldtuple);
    heap_freetuple_ext(newtuple);
}
