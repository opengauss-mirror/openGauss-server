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
 * cbmfuncs.cpp
 *	  Support Functions for cbm tracking
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/cbmfuncs.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cbmparsexlog.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace.h"
#include "funcapi.h"
#include "port.h"
#include "postmaster/cbmwriter.h"
#include "postmaster/postmaster.h"
#include "storage/copydir.h"
#include "storage/smgr/fd.h"
#include "storage/lock/lwlock.h"
#include "storage/smgr/segment.h"
#include "utils/builtins.h"

extern void validate_xlog_location(char *str);
static void validate_start_end_lsn(char *start_lsn_str, char *end_lsn_str, XLogRecPtr *start_lsn, XLogRecPtr *end_lsn);
static void validate_get_lsn(char *lsn_str, XLogRecPtr *lsn_ptr);

/*
 * Report the end LSN position of already tracked xlog by CBM
 */
Datum pg_cbm_tracked_location(PG_FUNCTION_ARGS)
{
    XLogRecPtr recptr;
    char location[MAXFNAMELEN];
    int rc;

    recptr = GetCBMTrackedLSN();
    if (recptr == 0)
        PG_RETURN_NULL();

    rc = snprintf_s(location, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(recptr >> 32), (uint32)recptr);
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(location));
}

/*
 * rotate cbm file when build
 */
Datum pg_cbm_rotate_file(PG_FUNCTION_ARGS)
{
    text *lsn_arg = PG_GETARG_TEXT_P(0);
    char *lsn_str = text_to_cstring(lsn_arg);
    XLogRecPtr recptr;
    validate_get_lsn(lsn_str, &recptr);
    cbm_rotate_file(recptr);
    PG_RETURN_NULL();
}

Datum pg_cbm_get_merged_file(PG_FUNCTION_ARGS)
{
    text *start_lsn_arg = PG_GETARG_TEXT_P(0);
    text *end_lsn_arg = PG_GETARG_TEXT_P(1);
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        (errmsg("Must be system admin or operator admin in operation mode to get cbm merged file."))));

    /* At present, we only allow merging CBM files on master */
    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("pg_cbm_get_merged_file() cannot be executed during recovery.")));

    if (SSIsServerModeReadOnly()) {
        ereport(ERROR, (errmsg("pg_cbm_get_merged_file() cannot be executed at Standby with DMS enabled")));
    }

    char *start_lsn_str = text_to_cstring(start_lsn_arg);
    char *end_lsn_str = text_to_cstring(end_lsn_arg);
    char merged_file_name[MAXPGPATH] = {'\0'};
    XLogRecPtr start_lsn, end_lsn;

    validate_start_end_lsn(start_lsn_str, end_lsn_str, &start_lsn, &end_lsn);

    pfree(start_lsn_str);
    pfree(end_lsn_str);

    /* quick return if start lsn and end lsn equals */
    if (XLByteEQ(start_lsn, end_lsn)) {
        ereport(WARNING, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("Start lsn equals end lsn, nothing to merge.")));
        PG_RETURN_NULL();
    }

    (void)LWLockAcquire(CBMParseXlogLock, LW_SHARED);

    CBMGetMergedFile(start_lsn, end_lsn, merged_file_name);

    LWLockRelease(CBMParseXlogLock);

    PG_RETURN_TEXT_P(cstring_to_text(merged_file_name));
}

/*
 * Normally, we return one row for each changed tblspc/db/rel/fork.
 * However, since string length for one blocknumber output can be as long as MAX_STRLEN_PER_BLOCKNO,
 * considering additional one space and one comma, we may return multiple rows
 * if total changed block number is above MAX_BLOCKNO_PER_TUPLE.
 * In latter scenario, drop/create/truncate information should be returned in the first row, which may
 * need additional order by clause by user.
 */
Datum pg_cbm_get_changed_block(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(13, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "merged_start_lsn", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "merged_end_lsn", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "tablespace_oid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "database_oid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "relfilenode", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "fork_number", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "path", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "rel_dropped", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "rel_created", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)10, "rel_truncated", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)11, "truncate_blocknum", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)12, "changed_block_number", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)13, "changed_block_list", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        text *start_lsn_arg = PG_GETARG_TEXT_P(0);
        text *end_lsn_arg = PG_GETARG_TEXT_P(1);

#ifdef ENABLE_MULTIPLE_NODES
        /* At present, we only allow merging CBM files on master */
        if (RecoveryInProgress())
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                            errhint("pg_cbm_get_changed_block() cannot be executed"
                                    "during recovery.")));
#endif

        char *start_lsn_str = text_to_cstring(start_lsn_arg);
        char *end_lsn_str = text_to_cstring(end_lsn_arg);
        XLogRecPtr start_lsn, end_lsn;
        CBMArray *cbm_array = NULL;

        validate_start_end_lsn(start_lsn_str, end_lsn_str, &start_lsn, &end_lsn);

        pfree(start_lsn_str);
        pfree(end_lsn_str);

        /* quick return if start lsn and end lsn equals */
        if (XLByteEQ(start_lsn, end_lsn)) {
            SRF_RETURN_DONE(funcctx);
        }

        (void)LWLockAcquire(CBMParseXlogLock, LW_SHARED);

        cbm_array = CBMGetMergedArray(start_lsn, end_lsn);
        funcctx->user_fctx = (void *)SplitCBMArray(&cbm_array);

        LWLockRelease(CBMParseXlogLock);

        funcctx->max_calls = ((CBMArray *)funcctx->user_fctx)->arrayLength;

        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[13];
        bool nulls[13];
        CBMArray *cbm_array = (CBMArray *)funcctx->user_fctx;
        CBMArrayEntry cur_array_entry = (cbm_array->arrayEntry)[funcctx->call_cntr];
        HeapTuple tuple;
        char start_lsn_str[MAXFNAMELEN];
        char end_lsn_str[MAXFNAMELEN];
        char *changed_block_str = NULL;
        uint64 max_block_str_length, cur_block_str_length;
        uint32 i;
        int rc;

        rc = snprintf_s(start_lsn_str, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(cbm_array->startLSN >> 32),
                        (uint32)cbm_array->startLSN);
        securec_check_ss(rc, "\0", "\0");

        rc = snprintf_s(end_lsn_str, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(cbm_array->endLSN >> 32),
                        (uint32)(cbm_array->endLSN));
        securec_check_ss(rc, "\0", "\0");

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Values available to all callers */
        values[0] = CStringGetTextDatum(start_lsn_str);
        values[1] = CStringGetTextDatum(end_lsn_str);
        values[2] = ObjectIdGetDatum(cur_array_entry.cbmTag.rNode.spcNode);
        values[3] = ObjectIdGetDatum(cur_array_entry.cbmTag.rNode.dbNode);
        values[4] = ObjectIdGetDatum(cur_array_entry.cbmTag.rNode.relNode);
        values[5] = Int32GetDatum(cur_array_entry.cbmTag.forkNum);

        Assert(cur_array_entry.cbmTag.rNode.spcNode != InvalidOid);

        if (IS_UNDO_RELFILENODE(cur_array_entry.cbmTag.rNode)) {
            /* undo file and transaction slot */
            char file_path[UNDO_FILE_PATH_LEN] = {'\0'};
            char dir_path[UNDO_FILE_DIR_LEN] = {'\0'};
            DECLARE_NODE_COUNT();
            GET_UPERSISTENCE_BY_ZONEID((int)cur_array_entry.cbmTag.rNode.relNode, nodeCount);
            GetUndoFileDirectory(dir_path, UNDO_FILE_DIR_LEN, upersistence);
            if (cur_array_entry.cbmTag.rNode.dbNode == UNDO_DB_OID) {
                rc = snprintf_s(file_path, UNDO_FILE_PATH_LEN, UNDO_FILE_PATH_LEN - 1,
                        "%s/%05X", dir_path, cur_array_entry.cbmTag.rNode.relNode);
            } else {
                rc = snprintf_s(file_path, UNDO_FILE_PATH_LEN, UNDO_FILE_PATH_LEN - 1,
                        "%s/%05X.meta", dir_path, cur_array_entry.cbmTag.rNode.relNode);
            }
            securec_check_ss(rc, "\0", "\0");
            values[6] = CStringGetTextDatum(file_path);
        } else if (IsValidColForkNum(cur_array_entry.cbmTag.forkNum)) {
            /* column store change */
            char file_path[MAXPGPATH] = {'\0'};
            CFileNode cfile_node(cur_array_entry.cbmTag.rNode, ColForkNum2ColumnId(cur_array_entry.cbmTag.forkNum),
                                 MAIN_FORKNUM);
            CUStorage cu_storage(cfile_node);
            cu_storage.GetFileName(file_path, MAXPGPATH, 0);
            file_path[strlen(file_path) - 2] = '\0';
            values[6] = CStringGetTextDatum(file_path);
            cu_storage.Destroy();
        } else if (cur_array_entry.cbmTag.rNode.relNode != InvalidOid) {
            /* row store file change */
            Assert(cur_array_entry.cbmTag.forkNum <= MAX_FORKNUM);
            char *file_path = relpathperm(cur_array_entry.cbmTag.rNode, cur_array_entry.cbmTag.forkNum);
            values[6] = CStringGetTextDatum(file_path);
            pfree(file_path);
        } else if (cur_array_entry.cbmTag.rNode.dbNode != InvalidOid) {
            /* database create/drop and non-shared relmap change */
            char *db_path = GetDatabasePath(cur_array_entry.cbmTag.rNode.dbNode, cur_array_entry.cbmTag.rNode.spcNode);
            values[6] = CStringGetTextDatum(db_path);
            pfree(db_path);
        } else if (cur_array_entry.cbmTag.rNode.spcNode == GLOBALTABLESPACE_OID) {
            /* shared relmap change */
            Assert(cur_array_entry.changeType & PAGETYPE_TRUNCATE);

            char *db_path = GetDatabasePath(cur_array_entry.cbmTag.rNode.dbNode, cur_array_entry.cbmTag.rNode.spcNode);
            values[6] = CStringGetTextDatum(db_path);
            pfree(db_path);
        } else {
            /* tablespace create/drop */
            int len = 0;
            if (ENABLE_DSS) {
                len = strlen("pg_tblspc") + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 2;
            } else {
                len = strlen("pg_tblspc") + 1 + OIDCHARS + 1 + strlen(g_instance.attr.attr_common.PGXCNodeName) +
                    1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 2;
            }
            char *tblspc_path = (char *)palloc(len);
            if (ENABLE_DSS) {
                rc = snprintf_s(tblspc_path, len, len - 1, "pg_tblspc/%u/%s", cur_array_entry.cbmTag.rNode.spcNode,
                    TABLESPACE_VERSION_DIRECTORY);
                securec_check_ss(rc, "\0", "\0");
            } else {
                rc = snprintf_s(tblspc_path, len, len - 1, "pg_tblspc/%u/%s_%s", cur_array_entry.cbmTag.rNode.spcNode,
                    TABLESPACE_VERSION_DIRECTORY, g_instance.attr.attr_common.PGXCNodeName);
                securec_check_ss(rc, "\0", "\0");
            }
            values[6] = CStringGetTextDatum(tblspc_path);
            pfree(tblspc_path);
        }

        values[7] = BoolGetDatum(cur_array_entry.changeType & PAGETYPE_DROP);
        values[8] = BoolGetDatum(cur_array_entry.changeType & PAGETYPE_CREATE);
        values[9] = BoolGetDatum(cur_array_entry.changeType & PAGETYPE_TRUNCATE);

        nulls[10] = true;
        if (BlockNumberIsValid(cur_array_entry.truncBlockNum)) {
            values[10] = ObjectIdGetDatum(cur_array_entry.truncBlockNum);
            nulls[10] = false;
        }

        values[11] = ObjectIdGetDatum(cur_array_entry.totalBlockNum);

        nulls[12] = true;
        if (cur_array_entry.totalBlockNum > 0) {
            max_block_str_length = MAX_STRLEN_PER_BLOCKNO * cur_array_entry.totalBlockNum + 1;
            changed_block_str = (char *)palloc_extended(max_block_str_length * sizeof(char),
                                                        MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
            if (changed_block_str == NULL)
                ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                                errmsg("memory is temporarily unavailable while allocate block string")));

            cur_block_str_length = 0;
            for (i = 0; i < cur_array_entry.totalBlockNum; i++) {
                rc = snprintf_s(changed_block_str + cur_block_str_length, max_block_str_length - cur_block_str_length,
                                max_block_str_length - cur_block_str_length - 1, "%u, ",
                                cur_array_entry.changedBlock[i]);
                securec_check_ss(rc, "\0", "\0");

                cur_block_str_length += rc;
            }

            Assert(cur_block_str_length <= max_block_str_length);

            changed_block_str[cur_block_str_length - 2] = '\0';
            values[12] = CStringGetTextDatum(changed_block_str);

            pfree(changed_block_str);
            nulls[12] = false;
        }

        pfree_ext(cur_array_entry.changedBlock);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        if (funcctx->max_calls > 0)
            pfree(((CBMArray *)funcctx->user_fctx)->arrayEntry);

        pfree(funcctx->user_fctx);
        /* nothing left */
        SRF_RETURN_DONE(funcctx);
    }
}

Datum pg_cbm_recycle_file(PG_FUNCTION_ARGS)
{
    text *target_lsn_arg = PG_GETARG_TEXT_P(0);
    char *target_lsn_str = text_to_cstring(target_lsn_arg);
    XLogRecPtr target_lsn, end_lsn;
    char end_lsn_str[MAXFNAMELEN];
    int rc;

    validate_get_lsn(target_lsn_str, &target_lsn);

    pfree(target_lsn_str);

    (void)LWLockAcquire(CBMParseXlogLock, LW_EXCLUSIVE);

    CBMRecycleFile(target_lsn, &end_lsn);

    LWLockRelease(CBMParseXlogLock);

    rc = snprintf_s(end_lsn_str, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(end_lsn >> 32), (uint32)end_lsn);
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(end_lsn_str));
}

Datum pg_cbm_force_track(PG_FUNCTION_ARGS)
{
    text *target_lsn_arg = PG_GETARG_TEXT_P(0);
    int time_out = PG_GETARG_INT32(1);

    /* At present, we only allow force track CBM files on master */
    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("pg_cbm_force_track() cannot be executed during recovery.")));

    if (SSIsServerModeReadOnly()) {
        ereport(ERROR, (errmsg("pg_cbm_force_track() cannot be executed at Standby with DMS enabled")));
    }

    if (time_out < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Negative timeout for force track cbm!")));

    if (!IsCBMWriterRunning())
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("CBM writer thread is not running!")));

    char *target_lsn_str = text_to_cstring(target_lsn_arg);
    XLogRecPtr target_lsn, end_lsn;
    char end_lsn_str[MAXFNAMELEN];
    int rc;

    validate_get_lsn(target_lsn_str, &target_lsn);

    pfree(target_lsn_str);

    end_lsn = ForceTrackCBMOnce(target_lsn, time_out, time_out > 0, false);
    if (XLogRecPtrIsInvalid(end_lsn))
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_TIMED_OUT), errmsg("Timeout happened during force track cbm!")));

    rc = snprintf_s(end_lsn_str, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(end_lsn >> 32), (uint32)end_lsn);
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(end_lsn_str));
}

static void validate_start_end_lsn(char *start_lsn_str, char *end_lsn_str, XLogRecPtr *start_lsn, XLogRecPtr *end_lsn)
{
    XLogRecPtr cbm_tracked_lsn;

    validate_get_lsn(start_lsn_str, start_lsn);
    validate_get_lsn(end_lsn_str, end_lsn);

    if (XLByteLT(*end_lsn, *start_lsn))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("start xlog location %X/%X should be smaller than or equal to end xlog location %X/%X",
                (uint32)(*start_lsn >> 32), (uint32)(*start_lsn), (uint32)(*end_lsn >> 32), (uint32)(*end_lsn))));

    cbm_tracked_lsn = GetCBMTrackedLSN();
    if (XLByteLT(cbm_tracked_lsn, *end_lsn))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("end xlog location %X/%X should be smaller than or equal to "
                               "already tracked xlog location %X/%X",
                               (uint32)(*end_lsn >> 32), (uint32)(*end_lsn), (uint32)(cbm_tracked_lsn >> 32),
                               (uint32)cbm_tracked_lsn)));
}

static void validate_get_lsn(char *lsn_str, XLogRecPtr *lsn_ptr)
{
    uint32 hi = 0;
    uint32 lo = 0;
    validate_xlog_location(lsn_str);

    if (sscanf_s(lsn_str, "%X/%X", &hi, &lo) != 2)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("could not parse xlog location \"%s\"", lsn_str)));
    *lsn_ptr = (((uint64)hi) << 32) | lo;
}
