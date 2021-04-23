/* -------------------------------------------------------------------------
 *
 * xlogfuncs.cpp
 *
 * PostgreSQL transaction log manager user interface functions
 *
 * This file contains WAL control and information functions.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/xlogfuncs.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xlog.h"
#include "access/obs/obs_am.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "replication/obswalreceiver.h"
#include "replication/walreceiver.h"
#include "replication/walsender_private.h"
#include "replication/slot.h"
#include "replication/syncrep.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"

extern void validate_xlog_location(char *str);

static inline void pg_check_xlog_func_permission()
{
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg("Must be system admin or operator admin in operation mode to call this xlog function."))));
    }
    if (RecoveryInProgress()) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("This xlog function cannot be executed during recovery.")));
    }
}

/*
 * pg_start_backup: set up for taking an on-line backup dump
 *
 * Essentially what this does is to create a backup label file in $PGDATA,
 * where it will be archived as part of the backup dump.  The label file
 * contains the user-supplied label string (typically this would be used
 * to tell where the backup dump will be stored) and the starting time and
 * starting WAL location for the dump.
 */
Datum pg_start_backup(PG_FUNCTION_ARGS)
{
    text *backupid = PG_GETARG_TEXT_P(0);
    bool fast = PG_GETARG_BOOL(1);
    char *backupidstr = NULL;
    XLogRecPtr startpoint;
    DIR *dir;
    char startxlogstr[MAXFNAMELEN];
    errno_t errorno = EOK;

    SessionBackupState status = u_sess->proc_cxt.sessionBackupState;

    if (status == SESSION_BACKUP_NON_EXCLUSIVE)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("a non-exclusive backup is already in progress in this session")));

    backupidstr = text_to_cstring(backupid);
    dir = AllocateDir("pg_tblspc");
    if (!dir) {
        ereport(ERROR, (errmsg("could not open directory \"%s\": %m", "pg_tblspc")));
    }

    if (strncmp(backupidstr, "gs_roach", strlen("gs_roach")) == 0) {
        startpoint = do_roach_start_backup(backupidstr);
    } else {
	startpoint = do_pg_start_backup(backupidstr, fast, NULL, dir, NULL, NULL, false, true);
        RegisterAbortExclusiveBackup();
    }

    errorno = snprintf_s(startxlogstr, sizeof(startxlogstr), sizeof(startxlogstr) - 1, "%X/%X",
                         (uint32)(startpoint >> 32), (uint32)startpoint);
    securec_check_ss(errorno, "", "");

    PG_RETURN_TEXT_P(cstring_to_text(startxlogstr));
}

/*
 * pg_stop_backup: finish taking an on-line backup dump
 *
 * We write an end-of-backup WAL record, and remove the backup label file
 * created by pg_start_backup, creating a backup history file in pg_xlog
 * instead (whence it will immediately be archived). The backup history file
 * contains the same info found in the label file, plus the backup-end time
 * and WAL location. Before 9.0, the backup-end time was read from the backup
 * history file at the beginning of archive recovery, but we now use the WAL
 * record for that and the file is for informational and debug purposes only.
 *
 * Note: different from CancelBackup which just cancels online backup mode.
 */
Datum pg_stop_backup(PG_FUNCTION_ARGS)
{
    XLogRecPtr stoppoint;
    char stopxlogstr[MAXFNAMELEN];
    errno_t errorno = EOK;

    SessionBackupState status = u_sess->proc_cxt.sessionBackupState;

    if (status == SESSION_BACKUP_NON_EXCLUSIVE)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("a non-exclusive backup is already in progress in this session")));

    /* when delay xlog recycle is true, we do not copy xlog from archive */
    stoppoint = do_pg_stop_backup(NULL, !GetDelayXlogRecycle());

    errorno = snprintf_s(stopxlogstr, sizeof(stopxlogstr), sizeof(stopxlogstr) - 1, "%X/%X", (uint32)(stoppoint >> 32),
                         (uint32)stoppoint);
    securec_check_ss(errorno, "", "");

    PG_RETURN_TEXT_P(cstring_to_text(stopxlogstr));
}

/*
 * pg_start_backup_v2: set up for taking an on-line backup dump
 *
 */
Datum pg_start_backup_v2(PG_FUNCTION_ARGS)
{
    text* backupid = PG_GETARG_TEXT_P(0);
    bool fast = PG_GETARG_BOOL(1);
    bool  exclusive = PG_GETARG_BOOL(2);
    char* backupidstr = NULL;
    char* labelfile = NULL;
    char* tblspcmapfile = NULL;
    XLogRecPtr startpoint;
    DIR *dir;
    char startxlogstr[MAXFNAMELEN];
    errno_t errorno = EOK;
    MemoryContext oldContext;

    u_sess->probackup_context = AllocSetContextCreate(u_sess->top_mem_cxt, "probackup context",
                                                      ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                      ALLOCSET_DEFAULT_MAXSIZE);
    oldContext = MemoryContextSwitchTo(u_sess->probackup_context);
    
    SessionBackupState status = u_sess->proc_cxt.sessionBackupState;

    if (status == SESSION_BACKUP_NON_EXCLUSIVE)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("a non-exclusive backup is already in progress in this session")));

    backupidstr = text_to_cstring(backupid);
    dir = AllocateDir("pg_tblspc");
    if (!dir) {
        ereport(ERROR, (errmsg("could not open directory \"%s\": %m", "pg_tblspc")));
    }

    if (exclusive) {
        startpoint = do_pg_start_backup(backupidstr, fast, NULL, dir, NULL, NULL, false, true);
    } else {
        if (status == SESSION_BACKUP_EXCLUSIVE)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("a  backup is already in progress in this session")));

        startpoint = do_pg_start_backup(backupidstr, fast, &labelfile,dir, &tblspcmapfile, NULL,false,true);
        u_sess->proc_cxt.LabelFile = MemoryContextStrdup(u_sess->probackup_context, labelfile);
        if (tblspcmapfile != NULL) {
            u_sess->proc_cxt.TblspcMapFile = MemoryContextStrdup(u_sess->probackup_context, tblspcmapfile);
        } else {
            u_sess->proc_cxt.TblspcMapFile = NULL;
        }
    }

    errorno = snprintf_s(startxlogstr, sizeof(startxlogstr), sizeof(startxlogstr) - 1, "%X/%X",
                         (uint32)(startpoint >> 32), (uint32)startpoint);
    securec_check_ss(errorno, "", "");

    PG_RETURN_TEXT_P(cstring_to_text(startxlogstr));

    MemoryContextSwitchTo(oldContext);
}

/*
 * pg_stop_backup_v2: finish taking an on-line backup dump
 *
 */
Datum pg_stop_backup_v2(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc    tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext perqueryctx, oldcontext, oldcontext2;
    Datum        values[3];
    bool         nulls[3];
    XLogRecPtr stoppoint;
    char stopxlogstr[MAXFNAMELEN];
    errno_t errorno = EOK;
    bool  exclusive = PG_GETARG_BOOL(0);
    errno_t rc;

    SessionBackupState status = u_sess->proc_cxt.sessionBackupState;

    oldcontext2 = MemoryContextSwitchTo(u_sess->probackup_context);

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    perqueryctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(perqueryctx);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    if (exclusive) {
        if (status == SESSION_BACKUP_NON_EXCLUSIVE)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("non-exclusive backup in progress"), errhint("Did you mean to use pg_stop_backup('f')?")));
        /* when delay xlog recycle is true, we do not copy xlog from archive */
        stoppoint = do_pg_stop_backup(NULL, !GetDelayXlogRecycle());
        nulls[1] = true;
        nulls[2] = true;
    } else {
        if (status != SESSION_BACKUP_NON_EXCLUSIVE)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("non-exclusive backup is not in progress")));

        stoppoint = do_pg_stop_backup(u_sess->proc_cxt.LabelFile, !GetDelayXlogRecycle());

        values[1] = CStringGetTextDatum(u_sess->proc_cxt.LabelFile);
        pfree(u_sess->proc_cxt.LabelFile);
        u_sess->proc_cxt.LabelFile = NULL;

        if (u_sess->proc_cxt.TblspcMapFile) {
            values[2] = CStringGetTextDatum(u_sess->proc_cxt.TblspcMapFile);
            pfree(u_sess->proc_cxt.TblspcMapFile);
            u_sess->proc_cxt.TblspcMapFile = NULL;
        } else {
            nulls[2] = true;
        }
    }

    errorno = snprintf_s(stopxlogstr, sizeof(stopxlogstr), sizeof(stopxlogstr) - 1, "%X/%X",
                         (uint32)(stoppoint >> 32), (uint32)stoppoint);
    securec_check_ss(errorno, "", "");
    values[0] = CStringGetTextDatum(stopxlogstr);

    tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    tuplestore_donestoring(tupstore);

    MemoryContextSwitchTo(oldcontext2);

    return (Datum) 0;
}

/*
 * gs_roach_stop_backup: stop roach backup with passed-in backup name
 *
 * This is a simplified version of pg_stop_backup, because gs_roach will take care
 * of other relevant issues, e.g. xlog backup consistency.
 */
Datum gs_roach_stop_backup(PG_FUNCTION_ARGS)
{
    text *backupid = PG_GETARG_TEXT_P(0);
    char *backupidstr = NULL;
    XLogRecPtr stoppoint;
    char stopxlogstr[MAXFNAMELEN];
    errno_t errorno = EOK;

    backupidstr = text_to_cstring(backupid);

    stoppoint = do_roach_stop_backup(backupidstr);

    errorno = snprintf_s(stopxlogstr, sizeof(stopxlogstr), sizeof(stopxlogstr) - 1, "%X/%X", (uint32)(stoppoint >> 32),
                         (uint32)stoppoint);
    securec_check_ss(errorno, "", "");

    PG_RETURN_TEXT_P(cstring_to_text(stopxlogstr));
}

/*
 * pg_switch_xlog: switch to next xlog file
 */
Datum pg_switch_xlog(PG_FUNCTION_ARGS)
{
    XLogRecPtr switchpoint;
    char location[MAXFNAMELEN];
    errno_t errorno = EOK;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg(
                     "Must be system admin or operator admin in operation mode to switch transaction log files."))));

    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("WAL control functions cannot be executed during recovery.")));

    switchpoint = RequestXLogSwitch();
    RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_IMMEDIATE);

    /*
     * As a convenience, return the WAL location of the switch record
     */
    errorno = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(switchpoint >> 32),
                         (uint32)switchpoint);
    securec_check_ss(errorno, "", "");
    PG_RETURN_TEXT_P(cstring_to_text(location));
}

/*
 * gs_roach_switch_xlog: switch to next xlog file, create checkpoint according to input parameter
 */
Datum gs_roach_switch_xlog(PG_FUNCTION_ARGS)
{
    bool request_ckpt = PG_GETARG_BOOL(0);

    XLogRecPtr switchpoint;
    char location[MAXFNAMELEN];
    errno_t errorno = EOK;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg(
                     "Must be system admin or operator admin in operation mode to switch transaction log files."))));

    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("WAL control functions cannot be executed during recovery.")));

    switchpoint = RequestXLogSwitch();

    if (request_ckpt) {
        RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_IMMEDIATE);
    }

    /*
     * As a convenience, return the WAL location of the switch record
     */
    errorno = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(switchpoint >> 32),
                         (uint32)switchpoint);
    securec_check_ss(errorno, "", "");
    PG_RETURN_TEXT_P(cstring_to_text(location));
}


/*
 * pg_create_restore_point: a named point for restore
 */
Datum pg_create_restore_point(PG_FUNCTION_ARGS)
{
    text *restore_name = PG_GETARG_TEXT_P(0);
    char *restore_name_str = NULL;
    XLogRecPtr restorepoint;
    char location[MAXFNAMELEN];
    errno_t errorno = EOK;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg("Must be system admin or operator admin in operation mode to create a restore point."))));

    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        (errmsg("recovery is in progress"),
                         errhint("WAL control functions cannot be executed during recovery."))));

    if (!XLogIsNeeded())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("WAL level not sufficient for creating a restore point"),
                        errhint("wal_level must be set to \"archive\" or \"hot_standby\" at server start.")));

    restore_name_str = text_to_cstring(restore_name);
    if (strlen(restore_name_str) >= MAXFNAMELEN)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("value too long for restore point (maximum %d characters)", MAXFNAMELEN - 1)));

    restorepoint = XLogRestorePoint(restore_name_str);

    /*
     * As a convenience, return the WAL location of the restore point record
     */
    errorno = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(restorepoint >> 32),
                         (uint32)restorepoint);
    securec_check_ss(errorno, "", "");
    PG_RETURN_TEXT_P(cstring_to_text(location));
}

/*
 * Report the current WAL write location (same format as pg_start_backup etc)
 *
 * This is useful for determining how much of WAL is visible to an external
 * archiving process.  Note that the data before this point is written out
 * to the kernel, but is not necessarily synced to disk.
 */
Datum pg_current_xlog_location(PG_FUNCTION_ARGS)
{
    XLogRecPtr current_recptr;
    char location[MAXFNAMELEN];
    errno_t errorno = EOK;

    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("WAL control functions cannot be executed during recovery.")));

    current_recptr = GetXLogWriteRecPtr();

    errorno = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(current_recptr >> 32),
                         (uint32)current_recptr);
    securec_check_ss(errorno, "", "");
    PG_RETURN_TEXT_P(cstring_to_text(location));
}

/*
 * Report the current WAL insert location (same format as pg_start_backup etc)
 *
 * This function is mostly for debugging purposes.
 */
Datum pg_current_xlog_insert_location(PG_FUNCTION_ARGS)
{
    XLogRecPtr current_recptr;
    char location[MAXFNAMELEN];
    errno_t errorno = EOK;

    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("WAL control functions cannot be executed during recovery.")));

    current_recptr = GetXLogInsertRecPtr();

    errorno = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(current_recptr >> 32),
                         (uint32)current_recptr);
    securec_check_ss(errorno, "", "");
    PG_RETURN_TEXT_P(cstring_to_text(location));
}

/*
 * Report the last WAL receive location (same format as pg_start_backup etc)
 *
 * This is useful for determining how much of WAL is guaranteed to be received
 * and synced to disk by walreceiver.
 */
Datum pg_last_xlog_receive_location(PG_FUNCTION_ARGS)
{
    XLogRecPtr recptr;
    char location[MAXFNAMELEN];
    errno_t errorno = EOK;

    recptr = GetWalRcvWriteRecPtr(NULL);
    if (recptr == 0)
        PG_RETURN_NULL();

    errorno = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(recptr >> 32),
                         (uint32)recptr);
    securec_check_ss(errorno, "", "");
    PG_RETURN_TEXT_P(cstring_to_text(location));
}

/*
 * Report the last WAL replay location (same format as pg_start_backup etc)
 *
 * This is useful for determining how much of WAL is visible to read-only
 * connections during recovery.
 */
Datum pg_last_xlog_replay_location(PG_FUNCTION_ARGS)
{
    XLogRecPtr recptr;
    char location[MAXFNAMELEN];

    char term_text[MAXFNAMELEN];
    errno_t errorno = EOK;
    TupleDesc tupdesc;

    /* insert tuple have 2 item */
    Datum values[2];
    bool nulls[2];
    Datum result;
    HeapTuple tuple;
    uint32 Term = g_InvalidTermDN;
    size_t cnt = 0;
    FILE *fp = NULL;
    if ((fp = fopen("term_file", "r")) != NULL) {
        cnt = fread(&Term, sizeof(uint32), 1, fp);
        (void)fclose(fp);
        fp = NULL;

        if (cnt != 1) {
            ereport(LOG, (0, errmsg("cannot read term file:  \n")));
        }
    }

    if (Term > g_instance.comm_cxt.localinfo_cxt.term_from_file &&
        t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE) {
        g_instance.comm_cxt.localinfo_cxt.term_from_file = Term;
    }
    uint32 max_term = Max(g_instance.comm_cxt.localinfo_cxt.term_from_xlog,
                          g_instance.comm_cxt.localinfo_cxt.term_from_file);

    recptr = GetXLogReplayRecPtrInPending();
    if (recptr == 0) {
        values[0] = CStringGetTextDatum("0");
        nulls[0] = false;
        values[1] = CStringGetTextDatum("0/0");
        nulls[1] = false;
    } else {
        /* Build a tuple descriptor for our result type */
        errorno = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(recptr >> 32),
                             (uint32)recptr);
        securec_check_ss(errorno, "", "");

        errorno = snprintf_s(term_text, sizeof(term_text), sizeof(term_text) - 1, "%u", max_term);
        securec_check_ss(errorno, "", "");

        values[0] = CStringGetTextDatum(term_text);
        nulls[0] = false;
        values[1] = CStringGetTextDatum(location);
        nulls[1] = false;
    }
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));
    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

/*
 * Compute an xlog file name and decimal byte offset given a WAL location,
 * such as is returned by pg_stop_backup() or pg_xlog_switch().
 *
 * Note that a location exactly at a segment boundary is taken to be in
 * the previous segment.  This is usually the right thing, since the
 * expected usage is to determine which xlog file(s) are ready to archive.
 */
Datum pg_xlogfile_name_offset(PG_FUNCTION_ARGS)
{
    text *location = PG_GETARG_TEXT_P(0);
    char *locationstr = NULL;
    uint32 hi = 0;
    uint32 lo = 0;
    XLogSegNo xlogsegno;
    XLogRecPtr locationpoint;
    char xlogfilename[MAXFNAMELEN];

    /* insert tuple have 2 item */
    Datum values[2];
    bool isnull[2];
    TupleDesc resultTupleDesc;
    HeapTuple resultHeapTuple;
    Datum result;
    errno_t errorno = EOK;

    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("pg_xlogfile_name_offset() cannot be executed during recovery.")));

    /*
     * Read input and parse
     */
    locationstr = text_to_cstring(location);

    validate_xlog_location(locationstr);

    /* test input string */
    if (sscanf_s(locationstr, "%X/%X", &hi, &lo) != 2)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not parse transaction log location \"%s\"", locationstr)));

    locationpoint = (((uint64)hi) << 32) | lo;

    /*
     * Construct a tuple descriptor for the result row.  This must match this
     * function's pg_proc entry!
     */
    resultTupleDesc = CreateTemplateTupleDesc(2, false, TAM_HEAP);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)1, "file_name", TEXTOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)2, "file_offset", INT4OID, -1, 0);

    resultTupleDesc = BlessTupleDesc(resultTupleDesc);

    /*
     * xlogfilename
     */
    XLByteToPrevSeg(locationpoint, xlogsegno);
    errorno = snprintf_s(xlogfilename, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                         (uint32)((xlogsegno) / XLogSegmentsPerXLogId), (uint32)((xlogsegno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    values[0] = CStringGetTextDatum(xlogfilename);
    isnull[0] = false;

    /* offset */
    values[1] = UInt32GetDatum(locationpoint % XLogSegSize);
    isnull[1] = false;

    /*
     * Tuple jam: Having first prepared your Datums, then squash together
     */
    resultHeapTuple = heap_form_tuple(resultTupleDesc, values, isnull);

    result = HeapTupleGetDatum(resultHeapTuple);

    PG_RETURN_DATUM(result);
}

/*
 * Compute an xlog file name given a WAL location,
 * such as is returned by pg_stop_backup() or pg_xlog_switch().
 */
Datum pg_xlogfile_name(PG_FUNCTION_ARGS)
{
    text *location = PG_GETARG_TEXT_P(0);
    char *locationstr = NULL;
    uint32 hi = 0;
    uint32 lo = 0;
    XLogSegNo xlogsegno;
    XLogRecPtr locationpoint;
    char xlogfilename[MAXFNAMELEN];
    errno_t errorno = EOK;

    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("pg_xlogfile_name() cannot be executed during recovery.")));

    locationstr = text_to_cstring(location);

    validate_xlog_location(locationstr);

    /* test input string */
    if (sscanf_s(locationstr, "%X/%X", &hi, &lo) != 2)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not parse transaction log location \"%s\"", locationstr)));

    locationpoint = (((uint64)hi) << 32) | lo;

    XLByteToPrevSeg(locationpoint, xlogsegno);
    errorno = snprintf_s(xlogfilename, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                         (uint32)((xlogsegno) / XLogSegmentsPerXLogId), (uint32)((xlogsegno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    PG_RETURN_TEXT_P(cstring_to_text(xlogfilename));
}

/*
 * pg_xlog_replay_pause - pause recovery now
 */
Datum pg_xlog_replay_pause(PG_FUNCTION_ARGS)
{
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        (errmsg("Must be system admin or operator admin in operation mode to control recovery."))));

    if (!RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is not in progress"),
                        errhint("Recovery control functions can only be executed during recovery.")));

    SetRecoveryPause(true);

    PG_RETURN_VOID();
}

/*
 * pg_xlog_replay_resume - resume recovery now
 */
Datum pg_xlog_replay_resume(PG_FUNCTION_ARGS)
{
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        (errmsg("Must be system admin or operator admin in operation mode to control recovery."))));

    if (!RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is not in progress"),
                        errhint("Recovery control functions can only be executed during recovery.")));

    SetRecoveryPause(false);

    PG_RETURN_VOID();
}

/*
 * pg_is_xlog_replay_paused
 */
Datum pg_is_xlog_replay_paused(PG_FUNCTION_ARGS)
{
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        (errmsg("Must be system admin or operator admin in operation mode to control recovery."))));

    if (!RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is not in progress"),
                        errhint("Recovery control functions can only be executed during recovery.")));

    PG_RETURN_BOOL(RecoveryIsPaused());
}

/*
 * Returns timestamp of latest processed commit/abort record.
 *
 * When the server has been started normally without recovery the function
 * returns NULL.
 */
Datum pg_last_xact_replay_timestamp(PG_FUNCTION_ARGS)
{
    TimestampTz xtime;

    xtime = GetLatestXTime();
    if (xtime == 0)
        PG_RETURN_NULL();

    PG_RETURN_TIMESTAMPTZ(xtime);
}

/*
 * Returns bool with current recovery mode, a global state.
 */
Datum pg_is_in_recovery(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(RecoveryInProgress());
}

/*
 * Validate the text form of a transaction log location.
 * (Just using sscanf_s() input allows incorrect values such as
 * negatives, so we have to be a bit more careful about that).
 */
void validate_xlog_location(char *str)
{
#define MAXLSNCOMPONENT 8

    int len1, len2;

    len1 = (int)strspn(str, "0123456789abcdefABCDEF");
    if (len1 < 1 || len1 > MAXLSNCOMPONENT || str[len1] != '/')
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for transaction log location: \"%s\"", str)));

    len2 = (int)strspn(str + len1 + 1, "0123456789abcdefABCDEF");
    if (len2 < 1 || len2 > MAXLSNCOMPONENT || str[len1 + 1 + len2] != '\0')
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for transaction log location: \"%s\"", str)));
}

/*
 * Compute the difference in bytes between two WAL locations.
 */
Datum pg_xlog_location_diff(PG_FUNCTION_ARGS)
{
    text *location1 = PG_GETARG_TEXT_P(0);
    text *location2 = PG_GETARG_TEXT_P(1);
    char *str1, *str2;
    XLogRecPtr loc1, loc2;
    Numeric result;
    uint32 hi = 0;
    uint32 lo = 0;

    /*
     * Read and parse input
     */
    str1 = text_to_cstring(location1);
    str2 = text_to_cstring(location2);

    validate_xlog_location(str1);
    validate_xlog_location(str2);

    /* test input string */
    if (sscanf_s(str1, "%X/%X", &hi, &lo) != 2)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not parse transaction log location \"%s\"", str1)));
    loc1 = (((uint64)hi) << 32) | lo;
    if (sscanf_s(str2, "%X/%X", &hi, &lo) != 2)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not parse transaction log location \"%s\"", str2)));
    loc2 = (((uint64)hi) << 32) | lo;

    /*
     * result is computed as: recptr1 minus recptr2
     */
    result = DatumGetNumeric(DirectFunctionCall2(numeric_sub,
                                                 DirectFunctionCall1(int8_numeric, Int64GetDatum((int64)loc1)),
                                                 DirectFunctionCall1(int8_numeric, Int64GetDatum((int64)loc2))));

    PG_RETURN_NUMERIC(result);
}

Datum pg_enable_delay_xlog_recycle(PG_FUNCTION_ARGS)
{
    pg_check_xlog_func_permission();

    enable_delay_xlog_recycle();

    PG_RETURN_VOID();
}

Datum pg_disable_delay_xlog_recycle(PG_FUNCTION_ARGS)
{
    pg_check_xlog_func_permission();

    disable_delay_xlog_recycle();

    PG_RETURN_VOID();
}

Datum pg_enable_delay_ddl_recycle(PG_FUNCTION_ARGS)
{
    XLogRecPtr startLSN;
    char location[MAXFNAMELEN];
    errno_t rc = EOK;

    pg_check_xlog_func_permission();

    startLSN = enable_delay_ddl_recycle();

    rc = snprintf_s(location, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(startLSN >> 32), (uint32)(startLSN));
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(location));
}

Datum pg_disable_delay_ddl_recycle(PG_FUNCTION_ARGS)
{
    text *barrierLSNArg = PG_GETARG_TEXT_P(0);
    bool isForce = PG_GETARG_BOOL(1);
    XLogRecPtr barrierLSN = InvalidXLogRecPtr;
    char *barrierLSNStr = text_to_cstring(barrierLSNArg);
    XLogRecPtr startLSN;
    XLogRecPtr endLSN;
    char startLocation[MAXFNAMELEN];
    char endLocation[MAXFNAMELEN];

    /* insert tuple have 2 item */
    Datum values[2];
    bool isnull[2];
    TupleDesc resultTupleDesc;
    HeapTuple resultHeapTuple;
    Datum result;
    int rc;
    uint32 hi = 0;
    uint32 lo = 0;

    pg_check_xlog_func_permission();

    validate_xlog_location(barrierLSNStr);

    /* test input string */
    if (sscanf_s(barrierLSNStr, "%X/%X", &hi, &lo) != 2)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not parse xlog location \"%s\"", barrierLSNStr)));

    barrierLSN = (((uint64)hi) << 32) | lo;

    disable_delay_ddl_recycle(barrierLSN, isForce, &startLSN, &endLSN);

    rc = snprintf_s(startLocation, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(startLSN >> 32),
                    (uint32)(startLSN));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(endLocation, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(endLSN >> 32), (uint32)(endLSN));
    securec_check_ss(rc, "\0", "\0");

    /*
     * Construct a tuple descriptor for the result row.
     */
    resultTupleDesc = CreateTemplateTupleDesc(2, false, TAM_HEAP);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)1, "ddl_delay_start_lsn", TEXTOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)2, "ddl_delay_end_lsn", TEXTOID, -1, 0);

    resultTupleDesc = BlessTupleDesc(resultTupleDesc);

    values[0] = CStringGetTextDatum(startLocation);
    isnull[0] = false;
    values[1] = CStringGetTextDatum(endLocation);
    isnull[1] = false;

    resultHeapTuple = heap_form_tuple(resultTupleDesc, values, isnull);

    result = HeapTupleGetDatum(resultHeapTuple);

    PG_RETURN_DATUM(result);
}

Datum gs_roach_enable_delay_ddl_recycle(PG_FUNCTION_ARGS)
{
    Name name = PG_GETARG_NAME(0);
    XLogRecPtr start_lsn;
    char location[MAXFNAMELEN];
    errno_t rc = EOK;

    (void)ValidateName(NameStr(*name));

    pg_check_xlog_func_permission();

    start_lsn = enable_delay_ddl_recycle_with_slot(NameStr(*name));

    rc = snprintf_s(location, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(start_lsn >> 32), (uint32)(start_lsn));
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(location));
}

Datum gs_roach_disable_delay_ddl_recycle(PG_FUNCTION_ARGS)
{
    Name name = PG_GETARG_NAME(0);
    XLogRecPtr start_lsn;
    XLogRecPtr end_lsn;
    char start_location[MAXFNAMELEN];
    char end_location[MAXFNAMELEN];
    errno_t rc = EOK;
    /* insert tuple have 2 item */
    Datum values[2];
    bool isnull[2];
    TupleDesc resultTupleDesc;
    HeapTuple resultHeapTuple;
    Datum result;

    (void)ValidateName(NameStr(*name));

    pg_check_xlog_func_permission();

    disable_delay_ddl_recycle_with_slot(NameStr(*name), &start_lsn, &end_lsn);

    rc = snprintf_s(start_location, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X",
        (uint32)(start_lsn >> 32), (uint32)(start_lsn));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(end_location, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X",
        (uint32)(end_lsn >> 32), (uint32)(end_lsn));
    securec_check_ss(rc, "\0", "\0");

    /* Construct a tuple descriptor for the result row. */
    resultTupleDesc = CreateTemplateTupleDesc(2, false, TAM_HEAP);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)1, "ddl_delay_start_lsn", TEXTOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)2, "ddl_delay_end_lsn", TEXTOID, -1, 0);

    resultTupleDesc = BlessTupleDesc(resultTupleDesc);

    values[0] = CStringGetTextDatum(start_location);
    isnull[0] = false;
    values[1] = CStringGetTextDatum(end_location);
    isnull[1] = false;

    resultHeapTuple = heap_form_tuple(resultTupleDesc, values, isnull);

    result = HeapTupleGetDatum(resultHeapTuple);

    PG_RETURN_DATUM(result);
}

Datum pg_resume_bkp_flag(PG_FUNCTION_ARGS)
{
    Name name = PG_GETARG_NAME(0);
    bool startBackupFlag = false;
    bool toDelay = false;
    const XLogRecPtr MAX_XLOG_REC_PTR = (XLogRecPtr)0xFFFFFFFFFFFFFFFF;
    XLogRecPtr delay_xlog_lsn = MAX_XLOG_REC_PTR;
    XLogRecPtr delay_ddl_lsn = InvalidXLogRecPtr;
    char location[MAXFNAMELEN];
    errno_t rc = EOK;
    int i;
    ReplicationSlot *slot = NULL;
    Datum values[4];
    bool isnull[4];
    TupleDesc resultTupleDesc;
    HeapTuple resultHeapTuple;
    Datum result;
    char *rewindTime = NULL;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        (errmsg("Must be system admin or operator admin in operation mode to "
                                "control resume_bkp_flag function."))));

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];

        if (slot->in_use && strcmp(NameStr(*name), NameStr(slot->data.name)) == 0) {
            delay_xlog_lsn = slot->data.restart_lsn;
            delay_ddl_lsn = slot->data.confirmed_flush;
        }
    }
    LWLockRelease(ReplicationSlotControlLock);

    /* If all slots are in use, we're out of luck. */
    if (slot == NULL && strcmp(NameStr(*name), "gs_roach_check_rewind") != 0) {
        ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                       errmsg("could not find backup slot with name %s", NameStr(*name))));
    }

    startBackupFlag = check_roach_start_backup(NameStr(*name));
    toDelay = !XLByteEQ(delay_xlog_lsn, MAX_XLOG_REC_PTR);
    rc = snprintf_s(location, MAXFNAMELEN, MAXFNAMELEN - 1,
            "%08X/%08X", (uint32)(delay_ddl_lsn >> 32), (uint32)(delay_ddl_lsn));
    securec_check_ss(rc, "", "");
    rewindTime = getLastRewindTime();

    /*
     * Construct a tuple descriptor for the result row.
     */
    resultTupleDesc = CreateTemplateTupleDesc(4, false, TAM_HEAP);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)1, "start_backup_flag", BOOLOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)2, "to_delay", BOOLOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)3, "ddl_delay_recycle_ptr", TEXTOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)4, "rewind_time", TEXTOID, -1, 0);
    resultTupleDesc = BlessTupleDesc(resultTupleDesc);

    /* set value */
    values[0] = BoolGetDatum(startBackupFlag);
    isnull[0] = false;
    values[1] = BoolGetDatum(toDelay);
    isnull[1] = false;
    values[2] = CStringGetTextDatum(location);
    isnull[2] = false;
    values[3] = CStringGetTextDatum(rewindTime);
    isnull[3] = false;
    resultHeapTuple = heap_form_tuple(resultTupleDesc, values, isnull);
    if (0 != pg_strcasecmp(rewindTime, "")) {
        pfree(rewindTime);
        rewindTime = NULL;
    }

    result = HeapTupleGetDatum(resultHeapTuple);

    PG_RETURN_DATUM(result);
}

Datum pg_get_sync_flush_lsn(PG_FUNCTION_ARGS)
{
    XLogRecPtr receiveLsn = InvalidXLogRecPtr;
    XLogRecPtr writeLsn = InvalidXLogRecPtr;
    XLogRecPtr flushLsn = InvalidXLogRecPtr;
    XLogRecPtr replayLsn = InvalidXLogRecPtr;
    bool amSync = false;
    char location[MAXFNAMELEN];
    int rc = EOK;
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to pg_get_sync_flush_lsn."))));
    if (g_instance.attr.attr_storage.replication_type == RT_WITH_MULTI_STANDBY) {
        if (SyncRepGetSyncRecPtr(&receiveLsn, &writeLsn, &flushLsn, &replayLsn, &amSync, false) == false) {
            ereport(ERROR, ((errmsg("get sync flush lsn error"))));
        }
    } else {
        /* dummy standy only get the walsender flsu lsn, cause 1p1s deployment is force sync */
        for (int i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
            /* use volatile pointer to prevent code rearrangement */
            volatile WalSnd* walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];
            if (walsnd->pid == 0) {
                continue;
            }
            SpinLockAcquire(&walsnd->mutex);
            flushLsn = walsnd->flush;
            SpinLockRelease(&walsnd->mutex);
            break;
        }
    }
    rc = snprintf_s(location, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(flushLsn >> 32), (uint32)(flushLsn));
    securec_check_ss(rc, "\0", "\0");
    PG_RETURN_TEXT_P(cstring_to_text(location));
}

Datum pg_get_flush_lsn(PG_FUNCTION_ARGS)
{
    XLogRecPtr flushLsn;
    char location[MAXFNAMELEN];
    int rc = EOK;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to pg_get_sync_flush_lsn."))));
    flushLsn = GetFlushRecPtr();
    rc = snprintf_s(location, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(flushLsn >> 32), (uint32)(flushLsn));
    securec_check_ss(rc, "\0", "\0");
    PG_RETURN_TEXT_P(cstring_to_text(location));
}

/*
 * Set obs delete location (same format as pg_start_backup etc)
 *
 * This is useful for determining how much of WAL will been removed from obs xlog files
 */
Datum gs_set_obs_delete_location(PG_FUNCTION_ARGS)
{
    text *location = PG_GETARG_TEXT_P(0);
    char *locationstr = NULL;
    uint32 hi = 0;
    uint32 lo = 0;
    XLogSegNo xlogsegno;
    XLogRecPtr locationpoint;
    char xlogfilename[MAXFNAMELEN];
    errno_t errorno = EOK;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to gs_set_obs_delete_location."))));
    }

    locationstr = text_to_cstring(location);

    validate_xlog_location(locationstr);

    /* test input string */
    if (sscanf_s(locationstr, "%X/%X", &hi, &lo) != 2)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not parse transaction log location \"%s\"", locationstr)));

    locationpoint = (((uint64)hi) << 32) | lo;

    if (getObsReplicationSlot()) {
        // Call the OBS deletion API.
        (void)obs_replication_cleanup(locationpoint);
    }

    XLByteToSeg(locationpoint, xlogsegno);
    errorno = snprintf_s(xlogfilename, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X_%02u", DEFAULT_TIMELINE_ID,
                         (uint32)((xlogsegno) / XLogSegmentsPerXLogId), (uint32)((xlogsegno) % XLogSegmentsPerXLogId),
                         (uint32)((locationpoint / OBS_XLOG_SLICE_BLOCK_SIZE) & OBS_XLOG_SLICE_NUM_MAX));
    securec_check_ss(errorno, "", "");    

    PG_RETURN_TEXT_P(cstring_to_text(xlogfilename));
}

Datum gs_get_global_barrier_status(PG_FUNCTION_ARGS) 
{
#define PG_GET_GLOBAL_BARRIER_STATUS_COLS 2
    char globalBarrierId[MAX_BARRIER_ID_LENGTH] = {0};
    char globalAchiveBarrierId[MAX_BARRIER_ID_LENGTH] = {0};
    Datum values[PG_GET_GLOBAL_BARRIER_STATUS_COLS];
    bool isnull[PG_GET_GLOBAL_BARRIER_STATUS_COLS];
    TupleDesc resultTupleDesc;
    HeapTuple resultHeapTuple;
    Datum result;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to gs_get_global_barrier_status."))));

    // global_barrierId:max global barrierId
    // global_achive_barrierId:max achive barrierId

    List *objectList = NIL;
    size_t readLen = 0;
    errno_t rc = 0;
    if (getObsReplicationSlot()) {
        objectList = obsList(HADR_BARRIER_ID_FILE);
        if (objectList == NIL || objectList->length <= 0) {
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                    (errmsg("The Barrier ID file named %s cannot be found.", HADR_BARRIER_ID_FILE))));
        }

        readLen = obsRead(HADR_BARRIER_ID_FILE, 0, globalBarrierId, MAX_BARRIER_ID_LENGTH);
        if (readLen == 0) {
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                    (errmsg("Cannot read global barrier ID in %s file!", HADR_BARRIER_ID_FILE))));
        }
    } else {
        rc = strncpy_s((char *)globalBarrierId, MAX_BARRIER_ID_LENGTH,
            "hadr_00000000000000000001_0000000000000", MAX_BARRIER_ID_LENGTH - 1);
        securec_check(rc, "\0", "\0");
    }

    rc = strncpy_s((char *)globalAchiveBarrierId, MAX_BARRIER_ID_LENGTH,
                   (char *)globalBarrierId, MAX_BARRIER_ID_LENGTH - 1);
    securec_check(rc, "\0", "\0");

    /*
     * Construct a tuple descriptor for the result row.
     */
    resultTupleDesc = CreateTemplateTupleDesc(PG_GET_GLOBAL_BARRIER_STATUS_COLS, false, TAM_HEAP);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)1, "global_barrier_id", TEXTOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)2, "global_achive_barrier_id", TEXTOID, -1, 0);

    resultTupleDesc = BlessTupleDesc(resultTupleDesc);

    values[0] = CStringGetTextDatum(globalBarrierId);
    isnull[0] = false;
    values[1] = CStringGetTextDatum(globalAchiveBarrierId);
    isnull[1] = false;

    resultHeapTuple = heap_form_tuple(resultTupleDesc, values, isnull);

    result = HeapTupleGetDatum(resultHeapTuple);

    PG_RETURN_DATUM(result);
}    

Datum gs_get_local_barrier_status(PG_FUNCTION_ARGS) 
{
#define PG_GET_LOCAL_BARRIER_STATUS_COLS 4
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    XLogRecPtr flushLsn = InvalidXLogRecPtr;
    XLogRecPtr archiveLsn = InvalidXLogRecPtr;
    XLogRecPtr barrierLsn = InvalidXLogRecPtr;
    char barrierId[MAX_BARRIER_ID_LENGTH] = {0};
    char archiveLocation[MAXFNAMELEN] = {0};
    char flushLocation[MAXFNAMELEN] = {0};
    char barrierLocation[MAXFNAMELEN] = {0};
    Datum values[PG_GET_LOCAL_BARRIER_STATUS_COLS];
    bool isnull[PG_GET_LOCAL_BARRIER_STATUS_COLS];
    TupleDesc resultTupleDesc;
    HeapTuple resultHeapTuple;
    Datum result;

    int rc = EOK;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to gs_get_local_barrier_status."))));

    // barrierID max barrierID
    // barrier_LSN max barrier LSN
    // archive_LSN max archive xlog LSN 
    // flush_LSN max flush xlog LSN 

    if (getObsReplicationSlot() && IsServerModeStandby() && !XLogArchivingActive()) {
        rc = strncpy_s((char *)barrierId, MAX_BARRIER_ID_LENGTH,
                       (char *)walrcv->lastRecoveredBarrierId, MAX_BARRIER_ID_LENGTH - 1);
        securec_check(rc, "\0", "\0");
        barrierLsn = walrcv->lastRecoveredBarrierLSN;
    }

    rc = snprintf_s(barrierLocation, MAXFNAMELEN, MAXFNAMELEN - 1, 
                    "%08X/%08X", (uint32)(barrierLsn >> 32), (uint32)(barrierLsn));
    securec_check_ss(rc, "\0", "\0");

    archiveLsn = g_instance.archive_obs_cxt.archive_task.targetLsn;
    rc = snprintf_s(archiveLocation, MAXFNAMELEN, MAXFNAMELEN - 1, 
                    "%08X/%08X", (uint32)(archiveLsn >> 32), (uint32)(archiveLsn));
    securec_check_ss(rc, "\0", "\0");

    flushLsn = GetFlushRecPtr();
    rc = snprintf_s(flushLocation, MAXFNAMELEN, MAXFNAMELEN - 1, 
                    "%08X/%08X", (uint32)(flushLsn >> 32), (uint32)(flushLsn));
    securec_check_ss(rc, "\0", "\0");

    /*
     * Construct a tuple descriptor for the result row.
     */
    resultTupleDesc = CreateTemplateTupleDesc(PG_GET_LOCAL_BARRIER_STATUS_COLS, false, TAM_HEAP);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)1, "barrier_id", TEXTOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)2, "barrier_lsn", TEXTOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)3, "archive_lsn", TEXTOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)4, "flush_lsn", TEXTOID, -1, 0);

    resultTupleDesc = BlessTupleDesc(resultTupleDesc);

    values[0] = CStringGetTextDatum(barrierId);
    isnull[0] = false;
    values[1] = CStringGetTextDatum(barrierLocation);
    isnull[1] = false;
    values[2] = CStringGetTextDatum(archiveLocation);
    isnull[2] = false;
    values[3] = CStringGetTextDatum(flushLocation);
    isnull[3] = false;

    resultHeapTuple = heap_form_tuple(resultTupleDesc, values, isnull);

    result = HeapTupleGetDatum(resultHeapTuple);

    PG_RETURN_DATUM(result);

    PG_RETURN_TEXT_P(cstring_to_text(flushLocation));
}

