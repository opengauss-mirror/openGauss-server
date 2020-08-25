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
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "replication/walreceiver.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"

static const int MAXLSNCOMPONENT = 8;

extern void validate_xlog_location(char* str);

static inline void pg_check_delay_xlog_recycle()
{
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin to control xlog delay function."))));
    }
    if (RecoveryInProgress()) {
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("recovery is in progress"),
                errhint("xlog delay function cannot be executed during recovery.")));
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
    text* backupid = PG_GETARG_TEXT_P(0);
    bool fast = PG_GETARG_BOOL(1);
    char* backupidstr = NULL;
    XLogRecPtr startpoint;
    DIR *dir;
    char startxlogstr[MAXFNAMELEN];
    errno_t errorno = EOK;

    backupidstr = text_to_cstring(backupid);
    dir = AllocateDir("pg_tblspc");
    startpoint = do_pg_start_backup(backupidstr, fast, NULL, dir, NULL, NULL, false, true);

    errorno = snprintf_s(startxlogstr,
        sizeof(startxlogstr),
        sizeof(startxlogstr) - 1,
        "%X/%X",
        (uint32)(startpoint >> 32),
        (uint32)startpoint);
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

    /* when delay xlog recycle is true, we do not copy xlog from archive */
    stoppoint = do_pg_stop_backup(NULL, !GetDelayXlogRecycle());

    errorno = snprintf_s(stopxlogstr,
        sizeof(stopxlogstr),
        sizeof(stopxlogstr) - 1,
        "%X/%X",
        (uint32)(stoppoint >> 32),
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

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin to switch transaction log files."))));

    if (RecoveryInProgress())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("recovery is in progress"),
                errhint("WAL control functions cannot be executed during recovery.")));

    switchpoint = RequestXLogSwitch();
    RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_IMMEDIATE);

    /*
     * As a convenience, return the WAL location of the switch record
     */
    errorno = snprintf_s(
        location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(switchpoint >> 32), (uint32)switchpoint);
    securec_check_ss(errorno, "", "");
    PG_RETURN_TEXT_P(cstring_to_text(location));
}

/*
 * pg_create_restore_point: a named point for restore
 */
Datum pg_create_restore_point(PG_FUNCTION_ARGS)
{
    text* restore_name = PG_GETARG_TEXT_P(0);
    char* restore_name_str = NULL;
    XLogRecPtr restorepoint;
    char location[MAXFNAMELEN];
    errno_t errorno = EOK;

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("Must be system admin to create a restore point."))));

    if (RecoveryInProgress())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                (errmsg("recovery is in progress"),
                    errhint("WAL control functions cannot be executed during recovery."))));

    if (!XLogIsNeeded())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("WAL level not sufficient for creating a restore point"),
                errhint("wal_level must be set to \"archive\" or \"hot_standby\" at server start.")));

    restore_name_str = text_to_cstring(restore_name);
    if (strlen(restore_name_str) >= MAXFNAMELEN)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("value too long for restore point (maximum %d characters)", MAXFNAMELEN - 1)));

    restorepoint = XLogRestorePoint(restore_name_str);

    /*
     * As a convenience, return the WAL location of the restore point record
     */
    errorno = snprintf_s(
        location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(restorepoint >> 32), (uint32)restorepoint);
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
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("recovery is in progress"),
                errhint("WAL control functions cannot be executed during recovery.")));

    current_recptr = GetXLogWriteRecPtr();

    errorno = snprintf_s(location,
        sizeof(location),
        sizeof(location) - 1,
        "%X/%X",
        (uint32)(current_recptr >> 32),
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
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("recovery is in progress"),
                errhint("WAL control functions cannot be executed during recovery.")));

    current_recptr = GetXLogInsertRecPtr();

    errorno = snprintf_s(location,
        sizeof(location),
        sizeof(location) - 1,
        "%X/%X",
        (uint32)(current_recptr >> 32),
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

    errorno =
        snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(recptr >> 32), (uint32)recptr);
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
    FILE* fp = NULL;
    if ((fp = fopen("term_file", "r")) != NULL) {
        cnt = fread(&Term, sizeof(uint32), 1, fp);
        (void)fclose(fp);
        fp = NULL;

        if (cnt != 1) {
            ereport(LOG, (0, errmsg("cannot read term file:  \n")));
        }
    }

    if (Term > g_instance.comm_cxt.localinfo_cxt.term &&
        t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE) {
        g_instance.comm_cxt.localinfo_cxt.term = Term;
    }
    uint32 max_term = g_instance.comm_cxt.localinfo_cxt.term;

    recptr = GetXLogReplayRecPtrInPending();
    if (recptr == 0) {
        values[0] = CStringGetTextDatum("0");
        nulls[0] = false;
        values[1] = CStringGetTextDatum("0/0");
        nulls[1] = false;
    } else {
        /* Build a tuple descriptor for our result type */
        errorno = snprintf_s(
            location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(recptr >> 32), (uint32)recptr);
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
    text* location = PG_GETARG_TEXT_P(0);
    char* locationstr = NULL;
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
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("recovery is in progress"),
                errhint("pg_xlogfile_name_offset() cannot be executed during recovery.")));

    /*
     * Read input and parse
     */
    locationstr = text_to_cstring(location);

    validate_xlog_location(locationstr);

    /* test input string */
    if (sscanf_s(locationstr, "%X/%X", &hi, &lo) != 2)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not parse transaction log location \"%s\"", locationstr)));

    locationpoint = (((uint64)hi) << 32) | lo;

    /*
     * Construct a tuple descriptor for the result row.  This must match this
     * function's pg_proc entry!
     */
    resultTupleDesc = CreateTemplateTupleDesc(2, false);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)1, "file_name", TEXTOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)2, "file_offset", INT4OID, -1, 0);

    resultTupleDesc = BlessTupleDesc(resultTupleDesc);

    /*
     * xlogfilename
     */
    XLByteToPrevSeg(locationpoint, xlogsegno);
    errorno = snprintf_s(xlogfilename,
        MAXFNAMELEN,
        MAXFNAMELEN - 1,
        "%08X%08X%08X",
        t_thrd.xlog_cxt.ThisTimeLineID,
        (uint32)((xlogsegno) / XLogSegmentsPerXLogId),
        (uint32)((xlogsegno) % XLogSegmentsPerXLogId));
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
    text* location = PG_GETARG_TEXT_P(0);
    char* locationstr = NULL;
    uint32 hi = 0;
    uint32 lo = 0;
    XLogSegNo xlogsegno;
    XLogRecPtr locationpoint;
    char xlogfilename[MAXFNAMELEN];
    errno_t errorno = EOK;

    if (RecoveryInProgress())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("recovery is in progress"),
                errhint("pg_xlogfile_name() cannot be executed during recovery.")));

    locationstr = text_to_cstring(location);

    validate_xlog_location(locationstr);

    /* test input string */
    if (sscanf_s(locationstr, "%X/%X", &hi, &lo) != 2)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not parse transaction log location \"%s\"", locationstr)));

    locationpoint = (((uint64)hi) << 32) | lo;

    XLByteToPrevSeg(locationpoint, xlogsegno);
    errorno = snprintf_s(xlogfilename,
        MAXFNAMELEN,
        MAXFNAMELEN - 1,
        "%08X%08X%08X",
        t_thrd.xlog_cxt.ThisTimeLineID,
        (uint32)((xlogsegno) / XLogSegmentsPerXLogId),
        (uint32)((xlogsegno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    PG_RETURN_TEXT_P(cstring_to_text(xlogfilename));
}

/*
 * pg_xlog_replay_pause - pause recovery now
 */
Datum pg_xlog_replay_pause(PG_FUNCTION_ARGS)
{
    if (!superuser())
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("Must be system admin to control recovery."))));

    if (!RecoveryInProgress())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("recovery is not in progress"),
                errhint("Recovery control functions can only be executed during recovery.")));

    SetRecoveryPause(true);

    PG_RETURN_VOID();
}

/*
 * pg_xlog_replay_resume - resume recovery now
 */
Datum pg_xlog_replay_resume(PG_FUNCTION_ARGS)
{
    if (!superuser())
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("Must be system admin to control recovery."))));

    if (!RecoveryInProgress())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("recovery is not in progress"),
                errhint("Recovery control functions can only be executed during recovery.")));

    SetRecoveryPause(false);

    PG_RETURN_VOID();
}

/*
 * pg_is_xlog_replay_paused
 */
Datum pg_is_xlog_replay_paused(PG_FUNCTION_ARGS)
{
    if (!superuser())
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("Must be system admin to control recovery."))));

    if (!RecoveryInProgress())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("recovery is not in progress"),
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
void validate_xlog_location(char* str)
{
    int len1, len2;

    len1 = (int)strspn(str, "0123456789abcdefABCDEF");
    if (len1 < 1 || len1 > MAXLSNCOMPONENT || str[len1] != '/')
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for transaction log location: \"%s\"", str)));

    len2 = (int)strspn(str + len1 + 1, "0123456789abcdefABCDEF");
    if (len2 < 1 || len2 > MAXLSNCOMPONENT || str[len1 + 1 + len2] != '\0')
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for transaction log location: \"%s\"", str)));
}

/*
 * Compute the difference in bytes between two WAL locations.
 */
Datum pg_xlog_location_diff(PG_FUNCTION_ARGS)
{
    text* location1 = PG_GETARG_TEXT_P(0);
    text* location2 = PG_GETARG_TEXT_P(1);
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
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not parse transaction log location \"%s\"", str1)));
    loc1 = (((uint64)hi) << 32) | lo;
    if (sscanf_s(str2, "%X/%X", &hi, &lo) != 2)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
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
    pg_check_delay_xlog_recycle();

    enable_delay_xlog_recycle();

    PG_RETURN_VOID();
}

Datum pg_disable_delay_xlog_recycle(PG_FUNCTION_ARGS)
{
    pg_check_delay_xlog_recycle();

    disable_delay_xlog_recycle();

    PG_RETURN_VOID();
}

Datum pg_enable_delay_ddl_recycle(PG_FUNCTION_ARGS)
{
    XLogRecPtr startLSN;
    char location[MAXFNAMELEN];
    errno_t rc = EOK;

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("Must be system admin to control ddl delay function."))));

    if (RecoveryInProgress())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("recovery is in progress"),
                errhint("ddl delay function cannot be executed during recovery.")));

    startLSN = enable_delay_ddl_recycle();

    rc = snprintf_s(location, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(startLSN >> 32), (uint32)(startLSN));
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(location));
}

Datum pg_disable_delay_ddl_recycle(PG_FUNCTION_ARGS)
{
    text* barrierLSNArg = PG_GETARG_TEXT_P(0);
    bool isForce = PG_GETARG_BOOL(1);
    XLogRecPtr barrierLSN = InvalidXLogRecPtr;
    char* barrierLSNStr = text_to_cstring(barrierLSNArg);
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

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("Must be system admin to control ddl delay function."))));

    if (RecoveryInProgress())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("recovery is in progress"),
                errhint("ddl delay function cannot be executed during recovery.")));

    validate_xlog_location(barrierLSNStr);

    /* test input string */
    if (sscanf_s(barrierLSNStr, "%X/%X", &hi, &lo) != 2)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("could not parse xlog location \"%s\"", barrierLSNStr)));

    barrierLSN = (((uint64)hi) << 32) | lo;

    disable_delay_ddl_recycle(barrierLSN, isForce, &startLSN, &endLSN);

    rc = snprintf_s(
        startLocation, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(startLSN >> 32), (uint32)(startLSN));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(endLocation, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(endLSN >> 32), (uint32)(endLSN));
    securec_check_ss(rc, "\0", "\0");

    /*
     * Construct a tuple descriptor for the result row.
     */
    resultTupleDesc = CreateTemplateTupleDesc(2, false);
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

Datum pg_resume_bkp_flag(PG_FUNCTION_ARGS)
{
    bool startBackupFlag = false;
    bool toDelay = false;
    XLogRecPtr startLSN;
    char location[MAXFNAMELEN];
    errno_t rc = EOK;

    /* insert tuple have 4 item */
    Datum values[4];
    bool isnull[4];
    TupleDesc resultTupleDesc;
    HeapTuple resultHeapTuple;
    Datum result;
    char* rewindTime = NULL;

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin to control resume_bkp_flag function."))));

    startBackupFlag = get_startBackup_flag();
    toDelay = GetDelayXlogRecycle();
    startLSN = GetDDLDelayStartPtr();
    rc = snprintf_s(location, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(startLSN >> 32), (uint32)(startLSN));
    securec_check_ss(rc, "\0", "\0");
    rewindTime = getLastRewindTime();

    /*
     * Construct a tuple descriptor for the result row.
     */
    resultTupleDesc = CreateTemplateTupleDesc(4, false);
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
    }

    result = HeapTupleGetDatum(resultHeapTuple);

    PG_RETURN_DATUM(result);
}
