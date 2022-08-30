/* -------------------------------------------------------------------------
 *
 * xlogfuncs.cpp
 *
 * openGauss transaction log manager user interface functions
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

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cbmparsexlog.h"
#include "access/xlog.h"
#include "access/obs/obs_am.h"
#include "access/archive/archive_am.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "replication/archive_walreceiver.h"
#include "replication/walreceiver.h"
#include "replication/walsender_private.h"
#include "replication/slot.h"
#include "replication/syncrep.h"
#include "storage/smgr/smgr.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "postmaster/barrier_creator.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"

const int msecPerSec = 1000;

typedef ArchiveSlotConfig*(*get_slot_func)(const char *);

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
    /* we are about to start streaming switch over, stop any xlog insert. */
    if (t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("This xlog function cannot be executed during streaming disaster recovery")));
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

        RegisterPersistentAbortBackupHandler();

        startpoint = do_pg_start_backup(backupidstr, fast, &labelfile,dir, &tblspcmapfile, NULL,false,true);
        
        if (u_sess->probackup_context == NULL) {
            u_sess->probackup_context = AllocSetContextCreate(u_sess->top_mem_cxt, "probackup context",
                                                              ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                              ALLOCSET_DEFAULT_MAXSIZE);
        }
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
    MemoryContext perqueryctx, oldcontext;
    Datum        values[3];
    bool         nulls[3];
    XLogRecPtr stoppoint;
    char stopxlogstr[MAXFNAMELEN];
    errno_t errorno = EOK;
    bool  exclusive = PG_GETARG_BOOL(0);
    errno_t rc;

    SessionBackupState status = u_sess->proc_cxt.sessionBackupState;
    if (status == SESSION_BACKUP_NONE) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("a backup is not in progress")));
    }
    if (exclusive) {
        if (status == SESSION_BACKUP_NON_EXCLUSIVE)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("non-exclusive backup in progress"), errhint("Did you mean to use pg_stop_backup('f')?")));
    } else {
        if (status != SESSION_BACKUP_NON_EXCLUSIVE)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("non-exclusive backup is not in progress")));
    }

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
        /* when delay xlog recycle is true, we do not copy xlog from archive */
        stoppoint = do_pg_stop_backup(NULL, !GetDelayXlogRecycle());
        nulls[1] = true;
        nulls[2] = true;
    } else {
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

    /* we are about to start streaming switch over, stop any xlog insert. */
    if (t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("cannot switch xlog during streaming disaster recovery")));
    }

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
    XLogRecPtr trackpoint;
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

    /* we are about to start streaming switch over, stop any xlog insert. */
    if (t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("cannot roach switch xlog during streaming disaster recovery")));
    }

    /* hold lock to force cbm track */
    if (u_sess->attr.attr_storage.enable_cbm_tracking) {
        LWLockAcquire(CBMParseXlogLock, LW_EXCLUSIVE);
    }
    switchpoint = trackpoint = RequestXLogSwitch();

    if (request_ckpt) {
        RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_IMMEDIATE);
    }

    /* track cbm to end of segment */
    trackpoint += XLogSegSize - 1;
    trackpoint -= trackpoint % XLogSegSize;
    if (u_sess->attr.attr_storage.enable_cbm_tracking) {
        (void)ForceTrackCBMOnce(trackpoint, ENABLE_DDL_DELAY_TIMEOUT, true, true);
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

    /* we are about to start streaming switch over, stop any xlog insert. */
    if (t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("cannot create restore point during streaming disaster recovery")));
    }

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
 * Report the current WAL insert end location.
 *
 */
Datum gs_current_xlog_insert_end_location(PG_FUNCTION_ARGS)
{
    XLogRecPtr current_recptr;
    char location[MAXFNAMELEN];
    errno_t errorno = EOK;

    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("WAL control functions cannot be executed during recovery.")));

    current_recptr = GetXLogInsertEndRecPtr();

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

Datum gs_set_obs_delete_location_with_slotname(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    char* lsnLocation = PG_GETARG_CSTRING(0);
    char* currentSlotName = PG_GETARG_CSTRING(1);

    uint32 hi = 0;
    uint32 lo = 0;
    XLogSegNo xlogsegno;
    XLogRecPtr locationpoint;
    ArchiveSlotConfig *archive_conf = NULL;
    char xlogfilename[MAXFNAMELEN];
    errno_t errorno = EOK;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to gs_set_obs_delete_location."))));
    }

    validate_xlog_location(lsnLocation);

    /* test input string */
    if (sscanf_s(lsnLocation, "%X/%X", &hi, &lo) != 2)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not parse transaction log location \"%s\"", lsnLocation)));

    locationpoint = (((uint64)hi) << 32) | lo;
    if ((archive_conf = getArchiveReplicationSlotWithName(currentSlotName)) != NULL) {
        (void)archive_replication_cleanup(locationpoint, &archive_conf->archive_config);
    }

    XLByteToSeg(locationpoint, xlogsegno);
    errorno = snprintf_s(xlogfilename, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X_%02u", DEFAULT_TIMELINE_ID,
                         (uint32)((xlogsegno) / XLogSegmentsPerXLogId), (uint32)((xlogsegno) % XLogSegmentsPerXLogId),
                         (uint32)((locationpoint / OBS_XLOG_SLICE_BLOCK_SIZE) & OBS_XLOG_SLICE_NUM_MAX));
    securec_check_ss(errorno, "", "");

    PG_RETURN_TEXT_P(cstring_to_text(xlogfilename));
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_TEXT_P(NULL);
#endif
}

/*
 * Set obs delete location (same format as pg_start_backup etc)
 *
 * This is useful for determining how much of WAL will been removed from obs xlog files
 */
Datum gs_set_obs_delete_location(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
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

    if (GetArchiveRecoverySlot()) {
        // Call the OBS deletion API.
        (void)archive_replication_cleanup(locationpoint, NULL);
    }

    XLByteToSeg(locationpoint, xlogsegno);
    errorno = snprintf_s(xlogfilename, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X_%02u", DEFAULT_TIMELINE_ID,
                         (uint32)((xlogsegno) / XLogSegmentsPerXLogId), (uint32)((xlogsegno) % XLogSegmentsPerXLogId),
                         (uint32)((locationpoint / OBS_XLOG_SLICE_BLOCK_SIZE) & OBS_XLOG_SLICE_NUM_MAX));
    securec_check_ss(errorno, "", "");    

    PG_RETURN_TEXT_P(cstring_to_text(xlogfilename));
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_TEXT_P(NULL);
#endif
}

Datum gs_get_global_barrier_status(PG_FUNCTION_ARGS) 
{
#ifndef ENABLE_LITE_MODE
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
    get_slot_func slot_func;
    List *all_archive_slots = NIL;
    if (IS_OBS_DISASTER_RECOVER_MODE || IS_CN_OBS_DISASTER_RECOVER_MODE) {
        all_archive_slots = GetAllRecoverySlotsName();
        slot_func = &getArchiveRecoverySlotWithName;
    } else {
        all_archive_slots = GetAllArchiveSlotsName();
        slot_func = &getArchiveReplicationSlotWithName;
    }
    if (all_archive_slots == NIL || all_archive_slots->length == 0) {
        rc = strncpy_s((char *)globalBarrierId, MAX_BARRIER_ID_LENGTH,
            "hadr_00000000000000000001_0000000000000", MAX_BARRIER_ID_LENGTH - 1);
        securec_check(rc, "\0", "\0");
    } else {
        char *slotname = (char *)lfirst((list_head(all_archive_slots)));
        ArchiveSlotConfig *archive_conf = NULL;
        if ((archive_conf = slot_func(slotname)) == NULL) {
            rc = strncpy_s((char *)globalBarrierId, MAX_BARRIER_ID_LENGTH,
                "hadr_00000000000000000001_0000000000000", MAX_BARRIER_ID_LENGTH - 1);
            securec_check(rc, "\0", "\0");
        } else {
            char pathPrefix[MAXPGPATH] = {0};
            ArchiveConfig obsConfig;
            /* copy OBS configs to temporary variable for customising file path */
            rc = memcpy_s(&obsConfig, sizeof(ArchiveConfig), &archive_conf->archive_config, sizeof(ArchiveConfig));
            securec_check(rc, "", "");

            if (!IS_PGXC_COORDINATOR) {
                rc = strcpy_s(pathPrefix, MAXPGPATH, obsConfig.archive_prefix);
                securec_check_c(rc, "\0", "\0");

                char *p = strrchr(pathPrefix, '/');
                if (p == NULL) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("Obs path prefix is invalid")));
                }
                *p = '\0';
                obsConfig.archive_prefix = pathPrefix;
            }

            objectList = ArchiveList(HADR_BARRIER_ID_FILE, &obsConfig, true, true);
            if (objectList == NIL || objectList->length <= 0) {
                ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                        (errmsg("The Barrier ID file named %s cannot be found.", HADR_BARRIER_ID_FILE))));
            }

            readLen = ArchiveRead(HADR_BARRIER_ID_FILE, 0, globalBarrierId, MAX_BARRIER_ID_LENGTH, 
                &obsConfig);
            if (readLen == 0) {
                ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                        (errmsg("Cannot read global barrier ID in %s file!", HADR_BARRIER_ID_FILE))));
            }
            globalBarrierId[MAX_BARRIER_ID_LENGTH - 1] = '\0';
            list_free_deep(all_archive_slots);
        }
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
    list_free_deep(objectList);
    PG_RETURN_DATUM(result);
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_DATUM(0);
#endif
}    

Datum gs_get_global_barriers_status(PG_FUNCTION_ARGS) 
{
#ifndef ENABLE_LITE_MODE
#define PG_GET_GLOBAL_BARRIERS_STATUS_COLS 3
    char globalBarrierId[MAX_BARRIER_ID_LENGTH] = {0};
    char globalAchiveBarrierId[MAX_BARRIER_ID_LENGTH] = {0};
    Datum values[PG_GET_GLOBAL_BARRIERS_STATUS_COLS];
    bool isnull[PG_GET_GLOBAL_BARRIERS_STATUS_COLS];
    
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    get_slot_func slot_func;
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to gs_get_global_barrier_status."))));

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialize mode required, but it is not "
                                                                       "allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    /*
     * We don't require any special permission to see this function's data
     * because nothing should be sensitive. The most critical being the slot
     * name, which shouldn't contain anything particularly sensitive.
     */
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    (void)MemoryContextSwitchTo(oldcontext);

    // global_barrierId:max global barrierId
    // global_achive_barrierId:max achive barrierId
    List *all_archive_slots = NIL;
    if (IS_OBS_DISASTER_RECOVER_MODE || IS_CN_OBS_DISASTER_RECOVER_MODE) {
        all_archive_slots = GetAllRecoverySlotsName();
        slot_func = &getArchiveRecoverySlotWithName;
    } else {
        all_archive_slots = GetAllArchiveSlotsName();
        slot_func = &getArchiveReplicationSlotWithName;
    }
    if (all_archive_slots == NIL || all_archive_slots->length == 0) {
        tuplestore_donestoring(tupstore);
        PG_RETURN_NULL();
    }
    List *objectList = NIL;
    size_t readLen = 0;
    errno_t rc = 0;
    foreach_cell(cell, all_archive_slots) {
        char* slotname = (char*)lfirst(cell);
        ArchiveSlotConfig *archive_conf = NULL;
        if ((archive_conf = slot_func(slotname)) != NULL) {
            char pathPrefix[MAXPGPATH] = {0};
            ArchiveConfig obsConfig;
            /* copy OBS configs to temporary variable for customising file path */
            rc = memcpy_s(&obsConfig, sizeof(ArchiveConfig), &archive_conf->archive_config, sizeof(ArchiveConfig));
            securec_check(rc, "", "");

            if (!IS_PGXC_COORDINATOR) {
                rc = strcpy_s(pathPrefix, MAXPGPATH, obsConfig.archive_prefix);
                securec_check_c(rc, "\0", "\0");

                char *p = strrchr(pathPrefix, '/');
                if (p == NULL) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("Obs path prefix is invalid")));
                }
                *p = '\0';
                obsConfig.archive_prefix = pathPrefix;
            }

            objectList = ArchiveList(HADR_BARRIER_ID_FILE, &obsConfig, true, true);
            if (objectList == NIL || objectList->length <= 0) {
                ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                        (errmsg("The Barrier ID file named %s cannot be found.", HADR_BARRIER_ID_FILE))));
            }

            readLen = ArchiveRead(HADR_BARRIER_ID_FILE, 0, globalBarrierId, MAX_BARRIER_ID_LENGTH, 
                &obsConfig);
            if (readLen == 0) {
                ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                        (errmsg("Cannot read global barrier ID in %s file!", HADR_BARRIER_ID_FILE))));
            }
            globalBarrierId[MAX_BARRIER_ID_LENGTH - 1] = '\0';
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
        values[0] = CStringGetTextDatum(slotname);
        isnull[0] = false;
        values[1] = CStringGetTextDatum(globalBarrierId);
        isnull[1] = false;
        values[2] = CStringGetTextDatum(globalAchiveBarrierId);
        isnull[2] = false;
        tuplestore_putvalues(tupstore, tupdesc, values, isnull);
    }
    list_free_deep(all_archive_slots);
    list_free_deep(objectList);
    tuplestore_donestoring(tupstore);
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
    PG_RETURN_DATUM(0);
}    

Datum gs_get_local_barrier_status(PG_FUNCTION_ARGS) 
{
#ifndef ENABLE_LITE_MODE
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

    // barrierID max barrierID
    // barrier_LSN max barrier LSN
    // archive_LSN max archive xlog LSN 
    // flush_LSN max flush xlog LSN 

    if (IS_OBS_DISASTER_RECOVER_MODE || IS_CN_OBS_DISASTER_RECOVER_MODE || IS_DISASTER_RECOVER_MODE) {
        SpinLockAcquire(&walrcv->mutex);
        rc = strncpy_s((char *)barrierId, MAX_BARRIER_ID_LENGTH,
                       (char *)walrcv->lastRecoveredBarrierId, MAX_BARRIER_ID_LENGTH - 1);
        securec_check(rc, "\0", "\0");
        barrierLsn = walrcv->lastRecoveredBarrierLSN;
        SpinLockRelease(&walrcv->mutex);
    }

    rc = snprintf_s(barrierLocation, MAXFNAMELEN, MAXFNAMELEN - 1, 
                    "%08X/%08X", (uint32)(barrierLsn >> 32), (uint32)(barrierLsn));
    securec_check_ss(rc, "\0", "\0");

    archiveLsn = 0;
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
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_TEXT_P(NULL);
#endif
}

Datum gs_hadr_do_switchover(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
#define TIME_GET_MILLISEC(t) (((long)(t).tv_sec * 1000) + ((long)(t).tv_usec) / 1000)
    uint64_t barrier_index = 0;
    int ret;
    struct timeval tv;
    char barrier_name[MAX_BARRIER_ID_LENGTH];

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to gs_hadr_do_switchover."))));

    /* OBS-based DR archiving is enabled and the switchover process is in progress. */
    if (g_instance.archive_obs_cxt.in_switchover) {
        g_instance.archive_obs_cxt.in_service_truncate = true;
        ereport(LOG, (errmsg("OBS-based DR archiving is enabled and the switchover process is in progress.")));
    } else {
        ereport(LOG, (errmsg("OBS-based DR archiving is disabled or the switchover process is not in progress.")));
        PG_RETURN_BOOL(false);
    }

    List *archiveSlotNames = GetAllArchiveSlotsName();
    if (archiveSlotNames == NIL || archiveSlotNames->length == 0) {
        ereport(LOG, (errmsg("[hadr switchover]obs_archive_slot does not exist.")));
        PG_RETURN_BOOL(false);
    }
    barrier_index = GetObsBarrierIndex(archiveSlotNames, NULL);

    gettimeofday(&tv, NULL);

    // create switchover barrier
    barrier_index += 50;
    ret = snprintf_s(barrier_name, MAX_BARRIER_ID_LENGTH, MAX_BARRIER_ID_LENGTH - 1, 
        "hadr_%020" PRIu64 "_%013ld", barrier_index, TIME_GET_MILLISEC(tv));
    securec_check_ss_c(ret, "\0", "\0");
    ereport(LOG, (errmsg("[switchover] creating switchover barrier %s", barrier_name)));

#ifdef ENABLE_MULTIPLE_NODES
    RequestBarrier(barrier_name, NULL, true);
#else
    DisasterRecoveryRequestBarrier(barrier_name, true);
#endif

    errno_t rc = 0;
    ArchiveConfig obsConfig;
    char pathPrefix[MAXPGPATH] = {0};
    char *slotname = (char *)lfirst((list_head(archiveSlotNames)));
    ArchiveSlotConfig *archive_conf = NULL;
    if ((archive_conf = getArchiveReplicationSlotWithName(slotname)) == NULL) {
        list_free_deep(archiveSlotNames);
        archiveSlotNames = NULL;
        PG_RETURN_BOOL(false);
    }

    /* hadr only support OBS */
    if (archive_conf->archive_config.media_type != ARCHIVE_OBS) {
        list_free_deep(archiveSlotNames);
        archiveSlotNames = NULL;
        PG_RETURN_BOOL(false);
    }

    /* copy OBS configs to temporary variable for customising file path */
    rc = memcpy_s(&obsConfig, sizeof(ArchiveConfig), &archive_conf->archive_config, sizeof(ArchiveConfig));
    securec_check(rc, "", "");

    if (!IS_PGXC_COORDINATOR) {
        rc = strcpy_s(pathPrefix, MAXPGPATH, obsConfig.archive_prefix);
        securec_check(rc, "\0", "\0");

        char *p = strrchr(pathPrefix, '/');
        if (p == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Obs path prefix is invalid")));
            list_free_deep(archiveSlotNames);
            archiveSlotNames = NULL;
            PG_RETURN_BOOL(false);
        }
        *p = '\0';
        obsConfig.archive_prefix = pathPrefix;
    }
    ereport(LOG, (errmsg("write switchover barrier id %s to obs", barrier_name)));

    /* hadr_barrier_id is written by the BARRIER_ARCH thread. 
       The switchover barrier is the target barrier. 
       When the global barrier reaches the switchover barrier, the archiving is complete. */
    obsWrite(HADR_SWITCHOVER_BARRIER_ID_FILE, barrier_name, MAX_BARRIER_ID_LENGTH - 1, &obsConfig);

    list_free_deep(archiveSlotNames);
    archiveSlotNames = NULL;
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
    PG_RETURN_BOOL(true);
}

Datum gs_hadr_has_barrier_creator(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to gs_hadr_has_barrier_creator."))));

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && IsFirstCn()) {
#else
    if (IS_PGXC_DATANODE && g_instance.pid_cxt.BarrierCreatorPID != 0) {
#endif        
        PG_RETURN_BOOL(true);
    }
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
    PG_RETURN_BOOL(false);
}

/*
 * Used for cluster disaster recovery scenarios in switchover or failover process, 
 * whether the disaster recovery cluster has completed the final recovery
 */
Datum gs_hadr_in_recovery(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to gs_hadr_has_barrier_creator."))));

    if (knl_g_get_redo_finish_status()) {
        PG_RETURN_BOOL(false);
    }
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
    PG_RETURN_BOOL(true);
}

Datum gs_upload_obs_file(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    char* slotname = PG_GETARG_CSTRING(0);
    char* src = PG_GETARG_CSTRING(1);
    char* dest = PG_GETARG_CSTRING(2);
    char localFilePath[MAX_PATH_LEN] = {0};
    char netBackupPath[MAX_PATH_LEN] = {0};
    errno_t rc = 0;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("Must be system admin or operator admin in operation mode to gs_upload_obs_file."))));

    ArchiveSlotConfig *archive_conf = NULL;
    if ((archive_conf = getArchiveReplicationSlotWithName(slotname)) != NULL) {
        ArchiveConfig obsConfig;
        /* copy OBS configs to temporary variable for customising file path */
        rc = memcpy_s(&obsConfig, sizeof(ArchiveConfig), &archive_conf->archive_config, sizeof(ArchiveConfig));
        securec_check(rc, "", "");
        rc = snprintf_s(localFilePath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", t_thrd.proc_cxt.DataDir, src);
        securec_check_ss(rc, "\0", "\0");
        ereport(LOG, ((errmsg("There local file is %s.", localFilePath))));

        rc = snprintf_s(netBackupPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s",
                         archive_conf->archive_config.archive_prefix, dest);
        securec_check_ss(rc, "\0", "\0");
        ereport(LOG, ((errmsg("There netbackup file is %s.", netBackupPath))));

        if (UploadOneFileToOBS(localFilePath, netBackupPath, &obsConfig)) {
            ereport(LOG, (errmsg("[gs_upload_obs_file] upload obs file %s done", netBackupPath)));
        } else {
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                (errmsg("[gs_upload_obs_file] upload obs file %s failed", netBackupPath))));
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), (errmsg("There is no replication slots."))));
    }
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif

    PG_RETURN_VOID();
}

Datum gs_download_obs_file(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    char* slotname = PG_GETARG_CSTRING(0);
    char* src = PG_GETARG_CSTRING(1);
    char* dest = PG_GETARG_CSTRING(2);
    char netBackupPath[MAX_PATH_LEN] = {0};
    char localFilePath[MAX_PATH_LEN] = {0};
    errno_t rc = 0;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("Must be system admin or operator admin in operation mode to gs_download_obs_file."))));

    ArchiveSlotConfig *archive_conf = NULL;
    if (IS_CN_OBS_DISASTER_RECOVER_MODE) {
        archive_conf = GetArchiveRecoverySlot();
    } else {
        archive_conf = getArchiveReplicationSlotWithName(slotname);
    }
    if (archive_conf != NULL) {
        ArchiveConfig obsConfig;
        /* copy OBS configs to temporary variable for customising file path */
        rc = memcpy_s(&obsConfig, sizeof(ArchiveConfig), &archive_conf->archive_config, sizeof(ArchiveConfig));
        securec_check(rc, "", "");
        rc = snprintf_s(localFilePath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", t_thrd.proc_cxt.DataDir, dest);
        securec_check_ss(rc, "\0", "\0");
        ereport(LOG, ((errmsg("There local file path is %s.", localFilePath))));

        rc = snprintf_s(netBackupPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s",
            archive_conf->archive_config.archive_prefix, src);
        securec_check_ss(rc, "\0", "\0");
        ereport(LOG, ((errmsg("[gs_download_obs_file]There netbackup file is %s.", netBackupPath))));

        if (DownloadOneItemFromOBS(netBackupPath, localFilePath, &obsConfig)) {
            ereport(LOG, (errmsg("[gs_download_obs_file] download obs file %s done", netBackupPath)));
        } else {
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                (errmsg("[gs_download_obs_file] down obs file %s failed", netBackupPath))));
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), (errmsg("There is no replication slots."))));
    }
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif

    PG_RETURN_VOID();
}

Datum gs_get_obs_file_context(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    char fileContext[MAXPGPATH] = {0};
    size_t readLen = 0;
    char* setFileName = PG_GETARG_CSTRING(0);
    char* slotName = PG_GETARG_CSTRING(1);

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("Must be system admin or operator admin in operation mode to gs_get_obs_file_context."))));

    ArchiveSlotConfig *archive_conf = NULL;
    if (IS_CN_OBS_DISASTER_RECOVER_MODE) {
        archive_conf = GetArchiveRecoverySlot();
    } else {
        archive_conf = getArchiveReplicationSlotWithName(slotName);
    }
    if (archive_conf == NULL) {
        ereport(WARNING, (errcode(ERRCODE_NO_DATA_FOUND),
                (errmsg("The obs file from slot name %s cannot be found.", slotName))));
        PG_RETURN_NULL();
    }

    /* hadr only support OBS */
    if (archive_conf->archive_config.media_type != ARCHIVE_OBS) {
        ereport(WARNING, (errcode(ERRCODE_NO_DATA_FOUND),
                (errmsg("The slot %s is not obs slot.", slotName))));
        PG_RETURN_NULL();
    }

    if (checkOBSFileExist(setFileName, &archive_conf->archive_config) == false) {
        ereport(WARNING, (errcode(ERRCODE_NO_DATA_FOUND),
                    (errmsg("The file named %s cannot be found.", setFileName))));
        PG_RETURN_NULL();
    }
    readLen = obsRead(setFileName, 0, fileContext, MAXPGPATH, &archive_conf->archive_config);
    if (readLen == 0) {
        ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                (errmsg("Cannot read any context in %s file!", setFileName))));
    }

    PG_RETURN_TEXT_P(cstring_to_text(fileContext));
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_TEXT_P(NULL);
#endif
}

Datum gs_set_obs_file_context(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    int ret = 0;
    char* setFileName = PG_GETARG_CSTRING(0);
    char* setFileContext = PG_GETARG_CSTRING(1);
    char* slotName = PG_GETARG_CSTRING(2);

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("Must be system admin or operator admin in operation mode to gs_get_obs_file_context."))));

    ArchiveSlotConfig *archive_conf = NULL;
    if (IS_CN_OBS_DISASTER_RECOVER_MODE) {
        archive_conf = GetArchiveRecoverySlot();
    } else {
        archive_conf = getArchiveReplicationSlotWithName(slotName);
    }
    if (archive_conf == NULL) {
        ereport(WARNING, (errcode(ERRCODE_NO_DATA_FOUND),
                (errmsg("The obs file from slot name %s cannot be found.", slotName))));
        PG_RETURN_NULL();
    }

    /* hadr only support OBS */
    if (archive_conf->archive_config.media_type != ARCHIVE_OBS) {
        ereport(WARNING, (errcode(ERRCODE_NO_DATA_FOUND),
                (errmsg("The slot %s is not obs slot.", slotName))));
        PG_RETURN_NULL();
    }

    ret = obsWrite(setFileName, setFileContext, strlen(setFileContext), &archive_conf->archive_config);

    PG_RETURN_TEXT_P(cstring_to_text(setFileContext));
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_TEXT_P(NULL);
#endif
}

Datum gs_get_hadr_key_cn(PG_FUNCTION_ARGS) 
{
#ifndef ENABLE_LITE_MODE
#define GS_GET_HADR_KEY_CN_COLS 4
    bool needLocalKeyCn = false;
    char localKeyCn[MAXFNAMELEN] = {0};
    char hadrKeyCn[MAXFNAMELEN] = {0};
    char hadrDeleteCn[MAXPGPATH] = {0};
    Datum values[GS_GET_HADR_KEY_CN_COLS];
    bool isnull[GS_GET_HADR_KEY_CN_COLS];
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to gs_get_global_barrier_status."))));

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialize mode required, but it is not "
                                                                       "allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    /*
     * We don't require any special permission to see this function's data
     * because nothing should be sensitive. The most critical being the slot
     * name, which shouldn't contain anything particularly sensitive.
     */
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    (void)MemoryContextSwitchTo(oldcontext);

    List *all_slots =  NIL;
    if (IS_CN_OBS_DISASTER_RECOVER_MODE) {
        all_slots = GetAllRecoverySlotsName();
        needLocalKeyCn = true;
        int rc = 0;
        char* keyCN = NULL;
        keyCN = get_local_key_cn();
        rc = snprintf_s(localKeyCn, MAXFNAMELEN, MAXFNAMELEN - 1, "%s", keyCN);
        securec_check_ss(rc, "\0", "\0");
        pfree_ext(keyCN);
    } else {
        all_slots = GetAllArchiveSlotsName();
    }
    if (all_slots == NIL || all_slots->length == 0) {
        tuplestore_donestoring(tupstore);
        PG_RETURN_NULL();
    }

    foreach_cell(cell, all_slots) {
        char* slotname = (char*)lfirst(cell);
        bool isExitKey=  true;
        bool isExitDelete = true;
        ArchiveSlotConfig *archive_conf = NULL;
        if (IS_CN_OBS_DISASTER_RECOVER_MODE) {
            archive_conf = GetArchiveRecoverySlot();
        } else {
            archive_conf = getArchiveReplicationSlotWithName(slotname);
        }
        if (archive_conf != NULL) {
            if (archive_conf->archive_config.media_type == ARCHIVE_OBS) {
                get_hadr_cn_info((char *)hadrKeyCn, &isExitKey, (char *)hadrDeleteCn, &isExitDelete, archive_conf);
            } else {
                continue;
            }
        } else {
            tuplestore_donestoring(tupstore);
            PG_RETURN_NULL();
        }

        /*
         * Construct a tuple descriptor for the result row.
         */
        values[0] = CStringGetTextDatum(slotname);
        isnull[0] = false;
        values[1] = CStringGetTextDatum(localKeyCn);
        isnull[1] = needLocalKeyCn ? false : true;
        values[2] = CStringGetTextDatum(hadrKeyCn);
        isnull[2] = isExitKey ? false : true;
        values[3] = CStringGetTextDatum(hadrDeleteCn);
        isnull[3] = isExitDelete ? false : true;
        tuplestore_putvalues(tupstore, tupdesc, values, isnull);
    }
    list_free_deep(all_slots);
    tuplestore_donestoring(tupstore);
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
    PG_RETURN_DATUM(0);
}

Datum gs_streaming_dr_in_switchover(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to gs_streaming_dr_switchover."))));

    if (!g_instance.streaming_dr_cxt.isInSwitchover) {
        ereport(LOG, (errmsg("Streaming disaster recovery is not in switchover.")));
        PG_RETURN_BOOL(false);
    }    

#ifdef ENABLE_MULTIPLE_NODES
    char barrier_id[MAX_BARRIER_ID_LENGTH];
    int rc;
    rc = snprintf_s(barrier_id, MAX_BARRIER_ID_LENGTH, MAX_BARRIER_ID_LENGTH - 1, CSN_BARRIER_NAME);
    securec_check_ss_c(rc, "\0", "\0");

    RequestBarrier(barrier_id, NULL, true);
#else
    CreateHadrSwitchoverBarrier();
#endif
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
    PG_RETURN_BOOL(true);
}

Datum gs_streaming_dr_service_truncation_check(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    XLogRecPtr switchoverLsn = g_instance.streaming_dr_cxt.switchoverBarrierLsn;
    XLogRecPtr flushLsn = InvalidXLogRecPtr;
    bool isInteractionCompleted = false;
    int hadrWalSndNum = 0;
    int InteractionCompletedNum = 0;
    const uint32 shiftSize = 32;

    for (int i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile WalSnd *walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];
        if (walsnd->pid == 0) {
            continue;
        }

        if (walsnd->is_cross_cluster) {
            hadrWalSndNum++;
            SpinLockAcquire(&walsnd->mutex);
            flushLsn = walsnd->flush;
            isInteractionCompleted = walsnd->isInteractionCompleted;
            SpinLockRelease(&walsnd->mutex);
            if (isInteractionCompleted && 
                XLByteEQ(switchoverLsn, flushLsn)) {
                InteractionCompletedNum++;
            } else {
                ereport(LOG,
                        (errmsg("walsnd %d, the switchover Lsn is %X/%X, the hadr receiver flush Lsn is %X/%X",
                        i, (uint32)(switchoverLsn >> shiftSize), (uint32)switchoverLsn,
                        (uint32)(flushLsn >> shiftSize), (uint32)flushLsn)));
            }
        }
    }

    if (hadrWalSndNum != 0 && hadrWalSndNum == InteractionCompletedNum) {
        PG_RETURN_BOOL(true);
    } else {
        PG_RETURN_BOOL(false);
    }
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_BOOL(false);
#endif
}

Datum gs_streaming_dr_get_switchover_barrier(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to gs_streaming_dr_get_switchover_barrier."))));

    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    XLogRecPtr last_flush_location = walrcv->receiver_flush_location;
    XLogRecPtr last_replay_location = GetXLogReplayRecPtr(NULL);
    load_server_mode();

    ereport(LOG,
            (errmsg("is_hadr_main_standby: %d, is_cn: %d, last switchover Lsn is %X/%X, "
            "target switchover Lsn is %X/%X, last_replay_location %X/%X, last_flush_location %X/%X, ServerMode:%d",
            t_thrd.xlog_cxt.is_hadr_main_standby,
            IS_PGXC_COORDINATOR,
            (uint32)(walrcv->lastSwitchoverBarrierLSN >> 32), 
            (uint32)(walrcv->lastSwitchoverBarrierLSN),
            (uint32)(walrcv->targetSwitchoverBarrierLSN >> 32), 
            (uint32)(walrcv->targetSwitchoverBarrierLSN),
            (uint32)(last_replay_location >> 32), 
            (uint32)(last_replay_location),
            (uint32)(last_flush_location >> 32), 
            (uint32)(last_flush_location), t_thrd.xlog_cxt.server_mode)));

    if (t_thrd.xlog_cxt.server_mode != STANDBY_MODE) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("streaming switchover barrier can only be set in standby mode")));
    }
    if (t_thrd.xlog_cxt.is_hadr_main_standby || IS_PGXC_COORDINATOR) {
        g_instance.streaming_dr_cxt.isInSwitchover = true;
        if (g_instance.streaming_dr_cxt.isInteractionCompleted &&
            XLByteEQ(last_flush_location, last_replay_location) &&
            XLByteEQ(walrcv->targetSwitchoverBarrierLSN, last_replay_location)) {
            PG_RETURN_BOOL(true);
        }
    } 
    else {
        if (XLByteEQ(walrcv->lastSwitchoverBarrierLSN, last_replay_location)) {
            PG_RETURN_BOOL(true);
        }
    }
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif

    PG_RETURN_BOOL(false);
}

Datum gs_pitr_get_warning_for_xlog_force_recycle(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    if (g_instance.roach_cxt.isXLogForceRecycled) {
        PG_RETURN_BOOL(true);
    } else {
        PG_RETURN_BOOL(false);
    }
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_BOOL(false);
#endif
}

Datum gs_get_active_archiving_standby(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    int i;
    int rc;
    errno_t errorno = EOK;
    const int cols = 3;
    char standbyName[MAXPGPATH] = {0};
    char archiveLocation[MAXPGPATH] = {0};
    Datum values[cols];
    bool nulls[cols];
    int xlogFileCnt = 0;
    XLogRecPtr endLsn = InvalidXLogRecPtr;
    int j = 0;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore = NULL;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialize mode required, but it is not "
                                                                       "allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    /*
     * We don't require any special permission to see this function's data
     * because nothing should be sensitive. The most critical being the slot
     * name, which shouldn't contain anything particularly sensitive.
     */
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    (void)MemoryContextSwitchTo(oldcontext);
    if (!IS_PGXC_COORDINATOR) {
        if (g_instance.archive_obs_cxt.chosen_walsender_index == -1) {
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                (errmsg("Could not find the correct walsender for active archiving standby."))));
        }
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        int index = g_instance.archive_obs_cxt.chosen_walsender_index;
        volatile WalSnd *walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[index];
        SpinLockAcquire(&walsnd->mutex);
        if (walsnd->pid != 0 && ((walsnd->sendRole & SNDROLE_PRIMARY_STANDBY) == walsnd->sendRole)) {
            rc = strncpy_s(standbyName, MAXPGPATH, g_instance.rto_cxt.rto_standby_data[index].id,
                strlen(g_instance.rto_cxt.rto_standby_data[index].id));
            securec_check(rc, "\0", "\0");
        }
        SpinLockRelease(&walsnd->mutex);
    } else {
        rc = strncpy_s(standbyName, MAXPGPATH, g_instance.attr.attr_common.PGXCNodeName,
            strlen(g_instance.attr.attr_common.PGXCNodeName));
        securec_check(rc, "\0", "\0");
    }
    values[j++] = CStringGetTextDatum(standbyName);
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        char currArchiveLocation[MAXPGPATH] = {0};
        SpinLockAcquire(&slot->mutex);
        endLsn = slot->data.restart_lsn;
        SpinLockRelease(&slot->mutex);
        if (slot->in_use == true && slot->archive_config != NULL && slot->archive_config->is_recovery == false &&
            GET_SLOT_PERSISTENCY(slot->data) != RS_BACKUP) {
            rc = snprintf_s(currArchiveLocation, MAXPGPATH, MAXPGPATH - 1, "%08X/%08X ",
                (uint32)(endLsn >> 32), (uint32)(endLsn));
            securec_check_ss(errorno, "", "");
            xlogFileCnt += GetArchiveXLogFileTotalNum(slot->archive_config, endLsn);
            rc = strcat_s(archiveLocation, MAXPGPATH, currArchiveLocation);
            securec_check(rc, "\0", "\0");
        }
        continue;
    }
    values[j++] = CStringGetTextDatum(archiveLocation);
    values[j++] = Int32GetDatum(xlogFileCnt);
    tuplestore_putvalues(tupstore, tupdesc, values, nulls);

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);
    return (Datum)0;
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_DATUM(0);
#endif
}

#ifndef ENABLE_LITE_MODE
static bool checkIsDigit(const char* arg)
{
    int i = 0;
    while (arg[i] != '\0') {
        if (isdigit(arg[i]) == 0)
            return 0;
        i++;
    }
    return 1;
}
#endif

Datum gs_pitr_clean_history_global_barriers(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to "
                "gs_pitr_clean_history_global_barriers."))));
    }

    char* stopBarrierTimestamp = PG_GETARG_CSTRING(0);
    if (stopBarrierTimestamp == NULL || !checkIsDigit(stopBarrierTimestamp)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                (errmsg("Must input linux timestamp for gs_pitr_clean_history_global_barriers."))));
    }
    
    errno = 0;
    long barrierTimestamp = strtol(stopBarrierTimestamp, NULL, 10);
    if (errno == ERANGE) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            (errmsg("Must input linux timestamp for gs_pitr_clean_history_global_barriers."))));
    }
    barrierTimestamp *= msecPerSec;
    char* oldestBarrierForNow = DeleteStopBarrierRecordsOnMedia(barrierTimestamp);
    if (oldestBarrierForNow == NULL) {
        ereport(LOG, (errmsg("All barrier records have been deleted this time.")));
        PG_RETURN_TEXT_P(cstring_to_text("NULL"));
    }
    PG_RETURN_TEXT_P(cstring_to_text(oldestBarrierForNow));
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_TEXT_P(NULL);
#endif
}

Datum gs_pitr_archive_slot_force_advance(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE
    XLogSegNo currArchslotSegNo;
    XLogRecPtr archiveSlotLocNow = InvalidXLogRecPtr;
    char location[MAXFNAMELEN];
    get_slot_func slot_func;
    List *all_archive_slots = NIL;
    char globalBarrierId[MAX_BARRIER_ID_LENGTH] = {0};
    long endBarrierTimestamp = 0;
    int readLen = 0;
    char* oldestBarrierForNow = NULL;
    errno_t rc = EOK;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to "
                "gs_pitr_archive_slot_force_advance."))));
    }
    char* stopBarrierTimestamp = PG_GETARG_CSTRING(0);
    if (stopBarrierTimestamp == NULL || !checkIsDigit(stopBarrierTimestamp)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                (errmsg("Must input linux timestamp for gs_pitr_clean_history_global_barriers."))));
    }
    if (!g_instance.roach_cxt.isXLogForceRecycled) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), (errmsg("Must force advance when xlog is recycled"))));
    }

    errno = 0;
    long barrierTimestamp = strtol(stopBarrierTimestamp, NULL, 10);
    if (errno == ERANGE) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            (errmsg("Must input linux timestamp for gs_pitr_clean_history_global_barriers."))));
    }
    barrierTimestamp *= msecPerSec;
    all_archive_slots = GetAllArchiveSlotsName();
    if (all_archive_slots == NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            (errmsg("Archive slot does not exist."))));
    }
    slot_func = &getArchiveReplicationSlotWithName;
    foreach_cell(cell, all_archive_slots) {
        long currendTimestamp = 0;
        char* slotname = (char*)lfirst(cell);
        ArchiveSlotConfig *archive_conf = NULL;
        if ((archive_conf = slot_func(slotname)) == NULL) {
            readLen = ArchiveRead(HADR_BARRIER_ID_FILE, 0, globalBarrierId, MAX_BARRIER_ID_LENGTH,
                &archive_conf->archive_config);
            if (readLen == 0) {
                ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                    (errmsg("Cannot read global barrier ID in %s file!", HADR_BARRIER_ID_FILE))));
            }
            char *tmpPoint = strrchr(globalBarrierId, '_');
            if (tmpPoint == NULL) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    (errmsg("The hadr barrier id file record timestamp is invalid."))));
            }
            tmpPoint += 1;
            errno = 0;
            currendTimestamp = strtol(tmpPoint, NULL, 10);
            if (errno == ERANGE) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    (errmsg("Must input linux timestamp for gs_pitr_clean_history_global_barriers."))));
            }
        }
        if (endBarrierTimestamp == 0 || currendTimestamp > endBarrierTimestamp) {
            endBarrierTimestamp = currendTimestamp;
        }
    }
    oldestBarrierForNow = DeleteStopBarrierRecordsOnMedia(barrierTimestamp, endBarrierTimestamp);
    if (oldestBarrierForNow != NULL) {
        ereport(LOG, ((errmsg("[gs_pitr_archive_slot_force_advance]the last barrier record is %s.",
            oldestBarrierForNow))));
    }
    ereport(LOG, ((errmsg("[gs_pitr_archive_slot_force_advance]delete barrier record from %ld to %ld.",
        endBarrierTimestamp, barrierTimestamp))));
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        SpinLockAcquire(&slot->mutex);
        if (slot->in_use == true && slot->archive_config != NULL && slot->archive_config->is_recovery == false &&
            GET_SLOT_PERSISTENCY(slot->data) != RS_BACKUP) {
            XLByteToSeg(slot->data.restart_lsn, currArchslotSegNo);
            XLogSegNo lastRemovedSegno = XLogGetLastRemovedSegno();
            if (currArchslotSegNo <= lastRemovedSegno) {
                slot->data.restart_lsn = (lastRemovedSegno + 1) * XLogSegSize;
                archiveSlotLocNow = (lastRemovedSegno + 1) * XLogSegSize;
            }
        }
        SpinLockRelease(&slot->mutex);
    }

    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        if (g_instance.archive_thread_info.obsArchPID[i] != 0) {
            signal_child(g_instance.archive_thread_info.obsArchPID[i], SIGUSR2, -1);
        }
    }
    if (IS_PGXC_COORDINATOR) {
        g_instance.roach_cxt.isXLogForceRecycled = false;
    } else {
        g_instance.roach_cxt.forceAdvanceSlotTigger = true;
    }
    rc = snprintf_s(location, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X",
        (uint32)(archiveSlotLocNow >> 32), (uint32)(archiveSlotLocNow));
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(location));
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_TEXT_P(NULL);
#endif
}

Datum gs_get_standby_cluster_barrier_status(PG_FUNCTION_ARGS)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    XLogRecPtr barrierLsn = InvalidXLogRecPtr;
    char lastestbarrierId[MAX_BARRIER_ID_LENGTH] = {0};
    char recoveryBarrierId[MAX_BARRIER_ID_LENGTH] = {0};
    char targetBarrierId[MAX_BARRIER_ID_LENGTH] = {0};
    const uint32 PG_GET_STANDBY_BARRIER_STATUS_COLS = 4;
    Datum values[PG_GET_STANDBY_BARRIER_STATUS_COLS];
    bool isnull[PG_GET_STANDBY_BARRIER_STATUS_COLS];
    char barrierLocation[MAXFNAMELEN] = {0};
    TupleDesc resultTupleDesc;
    HeapTuple resultHeapTuple;
    Datum result;
    int rc = EOK;
    const uint32 shiftSize = 32;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        (errmsg("Must be system admin or operator admin in operation mode to "
                                "gs_get_standby_cluster_barrier_status."))));

    if (g_instance.attr.attr_common.stream_cluster_run_mode != RUN_MODE_STANDBY)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 (errmsg("gs_get_standby_cluster_barrier_status only support on standby-cluster-mode."))));

    rc = strncpy_s((char *)lastestbarrierId, MAX_BARRIER_ID_LENGTH, (char *)walrcv->lastReceivedBarrierId,
                   MAX_BARRIER_ID_LENGTH - 1);
    securec_check(rc, "\0", "\0");

    SpinLockAcquire(&walrcv->mutex);
    barrierLsn = walrcv->lastReceivedBarrierLSN;

    rc = snprintf_s(barrierLocation, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X/%08X", (uint32)(barrierLsn >> shiftSize),
                    (uint32)(barrierLsn));
    securec_check_ss(rc, "\0", "\0");

    rc = strncpy_s((char *)recoveryBarrierId, MAX_BARRIER_ID_LENGTH, (char *)walrcv->lastRecoveredBarrierId,
                   MAX_BARRIER_ID_LENGTH - 1);
    securec_check(rc, "\0", "\0");

    rc = strncpy_s((char *)targetBarrierId, MAX_BARRIER_ID_LENGTH, (char *)walrcv->recoveryTargetBarrierId,
                   MAX_BARRIER_ID_LENGTH - 1);
    securec_check(rc, "\0", "\0");
    SpinLockRelease(&walrcv->mutex);

    ereport(LOG, (errmsg("gs_get_standby_cluster_barrier_status get the barrier ID is %s, the lastReceivedBarrierId "
                         "is %s,recovery barrier ID is %s, target barrier ID is %s",
                         lastestbarrierId, walrcv->lastReceivedBarrierId, walrcv->lastRecoveredBarrierId,
                         walrcv->recoveryTargetBarrierId)));

    /*
     * Construct a tuple descriptor for the result row.
     */
    resultTupleDesc = CreateTemplateTupleDesc(PG_GET_STANDBY_BARRIER_STATUS_COLS, false, TAM_HEAP);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)1, "latest_id", TEXTOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)2, "barrier_lsn", TEXTOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)3, "recovery_id", TEXTOID, -1, 0);
    TupleDescInitEntry(resultTupleDesc, (AttrNumber)4, "target_id", TEXTOID, -1, 0);

    resultTupleDesc = BlessTupleDesc(resultTupleDesc);

    values[0] = CStringGetTextDatum(lastestbarrierId);
    isnull[0] = false;
    values[1] = CStringGetTextDatum(barrierLocation);
    isnull[1] = false;
    values[2] = CStringGetTextDatum(recoveryBarrierId);
    isnull[2] = false;
    values[3] = CStringGetTextDatum(targetBarrierId);
    isnull[3] = false;

    resultHeapTuple = heap_form_tuple(resultTupleDesc, values, isnull);

    result = HeapTupleGetDatum(resultHeapTuple);

    PG_RETURN_DATUM(result);
}

Datum gs_set_standby_cluster_target_barrier_id(PG_FUNCTION_ARGS)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    text *barrier = PG_GETARG_TEXT_P(0);
    char *barrierstr = NULL;
    char targetbarrier[MAX_BARRIER_ID_LENGTH];
    CommitSeqNo csn;
    int64 ts;
    char tmp[MAX_BARRIER_ID_LENGTH];
    errno_t errorno = EOK;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        (errmsg("Must be system admin or operator admin in operation mode to "
                                "gs_set_standby_cluster_target_barrier_id."))));
    }

    if (g_instance.attr.attr_common.stream_cluster_run_mode != RUN_MODE_STANDBY)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 (errmsg("gs_set_standby_cluster_target_barrier_id only support on standby-cluster-mode."))));

    barrierstr = text_to_cstring(barrier);

    /* test input string */
    int checkCsnBarrierRes = sscanf_s(barrierstr, "csn_%21lu_%13ld%s", &csn, &ts, &tmp, sizeof(tmp));
    int checkSwitchoverBarrierRes = sscanf_s(barrierstr, "csn_%21lu_%s", &csn, &tmp, sizeof(tmp));
    if (strlen(barrierstr) != (MAX_BARRIER_ID_LENGTH - 1) ||
        (checkCsnBarrierRes != 2 && (checkSwitchoverBarrierRes != 2 || strcmp(tmp, "dr_switchover") != 0)))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not parse barrier id %s", barrierstr)));

    errorno = snprintf_s(targetbarrier, MAX_BARRIER_ID_LENGTH, MAX_BARRIER_ID_LENGTH - 1, "%s", barrierstr);
    securec_check_ss(errorno, "", "");

    SpinLockAcquire(&walrcv->mutex);
    errorno = strncpy_s((char *)walrcv->recoveryTargetBarrierId, MAX_BARRIER_ID_LENGTH, (char *)barrierstr,
                        MAX_BARRIER_ID_LENGTH - 1);
    SpinLockRelease(&walrcv->mutex);
    securec_check(errorno, "\0", "\0");

    ereport(LOG, (errmsg("gs_set_standby_cluster_target_barrier_id set the barrier ID is %s", targetbarrier)));

    PG_RETURN_TEXT_P(cstring_to_text((char *)barrierstr));
}

Datum gs_query_standby_cluster_barrier_id_exist(PG_FUNCTION_ARGS)
{
    text *barrier = PG_GETARG_TEXT_P(0);
    char *barrierstr = NULL;
    CommitSeqNo *hentry = NULL;
    bool found = false;
    CommitSeqNo csn;
    int64 ts;
    char tmp[MAX_BARRIER_ID_LENGTH];

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        (errmsg("Must be system admin or operator admin in operation mode to "
                                "gs_query_standby_cluster_barrier_id_exist."))));
    }

    if (g_instance.attr.attr_common.stream_cluster_run_mode != RUN_MODE_STANDBY)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 (errmsg("gs_query_standby_cluster_barrier_id_exist only support on standby-cluster-mode."))));

    if (g_instance.csn_barrier_cxt.barrier_hash_table == NULL) {
        ereport(WARNING, (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), (errmsg("barrier hash table is NULL."))));
        PG_RETURN_BOOL(found);
    }

    barrierstr = text_to_cstring(barrier);

    /* test input string */
    int checkCsnBarrierRes = sscanf_s(barrierstr, "csn_%21lu_%13ld%s", &csn, &ts, &tmp, sizeof(tmp));
    int checkSwitchoverBarrierRes = sscanf_s(barrierstr, "csn_%21lu_%s", &csn, &tmp, sizeof(tmp));
    if (strlen(barrierstr) != (MAX_BARRIER_ID_LENGTH - 1) ||
        (checkCsnBarrierRes != 2 && (checkSwitchoverBarrierRes != 2 || strcmp(tmp, "dr_switchover") != 0)))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not parse barrier id %s", barrierstr)));

    ereport(LOG, (errmsg("gs_query_standby_cluster_barrier_id_exist query the barrier ID is %s", barrierstr)));

    LWLockAcquire(g_instance.csn_barrier_cxt.barrier_hashtbl_lock, LW_SHARED);
    hentry = (CommitSeqNo *)hash_search(g_instance.csn_barrier_cxt.barrier_hash_table,
                                        (void *)barrierstr, HASH_FIND, NULL);
    LWLockRelease(g_instance.csn_barrier_cxt.barrier_hashtbl_lock);
    if (hentry != NULL) {
        found = true;
    }
    PG_RETURN_BOOL(found);
}
