/* -------------------------------------------------------------------------
 *
 * pg_controldata.cpp
 *
 * Routines to expose the contents of the control data file via
 * a set of SQL functions.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/pg_controldata.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/transam.h"
#include "access/xlog_internal.h"
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "catalog/pg_type.h"
#include "utils/elog.h"
#include "catalog/pg_control.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"

ControlFileData* GetControlfile(const char *dataDir, bool *crc_ok_p);

static inline char* GetDataDir()
{
    if (t_thrd.proc_cxt.DataDir != NULL && strlen(t_thrd.proc_cxt.DataDir) > 0) {
        return t_thrd.proc_cxt.DataDir;
    } else {
        char *dataDir = gs_getenv_r("GAUSSHOME");
        if (dataDir == NULL) {
            ereport(ERROR, (errmsg("Get GAUSSHOME failed, please check.")));
            return NULL;
        }
        char realDir[PATH_MAX + 1] = {'\0'};
        if (realpath(dataDir, realDir) == NULL) {
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR), errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("Failed to obtain environment value $GAUSSHOME!"),
                errdetail("N/A"),
                errcause("Incorrect environment value."),
                erraction("Please refer to backend log for more details.")));
        }
        dataDir = NULL;
        check_backend_env(realDir);
        return pstrdup(realDir);
    }
}

const int CONTROL_SYSTEM_ATTRNUM = 4;
Datum pg_control_system(PG_FUNCTION_ARGS)
{
    Datum values[CONTROL_SYSTEM_ATTRNUM];
    bool nulls[CONTROL_SYSTEM_ATTRNUM];
    TupleDesc tupdesc;
    HeapTuple htup;
    ControlFileData *controlFile;
    bool crcOk;
    int i = 0;

    /*
     * Construct a tuple descriptor for the result row.  This must match this
     * function's pg_proc entry!
     */
    tupdesc = CreateTemplateTupleDesc(CONTROL_SYSTEM_ATTRNUM, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "pg_control_version", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "catalog_version_no", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "system_identifier", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "pg_control_last_modified", TIMESTAMPTZOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    /* read the control file */
    controlFile = GetControlfile(GetDataDir(), &crcOk);
    if (!crcOk) {
        ereport(ERROR,
                (errmsg("calculated CRC checksum does not match value stored in file")));
    }

    i = 0;
    values[i] = Int32GetDatum(controlFile->pg_control_version);
    nulls[i] = false;
    i++;

    values[i] = Int32GetDatum(controlFile->catalog_version_no);
    nulls[i] = false;
    i++;

    values[i] = Int64GetDatum(controlFile->system_identifier);
    nulls[i] = false;
    i++;

    values[i] = TimestampTzGetDatum(time_t_to_timestamptz(controlFile->time));
    nulls[i] = false;

    htup = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

static int OpenControlFile(const char* controlFilePath)
{
    int fd;

    if ((fd = OpenTransientFile((char*)controlFilePath,
        O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR)) == -1) {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not open file \"%s\" for reading: %m",  controlFilePath)));
    }

    return fd;
}

const int ORDER_BYTE = 65536;
/*
 * get_controlfile()
 *
 * Get controlfile values.  The result is returned as a palloc'd copy of the
 * control file data.
 *
 * crc_ok_p can be used by the caller to see whether the CRC of the control
 * file data is correct.
 */
ControlFileData* GetControlfile(const char *dataDir, bool *crc_ok_p)
{
    ControlFileData *controlFile;
    int fd;
    char controlFilePath[MAXPGPATH];
    pg_crc32c crc;
    int r;

    AssertArg(crc_ok_p);

    controlFile = (ControlFileData*)palloc(sizeof(ControlFileData));
    errno_t rc = snprintf_s(controlFilePath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_control", dataDir);
    securec_check_ss_c(rc, "\0", "\0");

    fd = OpenControlFile(controlFilePath);
    r = read(fd, controlFile, sizeof(ControlFileData));
    if (r != sizeof(ControlFileData)) {
        if (r < 0) {
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not read file \"%s\": %m", controlFilePath)));
        } else {
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("could not read file \"%s\": read %d of %zu",
                     controlFilePath, r, sizeof(ControlFileData))));
        }
    }

    if (CloseTransientFile(fd)) {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not close file \"%s\": %m",  controlFilePath)));
    }

    /* Check the CRC. */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc,
                (char *) controlFile,
                offsetof(ControlFileData, crc));
    FIN_CRC32C(crc);

    *crc_ok_p = EQ_CRC32C(crc, controlFile->crc);

    /* Make sure the control file is valid byte order. */
    if (controlFile->pg_control_version % ORDER_BYTE == 0 &&
        controlFile->pg_control_version / ORDER_BYTE != 0) {
        elog(ERROR, _("byte ordering mismatch"));
    }

    return controlFile;
}

static void InitCheckpointTupledesc(TupleDesc tupdesc)
{
    int i = 0;

    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "checkpoint_lsn", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "redo_lsn", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "redo_wal_file", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "timeline_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "full_page_writes", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "next_oid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "next_multixact_id", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "next_multi_offset", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "oldest_xid", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "oldest_xid_dbid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "oldest_active_xid", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "checkpoint_time", TIMESTAMPTZOID, -1, 0);
}

const int CONTROL_CHECKPOINT_ATTRNUM = 12;
Datum pg_control_checkpoint(PG_FUNCTION_ARGS)
{
    Datum values[CONTROL_CHECKPOINT_ATTRNUM];
    bool nulls[CONTROL_CHECKPOINT_ATTRNUM];
    TupleDesc tupdesc;
    HeapTuple htup;
    ControlFileData *controlFile;
    XLogSegNo segno;
    char xlogfilename[MAXFNAMELEN];
    bool crcOk;
    int i = 0;

    /*
     * Construct a tuple descriptor for the result row.  This must match this
     * function's pg_proc entry!
     */
    tupdesc = CreateTemplateTupleDesc(CONTROL_CHECKPOINT_ATTRNUM, false);
    InitCheckpointTupledesc(tupdesc);
    tupdesc = BlessTupleDesc(tupdesc);

    /* Read the control file. */
    controlFile = GetControlfile(GetDataDir(), &crcOk);
    if (!crcOk) {
        ereport(ERROR,
                (errmsg("calculated CRC checksum does not match value stored in file")));
    }

    /*
     * Calculate name of the WAL file containing the latest checkpoint's REDO
     * start point.
     */
    XLByteToSeg(controlFile->checkPointCopy.redo, segno);

    XLogFileName(xlogfilename, MAXFNAMELEN, controlFile->checkPointCopy.ThisTimeLineID, segno);

    i = 0;
    /* Populate the values and null arrays */
    values[i] = LSNGetDatum(controlFile->checkPoint);
    nulls[i] = false;
    i++;

    values[i] = LSNGetDatum(controlFile->checkPointCopy.redo);
    nulls[i] = false;
    i++;

    values[i] = CStringGetTextDatum(xlogfilename);
    nulls[i] = false;
    i++;

    values[i] = Int32GetDatum(controlFile->checkPointCopy.ThisTimeLineID);
    nulls[i] = false;
    i++;

    values[i] = BoolGetDatum(controlFile->checkPointCopy.fullPageWrites);
    nulls[i] = false;

    values[i] = ObjectIdGetDatum(controlFile->checkPointCopy.nextOid);
    nulls[i] = false;
    i++;

    values[i] = TransactionIdGetDatum(controlFile->checkPointCopy.nextMulti);
    nulls[i] = false;
    i++;

    values[i] = TransactionIdGetDatum(controlFile->checkPointCopy.nextMultiOffset);
    nulls[i] = false;
    i++;

    values[i] = TransactionIdGetDatum(controlFile->checkPointCopy.oldestXid);
    nulls[i] = false;
    i++;

    values[i] = ObjectIdGetDatum(controlFile->checkPointCopy.oldestXidDB);
    nulls[i] = false;
    i++;

    values[i] = TransactionIdGetDatum(controlFile->checkPointCopy.oldestActiveXid);
    nulls[i] = false;
    i++;

    values[i] = TimestampTzGetDatum(time_t_to_timestamptz(controlFile->checkPointCopy.time));
    nulls[i] = false;

    htup = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
