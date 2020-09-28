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

// #include "access/htup_details.h"
#include "access/transam.h"
#include "access/xlog_internal.h"
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "catalog/pg_type.h"
// #include "common/controldata_utils.h"
#include "utils/elog.h"
#include "catalog/pg_control.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"

ControlFileData* get_controlfile(const char *DataDir, bool *crc_ok_p);

static inline char* getDataDir()
{
    if (t_thrd.proc_cxt.DataDir != NULL && strlen(t_thrd.proc_cxt.DataDir) > 0) {
        return t_thrd.proc_cxt.DataDir;
    } else {
        char *dataDir = getenv("PGDATA");
        if (dataDir == NULL) {
            ereport(ERROR, (errmsg("Cannot open control file")));
            return NULL;
        }
        return dataDir;
    }
}

Datum pg_control_system(PG_FUNCTION_ARGS)
{
    Datum		values[4];
    bool		nulls[4];
    TupleDesc	tupdesc;
    HeapTuple	htup;
    ControlFileData *ControlFile;
    bool		crc_ok;

    /*
     * Construct a tuple descriptor for the result row.  This must match this
     * function's pg_proc entry!
     */
    tupdesc = CreateTemplateTupleDesc(4, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pg_control_version", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "catalog_version_no", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "system_identifier", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 4, "pg_control_last_modified", TIMESTAMPTZOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    /* read the control file */
    ControlFile = get_controlfile(getDataDir(), &crc_ok);
    if (!crc_ok)
        ereport(ERROR,
                (errmsg("calculated CRC checksum does not match value stored in file")));

    values[0] = Int32GetDatum(ControlFile->pg_control_version);
    nulls[0] = false;

    values[1] = Int32GetDatum(ControlFile->catalog_version_no);
    nulls[1] = false;

    values[2] = Int64GetDatum(ControlFile->system_identifier);
    nulls[2] = false;

    values[3] = TimestampTzGetDatum(time_t_to_timestamptz(ControlFile->time));
    nulls[3] = false;

    htup = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

/*
 * get_controlfile()
 *
 * Get controlfile values.  The result is returned as a palloc'd copy of the
 * control file data.
 *
 * crc_ok_p can be used by the caller to see whether the CRC of the control
 * file data is correct.
 */
ControlFileData* get_controlfile(const char *DataDir, bool *crc_ok_p)
{
    ControlFileData *ControlFile;
    int			fd;
    char		ControlFilePath[MAXPGPATH];
    pg_crc32c	crc;
    int			r;

    AssertArg(crc_ok_p);

    ControlFile = (ControlFileData*)palloc(sizeof(ControlFileData));
    snprintf(ControlFilePath, MAXPGPATH, "%s/global/pg_control", DataDir);

#ifndef FRONTEND
    if ((fd = OpenTransientFile(ControlFilePath, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR)) == -1)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not open file \"%s\" for reading: %m",  ControlFilePath)));
#else
    if ((fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0)) == -1)
    {
        pg_log_fatal("could not open file \"%s\" for reading: %m",
                     ControlFilePath);
        exit(EXIT_FAILURE);
    }
#endif

    r = read(fd, ControlFile, sizeof(ControlFileData));
    if (r != sizeof(ControlFileData))
    {
        if (r < 0)
#ifndef FRONTEND
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not read file \"%s\": %m", ControlFilePath)));
#else
        {
            pg_log_fatal("could not read file \"%s\": %m", ControlFilePath);
            exit(EXIT_FAILURE);
        }
#endif
        else
#ifndef FRONTEND
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("could not read file \"%s\": read %d of %zu",
                     ControlFilePath, r, sizeof(ControlFileData))));
#else
        {
            pg_log_fatal("could not read file \"%s\": read %d of %zu",   ControlFilePath, r, sizeof(ControlFileData));
            exit(EXIT_FAILURE);
        }
#endif
    }

#ifndef FRONTEND
    if (CloseTransientFile(fd))
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not close file \"%s\": %m",  ControlFilePath)));
#else
    if (close(fd))
    {
        pg_log_fatal("could not close file \"%s\": %m", ControlFilePath);
        exit(EXIT_FAILURE);
    }
#endif

    /* Check the CRC. */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc,
                (char *) ControlFile,
                offsetof(ControlFileData, crc));
    FIN_CRC32C(crc);

    *crc_ok_p = EQ_CRC32C(crc, ControlFile->crc);

    /* Make sure the control file is valid byte order. */
    if (ControlFile->pg_control_version % 65536 == 0 &&
        ControlFile->pg_control_version / 65536 != 0)
#ifndef FRONTEND
        elog(ERROR, _("byte ordering mismatch"));
#else
        pg_log_warning("possible byte ordering mismatch\n" 
            "The byte ordering used to store the pg_control file might not match the one\n" 
            "used by this program.  In that case the results below would be incorrect, and\n"
            "the PostgreSQL installation would be incompatible with this data directory.");
#endif

    return ControlFile;
}

Datum pg_control_checkpoint(PG_FUNCTION_ARGS)
{
    Datum		values[12];
    bool		nulls[12];
    TupleDesc	tupdesc;
    HeapTuple	htup;
    ControlFileData *controlFile;
    XLogSegNo	segno;
    char		xlogfilename[MAXFNAMELEN];
    bool		crc_ok;

    /*
     * Construct a tuple descriptor for the result row.  This must match this
     * function's pg_proc entry!
     */
    tupdesc = CreateTemplateTupleDesc(12, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "checkpoint_lsn", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "redo_lsn", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "redo_wal_file", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 4, "timeline_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 5, "full_page_writes", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 6, "next_oid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 7, "next_multixact_id", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 8, "next_multi_offset", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 9, "oldest_xid", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 10, "oldest_xid_dbid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 11, "oldest_active_xid", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 12, "checkpoint_time", TIMESTAMPTZOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    /* Read the control file. */
    controlFile = get_controlfile(getDataDir(), &crc_ok);
    if (!crc_ok)
        ereport(ERROR,
                (errmsg("calculated CRC checksum does not match value stored in file")));

    /*
     * Calculate name of the WAL file containing the latest checkpoint's REDO
     * start point.
     */
    XLByteToSeg(controlFile->checkPointCopy.redo, segno);

    XLogFileName(xlogfilename, controlFile->checkPointCopy.ThisTimeLineID, segno);

    /* Populate the values and null arrays */
    values[0] = LSNGetDatum(controlFile->checkPoint);
    nulls[0] = false;

    values[1] = LSNGetDatum(controlFile->checkPointCopy.redo);
    nulls[1] = false;

    values[2] = CStringGetTextDatum(xlogfilename);
    nulls[2] = false;

    values[3] = Int32GetDatum(controlFile->checkPointCopy.ThisTimeLineID);
    nulls[3] = false;

    values[4] = BoolGetDatum(controlFile->checkPointCopy.fullPageWrites);
    nulls[4] = false;

    values[5] = ObjectIdGetDatum(controlFile->checkPointCopy.nextOid);
    nulls[5] = false;

    values[6] = TransactionIdGetDatum(controlFile->checkPointCopy.nextMulti);
    nulls[6] = false;

    values[7] = TransactionIdGetDatum(controlFile->checkPointCopy.nextMultiOffset);
    nulls[7] = false;

    values[8] = TransactionIdGetDatum(controlFile->checkPointCopy.oldestXid);
    nulls[8] = false;

    values[9] = ObjectIdGetDatum(controlFile->checkPointCopy.oldestXidDB);
    nulls[9] = false;

    values[10] = TransactionIdGetDatum(controlFile->checkPointCopy.oldestActiveXid);
    nulls[10] = false;

    values[11] = TimestampTzGetDatum(time_t_to_timestamptz(controlFile->checkPointCopy.time));
    nulls[11] = false;

    htup = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}